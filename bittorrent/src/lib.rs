use std::{
    io,
    net::{SocketAddrV4, TcpListener},
    os::fd::AsRawFd,
    path::Path,
    sync::mpsc::{Receiver, Sender},
};

use event_loop::{EventLoop, EventType};
use file_store::FileStore;
use io_uring::{
    IoUring, opcode,
    types::{self},
};
use io_utils::UserData;
use piece_selector::{CompletedPiece, PieceSelector};
use slab::Slab;
use thiserror::Error;

mod buf_pool;
mod buf_ring;
mod event_loop;
mod file_store;
mod io_utils;
mod peer_comm;
mod piece_selector;

use peer_comm::*;

#[cfg(feature = "fuzzing")]
pub use peer_protocol::*;

pub use peer_protocol::PeerId;
pub use peer_protocol::generate_peer_id;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Encountered IO issue: {0}")]
    Io(#[from] io::Error),
    #[error("Peer provider disconnected")]
    PeerProviderDisconnect,
}

pub struct Torrent {
    our_id: PeerId,
    torrent_info: lava_torrent::torrent::v1::Torrent,
}

impl Torrent {
    pub fn new(torrent_info: lava_torrent::torrent::v1::Torrent, our_id: PeerId) -> Self {
        Self {
            our_id,
            torrent_info,
        }
    }

    pub fn start(
        &self,
        peer_provider: Receiver<SocketAddrV4>,
        downloads_path: impl AsRef<Path>,
    ) -> Result<(), Error> {
        // check ulimit
        let mut ring: IoUring = IoUring::builder()
            .setup_single_issuer()
            .setup_clamp()
            .setup_cqsize(4096)
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(4096)
            .unwrap();

        let mut events = Slab::with_capacity(4096);
        let event_idx = events.insert(EventType::Accept);
        let user_data = UserData::new(event_idx, None);

        let listener = TcpListener::bind(("0.0.0.0", 6881)).unwrap();
        let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
            .build()
            .user_data(user_data.as_u64());

        unsafe {
            ring.submission().push(&accept_op).unwrap();
        }
        ring.submission().sync();
        let file_store = FileStore::new(downloads_path, &self.torrent_info).unwrap();
        let torrent_state = TorrentState::new(&self.torrent_info);
        let mut event_loop = EventLoop::new(self.our_id, events, peer_provider);
        event_loop.run(ring, torrent_state, &file_store, &self.torrent_info)
    }
}

struct TorrentState {
    info_hash: [u8; 20],
    piece_selector: PieceSelector,
    num_unchoked: u32,
    max_unchoked: u32,
    num_pieces: usize,
    completed_piece_rc: Receiver<CompletedPiece>,
    completed_piece_tx: Sender<CompletedPiece>,
    is_complete: bool,
}

impl TorrentState {
    pub fn new(torrent: &lava_torrent::torrent::v1::Torrent) -> Self {
        let info_hash = torrent.info_hash_bytes().try_into().unwrap();

        let (tx, rc) = std::sync::mpsc::channel();
        Self {
            info_hash,
            piece_selector: PieceSelector::new(torrent),
            num_pieces: torrent.pieces.len(),
            num_unchoked: 0,
            max_unchoked: 8,
            completed_piece_rc: rc,
            completed_piece_tx: tx,
            is_complete: false,
        }
    }

    #[inline]
    pub fn num_pieces(&self) -> usize {
        self.num_pieces
    }

    pub(crate) fn update_torrent_status(&mut self) {
        while let Ok(completed_piece) = self.completed_piece_rc.try_recv() {
            match completed_piece.hash_matched {
                Ok(hash_matched) => {
                    if hash_matched {
                        self.piece_selector.mark_complete(completed_piece.index);
                        log::info!(
                            "Piece {}/{} completed!",
                            self.piece_selector.total_completed(),
                            self.piece_selector.pieces()
                        );
                        // TODO: send have messages
                        if self.piece_selector.completed_all() {
                            self.is_complete = true;
                        }
                    } else {
                        log::error!("Piece hash didn't match expected hash!");
                        self.piece_selector.mark_not_inflight(completed_piece.index);
                    }
                }
                Err(err) => {
                    log::error!(
                        "Failed to sync and hash piece: {} Error: {err}",
                        completed_piece.index
                    );
                }
            }
        }
    }

    // TODO: Something like release in flight pieces?

    pub fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }
}

/// Common test setup utils
#[cfg(test)]
mod test_utils {
    use std::{collections::HashMap, path::PathBuf};

    use crate::{
        TorrentState, file_store::FileStore, generate_peer_id, peer_connection::PeerConnection,
        piece_selector::SUBPIECE_SIZE,
    };
    use lava_torrent::torrent::v1::{Torrent, TorrentBuilder};
    use socket2::{Domain, Protocol, Socket, Type};

    pub fn generate_peer<'a>(fast_ext: bool, conn_id: usize) -> PeerConnection<'a> {
        let socket_a = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
        PeerConnection::new(socket_a, generate_peer_id(), conn_id, fast_ext)
    }

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(path: &str) -> Self {
            let path = format!("/tmp/{path}");
            std::fs::create_dir_all(&path).unwrap();
            let path: PathBuf = path.into();
            Self {
                path: std::fs::canonicalize(path).unwrap(),
            }
        }

        fn add_file(&self, file_path: &str, data: &[u8]) {
            let file_path = self.path.as_path().join(file_path);
            if let Some(parent) = file_path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(file_path, data).unwrap();
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.path).unwrap();
        }
    }

    pub fn setup_torrent(
        torrent_name: &str,
        piece_len: usize,
        file_data: HashMap<String, Vec<u8>>,
    ) -> (FileStore, Torrent) {
        let torrent_tmp_dir = TempDir::new(&format!("{torrent_name}_torrent"));
        let download_tmp_dir = TempDir::new(&format!("{torrent_name}_download_dir"));
        file_data.iter().for_each(|(path, data)| {
            torrent_tmp_dir.add_file(path, data);
        });

        let torrent_info = TorrentBuilder::new(&torrent_tmp_dir.path, piece_len as i64)
            .set_name(torrent_name.to_string())
            .build()
            .unwrap();

        let download_tmp_dir_path = download_tmp_dir.path.clone();
        let file_store = FileStore::new(&download_tmp_dir_path, &torrent_info).unwrap();
        (file_store, torrent_info)
    }

    pub fn setup_test() -> (FileStore, Torrent, TorrentState) {
        let files: HashMap<String, Vec<u8>> = [
            ("f1.txt".to_owned(), vec![1_u8; 64]),
            ("f2.txt".to_owned(), vec![2_u8; 100]),
            ("f3.txt".to_owned(), vec![3_u8; SUBPIECE_SIZE as usize * 16]),
        ]
        .into_iter()
        .collect();
        let (file_store, torrent_info) = setup_torrent(
            &format!("{}", rand::random::<u16>()),
            (SUBPIECE_SIZE * 2) as usize,
            files,
        );

        let torrent_state = TorrentState::new(&torrent_info);
        (file_store, torrent_info, torrent_state)
    }
}
