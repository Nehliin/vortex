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
    opcode,
    types::{self},
    IoUring,
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
mod peer_connection;
mod peer_protocol;
mod piece_selector;

#[cfg(feature = "fuzzing")]
pub use peer_protocol::*;

pub use peer_protocol::generate_peer_id;
pub use peer_protocol::PeerId;

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
