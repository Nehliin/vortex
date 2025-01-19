use std::{
    io,
    net::{SocketAddrV4, TcpListener},
    os::fd::AsRawFd,
    sync::mpsc::Receiver,
    time::Instant,
};

use event_loop::{tick, EventLoop, EventType};
use file_store::FileStore;
use io_uring::{
    opcode,
    types::{self},
    IoUring,
};
use io_utils::UserData;
use piece_selector::PieceSelector;
use sha1::Digest;
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

    pub fn start(&self, peer_provider: Receiver<SocketAddrV4>) -> Result<(), Error> {
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

        let listener = TcpListener::bind(("0.0.0.0", 3456)).unwrap();
        let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
            .build()
            .user_data(user_data.as_u64());

        unsafe {
            ring.submission().push(&accept_op).unwrap();
        }
        ring.submission().sync();

        let mut event_loop = EventLoop::new(self.our_id, events, peer_provider);

        let torrent_state = TorrentState::new(self.torrent_info.clone());

        event_loop.run(ring, torrent_state, tick)
    }
}

struct TorrentState {
    info_hash: [u8; 20],
    piece_selector: PieceSelector,
    torrent_info: lava_torrent::torrent::v1::Torrent,
    num_unchoked: u32,
    max_unchoked: u32,
    file_store: FileStore,
    is_complete: bool,
}

impl TorrentState {
    pub fn new(torrent: lava_torrent::torrent::v1::Torrent) -> Self {
        let info_hash = torrent.info_hash_bytes().try_into().unwrap();
        let file_store =
            FileStore::new("/home/popuser/vortex/bittorrent/downloaded/", &torrent).unwrap();
        Self {
            info_hash,
            piece_selector: PieceSelector::new(&torrent),
            torrent_info: torrent,
            num_unchoked: 0,
            max_unchoked: 8,
            file_store,
            is_complete: false,
        }
    }

    #[inline]
    pub fn num_pieces(&self) -> usize {
        self.torrent_info.pieces.len()
    }

    // Returns if the piece is valid
    pub(crate) fn on_piece_completed(&mut self, index: i32, data: Vec<u8>) -> bool {
        let hash_time = Instant::now();
        let mut hasher = sha1::Sha1::new();
        hasher.update(&data);
        let data_hash = hasher.finalize();
        let hash_time = hash_time.elapsed();
        log::info!("Piece hashed in: {} microsec", hash_time.as_micros());
        // The hash can be provided to the data storage or the peer connection
        // when the piece is requested so it can be used for validation later on
        let expected_hash = &self.torrent_info.pieces[index as usize];
        if expected_hash == data_hash.as_slice() {
            log::info!("Piece hash matched downloaded data");
            self.piece_selector.mark_complete(index as usize);
            self.file_store.write_piece(index, &data).unwrap();

            // Purge disconnected peers TODO move to tick instead
            //self.peer_list.connections.retain(|_, peer| {
            //   peer.have(index).is_ok()
            //});
            if self.piece_selector.completed_all() {
                let file_store = std::mem::replace(&mut self.file_store, FileStore::dummy());
                file_store.sync().unwrap();
                self.is_complete = true;
            }
            true
        } else {
            log::error!("Piece hash didn't match expected hash!");
            self.piece_selector.mark_not_inflight(index as usize);
            false
        }
    }

    pub fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }
}
