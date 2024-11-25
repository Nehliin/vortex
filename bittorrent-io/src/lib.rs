use std::{
    net::{SocketAddr, TcpListener},
    os::fd::AsRawFd,
    time::Duration,
};

use buf_pool::BufferPool;
use buf_ring::{Bid, BufferRing};
use event_loop::{Event, EventLoop, UserData};
use file_store::FileStore;
use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring, SubmissionQueue,
};
use lava_torrent::torrent::v1::Torrent;
use peer_connection::PeerConnection;
use peer_protocol::generate_peer_id;
use piece_selector::PieceSelector;
use sha1::Digest;
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

mod buf_pool;
mod buf_ring;
mod event_loop;
mod file_store;
mod peer_connection;
mod peer_protocol;
mod piece_selector;

pub fn setup_listener(torrent_state: TorrentState) {
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .setup_clamp()
        .setup_cqsize(1024)
        .setup_defer_taskrun()
        .build(1024)
        .unwrap();

    let mut events = Slab::with_capacity(256);
    let event_idx = events.insert(Event::Accept);
    let user_data = UserData::new(event_idx, None);

    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
        .build()
        .user_data(user_data.as_u64());

    unsafe {
        ring.submission().push(&accept_op).unwrap();
    }
    ring.submission().sync();

    let our_id = generate_peer_id();
    let mut event_loop = EventLoop::new(our_id, events);

    event_loop.run(ring, torrent_state, tick).unwrap()
}

pub struct TorrentState {
    info_hash: [u8; 20],
    piece_selector: PieceSelector,
    torrent_info: Torrent,
    num_unchoked: u32,
    max_unchoked: u32,
    file_store: FileStore,
}

impl TorrentState {
    pub fn new(torrent: Torrent) -> Self {
        let info_hash = torrent.info_hash_bytes().try_into().unwrap();
        Self {
            info_hash,
            piece_selector: PieceSelector::new(&torrent),
            torrent_info: torrent,
            num_unchoked: 0,
            max_unchoked: 4,
            file_store: Default::default(),
        }
    }

    pub(crate) fn on_piece_completed(&mut self, index: i32, data: Vec<u8>) {
        let hash_time = Instant::now();
        let mut hasher = sha1::Sha1::new();
        hasher.update(&data);
        let data_hash = hasher.finalize();
        let hash_time = hash_time.elapsed();
        log::info!("Piece hashed in: {} microsec", hash_time.as_micros());
        // The hash can be provided to the data storage or the peer connection
        // when the piece is requested so it can be used for validation later on
        let position = self
            .torrent_info
            .pieces
            .iter()
            .position(|piece_hash| data_hash.as_slice() == piece_hash);
        match position {
            Some(piece_index) if piece_index == index as usize => {
                log::info!("Piece hash matched downloaded data");
                self.piece_selector.mark_complete(piece_index);
                self.file_store.write_piece(index, &data).unwrap();

                // Purge disconnected peers TODO move to tick instead
                //self.peer_list.connections.retain(|_, peer| {
                 //   peer.have(index).is_ok()
                //});

                if self.piece_selector.completed_all() {
                    log::info!("Torrent completed!");
                    let file_store = std::mem::take(&mut self.file_store);
                    file_store.close().unwrap();
                    //self.download_complete_tx.take().unwrap().send(()).unwrap();
                }
            }
            Some(piece_index) => log::error!(
                    "Piece hash didn't match expected index! expected index: {index}, piece_index: {piece_index}"
            ),
            None => {
                log::error!("Piece sha1 hash not found!");
            }
        }
    }

    pub fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }
}

pub fn connect_to(addr: SocketAddr, torrent_state: TorrentState) {
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .setup_clamp()
        .setup_cqsize(1024)
        .setup_defer_taskrun()
        .setup_coop_taskrun()
        .build(1024)
        .unwrap();

    let mut events = Slab::with_capacity(256);
    let stream = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();

    let event_idx = events.insert(Event::Connect {
        addr,
        fd: stream.as_raw_fd(),
    });
    let user_data = UserData::new(event_idx, None);
    let addr = SockAddr::from(addr);
    let connect_op = opcode::Connect::new(
        types::Fd(stream.as_raw_fd()),
        addr.as_ptr() as *const _,
        addr.len(),
    )
    .build()
    //    .flags(io_uring::squeue::Flags::IO_LINK)
    .user_data(user_data.as_u64());
    unsafe {
        ring.submission().push(&connect_op).unwrap();
    }
    //   let timeout_op = opcode::LinkTimeout::new(TIMESPEC).build().user_data(0xdead);
    //  unsafe {
    //     ring.submission().push(&timeout_op).unwrap();
    //}

    ring.submission().sync();

    // event loop
    let our_id = generate_peer_id();
    let mut event_loop = EventLoop::new(our_id, events);
    event_loop.run(ring, torrent_state, tick).unwrap()
}

// Validate hashes in here and simply use one shot channels
fn tick(tick_delta: &Duration, connections: &mut Slab<PeerConnection>) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
    // 1. Calculate bandwidth (deal with initial start up)
    // 2. Go through them in order
    // 3. select pieces
}
