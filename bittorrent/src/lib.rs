use std::{
    io,
    net::{SocketAddrV4, TcpListener},
    os::fd::AsRawFd,
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use event_loop::{Event, EventLoop, UserData};
use file_store::FileStore;
use io_uring::{
    opcode,
    types::{self},
    IoUring,
};
use peer_connection::PeerConnection;
use piece_selector::{Piece, PieceSelector};
use sha1::Digest;
use slab::Slab;
use thiserror::Error;

mod buf_pool;
mod buf_ring;
mod event_loop;
mod file_store;
mod peer_connection;
mod peer_protocol;
mod piece_selector;

pub use peer_protocol::generate_peer_id;
pub use peer_protocol::PeerId;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Peer is being disconnected, Reason {0}")]
    Disconnect(&'static str),
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
        let event_idx = events.insert(Event::Accept);
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
            max_unchoked: 4,
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
                file_store.close().unwrap();
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

fn tick(
    tick_delta: &Duration,
    connections: &mut Slab<PeerConnection>,
    torrent_state: &mut TorrentState,
) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
    // 1. Calculate bandwidth (deal with initial start up)
    // 2. Go through them in order
    // 3. select pieces
    // TODO use retain instead of iter mut in the loop below and get rid of this
    let mut disconnects = Vec::new();
    for (id, connection) in connections.iter_mut() {
        // If this connection have no inflight 2 iterations in a row
        // disconnect it and clear all pieces it was currently downloading
        // this is not very granular and will need tweaking

        if let Some(time) = connection.timeout_point {
            if time.elapsed() > connection.request_timeout() {
                log::warn!("TIMEOUT: {id}");
                connection.on_request_timeout(torrent_state);
            }
        }

        if connection.pending_disconnect {
            log::debug!("Disconnect: {id}");
            disconnects.push(id);
            continue;
        }

        if !connection.peer_choking {
            // slow start win size increase is handled in update_stats
            if !connection.slow_start {
                // From the libtorrent impl, request queue time = 3
                let new_queue_capacity =
                    3 * connection.throughput / piece_selector::SUBPIECE_SIZE as u64;
                connection.desired_queue_size = new_queue_capacity as usize;
                log::debug!("Updated desired queue size: {new_queue_capacity}");
                connection.desired_queue_size = connection.desired_queue_size.clamp(0, 500);
            }
            connection.desired_queue_size = connection.desired_queue_size.max(1);
        }
        log::info!(
            "[Peer {}]: throughput: {} bytes/s, queue: {}/{}, time between subpieces: {}ms, currently_downloading: {}",
            connection.peer_id,
            connection.throughput,
            connection.queued.len(),
            connection.desired_queue_size,
            connection.moving_rtt.mean().as_millis(),
            connection.currently_downloading.len()
        );
        if !connection.peer_choking
            && connection.slow_start
            && connection.throughput > 0
            && connection.throughput < connection.prev_throughput + 5000
        {
            log::debug!("[Peer {}] Exiting slow start", connection.peer_id);
            connection.slow_start = false;
        }
        connection.prev_throughput = connection.throughput;
        connection.throughput = 0;
        // TODO: add to throughput total stats
    }

    // Request new pieces and fill up request queues
    let mut peer_bandwidth: Vec<_> = connections
        .iter_mut()
        .map(|(key, peer)| (key, peer.remaining_request_queue_spots()))
        .collect();

    peer_bandwidth.sort_unstable_by(|(_, a), (_, b)| a.cmp(b).reverse());
    for (peer_key, mut bandwidth) in peer_bandwidth {
        //dbg!(bandwidth);
        let peer = &mut connections[peer_key];

        while {
            let bandwitdth_available_for_new_piece =
                bandwidth > (torrent_state.piece_selector.avg_num_subpieces() as usize / 2);
            let first_piece = peer.currently_downloading.is_empty();
            (bandwitdth_available_for_new_piece || first_piece) && !peer.peer_choking
        } {
            if let Some(next_piece) = torrent_state.piece_selector.next_piece(peer_key) {
                if peer.is_choking {
                    // TODO highly unclear if unchoke is desired here
                    //if let Err(err) = peer.unchoke() {
                    //   log::error!("{err}");
                    //disconnects.push(peer_key);
                    break;
                    //} else {
                    //   self.num_unchoked += 1;
                    //}
                }
                peer.currently_downloading.push(Piece::new(
                    next_piece,
                    torrent_state.piece_selector.piece_len(next_piece),
                ));
                // Remove all subpieces from available bandwidth
                bandwidth -= (torrent_state.piece_selector.num_subpieces(next_piece) as usize)
                    .min(bandwidth);
                torrent_state
                    .piece_selector
                    .mark_inflight(next_piece as usize);
            } else {
                break;
            }
        }
        peer.fill_request_queue();
    }

    for id in disconnects {
        let mut conn = connections.remove(id);
        conn.release_pieces(torrent_state);
    }
}
