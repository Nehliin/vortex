use std::{
    cell::OnceCell,
    collections::VecDeque,
    io::{self},
    net::{SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant},
};

use crate::{
    buf_pool::Buffer, event_loop::{ConnectionId, EventLoop}, file_store::DiskOpType, peer_comm::peer_protocol::PeerId
};
use crate::{
    file_store::DiskOp,
    piece_selector::{CompletedPiece, Piece, PieceSelector, SUBPIECE_SIZE, Subpiece},
};
use crate::{file_store::FileStore, peer_connection::PeerConnection};
use ahash::HashSetExt;
use bitvec::{boxed::BitBox, order::Msb0, vec::BitVec};
use heapless::spsc::Producer;
use io_uring::IoUring;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use slotmap::SlotMap;
use thiserror::Error;

use crate::peer_connection::DisconnectReason;

use crate::TorrentMetadata;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Encountered IO issue: {0}")]
    Io(#[from] io::Error),
    #[error("Peer provider disconnected")]
    PeerProviderDisconnect,
}

pub const CQE_WAIT_TIME_NS: u32 = 150_000_000;

/// Configuration settings for a given torrent
#[derive(Debug, Clone, Copy)]
pub struct Config {
    /// The max number of total connections that can be open at a time for the torrent
    pub max_connections: usize,
    /// The max number of outstanding requests this vortex should report to other peers via the
    /// `reqq` extension. This currently does not impact the behavoir of vortex but it should impact
    /// the amount of load connected peers send to us. Vortex does respect connected peers reported
    /// `reqq` value.
    pub max_reported_outstanding_requests: u64,
    /// The maximal amount of allowed unchoked peers at any given time
    pub max_unchoked: u32,
    /// Controls how frequently "which peers should be unchoked" is calculated.
    /// After this number of ticks, vortex will look over all peers and redistribute
    /// which ones are unchoked and not.
    pub num_ticks_before_unchoke_recalc: u32,
    /// Controls the longest possible interval "which peers should be optimistically unchoked" is calculated.
    /// After this number of ticks, vortex will look over all peers and redistribute
    /// which ones are optimistically unchoked and not. Note that this may happen more frequently
    /// than this number suggests due to the normal unchoke distribution "promoting" optimistically
    /// unchoked peers to normally unchoked peers. In that case this recaluclation will happen
    /// immedieately afterwards.
    pub num_ticks_before_optimistic_unchoke_recalc: u32,
    /// This determines the minimal target of pieces we want to upload to a peer before
    /// moving on to another peer when the "round-robin" unchoking strategy is used. The
    /// "round-robin" strategy is currently only used when seeding.
    pub seeding_piece_quota: u32,
    /// Controls the size of the io_uring completion queue
    pub cq_size: u32,
    /// Controls the size of the io_uring submission queue
    pub sq_size: u32,
    /// The event loop will wait for at least these amont of completion events
    /// before it starts processing them. If the target isn't reached it will wait for
    /// at most [ `CQE_WAIT_TIME_NS` ] nanoseconds before processing the ones currently in the completion queue.
    pub completion_event_want: usize,
    /// The size of the Read buffers used for network operations. Defaults to SUBPIECE_SIZE * 2
    pub network_read_buffer_size: usize,
    /// The size of the Write buffers used for network operations. Defaults to SUBPIECE_SIZE + 4Kb
    pub network_write_buffer_size: usize,
    /// The size of the Write network buffer pools
    pub write_buffer_pool_size: usize,
    /// The size of the Reaad network buffer pools
    pub read_buffer_pool_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_connections: 128,
            max_reported_outstanding_requests: 512,
            max_unchoked: 8,
            num_ticks_before_unchoke_recalc: 15,
            num_ticks_before_optimistic_unchoke_recalc: 30,
            seeding_piece_quota: 20,
            cq_size: 4096,
            sq_size: 4096,
            completion_event_want: 32,
            network_read_buffer_size: (SUBPIECE_SIZE * 2) as usize,
            read_buffer_pool_size: 256,
            network_write_buffer_size: (SUBPIECE_SIZE + 4096) as usize,
            write_buffer_pool_size: 256,
        }
    }
}

/// A single torrent containing the current torrent state
/// and our peer id. This object is responsible for starting
/// the I/O event loop.
pub struct Torrent {
    our_id: PeerId,
    state: State,
}

impl Torrent {
    /// Create a new torrent from the given state. Use the given peer id
    /// when communicating with other peers.
    pub fn new(our_id: PeerId, state: State) -> Self {
        Self { our_id, state }
    }

    /// Returns if the torrent is completed or not.
    /// This is mostly useful to check status BEFORE
    /// starting the torrent. Use [ `TorrentEvent::TorrentComplete` ]
    /// to detect if a torrent have been completed download if you start
    /// an incomplete torrent.
    pub fn is_complete(&self) -> bool {
        self.state
            .torrent_state
            .as_ref()
            .is_some_and(|state| state.is_complete)
    }

    /// Start the I/O event loop and the torrent itself. This will start
    /// a new io_uring instance and new buffer pools
    /// will be allocated and registered to the io uring instance.
    /// This is expected to run in a separate thread and interaction with
    /// the event loop happens via the mpsc command queue. Torrent events will be sent to the
    /// consumer of the `event_tx` spsc channel.
    pub fn start(
        &mut self,
        event_tx: Producer<TorrentEvent>,
        command_rc: Receiver<Command>,
        listener: TcpListener,
    ) -> Result<(), Error> {
        // check ulimit
        let ring: IoUring = IoUring::builder()
            .setup_single_issuer()
            .setup_clamp()
            .setup_cqsize(self.state.config.cq_size)
            .setup_defer_taskrun()
            .setup_coop_taskrun()
            .build(self.state.config.sq_size)
            .unwrap();
        let events = SlotMap::with_capacity_and_key(self.state.config.cq_size as usize);
        let mut event_loop = EventLoop::new(self.our_id, events, &self.state.config);
        event_loop.run(ring, &mut self.state, event_tx, command_rc, listener)
    }
}

/// Commands that can be sent to the torrent event loop through the command channel
#[derive(Debug)]
pub enum Command {
    /// Connect to peers at the given address.
    /// Already connected peers will be filtered out
    ConnectToPeers(Vec<SocketAddrV4>),
    /// Stop the event loop gracefully
    Stop,
}

/// Metrics for a given peer
#[derive(Debug)]
pub struct PeerMetrics {
    /// The numer of bytes per second we are currently downloading from the peer
    pub download_throughput: u64,
    /// The number of bytes per second we are currently uploading to the peer
    pub upload_throughput: u64,
    /// If this peer is in "endgame" mode. Aka we are downloading pieces from it
    /// that may be allocated by other peers.
    pub endgame: bool,
    /// If we suspect this peer is snubbing us
    pub snubbed: bool,
}

/// Events from the torrent
#[derive(Debug)]
pub enum TorrentEvent {
    /// The torrent finished downloading. Note you will NOT receive
    /// this event if the torrent already is completed when you are
    /// starting the torrent. Use [ `Torrent::is_complete` ] to check if it's already completed
    /// before starting the torrent.
    TorrentComplete,
    /// The metadata has finished downloading from the connected peers.
    /// Note you will NOT receive this event if the metadata already is
    /// completed when starting the torrent.
    MetadataComplete(Box<TorrentMetadata>),
    /// The listener for incoming connection has finished set up
    /// on the provided port.
    ListenerStarted { port: u16 },
    /// Over all metrics for the torrent, sent every tick.
    TorrentMetrics {
        /// How many pieces have currently been completed
        pieces_completed: usize,
        /// How many pieces have been allocated by peers for
        /// download
        pieces_allocated: usize,
        /// Peer metrics for all currently connected peers
        peer_metrics: Vec<PeerMetrics>,
        /// The currently number of unchoked peers
        num_unchoked: usize,
    },
}

pub struct InitializedState {
    pub piece_selector: PieceSelector,
    pub num_unchoked: u32,
    pub config: Config,
    pub ticks_to_recalc_unchoke: u32,
    pub ticks_to_recalc_optimistic_unchoke: u32,
    pub completed_piece_rc: Receiver<CompletedPiece>,
    pub completed_piece_tx: Sender<CompletedPiece>,
    pub pieces: Vec<Option<Piece>>,
    pub file_store: FileStore,
    pub is_complete: bool,
}

impl InitializedState {
    pub fn new(root: &Path, metadata: &TorrentMetadata, config: Config) -> Self {
        let mut pieces = Vec::with_capacity(metadata.pieces.len());
        for _ in 0..metadata.pieces.len() {
            pieces.push(None);
        }
        let (tx, rc) = std::sync::mpsc::channel();
        Self {
            piece_selector: PieceSelector::new(metadata),
            num_unchoked: 0,
            config,
            ticks_to_recalc_unchoke: config.num_ticks_before_unchoke_recalc,
            ticks_to_recalc_optimistic_unchoke: config.num_ticks_before_optimistic_unchoke_recalc,
            completed_piece_rc: rc,
            completed_piece_tx: tx,
            pieces,
            is_complete: false,
            file_store: FileStore::new(root, metadata).unwrap(),
        }
    }

    #[allow(dead_code)]
    pub fn num_allocated(&self) -> usize {
        self.pieces
            .iter()
            .filter(|piece| piece.as_ref().is_some_and(|piece| piece.ref_count > 0))
            .count()
    }

    #[inline]
    pub fn num_pieces(&self) -> usize {
        self.pieces.len()
    }

    pub(crate) fn complete_piece(
        &mut self,
        piece_idx: i32,
        connections: &mut SlotMap<ConnectionId, PeerConnection>,
        event_tx: &mut Producer<'_, TorrentEvent, 512>,
        piece_buffer: Buffer,
    ) {
        self.piece_selector
            .piece_buffer_pool
            .return_buffer(piece_buffer);
        self.piece_selector.mark_complete(piece_idx as usize);
        if !self.is_complete && self.piece_selector.completed_all() {
            log::info!("Torrent complete!");
            self.is_complete = true;
            // send later
            if event_tx.enqueue(TorrentEvent::TorrentComplete).is_err() {
                log::error!("Torrent completion event missed");
            }
            // We are no longer interestead in any of the
            // peers
            for (_, peer) in connections.iter_mut() {
                peer.not_interested(false);
                // If the peer is upload only and
                // we are upload only there is no reason
                // to stay connected
                if peer.is_upload_only {
                    peer.pending_disconnect = Some(DisconnectReason::RedundantConnection);
                }
                // Notify all extensions that the torrent completed
                for (_, extension) in peer.extensions.iter_mut() {
                    extension.on_torrent_complete(&mut peer.outgoing_msgs_buffer);
                }
            }
        }
        for (conn_id, peer) in connections.iter_mut() {
            if let Some(bitfield) = self.piece_selector.interesting_peer_pieces(conn_id)
                && !bitfield.any()
                && peer.is_interesting
                && peer.queued.is_empty()
                && peer.inflight.is_empty()
            {
                // We are no longer interestead in this peer
                peer.not_interested(false);
                // if it's upload only we can close the
                // connection since it will never download from
                // us
                if peer.is_upload_only {
                    peer.pending_disconnect = Some(DisconnectReason::RedundantConnection);
                }
            }
            peer.have(piece_idx, false);
        }
        log::debug!("Piece {piece_idx} completed!");
    }

    // 1. send to hash thread mark as downloaded (remove is hashing)
    // 2. when complete write to disk
    // 3. when complete to disk mark as complete and run the below logic
    //  How to know if it's complted? if RC == 0 all writes for the piece have completed
    //  store index as part of the event

    // TODO: Put this in the event loop directly instead when that is easier to test
    pub(crate) fn update_torrent_status(
        &mut self,
        connections: &mut SlotMap<ConnectionId, PeerConnection>,
        event_tx: &mut Producer<'_, TorrentEvent>,
    ) {
        while let Ok(completed_piece) = self.completed_piece_rc.try_recv() {
            if completed_piece.hash_matched {
                let piece_len = self.piece_selector.piece_len(completed_piece.index as i32);
                self.file_store.queue_piece_disk_operation(
                    completed_piece.index as i32,
                    completed_piece.buffer,
                    piece_len as usize,
                    DiskOpType::Write,
                    pending_disk_operations,
                );
            } else {
                // Only need to mark this as not downloaded when it fails
                // since otherwise it will be marked as completed and this is moot
                self.piece_selector
                    .mark_not_downloaded(completed_piece.index);
                // deallocate piece peer peer
                // TODO: disconnect, there also might be a minimal chance of a race
                // condition here where the connection id is replaced (by disconnect +
                // new connection so that the wrong peer is marked) but this should be
                // EXTREMELY rare
                log::error!("Piece hash didn't match expected hash!");
                self.piece_selector
                    .mark_not_allocated(completed_piece.index as i32, completed_piece.conn_id);
            }
        }
    }

    // Allocates a piece and increments the piece ref count
    pub fn allocate_piece(&mut self, index: i32, conn_id: ConnectionId) -> VecDeque<Subpiece> {
        log::debug!("Allocating piece: conn_id: {conn_id:?}, index: {index}");
        self.piece_selector.mark_allocated(index, conn_id);
        match &mut self.pieces[index as usize] {
            Some(allocated_piece) => allocated_piece.allocate_remaining_subpieces(),
            None => {
                let length = self.piece_selector.piece_len(index);
                let buffer = self.piece_selector.piece_buffer_pool.get_buffer();
                let mut piece = Piece::new(index, length, buffer);
                let subpieces = piece.allocate_remaining_subpieces();
                self.pieces[index as usize] = Some(piece);
                subpieces
            }
        }
    }

    // Deallocate a piece
    pub fn deallocate_piece(&mut self, index: i32, conn_id: ConnectionId) {
        log::debug!("Deallocating piece: conn_id: {conn_id:?}, index: {index}");
        // Mark the piece as interesting again so it can be picked again
        // if necessary
        self.piece_selector
            .update_peer_piece_intrest(conn_id, index as usize);
        // The piece might have been mid hashing when a timeout is received
        // (two separate peer) which causes to be completed whilst another peer
        // tried to download it. It's fine (TODO: confirm)
        if let Some(piece) = self.pieces[index as usize].as_mut() {
            // Will we reach 0 in the ref count?
            if piece.ref_count == 1 {
                log::debug!("marked as not allocated: conn_id: {conn_id:?}, index: {index}");
                self.piece_selector.mark_not_allocated(index, conn_id);
            }
            piece.ref_count = piece.ref_count.saturating_sub(1)
        }
    }

    pub fn can_preemtively_unchoke(&self) -> bool {
        self.num_unchoked < self.config.max_unchoked
    }

    /// Determine the most suited peers to be unchoked based on the selected unchoking strategy.
    /// Some unchoke slots are left for optimistic unchokes. The strategy currently is only
    /// affected if the torrent is completed or not.
    pub fn recalculate_unchokes(
        &mut self,
        connections: &mut SlotMap<ConnectionId, PeerConnection>,
    ) {
        struct ComparisonData {
            is_choking: bool,
            uploaded_since_unchoked: u64,
            downloaded_in_last_round: u64,
            uploaded_in_last_round: u64,
            last_unchoked: Option<Instant>,
        }
        log::info!("Recalculating unchokes");
        let mut peers = Vec::with_capacity(connections.len());
        for (id, peer) in connections.iter_mut() {
            if !peer.peer_interested || peer.pending_disconnect.is_some() {
                peer.network_stats.reset_round();
                if !peer.is_choking {
                    if peer.optimistically_unchoked {
                        peer.optimistically_unchoked = false;
                        // reset so another peer can be optimistically unchoked
                        self.ticks_to_recalc_optimistic_unchoke = 0;
                    }
                    peer.choke(self, true);
                }

                continue;
            }
            peers.push((
                id,
                ComparisonData {
                    is_choking: peer.is_choking,
                    uploaded_since_unchoked: peer.network_stats.upload_since_unchoked,
                    downloaded_in_last_round: peer.network_stats.downloaded_in_last_round,
                    uploaded_in_last_round: peer.network_stats.uploaded_in_last_round,
                    last_unchoked: peer.last_unchoked,
                },
            ));
        }
        if !self.is_complete {
            // Sort based of downloaded_in_last_round
            peers.sort_unstable_by(|(_, a), (_, b)| {
                a.downloaded_in_last_round
                    .cmp(&b.downloaded_in_last_round)
                    .reverse()
            });
        } else {
            let piece_length = self.piece_selector.avg_piece_length();
            let quota_bytes = (piece_length * self.config.seeding_piece_quota) as u64;
            // The torrent is completed. Do round robin sorting like in libtorrent
            peers.sort_unstable_by(|(_, a), (_, b)| {
                let a_quota_complete = !a.is_choking
                    && a.uploaded_since_unchoked > quota_bytes
                    && a.last_unchoked
                        .is_some_and(|time| time.elapsed() > Duration::from_mins(1));
                let b_quota_complete = !b.is_choking
                    && b.uploaded_since_unchoked > quota_bytes
                    && b.last_unchoked
                        .is_some_and(|time| time.elapsed() > Duration::from_mins(1));
                if a_quota_complete != b_quota_complete {
                    a_quota_complete.cmp(&b_quota_complete)
                } else if a.uploaded_in_last_round != b.uploaded_in_last_round {
                    a.uploaded_in_last_round
                        .cmp(&b.uploaded_in_last_round)
                        .reverse()
                } else {
                    a.last_unchoked
                        .map_or(Duration::MAX, |time| time.elapsed())
                        .cmp(&b.last_unchoked.map_or(Duration::MAX, |time| time.elapsed()))
                        .reverse()
                }
            });
        }
        let optimistic_unchoke_slots = std::cmp::max(1, self.config.max_unchoked / 5);
        let mut remaining_unchoke_slots = self.config.max_unchoked - optimistic_unchoke_slots;
        for (id, _) in peers {
            let peer = &mut connections[id];
            peer.network_stats.reset_round();
            if remaining_unchoke_slots > 0 {
                if peer.is_choking {
                    log::debug!(
                        "Peer[{}] now unchoked after recalculating throughputs",
                        peer.peer_id
                    );
                    peer.unchoke(self, true);
                }
                remaining_unchoke_slots -= 1;
                if peer.optimistically_unchoked {
                    log::trace!(
                        "Peer[{}] previously optimistically unchoked, promoted to normal unchoke",
                        peer.peer_id
                    );
                    // no longer optimistic, the peer is "promoted"
                    // to a normal unchoke slot
                    peer.optimistically_unchoked = false;
                    // reset so another peer can be optimistically unchoked
                    self.ticks_to_recalc_optimistic_unchoke = 0;
                }
            } else if !peer.is_choking && !peer.optimistically_unchoked {
                log::debug!(
                    "Peer[{}] no longer unchoked after recalculating throughputs",
                    peer.peer_id
                );
                peer.choke(self, true);
            }
        }
    }

    // Give some lucky winners unchokes to test if they will have better throughput than the
    // currently unchoked peers
    pub fn recalculate_optimistic_unchokes(
        &mut self,
        connections: &mut SlotMap<ConnectionId, PeerConnection>,
    ) {
        log::info!("Recalculating optimistic unchokes");
        let num_opt_unchoked = std::cmp::max(1, self.config.max_unchoked / 5) as usize;
        let mut previously_opt_unchoked = ahash::HashSet::with_capacity(num_opt_unchoked);
        let mut candidates = Vec::with_capacity(self.config.max_unchoked as usize);
        for (id, peer) in connections.iter_mut() {
            if peer.optimistically_unchoked {
                previously_opt_unchoked.insert(id);
            }
            if peer.pending_disconnect.is_none()
                && peer.peer_interested
                && (peer.is_choking || peer.optimistically_unchoked)
            {
                candidates.push((
                    id,
                    peer.last_optimistically_unchoked
                        .map_or(u64::MAX, |time| time.elapsed().as_secs()),
                ));
            }
        }
        // Sort in the order of peers that have waited the longest
        candidates.sort_unstable_by(|(_, a), (_, b)| a.cmp(b).reverse());
        for (id, _) in candidates.iter().take(num_opt_unchoked) {
            let peer = &mut connections[*id];
            if peer.optimistically_unchoked {
                log::debug!("Peer[{}] optmistically unchoked again", peer.peer_id);
                previously_opt_unchoked.remove(id);
            } else {
                peer.optimistically_unchoke(self, true);
            }
        }

        for id in previously_opt_unchoked {
            let peer = &mut connections[id];
            peer.choke(self, true);
        }
    }
}

/// Current state of the torrent
pub struct State {
    pub(crate) info_hash: [u8; 20],
    pub(crate) listener_port: Option<u16>,
    // TODO: Consider checking this is accessible at construction
    root: PathBuf,
    torrent_state: Option<InitializedState>,
    file: OnceCell<Box<TorrentMetadata>>,
    pub(crate) config: Config,
}

impl State {
    /// The torrent info hash
    pub fn info_hash(&self) -> [u8; 20] {
        self.info_hash
    }

    /// Use this constructor if the torrent is unstarted.
    /// `info_hash` is the info hash of the torrent that should be downloaded.
    /// `root` is the directory where the torrent will be downloaded into.
    /// Vortex will create this directory if it doesn't already exist.
    /// `config` is the vortex config that should be used
    pub fn unstarted(info_hash: [u8; 20], root: PathBuf, config: Config) -> Self {
        Self {
            info_hash,
            root,
            listener_port: None,
            torrent_state: None,
            file: OnceCell::new(),
            config,
        }
    }

    /// Use this constructor if you have access to the torrent metadata
    /// and/or if the torrent has already started.
    ///
    /// `metadata` is the metadata associated with the torrent.
    /// `root` is the directory where potentially already started torrent files
    /// are expected to be found. If the folder doesn't exist vortex will create it.
    ///
    /// `config` is the vortex config that should be used.
    ///
    /// NOTE: This will go through all files in `root` and hash their pieces (in parallel) to determine torrent progress
    /// which may be slow on large torrents.
    pub fn from_metadata_and_root(
        metadata: TorrentMetadata,
        root: PathBuf,
        config: Config,
    ) -> io::Result<Self> {
        let mut initialized_state = InitializedState::new(&root, &metadata, config);
        let file_store = &initialized_state.file_store;
        let completed_pieces: Box<[bool]> = metadata
            .pieces
            .as_slice()
            .par_iter()
            .enumerate()
            .map(|(idx, hash)| {
                match file_store.check_piece_hash_sync(idx as i32, hash) {
                    Ok(is_valid) => is_valid,
                    Err(err) => {
                        if err.kind() != io::ErrorKind::NotFound {
                            log::warn!("Error checking piece {idx}: {err}");
                        }
                        false
                    }
                }
            })
            .collect();
        let completed_pieces: BitVec<u8, Msb0> = completed_pieces.into_iter().collect();
        let completed_pieces: BitBox<u8, Msb0> = completed_pieces.into_boxed_bitslice();
        log::trace!("Completed pieces: {completed_pieces}");
        initialized_state
            .piece_selector
            .set_completed_bitfield(completed_pieces);
        initialized_state.is_complete = initialized_state.piece_selector.completed_all();

        Ok(Self {
            info_hash: metadata
                .info_hash_bytes()
                .try_into()
                .expect("Invalid info hash"),
            root,
            listener_port: None,
            torrent_state: Some(initialized_state),
            file: OnceCell::from(Box::new(metadata)),
            config,
        })
    }

    #[cfg(test)]
    pub fn inprogress(
        info_hash: [u8; 20],
        root: PathBuf,
        metadata: lava_torrent::torrent::v1::Torrent,
        state: InitializedState,
        config: Config,
    ) -> Self {
        Self {
            info_hash,
            root,
            listener_port: None,
            torrent_state: Some(state),
            file: OnceCell::from(Box::new(metadata)),
            config,
        }
    }

    pub fn as_ref(&mut self) -> StateRef<'_> {
        StateRef {
            info_hash: self.info_hash,
            root: &self.root,
            listener_port: &self.listener_port,
            torrent: &mut self.torrent_state,
            full: &self.file,
            config: &self.config,
        }
    }
}

// is this even needed?
pub struct StateRef<'state> {
    info_hash: [u8; 20],
    root: &'state Path,
    pub listener_port: &'state Option<u16>,
    torrent: &'state mut Option<InitializedState>,
    full: &'state OnceCell<Box<TorrentMetadata>>,
    pub config: &'state Config,
}

impl<'e_iter, 'state: 'e_iter> StateRef<'state> {
    pub fn info_hash(&self) -> &[u8; 20] {
        &self.info_hash
    }

    pub fn state(&'e_iter mut self) -> Option<&'e_iter mut InitializedState> {
        self.torrent.as_mut()
    }

    pub fn metadata(&'e_iter mut self) -> Option<&'state Box<TorrentMetadata>> {
        self.full.get()
    }

    #[inline]
    pub fn is_initialzied(&self) -> bool {
        self.full.get().is_some()
    }

    pub fn init(&'e_iter mut self, metadata: TorrentMetadata) -> io::Result<()> {
        if self.is_initialzied() {
            return Err(io::Error::other("State initialized twice"));
        }
        *self.torrent = Some(InitializedState::new(self.root, &metadata, *self.config));
        self.full
            .set(Box::new(metadata))
            .map_err(|_e| io::Error::other("State initialized twice"))?;
        Ok(())
    }
}
