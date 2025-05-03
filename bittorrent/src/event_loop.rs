use std::{
    io,
    net::SocketAddrV4,
    os::fd::{AsRawFd, FromRawFd},
    sync::mpsc::{Receiver, TryRecvError},
    time::{Duration, Instant},
};

use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
};
use lava_torrent::torrent::v1::Torrent;
use rayon::Scope;
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    Error, TorrentState,
    buf_pool::BufferPool,
    buf_ring::{Bgid, BufferRing},
    file_store::FileStore,
    io_utils::{self, BackloggedSubmissionQueue, SubmissionQueue, UserData},
    peer_connection::{DisconnectReason, OutgoingMsg, PeerConnection},
    peer_protocol::{self, HANDSHAKE_SIZE, PeerId, parse_handshake, write_handshake},
    piece_selector::{self, SUBPIECE_SIZE},
};

const HANDSHAKE_TIMEOUT_SECS: u64 = 10;

#[derive(Debug)]
pub enum EventType {
    Accept,
    Connect { socket: Socket, addr: SocketAddrV4 },
    Write { socket: Socket },
    Recv { socket: Socket },
    ConnectedWrite { connection_idx: usize },
    ConnectedRecv { connection_idx: usize },
    ConnectionStopped { connection_idx: usize },
    // Dummy used to allow stable keys in the slab
    Dummy,
}

fn event_error_handler<Q: SubmissionQueue>(
    sq: &mut BackloggedSubmissionQueue<Q>,
    error_code: u32,
    user_data: UserData,
    events: &mut Slab<EventType>,
    torrent_state: &mut TorrentState<'_>,
    connections: &mut Slab<PeerConnection>,
    bgid: Bgid,
) -> io::Result<()> {
    match error_code as i32 {
        libc::ENOBUFS => {
            // TODO: statistics
            let event = &events[user_data.event_idx as _];
            log::warn!("Ran out of buffers!, resubmitting recv op");
            // Ran out of buffers!
            match event {
                EventType::Recv { socket } => {
                    let fd = socket.as_raw_fd();
                    io_utils::recv(sq, user_data, fd, bgid, HANDSHAKE_TIMEOUT_SECS);
                    Ok(())
                }
                EventType::ConnectedRecv { connection_idx } => {
                    let fd = connections[*connection_idx].socket.as_raw_fd();
                    io_utils::recv_multishot(sq, user_data, fd, bgid);
                    Ok(())
                }
                _ => {
                    panic!("Ran out of buffers on a non recv operation: {event:?}");
                }
            }
        }
        libc::ETIME => {
            let event = events.remove(user_data.event_idx as _);
            let socket = match event {
                EventType::Connect { socket, addr } => {
                    log::debug!("Connect timed out!: {}", addr);
                    socket
                }
                EventType::Recv { socket } => {
                    log::debug!(
                        "Handshake timed out!: {:?}",
                        socket.peer_addr().expect("must have connected")
                    );
                    socket
                }
                _ => panic!("Timed out unexpected event: {event:?}"),
            };

            socket.shutdown(std::net::Shutdown::Both)?;
            Ok(())
        }
        libc::ECONNRESET => {
            let event = events.remove(user_data.event_idx as _);
            match event {
                EventType::Write { socket } | EventType::Recv { socket } => {
                    log::error!(
                        "Connection to {:?} reset before handshake completed",
                        socket.peer_addr().expect("Must have connected")
                    );
                    socket.shutdown(std::net::Shutdown::Both)?;
                }
                EventType::ConnectedRecv { connection_idx }
                | EventType::ConnectedWrite { connection_idx } => {
                    let connection = &mut connections[connection_idx];
                    log::error!("Peer [{}] Connection reset", connection.peer_id);
                    connection.pending_disconnect = Some(DisconnectReason::TcpReset);
                }
                EventType::ConnectionStopped { connection_idx } => {
                    let mut connection = connections.remove(connection_idx);
                    log::error!(
                        "Peer [{}] Connection reset during shutdown",
                        connection.peer_id
                    );
                    connection.release_all_pieces(torrent_state);
                }
                EventType::Dummy | EventType::Connect { .. } | EventType::Accept => unreachable!(),
            }
            Ok(())
        }
        libc::ECONNREFUSED => {
            // Failling to connect due to this is not really an error due to
            // the likelyhood of being stale info in the DHT
            log::debug!("Connection refused");
            Ok(())
        }
        libc::ECANCELED => {
            // This is the timeout or the connect operation being cancelled, the event should be deleted by the successful
            // the ETIME handler or the successful connection event
            // NOTE: the event idx might have been overwritten and reused by the time this is handled
            // so don't trust the event connected to the index
            log::trace!("Event cancelled");
            Ok(())
        }
        _ => {
            events.remove(user_data.event_idx as _);
            let err = std::io::Error::from_raw_os_error(error_code as i32);
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IoEvent {
    user_data: UserData,
    result: Result<i32, u32>,
    read_bid: Option<u16>,
    is_more: bool,
}

impl From<Entry> for IoEvent {
    fn from(cqe: Entry) -> Self {
        let user_data = UserData::from_u64(cqe.user_data());
        let result = if cqe.result() < 0 {
            Err((-cqe.result()) as u32)
        } else {
            Ok(cqe.result())
        };
        let read_bid = io_uring::cqueue::buffer_select(cqe.flags());
        let is_more = io_uring::cqueue::more(cqe.flags());
        Self {
            user_data,
            result,
            read_bid,
            is_more,
        }
    }
}

const CQE_WAIT_TIME: &Timespec = &Timespec::new().nsec(250_000_000);

pub struct EventLoop {
    events: Slab<EventType>,
    write_pool: BufferPool,
    read_ring: BufferRing,
    connections: Slab<PeerConnection>,
    peer_provider: Receiver<SocketAddrV4>,
    our_id: PeerId,
}

impl<'scope, 'f_store: 'scope> EventLoop {
    pub fn new(
        our_id: PeerId,
        events: Slab<EventType>,
        peer_provider: Receiver<SocketAddrV4>,
    ) -> Self {
        Self {
            events,
            write_pool: BufferPool::new(256, (SUBPIECE_SIZE * 2) as _),
            read_ring: BufferRing::new(1, 256, (SUBPIECE_SIZE * 2) as _).unwrap(),
            connections: Slab::with_capacity(64),
            peer_provider,
            our_id,
        }
    }

    pub fn run(
        &mut self,
        mut ring: IoUring,
        mut torrent_state: TorrentState<'f_store>,
        file_store: &'f_store FileStore,
        torrent_info: &'f_store Torrent,
    ) -> Result<(), Error> {
        self.read_ring.register(&ring.submitter())?;
        // lambda to be able to catch errors an always unregistering the read ring
        let result = rayon::in_place_scope(|scope| {
            let (submitter, sq, mut cq) = ring.split();
            let mut sq = BackloggedSubmissionQueue::new(sq);
            let mut last_tick = Instant::now();
            loop {
                let args = types::SubmitArgs::new().timespec(CQE_WAIT_TIME);
                match submitter.submit_with_args(8, &args) {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {
                        log::warn!("Ring busy")
                    }
                    Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => {
                        log::debug!("CQE_WAIT_TIME was reached before target events")
                    }
                    Err(err) => {
                        log::error!("Failed ring submission, aborting: {err}");
                        return Err(Error::Io(err));
                    }
                }
                cq.sync();
                if cq.overflow() > 0 {
                    log::error!("CQ overflow");
                }

                if let Err(err) = sq.submit_and_drain_backlog(&submitter) {
                    return Err(Error::Io(err));
                }

                let tick_delta = last_tick.elapsed();
                if tick_delta > Duration::from_secs(1) {
                    tick(
                        &tick_delta,
                        &mut self.connections,
                        file_store,
                        &mut torrent_state,
                    );
                    last_tick = Instant::now();
                    // Dealt with here to make tick easier to test
                    for (conn_id, connection) in self.connections.iter_mut() {
                        if let Some(reason) = &connection.pending_disconnect {
                            log::warn!("Disconnect: {} reason {reason}", connection.peer_id);
                            // Event handler for this deals with releasing pieces and decrements
                            // num unchoked
                            io_utils::stop_connection(
                                &mut sq,
                                conn_id,
                                connection.socket.as_raw_fd(),
                                &mut self.events,
                            );
                        }
                    }
                    sq.sync();
                }

                for cqe in &mut cq {
                    let io_event = IoEvent::from(cqe);
                    if let Err(err) = self.event_handler(
                        &mut sq,
                        io_event,
                        &mut torrent_state,
                        file_store,
                        torrent_info,
                        scope,
                    ) {
                        log::error!("Error handling event: {err}");
                    }
                    // time to return any potential write buffers
                    if let Some(write_idx) = io_event.user_data.buffer_idx {
                        self.write_pool.return_buffer(write_idx as usize);
                    }

                    // Ensure bids are always returned
                    if let Some(bid) = io_event.read_bid {
                        self.read_ring.return_bid(bid);
                    }
                }

                torrent_state.update_torrent_status(&mut self.connections);

                for (conn_id, connection) in self.connections.iter_mut() {
                    for msg in connection.outgoing_msgs_buffer.iter_mut() {
                        let conn_fd = connection.socket.as_raw_fd();
                        let buffer = self.write_pool.get_buffer();
                        msg.message.encode(buffer.inner);
                        let size = msg.message.encoded_size();
                        io_utils::write_to_connection(
                            conn_id,
                            conn_fd,
                            &mut self.events,
                            &mut sq,
                            buffer.index,
                            &buffer.inner[..size],
                            msg.ordered,
                        );
                    }
                    connection.outgoing_msgs_buffer.clear();
                }
                sq.sync();
                self.connect_to_new_peers(&mut sq)?;
                if torrent_state.is_complete {
                    log::info!("Torrent complete!");
                    return Ok(());
                }
            }
        });
        self.read_ring.unregister(&ring.submitter())?;
        result
    }

    fn connect_to_new_peers<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
    ) -> Result<(), Error> {
        let result = loop {
            match self.peer_provider.try_recv() {
                Ok(addr) => {
                    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
                    //socket.set_recv_buffer_size(1 << 19).unwrap();
                    let fd = socket.as_raw_fd();
                    let event_idx = self.events.insert(EventType::Connect { socket, addr });
                    let user_data = UserData::new(event_idx, None);
                    let addr = SockAddr::from(addr);
                    let connect_op =
                        opcode::Connect::new(types::Fd(fd), addr.as_ptr() as *const _, addr.len())
                            .build()
                            .flags(io_uring::squeue::Flags::IO_LINK)
                            .user_data(user_data.as_u64());
                    let timeout = Timespec::new().sec(10);
                    let user_data = UserData::new(event_idx, None);
                    let timeout_op = opcode::LinkTimeout::new(&timeout)
                        .build()
                        .user_data(user_data.as_u64());
                    // If the queue doesn't fit both events they need
                    // to be sent to the backlog so they can be submitted
                    // together and not with a arbitrary delay inbetween.
                    // That would mess up the timeout
                    if sq.remaining() >= 2 {
                        sq.push(connect_op);
                        sq.push(timeout_op);
                    } else {
                        sq.push_backlog(connect_op);
                        sq.push_backlog(timeout_op);
                    }
                }
                Err(TryRecvError::Disconnected) => break Err(Error::PeerProviderDisconnect),
                Err(TryRecvError::Empty) => break Ok(()),
            }
        };
        sq.sync();
        result
    }

    fn event_handler<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        io_event: IoEvent,
        torrent_state: &mut TorrentState<'f_store>,
        file_store: &'f_store FileStore,
        torrent_info: &'scope Torrent,
        scope: &Scope<'scope>,
    ) -> io::Result<()> {
        let ret = match io_event.result {
            Ok(ret) => ret,
            Err(error_code) => {
                return event_error_handler(
                    sq,
                    error_code,
                    io_event.user_data,
                    &mut self.events,
                    torrent_state,
                    &mut self.connections,
                    self.read_ring.bgid(),
                );
            }
        };
        let mut event = EventType::Dummy;
        std::mem::swap(
            &mut event,
            &mut self.events[io_event.user_data.event_idx as usize],
        );
        match event {
            EventType::Accept => {
                // The event is reused and not replaced
                std::mem::swap(
                    &mut event,
                    &mut self.events[io_event.user_data.event_idx as usize],
                );
                let fd = ret;
                let socket = unsafe { Socket::from_raw_fd(fd) };
                if socket.peer_addr()?.is_ipv6() {
                    log::error!("Received connection from non ipv4 addr");
                    return Ok(());
                };

                log::info!("Accepted connection: {:?}", socket.peer_addr()?);
                // Construct new recv token on accept, after that it lives forever and or is reused
                // since this is a recvmulti operation
                let read_token = self.events.insert(EventType::Recv { socket });
                let user_data = UserData::new(read_token, None);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                sq.push(read_op);
            }
            EventType::Connect { socket, addr } => {
                log::info!("Connected to: {addr}");
                let buffer = self.write_pool.get_buffer();
                write_handshake(self.our_id, torrent_state.info_hash, buffer.inner);
                let fd = socket.as_raw_fd();
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[io_event.user_data.event_idx as usize],
                    EventType::Write { socket },
                );
                debug_assert!(matches!(old, EventType::Dummy));
                let write_token = io_event.user_data.event_idx as usize;
                let user_data = UserData::new(write_token, Some(buffer.index));
                let write_op = opcode::Write::new(
                    types::Fd(fd),
                    buffer.inner.as_ptr(),
                    // TODO: Handle this better
                    HANDSHAKE_SIZE as u32,
                )
                .build()
                .user_data(user_data.as_u64());
                sq.push(write_op);
            }
            EventType::Write { socket } => {
                let fd = socket.as_raw_fd();
                log::debug!("Wrote to unestablsihed connection");
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[io_event.user_data.event_idx as usize],
                    EventType::Recv { socket },
                );
                debug_assert!(matches!(old, EventType::Dummy));
                let read_token = io_event.user_data.event_idx as usize;
                let user_data = UserData::new(read_token, None);
                // Multishot isn't used here to simplify error handling
                // when the read is invalid or otherwise doesn't lead to
                // a full connection which does have graceful shutdown mechanisms
                io_utils::recv(
                    sq,
                    user_data,
                    fd,
                    self.read_ring.bgid(),
                    HANDSHAKE_TIMEOUT_SECS,
                );
            }
            EventType::ConnectedWrite { connection_idx: _ } => {
                // neither replaced nor modified
                // TODO: add to metrics
                self.events.remove(io_event.user_data.event_idx as _);
            }
            EventType::Recv { socket } => {
                let fd = socket.as_raw_fd();
                let len = ret as usize;
                if len == 0 {
                    log::debug!("No more data when expecting handshake");
                    self.events.remove(io_event.user_data.event_idx as _);
                    return Ok(());
                }
                // TODO: This could happen due to networks splitting the handshake up
                // so it should be dealt with better, but since the handshake is so
                // small (well below MTU) I suspect that to be rare
                if len < HANDSHAKE_SIZE {
                    log::error!("Didn't receive enough data to parse handshake");
                    self.events.remove(io_event.user_data.event_idx as _);
                    return Err(io::ErrorKind::InvalidData.into());
                }
                // We always have a buffer associated
                let buffer = io_event
                    .read_bid
                    .map(|bid| self.read_ring.get(bid))
                    .unwrap();
                let (handshake_data, remainder) = buffer[..len].split_at(HANDSHAKE_SIZE);
                // Expect this to be the handshake response
                let parsed_handshake =
                    parse_handshake(torrent_state.info_hash, handshake_data).unwrap();
                let entry = self.connections.vacant_entry();
                let conn_id = entry.key();
                let peer_connection = PeerConnection::new(
                    socket,
                    parsed_handshake.peer_id,
                    conn_id,
                    parsed_handshake.fast_ext,
                );
                let id = peer_connection.peer_id;
                entry.insert(peer_connection);
                log::info!("Finished handshake! [{conn_id}]: {id}");
                // We are now connected!
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[io_event.user_data.event_idx as usize],
                    EventType::ConnectedRecv {
                        connection_idx: conn_id,
                    },
                );
                debug_assert!(matches!(old, EventType::Dummy));

                // The initial Recv might have contained more data
                // than just the handshake so need to handle that here
                // since the read_buffer will be overwritten by the next
                // incoming recv cqe
                let connection = &mut self.connections[conn_id];
                connection.stateful_decoder.append_data(remainder);
                let completed = torrent_state.piece_selector.completed_clone();
                conn_parse_and_handle_msgs(
                    connection,
                    torrent_state,
                    file_store,
                    torrent_info,
                    scope,
                );
                // Recv has been complete, move over to multishot, same user data
                io_utils::recv_multishot(sq, io_event.user_data, fd, self.read_ring.bgid());
                let message = if completed.all() {
                    peer_protocol::PeerMessage::HaveAll
                } else if completed.not_any() {
                    peer_protocol::PeerMessage::HaveNone
                } else {
                    peer_protocol::PeerMessage::Bitfield(completed.into())
                };
                // sent as first message after handshake
                let bitfield_msg = OutgoingMsg {
                    message,
                    ordered: true,
                };
                let buffer = self.write_pool.get_buffer();
                bitfield_msg.message.encode(buffer.inner);
                let size = bitfield_msg.message.encoded_size();
                io_utils::write_to_connection(
                    conn_id,
                    fd,
                    &mut self.events,
                    sq,
                    buffer.index,
                    &buffer.inner[..size],
                    bitfield_msg.ordered,
                );
            }
            EventType::ConnectedRecv { connection_idx } => {
                // The event is reused and not replaced
                std::mem::swap(
                    &mut event,
                    &mut self.events[io_event.user_data.event_idx as usize],
                );
                let connection = &mut self.connections[connection_idx];
                let len = ret as usize;
                if len == 0 {
                    log::debug!(
                        "[PeerId: {}] No more data, mark as pending disconnect",
                        connection.peer_id
                    );
                    self.events.remove(io_event.user_data.event_idx as _);
                    connection.pending_disconnect = Some(DisconnectReason::ClosedConnection);
                    return Ok(());
                }
                if !io_event.is_more {
                    let fd = connection.socket.as_raw_fd();
                    // restart the operation
                    io_utils::recv_multishot(sq, io_event.user_data, fd, self.read_ring.bgid());
                }

                // We always have a buffer associated
                let buffer = io_event
                    .read_bid
                    .map(|bid| self.read_ring.get(bid))
                    .unwrap();
                let buffer = &buffer[..len];
                connection.stateful_decoder.append_data(buffer);
                conn_parse_and_handle_msgs(
                    connection,
                    torrent_state,
                    file_store,
                    torrent_info,
                    scope,
                );
            }
            EventType::ConnectionStopped { connection_idx } => {
                let mut connection = self.connections.remove(connection_idx);
                connection.release_all_pieces(torrent_state);
                // Don't count disconnected peers
                if !connection.is_choking {
                    torrent_state.num_unchoked -= 1;
                }
                log::debug!(
                    "Pending operations cancelled, disconnecting: {}",
                    connection.peer_id
                );
            }
            EventType::Dummy => unreachable!(),
        }
        Ok(())
    }
}

fn conn_parse_and_handle_msgs<'scope, 'f_store: 'scope>(
    connection: &mut PeerConnection,
    torrent_state: &mut TorrentState<'f_store>,
    file_store: &'f_store FileStore,
    torrent_info: &'scope Torrent,
    scope: &Scope<'scope>,
) {
    while let Some(parse_result) = connection.stateful_decoder.next() {
        match parse_result {
            Ok(peer_message) => {
                connection.handle_message(
                    peer_message,
                    torrent_state,
                    file_store,
                    torrent_info,
                    scope,
                );
            }
            Err(err) => {
                log::error!("Failed {} decoding message: {err}", connection.conn_id);
                connection.pending_disconnect = Some(DisconnectReason::InvalidMessage);
                break;
            }
        }
    }
    connection.fill_request_queue();
}

fn report_metrics(torrent_state: &TorrentState<'_>, connections: &Slab<PeerConnection>) {
    let counter = metrics::counter!("pieces_completed");
    counter.absolute(torrent_state.piece_selector.total_completed() as u64);
    let gauge = metrics::gauge!("pieces_inflight");
    gauge.set(torrent_state.piece_selector.total_inflight() as u32);
    let gauge = metrics::gauge!("num_unchoked");
    gauge.set(torrent_state.num_unchoked);
    let gauge = metrics::gauge!("num_connections");
    gauge.set(connections.len() as u32);
}

pub(crate) fn tick<'scope, 'f_store: 'scope>(
    tick_delta: &Duration,
    connections: &mut Slab<PeerConnection>,
    file_store: &'f_store FileStore,
    torrent_state: &mut TorrentState<'scope>,
) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
    // 1. Calculate bandwidth (deal with initial start up)
    // 2. Go through them in order
    // 3. select pieces
    for (_, connection) in connections
        .iter_mut()
        // Filter out connections that are pending diconnect
        .filter(|(_, conn)| conn.pending_disconnect.is_none())
    {
        if connection.last_seen.elapsed() > Duration::from_secs(120) {
            log::warn!("Timeout due to inactivity: {}", connection.peer_id);
            connection.pending_disconnect = Some(DisconnectReason::Idle);
            continue;
        }
        // TODO: If we are not using fast extension this might be triggered by a snub
        if let Some(time) = connection.last_received_subpiece {
            if time.elapsed() > connection.request_timeout() {
                // error just to make more visible
                log::error!("TIMEOUT: {}", connection.peer_id);
                connection.on_request_timeout(torrent_state);
            }
        }

        // Take delta into account when calculating throughput
        connection.throughput =
            (connection.throughput as f64 / tick_delta.as_secs_f64()).round() as u64;
        if !connection.peer_choking {
            // slow start win size increase is handled in update_stats
            if !connection.slow_start {
                // From the libtorrent impl, request queue time = 3
                let new_queue_capacity =
                    3 * connection.throughput / piece_selector::SUBPIECE_SIZE as u64;
                connection.update_target_inflight(new_queue_capacity as usize);
            }
        }

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
        .filter_map(|(key, peer)| {
            // Skip connections that are pending disconnect
            if peer.pending_disconnect.is_none() {
                Some((key, peer.remaining_request_queue_spots()))
            } else {
                None
            }
        })
        .collect();

    peer_bandwidth.sort_unstable_by(|(_, a), (_, b)| a.cmp(b).reverse());
    for (peer_key, mut bandwidth) in peer_bandwidth {
        let peer = &mut connections[peer_key];

        while {
            let bandwitdth_available_for_new_piece =
                bandwidth > (torrent_state.piece_selector.avg_num_subpieces() as usize / 2);
            let nothing_queued = peer.queued.is_empty();
            (bandwitdth_available_for_new_piece || nothing_queued) && !peer.peer_choking
        } {
            if let Some(next_piece) = torrent_state.piece_selector.next_piece(peer_key) {
                let mut queue = torrent_state.allocate_piece(next_piece, file_store);
                let queue_len = queue.len();
                peer.append_and_fill(&mut queue);
                // Remove all subpieces from available bandwidth
                bandwidth -= (queue_len).min(bandwidth);
            } else {
                break;
            }
        }
        peer.fill_request_queue();
        peer.report_metrics();
    }

    report_metrics(torrent_state, connections);
}

#[cfg(test)]
mod tests {

    // use crate::test_utils::{generate_peer, setup_test};

    // use super::*;

    // TODO: probably should be checked in an integration test
    //#[test]
    //fn tick_last_seen() {}

    // TODO
    // #[test]
    // fn tick_bandwidth_calculation() {}

    // #[test]
    // fn tick_piece_distibution() {}
}
