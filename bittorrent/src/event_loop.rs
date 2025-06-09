use std::{
    collections::HashSet,
    io,
    os::fd::{AsRawFd, FromRawFd},
    time::{Duration, Instant},
};

use heapless::spsc::{Consumer, Producer};
use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
};
use rayon::Scope;
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    Command, Error, State, StateRef, TorrentEvent,
    buf_pool::BufferPool,
    buf_ring::{Bgid, BufferRing},
    io_utils::{self, BackloggedSubmissionQueue, SubmissionQueue, UserData},
    peer_comm::extended_protocol::extension_handshake_msg,
    peer_connection::{DisconnectReason, OutgoingMsg, PeerConnection},
    peer_protocol::{self, HANDSHAKE_SIZE, PeerId, parse_handshake, write_handshake},
    piece_selector::{self, SUBPIECE_SIZE},
};

const MAX_CONNECTIONS: usize = 100;
pub const MAX_OUTSTANDING_REQUESTS: u64 = 512;
const CONNECT_TIMEOUT: Timespec = Timespec::new().sec(10);
const HANDSHAKE_TIMEOUT: Timespec = Timespec::new().sec(7);

#[derive(Debug)]
pub enum EventType {
    Accept,
    Connect { socket: Socket, addr: SockAddr },
    Write { socket: Socket, addr: SockAddr },
    Recv { socket: Socket, addr: SockAddr },
    ConnectedWrite { connection_idx: usize },
    ConnectedRecv { connection_idx: usize },
    Cancel { connection_idx: usize },
    Shutdown { connection_idx: usize },
    Close,
    // Dummy used to allow stable keys in the slab
    Dummy,
}

#[allow(clippy::too_many_arguments)]
fn event_error_handler<'state, Q: SubmissionQueue>(
    sq: &mut BackloggedSubmissionQueue<Q>,
    error_code: u32,
    user_data: UserData,
    events: &mut Slab<EventType>,
    state_ref: &mut StateRef<'state>,
    connections: &mut Slab<PeerConnection>,
    pending_connections: &mut HashSet<SockAddr>,
    bgid: Bgid,
) -> io::Result<()> {
    match error_code as i32 {
        libc::ENOBUFS => {
            // TODO: statistics
            let event = &events[user_data.event_idx as _];
            log::warn!("Ran out of buffers!, resubmitting recv op");
            // Ran out of buffers!
            match event {
                EventType::Recv { socket, addr: _ } => {
                    let fd = socket.as_raw_fd();
                    io_utils::recv(sq, user_data, fd, bgid, &HANDSHAKE_TIMEOUT);
                    Ok(())
                }
                EventType::ConnectedRecv { connection_idx } => {
                    let fd = connections[*connection_idx].socket.as_raw_fd();
                    io_utils::recv_multishot(sq, user_data, fd, bgid);
                    Ok(())
                }
                _ => unreachable!(),
            }
        }
        libc::ETIME => {
            let event = events.remove(user_data.event_idx as _);
            let socket = match event {
                EventType::Connect { socket, addr } => {
                    log::debug!("Connect timed out!: {:?}", addr);
                    assert!(pending_connections.remove(&addr));
                    let connect_fail_counter = metrics::counter!("peer_connect_timeout");
                    connect_fail_counter.increment(1);
                    socket
                }
                EventType::Recv { socket, addr } => {
                    log::debug!("Handshake timed out!: {addr:?}",);
                    assert!(pending_connections.remove(&addr));
                    let handshake_timeout_counter = metrics::counter!("peer_handshake_timeout");
                    handshake_timeout_counter.increment(1);
                    socket
                }
                _ => unreachable!(),
            };
            io_utils::close_socket(sq, socket, events);
            Ok(())
        }
        libc::ECONNRESET => {
            let event = events.remove(user_data.event_idx as _);
            match event {
                EventType::Write { socket, addr } | EventType::Recv { socket, addr } => {
                    log::error!("Connection to {addr:?} reset before handshake completed ");
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, events);
                }
                EventType::ConnectedRecv { connection_idx }
                | EventType::ConnectedWrite { connection_idx } => {
                    let mut connection = connections.remove(connection_idx);
                    log::error!("Peer [{}] Connection reset", connection.peer_id);
                    if let Some((_, torrent_state)) = state_ref.state() {
                        connection.release_all_pieces(torrent_state);
                        if !connection.is_choking {
                            torrent_state.num_unchoked -= 1;
                        }
                    }
                    io_utils::close_socket(sq, connection.socket, events);
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::EPIPE => {
            let event = events.remove(user_data.event_idx as _);
            match event {
                EventType::Write { socket, addr } => {
                    log::warn!("Attempted to write to closed connection {addr:?}");
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, events);
                }
                EventType::ConnectedWrite { connection_idx } => {
                    // TODO: CANCEL EPIPE CONNS
                    if let Some(mut connection) = connections.try_remove(connection_idx) {
                        log::error!(
                            "Peer [{}] EPIPE received when writing to connection",
                            connection.peer_id
                        );
                        if let Some((_, torrent_state)) = state_ref.state() {
                            connection.release_all_pieces(torrent_state);
                            // Don't count disconnected peers
                            if !connection.is_choking {
                                torrent_state.num_unchoked -= 1;
                            }
                        }
                        io_utils::close_socket(sq, connection.socket, events);
                    } else {
                        // I guess this might happpen when multiple writes are queued up after
                        // each other
                        log::warn!("PIPE received after connection has already been removed",);
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::ECONNREFUSED | libc::EHOSTUNREACH => {
            // Failling to connect due to this is not really an error due to
            // the likelyhood of being stale info in the DHT
            let event = events.remove(user_data.event_idx as _);
            match event {
                EventType::Connect { socket, addr } => {
                    log::debug!("Connection to {addr:?} failed");
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, events);
                }
                _ => unreachable!(),
            }
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
            let err = std::io::Error::from_raw_os_error(error_code as i32);
            if !events.contains(user_data.event_idx as _) {
                log::error!(
                    "Failing event didn't exist in events, id: {}",
                    user_data.event_idx
                );
            } else {
                let event = events.remove(user_data.event_idx as _);
                log::error!("Unhandled error of typ: {event:?}");
                match event {
                    EventType::Connect { socket, addr: _ }
                    | EventType::Write { socket, addr: _ }
                    | EventType::Recv { socket, addr: _ } => {
                        io_utils::close_socket(sq, socket, events);
                    }
                    EventType::ConnectedWrite { connection_idx }
                    | EventType::ConnectedRecv { connection_idx }
                    | EventType::Cancel { connection_idx }
                    | EventType::Shutdown { connection_idx } => {
                        let mut connection = connections.remove(connection_idx);
                        log::error!("Peer [{}] unhandled error: {err}", connection.peer_id);
                        if let Some((_, torrent_state)) = state_ref.state() {
                            connection.release_all_pieces(torrent_state);
                            // Don't count disconnected peers
                            if !connection.is_choking {
                                torrent_state.num_unchoked -= 1;
                            }
                        }
                        io_utils::close_socket(sq, connection.socket, events);
                    }
                    EventType::Close | EventType::Accept | EventType::Dummy => return Err(err),
                }
            }
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
    // TODO: Merge these or consider disconnecting
    // pending peers as soon as max connections is reached
    connections: Slab<PeerConnection>,
    pending_connections: HashSet<SockAddr>,
    our_id: PeerId,
}

impl<'scope, 'state: 'scope> EventLoop {
    pub fn new(our_id: PeerId, events: Slab<EventType>) -> Self {
        Self {
            events,
            write_pool: BufferPool::new(256, (SUBPIECE_SIZE * 2) as _),
            read_ring: BufferRing::new(1, 256, (SUBPIECE_SIZE * 2) as _).unwrap(),
            connections: Slab::with_capacity(MAX_CONNECTIONS),
            pending_connections: HashSet::with_capacity(MAX_CONNECTIONS),
            our_id,
        }
    }

    pub fn run(
        &mut self,
        mut ring: IoUring,
        state: &'state mut State,
        mut event_tx: Producer<TorrentEvent, 512>,
        mut command_rc: Consumer<Command, 64>,
    ) -> Result<(), Error> {
        self.read_ring.register(&ring.submitter())?;

        let mut state_ref = state.as_ref();

        let mut prev_state_initialized = state_ref.is_initialzied();
        // lambda to be able to catch errors an always unregistering the read ring
        let result = rayon::in_place_scope(|scope| {
            let (submitter, sq, mut cq) = ring.split();
            let mut sq = BackloggedSubmissionQueue::new(sq);
            let mut last_tick = Instant::now();
            let mut shutting_down = false;

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
                        &self.pending_connections,
                        &mut state_ref,
                        &mut event_tx,
                    );

                    if let Some((file_and_meta, _)) = state_ref.state() {
                        if !prev_state_initialized {
                            prev_state_initialized = true;
                            event_tx
                                .enqueue(TorrentEvent::MetadataComplete(
                                    file_and_meta.metadata.clone(),
                                ))
                                .expect("event queue should never be full here");
                            for (_, connection) in self.connections.iter_mut() {
                                let msgs = std::mem::take(&mut connection.pre_meta_have_msgs);
                                // Get all piece msgs
                                for msg in msgs {
                                    connection.handle_message(msg, &mut state_ref, scope);
                                }
                                // TODO: Trigger unchoked peers
                            }
                        }
                    }

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
                    if let Err(err) = self.event_handler(&mut sq, io_event, &mut state_ref, scope) {
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

                if let Some((_, torrent_state)) = state_ref.state() {
                    torrent_state.update_torrent_status(&mut self.connections);
                }

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

                self.handle_commands(&mut sq, &mut shutting_down, &mut command_rc);
                if shutting_down && self.connections.is_empty() {
                    log::info!("All connections closed, shutdown complete");
                    return Ok(());
                }

                if let Some((_, torrent_state)) = state_ref.state() {
                    if torrent_state.is_complete {
                        log::info!("Torrent complete!");
                        if event_tx.enqueue(TorrentEvent::TorrentComplete).is_err() {
                            log::error!("Torrent completion event missed");
                        }
                    }
                }
            }
        });

        self.read_ring.unregister(&ring.submitter())?;
        result
    }

    fn handle_commands<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        shutting_down: &mut bool,
        command_rc: &mut Consumer<Command, 64>,
    ) {
        let existing_connections: HashSet<SockAddr> = self
            .connections
            .iter()
            .filter_map(|(_, peer)| peer.socket.peer_addr().ok())
            .collect();
        while let Some(command) = command_rc.dequeue() {
            match command {
                Command::ConnectToPeers(addrs) => {
                    // Don't connect to new peers if we are shutting down
                    if *shutting_down {
                        continue;
                    }
                    for addr in addrs.into_iter().map(|addr| addr.into()) {
                        if self.pending_connections.contains(&addr)
                            || existing_connections.contains(&addr)
                        {
                            continue;
                        }
                        if self.pending_connections.len() + self.connections.len() < MAX_CONNECTIONS
                        {
                            self.pending_connections.insert(addr.clone());
                            self.connect_to_peer(addr, sq);
                        }
                    }
                }
                Command::Stop => {
                    if !*shutting_down {
                        log::info!("Shutdown requested, closing all connections");
                        *shutting_down = true;
                        // Initiate graceful shutdown for all connections
                        for (conn_id, connection) in self.connections.iter_mut() {
                            log::info!("Closing connection to peer: {}", connection.peer_id);
                            connection.pending_disconnect = Some(DisconnectReason::ShuttingDown);
                            io_utils::stop_connection(
                                sq,
                                conn_id,
                                connection.socket.as_raw_fd(),
                                &mut self.events,
                            );
                        }
                    }
                }
            }
        }
        sq.sync();
    }

    fn connect_to_peer<Q: SubmissionQueue>(
        &mut self,
        addr: SockAddr,
        sq: &mut BackloggedSubmissionQueue<Q>,
    ) {
        let socket = match Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)) {
            Ok(socket) => socket,
            Err(e) => {
                log::error!("Failed to create socket: {}", e);
                return;
            }
        };
        //socket.set_recv_buffer_size(1 << 19).unwrap();
        let fd = socket.as_raw_fd();
        let event_idx = self.events.insert(EventType::Connect { socket, addr });
        let user_data = UserData::new(event_idx, None);

        let EventType::Connect { socket: _, addr } = &self.events[event_idx] else {
            unreachable!();
        };

        log::debug!("Connecting to peer: {:?}", addr);
        let connect_counter = metrics::counter!("peer_connect_attempts");
        connect_counter.increment(1);

        let connect_op = opcode::Connect::new(types::Fd(fd), addr.as_ptr() as *const _, addr.len())
            .build()
            .flags(io_uring::squeue::Flags::IO_LINK)
            .user_data(user_data.as_u64());
        let user_data = UserData::new(event_idx, None);
        let timeout_op = opcode::LinkTimeout::new(&CONNECT_TIMEOUT)
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

    fn event_handler<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        io_event: IoEvent,
        state: &mut StateRef<'state>,
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
                    state,
                    &mut self.connections,
                    &mut self.pending_connections,
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
                let addr = socket.peer_addr()?;
                if addr.is_ipv6() {
                    log::error!("Received connection from non ipv4 addr");
                    return Ok(());
                };

                log::info!("Accepted connection: {addr:?}");
                // Construct new recv token on accept, after that it lives forever and or is reused
                // since this is a recvmulti operation
                let read_token = self.events.insert(EventType::Recv { socket, addr });
                let user_data = UserData::new(read_token, None);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                sq.push(read_op);
            }
            EventType::Connect { socket, addr } => {
                log::info!("Connected to: {addr:?}");
                let connect_success_counter = metrics::counter!("peer_connect_success");
                connect_success_counter.increment(1);

                let buffer = self.write_pool.get_buffer();
                write_handshake(self.our_id, state.info_hash, buffer.inner);
                let fd = socket.as_raw_fd();
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[io_event.user_data.event_idx as usize],
                    EventType::Write { socket, addr },
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
            EventType::Write { socket, addr } => {
                let fd = socket.as_raw_fd();
                log::debug!("Wrote to unestablsihed connection");
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[io_event.user_data.event_idx as usize],
                    EventType::Recv { socket, addr },
                );
                // Write is only used for unestablished connections aka when doing handshake
                let handshake_counter = metrics::counter!("peer_handshake_attempt");
                handshake_counter.increment(1);
                debug_assert!(matches!(old, EventType::Dummy));
                let read_token = io_event.user_data.event_idx as usize;
                let user_data = UserData::new(read_token, None);
                // Multishot isn't used here to simplify error handling
                // when the read is invalid or otherwise doesn't lead to
                // a full connection which does have graceful shutdown mechanisms
                io_utils::recv(sq, user_data, fd, self.read_ring.bgid(), &HANDSHAKE_TIMEOUT);
            }
            EventType::ConnectedWrite { connection_idx: _ }
            | EventType::Cancel { connection_idx: _ }
            | EventType::Shutdown { connection_idx: _ } => {
                // neither replaced nor modified
                // TODO: add to metrics for writes?
                self.events.remove(io_event.user_data.event_idx as _);
            }
            EventType::Recv { socket, addr: _ } => {
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
                let parsed_handshake = parse_handshake(state.info_hash, handshake_data).unwrap();
                assert!(
                    self.pending_connections
                        .remove(&socket.peer_addr().unwrap())
                );

                let entry = self.connections.vacant_entry();
                let conn_id = entry.key();
                let peer_connection = PeerConnection::new(socket, conn_id, parsed_handshake);
                let id = peer_connection.peer_id;
                entry.insert(peer_connection);
                log::info!("Finished handshake! [{conn_id}]: {id}");
                let handshake_success_counter = metrics::counter!("peer_handshake_success");
                handshake_success_counter.increment(1);
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
                conn_parse_and_handle_msgs(connection, state, scope);
                if connection.extended_extension {
                    connection.outgoing_msgs_buffer.push(OutgoingMsg {
                        message: extension_handshake_msg(state),
                        ordered: true,
                    });
                }
                // Recv has been complete, move over to multishot, same user data
                io_utils::recv_multishot(sq, io_event.user_data, fd, self.read_ring.bgid());

                let bitfield_msg = if let Some((_, torrent_state)) = state.state() {
                    let completed = torrent_state.piece_selector.completed_clone();
                    let message = if completed.all() {
                        peer_protocol::PeerMessage::HaveAll
                    } else if completed.not_any() {
                        peer_protocol::PeerMessage::HaveNone
                    } else {
                        peer_protocol::PeerMessage::Bitfield(completed.into())
                    };
                    // sent as first message after handshake
                    OutgoingMsg {
                        message,
                        ordered: true,
                    }
                } else {
                    OutgoingMsg {
                        message: peer_protocol::PeerMessage::HaveNone,
                        ordered: true,
                    }
                };
                connection.outgoing_msgs_buffer.push(bitfield_msg);
            }
            EventType::ConnectedRecv { connection_idx } => {
                // The event is reused and not replaced
                std::mem::swap(
                    &mut event,
                    &mut self.events[io_event.user_data.event_idx as usize],
                );
                let len = ret as usize;
                if len == 0 {
                    let mut connection = self.connections.remove(connection_idx);
                    log::debug!(
                        "[PeerId: {}] No more data, graceful shutdown complete",
                        connection.peer_id
                    );
                    self.events.remove(io_event.user_data.event_idx as _);
                    // Consider moving to func
                    if let Some((_, torrent_state)) = state.state() {
                        connection.release_all_pieces(torrent_state);
                        // Don't count disconnected peers
                        if !connection.is_choking {
                            torrent_state.num_unchoked -= 1;
                        }
                    }
                    io_utils::close_socket(sq, connection.socket, &mut self.events);
                    return Ok(());
                }
                let connection = &mut self.connections[connection_idx];
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
                conn_parse_and_handle_msgs(connection, state, scope);
            }
            EventType::Close => {
                self.events.remove(io_event.user_data.event_idx as _);
            }
            EventType::Dummy => unreachable!(),
        }
        Ok(())
    }
}

fn conn_parse_and_handle_msgs<'scope, 'f_store: 'scope>(
    connection: &mut PeerConnection,
    state: &mut StateRef<'f_store>,
    scope: &Scope<'scope>,
) {
    while let Some(parse_result) = connection.stateful_decoder.next() {
        match parse_result {
            Ok(peer_message) => {
                connection.handle_message(peer_message, state, scope);
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

fn report_tick_metrics(
    state: &mut StateRef<'_>,
    connections: &Slab<PeerConnection>,
    pending_connections: &HashSet<SockAddr>,
    event_tx: &mut Producer<TorrentEvent, 512>,
) {
    let mut pieces_completed = 0;
    let mut pieces_allocated = 0;
    if let Some((_, torrent_state)) = state.state() {
        let total_completed = torrent_state.piece_selector.total_completed();
        let counter = metrics::counter!("pieces_completed");
        counter.absolute(total_completed as u64);
        let total_allocated = torrent_state.piece_selector.total_allocated();
        let gauge = metrics::gauge!("pieces_allocated");
        gauge.set(total_allocated as u32);
        let gauge = metrics::gauge!("num_unchoked");
        gauge.set(torrent_state.num_unchoked);
        pieces_completed = total_completed;
        pieces_allocated = total_allocated;
    }
    let gauge = metrics::gauge!("num_connections");
    gauge.set(connections.len() as u32);
    let gauge = metrics::gauge!("num_pending_connections");
    gauge.set(pending_connections.len() as u32);

    if event_tx
        .enqueue(TorrentEvent::TorrentMetrics {
            pieces_completed,
            pieces_allocated,
            num_connections: connections.len(),
        })
        .is_err()
    {
        log::error!("Torrent metrics event missed")
    }
}

pub(crate) fn tick<'scope, 'state: 'scope>(
    tick_delta: &Duration,
    connections: &mut Slab<PeerConnection>,
    pending_connections: &HashSet<SockAddr>,
    torrent_state: &mut StateRef<'state>,
    event_tx: &mut Producer<TorrentEvent, 512>,
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
        if let Some((file_and_info, torrent_state)) = torrent_state.state() {
            // TODO: If we are not using fast extension this might be triggered by a snub
            if let Some(time) = connection.last_received_subpiece {
                if time.elapsed() > connection.request_timeout() {
                    // error just to make more visible
                    log::error!("TIMEOUT: {}", connection.peer_id);
                    connection.on_request_timeout(torrent_state, &file_and_info.file_store);
                } else if connection.snubbed {
                    // Did not timeout
                    connection.snubbed = false;
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
    }
    if let Some((file_and_info, torrent_state)) = torrent_state.state() {
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
                if let Some(next_piece) = torrent_state
                    .piece_selector
                    .next_piece(peer_key, &mut peer.endgame)
                {
                    let mut queue = torrent_state.allocate_piece(
                        next_piece,
                        peer.conn_id,
                        &file_and_info.file_store,
                    );
                    let queue_len = queue.len();
                    peer.append_and_fill(&mut queue);
                    // Remove all subpieces from available bandwidth
                    bandwidth -= (queue_len).min(bandwidth);
                } else {
                    break;
                }
            }
            peer.fill_request_queue();
            peer.report_metrics(event_tx);
        }
    }

    report_tick_metrics(torrent_state, connections, pending_connections, event_tx);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;
    use crate::peer_protocol::generate_peer_id;
    use crate::test_utils::setup_test;
    use heapless::spsc::Queue;
    use io_uring::IoUring;
    use metrics::Key;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};
    use std::net::{SocketAddrV4, TcpListener};
    use std::time::Duration;

    #[test]
    fn handshake_timeout() {
        env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .init();

        let debbuging = DebuggingRecorder::new();
        let snapshotter = debbuging.snapshotter();
        // Setup test environment

        const HANDSHAKE_SHOULD_TIMEOUT: u64 = 8;

        // Create a listener that will accept connections but not respond
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let addr = SocketAddrV4::new([127, 0, 0, 1].into(), addr.port());

        let mut command_q = heapless::spsc::Queue::new();
        let (mut command_tx, command_rc) = command_q.split();
        let mut event_q = Queue::<TorrentEvent, 512>::new();
        let (event_tx, _event_rx) = event_q.split();
        // Spawn a thread to accept the connection but not respond
        let simulated_peer_thread = std::thread::spawn(move || {
            // Send a connection attempt to our listener
            let (_socket, _) = listener.accept().unwrap();
            // Keep the socket open but don't send any data
            std::thread::sleep(Duration::from_secs(HANDSHAKE_SHOULD_TIMEOUT));
        });
        std::thread::scope(|s| {
            s.spawn(move || {
                let mut download_state = setup_test();
                metrics::with_local_recorder(&debbuging, || {
                    let our_id = generate_peer_id();
                    let mut event_loop = EventLoop::new(our_id, Slab::new());
                    let ring = IoUring::builder()
                        .setup_single_issuer()
                        .setup_clamp()
                        .setup_cqsize(4096)
                        .setup_defer_taskrun()
                        .setup_coop_taskrun()
                        .build(4096)
                        .unwrap();
                    let result = event_loop.run(ring, &mut download_state, event_tx, command_rc);
                    assert!(result.is_ok());
                })
            });
            command_tx
                .enqueue(Command::ConnectToPeers(vec![addr]))
                .unwrap();
            std::thread::sleep(Duration::from_secs(HANDSHAKE_SHOULD_TIMEOUT));
            command_tx.enqueue(Command::Stop).unwrap();
            simulated_peer_thread.join().unwrap();

            let snapshot = snapshotter.snapshot();
            #[allow(clippy::mutable_key_type)]
            let metrics = snapshot.into_hashmap();
            let val = metrics.get(&CompositeKey::new(
                MetricKind::Counter,
                Key::from_name("peer_handshake_timeout"),
            ));
            let DebugValue::Counter(num_timeouts) = val.unwrap().2 else {
                unreachable!();
            };
            assert_eq!(num_timeouts, 1);
        });
    }

    // // Timeouts when accepting an incoming connection is handled properly
    // #[test]
    // fn accept_handshake_timeout() {
    //     todo!()
    // }

    // // Invalid handshakes are dealt with properly
    // #[test]
    // fn invalid_handshake() {
    //     todo!()
    // }

    // // Tests that the handshake is valid and that we send a proper bitfield afterwards
    // #[test]
    // fn valid_handshake() {
    //     todo!()
    // }
}
