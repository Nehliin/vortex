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
use libc::ECANCELED;
use rayon::Scope;
use slotmap::{Key, KeyData, SlotMap, new_key_type};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    Command, Error, State, StateRef, TorrentEvent,
    buf_pool::BufferPool,
    buf_ring::{Bgid, BufferRing},
    io_utils::{self, BackloggedSubmissionQueue, SubmissionQueue},
    peer_comm::{extended_protocol::extension_handshake_msg, peer_connection::ConnectionState},
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
    Connect {
        socket: Socket,
        addr: SockAddr,
    },
    Write {
        socket: Socket,
        addr: SockAddr,
    },
    Recv {
        socket: Socket,
        addr: SockAddr,
    },
    ConnectedWrite {
        connection_idx: ConnectionId,
    },
    ConnectedRecv {
        connection_idx: ConnectionId,
    },
    Cancel,
    Close {
        maybe_connection_idx: Option<ConnectionId>,
    },
    // Dummy used to allow stable keys in the slab
    Dummy,
}

new_key_type! {
    pub struct EventId;
}

new_key_type! {
    pub struct ConnectionId;
}

#[derive(Debug)]
pub struct EventData {
    pub typ: EventType,
    pub buffer_idx: Option<usize>,
}

#[allow(clippy::too_many_arguments)]
fn event_error_handler<'state, Q: SubmissionQueue>(
    sq: &mut BackloggedSubmissionQueue<Q>,
    error_code: u32,
    event_data_idx: EventId,
    events: &mut SlotMap<EventId, EventData>,
    state_ref: &mut StateRef<'state>,
    connections: &mut SlotMap<ConnectionId, PeerConnection>,
    pending_connections: &mut HashSet<SockAddr>,
    bgid: Bgid,
) -> io::Result<()> {
    match error_code as i32 {
        libc::ENOBUFS => {
            // TODO: statistics
            let event = &events[event_data_idx];
            log::warn!("Ran out of buffers!, resubmitting recv op");
            // Ran out of buffers!
            match &event.typ {
                EventType::Recv { socket, addr: _ } => {
                    let fd = socket.as_raw_fd();
                    io_utils::recv(sq, event_data_idx, fd, bgid, &HANDSHAKE_TIMEOUT);
                    Ok(())
                }
                EventType::ConnectedRecv { connection_idx } => {
                    if let ConnectionState::Connected(socket) =
                        &connections[*connection_idx].connection_state
                    {
                        io_utils::recv_multishot(sq, event_data_idx, socket.as_raw_fd(), bgid);
                    }
                    Ok(())
                }
                _ => unreachable!(),
            }
        }
        libc::ETIME => {
            let event = events.remove(event_data_idx).unwrap();
            let socket = match event.typ {
                EventType::Connect { socket, addr } => {
                    log::debug!("[{}] Connect timed out!", addr.as_socket().unwrap());
                    assert!(pending_connections.remove(&addr));
                    let connect_fail_counter = metrics::counter!("peer_connect_timeout");
                    connect_fail_counter.increment(1);
                    socket
                }
                EventType::Recv { socket, addr } => {
                    log::debug!("[{}] Handshake timed out!", addr.as_socket().unwrap());
                    assert!(pending_connections.remove(&addr));
                    let handshake_timeout_counter = metrics::counter!("peer_handshake_timeout");
                    handshake_timeout_counter.increment(1);
                    socket
                }
                _ => unreachable!(),
            };
            io_utils::close_socket(sq, socket, None, events);
            Ok(())
        }
        libc::ECONNRESET => {
            let event = events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Write { socket, addr } | EventType::Recv { socket, addr } => {
                    log::error!(
                        "[{}] Connection reset before handshake completed",
                        addr.as_socket().unwrap()
                    );
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, None, events);
                }
                EventType::ConnectedRecv { connection_idx }
                | EventType::ConnectedWrite { connection_idx } => {
                    let connection = &mut connections[connection_idx];
                    log::error!("Peer [{}] Connection reset", connection.peer_id);
                    connection.disconnect(sq, events, state_ref);
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::EPIPE => {
            let event = events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Write { socket, addr } => {
                    log::warn!(
                        "[{}] Attempted to write to closed connection",
                        addr.as_socket().unwrap()
                    );
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, None, events);
                }
                EventType::ConnectedWrite { connection_idx } => {
                    if let Some(connection) = connections.get_mut(connection_idx) {
                        log::error!(
                            "Peer [{}] EPIPE received when writing to connection",
                            connection.peer_id
                        );
                        connection.disconnect(sq, events, state_ref);
                    } else {
                        // I guess this might happpen when multiple writes are queued up after
                        // each other
                        log::error!("PIPE received after connection has already been removed",);
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::ECONNREFUSED | libc::EHOSTUNREACH => {
            // Failling to connect due to this is not really an error due to
            // the likelyhood of being stale info in the DHT
            let event = events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Connect { socket, addr } => {
                    log::debug!(
                        "[{}] Connection failed {event_data_idx:?}",
                        addr.as_socket().unwrap()
                    );
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, None, events);
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::ECANCELED => {
            // This is the timeout or the connect operation being cancelled
            // the event should have be deleted by the ETIME handler or the
            // successful connection event
            log::trace!("Event cancelled");
            Ok(())
        }
        _ => {
            let err = std::io::Error::from_raw_os_error(error_code as i32);
            if !events.contains_key(event_data_idx) {
                log::error!(
                    "Unhandled error: {err}, event didn't exist in events, id: {event_data_idx:?}",
                );
            } else {
                let event = events.remove(event_data_idx).unwrap();
                let err_str = format!("Unhandled error: {err}, event type: {event:?}");
                match event.typ {
                    EventType::Connect { socket, addr }
                    | EventType::Write { socket, addr }
                    | EventType::Recv { socket, addr } => {
                        log::error!("[{}] {err_str}", addr.as_socket().unwrap());
                        io_utils::close_socket(sq, socket, None, events);
                    }
                    EventType::ConnectedWrite { connection_idx }
                    | EventType::ConnectedRecv { connection_idx } => {
                        let connection = &mut connections[connection_idx];
                        log::error!("Peer [{}] unhandled error: {err}", connection.peer_id);
                        connection.disconnect(sq, events, state_ref);
                    }
                    EventType::Close {
                        maybe_connection_idx,
                    } => {
                        log::error!("{err_str}, attempting to close: {maybe_connection_idx:?}");
                        return Err(err);
                    }
                    EventType::Cancel | EventType::Accept | EventType::Dummy => {
                        log::error!("{err_str}");
                        return Err(err);
                    }
                }
            }
            Err(err)
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RawIoEvent {
    event_data_idx: EventId,
    result: Result<i32, u32>,
    read_bid: Option<u16>,
    is_more: bool,
}

impl From<Entry> for RawIoEvent {
    fn from(cqe: Entry) -> Self {
        let event_data_idx = EventId::from(KeyData::from_ffi(cqe.user_data()));
        let result = if cqe.result() < 0 {
            Err((-cqe.result()) as u32)
        } else {
            Ok(cqe.result())
        };
        let read_bid = io_uring::cqueue::buffer_select(cqe.flags());
        let is_more = io_uring::cqueue::more(cqe.flags());
        Self {
            event_data_idx,
            result,
            read_bid,
            is_more,
        }
    }
}

const CQE_WAIT_TIME: &Timespec = &Timespec::new().nsec(250_000_000);

pub struct EventLoop {
    events: SlotMap<EventId, EventData>,
    write_pool: BufferPool,
    read_ring: BufferRing,
    // TODO: Merge these or consider disconnecting
    // pending peers as soon as max connections is reached
    connections: SlotMap<ConnectionId, PeerConnection>,
    pending_connections: HashSet<SockAddr>,
    our_id: PeerId,
}

impl<'scope, 'state: 'scope> EventLoop {
    pub fn new(our_id: PeerId, events: SlotMap<EventId, EventData>) -> Self {
        Self {
            events,
            write_pool: BufferPool::new(256, (SUBPIECE_SIZE * 2) as _),
            read_ring: BufferRing::new(1, 256, (SUBPIECE_SIZE * 2) as _).unwrap(),
            connections: SlotMap::with_capacity_and_key(MAX_CONNECTIONS),
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
                    for connection in self.connections.values_mut() {
                        if let Some(reason) = &connection.pending_disconnect {
                            log::warn!("Disconnect: {} reason {reason}", connection.peer_id,);
                            let counter = metrics::counter!("disconnects");
                            counter.increment(1);
                            connection.disconnect(&mut sq, &mut self.events, &mut state_ref);
                        }
                    }
                    sq.sync();
                }

                for cqe in &mut cq {
                    let io_event = RawIoEvent::from(cqe);
                    if let Some(event) = self.events.get_mut(io_event.event_data_idx) {
                        log::trace!(
                            "idx: {:?}, type: {:?}, io_event {io_event:?}",
                            io_event.event_data_idx,
                            event
                        );
                        let buffer_idx = event.buffer_idx.take();
                        if let Err(err) =
                            self.event_handler(&mut sq, io_event, &mut state_ref, scope)
                        {
                            log::error!("Error handling event: {err}");
                        }
                        // time to return any potential write buffers
                        if let Some(write_idx) = buffer_idx {
                            self.write_pool.return_buffer(write_idx);
                        }
                    } else {
                        let err = io_event.result.unwrap_err();
                        // Only cancellation errors are expected here
                        // since linked timeouts share event id with
                        // the event being on a timer
                        assert_eq!(err as i32, ECANCELED);
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
                    if let ConnectionState::Connected(socket) = &connection.connection_state {
                        for msg in connection.outgoing_msgs_buffer.iter_mut() {
                            let conn_fd = socket.as_raw_fd();
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
                    }
                    connection.outgoing_msgs_buffer.clear();
                }
                sq.sync();

                self.handle_commands(&mut sq, &mut shutting_down, &mut command_rc, &mut state_ref);
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
        state_ref: &mut StateRef<'state>,
    ) {
        let existing_connections: HashSet<SockAddr> = self
            .connections
            .iter()
            .map(|(_, peer)| SockAddr::from(peer.peer_addr))
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
                        for connection in self.connections.values_mut() {
                            log::info!(
                                "[{}] Closing connection to peer: {}",
                                connection.peer_id,
                                connection.peer_addr,
                            );
                            connection.disconnect(sq, &mut self.events, state_ref);
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
                log::error!("Failed to create socket: {e}");
                return;
            }
        };
        
        let event_idx = self.events.insert(EventData {
            typ: EventType::Connect { socket, addr },
            buffer_idx: None,
        });

        let EventType::Connect { socket, addr } = &self.events[event_idx].typ else {
            unreachable!();
        };

        log::debug!(
            "[{}] Connecting to peer",
            addr.as_socket().expect("must be AF_INET")
        );
        let connect_counter = metrics::counter!("peer_connect_attempts");
        connect_counter.increment(1);

        let connect_op = opcode::Connect::new(
            types::Fd(socket.as_raw_fd()),
            addr.as_ptr() as *const _,
            addr.len(),
        )
        .build()
        .flags(io_uring::squeue::Flags::IO_LINK)
        .user_data(event_idx.data().as_ffi());
        let timeout_op = opcode::LinkTimeout::new(&CONNECT_TIMEOUT)
            .build()
            .user_data(event_idx.data().as_ffi());
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
        io_event: RawIoEvent,
        state: &mut StateRef<'state>,
        scope: &Scope<'scope>,
    ) -> io::Result<()> {
        let ret = match io_event.result {
            Ok(ret) => ret,
            Err(error_code) => {
                return event_error_handler(
                    sq,
                    error_code,
                    io_event.event_data_idx,
                    &mut self.events,
                    state,
                    &mut self.connections,
                    &mut self.pending_connections,
                    self.read_ring.bgid(),
                );
            }
        };
        let mut event = EventType::Dummy;
        std::mem::swap(&mut event, &mut self.events[io_event.event_data_idx].typ);
        match event {
            EventType::Accept => {
                // The event is reused and not replaced
                std::mem::swap(&mut event, &mut self.events[io_event.event_data_idx].typ);
                let fd = ret;
                let socket = unsafe { Socket::from_raw_fd(fd) };
                let addr = socket.peer_addr()?;
                if addr.is_ipv6() {
                    log::error!("Received connection from non ipv4 addr");
                    return Ok(());
                };

                log::info!(
                    "Accepted connection: {:?}",
                    addr.as_socket().expect("must be AF_INET")
                );
                // Construct new recv token on accept, after that it lives forever and or is reused
                // since this is a recvmulti operation
                let read_event_id = self.events.insert(EventData {
                    typ: EventType::Recv { socket, addr },
                    buffer_idx: None,
                });
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(read_event_id.data().as_ffi())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                sq.push(read_op);
            }
            EventType::Connect { socket, addr } => {
                log::info!(
                    "Connected to: {}",
                    addr.as_socket().expect("must be AF_INET")
                );
                let connect_success_counter = metrics::counter!("peer_connect_success");
                connect_success_counter.increment(1);

                let buffer = self.write_pool.get_buffer();
                write_handshake(self.our_id, state.info_hash, buffer.inner);
                let fd = socket.as_raw_fd();
                let old = self.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));
                let write_event_id = self.events.insert(EventData {
                    typ: EventType::Write { socket, addr },
                    buffer_idx: Some(buffer.index),
                });
                let write_op = opcode::Write::new(
                    types::Fd(fd),
                    buffer.inner.as_ptr(),
                    // TODO: Handle this better
                    HANDSHAKE_SIZE as u32,
                )
                .build()
                .user_data(write_event_id.data().as_ffi());
                sq.push(write_op);
            }
            EventType::Write { socket, addr } => {
                let fd = socket.as_raw_fd();
                log::debug!(
                    "Wrote to unestablsihed connection: {}",
                    addr.as_socket().expect("must be AF_INET")
                );
                let old = self.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));
                let read_event_id = self.events.insert(EventData {
                    typ: EventType::Recv { socket, addr },
                    buffer_idx: None,
                });
                // Write is only used for unestablished connections aka when doing handshake
                let handshake_counter = metrics::counter!("peer_handshake_attempt");
                handshake_counter.increment(1);
                // Multishot isn't used here to simplify error handling
                // when the read is invalid or otherwise doesn't lead to
                // a full connection which does have graceful shutdown mechanisms
                io_utils::recv(
                    sq,
                    read_event_id,
                    fd,
                    self.read_ring.bgid(),
                    &HANDSHAKE_TIMEOUT,
                );
            }
            EventType::Cancel => {
                log::trace!("Cancel event completed");
                self.events.remove(io_event.event_data_idx);
            }
            EventType::ConnectedWrite { connection_idx: _ } => {
                // TODO: add to metrics for writes?
                self.events.remove(io_event.event_data_idx);
            }
            EventType::Recv { socket, addr } => {
                let fd = socket.as_raw_fd();
                let len = ret as usize;
                let addr = addr.as_socket().expect("must be AF_INET");
                if len == 0 {
                    log::debug!("[{addr}] No more data when expecting handshake from connection",);
                    self.events.remove(io_event.event_data_idx);
                    io_utils::close_socket(sq, socket, None, &mut self.events);
                    return Ok(());
                }
                // TODO: This could happen due to networks splitting the handshake up
                // so it should be dealt with better, but since the handshake is so
                // small (well below MTU) I suspect that to be rare
                if len < HANDSHAKE_SIZE {
                    log::error!("[{addr}] Didn't receive enough data to parse handshake",);
                    self.events.remove(io_event.event_data_idx);
                    io_utils::close_socket(sq, socket, None, &mut self.events);
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
                        .remove(&<std::net::SocketAddr as Into<socket2::SockAddr>>::into(
                            addr
                        ))
                );

                let conn_id = self.connections.insert_with_key(|conn_id| {
                    PeerConnection::new(socket, addr, conn_id, parsed_handshake)
                });
                log::info!("[{addr}] Finished handshake! [{conn_id:?}]");
                let handshake_success_counter = metrics::counter!("peer_handshake_success");
                handshake_success_counter.increment(1);
                // We are now connected!
                // The event is replaced (this removes the dummy)
                let old = self.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));
                let recv_multi_id = self.events.insert(EventData {
                    typ: EventType::ConnectedRecv {
                        connection_idx: conn_id,
                    },
                    buffer_idx: None,
                });

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
                io_utils::recv_multishot(sq, recv_multi_id, fd, self.read_ring.bgid());

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
                std::mem::swap(&mut event, &mut self.events[io_event.event_data_idx].typ);
                let len = ret as usize;
                if len == 0 {
                    let connection = &mut self.connections[connection_idx];
                    log::debug!(
                        "[PeerId: {}] No more data: {}",
                        connection.peer_id,
                        connection.peer_addr,
                    );
                    let counter = metrics::counter!("graceful_disconnect");
                    counter.increment(1);
                    self.events.remove(io_event.event_data_idx);
                    connection.disconnect(sq, &mut self.events, state);
                    return Ok(());
                }
                let connection = &mut self.connections[connection_idx];
                if !io_event.is_more {
                    if let ConnectionState::Connected(socket) = &connection.connection_state {
                        let fd = socket.as_raw_fd();
                        // restart the operation
                        io_utils::recv_multishot(
                            sq,
                            io_event.event_data_idx,
                            fd,
                            self.read_ring.bgid(),
                        );
                    }
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
            EventType::Close {
                maybe_connection_idx,
            } => {
                if let Some(connection_idx) = maybe_connection_idx {
                    self.connections.remove(connection_idx).unwrap();
                }
                self.events.remove(io_event.event_data_idx);
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
                log::error!("Failed {:?} decoding message: {err}", connection.conn_id);
                connection.pending_disconnect = Some(DisconnectReason::InvalidMessage);
                break;
            }
        }
    }
    connection.fill_request_queue();
}

fn report_tick_metrics(
    state: &mut StateRef<'_>,
    connections: &SlotMap<ConnectionId, PeerConnection>,
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
    connections: &mut SlotMap<ConnectionId, PeerConnection>,
    pending_connections: &HashSet<SockAddr>,
    torrent_state: &mut StateRef<'state>,
    event_tx: &mut Producer<TorrentEvent, 512>,
) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
    // 1. Calculate bandwidth (deal with initial start up)
    // 2. Go through them in order
    // 3. select pieces
    for connection in connections
        .values_mut()
        // Filter out connections that are pending diconnect
        .filter(|conn| conn.pending_disconnect.is_none())
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
                    // warn just to make more visible
                    log::warn!("TIMEOUT: {}", connection.peer_id);
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
