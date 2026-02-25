use std::{
    io,
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd},
    rc::Rc,
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use ahash::HashSet;
use ahash::HashSetExt;
use bytes::{BufMut, Bytes};
use heapless::spsc::Producer;
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
    buf_pool::{Buffer, BufferPool},
    buf_ring::{Bgid, BufferRing},
    file_store::DiskOp,
    io_utils::{self, BackloggedSubmissionQueue, SubmissionQueue},
    peer_comm::{
        extended_protocol::extension_handshake_msg,
        peer_connection::{ConnectionState, DisconnectReason, PeerConnection},
        peer_protocol::{self, HANDSHAKE_SIZE, PeerId, parse_handshake, write_handshake},
    },
    piece_selector::{self, SUBPIECE_SIZE},
    torrent::{
        CQE_WAIT_TIME_NS, Command, Config, Error, PeerMetrics, State, StateRef, TorrentEvent,
    },
};

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
        expected_write: usize,
    },
    Recv {
        socket: Socket,
        addr: SockAddr,
    },
    ConnectedWriteV {
        connection_idx: ConnectionId,
        // References to the buffers used in the vectored writes
        iovecs: Vec<libc::iovec>,
        // Cumulative offset into the buffers (for partial write retries)
        io_vec_offset: usize,
    },
    ConnectedRecv {
        connection_idx: ConnectionId,
    },
    DiskWrite {
        data: Rc<Buffer>,
        piece_idx: i32,
        #[cfg(feature = "metrics")]
        scheduled: Instant,
    },
    DiskRead {
        // Peer that requested the piece
        connection_idx: ConnectionId,
        // Full piece data
        data: Rc<Buffer>,
        piece_idx: i32,
        // Offset inside piece
        piece_offset: i32,
        #[cfg(feature = "metrics")]
        scheduled: Instant,
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
    pub buffers: Option<Vec<Buffer>>,
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
    inflight_disk_ops: &mut usize,
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
                    #[cfg(feature = "metrics")]
                    {
                        let connect_fail_counter = metrics::counter!("peer_connect_timeout");
                        connect_fail_counter.increment(1);
                    }
                    socket
                }
                EventType::Recv { socket, addr } => {
                    log::debug!("[{}] Handshake timed out!", addr.as_socket().unwrap());
                    assert!(pending_connections.remove(&addr));
                    #[cfg(feature = "metrics")]
                    {
                        let handshake_timeout_counter = metrics::counter!("peer_handshake_timeout");
                        handshake_timeout_counter.increment(1);
                    }
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
                EventType::Connect { socket, addr }
                | EventType::Write { socket, addr, .. }
                | EventType::Recv { socket, addr } => {
                    log::error!(
                        "[{}] Connection reset before handshake completed",
                        addr.as_socket().unwrap()
                    );
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, None, events);
                }
                EventType::ConnectedRecv { connection_idx }
                | EventType::ConnectedWriteV { connection_idx, .. } => {
                    // Don't worry about this if we've already removed the connection
                    if let Some(connection) = connections.get_mut(connection_idx) {
                        log::error!("Peer [{}] Connection reset", connection.peer_id);
                        connection.disconnect(sq, events, state_ref);
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::EPIPE => {
            let event = events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Write { socket, addr, .. } => {
                    log::warn!(
                        "[{}] Attempted to write to closed connection",
                        addr.as_socket().unwrap()
                    );
                    assert!(pending_connections.remove(&addr));
                    io_utils::close_socket(sq, socket, None, events);
                }
                EventType::ConnectedWriteV { connection_idx, .. } => {
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
            if let Some(event) = events.remove(event_data_idx) {
                let err_str = format!("Unhandled error code: {err}, event type: {event:?}");
                match event.typ {
                    EventType::Connect { socket, addr }
                    | EventType::Write { socket, addr, .. }
                    | EventType::Recv { socket, addr } => {
                        log::error!("[{}] {err_str}", addr.as_socket().unwrap());
                        io_utils::close_socket(sq, socket, None, events);
                    }
                    EventType::ConnectedWriteV { connection_idx, .. }
                    | EventType::ConnectedRecv { connection_idx } => {
                        if let Some(connection) = connections.get_mut(connection_idx) {
                            log::error!("Peer [{}] unhandled error: {err}", connection.peer_id);
                            connection.disconnect(sq, events, state_ref);
                        }
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
                    EventType::DiskWrite {
                        data, piece_idx, ..
                    }
                    | EventType::DiskRead {
                        data, piece_idx, ..
                    } => {
                        log::error!(
                            "{err_str} - Failed to write or read piece_idx to/from disk: {piece_idx}"
                        );
                        let state = state_ref
                            .state()
                            .expect("must have initialized state before starting disk io");
                        if let Ok(buffer) = Rc::try_unwrap(data) {
                            state.piece_buffer_pool.return_buffer(buffer);
                        }
                        *inflight_disk_ops -= 1;
                    }
                }
            } else {
                log::error!(
                    "Unhandled error: {err}, event didn't exist in events, id: {event_data_idx:?}",
                )
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

const CQE_WAIT_TIME: &Timespec = &Timespec::new().nsec(CQE_WAIT_TIME_NS);

pub struct EventLoop {
    events: SlotMap<EventId, EventData>,
    write_pool: BufferPool,
    read_ring: BufferRing,
    // TODO: Merge these or consider disconnecting
    // pending peers as soon as max connections is reached
    connections: SlotMap<ConnectionId, PeerConnection>,
    pending_connections: HashSet<SockAddr>,
    // How many file operations are inflight in the kernel
    inflight_disk_ops: usize,
    // This contains the queued up disk operations. It's owned
    // by the event loop instead of the file store so that FileStore
    // can remain Send
    queued_disk_operations: Vec<DiskOp>,
    our_id: PeerId,
}

impl<'scope, 'state: 'scope> EventLoop {
    pub fn new(our_id: PeerId, events: SlotMap<EventId, EventData>, config: &Config) -> Self {
        Self {
            events,
            write_pool: BufferPool::new(
                "network_write",
                config.write_buffer_pool_size,
                config.network_write_buffer_size,
            ),
            read_ring: BufferRing::new(
                1,
                config.read_buffer_pool_size,
                config.network_read_buffer_size,
            )
            .unwrap(),
            connections: SlotMap::with_capacity_and_key(config.max_connections),
            pending_connections: HashSet::with_capacity(config.max_connections),
            our_id,
            inflight_disk_ops: 0,
            queued_disk_operations: Vec::with_capacity(32),
        }
    }

    pub fn run(
        &mut self,
        mut ring: IoUring,
        state: &'state mut State,
        mut event_tx: Producer<TorrentEvent>,
        mut command_rc: Receiver<Command>,
        listener: TcpListener,
    ) -> Result<(), Error> {
        self.read_ring.register(&ring.submitter())?;

        let port = listener.local_addr().unwrap().port();
        self.setup_listener(
            types::Fd(listener.into_raw_fd()),
            port,
            &mut ring,
            &mut event_tx,
        );
        state.listener_port = Some(port);

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
                match submitter.submit_with_args(state_ref.config.completion_event_want, &args) {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {
                        log::warn!("Ring busy")
                    }
                    Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => {
                        #[cfg(feature = "metrics")]
                        {
                            let counter = metrics::counter!("cqe_wait_time_hit");
                            counter.increment(1);
                        }
                        log::trace!("CQE_WAIT_TIME was reached before target events")
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

                #[cfg(feature = "metrics")]
                {
                    let gauge = metrics::gauge!("write_pool_free_buffers");
                    gauge.set(self.write_pool.free_buffers() as u32);
                    let gauge = metrics::gauge!("write_pool_allocated_buffers");
                    gauge.set(self.write_pool.total_buffers() as u32);
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

                    if let Some(metadata) = state_ref.metadata()
                        && !prev_state_initialized
                    {
                        prev_state_initialized = true;
                        event_tx
                            .enqueue(TorrentEvent::MetadataComplete(metadata.clone()))
                            .expect("event queue should never be full here");
                        for (_, connection) in self.connections.iter_mut() {
                            let msgs = std::mem::take(&mut connection.pre_meta_have_msgs);
                            // Get all piece msgs
                            for msg in msgs {
                                connection.handle_message(
                                    msg,
                                    &mut state_ref,
                                    &mut self.queued_disk_operations,
                                    scope,
                                );
                            }
                        }
                    }

                    last_tick = Instant::now();
                    // Dealt with here to make tick easier to test
                    for connection in self.connections.values_mut() {
                        if let Some(reason) = &connection.pending_disconnect {
                            log::warn!("Disconnect: {} reason {reason}", connection.peer_id,);
                            #[cfg(feature = "metrics")]
                            {
                                let counter = metrics::counter!("disconnects");
                                counter.increment(1);
                            }
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
                        // Buffers must be provided to the event handler here
                        // so that partial writes can reschedule writes with
                        // the same buffer
                        let mut maybe_buffers = event.buffers.take();
                        if let Err(err) = self.event_handler(
                            &mut sq,
                            io_event,
                            &mut maybe_buffers,
                            &mut state_ref,
                            &mut event_tx,
                            scope,
                        ) {
                            log::error!("Error handling event: {err}");
                        }
                        // Now it's time to return any potential write buffers
                        if let Some(buffers) = maybe_buffers {
                            for buffer in buffers {
                                self.write_pool.return_buffer(buffer);
                            }
                        }
                    } else {
                        let err = io_event.result.unwrap_err();
                        // Only cancellation errors are expected here
                        // since linked timeouts share event id with
                        // the event being on a timer
                        //
                        // TODO: We might also end up here if we remove event id for a
                        // reoccuring event like recv_multi even though we've cancelled
                        // all events + close the socket if we've received multiple cqe for
                        // the event in one submission
                        assert_eq!(err as i32, ECANCELED);
                    }
                    // Ensure bids are always returned
                    if let Some(bid) = io_event.read_bid {
                        self.read_ring.return_bid(bid);
                    }
                }

                if let Some(torrent_state) = state_ref.state() {
                    torrent_state
                        .queue_disk_write_for_downloaded_pieces(&mut self.queued_disk_operations);
                    for disk_op in self.queued_disk_operations.drain(..) {
                        io_utils::disk_operation(
                            &mut self.events,
                            &mut sq,
                            disk_op,
                            &mut self.inflight_disk_ops,
                        );
                    }
                }

                for (conn_id, connection) in self
                    .connections
                    .iter_mut()
                    .filter(|(_, conn)| {
                        // The connection must have something to send
                        !conn.outgoing_msgs_buffer.is_empty()
                    })
                    .filter(|(_, conn)| {
                        // The connection may not have anything inflight, it can cause
                        // interleaved writes under high load.
                        #[cfg(feature = "metrics")]
                        {
                            if conn.network_write_inflight {
                                let counter = metrics::counter!("network_write_blocked");
                                counter.increment(1);
                            }
                        }
                        !conn.network_write_inflight
                    })
                {
                    if let ConnectionState::Connected(socket) = &connection.connection_state {
                        let mut buffers = Vec::new();
                        let mut current_buffer = self.write_pool.get_buffer();
                        let conn_fd = socket.as_raw_fd();
                        for message in connection.outgoing_msgs_buffer.iter() {
                            let size = message.encoded_size();
                            if current_buffer.remaining_mut() >= size {
                                message.encode(&mut current_buffer);
                            } else {
                                // Buffer is full, get a new one
                                buffers.push(current_buffer);
                                current_buffer = self.write_pool.get_buffer();
                                message.encode(&mut current_buffer);
                            }
                        }
                        buffers.push(current_buffer);
                        connection.network_write_inflight = true;
                        io_utils::writev_to_connection(
                            conn_id,
                            conn_fd,
                            &mut self.events,
                            &mut sq,
                            buffers,
                            0,
                        );
                    }
                    connection.outgoing_msgs_buffer.clear();
                }
                sq.sync();

                self.handle_commands(&mut sq, &mut shutting_down, &mut command_rc, &mut state_ref);
                if shutting_down && self.connections.is_empty() && self.inflight_disk_ops == 0 {
                    log::info!("All connections closed, shutdown complete");
                    return Ok(());
                }
            }
        });

        self.read_ring.unregister(&ring.submitter())?;
        result
    }

    fn setup_listener(
        &mut self,
        listener_fd: Fd,
        port: u16,
        ring: &mut IoUring,
        event_tx: &mut Producer<TorrentEvent>,
    ) {
        let event_idx: EventId = self.events.insert(EventData {
            typ: EventType::Accept,
            buffers: None,
        });
        let listener_user_data = event_idx.data().as_ffi();
        let accept_op = opcode::AcceptMulti::new(listener_fd)
            .build()
            .user_data(listener_user_data);
        unsafe {
            ring.submission().push(&accept_op).unwrap();
        }
        ring.submission().sync();
        self.state = EventLoopState::Running {
            listener_fd,
            listener_user_data,
        };
        // Emit listener started event
        if event_tx
            .enqueue(TorrentEvent::ListenerStarted { port })
            .is_err()
        {
            log::error!("Failed to enqueue ListenerStarted event");
        }
    }

    fn write_handshake<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        info_hash: [u8; 20],
        socket: Socket,
        addr: SockAddr,
    ) {
        let mut buffer = self.write_pool.get_buffer();
        if buffer.remaining_mut() < HANDSHAKE_SIZE {
            panic!("Buffer size is too small for sending a handshake");
        }
        write_handshake(self.our_id, info_hash, &mut buffer);
        io_utils::write(sq, &mut self.events, socket, addr, buffer)
    }

    fn handle_commands<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        shutting_down: &mut bool,
        command_rc: &mut Receiver<Command>,
        state_ref: &mut StateRef<'state>,
    ) {
        let existing_connections: HashSet<SockAddr> = self
            .connections
            .iter()
            .map(|(_, peer)| SockAddr::from(peer.peer_addr))
            .collect();
        for command in command_rc.try_iter() {
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
                        if self.pending_connections.len() + self.connections.len()
                            < state_ref.config.max_connections
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
            buffers: None,
        });

        let EventType::Connect { socket, addr } = &self.events[event_idx].typ else {
            unreachable!();
        };

        log::debug!(
            "[{}] Connecting to peer",
            addr.as_socket().expect("must be AF_INET")
        );
        #[cfg(feature = "metrics")]
        {
            let connect_counter = metrics::counter!("peer_connect_attempts");
            connect_counter.increment(1);
        }

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
        write_buffers: &mut Option<Vec<Buffer>>,
        state: &mut StateRef<'state>,
        event_tx: &mut Producer<TorrentEvent>,
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
                    &mut self.inflight_disk_ops,
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
                    io_utils::close_socket(sq, socket, None, &mut self.events);
                    return Ok(());
                };

                log::info!(
                    "Accepted connection: {:?}",
                    addr.as_socket().expect("must be AF_INET")
                );
                // Trigger a write handshake here so we end up in the same code path
                // as outgoing connections. It will simplify things greatly
                self.write_handshake(sq, *state.info_hash(), socket, addr);
            }
            EventType::Connect { socket, addr } => {
                log::info!(
                    "Connected to: {}",
                    addr.as_socket().expect("must be AF_INET")
                );
                #[cfg(feature = "metrics")]
                {
                    let connect_success_counter = metrics::counter!("peer_connect_success");
                    connect_success_counter.increment(1);
                }
                let old = self.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));

                self.write_handshake(sq, *state.info_hash(), socket, addr);
            }
            EventType::Write {
                socket,
                addr,
                expected_write,
            } => {
                if ret as usize == expected_write {
                    let fd = socket.as_raw_fd();
                    log::debug!(
                        "Wrote to unestablsihed connection: {}",
                        addr.as_socket().expect("must be AF_INET")
                    );
                    let old = self.events.remove(io_event.event_data_idx).unwrap();
                    debug_assert!(matches!(old.typ, EventType::Dummy));
                    let read_event_id = self.events.insert(EventData {
                        typ: EventType::Recv { socket, addr },
                        buffers: None,
                    });
                    // Write is only used for unestablished connections aka when doing handshake
                    #[cfg(feature = "metrics")]
                    {
                        let handshake_counter = metrics::counter!("peer_handshake_attempt");
                        handshake_counter.increment(1);
                    }
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
                } else {
                    // We don't deal with partial writes for handshakes, it should never happen
                    log::error!(
                        "Failed to write to unestablished connection: {}",
                        addr.as_socket().expect("must be AF_INET")
                    );
                    io_utils::close_socket(sq, socket, None, &mut self.events);
                }
            }
            EventType::DiskWrite {
                data,
                piece_idx,
                #[cfg(feature = "metrics")]
                scheduled,
            } => {
                self.events.remove(io_event.event_data_idx);
                let state = state
                    .state()
                    .expect("must have initialized state before starting disk io");
                #[cfg(feature = "metrics")]
                {
                    use metrics::histogram;
                    let histogram = histogram!("disk_write_time_ms");
                    histogram.record(scheduled.elapsed().as_millis() as u32);
                }
                if let Ok(buffer) = Rc::try_unwrap(data) {
                    // If we are here we have completed the piece
                    state.complete_piece(piece_idx, &mut self.connections, event_tx, buffer);
                }
                self.inflight_disk_ops -= 1;
            }
            EventType::DiskRead {
                data,
                piece_idx,
                connection_idx,
                piece_offset,
                #[cfg(feature = "metrics")]
                scheduled,
            } => {
                self.events.remove(io_event.event_data_idx);
                let state = state
                    .state()
                    .expect("must have initialized state before starting disk io");
                #[cfg(feature = "metrics")]
                {
                    use metrics::histogram;
                    let histogram = histogram!("disk_read_time_ms");
                    histogram.record(scheduled.elapsed().as_millis() as u32);
                }
                if let Ok(buffer) = Rc::try_unwrap(data) {
                    // The connection may have been closed inbetween the read being scheduled
                    // and it completing. That's fine
                    if let Some(connection) = self.connections.get_mut(connection_idx) {
                        let start_idx = piece_offset as usize;
                        let end_idx = start_idx
                            + state
                                .piece_selector
                                .piece_len(piece_idx)
                                .min(SUBPIECE_SIZE as u32) as usize;
                        connection.send_piece(
                            piece_idx,
                            piece_offset,
                            // TODO: avoid this copy by caching the piece buffer and make the Piece message
                            // take an enum of either Buffer or Bytes?
                            Bytes::copy_from_slice(&buffer.raw_slice()[start_idx..end_idx]),
                        );
                    }
                    state.piece_buffer_pool.return_buffer(buffer);
                }
                self.inflight_disk_ops -= 1;
            }
            EventType::Cancel => {
                log::trace!("Cancel event completed");
                self.events.remove(io_event.event_data_idx);
            }
            EventType::ConnectedWriteV {
                connection_idx,
                iovecs,
                io_vec_offset,
            } => {
                // TODO: add to metrics for writes?
                self.events.remove(io_event.event_data_idx);
                let expected_written = iovecs.iter().map(|io| io.iov_len).sum();
                let bytes_written = ret as usize;
                let Some(connection) = self.connections.get_mut(connection_idx) else {
                    log::warn!("Connection was lost after write was handled");
                    return Ok(());
                };
                connection.on_network_write(bytes_written);
                if bytes_written < expected_written {
                    log::warn!(
                        "[PeerId: {}] Partial write {bytes_written}, expected {expected_written}, TCP send buffer is most likely full",
                        connection.peer_id,
                    );
                    if let ConnectionState::Connected(socket) = &connection.connection_state {
                        let buffer = write_buffers.take().unwrap();
                        // Reschedule a write for the remaining data using cumulative offset
                        let new_offset = io_vec_offset + bytes_written;
                        io_utils::writev_to_connection(
                            connection.conn_id,
                            socket.as_raw_fd(),
                            &mut self.events,
                            sq,
                            buffer,
                            new_offset,
                        );
                    } else {
                        log::warn!(
                            "[PeerId: {}] peer not connected when scheduling new write after a partial write",
                            connection.peer_id,
                        );
                    }
                } else {
                    connection.network_write_inflight = false;
                }
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
                let parsed_handshake = match parse_handshake(*state.info_hash(), handshake_data) {
                    Ok(handshake) => handshake,
                    Err(err) => {
                        log::error!("[{addr}] Failed to parse handshake: {err}",);
                        self.events.remove(io_event.event_data_idx);
                        io_utils::close_socket(sq, socket, None, &mut self.events);
                        return Err(io::ErrorKind::InvalidData.into());
                    }
                };
                // Remove from pending connections if this was an outgoing connection
                // For incoming connections (from Accept), this will be false and that's ok
                self.pending_connections
                    .remove(&<std::net::SocketAddr as Into<socket2::SockAddr>>::into(
                        addr,
                    ));

                let conn_id = self.connections.insert_with_key(|conn_id| {
                    PeerConnection::new(socket, addr, conn_id, parsed_handshake)
                });
                log::info!("[{addr}] Finished handshake! [{conn_id:?}]");

                #[cfg(feature = "metrics")]
                {
                    let handshake_success_counter = metrics::counter!("peer_handshake_success");
                    handshake_success_counter.increment(1);
                }

                // We are now connected!
                // The event is replaced (this removes the dummy)
                let old = self.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));
                let recv_multi_id = self.events.insert(EventData {
                    typ: EventType::ConnectedRecv {
                        connection_idx: conn_id,
                    },
                    buffers: None,
                });

                // The initial Recv might have contained more data
                // than just the handshake so need to handle that here
                // since the read_buffer will be overwritten by the next
                // incoming recv cqe
                let connection = &mut self.connections[conn_id];
                connection.stateful_decoder.append_data(remainder);
                conn_parse_and_handle_msgs(
                    connection,
                    state,
                    &mut self.queued_disk_operations,
                    scope,
                );
                if connection.extended_extension {
                    connection
                        .outgoing_msgs_buffer
                        .push(extension_handshake_msg(state, state.config));
                }
                // Recv has been complete, move over to multishot, same user data
                io_utils::recv_multishot(sq, recv_multi_id, fd, self.read_ring.bgid());

                // TODO: only if fast ext is enabled
                let bitfield_msg = if let Some(torrent_state) = state.state() {
                    let completed = torrent_state.piece_selector.downloaded_clone();
                    // sent as first message after handshake
                    if completed.all() {
                        peer_protocol::PeerMessage::HaveAll
                    } else if completed.not_any() {
                        peer_protocol::PeerMessage::HaveNone
                    } else {
                        peer_protocol::PeerMessage::Bitfield(completed.into())
                    }
                } else {
                    peer_protocol::PeerMessage::HaveNone
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
                    #[cfg(feature = "metrics")]
                    {
                        let counter = metrics::counter!("graceful_disconnect");
                        counter.increment(1);
                    }
                    self.events.remove(io_event.event_data_idx);
                    connection.disconnect(sq, &mut self.events, state);
                    return Ok(());
                }
                let connection = &mut self.connections[connection_idx];
                if !io_event.is_more
                    && let ConnectionState::Connected(socket) = &connection.connection_state
                {
                    let fd = socket.as_raw_fd();
                    // restart the operation
                    io_utils::recv_multishot(
                        sq,
                        io_event.event_data_idx,
                        fd,
                        self.read_ring.bgid(),
                    );
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
                    state,
                    &mut self.queued_disk_operations,
                    scope,
                );
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
    pending_disk_operations: &mut Vec<DiskOp>,
    scope: &Scope<'scope>,
) {
    while let Some(parse_result) = connection.stateful_decoder.next() {
        match parse_result {
            Ok(peer_message) => {
                connection.handle_message(peer_message, state, pending_disk_operations, scope);
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
    peer_metrics: Vec<PeerMetrics>,
    _pending_connections: &HashSet<SockAddr>,
    _num_connections: usize,
    event_tx: &mut Producer<TorrentEvent>,
) {
    let mut pieces_completed = 0;
    let mut pieces_allocated = 0;

    if let Some(torrent_state) = state.state() {
        let total_completed = torrent_state.piece_selector.total_completed();
        let total_allocated = torrent_state.piece_selector.total_allocated();
        pieces_completed = total_completed;
        pieces_allocated = total_allocated;
        #[cfg(feature = "metrics")]
        {
            let counter = metrics::counter!("pieces_completed");
            counter.absolute(total_completed as u64);
            let gauge = metrics::gauge!("pieces_allocated");
            gauge.set(total_allocated as u32);
            let gauge = metrics::gauge!("num_unchoked");
            gauge.set(torrent_state.num_unchoked);
        }
    }
    #[cfg(feature = "metrics")]
    {
        let gauge = metrics::gauge!("num_connections");
        gauge.set(_num_connections as u32);
        let gauge = metrics::gauge!("num_pending_connections");
        gauge.set(_pending_connections.len() as u32);
    }
    if event_tx
        .enqueue(TorrentEvent::TorrentMetrics {
            pieces_completed,
            pieces_allocated,
            peer_metrics,
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
    event_tx: &mut Producer<TorrentEvent>,
) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
    if let Some(torrent_state) = torrent_state.state() {
        torrent_state.ticks_to_recalc_unchoke =
            torrent_state.ticks_to_recalc_unchoke.saturating_sub(1);
        torrent_state.ticks_to_recalc_optimistic_unchoke = torrent_state
            .ticks_to_recalc_optimistic_unchoke
            .saturating_sub(1);

        if torrent_state.ticks_to_recalc_unchoke == 0 && !connections.is_empty() {
            torrent_state.ticks_to_recalc_unchoke =
                torrent_state.config.num_ticks_before_unchoke_recalc;
            torrent_state.recalculate_unchokes(connections);
        }

        if torrent_state.ticks_to_recalc_optimistic_unchoke == 0 && !connections.is_empty() {
            torrent_state.ticks_to_recalc_optimistic_unchoke = torrent_state
                .config
                .num_ticks_before_optimistic_unchoke_recalc;
            torrent_state.recalculate_optimistic_unchokes(connections);
        }
    }

    for connection in connections
        .values_mut()
        // Filter out connections that are pending diconnect
        .filter(|conn| conn.pending_disconnect.is_none())
    {
        if connection.last_seen.elapsed() > Duration::from_secs(120) {
            log::warn!("Timeout due to inactivity: {}", connection.peer_id);
            // TODO: This will not release it's unchoke slot until next interval
            connection.pending_disconnect = Some(DisconnectReason::Idle);
            continue;
        }
        if connection.last_keepalive_sent.elapsed() > Duration::from_secs(100) {
            connection.keep_alive();
        }
        // Take delta into account when calculating throughput
        connection.network_stats.download_throughput = (connection.network_stats.download_throughput
            as f64
            / tick_delta.as_secs_f64())
        .round() as u64;
        connection.network_stats.upload_throughput =
            (connection.network_stats.upload_throughput as f64 / tick_delta.as_secs_f64()).round()
                as u64;

        if let Some(torrent_state) = torrent_state.state() {
            // TODO: If we are not using fast extension this might be triggered by a snub
            if let Some(time) = connection.last_received_subpiece {
                if time.elapsed() > connection.request_timeout() {
                    // warn just to make more visible
                    log::warn!("TIMEOUT: {}", connection.peer_id);
                    connection.on_request_timeout(torrent_state);
                } else if connection.snubbed {
                    // Did not timeout
                    connection.snubbed = false;
                }
            }
            if !connection.peer_choking {
                // slow start win size increase is handled in update_stats
                if !connection.slow_start {
                    // mimics libtorrent impl
                    let new_queue_capacity = 3 * connection.network_stats.download_throughput
                        / piece_selector::SUBPIECE_SIZE as u64;
                    connection.update_target_inflight(new_queue_capacity as usize);
                }
            }

            if !connection.peer_choking
                && connection.slow_start
                && connection.network_stats.download_throughput > 0
                && connection.network_stats.download_throughput
                    < connection.network_stats.prev_download_throughput + 5000
            {
                log::debug!("[Peer {}] Exiting slow start", connection.peer_id);
                connection.slow_start = false;
            }
        }
        connection.network_stats.prev_download_throughput =
            connection.network_stats.download_throughput;
        connection.network_stats.prev_upload_throughput =
            connection.network_stats.upload_throughput;
        connection.network_stats.download_throughput = 0;
        connection.network_stats.upload_throughput = 0;
    }
    let mut peer_metrics = Vec::with_capacity(connections.len());
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
        if let Some(torrent_state) = torrent_state.state() {
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
                    let mut queue = torrent_state.allocate_piece(next_piece, peer.conn_id);
                    let queue_len = queue.len();
                    peer.append_and_fill(&mut queue);
                    // Remove all subpieces from available bandwidth
                    bandwidth -= (queue_len).min(bandwidth);
                } else {
                    break;
                }
            }
            peer.fill_request_queue();
        }
        let metrics = peer.report_metrics();
        peer_metrics.push(metrics);
    }
    report_tick_metrics(
        torrent_state,
        peer_metrics,
        pending_connections,
        connections.len(),
        event_tx,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_protocol::PeerId;
    use crate::test_utils::setup_test;
    use crate::torrent::Command;
    use heapless::spsc::Queue;
    use io_uring::IoUring;
    use metrics::Key;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};
    use std::net::{SocketAddrV4, TcpListener};
    use std::time::Duration;

    #[test]
    #[cfg(feature = "metrics")]
    fn handshake_timeout() {
        env_logger::builder()
            .is_test(true)
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

        let (command_tx, command_rc) = std::sync::mpsc::sync_channel(64);
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
                    let config = Config::default();
                    let our_id = PeerId::generate();
                    let mut event_loop =
                        EventLoop::new(our_id, SlotMap::<EventId, EventData>::with_key(), &config);
                    let ring = IoUring::builder()
                        .setup_single_issuer()
                        .setup_clamp()
                        .setup_cqsize(config.cq_size)
                        .setup_defer_taskrun()
                        .setup_coop_taskrun()
                        .build(config.sq_size)
                        .unwrap();
                    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                    let result =
                        event_loop.run(ring, &mut download_state, event_tx, command_rc, listener);
                    assert!(result.is_ok());
                })
            });
            command_tx
                .send(Command::ConnectToPeers(vec![addr]))
                .unwrap();
            std::thread::sleep(Duration::from_secs(HANDSHAKE_SHOULD_TIMEOUT));
            command_tx.send(Command::Stop).unwrap();
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

    // Tests that a peer can successfully connect to our listener
    #[test]
    #[cfg(feature = "metrics")]
    fn peer_can_connect_to_listener() {
        env_logger::builder()
            .is_test(true)
            .filter_level(log::LevelFilter::Trace)
            .init();

        let debbuging = DebuggingRecorder::new();
        let snapshotter = debbuging.snapshotter();

        let (command_tx, command_rc) = std::sync::mpsc::sync_channel(64);
        let mut event_q = Queue::<TorrentEvent, 512>::new();
        let (event_tx, mut event_rx) = event_q.split();

        let (info_hash_tx, info_hash_rx) = std::sync::mpsc::channel();
        let our_id = PeerId::generate();

        std::thread::scope(|s| {
            let event_loop_thread = s.spawn(move || {
                let mut download_state = setup_test();
                let info_hash = download_state.info_hash;
                info_hash_tx.send(info_hash).unwrap();

                metrics::with_local_recorder(&debbuging, || {
                    let config = Config::default();
                    let mut event_loop =
                        EventLoop::new(our_id, SlotMap::<EventId, EventData>::with_key(), &config);
                    let ring = IoUring::builder()
                        .setup_single_issuer()
                        .setup_clamp()
                        .setup_cqsize(config.cq_size)
                        .setup_defer_taskrun()
                        .setup_coop_taskrun()
                        .build(config.sq_size)
                        .unwrap();

                    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                    let result =
                        event_loop.run(ring, &mut download_state, event_tx, command_rc, listener);
                    assert!(result.is_ok());
                })
            });

            // Get the info hash first
            let info_hash = info_hash_rx.recv().unwrap();

            // Wait for the ListenerStarted event to get the port
            let listener_port = loop {
                if let Some(event) = event_rx.dequeue() {
                    match event {
                        TorrentEvent::ListenerStarted { port } => {
                            break port;
                        }
                        _ => continue,
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            };

            // Spawn a thread to connect as a peer and perform handshake
            let simulated_peer_thread = std::thread::spawn(move || {
                use std::io::{Read, Write};
                use std::net::TcpStream;

                // Connect to the listener
                let mut stream =
                    TcpStream::connect(format!("127.0.0.1:{}", listener_port)).unwrap();

                // Send a valid handshake
                let mut handshake = Vec::with_capacity(HANDSHAKE_SIZE);
                let peer_id = PeerId::generate();
                write_handshake(peer_id, info_hash, &mut handshake);
                stream.write_all(&handshake).unwrap();

                // Read the handshake response
                let mut response = vec![0u8; HANDSHAKE_SIZE];
                stream.read_exact(&mut response).unwrap();

                // Verify we got a valid handshake back
                assert_eq!(response.len(), HANDSHAKE_SIZE);
                let handshake = parse_handshake(info_hash, &response).unwrap();
                assert!(handshake.fast_ext);
                assert!(handshake.extension_protocol);
                assert_eq!(handshake.peer_id, our_id);

                stream.shutdown(std::net::Shutdown::Write).unwrap();
                // Keep connection alive for a moment to allow processing
                std::thread::sleep(Duration::from_secs(1));
            });

            // Give some time for the handshake to complete
            std::thread::sleep(Duration::from_secs(1));
            command_tx.send(Command::Stop).unwrap();
            simulated_peer_thread.join().unwrap();
            event_loop_thread.join().unwrap();

            let snapshot = snapshotter.snapshot();
            #[allow(clippy::mutable_key_type)]
            let metrics = snapshot.into_hashmap();

            // Verify successful handshake metrics
            let val = metrics.get(&CompositeKey::new(
                MetricKind::Counter,
                Key::from_name("peer_handshake_success"),
            ));
            if let Some((_, _, DebugValue::Counter(num_success))) = val {
                assert_eq!(*num_success, 1);
            } else {
                panic!("Expected peer_handshake_success metric to be recorded");
            }
        });
    }

    // // Tests that the handshake is valid and that we send a proper bitfield afterwards
    // #[test]
    // fn valid_handshake() {
    //     todo!()
    // }
}
