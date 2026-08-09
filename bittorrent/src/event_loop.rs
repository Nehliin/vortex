use std::{
    io,
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd},
    rc::Rc,
    sync::mpsc::Receiver,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes};
use heapless::spsc::Producer;
use io_uring::{
    IoUring,
    cqueue::Entry,
    opcode,
    types::{self, CancelBuilder, Fd, Timespec},
};
use libc::{ECANCELED, ENOENT};
use rayon::Scope;
use slotmap::{Key, KeyData, new_key_type};
use socket2::{SockAddr, Socket};

use crate::{
    buf_pool::Buffer,
    buf_ring::Bid,
    connection_manager::{ConnectionId, ConnectionManager},
    file_store::DiskOp,
    io::{BackloggedSubmissionQueue, Io, SubmissionQueue},
    peer_comm::peer_connection::{DisconnectReason, PeerConnection},
    piece_selector::{self, SUBPIECE_SIZE},
    torrent::{CQE_WAIT_TIME_NS, Command, Error, PeerMetrics, State, StateRef, TorrentEvent},
};

pub(crate) const HANDSHAKE_TIMEOUT: Timespec = Timespec::new().sec(7);

// A CQE is "orphan" when its event id has already been removed from `events`.
// Expected causes:
//   * the linked-timeout half of a Recv/Connect + LinkTimeout pair (both share
//     user_data), so the second CQE lands after the first removed the event;
//   * a trailing CQE from a cancelled multishot recv. (ECANCELED)
//
// The linked-timeout half normally completes with ECANCELED but
// may return  ENOENT in the race where the timer fires *and* the target completes
// at nearly the same instant.
fn is_expected_orphan_error(err: u32) -> bool {
    matches!(err as i32, ECANCELED | ENOENT)
}

#[derive(Debug)]
pub enum EventType {
    Accept,
    Connect {
        connection_idx: ConnectionId,
        // The SQE needs a stable pointer to the addrs until submission
        // hence the Box
        addr: Box<SockAddr>,
    },
    Write {
        connection_idx: ConnectionId,
        expected_write: usize,
    },
    Recv {
        connection_idx: ConnectionId,
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

#[derive(Debug)]
pub struct EventData {
    pub typ: EventType,
    pub buffers: Option<Vec<Buffer>>,
}

#[derive(Debug, Clone, Copy)]
enum EventLoopState {
    ShuttingDown {
        // Fd for the TcpListener provided to the event loop
        listener_fd: Option<Fd>,
    },
    Pausing {
        // Fd for the TcpListener provided to the event loop
        listener_fd: Fd,
    },
    Paused {
        // Fd for the TcpListener provided to the event loop
        listener_fd: Option<Fd>,
    },
    Running {
        // Fd for the TcpListener provided to the event loop
        listener_fd: Fd,
        // Associated user data to the AcceptMulti SQE
        listener_user_data: u64,
    },
}

fn event_error_handler<'state, Q: SubmissionQueue>(
    io: &mut Io<Q>,
    error_code: u32,
    event_data_idx: EventId,
    connection_manager: &mut ConnectionManager,
    state_ref: &mut StateRef<'state>,
) -> io::Result<()> {
    match error_code as i32 {
        libc::ENOBUFS => {
            // TODO: statistics
            log::warn!("Ran out of buffers!, resubmitting recv op");
            // Ran out of buffers! Resolve (fd, is_multishot) first since
            // rearming the recv requires exclusive access to the io struct
            let rearm = match &io.events[event_data_idx].typ {
                EventType::Recv { connection_idx } => {
                    connection_manager.fd(*connection_idx).map(|fd| (fd, false))
                }
                EventType::ConnectedRecv { connection_idx } => {
                    connection_manager.fd(*connection_idx).map(|fd| (fd, true))
                }
                _ => unreachable!(),
            };
            match rearm {
                Some((fd, false)) => io.recv(event_data_idx, fd, &HANDSHAKE_TIMEOUT),
                Some((fd, true)) => io.recv_multishot(event_data_idx, fd),
                None => {
                    // Prevent "leaking" the event since it won't be re-armed and won't be completed
                    // unclear if we ever end up with a ENOBUFS on a closing connection but I guess
                    // it might be possible
                    io.events.remove(event_data_idx).unwrap();
                }
            }
            Ok(())
        }
        libc::ETIME => {
            let event = io.events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Connect { connection_idx, .. } => {
                    // The connection may already be closing if it was disconnected
                    // (by a pause or shutdown) after the timer fired but before this
                    // completion was handled
                    if let Some(addr) = connection_manager.disconnect(connection_idx, io, state_ref)
                    {
                        log::debug!("[{addr}] Connect timed out!");
                        #[cfg(feature = "metrics")]
                        {
                            let connect_fail_counter = metrics::counter!("peer_connect_timeout");
                            connect_fail_counter.increment(1);
                        }
                    }
                }
                EventType::Recv { connection_idx } => {
                    if let Some(addr) = connection_manager.disconnect(connection_idx, io, state_ref)
                    {
                        log::debug!("[{addr}] Handshake timed out!");
                        #[cfg(feature = "metrics")]
                        {
                            let handshake_timeout_counter =
                                metrics::counter!("peer_handshake_timeout");
                            handshake_timeout_counter.increment(1);
                        }
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::ECONNRESET => {
            let event = io.events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Connect { connection_idx, .. }
                | EventType::Write { connection_idx, .. }
                | EventType::Recv { connection_idx }
                | EventType::ConnectedRecv { connection_idx }
                | EventType::ConnectedWriteV { connection_idx, .. } => {
                    // Don't worry about this if the connection is already closing
                    if let Some(addr) = connection_manager.disconnect(connection_idx, io, state_ref)
                    {
                        log::error!("[{addr}] Connection reset");
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::EPIPE => {
            let event = io.events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Write { connection_idx, .. }
                | EventType::ConnectedWriteV { connection_idx, .. } => {
                    if let Some(addr) = connection_manager.disconnect(connection_idx, io, state_ref)
                    {
                        log::error!("[{addr}] EPIPE received when writing to connection");
                    } else {
                        // I guess this might happpen when multiple writes are queued up after
                        // each other
                        log::error!("EPIPE received after connection has already been closed");
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        libc::ECONNREFUSED | libc::EHOSTUNREACH => {
            // Failling to connect due to this is not really an error due to
            // the likelyhood of being stale info in the DHT
            let event = io.events.remove(event_data_idx).unwrap();
            match event.typ {
                EventType::Connect { connection_idx, .. } => {
                    if let Some(addr) = connection_manager.disconnect(connection_idx, io, state_ref)
                    {
                        log::debug!("[{addr}] Connection failed {event_data_idx:?}");
                    }
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
        err_code => {
            let err = std::io::Error::from_raw_os_error(err_code);
            if let Some(event) = io.events.remove(event_data_idx) {
                let err_str = format!("Unhandled error code: {err}, event type: {event:?}");
                match event.typ {
                    EventType::Connect { connection_idx, .. }
                    | EventType::Write { connection_idx, .. }
                    | EventType::Recv { connection_idx }
                    | EventType::ConnectedWriteV { connection_idx, .. }
                    | EventType::ConnectedRecv { connection_idx } => {
                        if let Some(addr) =
                            connection_manager.disconnect(connection_idx, io, state_ref)
                        {
                            log::error!("[{addr}] {err_str}");
                        } else {
                            log::error!("{err_str}");
                        }
                    }
                    EventType::Close {
                        maybe_connection_idx,
                    } => {
                        log::error!("{err_str}, attempting to close: {maybe_connection_idx:?}");
                        // The fd is gone regardless of the close failing, so the slot
                        // is freed here as well. Otherwise the connection would linger
                        // forever, blocking both reconnects and shutdown.
                        if let Some(connection_idx) = maybe_connection_idx {
                            connection_manager.remove_closed(connection_idx);
                        }
                        return Err(err);
                    }
                    EventType::Cancel => {
                        // This might happen for rejected incoming connections
                        // for example. io.close_socket will Cancel + Close and if
                        // nothing has started the Cancel will return ENOENT
                        if err_code != libc::ENOENT {
                            log::error!("{err_str}");
                            return Err(err);
                        }
                        return Ok(());
                    }
                    EventType::Accept | EventType::Dummy => {
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
                        io.inflight_disk_ops -= 1;
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
    read_bid: Option<Bid>,
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
    state: EventLoopState,
}

impl<'scope, 'state: 'scope> EventLoop {
    pub fn new() -> Self {
        Self {
            state: EventLoopState::Paused { listener_fd: None },
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
        let port = listener.local_addr().unwrap().port();
        state.listener_port = Some(port);

        let mut connection_manager =
            ConnectionManager::new(state.our_id(), state.config.max_connections);
        let mut state_ref = state.as_ref();
        let mut prev_state_initialized = state_ref.is_initialzied();
        // lambda to be able to catch errors an always unregistering the read ring
        rayon::in_place_scope(|scope| {
            let (submitter, sq, mut cq) = ring.split();
            let mut io = Io::new(BackloggedSubmissionQueue::new(sq), state_ref.config);
            io.read_ring.register(&submitter)?;
            let mut last_tick = Instant::now();

            self.setup_and_mark_running(
                types::Fd(listener.into_raw_fd()),
                port,
                &mut io,
                &mut event_tx,
            );

            let result = loop {
                // Handle commands first of all so we can block the event loop when in a paused
                // state. The "pause_ready" check should ensure all meaningful CQE:s have been
                // handled before we block the loop.
                self.handle_commands(
                    &mut io,
                    &mut command_rc,
                    &mut connection_manager,
                    &mut state_ref,
                    &mut event_tx,
                );
                // All connections, including the ones that never established, are
                // torn down before pausing or shutting down. Waiting for the map to
                // empty means waiting for every socket to actually have been closed.
                let pause_ready = connection_manager.is_empty() && io.inflight_disk_ops == 0;
                match self.state {
                    EventLoopState::ShuttingDown { listener_fd } if pause_ready => {
                        if let Some(listener_fd) = listener_fd {
                            // Blocking close here since we are shutting down regardless
                            let ret = unsafe { libc::close(listener_fd.0) };
                            if ret != 0 {
                                log::error!("Failed closing listener errno: {}", unsafe {
                                    libc::__errno_location().read()
                                })
                            }
                        };
                        log::info!("All connections closed, shutdown complete");
                        break Ok(());
                    }
                    EventLoopState::Pausing { listener_fd } if pause_ready => {
                        if event_tx.enqueue(TorrentEvent::Paused).is_err() {
                            log::error!("Failed to enqueue Paused event");
                        }
                        self.state = EventLoopState::Paused {
                            listener_fd: Some(listener_fd),
                        };
                    }
                    _ => {}
                }

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
                        break Err(Error::Io(err));
                    }
                }
                cq.sync();
                if cq.overflow() > 0 {
                    log::error!("CQ overflow");
                }

                if let Err(err) = io.sq.submit_and_drain_backlog(&submitter) {
                    break Err(Error::Io(err));
                }

                #[cfg(feature = "metrics")]
                {
                    let gauge = metrics::gauge!("write_pool_free_buffers");
                    gauge.set(io.write_pool.free_buffers() as u32);
                    let gauge = metrics::gauge!("write_pool_allocated_buffers");
                    gauge.set(io.write_pool.total_buffers() as u32);
                }

                let tick_delta = last_tick.elapsed();
                if tick_delta > Duration::from_secs(1) {
                    tick(
                        &tick_delta,
                        &mut connection_manager,
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
                        for (_, connection) in connection_manager.iter_established_mut() {
                            let msgs = std::mem::take(&mut connection.pre_meta_have_msgs);
                            // Get all piece msgs
                            for msg in msgs {
                                connection.handle_message(
                                    msg,
                                    &mut state_ref,
                                    &mut io.queued_disk_operations,
                                    scope,
                                );
                            }
                        }
                    }

                    last_tick = Instant::now();
                    // Dealt with here to make tick easier to test
                    connection_manager.execute_pending_disconnects(&mut io, &mut state_ref);
                    io.sq.sync();
                }

                for cqe in &mut cq {
                    let io_event = RawIoEvent::from(cqe);
                    if let Some(event) = io.events.get_mut(io_event.event_data_idx) {
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
                            &mut io,
                            io_event,
                            &mut maybe_buffers,
                            &mut connection_manager,
                            &mut state_ref,
                            &mut event_tx,
                            scope,
                        ) {
                            log::error!("Error handling event: {err}");
                        }
                        // Now it's time to return any potential write buffers
                        if let Some(buffers) = maybe_buffers {
                            for buffer in buffers {
                                io.write_pool.return_buffer(buffer);
                            }
                        }
                    } else {
                        let err = io_event.result.unwrap_err();
                        // The event was already removed by an earlier CQE in this batch.
                        assert!(
                            is_expected_orphan_error(err),
                            "unexpected orphan CQE errno {err} for event id {:?}",
                            io_event.event_data_idx
                        );
                    }
                    // Ensure bids are always returned
                    if let Some(bid) = io_event.read_bid {
                        io.read_ring.return_bid(bid);
                    }
                }

                if let Some(torrent_state) = state_ref.state() {
                    torrent_state
                        .queue_disk_write_for_downloaded_pieces(&mut io.queued_disk_operations);
                }
                io.submit_queued_disk_operations();

                for (conn_id, connection) in connection_manager
                    .iter_established_mut()
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
                    let mut buffers = Vec::new();
                    let mut current_buffer = io.write_pool.get_buffer();
                    let conn_fd = connection.socket.as_raw_fd();
                    for message in connection.outgoing_msgs_buffer.iter() {
                        let size = message.encoded_size();
                        if current_buffer.remaining_mut() >= size {
                            message.encode(&mut current_buffer);
                        } else {
                            // Buffer is full, get a new one
                            buffers.push(current_buffer);
                            current_buffer = io.write_pool.get_buffer();
                            message.encode(&mut current_buffer);
                        }
                    }
                    buffers.push(current_buffer);
                    connection.network_write_inflight = true;
                    io.writev_to_connection(conn_id, conn_fd, buffers, 0);
                    connection.outgoing_msgs_buffer.clear();
                }
                io.sq.sync();
            };
            io.read_ring.unregister(&submitter)?;
            result
        })
    }

    fn setup_and_mark_running<Q: SubmissionQueue>(
        &mut self,
        listener_fd: Fd,
        port: u16,
        io: &mut Io<Q>,
        event_tx: &mut Producer<TorrentEvent>,
    ) {
        let event_idx: EventId = io.events.insert(EventData {
            typ: EventType::Accept,
            buffers: None,
        });
        let listener_user_data = event_idx.data().as_ffi();
        let accept_op = opcode::AcceptMulti::new(listener_fd)
            .build()
            .user_data(listener_user_data);
        io.sq.push(accept_op);
        io.sq.sync();
        self.state = EventLoopState::Running {
            listener_fd,
            listener_user_data,
        };
        // Emit running event
        if event_tx.enqueue(TorrentEvent::Running { port }).is_err() {
            log::error!("Failed to enqueue Running event");
        }
    }

    fn handle_commands<Q: SubmissionQueue>(
        &mut self,
        io: &mut Io<Q>,
        command_rc: &mut Receiver<Command>,
        connection_manager: &mut ConnectionManager,
        state_ref: &mut StateRef<'state>,
        event_tx: &mut Producer<TorrentEvent>,
    ) {
        // Block on new commands if we are paused, otherwise do a nonblocking iter
        let command_iter: &mut dyn Iterator<Item = Command> = match self.state {
            EventLoopState::Paused { .. } => &mut command_rc.iter(),
            _ => &mut command_rc.try_iter(),
        };
        for command in command_iter {
            match command {
                Command::ConnectToPeers(addrs) => {
                    // Don't connect to new peers if we are shutting down
                    // or pausing
                    if !matches!(self.state, EventLoopState::Running { .. }) {
                        continue;
                    }
                    for addr in addrs.into_iter().map(|addr| addr.into()) {
                        // De-duplication and the max-connection cap are enforced
                        // by the connection manager across incoming and outgoing.
                        connection_manager.maybe_connect_to_peer(addr, io);
                    }
                }
                Command::Pause => {
                    if let EventLoopState::Running {
                        listener_fd,
                        listener_user_data,
                    } = self.state
                    {
                        log::info!("Pause requested, closing all connections");
                        self.state = EventLoopState::Pausing { listener_fd };
                        // Cancel the listener
                        io.cancel(CancelBuilder::user_data(listener_user_data).all(), None);
                        assert!(
                            io.events
                                .remove(EventId::from(KeyData::from_ffi(listener_user_data)))
                                .is_some(),
                            "Listener AcceptMulti removed more than once"
                        );
                        connection_manager.disconnect_all(io, state_ref);
                    } else {
                        log::warn!("Received Pause command when in a non running state. Ignoring");
                    }
                }
                Command::Resume => {
                    if let EventLoopState::Paused { listener_fd } = self.state {
                        let port = state_ref
                            .listener_port
                            .expect("Resume must be called after having been explicitly paused");
                        let listener_fd = listener_fd
                            .expect("Resume must be called after having been explicitly paused");
                        self.setup_and_mark_running(listener_fd, port, io, event_tx);
                        // Break out of the command loop since we were iterating with
                        // the blocking iter. We need to return to the main event loop
                        // so CQEs can be processed.
                        break;
                    } else {
                        log::error!(
                            "Resume requested when in a non paused state. Current state: {:?}",
                            self.state
                        );
                    }
                }
                Command::Stop => {
                    if !matches!(self.state, EventLoopState::ShuttingDown { .. }) {
                        log::info!("Shutdown requested, closing all connections");
                        let listener_fd = match self.state {
                            EventLoopState::ShuttingDown { listener_fd }
                            | EventLoopState::Paused { listener_fd } => listener_fd,
                            EventLoopState::Pausing { listener_fd }
                            | EventLoopState::Running { listener_fd, .. } => Some(listener_fd),
                        };
                        self.state = EventLoopState::ShuttingDown { listener_fd };
                        connection_manager.disconnect_all(io, state_ref);
                    }
                }
            }
        }
        io.sq.sync();
    }

    // Each parameter is a disjointly-borrowed piece of the event loop's
    // working set (io, connections, torrent state)
    #[allow(clippy::too_many_arguments)]
    fn event_handler<Q: SubmissionQueue>(
        &mut self,
        io: &mut Io<Q>,
        io_event: RawIoEvent,
        write_buffers: &mut Option<Vec<Buffer>>,
        connection_manager: &mut ConnectionManager,
        state: &mut StateRef<'state>,
        event_tx: &mut Producer<TorrentEvent>,
        scope: &Scope<'scope>,
    ) -> io::Result<()> {
        let ret = match io_event.result {
            Ok(ret) => ret,
            Err(error_code) => {
                return event_error_handler(
                    io,
                    error_code,
                    io_event.event_data_idx,
                    connection_manager,
                    state,
                );
            }
        };
        let mut event = EventType::Dummy;
        std::mem::swap(&mut event, &mut io.events[io_event.event_data_idx].typ);
        match event {
            EventType::Accept => {
                // The event is reused and not replaced
                std::mem::swap(&mut event, &mut io.events[io_event.event_data_idx].typ);
                let fd = ret;
                let socket = unsafe { Socket::from_raw_fd(fd) };
                // There is a race here where new connections may show up
                // after we've paused or shut down but before the AcceptMulti
                // operation has been fully cancelled
                if !matches!(self.state, EventLoopState::Running { .. }) {
                    log::warn!("Received incoming connection without being in the running state");
                    io.close_socket(socket, None);
                    return Ok(());
                }
                let info_hash = *state.info_hash();
                connection_manager.on_accepted(socket, info_hash, io)?;
            }
            EventType::Connect { connection_idx, .. } => {
                // Event removal stays in the event loop.
                let old = io.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));
                let info_hash = *state.info_hash();
                connection_manager.on_connect(connection_idx, info_hash, io);
            }
            EventType::Write {
                connection_idx,
                expected_write,
            } => {
                let old = io.events.remove(io_event.event_data_idx).unwrap();
                debug_assert!(matches!(old.typ, EventType::Dummy));
                connection_manager.on_write(
                    connection_idx,
                    ret as usize,
                    expected_write,
                    io,
                    state,
                );
            }
            EventType::DiskWrite {
                data,
                piece_idx,
                #[cfg(feature = "metrics")]
                scheduled,
            } => {
                io.events.remove(io_event.event_data_idx);
                let torrent_state = state
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
                    torrent_state.complete_piece(piece_idx, connection_manager, event_tx, buffer);
                }
                io.inflight_disk_ops -= 1;
            }
            EventType::DiskRead {
                data,
                piece_idx,
                connection_idx,
                piece_offset,
                #[cfg(feature = "metrics")]
                scheduled,
            } => {
                io.events.remove(io_event.event_data_idx);
                let torrent_state = state
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
                    if let Some(connection) = connection_manager.established_mut(connection_idx) {
                        let start_idx = piece_offset as usize;
                        let piece_len = torrent_state.piece_selector.piece_len(piece_idx) as usize;
                        let end_idx = (start_idx + SUBPIECE_SIZE as usize).min(piece_len);
                        connection.send_piece(
                            piece_idx,
                            piece_offset,
                            // TODO: avoid this copy by caching the piece buffer and make the Piece message
                            // take an enum of either Buffer or Bytes?
                            Bytes::copy_from_slice(&buffer.raw_slice()[start_idx..end_idx]),
                        );
                    }
                    torrent_state.piece_buffer_pool.return_buffer(buffer);
                }
                io.inflight_disk_ops -= 1;
            }
            EventType::Cancel => {
                log::trace!("Cancel event completed");
                io.events.remove(io_event.event_data_idx);
            }
            EventType::ConnectedWriteV {
                connection_idx,
                iovecs,
                io_vec_offset,
            } => {
                // TODO: add to metrics for writes?
                io.events.remove(io_event.event_data_idx);
                let expected_written = iovecs.iter().map(|io| io.iov_len).sum();
                let bytes_written = ret as usize;
                let Some(connection) = connection_manager.established_mut(connection_idx) else {
                    log::warn!("Connection was lost after write was handled");
                    return Ok(());
                };
                connection.on_network_write(bytes_written);
                if bytes_written < expected_written {
                    log::warn!(
                        "[PeerId: {}] Partial write {bytes_written}, expected {expected_written}, TCP send buffer is most likely full",
                        connection.peer_id,
                    );
                    let buffer = write_buffers.take().unwrap();
                    // Reschedule a write for the remaining data using cumulative offset
                    let new_offset = io_vec_offset + bytes_written;
                    io.writev_to_connection(
                        connection.conn_id,
                        connection.socket.as_raw_fd(),
                        buffer,
                        new_offset,
                    );
                } else {
                    connection.network_write_inflight = false;
                }
            }
            EventType::Recv { connection_idx } => {
                let len = ret as usize;
                io.events.remove(io_event.event_data_idx);
                connection_manager.on_read(
                    connection_idx,
                    io_event.read_bid,
                    len,
                    io,
                    state,
                    scope,
                )?;
            }
            EventType::ConnectedRecv { connection_idx } => {
                // The event is reused and not replaced
                std::mem::swap(&mut event, &mut io.events[io_event.event_data_idx].typ);
                let len = ret as usize;
                if len == 0 {
                    io.events.remove(io_event.event_data_idx);
                    if let Some(addr) = connection_manager.disconnect(connection_idx, io, state) {
                        log::debug!("[{addr}] No more data");
                        #[cfg(feature = "metrics")]
                        {
                            let counter = metrics::counter!("graceful_disconnect");
                            counter.increment(1);
                        }
                    }
                    return Ok(());
                }
                // The connection may have been disconnected by an earlier completion
                // in this same batch. The event is deliberately left in place, the
                // multishot recv is still armed until the cancel lands and removing
                // it would turn its remaining completions into unexpected orphans.
                let Some(connection) = connection_manager.established_mut(connection_idx) else {
                    log::debug!("Data received for a disconnected connection");
                    return Ok(());
                };
                if !io_event.is_more {
                    // restart the operation
                    let fd = connection.socket.as_raw_fd();
                    io.recv_multishot(io_event.event_data_idx, fd);
                }

                // We always have a buffer associated
                let buffer = io_event.read_bid.map(|bid| io.read_ring.get(bid)).unwrap();
                let buffer = &buffer[..len];
                connection.stateful_decoder.append_data(buffer);
                conn_parse_and_handle_msgs(
                    connection,
                    state,
                    &mut io.queued_disk_operations,
                    scope,
                );
            }
            EventType::Close {
                maybe_connection_idx,
            } => {
                if let Some(connection_idx) = maybe_connection_idx {
                    connection_manager.remove_closed(connection_idx);
                }
                io.events.remove(io_event.event_data_idx);
            }
            EventType::Dummy => unreachable!(),
        }
        Ok(())
    }
}

pub(crate) fn conn_parse_and_handle_msgs<'scope, 'f_store: 'scope>(
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
    _connection_manager: &ConnectionManager,
    event_tx: &mut Producer<TorrentEvent>,
) {
    let mut pieces_allocated = 0;
    let mut progress = None;

    if let Some(torrent_state) = state.state() {
        pieces_allocated = torrent_state.piece_selector.total_allocated();
        progress = Some(torrent_state.piece_selector.progress());
        #[cfg(feature = "metrics")]
        {
            let counter = metrics::counter!("pieces_completed");
            counter.absolute(torrent_state.piece_selector.total_completed() as u64);
            let gauge = metrics::gauge!("pieces_allocated");
            gauge.set(pieces_allocated as u32);
            let gauge = metrics::gauge!("num_unchoked");
            gauge.set(torrent_state.num_unchoked);
        }
    }
    #[cfg(feature = "metrics")]
    {
        let gauge = metrics::gauge!("num_connections");
        gauge.set(_connection_manager.num_established() as u32);
        let gauge = metrics::gauge!("num_pending_connections");
        gauge.set(_connection_manager.num_pending() as u32);
    }
    if event_tx
        .enqueue(TorrentEvent::TorrentMetrics {
            pieces_allocated,
            peer_metrics,
            progress,
        })
        .is_err()
    {
        log::error!("Torrent metrics event missed")
    }
}

pub(crate) fn tick<'scope, 'state: 'scope>(
    tick_delta: &Duration,
    connections: &mut ConnectionManager,
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

        if torrent_state.ticks_to_recalc_unchoke == 0 && connections.any_established() {
            torrent_state.ticks_to_recalc_unchoke =
                torrent_state.config.num_ticks_before_unchoke_recalc;
            torrent_state.recalculate_unchokes(connections);
        }

        if torrent_state.ticks_to_recalc_optimistic_unchoke == 0 && connections.any_established() {
            torrent_state.ticks_to_recalc_optimistic_unchoke = torrent_state
                .config
                .num_ticks_before_optimistic_unchoke_recalc;
            torrent_state.recalculate_optimistic_unchokes(connections);
        }
    }

    for (_, connection) in connections
        .iter_established_mut()
        // Filter out connections that are pending diconnect
        .filter(|(_, conn)| conn.pending_disconnect.is_none())
    {
        if connection.last_seen.elapsed() > Duration::from_secs(120) {
            log::warn!("Inactivity timeout: {}", connection.peer_id);
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
            if let Some(time) = connection.last_received_subpiece
                && time.elapsed() > connection.request_timeout()
                && !connection.inflight.is_empty()
            {
                // warn just to make more visible
                log::warn!("Adaptive timeout: {}", connection.peer_id);
                connection.on_request_timeout(torrent_state);
            } else if connection.last_req_resp.elapsed() > Duration::from_secs(15)
                && !connection.inflight.is_empty()
            {
                log::warn!("Stalled connection timeout: {}", connection.peer_id);
                connection.on_request_timeout(torrent_state);
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
    let mut peer_metrics = Vec::with_capacity(connections.total_connections());
    // Request new pieces and fill up request queues
    let mut peer_bandwidth: Vec<_> = connections
        .iter_established_mut()
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
    report_tick_metrics(torrent_state, peer_metrics, connections, event_tx);
}

// All tests in this module currently rely on metric snapshots for assertions
#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;
    use crate::peer_comm::peer_protocol::{HANDSHAKE_SIZE, parse_handshake, write_handshake};
    use crate::peer_protocol::PeerId;
    use crate::test_utils::setup_test;
    use crate::torrent::{Command, Config};
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
                    let mut event_loop = EventLoop::new();
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

        std::thread::scope(|s| {
            let event_loop_thread = s.spawn(move || {
                let mut download_state = setup_test();
                let info_hash = download_state.info_hash;
                let our_id = download_state.our_id();
                info_hash_tx.send((info_hash, our_id)).unwrap();

                metrics::with_local_recorder(&debbuging, || {
                    let config = Config::default();
                    let mut event_loop = EventLoop::new();
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

            // Get the info hash and peer id first
            let (info_hash, our_id) = info_hash_rx.recv().unwrap();

            // Wait for the ListenerStarted event to get the port
            let listener_port = loop {
                if let Some(event) = event_rx.dequeue() {
                    match event {
                        TorrentEvent::Running { port } => {
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
