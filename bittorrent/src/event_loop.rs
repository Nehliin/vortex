use std::{
    collections::VecDeque,
    io,
    net::SocketAddrV4,
    os::fd::{AsRawFd, FromRawFd, RawFd},
    sync::mpsc::{Receiver, TryRecvError},
    time::{Duration, Instant},
};

use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring, SubmissionQueue,
};
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    buf_pool::BufferPool,
    buf_ring::{Bgid, Bid, BufferRing},
    peer_connection::{OutgoingMsg, PeerConnection},
    peer_protocol::{self, parse_handshake, write_handshake, PeerId, HANDSHAKE_SIZE},
    piece_selector::SUBPIECE_SIZE,
    Error, TorrentState,
};

#[derive(Debug)]
pub enum Event {
    Accept,
    Connect { socket: Socket, addr: SocketAddrV4 },
    Write { socket: Socket },
    Recv { socket: Socket },
    ConnectedWrite { connection_idx: usize },
    ConnectedRecv { connection_idx: usize },
    // Dummy used to allow stable keys in the slab
    Dummy,
}

pub fn push_connected_write(
    conn_id: usize,
    fd: RawFd,
    events: &mut Slab<Event>,
    sq: &mut SubmissionQueue<'_>,
    buffer_index: usize,
    buffer: &[u8],
    ordered: bool,
    backlog: &mut VecDeque<io_uring::squeue::Entry>,
) {
    let event = events.insert(Event::ConnectedWrite {
        connection_idx: conn_id,
    });
    let user_data = UserData::new(event, Some(buffer_index));
    let flags = if ordered {
        io_uring::squeue::Flags::IO_LINK
    } else {
        io_uring::squeue::Flags::empty()
    };
    let write_op = opcode::Write::new(types::Fd(fd), buffer.as_ptr(), buffer.len() as u32)
        .build()
        .user_data(user_data.as_u64())
        .flags(flags);
    unsafe {
        if sq.push(&write_op).is_err() {
            log::warn!("SQ buffer full, pushing to backlog");
            backlog.push_back(write_op);
        }
    }
}

fn restart_multishot_recv(
    sq: &mut SubmissionQueue<'_>,
    user_data: UserData,
    fd: RawFd,
    backlog: &mut VecDeque<io_uring::squeue::Entry>,
    bgid: Bgid,
) {
    log::warn!("Starting new recv multishot");
    let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
        .build()
        .user_data(user_data.as_u64())
        .flags(io_uring::squeue::Flags::BUFFER_SELECT);
    unsafe {
        if sq.push(&read_op).is_err() {
            log::warn!("SQ buffer full, pushing to backlog");
            backlog.push_back(read_op);
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct UserData {
    buffer_idx: Option<u32>,
    event_idx: u32,
}

impl UserData {
    pub fn new(event_idx: usize, buffer_idx: Option<usize>) -> Self {
        Self {
            buffer_idx: buffer_idx.map(|idx| idx.try_into().unwrap()),
            event_idx: event_idx.try_into().unwrap(),
        }
    }

    pub fn as_u64(&self) -> u64 {
        ((self.event_idx as u64) << 32) | self.buffer_idx.unwrap_or(u32::MAX) as u64
    }

    pub fn from_u64(val: u64) -> Self {
        Self {
            event_idx: (val >> 32) as u32,
            buffer_idx: ((val as u32) != u32::MAX).then_some(val as u32),
        }
    }
}

fn event_error_handler(
    sq: &mut SubmissionQueue<'_>,
    error_code: u32,
    user_data: UserData,
    events: &mut Slab<Event>,
    connections: &mut Slab<PeerConnection>,
    bgid: Bgid,
    backlog: &mut VecDeque<io_uring::squeue::Entry>,
) -> io::Result<()> {
    match error_code as i32 {
        libc::ENOBUFS => {
            // TODO: statistics
            let event = &events[user_data.event_idx as _];
            log::warn!("Ran out of buffers!, resubmitting recv op");
            // Ran out of buffers!
            match event {
                Event::Recv { socket } => {
                    let fd = socket.as_raw_fd();
                    let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
                        .build()
                        // Reuse the user data
                        .user_data(user_data.as_u64())
                        .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                    unsafe {
                        if sq.push(&read_op).is_err() {
                            log::warn!("SQ buffer full, pushing to backlog");
                            backlog.push_back(read_op);
                        }
                    }
                    Ok(())
                }
                Event::ConnectedRecv { connection_idx } => {
                    let fd = connections[*connection_idx].socket.as_raw_fd();
                    let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
                        .build()
                        // Reuse the user data
                        .user_data(user_data.as_u64())
                        .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                    unsafe {
                        if sq.push(&read_op).is_err() {
                            log::warn!("SQ buffer full, pushing to backlog");
                            backlog.push_back(read_op);
                        }
                    }
                    Ok(())
                }
                _ => {
                    panic!("Ran out of buffers on a non recv operation: {event:?}");
                }
            }
        }
        libc::ETIME => {
            let event = events.remove(user_data.event_idx as _);
            let Event::Connect { socket, addr } = event else {
                panic!("Timed out something other than a connect: {event:?}");
            };
            log::warn!("Connect timed out!: {addr}");
            socket.shutdown(std::net::Shutdown::Both)?;
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
            let event = events.remove(user_data.event_idx as _);
            log::error!("Unhandled error event: {event:?}");
            let err = std::io::Error::from_raw_os_error(error_code as i32);
            Err(err)
        }
    }
}

const TIMESPEC: &Timespec = &Timespec::new().sec(1);

pub struct EventLoop {
    events: Slab<Event>,
    write_pool: BufferPool,
    read_ring: BufferRing,
    connections: Slab<PeerConnection>,
    peer_provider: Receiver<SocketAddrV4>,
    our_id: PeerId,
}

impl EventLoop {
    pub fn new(our_id: PeerId, events: Slab<Event>, peer_provider: Receiver<SocketAddrV4>) -> Self {
        Self {
            events,
            write_pool: BufferPool::new(128, 4096),
            read_ring: BufferRing::new(1, 128, (SUBPIECE_SIZE * 2) as _).unwrap(),
            connections: Slab::with_capacity(64),
            peer_provider,
            our_id,
        }
    }

    pub fn run(
        &mut self,
        mut ring: IoUring,
        mut torrent_state: TorrentState,
        mut tick: impl FnMut(&Duration, &mut Slab<PeerConnection>, &mut TorrentState),
    ) -> Result<(), Error> {
        self.read_ring.register(&ring.submitter())?;
        // lambda to be able to catch errors an always unregistering the read ring
        let mut actual_loop = || {
            let (submitter, mut sq, mut cq) = ring.split();
            let mut backlog: VecDeque<io_uring::squeue::Entry> = VecDeque::new();
            let mut last_tick = Instant::now();
            loop {
                let args = types::SubmitArgs::new().timespec(TIMESPEC);
                match submitter.submit_with_args(1, &args) {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {
                        log::warn!("Ring busy")
                    }
                    Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => {
                        log::debug!("Tick hit ETIME")
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

                loop {
                    if sq.is_full() {
                        match submitter.submit() {
                            Ok(_) => (),
                            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {
                                log::warn!("Ring busy")
                            }
                            Err(err) => {
                                log::error!("Failed ring submission, aborting: {err}");
                                return Err(Error::Io(err));
                            }
                        }
                    }
                    sq.sync();
                    if backlog.is_empty() {
                        break;
                    }
                    let sq_remaining_capacity = sq.capacity() - sq.len();
                    let num_to_drain = backlog.len().min(sq_remaining_capacity);
                    for sqe in backlog.drain(..num_to_drain) {
                        unsafe {
                            sq.push(&sqe)
                                .expect("SQE should never be full when clearing backlog")
                        }
                    }
                }

                let tick_delta = last_tick.elapsed();
                if tick_delta > Duration::from_secs(1) {
                    tick(&tick_delta, &mut self.connections, &mut torrent_state);
                    last_tick = Instant::now();
                    // TODO deal with this in the tick itself
                    for (conn_id, connection) in self.connections.iter_mut() {
                        for msg in connection.outgoing_msgs_buffer.iter_mut() {
                            let conn_fd = connection.socket.as_raw_fd();
                            let buffer = self.write_pool.get_buffer();
                            msg.message.encode(buffer.inner);
                            let size = msg.message.encoded_size();
                            push_connected_write(
                                conn_id,
                                conn_fd,
                                &mut self.events,
                                &mut sq,
                                buffer.index,
                                &buffer.inner[..size],
                                msg.ordered,
                                &mut backlog,
                            );
                        }
                        connection.outgoing_msgs_buffer.clear();
                    }
                    sq.sync();
                }

                for cqe in &mut cq {
                    let user_data = UserData::from_u64(cqe.user_data());
                    let read_bid = io_uring::cqueue::buffer_select(cqe.flags());

                    if let Err(err) =
                        self.event_handler(&mut sq, cqe, read_bid, &mut torrent_state, &mut backlog)
                    {
                        log::error!("Error handling event: {err}");
                    }
                    // time to return any potential write buffers
                    if let Some(write_idx) = user_data.buffer_idx {
                        self.write_pool.return_buffer(write_idx as usize);
                    }

                    // Ensure bids are always returned
                    if let Some(bid) = read_bid {
                        self.read_ring.return_bid(bid);
                    }
                }
                self.connect_to_new_peers(&mut sq)?;
                sq.sync();
                if torrent_state.is_complete {
                    log::info!("Torrent complete!");
                    return Ok(());
                }
            }
        };
        let result = actual_loop();
        self.read_ring.unregister(&ring.submitter())?;
        result
    }

    fn connect_to_new_peers(&mut self, sq: &mut SubmissionQueue<'_>) -> Result<(), Error> {
        loop {
            match self.peer_provider.try_recv() {
                Ok(addr) => {
                    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
                    //socket.set_recv_buffer_size(1 << 19).unwrap();
                    let fd = socket.as_raw_fd();
                    let event_idx = self.events.insert(Event::Connect { socket, addr });
                    let user_data = UserData::new(event_idx, None);
                    let addr = SockAddr::from(addr);
                    let connect_op =
                        opcode::Connect::new(types::Fd(fd), addr.as_ptr() as *const _, addr.len())
                            .build()
                            .flags(io_uring::squeue::Flags::IO_LINK)
                            .user_data(user_data.as_u64());
                    unsafe {
                        sq.push(&connect_op).unwrap();
                    }
                    let timeout = Timespec::new().sec(3);
                    let user_data = UserData::new(event_idx, None);
                    let timeout_op = opcode::LinkTimeout::new(&timeout)
                        .build()
                        .user_data(user_data.as_u64());
                    unsafe {
                        sq.push(&timeout_op).unwrap();
                    }
                }
                Err(TryRecvError::Disconnected) => return Err(Error::PeerProviderDisconnect),
                Err(TryRecvError::Empty) => break,
            }
        }
        Ok(())
    }

    fn event_handler(
        &mut self,
        sq: &mut SubmissionQueue<'_>,
        cqe: Entry,
        read_bid: Option<Bid>,
        torrent_state: &mut TorrentState,
        backlog: &mut VecDeque<io_uring::squeue::Entry>,
    ) -> io::Result<()> {
        let ret = cqe.result();
        let user_data = UserData::from_u64(cqe.user_data());
        if ret < 0 {
            return event_error_handler(
                sq,
                -ret as _,
                user_data,
                &mut self.events,
                &mut self.connections,
                self.read_ring.bgid(),
                backlog,
            );
        }
        let mut event = Event::Dummy;
        std::mem::swap(&mut event, &mut self.events[user_data.event_idx as usize]);
        match event {
            Event::Accept => {
                // The event is reused and not replaced
                std::mem::swap(&mut event, &mut self.events[user_data.event_idx as usize]);
                let fd = ret;
                let socket = unsafe { Socket::from_raw_fd(fd) };
                if socket.peer_addr()?.is_ipv6() {
                    log::error!("Received connection from non ipv4 addr");
                    return Ok(());
                };

                log::info!("Accepted connection: {:?}", socket.peer_addr()?);
                // Construct new recv token on accept, after that it lives forever and or is reused
                // since this is a recvmulti operation
                let read_token = self.events.insert(Event::Recv { socket });
                let user_data = UserData::new(read_token, None);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    if sq.push(&read_op).is_err() {
                        log::warn!("SQ buffer full, pushing to backlog");
                        backlog.push_back(read_op);
                    }
                }
            }
            Event::Connect { socket, addr } => {
                log::info!("Connected to: {addr}");
                // TODO: TIMEOUT HANDSHAKE
                let buffer = self.write_pool.get_buffer();
                write_handshake(self.our_id, torrent_state.info_hash, buffer.inner);
                let fd = socket.as_raw_fd();
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[user_data.event_idx as usize],
                    Event::Write { socket },
                );
                debug_assert!(matches!(old, Event::Dummy));
                let write_token = user_data.event_idx as usize;
                let user_data = UserData::new(write_token, Some(buffer.index));
                let write_op = opcode::Write::new(
                    types::Fd(fd),
                    buffer.inner.as_ptr(),
                    // TODO: Handle this better
                    HANDSHAKE_SIZE as u32,
                )
                .build()
                .user_data(user_data.as_u64());
                unsafe {
                    if sq.push(&write_op).is_err() {
                        log::warn!("SQ buffer full, pushing to backlog");
                        backlog.push_back(write_op);
                    }
                }
            }
            Event::Write { socket } => {
                let fd = socket.as_raw_fd();
                log::debug!("Wrote to unestablsihed connection");
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[user_data.event_idx as usize],
                    Event::Recv { socket },
                );
                debug_assert!(matches!(old, Event::Dummy));
                let read_token = user_data.event_idx as usize;
                let user_data = UserData::new(read_token, None);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    if sq.push(&read_op).is_err() {
                        log::warn!("SQ buffer full, pushing to backlog");
                        backlog.push_back(read_op);
                    }
                }
            }
            Event::ConnectedWrite { connection_idx: _ } => {
                // neither replaced nor modified
                // TODO: add to metrics
                self.events.remove(user_data.event_idx as _);
            }
            Event::Recv { socket } => {
                let fd = socket.as_raw_fd();
                let len = ret as usize;
                if len == 0 {
                    log::warn!("READ 0");
                    self.events.remove(user_data.event_idx as _);
                    return Ok(());
                }
                let is_more = io_uring::cqueue::more(cqe.flags());
                if !is_more {
                    restart_multishot_recv(sq, user_data, fd, backlog, self.read_ring.bgid());
                }
                // We always have a buffer associated
                let buffer = read_bid.map(|bid| self.read_ring.get(bid)).unwrap();
                // Expect this to be the handshake response
                let parsed_handshake =
                    parse_handshake(torrent_state.info_hash, &buffer[..len]).unwrap();
                let peer_connection = PeerConnection::new(
                    socket,
                    parsed_handshake.peer_id,
                    parsed_handshake.fast_ext,
                );
                log::info!("Finished handshake!: {peer_connection:?}");
                let connection_idx = self.connections.insert(peer_connection);
                // We are now connected!
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[user_data.event_idx as usize],
                    Event::ConnectedRecv { connection_idx },
                );
                debug_assert!(matches!(old, Event::Dummy));

                let completed = torrent_state.piece_selector.completed_clone();
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
                push_connected_write(
                    connection_idx,
                    fd,
                    &mut self.events,
                    sq,
                    buffer.index,
                    &buffer.inner[..size],
                    bitfield_msg.ordered,
                    backlog,
                );
            }
            Event::ConnectedRecv { connection_idx } => {
                // The event is reused and not replaced
                std::mem::swap(&mut event, &mut self.events[user_data.event_idx as usize]);
                let connection = &mut self.connections[connection_idx];
                let len = ret as usize;
                if len == 0 {
                    log::warn!("READ 0");
                    self.events.remove(user_data.event_idx as _);
                    connection.pending_disconnect = true;
                    return Ok(());
                }
                let is_more = io_uring::cqueue::more(cqe.flags());
                if !is_more {
                    let fd = connection.socket.as_raw_fd();
                    restart_multishot_recv(sq, user_data, fd, backlog, self.read_ring.bgid());
                }

                // We always have a buffer associated
                let buffer = read_bid.map(|bid| self.read_ring.get(bid)).unwrap();
                let buffer = &buffer[..len];
                connection.stateful_decoder.append_data(buffer);
                let conn_fd = connection.socket.as_raw_fd();
                while let Some(parse_result) = connection.stateful_decoder.next() {
                    match parse_result {
                        Ok(peer_message) => {
                            match connection.handle_message(
                                connection_idx,
                                peer_message,
                                torrent_state,
                            ) {
                                Ok(outgoing_messages) => {
                                    for outgoing in outgoing_messages {
                                        // Buffers are returned in the event loop
                                        let buffer = self.write_pool.get_buffer();
                                        outgoing.message.encode(buffer.inner);
                                        let size = outgoing.message.encoded_size();
                                        push_connected_write(
                                            connection_idx,
                                            conn_fd,
                                            &mut self.events,
                                            sq,
                                            buffer.index,
                                            &buffer.inner[..size],
                                            outgoing.ordered,
                                            backlog,
                                        )
                                    }
                                }
                            }
                            Err(err) => {
                                log::error!("Failed decoding message: {err}");
                            }
                        }
                    }
                }
            }
            Event::Dummy => unreachable!(),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_user_data_roundtrip() {
        let data = UserData::new((u32::MAX as usize) - 1, None);
        let serialized = data.as_u64();
        assert_eq!(data, UserData::from_u64(serialized));

        let data = UserData::new((u32::MAX as usize) - 1, Some(0));
        let serialized = data.as_u64();
        assert_eq!(data, UserData::from_u64(serialized));

        let data = UserData::new(0, Some((u32::MAX as usize) - 1));
        let serialized = data.as_u64();
        assert_eq!(data, UserData::from_u64(serialized));
    }
}
