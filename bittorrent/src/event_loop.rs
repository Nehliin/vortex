use std::{
    io,
    net::SocketAddrV4,
    os::fd::{AsRawFd, FromRawFd},
    sync::mpsc::{Receiver, TryRecvError},
    time::{Duration, Instant},
};

use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring,
};
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use crate::{
    buf_pool::BufferPool,
    buf_ring::{Bgid, Bid, BufferRing},
    io_utils::{self, BackloggedSubmissionQueue, SubmissionQueue, UserData},
    peer_connection::{DisconnectReason, OutgoingMsg, PeerConnection},
    peer_protocol::{self, parse_handshake, write_handshake, PeerId, HANDSHAKE_SIZE},
    piece_selector::SUBPIECE_SIZE,
    Error, TorrentState,
};

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
    torrent_state: &mut TorrentState,
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
                    io_utils::recv(sq, user_data, fd, bgid);
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
            let EventType::Connect { socket, addr } = event else {
                panic!("Timed out something other than a connect: {event:?}");
            };
            log::debug!("Connect timed out!: {addr}");
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
                    connection.release_pieces(torrent_state);
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

const CQE_WAIT_TIME: &Timespec = &Timespec::new().nsec(250_000_000);

pub struct EventLoop {
    events: Slab<EventType>,
    write_pool: BufferPool,
    read_ring: BufferRing,
    connections: Slab<PeerConnection>,
    peer_provider: Receiver<SocketAddrV4>,
    our_id: PeerId,
}

impl EventLoop {
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
        mut torrent_state: TorrentState,
        mut tick: impl FnMut(&Duration, &mut Slab<PeerConnection>, &mut TorrentState),
    ) -> Result<(), Error> {
        self.read_ring.register(&ring.submitter())?;
        // lambda to be able to catch errors an always unregistering the read ring
        let mut actual_loop = || {
            let (submitter, sq, mut cq) = ring.split();
            let mut sq = BackloggedSubmissionQueue::new(sq);
            let mut last_tick = Instant::now();
            loop {
                let args = types::SubmitArgs::new().timespec(CQE_WAIT_TIME);
                match submitter.submit_with_args(16, &args) {
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
                    tick(&tick_delta, &mut self.connections, &mut torrent_state);
                    last_tick = Instant::now();
                    // TODO deal with this in the tick itself
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
                        if let Some(reason) = &connection.pending_disconnect {
                            log::warn!("Disconnect: {} reason {reason}", connection.peer_id);
                            io_utils::stop_connection(
                                &mut sq,
                                conn_id,
                                connection.socket.as_raw_fd(),
                                &mut self.events,
                            );
                        }
                        connection.outgoing_msgs_buffer.clear();
                    }
                    sq.sync();
                }

                for cqe in &mut cq {
                    let user_data = UserData::from_u64(cqe.user_data());
                    let read_bid = io_uring::cqueue::buffer_select(cqe.flags());

                    if let Err(err) = self.event_handler(&mut sq, cqe, read_bid, &mut torrent_state)
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

    fn connect_to_new_peers<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
    ) -> Result<(), Error> {
        loop {
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
                    let timeout = Timespec::new().sec(3);
                    let user_data = UserData::new(event_idx, None);
                    let timeout_op = opcode::LinkTimeout::new(&timeout)
                        .build()
                        .user_data(user_data.as_u64());
                    }
                }
                Err(TryRecvError::Disconnected) => return Err(Error::PeerProviderDisconnect),
                Err(TryRecvError::Empty) => break,
            }
        }
        Ok(())
    }

    fn event_handler<Q: SubmissionQueue>(
        &mut self,
        sq: &mut BackloggedSubmissionQueue<Q>,
        cqe: Entry,
        read_bid: Option<Bid>,
        torrent_state: &mut TorrentState,
    ) -> io::Result<()> {
        let ret = cqe.result();
        let user_data = UserData::from_u64(cqe.user_data());
        if ret < 0 {
            return event_error_handler(
                sq,
                -ret as _,
                user_data,
                &mut self.events,
                torrent_state,
                &mut self.connections,
                self.read_ring.bgid(),
            );
        }
        let mut event = EventType::Dummy;
        std::mem::swap(&mut event, &mut self.events[user_data.event_idx as usize]);
        match event {
            EventType::Accept => {
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
                // TODO: TIMEOUT HANDSHAKE
                let buffer = self.write_pool.get_buffer();
                write_handshake(self.our_id, torrent_state.info_hash, buffer.inner);
                let fd = socket.as_raw_fd();
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[user_data.event_idx as usize],
                    EventType::Write { socket },
                );
                debug_assert!(matches!(old, EventType::Dummy));
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
                sq.push(write_op);
            }
            EventType::Write { socket } => {
                let fd = socket.as_raw_fd();
                log::debug!("Wrote to unestablsihed connection");
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[user_data.event_idx as usize],
                    EventType::Recv { socket },
                );
                debug_assert!(matches!(old, EventType::Dummy));
                let read_token = user_data.event_idx as usize;
                let user_data = UserData::new(read_token, None);
                // Multishot isn't used here to simplify error handling
                // when the read is invalid or otherwise doesn't lead to
                // a full connection which does have graceful shutdown mechanisms
                io_utils::recv(sq, user_data, fd, self.read_ring.bgid());
            }
            EventType::ConnectedWrite { connection_idx: _ } => {
                // neither replaced nor modified
                // TODO: add to metrics
                self.events.remove(user_data.event_idx as _);
            }
            EventType::Recv { socket } => {
                let fd = socket.as_raw_fd();
                let len = ret as usize;
                if len == 0 {
                    log::debug!("No more data when expecting handshake");
                    self.events.remove(user_data.event_idx as _);
                    return Ok(());
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
                let id = peer_connection.peer_id;
                let connection_idx = self.connections.insert(peer_connection);
                log::info!("Finished handshake! [{connection_idx}]: {id}");
                // We are now connected!
                // The event is replaced (this removes the dummy)
                let old = std::mem::replace(
                    &mut self.events[user_data.event_idx as usize],
                    EventType::ConnectedRecv { connection_idx },
                );
                debug_assert!(matches!(old, EventType::Dummy));
                // Recv has been complete, move over to multishot, same user data
                io_utils::recv_multishot(sq, user_data, fd, self.read_ring.bgid());
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
                io_utils::write_to_connection(
                    connection_idx,
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
                std::mem::swap(&mut event, &mut self.events[user_data.event_idx as usize]);
                let connection = &mut self.connections[connection_idx];
                let len = ret as usize;
                if len == 0 {
                    log::debug!(
                        "[PeerId: {}] No more data, mark as pending disconnect",
                        connection.peer_id
                    );
                    self.events.remove(user_data.event_idx as _);
                    connection.pending_disconnect = Some(DisconnectReason::ClosedConnection);
                    return Ok(());
                }
                let is_more = io_uring::cqueue::more(cqe.flags());
                if !is_more {
                    let fd = connection.socket.as_raw_fd();
                    // restart the operation
                    io_utils::recv_multishot(sq, user_data, fd, self.read_ring.bgid());
                }

                // We always have a buffer associated
                let buffer = read_bid.map(|bid| self.read_ring.get(bid)).unwrap();
                let buffer = &buffer[..len];
                connection.stateful_decoder.append_data(buffer);
                let conn_fd = connection.socket.as_raw_fd();
                while let Some(parse_result) = connection.stateful_decoder.next() {
                    match parse_result {
                        Ok(peer_message) => {
                            connection.handle_message(connection_idx, peer_message, torrent_state);
                            for outgoing in connection.outgoing_msgs_buffer.iter() {
                                // Buffers are returned in the event loop
                                let buffer = self.write_pool.get_buffer();
                                outgoing.message.encode(buffer.inner);
                                let size = outgoing.message.encoded_size();
                                io_utils::write_to_connection(
                                    connection_idx,
                                    conn_fd,
                                    &mut self.events,
                                    sq,
                                    buffer.index,
                                    &buffer.inner[..size],
                                    outgoing.ordered,
                                )
                            }
                        }
                        Err(err) => {
                            log::error!("Failed {connection_idx} decoding message: {err}");
                            connection.pending_disconnect = Some(DisconnectReason::InvalidMessage);
                        }
                    }
                }
            }
            EventType::ConnectionStopped { connection_idx } => {
                let mut connection = self.connections.remove(connection_idx);
                connection.release_pieces(torrent_state);
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_user_data_roundtrip() {}
}
