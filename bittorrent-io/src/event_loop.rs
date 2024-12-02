use std::{
    io,
    net::SocketAddr,
    os::fd::RawFd,
    time::{Duration, Instant},
    u32,
};

use bytes::BufMut;
use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring, SubmissionQueue,
};
use slab::Slab;

use crate::{
    buf_pool::{Buffer, BufferPool},
    buf_ring::{Bgid, Bid, BufferRing},
    peer_connection::{Error, PeerConnection},
    peer_protocol::{parse_handshake, write_handshake, PeerMessage, HANDSHAKE_SIZE},
    TorrentState,
};

// Write fd Read fd
// Write ConnectionId Read ConnectionId !!!
#[derive(Debug, Clone)]
pub enum Event {
    Accept,
    // fd?
    Connect {
        fd: RawFd,
        addr: SocketAddr,
    },
    Write {
        fd: RawFd,
    },
    Recv {
        fd: RawFd,
    },
    ConnectedWrite {
        connection_idx: usize,
    },
    ConnectedRecv {
        connection_idx: usize,
    },
}

pub fn push_connected_write(
    conn_id: usize,
    fd: RawFd,
    events: &mut Slab<Event>,
    sq: &mut SubmissionQueue<'_>,
    buffer_index: usize,
    buffer: &[u8],
    ordered: bool,
    timeout: Option<(Subpiece, Duration)>,
) {
    let event = events.insert(Event::ConnectedWrite {
        connection_idx: conn_id,
        msg,
    });
    let user_data = UserData::new(event, Some(buffer_index));
    let flags = if ordered {
        io_uring::squeue::Flags::IO_LINK
    } else {
        io_uring::squeue::Flags::empty()
    };
    let write_op = opcode::Write::new(
        types::Fd(fd),
        buffer.as_ptr(),
        buffer.len() as u32,
    )
    .build()
    .user_data(user_data.as_u64())
    .flags(flags);

    unsafe {
        sq.push(&write_op)
            .expect("SubmissionQueue should never be full");
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
) -> io::Result<()> {
    if error_code as i32 == libc::ENOBUFS {
        // TODO: statistics
        log::warn!("Ran out of buffers!, resubmitting recv op");
        let event = &events[user_data.event_idx as _];
        // Ran out of buffers!
        match event {
            Event::Recv { fd } => {
                let read_op = opcode::RecvMulti::new(types::Fd(*fd), bgid)
                    .build()
                    // Reuse the user data
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op)
                        .expect("SubmissionQueue should never be full");
                }
                Ok(())
            }
            Event::ConnectedRecv { connection_idx } => {
                let fd = connections[*connection_idx].fd;
                let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
                    .build()
                    // Reuse the user data
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op)
                        .expect("SubmissionQueue should never be full");
                }
                Ok(())
            }
            _ => {
                panic!("Ran out of buffers on a non recv operation: {event:?}");
            }
        }
    } else {
        let event = events.remove(user_data.event_idx as _);
        dbg!(event);
        let err = std::io::Error::from_raw_os_error(error_code as i32);
        Err(err)
    }
}

const TIMESPEC: &Timespec = &Timespec::new().sec(1);

pub struct EventLoop {
    events: Slab<Event>,
    write_pool: BufferPool,
    read_ring: BufferRing,
    connections: Slab<PeerConnection>,
    our_id: [u8; 20],
}

impl EventLoop {
    pub fn new(our_id: [u8; 20], events: Slab<Event>) -> Self {
        Self {
            events,
            write_pool: BufferPool::new(64, 128),
            read_ring: BufferRing::new(1, 64, 512).unwrap(),
            connections: Slab::with_capacity(64),
            our_id,
        }
    }

    pub fn run(
        &mut self,
        mut ring: IoUring,
        mut torrent_state: TorrentState,
        mut tick: impl FnMut(&Duration, &mut Slab<PeerConnection>),
    ) -> io::Result<()> {
        let (submitter, mut sq, mut cq) = ring.split();

        self.read_ring.register(&submitter).unwrap();

        let mut last_tick = Instant::now();
        loop {
            let args = types::SubmitArgs::new().timespec(TIMESPEC);
            match submitter.submit_with_args(1, &args) {
                Ok(_) => (),
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => log::warn!("Ring busy"),
                Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => {
                    log::debug!("Tick hit ETIME")
                }
                Err(err) => {
                    log::error!("Failed ring submission, aborting: {err}");
                    return Err(err);
                }
            }
            cq.sync();
            if cq.overflow() > 0 {
                log::error!("CQ overflow");
            }

            // TODO: Loop this and track backlog like the example if necessary
            if sq.is_full() {
                match submitter.submit() {
                    Ok(_) => (),
                    Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => {
                        log::warn!("Ring busy")
                    }
                    Err(err) => {
                        log::error!("Failed ring submission, aborting: {err}");
                        return Err(err);
                    }
                }
            }
            sq.sync();

            let tick_delta = last_tick.elapsed();
            if tick_delta > Duration::from_secs(1) {
                tick(&tick_delta, &mut self.connections);
                last_tick = Instant::now();
            }

            for cqe in &mut cq {
                let user_data = UserData::from_u64(cqe.user_data());
                let read_bid = io_uring::cqueue::buffer_select(cqe.flags());

                if let Err(err) = self.event_handler(&mut sq, cqe, read_bid, &mut torrent_state) {
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
            sq.sync();
        }
    }

    fn event_handler(
        &mut self,
        sq: &mut SubmissionQueue<'_>,
        cqe: Entry,
        read_bid: Option<Bid>,
        torrent_state: &mut TorrentState,
    ) -> io::Result<()> {
        let ret = cqe.result();
        let user_data = UserData::from_u64(cqe.user_data());
        let event = &mut self.events[user_data.event_idx as usize];
        if ret < 0 {
            return event_error_handler(
                sq,
                -ret as _,
                user_data,
                &mut self.events,
                &mut self.connections,
                self.read_ring.bgid(),
            );
        }
        match event.clone() {
            Event::Accept => {
                log::info!("Accepted connection!");
                let fd = ret;
                // Construct new recv token on accept, after that it lives forever and or is reused
                // since this is a recvmulti operation
                let read_token = self.events.insert(Event::Recv { fd });
                let user_data = UserData::new(read_token, None);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op)
                        .expect("SubmissionQueue should never be full");
                }
            }
            Event::Connect { addr, fd } => {
                self.events.remove(user_data.event_idx as _);
                // TODO: TIMEOUT
                let buffer = self.write_pool.get_buffer();
                write_handshake(self.our_id, torrent_state.info_hash, buffer.inner);
                let write_token = self.events.insert(Event::Write { fd });
                let user_data = UserData::new(write_token, Some(buffer.index));
                let write_op = opcode::Write::new(
                    types::Fd(fd),
                    buffer.inner.as_ptr(),
                    // TODO: Handle this better
                    HANDSHAKE_SIZE as u32,
                )
                .build()
                .user_data(user_data.as_u64())
                // Link with read
                .flags(io_uring::squeue::Flags::IO_LINK);
                unsafe {
                    sq.push(&write_op)
                        .expect("SubmissionQueue should never be full");
                }
                let read_token = self.events.insert(Event::Recv { fd });
                let user_data = UserData::new(read_token, None);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op)
                        .expect("SubmissionQueue should never be full");
                }
                // send write + read linked
                // This only reports connect complete user data is needed to provide more info
                println!("CONNECT: {addr}");
            }
            Event::Write { fd } => {
                log::debug!("Wrote to unestablsihed connection");
                self.events.remove(user_data.event_idx as _);
            }
            Event::ConnectedWrite {
                connection_idx,
                msg,
            } => {
                dbg!(msg);
                log::debug!("Wrote to established connection");
                self.events.remove(user_data.event_idx as _);
            }
            Event::Recv { fd } => {
                log::info!("RECV");
                let len = ret as usize;
                // We always have a buffer associated
                let buffer = read_bid.map(|bid| self.read_ring.get(bid)).unwrap();
                // Expect this to be the handshake response
                let peer_id = parse_handshake(torrent_state.info_hash, &buffer[..len]).unwrap();
                let peer_connection = PeerConnection::new(fd, peer_id);
                log::info!("Finished handshake!: {peer_connection:?}");
                let connection_idx = self.connections.insert(peer_connection);
                // We are now connected!
                *event = Event::ConnectedRecv { connection_idx };
            }
            Event::ConnectedRecv { connection_idx } => {
                let connection = &mut self.connections[connection_idx];
                let is_more = io_uring::cqueue::more(cqe.flags());
                if !is_more {
                    log::warn!("No more, starting new recv");
                    // TODO? Return bids and or read the current buffer?
                    //buf_ring.return_bid(bid);
                    let read_op =
                        opcode::RecvMulti::new(types::Fd(connection.fd), self.read_ring.bgid())
                            .build()
                            .user_data(user_data.as_u64())
                            .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                    unsafe {
                        sq.push(&read_op).unwrap();
                    }
                    // This event doesn't contain any data
                    //return Ok(());
                }

                log::info!("CONNRECV");
                let len = ret as usize;

                // We always have a buffer associated
                let buffer = read_bid.map(|bid| self.read_ring.get(bid)).unwrap();
                let buffer = &buffer[..len];
                if buffer.is_empty() {
                    println!("READ 0");
                    self.events.remove(user_data.event_idx as _);
                    let fd = connection.fd;
                    self.connections.remove(connection_idx as _);
                    println!("shutting down connection");
                    // TODO graceful shutdown
                    unsafe {
                        libc::close(fd);
                    }
                } else {
                    connection.stateful_decoder.append_data(buffer);
                    let conn_fd = connection.fd;
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
                                            let copy = outgoing.message.clone();
                                            dbg!(buffer.index);
                                            push_connected_write(
                                                connection_idx,
                                                conn_fd,
                                                &mut self.events,
                                                sq,
                                                buffer.index,
                                                &buffer.inner[..size],
                                                outgoing.ordered,
                                                copy,
                                            )
                                        }
                                    }
                                    Err(Error::Disconnect) => {
                                        log::warn!(
                                            "[Peer {}] is being disconnected",
                                            connection.peer_id
                                        );
                                        // TODO proper shutdown
                                    }
                                    Err(err) => {
                                        log::error!("Failed handling message: {err}");
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
