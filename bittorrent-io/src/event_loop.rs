use std::{
    io,
    net::SocketAddr,
    os::fd::RawFd,
    time::{Duration, Instant},
    u32,
};

use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring, SubmissionQueue,
};
use slab::Slab;

use crate::{
    buf_pool::BufferPool,
    buf_ring::{Bgid, BufferRing},
    peer_connection::PeerConnection,
    peer_protocol::{generate_peer_id, parse_handshake, write_handshake},
};

// Write fd Read fd
// Write ConnectionId Read ConnectionId !!!
#[derive(Debug, Clone)]
pub enum Event {
    Accept,
    // fd?
    Connect { fd: RawFd, addr: SocketAddr },
    Write { fd: RawFd },
    Recv { fd: RawFd },
    ConnectedWrite { connection_idx: usize },
    ConnectedRecv { connection_idx: usize },
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
    bgid: Bgid,
) -> io::Result<()> {
    if error_code as i32 == libc::ENOBUFS {
        // TODO: statistics
        log::warn!("Ran out of buffers!, resubmitting recv op");
        let event = &events[user_data.event_idx as _];
        // Ran out of buffers!
        if let Event::Recv { fd } = event {
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
        } else {
            panic!("Ran out of buffers on a non recv operation");
        }
    } else {
        let event = events.remove(user_data.event_idx as _);
        dbg!(event);
        let err = std::io::Error::from_raw_os_error(error_code as i32);
        return Err(err);
    }
}

fn event_handler(
    sq: &mut SubmissionQueue<'_>,
    cqe: Entry,
    events: &mut Slab<Event>,
    connections: &mut Slab<PeerConnection>,
    write_pool: &mut BufferPool,
    read_buffer: Option<&[u8]>,
    bgid: Bgid,
    info_hash: [u8; 20],
) -> io::Result<()> {
    let ret = cqe.result();
    let user_data = UserData::from_u64(cqe.user_data());
    let event = &mut events[user_data.event_idx as usize];
    if ret < 0 {
        return event_error_handler(sq, -ret as _, user_data, events, bgid);
    }
    match event.clone() {
        Event::Accept => {
            log::info!("Accepted connection!");
            let fd = ret;
            // Construct new recv token on accept, after that it lives forever and or is reused
            // since this is a recvmulti operation
            let read_token = events.insert(Event::Recv { fd });
            let user_data = UserData::new(read_token, None);
            let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
                .build()
                .user_data(user_data.as_u64())
                .flags(io_uring::squeue::Flags::BUFFER_SELECT);
            unsafe {
                sq.push(&read_op)
                    .expect("SubmissionQueue should never be full");
            }
        }
        Event::Connect { addr, fd } => {
            let our_id = generate_peer_id();
            let (buffer_idx, buffer) = write_pool.get_buffer();
            write_handshake(our_id, info_hash, buffer);
            let write_token = events.insert(Event::Write { fd });
            let user_data = UserData::new(write_token, Some(buffer_idx));
            let write_op =
                opcode::Write::new(types::Fd(fd), buffer.as_mut_ptr(), buffer.len() as u32)
                    .build()
                    .user_data(user_data.as_u64())
                    // Link with read
                    .flags(io_uring::squeue::Flags::IO_LINK);
            unsafe {
                sq.push(&write_op)
                    .expect("SubmissionQueue should never be full");
            }
            let read_token = events.insert(Event::Recv { fd });
            let user_data = UserData::new(read_token, None);
            let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
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
            events.remove(user_data.event_idx as _);
        }
        Event::ConnectedWrite { connection_idx } => {
            log::debug!("Wrote to established connection");
            events.remove(user_data.event_idx as _);
        }
        Event::Recv { fd } => {
            log::info!("RECV");
            let len = ret as usize;
            // We always have a buffer associated
            let buffer = read_buffer.unwrap();
            // Expect this to be the handshake response
            let peer_id = parse_handshake(info_hash, &buffer[..len]).unwrap();
            let peer_connection = PeerConnection::new(fd, peer_id);
            log::info!("Finished handshake!: {peer_connection:?}");
            let connection_idx = connections.insert(peer_connection);
            // We are now connected!
            *event = Event::ConnectedRecv { connection_idx };
        }
        Event::ConnectedRecv { connection_idx } => {
            log::info!("CONNRECV");
            let connection = &mut connections[connection_idx];
            let is_more = io_uring::cqueue::more(cqe.flags());
            if !is_more {
                println!("No more, starting new recv");
                // TODO? Return bids and or read the current buffer?
                //buf_ring.return_bid(bid);
                let read_op = opcode::RecvMulti::new(types::Fd(connection.fd), bgid)
                    .build()
                    .user_data(user_data.as_u64())
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op).unwrap();
                }
            }

            let len = ret as usize;

            let buffer = read_buffer.unwrap();
            let buffer = &buffer[..len];
            if buffer.is_empty() {
                println!("READ 0");
                events.remove(user_data.event_idx as _);
                let fd = connection.fd;
                connections.remove(connection_idx as _);
                println!("shutting down connection");
                // TODO graceful shutdown
                unsafe {
                    libc::close(fd);
                }
            } else {
                connection.stateful_decoder.append_data(buffer);
                while let Some(parse_result) = connection.stateful_decoder.next() {
                    match parse_result {
                        Ok(peer_message) => {
                            connection.handle_message(peer_message)?;
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

const TIMESPEC: &Timespec = &Timespec::new().sec(1);

pub fn event_loop(
    mut ring: IoUring,
    events: &mut Slab<Event>,
    info_hash: [u8; 20],
    mut tick: impl FnMut(&Duration),
) -> io::Result<()> {
    let (submitter, mut sq, mut cq) = ring.split();
    let mut write_pool = BufferPool::new(64, 128);

    let mut read_ring = BufferRing::new(1, 64, 512).unwrap();
    read_ring.register(&submitter).unwrap();

    let mut connections = Slab::with_capacity(64);

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
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => log::warn!("Ring busy"),
                Err(err) => {
                    log::error!("Failed ring submission, aborting: {err}");
                    return Err(err);
                }
            }
        }
        sq.sync();

        let tick_delta = last_tick.elapsed();
        if tick_delta > Duration::from_secs(1) {
            tick(&tick_delta);
            last_tick = Instant::now();
        }

        let bgid = read_ring.bgid();
        for cqe in &mut cq {
            let user_data = UserData::from_u64(cqe.user_data());
            let read_bid = io_uring::cqueue::buffer_select(cqe.flags());

            let read_buffer = read_bid.map(|bid| read_ring.get(bid));

            if let Err(err) = event_handler(
                &mut sq,
                cqe,
                events,
                &mut connections,
                &mut write_pool,
                read_buffer,
                bgid,
                info_hash,
            ) {
                log::error!("Error handling event: {err}");
            }

            // time to return any potential write buffers
            if let Some(write_idx) = user_data.buffer_idx {
                write_pool.return_buffer(write_idx as usize);
            }

            // Ensure bids are always returned
            if let Some(bid) = read_bid {
                read_ring.return_bid(bid);
            }
        }
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
