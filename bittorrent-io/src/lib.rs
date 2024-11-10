use core::panic;
use std::{
    io,
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd},
    time::{Duration, Instant},
};

use buf_pool::BufferPool;
use buf_ring::{Bid, BufferRing};
use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring, SubmissionQueue,
};
use peer_connection::PeerConnection;
use peer_protocol::{generate_peer_id, parse_handshake, write_handshake, PeerMessage};
use slab::Slab;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

mod buf_pool;
mod buf_ring;
mod file;
mod peer_connection;
mod peer_protocol;

const TIMESPEC: &Timespec = &Timespec::new().sec(1);

// Write fd Read fd
// Write ConnectionId Read ConnectionId !!!
#[derive(Debug, Clone)]
enum Event {
    Accept,
    // fd?
    Connect {
        fd: RawFd,
        addr: SocketAddr,
    },
    Write {
        fd: RawFd,
        buffer_idx: usize,
    },
    Recv {
        fd: RawFd,
    },
    ConnectedWrite {
        connection_idx: usize,
        buffer_idx: usize,
    },
    ConnectedRecv {
        connection_idx: usize,
    },
}

pub fn setup_listener(info_hash: [u8; 20]) {
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .setup_clamp()
        .setup_cqsize(1024)
        .setup_defer_taskrun()
        .build(1024)
        .unwrap();

    let mut events = Slab::with_capacity(256);
    let event_idx = events.insert(Event::Accept);

    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
        .build()
        .user_data(event_idx as _);

    unsafe {
        ring.submission().push(&accept_op).unwrap();
    }
    ring.submission().sync();

    // event loop
    event_loop(ring, &mut events, info_hash).unwrap()
}

pub fn connect_to(addr: SocketAddr, info_hash: [u8; 20]) {
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .setup_clamp()
        .setup_cqsize(1024)
        .setup_defer_taskrun()
        .setup_coop_taskrun()
        .build(1024)
        .unwrap();

    let mut event = Slab::with_capacity(256);
    let stream = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();

    let event_idx = event.insert(Event::Connect {
        addr,
        fd: stream.as_raw_fd(),
    });
    let addr = SockAddr::from(addr);
    let connect_op = opcode::Connect::new(
        types::Fd(stream.as_raw_fd()),
        addr.as_ptr() as *const _,
        addr.len(),
    )
    .build()
    //    .flags(io_uring::squeue::Flags::IO_LINK)
    .user_data(event_idx as _);
    unsafe {
        ring.submission().push(&connect_op).unwrap();
    }
    //   let timeout_op = opcode::LinkTimeout::new(TIMESPEC).build().user_data(0xdead);
    //  unsafe {
    //     ring.submission().push(&timeout_op).unwrap();
    //}

    ring.submission().sync();

    // event loop
    event_loop(ring, &mut event, info_hash).unwrap()
}

// Validate hashes in here and simply use one shot channels
fn tick(tick_delta: &Duration) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
}

fn event_handler(
    sq: &mut SubmissionQueue<'_>,
    cqe: Entry,
    events: &mut Slab<Event>,
    connections: &mut Slab<PeerConnection>,
    write_pool: &mut BufferPool,
    read_ring: &mut BufferRing,
    info_hash: [u8; 20],
) -> io::Result<()> {
    let ret = cqe.result();
    let event_idx = cqe.user_data();
    let token = &mut events[event_idx as usize];
    if ret < 0 {
        if -ret == libc::ENOBUFS {
            // TODO: statistics
            log::warn!("Ran out of buffers!, resubmitting recv op");
            // Ran out of buffers!
            if let Event::Recv { fd } = token {
                let read_op = opcode::RecvMulti::new(types::Fd(*fd), read_ring.bgid())
                    .build()
                    // Reuse the token
                    .user_data(event_idx as _)
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op)
                        .expect("SubmissionQueue should never be full");
                }
            } else {
                panic!("Ran out of buffers on a non recv operation");
            }
        } else {
            let event_idx = cqe.user_data();
            let token = &mut events[event_idx as usize];
            dbg!(token);
            let err = std::io::Error::from_raw_os_error(-ret);
            return Err(err);
        }
    }
    match token.clone() {
        Event::Accept => {
            log::info!("Accepted connection!");
            let fd = ret;
            // Construct new recv token on accept, after that it lives forever and or is reused
            // since this is a recvmulti operation
            let read_token = events.insert(Event::Recv { fd });
            let read_op = opcode::RecvMulti::new(types::Fd(fd), read_ring.bgid())
                .build()
                .user_data(read_token as _)
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
            let write_token = events.insert(Event::Write { fd, buffer_idx });
            let write_op =
                opcode::Write::new(types::Fd(fd), buffer.as_mut_ptr(), buffer.len() as u32)
                    .build()
                    .user_data(write_token as _)
                    // Link with read
                    .flags(io_uring::squeue::Flags::IO_LINK);
            unsafe {
                sq.push(&write_op)
                    .expect("SubmissionQueue should never be full");
            }
            let read_token = events.insert(Event::Recv { fd });
            let read_op = opcode::RecvMulti::new(types::Fd(fd), read_ring.bgid())
                .build()
                .user_data(read_token as _)
                .flags(io_uring::squeue::Flags::BUFFER_SELECT);
            unsafe {
                sq.push(&read_op)
                    .expect("SubmissionQueue should never be full");
            }
            // send write + read linked
            // This only reports connect complete user data is needed to provide more info
            println!("CONNECT: {addr}");
        }
        Event::Write { fd, buffer_idx } => {
            log::debug!("Wrote to unestablsihed connection");
            events.remove(event_idx as _);
            write_pool.return_buffer(buffer_idx);
        }
        Event::ConnectedWrite {
            connection_idx,
            buffer_idx,
        } => {
            log::debug!("Wrote to established connection");
            events.remove(event_idx as _);
            write_pool.return_buffer(buffer_idx);
        }
        Event::Recv { fd } => {
            log::info!("RECV");
            let len = ret as usize;
            let bid = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();
            let buffer = read_ring.get(dbg!(bid));
            // Expect this to be the handshake response
            let peer_id = parse_handshake(info_hash, &buffer[..len]).unwrap();
            let peer_connection = PeerConnection::new(fd, peer_id);
            log::info!("Finished handshake!: {peer_connection:?}");
            let connection_idx = connections.insert(peer_connection);
            // We are now connected!
            *token = Event::ConnectedRecv { connection_idx };
            read_ring.return_bid(bid);
        }
        Event::ConnectedRecv { connection_idx } => {
            log::info!("CONNRECV");
            let connection = &mut connections[connection_idx];
            let is_more = io_uring::cqueue::more(cqe.flags());
            if !is_more {
                println!("No more, starting new recv");
                // TODO? Return bids and or read the current buffer?
                //buf_ring.return_bid(bid);
                let read_op = opcode::RecvMulti::new(types::Fd(connection.fd), read_ring.bgid())
                    .build()
                    .user_data(event_idx as _)
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op).unwrap();
                }
            }

            let len = ret as usize;

            let bid = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();
            let buffer = read_ring.get(dbg!(bid));
            let buffer = &buffer[..len];
            if buffer.is_empty() {
                println!("READ 0");
                events.remove(event_idx as _);
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
                            recv_handler(peer_message, connection);
                        }
                        Err(err) => {
                            log::error!("Failed decoding message: {err}");
                        }
                    }
                }
            }
            // TODO: This is not safe, need to always return these
            // (DO THIS OUTSIDE THIS FUNC)
            read_ring.return_bid(bid);
        }
    }
    Ok(())
}

fn recv_handler(message: PeerMessage, connection: &mut PeerConnection) -> io::Result<()> {
    log::info!("RECIEVED: {message:?} from {:?}", connection.peer_id);
    Ok(())
}

fn event_loop(mut ring: IoUring, events: &mut Slab<Event>, info_hash: [u8; 20]) -> io::Result<()> {
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

        for cqe in &mut cq {
            if let Err(err) = event_handler(
                &mut sq,
                cqe,
                events,
                &mut connections,
                &mut write_pool,
                &mut read_ring,
                info_hash,
            ) {
                log::error!("Error handling event: {err}");
            }
        }
    }
}
