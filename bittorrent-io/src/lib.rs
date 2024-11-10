use core::panic;
use std::{
    io,
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd},
    time::{Duration, Instant},
};

use buf_pool::BufferPool;
use buf_ring::{Bid, BufferRing};
use event_loop::Event;
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
mod event_loop;
mod file;
mod peer_connection;
mod peer_protocol;

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
    event_loop::event_loop(ring, &mut events, info_hash, tick).unwrap()
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

    let mut events = Slab::with_capacity(256);
    let stream = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();

    let event_idx = events.insert(Event::Connect {
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
    event_loop::event_loop(ring, &mut events, info_hash, tick).unwrap()
}

// Validate hashes in here and simply use one shot channels
fn tick(tick_delta: &Duration) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
}

fn recv_handler(message: PeerMessage, connection: &mut PeerConnection) -> io::Result<()> {
    log::info!("RECIEVED: {message:?} from {:?}", connection.peer_id);
    Ok(())
}
