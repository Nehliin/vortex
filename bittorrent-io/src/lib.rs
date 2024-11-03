use core::panic;
use std::{
    io,
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd},
    time::{Duration, Instant},
};

use buf_ring::{Bid, BufferRing};
use file::MmapFile;
use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, Timespec},
    IoUring, SubmissionQueue,
};
use slab::Slab;

mod buf_ring;
mod file;
pub mod peer_message;

const TIMESPEC: &Timespec = &Timespec::new().sec(1);

#[derive(Debug, Clone)]
enum Event {
    Accept,
    Recv { fd: RawFd },
}

pub fn setup_listener() {
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .setup_clamp()
        .setup_cqsize(1024)
        .setup_defer_taskrun()
        .build(1024)
        .unwrap();

    let mut tokens = Slab::with_capacity(256);
    let token_idx = tokens.insert(Event::Accept);

    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
        .build()
        .user_data(token_idx as _);

    unsafe {
        ring.submission().push(&accept_op).unwrap();
    }
    ring.submission().sync();

    // event loop
    event_loop(ring, &mut tokens).unwrap()
}

// Validate hashes in here and simply use one shot channels
fn tick(tick_delta: &Duration) {
    log::info!("Tick!: {}", tick_delta.as_secs_f32());
}

fn event_handler(
    sq: &mut SubmissionQueue<'_>,
    cqe: Entry,
    events: &mut Slab<Event>,
    buf_ring: &mut BufferRing,
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
                let read_op = opcode::RecvMulti::new(types::Fd(*fd), buf_ring.bgid())
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
            let read_op = opcode::RecvMulti::new(types::Fd(fd), buf_ring.bgid())
                .build()
                .user_data(read_token as _)
                .flags(io_uring::squeue::Flags::BUFFER_SELECT);
            unsafe {
                sq.push(&read_op)
                    .expect("SubmissionQueue should never be full");
            }
        }
        Event::Recv { fd } => {
            let is_more = io_uring::cqueue::more(cqe.flags());
            if !is_more {
                println!("No more, starting new recv");
                // TODO? Return bids and or read the current buffer?
                //buf_ring.return_bid(bid);
                let read_op = opcode::RecvMulti::new(types::Fd(fd), buf_ring.bgid())
                    .build()
                    .user_data(event_idx as _)
                    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                unsafe {
                    sq.push(&read_op).unwrap();
                }
            }

            let len = ret as usize;

            let bid = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();
            let buffer = buf_ring.get(dbg!(bid));
            recv_handler(fd, &buffer[..len], event_idx, events)?;
            buf_ring.return_bid(bid);
        }
    }
    Ok(())
}

fn recv_handler(fd: i32, data: &[u8], event_idx: u64, events: &mut Slab<Event>) -> io::Result<()> {
    if data.is_empty() {
        println!("READ 0");
        events.remove(event_idx as _);
        println!("shutting down connection");
        // TODO graceful shutdown
        unsafe {
            libc::close(fd);
        }
    } else {
        let string = String::from_utf8_lossy(data);
        log::info!("RECIEVED: {string}");
    }
    Ok(())
}

fn event_loop(mut ring: IoUring, events: &mut Slab<Event>) -> io::Result<()> {
    let (submitter, mut sq, mut cq) = ring.split();

    let mut buf_ring = BufferRing::new(1, 64, 32).unwrap();
    buf_ring.register(&submitter).unwrap();

    let mut offset = 0;
    let mut file_b = MmapFile::create("file_b.txt", 3036).unwrap();

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
            if let Err(err) = event_handler(&mut sq, cqe, events, &mut buf_ring) {
                log::error!("Error handling event: {err}");
            }
        }
    }
}
