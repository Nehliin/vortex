use core::panic;
use std::{
    fs::OpenOptions,
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd},
    ptr, time::{Duration, Instant},
};

use buf_ring::BufferRing;
use file::MmapFile;
use io_uring::{
    opcode,
    types::{self, Timespec},
    IoUring,
};
use slab::Slab;

mod buf_ring;
mod file;

const TIMESPEC: &Timespec = &Timespec::new().sec(1);


#[derive(Debug, Clone)]
enum Operation {
    Accept,
    Recv {
        fd: RawFd,
        //buf_index: usize,
    },
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
    let token_idx = tokens.insert(Operation::Accept);

    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
        .build()
        .user_data(token_idx as _);

    unsafe {
        ring.submission().push(&accept_op).unwrap();
    }
    ring.submission().sync();

    // event loop
    event_loop(ring, &mut tokens)
}

// Validate hashes in here and simply use one shot channels
fn tick(last_tick: &Duration) {
    println!("Tick!: {}", last_tick.as_secs_f32());
}

fn event_loop(mut ring: IoUring, tokens: &mut Slab<Operation>) {
    let (submitter, mut sq, mut cq) = ring.split();

    let mut buf_ring = BufferRing::new(1, 64, 32).unwrap();
    buf_ring.register(&submitter).unwrap();

    let mut offset = 0;
    let mut file_b = MmapFile::create("file_b.txt", 3036).unwrap();

    let mut last_tick = Instant::now();
    loop {
        let args = types::SubmitArgs::new().timespec(TIMESPEC);
        //println!("Submit and wait");
        match submitter.submit_with_args(1, &args) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => println!("busy"),
            Err(ref err) if err.raw_os_error() == Some(libc::ETIME) => println!("TIMEOUT"),
            Err(err) => panic!("failed: {err}"),
        }
        cq.sync();
        if cq.overflow() > 0 {
            println!("overflow");
        }

        //loop {
        if sq.is_full() {
            match submitter.submit() {
                Ok(_) => (),
                Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => println!("busy"),
                Err(err) => panic!("failed: {err}"),
            }
        }
        sq.sync();
        //backlog
        //}
        
        if last_tick.elapsed() > Duration::from_secs(1) {
            tick(&last_tick.elapsed());
            last_tick = Instant::now();
        }
        

        for cqe in &mut cq {
            let ret = cqe.result();
            let token_idx = cqe.user_data();
            let token = &mut tokens[token_idx as usize];
            if ret < 0 {
                let err = std::io::Error::from_raw_os_error(-ret);
                println!("ERROR: {err}");
                if -ret == libc::ENOBUFS {
                    // Ran out of buffers!
                    if let Operation::Recv { fd } = token {
                        let read_op = opcode::RecvMulti::new(types::Fd(*fd), buf_ring.bgid())
                            .build()
                            .user_data(token_idx as _)
                            .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                        unsafe {
                            sq.push(&read_op).unwrap();
                        }
                    } else {
                        panic!("Not expected");
                    }
                }
                continue;
            }
            match token.clone() {
                Operation::Accept => {
                    println!("Accepted connection!");
                    let fd = ret;
                    let read_token = tokens.insert(Operation::Recv { fd });
                    let read_op = opcode::RecvMulti::new(types::Fd(fd), buf_ring.bgid())
                        .build()
                        .user_data(read_token as _)
                        .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                    unsafe {
                        sq.push(&read_op).unwrap();
                    }
                }
                Operation::Recv { fd } => {
                    let len = ret;
                    let bid = io_uring::cqueue::buffer_select(cqe.flags()).unwrap();
                    let is_more = io_uring::cqueue::more(cqe.flags());
                    if !is_more {
                        println!("No more, starting new recv");
                        // TODO? Return bids and or read the current buffer?
                        //buf_ring.return_bid(bid);
                        let read_op = opcode::RecvMulti::new(types::Fd(fd), buf_ring.bgid())
                            .build()
                            .user_data(token_idx as _)
                            .flags(io_uring::squeue::Flags::BUFFER_SELECT);
                        unsafe {
                            sq.push(&read_op).unwrap();
                        }
                    }

                    dbg!(len);
                    if len == 0 {
                        println!("READ 0");
                        buf_ring.return_bid(bid);
                        tokens.remove(token_idx as _);
                        println!("shutting down connection");
                        unsafe {
                            libc::close(fd);
                        }
                    } else {
                        dbg!(bid);
                        let len = len as usize;
                        let buffer = buf_ring.get(bid);
                        let string = String::from_utf8_lossy(&buffer[..len]);
                        println!("RECIEVED: {string}");
                        let file_buf = file_b.get_mut();
                        file_buf[offset..offset + len].copy_from_slice(&buffer[..len]);
                        offset += len;

                        buf_ring.return_bid(bid);
                    }
                }
            }
        }
    }
}
