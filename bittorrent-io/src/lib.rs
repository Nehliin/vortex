use std::{
    fs::OpenOptions,
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd},
    ptr,
};

use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, OpenHow, SubmitArgs, Timespec},
    IoUring,
};
use slab::Slab;

//const OPEN_HOW: OpenHow = OpenHow::new().flags((libc::O_CREAT | libc::O_RDWR) as u64);
/* if !already_created {
    let open_token = tokens.insert(Operation::Open {});
    let open_op = opcode::OpenAt2::new(
        types::Fd(libc::AT_FDCWD),
        "file_b.txt".as_ptr() as _,
        &OPEN_HOW as _,
    )
    .build()
    .user_data(open_token as _);
    unsafe {
        sq.push(&open_op).unwrap();
    }
    // good enugh
    already_created = true;
}*/

struct BufferPool {
    free: Vec<usize>,
    allocated_buffers: Slab<Box<[u8]>>,
}

impl BufferPool {
    fn get_buffer(&mut self) -> (usize, &mut [u8]) {
        match self.free.pop() {
            Some(free_idx) => (free_idx, &mut self.allocated_buffers[free_idx]),
            None => {
                let buf = vec![0u8; 2048].into_boxed_slice();
                let buf_entry = self.allocated_buffers.vacant_entry();
                let buf_index = buf_entry.key();
                (buf_index, buf_entry.insert(buf))
            }
        }
    }

    fn return_buffer(&mut self, index: usize) {
        self.free.push(index);
    }
}

#[derive(Debug, Clone)]
enum Operation {
    Accept,
    Recv {
        fd: RawFd,
        buf_index: usize,
    },
    Read {
        fd: RawFd,
        buf_index: usize,
    },
    Write {
        fd: RawFd,
        buf_index: usize,
        offset: usize,
        len: usize,
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

    let mut buffer_pool = BufferPool {
        free: Vec::with_capacity(64),
        allocated_buffers: Slab::with_capacity(64),
    };

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
    event_loop(ring, &mut tokens, &mut buffer_pool)
}

fn event_loop(mut ring: IoUring, tokens: &mut Slab<Operation>, buffer_pool: &mut BufferPool) {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open("file_b.txt")
        .unwrap();
    let write_fd = file.as_raw_fd();
    let (submitter, mut sq, mut cq) = ring.split();
    /*let sigmask: libc::sigset_t = unsafe { std::mem::zeroed() };
    let mut args = SubmitArgs::new();
    let ts = Timespec::new().sec(1);
    args = args.timespec(&ts);
    args = args.sigmask(&sigmask);*/
    let mut offset = 0;
    loop {
        println!("Submit and wait");
        match submitter.submit_and_wait(1) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => println!("busy"),
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

        for cqe in &mut cq {
            let ret = cqe.result();
            if ret < 0 {
                println!("ERROR");
                continue;
            }
            let token_idx = cqe.user_data();
            let token = &mut tokens[token_idx as usize];
            match token.clone() {
                Operation::Accept => {
                    println!("Accepted connection!");
                    let fd = ret;
                    let (buf_idx, buffer) = buffer_pool.get_buffer();
                    let read_token = tokens.insert(Operation::Recv {
                        fd,
                        buf_index: buf_idx,
                    });
                    let read_op =
                        opcode::Recv::new(types::Fd(fd), buffer.as_mut_ptr(), buffer.len() as _)
                            .build()
                            .user_data(read_token as _);
                    unsafe {
                        sq.push(&read_op).unwrap();
                    }
                }
                Operation::Recv { fd, buf_index } => {
                    let len = ret;
                    if len == 0 {
                        println!("READ 0");
                        buffer_pool.return_buffer(buf_index);
                        tokens.remove(token_idx as _);
                        println!("shutting down connection");
                        unsafe {
                            libc::close(fd);
                        }
                    } else {
                        let buffer = &mut buffer_pool.allocated_buffers[buf_index];
                        let string = String::from_utf8_lossy(&buffer[..len as usize]);
                        println!("RECIEVED: {string}");
                        dbg!(offset);
                        *token = Operation::Write {
                            fd: write_fd,
                            buf_index,
                            offset,
                            len: len as usize,
                        };
                        let write_op = opcode::Write::new(
                            types::Fd(write_fd),
                            buffer.as_mut_ptr(),
                            len as u32,
                        )
                        .offset(offset as u64)
                        .build()
                        .user_data(token_idx as _);
                        offset += len as usize;
                        unsafe {
                            sq.push(&write_op).unwrap();
                        }
                        // new read
                        let (buf_idx, buffer) = buffer_pool.get_buffer();
                        let read_token = tokens.insert(Operation::Recv {
                            fd,
                            buf_index: buf_idx,
                        });
                        let read_op = opcode::Recv::new(
                            types::Fd(fd),
                            buffer.as_mut_ptr(),
                            buffer.len() as _,
                        )
                        .build()
                        .user_data(read_token as _);
                        unsafe {
                            sq.push(&read_op).unwrap();
                        }
                    }
                }
                Operation::Read { fd, buf_index } => {
                    let fd = ret;
                }
                Operation::Write {
                    fd,
                    buf_index,
                    offset,
                    len,
                } => {
                    let written = ret;
                    println!("Written: {written}");
                    buffer_pool.return_buffer(buf_index);
                    tokens.remove(token_idx as _);
                }
            }
        }
    }
}
