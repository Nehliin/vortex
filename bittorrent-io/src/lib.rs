use std::{
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd},
    ptr,
};

use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, SubmitArgs, Timespec},
    IoUring,
};
use slab::Slab;

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
    Read { fd: RawFd, buf_index: usize },
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
    //let (submitter, mut sq, mut cq) = ring.split();

    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    let accept_op = opcode::Accept::new(
        types::Fd(listener.as_raw_fd()),
        ptr::null_mut(),
        ptr::null_mut(),
    )
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
    let (submitter, mut sq, mut cq) = ring.split();
    /*let sigmask: libc::sigset_t = unsafe { std::mem::zeroed() };
    let mut args = SubmitArgs::new();
    let ts = Timespec::new().sec(1);
    args = args.timespec(&ts);
    args = args.sigmask(&sigmask);*/
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
                    let read_token = tokens.insert(Operation::Read {
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
                Operation::Read { fd, buf_index } => {
                    let len = ret;
                    if len == 0 {
                        buffer_pool.return_buffer(buf_index);
                        tokens.remove(token_idx as _);
                        println!("shutting down connection");
                        unsafe {
                            libc::close(fd);
                        }
                    } else {
                        let buffer = &mut buffer_pool.allocated_buffers[buf_index];
                        let string = String::from_utf8_lossy(buffer.as_ref());
                        println!("RECIEVED: {string}")
                    }
                }
            }
        }
    }
}

struct Connection {
    // ring if multi threaded?
    //in_br: ConnectionBufferRing,
    //out_br: ConnectionBufferRing,
    tid: u16,
    in_fd: i32,
    flags: i32,
    addr: SocketAddr,
    //conn_dir: [ConnectionDir; 2],
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn it_works() {}
}
