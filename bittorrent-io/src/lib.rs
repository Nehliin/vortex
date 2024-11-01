use std::{
    net::{SocketAddr, TcpListener},
    os::fd::{AsRawFd, RawFd}, ptr,
};

use io_uring::{
    cqueue::Entry,
    opcode,
    types::{self, SubmitArgs, Timespec},
    IoUring,
};
use slab::Slab;

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct Userdata {
    op_tid: u16,
    bid: u16,
    fd: u16,
}

const OP_SHIFT: usize = 12;

#[repr(C)]
union UserdataUnion {
    data: Userdata,
    val: u64,
}

impl UserdataUnion {
    fn new(tid: u16, op: Operation, bid: u16, fd: RawFd) -> Self {
        Self {
            data: Userdata {
                op_tid: ((op as u16) << OP_SHIFT) | tid,
                bid,
                fd: fd.try_into().unwrap(),
            },
        }
    }
    fn op(&self) -> Operation {
        unsafe {
            match self.data.op_tid >> OP_SHIFT {
                1 => Operation::Accept,
                _ => unreachable!(),
            }
        }
    }

    fn bid(&self) -> u16 {
        unsafe { self.data.bid }
    }

    fn fd(&self) -> usize {
        unsafe { self.data.fd as usize }
    }
}

impl Into<u64> for UserdataUnion {
    fn into(self) -> u64 {
        unsafe { self.val }
    }
}

#[repr(u16)]
enum Operation {
    Accept = 1,
}

pub fn setup_listener() {
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .setup_clamp()
        .setup_cqsize(1024)
        .setup_defer_taskrun()
        .build(1024)
        .unwrap();

    let mut nr_conn = 0;
    //let (submitter, mut sq, mut cq) = ring.split();

    let listener = TcpListener::bind(("127.0.0.1", 3456)).unwrap();
    let accept_op = opcode::AcceptMulti::new(types::Fd(listener.as_raw_fd()))
        .build()
        .user_data(UserdataUnion::new(0, Operation::Accept, 0, listener.as_raw_fd()).into());

    unsafe {
        ring.submission().push(&accept_op).unwrap();
    }
    ring.submission().sync();

    // event loop
    event_loop(ring, &mut nr_conn)
}

fn event_loop(mut ring: IoUring, nr_conn: &mut i32) {
    let (submitter, mut sq, mut cq) = ring.split();
    loop {
        println!("Submit and wait");
        let sigmask: libc::sigset_t = unsafe { std::mem::zeroed() };
        let mut args = SubmitArgs::new();
        let ts = Timespec::new().sec(0).nsec(100000000);
        args = args.timespec(&ts);
        args = args.sigmask(&sigmask);
        match submitter.submit_with_args(1, &args) {
            Ok(_) => (),
            Err(ref err) if err.raw_os_error() == Some(libc::EBUSY) => println!("busy"),
            Err(_) => panic!("failed"),
        }
        cq.sync();
        if cq.overflow() > 0 {
            println!("overflow");
        }

        for cqe in &mut cq {
            let ret = cqe.result();
            if ret < 0 {
                println!("ERROr");
                continue;
            }
            handle_event(&cqe, nr_conn);
        }
    }
}


/*struct ConnectionDir {
    index: i32,
    pending_shutdown: bool,
    pending_send: i32,
    pending_recv: i32,
    snd_notif: i32,
    out_buffers: i32,
    rcv: i32,
    rcv_shrt: i32,
    rcv_enobufs: i32,
    rcv_mshot: i32,
    snd: i32,
    snd_shrt: i32,
    snd_enobufs: i32,
    snd_busy: i32,
    snd_mshot: i32,

    snd_next_bid: i32,
    rcv_next_bid: i32,

    rcv_bucket: i32,
    snd_bucket: i32,

    io_rcv_msg: IoMsg,
    io_snd_msg: IoMsg,
}*/


struct BufferPool {
    free: Vec<usize>,
    allocated_buffers: Slab<Box<[u8]>>,
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

fn handle_event(cqe: &Entry, nr_conn: &mut i32, cur_bgid: &mut i32) {
    let user_data = unsafe { std::mem::transmute::<_, UserdataUnion>(cqe.user_data()) };
    //dbg!(user_data);

    match user_data.op() {
        Operation::Accept => {


            
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
