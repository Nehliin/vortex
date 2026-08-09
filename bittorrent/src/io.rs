use std::{
    collections::VecDeque,
    io,
    os::fd::{IntoRawFd, RawFd},
    ptr::null_mut,
};

use io_uring::{
    Submitter,
    opcode::{self},
    squeue::{Flags, PushError},
    types::{self, CancelBuilder, Timespec},
};
use slotmap::{Key, SlotMap};
use socket2::{SockAddr, Socket};

use crate::{
    buf_pool::{Buffer, BufferPool},
    buf_ring::BufferRing,
    connection_manager::ConnectionId,
    event_loop::{EventData, EventId, EventType},
    file_store::{DiskOp, DiskOpType},
    torrent::Config,
};

const CONNECT_TIMEOUT: Timespec = Timespec::new().sec(10);

pub trait SubmissionQueue {
    fn sync(&mut self);
    fn capacity(&self) -> usize;
    fn len(&self) -> usize;
    fn is_full(&self) -> bool;
    unsafe fn push(&mut self, entry: &io_uring::squeue::Entry) -> Result<(), PushError>;
}

impl SubmissionQueue for io_uring::SubmissionQueue<'_> {
    fn sync(&mut self) {
        self.sync();
    }

    fn capacity(&self) -> usize {
        self.capacity()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }

    unsafe fn push(&mut self, entry: &io_uring::squeue::Entry) -> Result<(), PushError> {
        unsafe { self.push(entry) }
    }
}

pub struct BackloggedSubmissionQueue<Q> {
    queue: Q,
    backlog: VecDeque<io_uring::squeue::Entry>,
}

impl<Q: SubmissionQueue> BackloggedSubmissionQueue<Q> {
    pub fn new(queue: Q) -> Self {
        Self {
            queue,
            backlog: Default::default(),
        }
    }

    // TODO: consider making unsafe
    /// Push into the queue or the backlog if it's full
    pub fn push(&mut self, entry: io_uring::squeue::Entry) {
        unsafe {
            if self.queue.push(&entry).is_err() {
                log::warn!("SQ buffer full, pushing to backlog");
                self.backlog.push_back(entry);
            }
        }
    }

    /// Push directly into the backlog
    pub fn push_backlog(&mut self, entry: io_uring::squeue::Entry) {
        self.backlog.push_back(entry);
    }

    pub fn sync(&mut self) {
        self.queue.sync();
    }

    /// Returns remaining space in the queue before new entries
    /// needs to be pushed to the backlog
    pub fn remaining(&self) -> usize {
        self.queue.capacity() - self.queue.len()
    }

    pub fn submit_and_drain_backlog(&mut self, submitter: &Submitter<'_>) -> io::Result<()> {
        loop {
            if self.queue.is_full() {
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
            self.queue.sync();
            if self.backlog.is_empty() {
                break Ok(());
            }
            let sq_remaining_capacity = self.remaining();
            let num_to_drain = self.backlog.len().min(sq_remaining_capacity);
            for sqe in self.backlog.drain(..num_to_drain) {
                unsafe {
                    self.queue
                        .push(&sqe)
                        .expect("SQE should never be full when clearing backlog")
                }
            }
        }
    }
}

pub struct Io<Q> {
    pub sq: BackloggedSubmissionQueue<Q>,
    pub events: SlotMap<EventId, EventData>,
    pub write_pool: BufferPool,
    pub read_ring: BufferRing,
    /// How many file operations are inflight in the kernel
    pub inflight_disk_ops: usize,
    /// The queued up disk operations. Owned here rather than by the
    /// FileStore so that FileStore can remain Send
    pub queued_disk_operations: Vec<DiskOp>,
}

impl<Q: SubmissionQueue> Io<Q> {
    pub fn new(sq: BackloggedSubmissionQueue<Q>, config: &Config) -> Self {
        Self {
            sq,
            events: SlotMap::with_capacity_and_key(config.cq_size as usize),
            write_pool: BufferPool::new(
                "network_write",
                config.write_buffer_pool_size,
                config.network_write_buffer_size,
            ),
            read_ring: BufferRing::new(
                1,
                config.read_buffer_pool_size,
                config.network_read_buffer_size,
            )
            .unwrap(),
            inflight_disk_ops: 0,
            queued_disk_operations: Vec::with_capacity(32),
        }
    }

    /// Schedule a connect operation with an attached timeout. The socket stays
    /// owned by the connection manager entry.
    pub fn connect(&mut self, conn_id: ConnectionId, fd: RawFd, addr: SockAddr) {
        let event_idx = self.events.insert(EventData {
            typ: EventType::Connect {
                connection_idx: conn_id,
                addr: Box::new(addr),
            },
            buffers: None,
        });

        let EventType::Connect { addr, .. } = &self.events[event_idx].typ else {
            unreachable!();
        };

        let connect_op = opcode::Connect::new(types::Fd(fd), addr.as_ptr().cast(), addr.len())
            .build()
            .flags(Flags::IO_LINK)
            .user_data(event_idx.data().as_ffi());
        let timeout_op = opcode::LinkTimeout::new(&CONNECT_TIMEOUT)
            .build()
            .user_data(event_idx.data().as_ffi());
        // If the queue doesn't fit both events they need
        // to be sent to the backlog so they can be submitted
        // together and not with a arbitrary delay inbetween.
        // That would mess up the timeout
        if self.sq.remaining() >= 2 {
            self.sq.push(connect_op);
            self.sq.push(timeout_op);
        } else {
            self.sq.push_backlog(connect_op);
            self.sq.push_backlog(timeout_op);
        }
    }

    /// Write to an unestablished (from a bittorrent perspective) connection
    pub fn write(&mut self, conn_id: ConnectionId, fd: RawFd, buffer: Buffer) {
        let buffer_slice = buffer.filled_slice();
        let buffer_ptr = buffer_slice.as_ptr();
        let buffer_len = buffer_slice.len();
        let write_event_id = self.events.insert(EventData {
            typ: EventType::Write {
                connection_idx: conn_id,
                expected_write: buffer_len,
            },
            buffers: Some(vec![buffer]),
        });
        let write_op = opcode::Write::new(types::Fd(fd), buffer_ptr, buffer_len as u32)
            .build()
            .user_data(write_event_id.data().as_ffi());
        self.sq.push(write_op);
    }

    /// Single-shot recv with an attached timeout
    pub fn recv(&mut self, event_data_idx: EventId, fd: RawFd, timeout: &Timespec) {
        log::debug!("Starting recv");
        let read_op = opcode::Recv::new(types::Fd(fd), null_mut(), 0)
            .buf_group(self.read_ring.bgid())
            .build()
            .user_data(event_data_idx.data().as_ffi())
            .flags(Flags::BUFFER_SELECT | Flags::IO_LINK);

        let timeout_op = opcode::LinkTimeout::new(timeout)
            .build()
            .user_data(event_data_idx.data().as_ffi());
        // If the queue doesn't fit both events they need
        // to be sent to the backlog so they can be submitted
        // together and not with a arbitrary delay inbetween.
        // That would mess up the timeout
        if self.sq.remaining() >= 2 {
            self.sq.push(read_op);
            self.sq.push(timeout_op);
            // Need to sync so timeout isn't dropped prematurely?
            self.sq.sync();
        } else {
            self.sq.push_backlog(read_op);
            self.sq.push_backlog(timeout_op);
        }
    }

    pub fn recv_multishot(&mut self, event_data_idx: EventId, fd: RawFd) {
        log::debug!("Starting recv multishot: {event_data_idx:?}");
        let read_op = opcode::RecvMulti::new(types::Fd(fd), self.read_ring.bgid())
            .build()
            .user_data(event_data_idx.data().as_ffi())
            .flags(Flags::BUFFER_SELECT);
        self.sq.push(read_op);
    }

    /// Writes the buffers from buffer_offset -> buffer end to the connection
    pub fn writev_to_connection(
        &mut self,
        conn_id: ConnectionId,
        fd: RawFd,
        buffers: Vec<Buffer>,
        // Offset in the buffer the write should start from
        io_vec_offset: usize,
    ) {
        debug_assert!(io_vec_offset <= buffers.iter().map(|buf| buf.filled_slice().len()).sum());
        let mut remaining_offset = io_vec_offset as i64;
        let iovecs: Vec<libc::iovec> = buffers
            .iter()
            .map(|buf| buf.filled_slice())
            .filter_map(|buf| {
                // Skip buffers that end before the offset
                // if the offset becomes negative we know the offset is inside of
                // the given buffer
                remaining_offset -= buf.len() as i64;
                if remaining_offset < 0 {
                    // How much of the buffer wasn't skipped = remaining data in
                    // the buffer
                    let relevant_buffer_length = (-remaining_offset) as usize;
                    // Gives where in the buffer the write should start from
                    let buffer_offset = buf.len() - relevant_buffer_length;
                    let io_vec = libc::iovec {
                        iov_base: unsafe { buf.as_ptr().add(buffer_offset) as *mut _ },
                        iov_len: relevant_buffer_length,
                    };
                    // Reset so all other buffers are fully included
                    remaining_offset = 0;
                    Some(io_vec)
                } else {
                    None
                }
            })
            .collect();

        let iovecs_len = iovecs.len();

        let event_id = self.events.insert(EventData {
            typ: EventType::ConnectedWriteV {
                connection_idx: conn_id,
                iovecs,
                io_vec_offset,
            },
            buffers: Some(buffers),
        });

        // Need a stable pointer to the iovec structure, that means
        // the pointer after it's inserted in the event structure
        let stable_iovec_ptr = match &self.events[event_id].typ {
            EventType::ConnectedWriteV { iovecs, .. } => iovecs.as_ptr(),
            _ => unreachable!(),
        };
        let write_op = opcode::Writev::new(types::Fd(fd), stable_iovec_ptr, iovecs_len as u32)
            .build()
            .user_data(event_id.data().as_ffi());
        self.sq.push(write_op);
    }

    pub fn disk_operation(&mut self, disk_op: DiskOp) {
        let op = match disk_op.op_type {
            DiskOpType::Write => {
                let write_ptr = unsafe {
                    disk_op
                        .buffer
                        .raw_slice()
                        .as_ptr()
                        .add(disk_op.buffer_offset)
                };
                let write_len = disk_op.operation_len;
                let event_id = self.events.insert(EventData {
                    typ: EventType::DiskWrite {
                        data: disk_op.buffer,
                        piece_idx: disk_op.piece_idx,
                        #[cfg(feature = "metrics")]
                        scheduled: std::time::Instant::now(),
                    },
                    buffers: None,
                });
                opcode::Write::new(types::Fd(disk_op.fd), write_ptr, write_len as u32)
                    .offset(disk_op.file_offset as u64)
                    .build()
                    .user_data(event_id.data().as_ffi())
            }
            DiskOpType::Read {
                connection_idx,
                piece_offset,
            } => {
                let read_ptr = unsafe {
                    disk_op
                        .buffer
                        .raw_slice()
                        .as_ptr()
                        .add(disk_op.buffer_offset)
                };
                let read_len = disk_op.operation_len;
                let event_id = self.events.insert(EventData {
                    typ: EventType::DiskRead {
                        data: disk_op.buffer,
                        piece_idx: disk_op.piece_idx,
                        connection_idx,
                        piece_offset,
                        #[cfg(feature = "metrics")]
                        scheduled: std::time::Instant::now(),
                    },
                    // TODO: consider using this instead
                    buffers: None,
                });
                opcode::Read::new(types::Fd(disk_op.fd), read_ptr as *mut _, read_len as u32)
                    .offset(disk_op.file_offset as u64)
                    .build()
                    .user_data(event_id.data().as_ffi())
            }
        };
        self.inflight_disk_ops += 1;
        self.sq.push(op);
    }

    /// Schedule all queued up disk operations
    pub fn submit_queued_disk_operations(&mut self) {
        // Swapped out and back to allow &mut self methods in the loop
        // whilst keeping the allocation
        let mut queued = std::mem::take(&mut self.queued_disk_operations);
        for disk_op in queued.drain(..) {
            self.disk_operation(disk_op);
        }
        self.queued_disk_operations = queued;
    }

    // NOTE: Socket contains an OwnedFd which automatically closes
    // the file descriptor in a blocking fashion upon dropping it.
    // That's great for a fallback since closing sockets should rarely block
    // and be fast enough. But to keep the io operations consistent I want to close
    // the socket the io_uring way which means transferring the ownership via `into_raw_fd`
    //
    // It is important that this function takes ownership of the socket, that should prevent
    // issues with closing the socket multiple times. For connected sockets, ownership can only
    // be provided after they have been removed from the `connections` slab. Freestanding
    // Connect/Write/Read all pass along the socket which means there should never exist
    // two separate events with the same socket meaning the socket can ONLY be closed once.
    pub fn close_socket(&mut self, socket: Socket, maybe_connection_idx: Option<ConnectionId>) {
        let fd = socket.into_raw_fd();
        // If more events are received in the same cqe loop there might still linger events
        // that have been removed due to a earlier event in the loop causing the socket to close
        self.cancel(
            CancelBuilder::fd(types::Fd(fd)).all(),
            // IO_HARDLINK ensures the cancel is guaranteed
            // to happen before the close. Without this we might
            // see ENOENT errors in the event loop due to the socket
            // closing before the cancel happens. This will result in
            // a panic in the event loop which only "accept" ECANCELED.
            // We could accept ENOENT as well but it's cleaner to cancel -> close.
            // IO_HARDLINK is also used instead of IO_LINK to ensure we always
            // close the socket regardless if the the cancel fails or not.
            // (This might be overly paranoid but whatever)
            Some(Flags::IO_HARDLINK),
        );
        let event_id = self.events.insert(EventData {
            typ: EventType::Close {
                maybe_connection_idx,
            },
            buffers: None,
        });
        let close_op = opcode::Close::new(types::Fd(fd))
            .build()
            .user_data(event_id.data().as_ffi());
        self.sq.push(close_op);
    }

    pub fn cancel(&mut self, cancel_builder: CancelBuilder, flags: Option<Flags>) {
        let event_id = self.events.insert(EventData {
            typ: EventType::Cancel,
            buffers: None,
        });
        let cancel_op = opcode::AsyncCancel2::new(cancel_builder)
            .build()
            .user_data(event_id.data().as_ffi());
        let cancel_op = if let Some(flags) = flags {
            cancel_op.flags(flags)
        } else {
            cancel_op
        };
        self.sq.push(cancel_op);
    }
}
