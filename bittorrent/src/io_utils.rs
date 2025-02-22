use std::{collections::VecDeque, io, os::fd::RawFd, ptr::null_mut};

use io_uring::{
    Submitter, opcode,
    squeue::PushError,
    types::{self, CancelBuilder, Timespec},
};
use slab::Slab;

use crate::{buf_ring::Bgid, event_loop::EventType};

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

#[allow(clippy::too_many_arguments)]
pub fn write_to_connection<Q: SubmissionQueue>(
    conn_id: usize,
    fd: RawFd,
    events: &mut Slab<EventType>,
    sq: &mut BackloggedSubmissionQueue<Q>,
    buffer_index: usize,
    buffer: &[u8],
    ordered: bool,
) {
    let event = events.insert(EventType::ConnectedWrite {
        connection_idx: conn_id,
    });
    let user_data = UserData::new(event, Some(buffer_index));
    let flags = if ordered {
        io_uring::squeue::Flags::IO_LINK
    } else {
        io_uring::squeue::Flags::empty()
    };
    let write_op = opcode::Write::new(types::Fd(fd), buffer.as_ptr(), buffer.len() as u32)
        .build()
        .user_data(user_data.as_u64())
        .flags(flags);
    sq.push(write_op);
}

// Cancelles all operations on the socket
pub fn stop_connection<Q: SubmissionQueue>(
    sq: &mut BackloggedSubmissionQueue<Q>,
    conn_id: usize,
    fd: RawFd,
    events: &mut Slab<EventType>,
) {
    let event = events.insert(EventType::ConnectionStopped {
        connection_idx: conn_id,
    });
    let user_data = UserData::new(event, None);
    let cancel_op = opcode::AsyncCancel2::new(CancelBuilder::fd(types::Fd(fd)).all())
        .build()
        .user_data(user_data.as_u64());
    sq.push(cancel_op);
}

pub fn recv<Q: SubmissionQueue>(
    sq: &mut BackloggedSubmissionQueue<Q>,
    user_data: UserData,
    fd: RawFd,
    bgid: Bgid,
    timeout_secs: u64,
) {
    log::debug!("Starting recv");
    let read_op = opcode::Recv::new(types::Fd(fd), null_mut(), 0)
        .buf_group(bgid)
        .build()
        .user_data(user_data.as_u64())
        .flags(io_uring::squeue::Flags::BUFFER_SELECT | io_uring::squeue::Flags::IO_LINK);

    let timeout = Timespec::new().sec(timeout_secs);
    let user_data = UserData::new(user_data.event_idx as _, None);
    let timeout_op = opcode::LinkTimeout::new(&timeout)
        .build()
        .user_data(user_data.as_u64());
    // If the queue doesn't fit both events they need
    // to be sent to the backlog so they can be submitted
    // together and not with a arbitrary delay inbetween.
    // That would mess up the timeout
    if sq.remaining() >= 2 {
        sq.push(read_op);
        sq.push(timeout_op);
        // Need to sync so timeout isn't dropped prematurely?
        sq.sync();
    } else {
        sq.push_backlog(read_op);
        sq.push_backlog(timeout_op);
    }
}

pub fn recv_multishot<Q: SubmissionQueue>(
    sq: &mut BackloggedSubmissionQueue<Q>,
    user_data: UserData,
    fd: RawFd,
    bgid: Bgid,
) {
    log::debug!("Starting recv multishot");
    let read_op = opcode::RecvMulti::new(types::Fd(fd), bgid)
        .build()
        .user_data(user_data.as_u64())
        .flags(io_uring::squeue::Flags::BUFFER_SELECT);
    sq.push(read_op);
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct UserData {
    pub buffer_idx: Option<u32>,
    pub event_idx: u32,
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
