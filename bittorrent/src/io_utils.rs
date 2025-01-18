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
) {
    log::debug!("Starting recv");
    let read_op = opcode::Recv::new(types::Fd(fd), null_mut(), 0)
        .buf_group(bgid)
        .build()
        .user_data(user_data.as_u64())
        .flags(io_uring::squeue::Flags::BUFFER_SELECT);
    sq.push(read_op);
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
