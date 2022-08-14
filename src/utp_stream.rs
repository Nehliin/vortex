use std::{
    cell::{Ref, RefCell, RefMut},
    net::SocketAddr,
    rc::Rc,
};

#[derive(Debug)]
pub(crate) enum ConnectionState {
    Idle,
    // TODO syn received to handle incoming traffic
    SynSent {
        connect_notifier: tokio::sync::oneshot::Sender<()>,
    },
    Connected,
}

impl PartialEq for ConnectionState {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Eq for ConnectionState {}

#[derive(Debug)]
pub(crate) struct StreamState {
    // Current socket state
    pub(crate) connection_state: ConnectionState,
    // Sequence number for next packet to be sent
    pub(crate) seq_nr: u16,
    // All sequence numbers up until and including this which have been
    // properly recived
    pub(crate) ack_nr: u16,
    // Connection id for packets I receive
    pub(crate) conn_id_recv: u16,
    // Connection id for packets I send
    pub(crate) conn_id_send: u16,
    // Current amount of packets sent but not acked
    pub(crate) cur_window_packets: u16,
    // Last received window this socket advertised in bytes
    pub(crate) last_recv_window: u32,
    // Last delay measurement from other endpoint
    // whenever a packet is received this state is updated
    // by subtracting timestamp_microseconds from the host current time
    pub(crate) reply_micro: u32,
    // Last packet in sequence, taken from the FIN packet
    pub(crate) eof_pkt: Option<u16>,
    // The adder the stream is connected to
    pub(crate) addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct UtpStream {
    inner: Rc<RefCell<StreamState>>,
}

impl UtpStream {
    pub(crate) fn new(state: StreamState) -> Self {
        UtpStream {
            inner: Rc::new(RefCell::new(state)),
        }
    }
}

impl UtpStream {
    pub(crate) fn state_mut(&self) -> RefMut<'_, StreamState> {
        self.inner.borrow_mut()
    }

    pub(crate) fn state(&self) -> Ref<'_, StreamState> {
        self.inner.borrow()
    }
}
