use std::{
    cell::{Ref, RefCell, RefMut},
    net::SocketAddr,
    rc::Rc,
};

use crate::utp_socket::{get_microseconds, PacketHeader, PacketType};

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

    pub(crate) fn process_incoming(&self, packet: PacketHeader) -> anyhow::Result<()> {
        // TODO ignore packets who have invalid ack nr

        let (conn_state, dist_from_expected) = {
            let mut state = self.state_mut();
            if matches!(state.connection_state, ConnectionState::SynSent { .. }) {
                // This must be a syn-ack and the state ack_nr is initialzied here
                // to match the seq_nr received from the other end since this is the first
                // nr of the connection. I suspect this is initialzied early because
                // packets may be received out of order.
                //
                // Ah yes the ack_nr just indicates that we've (since this is the state
                // ack_nr) acked up until the SYN
                // packet since we don't always start from 1
                state.ack_nr = packet.seq_nr - 1;
            }

            let their_delay = if packet.timestamp_microseconds == 0 {
                // I supose this is for incoming traffic that wants to open
                // new connections?
                0
            } else {
                let time = get_microseconds();
                time - packet.timestamp_microseconds as u64
            };
            state.reply_micro = their_delay as u32;
            // The number of packets past the expected packet. Diff between acked
            // up until and current -1 gives 0 the meaning of this being the next
            // expected packet in the sequence.
            let dist_from_expected = packet.seq_nr - state.ack_nr - 1;
            (
                std::mem::replace(&mut state.connection_state, ConnectionState::Idle),
                dist_from_expected,
            )
        };

        let addr = self.state().addr;

        match (packet.packet_type, conn_state) {
            // Outgoing connection completion
            (PacketType::State, ConnectionState::SynSent { connect_notifier }) => {
                log::trace!("Packet dist_from_expected: {dist_from_expected}");
                let mut state = self.state_mut();
                state.cur_window_packets -= 1;
                state.connection_state = ConnectionState::Connected;
                connect_notifier.send(()).unwrap();

                if dist_from_expected == 0 {
                    log::trace!("SYN_ACK");
                } else {
                    // out of order packets we can't handle yet
                }
            }
            (PacketType::State, _) => {
                log::trace!("Packet dist_from_expected: {dist_from_expected}");
                log::trace!("Received ACK: {}", addr);
                let mut state = self.state_mut();
                state.cur_window_packets -= 1;
            }
            (PacketType::Data, _) => {
                log::trace!("Packet dist_from_expected: {dist_from_expected}");
                if dist_from_expected == 0 {
                    // in order packet
                    {
                        let mut state = self.state_mut();
                        state.ack_nr += 1;
                    }
                    log::trace!("Sending ACK (almost)");
                    // TODOOO
                    //stream.ack(addr).await.unwrap();
                } else {
                    // out of order packets we can't handle yet
                }
            }
            (PacketType::Fin, _) => {
                log::trace!("Received FIN: {}", addr);
                let mut state = self.state_mut();
                state.eof_pkt = Some(packet.seq_nr);
                if dist_from_expected == 0 {
                    log::info!("Connection closed: {}", addr);
                } else {
                    // TODO handle out of order packets
                    log::warn!("Received FIN out of order, packets will be lost");
                }
            }
            _ => {
                // READ bytes after header
                log::error!("Unhandled packet type!: {:?}", packet.packet_type);
            }
        }
        Ok(())
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
