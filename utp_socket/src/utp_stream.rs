use std::{
    cell::{Ref, RefCell, RefMut},
    net::SocketAddr,
    rc::Rc,
};

use crate::{
    reorder_buffer::ReorderBuffer,
    utp_packet::{get_microseconds, Packet, PacketType},
};

#[derive(Debug)]
pub(crate) enum ConnectionState {
    Idle,
    // TODO syn received to handle incoming traffic
    SynSent {
        connect_notifier: tokio::sync::oneshot::Sender<()>,
    },
    Connected,
    FinSent,
}

impl PartialEq for ConnectionState {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Eq for ConnectionState {}

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
    pub(crate) our_advertised_window: u32,
    pub(crate) their_advertised_window: u32,
    // Last delay measurement from other endpoint
    // whenever a packet is received this state is updated
    // by subtracting timestamp_microseconds from the host current time
    pub(crate) reply_micro: u32,
    // Last packet in sequence, taken from the FIN packet
    pub(crate) eof_pkt: Option<u16>,
    // The adder the stream is connected to
    pub(crate) addr: SocketAddr,
    // incoming buffer, used to reorder packets
    pub(crate) incoming_buffer: ReorderBuffer,
    // Receive buffer, used to store packet data before read requests
    // this is what's used to determine window size
    pub(crate) receive_buf: Vec<u8>,
}

#[derive(Clone)]
pub struct UtpStream {
    inner: Rc<RefCell<StreamState>>,
}

impl UtpStream {
    pub(crate) fn new(state: StreamState) -> Self {
        UtpStream {
            inner: Rc::new(RefCell::new(state)),
        }
    }

    pub(crate) fn process_incoming(&self, packet: Packet) -> anyhow::Result<bool> {
        let packet_header = packet.header;
        // Mismatching id
        if packet_header.conn_id != self.state().conn_id_recv {
            // sanity check
            assert!(
                packet_header.packet_type != PacketType::Syn,
                "Syn packets should be handled elsewhere"
            );
            anyhow::bail!(
                "Received invalid packet connection id: {}, expected: {}",
                packet_header.conn_id,
                self.state().conn_id_recv
            )
        }

        let dist_from_expected = {
            let mut state = self.state_mut();

            let syn_sent = matches!(state.connection_state, ConnectionState::SynSent { .. });

            // Sequence number used to check that the ack is valid.
            // If we receive an ack for a packet past this sequence number
            // we have received an ack for an unsent packet which is incorrect.
            // Syn is the first packet sent so no - 1 there and same goes for Fin I guess?
            //
            // TODO: move this to be part of the match or something
            // ALSO TODO: handle wrapping ack/seq nr.
            let cmp_seq_nr = if (syn_sent || state.connection_state == ConnectionState::FinSent)
                && packet_header.packet_type == PacketType::State
            {
                state.seq_nr
            } else {
                state.seq_nr - 1
            };

            if cmp_seq_nr < packet_header.ack_nr {
                anyhow::bail!("Incoming ack_nr was invalid");
            }

            // TODO: handle eof

            if syn_sent {
                // This must be a syn-ack and the state ack_nr is initialzied here
                // to match the seq_nr received from the other end since this is the first
                // nr of the connection. I suspect this is initialzied early because
                // packets may be received out of order.
                //
                // Ah yes the ack_nr just indicates that we've (since this is the state
                // ack_nr) acked up until and including the SYN
                // packet since we don't always start from 1
                state.ack_nr = packet_header.seq_nr - 1;
            }

            let their_delay = if packet_header.timestamp_microseconds == 0 {
                // I supose this is for incoming traffic that wants to open
                // new connections?
                0
            } else {
                let time = get_microseconds();
                time - packet_header.timestamp_microseconds as u64
            };
            state.reply_micro = their_delay as u32;
            state.their_advertised_window = packet_header.wnd_size;
            // The number of packets past the expected packet. Diff between acked
            // up until and current -1 gives 0 the meaning of this being the next
            // expected packet in the sequence.
            packet_header.seq_nr - state.ack_nr - 1
        };

        let need_to_ack = packet_header.packet_type == PacketType::Data
            || packet_header.packet_type == PacketType::Syn // TODO handled elsewhere
            || packet_header.packet_type == PacketType::Fin;

        if dist_from_expected != 0 {
            log::debug!("Got out of order packet");
            // Out of order packet
            self.state_mut().incoming_buffer.insert(packet);
            return Ok(need_to_ack);
        }

        self.handle_inorder_packet(packet);

        let mut seq_nr = packet_header.seq_nr;
        while let Some(packet) = self.state_mut().incoming_buffer.remove(seq_nr) {
            self.handle_inorder_packet(packet);
            seq_nr += 1;
        }
        Ok(need_to_ack)
    }

    fn handle_inorder_packet(&self, packet: Packet) {
        let mut state = self.state_mut();
        let addr = state.addr;
        let conn_state = std::mem::replace(&mut state.connection_state, ConnectionState::Idle);
        match (packet.header.packet_type, conn_state) {
            // Outgoing connection completion
            (PacketType::State, ConnectionState::SynSent { connect_notifier }) => {
                let mut state = self.state_mut();
                state.cur_window_packets -= 1;
                state.connection_state = ConnectionState::Connected;
                connect_notifier.send(()).unwrap();

                log::trace!("SYN_ACK");
            }
            (PacketType::State, conn_state) => {
                log::trace!("Received ACK: {}", addr);
                let mut state = self.state_mut();
                state.cur_window_packets -= 1;
                // Reset connection state if it wasn't modified
                state.connection_state = conn_state;
            }
            (PacketType::Data, conn_state) => {
                // in order packet
                {
                    let mut state = self.state_mut();
                    state.ack_nr += 1;
                }
                log::trace!("Sending ACK (almost)");
                // TODOOO
                // consume_incoming_data in libtorrent
                // basically moves bytes over to receive buf until it's filled
                // receive buf remaining space is our window
                //
                // Reset connection state if it wasn't modified
                state.connection_state = conn_state;
            }
            (PacketType::Fin, conn_state) => {
                log::trace!("Received FIN: {}", addr);
                let mut state = self.state_mut();
                state.eof_pkt = Some(packet.header.seq_nr);
                log::info!("Connection closed: {}", addr);

                // more stuff here
                //
                // Reset connection state if it wasn't modified
                state.connection_state = conn_state;
            }
            (p_type, conn_state) => {
                // READ bytes after header
                log::error!("Unhandled packet type!: {:?}", p_type);
                // Reset connection state if it wasn't modified
                state.connection_state = conn_state;
            }
        }
    }

    pub(crate) fn state_mut(&self) -> RefMut<'_, StreamState> {
        self.inner.borrow_mut()
    }

    pub(crate) fn state(&self) -> Ref<'_, StreamState> {
        self.inner.borrow()
    }
}
