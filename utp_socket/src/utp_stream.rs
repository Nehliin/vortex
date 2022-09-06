use std::{
    cell::{Ref, RefCell, RefMut},
    net::SocketAddr,
    rc::{Rc, Weak},
    time::Duration,
};

use bytes::Bytes;
use tokio::sync::{oneshot::Receiver, Notify};
use tokio_uring::net::UdpSocket;

use crate::{
    reorder_buffer::ReorderBuffer,
    utp_packet::{get_microseconds, Packet, PacketHeader, PacketType},
};

#[derive(Debug)]
pub(crate) enum ConnectionState {
    Idle,
    SynReceived,
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

// Could be moved to separate module
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
    pub(crate) our_advertised_window: u32,
    pub(crate) their_advertised_window: u32,
    // Last delay measurement from other endpoint
    // whenever a packet is received this state is updated
    // by subtracting timestamp_microseconds from the host current time
    pub(crate) reply_micro: u32,
    // Last packet in sequence, taken from the FIN packet
    pub(crate) eof_pkt: Option<u16>,
    // incoming buffer, used to reorder packets
    pub(crate) incoming_buffer: ReorderBuffer,
    // outgoing buffer (TODO does this need to be an ReorderBuffer?)
    pub(crate) outgoing_buffer: ReorderBuffer,
    // Receive buffer, used to store packet data before read requests
    // this is what's used to determine window size.
    // Have the same size like the initial our_advertised_window
    pub(crate) receive_buf: Box<[u8]>,
    receive_buf_cursor: usize,

    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
}

impl StreamState {
    fn syn_header(&mut self) -> (PacketHeader, Receiver<()>) {
        let (tx, rc) = tokio::sync::oneshot::channel();
        // move to state method
        self.connection_state = ConnectionState::SynSent {
            connect_notifier: tx,
        };

        let header = PacketHeader {
            seq_nr: self.seq_nr,
            ack_nr: 0,
            conn_id: self.conn_id_recv,
            packet_type: PacketType::Syn,
            timestamp_microseconds: get_microseconds() as u32,
            timestamp_difference_microseconds: self.reply_micro,
            wnd_size: self.our_advertised_window,
            extension: 0,
        };
        (header, rc)
    }

    fn ack(&self) -> PacketHeader {
        // Move this closer to send time?
        let timestamp_microseconds = get_microseconds();
        PacketHeader {
            seq_nr: self.seq_nr,
            ack_nr: self.ack_nr,
            conn_id: self.conn_id_send,
            packet_type: PacketType::State,
            timestamp_microseconds: timestamp_microseconds as u32,
            timestamp_difference_microseconds: self.reply_micro,
            wnd_size: self.our_advertised_window,
            extension: 0,
        }
    }

    fn data(&mut self) -> PacketHeader {
        // Move this closer to send time?
        let timestamp_microseconds = get_microseconds();
        self.seq_nr += 1;
        PacketHeader {
            seq_nr: self.seq_nr,
            ack_nr: self.ack_nr,
            conn_id: self.conn_id_send,
            packet_type: PacketType::Data,
            timestamp_microseconds: timestamp_microseconds as u32,
            timestamp_difference_microseconds: self.reply_micro,
            wnd_size: self.our_advertised_window,
            extension: 0,
        }
    }
}

// TODO should this really be publicly derived?
#[derive(Clone)]
pub struct UtpStream {
    inner: Rc<RefCell<StreamState>>,
    // The adder the stream is connected to
    addr: SocketAddr,
    weak_socket: Weak<UdpSocket>,
    // Used to notify pending readers that
    // there is data available to read
    // (This could be adapted to work single threaded but needs custom impl)
    data_available: Rc<Notify>,
}

impl std::fmt::Debug for UtpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpStream")
            .field("state", &self.inner)
            .field("addr", &self.addr)
            .field("data_available", &self.data_available)
            .finish()
    }
}

const MTU: u32 = 1500;
const HEADER_SIZE: usize = 20;

impl UtpStream {
    pub(crate) fn new(conn_id: u16, addr: SocketAddr, weak_socket: Weak<UdpSocket>) -> Self {
        let (shutdown_signal, mut shutdown_receiver) = tokio::sync::oneshot::channel();
        let stream = UtpStream {
            inner: Rc::new(RefCell::new(StreamState {
                connection_state: ConnectionState::Idle,
                // start from 1 for compability with older clients but not as secure
                seq_nr: rand::random::<u16>(),
                conn_id_recv: conn_id,
                cur_window_packets: 0,
                ack_nr: 0,
                // mimic libutp without a callback set (default behavior)
                // this is the receive buffer initial size
                our_advertised_window: 1024 * 1024,
                conn_id_send: conn_id + 1,
                reply_micro: 0,
                eof_pkt: None,
                // mtu
                their_advertised_window: MTU,
                incoming_buffer: ReorderBuffer::new(256),
                outgoing_buffer: ReorderBuffer::new(256),
                receive_buf: vec![0; 1024 * 1024].into_boxed_slice(),
                receive_buf_cursor: 0,
                shutdown_signal: Some(shutdown_signal),
            })),
            weak_socket,
            data_available: Rc::new(Notify::new()),
            addr,
        };

        let stream_clone = stream.clone();
        // Send loop
        tokio_uring::spawn(async move {
            let mut tick_interval = tokio::time::interval(Duration::from_millis(250));
            tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick_interval.tick() => {
                        if let Err(err) = stream_clone.flush_outbuf().await {
                            log::error!("Error: {err}, shutting down stream send loop");
                            break;
                        }
                    },
                    _ = &mut shutdown_receiver =>  {
                        log::info!("Shutting down stream send loop");
                        break;
                    },
                }
            }
        });
        stream
    }

    pub(crate) fn new_incoming(
        seq_nr: u16,
        conn_id: u16,
        addr: SocketAddr,
        weak_socket: Weak<UdpSocket>,
    ) -> Self {
        let (shutdown_signal, mut shutdown_receiver) = tokio::sync::oneshot::channel();
        let stream = UtpStream {
            inner: Rc::new(RefCell::new(StreamState {
                connection_state: ConnectionState::SynReceived,
                // start from 1 for compability with older clients but not as secure
                seq_nr: rand::random::<u16>(),
                conn_id_recv: conn_id + 1,
                cur_window_packets: 0,
                // We have yet to ack the SYN packet
                ack_nr: seq_nr - 1,
                // mimic libutp without a callback set (default behavior)
                // this is the receive buffer initial size
                our_advertised_window: 1024 * 1024,
                conn_id_send: conn_id,
                reply_micro: 0,
                eof_pkt: None,
                // mtu
                their_advertised_window: MTU,
                incoming_buffer: ReorderBuffer::new(256),
                outgoing_buffer: ReorderBuffer::new(256),
                receive_buf: vec![0; 1024 * 1024].into_boxed_slice(),
                receive_buf_cursor: 0,
                shutdown_signal: Some(shutdown_signal),
            })),
            weak_socket,
            data_available: Rc::new(Notify::new()),
            addr,
        };

        let stream_clone = stream.clone();
        // Send loop
        tokio_uring::spawn(async move {
            let mut tick_interval = tokio::time::interval(Duration::from_millis(250));
            tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = tick_interval.tick() => {
                        if let Err(err) = stream_clone.flush_outbuf().await {
                            log::error!("Error: {err}, shutting down stream send loop");
                            break;
                        }
                    },
                    _ = &mut shutdown_receiver =>  {
                        log::info!("Shutting down stream send loop");
                        break;
                    },
                }
            }
        });

        stream
    }

    // Maybe take addr into this instead for a bit nicer api
    pub async fn connect(&self) -> anyhow::Result<()> {
        // Extra brackets to ensure state_mut is dropped pre .await
        let (header, rc) = { self.state_mut().syn_header() };

        log::debug!("Sending SYN");
        self.send_packet(Packet {
            header,
            data: Bytes::new(),
        })
        .await?;
        rc.await?;
        Ok(())
    }

    async fn flush_outbuf(&self) -> anyhow::Result<()> {
        // TODO avoid cloning here, perhaps an extra layer like "Outgoing packet"
        // which could also help with keeping track of resends etc. The reorder buffer needs
        // to support draining operations or a normal buffer is used instead
        let packets: Vec<Packet> = {
            let state = self.state();
            if state.connection_state != ConnectionState::Connected
                && !matches!(state.connection_state, ConnectionState::SynSent { .. })
            {
                // not connected yet
                log::debug!("Not yet connected, holding on to outgoing buffer");
                return Ok(());
            }
            // TODO: Since there is no filtering based on rtt here we will
            // spam the receiver until everything is acked
            state.outgoing_buffer.iter().cloned().collect()
        };
        if let Some(socket) = self.weak_socket.upgrade() {
            for packet in packets.into_iter() {
                // TODO ofc the entire packet and not only the header should be sent
                let packet_bytes = packet.header.to_bytes();
                log::debug!(
                    "Sending {:?} bytes: {} to addr: {}",
                    packet.header.packet_type,
                    packet_bytes.len(),
                    self.addr,
                );
                // reuse buf?
                let (result, _buf) = socket.send_to(packet_bytes, self.addr).await;
                let _ = result?;
                let mut state = self.state_mut();
                // Might be certain situations where this shouldn't be appended?
                // seems like only ST_DATA and ST_FIN. Also count bytes instead of packets
                state.cur_window_packets += 1;
            }
        } else {
            anyhow::bail!("Failed to send packets, socket dropped");
        }
        Ok(())
    }

    async fn send_packet(&self, packet: Packet) -> anyhow::Result<()> {
        self.state_mut().outgoing_buffer.insert(packet);
        self.flush_outbuf().await?;
        Ok(())
    }

    pub(crate) async fn process_incoming(&self, packet: Packet) -> anyhow::Result<()> {
        let packet_header = packet.header;
        let matching_conn_id = {
            let state = self.state();
            // Special case where the initiator might resend the SYN packet 
            // which will have conn_id - 1 of the expected id
            if state.connection_state == ConnectionState::SynReceived {
                packet_header.conn_id + 1 == state.conn_id_recv
            } else {
                packet_header.conn_id == state.conn_id_recv
            }
        };
        if !matching_conn_id {
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

        if dist_from_expected != 0 {
            log::debug!("Got out of order packet");
            // Out of order packet
            self.state_mut().incoming_buffer.insert(packet);
            return Ok(());
        }

        // Did we receive new data?
        let mut data_available = packet.header.packet_type == PacketType::Data;
        self.handle_inorder_packet(packet).await;

        let mut seq_nr = packet_header.seq_nr;
        // Avoid borrowing across await point
        let get_next = |seq_nr: u16| self.state_mut().incoming_buffer.remove(seq_nr);
        while let Some(packet) = get_next(seq_nr) {
            data_available |= packet.header.packet_type == PacketType::Data;
            self.handle_inorder_packet(packet).await;
            seq_nr += 1;
        }
        if data_available {
            self.data_available.notify_waiters();
        }
        Ok(())
    }

    // Perhaps take ownership here instead?
    // Also since this risks reading one packet at a time a read_exact
    // method or equivalent should probably also be added
    pub async fn read(&self, buffer: &mut [u8]) -> usize {
        // If there exists data in the recieve buffer we return it
        // otherwise this should block until either a FIN, RESET or
        // new data is received.
        loop {
            let data_available = { self.state().receive_buf_cursor };
            if data_available == 0 {
                self.data_available.notified().await;
            } else {
                break;
            }
        }

        let mut state = self.state_mut();
        if buffer.len() <= state.receive_buf_cursor {
            let len = buffer.len();
            buffer[..].copy_from_slice(&state.receive_buf[..len]);
            state.receive_buf.copy_within(len.., 0);
            state.receive_buf_cursor -= len;
            buffer.len()
        } else {
            let data_read = state.receive_buf_cursor;
            buffer[0..state.receive_buf_cursor].copy_from_slice(&state.receive_buf[..]);
            state.receive_buf_cursor = 0;
            data_read
        }
    }

    pub async fn write(&self, data: Vec<u8>) -> anyhow::Result<()> {
        if (data.len() - HEADER_SIZE) > MTU as usize {
            log::warn!("Fragmentation is not supported yet");
            Ok(())
        } else {
            let packet = {
                let mut state = self.state_mut();
                let header = state.data();
                Packet {
                    header,
                    data: data.into(),
                }
            };
            self.send_packet(packet).await
        }
    }

    async fn handle_inorder_packet(&self, packet: Packet) {
        let conn_state = std::mem::replace(
            &mut self.state_mut().connection_state,
            ConnectionState::Idle,
        );
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
                let mut state = self.state_mut();
                log::trace!("Received ACK: {}", self.addr);
                if state.outgoing_buffer.remove(packet.header.ack_nr).is_none() {
                    log::error!("Recevied ack for packet not inside the outgoing_buffer");
                }
                //state.cur_window_packets -= 1;
                // Reset connection state if it wasn't modified
                state.connection_state = conn_state;
            }
            (PacketType::Data, conn_state) => {
                let packet = {
                    let mut state = self.state_mut();
                    // in order packet
                    state.ack_nr = packet.header.seq_nr;
                    // Does the packet fit witin the receive buffer? otherwise drop it
                    if packet.data.len() <= (state.receive_buf.len() - state.receive_buf_cursor) {
                        let cursor = state.receive_buf_cursor;
                        // TODO perhaps more of a io_uring kind of approach would make sense
                        // so copies can be avoided either here or in the read method
                        state.receive_buf[cursor..cursor + packet.data.len()]
                            .copy_from_slice(&packet.data);
                        state.receive_buf_cursor += packet.data.len();
                        state.our_advertised_window =
                            (state.receive_buf.len() - state.receive_buf_cursor) as u32;

                        log::trace!("Sending ACK");
                        let header = state.ack();
                        state.connection_state = conn_state;
                        Some(Packet {
                            header,
                            data: Bytes::new(),
                        })
                    } else {
                        log::warn!("Receiv buf full, packet dropped");
                        None
                    }
                };
                if let Some(packet) = packet {
                    self.send_packet(packet).await.unwrap();
                }
                // TODOOO
                // consume_incoming_data in libtorrent
                // basically moves bytes over to receive buf until it's filled
                // receive buf remaining space is our window
                //
                // Reset connection state if it wasn't modified
            }
            (PacketType::Fin, conn_state) => {
                let mut state = self.state_mut();
                log::trace!("Received FIN: {}", self.addr);
                state.eof_pkt = Some(packet.header.seq_nr);
                log::info!("Connection closed: {}", self.addr);

                // more stuff here
                //
                // Reset connection state if it wasn't modified
                state.connection_state = conn_state;
            }
            (p_type, conn_state) => {
                let mut state = self.state_mut();
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

impl Drop for UtpStream {
    fn drop(&mut self) {
        // Only shutdown if this is the last clone
        if Rc::strong_count(&self.inner) == 1 {
            // TODO notify the socket so the stream can be removed from the stream map?
            let _ = self.state_mut().shutdown_signal.take().unwrap().send(());
        }
    }
}
