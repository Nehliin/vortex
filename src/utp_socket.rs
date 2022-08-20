use std::{cell::RefCell, collections::HashMap, net::SocketAddr, rc::Rc};

use bytes::Bytes;
use tokio_uring::net::UdpSocket;

use crate::{
    utp_packet::{get_microseconds, PacketHeader, PacketType, Packet},
    utp_stream::{ConnectionState, StreamState, UtpStream}, packet_buffer::PacketBuffer,
};

// Conceptually there is a single socket that handles multiple connections
// The socket context keeps a hashmap of all connections keyed by the socketaddr
// the network loop listens on data and then finds the relevant connection based on addr
// and also double checks the connection ids
pub struct UtpSocket {
    socket: Rc<UdpSocket>,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    streams: Rc<RefCell<HashMap<StreamKey, UtpStream>>>,
}

#[derive(PartialEq, Eq, Debug, Copy, Clone, Hash)]
struct StreamKey {
    conn_id: u16,
    addr: SocketAddr,
}

// TODO better error handling
// This is more similar to TcpListener
impl UtpSocket {
    pub async fn bind(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = Rc::new(UdpSocket::bind(bind_addr).await?);
        let net_loop_socket = socket.clone();

        let (shutdown_signal, mut shutdown_receiver) = tokio::sync::oneshot::channel();
        //connections: HashMap<SocketKey, Rc<RefCell<UtpStream>>>,
        let utp_socket = UtpSocket {
            socket,
            shutdown_signal: Some(shutdown_signal),
            streams: Default::default(),
        };

        let streams_clone = utp_socket.streams.clone();
        // Net loop
        tokio_uring::spawn(async move {
            // TODO check how this relates to windows size and opt_rcvbuf
            let mut recv_buf = vec![0; 1024 * 1024];
            loop {
                // Double check if this is cancellation safe
                // (I don't think it is but shouldn't matter anyways)
                tokio::select! {
                    buf = process_incomming(&net_loop_socket, &streams_clone, std::mem::take(&mut recv_buf)) => {
                           recv_buf = buf;
                        }
                    _ = &mut shutdown_receiver =>  {
                        log::info!("Shutting down network loop");
                        // TODO shutdown all streams gracefully
                        break;
                    }
                }
            }
        });

        Ok(utp_socket)
    }

    pub async fn connect(&self, addr: SocketAddr) -> anyhow::Result<UtpStream> {
        let mut stream_key = StreamKey {
            conn_id: rand::random(),
            addr,
        };

        while self.streams.borrow().contains_key(&stream_key) {
            log::debug!("Stream with same conn_id and addr already exists, regenerating conn_id");
            stream_key = StreamKey {
                conn_id: rand::random::<u16>(),
                addr,
            }
        }

        let stream = UtpStream::new(StreamState {
            connection_state: ConnectionState::Idle,
            // start from 1 for compability with older clients but not as secure
            seq_nr: rand::random::<u16>(),
            conn_id_recv: stream_key.conn_id,
            cur_window_packets: 0,
            ack_nr: 0,
            // mimic libutp without a callback set (default behavior)
            // this is the receive buffer initial size
            our_advertised_window: 1024 * 1024,
            conn_id_send: stream_key.conn_id + 1,
            reply_micro: 0,
            eof_pkt: None,
            addr,
            // mtu
            their_advertised_window: 1500,
            incoming_buffer: PacketBuffer::new(256),
            receive_buf: Vec::with_capacity(1024 * 1024),
        });

        self.streams.borrow_mut().insert(stream_key, stream.clone());

        let timestamp_microseconds = get_microseconds() as u32;

        let (tx, rc) = tokio::sync::oneshot::channel();
        let packet_header = {
            let mut state = stream.state_mut();
            state.connection_state = ConnectionState::SynSent {
                connect_notifier: tx,
            };

            let header = PacketHeader {
                seq_nr: state.seq_nr,
                ack_nr: 0,
                conn_id: state.conn_id_recv,
                packet_type: PacketType::Syn,
                timestamp_microseconds,
                timestamp_difference_microseconds: state.reply_micro,
                wnd_size: state.our_advertised_window,
                extension: 0,
            };
            state.seq_nr += 1;
            header
        };

        UtpSocket::send_packet(&self.socket, packet_header, &stream).await?;
        rc.await?;
        Ok(stream)
    }

    async fn ack(socket: &UdpSocket, stream: &UtpStream) -> anyhow::Result<()> {
        let timestamp_microseconds = get_microseconds();
        let packet_header = {
            let state = stream.state();
            PacketHeader {
                seq_nr: state.seq_nr,
                ack_nr: state.ack_nr,
                conn_id: state.conn_id_send,
                packet_type: PacketType::State,
                timestamp_microseconds: timestamp_microseconds as u32,
                timestamp_difference_microseconds: state.reply_micro,
                wnd_size: dbg!(state.our_advertised_window),
                extension: 0,
            }
        };
        UtpSocket::send_packet(socket, packet_header, stream).await?;
        Ok(())
    }

    async fn send_packet(
        socket: &UdpSocket,
        packet: PacketHeader,
        stream: &UtpStream,
    ) -> anyhow::Result<()> {
        let addr = stream.state().addr;
        let packet_bytes = packet.to_bytes();
        log::debug!(
            "Sending {:?} bytes: {} to addr: {addr}",
            packet.packet_type,
            packet_bytes.len()
        );
        // TODO check how this relates to windows size and opt_sndbuf
        let (result, _buf) = socket.send_to(packet_bytes, addr).await;
        let _ = result?;
        let mut state = stream.state_mut();
        // Might be certain situations where this shouldn't be appended?
        // seems like only ST_DATA and ST_FIN
        state.cur_window_packets += 1;
        Ok(())
    }
}

impl Drop for UtpSocket {
    fn drop(&mut self) {
        log::debug!("Shutting down socket net noop");
        let _ = self.shutdown_signal.take().unwrap().send(());
    }
}

// Socket reads and parses out a vec of packets per read
// the packets are then sent to the streams incoming circular packet buffer
// in the stream specific packet handler they check if it's the expected packet or out of order
// if it's out of order they then just insert it into the buffer
// if it's in order they handle it together with all other potential packets that are orderd
// after it and stored in the incoming buffer
//
// outbuffer is written to by the stream and handled in a separate task i think
// the packets can get stored in that task if they need to be resent?
// the incoming task could keep a channel of acks received that can be removed from resend buf
async fn process_incomming(
    socket: &UdpSocket,
    connections: &Rc<RefCell<HashMap<StreamKey, UtpStream>>>,
    recv_buf: Vec<u8>,
) -> Vec<u8> {
    let (result, buf) = socket.recv_from(recv_buf).await;
    match result {
        Ok((recv, addr)) => {
            log::info!("Received {recv} from {addr}");
            match PacketHeader::try_from(&buf[..recv]) {
                Ok(packet_header) => {
                    let key = StreamKey {
                        conn_id: packet_header.conn_id,
                        addr,
                    };

                    let packet = Packet {
                        header: packet_header,
                        data: Bytes::copy_from_slice(&buf[recv..]),
                    };

                    if let Some(stream) = connections.borrow().get(&key) {
                        match stream.process_incoming(packet) {
                            Ok(needs_ack) => {
                                UtpSocket::ack(&socket, stream).await;
                            }
                            Err(err) => {
                                log::error!("Error: Failed processing incoming packet: {err}");
                            }
                        }
                    } else {
                        log::warn!("Connection not established prior");
                        // Can't handle incoming traffic yet
                    }
                }
                Err(err) => log::error!("Error parsing packet: {err}"),
            }
        }
        Err(err) => log::error!("Failed to receive on utp socket: {err}"),
    }
    buf
}

// Process outgoing
