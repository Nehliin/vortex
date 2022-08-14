use std::{cell::RefCell, collections::HashMap, net::SocketAddr, rc::Rc};

use bytes::{Buf, Bytes, BytesMut};
use tokio_uring::net::UdpSocket;

use crate::utp_stream::{ConnectionState, StreamState, UtpStream};

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
                    buf_res = process_incomming(&net_loop_socket, &streams_clone, std::mem::take(&mut recv_buf)) => {
                            match buf_res {
                                Ok(buffer) => recv_buf = buffer,
                                Err(err) => {
                                    log::error!("Error {err}: Shutting down network loop");
                                    break;
                                },
                            }
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
            last_recv_window: 1024 * 1024,
            conn_id_send: stream_key.conn_id + 1,
            reply_micro: 0,
            eof_pkt: None,
            addr,
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
                wnd_size: state.last_recv_window,
                extension: 0,
            };
            state.seq_nr += 1;
            header
        };

        self.send_packet(packet_header, &stream).await?;
        rc.await?;
        Ok(stream)
    }

    async fn ack(&self, stream: &UtpStream) -> anyhow::Result<()> {
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
                wnd_size: dbg!(state.last_recv_window),
                extension: 0,
            }
        };
        self.send_packet(packet_header, stream).await?;
        Ok(())
    }

    async fn send_packet(&self, packet: PacketHeader, stream: &UtpStream) -> anyhow::Result<()> {
        let addr = stream.state().addr;
        let packet_bytes = packet.to_bytes();
        log::debug!(
            "Sending {:?} bytes: {} to addr: {addr}",
            packet.packet_type,
            packet_bytes.len()
        );
        // TODO check how this relates to windows size and opt_sndbuf
        let (result, _buf) = self.socket.send_to(packet_bytes, addr).await;
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

async fn process_incomming(
    socket: &UdpSocket,
    connections: &Rc<RefCell<HashMap<StreamKey, UtpStream>>>,
    recv_buf: Vec<u8>,
) -> anyhow::Result<Vec<u8>> {
    let (result, buf) = socket.recv_from(recv_buf).await;
    match result {
        Ok((recv, addr)) => {
            log::info!("Received {recv} from {addr}");
            let packet = PacketHeader::from(&buf[..recv]);

            let key = StreamKey {
                conn_id: packet.conn_id,
                addr,
            };

            // Check version here instead of panicking in packetHeader impl 

            if let Some(stream) = connections.borrow().get(&key) {
                stream.process_incoming(packet);
            } else {
                log::warn!("Connection not established prior");
                // Can't handle incoming traffic yet
                return Ok(buf);
            }
        }
        Err(err) => log::error!("Failed to receive on utp socket: {err}"),
    }
    Ok(buf)
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PacketType {
    Data = 0,
    Fin = 1,
    State = 2,
    Reset = 3,
    Syn = 4,
}

// repr c instead? and just send directly over socket?
#[derive(Debug)]
pub struct PacketHeader {
    pub seq_nr: u16,
    pub ack_nr: u16,
    pub conn_id: u16,
    pub packet_type: PacketType,
    pub timestamp_microseconds: u32,
    pub timestamp_difference_microseconds: u32,
    pub wnd_size: u32,
    pub extension: u8,
}

impl From<&[u8]> for PacketHeader {
    fn from(mut bytes: &[u8]) -> Self {
        let first_byte = bytes.get_u8();
        let packet_type = first_byte >> 4;
        assert!(packet_type < 5);
        let packet_type: PacketType = unsafe { std::mem::transmute(packet_type) };
        dbg!(packet_type);
        let version = first_byte & 0b0000_1111;
        assert!(version == 1);
        let extension = bytes.get_u8();
        let conn_id = bytes.get_u16();
        let timestamp_microseconds = bytes.get_u32();
        let timestamp_difference_microseconds = bytes.get_u32();
        let wnd_size = bytes.get_u32();
        let seq_nr = bytes.get_u16();
        let ack_nr = bytes.get_u16();

        Self {
            seq_nr,
            ack_nr,
            conn_id,
            packet_type,
            timestamp_microseconds,
            timestamp_difference_microseconds,
            wnd_size,
            extension,
        }
    }
}

// Not very rusty at all, stolen from libutp to test
// impact on connection errors
pub fn get_microseconds() -> u64 {
    static mut OFFSET: u64 = 0;
    static mut PREVIOUS: u64 = 0;

    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let res = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };

    if res < 0 {
        panic!("clock get time failed");
    }

    let mut now = ts.tv_sec as u64 * 1000000 + ts.tv_nsec as u64 / 1000;
    unsafe {
        now += OFFSET;
        if PREVIOUS > now {
            OFFSET += PREVIOUS - now;
            now = PREVIOUS;
        }
        PREVIOUS = now;
    }

    now
}

impl PacketHeader {
    fn to_bytes(&self) -> Bytes {
        use bytes::BufMut;
        let mut bytes = BytesMut::new();

        let mut first_byte = self.packet_type as u8;
        first_byte <<= 4;
        first_byte |= 0b0000_0001;

        // type and version
        bytes.put_u8(first_byte);
        // 0 so doesn't matter for now if to_be should be used or not
        bytes.put_u8(self.extension);
        bytes.put_u16(self.conn_id);
        bytes.put_u32(self.timestamp_microseconds);
        bytes.put_u32(self.timestamp_difference_microseconds);
        bytes.put_u32(self.wnd_size);
        bytes.put_u16(self.seq_nr);
        bytes.put_u16(self.ack_nr);
        let res = bytes.freeze();
        log::debug!("{:02x?}", &res[..]);
        res
    }
}
