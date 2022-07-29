use std::net::SocketAddr;

use bytes::{Buf, Bytes, BytesMut};
use time::OffsetDateTime;
use tokio::time::Instant;
use tokio_uring::net::UdpSocket;

pub enum ConnectionState {
    // TODO syn received to handle incoming traffic
    SynSent,
    Connected,
}

pub struct UTPSocket {
    socket: UdpSocket,
    state: ConnectionState,
    seq_nr: u16,
    conn_id_recv: u16,
    conn_id_send: u16,
}

impl UTPSocket {
    // TODO better error handling
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0".parse().unwrap()).await?;

        // TODO neither guaranteed to be unique nor kept track of outside the socket
        let conn_id = rand::random::<u16>();
        let mut utp_socket = Self {
            socket,
            state: ConnectionState::SynSent,
            seq_nr: 1,
            conn_id_recv: conn_id,
            conn_id_send: conn_id + 1,
        };

        let sent_packet = Packet::connect(utp_socket.conn_id_recv, utp_socket.seq_nr);
        utp_socket.seq_nr += 1;

        log::debug!("Sending ST_SYN for addr: {addr}");
        let (result, _buf) = utp_socket
            .socket
            .send_to(sent_packet.to_bytes(), addr)
            .await;
        let _ = result?;

        log::debug!("Waiting for ST_STATE for addr: {addr}");
        let buf = vec![0; 4096];
        // Wait for resposse
        let (result, buf) = utp_socket.socket.recv_from(buf).await;
        let (recv, from_addr) = result?;

        log::debug!("Received packet from addr: {from_addr}");
        assert!(addr == from_addr);

        let packet = Packet::from(Bytes::copy_from_slice(&buf[..recv]));
        dbg!(&packet);

        if packet.packet_type == PacketType::StState {
            log::info!("Connected??");
            log::info!("sent_packet: {sent_packet:?}");
            log::info!("recv_packet: {packet:?}");
        }
        Ok(utp_socket)
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PacketType {
    StData = 0,
    StFin = 1,
    StState = 2,
    StReset = 3,
    StSyn = 4,
}

#[derive(Debug)]
pub struct Packet {
    seq_nr: u16,
    ack_nr: u16,
    conn_id: u16,
    packet_type: PacketType,
    timestamp_microseconds: u32,
    timestamp_difference_microseconds: u32,
    wnd_size: u32,
    extension: u8,
}

impl From<Bytes> for Packet {
    fn from(mut bytes: Bytes) -> Self {
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

impl Packet {
    fn connect(conn_id_recv: u16, seq_nr: u16) -> Self {
        let timestamp_microseconds = OffsetDateTime::now_local().unwrap().microsecond();

        Self {
            seq_nr,
            ack_nr: 0,
            conn_id: conn_id_recv,
            packet_type: PacketType::StSyn,
            timestamp_microseconds,
            timestamp_difference_microseconds: 0,
            wnd_size: 256,
            extension: 0,
        }
    }

    fn to_bytes(&self) -> Bytes {
        use bytes::BufMut;
        let mut bytes = BytesMut::new();
        let mut first_byte = (self.packet_type as u8).to_be();
        first_byte <<= 4;
        first_byte |= 0b0000_0001;
        dbg!(first_byte);
        // type and version
        bytes.put_u8(first_byte);
        bytes.put_u8(self.extension.to_be());
        bytes.put_u16(self.conn_id.to_be());
        bytes.put_u32(self.timestamp_microseconds.to_be());
        bytes.put_u32(self.timestamp_difference_microseconds.to_be());
        bytes.put_u32(self.wnd_size.to_be());
        bytes.put_u16(self.seq_nr.to_be());
        bytes.put_u16(self.ack_nr.to_be());
        bytes.freeze()
    }
}
