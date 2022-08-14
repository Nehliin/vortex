use std::{
    cell::{Ref, RefCell, RefMut},
    collections::HashMap,
    net::SocketAddr,
    rc::Rc,
};

use bytes::{Buf, Bytes, BytesMut};
use tokio_uring::net::UdpSocket;

#[derive(Debug)]
enum ConnectionState {
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
struct StreamState {
    // Current socket state
    connection_state: ConnectionState,
    // Sequence number for next packet to be sent
    seq_nr: u16,
    // All sequence numbers up until and including this which have been
    // properly recived
    ack_nr: u16,
    // Connection id for packets I receive
    conn_id_recv: u16,
    // Connection id for packets I send
    conn_id_send: u16,
    // Current amount of packets sent but not acked
    cur_window_packets: u16,
    // Last received window this socket advertised in bytes
    last_recv_window: u32,
    // Last delay measurement from other endpoint
    // whenever a packet is received this state is updated
    // by subtracting timestamp_microseconds from the host current time
    reply_micro: u32,
    // Last packet in sequence, taken from the FIN packet
    eof_pkt: Option<u16>,
    // The adder the stream is connected to
    addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub struct UtpStream {
    inner: Rc<RefCell<StreamState>>,
}

impl UtpStream {
    fn state_mut(&self) -> RefMut<'_, StreamState> {
        self.inner.borrow_mut()
    }

    fn state(&self) -> Ref<'_, StreamState> {
        self.inner.borrow()
    }
}

pub struct UtpSocket {
    socket: Rc<UdpSocket>,
    shutdown_signal: tokio::sync::oneshot::Sender<()>,
    streams: Rc<RefCell<HashMap<StreamKey, UtpStream>>>,
}

// One could do callbacks like libutp but it would require allocations
// unlessa a manual vtable was employed which I'd rather avoid

// Conceptually there is a single socket that handles multiple connections
// The socket context keeps a hashmap of all connections keyed by the socketaddr
// the network loop listens on data and then finds the relevant connection based on addr
// and also double checks the connection ids

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
            shutdown_signal,
            streams: Default::default(),
        };

        let streams_clone = utp_socket.streams.clone();
        // Net loop
        tokio_uring::spawn(async move {
            loop {
                // TODO check how this relates to windows size and opt_rcvbuf
                let mut recv_buf = vec![0; 1024 * 1024];
                // Double check if this is cancellation safe
                // (I don't think it is but shouldn't matter anyways)
                tokio::select! {
                    buf_res = UtpSocket::process_incomming(&net_loop_socket, &streams_clone, std::mem::take(&mut recv_buf)) => {
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

    async fn process_incomming(
        socket: &UdpSocket,
        connections: &Rc<RefCell<HashMap<StreamKey, UtpStream>>>,
        mut recv_buf: Vec<u8>,
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

                let stream = {
                    if let Some(stream) = connections.borrow().get(&key) {
                        stream.clone()
                    } else {
                        // Can't handle incoming traffic yet
                        return Ok(buf);
                    }
                };
                // TODO ignore packets who have invalid ack nr

                let (conn_state, dist_from_expected) = {
                    let mut state = stream.state_mut();
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

                let addr = stream.state().addr;

                match (packet.packet_type, conn_state) {
                    // Outgoing connection completion
                    (PacketType::State, ConnectionState::SynSent { connect_notifier }) => {
                        log::trace!("Packet dist_from_expected: {dist_from_expected}");
                        let mut state = stream.state_mut();
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
                        let mut state = stream.state_mut();
                        state.cur_window_packets -= 1;
                    }
                    (PacketType::Data, _) => {
                        log::trace!("Packet dist_from_expected: {dist_from_expected}");
                        if dist_from_expected == 0 {
                            // in order packet
                            {
                                let mut state = stream.state_mut();
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
                        let mut state = stream.state_mut();
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
            }
            Err(err) => log::error!("Failed to receive on utp socket: {err}"),
        }
        Ok(buf)
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

        let stream = UtpStream {
            inner: Rc::new(RefCell::new(StreamState {
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
            })),
        };

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
        let (packet_header, addr) = {
            let state = stream.state();
            (
                PacketHeader {
                    seq_nr: state.seq_nr,
                    ack_nr: state.ack_nr,
                    conn_id: state.conn_id_send,
                    packet_type: PacketType::State,
                    timestamp_microseconds: timestamp_microseconds as u32,
                    timestamp_difference_microseconds: state.reply_micro,
                    wnd_size: dbg!(state.last_recv_window),
                    extension: 0,
                },
                state.addr,
            )
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
    seq_nr: u16,
    ack_nr: u16,
    conn_id: u16,
    packet_type: PacketType,
    timestamp_microseconds: u32,
    timestamp_difference_microseconds: u32,
    wnd_size: u32,
    extension: u8,
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
fn get_microseconds() -> u64 {
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
