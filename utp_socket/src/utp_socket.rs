use std::{cell::RefCell, collections::HashMap, net::SocketAddr, rc::Rc};

use anyhow::Context;
use bytes::Bytes;
use tokio_uring::net::UdpSocket;

use crate::{
    utp_packet::{Packet, PacketHeader, PacketType},
    utp_stream::UtpStream,
};

// Conceptually there is a single socket that handles multiple connections
// The socket context keeps a hashmap of all connections keyed by the socketaddr
// the network loop listens on data and then finds the relevant connection based on addr
// and also double checks the connection ids
pub struct UtpSocket {
    socket: Rc<UdpSocket>,
    shutdown_signal: Option<tokio::sync::oneshot::Sender<()>>,
    accept_chan: Rc<RefCell<Option<tokio::sync::oneshot::Sender<UtpStream>>>>,
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
            accept_chan: Default::default(),
            streams: Default::default(),
        };

        let streams_clone = utp_socket.streams.clone();
        let accept_chan = utp_socket.accept_chan.clone();
        // Net loop
        tokio_uring::spawn(async move {
            // TODO check how this relates to windows size and opt_rcvbuf
            let mut recv_buf = vec![0; 1024 * 1024];
            loop {
                // Double check if this is cancellation safe
                // (I don't think it is but shouldn't matter anyways)
                tokio::select! {
                    buf = process_incomming(&net_loop_socket, &streams_clone, &accept_chan, std::mem::take(&mut recv_buf)) => {
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

        let stream = UtpStream::new(stream_key.conn_id, addr, Rc::downgrade(&self.socket));
        self.streams.borrow_mut().insert(stream_key, stream.clone());

        stream.connect().await?;

        Ok(stream)
    }

    pub async fn accept(&self) -> anyhow::Result<UtpStream> {
        let (tx, rc) = tokio::sync::oneshot::channel();
        {
            let mut chan = self.accept_chan.borrow_mut();
            *chan = Some(tx);
        }
        rc.await.context("Net loop exited")
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
    socket: &Rc<UdpSocket>,
    connections: &Rc<RefCell<HashMap<StreamKey, UtpStream>>>,
    accept_chan: &Rc<RefCell<Option<tokio::sync::oneshot::Sender<UtpStream>>>>,
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

                    let maybe_stream = { connections.borrow_mut().remove(&key) };
                    if let Some(stream) = maybe_stream {
                        match stream.process_incoming(packet).await {
                            Ok(()) => {
                                connections.borrow_mut().insert(key, stream);
                            }
                            Err(err) => {
                                log::error!("Error: Failed processing incoming packet: {err}");
                            }
                        }
                    } else if packet_header.packet_type == PacketType::Syn {
                        let maybe_chan = { accept_chan.borrow_mut().take() };
                        if let Some(chan) = maybe_chan {
                            let stream = UtpStream::new_incoming(
                                packet_header.seq_nr,
                                packet_header.conn_id,
                                addr,
                                Rc::downgrade(socket),
                            );
                            // Ensure the initial packet can be processed
                            // before inserting the stream in the connections table.
                            // This is primarily here so that the ACK for the initial syn
                            // can be sent back
                            match stream.process_incoming(packet).await {
                                Ok(()) => {
                                    log::info!("New incoming connection!");
                                    // TODO: This will break silently when 
                                    // a SYN is resent to a connection not yet 
                                    // fully established
                                    connections.borrow_mut().insert(
                                        StreamKey {
                                            // Special case for initial stream setup
                                            // Same as stream recv conn id
                                            conn_id: packet_header.conn_id + 1,
                                            addr,
                                        },
                                        stream.clone(),
                                    );
                                    chan.send(stream).unwrap();
                                }
                                Err(err) => {
                                    log::error!("Error accepting connection: {err}");
                                    // If the packet couldn't be processed
                                    // we the accept chan is reset
                                    *accept_chan.borrow_mut() = Some(chan);
                                }
                            }
                        }
                    } else {
                        log::warn!("Connection not established prior");
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
