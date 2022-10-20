// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::{
    cell::{Ref, RefCell, RefMut},
    net::SocketAddr,
    rc::Rc,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_uring::{buf::IoBuf, net::TcpStream};

struct PeerConnectionState {
    /// This side is choking the peer
    is_choking: bool,
    /// This side is interested what the peer has to offer
    is_interested: bool,
    /// The peer have informed us that it is choking us.
    peer_choking: bool,
    /// The peer is interested what we have to offer
    peer_interested: bool,
}

#[derive(Clone)]
pub struct PeerConnection {
    // TODO use utp
    // Also is a single socket needed to be used?
    stream: Rc<TcpStream>,
    state: Rc<RefCell<PeerConnectionState>>,
}

impl PeerConnection {
    pub async fn new(
        addr: SocketAddr,
        our_id: [u8; 20],
        their_id: [u8; 20],
        info_hash: [u8; 20],
    ) -> anyhow::Result<PeerConnection> {
        let stream = Rc::new(TcpStream::connect(addr).await?);

        let handshake_msg = Self::handshake(info_hash, our_id).to_vec();
        let (res, buf) = stream.write(handshake_msg).await;
        let _res = res?;

        let (res, buf) = stream.read(buf).await;
        let read = res?;
        let mut buf = buf.as_slice();
        if read >= 67 {
            log::info!("HANDSHAKE RECV");
            let str_len = buf.get_u8();
            assert_eq!(str_len, 19);
            assert_eq!(
                buf.get(1..(str_len as usize + 1)),
                Some(b"BitTorrent protocol" as &[u8])
            );
            assert_eq!(
                buf.get((str_len as usize + 1)..(str_len as usize + 9)),
                Some(&[0_u8; 8] as &[u8])
            );
            assert_eq!(
                Some(&info_hash as &[u8]),
                buf.get((str_len as usize + 9)..(str_len as usize + 21))
            );
            assert_eq!(
                Some(&their_id as &[u8]),
                buf.get((str_len as usize + 21)..(str_len as usize + 41))
            );
            let stream_state = PeerConnectionState {
                is_choking: false,
                peer_choking: false,
                is_interested: false,
                peer_interested: false,
            };

            let connection = PeerConnection {
                stream: stream.clone(),
                state: Rc::new(RefCell::new(stream_state)),
            };
            let connection_clone = connection.clone();
            // TODO Handle shutdowns
            tokio_uring::spawn(async move {
                // Spec says max size of request is 2^14 so double that for safety
                let mut read_buf = vec![0_u8; 2 ^ 15];
                loop {
                    let (result, buf) = connection_clone.stream.read(read_buf).await;
                    match result {
                        Ok(bytes_read) => {
                            log::debug!("Read data from peer connection: {bytes_read}");
                            let msg = PeerMessage::try_from(&buf[..bytes_read]).unwrap();
                            connection_clone.process_incoming(msg).await.unwrap();
                        }
                        Err(err) => {
                            log::error!("Failed to read from peer connection: {err}");
                            break;
                        }
                    }
                    read_buf = buf;
                }
            });
            return Ok(connection);
        }
        anyhow::bail!("Didn't get enough data");
    }

    pub async fn request(&self, index: i32, begin: i32, length: i32) {
        let msg = PeerMessage::Request {
            index,
            begin,
            length,
        };
    }

    // Consider adding handlers for each msg as a trait which extensions can implement
    async fn process_incoming(&self, msg: PeerMessage<'_>) -> anyhow::Result<()> {
        match msg {
            PeerMessage::Choke => {
                let mut state = self.state_mut();
                log::info!("Peer is choking us!");
                state.peer_choking = true;
                // TODO clear outgoing requests
            }
            PeerMessage::Unchoke => {
                let mut state = self.state_mut();
                log::info!("Peer is no longer choking us!");
                state.peer_choking = false;
                if state.is_interested {
                    // Peer have stuff we are interested in!
                    // TODO: request more stuff
                    log::warn!("TODO: We are interested and should request stuff");
                }
            }
            PeerMessage::Interested => {
                let mut state = self.state_mut();
                log::info!("Peer is interested in us!");
                state.peer_interested = true;
                if !state.is_choking {
                    // TODO if we are not choking them we might need to send a
                    // unchoke to avoid some race conditions. Libtorrent
                    // uses the same type of logic
                    log::info!("Should perhaps send a redundant unchoke here");
                } else {
                    //TODO
                    log::warn!("Should maybe unchoke");
                }
            }
            PeerMessage::NotInterested => {
                let mut state = self.state_mut();
                log::info!("Peer is no longer interested in us!");
                state.peer_interested = false;
                state.is_choking = true;
                // TODO: send choke to the peer
            }
            PeerMessage::Have { index } => {
                log::info!("Peer have piece with index: {index}");
            }
            PeerMessage::Bitfield(_) => unimplemented!(),
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                log::info!(
                    "Peer wants piece with index: {index}, begin: {begin}, length: {length}"
                );
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                log::info!(
                    "Peer cancels request with index: {index}, begin: {begin}, length: {length}"
                );
                //TODO cancel if has yet to been sent I guess
            }
            PeerMessage::Piece {
                index,
                begin,
                piece,
                data,
            } => {
                log::info!("Recived a piece index: {index}, begin: {begin}, piece: {piece}");
                log::info!("Data len: {}", data.len());
            }
        }
        Ok(())
    }

    fn handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 67] {
        const PROTOCOL: &[u8] = b"BitTorrent protocol";
        let mut buffer: [u8; 67] = [0; PROTOCOL.len() + 8 + 20 + 20];
        let mut writer: &mut [u8] = &mut buffer;
        writer.put_u8(PROTOCOL.len() as u8);
        writer.put(PROTOCOL);
        writer.put(&[0_u8; 8] as &[u8]);
        writer.put(&info_hash as &[u8]);
        writer.put(&peer_id as &[u8]);
        buffer
    }

    fn state_mut(&self) -> RefMut<'_, PeerConnectionState> {
        self.state.borrow_mut()
    }

    fn state(&self) -> Ref<'_, PeerConnectionState> {
        self.state.borrow()
    }
}

pub enum PeerMessage<'a> {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        index: i32,
    },
    Bitfield(i32),
    Request {
        index: i32,
        begin: i32,
        length: i32,
    },
    Cancel {
        index: i32,
        begin: i32,
        length: i32,
    },
    Piece {
        index: i32,
        begin: i32,
        piece: i32,
        data: &'a [u8],
    },
}

impl PeerMessage<'_> {
    pub const CHOKE: u8 = 0;
    pub const UNCHOKE: u8 = 1;
    pub const INTERESTED: u8 = 2;
    pub const NOT_INTERESTED: u8 = 3;
    pub const HAVE: u8 = 4;
    pub const BITFIELD: u8 = 5;
    pub const REQUEST: u8 = 6;
    pub const PIECE: u8 = 7;
    pub const CANCEL: u8 = 8;

    pub fn to_bytes(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            PeerMessage::Choke => {
                bytes.put_u8(Self::CHOKE);
                bytes.freeze()
            }
            PeerMessage::Unchoke => {
                bytes.put_u8(Self::UNCHOKE);
                bytes.freeze()
            }
            PeerMessage::Interested => {
                bytes.put_u8(Self::INTERESTED);
                bytes.freeze()
            }
            PeerMessage::NotInterested => {
                bytes.put_u8(Self::NOT_INTERESTED);
                bytes.freeze()
            }
            PeerMessage::Have { index } => {
                bytes.put_u8(Self::HAVE);
                bytes.put_i32(index);
                bytes.freeze()
            }
            PeerMessage::Bitfield(bitfield) => {
                bytes.put_u8(Self::BITFIELD);
                bytes.put_i32(bitfield);
                bytes.freeze()
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                bytes.put_u8(Self::REQUEST);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(length);
                bytes.freeze()
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                bytes.put_u8(Self::CANCEL);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(length);
                bytes.freeze()
            }
            PeerMessage::Piece {
                index,
                begin,
                piece,
                data,
            } => {
                bytes.put_u8(Self::PIECE);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(piece);
                bytes.put(data);
                bytes.freeze()
            }
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for PeerMessage<'a> {
    type Error = anyhow::Error;

    fn try_from(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let msg_type = bytes.get_u8();
        match msg_type {
            CHOKE => Ok(PeerMessage::Choke),
            UNCHOKE => Ok(PeerMessage::Unchoke),
            INTERESTED => Ok(PeerMessage::Interested),
            NOT_INTERESTED => Ok(PeerMessage::NotInterested),
            HAVE => Ok(PeerMessage::Have {
                index: bytes.get_i32(),
            }),
            BITFIELD => {
                log::debug!("Bitfield received");
                Ok(PeerMessage::Bitfield(bytes.get_i32()))
            }
            REQUEST => {
                let index = bytes.get_i32();
                let begin = bytes.get_i32();
                let length = bytes.get_i32();
                if length > 2 ^ 14 {
                    log::error!("Too large piece requested failing");
                    anyhow::bail!("Invalid request");
                } else {
                    Ok(PeerMessage::Request {
                        index,
                        begin,
                        length,
                    })
                }
            }
            PIECE => Ok(PeerMessage::Piece {
                index: bytes.get_i32(),
                begin: bytes.get_i32(),
                piece: bytes.get_i32(),
                data: bytes,
            }),
            CANCEL => Ok(PeerMessage::Cancel {
                index: bytes.get_i32(),
                begin: bytes.get_i32(),
                length: bytes.get_i32(),
            }),
            _ => anyhow::bail!("Invalid message type: {msg_type}"),
        }
    }
}

// choke unchoke (not) interestead doesn't have a payload
// bitfield is the first message

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
