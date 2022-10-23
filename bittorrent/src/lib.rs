// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::{
    cell::{Ref, RefCell, RefMut},
    net::SocketAddr,
    rc::Rc,
    sync::Arc,
};

use bitvec::prelude::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use tokio_uring::net::TcpStream;

struct PeerConnectionState {
    /// This side is choking the peer
    is_choking: bool,
    /// This side is interested what the peer has to offer
    is_interested: bool,
    /// The peer have informed us that it is choking us.
    peer_choking: bool,
    /// The peer is interested what we have to offer
    peer_interested: bool,
    /// Which pieces do the peer have
    peer_pieces: BitBox<u8, Msb0>,
}

impl PeerConnectionState {
    fn new(peer_pieces: BitBox<u8, Msb0>) -> Self {
        Self {
            is_choking: true,
            is_interested: false,
            // TODO: is this correct?
            peer_choking: false,
            peer_interested: false,
            peer_pieces,
        }
    }
}

pub struct TorrentState {
    completed_pieces: BitBox<u8, Msb0>,
    pretended_file: BytesMut,
pub struct PeerConnectionHandle {
    peer_id: [u8; 20],
    //ip?
    sender: Sender<PeerOrder>,
}

pub struct TorrentManager {
    torrent_info: bip_metainfo::Info,
    peer_connections: Vec<PeerConnectionHandle>,
    // Maybe use a channel to communicate instead?
    torrent_state: Arc<Mutex<TorrentState>>,
}

impl TorrentManager {
    pub fn new(torrent_info: bip_metainfo::Info, max_unchoked: u32) -> Self {
        let completed_pieces: BitBox<u8, Msb0> = torrent_info.pieces().map(|_| false).collect();
        let torrent_state = TorrentState {
            completed_pieces,
            pretended_file: BytesMut::new(),
        };
        Self {
            torrent_info,
            peer_connections: Vec::new(),
            torrent_state: Arc::new(Mutex::new(torrent_state)),
        }
    }
}

// Torrentmanager
// tracks peers "haves"
// includes piece stategy
// chokes and unchokes
// owns peer connections
// includes mmapped file(s)?

// TorrentDownloadManager
// 1. Get meta data about pieces and info hashes
// 2. Fetch peers from DHT for the pieces
// 3. Connect to all peers
// 4. Do piece selection and distribute pieces across peers that have them
// 5. PeerConnection requests subpieces automatically
// 6. Manager is informed about pieces that have completed (and peer choking us)

#[derive(Clone)]
pub struct PeerConnection {
    // TODO use utp
    stream: Rc<TcpStream>,
    state: Rc<RefCell<PeerConnectionState>>,
    torrent_state: Arc<Mutex<TorrentState>>,
}

impl PeerConnection {
    pub async fn new(
        addr: SocketAddr,
        our_id: [u8; 20],
        their_id: [u8; 20],
        info_hash: [u8; 20],
        torrent_state: Arc<Mutex<TorrentState>>,
    ) -> anyhow::Result<PeerConnection> {
        let stream = Rc::new(TcpStream::connect(addr).await?);

        let handshake_msg = Self::handshake(info_hash, our_id).to_vec();
        let (res, buf) = stream.write(handshake_msg).await;
        let _res = res?;

        let (res, buf) = stream.read(buf).await;
        let read = res?;
        let mut buf = buf.as_slice();
        if read >= 68 {
            log::info!("HANDSHAKE RECV");
            let str_len = buf.get_u8();
            assert_eq!(str_len, 19);
            assert_eq!(
                buf.get(..str_len as usize),
                Some(b"BitTorrent protocol" as &[u8])
            );
            assert_eq!(
                buf.get((str_len as usize)..(str_len as usize + 8)),
                Some(&[0_u8; 8] as &[u8])
            );
            assert_eq!(
                Some(&info_hash as &[u8]),
                buf.get((str_len as usize + 8)..(str_len as usize + 28))
            );
            assert_eq!(
                Some(&their_id as &[u8]),
                buf.get((str_len as usize + 28)..(str_len as usize + 48))
            );
            let mut peer_pieces = torrent_state.lock().completed_pieces.clone();
            peer_pieces.fill(false);
            let stream_state = PeerConnectionState::new(peer_pieces);

            let connection = PeerConnection {
                stream: stream.clone(),
                state: Rc::new(RefCell::new(stream_state)),
                torrent_state,
            };
            let connection_clone = connection.clone();
            // TODO Handle shutdowns
            tokio_uring::spawn(async move {
                // Spec says max size of request is 2^14 so double that for safety
                let mut read_buf = vec![0_u8; 2 ^ 15];
                loop {
                    let (result, buf) = connection_clone.stream.read(read_buf).await;
                    match result {
                        Ok(0) => log::info!("Shutting down connection"),
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
                    // TODO call to the torrent manager which might call back into
                    // this peer connection and send an unchoke
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
                self.state_mut().peer_pieces.set(index as usize, true);
            }
            PeerMessage::Bitfield(field) => {
                log::info!("Bifield received");
                self.state_mut().peer_pieces |= field;
            }
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
                /*self.manager_coms
                .send(ManagerMsg::Piece {
                    index,
                    begin,
                    piece,
                    // TODO don't copy here
                    data: Bytes::copy_from_slice(data),
                })
                .await
                .unwrap();*/
            }
        }
        Ok(())
    }

    fn handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 68] {
        const PROTOCOL: &[u8] = b"BitTorrent protocol";
        let mut buffer: [u8; 68] = [0; PROTOCOL.len() + 8 + 20 + 20 + 1];
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
    Bitfield(&'a BitSlice<u8, Msb0>),
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
                // TODO
                //bytes.put_i32(bitfield);
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
            PeerMessage::CHOKE => Ok(PeerMessage::Choke),
            PeerMessage::UNCHOKE => Ok(PeerMessage::Unchoke),
            PeerMessage::INTERESTED => Ok(PeerMessage::Interested),
            PeerMessage::NOT_INTERESTED => Ok(PeerMessage::NotInterested),
            PeerMessage::HAVE => Ok(PeerMessage::Have {
                index: bytes.get_i32(),
            }),
            PeerMessage::BITFIELD => {
                log::debug!("Bitfield received");
                let bits = BitSlice::<_, Msb0>::try_from_slice(bytes).unwrap();
                Ok(PeerMessage::Bitfield(bits))
            }
            PeerMessage::REQUEST => {
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
            PeerMessage::PIECE => Ok(PeerMessage::Piece {
                index: bytes.get_i32(),
                begin: bytes.get_i32(),
                piece: bytes.get_i32(),
                data: bytes,
            }),
            PeerMessage::CANCEL => Ok(PeerMessage::Cancel {
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

    use tokio_uring::net::TcpListener;

    use super::*;
    #[test]
    fn it_works() {
        let torrent = std::fs::read("../test.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
        println!("infohash: {:?}", metainfo.info().info_hash());
        println!("pices: {}", metainfo.info().pieces().count());
        println!("files: {}", metainfo.info().files().count());
        for piece in metainfo.info().pieces() {
            println!("Piece size: {}", piece.len());
        }
    }

    fn setup_test() -> (TorrentManager, [u8; 20], SocketAddr) {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let torrent = std::fs::read("../test.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
        let info_hash: [u8; 20] = metainfo.info().info_hash().try_into().unwrap();

        let port: u16 = (rand::random::<f32>() * (u16::MAX - 2000) as f32) as u16 + 2000;
        let addr = format!("127.0.0.1:{port}").parse().unwrap();
        let info_hash_clone = info_hash;
        std::thread::spawn(move || {
            let listener = TcpListener::bind(addr).unwrap();
            tokio_uring::start(async move {
                let (connection, _conn_addr) = listener.accept().await.unwrap();
                let buf = vec![0u8; 68];
                let (bytes_read, buf) = connection.read(buf).await;
                let bytes_read = bytes_read.unwrap();
                assert_eq!(bytes_read, 68);
                assert_eq!(
                    PeerConnection::handshake(info_hash_clone, [0; 20]),
                    buf[..bytes_read]
                );

                let buf = PeerConnection::handshake(info_hash, [1; 20]).to_vec();
                let (res, _buf) = connection.write(buf).await;
                res.unwrap();
                log::info!("Connected!");
            });
        });

        let torrent_manager = TorrentManager::new(metainfo.info().clone());
        (torrent_manager, info_hash, addr)
    }

    #[test]
    fn test_incoming_choke() {
        let (torrent_manager, info_hash, addr) = setup_test();

        let state = torrent_manager.torrent_state;
        tokio_uring::start(async move {
            let connection = PeerConnection::new(addr, [0; 20], [1; 20], info_hash, state)
                .await
                .unwrap();
            connection
                .process_incoming(PeerMessage::Choke)
                .await
                .unwrap();
            assert!(connection.state().peer_choking)
        });
    }
}
