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

use anyhow::Context;
use bitvec::prelude::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_uring::net::TcpStream;

struct Piece {
    index: i32,
    completed_subpieces: BitBox,
    inflight_subpieces: BitBox,
    last_subpiece_length: i32,
    // TODO this should be a memory mapped region in
    // the actual file
    memory: Vec<u8>,
}

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
    piece: Option<Piece>,
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
            piece: None,
        }
    }
}

pub struct TorrentState {
    completed_pieces: BitBox<u8, Msb0>,
    pretended_file: Vec<u8>,
    max_unchoked: u32,
    num_unchoked: u32,
}

impl TorrentState {
    fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }
}

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
        assert!(torrent_info.files().count() == 1);
        let file_lenght = torrent_info.files().next().unwrap().length();
        let torrent_state = TorrentState {
            completed_pieces,
            num_unchoked: 0,
            max_unchoked,
            pretended_file: vec![0; file_lenght as usize],
        };
        Self {
            torrent_info,
            peer_connections: Vec::new(),
            torrent_state: Arc::new(Mutex::new(torrent_state)),
        }
    }

    pub fn add_peer(&mut self, addr: SocketAddr, our_id: [u8; 20], peer_id: [u8; 20]) {
        // Connect first perhaps so errors can be handled
        let (sender, receiver) = tokio::sync::mpsc::channel(256);
        let peer_handle = PeerConnectionHandle { peer_id, sender };
        let info_hash = self.torrent_info.info_hash().into();
        let state_clone = self.torrent_state.clone();
        std::thread::spawn(move || {
            tokio_uring::start(async move {
                let mut peer_connection =
                    PeerConnection::new(addr, our_id, peer_id, info_hash, state_clone, receiver)
                        .await
                        .unwrap();

                peer_connection.connection_send_loop().await.unwrap();
            })
        });
        self.peer_connections.push(peer_handle);
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

// Operations the manager can request of the peer connection
pub enum PeerOrder {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    RequestPiece { index: i32, total_len: u32 },
    SendHave(i32),
}

pub struct PeerConnection {
    // TODO use utp
    stream: Rc<TcpStream>,
    state: Rc<RefCell<PeerConnectionState>>,
    torrent_state: Arc<Mutex<TorrentState>>,
    // ew
    send_queue: Option<Receiver<PeerOrder>>,
}

// this isn't super nice
impl Clone for PeerConnection {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            state: self.state.clone(),
            torrent_state: self.torrent_state.clone(),
            send_queue: None,
        }
    }
}

const SUBPIECE_SIZE: i32 = 16_000;

impl PeerConnection {
    pub async fn new(
        addr: SocketAddr,
        our_id: [u8; 20],
        their_id: [u8; 20],
        info_hash: [u8; 20],
        torrent_state: Arc<Mutex<TorrentState>>,
        send_queue: Receiver<PeerOrder>,
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
                send_queue: Some(send_queue),
            };
            let connection_clone = connection.clone();
            // TODO Handle shutdowns and move out to separate function
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

    // TODO: unify all of these
    async fn choke(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Choke;
        let (result, _buf) = self.stream.write_all(msg.to_bytes()).await;
        result.context("Failed to write choke msg")
    }

    async fn unchoke(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Unchoke;
        let (result, _buf) = self.stream.write_all(msg.to_bytes()).await;
        result.context("Failed to write unchoke msg")
    }

    async fn interested(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Interested;
        let (result, _buf) = self.stream.write_all(msg.to_bytes()).await;
        result.context("Failed to write interested msg")
    }

    async fn not_interested(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::NotInterested;
        let (result, _buf) = self.stream.write_all(msg.to_bytes()).await;
        result.context("Failed to write not interested msg")
    }

    async fn have(&self, index: i32) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Have { index };
        let (result, _buf) = self.stream.write_all(msg.to_bytes()).await;
        result.context("Failed to write have msg")
    }

    async fn request(&self) -> anyhow::Result<()> {
        let mut bytes = BytesMut::new();
        {
            let mut state = self.state_mut();
            let piece = state.piece.as_mut().unwrap();
            // First subpiece that isn't already completed or inflight
            let mut unstarted_subpieces = piece.completed_subpieces.clone();
            unstarted_subpieces |= &piece.inflight_subpieces;
            // Max 5 packets per request
            for _ in 0..5 {
                if let Some(subindex) = unstarted_subpieces.first_zero() {
                    piece.inflight_subpieces.set(subindex, true);
                    unstarted_subpieces.set(subindex, true);
                    bytes.put(
                        PeerMessage::Request {
                            index: piece.index,
                            begin: subindex as i32 * SUBPIECE_SIZE as i32,
                            length: SUBPIECE_SIZE as i32,
                        }
                        .to_bytes(),
                    )
                }
            }
        }
        let (result, _buf) = self.stream.write_all(bytes.freeze()).await;
        result.context("Failed to write request(s) msg")
    }

    async fn connection_send_loop(&mut self) -> anyhow::Result<()> {
        let mut send_queue = self.send_queue.take().unwrap();
        while let Some(order) = send_queue.recv().await {
            match order {
                PeerOrder::Choke => self.choke().await?,
                PeerOrder::Unchoke => self.unchoke().await?,
                PeerOrder::Interested => self.interested().await?,
                PeerOrder::NotInterested => self.not_interested().await?,
                PeerOrder::RequestPiece { index, total_len } => {
                    {
                        let mut state = self.state_mut();
                        // Don't start on a new piece before the current one is completed
                        assert!(state.piece.is_none());
                        assert!(state.peer_pieces[index as usize]);
                        // extremely ineffective
                        let memory = vec![0; total_len as usize];
                        let subpieces = (total_len / SUBPIECE_SIZE as u32).max(1);
                        let completed_subpieces: BitBox = (0..subpieces).map(|_| false).collect();
                        let inflight_subpieces = completed_subpieces.clone();

                        let last_subpiece_length = if total_len as i32 % SUBPIECE_SIZE == 0 {
                            SUBPIECE_SIZE
                        } else {
                            total_len as i32 % SUBPIECE_SIZE
                        };
                        log::info!("Last subpiece lenght: {last_subpiece_length}");
                        state.piece = Some(Piece {
                            index,
                            completed_subpieces,
                            inflight_subpieces,
                            memory,
                            last_subpiece_length,
                        });
                    }
                    self.request().await?
                }
                PeerOrder::SendHave(index) => self.have(index).await?,
            }
        }
        Ok(())
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
                let is_choking = {
                    let mut state = self.state_mut();
                    log::info!("Peer is interested in us!");
                    state.peer_interested = true;
                    state.is_choking
                };
                if !is_choking {
                    // if we are not choking them we might need to send a
                    // unchoke to avoid some race conditions. Libtorrent
                    // uses the same type of logic
                    self.unchoke().await.unwrap();
                } else if self.torrent_state.lock().should_unchoke() {
                    log::debug!("Unchoking peer after intrest");
                    self.unchoke().await.unwrap();
                }
            }
            PeerMessage::NotInterested => {
                {
                    let mut state = self.state_mut();
                    log::info!("Peer is no longer interested in us!");
                    state.peer_interested = false;
                    state.is_choking = true;
                }
                self.choke().await.unwrap();
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
                unimplemented!()
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
                lenght,
                data,
            } => {
                log::info!("Recived a piece index: {index}, begin: {begin}, piece: {lenght}");
                log::info!("Data len: {}", data.len());
                let mut state = self.state_mut();
                if let Some(mut piece) = state.piece.take() {
                    let subpiece_index = begin / SUBPIECE_SIZE;
                    log::info!("Subpiece index received: {subpiece_index}");
                    let last_subpiece =
                        subpiece_index == (piece.completed_subpieces.len() - 1) as i32;
                    if last_subpiece {
                        log::info!("Last subpiece");
                        assert_eq!(lenght, piece.last_subpiece_length);
                    } else {
                        log::info!("Not last subpiece");
                        assert_eq!(lenght, SUBPIECE_SIZE);
                    }
                    piece.completed_subpieces.set(subpiece_index as usize, true);
                    piece.inflight_subpieces.set(subpiece_index as usize, false);
                    piece.memory[begin as usize..lenght as usize].copy_from_slice(data);
                    let piece_completed = piece.completed_subpieces.all();

                    if !piece_completed {
                        state.piece = Some(piece);
                    } else {
                        log::info!("Piece completed!");
                        drop(state);
                        let mut torrent_state = self.torrent_state.lock();
                        torrent_state.completed_pieces.set(index as usize, true);
                    }
                } else {
                    log::error!("Piece received before it was expected");
                }
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

#[derive(Debug, PartialEq, Eq)]
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
        lenght: i32,
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
                lenght: piece,
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
                lenght: bytes.get_i32(),
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

    use std::{future::Future, time::Duration};

    use tokio_uring::net::TcpListener;

    use super::*;
    #[test]
    fn it_works() {
        let torrent = std::fs::read("../test.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
        println!("infohash: {:?}", metainfo.info().info_hash());
        println!("pices: {}", metainfo.info().pieces().count());
        println!("files: {}", metainfo.info().files().count());
    }

    fn setup_test() -> (
        TorrentManager,
        PeerConnectionHandle,
        impl Future<Output = anyhow::Result<PeerConnection>>,
        Receiver<Vec<u8>>,
    ) {
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
        let (tx, rx) = tokio::sync::mpsc::channel(256);
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
                let mut buf = vec![0; 1024];
                loop {
                    let (res, used_buf) = connection.read(buf).await;
                    let bytes_read = res.unwrap();
                    if bytes_read == 0 || (tx.send(used_buf[..bytes_read].to_vec()).await).is_err()
                    {
                        break;
                    }
                    buf = used_buf;
                }
            });
        });

        let torrent_manager = TorrentManager::new(metainfo.info().clone(), 1);
        let (sender, receiver) = tokio::sync::mpsc::channel(256);
        let peer_handle = PeerConnectionHandle {
            peer_id: [1; 20],
            sender,
        };
        let state = torrent_manager.torrent_state.clone();
        let peer_connection =
            PeerConnection::new(addr, [0; 20], [1; 20], info_hash, state, receiver);
        (torrent_manager, peer_handle, peer_connection, rx)
    }

    #[test]
    fn test_incoming_choke() {
        let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            assert!(!connection.state().peer_choking);
            connection
                .process_incoming(PeerMessage::Choke)
                .await
                .unwrap();
            assert!(connection.state().peer_choking)
        });
    }

    // TODO: Test actions within the torrent manager here
    #[test]
    fn test_incoming_unchoke_when_not_interested() {
        let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            assert!(!connection.state().peer_choking);
            assert!(!connection.state().is_interested);
            connection
                .process_incoming(PeerMessage::Choke)
                .await
                .unwrap();
            assert!(connection.state().peer_choking);
            connection
                .process_incoming(PeerMessage::Unchoke)
                .await
                .unwrap();
            assert!(!connection.state().peer_choking);
        });
    }

    #[test]
    fn test_incoming_interestead_when_not_choking() {
        let (torrent_manager, _peer_handle, peer_connection_fut, mut sent_data) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            assert!(!connection.state().peer_choking);
            assert!(!connection.state().is_interested);
            connection.state_mut().is_choking = false;
            torrent_manager.torrent_state.lock().num_unchoked += 1;
            connection
                .process_incoming(PeerMessage::Interested)
                .await
                .unwrap();
            let data = sent_data.recv().await.unwrap();
            let msg = PeerMessage::try_from(data.as_slice()).unwrap();
            assert_eq!(msg, PeerMessage::Unchoke);
            connection
                .stream
                .shutdown(std::net::Shutdown::Both)
                .unwrap();
        });
    }

    #[test]
    fn test_incoming_interestead_when_choking_with_free_unchoke_spots() {
        let (torrent_manager, _peer_handle, peer_connection_fut, mut sent_data) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            assert!(!connection.state().peer_choking);
            assert!(!connection.state().is_interested);
            connection
                .process_incoming(PeerMessage::Interested)
                .await
                .unwrap();
            // There is one free unchoke spot left
            let num_unchoked = torrent_manager.torrent_state.lock().num_unchoked;
            let max_unchoked = torrent_manager.torrent_state.lock().max_unchoked;
            assert!(num_unchoked < max_unchoked);
            let data = sent_data.recv().await.unwrap();
            let msg = PeerMessage::try_from(data.as_slice()).unwrap();
            assert_eq!(msg, PeerMessage::Unchoke);
            // Needed to prevent dead lock
            connection
                .stream
                .shutdown(std::net::Shutdown::Both)
                .unwrap();
        });
    }

    #[test]
    fn test_incoming_interestead_when_choking_without_free_unchoke_spots() {
        let (torrent_manager, _peer_handle, peer_connection_fut, mut sent_data) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            assert!(!connection.state().peer_choking);
            assert!(!connection.state().is_interested);
            torrent_manager.torrent_state.lock().num_unchoked += 1;
            connection
                .process_incoming(PeerMessage::Interested)
                .await
                .unwrap();
            // There are no free unchoke spot left
            let num_unchoked = torrent_manager.torrent_state.lock().num_unchoked;
            let max_unchoked = torrent_manager.torrent_state.lock().max_unchoked;
            assert!(num_unchoked >= max_unchoked);
            let timeout = tokio::time::timeout(Duration::from_secs(3), sent_data.recv()).await;
            match timeout {
                Err(_) => {}
                Ok(_) => panic!("should timeout"),
            }
            connection
                .stream
                .shutdown(std::net::Shutdown::Both)
                .unwrap();
        });
    }

    // TODO: Test actions within the torrent manager here
    #[test]
    fn test_incoming_bitfield() {
        let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            let mut bitfield_one: BitBox<u8, Msb0> = connection.state().peer_pieces.clone();
            let mut bitfield_two: BitBox<u8, Msb0> = connection.state().peer_pieces.clone();
            assert_eq!(bitfield_one.leading_zeros(), bitfield_one.len());
            bitfield_one.set(7, true);
            bitfield_one.set(100, true);
            connection
                .process_incoming(PeerMessage::Bitfield(bitfield_one.clone().as_ref()))
                .await
                .unwrap();
            assert_eq!(connection.state().peer_pieces, bitfield_one);
            bitfield_two.set(3, true);
            connection
                .process_incoming(PeerMessage::Bitfield(bitfield_two.as_ref()))
                .await
                .unwrap();
            assert_eq!(connection.state().peer_pieces, bitfield_one | bitfield_two);
        });
    }

    #[test]
    fn test_incoming_have() {
        let (torrent_manager, _peer_handle, peer_connection_fut, _) = setup_test();

        tokio_uring::start(async move {
            let connection = peer_connection_fut.await.unwrap();
            assert!(!connection.state().peer_pieces.get(5).unwrap());
            connection
                .process_incoming(PeerMessage::Have { index: 5 })
                .await
                .unwrap();
            assert!(connection.state().peer_pieces.get(5).unwrap());
            connection
                .process_incoming(PeerMessage::Have { index: 7 })
                .await
                .unwrap();
            assert!(connection.state().peer_pieces.get(5).unwrap());
            assert!(connection.state().peer_pieces.get(7).unwrap());
        });
    }
}
