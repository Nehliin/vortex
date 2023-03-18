// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::{net::SocketAddr, path::Path, sync::Arc, time::Instant, borrow::BorrowMut};

use anyhow::Context;
use bitvec::prelude::*;
use lava_torrent::torrent::v1::Torrent;
use parking_lot::Mutex;
use peer_connection::PeerConnection;
use peer_events::{PeerEvent, PeerEventType};
use piece_selector::PieceSelector;
use sha1::{Digest, Sha1};
use slotmap::{new_key_type, DenseSlotMap, Key, SecondaryMap};
use tokio::sync::oneshot;
use tokio_uring::net::{TcpListener, TcpStream};

use crate::drive_io::FileStore;

const SUBPIECE_SIZE: i32 = 16_384;

pub mod drive_io;
pub mod peer_connection;
pub mod peer_events;
pub mod peer_message;
pub mod piece_selector;

new_key_type! {
    pub struct PeerKey;
}

struct PeerList {
    connections: DenseSlotMap<PeerKey, PeerConnection>,
    addrs: Arc<Mutex<SecondaryMap<PeerKey, SocketAddr>>>,
}

impl PeerList {
    pub fn new() -> Self {
        Self {
            connections: Default::default(),
            addrs: Default::default(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    // TODO rename
    pub fn insert_peer(
        &mut self,
        addr: SocketAddr,
        our_peer_id: [u8; 20],
        info_hash: [u8; 20],
        peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
    ) {
        log::debug!("Inserting peer: {addr}");
        let peer_key = self.connections.insert_with_key(|peer_key| {
            let connection =
                PeerConnection::new(peer_key, addr, peer_event_sender.clone()).unwrap();
            connection.connect(our_peer_id, info_hash).unwrap();
            connection
        });

        // TODO: Only do this when connection is completed?
        self.addrs.lock().insert(peer_key, addr);
    }

    pub fn handle(
        &self,
        peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
    ) -> PeerListHandle {
        let (tx, mut rc) = tokio::sync::mpsc::unbounded_channel();
        // TODO consider cutting out the middleman if no new event is needed (other than peer event)
        tokio_uring::spawn(async move {
            while let Some(addr) = rc.recv().await {
                peer_event_sender
                    .send(PeerEvent {
                        peer_key: PeerKey::null(),
                        event_type: PeerEventType::NewConnection { addr },
                    })
                    .await
                    .unwrap();
            }
        });
        PeerListHandle {
            peer_sender: tx,
            addrs: self.addrs.clone(),
        }
    }
}

#[derive(Clone)]
pub struct PeerListHandle {
    peer_sender: tokio::sync::mpsc::UnboundedSender<SocketAddr>,
    addrs: Arc<Mutex<SecondaryMap<PeerKey, SocketAddr>>>,
}

impl PeerListHandle {
    pub fn insert(&self, peer: SocketAddr) {
        self.peer_sender.send(peer).unwrap()
    }

    pub fn peers(&self) -> Vec<SocketAddr> {
        self.addrs.lock().values().copied().collect()
    }
}

#[derive(Debug)]
pub struct Piece {
    index: i32,
    // Contains only completed subpieces
    completed_subpieces: BitBox,
    // Contains both completed and inflight subpieces
    inflight_subpieces: BitBox,
    last_subpiece_length: i32,
    // TODO used uninit memory here instead
    memory: Vec<u8>,
}

impl Piece {
    fn new(index: i32, lenght: u32) -> Self {
        let memory = vec![0; lenght as usize];
        let last_subpiece_length = if lenght as i32 % SUBPIECE_SIZE == 0 {
            SUBPIECE_SIZE
        } else {
            lenght as i32 % SUBPIECE_SIZE
        };
        let subpieces =
            (lenght / SUBPIECE_SIZE as u32) + u32::from(last_subpiece_length != SUBPIECE_SIZE);
        let completed_subpieces: BitBox = (0..subpieces).map(|_| false).collect();
        let inflight_subpieces = completed_subpieces.clone();
        Self {
            index,
            completed_subpieces,
            inflight_subpieces,
            last_subpiece_length,
            memory,
        }
    }

    fn on_subpiece(&mut self, index: i32, begin: i32, data: &[u8]) {
        // This subpice is part of the currently downloading piece
        assert_eq!(self.index, index);
        let subpiece_index = begin / SUBPIECE_SIZE;
        log::trace!("Subpiece index received: {subpiece_index}");
        let last_subpiece = subpiece_index == self.last_subpiece_index();
        if last_subpiece {
            assert_eq!(data.len() as i32, self.last_subpiece_length);
        } else {
            assert_eq!(data.len() as i32, SUBPIECE_SIZE);
        }
        self.completed_subpieces.set(subpiece_index as usize, true);
        self.memory[begin as usize..begin as usize + data.len()].copy_from_slice(data);
    }

    // Perhaps this can return the subpice or a peer request directly?
    #[inline]
    fn next_unstarted_subpice(&self) -> Option<usize> {
        self.inflight_subpieces.first_zero()
    }

    #[inline]
    fn last_subpiece_index(&self) -> i32 {
        self.completed_subpieces.len() as i32 - 1
    }

    #[inline]
    fn is_complete(&self) -> bool {
        self.completed_subpieces.all()
    }
}

type OnSubpieceCallback = Box<dyn FnMut(&[u8])>;

pub struct TorrentState {
    torrent_info: Torrent,
    on_subpiece_callback: Option<OnSubpieceCallback>,
    file_store: FileStore,
    download_complete_tx: Option<oneshot::Sender<()>>,
    max_unchoked: u32,
    pub num_unchoked: u32,
    peer_list: PeerList,
    piece_selector: PieceSelector,
}

impl TorrentState {
    #[inline(always)]
    pub fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }

    pub(crate) async fn on_piece_request(
        &self,
        index: i32,
        begin: i32,
        length: i32,
    ) -> anyhow::Result<Vec<u8>> {
        let piece_len = self.piece_selector.piece_len(index) as i32;
        if begin < 0
            || begin > piece_len
            || length < 0
            || length > piece_len
            || index >= self.piece_selector.pieces() as i32
        {
            anyhow::bail!("Invalid piece request");
        }
        if length > SUBPIECE_SIZE {
            log::warn!("Incoming piece request for more than SUBPIECE_SIZE: {length}");
        }
        if self.piece_selector.has_completed(index as usize) {
            // TODO: No real need to read the entire piece here actually
            let mut piece_data = self.file_store.read_piece(index).await?;
            // TODO: inefficient, extra alloc here
            let mut piece_data = piece_data.split_off(begin as usize);
            piece_data.truncate(length as usize);
            Ok(piece_data)
        } else {
            anyhow::bail!("Piece requested isn't available");
        }
    }

    pub(crate) async fn on_piece_completed(&mut self, index: i32, data: Vec<u8>) {
        let hash_time = Instant::now();
        let mut hasher = Sha1::new();
        hasher.update(&data);
        let data_hash = hasher.finalize();
        let hash_time = hash_time.elapsed();
        log::info!("Piece hashed in: {} microsec", hash_time.as_micros());
        // The hash can be provided to the data storage or the peer connection
        // when the piece is requested so it can be used for validation later on
        let position = self
            .torrent_info
            .pieces
            .iter()
            .position(|piece_hash| data_hash.as_slice() == piece_hash);
        match position {
            Some(piece_index) if piece_index == index as usize => {
                log::info!("Piece hash matched downloaded data");
                self.piece_selector.mark_complete(piece_index);
                self.file_store.write_piece(index, data).await.unwrap();

                // Purge disconnected peers 
                let mut disconnected_peers:Vec<PeerKey> = self.peer_list.connections.iter().filter_map(|(peer_key,peer)| peer.have(index).map(|_| peer_key).ok()).collect();

                if self.piece_selector.completed_all() {
                    log::info!("Torrent completed!");
                    let file_store = std::mem::take(&mut self.file_store);
                    file_store.close().await.unwrap();
                    self.download_complete_tx.take().unwrap().send(()).unwrap();
                    return;
                }

                // TODO: this entire logic needs to be adapted to the new flow eventually
                let mut requested_piece = false;
                let available_peers = self.peer_list.connections.iter_mut()
                    .filter(|(_,peer)| !peer.state().peer_choking && peer.state().currently_downloading.is_none());
                for (peer_key,peer) in available_peers {
                    if let Some(next_piece) = self.piece_selector.next_piece(peer_key)  {
                        if peer.state().is_choking {
                            // TODO highly unclear if unchoke is desired here 
                            if let Err(err) = peer.unchoke() {
                                log::error!("{err}");
                                disconnected_peers.push(peer_key);
                                continue;
                            } else {
                                self.num_unchoked += 1;
                            }
                        }
                        // Group to a single operation
                        if let Err(err) = peer.request_piece(next_piece, self.piece_selector.piece_len(next_piece)) {
                            log::error!("{err}");
                            disconnected_peers.push(peer_key);
                            continue;
                        } else {
                            requested_piece = true;
                            self.piece_selector.mark_inflight(next_piece as usize);
                            break;
                        }
                    }
                }
                if requested_piece {
                    log::error!("No piece can be downloaded from any peer");
                }
               self
                .peer_list.connections
                .retain(|peer_key, _| !disconnected_peers.contains(&peer_key));
            }
            Some(piece_index) => log::error!(
                    "Piece hash didn't match expected index! expected index: {index}, piece_index: {piece_index}"
            ),
            None => {
                log::error!("Piece sha1 hash not found!");
            }
        }
    }

    async fn handle_event(
        &mut self,
        our_peer_id: &[u8; 20],
        peer_event: PeerEvent,
        // Need to propagate this when new connections are incoming
        peer_event_sender: &tokio::sync::mpsc::Sender<PeerEvent>,
    ) -> anyhow::Result<()> {
        macro_rules! connection_mut_or_return {
            () => {
                {
                    let Some(peer_connection) = self.peer_list.connections.get_mut(peer_event.peer_key) else {
                        log::warn!("received event for removed peer");
                        return Ok(());
                    };
                    peer_connection
                }

            }
        }
        match peer_event.event_type {
            // TODO: expected flow should probably convert NewConnecition event ->
            // ConnectionEstablished and have a channel interacting directly with
            // the network io thread
            PeerEventType::NewConnection { addr } => self.peer_list.insert_peer(
                addr,
                *our_peer_id,
                self.torrent_info.info_hash_bytes().try_into().unwrap(),
                peer_event_sender.clone(),
            ),
            PeerEventType::HandshakeComplete { peer_id, info_hash } => {
                let peer_connection = connection_mut_or_return!();
                peer_connection.peer_id = Some(peer_id);
                // TODO handle this more gracefully
                assert_eq!(&info_hash, self.torrent_info.info_hash_bytes().as_slice());
                log::info!("Handshake received");
                peer_connection.interested()?;
            }
            PeerEventType::Choked => {
                let peer_connection = connection_mut_or_return!();
                log::info!("Peer is choking us!");
                peer_connection.state_mut().peer_choking = true;
                // TODO clear outgoing requests
            }
            PeerEventType::Unchoke => {
                let peer_connection = connection_mut_or_return!();
                peer_connection.state_mut().peer_choking = false;
                if !peer_connection.state().is_interested {
                    // Not interested so don't do anything
                    return Ok(());
                }
                if let Some(piece_idx) = self.piece_selector.next_piece(peer_event.peer_key) {
                    if let Err(err) = peer_connection.unchoke() {
                        log::error!("Peer disconnected: {err}");
                        // TODO: cleaner fix here
                        self.peer_list
                            .connections
                            .remove(peer_event.peer_key)
                            .unwrap();
                        return Ok(());
                    } else {
                        self.num_unchoked += 1;
                    }
                    if let Err(err) = peer_connection
                        .request_piece(piece_idx, self.piece_selector.piece_len(piece_idx))
                    {
                        log::error!("Peer disconnected: {err}");
                        // TODO: cleaner fix here
                        self.peer_list
                            .connections
                            .remove(peer_event.peer_key)
                            .unwrap();
                        return Ok(());
                    }
                    self.piece_selector.mark_inflight(piece_idx as usize);
                } else {
                    log::warn!("No more pieces available from {:?}", peer_event.peer_key);
                }
            }
            PeerEventType::Intrest => {
                let should_unchoke = self.should_unchoke();
                let peer_connection = connection_mut_or_return!();
                log::info!("Peer is interested in us!");
                peer_connection.state_mut().peer_interested = true;
                if !peer_connection.state().is_choking {
                    // if we are not choking them we might need to send a
                    // unchoke to avoid some race conditions. Libtorrent
                    // uses the same type of logic
                    peer_connection.unchoke()?;
                } else if should_unchoke {
                    log::debug!("Unchoking peer after intrest");
                    peer_connection.unchoke()?;
                }
            }
            PeerEventType::NotInterested => {
                let peer_connection = connection_mut_or_return!();
                log::info!("Peer is no longer interested in us!");
                peer_connection.state_mut().peer_interested = false;
                peer_connection.choke()?;
            }
            PeerEventType::Have { index } => {
                log::info!("Peer have piece with index: {index}");
                self.piece_selector
                    .set_peer_piece(peer_event.peer_key, index as usize);
            }
            PeerEventType::Bitfield(field) => {
                if self.torrent_info.pieces.len() != field.len() {
                    // TODO: disconnect?
                    log::error!("Received invalid bitfield");
                    return Ok(());
                }
                log::info!("Bifield received: {field}");
                self.piece_selector
                    .update_peer_pieces(peer_event.peer_key, field);
            }
            PeerEventType::PieceRequest {
                index,
                begin,
                length,
            } => {
                let should_unchoke = self.should_unchoke();
                let peer_connection = connection_mut_or_return!();
                if should_unchoke && peer_connection.state().is_choking {
                    peer_connection.unchoke()?;
                }
                if !peer_connection.state().is_choking {
                    match self.on_piece_request(index, begin, length).await {
                        Ok(piece_data) => {
                            let peer_connection = connection_mut_or_return!();
                            peer_connection.piece(index, begin, piece_data)?;
                        }
                        Err(err) => log::error!("Invalid piece request: {err}"),
                    }
                } else {
                    log::info!("Piece request ignored, peer can't be unchoked");
                }
            }
            PeerEventType::PieceRequestSucceeded(piece) => {
                log::debug!("Piece completed!");
                self.on_piece_completed(piece.index, piece.memory).await;
            }
            PeerEventType::PieceRequestFailed => todo!(),
        }
        Ok(())
    }
}

pub struct TorrentManager {
    pub torrent_info: Arc<Torrent>,
    // Keeping this bounded have the neat benefit of
    // automatically throttling the peer connection threads
    // from overloading the "main" thread
    peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
    peer_list_handle: PeerListHandle,
    download_complete: Option<tokio::sync::oneshot::Receiver<()>>,
}

fn generate_peer_id() -> [u8; 20] {
    // Based on http://www.bittorrent.org/beps/bep_0020.html
    const PREFIX: [u8; 8] = *b"-VT0010-";
    let generatated = rand::random::<[u8; 12]>();
    let mut result: [u8; 20] = [0; 20];
    result[0..8].copy_from_slice(&PREFIX);
    result[8..].copy_from_slice(&generatated);
    result
}

async fn read_all(path: &Path) -> anyhow::Result<Vec<u8>> {
    let file = tokio_uring::fs::File::open(path).await?;
    let metadata = file.statx().await?;
    let file_size = metadata.stx_size as usize;
    let buf = vec![0; file_size];
    let (result, buf) = file.read_exact_at(buf, 0).await;
    result?;
    Ok(buf)
}

// How many peers do we aim to have unchoked overtime
const UNCHOKED_PEERS: usize = 4;

impl TorrentManager {
    pub async fn new(torrent_file: impl AsRef<Path>) -> Self {
        let torrent_bytes = read_all(torrent_file.as_ref()).await.unwrap();
        let torrent_info = Torrent::read_from_bytes(&torrent_bytes).unwrap();

        let file_store = FileStore::new("downloaded", &torrent_info).await.unwrap();
        let piece_selector = PieceSelector::new(&torrent_info);
        let our_peer_id = generate_peer_id();
        let peer_list = PeerList::new();
        let (tx, rc) = tokio::sync::oneshot::channel();
        let (peer_event_tx, mut peer_event_rc) = tokio::sync::mpsc::channel(512);
        let torrent_info_clone = torrent_info.clone();
        let peer_event_tx_clone = peer_event_tx.clone();
        let peer_list_handle = peer_list.handle(peer_event_tx.clone());
        tokio_uring::spawn(async move {
            let mut torrent_state = TorrentState {
                num_unchoked: 0,
                max_unchoked: UNCHOKED_PEERS as u32,
                on_subpiece_callback: None,
                file_store,
                download_complete_tx: Some(tx),
                torrent_info: torrent_info_clone,
                piece_selector,
                peer_list,
            };

            while let Some(event) = peer_event_rc.recv().await {
                // Spawn as separate tasks potentially
                torrent_state
                    .handle_event(&our_peer_id, event, &peer_event_tx_clone)
                    .await;
            }
        });

        Self {
            torrent_info: Arc::new(torrent_info),
            peer_event_sender: peer_event_tx,
            download_complete: Some(rc),
            peer_list_handle,
        }
    }

    pub fn peer_list_handle(&self) -> PeerListHandle {
        //let weak = Rc::downgrade(&self.torrent_state);
        //self.torrent_state.borrow().peer_list.handle(weak)
        self.peer_list_handle.clone()
    }

    pub fn set_subpiece_callback(&self, callback: impl FnMut(&[u8]) + 'static) {
        //self.torrent_state.borrow_mut().on_subpiece_callback = Some(Box::new(callback));
    }

    pub async fn download_complete(&mut self) {
        if let Some(download_waiter) = self.download_complete.take() {
            download_waiter.await.unwrap();
        }
    }

    // Fix support via dedicated IO thread instead
    /*pub async fn accept_incoming(&self, listener: &TcpListener) -> anyhow::Result<()> {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        log::info!("Incomming peer connection: {peer_addr}");
        self.peer_event_sender
            .send(PeerEvent {
                peer_key: PeerKey::null(),
                event_type: PeerEventType::NewConnection {
                    stream: SendableStream(stream),
                    addr: peer_addr,
                },
            })
            .await
            .unwrap();
        Ok(())
    }*/
}
