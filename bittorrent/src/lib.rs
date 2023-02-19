// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::{
    cell::RefCell,
    io::{Cursor, Write},
    net::SocketAddr,
    ops::Deref,
    rc::{Rc, Weak},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use bitvec::prelude::*;
use bytes::Buf;
use disk_io::FileHandle;
use parking_lot::Mutex;
use peer_connection::PeerConnection;
use piece_selector::PieceSelector;
use sha1::{Digest, Sha1};
use slotmap::{new_key_type, DenseSlotMap, SecondaryMap};
use tokio::sync::oneshot;
use tokio_uring::net::{TcpListener, TcpStream};

const SUBPIECE_SIZE: i32 = 16_384;

pub mod disk_io;
pub mod peer_connection;
pub mod peer_message;
pub mod piece_selector;

new_key_type! {
    pub struct PeerKey;
}

#[derive(Clone)]
struct PeerList {
    peer_connection_states: Rc<RefCell<DenseSlotMap<PeerKey, PeerConnection>>>,
    addrs: Arc<Mutex<SecondaryMap<PeerKey, SocketAddr>>>,
    our_peer_id: [u8; 20],
}

impl PeerList {
    pub fn new(our_peer_id: [u8; 20]) -> Self {
        Self {
            peer_connection_states: Default::default(),
            addrs: Default::default(),
            our_peer_id,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.peer_connection_states.borrow().is_empty()
    }

    pub fn insert_peer(&self, addr: SocketAddr, connection: PeerConnection) {
        let peer_key = self.peer_connection_states.borrow_mut().insert(connection);
        self.addrs.lock().insert(peer_key, addr);
    }

    pub fn handle(&self, torrent_state: Weak<RefCell<TorrentState>>) -> PeerListHandle {
        let (tx, mut rc) = tokio::sync::mpsc::unbounded_channel();
        let this = self.clone();
        tokio_uring::spawn(async move {
            while let Some(addr) = rc.recv().await {
                if let Ok(stream) = TcpStream::connect(addr).await {
                    if let Some(state) = torrent_state.upgrade() {
                        let our_id = this.our_peer_id;
                        let num_pieces = state.borrow().piece_selector.pieces();
                        let info_hash = state.borrow().torrent_info.info_hash();
                        let peer_connection = PeerConnection::new(
                            stream,
                            our_id,
                            info_hash.into(),
                            num_pieces,
                            torrent_state.clone(),
                        )
                        .await
                        .unwrap();
                        this.insert_peer(addr, peer_connection);
                    }
                } else {
                    log::warn!("Failed to connect to peer that was announced")
                }
            }
        });
        PeerListHandle {
            peer_sender: tx,
            addrs: self.addrs.clone(),
        }
    }
}

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

//#[cfg(test)]
//mod test;

// Perhaps also create a subpiece type that can be converted into a peer request
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

pub struct TorrentState {
    pub torrent_info: bip_metainfo::Info,
    pub on_subpiece_callback: Option<Box<dyn FnMut(&[u8])>>,
    file_handle: FileHandle,
    download_rc: Option<oneshot::Receiver<()>>,
    download_tx: Option<oneshot::Sender<()>>,
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


    pub(crate) fn on_piece_request(
        &mut self,
        index: i32,
        begin: i32,
        length: i32,
    ) -> anyhow::Result<Vec<u8>> {
        // TODO: Take choking into account
        //let piece_size = self.piece_selector.piece_len(index) as i32;
        //anyhow::ensure!(piece_size >= length);
        /*if *self
            .completed_pieces
            .get(index as usize)
            .as_deref()
            .unwrap_or(&false)
        {
            log::info!("Piece is available!");
            unimplemented!()
            //self.file_handle.write(offset, bytes)
            /*if self.pretended_file.len()
                < ((index as u64 * piece_size) + begin as u64 + length as u64) as usize
            {
                anyhow::bail!("Invalid piece request, out of bounds of file");
            }
            let mut data = vec![0; length as usize];
            let mut cursor = Cursor::new(std::mem::take(&mut self.pretended_file));
            cursor.set_position(piece_size * index as u64);
            cursor.copy_to_slice(&mut data);
            self.pretended_file = cursor.into_inner();

            Ok(data)*/
        } else {
            anyhow::bail!("Piece requested isn't available");
        }*/
        todo!()
    }

    pub(crate) fn on_piece_completed(&mut self, index: i32, data: Vec<u8>) {
        let mut hasher = Sha1::new();
        hasher.update(&data);
        let data_hash = hasher.finalize();
        // The hash can be provided to the data storage or the peer connection
        // when the piece is requested so it can be used for validation later on
        let position = self
            .torrent_info
            .pieces()
            .position(|piece_hash| data_hash.as_slice() == piece_hash);
        match position {
            Some(piece_index) if piece_index == index as usize => {
                log::info!("Piece hash matched downloaded data");
                self.piece_selector.mark_complete(piece_index);
                self.file_handle.write(self.torrent_info.piece_length() * index as u64, data).unwrap();

                // Purge disconnected peers 
                let mut disconnected_peers:Vec<PeerKey> = self.peer_list.peer_connection_states.borrow().iter().filter_map(|(peer_key,peer)| peer.have(index).map(|_| peer_key).ok()).collect();

                if self.piece_selector.completed() {
                    log::info!("Torrent completed!");
                    self.file_handle.close().unwrap();
                    self.download_tx.take().unwrap().send(()).unwrap();
                    return;
                }
                // TODO use a proper piece strategy here 
               for _ in 0..5 {
                    if let Some(next_piece) = self.piece_selector.next_piece(&self.peer_list) {
                        // only peers that haven't choked us and that aren't currently downloading. 
                        // At least one peer must be available here to download, it might not have
                        // the desired piece though.
                        let peer_connections = self.peer_list.peer_connection_states.borrow();
                        for (peer_key,peer) in peer_connections.iter().filter(|(_,peer)| !peer.state().peer_choking && peer.state().currently_downloading.is_none()) {
                            if peer.state().peer_pieces[next_piece as usize] {
                                if peer.state().is_choking {
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
                                    self.piece_selector.mark_inflight(next_piece as usize);
                                    return;
                                }
                            }
                        }
                    } else {
                       log::error!("No piece can be downloaded from any peer"); 
                        return;
                    }
                }
               self
                .peer_list.peer_connection_states.borrow_mut()
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
}

#[derive(Clone)]
pub struct TorrentManager {
    pub torrent_info: Arc<bip_metainfo::Info>,
    // TODO create newtype
    our_peer_id: [u8; 20],
    pub torrent_state: Rc<RefCell<TorrentState>>,
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

// How many peers do we aim to have unchoked overtime
const UNCHOKED_PEERS: usize = 4;

impl TorrentManager {
    pub fn new(torrent_info: bip_metainfo::Info) -> Self {
        assert!(torrent_info.files().count() == 1);
        let file = torrent_info.files().next().unwrap();
        let file_handle = FileHandle::new(file.path().to_path_buf());
        let piece_selector = PieceSelector::new(&torrent_info);
        let our_peer_id = generate_peer_id();
        let peer_list = PeerList::new(our_peer_id);
        let (tx, rc) = tokio::sync::oneshot::channel();
        let torrent_state = TorrentState {
            num_unchoked: 0,
            max_unchoked: UNCHOKED_PEERS as u32,
            on_subpiece_callback: None,
            file_handle,
            download_rc: Some(rc),
            download_tx: Some(tx),
            torrent_info: torrent_info.clone(),
            piece_selector,
            peer_list,
        };
        Self {
            our_peer_id,
            torrent_info: Arc::new(torrent_info),
            torrent_state: Rc::new(RefCell::new(torrent_state)),
        }
    }

    pub fn peer_list_handle(&self) -> PeerListHandle {
        let weak = Rc::downgrade(&self.torrent_state);
        self.torrent_state.borrow().peer_list.handle(weak)
    }

    pub fn set_subpiece_callback(&self, callback: impl FnMut(&[u8]) + 'static) {
        self.torrent_state.borrow_mut().on_subpiece_callback = Some(Box::new(callback));
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let mut disconnected_peers = Vec::new();
        let rc = {
            {
                let state = self.torrent_state.borrow();
                if state.peer_list.is_empty() {
                    anyhow::bail!("No peers to download from");
                }
                for (peer_key, peer) in state.peer_list.peer_connection_states.borrow().iter() {
                    if let Err(err) = peer.interested() {
                        log::error!("Peer disconnected: {err}");
                        disconnected_peers.push(peer_key);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(1500)).await;
            {
                let peer_list = self.torrent_state.borrow().peer_list.clone();
                for (peer_key, peer) in peer_list
                    .peer_connection_states
                    .borrow()
                    .iter()
                    .take(UNCHOKED_PEERS)
                {
                    if let Err(err) = peer.interested() {
                        log::error!("Peer disconnected: {err}");
                        disconnected_peers.push(peer_key);
                        continue;
                    }
                    let mut state = self.torrent_state.borrow_mut();
                    if let Some(piece_idx) = state.piece_selector.next_piece(&peer_list) {
                        let peer_owns_piece = peer.state().peer_pieces[piece_idx as usize];
                        if peer_owns_piece {
                            if let Err(err) = peer.unchoke() {
                                log::error!("Peer disconnected: {err}");
                                disconnected_peers.push(peer_key);
                                continue;
                            } else {
                                state.num_unchoked += 1;
                            }
                            if let Err(err) = peer
                                .request_piece(piece_idx, state.piece_selector.piece_len(piece_idx))
                            {
                                log::error!("Peer disconnected: {err}");
                                disconnected_peers.push(peer_key);
                                continue;
                            }
                            state.piece_selector.mark_inflight(piece_idx as usize);
                        }
                    } else {
                        log::warn!("No more pieces available");
                    }
                }
            }
            let mut state = self.torrent_state.borrow_mut();
            state
                .peer_list
                .peer_connection_states
                .borrow_mut()
                .retain(|peer_key, _| !disconnected_peers.contains(&peer_key));
            state.download_rc.take()
        };

        rc.unwrap().await?;
        Ok(())
    }

    pub async fn accept_incoming(&self, listener: &TcpListener) -> anyhow::Result<PeerConnection> {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        log::info!("Incomming peer connection: {peer_addr}");
        PeerConnection::new(
            stream,
            self.our_peer_id,
            self.torrent_info.info_hash().into(),
            self.torrent_info.pieces().count(),
            Rc::downgrade(&self.torrent_state),
        )
        .await
        .map(|peer_connection| {
            log::info!("Connection established: {peer_addr}");
            let peer_connection_clone = peer_connection.clone();
            self.torrent_state
                .borrow_mut()
                .peer_list
                .insert_peer(peer_addr, peer_connection_clone);
            peer_connection
        })
    }

    pub async fn add_peer(&self, addr: SocketAddr) -> anyhow::Result<PeerConnection> {
        let stream = TcpStream::connect(addr)
            .await
            .context("Failed to connect")?;
        let peer_connection = PeerConnection::new(
            stream,
            self.our_peer_id,
            self.torrent_info.info_hash().into(),
            self.torrent_info.pieces().count(),
            Rc::downgrade(&self.torrent_state),
        )
        .await?;

        self.torrent_state
            .borrow_mut()
            .peer_list
            .insert_peer(addr, peer_connection.clone());
        Ok(peer_connection)
    }
    
}

/*#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn next_piece() {
        let torrent = std::fs::read("final_test.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
        let pieces: BitBox<u8, Msb0> = (0..10).map(|_| false).collect();
        let state = TorrentState {
            completed_pieces: pieces.clone(),
            inflight_pieces: pieces.clone(),
            peer_connections: ,
            pretended_file:Default::default(),
            torrent_info: metainfo.info().clone(),
            last_piece_len: 0,
            downloaded: Default::default(),
            download_rc:Default::default(),
            download_tx:  Default::default(),
            max_unchoked: Default::default(),
            num_unchoked: Default::default(),
        };

    }
}*/

// Peer info needed:
// up/download rate
// haves is choked/interestead or not

// Torrentmanager
// tracks peers "haves"
// includes piece stategy
// chokes and unchokes
// owns peer connections and connections have weak ref back?
// includes mmapped file(s)?
//
// TorrentManager owns peer connections and peers have weak ref back
// pro: peer connection state and torrent state is accessible to both synchronously
// con: locking needed

// Start with this!
// TorrentManager owns peer connection state, connection on separate thread keeps channel to
// communicate. Separate thread sends back parsed messages and recieved messages to be sent out
// pro: clean separation, less locking most likely
// con: More cloning? async might not always be desired

// TorrentDownloadManager
// 1. Get meta data about pieces and info hashes
// 2. Fetch peers from DHT for the pieces
// 3. Connect to all peers
// 4. Do piece selection and distribute pieces across peers that have them
// 5. PeerConnection requests subpieces automatically
// 6. Manager is informed about pieces that have completed (and peer choking us)
