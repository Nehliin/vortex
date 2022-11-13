// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::{
    cell::RefCell,
    io::{Cursor, Write},
    net::SocketAddr,
    rc::Rc,
    sync::Arc, time::Duration,
};

use bitvec::prelude::*;
use bytes::Buf;
use peer_connection::PeerConnection;
use sha1::{Digest, Sha1};
use tokio::sync::oneshot;
use tokio_uring::net::{TcpListener, TcpStream};

const SUBPIECE_SIZE: i32 = 16_384;

pub mod peer_connection;
pub mod peer_message;

//#[cfg(test)]
//mod test;

// Perhaps also create a subpiece type that can be converted into a peer request
#[derive(Debug)]
struct Piece {
    index: i32,
    // Contains only completed subpieces
    completed_subpieces: BitBox,
    // Contains both completed and inflight subpieces
    inflight_subpieces: BitBox,
    last_subpiece_length: i32,
    // TODO this should be a memory mapped region in
    // the actual file
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

    fn on_subpiece(&mut self, index: i32, begin: i32, length: i32, data: &[u8]) {
        // This subpice is part of the currently downloading piece
        assert_eq!(self.index, index);
        let subpiece_index = begin / SUBPIECE_SIZE;
        log::info!("Subpiece index received: {subpiece_index}");
        let last_subpiece = subpiece_index == self.last_subpiece_index();
        if last_subpiece {
            log::info!("Last subpiece");
            assert_eq!(length, self.last_subpiece_length);
        } else {
            log::info!("Not last subpiece");
            assert_eq!(length, SUBPIECE_SIZE);
        }
        assert_eq!(data.len(), length as usize);
        self.completed_subpieces.set(subpiece_index as usize, true);
        self.memory[begin as usize..begin as usize + data.len() as usize].copy_from_slice(data);
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
    pub completed_pieces: BitBox<u8, Msb0>,
    pub inflight_pieces: BitBox<u8, Msb0>,
    pub pretended_file: Vec<u8>,
    pub torrent_info: bip_metainfo::Info,
    last_piece_len: u64,
    // Temp
    downloaded: usize,
    download_rc: Option<oneshot::Receiver<()>>,
    download_tx: Option<oneshot::Sender<()>>,
    max_unchoked: u32,
    num_unchoked: u32,
    peer_connections: Vec<PeerConnection>,
}

impl TorrentState {
    #[inline(always)]
    pub(crate) fn should_unchoke(&self) -> bool {
        self.num_unchoked < self.max_unchoked
    }

    // Returnes the next piece that can be downloaded
    // from the current connected peers based on current state.
    // Starts by picking random and then transitions to rarest first
    fn next_piece(&self) -> Option<i32> {
        let pieces_left = self.completed_pieces.count_zeros();
        if pieces_left == 0 {
            log::info!("Torrent is completed, no next piece found");
            return None;
        }
        // All pieces we haven't downloaded that peers have
        let mut available_pieces: BitBox<u8, Msb0> =
            (0..self.completed_pieces.len()).map(|_| false).collect();

        for peer in self.peer_connections.iter() {
            available_pieces |= &peer.state().peer_pieces;
        }
        // Get the available pieces - all already completed or inflight pieces
        let mut tmp = self.completed_pieces.clone();
        tmp |= &self.inflight_pieces;
        available_pieces &= !tmp;

        if available_pieces.not_any() {
            log::error!("There are no available pieces!");
            return None;
        }

        let procentage_left = pieces_left as f32 / self.completed_pieces.len() as f32;

        if procentage_left > 0.95 {
            loop {
                let index = (rand::random::<f32>() * self.completed_pieces.len() as f32) as usize;
                log::info!("Picking random piece to download, index: {index}");
                if available_pieces[index] {
                    return Some(index as i32);
                }
            }
        } else {
            // Rarest first
            let mut count = vec![0; available_pieces.len()];
            for available in available_pieces.iter_ones() {
                for peer in self.peer_connections.iter() {
                    if peer.state().peer_pieces[available] {
                        count[available] += 1;
                    }
                }
            }
            let index = count.into_iter().enumerate().filter(|(_pos, count)| count > &0).min_by_key(|(_pos, val)| *val).map(|(pos, _)| pos as i32);
            log::info!("Picking rarest piece to download, index: {index:?}");
            index
        }
    }

    // TODO fixme
    fn piece_length(&self, index: i32) -> u32 {
        if self.torrent_info.pieces().count() == (index as usize + 1) {
            self.last_piece_len as u32
        } else {
            self.torrent_info.piece_length() as u32
        }
    }

    pub(crate) fn on_piece_request(
        &mut self,
        index: i32,
        begin: i32,
        length: i32,
    ) -> anyhow::Result<Vec<u8>> {
        // TODO: Take choking into account
        let piece_size = self.torrent_info.piece_length();
        if *self
            .completed_pieces
            .get(index as usize)
            .as_deref()
            .unwrap_or(&false)
        {
            log::info!("Piece is available!");
            if self.pretended_file.len()
                < ((index as u64 * piece_size) + begin as u64 + length as u64) as usize
            {
                anyhow::bail!("Invalid piece request, out of bounds of file");
            }
            let mut data = vec![0; length as usize];
            let mut cursor = Cursor::new(std::mem::take(&mut self.pretended_file));
            cursor.set_position(piece_size * index as u64);
            cursor.copy_to_slice(&mut data);
            self.pretended_file = cursor.into_inner();

            Ok(data)
        } else {
            anyhow::bail!("Piece requested isn't available");
        }
    }

    pub(crate) fn on_piece_completed(&mut self, index: i32, data: Vec<u8>) {
        let mut hasher = Sha1::new();
        hasher.update(&data);
        let data_hash = hasher.finalize();
        let position = self
            .torrent_info
            .pieces()
            .position(|piece_hash| data_hash.as_slice() == piece_hash);
        match position {
            Some(piece_index) if piece_index == index as usize => {
                log::info!("Piece hash matched downloaded data");
                self.completed_pieces.set(piece_index, true);
                self.inflight_pieces.set(piece_index, false);
                let mut cursor = Cursor::new(std::mem::take(&mut self.pretended_file));
                cursor.set_position(self.torrent_info.piece_length() * index as u64);
                cursor.write_all(&data).unwrap();
                self.pretended_file = cursor.into_inner();
                self.downloaded += data.len();
                log::info!("Downloaded: {}", self.downloaded);


                for peer in self.peer_connections.iter() {
                    peer.have(index).unwrap();
                }

                // TODO use a proper piece strategy here 
               loop {
                    if let Some(next_piece) = self.next_piece() {
                        // only peers that haven't choked us 
                        for peer in self.peer_connections.iter().filter(|peer| !peer.state().peer_choking) {
                            if peer.state().peer_pieces[next_piece as usize] {
                                if peer.state().is_choking {
                                    self.num_unchoked += 1;
                                    peer.unchoke().unwrap();
                                }
                                // Group to a single operation
                                self.inflight_pieces.set(next_piece as usize, true);
                                peer.request_piece(next_piece, self.piece_length(next_piece)).unwrap();
                                return;
                            }
                        }
                    } else if self.completed_pieces.all() {
                        log::info!("Torrent completed! Downloaded: {}",self.pretended_file.len());
                        self.download_tx.take().unwrap().send(()).unwrap();
                        return;
                    } else {
                       log::error!("No piece can be downloaded from any peer"); 
                        return;
                    }
                }
                
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
        let completed_pieces: BitBox<u8, Msb0> = torrent_info.pieces().map(|_| false).collect();
        assert!(torrent_info.files().count() == 1);
        let file_lenght = torrent_info.files().next().unwrap().length();
        let last_piece_len = file_lenght % torrent_info.piece_length() as u64;
        let (tx, rc) = tokio::sync::oneshot::channel();
        let torrent_state = TorrentState {
            inflight_pieces: completed_pieces.clone(),
            completed_pieces,
            num_unchoked: 0,
            max_unchoked: UNCHOKED_PEERS as u32,
            pretended_file: vec![0; file_lenght as usize],
            downloaded: 0,
            peer_connections: Vec::new(),
            download_rc: Some(rc),
            download_tx: Some(tx),
            torrent_info: torrent_info.clone(),
            last_piece_len,
        };
        Self {
            our_peer_id: generate_peer_id(),
            torrent_info: Arc::new(torrent_info),
            torrent_state: Rc::new(RefCell::new(torrent_state)),
        }
    }

    pub async fn start(&self) {
        let rc = {
            let mut state = self.torrent_state.borrow_mut();
            let tmp = state.peer_connections.clone();
            drop(state);
            for peer in tmp.iter().take(UNCHOKED_PEERS) {
                peer.interested().unwrap();
                tokio::time::sleep(Duration::from_secs(3)).await;
                let mut state = self.torrent_state.borrow_mut();
                if let Some(piece_idx) = state.next_piece() {
                    let peer_owns_piece = peer.state().peer_pieces[piece_idx as usize];
                    if peer_owns_piece {
                        peer.unchoke().unwrap();
                        state.num_unchoked += 1;
                        state.inflight_pieces.set(piece_idx as usize, true);
                        peer.request_piece(piece_idx, state.piece_length(piece_idx))
                            .unwrap();
                    }
                } else {
                    log::warn!("No more pieces available");
                }
            }
            let mut state = self.torrent_state.borrow_mut();
            state.peer_connections = tmp;
            state.download_rc.take()
        };

        rc.unwrap().await.unwrap();
    }

    pub async fn accept_incoming(&self, listener: &TcpListener) {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        log::info!("Incomming peer connection: {peer_addr}");
        let peer_connection = PeerConnection::new(
            stream,
            self.our_peer_id,
            self.torrent_info.info_hash().into(),
            self.torrent_info.pieces().count(),
            Rc::downgrade(&self.torrent_state),
        )
        .await
        .unwrap();
        self.torrent_state
            .borrow_mut()
            .peer_connections
            .push(peer_connection);
    }

    pub async fn add_peer(&self, addr: SocketAddr) {
        let stream = TcpStream::connect(addr).await.unwrap();
        let peer_connection = PeerConnection::new(
            stream,
            self.our_peer_id,
            self.torrent_info.info_hash().into(),
            self.torrent_info.pieces().count(),
            Rc::downgrade(&self.torrent_state),
        )
        .await
        .unwrap();

        self.torrent_state
            .borrow_mut()
            .peer_connections
            .push(peer_connection);
    }

    pub fn peer(&self, index: usize) -> Option<PeerConnection> {
        self.torrent_state
            .borrow()
            .peer_connections
            .get(index)
            .cloned()
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
