// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::{
    io::{Cursor, Write},
    net::SocketAddr,
    sync::Arc,
};

use bip_metainfo::Accessor;
use bitvec::prelude::*;
use bytes::BytesMut;
use parking_lot::Mutex;
use peer_connection::{PeerConnection, PeerConnectionHandle};
use sha1::{Digest, Sha1};

use crate::peer_connection::PeerOrder;

const SUBPIECE_SIZE: i32 = 16_384;

pub mod peer_connection;
pub mod peer_message;
#[cfg(test)]
mod test;

// Perhaps also create a subpiece type that can be converted into a peer request
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
        let subpieces = (lenght / SUBPIECE_SIZE as u32)
            + if last_subpiece_length != SUBPIECE_SIZE {
                1
            } else {
                0
            };
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
    completed_pieces: BitBox<u8, Msb0>,
    pretended_file: Vec<u8>,
    // Temp
    downloaded: usize,
    max_unchoked: u32,
    num_unchoked: u32,
    peer_connections: Vec<PeerConnectionHandle>,
}

#[derive(Clone)]
pub struct TorrentManager {
    torrent_info: Arc<bip_metainfo::Info>,
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
            downloaded: 0,
            peer_connections: Vec::new(),
        };
        Self {
            torrent_info: Arc::new(torrent_info),
            torrent_state: Arc::new(Mutex::new(torrent_state)),
        }
    }

    pub async fn add_peer(
        &self,
        addr: SocketAddr,
        our_id: [u8; 20],
        peer_id: [u8; 20],
    ) -> tokio::sync::oneshot::Receiver<()> {
        // Connect first perhaps so errors can be handled
        let (sender, receiver) = tokio::sync::mpsc::channel(256);
        let peer_handle = PeerConnectionHandle { peer_id, sender };
        let info_hash = self.torrent_info.info_hash().into();
        let this = self.clone();
        let (tx, rc) = tokio::sync::oneshot::channel();
        // TEMP UGLY HACK
        let (closed_sender, closed_recv) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || {
            tokio_uring::start(async move {
                let mut peer_connection =
                    PeerConnection::new(addr, our_id, peer_id, info_hash, this, receiver)
                        .await
                        .unwrap();

                tx.send(()).unwrap();
                peer_connection.connection_send_loop().await.unwrap();
                closed_sender.send(()).unwrap();
            })
        });
        self.torrent_state.lock().peer_connections.push(peer_handle);
        rc.await.unwrap();
        closed_recv
    }

    pub fn peer(&self, index: usize) -> Option<PeerConnectionHandle> {
        self.torrent_state
            .lock()
            .peer_connections
            .get(index)
            .cloned()
    }

    pub(crate) fn should_unchoke(&self) -> bool {
        let state = self.torrent_state.lock();
        state.num_unchoked < state.max_unchoked
    }

    pub(crate) async fn on_piece_completed(&self, index: i32, data: Vec<u8>) {
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
                let (peer_connections, next_piece): (Vec<_>, Option<usize>) = {
                    let mut state = self.torrent_state.lock();
                    state.completed_pieces.set(piece_index, true);
                    let mut cursor = Cursor::new(std::mem::take(&mut state.pretended_file));
                    cursor.set_position((SUBPIECE_SIZE * index) as u64);
                    cursor.write_all(&data).unwrap();
                    state.pretended_file = cursor.into_inner();
                    state.downloaded += data.len();
                    log::info!("Downloaded: {}", state.downloaded);

                    // TODO avoid clone here
                    (state.peer_connections.clone(), state.completed_pieces.first_zero())
                };

                for peer in peer_connections.iter() {
                    peer.sender.send(PeerOrder::SendHave(index)).await.unwrap();
                }

                // TODO Remove me 
                if let Some(next_piece) = next_piece {
                    log::info!("Requesting next piece: {next_piece}");
                    peer_connections[0]
                        .sender
                        .send(
                        peer_connection::PeerOrder::RequestPiece {
                                index: next_piece as i32,
                                total_len: self.torrent_info.piece_length() as u32
                        }).await.unwrap();
                } else {
                    log::info!("Torrent completed! Downloaded: {}", self.torrent_state.lock().pretended_file.len());
                }

            }
            Some(piece_index) => log::error!(
                "Piece hash didn't match expected index! expected index: {index}, piece_index: {piece_index}"
            ),
            None => log::error!("Piece sha1 hash not found!"),
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
