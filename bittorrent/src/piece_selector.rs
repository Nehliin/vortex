use bitvec::prelude::{BitBox, Msb0};

use crate::PeerList;

/*pub trait PieceSelectionStrategy {
    // peer list
    fn next_piece(
        &self,
        peer_list: &PeerList,
        completed_pieces: BitBox<u8, Msb0>,
        inflight_pieces: BitBox<u8, Msb0>,
    ) -> Option<i32>;
}

pub struct RandomPiece;*/

pub(crate) struct PieceSelector {
    //    strategy: T,
    completed_pieces: BitBox<u8, Msb0>,
    inflight_pieces: BitBox<u8, Msb0>,
    last_piece_length: u32,
    piece_length: u32,
}

impl PieceSelector {
    pub fn new(torrent_info: &bip_metainfo::Info) -> Self {
        let completed_pieces: BitBox<u8, Msb0> = torrent_info.pieces().map(|_| false).collect();
        let inflight_pieces = completed_pieces.clone();
        let piece_length = torrent_info.piece_length();
        let file_length = torrent_info.files().next().unwrap().length();
        // TODO: Should probably be a sum of all the files?
        let last_piece_length = file_length % piece_length;

        Self {
            completed_pieces,
            inflight_pieces,
            last_piece_length: last_piece_length as u32,
            piece_length: piece_length as u32,
        }
    }

    pub fn next_piece(&self, peer_list: &PeerList) -> Option<i32> {
        let pieces_left = self.completed_pieces.count_zeros();
        if pieces_left == 0 {
            log::info!("Torrent is completed, no next piece found");
            return None;
        }
        // All pieces we haven't downloaded that peers have
        let mut available_pieces: BitBox<u8, Msb0> =
            (0..self.completed_pieces.len()).map(|_| false).collect();

        let peer_connections = peer_list.peer_connection_states.borrow();
        for (_, peer) in peer_connections.iter() {
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
            for _ in 0..5 {
                let index = (rand::random::<f32>() * self.completed_pieces.len() as f32) as usize;
                log::debug!("Picking random piece to download, index: {index}");
                if available_pieces[index] {
                    return Some(index as i32);
                }
            }
            log::warn!("Random piece selection failed");
            available_pieces.first_one().map(|index| index as i32)
        } else {
            // Rarest first
            let mut count = vec![0; available_pieces.len()];
            for available in available_pieces.iter_ones() {
                for (_, peer) in peer_connections.iter() {
                    if peer.state().peer_pieces[available] {
                        count[available] += 1;
                    }
                }
            }
            let index = count
                .into_iter()
                .enumerate()
                .filter(|(_pos, count)| count > &0)
                .min_by_key(|(_pos, val)| *val)
                .map(|(pos, _)| pos as i32);
            log::debug!("Picking rarest piece to download, index: {index:?}");
            index
        }
    }

    #[inline]
    pub fn mark_complete(&mut self, index: usize) {
        debug_assert!(self.inflight_pieces[index]);
        debug_assert!(!self.completed_pieces[index]);
        self.completed_pieces.set(index, true);
        self.inflight_pieces.set(index, false);
    }

    #[inline]
    pub fn mark_inflight(&mut self, index: usize) {
        debug_assert!(!self.completed_pieces[index]);
        self.inflight_pieces.set(index, true);
    }

    #[inline]
    pub fn completed(&self) -> bool {
        self.completed_pieces.all()
    }

    #[inline]
    pub fn pieces(&self) -> usize {
        self.completed_pieces.len()
    }

    #[inline]
    pub fn piece_len(&self, index: i32) -> u32 {
        if index == (self.completed_pieces.len() as i32 - 1) {
            self.last_piece_length
        } else {
            self.piece_length
        }
    }

    // TODO unused, maybe shouldn't be part of this 
    /*pub fn download_next_piece(
        &mut self,
        peer_list: &PeerList,
        num_unchoked: &mut u32,
    ) -> anyhow::Result<bool> {
        let mut disconnected_peers = Vec::new();
        if let Some(next_piece) = self.next_piece(peer_list) {
            let peer_connections = peer_list.peer_connection_states.borrow_mut();
            for (_, peer) in peer_connections.iter().filter(|(_, peer)| {
                !peer.state().peer_choking && peer.state().currently_downloading.is_none()
            }) {
                if peer.state().peer_pieces[next_piece as usize] {
                    if peer.state().is_choking {
                        if let Err(err) = peer.unchoke() {
                            log::error!("{err}");
                            disconnected_peers.push(peer.peer_id);
                            continue;
                        } else {
                            *num_unchoked += 1;
                        }
                    }
                    // Group to a single operation
                    if let Err(err) = peer.request_piece(next_piece, self.piece_len(next_piece)) {
                        log::error!("{err}");
                        disconnected_peers.push(peer.peer_id);
                        continue;
                    } else {
                        self.inflight_pieces.set(next_piece as usize, true);
                        break;
                    }
                }
            }
            Ok(false)
        } else if self.completed_pieces.all() {
            log::info!("Torrent completed!");
            return Ok(true);
        } else {
            anyhow::bail!("No piece can be downloaded from any peer");
        }
    }*/
}
// borde ha en tråd per torrent. mkt lättare o ej värt med fler pga så får är unchoked samtidigt

// Data flows
// Peer connection needs to know if it should unchoke or not
// Needs access to file storage and know which pieces are completed
// Needs access to storage to write data and piece selector update completed/inflight pieces
//
// PieceSelector needs to know and update completed/inflight pieces
// needs to have access to peer list
//
// Dht needs access to peer list ips and write in new peers (but an indirection is needed probably
// long term since it might receive announces for torrents we aren't downloading)
//
// Peer selector is needed as well kind of to decide from who we should download?
// keep track of num unchoked and chocked and what the different download speeds are
// as well as if we are snubbed.
//
