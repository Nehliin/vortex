use bitvec::prelude::{BitBox, Msb0};

use crate::PeerList;

// TODO
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
        let total_length: u64 = torrent_info.files().map(|file| file.length()).sum();
        let last_piece_length = total_length % piece_length;

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

    // TODO consider actually triggering the piece download from this struct? 
}

