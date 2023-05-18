use bitvec::prelude::{BitBox, Msb0};
use lava_torrent::torrent::v1::Torrent;
use slotmap::SecondaryMap;

use crate::PeerKey;

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
    peer_pieces: SecondaryMap<PeerKey, BitBox<u8, Msb0>>,
    last_piece_length: u32,
    piece_length: u32,
}

impl PieceSelector {
    pub fn new(torrent_info: &Torrent) -> Self {
        let completed_pieces: BitBox<u8, Msb0> =
            torrent_info.pieces.iter().map(|_| false).collect();
        let inflight_pieces = completed_pieces.clone();
        let piece_length = torrent_info.piece_length;
        let last_piece_length = torrent_info.length % piece_length;

        Self {
            completed_pieces,
            inflight_pieces,
            last_piece_length: last_piece_length as u32,
            piece_length: piece_length as u32,
            peer_pieces: Default::default(),
        }
    }

    pub fn next_piece(&self, peer_key: PeerKey) -> Option<i32> {
        let pieces_left = self.completed_pieces.count_zeros();
        if pieces_left == 0 {
            log::info!("Torrent is completed, no next piece found");
            return None;
        }

        // TODO: avoid clone
        let mut available_pieces = self.peer_pieces.get(peer_key)?.clone();
        // Discount completed or inflight pieces
        let mut tmp = self.completed_pieces.clone();
        tmp |= &self.inflight_pieces;
        available_pieces &= !tmp;

        if available_pieces.not_any() {
            log::warn!(
                "There are no available pieces, inflight_pieces: {}",
                self.inflight_pieces.count_ones()
            );
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
                for peer_pieces in self.peer_pieces.values() {
                    if peer_pieces[available] {
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

    pub fn update_peer_pieces(&mut self, peer_key: PeerKey, peer_pieces: BitBox<u8, Msb0>) {
        if let Some(entry) = self.peer_pieces.entry(peer_key) {
            entry
                .and_modify(|pieces| *pieces |= &peer_pieces)
                .or_insert(peer_pieces);
        } else {
            // TODO: this really isn't an error but want to make it visible for now
            log::error!("Attempted to update piece for peer that was removed");
        }
    }

    pub fn set_peer_piece(&mut self, peer_key: PeerKey, piece_index: usize) {
        if let Some(entry) = self.peer_pieces.entry(peer_key) {
            entry
                .and_modify(|pieces| pieces.set(piece_index, true))
                .or_insert_with(|| (0..self.completed_pieces.len()).map(|_| false).collect());
        } else {
            // TODO: this really isn't an error but want to make it visible for now
            log::error!("Attempted to update piece for peer that was removed");
        }
    }

    // TODO: Get rid of this?
    #[inline]
    pub fn mark_complete(&mut self, index: usize) {
        debug_assert!(self.inflight_pieces[index]);
        debug_assert!(!self.completed_pieces[index]);
        self.completed_pieces.set(index, true);
        self.inflight_pieces.set(index, false);
    }

    // TODO: Get rid of this?
    #[inline]
    pub fn mark_inflight(&mut self, index: usize) {
        debug_assert!(!self.completed_pieces[index]);
        self.inflight_pieces.set(index, true);
    }

    #[inline]
    pub fn mark_not_inflight(&mut self, index: usize) {
        debug_assert!(self.inflight_pieces[index]);
        self.inflight_pieces.set(index, false);
    }

    #[inline]
    pub fn completed_all(&self) -> bool {
        self.completed_pieces.all()
    }

    #[inline]
    pub fn has_completed(&self, index: usize) -> bool {
        self.completed_pieces[index]
    }

    #[inline]
    pub fn is_inflight(&self, index: usize) -> bool {
        self.inflight_pieces[index]
    }

    #[inline]
    pub fn pieces(&self) -> usize {
        self.completed_pieces.len()
    }

    #[inline]
    pub fn total_completed(&self) -> usize {
        self.completed_pieces.count_ones()
    }

    #[inline]
    pub fn piece_len(&self, index: i32) -> u32 {
        if index == (self.completed_pieces.len() as i32 - 1) {
            self.last_piece_length
        } else {
            self.piece_length
        }
    }
}
