use std::collections::HashMap;

use bitvec::prelude::{BitBox, Msb0};
use lava_torrent::torrent::v1::Torrent;

use crate::peer_protocol::PeerId;

pub const SUBPIECE_SIZE: i32 = 16_384;

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

#[derive(Debug, Clone, Copy)]
pub struct Subpiece {
    pub index: i32,
    pub offset: i32,
    pub size: i32,
}

pub(crate) struct PieceSelector {
    //    strategy: T,
    completed_pieces: BitBox<u8, Msb0>,
    // TODO: rename to assinged pieces instead
    inflight_pieces: BitBox<u8, Msb0>,
    // TODO: ability to remove
    peer_pieces: HashMap<usize, BitBox<u8, Msb0>>,
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

    pub fn next_piece(&self, connection_id: usize) -> Option<i32> {
        let pieces_left = self.completed_pieces.count_zeros();
        if pieces_left == 0 {
            log::info!("Torrent is completed, no next piece found");
            return None;
        }

        // TODO: avoid clone
        let mut available_pieces = self.peer_pieces.get(&connection_id)?.clone();
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
                //log::debug!("Picking random piece to download, index: {index}");
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

    pub fn update_peer_pieces(&mut self, connection_id: usize, peer_pieces: BitBox<u8, Msb0>) {
        let entry = self.peer_pieces.entry(connection_id);
        entry
            .and_modify(|pieces| *pieces |= &peer_pieces)
            .or_insert(peer_pieces);
    }

    pub fn set_peer_piece(&mut self, connection_id: usize, piece_index: usize) {
        let entry = self.peer_pieces.entry(connection_id);
        entry
            .and_modify(|pieces| pieces.set(piece_index, true))
            .or_insert_with(|| (0..self.completed_pieces.len()).map(|_| false).collect());
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

    #[inline]
    pub fn avg_num_subpieces(&self) -> u32 {
        self.piece_length / SUBPIECE_SIZE as u32
    }

    #[inline]
    pub fn num_subpieces(&self, index: i32) -> u32 {
        if index == (self.completed_pieces.len() as i32 - 1) {
            self.last_piece_length / SUBPIECE_SIZE as u32
        } else {
            self.piece_length / SUBPIECE_SIZE as u32
        }
    }
}

#[derive(Debug)]
pub struct Piece {
    pub index: i32,
    // Contains only completed subpieces
    pub completed_subpieces: BitBox,
    // Contains both completed and inflight subpieces
    pub inflight_subpieces: BitBox,
    pub last_subpiece_length: i32,
    // TODO used uninit memory here instead
    pub memory: Vec<u8>,
}

impl Piece {
    pub fn new(index: i32, lenght: u32) -> Self {
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

    pub fn on_subpiece(&mut self, index: i32, begin: i32, data: &[u8], peer_id: PeerId) {
        // This subpice is part of the currently downloading piece
        assert_eq!(self.index, index);
        let subpiece_index = begin / SUBPIECE_SIZE;
        log::trace!("[Peer: {}] Subpiece index received: {subpiece_index}", peer_id);
        let last_subpiece = subpiece_index == self.last_subpiece_index();
        if last_subpiece {
            assert_eq!(data.len() as i32, self.last_subpiece_length);
        } else {
            assert_eq!(data.len() as i32, SUBPIECE_SIZE);
        }
        self.completed_subpieces.set(subpiece_index as usize, true);
        self.memory[begin as usize..begin as usize + data.len()].copy_from_slice(data);
    }

    #[inline]
    pub fn on_subpiece_failed(&mut self, index: i32, begin: i32) {
        // This subpice is part of the currently downloading piece
        assert_eq!(self.index, index);
        let subpiece_index = begin / SUBPIECE_SIZE;
        self.inflight_subpieces.set(subpiece_index as usize, false);
    }

    // Perhaps this can return the subpice or a peer request directly?
    #[inline]
    pub fn next_unstarted_subpice(&self) -> Option<usize> {
        self.inflight_subpieces.first_zero()
    }

    #[inline]
    pub fn last_subpiece_index(&self) -> i32 {
        self.completed_subpieces.len() as i32 - 1
    }

    #[inline]
    pub fn is_complete(&self) -> bool {
        self.completed_subpieces.all()
    }
}
