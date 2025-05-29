use std::collections::{HashMap, VecDeque};

use bitvec::{
    prelude::{BitBox, Msb0},
    vec::BitVec,
};
use lava_torrent::torrent::v1::Torrent;
use rand::{Rng, SeedableRng, rngs::SmallRng};

use crate::file_store::{FileStore, ReadablePieceFileView, WritablePieceFileView};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subpiece {
    pub index: i32,
    pub offset: i32,
    pub size: i32,
    pub timed_out: bool,
}

pub struct PieceSelector {
    //    strategy: T,
    // Completed -> 1 completed 0 = not completed (global)
    completed_pieces: BitBox<u8, Msb0>,
    // Allocated -> 1 allocated 0 = not allocated (global)
    allocated_pieces: BitBox<u8, Msb0>,
    // Hashing -> 1 is currently being hashed 0 = not hashing (global)
    hashing_pieces: BitBox<u8, Msb0>,
    // These are all pieces the peer have that we have yet to complete
    // it should be kept up to date as the torrent is downloaded, completed
    // pieces are "turned off" and Have messages only set a bit if we do not already
    // have it. If a peer requests a piece it is also turned off here to prevent it being
    // picked again. TODO: feels fragile
    interesting_peer_pieces: HashMap<usize, BitBox<u8, Msb0>>,
    last_piece_length: u32,
    piece_length: u32,
    rng_gen: SmallRng,
}

impl PieceSelector {
    pub fn new(torrent_info: &Torrent) -> Self {
        let completed_pieces: BitBox<u8, Msb0> =
            BitVec::repeat(false, torrent_info.pieces.len()).into();
        let allocated_pieces = completed_pieces.clone();
        let hashing_pieces = completed_pieces.clone();
        let piece_length = torrent_info.piece_length;
        let last_piece_length = torrent_info.length % piece_length;

        Self {
            completed_pieces,
            allocated_pieces,
            hashing_pieces,
            last_piece_length: last_piece_length as u32,
            piece_length: piece_length as u32,
            interesting_peer_pieces: Default::default(),
            #[cfg(not(test))]
            rng_gen: SmallRng::from_os_rng(),
            #[cfg(test)]
            rng_gen: SmallRng::seed_from_u64(0xbeefdead),
        }
    }

    // Returns index and if the peer is in endgame mode
    pub fn next_piece(&mut self, connection_id: usize, endgame_mode: &mut bool) -> Option<i32> {
        let interesting_pieces = self.interesting_peer_pieces.get(&connection_id)?;
        let pickable = !self.hashing_pieces.clone() & interesting_pieces;
        // due to lifetime issues
        let first_pickable = pickable.first_one();
        let unallocated_pickable = !self.allocated_pieces.clone() & pickable;

        if unallocated_pickable.not_any() {
            let pickable = first_pickable?;
            // if we still have interesting pieces not completed we should enter endgame mode
            // and pick one of those
            log::debug!("Peer {connection_id} is entering endgame mode");
            *endgame_mode = true;
            return Some(pickable as i32);
        }

        let procentage_left =
            self.completed_pieces.count_zeros() as f32 / self.completed_pieces.len() as f32;
        if procentage_left > 0.95 {
            for _ in 0..5 {
                let index =
                    (self.rng_gen.random::<f32>() * self.completed_pieces.len() as f32) as usize;
                if unallocated_pickable[index] {
                    *endgame_mode = false;
                    return Some(index as i32);
                }
            }
            log::warn!("Random piece selection failed");
            let available_index = unallocated_pickable.first_one()?;
            *endgame_mode = false;
            Some(available_index as i32)
        } else {
            // Note: This won't count allocated piece but that should be fine
            // Rarest first
            let mut count = vec![0; unallocated_pickable.len()];
            for available in unallocated_pickable.iter_ones() {
                for peer_pieces in self.interesting_peer_pieces.values() {
                    if peer_pieces[available] {
                        count[available] += 1;
                    }
                }
            }
            let (rarest_index, _) = count
                .into_iter()
                .enumerate()
                .filter(|(_pos, count)| count > &0)
                .min_by_key(|(_pos, val)| *val)?;
            *endgame_mode = false;
            Some(rarest_index as i32)
        }
    }

    #[inline]
    pub fn bitfield_received(&self, connection_id: usize) -> bool {
        self.interesting_peer_pieces.contains_key(&connection_id)
    }

    // Updates the interesting peer pieces and returns if the peer has any interesting pieces
    pub fn peer_bitfield(&mut self, connection_id: usize, peer_pieces: BitBox<u8, Msb0>) -> bool {
        let not_completed = !self.completed_pieces.clone();
        let interesting_pieces = peer_pieces & not_completed;
        let is_interesting = interesting_pieces.any();
        self.interesting_peer_pieces
            .insert(connection_id, interesting_pieces);
        is_interesting
    }

    // Updates the interesting peer pieces tracking and returns if the piece index was interesting
    pub fn update_peer_piece_intrest(&mut self, connection_id: usize, piece_index: usize) -> bool {
        let is_interesting = !self.completed_pieces[piece_index];
        let entry = self.interesting_peer_pieces.entry(connection_id);
        entry
            .and_modify(|pieces| pieces.set(piece_index, is_interesting))
            .or_insert_with(|| {
                let mut all_pieces: BitBox<u8, Msb0> =
                    BitVec::repeat(false, self.completed_pieces.len()).into();
                all_pieces.set(piece_index, is_interesting);
                all_pieces
            });
        is_interesting
    }

    // All interesting peer pieces if a bitfield has been received
    pub fn interesting_peer_pieces(&self, connection_id: usize) -> Option<&BitBox<u8, Msb0>> {
        self.interesting_peer_pieces.get(&connection_id)
    }

    #[inline]
    pub fn mark_complete(&mut self, index: usize) {
        assert!(!self.completed_pieces[index]);
        self.completed_pieces.set(index, true);
        self.allocated_pieces.set(index, false);
        // The piece is no longer interesting if we've completed it
        for interesting_pieces in self.interesting_peer_pieces.values_mut() {
            interesting_pieces.set(index, false);
        }
    }

    #[inline]
    pub fn mark_hashing(&mut self, index: usize) {
        assert!(!self.completed_pieces[index]);
        assert!(!self.hashing_pieces[index]);
        self.hashing_pieces.set(index, true);
    }

    #[inline]
    pub fn mark_not_hashing(&mut self, index: usize) {
        assert!(!self.completed_pieces[index]);
        assert!(self.hashing_pieces[index]);
        self.hashing_pieces.set(index, false);
    }

    #[inline]
    pub fn mark_allocated(&mut self, index: i32, connection_id: usize) {
        let index = index as usize;
        self.allocated_pieces.set(index, true);
        // Mark this as no longer interesting to prevent it from being repicked.
        // If this is rejected we can mark it as interesting again when deallocating
        let interesting_pieces = &mut self
            .interesting_peer_pieces
            .get_mut(&connection_id)
            .unwrap();
        let old = interesting_pieces.replace(index, false);
        // Must have been interesting to this peer before allocating it
        assert!(old);
    }

    #[inline]
    pub fn mark_not_allocated(&mut self, index: i32, connection_id: usize) {
        let index = index as usize;
        assert!(self.allocated_pieces[index]);
        self.allocated_pieces.set(index, false);
        // Mark the piece as interesting again so it can be picked again
        // if necessary
        self.update_peer_piece_intrest(connection_id, index);
    }

    #[inline]
    pub fn completed_all(&self) -> bool {
        self.completed_pieces.all()
    }

    #[inline]
    pub fn completed_clone(&self) -> BitBox<u8, Msb0> {
        self.completed_pieces.clone()
    }

    #[inline]
    pub fn has_completed(&self, index: usize) -> bool {
        self.completed_pieces[index]
    }

    #[inline]
    pub fn is_hashing(&self, index: usize) -> bool {
        self.hashing_pieces[index]
    }

    #[inline]
    pub fn is_allocated(&self, index: usize) -> bool {
        self.allocated_pieces[index]
    }

    #[inline]
    pub fn total_completed(&self) -> usize {
        self.completed_pieces.count_ones()
    }

    #[inline]
    pub fn total_allocated(&self) -> usize {
        self.allocated_pieces.count_ones()
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
}

#[derive(Debug)]
pub struct CompletedPiece {
    pub index: usize,
    pub conn_id: usize,
    pub hash_matched: std::io::Result<bool>,
}

#[derive(Debug)]
// TODO flatten this
pub struct Piece {
    pub index: i32,
    // Contains only completed subpieces
    pub completed_subpieces: BitBox,
    pub last_subpiece_length: i32,
    pub piece_view: WritablePieceFileView,
    pub ref_count: u8,
}

impl Piece {
    pub fn new(index: i32, lenght: u32, piece_view: WritablePieceFileView) -> Self {
        let last_subpiece_length = if lenght as i32 % SUBPIECE_SIZE == 0 {
            SUBPIECE_SIZE
        } else {
            lenght as i32 % SUBPIECE_SIZE
        };
        let subpieces =
            (lenght / SUBPIECE_SIZE as u32) + u32::from(last_subpiece_length != SUBPIECE_SIZE);
        let completed_subpieces: BitBox = (0..subpieces).map(|_| false).collect();
        Self {
            index,
            completed_subpieces,
            last_subpiece_length,
            piece_view,
            ref_count: 0,
        }
    }

    /// Increases the ref count of this piece and returns all remaining subpieces
    /// to download
    pub fn allocate_remaining_subpieces(&mut self) -> VecDeque<Subpiece> {
        let mut deque = VecDeque::with_capacity(self.completed_subpieces.len());
        let last_subpiece_index = self.completed_subpieces.len() - 1;
        // Do we need to adjust the piece size of the last subpiece?
        let mut last_is_last_index = false;

        for subpiece_index in self.completed_subpieces.iter_zeros() {
            deque.push_back(Subpiece {
                index: self.index,
                offset: SUBPIECE_SIZE * subpiece_index as i32,
                size: SUBPIECE_SIZE,
                timed_out: false,
            });
            last_is_last_index = subpiece_index == last_subpiece_index;
        }
        if last_is_last_index {
            // will never panic
            let last_subpiece = deque.back_mut().unwrap();
            last_subpiece.size = self.last_subpiece_length;
        }
        self.ref_count += 1;
        deque
    }

    pub fn into_readable(self) -> ReadablePieceFileView {
        self.piece_view.into_readable()
    }

    pub fn on_subpiece(&mut self, index: i32, begin: i32, data: &[u8], file_store: &FileStore) {
        // This subpice is part of the currently downloading piece
        debug_assert_eq!(self.index, index);
        let subpiece_index = begin / SUBPIECE_SIZE;
        if self.completed_subpieces[subpiece_index as usize] {
            return;
        }
        log::trace!("Subpiece index received: {subpiece_index}",);
        let last_subpiece = subpiece_index == self.last_subpiece_index();
        if last_subpiece {
            debug_assert_eq!(data.len() as i32, self.last_subpiece_length);
        } else {
            debug_assert_eq!(data.len() as i32, SUBPIECE_SIZE);
        }
        self.piece_view
            .write_subpiece(begin as usize, data, file_store);
        self.completed_subpieces.set(subpiece_index as usize, true);
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
