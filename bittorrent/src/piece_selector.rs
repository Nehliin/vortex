use std::collections::VecDeque;

use bitvec::{
    prelude::{BitBox, Msb0},
    vec::BitVec,
};
use lava_torrent::torrent::v1::Torrent;
#[cfg(test)]
use rand::SeedableRng;
use rand::{RngExt, rngs::SmallRng};
use slotmap::SecondaryMap;
use smallvec::SmallVec;

use crate::{buf_pool::Buffer, connection_manager::ConnectionId, torrent::TorrentProgress};

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
    // Downloading -> 1 downloaded 0 = not downloaded (global)
    downloaded_pieces: BitBox<u8, Msb0>,
    // Allocated -> 1 allocated 0 = not allocated (global)
    allocated_pieces: BitBox<u8, Msb0>,
    // Completed -> 1 completed 0 = not completed (global)
    completed_pieces: BitBox<u8, Msb0>,
    // These are all pieces the peer have that we have yet to complete
    // it should be kept up to date as the torrent is downloaded, completed
    // pieces are "turned off" and Have messages only set a bit if we do not already
    // have it. If a peer requests a piece it is also turned off here to prevent it being
    // picked again. TODO: feels fragile
    interesting_peer_pieces: SecondaryMap<ConnectionId, BitBox<u8, Msb0>>,
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
        let mut last_piece_length = torrent_info.length % piece_length;
        // if it's perfectly divisible the last piece size is the normal
        // piece_length
        if last_piece_length == 0 {
            last_piece_length = piece_length;
        }
        Self {
            downloaded_pieces: completed_pieces,
            allocated_pieces,
            completed_pieces: hashing_pieces,
            last_piece_length: last_piece_length as u32,
            piece_length: piece_length as u32,
            interesting_peer_pieces: Default::default(),
            #[cfg(not(test))]
            rng_gen: rand::make_rng(),
            #[cfg(test)]
            rng_gen: SmallRng::seed_from_u64(0xbeefdead),
        }
    }

    pub(crate) fn set_completed_bitfield(&mut self, completed_pieces: BitBox<u8, Msb0>) {
        assert_eq!(self.completed_pieces.len(), completed_pieces.len());
        self.completed_pieces = completed_pieces.clone();
        self.downloaded_pieces = completed_pieces;
    }

    // Returns index and if the peer is in endgame mode
    pub fn next_piece(
        &mut self,
        connection_id: ConnectionId,
        endgame_mode: &mut bool,
    ) -> Option<i32> {
        let interesting_pieces = self.interesting_peer_pieces.get(connection_id)?;
        let pickable = !self.downloaded_pieces.clone() & interesting_pieces;
        // due to lifetime issues
        let first_pickable = pickable.first_one();
        let unallocated_pickable = !self.allocated_pieces.clone() & pickable;

        if unallocated_pickable.not_any() {
            let pickable = first_pickable?;
            // if we still have interesting pieces not completed we should enter endgame mode
            // and pick one of those
            log::debug!("Peer {connection_id:?} is entering endgame mode");
            *endgame_mode = true;
            return Some(pickable as i32);
        }

        let procentage_left =
            self.downloaded_pieces.count_zeros() as f32 / self.downloaded_pieces.len() as f32;
        if procentage_left > 0.95 {
            for _ in 0..5 {
                let index =
                    (self.rng_gen.random::<f32>() * self.downloaded_pieces.len() as f32) as usize;
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
    pub fn bitfield_received(&self, connection_id: ConnectionId) -> bool {
        self.interesting_peer_pieces.contains_key(connection_id)
    }

    // Updates the interesting peer pieces and returns if the peer has any interesting pieces
    pub fn peer_bitfield(
        &mut self,
        connection_id: ConnectionId,
        peer_pieces: BitBox<u8, Msb0>,
    ) -> bool {
        let not_completed = !self.downloaded_pieces.clone();
        let interesting_pieces = peer_pieces & not_completed;
        let is_interesting = interesting_pieces.any();
        self.interesting_peer_pieces
            .insert(connection_id, interesting_pieces);
        is_interesting
    }

    // Updates the interesting peer pieces tracking and returns if the piece index was interesting
    pub fn update_peer_piece_intrest(
        &mut self,
        connection_id: ConnectionId,
        piece_index: usize,
    ) -> bool {
        let is_interesting = !self.downloaded_pieces[piece_index];
        let entry = self
            .interesting_peer_pieces
            .entry(connection_id)
            .expect("peer must remain in primary map");
        entry
            .and_modify(|pieces| pieces.set(piece_index, is_interesting))
            .or_insert_with(|| {
                let mut all_pieces: BitBox<u8, Msb0> =
                    BitVec::repeat(false, self.downloaded_pieces.len()).into();
                all_pieces.set(piece_index, is_interesting);
                all_pieces
            });
        is_interesting
    }

    // All interesting peer pieces if a bitfield has been received
    pub fn interesting_peer_pieces(
        &self,
        connection_id: ConnectionId,
    ) -> Option<&BitBox<u8, Msb0>> {
        self.interesting_peer_pieces.get(connection_id)
    }

    #[inline]
    pub fn mark_complete(&mut self, index: usize) {
        debug_assert!(!self.completed_pieces[index]);
        debug_assert!(self.downloaded_pieces[index]);
        self.completed_pieces.set(index, true);
        self.allocated_pieces.set(index, false);
        // The piece is no longer interesting if we've completed it
        for interesting_pieces in self.interesting_peer_pieces.values_mut() {
            interesting_pieces.set(index, false);
        }
    }

    #[inline]
    pub fn mark_downloaded(&mut self, index: usize) {
        debug_assert!(!self.downloaded_pieces[index]);
        debug_assert!(!self.completed_pieces[index]);
        self.downloaded_pieces.set(index, true);
    }

    #[inline]
    pub fn mark_not_downloaded(&mut self, index: usize) {
        debug_assert!(self.downloaded_pieces[index]);
        debug_assert!(!self.completed_pieces[index]);
        self.downloaded_pieces.set(index, false);
    }

    #[inline]
    pub fn mark_allocated(&mut self, index: i32, connection_id: ConnectionId) {
        let index = index as usize;
        self.allocated_pieces.set(index, true);
        // Mark this as no longer interesting to prevent it from being repicked.
        // If this is rejected we can mark it as interesting again when deallocating
        let interesting_pieces = &mut self.interesting_peer_pieces.get_mut(connection_id).unwrap();
        let old = interesting_pieces.replace(index, false);
        // Must have been interesting to this peer before allocating it
        debug_assert!(old);
    }

    #[inline]
    pub fn mark_not_allocated(&mut self, index: i32, connection_id: ConnectionId) {
        let index = index as usize;
        debug_assert!(self.allocated_pieces[index]);
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
    pub fn completed_none(&self) -> bool {
        self.completed_pieces.not_any()
    }

    #[inline]
    pub fn completed_clone(&self) -> BitBox<u8, Msb0> {
        self.completed_pieces.clone()
    }

    /// Per-piece completion progress (downloaded *and* hash-verified),
    /// built by cloning the completed-piece bitfield directly.
    #[inline]
    pub fn progress(&self) -> TorrentProgress {
        TorrentProgress::new(self.completed_pieces.clone())
    }

    #[inline]
    pub fn has_downloaded(&self, index: usize) -> bool {
        self.downloaded_pieces[index]
    }

    #[inline]
    pub fn is_complete(&self, index: usize) -> bool {
        self.completed_pieces[index]
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
        if index == (self.downloaded_pieces.len() as i32 - 1) {
            self.last_piece_length
        } else {
            self.piece_length
        }
    }

    #[inline]
    pub fn avg_piece_length(&self) -> u32 {
        self.piece_length
    }

    #[inline]
    pub fn avg_num_subpieces(&self) -> u32 {
        self.piece_length / SUBPIECE_SIZE as u32
    }
}

#[derive(Debug)]
pub struct DownloadedPiece {
    pub index: usize,
    pub conn_id: ConnectionId,
    pub hash_matched: bool,
    pub buffer: Buffer,
    pub downloaders: SmallVec<[ConnectionId; 5]>,
}

#[derive(Debug)]
// TODO flatten this
pub struct Piece {
    pub index: i32,
    // Contains only completed subpieces
    pub completed_subpieces: BitBox,
    pub last_subpiece_length: i32,
    // Contains the piece data, will be sized as like the average piece size
    pub piece_data: Buffer,
    pub ref_count: u8,
    pub downloaders: SmallVec<[ConnectionId; 5]>,
}

impl Piece {
    pub fn new(index: i32, lenght: u32, piece_view: Buffer) -> Self {
        assert!(lenght > 0, "Piece lenght must be non zero");
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
            piece_data: piece_view,
            ref_count: 0,
            downloaders: Default::default(),
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
        // TODO should this be done if deque is empty??
        self.ref_count += 1;
        deque
    }

    pub fn into_downloaders_and_buffer(self) -> (SmallVec<[ConnectionId; 5]>, Buffer) {
        (self.downloaders, self.piece_data)
    }

    /// Returns if the subpiece is valid or not
    pub fn on_subpiece(
        &mut self,
        conn_id: ConnectionId,
        index: i32,
        begin: i32,
        data: &[u8],
    ) -> bool {
        // This subpice is part of the currently downloading piece
        debug_assert_eq!(self.index, index);
        let subpiece_index = begin / SUBPIECE_SIZE;
        if self.completed_subpieces[subpiece_index as usize] {
            return true;
        }
        log::trace!("Subpiece index received: {subpiece_index}",);
        let last_subpiece = subpiece_index == self.last_subpiece_index();
        if last_subpiece {
            if data.len() as i32 != self.last_subpiece_length {
                return false;
            }
        } else {
            if data.len() as i32 != SUBPIECE_SIZE {
                return false;
            }
        }
        let begin = begin as usize;
        if !self.downloaders.contains(&conn_id) {
            self.downloaders.push(conn_id);
        }
        self.piece_data.raw_mut_slice()[begin..(begin + data.len())].copy_from_slice(data);
        self.completed_subpieces.set(subpiece_index as usize, true);
        true
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf_pool::BufferPool;

    fn setup_piece(index: i32, piece_len: u32) -> (BufferPool, Piece) {
        let mut pool = BufferPool::new("test_pieces", 1, piece_len as usize);
        let buffer = pool.get_buffer();
        (pool, Piece::new(index, piece_len, buffer))
    }

    #[test]
    fn on_subpiece_rejects_truncated_data() {
        let (mut pool, mut piece) = setup_piece(0, (SUBPIECE_SIZE * 2) as u32);
        // Subpieces that aren't the last one must be exactly SUBPIECE_SIZE long
        assert!(!piece.on_subpiece(0, 0, &vec![1; SUBPIECE_SIZE as usize - 1]));
        assert!(!piece.completed_subpieces[0]);
        // Empty responses are truncated as well
        assert!(!piece.on_subpiece(0, 0, &[]));
        assert!(!piece.completed_subpieces[0]);
        assert!(piece.on_subpiece(0, 0, &vec![1; SUBPIECE_SIZE as usize]));
        assert!(piece.completed_subpieces[0]);
        assert!(!piece.is_complete());
        pool.return_buffer(piece.into_buffer());
    }

    #[test]
    fn on_subpiece_rejects_oversized_data() {
        let (mut pool, mut piece) = setup_piece(0, (SUBPIECE_SIZE * 2) as u32);
        assert!(!piece.on_subpiece(0, 0, &vec![1; SUBPIECE_SIZE as usize + 1]));
        assert!(!piece.completed_subpieces[0]);
        pool.return_buffer(piece.into_buffer());
    }

    #[test]
    fn on_subpiece_last_subpiece_length_must_match() {
        // The last subpiece of this piece is only 100 bytes long
        let (mut pool, mut piece) = setup_piece(3, SUBPIECE_SIZE as u32 + 100);
        assert_eq!(piece.last_subpiece_index(), 1);
        assert_eq!(piece.last_subpiece_length, 100);
        // A full sized subpiece would overflow the piece
        assert!(!piece.on_subpiece(3, SUBPIECE_SIZE, &vec![1; SUBPIECE_SIZE as usize]));
        assert!(!piece.completed_subpieces[1]);
        // and a truncated one is rejected just like for any other subpiece
        assert!(!piece.on_subpiece(3, SUBPIECE_SIZE, &[1; 99]));
        assert!(!piece.completed_subpieces[1]);
        assert!(piece.on_subpiece(3, 0, &vec![1; SUBPIECE_SIZE as usize]));
        assert!(!piece.is_complete());
        assert!(piece.on_subpiece(3, SUBPIECE_SIZE, &[1; 100]));
        assert!(piece.is_complete());
        pool.return_buffer(piece.into_buffer());
    }

    #[test]
    fn on_subpiece_accepts_duplicates() {
        let (mut pool, mut piece) = setup_piece(0, (SUBPIECE_SIZE * 2) as u32);
        assert!(piece.on_subpiece(0, 0, &vec![1; SUBPIECE_SIZE as usize]));
        assert!(piece.on_subpiece(0, 0, &vec![2; SUBPIECE_SIZE as usize]));
        assert!(piece.completed_subpieces[0]);
        // The first received copy is kept
        assert_eq!(
            &piece.piece_data.raw_slice()[..SUBPIECE_SIZE as usize],
            &vec![1; SUBPIECE_SIZE as usize][..]
        );
        pool.return_buffer(piece.into_buffer());
    }
}
