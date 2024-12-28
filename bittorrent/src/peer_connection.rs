use std::{
    collections::VecDeque,
    io::{self},
    os::fd::RawFd,
    time::{Duration, Instant},
};

use bytes::Bytes;
use thiserror::Error;

use crate::{
    peer_protocol::{PeerId, PeerMessage, PeerMessageDecoder},
    piece_selector::{Piece, PieceSelector, Subpiece, SUBPIECE_SIZE},
    TorrentState,
};

// Taken from
// https://github.com/arvidn/moving_average/blob/master/moving_average.hpp
#[derive(Debug)]
pub struct MovingRttAverage {
    // u32?
    mean: i32,
    average_deviation: i32,
    num_samples: i32,
    inverted_gain: i32,
}

impl Default for MovingRttAverage {
    fn default() -> Self {
        Self {
            mean: 0,
            average_deviation: 0,
            num_samples: 0,
            inverted_gain: 10,
        }
    }
}

impl MovingRttAverage {
    pub fn add_sample(&mut self, rtt_sample: &Duration) {
        let mut sample = rtt_sample.as_millis() as i32;
        sample *= 64;

        let old_mean = self.mean;

        if self.num_samples < self.inverted_gain {
            self.num_samples += 1;
        }

        self.mean += (sample - self.mean) / self.num_samples;
        if self.num_samples > 1 {
            let deviation = (old_mean - sample).abs();
            self.average_deviation += (deviation - self.average_deviation) / (self.num_samples - 1);
        }
    }

    #[inline]
    pub fn mean(&self) -> Duration {
        if self.num_samples > 0 {
            let mean = (self.mean + 32) / 64;
            Duration::from_millis(mean as u64)
        } else {
            Duration::from_millis(0)
        }
    }

    #[inline]
    pub fn average_deviation(&self) -> Duration {
        if self.num_samples > 1 {
            let avg_mean = (self.average_deviation + 32) / 64;
            Duration::from_millis(avg_mean as u64)
        } else {
            Duration::from_millis(0)
        }
    }
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Peer is being disconnected, Reason {0}")]
    Disconnect(&'static str),
    #[error("Peer encountered IO issue")]
    Io(#[source] io::Error),
}

#[derive(Debug)]
pub struct OutgoingMsg {
    pub message: PeerMessage,
    pub ordered: bool,
}

#[derive(Debug)]
pub struct PeerConnection {
    pub fd: RawFd,
    // TODO: Make this a type that impl display
    pub peer_id: PeerId,
    /// This side is choking the peer
    pub is_choking: bool,
    /// This side is interested what the peer has to offer
    pub is_interested: bool,
    pub fast_ext: bool,
    /// The peer have informed us that it is choking us.
    pub peer_choking: bool,
    /// The peer is interested what we have to offer
    pub peer_interested: bool,
    // TODO: do we need this both with queued?
    pub currently_downloading: Vec<Piece>,
    pub desired_queue_size: usize,
    pub queued: VecDeque<Subpiece>,
    pub timeout_point: Option<Instant>,
    pub slow_start: bool,
    // The averge time between pieces being received
    pub moving_rtt: MovingRttAverage,
    pub throughput: u64,
    pub prev_throughput: u64,
    // If this connection is about to be disconnected
    // because of low througput. (Choke instead?)
    pub pending_disconnect: bool,
    pub stateful_decoder: PeerMessageDecoder,
    // Stored here to prevent reallocations
    pub outgoing_msgs_buffer: Vec<OutgoingMsg>,
    // Uses vec instead of hashset since this is expected to be small
    pub allowed_fast_pieces: Vec<i32>,
    // The pieces we allow others to request when choked
    pub accept_fast_pieces: Vec<i32>,
}

impl PeerConnection {
    pub fn new(fd: RawFd, peer_id: PeerId, fast_ext: bool) -> Self {
        PeerConnection {
            fd,
            peer_id,
            is_choking: true,
            is_interested: true,
            peer_choking: true,
            peer_interested: false,
            timeout_point: None,
            fast_ext,
            queued: VecDeque::with_capacity(64),
            desired_queue_size: 4,
            slow_start: true,
            moving_rtt: Default::default(),
            currently_downloading: Default::default(),
            pending_disconnect: false,
            throughput: 0,
            prev_throughput: 0,
            outgoing_msgs_buffer: Default::default(),
            stateful_decoder: PeerMessageDecoder::new(2 << 15),
            allowed_fast_pieces: Default::default(),
            accept_fast_pieces: Default::default(),
        }
    }

    fn unchoke(&mut self, torrent_state: &mut TorrentState, ordered: bool) {
        if self.is_choking {
            torrent_state.num_unchoked += 1;
        }
        self.is_choking = false;
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Unchoke,
            ordered,
        });
    }

    fn interested(&mut self, ordered: bool) {
        self.is_interested = true;
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Interested,
            ordered,
        });
    }

    fn choke(&mut self, torrent_state: &mut TorrentState, ordered: bool) {
        if !self.is_choking {
            torrent_state.num_unchoked -= 1;
        }
        self.is_choking = true;
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Choke,
            ordered,
        });
    }

    fn request_piece(&mut self, index: i32, piece_selector: &mut PieceSelector) {
        let length = piece_selector.piece_len(index);
        self.currently_downloading.push(Piece::new(index, length));
        piece_selector.mark_inflight(index as usize);
        self.fill_request_queue()
    }

    pub fn fill_request_queue(&mut self) {
        'outer: for piece in self.currently_downloading.iter_mut() {
            let mut available_pieces = piece.completed_subpieces.clone();
            available_pieces |= &piece.inflight_subpieces;
            while let Some(subindex) = available_pieces.first_zero() {
                if self.queued.len() < self.desired_queue_size {
                    // must update to prevent re-requesting same piece
                    available_pieces.set(subindex, true);
                    Self::push_subpiece_request(
                        &mut self.outgoing_msgs_buffer,
                        &mut self.queued,
                        &mut self.timeout_point,
                        piece,
                        subindex,
                    );
                } else {
                    break 'outer;
                }
            }
        }
    }

    pub fn request_timeout(&mut self) -> Duration {
        let timeout_threshold = if self.moving_rtt.num_samples < 2 {
            if self.moving_rtt.num_samples == 0 {
                Duration::from_secs(2)
            } else {
                self.moving_rtt.mean() + self.moving_rtt.mean() / 5
            }
        } else {
            self.moving_rtt.mean() + (self.moving_rtt.average_deviation() * 4)
        };
        timeout_threshold.max(Duration::from_secs(2))
    }

    fn push_subpiece_request(
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
        queued: &mut VecDeque<Subpiece>,
        timeout_timer: &mut Option<Instant>,
        piece: &mut Piece,
        subindex: usize,
    ) {
        piece.inflight_subpieces.set(subindex, true);
        let length = if subindex as i32 == piece.last_subpiece_index() {
            piece.last_subpiece_length
        } else {
            SUBPIECE_SIZE
        };
        let subpiece_request = PeerMessage::Request {
            index: piece.index,
            begin: SUBPIECE_SIZE * subindex as i32,
            length,
        };
        let subpiece = Subpiece {
            size: length,
            index: piece.index,
            offset: SUBPIECE_SIZE * subindex as i32,
        };
        queued.push_back(subpiece);
        // only if we didnt previously have
        if timeout_timer.is_none() {
            *timeout_timer = Some(Instant::now());
        }

        outgoing_msgs_buffer.push(OutgoingMsg {
            message: subpiece_request,
            ordered: false,
        });
    }

    pub fn remaining_request_queue_spots(&self) -> usize {
        if self.peer_choking {
            return 0;
        }
        // This will work even if we are in a slow start since
        // the window will continue to increase until a timeout is hit
        // TODO: Should we really return 0 here?
        self.desired_queue_size - self.queued.len().min(self.desired_queue_size)
    }

    fn on_subpiece(&mut self, m_index: i32, m_begin: i32, data: Bytes) -> Option<Piece> {
        let position = self
            .currently_downloading
            .iter()
            .position(|piece| piece.index == m_index);
        if let Some(position) = position {
            let mut piece = self.currently_downloading.swap_remove(position);
            piece.on_subpiece(m_index, m_begin, &data[..], self.peer_id);
            self.timeout_point = Some(Instant::now());
            if !piece.is_complete() {
                // Next subpice to download (that isn't already inflight)
                while let Some(next_subpiece) = piece.next_unstarted_subpice() {
                    if self.queued.len() < self.desired_queue_size {
                        Self::push_subpiece_request(
                            &mut self.outgoing_msgs_buffer,
                            &mut self.queued,
                            &mut self.timeout_point,
                            &mut piece,
                            next_subpiece,
                        );
                    } else {
                        break;
                    }
                }
                // still downloading
                self.currently_downloading.push(piece);
            } else {
                log::debug!("[Peer {}] Piece completed", self.peer_id);
                return Some(piece);
            }
        } else {
            log::error!(
                "[Peer {}] Recieved unexpected piece message, index: {m_index}",
                self.peer_id
            );
        }
        None
    }

    pub fn update_stats(&mut self, m_index: i32, m_begin: i32, length: u32) {
        // horribly inefficient
        let Some(pos) = self
            .queued
            .iter()
            .position(|sub| sub.index == m_index && m_begin == sub.offset)
        else {
            // If this supiece was already considered timed out but then arrived slightly later
            // we may up here. This means the timeout (based off RTT) is too strict so should be
            // updated.
            log::error!(
                "Received pieced i: {} begin: {} was not found in queue",
                m_index,
                m_begin
            );
            // TODO: This not how to do this
            //  self.moving_rtt.add_sample(&Duration::from_secs(3));
            return;
        };
        if self.slow_start {
            self.desired_queue_size += 1;
            self.desired_queue_size = self.desired_queue_size.clamp(0, 500);
        }
        if self.pending_disconnect {
            // Restart slow_start here? Or clear rrt?
            self.pending_disconnect = false;
        }
        self.throughput += length as u64;
        let request = self.queued.remove(pos).unwrap();
        log::debug!("Subpiece completed: {}, {}", request.index, request.offset);
        let rtt = self.timeout_point.take().unwrap().elapsed();
        if !self.queued.is_empty() {
            self.timeout_point = Some(Instant::now());
        }
        self.moving_rtt.add_sample(&rtt);
    }

    // returns if the supiece actually was marked as a failure
    fn try_mark_subpiece_failed(&mut self, subpiece: Subpiece) -> bool {
        if let Some(piece) = self
            .currently_downloading
            .iter_mut()
            .find(|piece| piece.index == subpiece.index)
        {
            let subpiece_index = subpiece.offset / SUBPIECE_SIZE;
            if piece.completed_subpieces[subpiece_index as usize] {
                log::error!(
                    "[PeerId {}]: Subpiece is already completed, can't mark as failed {}, {}",
                    self.peer_id,
                    subpiece.index,
                    subpiece.offset
                );
                return false;
            }
            piece.on_subpiece_failed(subpiece.index, subpiece.offset);
            true
            // self.pending_disconnect = true;
        } else {
            false
            // This might race with it completing so this isn't really an error
        }
    }

    pub fn on_request_timeout(&mut self) {
        let Some(subpiece) = self.queued.pop_back() else {
            // might have been received later?
            log::warn!("Piece timed out but not found in queue");
            return;
        };
        if self.try_mark_subpiece_failed(subpiece) {
            log::warn!(
                "[PeerId {}]: Subpiece timed out: {}, {}",
                self.peer_id,
                subpiece.index,
                subpiece.offset
            );
            self.slow_start = false;
            // TODO: time out recovery
            self.desired_queue_size = 1;
        }
    }

    #[inline]
    fn is_valid_piece_req(&self, subpiece: Subpiece, num_pieces: i32) -> bool {
        subpiece.index >= 0
            && subpiece.index <= num_pieces
            && subpiece.offset % SUBPIECE_SIZE == 0
            && subpiece.size <= SUBPIECE_SIZE
    }

    pub fn handle_message(
        &mut self,
        conn_id: usize,
        peer_message: PeerMessage,
        torrent_state: &mut TorrentState,
    ) -> Result<&[OutgoingMsg], Error> {
        self.outgoing_msgs_buffer.clear();
        //log::debug!("Received: {peer_message:?}");
        match peer_message {
            PeerMessage::Choke => {
                log::error!("[Peer: {}] Peer is choking us!", self.peer_id);
                self.peer_choking = true;
            }
            PeerMessage::Unchoke => {
                self.peer_choking = false;
                if !self.is_interested {
                    // Not interested so don't do anything
                    return Ok(&self.outgoing_msgs_buffer);
                }
                // TODO: Get rid of this, should be allowed to continue here?
                // or this is controlled by the tick?
                if !self.currently_downloading.is_empty() {
                    return Ok(&self.outgoing_msgs_buffer);
                }
                if let Some(piece_idx) = torrent_state.piece_selector.next_piece(conn_id) {
                    log::info!("[Peer: {}] Unchoked and start downloading", self.peer_id);
                    self.unchoke(torrent_state, true);
                    self.request_piece(piece_idx, &mut torrent_state.piece_selector);
                } else {
                    log::warn!("[Peer: {}] No more pieces available", self.peer_id);
                }
            }
            PeerMessage::Interested => {
                let should_unchoke = torrent_state.should_unchoke();
                log::info!("[Peer: {}] Peer is interested in us!", self.peer_id);
                self.peer_interested = true;
                if !self.is_choking {
                    // if we are not choking them we might need to send a
                    // unchoke to avoid some race conditions. Libtorrent
                    // uses the same type of logic
                    self.unchoke(torrent_state, true);
                } else if should_unchoke {
                    log::debug!("[Peer: {}] Unchoking peer after intrest", self.peer_id);
                    self.unchoke(torrent_state, true);
                }
            }
            PeerMessage::NotInterested => {
                self.peer_interested = false;
                log::info!(
                    "[Peer: {}] Peer is no longer interested in us!",
                    self.peer_id
                );
                self.choke(torrent_state, false);
            }
            PeerMessage::Have { index } => {
                log::info!(
                    "[Peer: {}] Peer have piece with index: {index}",
                    self.peer_id
                );
                torrent_state
                    .piece_selector
                    .set_peer_piece(conn_id, index as usize);
            }
            PeerMessage::AllowedFast { index } => {
                if !self.fast_ext {
                    return Err(Error::Disconnect(
                        "Allowed fast received when fast_ext wasn't enabled",
                    ));
                }
                if index < 0 || index > torrent_state.torrent_info.pieces.len() as i32 {
                    log::warn!("[PeerId: {}] Invalid allowed fast message", self.peer_id);
                } else if !self.allowed_fast_pieces.contains(&index) {
                    self.allowed_fast_pieces.push(index);
                    if !torrent_state.piece_selector.has_completed(index as usize)
                        && !torrent_state.piece_selector.is_inflight(index as usize)
                        && torrent_state
                            .piece_selector
                            .do_peer_have_piece(conn_id, index as usize)
                    {
                        log::info!(
                            "[PeerId: {}] Requesting new piece {index} via Allowed fast set!",
                            self.peer_id
                        );
                        // Mark ourselves as interested
                        self.interested(true);
                        torrent_state.piece_selector.mark_inflight(index as usize);
                        self.currently_downloading.push(Piece::new(
                            index,
                            torrent_state.piece_selector.piece_len(index),
                        ));
                        self.fill_request_queue();
                    }
                }
            }
            PeerMessage::HaveAll => {
                if !self.fast_ext {
                    return Err(Error::Disconnect(
                        "Have all received when fast_ext wasn't enabled",
                    ));
                }
                if torrent_state.piece_selector.bitfield_received(conn_id) {
                    log::error!(
                        "[PeerId: {}] (HaveAll) Bitfield already received",
                        self.peer_id
                    );
                }
                let num_pieces = torrent_state.torrent_info.pieces.len();
                log::info!("[Peer: {}] Have all received", self.peer_id);
                let bitfield = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
                torrent_state
                    .piece_selector
                    .update_peer_pieces(conn_id, bitfield.into_boxed_bitslice());
                // Mark ourselves as interested
                self.interested(true);
            }
            PeerMessage::HaveNone => {
                if !self.fast_ext {
                    return Err(Error::Disconnect(
                        "Have none received when fast_ext wasn't enabled",
                    ));
                }
                if torrent_state.piece_selector.bitfield_received(conn_id) {
                    log::error!(
                        "[PeerId: {}] (HaveNone) Bitfield already received",
                        self.peer_id
                    );
                }
                let num_pieces = torrent_state.torrent_info.pieces.len();
                log::info!("[Peer: {}] Have None received", self.peer_id);
                let bitfield = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
                torrent_state
                    .piece_selector
                    .update_peer_pieces(conn_id, bitfield.into_boxed_bitslice());
            }
            PeerMessage::Bitfield(mut field) => {
                if torrent_state.piece_selector.bitfield_received(conn_id) && self.fast_ext {
                    log::error!("[PeerId: {}] Bitfield already received", self.peer_id);
                }
                if torrent_state.torrent_info.pieces.len() != field.len() {
                    if field.len() < torrent_state.torrent_info.pieces.len() {
                        log::error!(
                            "[Peer: {}] Received invalid bitfield, expected {}, got: {}",
                            self.peer_id,
                            torrent_state.torrent_info.pieces.len(),
                            field.len()
                        );
                        return Err(Error::Disconnect("Invalid bitfield"));
                    }
                    // The bitfield might be padded with zeros, remove them first
                    log::debug!(
                        "[Peer: {}] Received padded bitfield, expected {}, got: {}",
                        self.peer_id,
                        torrent_state.torrent_info.pieces.len(),
                        field.len()
                    );
                    field.truncate(torrent_state.torrent_info.pieces.len());
                }
                log::info!("[Peer: {}] Bifield received", self.peer_id);
                torrent_state
                    .piece_selector
                    .update_peer_pieces(conn_id, field.into_boxed_bitslice());
                // Mark ourselves as interested
                self.interested(true);
            }
            PeerMessage::SuggestPiece { index } => {
                if !self.fast_ext {
                    return Err(Error::Disconnect(
                        "Received suggest piece without fast_ext being enabled",
                    ));
                }
                log::info!("[Peer: {}] received suggested piece: {index}", self.peer_id);
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let subpiece = Subpiece {
                    index,
                    offset: begin,
                    size: length,
                };
                if !self
                    .is_valid_piece_req(subpiece, torrent_state.torrent_info.pieces.len() as i32)
                {
                    log::warn!(
                        "[Peer: {}] Piece request ignored, invalid request",
                        self.peer_id
                    );
                    todo!("reqject request")
                }
                let should_unchoke = torrent_state.should_unchoke();
                if should_unchoke && self.is_choking {
                    self.unchoke(torrent_state, true);
                }
                if !self.is_choking {
                    unimplemented!();
                    /*match self.on_piece_request(index, begin, length).await {
                        Ok(piece_data) => {
                            let peer_connection = connection_mut_or_return!();
                            peer_connection.piece(index, begin, piece_data)?;
                        }
                        Err(err) => {
                            log::error!("[Peer: {}] Invalid piece request: {err}", self.peer_id)
                        }
                    }*/
                } else {
                    log::info!(
                        "[Peer: {}] Piece request ignored, peer can't be unchoked",
                        self.peer_id
                    );
                }
            }
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                if !self.fast_ext {
                    return Err(Error::Disconnect(
                        "Received reject request without fast_ext being enabled",
                    ));
                }
                let subpiece = Subpiece {
                    index,
                    offset: begin,
                    size: length,
                };
                if !self
                    .is_valid_piece_req(subpiece, torrent_state.torrent_info.pieces.len() as i32)
                {
                    log::warn!(
                        "[Peer: {}] Piece Reject request ignored, invalid request",
                        self.peer_id
                    );
                }
                // TODO disconnect if receiving a reject for a never requested piece
                if self.peer_choking {
                    // Remove from the allowed fast set if it was reported there since it
                    // apparently wasn't allowed fast
                    if let Some(i) = self.allowed_fast_pieces.iter().position(|i| index == *i) {
                        self.allowed_fast_pieces.swap_remove(i);
                    }
                } else if self.try_mark_subpiece_failed(subpiece) {
                    if let Some(i) = self.queued.iter().position(|q_sub| *q_sub == subpiece) {
                        log::warn!(
                            "[PeerId {}]: Subpiece request rejected: {index}, {begin}",
                            self.peer_id,
                        );
                        self.queued.remove(i);
                    } else {
                        log::error!(
                            "[PeerId {}]: Subpiece request rejected twice: {index}, {begin}",
                            self.peer_id,
                        );
                    }
                }
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => todo!(),
            PeerMessage::Piece { index, begin, data } => {
                // TODO: disconnect on recv piece never requested if fast_ext is enabled
                log::trace!(
                    "[Peer: {}] Recived a piece index: {index}, begin: {begin}, length: {}",
                    self.peer_id,
                    data.len(),
                );
                self.update_stats(index, begin, data.len() as u32);
                if let Some(piece) = self.on_subpiece(index, begin, data) {
                    torrent_state.on_piece_completed(piece.index, piece.memory);
                    log::info!(
                        "[Peer: {}] Piece {}/{} completed!",
                        self.peer_id,
                        torrent_state.piece_selector.total_completed(),
                        torrent_state.piece_selector.pieces()
                    );
                }
            }
        }
        Ok(&self.outgoing_msgs_buffer)
    }
}
