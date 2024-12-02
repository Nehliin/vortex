use std::{
     io::{self}, os::fd::RawFd, time::Duration
};

use bytes::Bytes;
use thiserror::Error;

use crate::{
    peer_protocol::{PeerId, PeerMessage, PeerMessageDecoder},
    piece_selector::{Piece, Subpiece, SUBPIECE_SIZE},
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
    #[error("Peer should be disconnected")]
    Disconnect,
    #[error("Peer encountered IO issue")]
    Io(#[source] io::Error),
}

#[derive(Debug)]
pub struct OutgoingMsg {
    pub message: PeerMessage,
    pub ordered: bool,
    pub timeout: Option<(Subpiece, Duration)>,
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
    /// The peer have informed us that it is choking us.
    pub peer_choking: bool,
    /// The peer is interested what we have to offer
    pub peer_interested: bool,
    // TODO: do we need this both with queued?
    pub currently_downloading: Vec<Piece>,
    pub queue_capacity: usize,
    pub queued: Vec<(Instant, Subpiece)>,
    pub slow_start: bool,
    pub moving_rtt: MovingRttAverage,
    // TODO calculate this meaningfully
    //pub througput: u64,
    // If this connection is about to be disconnected
    // because of low througput. (Choke instead?)
    //pub pending_disconnect: bool,
    pub stateful_decoder: PeerMessageDecoder,
    // Stored here to prevent reallocations
    pub outgoing_msgs_buffer: Vec<OutgoingMsg>,
}

impl PeerConnection {
    pub fn new(fd: RawFd, peer_id: PeerId) -> Self {
        PeerConnection {
            fd,
            peer_id,
            is_choking: true,
            is_interested: true,
            peer_choking: true,
            peer_interested: false,
            queued: Vec::with_capacity(4),
            queue_capacity: 4,
            slow_start: true,
            moving_rtt: Default::default(),
            currently_downloading: Default::default(),
            outgoing_msgs_buffer: Default::default(),
            stateful_decoder: PeerMessageDecoder::new(2 << 15),
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
            timeout: None,
        });
    }

    fn interested(&mut self, ordered: bool) {
        self.is_interested = true;
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Interested,
            ordered,
            timeout: None,
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
            timeout: None,
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
                if self.queued.len() < self.queue_capacity {
                    // must update to prevent re-requesting same piece
                    available_pieces.set(subindex, true);
                    Self::push_subpiece_request(
                        &mut self.outgoing_msgs_buffer,
                        &mut self.queued,
                        piece,
                        subindex,
                        &self.moving_rtt,
                    );
                } else {
                    break 'outer;
                }
            }
        }
    }

    fn push_subpiece_request(
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
        queued: &mut Vec<(Instant, Subpiece)>,
        piece: &mut Piece,
        subindex: usize,
        moving_rtt: &MovingRttAverage,
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
        queued.push((Instant::now(), subpiece));
        let timeout_threshold = moving_rtt.mean() + (moving_rtt.average_deviation() * 4);
        let timeout_threshold = if timeout_threshold == Duration::from_millis(0) {
            Duration::from_millis(2500)
        } else {
            timeout_threshold
        };
        outgoing_msgs_buffer.push(OutgoingMsg {
            message: subpiece_request,
            ordered: false,
            timeout: Some((subpiece, timeout_threshold.min(Duration::from_millis(2500)))),
        });
    }
    fn on_subpiece(&mut self, m_index: i32, m_begin: i32, data: Bytes) -> Option<Piece> {
        let position = self
            .currently_downloading
            .iter()
            .position(|piece| piece.index == m_index);
        if let Some(position) = position {
            let mut piece = self.currently_downloading.swap_remove(position);
            piece.on_subpiece(m_index, m_begin, &data[..], self.peer_id);
            if !piece.is_complete() {
                // Next subpice to download (that isn't already inflight)
                while let Some(next_subpiece) = piece.next_unstarted_subpice() {
                    if self.queued.len() < self.queue_capacity {
                        Self::push_subpiece_request(
                            &mut self.outgoing_msgs_buffer,
                            &mut self.queued,
                            &mut piece,
                            next_subpiece,
                            &self.moving_rtt,
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

    pub fn on_request_timeout(&mut self, subpiece: Subpiece) {
        if let Some(piece) = self
            .currently_downloading
            .iter_mut()
            .find(|piece| piece.index == subpiece.index)
        {
            let subpiece_index = subpiece.offset / SUBPIECE_SIZE;
            // TODO HANDLE THIS BETTER
            if piece.completed_subpieces[subpiece_index as usize] {
                log::debug!(
                    "[PeerId {}]: Subpiece NOT timed out: {}, {}",
                    self.peer_id,
                    subpiece.index,
                    subpiece.offset
                );
                return;
            }
            log::warn!(
                "[PeerId {}]: Subpiece timeout: {}, {}",
                self.peer_id,
                subpiece.index,
                subpiece.offset
            );
            // This is too harsh and should not be determined by individual
            // timeouts?
            self.slow_start = false;
            // TODO: time out recovery
            self.queue_capacity = 1;
            piece.on_subpiece_failed(subpiece.index, subpiece.offset);
            let position = self
                .queued
                .iter()
                .map(|(_, sub)| sub)
                .position(|queued| {
                    queued.index == subpiece.index && queued.offset == subpiece.offset
                })
                .unwrap();
            self.queued.swap_remove(position);
            self.pending_disconnect = true;
        } else {
            // This might race with it completing so this isn't really an error
            //log::error!(
            //   "[PeerId {}]: Peer wasn't dowloading parent piece: {}",
            //  self.peer_id,
            // subpiece.index
            //);
        }
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
                log::info!("[Peer: {}] Peer is choking us!", self.peer_id);
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
            PeerMessage::Bitfield(mut field) => {
                if torrent_state.torrent_info.pieces.len() != field.len() {
                    if field.len() < torrent_state.torrent_info.pieces.len() {
                        log::error!(
                            "[Peer: {}] Received invalid bitfield, expected {}, got: {}",
                            self.peer_id,
                            torrent_state.torrent_info.pieces.len(),
                            field.len()
                        );
                        return Err(Error::Disconnect);
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
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
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
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => todo!(),
            PeerMessage::Piece { index, begin, data } => {
                log::info!(
                    "[Peer: {}] Recived a piece index: {index}, begin: {begin}, length: {}",
                    self.peer_id,
                    data.len(),
                );
                //let connection_state = peer_connection.state_mut();
                //connection_state.update_stats(index, begin, data.len() as u32);
                if let Some(piece) = self.on_subpiece(index, begin, data) {
                    log::info!(
                        "[Peer: {}] Piece {}/{} completed!",
                        self.peer_id,
                        torrent_state.piece_selector.total_completed(),
                        torrent_state.piece_selector.pieces()
                    );
                    torrent_state.on_piece_completed(piece.index, piece.memory);
                    torrent_state
                        .piece_selector
                        .mark_complete(piece.index as usize);
                    if torrent_state.piece_selector.completed_all() {
                        panic!("TOrrent complete!")
                    }
                }
            }
        }
        Ok(&self.outgoing_msgs_buffer)
    }
}
