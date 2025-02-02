use std::{
    collections::VecDeque,
    net::Ipv4Addr,
    time::{Duration, Instant},
};

use bytes::Bytes;
use rayon::Scope;
use sha1::Digest;
use socket2::Socket;

use crate::{
    file_store::{FileStore, ReadablePieceFileView},
    peer_protocol::{PeerId, PeerMessage, PeerMessageDecoder},
    piece_selector::{CompletedPiece, Piece, PieceSelector, Subpiece, SUBPIECE_SIZE},
    Error, TorrentState,
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

// TODO: don't send pieces the peer already have
fn generate_fast_set(
    set_size: u32,
    num_pieces: u32,
    info_hash: &[u8; 20],
    ip: Ipv4Addr,
    fast_set: &mut Vec<i32>,
) {
    fast_set.clear();
    let mut x = Vec::with_capacity(24);
    let ip = ip.to_bits() & 0xffffff00;
    x.extend_from_slice(&ip.to_be_bytes());
    x.extend_from_slice(info_hash);
    let mut max_attempts = 300;
    while fast_set.len() < set_size as usize && max_attempts > 0 {
        max_attempts -= 1;
        let mut hasher = sha1::Sha1::new();
        hasher.update(&x);
        x = hasher.finalize().to_vec();
        for i in 0..5 {
            if fast_set.len() >= set_size as usize {
                break;
            }
            let j = i * 4;
            let y = u32::from_be_bytes(x[j..j + 4].try_into().unwrap());
            let index = (y % num_pieces) as i32;
            if !fast_set.contains(&index) {
                fast_set.push(index);
            }
        }
    }
}

#[derive(Debug)]
pub struct OutgoingMsg {
    pub message: PeerMessage,
    pub ordered: bool,
}

#[derive(Error, Debug)]
pub enum DisconnectReason {
    #[error("Peer closed the connection")]
    ClosedConnection,
    #[error("Peer was idle for too long")]
    Idle,
    #[error("Peer reset the underlying connection")]
    TcpReset,
    #[error("Protocol error {0}")]
    ProtocolError(&'static str),
    #[error("Invalid message received")]
    InvalidMessage,
}

pub struct PeerConnection<'f_store> {
    pub socket: Socket,
    pub peer_id: PeerId,
    /// This side is choking the peer
    pub is_choking: bool,
    /// This side is interested what the peer has to offer
    pub is_interested: bool,
    /// Have we sent allowed fast set yet too the peer
    pub sent_allowed_fast: bool,
    pub fast_ext: bool,
    /// The peer have informed us that it is choking us.
    pub peer_choking: bool,
    /// The peer is interested what we have to offer
    pub peer_interested: bool,
    // TODO: do we need this both with queued?
    pub currently_downloading: Vec<Piece<'f_store>>,
    // Target queue size
    pub desired_queue_size: usize,
    // Currently queued requests that may be inflight
    pub queued: VecDeque<Subpiece>,
    // Time since any data was received
    pub last_seen: Instant,
    // Time since last received subpiece request, used to timeout
    // requests
    pub last_received_subpiece: Option<Instant>,
    pub slow_start: bool,
    // The averge time between pieces being received
    pub moving_rtt: MovingRttAverage,
    pub throughput: u64,
    pub prev_throughput: u64,
    // If this connection is about to be disconnected
    // because of low througput. (Choke instead?)
    pub pending_disconnect: Option<DisconnectReason>,
    pub stateful_decoder: PeerMessageDecoder,
    // Stored here to prevent reallocations
    pub outgoing_msgs_buffer: Vec<OutgoingMsg>,
    // Uses vec instead of hashset since this is expected to be small
    pub allowed_fast_pieces: Vec<i32>,
    // The pieces we allow others to request when choked
    pub accept_fast_pieces: Vec<i32>,
}

impl<'scope, 'f_store: 'scope> PeerConnection<'f_store> {
    pub fn new(socket: Socket, peer_id: PeerId, fast_ext: bool) -> Self {
        PeerConnection {
            socket,
            peer_id,
            is_choking: true,
            is_interested: false,
            sent_allowed_fast: false,
            peer_choking: true,
            peer_interested: false,
            last_received_subpiece: None,
            fast_ext,
            queued: VecDeque::with_capacity(64),
            desired_queue_size: 4,
            last_seen: Instant::now(),
            slow_start: true,
            moving_rtt: Default::default(),
            currently_downloading: Default::default(),
            pending_disconnect: None,
            throughput: 0,
            prev_throughput: 0,
            outgoing_msgs_buffer: Default::default(),
            stateful_decoder: PeerMessageDecoder::new(2 << 15),
            allowed_fast_pieces: Default::default(),
            accept_fast_pieces: Default::default(),
        }
    }

    pub fn release_pieces(&mut self, torrent_state: &mut TorrentState) {
        for piece in self.currently_downloading.iter() {
            torrent_state
                .piece_selector
                .mark_not_inflight(piece.index as usize)
        }
        self.currently_downloading.clear();
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

    fn reject_request(&mut self, subpiece: Subpiece, ordered: bool) {
        // TODO: Disconnect on too many rejected pieces
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::RejectRequest {
                index: subpiece.index,
                begin: subpiece.offset,
                length: subpiece.size,
            },
            ordered,
        });
    }

    fn send_piece(&mut self, index: i32, offset: i32, data: Bytes, ordered: bool) {
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Piece {
                index,
                begin: offset,
                data,
            },
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

    pub fn request_piece(
        &mut self,
        index: i32,
        piece_selector: &mut PieceSelector,
        file_store: &'f_store FileStore,
    ) {
        let length = piece_selector.piece_len(index);
        piece_selector.mark_inflight(index as usize);
        // SAFETY: we check before that the piece is not already in flight (via mark_inflight)
        // and is not already completed which means there can't exist any other
        // writable piece views for this index. The piece is also marked as
        // inflight right before.
        let piece_view = unsafe { file_store.writable_piece_view(index).unwrap() };
        self.currently_downloading
            .push(Piece::new(index, length, piece_view));
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
                        &mut self.last_received_subpiece,
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

    fn on_subpiece(
        &mut self,
        m_index: i32,
        m_begin: i32,
        data: Bytes,
    ) -> Option<ReadablePieceFileView<'f_store>> {
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
                    if self.queued.len() < self.desired_queue_size {
                        Self::push_subpiece_request(
                            &mut self.outgoing_msgs_buffer,
                            &mut self.queued,
                            &mut self.last_received_subpiece,
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
                return Some(piece.into_readable());
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
            return;
        };
        if self.slow_start {
            self.desired_queue_size += 1;
            self.desired_queue_size = self.desired_queue_size.clamp(0, 500);
        }
        self.throughput += length as u64;
        let request = self.queued.remove(pos).unwrap();
        log::debug!("Subpiece completed: {}, {}", request.index, request.offset);
        let rtt = self.last_received_subpiece.take().unwrap().elapsed();
        if !self.queued.is_empty() {
            self.last_received_subpiece = Some(Instant::now());
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

    pub fn on_request_timeout(&mut self, torrent_state: &mut TorrentState) {
        let Some(subpiece) = self.queued.pop_back() else {
            // Probably caused by the request being rejected or the timeout happen because
            // we had not requested anything more
            log::warn!(
                "[PeerId: {}] Piece timed out but not found in queue",
                self.peer_id
            );
            // Don't timeout again
            self.last_received_subpiece = None;
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
            self.release_pieces(torrent_state);
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
        file_store: &'f_store FileStore,
        torrent_info: &'scope lava_torrent::torrent::v1::Torrent,
        scope: &Scope<'scope>,
    ) {
        self.outgoing_msgs_buffer.clear();
        self.last_seen = Instant::now();
        match peer_message {
            PeerMessage::Choke => {
                log::error!("[Peer: {}] Peer is choking us!", self.peer_id);
                self.peer_choking = true;
            }
            PeerMessage::Unchoke => {
                self.peer_choking = false;
                if !self.is_interested {
                    // Not interested so don't do anything
                    return;
                }
                // TODO: Get rid of this, should be allowed to continue here?
                // or this is controlled by the tick?
                //if !self.currently_downloading.is_empty() {
                //   return Ok(&self.outgoing_msgs_buffer);
                //}
                if let Some(piece_idx) = torrent_state.piece_selector.next_piece(conn_id) {
                    log::info!("[Peer: {}] Unchoked and start downloading", self.peer_id);
                    self.unchoke(torrent_state, true);
                    self.request_piece(piece_idx, &mut torrent_state.piece_selector, file_store);
                } else {
                    log::warn!("[Peer: {}] No more pieces available", self.peer_id);
                }
            }
            PeerMessage::Interested => {
                let should_unchoke = torrent_state.should_unchoke();
                log::info!("[Peer: {}] Peer is interested in us!", self.peer_id);
                self.peer_interested = true;
                if !self.sent_allowed_fast && self.fast_ext {
                    self.sent_allowed_fast = true;
                    const ALLOWED_FAST_SET_SIZE: usize = 6;
                    if ALLOWED_FAST_SET_SIZE >= torrent_state.num_pieces() {
                        for index in 0..torrent_state.num_pieces() {
                            if !torrent_state
                                .piece_selector
                                .do_peer_have_piece(conn_id, index)
                            {
                                let index = index as i32;
                                if !self.accept_fast_pieces.contains(&index) {
                                    self.accept_fast_pieces.push(index);
                                }
                                self.outgoing_msgs_buffer.push(OutgoingMsg {
                                    message: PeerMessage::AllowedFast { index },
                                    ordered: false,
                                });
                            }
                        }
                    } else {
                        generate_fast_set(
                            ALLOWED_FAST_SET_SIZE as u32,
                            torrent_state.num_pieces() as u32,
                            &torrent_state.info_hash,
                            *self
                                .socket
                                .peer_addr()
                                .expect("Socket should be connected")
                                .as_socket_ipv4()
                                .expect("Only ipv4 addresses are supported")
                                .ip(),
                            &mut self.accept_fast_pieces,
                        );
                        for index in self.accept_fast_pieces.iter().copied() {
                            self.outgoing_msgs_buffer.push(OutgoingMsg {
                                message: PeerMessage::AllowedFast { index },
                                ordered: false,
                            });
                        }
                    }
                }
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
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Allowed fast received when fast_ext wasn't enabled",
                    ));
                    return;
                }
                if index < 0 || index > torrent_state.num_pieces() as i32 {
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
                        self.request_piece(index, &mut torrent_state.piece_selector, file_store);
                    }
                }
            }
            PeerMessage::HaveAll => {
                if !self.fast_ext {
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Have all received when fast_ext wasn't enabled",
                    ));
                    return;
                }
                if torrent_state.piece_selector.bitfield_received(conn_id) {
                    log::error!(
                        "[PeerId: {}] (HaveAll) Bitfield already received",
                        self.peer_id
                    );
                }
                let num_pieces = torrent_state.num_pieces();
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
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Have none received when fast_ext wasn't enabled",
                    ));
                    return;
                }
                if torrent_state.piece_selector.bitfield_received(conn_id) {
                    log::error!(
                        "[PeerId: {}] (HaveNone) Bitfield already received",
                        self.peer_id
                    );
                }
                let num_pieces = torrent_state.num_pieces();
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
                if torrent_state.num_pieces() != field.len() {
                    if field.len() < torrent_state.num_pieces() {
                        log::error!(
                            "[Peer: {}] Received invalid bitfield, expected {}, got: {}",
                            self.peer_id,
                            torrent_state.num_pieces(),
                            field.len()
                        );
                        self.pending_disconnect =
                            Some(DisconnectReason::ProtocolError("Invalid bitfield"));
                        return;
                    }
                    // The bitfield might be padded with zeros, remove them first
                    log::debug!(
                        "[Peer: {}] Received padded bitfield, expected {}, got: {}",
                        self.peer_id,
                        torrent_state.num_pieces(),
                        field.len()
                    );
                    field.truncate(torrent_state.num_pieces());
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
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Received suggest piece without fast_ext being enabled",
                    ));
                    return;
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
                // returns if it was accepted or not
                let mut handle_req = || {
                    if !self.is_valid_piece_req(subpiece, torrent_state.num_pieces() as i32) {
                        log::warn!(
                            "[Peer: {}] Piece request ignored/rejected, invalid request",
                            self.peer_id
                        );
                        None
                    } else {
                        let should_unchoke = torrent_state.should_unchoke();
                        if should_unchoke && self.is_choking {
                            self.unchoke(torrent_state, true);
                        }
                        // We are either not choking or the piece is part of the fast set and they
                        // support the fast ext
                        if !self.is_choking
                            || (self.accept_fast_pieces.contains(&subpiece.index) && self.fast_ext)
                        {
                            if !torrent_state
                                .piece_selector
                                .has_completed(subpiece.index as usize)
                            {
                                return None;
                            }
                            // TODO: cache this
                            // SAFETY: we've check that this is completed, in that case no other
                            // writers should exist for the piece
                            let Ok(readable_piece_view) =
                                (unsafe { file_store.readable_piece_view(index) })
                            else {
                                return None;
                            };
                            // TODO: consider using maybe uninit
                            let mut subpiece_data =
                                vec![0; subpiece.size as usize].into_boxed_slice();
                            readable_piece_view
                                .read_subpiece(subpiece.offset as usize, &mut subpiece_data);
                            Some(subpiece_data)
                        } else {
                            log::warn!(
                                "[Peer: {}] Piece request ignored/rejected, peer can't be unchoked",
                                self.peer_id
                            );
                            None
                        }
                    }
                };
                if let Some(piece_data) = handle_req() {
                    self.send_piece(subpiece.index, subpiece.offset, piece_data.into(), false);
                } else if self.fast_ext {
                    self.reject_request(subpiece, false);
                }
            }
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                if !self.fast_ext {
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Received reject request without fast_ext being enabled",
                    ));
                    return;
                }
                let subpiece = Subpiece {
                    index,
                    offset: begin,
                    size: length,
                };
                if !self.is_valid_piece_req(subpiece, torrent_state.num_pieces() as i32) {
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
            } => {
                log::trace!("[Peer: {}] Received cancel request, index: {index}, begin: {begin}, length: {length}", self.peer_id);
                // if we are talking to a fast_ext peer we need to respond with something here,
                // either reject or a piece
                if self.fast_ext {
                    let subpiece = Subpiece {
                        index,
                        offset: begin,
                        size: length,
                    };
                    if !self
                        .outgoing_msgs_buffer
                        .iter()
                        .any(|msg| match msg.message {
                            PeerMessage::RejectRequest {
                                index,
                                begin,
                                length,
                            } if index == subpiece.index
                                && subpiece.offset == begin
                                && subpiece.size == length =>
                            {
                                true
                            }
                            PeerMessage::Piece { index, begin, .. }
                                if index == subpiece.index && subpiece.offset == begin =>
                            {
                                true
                            }
                            _ => false,
                        })
                    {
                        // We've not already queued up a response
                        // so reject the request
                        self.reject_request(subpiece, false);
                    }
                }
            }
            PeerMessage::Piece { index, begin, data } => {
                // TODO: disconnect on recv piece never requested if fast_ext is enabled
                log::trace!(
                    "[Peer: {}] Recived a piece index: {index}, begin: {begin}, length: {}",
                    self.peer_id,
                    data.len(),
                );
                self.update_stats(index, begin, data.len() as u32);
                if let Some(readable_piece_view) = self.on_subpiece(index, begin, data) {
                    let complete_tx = torrent_state.completed_piece_tx.clone();
                    scope.spawn(move |_| {
                        let hash = &torrent_info.pieces[readable_piece_view.index as usize];
                        let hash_check_result = readable_piece_view.sync_and_check_hash(hash);
                        complete_tx
                            .send(CompletedPiece {
                                index: index as usize,
                                hash_matched: hash_check_result,
                            })
                            .unwrap();
                    });
                }
            }
        }
    }
}
