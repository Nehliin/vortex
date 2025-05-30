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
    Error, TorrentState,
    event_loop::MAX_QUEUE_SIZE,
    file_store::FileStore,
    peer_protocol::{PeerId, PeerMessage, PeerMessageDecoder},
    piece_selector::{CompletedPiece, SUBPIECE_SIZE, Subpiece},
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

#[derive(Debug, PartialEq)]
pub struct OutgoingMsg {
    pub message: PeerMessage,
    pub ordered: bool,
}

#[derive(Error, Debug)]
pub enum DisconnectReason {
    #[error("Peer was idle for too long")]
    Idle,
    #[error("Protocol error {0}")]
    ProtocolError(&'static str),
    #[error("Invalid message received")]
    InvalidMessage,
    #[error("Event loop is shutting down")]
    ShuttingDown,
}

pub struct PeerConnection {
    pub socket: Socket,
    pub conn_id: usize,
    pub peer_id: PeerId,
    /// This side is choking the peer
    pub is_choking: bool,
    /// This side is interested what the peer has to offer
    pub is_interesting: bool,
    /// Have we sent allowed fast set yet too the peer
    pub sent_allowed_fast: bool,
    pub fast_ext: bool,
    /// The peer have informed us that it is choking us.
    pub peer_choking: bool,
    /// The peer is interested what we have to offer
    pub peer_interested: bool,
    // Target number of inflight requests
    pub target_inflight: usize,
    // Current inflight requests, may have timed out
    pub inflight: VecDeque<Subpiece>,
    // Queued requests
    pub queued: VecDeque<Subpiece>,
    // Time since any data was received
    pub last_seen: Instant,
    // Is this peer in endgame mode?
    pub endgame: bool,
    // Time since last received subpiece request, used to timeout
    // requests
    pub last_received_subpiece: Option<Instant>,
    pub slow_start: bool,
    pub snubbed: bool,
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

impl<'scope, 'f_store: 'scope> PeerConnection {
    pub fn new(socket: Socket, peer_id: PeerId, conn_id: usize, fast_ext: bool) -> Self {
        PeerConnection {
            socket,
            conn_id,
            peer_id,
            is_choking: true,
            is_interesting: false,
            sent_allowed_fast: false,
            peer_choking: true,
            peer_interested: false,
            endgame: false,
            last_received_subpiece: None,
            fast_ext,
            inflight: VecDeque::with_capacity(64),
            queued: VecDeque::with_capacity(64),
            target_inflight: 4,
            last_seen: Instant::now(),
            slow_start: true,
            snubbed: false,
            moving_rtt: Default::default(),
            pending_disconnect: None,
            throughput: 0,
            prev_throughput: 0,
            outgoing_msgs_buffer: Default::default(),
            stateful_decoder: PeerMessageDecoder::new(2 << 15),
            allowed_fast_pieces: Default::default(),
            accept_fast_pieces: Default::default(),
        }
    }

    pub fn release_all_pieces(&mut self, torrent_state: &mut TorrentState) {
        let pieces =
            self.queued
                .iter()
                .map(|subpiece| subpiece.index)
                .fold(Vec::new(), |mut acc, s| {
                    if !acc.contains(&s) {
                        acc.push(s);
                    }
                    acc
                });
        for piece in pieces {
            torrent_state.deallocate_piece(piece, self.conn_id);
        }
        self.queued.clear();
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

    pub fn have(&mut self, index: i32, ordered: bool) {
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Have { index },
            ordered,
        });
    }

    fn reject_request(&mut self, index: i32, begin: i32, length: i32, ordered: bool) {
        // TODO: Disconnect on too many rejected pieces
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::RejectRequest {
                index,
                begin,
                length,
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
        // Consider requesting pieces here if we are unchoked
        // this might happen after an unchoke request
        self.is_interesting = true;
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::Interested,
            ordered,
        });
    }

    pub fn not_interested(&mut self, ordered: bool) {
        self.is_interesting = false;
        self.outgoing_msgs_buffer.push(OutgoingMsg {
            message: PeerMessage::NotInterested,
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

    pub fn update_target_inflight(&mut self, target_inflight: usize) {
        if self.snubbed {
            self.target_inflight = 1;
            return;
        }
        self.target_inflight = target_inflight.clamp(0, MAX_QUEUE_SIZE);
        self.target_inflight = self.target_inflight.max(1);
    }

    pub fn append_and_fill(&mut self, to_append: &mut VecDeque<Subpiece>) {
        self.queued.append(to_append);
        self.fill_request_queue();
    }

    pub fn fill_request_queue(&mut self) {
        while self.inflight.len() < self.target_inflight {
            if let Some(subpiece) = self.queued.pop_front() {
                Self::push_subpiece_request(
                    &mut self.outgoing_msgs_buffer,
                    &mut self.inflight,
                    &mut self.last_received_subpiece,
                    subpiece,
                );
            } else {
                break;
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
        inflight: &mut VecDeque<Subpiece>,
        timeout_timer: &mut Option<Instant>,
        subpiece: Subpiece,
    ) {
        let subpiece_request = PeerMessage::Request {
            index: subpiece.index,
            begin: subpiece.offset,
            length: subpiece.size,
        };
        inflight.push_back(subpiece);
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
        if self.snubbed {
            return 1;
        }
        // This will work even if we are in a slow start since
        // the window will continue to increase until a timeout is hit
        // TODO: Should we really return 0 here?
        self.target_inflight - self.inflight.len().min(self.target_inflight)
    }

    pub fn update_stats(&mut self, m_index: i32, m_begin: i32, length: u32) {
        // horribly inefficient
        let Some(pos) = self
            .inflight
            .iter()
            .position(|sub| sub.index == m_index && m_begin == sub.offset)
        else {
            log::error!("Received unexpected piece message, index: {m_index}");
            return;
        };
        if self.slow_start {
            self.update_target_inflight(self.target_inflight + 1);
        }
        self.throughput += length as u64;
        let request = self.inflight.remove(pos).unwrap();
        log::trace!("Subpiece completed: {}, {}", request.index, request.offset);
        let rtt = self.last_received_subpiece.take().unwrap().elapsed();
        if !self.inflight.is_empty() {
            self.last_received_subpiece = Some(Instant::now());
        }
        self.moving_rtt.add_sample(&rtt);
    }

    pub fn report_metrics(&self) {
        let gauge = metrics::gauge!("peer.throughput.bytes", "peer_id" => self.peer_id.to_string());
        // Prev throughput is used since the mertics are reported at the end of TICK and
        // throughput have been reset and stored here at that point
        gauge.set(self.prev_throughput as u32);

        let gauge = metrics::gauge!("peer.target_inflight", "peer_id" => self.peer_id.to_string());
        gauge.set(self.target_inflight as u32);

        let gauge = metrics::gauge!("peer.queued", "peer_id" => self.peer_id.to_string());
        gauge.set(self.queued.len() as u32);

        let gauge = metrics::gauge!("peer.snubbed", "peer_id" => self.peer_id.to_string());
        gauge.set(if self.snubbed { 1 } else { 0 } as u32);

        let gauge = metrics::gauge!("peer.endgame", "peer_id" => self.peer_id.to_string());
        gauge.set(if self.endgame { 1 } else { 0 } as u32);

        let gauge = metrics::gauge!("peer.inflight", "peer_id" => self.peer_id.to_string());
        gauge.set(self.inflight.len() as u32);

        let histogram = metrics::histogram!("rtt", "peer_id" => self.peer_id.to_string());
        histogram.record(self.moving_rtt.mean());
    }

    pub fn on_request_timeout(
        &mut self,
        torrent_state: &mut TorrentState<'f_store>,
        file_store: &'f_store FileStore,
    ) {
        if !self.snubbed {
            self.snubbed = true;
            self.slow_start = false;
        }
        for subpiece in self.inflight.iter_mut().rev() {
            if !subpiece.timed_out {
                subpiece.timed_out = true;
                log::warn!(
                    "[PeerId {}]: Subpiece timed out: {}, {}",
                    self.peer_id,
                    subpiece.index,
                    subpiece.offset
                );
                subpiece.timed_out = true;
                // Request a new different piece and do it before clearing
                // the queue so the same piece isn't picked again
                let maybe_new_piece = torrent_state
                    .piece_selector
                    .next_piece(self.conn_id, &mut self.endgame);
                // Ensure this piece specifically is deallocated
                // TODO: This can be improved probably
                torrent_state.deallocate_piece(subpiece.index, self.conn_id);
                self.release_all_pieces(torrent_state);
                // Make it possible to request one more piece if this
                // is the final inflight piece
                self.target_inflight = 2;
                if let Some(new_piece) = maybe_new_piece {
                    let mut subpieces =
                        torrent_state.allocate_piece(new_piece, self.conn_id, file_store);
                    self.append_and_fill(&mut subpieces);
                }
                // Update to actual target
                self.target_inflight = 1;
                return;
            }
        }
    }

    #[inline]
    fn is_valid_piece_req(&self, index: i32, begin: i32, length: i32, num_pieces: i32) -> bool {
        index >= 0 && index <= num_pieces && begin % SUBPIECE_SIZE == 0 && length <= SUBPIECE_SIZE
    }

    #[inline]
    fn is_valid_piece(&self, index: i32, begin: i32, data_len: usize, num_pieces: usize) -> bool {
        index >= 0
            && index <= num_pieces as i32
            && begin % SUBPIECE_SIZE == 0
            && data_len <= SUBPIECE_SIZE as usize
    }

    pub fn handle_message(
        &mut self,
        peer_message: PeerMessage,
        torrent_state: &mut TorrentState<'f_store>,
        file_store: &'f_store FileStore,
        torrent_info: &'scope lava_torrent::torrent::v1::Torrent,
        scope: &Scope<'scope>,
    ) {
        self.last_seen = Instant::now();
        match peer_message {
            PeerMessage::Choke => {
                log::debug!("[Peer: {}] Peer is choking us!", self.peer_id);
                self.peer_choking = true;
                // Interpret all sent pieces as rejected
                if !self.fast_ext {
                    // Append them to queue so the release_pieces logic can release the inflight
                    // pieces as well
                    self.queued.append(&mut self.inflight);
                    self.inflight.clear();
                    // TODO handle as reject piece
                }
                self.release_all_pieces(torrent_state);
            }
            PeerMessage::Unchoke => {
                self.peer_choking = false;
                if !self.is_interesting {
                    // Not interested so don't do anything
                    return;
                }
                if let Some(piece_idx) = torrent_state
                    .piece_selector
                    .next_piece(self.conn_id, &mut self.endgame)
                {
                    log::info!("[Peer: {}] Unchoked and start downloading", self.peer_id);
                    self.unchoke(torrent_state, true);
                    let mut subpieces =
                        torrent_state.allocate_piece(piece_idx, self.conn_id, file_store);
                    // TODO: might be more than the peer can handle
                    self.append_and_fill(&mut subpieces);
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
                    // TODO: don't send pieces the peer already have
                    if ALLOWED_FAST_SET_SIZE >= torrent_state.num_pieces() {
                        for index in 0..torrent_state.num_pieces() {
                            let index = index as i32;
                            if !self.accept_fast_pieces.contains(&index) {
                                self.accept_fast_pieces.push(index);
                            }
                            self.outgoing_msgs_buffer.push(OutgoingMsg {
                                message: PeerMessage::AllowedFast { index },
                                ordered: false,
                            });
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
                if 0 > index || index >= torrent_state.num_pieces() as i32 {
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Invalid have index received",
                    ));
                    return;
                }
                log::info!(
                    "[Peer: {}] Peer have piece with index: {index}",
                    self.peer_id
                );
                let index = index as usize;
                let is_interesting = torrent_state
                    .piece_selector
                    .update_peer_piece_intrest(self.conn_id, index);
                if is_interesting && !self.is_interesting {
                    self.interested(false);
                }
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
                    if let Some(interesting_pieces) = torrent_state
                        .piece_selector
                        .interesting_peer_pieces(self.conn_id)
                    {
                        if interesting_pieces[index as usize]
                            && !torrent_state.piece_selector.is_allocated(index as usize)
                        {
                            log::info!(
                                "[PeerId: {}] Requesting new piece {index} via Allowed fast set!",
                                self.peer_id
                            );
                            // Mark ourselves as interested
                            self.interested(true);
                            let mut subpieces =
                                torrent_state.allocate_piece(index, self.conn_id, file_store);
                            self.append_and_fill(&mut subpieces);
                        }
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
                if torrent_state.piece_selector.bitfield_received(self.conn_id) {
                    log::warn!(
                        "[PeerId: {}] (HaveAll) Bitfield already received",
                        self.peer_id
                    );
                }
                let num_pieces = torrent_state.num_pieces();
                log::info!("[Peer: {}] Have all received", self.peer_id);
                let bitfield = bitvec::bitvec!(u8, bitvec::order::Msb0; 1; num_pieces);
                torrent_state
                    .piece_selector
                    .peer_bitfield(self.conn_id, bitfield.into_boxed_bitslice());
                if !torrent_state.is_complete {
                    // Mark ourselves as interested
                    self.interested(true);
                }
            }
            PeerMessage::HaveNone => {
                if !self.fast_ext {
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Have none received when fast_ext wasn't enabled",
                    ));
                    return;
                }
                if torrent_state.piece_selector.bitfield_received(self.conn_id) {
                    log::warn!(
                        "[PeerId: {}] (HaveNone) Bitfield already received",
                        self.peer_id
                    );
                }
                let num_pieces = torrent_state.num_pieces();
                log::info!("[Peer: {}] Have None received", self.peer_id);
                let bitfield = bitvec::bitvec!(u8, bitvec::order::Msb0; 0; num_pieces);
                self.not_interested(false);
                torrent_state
                    .piece_selector
                    .peer_bitfield(self.conn_id, bitfield.into_boxed_bitslice());
            }
            PeerMessage::Bitfield(mut field) => {
                if torrent_state.piece_selector.bitfield_received(self.conn_id) && self.fast_ext {
                    log::warn!("[PeerId: {}] Bitfield already received", self.peer_id);
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
                let field = field.into_boxed_bitslice();
                log::info!("[Peer: {}] Bifield received", self.peer_id);
                let is_interesting = torrent_state
                    .piece_selector
                    .peer_bitfield(self.conn_id, field.clone());

                // Mark ourselves as interested if there are pieces we would like request
                if !self.is_interesting && is_interesting {
                    self.interested(true);
                }
                // TODO: if unchocked already we should request stuff (in case they are recvd out
                // of order)
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
                // returns if it was accepted or not
                let mut handle_req = || {
                    if !self.is_valid_piece_req(
                        index,
                        begin,
                        length,
                        torrent_state.num_pieces() as i32,
                    ) {
                        log::warn!(
                            "[Peer: {}] Piece request ignored/rejected, invalid request",
                            self.peer_id
                        );
                        None
                    } else {
                        if !self.peer_interested {
                            self.peer_interested = true;
                        }
                        let should_unchoke = torrent_state.should_unchoke();
                        if should_unchoke && self.is_choking {
                            self.unchoke(torrent_state, true);
                        }
                        // We are either not choking or the piece is part of the fast set and they
                        // support the fast ext
                        if !self.is_choking
                            || (self.accept_fast_pieces.contains(&index) && self.fast_ext)
                        {
                            if !torrent_state.piece_selector.has_completed(index as usize) {
                                return None;
                            }
                            assert!(torrent_state.pieces[index as usize].is_none());
                            // TODO: cache this
                            // SAFETY: we've check that this is completed and the writable piece is gone
                            // from the piece vector in that case no other writers should exist for the piece
                            let Ok(readable_piece_view) =
                                (unsafe { file_store.readable_piece_view(index) })
                            else {
                                return None;
                            };
                            // TODO: consider using maybe uninit
                            let mut subpiece_data = vec![0; length as usize].into_boxed_slice();
                            readable_piece_view.read_subpiece(begin as usize, &mut subpiece_data);
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
                    self.send_piece(index, begin, piece_data.into(), false);
                } else if self.fast_ext {
                    self.reject_request(index, begin, length, false);
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
                if !self.is_valid_piece_req(index, begin, length, torrent_state.num_pieces() as i32)
                {
                    log::warn!(
                        "[Peer: {}] Piece Reject request ignored, invalid request",
                        self.peer_id
                    );
                }
                let mut defer_deallocation = false;
                if let Some(i) = self.inflight.iter().position(|q_sub| {
                    q_sub.index == index && q_sub.offset == begin && q_sub.size == length
                }) {
                    log::warn!(
                        "[PeerId {}]: Subpiece request rejected: {index}, {begin}",
                        self.peer_id,
                    );
                    self.inflight.remove(i).unwrap();
                    defer_deallocation = true;
                } else {
                    log::error!(
                        "[PeerId {}]: Subpiece not inflight rejected: {index}, {begin}",
                        self.peer_id,
                    );
                }
                // TODO disconnect if receiving a reject for a never requested piece
                if self.peer_choking {
                    // Remove from the allowed fast set if it was reported there since it
                    // apparently wasn't allowed fast
                    if let Some(i) = self.allowed_fast_pieces.iter().position(|i| index == *i) {
                        self.allowed_fast_pieces.swap_remove(i);
                    }
                } else if self.inflight.len() < 2 && self.queued.is_empty() {
                    if let Some(new_index) = torrent_state
                        .piece_selector
                        .next_piece(self.conn_id, &mut self.endgame)
                    {
                        if defer_deallocation {
                            defer_deallocation = false;
                            torrent_state.deallocate_piece(index, self.conn_id);
                        }
                        let mut subpieces =
                            torrent_state.allocate_piece(new_index, self.conn_id, file_store);
                        self.append_and_fill(&mut subpieces);
                    }
                }
                if defer_deallocation {
                    torrent_state.deallocate_piece(index, self.conn_id);
                }
                self.fill_request_queue();
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                log::trace!(
                    "[Peer: {}] Received cancel request, index: {index}, begin: {begin}, length: {length}",
                    self.peer_id
                );
                // if we are talking to a fast_ext peer we need to respond with something here,
                // either reject or a piece
                if self.fast_ext {
                    let subpiece = Subpiece {
                        index,
                        offset: begin,
                        size: length,
                        timed_out: false,
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
                        self.reject_request(subpiece.index, subpiece.offset, subpiece.size, false);
                    }
                }
            }
            PeerMessage::Piece { index, begin, data } => {
                if !self.is_valid_piece(index, begin, data.len(), torrent_state.num_pieces()) {
                    self.pending_disconnect = Some(DisconnectReason::ProtocolError(
                        "Invalid piece message received",
                    ));
                    return;
                }
                // TODO: disconnect on recv piece never requested if fast_ext is enabled
                log::trace!(
                    "[Peer: {}] Recived a piece index: {index}, begin: {begin}, length: {}",
                    self.peer_id,
                    data.len(),
                );
                self.update_stats(index, begin, data.len() as u32);

                if let Some(readable_piece_view) = torrent_state.pieces[index as usize]
                    .take_if(|piece| {
                        piece.on_subpiece(index, begin, &data[..]);
                        piece.is_complete()
                    })
                    .map(|completed_piece| completed_piece.into_readable())
                {
                    if torrent_state.piece_selector.has_completed(index as usize)
                        // We assume the hash will match, if not we will just request it again
                        || torrent_state.piece_selector.is_hashing(index as usize)
                    {
                        // This might happen in end game mode when multiple peers race to complete the
                        // piece. Haven't implemented it yet though
                        log::debug!("Piece {index} already completed or pending hashing, skipping");
                        return;
                    }
                    log::debug!("Piece {index} download completed, sending to hash thread");
                    torrent_state.piece_selector.mark_hashing(index as usize);
                    let complete_tx = torrent_state.completed_piece_tx.clone();
                    let conn_id = self.conn_id;
                    scope.spawn(move |_| {
                        let hash = &torrent_info.pieces[readable_piece_view.index as usize];
                        let hash_check_result = readable_piece_view.sync_and_check_hash(hash);
                        complete_tx
                            .send(CompletedPiece {
                                index: index as usize,
                                conn_id,
                                hash_matched: hash_check_result,
                            })
                            .unwrap();
                    });
                }
            }
        }
    }
}
