use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

use anyhow::Context;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use slotmap::Key;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::Instant;
use tokio_uring::net::{TcpListener, TcpStream};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::peer_events::{PeerEvent, PeerEventType};
use crate::peer_message::{PeerMessage, PeerMessageDecoder};
use crate::{PeerKey, Piece, SUBPIECE_SIZE};

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

#[derive(Debug)]
pub struct InflightSubpiece {
    pub started: Instant,
    pub index: i32,
    pub begin: i32,
}

#[derive(Debug)]
pub struct PeerConnectionState {
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
    pub queued: Vec<InflightSubpiece>,
    pub slow_start: bool,
    pub moving_rtt: MovingRttAverage,
    // TODO calculate this meaningfully
    pub througput: u64,
    // If this connection is about to be disconnected
    // because of low througput. (Choke instead?)
    pub pending_disconnect: bool,

    _cancellation_token_guard: DropGuard,
}

impl PeerConnectionState {
    pub(crate) fn new(cancellation_token: CancellationToken) -> Self {
        Self {
            is_choking: true,
            is_interested: false,
            peer_choking: true,
            peer_interested: false,
            queue_capacity: 4,
            slow_start: true,
            queued: Vec::with_capacity(4),
            currently_downloading: Vec::new(),
            moving_rtt: MovingRttAverage::default(),
            througput: 0,
            pending_disconnect: false,
            _cancellation_token_guard: cancellation_token.drop_guard(),
        }
    }

    pub fn update_stats(&mut self, m_index: i32, m_begin: i32, length: u32) {
        // horribly inefficient
        let Some(pos) = self
            .queued
            .iter()
            .position(|sub| sub.index == m_index && m_begin == sub.begin) else {

            // TODO: not really an error? should be handled better
            log::error!("Received pieced i: {} begin: {} was marked as timed out", m_index, m_begin);
            return;
        };
        if self.slow_start {
            self.queue_capacity += 1;
        }
        if self.pending_disconnect {
            // Restart slow_start here? Or clear rrt?
            self.pending_disconnect = false;
        }
        self.througput += length as u64;
        let request = self.queued.swap_remove(pos);
        log::debug!("Subpiece completed: {}, {}", request.index, request.begin);
        let rtt = Instant::now() - request.started;
        self.moving_rtt.add_sample(&rtt);
    }

    pub fn on_subpiece(
        &mut self,
        m_index: i32,
        m_begin: i32,
        data: Bytes,
        outgoing_tx: &UnboundedSender<PeerMessage>,
    ) -> anyhow::Result<Option<Piece>> {
        let position = self
            .currently_downloading
            .iter()
            .position(|piece| piece.index == m_index);
        if let Some(position) = position {
            let mut piece = self.currently_downloading.swap_remove(position);
            piece.on_subpiece(m_index, m_begin, &data[..], PeerKey::null());
            if !piece.is_complete() {
                // Next subpice to download (that isn't already inflight)
                while let Some(next_subpice) = piece.next_unstarted_subpice() {
                    if self.queued.len() < self.queue_capacity {
                        piece.inflight_subpieces.set(next_subpice, true);
                        let subpiece_request = PeerMessage::Request {
                            index: piece.index,
                            begin: SUBPIECE_SIZE * next_subpice as i32,
                            length: if next_subpice as i32 == piece.last_subpiece_index() {
                                piece.last_subpiece_length
                            } else {
                                SUBPIECE_SIZE
                            },
                        };
                        self.queued.push(InflightSubpiece {
                            started: Instant::now(),
                            index: piece.index,
                            begin: SUBPIECE_SIZE * next_subpice as i32,
                        });
                        // Write a new request, would slab + writev make sense?
                        outgoing_tx.send(subpiece_request)?;
                    } else {
                        break;
                    }
                }
                // still downloading
                self.currently_downloading.push(piece);
            } else {
                log::debug!("[PeerKey] Piece completed");
                return Ok(Some(piece));
            }
        } else {
            log::error!("[PeerKey] Recieved unexpected piece message, index: {m_index}");
        }
        Ok(None)
    }
}

// Consider adding handlers for each msg as a trait which extensions can implement
async fn process_incoming(
    peer_key: PeerKey,
    msg: PeerMessage,
    // TODO: This is unnecessary and should be replaced
    // with writing directly to the socket
    peer_event_sender: &tokio::sync::mpsc::Sender<PeerEvent>,
) -> anyhow::Result<()> {
    match msg {
        PeerMessage::Choke => {
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::Choked,
                })
                .await?
        }
        PeerMessage::Unchoke => {
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::Unchoke,
                })
                .await?
        }
        PeerMessage::Interested => {
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::Intrest,
                })
                .await?
        }
        PeerMessage::NotInterested => {
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::NotInterested,
                })
                .await?
        }
        PeerMessage::Have { index } => {
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::Have { index },
                })
                .await?;
        }
        PeerMessage::Bitfield(field) => {
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::Bitfield(field),
                })
                .await?;
        }
        PeerMessage::Request {
            index,
            begin,
            length,
        } => {
            // Potentially check for invalid indexes here already
            log::info!("[PeerKey: {peer_key:?}] Peer wants piece with index: {index}, begin: {begin}, length: {length}");
            if length > SUBPIECE_SIZE {
                log::error!(
                    "[PeerKey: {peer_key:?}] Piece request is too large, ignoring. Lenght: {length}"
                );
                return Ok(());
            }
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::PieceRequest {
                        index,
                        begin,
                        length,
                    },
                })
                .await?;
        }
        PeerMessage::Cancel {
            index,
            begin,
            length,
        } => {
            log::info!(
                "[PeerKey: {peer_key:?}] Peer cancels request with index: {index}, begin: {begin}, length: {length}"
            );
            //TODO cancel if has yet to been sent I guess
            unimplemented!()
        }
        PeerMessage::Piece { index, begin, data } => {
            log::debug!(
                "[PeerKey: {peer_key:?}] Recived a piece index: {index}, begin: {begin}, length: {}",
                data.len()
            );
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::Subpiece { index, begin, data },
                })
                .await?;
        }
    }
    Ok(())
}

#[derive(Debug)]
pub struct PeerConnection {
    pub peer_id: [u8; 20],
    pub peer_addr: SocketAddr,
    state: PeerConnectionState,
    pub outgoing: UnboundedSender<PeerMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: [u8; 20],
        peer_addr: SocketAddr,
        outgoing_tx: UnboundedSender<PeerMessage>,
        cancellation_token: CancellationToken,
    ) -> PeerConnection {
        PeerConnection {
            peer_id,
            peer_addr,
            state: PeerConnectionState::new(cancellation_token),
            outgoing: outgoing_tx,
        }
    }

    #[inline(always)]
    pub fn is_alive(&self) -> bool {
        !self.outgoing.is_closed()
    }

    #[inline(always)]
    pub fn choke(&mut self) -> anyhow::Result<()> {
        self.state.is_choking = true;
        self.outgoing
            .send(PeerMessage::Choke)
            .context("Failed to queue outoing choke msg")
    }

    #[inline(always)]
    pub fn unchoke(&mut self) -> anyhow::Result<()> {
        self.state.is_choking = false;
        self.outgoing
            .send(PeerMessage::Unchoke)
            .context("Failed to queue outoing unchoke msg")
    }

    #[inline(always)]
    pub fn interested(&mut self) -> anyhow::Result<()> {
        self.state.is_interested = true;
        self.outgoing
            .send(PeerMessage::Interested)
            .context("Failed to queue outoing interested msg")
    }

    #[inline(always)]
    pub fn not_interested(&mut self) -> anyhow::Result<()> {
        self.state.is_interested = false;
        self.outgoing
            .send(PeerMessage::NotInterested)
            .context("Failed to queue outoing not interestead msg")
    }

    #[inline(always)]
    pub fn have(&self, index: i32) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Have { index })
            .context("Failed to queue outoing have msg")
    }

    pub fn request_piece(&mut self, index: i32, length: u32) -> anyhow::Result<()> {
        self.state
            .currently_downloading
            .push(Piece::new(index, length));
        self.fill_request_queue()
    }

    pub fn fill_request_queue(&mut self) -> anyhow::Result<()> {
        'outer: for piece in self.state.currently_downloading.iter_mut() {
            let mut available_pieces = piece.completed_subpieces.clone();
            available_pieces |= &piece.inflight_subpieces;
            while let Some(subindex) = available_pieces.first_zero() {
                if self.state.queued.len() < self.state.queue_capacity {
                    piece.inflight_subpieces.set(subindex, true);
                    // must update to prevent re-requesting same piece
                    available_pieces.set(subindex, true);
                    let subpiece_request = PeerMessage::Request {
                        index: piece.index,
                        begin: subindex as i32 * SUBPIECE_SIZE,
                        length: if subindex as i32 == piece.last_subpiece_index() {
                            piece.last_subpiece_length
                        } else {
                            SUBPIECE_SIZE
                        },
                    };
                    self.state.queued.push(InflightSubpiece {
                        started: Instant::now(),
                        begin: subindex as i32 * SUBPIECE_SIZE,
                        index: piece.index,
                    });
                    self.outgoing
                        .send(subpiece_request)
                        .context("Failed to queue outgoing msg")?;
                } else {
                    break 'outer;
                }
            }
        }
        Ok(())
    }

    pub fn remaining_request_queue_spots(&self) -> usize {
        if self.state.peer_choking {
            return 0;
        }
        // This will work even if we are in a slow start since
        // the window will continue to increase until a timeout is hit
        // TODO: Should we really return 0 here?
        self.state.queue_capacity - self.state.queued.len().min(self.state.queue_capacity)
    }

    pub fn piece(&self, index: i32, begin: i32, data: Vec<u8>) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Piece {
                index,
                begin,
                data: data.into(),
            })
            .context("Failed to queue outgoing msg")
    }

    // Remove these
    pub fn state(&self) -> &PeerConnectionState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut PeerConnectionState {
        &mut self.state
    }
}

async fn send_loop(
    stream: Rc<TcpStream>,
    peer_key: PeerKey,
    mut outgoing_rc: UnboundedReceiver<PeerMessage>,
    cancellation_token: CancellationToken,
) {
    let mut send_buf = BytesMut::with_capacity(1 << 15);
    // TODO: consider using drop guards here as well to avoid accidentally forgetting to
    // get cancel the other task
    loop {
        tokio::select! {
            maybe_message = outgoing_rc.recv() => {
                let Some(outgoing) = maybe_message else {
                    log::info!("[PeerKey: {peer_key:?}] Nothing more to send, shutting down connection");
                    cancellation_token.cancel();
                    break;
                };
                // TODO Reuse buf and also try to coalece messages
                // and use write vectored instead. I.e try to receive 3-5
                // and write vectored. Have a timeout so it's not stalled forever
                // and writes less if no more msgs are incoming
                outgoing.encode(&mut send_buf);
                // Write all since the buffer has only been filled with encoded data
                let (result, buf) = stream.write_all(send_buf).await;
                send_buf = buf;
                // Need to prevent resending the same message
                send_buf.clear();
                if let Err(err) = result {
                    log::error!("[PeerKey: {peer_key:?}] Sending PeerMessage failed: {err}");
                }
            },
            _ = cancellation_token.cancelled() => {
                log::info!("[PeerKey: {peer_key:?}] Cancelling tcp stream send loop");
                break;
            }
        }
    }
}

async fn recv_loop(
    stream: Rc<TcpStream>,
    peer_key: PeerKey,
    peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
    cancellation_token: CancellationToken,
) {
    // Spec says max size of request is 2^14 so double that for safety
    let mut read_buf = BytesMut::zeroed(1 << 15);
    let mut message_decoder = PeerMessageDecoder::default();
    loop {
        read_buf.resize(1 << 15, 0);
        debug_assert_eq!(read_buf.len(), 1 << 15);
        tokio::select! {
            (result,buf) = stream.read(read_buf) => {
                read_buf = buf;
                match result {
                    Ok(0) => {
                        log::info!("[PeerKey: {peer_key:?}] Nothing more to read, shutting down connection");
                        cancellation_token.cancel();
                        break;
                    }
                    Ok(bytes_read) => {
                        let remainder = read_buf.split_off(bytes_read);
                        while let Some(message) = message_decoder.decode(&mut read_buf) {
                            if let Err(err) = process_incoming(peer_key, message, &peer_event_sender).await {
                                log::error!("[PeerKey: {peer_key:?}] Error processing incoming: {err}");
                                cancellation_token.cancel();
                                return;
                            }
                        }
                        read_buf.unsplit(remainder);
                    }
                    Err(err) => {
                        log::error!("[PeerKey: {peer_key:?}] Failed to read from peer connection: {err}");
                        cancellation_token.cancel();
                        break;
                    }
                }
            },
            _ = cancellation_token.cancelled() => {
                log::info!("[PeerKey: {peer_key:?}] Cancelling tcp stream read loop");
                break;
            }
        }
    }
}

async fn handshake(
    our_peer_id: [u8; 20],
    info_hash: [u8; 20],
    stream: &TcpStream,
) -> anyhow::Result<[u8; 20]> {
    let mut buf: Vec<u8> = Vec::with_capacity(68);
    const PROTOCOL: &[u8] = b"BitTorrent protocol";
    buf.put_u8(PROTOCOL.len() as u8);
    buf.put_slice(PROTOCOL);
    buf.put_slice(&[0_u8; 8] as &[u8]);
    buf.put_slice(&info_hash as &[u8]);
    buf.put_slice(&our_peer_id as &[u8]);
    let (res, buf) = stream.write_all(buf).await;
    res?;
    let (res, buf) = stream.read(buf).await;
    let read_bytes = res?;
    anyhow::ensure!(read_bytes == 68);
    let mut buf = buf.as_slice();
    let str_len = buf.get_u8();
    anyhow::ensure!(str_len == 19);
    anyhow::ensure!(buf.chunk().get(..str_len as usize) == Some(b"BitTorrent protocol" as &[u8]));
    buf.advance(str_len as usize);
    // Skip extensions for now
    buf.advance(8);
    let peer_info_hash: [u8; 20] = buf.chunk()[..20].try_into()?;
    anyhow::ensure!(info_hash == peer_info_hash);
    buf.advance(20_usize);
    let peer_id = buf.chunk()[..20].try_into()?;
    buf.advance(20_usize);
    anyhow::ensure!(!buf.has_remaining());
    Ok(peer_id)
}

async fn setup_peer_connection(
    stream: TcpStream,
    our_peer_id: [u8; 20],
    info_hash: [u8; 20],
    peer_addr: SocketAddr,
    peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    let peer_id = match handshake(our_peer_id, info_hash, &stream).await {
        Ok(peer_id) => peer_id,
        Err(err) => {
            log::debug!("Handshake failed: {err}");
            return Err(err);
        }
    };
    let (outgoing_tx, outgoing_rc) = tokio::sync::mpsc::unbounded_channel();
    let connection = PeerConnection::new(
        peer_id,
        peer_addr,
        outgoing_tx.clone(),
        cancellation_token.clone(),
    );
    let (accept_tx, accept_rc) = tokio::sync::oneshot::channel();
    peer_event_sender
        .send(PeerEvent {
            peer_key: PeerKey::null(),
            event_type: PeerEventType::ConnectionEstablished {
                connection,
                accept: accept_tx,
            },
        })
        .await
        .context("Failed to send connection established")?;

    let Ok(peer_key) = accept_rc.await else {
        log::debug!("[Peer {peer_addr}]: Connection not accepted");
        cancellation_token.cancel();
        return Ok(());
    };

    let stream = Rc::new(stream);
    tokio_uring::spawn(send_loop(
        stream.clone(),
        peer_key,
        outgoing_rc,
        cancellation_token.clone(),
    ));
    tokio_uring::spawn(recv_loop(
        stream,
        peer_key,
        peer_event_sender.clone(),
        cancellation_token.clone(),
    ));

    Ok(())
}

// TODO: return cancellation token here
pub fn start_network_thread(
    our_peer_id: [u8; 20],
    info_hash: [u8; 20],
    bind_addr: SocketAddr,
    peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
) -> UnboundedSender<SocketAddr> {
    let (connect_tx, mut connect_rc): (UnboundedSender<SocketAddr>, UnboundedReceiver<SocketAddr>) =
        tokio::sync::mpsc::unbounded_channel();

    std::thread::spawn(move || {
        tokio_uring::start(async move {
            log::info!("Starting network thread: {bind_addr}");
            let root_token = CancellationToken::new();
            // Accept incoming task
            let peer_event_sender_clone = peer_event_sender.clone();
            let root_token_clone = root_token.clone();
            tokio_uring::spawn(async move {
                let listener = TcpListener::bind(bind_addr).unwrap();
                while let Ok((stream, peer_addr)) = listener.accept().await {
                    let peer_token = root_token_clone.child_token();
                    if let Err(err) = setup_peer_connection(
                        stream,
                        our_peer_id,
                        info_hash,
                        peer_addr,
                        peer_event_sender_clone.clone(),
                        peer_token,
                    )
                    .await
                    {
                        log::error!("[Peer: {peer_addr}] setup fail: {err}");
                    }
                }
            });

            while let Some(peer_addr) = connect_rc.recv().await {
                let child_token = root_token.child_token();
                let event_sender = peer_event_sender.clone();
                tokio_uring::spawn(async move {
                    match tokio::time::timeout(
                        Duration::from_secs(3),
                        TcpStream::connect(peer_addr),
                    )
                    .await
                    {
                        Ok(Ok(stream)) => {
                            log::debug!("TCP connect successful: {peer_addr}");
                            if let Err(err) = setup_peer_connection(
                                stream,
                                our_peer_id,
                                info_hash,
                                peer_addr,
                                event_sender,
                                child_token,
                            )
                            .await
                            {
                                log::error!("[Peer: {peer_addr}] setup fail: {err}");
                            }
                        }
                        Ok(Err(err)) => {
                            log::warn!("[Peer: {peer_addr}] Connection failed: {err}");
                        }
                        Err(_) => {
                            log::warn!("[Peer: {peer_addr}] Connection timedout");
                        }
                    };
                });
            }
        });
    });
    connect_tx
}
