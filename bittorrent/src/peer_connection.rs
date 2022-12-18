use std::cell::{Ref, RefMut};
use std::rc::Weak;
use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use bitvec::prelude::{BitBox, Msb0};
use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_uring::net::TcpStream;
use tokio_util::sync::CancellationToken;

use crate::peer_message::{PeerMessage, PeerMessageDecoder};
use crate::{Piece, TorrentState, SUBPIECE_SIZE};

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
    /// Which pieces do the peer have
    pub peer_pieces: BitBox<u8, Msb0>,
    /// Piece that is being currently downloaded
    /// from the peer. Might allow for more than 1 per peer
    /// in the future
    pub(crate) currently_downloading: Option<Piece>,
    cancellation_token: CancellationToken,
}

impl Drop for PeerConnectionState {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

impl PeerConnectionState {
    pub(crate) fn new(
        peer_pieces: BitBox<u8, Msb0>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            is_choking: true,
            is_interested: false,
            // TODO: is this correct?
            peer_choking: true,
            peer_interested: false,
            peer_pieces,
            currently_downloading: None,
            cancellation_token,
        }
    }

    // on subpiece
}

fn handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 68] {
    const PROTOCOL: &[u8] = b"BitTorrent protocol";
    let mut buffer: [u8; 68] = [0; PROTOCOL.len() + 8 + 20 + 20 + 1];
    let mut writer: &mut [u8] = &mut buffer;
    writer.put_u8(PROTOCOL.len() as u8);
    writer.put(PROTOCOL);
    writer.put(&[0_u8; 8] as &[u8]);
    writer.put(&info_hash as &[u8]);
    writer.put(&peer_id as &[u8]);
    buffer
}

// Safe since the inner Rc have yet to
// have been cloned at this point and importantly
// there are not inflight operations at this point
//
// TODO this should be safe but consider passing around an
// Arc wrapped listener instead
struct SendableStream(TcpStream);
unsafe impl Send for SendableStream {}

fn start_network_thread(
    peer_id: [u8; 20],
    sendable_stream: SendableStream,
    cancellation_token: CancellationToken,
) -> (UnboundedSender<PeerMessage>, UnboundedReceiver<PeerMessage>) {
    let (incoming_tx, incoming_rc) = tokio::sync::mpsc::unbounded_channel();
    let (outgoing_tx, mut outgoing_rc): (
        UnboundedSender<PeerMessage>,
        UnboundedReceiver<PeerMessage>,
    ) = tokio::sync::mpsc::unbounded_channel();

    std::thread::spawn(move || {
        tokio_uring::start(async move {
            let sendable_stream = sendable_stream;
            let stream = Rc::new(sendable_stream.0);
            let stream_clone = stream.clone();

            // Send loop, should be cancelled automatically in the next iteration when outgoing_rc is dropped.
            tokio_uring::spawn(async move {
                let mut send_buf = BytesMut::zeroed(1 << 15);
                while let Some(outgoing) = outgoing_rc.recv().await {
                    // TODO Reuse buf and also try to coalece messages
                    // and use write vectored instead. I.e try to receive 3-5
                    // and write vectored. Have a timeout so it's not stalled forever
                    // and writes less if no more msgs are incoming
                    outgoing.encode(&mut send_buf);
                    let (result, buf) = stream_clone.write_all(send_buf).await;
                    send_buf = buf;
                    send_buf.clear();
                    if let Err(err) = result {
                        log::error!("[Peer: {peer_id:?}] Sending PeerMessage failed: {err}");
                    }
                }
            });

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
                                log::info!("Shutting down connection");
                                break;
                            }
                            Ok(bytes_read) => {
                                let remainder = read_buf.split_off(bytes_read);
                                while let Some(message) = message_decoder.decode(&mut read_buf) {
                                    incoming_tx.send(message).unwrap();
                                }
                                /*if maybe_msg_len.is_none() && bytes_read < std::mem::size_of::<i32>() {
                                    panic!("Not enough data received");
                                }
                                let mut buf = read_buf.clone();
                                if maybe_msg_len.is_none() {
                                    let pending = PendingMsg {
                                        remaining_bytes: buf.get_i32(),
                                        partial: BytesMut::new(),
                                    };
                                    maybe_msg_len = Some(pending);
                                }
                                parse_msgs(&incoming_tx, buf, &mut maybe_msg_len);*/
                                read_buf.unsplit(remainder);
                            }
                            Err(err) => {
                                log::error!("Failed to read from peer connection: {err}");
                                break;
                            }
                        }
                    },
                    _ = cancellation_token.cancelled() => {
                        log::info!("Cancelling tcp stream read");
                        break;
                    }
                }
            }
        });
    });
    (outgoing_tx, incoming_rc)
}

#[derive(Debug, Clone)]
pub struct PeerConnection {
    pub peer_id: [u8; 20],
    state: Rc<RefCell<PeerConnectionState>>,
    outgoing: UnboundedSender<PeerMessage>,
}

impl PeerConnection {
    // unsafe
    pub(crate) async fn new(
        stream: TcpStream,
        our_id: [u8; 20],
        info_hash: [u8; 20],
        num_pieces: usize,
        torrent_state: Weak<RefCell<TorrentState>>,
    ) -> anyhow::Result<PeerConnection> {
        let handshake_msg = handshake(info_hash, our_id).to_vec();
        let (res, buf) = stream.write(handshake_msg).await;
        let _res = res?;

        // TODO Add timeout here
        let (res, buf) = stream.read(buf).await;
        let read = res?;
        let mut buf = buf.as_slice();
        log::info!("Received data: {read}");
        anyhow::ensure!(read >= 68);
        log::info!("Handshake received");
        let str_len = buf.get_u8();
        anyhow::ensure!(str_len == 19);
        anyhow::ensure!(buf.get(..str_len as usize) == Some(b"BitTorrent protocol" as &[u8]));
        buf.advance(str_len as usize);
        // Extensions!
        /*assert_eq!(
            buf.get((str_len as usize)..(str_len as usize + 8)),
            Some(&[0_u8; 8] as &[u8])
        );*/
        // Skip extensions for now
        buf.advance(8);
        anyhow::ensure!(Some(&info_hash as &[u8]) == buf.get(..20));
        buf.advance(20_usize);
        // Read their peer id
        let peer_id: [u8; 20] = buf.get(..20).unwrap().try_into().unwrap();
        buf.advance(20_usize);
        if buf.has_remaining() {
            log::warn!(
                "{} is remaining after handshake data will be lost",
                buf.remaining()
            );
        }
        let cancellation_token = CancellationToken::new();

        let (outgoing_tx, mut incoming_rc) = start_network_thread(
            peer_id,
            SendableStream(stream),
            cancellation_token.child_token(),
        );

        let peer_pieces = (0..num_pieces).map(|_| false).collect();
        let connection = PeerConnection {
            peer_id,
            state: Rc::new(RefCell::new(PeerConnectionState::new(
                peer_pieces,
                cancellation_token,
            ))),
            outgoing: outgoing_tx,
        };

        let connection_clone = connection.clone();
        // process incoming, should cancel automatically when incoming_tx is dropped
        tokio_uring::spawn(async move {
            while let Some(incoming) = incoming_rc.recv().await {
                if let Some(torrent_state) = torrent_state.upgrade() {
                    let mut torrent_state = torrent_state.borrow_mut();
                    if let Err(err) =
                        connection_clone.process_incoming(incoming, &mut torrent_state)
                    {
                        log::error!("[Peer: {peer_id:?}] Error processing incoming message: {err}");
                    }
                } else {
                    log::warn!("[Peer: {peer_id:?}] Torrent state is gone, shutting down incoming process loop");
                    break;
                }
            }
        });

        Ok(connection)
    }

    #[inline(always)]
    pub fn choke(&self) -> anyhow::Result<()> {
        self.state_mut().is_choking = true;
        self.outgoing
            .send(PeerMessage::Choke)
            .context("Failed to queue outoing choke msg")
    }

    #[inline(always)]
    pub fn unchoke(&self) -> anyhow::Result<()> {
        self.state_mut().is_choking = false;
        self.outgoing
            .send(PeerMessage::Unchoke)
            .context("Failed to queue outoing unchoke msg")
    }

    #[inline(always)]
    pub fn interested(&self) -> anyhow::Result<()> {
        self.state_mut().is_interested = true;
        self.outgoing
            .send(PeerMessage::Interested)
            .context("Failed to queue outoing interested msg")
    }

    #[inline(always)]
    pub fn not_interested(&self) -> anyhow::Result<()> {
        self.state_mut().is_interested = false;
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

    // Is this were we want to du subpice splitting?
    pub fn request_piece(&self, index: i32, length: u32) -> anyhow::Result<()> {
        let mut state = self.state_mut();
        // Don't start on a new piece before the current one is completed
        assert!(state.currently_downloading.is_none());
        // TODO This is racy
        //assert!(state.peer_pieces[index as usize]);
        let mut piece = Piece::new(index, length);
        // First subpiece that isn't already completed or inflight
        let last_subpiece_index = piece.completed_subpieces.len() - 1;
        // Should have 64 in flight subpieces at all times
        for _ in 0..piece.completed_subpieces.len().min(64) {
            if let Some(subindex) = piece.next_unstarted_subpice() {
                piece.inflight_subpieces.set(subindex, true);
                self.outgoing
                    .send(PeerMessage::Request {
                        index: piece.index,
                        begin: subindex as i32 * SUBPIECE_SIZE as i32,
                        length: if last_subpiece_index == subindex {
                            piece.last_subpiece_length
                        } else {
                            SUBPIECE_SIZE as i32
                        },
                    })
                    .context("Failed to queue outgoing msg")?;
                if subindex == last_subpiece_index {
                    break;
                }
            }
        }
        state.currently_downloading = Some(piece);
        Ok(())
    }

    fn piece(&self, index: i32, begin: i32, data: Vec<u8>) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Piece {
                index,
                begin,
                data: data.into(),
            })
            .context("Failed to queue outgoing msg")
    }

    // Consider adding handlers for each msg as a trait which extensions can implement
    pub(crate) fn process_incoming(
        &self,
        msg: PeerMessage,
        torrent_state: &mut TorrentState,
    ) -> anyhow::Result<()> {
        let mut state = self.state_mut();
        match msg {
            PeerMessage::Choke => {
                log::info!("Peer is choking us!");
                state.peer_choking = true;
                // TODO clear outgoing requests
            }
            PeerMessage::Unchoke => {
                log::info!("Peer is no longer choking us!");
                state.peer_choking = false;
                if state.is_interested {
                    // Peer have stuff we are interested in!
                    // TODO: request more stuff
                    log::warn!("TODO: We are interested and should request stuff");
                }
            }
            PeerMessage::Interested => {
                log::info!("Peer is interested in us!");
                state.peer_interested = true;
                if !state.is_choking {
                    // if we are not choking them we might need to send a
                    // unchoke to avoid some race conditions. Libtorrent
                    // uses the same type of logic
                    self.unchoke()?;
                } else if torrent_state.should_unchoke() {
                    log::debug!("Unchoking peer after intrest");
                    self.unchoke()?;
                }
            }
            PeerMessage::NotInterested => {
                log::info!("Peer is no longer interested in us!");
                state.peer_interested = false;
                state.is_choking = true;
                self.choke()?;
            }
            PeerMessage::Have { index } => {
                log::info!("Peer have piece with index: {index}");
                state.peer_pieces.set(index as usize, true);
            }
            PeerMessage::Bitfield(field) => {
                log::info!("Bifield received: {field}");
                state.peer_pieces |= field;
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                log::info!(
                    "Peer wants piece with index: {index}, begin: {begin}, length: {length}"
                );
                if length > SUBPIECE_SIZE {
                    log::error!("Piece request is too large, ignoring. Lenght: {length}");
                    return Ok(());
                }

                match torrent_state.on_piece_request(index, begin, length) {
                    Ok(piece_data) => {
                        self.piece(index, begin, piece_data)?;
                    }
                    Err(err) => log::error!("Inavlid piece request: {err}"),
                }
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                log::info!(
                    "Peer cancels request with index: {index}, begin: {begin}, length: {length}"
                );
                //TODO cancel if has yet to been sent I guess
                unimplemented!()
            }
            PeerMessage::Piece { index, begin, data } => {
                log::debug!(
                    "Recived a piece index: {index}, begin: {begin}, length: {}",
                    data.len()
                );
                let currently_downloading = state.currently_downloading.take();
                if let Some(mut piece) = currently_downloading {
                    if piece.index != index {
                        log::warn!("Stale piece received, ignoring");
                        return Ok(());
                    }
                    // Should this be called unconditionally?
                    if let Some(callback) = torrent_state.on_subpiece_callback.as_mut() {
                        callback(&data[..]);
                    }
                    piece.on_subpiece(index, begin, &data[..]);
                    if !piece.is_complete() {
                        // Next subpice to download (that isn't already inflight)
                        if let Some(next_subpice) = piece.next_unstarted_subpice() {
                            piece.inflight_subpieces.set(next_subpice, true);
                            // Write a new request, would slab + writev make sense?
                            self.outgoing.send(PeerMessage::Request {
                                index: piece.index,
                                begin: SUBPIECE_SIZE * next_subpice as i32,
                                length: if next_subpice as i32 == piece.last_subpiece_index() {
                                    piece.last_subpiece_length
                                } else {
                                    SUBPIECE_SIZE
                                },
                            })?;
                        }
                        // Still downloading the same piece
                        state.currently_downloading = Some(piece);
                    } else {
                        log::debug!("Piece completed!");
                        // Required since the state is borrowed within the on_piece function
                        drop(state);
                        torrent_state.on_piece_completed(piece.index, piece.memory);
                    }
                } else {
                    log::error!("Recieved unexpected piece message");
                }
            }
        }
        Ok(())
    }
    // remove these and don't expose it directly?
    pub fn state_mut(&self) -> RefMut<'_, PeerConnectionState> {
        self.state.borrow_mut()
    }

    pub fn state(&self) -> Ref<'_, PeerConnectionState> {
        self.state.borrow()
    }
}
