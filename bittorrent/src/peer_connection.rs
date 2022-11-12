use std::cell::{Ref, RefMut};
use std::rc::Weak;
use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use bitvec::prelude::{BitBox, Msb0};
use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};
use tokio_uring::net::TcpStream;

use crate::peer_message::PeerMessage;
use crate::{Piece, TorrentState, SUBPIECE_SIZE};

#[derive(Debug)]
pub(crate) struct PeerConnectionState {
    /// This side is choking the peer
    is_choking: bool,
    /// This side is interested what the peer has to offer
    is_interested: bool,
    /// The peer have informed us that it is choking us.
    peer_choking: bool,
    /// The peer is interested what we have to offer
    peer_interested: bool,
    /// Which pieces do the peer have
    peer_pieces: BitBox<u8, Msb0>,
    /// Piece that is being currently downloaded
    /// from the peer. Might allow for more than 1 per peer
    /// in the future
    currently_downloading: Option<Piece>,
}

impl PeerConnectionState {
    pub(crate) fn new(peer_pieces: BitBox<u8, Msb0>) -> Self {
        Self {
            is_choking: true,
            is_interested: false,
            // TODO: is this correct?
            peer_choking: true,
            peer_interested: false,
            peer_pieces,
            currently_downloading: None,
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



async fn parse_msgs(
    incoming_tx: &Sender<PeerMessage>,
    mut incoming: BytesMut,
    pending_msg: &mut Option<PendingMsg>,
) {
    while let Some(mut pending) = pending_msg.take() {
        // There exist enough data to finish the pending message
        if incoming.remaining() as i32 >= pending.remaining_bytes {
            let mut remainder = incoming.split_off(pending.remaining_bytes as usize);
            pending.partial.unsplit(incoming.split());
            log::trace!("Extending partial: {}", pending.remaining_bytes);
            let msg = PeerMessage::try_from(pending.partial.freeze()).unwrap();
            incoming_tx.send(msg).await.unwrap();
            // Should we try to start parsing a new msg?
            if remainder.remaining() >= std::mem::size_of::<i32>() {
                let len_rem = remainder.get_i32();
                log::debug!("Starting new message with len: {len_rem}");
                // If we don't start from the 0 here the extend from slice will
                // duplicate the partial data
                let partial: BytesMut = BytesMut::new();
                *pending_msg = Some(PendingMsg {
                    remaining_bytes: len_rem,
                    partial,
                });
            } else {
                log::trace!("Buffer spent");
                // This might not be true if we are unlucky
                // and it's possible for a i32 to split between
                // to separate receive operations
                assert_eq!(remainder.remaining(), 0);
                *pending_msg = None;
            }
            incoming = remainder;
        } else {
            log::trace!("More data needed");
            pending.remaining_bytes -= incoming.len() as i32;
            pending.partial.unsplit(incoming);
            *pending_msg = Some(pending);
            // Return to read more data!
            return;
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerConnection {
    pub peer_id: [u8; 20],
    state: Rc<RefCell<PeerConnectionState>>,
    outgoing: UnboundedSender<PeerMessage>,
}

// Safe since the inner Rc have yet to
// have been cloned at this point and importantly
// there are not inflight operations at this point
//
// TODO this should be safe but consider passing around an
// Arc wrapped listener instead
struct SendableStream(TcpStream);
unsafe impl Send for SendableStream {}

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
        if read >= 68 {
            log::info!("Handshake received");
            let str_len = buf.get_u8();
            assert_eq!(str_len, 19);
            assert_eq!(
                buf.get(..str_len as usize),
                Some(b"BitTorrent protocol" as &[u8])
            );
            // Extensions!
            /*assert_eq!(
                buf.get((str_len as usize)..(str_len as usize + 8)),
                Some(&[0_u8; 8] as &[u8])
            );*/
            assert_eq!(
                Some(&info_hash as &[u8]),
                buf.get((str_len as usize + 8)..(str_len as usize + 28))
            );
            // Read their peer id
            let peer_id: [u8; 20] = buf
                .get((str_len as usize + 28)..(str_len as usize + 48))
                .unwrap()
                .try_into()
                .unwrap();

            // Should perhaps be unbounded as well
            let (incoming_tx, mut incoming_rc) = tokio::sync::mpsc::channel(512);
            let (outgoing_tx, mut outgoing_rc): (
                UnboundedSender<PeerMessage>,
                UnboundedReceiver<PeerMessage>,
            ) = tokio::sync::mpsc::unbounded_channel();

            let sendable_stream = SendableStream(stream);
            std::thread::spawn(move || {
                tokio_uring::start(async move {
                    let sendable_stream = sendable_stream;
                    let stream = Rc::new(sendable_stream.0);
                    let stream_clone = stream.clone();

                    // Send loop
                    tokio_uring::spawn(async move {
                        while let Some(outgoing) = outgoing_rc.recv().await {
                            // TODO Reuse buf and also try to coalece messages
                            // and use write vectored instead. I.e try to receive 3-5
                            // and write vectored. Have a timeout so it's not stalled forever
                            // and writes less if no more msgs are incoming
                            let (result, _buf) =
                                stream_clone.write_all(outgoing.into_bytes()).await;
                            if let Err(err) = result {
                                log::error!(
                                    "[Peer: {peer_id:?}] Sending PeerMessage failed: {err}"
                                );
                            }
                        }
                    });

                    // Spec says max size of request is 2^14 so double that for safety
                    let mut read_buf = BytesMut::zeroed(1 << 15);
                    let mut maybe_msg_len: Option<PendingMsg> = None;
                    loop {
                        assert_eq!(read_buf.len(), 1 << 15);
                        let (result, buf) = stream.read(read_buf).await;
                        read_buf = buf;
                        match result {
                            Ok(0) => {
                                log::info!("Shutting down connection");
                                break;
                            }
                            Ok(bytes_read) => {
                                let remainder = read_buf.split_off(bytes_read);
                                if maybe_msg_len.is_none()
                                    && bytes_read < std::mem::size_of::<i32>()
                                {
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
                                parse_msgs(&incoming_tx, buf, &mut maybe_msg_len).await;
                                read_buf.unsplit(remainder);
                            }
                            Err(err) => {
                                log::error!("Failed to read from peer connection: {err}");
                                break;
                            }
                        }
                    }
                });
            });

            let peer_pieces = (0..num_pieces).map(|_| false).collect();
            let connection = PeerConnection {
                peer_id,
                state: Rc::new(RefCell::new(PeerConnectionState::new(peer_pieces))),
                outgoing: outgoing_tx,
            };

            let connection_clone = connection.clone();
            // process incoming
            tokio_uring::spawn(async move {
                while let Some(incoming) = incoming_rc.recv().await {
                    if let Some(torrent_state) = torrent_state.upgrade() {
                        let mut torrent_state = torrent_state.borrow_mut();
                        if let Err(err) =
                            connection_clone.process_incoming(incoming, &mut torrent_state)
                        {
                            log::error!(
                                "[Peer: {peer_id:?}] Error processing incoming message: {err}"
                            );
                        }
                    } else {
                        log::warn!("[Peer: {peer_id:?}] Torrent state is gone, shutting down incoming process loop");
                        break;
                    }
                }
            });

            return Ok(connection);
        }
        anyhow::bail!("Didn't get enough data");
    }

    #[inline(always)]
    pub fn choke(&self) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Choke)
            .context("Failed to queue outoing msg")
    }

    #[inline(always)]
    pub fn unchoke(&self) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Unchoke)
            .context("Failed to queue outoing msg")
    }

    #[inline(always)]
    pub fn interested(&self) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Interested)
            .context("Failed to queue outoing msg")
    }

    #[inline(always)]
    pub fn not_interested(&self) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::NotInterested)
            .context("Failed to queue outoing msg")
    }

    #[inline(always)]
    pub fn have(&self, index: i32) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Have { index })
            .context("Failed to queue outoing msg")
    }

    // Is this were we want to du subpice splitting?
    pub fn request_piece(&self, index: i32, length: u32) -> anyhow::Result<()> {
        let mut state = self.state_mut();
        // Don't start on a new piece before the current one is completed
        assert!(state.currently_downloading.is_none());
        // This is racy
        //assert!(state.peer_pieces[index as usize]);
        let mut piece = Piece::new(index, length);
        // First subpiece that isn't already completed or inflight
        let last_subpiece_index = piece.completed_subpieces.len() - 1;
        // Should have 5 in flight subpieces at all times
        for _ in 0..5 {
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

    fn piece(&self, index: i32, begin: i32, length: i32, data: Vec<u8>) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Piece {
                index,
                begin,
                lenght: length,
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
                    self.unchoke().unwrap();
                } else if torrent_state.should_unchoke() {
                    log::debug!("Unchoking peer after intrest");
                    self.unchoke().unwrap();
                }
            }
            PeerMessage::NotInterested => {
                log::info!("Peer is no longer interested in us!");
                state.peer_interested = false;
                state.is_choking = true;
                self.choke().unwrap();
            }
            PeerMessage::Have { index } => {
                log::info!("Peer have piece with index: {index}");
                state.peer_pieces.set(index as usize, true);
            }
            PeerMessage::Bitfield(field) => {
                log::info!("Bifield received");
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
                        self.piece(index, begin, length, piece_data)?;
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
            PeerMessage::Piece {
                index,
                begin,
                lenght,
                data,
            } => {
                log::info!("Recived a piece index: {index}, begin: {begin}, length: {lenght}");
                log::info!("Data len: {}", data.len());
                let currently_downloading = state.currently_downloading.take();
                if let Some(mut piece) = currently_downloading {
                    piece.on_subpiece(index, begin, lenght, &data[..]);
                    if !piece.is_complete() {
                        // Next subpice to download (that isn't already inflight)
                        if let Some(next_subpice) = piece.next_unstarted_subpice() {
                            piece.inflight_subpieces.set(next_subpice, true);
                            // Write a new request, would slab + writev make sense?
                            self.outgoing
                                .send(PeerMessage::Request {
                                    index: piece.index,
                                    begin: SUBPIECE_SIZE * next_subpice as i32,
                                    length: if next_subpice as i32 == piece.last_subpiece_index() {
                                        piece.last_subpiece_length
                                    } else {
                                        SUBPIECE_SIZE
                                    },
                                })
                                .unwrap();
                        }
                        // Still downloading the same piece
                        state.currently_downloading = Some(piece);
                    } else {
                        log::info!("Piece completed!");
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

    fn state_mut(&self) -> RefMut<'_, PeerConnectionState> {
        self.state.borrow_mut()
    }

    fn state(&self) -> Ref<'_, PeerConnectionState> {
        self.state.borrow()
    }
}

struct PendingMsg {
    // Number of bytes remaining
    remaining_bytes: i32,
    // Bytes accumalated so far
    partial: BytesMut,
}
