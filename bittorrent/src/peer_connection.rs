use std::net::SocketAddr;
use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use bytes::BytesMut;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_uring::net::TcpStream;
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::peer_events::{PeerEvent, PeerEventType};
use crate::peer_message::{PeerMessage, PeerMessageDecoder};
use crate::{PeerKey, Piece, SUBPIECE_SIZE};

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
    /// Is a piece currently being downloaded
    /// from the peer? Might allow for more than 1 per peer
    /// in the future
    pub(crate) is_currently_downloading: bool,
    _cancellation_token_guard: DropGuard,
}

impl PeerConnectionState {
    pub(crate) fn new(cancellation_token: CancellationToken) -> Self {
        Self {
            is_choking: true,
            is_interested: false,
            peer_choking: true,
            peer_interested: false,
            is_currently_downloading: false,
            _cancellation_token_guard: cancellation_token.drop_guard(),
        }
    }

    // on subpiece
}

// Consider adding handlers for each msg as a trait which extensions can implement
async fn process_incoming(
    peer_key: PeerKey,
    msg: PeerMessage,
    peer_event_sender: &tokio::sync::mpsc::Sender<PeerEvent>,
    // TODO: Get rid of rc refcell?
    currently_downloading: &Rc<RefCell<Option<Piece>>>,
    // TODO: This is unnecessary and should be replaced
    // with writing directly to the socket
    outgoing_tx: &UnboundedSender<PeerMessage>,
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
            log::info!("Peer wants piece with index: {index}, begin: {begin}, length: {length}");
            if length > SUBPIECE_SIZE {
                log::error!("Piece request is too large, ignoring. Lenght: {length}");
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
            let downloading = { currently_downloading.borrow_mut().take() };
            if let Some(mut piece) = downloading {
                if piece.index != index {
                    log::warn!("Stale piece received, ignoring");
                    return Ok(());
                }
                // Should this be called unconditionally?
                /*if let Some(callback) = torrent_state.on_subpiece_callback.as_mut() {
                    callback(&data[..]);
                }*/
                piece.on_subpiece(index, begin, &data[..]);
                if !piece.is_complete() {
                    // Next subpice to download (that isn't already inflight)
                    if let Some(next_subpice) = piece.next_unstarted_subpice() {
                        piece.inflight_subpieces.set(next_subpice, true);
                        // Write a new request, would slab + writev make sense?
                        outgoing_tx.send(PeerMessage::Request {
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
                    *currently_downloading.borrow_mut() = Some(piece);
                } else {
                    log::debug!("Piece completed!");
                    peer_event_sender
                        .send(PeerEvent {
                            peer_key,
                            event_type: PeerEventType::PieceRequestSucceeded(piece),
                        })
                        .await?;
                }
            } else {
                log::error!("Recieved unexpected piece message");
            }
        }
        PeerMessage::Handshake { peer_id, info_hash } => {
            // TODO: Check if handshake was pending?
            peer_event_sender
                .send(PeerEvent {
                    peer_key,
                    event_type: PeerEventType::HandshakeComplete { peer_id, info_hash },
                })
                .await?;
        }
    }
    Ok(())
}

fn start_network_thread(
    peer_key: PeerKey,
    addr: SocketAddr,
    cancellation_token: CancellationToken,
    peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
) -> UnboundedSender<PeerMessage> {
    let (outgoing_tx, mut outgoing_rc): (
        UnboundedSender<PeerMessage>,
        UnboundedReceiver<PeerMessage>,
    ) = tokio::sync::mpsc::unbounded_channel();

    let outgoing_tx_clone = outgoing_tx.clone();
    std::thread::spawn(move || {
        tokio_uring::start(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let stream = Rc::new(stream);
            let stream_clone = stream.clone();
            // TODO remove rc/refcel after using tokio_select
            let currently_downloading = Rc::new(RefCell::new(None));

            let currently_downloading_clone = currently_downloading.clone();
            // Send loop, should be cancelled automatically in the next iteration when outgoing_rc is dropped.
            tokio_uring::spawn(async move {
                let mut send_buf = BytesMut::with_capacity(1 << 15);
                while let Some(outgoing) = outgoing_rc.recv().await {
                    if let PeerMessage::Request {
                        index,
                        begin: _,
                        length,
                    } = outgoing
                    {
                        let mut currently_downloading = currently_downloading_clone.borrow_mut();
                        // Split to subpieces and populate currently_downloading
                        // Don't start on a new piece before the current one is completed
                        assert!(currently_downloading.is_none());
                        let mut piece = Piece::new(index, length as u32);
                        // First subpiece that isn't already completed or inflight
                        let last_subpiece_index = piece.completed_subpieces.len() - 1;
                        // Should have 64 in flight subpieces at all times
                        for _ in 0..piece.completed_subpieces.len().min(64) {
                            if let Some(subindex) = piece.next_unstarted_subpice() {
                                piece.inflight_subpieces.set(subindex, true);
                                let subpiece_request = PeerMessage::Request {
                                    index: piece.index,
                                    begin: subindex as i32 * SUBPIECE_SIZE,
                                    length: if last_subpiece_index == subindex {
                                        piece.last_subpiece_length
                                    } else {
                                        SUBPIECE_SIZE
                                    },
                                };
                                subpiece_request.encode(&mut send_buf);
                                // TODO: This shouldn't be needed?
                                if subindex == last_subpiece_index {
                                    break;
                                }
                            }
                        }
                        *currently_downloading = Some(piece);
                    } else {
                        // TODO Reuse buf and also try to coalece messages
                        // and use write vectored instead. I.e try to receive 3-5
                        // and write vectored. Have a timeout so it's not stalled forever
                        // and writes less if no more msgs are incoming
                        outgoing.encode(&mut send_buf);
                    }
                    // Write all since the buffer has only been filled with encoded data
                    let (result, buf) = stream_clone.write_all(send_buf).await;
                    send_buf = buf;
                    // Need to prevent resending the same message
                    send_buf.clear();
                    if let Err(err) = result {
                        log::error!("[Peer: {peer_key:?}] Sending PeerMessage failed: {err}");
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
                                    if process_incoming(peer_key, message, &peer_event_sender, &currently_downloading, &outgoing_tx_clone).await.is_err() {
                                        log::error!("No one is listening for incoming traffic, channel dropped");
                                    }
                                }
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
    outgoing_tx
}

#[derive(Debug)]
pub struct PeerConnection {
    pub peer_id: Option<[u8; 20]>,
    state: PeerConnectionState,
    outgoing: UnboundedSender<PeerMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_key: PeerKey,
        addr: SocketAddr,
        peer_event_sender: tokio::sync::mpsc::Sender<PeerEvent>,
    ) -> anyhow::Result<PeerConnection> {
        let cancellation_token = CancellationToken::new();
        let outgoing_tx = start_network_thread(
            peer_key,
            addr,
            cancellation_token.child_token(),
            peer_event_sender,
        );

        let connection = PeerConnection {
            peer_id: None,
            state: PeerConnectionState::new(cancellation_token),
            outgoing: outgoing_tx,
        };

        Ok(connection)
    }

    #[inline(always)]
    pub fn connect(&self, our_id: [u8; 20], info_hash: [u8; 20]) -> anyhow::Result<()> {
        self.outgoing
            .send(PeerMessage::Handshake {
                peer_id: our_id,
                info_hash,
            })
            .context("Failed to queue outgoing handshake")
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
        // Don't start on a new piece before the current one is completed
        assert!(!self.state.is_currently_downloading);
        self.state.is_currently_downloading = true;
        // Subpiece spliting happens on the io thread
        self.outgoing
            .send(PeerMessage::Request {
                index,
                // Begin is calculated based off index and piece length
                // in the io thread
                begin: 0,
                length: length as i32,
            })
            .context("Failed to queue outgoing msg")?;

        Ok(())
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
