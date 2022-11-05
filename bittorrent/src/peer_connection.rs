use std::cell::{Ref, RefMut};
use std::io::Cursor;
use std::net::SocketAddr;
use std::{cell::RefCell, rc::Rc};

use anyhow::Context;
use bitvec::prelude::{BitBox, Msb0};
use bytes::{Buf, BufMut, BytesMut};
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;
use tokio_uring::net::TcpStream;

use crate::peer_message::PeerMessage;
use crate::{Piece, TorrentManager, SUBPIECE_SIZE};

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
}

#[derive(Debug, Clone)]
pub struct PeerConnectionHandle {
    pub peer_id: [u8; 20],
    //ip?
    pub sender: Sender<PeerOrder>,
}

struct PendingMsg {
    // Number of bytes remaining
    remaining_bytes: i32,
    // Bytes accumalated so far
    partial: BytesMut,
}

// Operations the manager can request of the peer connection
#[derive(Debug)]
pub enum PeerOrder {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    RequestPiece { index: i32, total_len: u32 },
    SendHave(i32),
}

pub(crate) struct PeerConnection {
    // TODO use utp
    stream: Rc<TcpStream>,
    state: Rc<RefCell<PeerConnectionState>>,
    torrent_manager: TorrentManager,
    // ew
    send_queue: Option<Receiver<PeerOrder>>,
}

// this isn't super nice
impl Clone for PeerConnection {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
            state: self.state.clone(),
            torrent_manager: self.torrent_manager.clone(),
            send_queue: None,
        }
    }
}

impl PeerConnection {
    pub(crate) async fn new(
        addr: SocketAddr,
        our_id: [u8; 20],
        their_id: [u8; 20],
        info_hash: [u8; 20],
        torrent_manager: TorrentManager,
        send_queue: Receiver<PeerOrder>,
    ) -> anyhow::Result<PeerConnection> {
        let stream = std::net::TcpStream::connect(addr).unwrap();
        log::info!("LOCAL_ADDR: {}", stream.local_addr().unwrap());
        /*let stream = TcpStream::connect(addr).await?;
        let fd = stream.as_raw_fd();
        let std_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
        let stream = TcpStream::from_std(std_stream);
        let stream = Rc::new(stream);*/

        let stream = Rc::new(TcpStream::from_std(stream));
        let handshake_msg = Self::handshake(info_hash, our_id).to_vec();
        let (res, buf) = stream.write(handshake_msg).await;
        let _res = res?;

        let (res, buf) = stream.read(buf).await;
        let read = res?;
        log::info!("RECEIVED: {read}");
        let mut buf = buf.as_slice();
        if read >= 68 {
            log::info!("HANDSHAKE RECV");
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
            assert_eq!(
                Some(&their_id as &[u8]),
                buf.get((str_len as usize + 28)..(str_len as usize + 48))
            );
            let peer_pieces = torrent_manager
                .torrent_info
                .pieces()
                .map(|_| false)
                .collect();
            let stream_state = PeerConnectionState::new(peer_pieces);

            let connection = PeerConnection {
                stream: stream.clone(),
                state: Rc::new(RefCell::new(stream_state)),
                torrent_manager,
                send_queue: Some(send_queue),
            };
            let connection_clone = connection.clone();
            // TODO Handle shutdowns and move out to separate function
            tokio_uring::spawn(async move {
                // Spec says max size of request is 2^14 so double that for safety
                let mut read_buf = BytesMut::zeroed(1 << 15);
                let mut maybe_msg_len: Option<PendingMsg> = None;
                loop {
                    assert_eq!(read_buf.len(), 1 << 15);
                    let (result, buf) = connection_clone.stream.read(read_buf).await;
                    read_buf = buf;
                    match result {
                        Ok(0) => {
                            log::info!("Shutting down connection");
                            break;
                        }
                        Ok(bytes_read) => {
                            let remainder = read_buf.split_off(bytes_read);
                            if maybe_msg_len.is_none() && bytes_read < std::mem::size_of::<i32>() {
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
                            connection_clone.parse_msgs(buf, &mut maybe_msg_len).await;
                            read_buf.unsplit(remainder);
                        }
                        Err(err) => {
                            log::error!("Failed to read from peer connection: {err}");
                            break;
                        }
                    }
                }
            });
            return Ok(connection);
        }
        anyhow::bail!("Didn't get enough data");
    }

    async fn parse_msgs(&self, mut incoming: BytesMut, pending_msg: &mut Option<PendingMsg>) {
        while let Some(pending) = pending_msg {
            // There exist enough data to finish the pending message
            if incoming.remaining() as i32 >= pending.remaining_bytes {
                let mut remainder = incoming.split_off(pending.remaining_bytes as usize);
                pending.partial.unsplit(incoming.split());
                log::trace!("Extending partial: {}", pending.remaining_bytes);
                let msg = PeerMessage::try_from(&pending.partial[..]).unwrap();
                self.process_incoming(msg).await.unwrap();
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
                // Return to read more data!
                return;
            }
        }
    }

    // TODO: unify all of these
    async fn choke(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Choke;
        let (result, _buf) = self.stream.write_all(msg.into_bytes()).await;
        result.context("Failed to write choke msg")
    }

    async fn unchoke(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Unchoke;
        let (result, _buf) = self.stream.write_all(msg.into_bytes()).await;
        result.context("Failed to write unchoke msg")
    }

    async fn interested(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Interested;
        let (result, _buf) = self.stream.write_all(msg.into_bytes()).await;
        result.context("Failed to write interested msg")
    }

    async fn not_interested(&self) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::NotInterested;
        let (result, _buf) = self.stream.write_all(msg.into_bytes()).await;
        result.context("Failed to write not interested msg")
    }

    async fn have(&self, index: i32) -> anyhow::Result<()> {
        // Reuse bufs?
        let msg = PeerMessage::Have { index };
        let (result, _buf) = self.stream.write_all(msg.into_bytes()).await;
        result.context("Failed to write have msg")
    }

    async fn request(&self) -> anyhow::Result<()> {
        let mut bytes = BytesMut::new();
        {
            let mut state = self.state_mut();
            let piece = state.currently_downloading.as_mut().unwrap();
            // First subpiece that isn't already completed or inflight
            let last_subpiece_index = piece.completed_subpieces.len() - 1;
            // Should have 5 in flight subpieces at all times
            for _ in 0..5 {
                if let Some(subindex) = piece.next_unstarted_subpice() {
                    piece.inflight_subpieces.set(subindex, true);
                    bytes.put(
                        PeerMessage::Request {
                            index: piece.index,
                            begin: subindex as i32 * SUBPIECE_SIZE as i32,
                            length: if last_subpiece_index == subindex {
                                piece.last_subpiece_length
                            } else {
                                SUBPIECE_SIZE as i32
                            },
                        }
                        .into_bytes(),
                    );
                    if subindex == last_subpiece_index {
                        break;
                    }
                }
            }
        }
        let (result, _buf) = self.stream.write_all(bytes.freeze()).await;
        result.context("Failed to write request(s) msg")
    }

    async fn piece(
        &self,
        index: i32,
        begin: i32,
        length: i32,
        data: Vec<u8>,
    ) -> anyhow::Result<()> {
        let msg = PeerMessage::Piece {
            index,
            begin,
            lenght: length,
            data: data.as_slice(),
        };
        let (result, _buf) = self.stream.write_all(msg.into_bytes()).await;
        result.context("Failed to write piece msg")
    }

    pub(crate) async fn connection_send_loop(&mut self) -> anyhow::Result<()> {
        let mut send_queue = self.send_queue.take().unwrap();
        while let Some(order) = send_queue.recv().await {
            match order {
                PeerOrder::Choke => self.choke().await?,
                PeerOrder::Unchoke => self.unchoke().await?,
                PeerOrder::Interested => self.interested().await?,
                PeerOrder::NotInterested => self.not_interested().await?,
                PeerOrder::RequestPiece { index, total_len } => {
                    {
                        let mut state = self.state_mut();
                        // Don't start on a new piece before the current one is completed
                        assert!(state.currently_downloading.is_none());
                        // This is racy
                        //assert!(state.peer_pieces[index as usize]);
                        let piece = Piece::new(index, total_len);
                        state.currently_downloading = Some(piece);
                    }
                    self.request().await?
                }
                PeerOrder::SendHave(index) => self.have(index).await?,
            }
        }
        Ok(())
    }

    // Consider adding handlers for each msg as a trait which extensions can implement
    pub(crate) async fn process_incoming(&self, msg: PeerMessage<'_>) -> anyhow::Result<()> {
        match msg {
            PeerMessage::Choke => {
                let mut state = self.state_mut();
                log::info!("Peer is choking us!");
                state.peer_choking = true;
                // TODO clear outgoing requests
            }
            PeerMessage::Unchoke => {
                let mut state = self.state_mut();
                log::info!("Peer is no longer choking us!");
                state.peer_choking = false;
                if state.is_interested {
                    // Peer have stuff we are interested in!
                    // TODO: request more stuff
                    log::warn!("TODO: We are interested and should request stuff");
                }
            }
            PeerMessage::Interested => {
                let is_choking = {
                    let mut state = self.state_mut();
                    log::info!("Peer is interested in us!");
                    state.peer_interested = true;
                    state.is_choking
                };
                if !is_choking {
                    // if we are not choking them we might need to send a
                    // unchoke to avoid some race conditions. Libtorrent
                    // uses the same type of logic
                    self.unchoke().await.unwrap();
                } else if self.torrent_manager.should_unchoke() {
                    log::debug!("Unchoking peer after intrest");
                    self.unchoke().await.unwrap();
                }
            }
            PeerMessage::NotInterested => {
                {
                    let mut state = self.state_mut();
                    log::info!("Peer is no longer interested in us!");
                    state.peer_interested = false;
                    state.is_choking = true;
                }
                self.choke().await.unwrap();
            }
            PeerMessage::Have { index } => {
                log::info!("Peer have piece with index: {index}");
                self.state_mut().peer_pieces.set(index as usize, true);
            }
            PeerMessage::Bitfield(field) => {
                log::info!("Bifield received");
                self.state_mut().peer_pieces |= field;
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

                match self.torrent_manager.on_piece_request(index, begin, length) {
                    Ok(piece_data) => {
                        self.piece(index, begin, length, piece_data).await?;
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
                let currently_downloading = self.state_mut().currently_downloading.take();
                if let Some(mut piece) = currently_downloading {
                    piece.on_subpiece(index, begin, lenght, data);
                    if !piece.is_complete() {
                        // Next subpice to download (that isn't already inflight)
                        if let Some(next_subpice) = piece.next_unstarted_subpice() {
                            piece.inflight_subpieces.set(next_subpice, true);
                            // Write a new request, would slab + writev make sense?
                            let (res, _buf) = self
                                .stream
                                .write_all(
                                    PeerMessage::Request {
                                        index: piece.index,
                                        begin: SUBPIECE_SIZE * next_subpice as i32,
                                        length: if next_subpice as i32
                                            == piece.last_subpiece_index()
                                        {
                                            piece.last_subpiece_length
                                        } else {
                                            SUBPIECE_SIZE
                                        },
                                    }
                                    .into_bytes(),
                                )
                                .await;
                            res.unwrap();
                        }
                        // Still downloading the same piece
                        self.state_mut().currently_downloading = Some(piece);
                    } else {
                        log::info!("Piece completed!");
                        self.torrent_manager
                            .on_piece_completed(piece.index, piece.memory)
                            .await;
                    }
                } else {
                    log::error!("Recieved unexpected piece message");
                }
            }
        }
        Ok(())
    }

    pub fn handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 68] {
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

    fn state_mut(&self) -> RefMut<'_, PeerConnectionState> {
        self.state.borrow_mut()
    }

    fn state(&self) -> Ref<'_, PeerConnectionState> {
        self.state.borrow()
    }
}
