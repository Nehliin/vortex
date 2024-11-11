use std::{
    io::{self, ErrorKind},
    os::fd::RawFd,
};

use bytes::{Buf, BufMut};

use crate::peer_protocol::{PeerMessage, PeerMessageDecoder};

#[derive(Debug)]
pub struct PeerConnection {
    pub fd: RawFd,
    // TODO: Make this a type that impl display
    pub peer_id: [u8; 20],
    /// This side is choking the peer
    pub is_choking: bool,
    /// This side is interested what the peer has to offer
    pub is_interested: bool,
    /// The peer have informed us that it is choking us.
    pub peer_choking: bool,
    /// The peer is interested what we have to offer
    pub peer_interested: bool,
    // TODO: do we need this both with queued?
    //pub currently_downloading: Vec<Piece>,
    //pub queue_capacity: usize,
    //pub queued: Vec<InflightSubpiece>,
    //pub slow_start: bool,
    //pub moving_rtt: MovingRttAverage,
    // TODO calculate this meaningfully
    //pub througput: u64,
    // If this connection is about to be disconnected
    // because of low througput. (Choke instead?)
    //pub pending_disconnect: bool,
    pub stateful_decoder: PeerMessageDecoder,
}

impl PeerConnection {
    pub fn new(fd: RawFd, peer_id: [u8; 20]) -> Self {
        PeerConnection {
            fd,
            peer_id,
            is_choking: true,
            is_interested: true,
            peer_choking: true,
            peer_interested: false,
            stateful_decoder: PeerMessageDecoder::new(2 << 15),
        }
    }

    pub fn handle_message(&mut self, peer_message: PeerMessage) -> io::Result<()> {
        log::debug!("Received: {peer_message:?}");
        /*match peer_message {
            PeerMessage::Choke => {
                log::info!("[Peer: {:?}] Peer is choking us!", self.peer_id);
                self.peer_choking = true;
            }
            PeerMessage::Unchoke => {
                self.peer_choking = false;
                if !self.is_interested {
                    // Not interested so don't do anything
                    return Ok(());
                }
                // TODO: Get rid of this, should be allowed to continue here
                if !peer_connection.state().currently_downloading.is_empty() {
                    return Ok(());
                }
                if let Some(piece_idx) = self.piece_selector.next_piece(peer_key) {
                    if let Err(err) = peer_connection.unchoke() {
                        log::error!("[PeerKey: {peer_key:?}] Peer disconnected: {err}");
                        // TODO: cleaner fix here
                        self.peer_list.connections.remove(peer_key);
                        return Ok(());
                    } else {
                        self.num_unchoked += 1;
                    }
                    if let Err(err) = peer_connection
                        .request_piece(piece_idx, self.piece_selector.piece_len(piece_idx))
                    {
                        log::error!("[PeerKey: {peer_key:?}] Peer disconnected: {err}");
                        // TODO: cleaner fix here
                        self.peer_list.connections.remove(peer_key);
                        return Ok(());
                    }
                    self.piece_selector.mark_inflight(piece_idx as usize);
                } else {
                    log::warn!("[PeerKey: {peer_key:?}] No more pieces available");
                }
            }
            PeerMessage::Interested => todo!(),
            PeerMessage::NotInterested => todo!(),
            PeerMessage::Have { index } => todo!(),
            PeerMessage::Bitfield(_) => todo!(),
            PeerMessage::Request {
                index,
                begin,
                length,
            } => todo!(),
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => todo!(),
            PeerMessage::Piece { index, begin, data } => todo!(),
        }*/
        Ok(())
    }
}

// pub peer_id: [u8; 20],
// pub peer_addr: SocketAddr,
