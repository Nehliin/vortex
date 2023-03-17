use std::{fmt::Debug, net::SocketAddr};

use bitvec::prelude::{BitBox, Msb0};
use tokio_uring::net::TcpStream;

use crate::{PeerKey, Piece};

// Safe since the inner Rc have yet to
// have been cloned at this point and importantly
// there are not inflight operations at this point
//
// TODO safety here is a bit more unclear but should be possible to
// remove this all together when accept_incoming case is handled
pub struct SendableStream(pub TcpStream);
unsafe impl Send for SendableStream {}
// this is temporary and only necessary for error handling afaik
unsafe impl Sync for SendableStream {}

impl Debug for SendableStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SendableStream").finish()
    }
}

#[derive(Debug)]
pub enum PeerEventType {
    /// We were choked by the peer
    Choked,
    /// Unchoke
    Unchoke,
    /// The peer is interested in us
    Intrest,
    /// The peer is not interested in us
    NotInterested,
    /// Peer have downloaded a piece
    Have { index: i32 },
    /// Peer pieces
    Bitfield(BitBox<u8, Msb0>),
    /// Peer requests a piece
    PieceRequest { index: i32, begin: i32, length: i32 },
    /// Piece request completed
    PieceRequestSucceeded(Piece),
    /// Piece request failed
    PieceRequestFailed,
    /// Connection handshake completed succesfully
    HandshakeComplete {
        peer_id: [u8; 20],
        // TODO: Perhaps this can be checked before this is sent?
        info_hash: [u8; 20],
    },
    // These sorts of events should probably live outside this particular enum
    // this + start + stop + total stats? probably lives better somewhere else
    NewConnection {
        // Skip this all together?
        stream: SendableStream,
        addr: SocketAddr,
    },
    // TODO: handshake failed
    // TODO: peer stats
}

#[derive(Debug)]
pub struct PeerEvent {
    pub peer_key: PeerKey,
    pub event_type: PeerEventType,
}
