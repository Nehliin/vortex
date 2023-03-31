use bitvec::{prelude::Msb0, vec::BitVec};

use crate::{peer_connection::PeerConnection, PeerKey, Piece};

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
    Bitfield(BitVec<u8, Msb0>),
    /// Peer requests a piece
    PieceRequest { index: i32, begin: i32, length: i32 },
    /// Piece request completed
    PieceRequestSucceeded(Piece),
    /// Piece request failed
    PieceRequestFailed { index: i32 },
    // These sorts of events should probably live outside this particular enum
    // this + start + stop + total stats? probably lives better somewhere else
    ConnectionEstablished {
        connection: PeerConnection,
        accept: tokio::sync::oneshot::Sender<PeerKey>,
    },
    // TODO: Connection failed
    // TODO: peer stats
}

#[derive(Debug)]
pub struct PeerEvent {
    pub peer_key: PeerKey,
    pub event_type: PeerEventType,
}
