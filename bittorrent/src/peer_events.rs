use std::{fmt::Debug, net::SocketAddr};

use bitvec::prelude::{BitBox, Msb0};

use crate::{PeerKey, Piece};

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
    Have {
        index: i32,
    },
    /// Peer pieces
    Bitfield(BitBox<u8, Msb0>),
    /// Peer requests a piece
    PieceRequest {
        index: i32,
        begin: i32,
        length: i32,
    },
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
    // connect fail could be another event
    NewConnection {
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
