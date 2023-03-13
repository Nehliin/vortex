use crate::{Piece, PeerKey};

#[derive(Debug)]
pub enum PeerEventType {
    /// We were choked by the peer
    Choked,
    /// Unchoke from a peer we are interested in
    InterestingUnchoke,
    /// The peer is interested in us
    Intrest,
    /// The peer is not interested in us
    NotInterested, 
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
    // TODO: Handshake complete/failed?
    // TODO: peer stats
}



#[derive(Debug)]
pub struct PeerEvent {
    pub peer_key: PeerKey,
    pub event_type: PeerEventType,
}
