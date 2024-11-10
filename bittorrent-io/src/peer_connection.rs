use std::{
    io::{self, ErrorKind},
    os::fd::RawFd,
};

use bytes::{Buf, BufMut};

use crate::peer_protocol::{PeerMessage, PeerMessageDecoder};

#[derive(Debug)]
pub struct PeerConnection {
    pub fd: RawFd,
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
            is_interested: false,
            peer_choking: true,
            peer_interested: false,
            stateful_decoder: PeerMessageDecoder::new(2 << 15),
        }
    }

    pub fn handle_message(&mut self, peer_message: PeerMessage) {

    }
}

// pub peer_id: [u8; 20],
// pub peer_addr: SocketAddr,
