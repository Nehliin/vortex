// DHT -> Find Nodes -> Find peers
// Request metadata from peer using http://www.bittorrent.org/beps/bep_0009.html
// which requires support for http://www.bittorrent.org/beps/bep_0010.html
// which needs the foundational http://www.bittorrent.org/beps/bep_0003.html implementation

use std::io::Write;

use bytes::{BufMut, Bytes, BytesMut};

pub struct PeerConnection {
    choked: bool,
    interested: bool,
}

impl PeerConnection {
    pub fn handshake(&self, info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 67] {
        const PROTOCOL: &[u8] = b"BitTorrent protocol";
        let mut buffer: [u8; 67] = [0; PROTOCOL.len() + 8 + 20 + 20];
        let mut writer: &mut [u8] = &mut buffer;
        writer.put_u8(PROTOCOL.len() as u8);
        writer.put(PROTOCOL);
        writer.put(&[0_u8; 8] as &[u8]);
        writer.put(&info_hash as &[u8]);
        writer.put(&peer_id as &[u8]);
        buffer
    }
}

pub enum PeerMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have { index: i32 },
    Bitfield(i32),
    Request { index: i32, begin: i32, length: i32 },
    Cancel { index: i32, begin: i32, length: i32 },
    Piece { index: i32, begin: i32, piece: i32 },
}

impl PeerMessage {
    pub const CHOKE: u8 = 0;
    pub const UNCHOKE: u8 = 1;
    pub const INTERESTED: u8 = 2;
    pub const NOT_INTERESTED: u8 = 3;
    pub const HAVE: u8 = 4;
    pub const BITFIELD: u8 = 5;
    pub const REQUEST: u8 = 6;
    pub const PIECE: u8 = 7;
    pub const CANCEL: u8 = 8;

    pub fn to_bytes(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            PeerMessage::Choke => {
                bytes.put_u8(Self::CHOKE);
                bytes.freeze()
            }
            PeerMessage::Unchoke => {
                bytes.put_u8(Self::UNCHOKE);
                bytes.freeze()
            }
            PeerMessage::Interested => {
                bytes.put_u8(Self::INTERESTED);
                bytes.freeze()
            }
            PeerMessage::NotInterested => {
                bytes.put_u8(Self::NOT_INTERESTED);
                bytes.freeze()
            }
            PeerMessage::Have { index } => {
                bytes.put_u8(Self::HAVE);
                bytes.put_i32(index);
                bytes.freeze()
            }
            PeerMessage::Bitfield(bitfield) => {
                bytes.put_u8(Self::BITFIELD);
                bytes.put_i32(bitfield);
                bytes.freeze()
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                bytes.put_u8(Self::REQUEST);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(length);
                bytes.freeze()
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                bytes.put_u8(Self::CANCEL);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(length);
                bytes.freeze()
            }
            PeerMessage::Piece {
                index,
                begin,
                piece,
            } => {
                bytes.put_u8(Self::PIECE);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(piece);
                bytes.freeze()
            }
        }
    }
}

// choke unchoke (not) interestead doesn't have a payload
// bitfield is the first message

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
