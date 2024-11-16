use std::fmt::Display;
use std::io;
use std::io::ErrorKind;

use arbitrary::Arbitrary;
use bitvec::{order::Msb0, vec::BitVec};
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

impl<'a> Arbitrary<'a> for PeerMessage {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let tag: i32 = u.int_in_range(0..=8)?;
        match tag as u8 {
            PeerMessage::CHOKE => Ok(PeerMessage::Choke),
            PeerMessage::UNCHOKE => Ok(PeerMessage::Unchoke),
            PeerMessage::INTERESTED => Ok(PeerMessage::Interested),
            PeerMessage::NOT_INTERESTED => Ok(PeerMessage::NotInterested),
            PeerMessage::HAVE => Ok(PeerMessage::Have {
                index: u.arbitrary()?,
            }),
            PeerMessage::BITFIELD => {
                let vec = u.arbitrary::<Vec<u8>>()?;
                let bits = BitVec::<_, Msb0>::from_slice(&vec);
                Ok(PeerMessage::Bitfield(bits))
            }
            PeerMessage::REQUEST => {
                let index = u.arbitrary()?;
                let begin = u.arbitrary()?;
                let length = u.arbitrary()?;
                if length > (1 << 14) {
                    Err(arbitrary::Error::IncorrectFormat)
                } else {
                    Ok(PeerMessage::Request {
                        index,
                        begin,
                        length,
                    })
                }
            }
            PeerMessage::PIECE => {
                let vec = u.arbitrary::<Vec<u8>>()?;
                Ok(PeerMessage::Piece {
                    index: u.arbitrary()?,
                    begin: u.arbitrary()?,
                    data: vec.into(),
                })
            }
            PeerMessage::CANCEL => Ok(PeerMessage::Cancel {
                index: u.arbitrary()?,
                begin: u.arbitrary()?,
                length: u.arbitrary()?,
            }),
            _ => Err(arbitrary::Error::IncorrectFormat),
        }
    }
}

pub fn generate_peer_id() -> [u8; 20] {
    // Based on http://www.bittorrent.org/beps/bep_0020.html
    const PREFIX: [u8; 8] = *b"-VT0010-";
    let generatated = rand::random::<[u8; 12]>();
    let mut result: [u8; 20] = [0; 20];
    result[0..8].copy_from_slice(&PREFIX);
    result[8..].copy_from_slice(&generatated);
    result
}

pub fn write_handshake(our_peer_id: [u8; 20], info_hash: [u8; 20], mut buffer: &mut [u8]) {
    const PROTOCOL: &[u8] = b"BitTorrent protocol";
    buffer.put_u8(PROTOCOL.len() as u8);
    buffer.put_slice(PROTOCOL);
    buffer.put_slice(&[0_u8; 8] as &[u8]);
    buffer.put_slice(&info_hash as &[u8]);
    buffer.put_slice(&our_peer_id as &[u8]);
}

#[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct PeerId([u8; 20]);

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(prefix) = core::str::from_utf8(&self.0[..8]) {
            write!(f, "{prefix}")?;
            for b in &self.0[8..] {
                write!(f, "{:#x}", b)?;
            }
            Ok(())
        } else {
            for b in &self.0[..] {
                write!(f, "{:#x}", b)?;
            }
            Ok(())
        }
    }
}

pub fn parse_handshake(info_hash: [u8; 20], mut buffer: &[u8]) -> io::Result<PeerId> {
    if buffer.len() < 68 {
        // Meh?
        return Err(ErrorKind::UnexpectedEof.into());
    }
    let str_len = buffer.get_u8() as usize;
    if &buffer[..str_len] != b"BitTorrent protocol" as &[u8] {
        return Err(ErrorKind::InvalidData.into());
    }
    buffer.advance(str_len);
    // Skip extensions for now
    buffer.advance(8);
    let peer_info_hash: [u8; 20] = buffer[..20]
        .try_into()
        .map_err(|_err| ErrorKind::InvalidData)?;
    if peer_info_hash != info_hash {
        return Err(ErrorKind::InvalidData.into());
    }
    buffer.advance(20_usize);
    let peer_id = buffer[..20]
        .try_into()
        .map_err(|_err| ErrorKind::InvalidData)?;
    Ok(PeerId(peer_id))
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PeerMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have { index: i32 },
    Bitfield(BitVec<u8, Msb0>),
    Request { index: i32, begin: i32, length: i32 },
    Cancel { index: i32, begin: i32, length: i32 },
    Piece { index: i32, begin: i32, data: Bytes },
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

    // TODO: make use of me outside fuzzing
    pub fn encoded_size(&self) -> usize {
        match self {
            PeerMessage::Choke
            | PeerMessage::Unchoke
            | PeerMessage::Interested
            | PeerMessage::NotInterested => 1,
            PeerMessage::Have { index: _ } => 5,
            PeerMessage::Bitfield(bitfield) => 1 + bitfield.as_raw_slice().len(),
            PeerMessage::Request { .. } | PeerMessage::Cancel { .. } => 13,
            PeerMessage::Piece { data, .. } => 13 + data.len(),
        }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        match self {
            PeerMessage::Choke => {
                buf.put_i32(1);
                buf.put_u8(Self::CHOKE);
            }
            PeerMessage::Unchoke => {
                buf.put_i32(1);
                buf.put_u8(Self::UNCHOKE);
            }
            PeerMessage::Interested => {
                buf.put_i32(1);
                buf.put_u8(Self::INTERESTED);
            }
            PeerMessage::NotInterested => {
                buf.put_i32(1);
                buf.put_u8(Self::NOT_INTERESTED);
            }
            PeerMessage::Have { index } => {
                buf.put_i32(5);
                buf.put_u8(Self::HAVE);
                buf.put_i32(*index);
            }
            PeerMessage::Bitfield(bitfield) => {
                buf.put_i32(1 + bitfield.as_raw_slice().len() as i32);
                buf.put_u8(Self::BITFIELD);
                buf.put_slice(bitfield.as_raw_slice());
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                buf.put_i32(13);
                buf.put_u8(Self::REQUEST);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_i32(*length);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                buf.put_i32(13);
                buf.put_u8(Self::CANCEL);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_i32(*length);
            }
            PeerMessage::Piece { index, begin, data } => {
                buf.put_i32(9 + data.len() as i32);
                buf.put_u8(Self::PIECE);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_slice(&data);
            }
        }
    }
}

#[derive(Debug)]
pub struct PeerMessageDecoder {
    length: Option<usize>,
    data: BytesMut,
}

impl PeerMessageDecoder {
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            length: None,
            data: BytesMut::with_capacity(buffer_capacity),
        }
    }

    pub fn append_data(&mut self, incoming: &[u8]) {
        self.data.extend_from_slice(incoming);
    }
}

impl Iterator for PeerMessageDecoder {
    type Item = io::Result<PeerMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.length {
                Some(length) => {
                    if self.data.remaining() < length {
                        break None;
                    }
                    self.length = None;
                    let message_bytes = self.data.split_to(length);
                    break Some(parse_message(message_bytes.freeze()));
                }
                None => {
                    if self.data.remaining() >= std::mem::size_of::<i32>() {
                        let msg_length = self.data.get_i32();
                        if msg_length > 0 {
                            self.length = Some(msg_length as usize);
                        } else {
                            return Some(Err(io::ErrorKind::InvalidData.into()));
                        }
                    } else {
                        // Need more bytes
                        break None;
                    }
                }
            }
        }
    }
}

pub fn parse_message(mut data: Bytes) -> io::Result<PeerMessage> {
    let msg_type = data.get_u8();
    match msg_type {
        PeerMessage::CHOKE => Ok(PeerMessage::Choke),
        PeerMessage::UNCHOKE => Ok(PeerMessage::Unchoke),
        PeerMessage::INTERESTED => Ok(PeerMessage::Interested),
        PeerMessage::NOT_INTERESTED => Ok(PeerMessage::NotInterested),
        PeerMessage::HAVE => {
            if data.remaining() < 4 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::Have {
                index: data.get_i32(),
            })
        }
        PeerMessage::BITFIELD => {
            let bits = BitVec::<_, Msb0>::from_slice(&data[..]);
            Ok(PeerMessage::Bitfield(bits))
        }
        PeerMessage::REQUEST => {
            if data.remaining() < 12 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            let index = data.get_i32();
            let begin = data.get_i32();
            let length = data.get_i32();
            Ok(PeerMessage::Request {
                index,
                begin,
                length,
            })
        }
        PeerMessage::PIECE => {
            if data.remaining() < 8 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::Piece {
                index: data.get_i32(),
                begin: data.get_i32(),
                data,
            })
        }
        PeerMessage::CANCEL => {
            if data.remaining() < 12 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::Cancel {
                index: data.get_i32(),
                begin: data.get_i32(),
                length: data.get_i32(),
            })
        }
        _ => Err(io::ErrorKind::InvalidData.into()),
    }
}

// TODO: tests
