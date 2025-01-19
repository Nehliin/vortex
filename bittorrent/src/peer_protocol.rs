use std::fmt::Display;
use std::io;
use std::io::ErrorKind;

use bitvec::{order::Msb0, vec::BitVec};
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

#[cfg(feature = "fuzzing")]
impl<'a> arbitrary::Arbitrary<'a> for PeerMessage {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let tag: i32 = u.int_in_range(0..=8)?;
        match tag as u8 {
            PeerMessage::CHOKE => Ok(PeerMessage::Choke),
            PeerMessage::UNCHOKE => Ok(PeerMessage::Unchoke),
            PeerMessage::INTERESTED => Ok(PeerMessage::Interested),
            PeerMessage::NOT_INTERESTED => Ok(PeerMessage::NotInterested),
            PeerMessage::HAVE_ALL => Ok(PeerMessage::HaveAll),
            PeerMessage::HAVE_NONE => Ok(PeerMessage::HaveNone),
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
            PeerMessage::REJECT_REQUEST => Ok(PeerMessage::RejectRequest {
                index: u.arbitrary()?,
                begin: u.arbitrary()?,
                length: u.arbitrary()?,
            }),
            PeerMessage::SUGGEST_PIECE => Ok(PeerMessage::SuggestPiece {
                index: u.arbitrary()?,
            }),
            PeerMessage::ALLOWED_FAST => Ok(PeerMessage::AllowedFast {
                index: u.arbitrary()?,
            }),
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

pub fn generate_peer_id() -> PeerId {
    // Based on http://www.bittorrent.org/beps/bep_0020.html
    const PREFIX: [u8; 8] = *b"-VT0010-";
    let generatated = rand::random::<[u8; 12]>();
    let mut result: [u8; 20] = [0; 20];
    result[0..8].copy_from_slice(&PREFIX);
    result[8..].copy_from_slice(&generatated);
    PeerId(result)
}

pub const HANDSHAKE_SIZE: usize = 68;

pub fn write_handshake(our_peer_id: PeerId, info_hash: [u8; 20], mut buffer: &mut [u8]) {
    const PROTOCOL: &[u8] = b"BitTorrent protocol";
    buffer.put_u8(PROTOCOL.len() as u8);
    buffer.put_slice(PROTOCOL);
    let mut extension = [0_u8; 8];
    extension[7] |= 0x04;
    buffer.put_slice(&extension as &[u8]);
    buffer.put_slice(&info_hash as &[u8]);
    buffer.put_slice(&our_peer_id.0 as &[u8]);
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

pub struct ParsedHandshake {
    pub peer_id: PeerId,
    pub fast_ext: bool,
}

pub fn parse_handshake(info_hash: [u8; 20], mut buffer: &[u8]) -> io::Result<ParsedHandshake> {
    if buffer.len() < HANDSHAKE_SIZE {
        // Meh?
        return Err(ErrorKind::UnexpectedEof.into());
    }
    let str_len = buffer.get_u8() as usize;
    if &buffer[..str_len] != b"BitTorrent protocol" as &[u8] {
        return Err(ErrorKind::InvalidData.into());
    }
    buffer.advance(str_len);
    // Extensions, only fast ext is checked for now
    let fast_ext = buffer[7] & 0x04 != 0;
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
    Ok(ParsedHandshake {
        peer_id: PeerId(peer_id),
        fast_ext,
    })
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PeerMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have { index: i32 },
    AllowedFast { index: i32 },
    Bitfield(BitVec<u8, Msb0>),
    HaveAll,
    HaveNone,
    Request { index: i32, begin: i32, length: i32 },
    RejectRequest { index: i32, begin: i32, length: i32 },
    SuggestPiece { index: i32 },
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
    pub const HAVE_ALL: u8 = 0x0E;
    pub const HAVE_NONE: u8 = 0x0F;
    pub const SUGGEST_PIECE: u8 = 0x0D;
    pub const REJECT_REQUEST: u8 = 0x10;
    pub const ALLOWED_FAST: u8 = 0x11;

    // TODO: make const and use of this more
    pub fn encoded_size(&self) -> usize {
        let message_size = match self {
            PeerMessage::Choke
            | PeerMessage::Unchoke
            | PeerMessage::HaveAll
            | PeerMessage::HaveNone
            | PeerMessage::Interested
            | PeerMessage::NotInterested => 1,
            PeerMessage::AllowedFast { index: _ }
            | PeerMessage::Have { index: _ }
            | PeerMessage::SuggestPiece { .. } => 5,
            PeerMessage::Bitfield(bitfield) => 1 + bitfield.as_raw_slice().len(),
            PeerMessage::Request { .. }
            | PeerMessage::RejectRequest { .. }
            | PeerMessage::Cancel { .. } => 13,
            PeerMessage::Piece { data, .. } => 13 + data.len(),
        };
        // Length prefix + message
        std::mem::size_of::<i32>() + message_size
    }

    pub fn encode(&self, mut buf: &mut [u8]) {
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
            PeerMessage::AllowedFast { index } => {
                buf.put_i32(5);
                buf.put_u8(Self::ALLOWED_FAST);
                buf.put_i32(*index);
            }
            PeerMessage::HaveAll => {
                buf.put_i32(1);
                buf.put_u8(Self::HAVE_ALL);
            }
            PeerMessage::HaveNone => {
                buf.put_i32(1);
                buf.put_u8(Self::HAVE_NONE);
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
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                buf.put_i32(13);
                buf.put_u8(Self::REJECT_REQUEST);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_i32(*length);
            }
            PeerMessage::SuggestPiece { index } => {
                buf.put_i32(5);
                buf.put_u8(Self::SUGGEST_PIECE);
                buf.put_i32(*index);
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
                buf.put_slice(data);
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
        PeerMessage::HAVE_ALL => Ok(PeerMessage::HaveAll),
        PeerMessage::HAVE_NONE => Ok(PeerMessage::HaveNone),
        PeerMessage::HAVE => {
            if data.remaining() < 4 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::Have {
                index: data.get_i32(),
            })
        }
        PeerMessage::ALLOWED_FAST => {
            if data.remaining() < 4 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::AllowedFast {
                index: data.get_i32(),
            })
        }
        PeerMessage::BITFIELD => {
            let bits = BitVec::<_, Msb0>::from_slice(&data[..]);
            Ok(PeerMessage::Bitfield(bits))
        }
        PeerMessage::REJECT_REQUEST => {
            if data.remaining() < 12 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            let index = data.get_i32();
            let begin = data.get_i32();
            let length = data.get_i32();
            Ok(PeerMessage::RejectRequest {
                index,
                begin,
                length,
            })
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
        PeerMessage::SUGGEST_PIECE => {
            if data.remaining() < 4 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::SuggestPiece {
                index: data.get_i32(),
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
