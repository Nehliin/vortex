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
                let bits = BitVec::<u8, Msb0>::from_vec(vec);
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

pub const HANDSHAKE_SIZE: usize = 68;

pub fn write_handshake(our_peer_id: PeerId, info_hash: [u8; 20], buffer: &mut impl BufMut) {
    const PROTOCOL: &[u8] = b"BitTorrent protocol";
    buffer.put_u8(PROTOCOL.len() as u8);
    buffer.put_slice(PROTOCOL);
    let mut extension = [0_u8; 8];
    extension[5] |= 0x10;
    extension[7] |= 0x04;
    buffer.put_slice(&extension as &[u8]);
    buffer.put_slice(&info_hash as &[u8]);
    buffer.put_slice(&our_peer_id.0 as &[u8]);
}

/// Constructs the peer ID prefix at compile time from version strings.
/// Format: `-VT{major:02}{minor:02}-` (e.g., `-VT0002-` for version 0.2.0)
const fn build_peer_id_prefix(major: &'static str, minor: &'static str) -> [u8; 8] {
    let major_bytes = major.as_bytes();
    let minor_bytes = minor.as_bytes();

    let mut prefix = [b'-', b'V', b'T', b'0', b'0', b'0', b'0', b'-'];

    // Parse major version (supports 0-99)
    match major_bytes.len() {
        1 => {
            prefix[4] = major_bytes[0];
        }
        2 => {
            prefix[3] = major_bytes[0];
            prefix[4] = major_bytes[1];
        }
        _ => panic!("Major version must be between 0-99"),
    }

    // Parse minor version (supports 0-99)
    match minor_bytes.len() {
        1 => {
            prefix[6] = minor_bytes[0];
        }
        2 => {
            prefix[5] = minor_bytes[0];
            prefix[6] = minor_bytes[1];
        }
        _ => panic!("Minor version must be between 0-99"),
    }

    prefix
}

/// Constructs the peer ID prefix
const fn build_peer_id_prefix_from_cargo() -> [u8; 8] {
    const MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
    const MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
    build_peer_id_prefix(MAJOR, MINOR)
}

/// Peer id
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(transparent)]
pub struct PeerId([u8; 20]);

impl PeerId {
    /// Generates a new random peer id following the BEP 20
    /// specification.
    pub fn generate() -> Self {
        // Based on http://www.bittorrent.org/beps/bep_0020.html
        const PREFIX: [u8; 8] = build_peer_id_prefix_from_cargo();
        let generatated = rand::random::<[u8; 12]>();
        let mut result: [u8; 20] = [0; 20];
        result[0..8].copy_from_slice(&PREFIX);
        result[8..].copy_from_slice(&generatated);
        PeerId(result)
    }
}

impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(prefix) = core::str::from_utf8(&self.0[..8]) {
            write!(f, "{prefix}")?;
            for b in &self.0[8..] {
                write!(f, "{b:#x}")?;
            }
            Ok(())
        } else {
            for b in &self.0[..] {
                write!(f, "{b:#x}")?;
            }
            Ok(())
        }
    }
}

pub struct ParsedHandshake {
    pub peer_id: PeerId,
    pub fast_ext: bool,
    pub extension_protocol: bool,
}

pub fn parse_handshake(info_hash: [u8; 20], mut buffer: &[u8]) -> io::Result<ParsedHandshake> {
    if buffer.len() < HANDSHAKE_SIZE {
        // Meh?
        return Err(ErrorKind::UnexpectedEof.into());
    }
    let str_len = buffer.get_u8() as usize;
    let expected_str = b"BitTorrent protocol";
    if str_len != expected_str.len() {
        return Err(ErrorKind::UnexpectedEof.into());
    }
    if &buffer[..expected_str.len()] != expected_str as &[u8] {
        return Err(ErrorKind::InvalidData.into());
    }
    buffer.advance(expected_str.len());
    // Extensions
    let fast_ext = buffer[7] & 0x04 != 0;
    let extension_protocol = buffer[5] & 0x10 != 0;
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
        extension_protocol,
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
    Extended { id: u8, data: Bytes },
}

impl PeerMessage {
    const CHOKE: u8 = 0;
    const UNCHOKE: u8 = 1;
    const INTERESTED: u8 = 2;
    const NOT_INTERESTED: u8 = 3;
    const HAVE: u8 = 4;
    const BITFIELD: u8 = 5;
    const REQUEST: u8 = 6;
    const PIECE: u8 = 7;
    const CANCEL: u8 = 8;
    const HAVE_ALL: u8 = 0x0E;
    const HAVE_NONE: u8 = 0x0F;
    const SUGGEST_PIECE: u8 = 0x0D;
    const REJECT_REQUEST: u8 = 0x10;
    const ALLOWED_FAST: u8 = 0x11;
    const EXTENDED: u8 = 20;

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
            PeerMessage::Piece { data, .. } => 9 + data.len(),
            PeerMessage::Extended { data, .. } => 2 + data.len(),
        };
        // Length prefix + message
        std::mem::size_of::<i32>() + message_size
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        // Message length, (encoded - size of len prefix)
        buf.put_i32((self.encoded_size() - std::mem::size_of::<i32>()) as i32);
        match self {
            PeerMessage::Choke => {
                buf.put_u8(Self::CHOKE);
            }
            PeerMessage::Unchoke => {
                buf.put_u8(Self::UNCHOKE);
            }
            PeerMessage::Interested => {
                buf.put_u8(Self::INTERESTED);
            }
            PeerMessage::NotInterested => {
                buf.put_u8(Self::NOT_INTERESTED);
            }
            PeerMessage::Have { index } => {
                buf.put_u8(Self::HAVE);
                buf.put_i32(*index);
            }
            PeerMessage::AllowedFast { index } => {
                buf.put_u8(Self::ALLOWED_FAST);
                buf.put_i32(*index);
            }
            PeerMessage::HaveAll => {
                buf.put_u8(Self::HAVE_ALL);
            }
            PeerMessage::HaveNone => {
                buf.put_u8(Self::HAVE_NONE);
            }
            PeerMessage::Bitfield(bitfield) => {
                buf.put_u8(Self::BITFIELD);
                buf.put_slice(bitfield.as_raw_slice());
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
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
                buf.put_u8(Self::REJECT_REQUEST);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_i32(*length);
            }
            PeerMessage::SuggestPiece { index } => {
                buf.put_u8(Self::SUGGEST_PIECE);
                buf.put_i32(*index);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                buf.put_u8(Self::CANCEL);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_i32(*length);
            }
            PeerMessage::Piece { index, begin, data } => {
                buf.put_u8(Self::PIECE);
                buf.put_i32(*index);
                buf.put_i32(*begin);
                buf.put_slice(data);
            }
            PeerMessage::Extended { id, data } => {
                buf.put_u8(Self::EXTENDED);
                buf.put_u8(*id);
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

    // Used in fuzzing for example
    #[allow(dead_code)]
    pub fn remaining(&self) -> usize {
        self.data.remaining()
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
            let bytes = data.to_vec();
            Ok(PeerMessage::Bitfield(BitVec::from_vec(bytes)))
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
        PeerMessage::EXTENDED => {
            if data.remaining() < 1 {
                return Err(io::ErrorKind::InvalidData.into());
            }
            Ok(PeerMessage::Extended {
                id: data.get_u8(),
                data,
            })
        }
        _ => Err(io::ErrorKind::InvalidData.into()),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_peer_id_prefix_current_version() {
        // Test with current version 0.2
        let prefix = build_peer_id_prefix("0", "2");
        assert_eq!(&prefix, b"-VT0002-");
        assert_eq!(std::str::from_utf8(&prefix).unwrap(), "-VT0002-");
    }

    #[test]
    fn test_peer_id_prefix_single_digits() {
        let prefix = build_peer_id_prefix("0", "1");
        assert_eq!(&prefix, b"-VT0001-");

        let prefix = build_peer_id_prefix("1", "5");
        assert_eq!(&prefix, b"-VT0105-");

        let prefix = build_peer_id_prefix("0", "0");
        assert_eq!(&prefix, b"-VT0000-");

        let prefix = build_peer_id_prefix("9", "9");
        assert_eq!(&prefix, b"-VT0909-");
    }

    #[test]
    fn test_peer_id_prefix_double_digits() {
        let prefix = build_peer_id_prefix("10", "20");
        assert_eq!(&prefix, b"-VT1020-");

        let prefix = build_peer_id_prefix("12", "34");
        assert_eq!(&prefix, b"-VT1234-");

        let prefix = build_peer_id_prefix("99", "99");
        assert_eq!(&prefix, b"-VT9999-");
    }

    #[test]
    fn test_peer_id_prefix_mixed_digits() {
        let prefix = build_peer_id_prefix("1", "23");
        assert_eq!(&prefix, b"-VT0123-");

        let prefix = build_peer_id_prefix("12", "3");
        assert_eq!(&prefix, b"-VT1203-");
    }

    #[test]
    fn fuzz_encoded_length_bug() {
        let messages = [
            PeerMessage::Piece {
                index: -65536,
                begin: -1375731957,
                data: b"\x01".as_slice().into(),
            },
            PeerMessage::Choke,
        ];

        let mut decoder = PeerMessageDecoder::new(1 << 12);
        let mut parsed = Vec::new();
        let mut encoded = BytesMut::new();
        for msg in messages.iter() {
            let mut msg_buf = Vec::with_capacity(msg.encoded_size());
            msg.encode(&mut msg_buf);
            encoded.extend_from_slice(&msg_buf);
        }

        decoder.append_data(&encoded);
        while let Some(Ok(decoded)) = decoder.next() {
            parsed.push(decoded);
        }

        assert_eq!(messages.as_slice(), &parsed);
    }

    #[test]
    fn bitfield_roundtrip() {
        let bitfield = [
            0b0010_0011_u8.to_be(),
            0b0111_0011_u8.to_be(),
            255,
            255,
            255,
            255,
            255,
            255,
            0b0110_1001_u8.to_be(),
        ];
        let message = PeerMessage::Bitfield(BitVec::from_slice(bitfield.as_slice()));
        let mut buf = Vec::with_capacity(message.encoded_size());
        message.encode(&mut buf);
        // length prefix + tag + bitfield bytes
        assert_eq!(buf.len(), 14);
        let mut buf: Bytes = buf.into();
        // skip length prefix
        buf.advance(std::mem::size_of::<i32>());
        let decoded = parse_message(buf).unwrap();

        let PeerMessage::Bitfield(bitfield) = decoded else {
            panic!("wrong message type")
        };

        assert!(!bitfield[0]);
        assert!(!bitfield[1]);
        assert!(bitfield[2]);
        assert!(!bitfield[3]);

        assert!(!bitfield[4]);
        assert!(!bitfield[5]);
        assert!(bitfield[6]);
        assert!(bitfield[7]);

        assert!(!bitfield[8]);
        assert!(bitfield[9]);
        assert!(bitfield[10]);
        assert!(bitfield[11]);

        assert!(!bitfield[12]);
        assert!(!bitfield[13]);
        assert!(bitfield[14]);
        assert!(bitfield[15]);

        assert!(!bitfield[64]);
        assert!(bitfield[65]);
        assert!(bitfield[66]);
        assert!(!bitfield[67]);

        assert!(bitfield[68]);
        assert!(!bitfield[69]);
        assert!(!bitfield[70]);
        assert!(bitfield[71]);
    }

    #[test]
    fn bitfield() {
        const MSG: &[u8] = &[
            PeerMessage::BITFIELD,
            0b0111_0011_u8.to_be(),
            0b0110_1001_u8.to_be(),
        ];
        let bitfield_msg = Bytes::from_static(MSG);
        let decoded = parse_message(bitfield_msg).unwrap();

        let PeerMessage::Bitfield(bitfield) = decoded else {
            panic!("wrong message type")
        };

        assert!(!bitfield[0]);
        assert!(bitfield[1]);
        assert!(bitfield[2]);
        assert!(bitfield[3]);

        assert!(!bitfield[4]);
        assert!(!bitfield[5]);
        assert!(bitfield[6]);
        assert!(bitfield[7]);

        assert!(!bitfield[8]);
        assert!(bitfield[9]);
        assert!(bitfield[10]);
        assert!(!bitfield[11]);

        assert!(bitfield[12]);
        assert!(!bitfield[13]);
        assert!(!bitfield[14]);
        assert!(bitfield[15]);
    }

    #[test]
    fn empty_bitfield() {
        let message = PeerMessage::Bitfield(BitVec::new());
        let mut buf = Vec::with_capacity(message.encoded_size());
        message.encode(&mut buf);
        let mut buf: Bytes = buf.into();
        // skip length prefix
        buf.advance(std::mem::size_of::<i32>());
        let decoded = parse_message(buf).unwrap();

        let PeerMessage::Bitfield(bitfield) = decoded else {
            panic!("wrong message type")
        };
        assert_eq!(bitfield.len(), 0);
        assert!(bitfield.is_empty());
    }
}
