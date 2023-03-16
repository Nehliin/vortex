use arbitrary::Arbitrary;
use bitvec::prelude::*;
use bytes::{Buf, BufMut, Bytes};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PeerMessage {
    Handshake {
        peer_id: [u8; 20],
        info_hash: [u8; 20],
    },
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        index: i32,
    },
    Bitfield(BitBox<u8, Msb0>),
    Request {
        index: i32,
        begin: i32,
        length: i32,
    },
    Cancel {
        index: i32,
        begin: i32,
        length: i32,
    },
    Piece {
        index: i32,
        begin: i32,
        data: Bytes,
    },
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
            PeerMessage::Handshake { .. } => 68,
        }
    }

    pub fn encode(self, mut buf: impl BufMut) {
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
                buf.put_i32(index);
            }
            PeerMessage::Bitfield(bitfield) => {
                buf.put_i32(1 + bitfield.as_raw_slice().len() as i32);
                buf.put_u8(Self::BITFIELD);
                buf.put(bitfield.as_raw_slice());
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                buf.put_i32(13);
                buf.put_u8(Self::REQUEST);
                buf.put_i32(index);
                buf.put_i32(begin);
                buf.put_i32(length);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                buf.put_i32(13);
                buf.put_u8(Self::CANCEL);
                buf.put_i32(index);
                buf.put_i32(begin);
                buf.put_i32(length);
            }
            PeerMessage::Piece { index, begin, data } => {
                buf.put_i32(9 + data.len() as i32);
                buf.put_u8(Self::PIECE);
                buf.put_i32(index);
                buf.put_i32(begin);
                buf.put(data);
            }
            PeerMessage::Handshake { peer_id, info_hash } => {
                const PROTOCOL: &[u8] = b"BitTorrent protocol";
                buf.put_u8(PROTOCOL.len() as u8);
                buf.put(PROTOCOL);
                buf.put(&[0_u8; 8] as &[u8]);
                buf.put(&info_hash as &[u8]);
                buf.put(&peer_id as &[u8]);
            }
        }
    }
}

// TODO: put behind feature flag
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
                Ok(PeerMessage::Bitfield(bits.into_boxed_bitslice()))
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

#[derive(Debug)]
pub struct PeerMessageDecoder {
    length: Option<i32>,
    data: Bytes,
    pending_handshake: bool,
}

impl Default for PeerMessageDecoder {
    fn default() -> Self {
        Self {
            length: Default::default(),
            data: Default::default(),
            pending_handshake: true,
        }
    }
}

impl PeerMessageDecoder {
    pub fn decode(&mut self, incoming: &mut impl Buf) -> Option<PeerMessage> {
        let stored_data = std::mem::take(&mut self.data);
        let mut data = stored_data.chain(incoming);
        loop {
            match self.length {
                Some(length) => {
                    if data.remaining() < length as usize {
                        // Should reset internal cursor?
                        self.data = data.copy_to_bytes(data.remaining());
                        break None;
                    }
                    let remaining = self.data.remaining();
                    match parse_message(&mut data, length) {
                        Ok(msg) => {
                            self.length = None;
                            break Some(msg);
                        }
                        Err(err) => {
                            log::error!("Failed to parse message: {err}");
                            // Skip the rest of the invalid packet if possible
                            let parsed = remaining - self.data.remaining();
                            if self.data.remaining() >= (length - parsed as i32) as usize {
                                self.data.advance((length - parsed as i32) as usize);
                            } else {
                                self.data = Bytes::new();
                            }
                            self.length = None;
                            break None;
                        }
                    }
                }
                None => {
                    if self.pending_handshake && data.remaining() >= 68 {
                        // TODO Move me and don't return errors here send handshake fail event
                        let str_len = data.get_u8();
                        //anyhow::ensure!(str_len == 19);
                        assert!(str_len == 19);
                        assert!(
                            data.chunk().get(..str_len as usize) == Some(b"BitTorrent protocol" as &[u8])
                        );
                        data.advance(str_len as usize);
                        // Skip extensions for now
                        data.advance(8);
                        let info_hash = &data.chunk()[..20];
                        data.advance(20_usize);
                        let peer_id = &data.chunk()[..20];
                        data.advance(20_usize);
                        self.pending_handshake = false;
                        break Some(PeerMessage::Handshake {
                            peer_id: peer_id.try_into().unwrap(),
                            info_hash: info_hash.try_into().unwrap(),
                        });
                    }
                    if data.remaining() >= std::mem::size_of::<i32>() {
                        let msg_length = data.get_i32();
                        if msg_length > 0 {
                            self.length = Some(msg_length);
                        } else {
                            log::warn!("Received invalid peer message length: {msg_length}");
                        }
                    } else {
                        self.data = data.copy_to_bytes(data.remaining());
                        break None;
                    }
                }
            }
        }
    }
}

pub fn parse_message(data: &mut impl Buf, length: i32) -> anyhow::Result<PeerMessage> {
    debug_assert!(data.remaining() >= length as usize);
    let msg_type = data.get_u8();
    match msg_type {
        PeerMessage::CHOKE => Ok(PeerMessage::Choke),
        PeerMessage::UNCHOKE => Ok(PeerMessage::Unchoke),
        PeerMessage::INTERESTED => Ok(PeerMessage::Interested),
        PeerMessage::NOT_INTERESTED => Ok(PeerMessage::NotInterested),
        PeerMessage::HAVE => {
            anyhow::ensure!(length >= 5);
            Ok(PeerMessage::Have {
                index: data.get_i32(),
            })
        }
        PeerMessage::BITFIELD => {
            // This is fine if it's not as efficient since it's a very rare message
            // - 1 for the message type
            let mut bitfield = vec![0; length as usize - 1];
            data.copy_to_slice(&mut bitfield);
            let bits = BitVec::<_, Msb0>::from_slice(&bitfield);
            Ok(PeerMessage::Bitfield(bits.into_boxed_bitslice()))
        }
        PeerMessage::REQUEST => {
            anyhow::ensure!(length >= 13);
            let index = data.get_i32();
            let begin = data.get_i32();
            let length = data.get_i32();
            anyhow::ensure!(length <= (1 << 14));
            Ok(PeerMessage::Request {
                index,
                begin,
                length,
            })
        }
        PeerMessage::PIECE => {
            // msg type + index + begin + length
            const HEADER_SIZE: i32 = 9;
            anyhow::ensure!(length >= HEADER_SIZE);
            anyhow::ensure!((length - HEADER_SIZE) as usize <= data.remaining());
            Ok(PeerMessage::Piece {
                index: data.get_i32(),
                begin: data.get_i32(),
                data: data.copy_to_bytes((length - HEADER_SIZE) as usize),
            })
        }
        PeerMessage::CANCEL => {
            anyhow::ensure!(length >= 13);
            Ok(PeerMessage::Cancel {
                index: data.get_i32(),
                begin: data.get_i32(),
                length: data.get_i32(),
            })
        }
        _ => anyhow::bail!("Invalid message type: {msg_type}"),
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use bytes::BytesMut;

    use super::*;

    // Found via fuzzing
    #[test]
    fn piece_roundtrip_zeroed() {
        let piece_msg = PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: Bytes::new(),
        };

        let mut buffer = BytesMut::new();
        buffer.reserve(100);
        piece_msg.clone().encode(&mut buffer);

        let mut decoder = PeerMessageDecoder::default();
        let decoded = decoder.decode(&mut buffer).unwrap();
        assert_eq!(decoded, piece_msg);
    }

    #[test]
    fn piece_request_lenght_one() {
        let piece_msg = PeerMessage::Piece {
            index: 0,
            begin: 0,
            data: vec![0x83].into(),
        };

        let mut buffer = BytesMut::new();
        buffer.reserve(100);
        piece_msg.clone().encode(&mut buffer);

        let mut decoder = PeerMessageDecoder::default();
        let decoded = decoder.decode(&mut buffer).unwrap();
        assert_eq!(decoded, piece_msg);
    }
}
