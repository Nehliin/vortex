use arbitrary::Arbitrary;
use bitvec::prelude::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum PeerMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        index: i32,
    },
    // TODO BITBOX
    Bitfield(BitVec<u8, Msb0>),
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
        lenght: i32,
        data: Bytes,
    },
}

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
                // msg type + index + begin
                const HEADER_SIZE: i32 = 9;
                let vec = u.arbitrary::<Vec<u8>>()?;
                Ok(PeerMessage::Piece {
                    index: u.arbitrary()?,
                    begin: u.arbitrary()?,
                    lenght: vec.len() as i32 - HEADER_SIZE,
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

    pub fn into_bytes(self) -> Bytes {
        let mut bytes = BytesMut::new();
        match self {
            PeerMessage::Choke => {
                bytes.put_i32(1);
                bytes.put_u8(Self::CHOKE);
                bytes.freeze()
            }
            PeerMessage::Unchoke => {
                bytes.put_i32(1);
                bytes.put_u8(Self::UNCHOKE);
                bytes.freeze()
            }
            PeerMessage::Interested => {
                bytes.put_i32(1);
                bytes.put_u8(Self::INTERESTED);
                bytes.freeze()
            }
            PeerMessage::NotInterested => {
                bytes.put_i32(1);
                bytes.put_u8(Self::NOT_INTERESTED);
                bytes.freeze()
            }
            PeerMessage::Have { index } => {
                bytes.put_i32(5);
                bytes.put_u8(Self::HAVE);
                bytes.put_i32(index);
                bytes.freeze()
            }
            PeerMessage::Bitfield(bitfield) => {
                bytes.put_i32(1 + bitfield.len() as i32);
                bytes.put_u8(Self::BITFIELD);
                bytes.put(bitfield.into_vec().as_slice());
                bytes.freeze()
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                bytes.put_i32(13);
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
                bytes.put_i32(13);
                bytes.put_u8(Self::CANCEL);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(length);
                bytes.freeze()
            }
            PeerMessage::Piece {
                index,
                begin,
                lenght: piece,
                data,
            } => {
                dbg!(piece);
                bytes.put_i32(13 + data.len() as i32);
                bytes.put_u8(Self::PIECE);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(piece);
                assert_eq!(piece, data.len() as i32);
                bytes.put(data);
                bytes.freeze()
            }
        }
    }
}

impl TryFrom<Bytes> for PeerMessage {
    type Error = anyhow::Error;

    fn try_from(mut bytes: Bytes) -> Result<Self, Self::Error> {
        if bytes.remaining() < 1 {
            anyhow::bail!("missing message type");
        }
        let msg_type = bytes.get_u8();
        match msg_type {
            PeerMessage::CHOKE => return Ok(PeerMessage::Choke),
            PeerMessage::UNCHOKE => return Ok(PeerMessage::Unchoke),
            PeerMessage::INTERESTED => return Ok(PeerMessage::Interested),
            PeerMessage::NOT_INTERESTED => return Ok(PeerMessage::NotInterested),
            PeerMessage::HAVE => {
                if bytes.remaining() >= 4 {
                    return Ok(PeerMessage::Have {
                        index: bytes.get_i32(),
                    });
                }
            }
            PeerMessage::BITFIELD => {
                log::debug!("Bitfield received");
                let bits = BitVec::<_, Msb0>::from_slice(&bytes);
                return Ok(PeerMessage::Bitfield(bits));
            }
            PeerMessage::REQUEST => {
                if bytes.remaining() >= 12 {
                    let index = bytes.get_i32();
                    let begin = bytes.get_i32();
                    let length = bytes.get_i32();
                    if length > (1 << 14) {
                        log::error!("Too large piece requested failing");
                        anyhow::bail!("Invalid request");
                    } else {
                        return Ok(PeerMessage::Request {
                            index,
                            begin,
                            length,
                        });
                    }
                }
            }
            PeerMessage::PIECE => {
                if bytes.remaining() >= 8 {
                    let index = bytes.get_i32();
                    let begin = bytes.get_i32();
                    let lenght = bytes.remaining() as i32;
                    let data = bytes.split_to(lenght as usize);
                    dbg!(&data);
                    dbg!(bytes);
                    // msg type + index + begin
                    //const HEADER_SIZE: i32 = 9;
                    return Ok(PeerMessage::Piece {
                        index,
                        begin,
                        lenght,
                        data,
                    });
                }
            }
            PeerMessage::CANCEL => {
                if bytes.remaining() >= 12 {
                    return Ok(PeerMessage::Cancel {
                        index: bytes.get_i32(),
                        begin: bytes.get_i32(),
                        length: bytes.get_i32(),
                    });
                }
            }
            _ => anyhow::bail!("Invalid message type: {msg_type}"),
        }
        anyhow::bail!("invalid msg")
    }
}
