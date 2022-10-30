use bitvec::{prelude::*, slice::BitSlice};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq)]
pub enum PeerMessage<'a> {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        index: i32,
    },
    Bitfield(&'a BitSlice<u8, Msb0>),
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
        data: &'a [u8],
    },
}

impl PeerMessage<'_> {
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
                bytes.put_i32(1);
                bytes.put_u8(Self::BITFIELD);
                unimplemented!()
                // TODO
                //bytes.put_i32(bitfield);
                //bytes.freeze()
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
                bytes.put_i32(13 + data.len() as i32);
                bytes.put_u8(Self::PIECE);
                bytes.put_i32(index);
                bytes.put_i32(begin);
                bytes.put_i32(piece);
                bytes.put(data);
                bytes.freeze()
            }
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for PeerMessage<'a> {
    type Error = anyhow::Error;

    fn try_from(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let length = bytes.len();
        let msg_type = bytes.get_u8();
        match msg_type {
            PeerMessage::CHOKE => Ok(PeerMessage::Choke),
            PeerMessage::UNCHOKE => Ok(PeerMessage::Unchoke),
            PeerMessage::INTERESTED => Ok(PeerMessage::Interested),
            PeerMessage::NOT_INTERESTED => Ok(PeerMessage::NotInterested),
            PeerMessage::HAVE => Ok(PeerMessage::Have {
                index: bytes.get_i32(),
            }),
            PeerMessage::BITFIELD => {
                log::debug!("Bitfield received");
                let bits = BitSlice::<_, Msb0>::try_from_slice(bytes).unwrap();
                Ok(PeerMessage::Bitfield(bits))
            }
            PeerMessage::REQUEST => {
                let index = bytes.get_i32();
                let begin = bytes.get_i32();
                let length = bytes.get_i32();
                if length > (1 << 14) {
                    log::error!("Too large piece requested failing");
                    anyhow::bail!("Invalid request");
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
                Ok(PeerMessage::Piece {
                    index: bytes.get_i32(),
                    begin: bytes.get_i32(),
                    lenght: length as i32 - HEADER_SIZE,
                    data: bytes,
                })
            }
            PeerMessage::CANCEL => Ok(PeerMessage::Cancel {
                index: bytes.get_i32(),
                begin: bytes.get_i32(),
                length: bytes.get_i32(),
            }),
            _ => anyhow::bail!("Invalid message type: {msg_type}"),
        }
    }
}
