use bitvec::prelude::*;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, PartialEq, Eq)]
pub enum PeerMessage {
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
        lenght: i32,
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

#[derive(Debug, Default)]
pub struct PeerMessageDecoder {
    length: Option<i32>,
    data: Bytes,
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
            // msg type + index + begin
            const HEADER_SIZE: i32 = 9;
            anyhow::ensure!(length >= HEADER_SIZE);
            anyhow::ensure!((length - HEADER_SIZE) as usize <= data.remaining());
            Ok(PeerMessage::Piece {
                index: data.get_i32(),
                begin: data.get_i32(),
                lenght: length - HEADER_SIZE,
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
