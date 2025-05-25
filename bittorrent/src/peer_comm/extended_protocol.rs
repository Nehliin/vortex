use bitvec::{boxed::BitBox, vec::BitVec};
use bt_bencode::Deserializer;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::piece_selector::SUBPIECE_SIZE;

use super::{
    peer_connection::{DisconnectReason, OutgoingMsg},
    peer_protocol::PeerMessage,
};

pub trait ExtensionProtocol {
    fn handle_message(
        &mut self,
        data: Bytes,
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
    ) -> Result<(), DisconnectReason>;
    // TODO: fn tick?
}

// enum MetaDataMessage {
//     Request { piece: i32 },
//     Data { piece: i32, total_size: i32, data },
//     Reject { piece: i32 },
// }

// impl MetaDataMessage {
const REQUEST: u8 = 0;
const DATA: u8 = 1;
const REJECT: u8 = 2;

//     pub fn encode(&self) -> Bytes {
//         bt_bencode::
//     }
// }
#[derive(Debug, Deserialize, Serialize)]
struct MetadataMessage {
    msg_type: u8,
    piece: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_size: Option<i32>,
}

// BEP 9
pub struct MetadataExtension {
    id: u8,
    // Box slice?
    metadata: Vec<u8>,
    inflight: BitBox,
    completed: BitBox,
}

impl MetadataExtension {
    pub fn new(id: u8, metadata_size: usize) -> Self {
        let num_pieces = metadata_size / SUBPIECE_SIZE as usize
            + if metadata_size % SUBPIECE_SIZE as usize != 0 {
                1
            } else {
                0
            };
        let inflight: BitBox = BitVec::repeat(false, num_pieces).into();
        let completed: BitBox = BitVec::repeat(false, num_pieces).into();
        Self {
            id,
            metadata: vec![0; metadata_size],
            inflight,
            completed,
        }
    }

    pub fn num_pieces(&self) -> usize {
        self.inflight.len()
    }

    pub fn request(&mut self, piece: i32) -> PeerMessage {
        self.inflight.set(piece as usize, true);
        let req = MetadataMessage {
            msg_type: REQUEST,
            piece,
            total_size: None,
        };
        PeerMessage::Extended {
            id: self.id,
            data: bt_bencode::to_vec(&req).unwrap().into(),
        }
    }
}

impl ExtensionProtocol for MetadataExtension {
    fn handle_message(
        &mut self,
        data: Bytes,
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
    ) -> Result<(), DisconnectReason> {
        let mut de = Deserializer::from_slice(&data[..]);
        let message: MetadataMessage = <MetadataMessage>::deserialize(&mut de).unwrap();
        match message.msg_type {
            REQUEST => {
                log::error!("Got request");
                de.end().unwrap();
            }
            DATA => {
                log::trace!("DATA: {}", message.piece);
                // TODO: check size
                let actual_data = &data[de.byte_offset()..];
                self.metadata[(SUBPIECE_SIZE * message.piece) as usize
                    ..(SUBPIECE_SIZE * message.piece + message.total_size.unwrap()) as usize]
                    .copy_from_slice(actual_data);

                self.completed.set(message.piece as usize, true);
                if let Some(index) = self.inflight.first_one() {
                    outgoing_msgs_buffer.push(OutgoingMsg {
                        message: self.request(index as i32),
                        ordered: false,
                    });
                } else if self.completed.all() {
                    let mut hasher = Sha1::new();
                    hasher.update(&self.metadata);
                    let hash = hasher.finalize();
                    log::error!("HASH: {:x?}", hash);
                }
            }
            REJECT => {
                self.inflight.set(message.piece as usize, true);
                log::error!("Got reject request");
                de.end().unwrap();
            }
            typ => {
                log::error!("Got unknown type: {typ}");
            }
        }
        Ok(())
    }
}
