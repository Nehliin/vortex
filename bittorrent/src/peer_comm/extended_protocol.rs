use std::collections::BTreeMap;

use bitvec::{boxed::BitBox, vec::BitVec};
use bt_bencode::{Deserializer, Value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::piece_selector::SUBPIECE_SIZE;

use super::{
    peer_connection::{DisconnectReason, OutgoingMsg},
    peer_protocol::PeerMessage,
};

// Supported extensions and this clients ID for them
pub const EXTENSIONS: [(&str, u8); 1] = [("ut_metadata", 1)];

/// The handshake message this peer should send to anyone supporting the
pub fn extension_handshake_msg() -> PeerMessage {
    let mut handshake = BTreeMap::new();
    let extensions: BTreeMap<_, _> = BTreeMap::from(EXTENSIONS);
    handshake.insert("m", bt_bencode::value::to_value(&extensions).unwrap());
    // TODO: t
    handshake.insert(
        "v",
        bt_bencode::value::to_value(dbg!(&format!("Vortex {}", env!("CARGO_PKG_VERSION")))).unwrap(),
    );
    PeerMessage::Extended {
        id: 0,
        data: bt_bencode::to_vec(&handshake).unwrap().into(),
    }
}

pub trait ExtensionProtocol {
    fn handle_message(
        &mut self,
        data: Bytes,
        info_hash: &[u8],
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
    ) -> Result<(), DisconnectReason>;
    // TODO: fn tick?
}

const REQUEST: u8 = 0;
const DATA: u8 = 1;
const REJECT: u8 = 2;

#[derive(Debug, Deserialize, Serialize)]
struct MetadataMessage {
    msg_type: u8,
    piece: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_size: Option<i32>,
}

// BEP 9
pub struct MetadataExtension {
    // The peers ID for the extension
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
        info_hash: &[u8],
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
    ) -> Result<(), DisconnectReason> {
        let mut de = Deserializer::from_slice(&data[..]);
        let message: MetadataMessage = <MetadataMessage>::deserialize(&mut de).unwrap();
        match message.msg_type {
            REQUEST => {
                // if we do not have all metadata yet then reject the requests
                // we also need to hash it first
                log::error!("Got request");
                de.end().unwrap();
            }
            DATA => {
                log::trace!("DATA: {}", message.piece);
                let end = ((SUBPIECE_SIZE * message.piece + SUBPIECE_SIZE) as usize)
                    .min(self.metadata.len());
                // TODO: check size
                let actual_data = &data[de.byte_offset()..];
                self.metadata[(SUBPIECE_SIZE * message.piece) as usize..end]
                    .copy_from_slice(actual_data);

                self.completed.set(message.piece as usize, true);
                if let Some(index) = self.inflight.first_zero() {
                    outgoing_msgs_buffer.push(OutgoingMsg {
                        message: self.request(index as i32),
                        ordered: false,
                    });
                } else if self.completed.all() {
                    let mut hasher = Sha1::new();
                    hasher.update(&self.metadata);
                    let hash = hasher.finalize();
                    log::error!("HASH: {:x?}, INFO: {:x?}", hash, info_hash);
                    let metadata: Value = bt_bencode::from_slice(&self.metadata).unwrap();
                    log::error!("META: {:?}", metadata);
                    // META is ALREADY the info value so one needs to either construct
                    let mut parsable = BTreeMap::new();
                    parsable.insert("info", metadata);
                    let torrent = lava_torrent::torrent::v1::Torrent::read_from_bytes(
                        bt_bencode::to_vec(&parsable).unwrap().as_slice(),
                    )
                    .unwrap();
                    for piece in torrent.pieces {
                        log::debug!("{:x?}", piece);
                    }
                    if hash.as_slice() != info_hash {
                        panic!("WROGN");
                    }
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
