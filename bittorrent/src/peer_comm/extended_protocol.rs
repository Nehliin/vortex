use std::collections::BTreeMap;

use bitvec::{boxed::BitBox, vec::BitVec};
use bt_bencode::{Deserializer, Value};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::{StateRef, event_loop::MAX_OUTSTANDING_REQUESTS, piece_selector::SUBPIECE_SIZE};

use super::{
    peer_connection::{DisconnectReason, OutgoingMsg},
    peer_protocol::PeerMessage,
};

// Supported extensions and this clients ID for them
pub const EXTENSIONS: [(&str, u8); 1] = [("ut_metadata", 1)];

/// The handshake message this peer should send to anyone supporting the
/// extension
pub fn extension_handshake_msg(state_ref: &mut StateRef) -> PeerMessage {
    let mut handshake = BTreeMap::new();
    let extensions: BTreeMap<_, _> = BTreeMap::from(EXTENSIONS);
    handshake.insert("m", bt_bencode::value::to_value(&extensions).unwrap());
    handshake.insert(
        "v",
        bt_bencode::value::to_value(&format!("Vortex {}", env!("CARGO_PKG_VERSION"))).unwrap(),
    );
    if let Some(listener_port) = state_ref.listener_port {
        handshake.insert("p", bt_bencode::value::to_value(listener_port).unwrap());
    }
    if let Some((file_and_meta, _)) = state_ref.state() {
        let metadata_size = file_and_meta.metadata.construct_info().encode().len();
        handshake.insert(
            "metadata_size",
            bt_bencode::to_value(&metadata_size).unwrap(),
        );
    }
    handshake.insert(
        "reqq",
        bt_bencode::value::to_value(&MAX_OUTSTANDING_REQUESTS).unwrap(),
    );
    PeerMessage::Extended {
        id: 0,
        data: bt_bencode::to_vec(&handshake).unwrap().into(),
    }
}

pub trait ExtensionProtocol {
    fn handle_message<'state>(
        &mut self,
        data: Bytes,
        state: &mut StateRef<'state>,
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
    ) -> Result<(), DisconnectReason>;
    // TODO: fn tick?
}

const REQUEST: u8 = 0;
const DATA: u8 = 1;
const REJECT: u8 = 2;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct MetadataMessage {
    pub msg_type: u8,
    pub piece: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_size: Option<i32>,
}

// BEP 9
pub struct MetadataExtension {
    // The peers ID for the extension
    id: u8,
    metadata: Vec<u8>,
    // These are only kept up to date
    // if we are activly downloading the metadata.
    // If the state is completed there is no guarantee
    // that these are accurate
    inflight: BitBox,
    completed: BitBox,
}

impl MetadataExtension {
    pub fn new(id: u8, metadata_size: usize) -> Self {
        let num_pieces = metadata_size.div_ceil(SUBPIECE_SIZE as usize);
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
            data: bt_bencode::to_vec(&req).expect("valid bencode").into(),
        }
    }

    pub fn data(&mut self, piece: i32, metadata_piece: &[u8]) -> PeerMessage {
        let req = MetadataMessage {
            msg_type: DATA,
            piece,
            // Note this is NOT the length of the piece
            total_size: Some(self.metadata.len() as i32),
        };
        let mut data: Vec<u8> = bt_bencode::to_vec(&req).expect("valid bencode");
        data.extend_from_slice(metadata_piece);
        PeerMessage::Extended {
            id: self.id,
            data: data.into(),
        }
    }

    pub fn reject(&mut self, piece: i32) -> PeerMessage {
        let req = MetadataMessage {
            msg_type: REJECT,
            piece,
            total_size: None,
        };
        PeerMessage::Extended {
            id: self.id,
            data: bt_bencode::to_vec(&req).expect("valid bencode").into(),
        }
    }
}

impl ExtensionProtocol for MetadataExtension {
    fn handle_message<'state>(
        &mut self,
        data: Bytes,
        state: &mut StateRef<'state>,
        outgoing_msgs_buffer: &mut Vec<OutgoingMsg>,
    ) -> Result<(), DisconnectReason> {
        // TODO: Consider reusing this
        let mut de = Deserializer::from_slice(&data[..]);
        let message: MetadataMessage = <MetadataMessage>::deserialize(&mut de)
            .map_err(|_err| DisconnectReason::ProtocolError("Invalid metadata message"))?;
        log::trace!(
            "Got metadata extension message of type: {}",
            message.msg_type
        );
        let piece_idx = usize::try_from(message.piece)
            .map_err(|_err| DisconnectReason::ProtocolError("Invalid metadata piece index"))?;
        let Some(start_offset) = piece_idx.checked_mul(SUBPIECE_SIZE as usize) else {
            return Err(DisconnectReason::ProtocolError(
                "Invalid metadata piece index",
            ));
        };
        match message.msg_type {
            REQUEST => {
                // if we do not have all metadata yet then reject the requests
                if let Some((file_and_meta, _)) = state.state() {
                    let info_bytes = file_and_meta.metadata.construct_info().encode();
                    if start_offset >= info_bytes.len() {
                        outgoing_msgs_buffer.push(OutgoingMsg {
                            message: self.reject(message.piece),
                            ordered: false,
                        });
                    } else {
                        let metadata_piece = &info_bytes[start_offset..];
                        outgoing_msgs_buffer.push(OutgoingMsg {
                            message: self.data(message.piece, metadata_piece),
                            ordered: false,
                        });
                    }
                } else {
                    outgoing_msgs_buffer.push(OutgoingMsg {
                        message: self.reject(message.piece),
                        ordered: false,
                    });
                }
                de.end().map_err(|_err| {
                    DisconnectReason::ProtocolError("Metadata request message longer than expected")
                })?;
            }
            DATA => {
                // We race compiletion of the metadata so we might
                // receive DATA messages late, in that case we ignore them
                if state.is_initialzied() {
                    return Ok(());
                }
                let end = (start_offset + SUBPIECE_SIZE as usize).min(self.metadata.len());
                let actual_data = &data[de.byte_offset()..];
                self.metadata[start_offset..end].copy_from_slice(actual_data);

                self.completed.set(piece_idx, true);
                if let Some(index) = self.inflight.first_zero() {
                    outgoing_msgs_buffer.push(OutgoingMsg {
                        message: self.request(index as i32),
                        ordered: false,
                    });
                } else if self.completed.all() {
                    let mut hasher = Sha1::new();
                    hasher.update(&self.metadata);
                    let hash = hasher.finalize();

                    if hash.as_slice() != state.info_hash() {
                        log::error!("Got wrong hash for metadata");
                        return Err(DisconnectReason::ProtocolError("Metadata hash mismatch"));
                    } else if state.state().is_none() {
                        let metadata: Value =
                            bt_bencode::from_slice(&self.metadata).map_err(|_err| {
                                DisconnectReason::ProtocolError("Metadata not parsable")
                            })?;
                        let mut parsable = BTreeMap::new();
                        parsable.insert("info", metadata);
                        let torrent = lava_torrent::torrent::v1::Torrent::read_from_bytes(
                            // should never panic
                            bt_bencode::to_vec(&parsable).unwrap().as_slice(),
                        )
                        .expect("metadata to be parsable");
                        state.init(torrent).expect("State to be initialized once");
                    }
                }
            }
            REJECT => {
                self.inflight.set(piece_idx, true);
                log::warn!("Got reject request");
                de.end().map_err(|_err| {
                    DisconnectReason::ProtocolError("Metadata request message longer than expected")
                })?;
            }
            typ => {
                log::error!("Got metadata extension unknown type: {typ}");
            }
        }
        Ok(())
    }
}
