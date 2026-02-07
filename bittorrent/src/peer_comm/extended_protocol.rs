use std::{collections::BTreeMap, default};

use bitvec::{boxed::BitBox, vec::BitVec};
use bt_bencode::{ByteString, Deserializer, Value};
use bytes::{Buf, Bytes};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use crate::{
    peer_comm::peer_connection::PeerConnection,
    piece_selector::SUBPIECE_SIZE,
    torrent::{Config, StateRef},
};

use super::{peer_connection::DisconnectReason, peer_protocol::PeerMessage};

pub const UT_METADATA: &str = "ut_metadata";
pub const UPLOAD_ONLY: &str = "upload_only";

pub fn init_extension<'state>(
    id: u8,
    name: &str,
    handshake_dict: &BTreeMap<ByteString, Value>,
    state_ref: &mut StateRef<'state>,
    outgoing_msgs_buffer: &mut Vec<PeerMessage>,
) -> Result<Option<Box<dyn ExtensionProtocol>>, DisconnectReason> {
    match name {
        UT_METADATA => {
            let Some(metadata_size) = handshake_dict
                .get("metadata_size".as_bytes())
                .and_then(|val| val.as_u64())
            else {
                return Err(DisconnectReason::ProtocolError("metadata size not valid"));
            };
            if let Some(metadata) = state_ref.metadata() {
                let expected_size = metadata.construct_info().encode().len();
                if metadata_size != expected_size as u64 {
                    return Err(DisconnectReason::ProtocolError("metadata size not valid"));
                }
            }

            let mut metadata = MetadataExtension::new(id, metadata_size as usize);
            if !state_ref.is_initialzied() {
                for i in 0..8.min(metadata.num_pieces()) {
                    outgoing_msgs_buffer.push(metadata.request(i as i32));
                }
            }
            Ok(Some(Box::new(metadata)))
        }
        UPLOAD_ONLY => Ok(Some(Box::new(UploadOnlyExtension::new(id)))),
        _ => Ok(None),
    }
}

// Supported extensions and this clients ID for them
pub const EXTENSIONS: [(&str, u8); 2] = [(UT_METADATA, 1), (UPLOAD_ONLY, 2)];

/// The handshake message this peer should send to anyone supporting the
/// extension
pub fn extension_handshake_msg(state_ref: &mut StateRef, config: &Config) -> PeerMessage {
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
    let is_complete = state_ref.state().is_some_and(|state| state.is_complete);
    if let Some(metadata) = state_ref.metadata() {
        let metadata_size = metadata.construct_info().encode().len();
        handshake.insert(
            "metadata_size",
            bt_bencode::to_value(&metadata_size).unwrap(),
        );
        let upload_only = if is_complete { 1 } else { 0 };
        handshake.insert(
            UPLOAD_ONLY,
            bt_bencode::value::to_value(&upload_only).unwrap(),
        );
    }
    handshake.insert(
        "reqq",
        bt_bencode::value::to_value(&config.max_reported_outstanding_requests).unwrap(),
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
        // Connection that received the message.
        // Note you can't modify the extensions map
        // from inside an extension handler
        connection: &mut PeerConnection,
    ) -> Result<(), DisconnectReason>;

    fn on_torrent_complete(&mut self, _outgoing_msgs_buffer: &mut Vec<PeerMessage>) {}
}

/// An extension protocol supported by libtorrent
/// to indicate that the peer is a pure seeder
#[derive(Debug, Deserialize, Serialize)]
pub struct UploadOnlyExtension {
    // The peers ID for the extension
    pub id: u8,
    pub enabled: bool,
}

impl UploadOnlyExtension {
    pub fn new(id: u8) -> Self {
        Self { id, enabled: false }
    }

    pub fn upload_only(&mut self, upload_only: bool) -> PeerMessage {
        let enabled = if upload_only { 1 } else { 0 };
        PeerMessage::Extended {
            id: self.id,
            data: vec![enabled].into(),
        }
    }
}

impl ExtensionProtocol for UploadOnlyExtension {
    fn handle_message<'state>(
        &mut self,
        mut data: Bytes,
        _state: &mut StateRef<'state>,
        connection: &mut PeerConnection,
    ) -> Result<(), DisconnectReason> {
        let enabled = data
            .try_get_u8()
            .map_err(|_err| DisconnectReason::InvalidMessage)?;
        connection.is_upload_only = enabled > 0;
        Ok(())
    }

    fn on_torrent_complete(&mut self, outgoing_msgs_buffer: &mut Vec<PeerMessage>) {
        outgoing_msgs_buffer.push(self.upload_only(true));
    }
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

/// Progress in downloading metadata per peer
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct MetadataProgress {
    /// Total number of metadata pieces to download
    pub total_piece: usize,
    /// Completed number of metadata pieces
    pub completed_pieces: usize,
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
        connection: &mut PeerConnection,
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
                if let Some(metadata) = state.metadata() {
                    let info_bytes = metadata.construct_info().encode();
                    if start_offset >= info_bytes.len() {
                        connection
                            .outgoing_msgs_buffer
                            .push(self.reject(message.piece));
                    } else {
                        let metadata_piece = &info_bytes[start_offset..];
                        connection
                            .outgoing_msgs_buffer
                            .push(self.data(message.piece, metadata_piece));
                    }
                } else {
                    connection
                        .outgoing_msgs_buffer
                        .push(self.reject(message.piece));
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
                connection.network_stats.download_throughput += actual_data.len() as u64;
                self.metadata[start_offset..end].copy_from_slice(actual_data);

                self.completed.set(piece_idx, true);
                if let Some(index) = self.inflight.first_zero() {
                    connection
                        .outgoing_msgs_buffer
                        .push(self.request(index as i32));
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
                if let Some(index) = self.inflight.first_zero() {
                    connection
                        .outgoing_msgs_buffer
                        .push(self.request(index as i32));
                }
                self.inflight.set(piece_idx, false);
                log::warn!("Got reject request");
                de.end().map_err(|_err| {
                    DisconnectReason::ProtocolError("Metadata request message longer than expected")
                })?;
            }
            typ => {
                if let Some(index) = self.inflight.first_zero() {
                    connection
                        .outgoing_msgs_buffer
                        .push(self.request(index as i32));
                }
                self.inflight.set(piece_idx, false);
                log::error!("Got metadata extension unknown type: {typ}");
            }
        }
        connection.metadata_progress = Some(MetadataProgress {
            total_piece: self.num_pieces(),
            completed_pieces: self.completed.count_ones(),
        });
        Ok(())
    }
}
