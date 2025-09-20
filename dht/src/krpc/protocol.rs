use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::{BufMut, BytesMut};
use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::node::{Node, NodeId, NodeStatus};

use super::{Rpc, error::KrpcError};
// TODO try to avoid allocations for ids
#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Query {
    FindNode {
        id: ByteBuf,
        target: ByteBuf,
    },
    GetPeers {
        id: ByteBuf,
        info_hash: ByteBuf,
    },
    AnnouncePeer {
        id: ByteBuf,
        implied_port: bool,
        info_hash: ByteBuf,
        port: u16,
        token: ByteBuf,
    },
    Ping {
        id: ByteBuf,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Answer {
    GetPeers {
        id: ByteBuf,
        token: ByteBuf,
        #[serde(skip_serializing_if = "Option::is_none")]
        values: Option<Vec<ByteBuf>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        nodes: Option<ByteBuf>,
    },
    FindNode {
        id: ByteBuf,
        nodes: ByteBuf,
    },
    // For both ping and announce peer
    QueriedNodeId {
        id: ByteBuf,
    },
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct KrpcPacket {
    pub t: ByteBuf,
    pub y: char,
    // TODO Borrow instead
    #[serde(skip_serializing_if = "Option::is_none")]
    pub q: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub a: Option<Query>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r: Option<Answer>,
    #[serde(deserialize_with = "super::error::deserialize_error")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "super::error::serialize_error")]
    #[serde(default)]
    pub e: Option<KrpcError>,
}

#[derive(Debug)]
pub struct Ping {
    pub id: NodeId,
}

impl Rpc for Ping {
    type Response = Pong;

    fn into_packet(self, transaction_id: ByteBuf) -> KrpcPacket {
        KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("ping".to_string()),
            a: Some(Query::Ping {
                id: ByteBuf::from(self.id.as_bytes()),
            }),
            r: None,
            e: None,
        }
    }
}

#[derive(Debug)]
pub struct Pong {
    pub id: NodeId,
}

impl TryFrom<Answer> for Pong {
    type Error = KrpcError;

    fn try_from(answer: Answer) -> Result<Self, Self::Error> {
        match answer {
            Answer::QueriedNodeId { id } => Ok(Pong {
                id: id.as_slice().into(),
            }),
            _ => Err(KrpcError::protocol(
                "Unexpected answer, expected pong".to_owned(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct FindNodes {
    pub id: NodeId,
    pub target: NodeId,
}

impl Rpc for FindNodes {
    type Response = FindNodesResponse;

    fn into_packet(self, transaction_id: ByteBuf) -> KrpcPacket {
        KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("find_node".to_string()),
            a: Some(Query::FindNode {
                id: ByteBuf::from(self.id.as_bytes()),
                target: ByteBuf::from(self.target.as_bytes()),
            }),
            r: None,
            e: None,
        }
    }
}

#[allow(dead_code)]
pub struct FindNodesResponse {
    pub id: NodeId,
    pub nodes: Vec<Node>,
}

impl TryFrom<Answer> for FindNodesResponse {
    type Error = KrpcError;

    fn try_from(answer: Answer) -> Result<Self, Self::Error> {
        match answer {
            Answer::FindNode { id, nodes } => {
                let nodes = deserialize_compact_nodes(nodes);
                Ok(FindNodesResponse {
                    id: id.as_slice().into(),
                    nodes,
                })
            }
            _ => Err(KrpcError::protocol(
                "Unexpected answer, expected find nodes response".to_owned(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct GetPeers {
    pub id: NodeId,
    pub info_hash: [u8; 20],
}

impl Rpc for GetPeers {
    type Response = GetPeersResponse;

    fn into_packet(self, transaction_id: ByteBuf) -> KrpcPacket {
        KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("get_peers".to_string()),
            a: Some(Query::GetPeers {
                id: ByteBuf::from(self.id.as_bytes()),
                info_hash: ByteBuf::from(self.info_hash),
            }),
            r: None,
            e: None,
        }
    }
}

pub(crate) fn serialize_compact_peers(peers: &[SocketAddr]) -> Vec<ByteBuf> {
    let mut result = Vec::with_capacity(peers.len());
    for peer in peers {
        let mut compact_peer = ByteBuf::with_capacity(6);
        match peer.ip() {
            IpAddr::V4(ip) => {
                compact_peer.put(ip.octets().as_slice());
                compact_peer.put_u16(peer.port());
                result.push(compact_peer);
            }
            IpAddr::V6(_) => {
                log::warn!("Only Ipv4 addresses can be serialized as compact peers");
            }
        }
    }
    result
}

// Will skip compact peer info that isn't 6 bytes long
pub(crate) fn deserialize_compact_peers(peers: Vec<ByteBuf>) -> Vec<SocketAddr> {
    peers
        .into_iter()
        .filter_map(|bytes| {
            if bytes.len() != 6 {
                log::warn!("Compact peer info wasn't 6 bytes long");
                None
            } else {
                Some(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])),
                    u16::from_be_bytes([bytes[4], bytes[5]]),
                ))
            }
        })
        .collect()
}

#[derive(Debug)]
pub enum GetPeersResponseBody {
    Nodes(Vec<Node>),
    Peers(Vec<SocketAddr>),
}

#[allow(dead_code)]
pub struct GetPeersResponse {
    pub id: NodeId,
    pub token: ByteBuf,
    pub body: GetPeersResponseBody,
}

impl TryFrom<Answer> for GetPeersResponse {
    type Error = KrpcError;

    fn try_from(answer: Answer) -> Result<Self, Self::Error> {
        match answer {
            Answer::GetPeers {
                id,
                token,
                values,
                nodes,
            } => {
                if let Some(peers) = values {
                    return Ok(GetPeersResponse {
                        id: id.as_slice().into(),
                        token,
                        body: GetPeersResponseBody::Peers(deserialize_compact_peers(peers)),
                    });
                }

                if let Some(nodes) = nodes {
                    let nodes = deserialize_compact_nodes(nodes);

                    return Ok(GetPeersResponse {
                        id: id.as_slice().into(),
                        token,
                        body: GetPeersResponseBody::Nodes(nodes),
                    });
                }
                Err(KrpcError::protocol(
                    "Response contained neither nodes nor peers".to_string(),
                ))
            }
            Answer::FindNode { id, nodes } => {
                let nodes = deserialize_compact_nodes(nodes);

                Ok(GetPeersResponse {
                    id: id.as_slice().into(),
                    token: ByteBuf::new(),
                    body: GetPeersResponseBody::Nodes(nodes),
                })
            }
            _ => Err(KrpcError::protocol(
                "Unexpected response from get_peers".to_string(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct AnnouncePeer {
    pub id: NodeId,
    pub info_hash: [u8; 20],
    pub implied_port: bool,
    pub port: u16,
    pub token: ByteBuf,
}

impl Rpc for AnnouncePeer {
    type Response = AnnounceResponse;

    fn into_packet(self, transaction_id: ByteBuf) -> KrpcPacket {
        KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("announce_peer".to_string()),
            a: Some(Query::AnnouncePeer {
                id: ByteBuf::from(self.id.as_bytes()),
                implied_port: self.implied_port,
                info_hash: ByteBuf::from(self.info_hash),
                port: self.port,
                token: self.token,
            }),
            r: None,
            e: None,
        }
    }
}

#[allow(dead_code)]
pub struct AnnounceResponse {
    pub id: NodeId,
}

impl TryFrom<Answer> for AnnounceResponse {
    type Error = KrpcError;

    fn try_from(answer: Answer) -> Result<Self, Self::Error> {
        match answer {
            Answer::QueriedNodeId { id } => Ok(AnnounceResponse {
                id: id.as_slice().into(),
            }),
            _ => Err(KrpcError::protocol(
                "Unexpected answer, expected announce peer response".to_owned(),
            )),
        }
    }
}

fn deserialize_compact_nodes(bytes: ByteBuf) -> Vec<Node> {
    if !bytes.len().is_multiple_of(26) {
        log::warn!("Invalid compact node buffer, not divisible by 26");
    }
    bytes
        .chunks_exact(26)
        .map(|chunk| {
            // Seems to be working? Should i reverse this?
            let id = chunk[..20].into();
            let ip: IpAddr = [chunk[20], chunk[21], chunk[22], chunk[23]].into();
            let port = u16::from_be_bytes([chunk[24], chunk[25]]);
            Node {
                id,
                addr: (ip, port).into(),
                last_seen: OffsetDateTime::now_utc(),
                last_status: NodeStatus::Unknown,
            }
        })
        .collect()
}

pub(crate) fn serialize_compact_nodes(nodes: &[Node]) -> ByteBuf {
    let mut result = BytesMut::with_capacity(nodes.len() * (20 + 4 + 2));

    for node in nodes {
        result.put(node.id.as_bytes().as_slice());
        let IpAddr::V4(ip_v4) = node.addr.ip() else {
            log::error!("Only IPv4 addresses are supported for nodes");
            continue;
        };
        result.put(ip_v4.octets().as_slice());
        result.put_u16(node.addr.port());
    }
    ByteBuf::from(result)
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::{
        generate_node_id,
        node::{ID_MAX, ID_ZERO},
        token_store::TokenStore,
    };

    #[test]
    fn roundtrip_ping() {
        let packet = KrpcPacket {
            t: ByteBuf::from(*b"ta"),
            y: 'q',
            q: Some("ping".to_string()),
            a: Some(Query::Ping {
                id: ByteBuf::from(crate::node::ID_MAX.as_bytes()),
            }),
            r: None,
            e: None,
        };
        let encoded = serde_bencoded::to_vec(&packet).unwrap();
        assert_eq!(packet, serde_bencoded::from_bytes(&encoded).unwrap());
    }

    #[test]
    fn serialize_get_peers() {
        // Token store spawns refresh task
        tokio_uring::start(async {
            let token_store = TokenStore::new();
            let ip = Ipv4Addr::new(123, 20, 13, 3);
            let peers = vec![
                "127.0.2.1:6666".parse().unwrap(),
                "127.1.2.1:1337".parse().unwrap(),
            ];
            let packet = KrpcPacket {
                t: ByteBuf::from(*b"ta"),
                y: 'r',
                q: None,
                a: None,
                r: Some(Answer::GetPeers {
                    id: serde_bytes::ByteBuf::from(generate_node_id().as_bytes()),
                    token: serde_bytes::ByteBuf::from(token_store.generate(ip).to_vec()),
                    values: Some(serialize_compact_peers(&peers)),
                    nodes: None,
                }),
                e: None,
            };
            let encoded = serde_bencoded::to_vec(&packet).unwrap();
            assert_eq!(packet, serde_bencoded::from_bytes(&encoded).unwrap());
        });
    }

    #[test]
    fn basic() {
        let encoded: &[u8] = &[
            100, 50, 58, 105, 112, 54, 58, 83, 249, 52, 208, 5, 57, 49, 58, 114, 100, 50, 58, 105,
            100, 50, 48, 58, 50, 245, 78, 105, 115, 81, 255, 74, 236, 41, 205, 186, 171, 242, 251,
            227, 70, 124, 194, 103, 101, 49, 58, 116, 50, 58, 121, 121, 49, 58, 121, 49, 58, 114,
            101,
        ];
        let decoded: KrpcPacket = serde_bencoded::from_bytes(encoded).unwrap();
        assert_eq!(
            decoded,
            KrpcPacket {
                t: ByteBuf::from([121, 121]),
                y: 'r',
                q: None,
                a: None,
                r: Some(Answer::QueriedNodeId {
                    id: ByteBuf::from([
                        50, 245, 78, 105, 115, 81, 255, 74, 236, 41, 205, 186, 171, 242, 251, 227,
                        70, 124, 194, 103
                    ])
                }),
                e: None,
            }
        );
    }

    #[test]
    fn roundtrip_compact_nodes() {
        let nodes = vec![
            Node {
                id: NodeId::new_in_range(&ID_ZERO, &ID_MAX),
                addr: "127.0.2.1:6666".parse().unwrap(),
                last_status: NodeStatus::Good,
                last_seen: OffsetDateTime::now_utc(),
            },
            Node {
                id: NodeId::new_in_range(&ID_ZERO, &ID_MAX),
                addr: "127.1.2.1:1337".parse().unwrap(),
                last_status: NodeStatus::Good,
                last_seen: OffsetDateTime::now_utc(),
            },
        ];

        let compact = serialize_compact_nodes(&nodes);
        let deserialized = deserialize_compact_nodes(compact);

        for (a, b) in deserialized.iter().zip(nodes.iter()) {
            assert_eq!(a.id, b.id);
            assert_eq!(a.addr, b.addr);
        }
    }

    #[test]
    fn roundtrip_compact_peers() {
        let peers = vec![
            "127.0.2.1:6666".parse().unwrap(),
            "127.1.2.1:1337".parse().unwrap(),
        ];

        let compact = serialize_compact_peers(&peers);
        let deserialized = deserialize_compact_peers(compact);

        for (a, b) in deserialized.iter().zip(peers.iter()) {
            assert_eq!(a, b);
        }
    }
}
