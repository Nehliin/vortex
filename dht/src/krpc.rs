use std::{
    cell::RefCell,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    rc::Rc,
    time::Duration,
};

use ahash::AHashMap;
use bytes::{BufMut, BytesMut};
use rand::Rng;
use serde::{de::Visitor, ser::SerializeSeq, Deserializer, Serializer};
use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::oneshot;
use tokio_uring::net::UdpSocket;

use crate::node::{Node, NodeId, NodeStatus};

// 1. KrpcSocket: Single UDP socket, contains transaction id map and generates transaction ids
// does all io
// 2. KrpcConnection: Node + weak ref to socket + all rpc methods
//
// KrpcQuery builder just like request builder, same with response
// within those you can provide timeout per request (but they have a default value)
// Both implement IntoKrpcPacket which is what the socket accept

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
        values: Option<Vec<ByteBuf>>,
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

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] KrpcError),
    #[error("Query timed out")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("Failed encoding packet")]
    PacketEncoding(#[from] serde_bencoded::SerError),
    #[error("Socket error")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug, PartialEq)]
pub struct KrpcError {
    pub code: u64,
    pub description: String,
}

impl KrpcError {
    fn generic(description: String) -> Self {
        Self {
            code: 201,
            description,
        }
    }

    fn server(description: String) -> Self {
        Self {
            code: 202,
            description,
        }
    }

    fn protocol(description: String) -> Self {
        Self {
            code: 203,
            description,
        }
    }

    fn method_unknown(description: String) -> Self {
        Self {
            code: 204,
            description,
        }
    }
}

fn parse_error<'de, D>(de: D) -> Result<Option<KrpcError>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ErrorVisitor;

    impl<'de> Visitor<'de> for ErrorVisitor {
        type Value = Option<KrpcError>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("Expected bencoded list with one error code + description")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let Some(code) = seq.next_element::<u64>()? else {
                return Ok(None);
            };
            let Some(description) = seq.next_element::<String>()? else {
                return Ok(None);
            };
            // The next_element must be called here for parsing of the rest of the
            // message to be successful for whatever reason. **shrug**
            if matches!(seq.next_element::<char>(), Ok(None)) {
                Ok(Some(KrpcError { code, description }))
            } else {
                Ok(None)
            }
        }
    }
    de.deserialize_any(ErrorVisitor)
}

fn serialize_error<S>(error: &Option<KrpcError>, se: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let Some(KrpcError { code, description }) = error else {
        return se.serialize_none();
    };

    let mut seq = se.serialize_seq(Some(2))?;
    seq.serialize_element::<u64>(code)?;
    seq.serialize_element::<String>(description)?;
    seq.end()
}

impl std::fmt::Display for KrpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KrpcError: {},  {}",
            self.code, self.description
        ))
    }
}

impl std::error::Error for KrpcError {}

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
    #[serde(deserialize_with = "parse_error")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_error")]
    #[serde(default)]
    pub e: Option<KrpcError>,
}

#[derive(Debug)]
pub struct Pong {
    pub id: NodeId,
}

#[derive(Debug)]
pub struct FindNodesResponse {
    pub id: NodeId,
    pub nodes: Vec<Node>,
}

// TODO: move
#[derive(Debug)]
pub struct Peer {
    pub addr: SocketAddr,
}

#[derive(Debug)]
pub enum GetPeerResponseBody {
    Nodes(Vec<Node>),
    Peers(Vec<Peer>),
}

#[derive(Debug)]
pub struct GetPeersResponse {
    pub id: NodeId,
    pub token: ByteBuf,
    pub body: GetPeerResponseBody,
}

#[derive(Debug)]
pub struct AnnounceResponse {
    pub id: NodeId,
}

fn deserialize_compact_nodes(bytes: ByteBuf) -> Vec<Node> {
    if (bytes.len() % 26) != 0 {
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

pub fn serialize_compact_nodes(nodes: &[Node]) -> ByteBuf {
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

#[derive(Clone, Default, Debug)]
struct InflightRpcs(Rc<RefCell<AHashMap<ByteBuf, oneshot::Sender<Result<Response, Error>>>>>);

// Ensures Refcell borrow is never held across await point
impl InflightRpcs {
    fn insert_rpc(&self, transaction_id: ByteBuf) -> oneshot::Receiver<Result<Response, Error>> {
        let mut table = (*self.0).borrow_mut();
        let (tx, rx) = oneshot::channel();
        table.insert(transaction_id, tx);
        rx
    }

    fn exists(&self, transaction_id: &ByteBuf) -> bool {
        let table = (*self.0).borrow();
        table.contains_key(transaction_id)
    }

    fn remove_rpc(
        &self,
        transaction_id: &ByteBuf,
    ) -> Option<oneshot::Sender<Result<Response, Error>>> {
        let mut table = (*self.0).borrow_mut();
        table.remove(transaction_id)
    }
}

// TODO use tower?
#[derive(Clone)]
pub struct KrpcSocket {
    // TODO Set cpu affinity for socket
    socket: Rc<UdpSocket>,
    connection_table: InflightRpcs,
}

impl KrpcSocket {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = Rc::new(UdpSocket::bind(bind_addr).await?);
        let connection_table = InflightRpcs::default();

        Ok(Self {
            socket,
            connection_table,
        })
    }

    // TODO refactor: This needs to be called before anything otherwise the socket becomes
    // useless since responses aren't handled
    pub fn listen(&self) -> tokio::sync::mpsc::Receiver<(ByteBuf, Query, SocketAddr)> {
        // Recv task
        let socket_clone = Rc::clone(&self.socket);
        let connection_table_clone = self.connection_table.clone();
        let (incoming_tx, incoming_rc) = tokio::sync::mpsc::channel(256);
        tokio_uring::spawn(async move {
            let mut recv_buffer = vec![0; 2048];
            loop {
                let (read, buf) = socket_clone
                    .recv_from(std::mem::take(&mut recv_buffer))
                    .await;
                // TODO cancellation and addr should be sent across with packet
                let (recv, addr) = read.unwrap();
                let packet: KrpcPacket = match serde_bencoded::from_bytes(&buf[..recv]) {
                    Ok(packet) => packet,
                    Err(err) => {
                        log::warn!(
                            "Failed parsing KRPC packet: {err}, packet: {:?}",
                            &buf[..recv]
                        );
                        recv_buffer = buf;
                        continue;
                    }
                };
                recv_buffer = buf;

                match packet.y {
                    // response
                    'r' => {
                        if let Some(response) = packet.r {
                            if let Some(response_sender) =
                                connection_table_clone.remove_rpc(&packet.t)
                            {
                                let _ = response_sender.send(Ok(response));
                            } else {
                                log::warn!("Unknown KRPC response recived, transaction id: {:?} not found in connection_table", packet.t);
                            }
                        } else {
                            log::warn!("Invalid KRPC message received, missing response");
                        }
                    }
                    // query
                    'q' => {
                        match (packet.q.as_deref(), packet.a) {
                            (Some("get_peers"), Some(query @ Query::GetPeers { .. }))
                            | (Some("announce_peer"), Some(query @ Query::AnnouncePeer { .. }))
                            | (Some("ping"), Some(query @ Query::Ping { .. }))
                            | (Some("find_node"), Some(query @ Query::FindNode { .. })) => {
                                // TODO handle
                                incoming_tx.send((packet.t, query, addr)).await.unwrap();
                            }
                            _ => log::warn!(
                                "Invalid incoming query: {}",
                                packet.q.as_deref().unwrap_or("<none>")
                            ),
                        }
                    }
                    // error
                    'e' => {
                        if let Some(error) = packet.e {
                            if let Some(response_sender) =
                                connection_table_clone.remove_rpc(&packet.t)
                            {
                                let _ = response_sender.send(Err(error));
                            } else {
                                log::warn!("Unknown KRPC response recived, transaction id: {:?} not found in connection_table", packet.t);
                            }
                        } else {
                            log::warn!("Invalid KRPC message received, missing error");
                        }
                    }
                    _ => log::warn!("Invalid KRPC message received, y: {}", packet.y),
                }
            }
        });
        incoming_rc
    }

    fn gen_transaction_id(&self) -> ByteBuf {
        use rand::distributions::Alphanumeric;
        let mut rng = rand::thread_rng();
        // handle transaction_ids in a saner way so that
        // multiple queries can be sent simultaneously per node
        loop {
            let id = ByteBuf::from([rng.sample(Alphanumeric), rng.sample(Alphanumeric)]);
            if !self.connection_table.exists(&id) {
                return id;
            }
        }
    }

    pub async fn send_req(&self, addr: SocketAddr, req: KrpcPacket) -> Result<Response, Error> {
        let encoded = serde_bencoded::to_vec(&req).unwrap();

        let rx = self.connection_table.insert_rpc(req.t.clone());

        let (res, _) = self.socket.send_to(encoded, addr).await;
        res.unwrap();

        match tokio::time::timeout(Duration::from_secs(3), rx).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Err(_elapsed) => Err(Error {
                code: 408,
                description: "Request timed out".to_string(),
            }),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => unreachable!(),
        }
    }

    pub async fn send_response(&self, addr: SocketAddr, req: KrpcPacket) -> Result<(), Error> {
        let encoded = serde_bencoded::to_vec(&req).unwrap();

        let (res, _) = self.socket.send_to(encoded, addr).await;
        res.unwrap();

        Ok(())
    }
    // TODO optimize and rework error handling
    pub async fn ping(&self, querying_node: &NodeId, node: &Node) -> Result<Pong, Error> {
        let transaction_id = self.gen_transaction_id();

        let req = KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("ping".to_string()),
            a: Some(Query::Ping {
                id: ByteBuf::from(querying_node.as_bytes()),
            }),
            r: None,
            e: None,
        };

        if let Response::QueriedNodeId { id } = self.send_req(node.addr, req).await? {
            Ok(Pong {
                id: id.as_slice().into(),
            })
        } else {
            panic!("unexpected response");
        }
    }

    pub async fn find_nodes(
        &self,
        querying_node: &NodeId,
        target: &NodeId,
        node: &Node,
    ) -> Result<FindNodesResponse, Error> {
        let transaction_id = self.gen_transaction_id();

        let req = KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("find_node".to_string()),
            a: Some(Query::FindNode {
                id: ByteBuf::from(querying_node.as_bytes()),
                target: ByteBuf::from(target.as_bytes()),
            }),
            r: None,
            e: None,
        };

        if let Response::FindNode { id, nodes } = self.send_req(node.addr, req).await? {
            let nodes = deserialize_compact_nodes(nodes);
            Ok(FindNodesResponse {
                id: id.as_slice().into(),
                nodes,
            })
        } else {
            panic!("unexpected response");
        }
    }

    pub async fn get_peers(
        &self,
        querying_node: &NodeId,
        info_hash: [u8; 20],
        node: &Node,
    ) -> Result<GetPeersResponse, Error> {
        let transaction_id = self.gen_transaction_id();

        let req = KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("get_peers".to_string()),
            a: Some(Query::GetPeers {
                id: ByteBuf::from(querying_node.as_bytes()),
                info_hash: ByteBuf::from(info_hash),
            }),
            r: None,
            e: None,
        };

        // get_peers may return other nodes
        match self.send_req(node.addr, req).await? {
            Response::GetPeers {
                id,
                token,
                values,
                nodes,
            } => {
                if let Some(peers) = values {
                    // Might miss if invalid format and its not divisible by chunk size
                    let peers = peers
                        .into_iter()
                        .flat_map(|peer_bytes| {
                            peer_bytes
                                .chunks_exact(6)
                                .map(|bytes| Peer {
                                    addr: SocketAddr::new(
                                        IpAddr::V4(Ipv4Addr::new(
                                            bytes[0], bytes[1], bytes[2], bytes[3],
                                        )),
                                        u16::from_be_bytes([bytes[4], bytes[5]]),
                                    ),
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect();

                    return Ok(GetPeersResponse {
                        id: id.as_slice().into(),
                        token,
                        body: GetPeerResponseBody::Peers(peers),
                    });
                }

                if let Some(nodes) = nodes {
                    let nodes = deserialize_compact_nodes(nodes);

                    return Ok(GetPeersResponse {
                        id: id.as_slice().into(),
                        token,
                        body: GetPeerResponseBody::Nodes(nodes),
                    });
                }
                Err(Error::protocol(
                    "Response contained neither nodes nor peers".to_string(),
                ))
            }
            Response::FindNode { id, nodes } => {
                let nodes = deserialize_compact_nodes(nodes);

                Ok(GetPeersResponse {
                    id: id.as_slice().into(),
                    token: ByteBuf::new(),
                    body: GetPeerResponseBody::Nodes(nodes),
                })
            }
            _ => Err(Error::protocol(
                "Unexpected response from get_peers".to_string(),
            )),
        }
    }

    pub async fn announce_peer(
        &self,
        querying_node: &NodeId,
        info_hash: [u8; 20],
        implied_port: bool,
        port: u16,
        token: ByteBuf,
        node: &Node,
    ) -> Result<AnnounceResponse, Error> {
        let transaction_id = self.gen_transaction_id();
        let req = KrpcPacket {
            t: transaction_id,
            y: 'q',
            q: Some("announce_peer".to_string()),
            a: Some(Query::AnnouncePeer {
                id: ByteBuf::from(querying_node.as_bytes()),
                implied_port,
                info_hash: ByteBuf::from(info_hash),
                port,
                token,
            }),
            r: None,
            e: None,
        };

        if let Response::QueriedNodeId { id } = self.send_req(node.addr, req).await? {
            Ok(AnnounceResponse {
                id: id.as_slice().into(),
            })
        } else {
            panic!("unexpected response");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::node::{ID_MAX, ID_ZERO};

    use super::*;

    #[test]
    fn roundtrip_generic_error() {
        let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
        let decoded: KrpcPacket = serde_bencoded::from_str(encoded).unwrap();
        let expected = KrpcPacket {
            t: ByteBuf::from(b"aa".to_vec()),
            y: 'e',
            r: None,
            e: Some(Error::generic("A Generic Error Ocurred".to_owned())),
            q: None,
            a: None,
        };
        assert_eq!(expected, decoded);
        assert_eq!(encoded, serde_bencoded::to_string(&expected).unwrap());
    }

    #[test]
    fn roundtrip_server_error() {
        let encoded = "d1:eli202e12:Server Errore1:t2:lC1:y1:ee";
        let decoded: KrpcPacket = serde_bencoded::from_str(encoded).unwrap();
        let expected = KrpcPacket {
            t: ByteBuf::from(b"lC".to_vec()),
            y: 'e',
            r: None,
            e: Some(Error::server("Server Error".to_owned())),
            q: None,
            a: None,
        };
        assert_eq!(expected, decoded);
        assert_eq!(encoded, serde_bencoded::to_string(&expected).unwrap());
    }

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
                r: Some(Response::QueriedNodeId {
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
}
