use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    rc::Rc,
    sync::Mutex,
    time::Duration,
};

use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio_uring::net::UdpSocket;

use crate::node::{Node, NodeId};

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Query {
    Ping {
        id: Bytes,
    },
    FindNode {
        id: Bytes,
        target: Bytes,
    },
    GetPeers {
        id: Bytes,
        info_hash: Bytes,
    },
    AnnouncePeer {
        id: Bytes,
        implied_port: bool,
        info_hash: Bytes,
        port: u16,
        token: Bytes,
    },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum Response {
    GetPeers {
        id: Bytes,
        token: Bytes,
        values: Option<Vec<Bytes>>,
        nodes: Option<Bytes>,
    },
    // For both ping and announce peer
    FindNode {
        id: Bytes,
        nodes: Bytes,
    },
    QueriedNodeId {
        id: Bytes,
    },
}

#[derive(Debug, Deserialize)]
pub struct Error {
    pub code: u32,
    pub description: Bytes,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KrpcRequest Failed: {} {}",
            self.code,
            String::from_utf8_lossy(&self.description)
        ))
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Serialize)]
struct KrpcReq {
    t: String,
    y: char,
    q: &'static str,
    a: Query,
}

#[derive(Debug, Deserialize)]
struct KrpcRes {
    t: String,
    y: String,
    r: Option<Response>,
    e: Option<Error>,
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
    pub token: Bytes,
    pub body: GetPeerResponseBody,
}

fn parse_compact_nodes(bytes: Bytes) -> Vec<Node> {
    // TODO this will panic on invalid input
    bytes
        .chunks(26)
        .map(|chunk| {
            // Seems to be working?
            let id = chunk[..20].into();
            let ip: IpAddr = [chunk[20], chunk[21], chunk[22], chunk[23]].into();
            let port = u16::from_be_bytes([chunk[24], chunk[25]]);
            Node {
                id,
                addr: (ip, port).into(),
                last_seen: OffsetDateTime::now_utc(),
            }
        })
        .collect()
}

// TODO improve
type ConnectionTable =
    Rc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<Result<Response, Error>>>>>;

// TODO use tower!
#[derive(Clone)]
pub struct KrpcService {
    socket: Rc<UdpSocket>,
    connection_table: ConnectionTable,
}

impl KrpcService {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = Rc::new(UdpSocket::bind(bind_addr).await?);
        let connection_table: ConnectionTable = Rc::new(Mutex::new(HashMap::new()));
        let connection_table_clone = Rc::clone(&connection_table);
        // too large
        let mut recv_buffer = vec![0; 4096];
        let socket_clone = Rc::clone(&socket);
        tokio_uring::spawn(async move {
            loop {
                let (read, mut buf) = socket_clone
                    .recv_from(std::mem::take(&mut recv_buffer))
                    .await;
                let (recv, _addr) = read.unwrap();
                let resp: KrpcRes = match serde_bencode::de::from_bytes(&buf[..recv]) {
                    Ok(resp) => resp,
                    Err(err) => {
                        log::error!("Error parsing packet: {err}, packet: {}", String::from_utf8_lossy(&buf[..recv]));
                        buf.clear();
                        recv_buffer = buf;
                        continue;
                    },
                };

                let mut table = connection_table_clone.lock().unwrap();
                if let Some(response_sender) = table.remove(&resp.t) {
                    if resp.y == "r" {
                        let _ = response_sender.send(Ok(resp.r.unwrap()));
                    } else if resp.y == "e" {
                        let _ = response_sender.send(Err(resp.e.unwrap()));
                    } else {
                        panic!("received unexpected response")
                    }
                } else {
                    log::warn!("Transaction_id not found in the connection_table");
                }
                buf.clear();
                recv_buffer = buf;
            }
        });

        Ok(Self {
            socket,
            connection_table,
        })
    }

    fn gen_transaction_id(&self) -> String {
        // handle transaction_ids in a saner way so that
        // multiple queries can be sent simultaneously per node
        let mut id = String::new();
        id.push(rand::random::<char>());
        id.push(rand::random::<char>());

        let conn_table = self.connection_table.lock().unwrap();
        if conn_table.contains_key(&id) {
            drop(conn_table);
            // need to generate another id
            self.gen_transaction_id()
        } else {
            id
        }
    }

    async fn send_req(&self, node: &Node, req: KrpcReq) -> Result<Response, Error> {
        let encoded = serde_bencode::ser::to_bytes(&req).unwrap();

        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut table = self.connection_table.lock().unwrap();
            table.insert(req.t.clone(), tx);
        }

        log::debug!("Sending");
        let (res, _) = self.socket.send_to(encoded, node.addr).await;
        res.unwrap();

        match tokio::time::timeout(Duration::from_secs(3), rx).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Err(_elapsed) => Err(Error {
                code: 408,
                description: b"Request timed out".as_slice().into(),
            }),
            Ok(Ok(Err(err))) => Err(err),
            Ok(Err(_)) => panic!("sender dropped"),
        }
    }
    // TODO optimize and rework error handling
    pub async fn ping(&self, querying_node: &NodeId, node: &Node) -> Result<Pong, Error> {
        const QUERY: &str = "ping";

        let transaction_id = self.gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id.to_string(),
            y: 'q',
            q: QUERY,
            a: Query::Ping {
                id: querying_node.to_bytes(),
            },
        };

        if let Response::QueriedNodeId { id } = self.send_req(node, req).await? {
            Ok(Pong { id: id.into() })
        } else {
            panic!("unexpected response");
        }
    }

    pub async fn find_nodes(
        &self,
        querying_node: &NodeId,
        target: &NodeId,
        queried_node: &Node,
    ) -> Result<FindNodesResponse, Error> {
        const QUERY: &str = "find_node";

        let transaction_id = self.gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id.to_string(),
            y: 'q',
            q: QUERY,
            a: Query::FindNode {
                id: querying_node.to_bytes(),
                target: target.to_bytes(),
            },
        };

        if let Response::FindNode { id, nodes } = self.send_req(queried_node, req).await? {
            let nodes = parse_compact_nodes(nodes);
            Ok(FindNodesResponse {
                id: id.into(),
                nodes,
            })
        } else {
            panic!("unexpected response");
        }
    }

    pub async fn get_peers(
        &self,
        querying_node: &NodeId,
        info_hash: Bytes,
        node: &Node,
    ) -> Result<GetPeersResponse, Error> {
        const QUERY: &str = "get_peers";

        let transaction_id = self.gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id.to_string(),
            y: 'q',
            q: QUERY,
            a: Query::GetPeers {
                id: querying_node.to_bytes(),
                info_hash,
            },
        };

        // Apparently nodes don't follow spec?
        match self.send_req(node, req).await? {
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
                        id: id.into(),
                        token,
                        body: GetPeerResponseBody::Peers(peers),
                    });
                }

                if let Some(nodes) = nodes {
                    let nodes = parse_compact_nodes(nodes);

                    return Ok(GetPeersResponse {
                        id: id.into(),
                        token,
                        body: GetPeerResponseBody::Nodes(nodes),
                    });
                }
                Err(Error {
                    code: 203,
                    description: b"Response contained neither nodes nor peers"
                        .as_slice()
                        .into(),
                })
            }
            Response::FindNode { id, nodes } => {
                let nodes = parse_compact_nodes(nodes);

                Ok(GetPeersResponse {
                    id: id.into(),
                    token: Bytes::new(),
                    body: GetPeerResponseBody::Nodes(nodes),
                })
            }
            _ => Err(Error {
                code: 203,
                description: b"Unexpected response from get_peers".as_slice().into(),
            }),
        }
    }
}
