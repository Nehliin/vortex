use std::{
    cell::RefCell,
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    rc::Rc,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::oneshot;
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
    t: Bytes,
    y: char,
    q: &'static str,
    a: Query,
}

#[derive(Debug, Deserialize)]
struct KrpcRes {
    t: Bytes,
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

#[derive(Clone, Default, Debug)]
struct InflightRpcs(Rc<RefCell<HashMap<Bytes, oneshot::Sender<Result<Response, Error>>>>>);

// Ensures Refcell borrow is never held across await point
impl InflightRpcs {
    fn insert_rpc(
        &self,
        transaction_id: Bytes,
    ) -> oneshot::Receiver<Result<Response, Error>> {
        let mut table = (*self.0).borrow_mut();
        let (tx, rx) = oneshot::channel();
        table.insert(transaction_id, tx);
        rx
    }

    fn exists(&self, transaction_id: &Bytes) -> bool {
        let table = (*self.0).borrow();
        table.contains_key(transaction_id)
    }

    fn remove_rpc(
        &self,
        transaction_id: &Bytes,
    ) -> Option<oneshot::Sender<Result<Response, Error>>> {
        let mut table = (*self.0).borrow_mut();
        table.remove(transaction_id)
    }
}

// EXAMPLE OF FAILURE:
// Error parsing packet: Missing Field: `t`, packet: d1:eli202e12:Server Errore1:t2:lC1:y1:ee
// Error parsing packet: Missing Field: `t`, packet: d1:eli202e12:Server Errore1:t2:zg1:y1:ee


// TODO use tower!
#[derive(Clone)]
pub struct KrpcService {
    // TODO Set cpu affinity for socket
    socket: Rc<UdpSocket>,
    connection_table: InflightRpcs,
}

impl KrpcService {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = Rc::new(UdpSocket::bind(bind_addr).await?);
        let connection_table = InflightRpcs::default();
        let connection_table_clone = connection_table.clone();
        // too large
        let mut recv_buffer = vec![0; 4096];
        let socket_clone = Rc::clone(&socket);
        tokio_uring::spawn(async move {
            loop {
                let (read, mut buf) = socket_clone
                    .recv_from(std::mem::take(&mut recv_buffer))
                    .await;
                let (recv, _addr) = read.unwrap();
                // TODO This might fail on errors where t can't be parsed
                // EX: Error parsing packet: Missing Field: `t`, packet: d1:eli202e12:Server Errore1:t2:��1:y1:ee
                let resp: KrpcRes = match serde_bencode::de::from_bytes(&buf[..recv]) {
                    Ok(resp) => resp,
                    Err(err) => {
                        log::error!(
                            "Error parsing packet: {err}, packet: {}",
                            String::from_utf8_lossy(&buf[..recv])
                        );
                        buf.clear();
                        recv_buffer = buf;
                        continue;
                    }
                };

                let response_sender = connection_table_clone.remove_rpc(&resp.t);
                if let Some(response_sender) = response_sender {
                    if resp.y == "r" {
                        let _ = response_sender.send(Ok(resp.r.unwrap()));
                    } else if resp.y == "e" {
                        let _ = response_sender.send(Err(resp.e.unwrap()));
                    } else {
                        panic!("received unexpected response")
                    }
                } else {
                    // TODO probably incomming connections
                    log::error!(
                        "Transaction_id: {:?} not found in the connection_table",
                        resp.t,
                    );
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

    fn gen_transaction_id(&self) -> Bytes {
        use rand::distributions::Alphanumeric;
        let mut rng = rand::thread_rng();
        // handle transaction_ids in a saner way so that
        // multiple queries can be sent simultaneously per node
        let mut id = BytesMut::new();
        id.put_u8(rng.sample(Alphanumeric) as u8);
        id.put_u8(rng.sample(Alphanumeric) as u8);
        let id = id.freeze();

        if self.connection_table.exists(&id) {
            // need to generate another id
            self.gen_transaction_id()
        } else {
            id
        }
    }

    async fn send_req(&self, node: &Node, req: KrpcReq) -> Result<Response, Error> {
        let encoded = serde_bencode::ser::to_bytes(&req).unwrap();

        let rx = self.connection_table.insert_rpc(req.t.clone());

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
            t: transaction_id,
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
            t: transaction_id,
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
            t: transaction_id,
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
