use std::{
    cell::RefCell,
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    rc::Rc,
    time::Duration,
};

use rand::Rng;
use serde::{de::Visitor, Deserializer};
use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};
use time::OffsetDateTime;
use tokio::sync::oneshot;
use tokio_uring::net::UdpSocket;

use crate::node::{Node, NodeId};

// 1. KrpcSocket: Single UDP socket, contains transaction id map and generates transaction ids  
// does all io
// 2. KrpcConnection: Node + weak ref to socket + all rpc methods

// TODO try to avoid allocations for ids
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Query {
    Ping {
        id: ByteBuf,
    },
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
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Response {
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

#[derive(Debug, PartialEq)]
pub struct Error {
    pub code: u64,
    pub description: String,
}

impl Error {
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

fn parse_error<'de, D>(de: D) -> Result<Option<Error>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ErrorVisitor;

    impl<'de> Visitor<'de> for ErrorVisitor {
        type Value = Option<Error>;

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
                Ok(Some(Error { code, description }))
            } else {
                Ok(None)
            }
        }
    }
    de.deserialize_any(ErrorVisitor)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KrpcError: {},  {}",
            self.code, self.description
        ))
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Serialize)]
struct KrpcReq {
    t: ByteBuf,
    y: char,
    q: &'static str,
    a: Query,
}

#[derive(Debug, Deserialize, PartialEq)]
struct KrpcRes {
    t: ByteBuf,
    y: char,
    r: Option<Response>,
    #[serde(deserialize_with = "parse_error")]
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
    pub token: ByteBuf,
    pub body: GetPeerResponseBody,
}

#[derive(Debug)]
pub struct AnnounceResponse {
    pub id: NodeId,
}

fn parse_compact_nodes(bytes: ByteBuf) -> Vec<Node> {
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
struct InflightRpcs(Rc<RefCell<HashMap<ByteBuf, oneshot::Sender<Result<Response, Error>>>>>);

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
                let resp: KrpcRes = match serde_bencoded::from_bytes(&buf[..recv]) {
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
                    if resp.y == 'r' {
                        let _ = response_sender.send(Ok(resp.r.unwrap()));
                    } else if resp.y == 'e' {
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

    fn gen_transaction_id(&self) -> ByteBuf {
        use rand::distributions::Alphanumeric;
        let mut rng = rand::thread_rng();
        // handle transaction_ids in a saner way so that
        // multiple queries can be sent simultaneously per node
        let id = ByteBuf::from(vec![rng.sample(Alphanumeric); 2]);
        if self.connection_table.exists(&id) {
            // need to generate another id
            self.gen_transaction_id()
        } else {
            id
        }
    }

    async fn send_req(&self, node: &Node, req: KrpcReq) -> Result<Response, Error> {
        let encoded = serde_bencoded::to_vec(&req).unwrap();

        let rx = self.connection_table.insert_rpc(req.t.clone());

        log::debug!("Sending");
        let (res, _) = self.socket.send_to(encoded, node.addr).await;
        res.unwrap();

        match tokio::time::timeout(Duration::from_secs(5), rx).await {
            Ok(Ok(Ok(response))) => Ok(response),
            Err(_elapsed) => Err(Error {
                code: 408,
                description: "Request timed out".to_string(),
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
                id: ByteBuf::from(querying_node.as_bytes()),
            },
        };

        if let Response::QueriedNodeId { id } = self.send_req(node, req).await? {
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
        queried_node: &Node,
    ) -> Result<FindNodesResponse, Error> {
        const QUERY: &str = "find_node";

        let transaction_id = self.gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id,
            y: 'q',
            q: QUERY,
            a: Query::FindNode {
                id: ByteBuf::from(querying_node.as_bytes()),
                target: ByteBuf::from(target.as_bytes()),
            },
        };

        if let Response::FindNode { id, nodes } = self.send_req(queried_node, req).await? {
            let nodes = parse_compact_nodes(nodes);
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
        const QUERY: &str = "get_peers";

        let transaction_id = self.gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id,
            y: 'q',
            q: QUERY,
            a: Query::GetPeers {
                id: ByteBuf::from(querying_node.as_bytes()),
                info_hash: ByteBuf::from(info_hash),
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
                        id: id.as_slice().into(),
                        token,
                        body: GetPeerResponseBody::Peers(peers),
                    });
                }

                if let Some(nodes) = nodes {
                    let nodes = parse_compact_nodes(nodes);

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
                let nodes = parse_compact_nodes(nodes);

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

    /*pub async fn announce_peer(
        &self,
        querying_node: &NodeId,
        info_hash: [u8; 20],
        port: u16,
        token: ByteBuf,
    ) -> Result<AnnounceResponse, Error> {
        //announce_peers Query = {"t":"aa", "y":"q", "q":"announce_peer", "a": {"id":"abcdefghij0123456789", "implied_port": 1, "info_hash":"mnopqrstuvwxyz123456", "port": 6881, "token": "aoeusnth"}}
        const QUERY: &str = "announce_peer";

        let transaction_id = self.gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id,
            y: 'q',
            q: QUERY,
            a: Query::AnnouncePeer {
                id: ByteBuf::from(querying_node.as_bytes()),
                implied_port: false,
                info_hash: ByteBuf::from(info_hash),
                port,
                token,
            },
        };

        //match self.send_req(node, req)
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_generic_error() {
        let encoded = "d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee";
        let decoded: KrpcRes = serde_bencoded::from_str(encoded).unwrap();
        assert_eq!(
            KrpcRes {
                t: ByteBuf::from(b"aa".to_vec()),
                y: 'e',
                r: None,
                e: Some(Error::generic("A Generic Error Ocurred".to_owned()))
            },
            decoded
        );
    }

    #[test]
    fn parse_server_error() {
        let encoded = "d1:eli202e12:Server Errore1:t2:lC1:y1:ee";
        let decoded: KrpcRes = serde_bencoded::from_str(encoded).unwrap();
        assert_eq!(
            KrpcRes {
                t: ByteBuf::from(b"lC".to_vec()),
                y: 'e',
                r: None,
                e: Some(Error::server("Server Error".to_owned()))
            },
            decoded
        );
    }
}
