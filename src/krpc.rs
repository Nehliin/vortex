use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
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
    // For both ping and announce peer
    FindNode {
        id: Bytes,
        nodes: Bytes,
    },
    GetPeers {
        id: Bytes,
        token: Bytes,
        values: Option<Vec<String>>,
        nodes: Option<String>,
    },
    QueriedNodeId {
        id: Bytes,
    },
}

#[derive(Debug, Deserialize)]
pub struct Error {
    pub code: u16,
    pub description: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "KrpcRequest Failed: {} {}",
            self.code, self.description
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
            }
        })
        .collect()
}

// TODO use tower!
pub struct KrpcService {
    socket: UdpSocket,
}

impl KrpcService {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self { socket })
    }

    fn gen_transaction_id() -> String {
        // handle transaction_ids in a saner way so that
        // multiple queries can be sent simultaneously per node
        let mut id = String::new();
        id.push(rand::random::<char>());
        id.push(rand::random::<char>());
        id
    }

    async fn send_req(&self, node: &Node, req: KrpcReq) -> Result<Response, Error> {
        let encoded = serde_bencode::ser::to_bytes(&req).unwrap();

        println!("Sending");
        let (res, _) = self.socket.send_to(encoded, node.addr).await;
        res.unwrap();

        let buf = vec![0; 4096];
        let (response, buf) =
            tokio::time::timeout(Duration::from_secs(3), self.socket.recv_from(buf))
                .await
                .map_err(|_err| Error {
                    code: 408,
                    description: "Request timed out".to_string(),
                })?;

        let (recv, addr) = response.unwrap();
        assert!(
            addr == node.addr,
            "Didn't receive response from right node FIXME"
        );
        println!("received: {recv}");
        let resp: KrpcRes = serde_bencode::de::from_bytes(&buf[..recv]).unwrap();
        assert!(resp.t == req.t, "Recived unrelated response");

        if resp.y == "r" {
            Ok(resp.r.unwrap())
        } else if resp.y == "e" {
            Err(resp.e.unwrap())
        } else {
            panic!("received unexpected response")
        }
    }
    // TODO optimize and rework error handling
    pub async fn ping(&self, node: &Node) -> Result<Pong, Error> {
        const QUERY: &str = "ping";

        let transaction_id = Self::gen_transaction_id();

        let req = KrpcReq {
            t: transaction_id.to_string(),
            y: 'q',
            q: QUERY,
            a: Query::Ping {
                id: node.id.to_bytes(),
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

        let transaction_id = Self::gen_transaction_id();

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
}
