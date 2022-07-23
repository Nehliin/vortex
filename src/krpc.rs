use std::net::SocketAddr;

use bytes::Bytes;
use serde_derive::{Deserialize, Serialize};
use tokio_uring::net::UdpSocket;

use crate::routing_table::Node;

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
    QueriedNodeId {
        id: Bytes,
    },
    FindNode {
        id: Bytes,
        nodes: String,
    },
    GetPeers {
        id: Bytes,
        token: Bytes,
        values: Option<Vec<String>>,
        nodes: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
pub struct Error {
    code: u8,
    description: String,
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

// TODO use tower!
pub struct KrpcService {
    socket: UdpSocket,
}

impl KrpcService {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self { socket })
    }

    // TODO optimize and rework error handling
    pub async fn ping(&self, node: &Node) -> Result<Response, Error> {
        const QUERY: &str = "ping";
        // handle transaction_ids in a saner way so that
        // multiple queries can be sent simultaneously per node
        let transaction_id: u16 = rand::random();
        let req = KrpcReq {
            t: transaction_id.to_string(),
            y: 'q',
            q: QUERY,
            a: Query::Ping {
                id: node.id.to_bytes(),
            },
        };

        let encoded = serde_bencode::ser::to_bytes(&req).unwrap();

        let node_addr = format!("{}:{}", node.host, node.port).parse().unwrap();
        let (res, _) = self.socket.send_to(encoded, node_addr).await;
        res.unwrap();

        let buf = vec![0; 256];
        let (response, buf) = self.socket.recv_from(buf).await;
        let (recv, addr) = response.unwrap();
        assert!(
            addr == node_addr,
            "Didn't receive response from right node FIXME"
        );
        let resp: KrpcRes = serde_bencode::de::from_bytes(&buf[..recv]).unwrap();
        assert!(
            resp.t == transaction_id.to_string(),
            "Recived unrelated response"
        );

        if resp.y == "r" {
            Ok(resp.r.unwrap())
        } else if resp.y == "e" {
            Err(resp.e.unwrap())
        } else {
            panic!("received unexpected response")
        }
    }
}
