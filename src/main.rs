use bytes::{BufMut, Bytes};
use magnet_url::Magnet;
use rand::prelude::*;
use routing_table::RoutingTable;
use serde_bytes::ByteBuf;
use serde_derive::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use tokio_uring::net::UdpSocket;
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    Resolver,
};

mod routing_table;

// https://www.bittorrent.org/beps/bep_0015.html
struct TrackerPacket {
    protocol_id: i64,
    action: i32,
    transaction_id: i32,
}

impl TrackerPacket {
    fn new(action: i32) -> Self {
        TrackerPacket {
            protocol_id: 0x41727101980, // magic constant
            action,
            transaction_id: rand::random::<i32>(),
        }
    }

    fn to_bytes(self) -> bytes::Bytes {
        let mut bytes = bytes::BytesMut::new();
        bytes.reserve(16);
        bytes.put_i64(self.protocol_id);
        bytes.put_i32(self.action);
        bytes.put_i32(self.transaction_id);
        bytes.freeze()
    }
}

#[derive(Debug)]
struct TrackerResponse {
    action: i32,
    transaction_id: i32,
    connection_id: i64,
}

impl From<Bytes> for TrackerResponse {
    fn from(mut bytes: Bytes) -> Self {
        use bytes::Buf;
        let action = bytes.get_i32();
        let transaction_id = bytes.get_i32();
        let connection_id = bytes.get_i64();
        TrackerResponse {
            action,
            transaction_id,
            connection_id,
        }
    }
}

#[derive(Debug)]
struct AnnounceRequest {
    connection_id: i64,
    action: i32,
    transaction_id: i32,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    downloaded: i64,
    left: i64,
    uploaded: i64,
    event: i32,
    ip_addr: i32,
    key: i32,
    num_want: i32,
    port: i32,
}

impl AnnounceRequest {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = bytes::BytesMut::new();
        bytes.put_i64(self.connection_id);
        bytes.put_i32(self.action);
        bytes.put_i32(self.transaction_id);
        bytes.put_slice(&self.info_hash);
        bytes.put_slice(&self.peer_id);
        bytes.put_i64(self.downloaded);
        bytes.put_i64(self.left);
        bytes.put_i64(self.uploaded);
        bytes.put_i64(self.uploaded);
        bytes.freeze()
    }
}

fn generate_node_id() -> Bytes {
    let id = rand::random::<[u8; 20]>();
    let mut hasher = Sha1::new();
    hasher.update(id);
    Bytes::copy_from_slice(hasher.finalize().as_slice())
    /*.into_iter()
    .take(10)
    .map(|byte| format!("{byte:02x}"))
    .collect()*/
}

// Bootstrap nodes
//dht.transmissionbt.com 6881
//router.bittorrent.com  6881
//router.bitcomet.com    6881
//dht.aelitis.com        6881
//bootstrap.jami.net     4222

const BOOTSTRAP: &str = "router.bittorrent.com";

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Query {
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
enum Response {
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

#[derive(Debug, Serialize)]
struct KrpcReq {
    t: String,
    y: char,
    q: String,
    a: Query,
}

#[derive(Debug, Deserialize)]
struct KrpcRes {
    t: String,
    // TODO errors
    y: String,
    r: Response,
}

fn main() {
    let node_id = generate_node_id();
    // Do this async
    let resolver = Resolver::new(ResolverConfig::default(), ResolverOpts::default()).unwrap();
    let ip = resolver.lookup_ip(BOOTSTRAP).unwrap();
    let ip = ip.iter().next().unwrap();

    //let mut routing_table = RoutingTable::new();

    tokio_uring::start(async move {
        let socket = UdpSocket::bind("0.0.0.0:1337".parse().unwrap())
            .await
            .unwrap();
        let msg = serde_bencode::ser::to_bytes(&KrpcReq {
            t: "ta".to_owned(),
            y: 'q',
            q: "ping".to_owned(),
            a: Query::Ping {
                id: node_id.clone(),
            },
        })
        .unwrap();

        dbg!(serde_bencode::ser::to_string(&KrpcReq {
            t: "ta".to_string(),
            y: 'q',
            q: "ping".to_string(),
            a: Query::Ping {
                id: "abcdefghij0123456789".as_bytes().into(),
            }
        })
        .unwrap());

        let (res, _) = socket
            .send_to(msg, format!("{ip}:6881").parse().unwrap())
            .await;
        res.unwrap();

        println!("sent!");
        let buf = vec![0; 256];
        let (response, buf) = socket.recv_from(buf).await;
        println!("recv");
        let (recv, _addr) = response.unwrap();
        let resp: KrpcRes = serde_bencode::de::from_bytes(&buf[..recv]).unwrap();
        dbg!(&resp);

        assert!(resp.t == "ta".to_string());
        if let Response::QueriedNodeId { id } = resp.r {
        } else {
            panic!("Incorrect response");
        }

        let magent_url =
            Magnet::new("magnet:?xt=urn:btih:VIJHHSNY6CICT7FIBXMBIIVNCHV4UIDA").unwrap();
    });
}
