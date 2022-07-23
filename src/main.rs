use std::path::Path;

use bytes::{BufMut, Bytes};
use krpc::{KrpcService, Response};
use magnet_url::Magnet;
use rand::prelude::*;
use routing_table::{Node, NodeId, RoutingTable, ID_ZERO};
use sha1::{Digest, Sha1};
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    Resolver,
};

mod krpc;
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

fn generate_node_id() -> NodeId {
    let id = rand::random::<[u8; 20]>();
    let mut hasher = Sha1::new();
    hasher.update(id);
    NodeId::from(hasher.finalize().as_slice())
}

// Bootstrap nodes
//dht.transmissionbt.com 6881
//router.bittorrent.com  6881
//router.bitcomet.com    6881
//dht.aelitis.com        6881
//bootstrap.jami.net     4222

const BOOTSTRAP: &str = "router.bittorrent.com";

fn load_table(path: &Path) -> Option<RoutingTable> {
    serde_json::from_reader(std::fs::File::open(path).ok()?).ok()?
}

fn save_table(path: &Path, table: &RoutingTable) -> anyhow::Result<()> {
    let table_json = serde_json::to_string(&table)?;
    std::fs::write(path, table_json)?;
    Ok(())
}

fn main() {
    // let magent_url = Magnet::new("magnet:?xt=urn:btih:VIJHHSNY6CICT7FIBXMBIIVNCHV4UIDA").unwrap();
    // Do this async
    let resolver = Resolver::new(ResolverConfig::default(), ResolverOpts::default()).unwrap();
    let ip = resolver.lookup_ip(BOOTSTRAP).unwrap();
    let ip = ip.iter().next().unwrap();

    let mut routing_table;
    if let Some(table) = load_table(Path::new("routing_table.json")) {
        routing_table = table;
    } else {
        let node_id = generate_node_id();
        routing_table = RoutingTable::new(node_id);
    }

    tokio_uring::start(async move {
        let service = KrpcService::new("0.0.0.0:1337".parse().unwrap())
            .await
            .unwrap();

        let mut node = Node {
            id: ID_ZERO,
            host: ip.to_string(),
            port: 6881,
        };

        let response = service.ping(&node).await.unwrap();

        if let Response::QueriedNodeId { id } = response {
            node.id = id.into();
            dbg!(&node);
            routing_table.insert_node(node);
            save_table(Path::new("routing_table.json"), &routing_table).unwrap();
        } else {
            panic!("unexpected response");
        }
    });
}
