use std::{collections::BTreeMap, net::IpAddr, path::Path};

use bytes::{BufMut, Bytes};
use krpc::KrpcService;
use magnet_url::Magnet;
use node::{NodeId, ID_MAX};
use routing_table::RoutingTable;
use sha1::{Digest, Sha1};
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    Resolver,
};

use crate::node::{Node, ID_ZERO};

mod krpc;
mod node;
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

async fn bootstrap(routing_table: &mut RoutingTable, service: &KrpcService, boostrap_ip: IpAddr) {
    let mut node = Node {
        id: ID_ZERO,
        addr: format!("{boostrap_ip}:6881").parse().unwrap(),
    };

    let response = service.ping(&routing_table.own_id, &node).await.unwrap();
    node.id = response.id;
    routing_table.insert_node(node);

    refresh(routing_table, service).await;
}

async fn refresh(routing_table: &mut RoutingTable, service: &KrpcService) {
    // Find closest nodes
    // 1. look in bootstrap for self
    // 2. recursivly look for the closest node to self in the resposne
    let own_id = routing_table.own_id;
    let mut prev_min = ID_MAX;
    loop {
        log::info!("Scanning");
        let next_to_query = routing_table
            .buckets
            .iter_mut()
            .flat_map(|bucket| bucket.nodes.iter_mut())
            .min_by_key(|node| {
                if let Some(node) = node {
                    own_id.distance(&node.id)
                } else {
                    ID_MAX
                }
            })
            .unwrap();

        let distance = own_id.distance(&next_to_query.as_ref().unwrap().id);
        if distance < prev_min {
            let response = match service
                .find_nodes(&own_id, &own_id, next_to_query.as_ref().unwrap())
                .await
            {
                Ok(reponse) => reponse,
                Err(err) => {
                    if err.code == 408 {
                        log::warn!("timeout for: {next_to_query:?}");
                        next_to_query.take();
                        continue;
                    } else {
                        panic!("{err}");
                    }
                }
            };

            log::debug!("Got nodes from: {next_to_query:?}");
            for node in response.nodes.into_iter() {
                if routing_table.insert_node(node) {
                    println!("Inserted node");
                }
            }
            prev_min = distance;
        } else {
            log::info!("Saving table");
            save_table(Path::new("routing_table.json"), routing_table).unwrap();
            break;
        }
    }
}

fn pop_first(btree_map: &mut BTreeMap<NodeId, Node>) -> Node {
    let key = *btree_map.iter().next().unwrap().0;
    btree_map.remove(&key).unwrap()
}

fn main() {
    env_logger::init();

    // Do this async
    let resolver = Resolver::new(ResolverConfig::default(), ResolverOpts::default()).unwrap();
    let ip = resolver.lookup_ip(BOOTSTRAP).unwrap();
    let ip = ip.iter().next().unwrap();

    let should_bootstrap;
    let mut routing_table;
    if let Some(table) = load_table(Path::new("routing_table.json")) {
        log::info!("Loading table!");
        routing_table = table;
        should_bootstrap = false;
    } else {
        let node_id = generate_node_id();
        routing_table = RoutingTable::new(node_id);
        should_bootstrap = true;
    }

    tokio_uring::start(async move {
        let service = KrpcService::new("0.0.0.0:1337".parse().unwrap())
            .await
            .unwrap();

        if should_bootstrap {
            bootstrap(&mut routing_table, &service, ip).await;
        } else {
            std::fs::remove_file(Path::new("routing_table.json")).unwrap();
            refresh(&mut routing_table, &service).await;
        }

        routing_table.ping_all_nodes(&service).await;
        log::info!("Done with pings");

        let remaining = routing_table
            .buckets
            .iter()
            .flat_map(|bucket| &bucket.nodes)
            .filter(|node| node.is_some())
            .count();
        log::info!("remaining: {remaining}");

        let magent_url =
            Magnet::new("magnet:?xt=urn:btih:VIJHHSNY6CICT7FIBXMBIIVNCHV4UIDA").unwrap();


        let info_hash = dbg!(NodeId::from(&magent_url.xt.unwrap().as_bytes()[..20]));

        let closest_node = routing_table.get_closest(&info_hash);
        let mut btree_map = BTreeMap::new();
        btree_map.insert(info_hash.distance(&closest_node.id), closest_node.clone());
        loop {
            let closest_node = pop_first(&mut btree_map);
            log::debug!("Closest node: {closest_node:?}");

            let response = match service
                .get_peers(&routing_table.own_id, info_hash.to_bytes(), &closest_node)
                .await
            {
                Ok(response) => response,
                Err(err) => {
                    log::error!("Failed get_peers request: {err}");
                    continue;
                }
            };

            dbg!(&response);
            match response.body {
                krpc::GetPeerResponseBody::Nodes(nodes) => {
                    for node in nodes.into_iter() {
                        btree_map.insert(info_hash.distance(&node.id), node);
                    }
                }
                krpc::GetPeerResponseBody::Peers(peers) => {
                    dbg!(peers);
                    break;
                }
            }
        }

        // announce peer
    });
}
