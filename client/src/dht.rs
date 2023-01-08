use std::{
    net::{IpAddr, SocketAddr},
    path::Path,
};

use sha1::{Digest, Sha1};
use time::OffsetDateTime;
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    Resolver,
};

use crate::{
    krpc::KrpcSocket,
    node::{Node, NodeId, NodeStatus, ID_MAX, ID_ZERO},
    routing_table::RoutingTable,
};

// Bootstrap nodes
//dht.transmissionbt.com 6881
//router.bittorrent.com  6881
//router.bitcomet.com    6881
//dht.aelitis.com        6881
//bootstrap.jami.net     4222

const BOOTSTRAP: &str = "router.bittorrent.com";

#[inline]
fn generate_node_id() -> NodeId {
    let id = rand::random::<[u8; 20]>();
    let mut hasher = Sha1::new();
    hasher.update(id);
    NodeId::from(hasher.finalize().as_slice())
}

// make these async
#[inline]
fn load_table(path: &Path) -> Option<RoutingTable> {
    serde_json::from_reader(std::fs::File::open(path).ok()?).ok()?
}

#[inline]
fn save_table(path: &Path, table: &RoutingTable) -> anyhow::Result<()> {
    let table_json = serde_json::to_string(&table)?;
    std::fs::write(path, table_json)?;
    Ok(())
}

async fn bootstrap(routing_table: &mut RoutingTable, service: &KrpcSocket, boostrap_ip: IpAddr) {
    let mut node = Node {
        id: ID_ZERO,
        addr: format!("{boostrap_ip}:6881").parse().unwrap(),
        last_seen: OffsetDateTime::now_utc(),
    };

    let response = service.ping(&routing_table.own_id, &node).await.unwrap();
    node.id = response.id;
    routing_table.insert_node(node);
}

pub struct Dht {
    transport: KrpcSocket,
    routing_table: RoutingTable,
}

impl Dht {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let transport = KrpcSocket::new(bind_addr).await?;
        let mut routing_table;
        if let Some(table) = load_table(Path::new("routing_table.json")) {
            log::info!("Loading table!");
            routing_table = table;
        } else {
            let resolver =
                Resolver::new(ResolverConfig::default(), ResolverOpts::default()).unwrap();
            let ip = resolver.lookup_ip(BOOTSTRAP).unwrap();
            let ip = ip.iter().next().unwrap();
            let node_id = generate_node_id();
            routing_table = RoutingTable::new(node_id);
            bootstrap(&mut routing_table, &transport, ip).await;
        }

        Ok(Dht {
            transport,
            routing_table,
        })
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        // Find closest nodes
        // 1. look in bootstrap for self
        // 2. recursivly look for the closest node to self in the resposne
        let own_id = self.routing_table.own_id;
        let mut prev_min = ID_MAX;

        loop {
            log::info!("Scanning");
            let next_to_query = self.routing_table.get_closest_mut(&own_id).unwrap();

            let distance = own_id.distance(&next_to_query.id);
            if distance < prev_min {
                let response = match self
                    .transport
                    .find_nodes(&own_id, &own_id, next_to_query)
                    .await
                {
                    Ok(reponse) => {
                        next_to_query.status = NodeStatus::Good;
                        reponse
                    }
                    Err(err) => {
                        log::trace!("Node ping timeout for: {next_to_query:?}");
                        match next_to_query.status {
                            NodeStatus::Good => next_to_query.status = NodeStatus::Bad,
                            NodeStatus::Bad => {
                                let clone = next_to_query.clone();
                                self.routing_table.remove(&clone).unwrap();
                            }
                            NodeStatus::Unknown => next_to_query.status = NodeStatus::Bad,
                        }
                        continue;
                    }
                };

                log::debug!("Got nodes from: {next_to_query:?}");
                for node in response.nodes.into_iter() {
                    if self.routing_table.insert_node(node) {
                        log::debug!("Inserted node");
                    }
                }
                prev_min = distance;
            } else {
                break;
            }
        }
        Ok(())
    }
}

impl Drop for Dht {
    // TODO don't block
    fn drop(&mut self) {
        log::info!("Saving table");
        save_table(Path::new("routing_table.json"), &self.routing_table).unwrap();
    }
}
