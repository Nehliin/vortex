use std::{
    cell::RefCell,
    net::{IpAddr, SocketAddr},
    path::Path,
    rc::Rc,
    time::Duration,
};

use anyhow::Context;
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
        status: NodeStatus::Unknown,
    };

    let response = service.ping(&routing_table.own_id, &node).await.unwrap();
    node.id = response.id;
    routing_table.insert_node(node);
}

const REFRESH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

pub struct Dht {
    transport: KrpcSocket,
    routing_table: Rc<RefCell<RoutingTable>>,
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
            routing_table: Rc::new(RefCell::new(routing_table)),
        })
    }

    fn insert_node(&self, node: Node) -> bool {
        let mut routing_table = self.routing_table.borrow_mut();
        if routing_table.insert_node(node) {
            drop(routing_table);
            let routing_table_clone = Rc::clone(&self.routing_table);
            let node_id = node.id;
            tokio_uring::spawn(async move {
                tokio::time::sleep(REFRESH_TIMEOUT).await;
                let mut routing_table = routing_table_clone.borrow_mut();
                if let Some(bucket) = routing_table.get_bucket(&node_id) {
                    if OffsetDateTime::now_utc() - bucket.last_changed() >= REFRESH_TIMEOUT {
                        // refresh bucket?
                        // also remember to mark as changed after pings and stuff
                    }
                }
            });
            true
        } else {
            false
        }
    }
    // continue on this
    // then add a "insert_node" method to this dht which inserts + starts refresh task wrap routing
    // table in rc + refcell
    pub async fn start(&self) -> anyhow::Result<()> {
        // Find closest nodes
        // 1. look in bootstrap for self
        // 2. recursivly look for the closest node to self in the resposne
        let own_id = self.routing_table.borrow().own_id;
        let mut prev_min = ID_MAX;

        loop {
            log::info!("Scanning");
            let next_to_query = *self
                .routing_table
                .borrow_mut()
                .get_closest_mut(&own_id)
                .context("No nodes in routing table")?;

            let distance = own_id.distance(&next_to_query.id);
            if distance < prev_min {
                let response = match self
                    .transport
                    .find_nodes(&own_id, &own_id, &next_to_query)
                    .await
                {
                    Ok(reponse) => {
                        let mut routing_table = self.routing_table.borrow_mut();
                        // Unwrap is fine here since it should always exists a node with the given
                        // id in the table at this point
                        let queried_node = routing_table.get_mut(&next_to_query.id).unwrap();
                        queried_node.status = NodeStatus::Good;
                        reponse
                    }
                    Err(err) => {
                        log::warn!("Node {next_to_query:?} ping failed: {err}");
                        match next_to_query.status {
                            NodeStatus::Good | NodeStatus::Unknown => {
                                let mut routing_table = self.routing_table.borrow_mut();
                                // Unwrap is fine here since it should always exists a node with the given
                                // id in the table at this point
                                let queried_node =
                                    routing_table.get_mut(&next_to_query.id).unwrap();
                                queried_node.status = NodeStatus::Bad;
                            }
                            NodeStatus::Bad => {
                                self.routing_table.borrow_mut().remove(&next_to_query)?;
                            }
                        }
                        continue;
                    }
                };

                log::debug!("Got nodes from: {next_to_query:?}");
                for node in response.nodes.into_iter() {
                    if self.insert_node(node) {
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
    fn drop(&mut self) {
        log::info!("Saving table");
        let routing_table = self.routing_table.borrow();
        save_table(Path::new("routing_table.json"), &routing_table).unwrap();
    }
}
