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
    krpc::{Error, KrpcSocket},
    node::{Node, NodeId, NodeStatus, ID_MAX, ID_ZERO},
    routing_table::{Bucket, RoutingTable},
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
        last_status: NodeStatus::Unknown,
    };

    let response = service.ping(&routing_table.own_id, &node).await.unwrap();
    node.id = response.id;
    routing_table.insert_node(node);
    // This should be done with the correct "insert_node"
    todo!()
}

const REFRESH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

// TODO: refresh our own id occasionally as well
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

    // TODO: update last refreshed timestamp
    // can't borrow bucket here
    async fn refresh_bucket(&self, target_id: &NodeId) {
        // 1. Generate random id within bucket range
        let id = bucket.random_id();
        let (our_id, is_full) = {
            let routing_table = self.routing_table.borrow();
            (routing_table.own_id, routing_table.is_full(bucket))
        };

        // 2. if the bucket is full (and not splittable) just ping all the nodes and update timestamps
        // (Försök pinga bara candidate noden men om det failar fortsätt pinga de andra)
        // kan göra samma när man anropar get_peers
        if is_full {
            let candiate = bucket.nodes().min_by_key(|node| id.distance(&node.id))
            for node in bucket.nodes() {
                //let routing_table_clone = Rc::clone(&self.routing_table);
                let transport = self.transport.clone();
                let mut node = *node;
                tokio_uring::spawn(async move {
                    match transport.ping(&our_id, &node).await {
                        Ok(_) => {
                            node.last_seen = OffsetDateTime::now_utc();
                            node.last_status = NodeStatus::Good;
                        }
                        Err(err) if err.is_timeout() && node.last_status == NodeStatus::Good => {
                            node.last_status = NodeStatus::Unknown;
                        }
                        Err(err) if err.is_timeout() && node.last_status == NodeStatus::Unknown => {
                            node.last_status = NodeStatus::Bad;
                        }
                        Err(_err) if node.last_status == NodeStatus::Good => {
                            node.last_status = NodeStatus::Unknown;
                            node.last_seen = OffsetDateTime::now_utc();
                        }
                        Err(_err) => {
                            node.last_status = NodeStatus::Bad;
                            node.last_seen = OffsetDateTime::now_utc();
                        }
                    }
                    node
                });
            }
        } else {
            // 3. if the bucket is not full run find nodes (or get peers) on the node closest to the target and insert
            // found nodes
        }
    }

    async fn insert_node(&self, node: Node) -> bool {
        let mut routing_table = self.routing_table.borrow_mut();
        // Insert node checks if it's "splittable"
        if routing_table.insert_node(node) {
            drop(routing_table);
            let routing_table_clone = Rc::clone(&self.routing_table);
            let node_id = node.id;
            // TODO: don't spawn these unconditionally since only one is relevant at a time
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
            // A bucket must exist at this point
            let (bucket_id, bucket) = routing_table.find_bucket(&node.id).unwrap();
            let our_id = routing_table.own_id;
            let mut unknown_nodes: Vec<_> = bucket
                .nodes()
                .cloned()
                .filter(|node| node.current_status() == NodeStatus::Unknown)
                .collect();
            unknown_nodes.sort_unstable_by(|a, b| a.last_seen.cmp(&b.last_seen));

            drop(bucket);
            drop(routing_table);
            'outer: loop {
                // track if all nodes are good
                let mut all_good = false;
                // Ping all unknown nodes to see if they might be replaced until either all are
                // good or a bad one is found
                for mut node in unknown_nodes
                    .iter_mut()
                    .filter(|node| node.current_status() == NodeStatus::Unknown)
                {
                    all_good = true;
                    match self.transport.ping(&our_id, &node).await {
                        Ok(_) => {
                            node.last_seen = OffsetDateTime::now_utc();
                            node.last_status = NodeStatus::Good;
                        }
                        Err(err) if err.is_timeout() && node.last_status == NodeStatus::Good => {
                            all_good = false;
                            node.last_status = NodeStatus::Unknown;
                        }
                        Err(err) if err.is_timeout() && node.last_status == NodeStatus::Unknown => {
                            node.last_status = NodeStatus::Bad;
                            break 'outer;
                        }
                        Err(_err) if node.last_status == NodeStatus::Good => {
                            all_good = false;
                            node.last_status = NodeStatus::Unknown;
                            node.last_seen = OffsetDateTime::now_utc();
                        }
                        Err(_err) => {
                            node.last_status = NodeStatus::Bad;
                            node.last_seen = OffsetDateTime::now_utc();
                            break 'outer;
                        }
                    }
                }
                if all_good {
                    break;
                }
            }

            let mut routing_table = self.routing_table.borrow_mut();
            let bucket = routing_table.get_bucket_mut(bucket_id).unwrap();
            // These buckets are very small so fine with O(N^2) here for now
            for updated_node in unknown_nodes {
                for node in bucket.nodes_mut() {
                    if updated_node.id == node.id {
                        *node = updated_node;
                    }
                }
            }
            false
        }
    }

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
                        queried_node.last_status = NodeStatus::Good;
                        reponse
                    }
                    Err(err) => {
                        log::warn!("Node {next_to_query:?} ping failed: {err}");
                        match next_to_query.last_status {
                            NodeStatus::Good => {
                                let mut routing_table = self.routing_table.borrow_mut();
                                // Unwrap is fine here since it should always exists a node with the given
                                // id in the table at this point
                                let queried_node =
                                    routing_table.get_mut(&next_to_query.id).unwrap();
                                queried_node.last_status = NodeStatus::Unknown;
                            }
                            NodeStatus::Unknown => {
                                let mut routing_table = self.routing_table.borrow_mut();
                                // Unwrap is fine here since it should always exists a node with the given
                                // id in the table at this point
                                let queried_node =
                                    routing_table.get_mut(&next_to_query.id).unwrap();
                                queried_node.last_status = NodeStatus::Bad;
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
                    if self.insert_node(node).await {
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
