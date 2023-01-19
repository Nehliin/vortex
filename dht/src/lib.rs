use std::{cell::RefCell, net::SocketAddr, path::Path, rc::Rc, time::Duration};

use anyhow::Context;
use sha1::{Digest, Sha1};
use slotmap::Key;
use time::OffsetDateTime;

use crate::{
    krpc::{FindNodesResponse, KrpcSocket},
    node::{Node, NodeId, NodeStatus, ID_MAX, ID_ZERO},
    routing_table::{BucketId, RoutingTable},
};

mod krpc;
mod node;
mod routing_table;

// Bootstrap nodes
//dht.transmissionbt.com 6881
//router.bittorrent.com  6881
//router.bitcomet.com    6881
//dht.aelitis.com        6881
//bootstrap.jami.net     4222

const BOOTSTRAP: &str = "router.bittorrent.com:6881";

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

const REFRESH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

// TODO: refresh our own id occasionally as well
#[derive(Clone)]
pub struct Dht {
    transport: KrpcSocket,
    routing_table: Rc<RefCell<RoutingTable>>,
}

impl Dht {
    pub async fn new(bind_addr: SocketAddr) -> anyhow::Result<Self> {
        let transport = KrpcSocket::new(bind_addr).await?;
        if let Some(table) = load_table(Path::new("routing_table.json")) {
            log::info!("Loading existing table");
            let bucket_ids: Vec<_> = table.bucket_ids().collect();
            let dht = Dht {
                transport,
                routing_table: Rc::new(RefCell::new(table)),
            };
            for bucket_id in bucket_ids {
                dht.schedule_refresh(bucket_id);
            }
            Ok(dht)
        } else {
            let node_id = generate_node_id();
            let routing_table = RoutingTable::new(node_id);

            let dht = Dht {
                transport,
                routing_table: Rc::new(RefCell::new(routing_table)),
            };

            log::info!("Bootstrapping");
            dht.bootstrap().await?;
            log::info!("Bootstrap successful");

            Ok(dht)
        }
    }

    pub async fn save(&self, path: &Path) -> anyhow::Result<()> {
        log::info!("Saving table");
        let routing_table = self.routing_table.borrow();
        let table_json = serde_json::to_string(&*routing_table)?;
        std::fs::write(path, table_json)?;
        Ok(())
    }

    async fn bootstrap(&self) -> anyhow::Result<()> {
        let mut addrs = tokio::net::lookup_host(BOOTSTRAP).await?;
        let addr = addrs.next().context("Bootstap failed, no nodes found")?;
        log::debug!("Found bootstrap node addr: {addr}");
        let mut node = Node {
            id: ID_ZERO,
            addr,
            last_seen: OffsetDateTime::now_utc(),
            last_status: NodeStatus::Unknown,
        };

        let our_id = self.routing_table.borrow().own_id;

        if let Ok(pong) = self.transport.ping(&our_id, &node).await {
            node.last_seen = OffsetDateTime::now_utc();
            node.last_status = NodeStatus::Good;
            node.id = pong.id;
            assert!(self.insert_node(node).await);
            Ok(())
        } else {
            anyhow::bail!("Bootstrap failed, node not responsive");
        }
    }

    // TODO: update last refreshed timestamp
    // also remember to mark as changed after pings and stuff
    async fn refresh_bucket(&self, bucket_id: BucketId) {
        let (is_full, mut candiate, our_id, id) = {
            let mut routing_table = self.routing_table.borrow_mut();
            let our_id = routing_table.own_id;

            let Some(bucket) = routing_table.get_bucket_mut(bucket_id) else {
                log::error!("Bucket with id {bucket_id:?} to be refreshed no longer exist");
                return;
            };
            // If the bucket has been updated since this refresh task was called a new
            // refresh task should have been spawned so this can be skipped.
            if OffsetDateTime::now_utc() - bucket.last_changed() < REFRESH_TIMEOUT {
                log::debug!(
                    "Refresh task for bucket with id {bucket_id:?} is stale, skipping refresh"
                );
                return;
            }

            log::debug!("Refreshing bucket: {bucket_id:?}");
            bucket.update_last_changed();

            // 1. Generate random id within bucket range
            let id = bucket.random_id();

            let is_full = !bucket.covers(&our_id) && bucket.is_full();

            let Some(candiate) = bucket
                .nodes_mut()
                .filter(|node| node.last_status != NodeStatus::Bad)
                .min_by_key(|node| id.distance(&node.id)) else {
                // TODO: Remove bucket
                log::error!("No nodes left in bucket, refresh failed");
                return;
            };
            (is_full, *candiate, our_id, id)
        };

        let mut need_refresh_scheduled = true;
        // 2. if the bucket is full (and not splittable) ping the node and update the timestamp
        if is_full {
            match self.transport.ping(&our_id, &candiate).await {
                Ok(_) => {
                    candiate.last_seen = OffsetDateTime::now_utc();
                    candiate.last_status = NodeStatus::Good;
                }
                Err(err) if err.is_timeout() && candiate.last_status == NodeStatus::Good => {
                    candiate.last_status = NodeStatus::Unknown;
                }
                Err(err) if err.is_timeout() && candiate.last_status == NodeStatus::Unknown => {
                    candiate.last_status = NodeStatus::Bad;
                }
                Err(_err) if candiate.last_status == NodeStatus::Good => {
                    candiate.last_status = NodeStatus::Unknown;
                    candiate.last_seen = OffsetDateTime::now_utc();
                }
                Err(_err) => {
                    candiate.last_status = NodeStatus::Bad;
                    candiate.last_seen = OffsetDateTime::now_utc();
                }
            }
            // Need to reinsert the node, theoretically the node or bucket
            // might have been removed when attepting the ping
            if let Some(node) = self.routing_table.borrow_mut().get_mut(&candiate.id) {
                *node = candiate;
            }
        } else {
            // 3. if the bucket is not full run find nodes (or get peers) on the node closest to the target and insert
            // found nodes (TODO: See if get_peers is more widely supported and perhaps use that
            // instead)
            match self.transport.find_nodes(&our_id, &id, &candiate).await {
                Ok(FindNodesResponse { id: _, nodes }) => {
                    candiate.last_seen = OffsetDateTime::now_utc();
                    candiate.last_status = NodeStatus::Good;
                    for node in nodes {
                        if self.insert_node(node).await {
                            // Insert will schedule refreshes on it's own
                            need_refresh_scheduled = false;
                            log::debug!("Refreshed bucket found new node: {:?}", node.id);
                        }
                    }
                }
                Err(err) if err.is_timeout() && candiate.last_status == NodeStatus::Good => {
                    candiate.last_status = NodeStatus::Unknown;
                }
                Err(err) if err.is_timeout() && candiate.last_status == NodeStatus::Unknown => {
                    candiate.last_status = NodeStatus::Bad;
                }
                Err(_err) if candiate.last_status == NodeStatus::Good => {
                    candiate.last_status = NodeStatus::Unknown;
                    candiate.last_seen = OffsetDateTime::now_utc();
                }
                Err(_err) => {
                    candiate.last_status = NodeStatus::Bad;
                    candiate.last_seen = OffsetDateTime::now_utc();
                }
            }
        }
        if need_refresh_scheduled {
            // Schedule new refresh later on
            self.schedule_refresh(bucket_id);
        }
    }

    fn schedule_refresh(&self, bucket_id: BucketId) {
        assert!(!bucket_id.is_null());
        let this = self.clone();
        // TODO: don't spawn these unconditionally since only one is relevant at a time
        tokio_uring::spawn(async move {
            log::debug!("Spawning refresh task for bucket with id {bucket_id:?}");
            tokio::time::sleep(REFRESH_TIMEOUT).await;
            this.refresh_bucket(bucket_id).await;
        });
    }

    async fn insert_node(&self, node: Node) -> bool {
        let mut inserted = false;
        let (our_id, updated_buckets @ [bucket_id_one, bucket_id_two]) = {
            let mut routing_table = self.routing_table.borrow_mut();
            (routing_table.own_id, routing_table.insert_node(node))
        };

        let failed_insert = bucket_id_one.is_null() && bucket_id_two.is_null();

        if failed_insert {
            // A bucket must exist at this point
            let (bucket_id, mut unknown_nodes) = self.find_bucket_unknown_nodes(&node.id).unwrap();

            // track if all nodes are good
            let mut all_good = false;
            'outer: loop {
                // Ping all unknown nodes to see if they might be replaced until either all are
                // good or a bad one is found
                for mut unknown_node in unknown_nodes
                    .iter_mut()
                    .filter(|node| node.current_status() == NodeStatus::Unknown)
                {
                    all_good = true;
                    match self.transport.ping(&our_id, unknown_node).await {
                        Ok(_) => {
                            unknown_node.last_seen = OffsetDateTime::now_utc();
                            unknown_node.last_status = NodeStatus::Good;
                        }
                        Err(err)
                            if err.is_timeout() && unknown_node.last_status == NodeStatus::Good =>
                        {
                            all_good = false;
                            unknown_node.last_status = NodeStatus::Unknown;
                        }
                        Err(err)
                            if err.is_timeout()
                                && unknown_node.last_status == NodeStatus::Unknown =>
                        {
                            unknown_node.last_status = NodeStatus::Bad;
                            break 'outer;
                        }
                        Err(_err) if unknown_node.last_status == NodeStatus::Good => {
                            all_good = false;
                            unknown_node.last_status = NodeStatus::Unknown;
                            unknown_node.last_seen = OffsetDateTime::now_utc();
                        }
                        Err(_err) => {
                            unknown_node.last_status = NodeStatus::Bad;
                            unknown_node.last_seen = OffsetDateTime::now_utc();
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
                for current_node in bucket.nodes_mut() {
                    if updated_node.id == current_node.id {
                        *current_node = updated_node;
                    }
                    if current_node.last_status == NodeStatus::Bad {
                        // Overwrite the bad one
                        *current_node = node;
                    }
                }
            }
            // We only inserted if we found a bad node
            inserted = !all_good;
            if inserted {
                // Need to manually update last changed since
                // the node wasn't written via insert_node
                bucket.update_last_changed();
                self.schedule_refresh(bucket_id);
            }
        } else {
            // Insert node checks if it's "splittable"
            for bucket_id in updated_buckets {
                // If the bucket was updated
                if !bucket_id.is_null() {
                    self.schedule_refresh(bucket_id);
                    inserted = true;
                }
            }
        }
        inserted
    }

    fn find_bucket_unknown_nodes(&self, target_id: &NodeId) -> Option<(BucketId, Vec<Node>)> {
        let routing_table = self.routing_table.borrow_mut();
        let (bucket_id, bucket) = routing_table.find_bucket(target_id)?;
        let mut unknown_nodes: Vec<_> = bucket
            .nodes()
            .cloned()
            .filter(|node| node.current_status() == NodeStatus::Unknown)
            .collect();
        unknown_nodes.sort_unstable_by(|a, b| a.last_seen.cmp(&b.last_seen));
        Some((bucket_id, unknown_nodes))
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

#[test]
fn test() {
    let mut log_builder = env_logger::builder();
    log_builder.filter_level(log::LevelFilter::Debug).init();
    tokio_uring::start(async {
        let dht = Dht::new("0.0.0.0:1337".parse().unwrap()).await.unwrap();
        dht.start().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    });
}
