use std::{
    cell::RefCell,
    net::{IpAddr, SocketAddr},
    path::Path,
    rc::Rc,
    time::Duration,
};

use anyhow::Context;
use sha1::{Digest, Sha1};
use slotmap::Key;
use time::OffsetDateTime;

use crate::{
    krpc::{Error, FindNodesResponse, KrpcSocket},
    node::{Node, NodeId, NodeStatus, ID_MAX, ID_ZERO},
    routing_table::{self, Bucket, BucketId, RoutingTable},
};

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

#[inline]
fn save_table(path: &Path, table: &RoutingTable) -> anyhow::Result<()> {
    let table_json = serde_json::to_string(&table)?;
    std::fs::write(path, table_json)?;
    Ok(())
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
        let routing_table = if let Some(table) = load_table(Path::new("routing_table.json")) {
            log::info!("Loading existing table");
            table
        } else {
            let node_id = generate_node_id();
            RoutingTable::new(node_id)
        };

        let dht = Dht {
            transport,
            routing_table: Rc::new(RefCell::new(routing_table)),
        };

        log::info!("Bootstrapping");
        dht.bootstrap().await?;
        log::info!("Bootstrap successful");

        Ok(dht)
    async fn save(&self, path: &Path) -> anyhow::Result<()> {
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
        let mut routing_table = self.routing_table.borrow_mut();
        let our_id = routing_table.own_id;

        let Some(bucket) = routing_table.get_bucket_mut(bucket_id) else {
            log::error!("Bucket with id {bucket_id:?} to be refreshed no longer exist");
            return;
        };
        // If the bucket has been updated since this refresh task was called a new
        // refresh task should have been spawned so this can be skipped.
        if OffsetDateTime::now_utc() - bucket.last_changed() < REFRESH_TIMEOUT {
            log::debug!("Refresh task for bucket with id {bucket_id:?} is stale, skipping refresh");
            return;
        }

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
        let mut need_refresh_scheduled = true;
        // 2. if the bucket is full (and not splittable) just ping all the nodes and update timestamps
        // (Försök pinga bara candidate noden men om det failar fortsätt pinga de andra)
        // kan göra samma när man anropar get_peers
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
        let (our_id, updated_buckets) = {
            let mut routing_table = self.routing_table.borrow_mut();
            (routing_table.own_id, routing_table.insert_node(node))
        };
        // Insert node checks if it's "splittable"
        for bucket_id in updated_buckets {
            // If the bucket was updated
            if !bucket_id.is_null() {
                self.schedule_refresh(bucket_id);
                inserted = true;
            } else {
                // A bucket must exist at this point
                let (bucket_id, mut unknown_nodes) =
                    self.find_bucket_unknown_nodes(&node.id).unwrap();

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
                        match self.transport.ping(&our_id, node).await {
                            Ok(_) => {
                                node.last_seen = OffsetDateTime::now_utc();
                                node.last_status = NodeStatus::Good;
                            }
                            Err(err)
                                if err.is_timeout() && node.last_status == NodeStatus::Good =>
                            {
                                all_good = false;
                                node.last_status = NodeStatus::Unknown;
                            }
                            Err(err)
                                if err.is_timeout() && node.last_status == NodeStatus::Unknown =>
                            {
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
                inserted = false;
            }
        }
        inserted
    }

    fn find_bucket_unknown_nodes(&self, target_id: &NodeId) -> Option<(BucketId, Vec<Node>)> {
        let mut routing_table = self.routing_table.borrow_mut();
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
