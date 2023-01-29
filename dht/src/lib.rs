use std::{
    cell::RefCell, collections::BTreeMap, net::SocketAddr, path::Path, rc::Rc, time::Duration,
};

use anyhow::Context;
use futures::future::join_all;
use krpc::{Peer, Query};
use sha1::{Digest, Sha1};
use slotmap::Key;
use time::OffsetDateTime;
use tokio::sync::Notify;

use crate::{
    krpc::{serialize_compact_nodes, FindNodesResponse, KrpcSocket},
    node::{Node, NodeId, NodeStatus, ID_MAX, ID_ZERO},
    routing_table::{BucketId, RoutingTable, BUCKET_SIZE},
};

mod krpc;
mod node;
mod routing_table;
mod token_store;

const BOOTSTRAP_NODES: [&str; 5] = [
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.bitcomet.com:6881",
    "dht.aelitis.com:6881",
    "bootstrap.jami.net:4222",
];

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


// Notes on integrating with the client:
// Dht handle that contains channel with operations (aka actor model). The handle can contain the
// receiver of the dht statistics as well

const REFRESH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

// TODO: refresh our own id occasionally as well
#[derive(Clone)]
pub struct Dht {
    transport: KrpcSocket,
    routing_table: Rc<RefCell<RoutingTable>>,
    // (This doesn't need to be multi threaded)
    node_added_notify: Rc<Notify>,
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
                node_added_notify: Rc::new(Notify::new()),
            };
            for bucket_id in bucket_ids {
                dht.schedule_refresh(bucket_id);
            }
            dht.handle_incoming();
            Ok(dht)
        } else {
            let node_id = generate_node_id();
            let routing_table = RoutingTable::new(node_id);

            let dht = Dht {
                transport,
                routing_table: Rc::new(RefCell::new(routing_table)),
                node_added_notify: Rc::new(Notify::new()),
            };

            log::info!("Bootstrapping");
            dht.handle_incoming();
            dht.bootstrap().await?;
            log::info!("Bootstrap successful");

            Ok(dht)
        }
    }

    // TODO look at the RFC overview. Can probably break out the recurisve search from this 
    // and the start method and reuse it for both.
    pub fn find_peers(&self, info_hash: &[u8]) -> tokio::sync::mpsc::Receiver<Vec<Peer>> {
        let this = self.clone();
        let (tx, rc) = tokio::sync::mpsc::channel(64);
        let info_hash = NodeId::from(info_hash);
        tokio_uring::spawn(async move {
            'outer: loop {
                if tx.is_closed() {
                    break 'outer;
                }
                let mut btree_map = BTreeMap::new();
                {
                    let mut routing_table = this.routing_table.borrow_mut();
                    let Some(closest_node) = routing_table.get_closest_mut(&info_hash) else {
                        continue;
                    };
                    btree_map.insert(info_hash.distance(&closest_node.id), *closest_node);
                }

                loop {
                    let Some((_, closest_node)) = btree_map.pop_first() else {
                        // Wait for more nodes
                        break;
                    };
                    log::debug!("Closest node: {closest_node:?}");
                    let our_id = this.routing_table.borrow().own_id;

                    let response = match this
                        .transport
                        .get_peers(&our_id, info_hash.as_bytes(), &closest_node)
                        .await
                    {
                        Ok(response) => response,
                        Err(err) => {
                            let mut routing_table = this.routing_table.borrow_mut();
                            if let Some(node) = routing_table.get_mut(&closest_node.id) {
                                match node.last_status {
                                    NodeStatus::Good => node.last_status = NodeStatus::Unknown,
                                    _ => node.last_status = NodeStatus::Bad,
                                }
                            }
                            log::error!("Failed get_peers request: {err}");
                            continue;
                        }
                    };

                    match response.body {
                        krpc::GetPeerResponseBody::Nodes(nodes) => {
                            for node in nodes.into_iter() {
                                btree_map.insert(info_hash.distance(&node.id), node);
                                log::debug!("Inserting: {node:?}");
                                this.insert_node(node).await;
                            }
                        }
                        krpc::GetPeerResponseBody::Peers(peers) => {
                            if tx.send(peers).await.is_err() {
                                break 'outer;
                            }
                        }
                    }
                }

                log::debug!("Waiting for notify more nodes");
                // Wait untill more nodes have been added
                let _ = tokio::time::timeout(
                    Duration::from_secs(30),
                    this.node_added_notify.notified(),
                )
                .await;
            }
        });
        rc
    }

    fn handle_incoming(&self) {
        let this = self.clone();
        tokio_uring::spawn(async move {
            let mut incoming_queries = this.transport.listen();
            while let Some((transaction_id, query, addr)) = incoming_queries.recv().await {
                log::debug!("Received query: {query:?}");
                let our_id = this.routing_table.borrow().own_id;
                match query {
                    Query::FindNode { id: _, target } => {
                        let target = NodeId::from(target.as_slice());
                        let closet = this
                            .routing_table
                            .borrow()
                            .get_k_closest(BUCKET_SIZE, &target);
                        log::debug!("Found: {} nodes closet to {target:?}", closet.len());
                        this.transport
                            .send_response(
                                addr,
                                krpc::KrpcPacket {
                                    t: transaction_id,
                                    y: 'r',
                                    q: None,
                                    a: None,
                                    r: Some(krpc::Response::FindNode {
                                        id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                                        nodes: serialize_compact_nodes(&closet),
                                    }),
                                    e: None,
                                },
                            )
                            .await
                            .unwrap();
                    }
                    Query::GetPeers { id: _, info_hash } => {
                        // Somehow inspect peer list and provide peers if they
                        // exist or otherwise return closest nodes. Consider having
                        // a trait that specifices the peer list interface that may be used.
                        // otherwise return find nodes response. Until then, just return find node
                        // response.
                        let target = NodeId::from(info_hash.as_slice());
                        let closet = this
                            .routing_table
                            .borrow()
                            .get_k_closest(BUCKET_SIZE, &target);
                        log::debug!("Found: {} nodes closet to {target:?}", closet.len());
                        this.transport
                            .send_response(
                                addr,
                                krpc::KrpcPacket {
                                    t: transaction_id,
                                    y: 'r',
                                    q: None,
                                    a: None,
                                    r: Some(krpc::Response::FindNode {
                                        id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                                        nodes: serialize_compact_nodes(&closet),
                                    }),
                                    e: None,
                                },
                            )
                            .await
                            .unwrap();
                    }
                    Query::AnnouncePeer {
                        id,
                        implied_port,
                        info_hash,
                        port,
                        token,
                    } => {}
                    Query::Ping { id: _ } => {
                        // Should we only respond to pings from nodes in the routing table?
                        // probably not, but some ratelimiting might be necessary in the future
                        this.transport
                            .send_response(
                                addr,
                                krpc::KrpcPacket {
                                    t: transaction_id,
                                    y: 'r',
                                    q: None,
                                    a: None,
                                    r: Some(krpc::Response::QueriedNodeId {
                                        id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                                    }),
                                    e: None,
                                },
                            )
                            .await
                            .unwrap();
                    }
                }
            }
        });
    }

    pub async fn save(&self, path: &Path) -> anyhow::Result<()> {
        log::info!("Saving table");
        let routing_table = self.routing_table.borrow();
        let table_json = serde_json::to_string(&*routing_table)?;
        std::fs::write(path, table_json)?;
        Ok(())
    }

    async fn bootstrap(&self) -> anyhow::Result<()> {
        let our_id = self.routing_table.borrow().own_id;
        log::debug!("Resolving bootstrap node addrs");
        let resolve_result = join_all(
            BOOTSTRAP_NODES
                .iter()
                .map(|node_addr| tokio_uring::spawn(tokio::net::lookup_host(node_addr))),
        )
        .await;

        log::debug!("Pinging bootstrap nodes");
        let bootstrap_ping_futures = resolve_result
            .into_iter()
            // Ignore any potential failures
            .filter_map(|result| result.map(|inner| inner.ok()).ok().flatten())
            // Pick the first addr we resolved too
            .filter_map(|mut node_addrs| node_addrs.next())
            .map(|addr| Node {
                id: ID_ZERO,
                addr,
                last_seen: OffsetDateTime::now_utc(),
                last_status: NodeStatus::Unknown,
            })
            .map(|node| async move {
                log::debug!("Pinging {}", node.addr);
                let result = self.transport.ping(&our_id, &node).await;
                (node, result)
            });

        let mut any_success = false;
        for (mut node, result) in join_all(bootstrap_ping_futures).await {
            if let Ok(pong) = result {
                node.last_seen = OffsetDateTime::now_utc();
                node.last_status = NodeStatus::Good;
                node.id = pong.id;
                log::debug!("Node {} responded", node.addr);
                assert!(self.insert_node(node).await);
                any_success = true;
            }
        }

        if !any_success {
            anyhow::bail!("Bootstrap failed, node not responsive");
        } else {
            Ok(())
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

            while unknown_nodes
                .iter()
                .any(|node| node.current_status() == NodeStatus::Unknown)
            {
                // Ping all unknown nodes to see if they might be replaced until either all are
                // good or a bad one is found
                for mut unknown_node in unknown_nodes
                    .iter_mut()
                    .filter(|node| node.current_status() == NodeStatus::Unknown)
                {
                    match self.transport.ping(&our_id, unknown_node).await {
                        Ok(_) => {
                            unknown_node.last_seen = OffsetDateTime::now_utc();
                            unknown_node.last_status = NodeStatus::Good;
                        }
                        Err(err)
                            if err.is_timeout() && unknown_node.last_status == NodeStatus::Good =>
                        {
                            unknown_node.last_status = NodeStatus::Unknown;
                        }
                        Err(err)
                            if err.is_timeout()
                                && unknown_node.last_status == NodeStatus::Unknown =>
                        {
                            unknown_node.last_status = NodeStatus::Bad;
                        }
                        Err(_err) if unknown_node.last_status == NodeStatus::Good => {
                            unknown_node.last_status = NodeStatus::Unknown;
                            unknown_node.last_seen = OffsetDateTime::now_utc();
                        }
                        Err(_err) => {
                            unknown_node.last_status = NodeStatus::Bad;
                            unknown_node.last_seen = OffsetDateTime::now_utc();
                        }
                    }
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
                        // We only inserted if we found a bad node
                        inserted = true;
                        // Overwrite the bad one
                        *current_node = node;
                    }
                }
            }
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
        if inserted {
            self.node_added_notify.notify_one();
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
                        log::warn!("{next_to_query:?} ping failed: {err}");
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