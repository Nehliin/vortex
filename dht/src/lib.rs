use std::{
    cell::RefCell,
    net::{IpAddr, SocketAddr},
    path::Path,
    rc::Rc,
    time::Duration,
};

use anyhow::Context;
use futures::future::join_all;
use krpc::{
    protocol::{FindNodesResponse, GetPeers, GetPeersResponseBody, Ping},
    setup_krpc, KrpcClient, KrpcServer,
};
use sha1::{Digest, Sha1};
use slotmap::Key;
use time::OffsetDateTime;
use token_store::TokenStore;
use tokio::sync::Notify;

use crate::{
    krpc::protocol::{serialize_compact_peers, AnnouncePeer, Answer, FindNodes, Query},
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

pub trait PeerProvider {
    fn get_peers(&self, info_hash: [u8; 20]) -> Option<Vec<SocketAddr>>;
    fn insert_peer(&self, info_hash: [u8; 20], peer: SocketAddr);
}

// Notes on integrating with the client:
// Dht handle that contains channel with operations (aka actor model). The handle can contain the
// receiver of the dht statistics as well

const REFRESH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

// TODO: refresh our own id occasionally as well
#[derive(Clone)]
pub struct Dht {
    krpc_client: KrpcClient,
    krpc_server: KrpcServer,
    routing_table: Rc<RefCell<RoutingTable>>,
    token_store: TokenStore,
    port: u16,
    // (This doesn't need to be multi threaded)
    node_added_notify: Rc<Notify>,
}

impl Dht {
    pub async fn new(
        bind_addr: SocketAddr,
        peer_provider: impl PeerProvider + 'static,
    ) -> anyhow::Result<Self> {
        let port = bind_addr.port();
        let (client, server) = setup_krpc(bind_addr).await?;
        if let Some(table) = load_table(Path::new("routing_table.json")) {
            log::info!("Loading existing table");
            let bucket_ids: Vec<_> = table.bucket_ids().collect();
            let dht = Dht {
                krpc_client: client,
                krpc_server: server,
                routing_table: Rc::new(RefCell::new(table)),
                node_added_notify: Rc::new(Notify::new()),
                token_store: TokenStore::new(),
                port,
            };
            for bucket_id in bucket_ids {
                dht.schedule_refresh(bucket_id);
            }
            dht.handle_incoming(peer_provider);
            Ok(dht)
        } else {
            let node_id = generate_node_id();
            let routing_table = RoutingTable::new(node_id);

            let dht = Dht {
                krpc_client: client,
                krpc_server: server,
                routing_table: Rc::new(RefCell::new(routing_table)),
                node_added_notify: Rc::new(Notify::new()),
                token_store: TokenStore::new(),
                port,
            };

            log::info!("Bootstrapping");
            dht.handle_incoming(peer_provider);
            dht.bootstrap().await?;
            log::info!("Bootstrap successful");

            Ok(dht)
        }
    }

    pub fn find_peers(&self, info_hash: &[u8]) -> tokio::sync::mpsc::Receiver<Vec<SocketAddr>> {
        // Search may take the sender as optional argument
        // 1  search for nodes close to target
        // 2. get k_closest
        // 3. get_peers
        // 4. announce to those (ensure token exists)
        // 3. periodically start from 1 again
        let this = self.clone();
        let (tx, rc) = tokio::sync::mpsc::channel(64);
        let info_hash = NodeId::from(info_hash);
        tokio_uring::spawn(async move {
            while !tx.is_closed() {
                log::debug!("Start search for peers");
                this.search(info_hash, true).await.unwrap();
                let nodes = this
                    .routing_table
                    .borrow()
                    .get_k_closest(BUCKET_SIZE, &info_hash);
                this.get_peers_from_nodes(&info_hash, &nodes, tx.clone())
                    .await
                    .unwrap();
                let own_id = this.routing_table.borrow().own_id;
                // Need to update the closest nodes here again since the search
                // might have repalced some of the nodes initially closest
                let nodes = this
                    .routing_table
                    .borrow()
                    .get_k_closest(BUCKET_SIZE, &info_hash);
                for node in nodes {
                    if let IpAddr::V4(addr) = node.addr.ip() {
                        if let Some(token) = this.token_store.get_token(addr) {
                            if this
                                .krpc_client
                                .announce_peer(AnnouncePeer {
                                    id: own_id,
                                    info_hash: *info_hash,
                                    implied_port: true,
                                    port: this.port,
                                    token: serde_bytes::ByteBuf::from(token),
                                })
                                .with_timeout(Duration::from_secs(3))
                                .send(node.addr)
                                .await
                                .is_err()
                            {
                                log::error!("Announce failed!");
                            }
                        } else {
                            log::warn!("Token not found for: {addr}");
                        }
                    } else {
                        panic!("Tokens may only be stored for nodes with Ipv4 addrs.");
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

    fn handle_incoming(&self, peer_provider: impl PeerProvider + 'static) {
        let this = self.clone();
        self.krpc_server.serve(move |mut addr, query| {
            log::debug!("Received query: {query:?}");
            let our_id = this.routing_table.borrow().own_id;
            let ip = match addr.ip() {
                IpAddr::V4(ip) => ip,
                IpAddr::V6(_) => {
                    log::error!("Ip v6 addresses aren't supported for token generation");
                    return Err(krpc::error::KrpcError::generic(
                        "Ip v6 addresses aren't supported for token generation".to_owned(),
                    ));
                }
            };
            match query {
                Query::FindNode { id: _, target } => {
                    let target = NodeId::from(target.as_slice());
                    let closet = this
                        .routing_table
                        .borrow()
                        .get_k_closest(BUCKET_SIZE, &target);
                    log::debug!("Found: {} nodes closet to {target:?}", closet.len());
                    Ok(Answer::FindNode {
                        id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                        // TODO: this shouldn't be done here
                        nodes: krpc::protocol::serialize_compact_nodes(&closet),
                    })
                }
                Query::GetPeers { id: _, info_hash } => {
                    if let Ok(info_hash) = info_hash.as_slice().try_into() {
                        if let Some(peers) = peer_provider.get_peers(info_hash) {
                            Ok(Answer::GetPeers {
                                id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                                token: serde_bytes::ByteBuf::from(
                                    this.token_store.generate(ip).to_vec(),
                                ),
                                values: Some(serialize_compact_peers(&peers)),
                                nodes: None,
                            })
                        } else {
                            let target = NodeId::from(info_hash.as_slice());
                            let closet = this
                                .routing_table
                                .borrow()
                                .get_k_closest(BUCKET_SIZE, &target);
                            log::debug!("Found: {} nodes closet to {target:?}", closet.len());

                            Ok(Answer::GetPeers {
                                id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                                token: serde_bytes::ByteBuf::from(
                                    this.token_store.generate(ip).to_vec(),
                                ),
                                values: None,
                                nodes: Some(krpc::protocol::serialize_compact_nodes(&closet)),
                            })
                        }
                    } else {
                        Err(krpc::error::KrpcError::protocol(
                            "Invalid infohash".to_owned(),
                        ))
                    }
                }
                Query::AnnouncePeer {
                    id: _,
                    implied_port,
                    info_hash,
                    port,
                    token,
                } => {
                    if this
                        .token_store
                        .validate(ip, bytes::Bytes::copy_from_slice(&token))
                    {
                        log::info!("Recived valid announce peer request");
                        if !implied_port {
                            addr.set_port(port);
                        }
                        if let Ok(info_hash) = info_hash.as_slice().try_into() {
                            peer_provider.insert_peer(info_hash, addr);
                            Ok(Answer::QueriedNodeId {
                                id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                            })
                        } else {
                            Err(krpc::error::KrpcError::protocol(
                                "Invalid infohash".to_owned(),
                            ))
                        }
                    } else {
                        Err(krpc::error::KrpcError::protocol("Invalid token".to_owned()))
                    }
                }
                Query::Ping { id: _ } => {
                    // Should we only respond to pings from nodes in the routing table?
                    // probably not, but some ratelimiting might be necessary in the future
                    Ok(Answer::QueriedNodeId {
                        id: serde_bytes::ByteBuf::from(our_id.as_bytes()),
                    })
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
                let result = self
                    .krpc_client
                    .ping(Ping { id: our_id })
                    .send(node.addr)
                    .await;
                (node, result)
            });

        let mut any_success = false;
        for (mut node, result) in join_all(bootstrap_ping_futures).await {
            if let Ok(pong) = result {
                node.last_seen = OffsetDateTime::now_utc();
                node.last_status = NodeStatus::Good;
                node.id = pong.id;
                log::debug!("Node {} responded", node.addr);
                assert!(self.insert_node(node, None).await);
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
            (is_full, candiate.clone(), our_id, id)
        };

        let mut need_refresh_scheduled = true;
        // 2. if the bucket is full (and not splittable) ping the node and update the timestamp
        if is_full {
            match self
                .krpc_client
                .ping(Ping { id: our_id })
                .with_timeout(Duration::from_secs(3))
                .send(candiate.addr)
                .await
            {
                Ok(_) => {
                    candiate.last_seen = OffsetDateTime::now_utc();
                    candiate.last_status = NodeStatus::Good;
                }
                Err(krpc::error::Error::Timeout(_)) if candiate.last_status == NodeStatus::Good => {
                    candiate.last_status = NodeStatus::Unknown;
                }
                Err(krpc::error::Error::Timeout(_))
                    if candiate.last_status == NodeStatus::Unknown =>
                {
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
            match self
                .krpc_client
                .find_nodes(FindNodes {
                    id: our_id,
                    target: id,
                })
                .with_timeout(Duration::from_secs(3))
                .send(candiate.addr)
                .await
            {
                Ok(FindNodesResponse { id: _, nodes }) => {
                    candiate.last_seen = OffsetDateTime::now_utc();
                    candiate.last_status = NodeStatus::Good;
                    // Need to reinsert the node, theoretically the node or bucket
                    // might have been removed when attepting the ping
                    if let Some(node) = self.routing_table.borrow_mut().get_mut(&candiate.id) {
                        *node = candiate;
                    }
                    for node in nodes {
                        if self.insert_node(node.clone(), None).await {
                            // Insert will schedule refreshes on it's own
                            need_refresh_scheduled = false;
                            log::debug!("Refreshed bucket found new node: {:?}", node.id);
                        }
                    }
                }
                Err(err) => {
                    match (err, candiate.last_status) {
                        (krpc::error::Error::Timeout(_), NodeStatus::Good) => {
                            candiate.last_status = NodeStatus::Unknown;
                        }
                        (krpc::error::Error::Timeout(_), NodeStatus::Unknown) => {
                            candiate.last_status = NodeStatus::Bad;
                        }
                        (krpc::error::Error::IoError(err), _) => {
                            log::warn!("Socket failure: {err}");
                        }
                        // TODO: Perhaps not something that should be done for all errors?
                        (_, NodeStatus::Good) => {
                            candiate.last_status = NodeStatus::Unknown;
                            candiate.last_seen = OffsetDateTime::now_utc();
                        }
                        (_, _) => {
                            candiate.last_status = NodeStatus::Bad;
                            // Not accurate for timeouts
                            candiate.last_seen = OffsetDateTime::now_utc();
                        }
                    }
                    // Need to reinsert the node, theoretically the node or bucket
                    // might have been removed when attepting the ping
                    if let Some(node) = self.routing_table.borrow_mut().get_mut(&candiate.id) {
                        *node = candiate;
                    }
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

    async fn insert_node(&self, node: Node, target_id: Option<NodeId>) -> bool {
        let mut inserted = false;
        let (our_id, updated_buckets @ [bucket_id_one, bucket_id_two]) = {
            let mut routing_table = self.routing_table.borrow_mut();
            (
                routing_table.own_id,
                routing_table.insert_node(node.clone()),
            )
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
                    match self
                        .krpc_client
                        .ping(Ping { id: our_id })
                        .with_timeout(Duration::from_secs(3))
                        .send(unknown_node.addr)
                        .await
                    {
                        Ok(_) => {
                            unknown_node.last_seen = OffsetDateTime::now_utc();
                            unknown_node.last_status = NodeStatus::Good;
                        }
                        Err(krpc::error::Error::Timeout(_))
                            if unknown_node.last_status == NodeStatus::Good =>
                        {
                            unknown_node.last_status = NodeStatus::Unknown;
                        }
                        Err(krpc::error::Error::Timeout(_))
                            if unknown_node.last_status == NodeStatus::Unknown =>
                        {
                            // TODO: Early return?
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
                        *current_node = updated_node.clone();
                    }
                    if current_node.last_status == NodeStatus::Bad && !inserted {
                        // We only insert once if we found a bad node
                        inserted = true;
                        // Overwrite the bad one
                        *current_node = node.clone();
                    }
                }
            }
            if inserted {
                // Need to manually update last changed since
                // the node wasn't written via insert_node
                bucket.update_last_changed();
                self.schedule_refresh(bucket_id);
            } else if let Some(target_id) = target_id {
                // Find the node in the bucket that is furthest away from the target
                // (Unwrap is fine since the bucket must be full at this point so the iterator can't
                // be empty)
                let furthest_away = bucket
                    .nodes_mut()
                    .max_by_key(|node| node.id.distance(&target_id))
                    .unwrap();
                *furthest_away = node;
                inserted = true;
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

    // Recursivly searches the routing table for the closest nodes to the target
    async fn search(&self, target: NodeId, force_insert: bool) -> anyhow::Result<()> {
        let mut prev_min = ID_MAX;
        let own_id = self.routing_table.borrow().own_id;
        loop {
            log::info!("Searching for: {target:?}");
            let next_to_query = self
                .routing_table
                .borrow_mut()
                .get_closest_mut(&target)
                .context("No nodes in routing table")?
                .clone();

            let distance = target.distance(&next_to_query.id);
            if distance < prev_min {
                let response = match self
                    .krpc_client
                    .find_nodes(FindNodes { id: own_id, target })
                    .send(next_to_query.addr)
                    .await
                {
                    Ok(reponse) => {
                        let mut routing_table = self.routing_table.borrow_mut();
                        // Unwrap is fine here since it should always exists a node with the given
                        // id in the table at this point
                        let queried_node = routing_table.get_mut(&next_to_query.id).unwrap();
                        queried_node.last_status = NodeStatus::Good;
                        queried_node.last_seen = OffsetDateTime::now_utc();
                        reponse
                    }
                    Err(err) => {
                        // TODO: Update last_seen
                        log::warn!("{next_to_query:?} find nodes query failed: {err}");
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
                                log::debug!("Setting status of {next_to_query:?} to bad");
                            }
                            NodeStatus::Bad => {
                                log::debug!("Removing {next_to_query:?} from table");
                                self.routing_table.borrow_mut().remove(&next_to_query)?;
                            }
                        }
                        continue;
                    }
                };
                log::debug!("Got nodes from: {next_to_query:?}");
                for node in response.nodes {
                    let target = force_insert.then_some(target);
                    if self.insert_node(node, target).await {
                        log::debug!("Inserted node");
                    } else {
                        log::debug!("Did not insert node because of full routing table");
                    }
                }
                prev_min = distance;
            } else {
                break;
            }
        }
        Ok(())
    }

    // Expected to be called after a search has been made
    async fn get_peers_from_nodes(
        &self,
        target: &NodeId,
        nodes: &[Node],
        peer_listener: tokio::sync::mpsc::Sender<Vec<SocketAddr>>,
    ) -> anyhow::Result<()> {
        let own_id = self.routing_table.borrow().own_id;

        // TODO: These nodes might be replaced during the peer search
        // Either update this dynamically each iteration or filter out
        // stale nodes
        for node in nodes {
            let response = match self
                .krpc_client
                .get_peers(GetPeers {
                    id: own_id,
                    info_hash: target.as_bytes(),
                })
                .with_timeout(Duration::from_secs(3))
                .send(node.addr)
                .await
            {
                Ok(reponse) => {
                    let mut routing_table = self.routing_table.borrow_mut();
                    if let Some(queried_node) = routing_table.get_mut(&node.id) {
                        queried_node.last_status = NodeStatus::Good;
                    }
                    reponse
                }
                Err(err) => {
                    log::warn!("{node:?} ping failed: {err}");
                    match node.last_status {
                        NodeStatus::Good => {
                            let mut routing_table = self.routing_table.borrow_mut();
                            if let Some(queried_node) = routing_table.get_mut(&node.id) {
                                queried_node.last_status = NodeStatus::Unknown;
                            }
                        }
                        NodeStatus::Unknown => {
                            let mut routing_table = self.routing_table.borrow_mut();
                            // Unwrap is fine here since it should always exists a node with the given
                            // id in the table at this point
                            if let Some(queried_node) = routing_table.get_mut(&node.id) {
                                queried_node.last_status = NodeStatus::Bad;
                            }
                        }
                        NodeStatus::Bad => {
                            let _ = self.routing_table.borrow_mut().remove(node);
                        }
                    }
                    continue;
                }
            };
            if let IpAddr::V4(addr) = node.addr.ip() {
                self.token_store
                    .store_token(addr, response.token.into_vec().into());
            } else {
                log::error!("Tokens may only be stored for nodes with Ipv4 addrs.");
            }

            match response.body {
                GetPeersResponseBody::Nodes(nodes) => {
                    log::debug!("Got nodes from: {node:?}");
                    for node in nodes.into_iter() {
                        assert!(self.insert_node(node, Some(*target)).await);
                        log::debug!("Inserted node");
                    }
                }
                GetPeersResponseBody::Peers(peers) => {
                    log::info!("Got peers! ({})", peers.len());
                    if peer_listener.send(peers).await.is_err() {
                        log::debug!("Peer listener disconnected");
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let own_id = self.routing_table.borrow().own_id;
        self.search(own_id, false).await
    }
}
