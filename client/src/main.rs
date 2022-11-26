use std::{cell::RefCell, collections::BTreeMap, net::IpAddr, path::Path, rc::Rc, time::Duration};

use bittorrent::TorrentManager;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use krpc::{KrpcService, Peer};
use magnet_url::Magnet;
use node::{NodeId, ID_MAX};
use routing_table::RoutingTable;
use sha1::{Digest, Sha1};
use time::OffsetDateTime;
use trust_dns_resolver::{
    config::{ResolverConfig, ResolverOpts},
    Resolver,
};

use crate::node::{Node, ID_ZERO};

mod krpc;
mod node;
mod routing_table;

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
        last_seen: OffsetDateTime::now_utc(),
    };

    let response = service.ping(&routing_table.own_id, &node).await.unwrap();
    node.id = response.id;
    routing_table.insert_node(node);
}

async fn refresh(routing_table: &mut RoutingTable, service: &KrpcService) {
    // Find closest nodes
    // 1. look in bootstrap for self
    // 2. recursivly look for the closest node to self in the resposne
    let own_id = routing_table.own_id;
    let mut prev_min = ID_MAX;

    loop {
        log::info!("Scanning");
        let next_to_query = routing_table.get_closest(&own_id).unwrap();

        let distance = own_id.distance(&next_to_query.id);
        if distance < prev_min {
            let response = match service.find_nodes(&own_id, &own_id, next_to_query).await {
                Ok(reponse) => reponse,
                Err(err) => {
                    if err.code == 408 {
                        log::warn!("timeout for: {next_to_query:?}");
                        // TODO unnecessary clone
                        let clone = next_to_query.clone();
                        routing_table.remove(&clone).unwrap();
                        continue;
                    } else {
                        panic!("{err}");
                    }
                }
            };

            log::debug!("Got nodes from: {next_to_query:?}");
            for node in response.nodes.into_iter() {
                if routing_table.insert_node(node) {
                    log::debug!("Inserted node");
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
    // TODO: somehow this fails randomly?
    let key = *btree_map.iter().next().unwrap().0;
    btree_map.remove(&key).unwrap()
}

async fn find_peers(
    service: &KrpcService,
    routing_table: &RoutingTable,
    info_hash: &[u8],
) -> Vec<Peer> {
    /*let bytes = base32::decode(
        base32::Alphabet::RFC4648 { padding: false },
        magent_url.xt.as_ref().unwrap(),
    );*/

    let info_hash = NodeId::from(info_hash);

    let closest_node = routing_table.get_closest(&info_hash).unwrap();

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

        match response.body {
            krpc::GetPeerResponseBody::Nodes(nodes) => {
                for node in nodes.into_iter() {
                    btree_map.insert(info_hash.distance(&node.id), node);
                }
            }
            krpc::GetPeerResponseBody::Peers(peers) => {
                break peers;
            }
        }
    }
}

fn main() {
    // Might not exist
    let log_file = std::fs::File::create("log.txt").unwrap();
    let mut log_builder = env_logger::builder();
    log_builder
        .target(env_logger::Target::Pipe(Box::new(log_file)))
        .filter_level(log::LevelFilter::Info)
        .init();

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
        let progress = MultiProgress::new();
        let service = KrpcService::new("0.0.0.0:1337".parse().unwrap())
            .await
            .unwrap();

        if should_bootstrap {
            let bootstrap_progress = ProgressBar::new_spinner();
            let pb = progress.add(bootstrap_progress);
            pb.set_style(ProgressStyle::with_template("{spinner:.blue} {msg}").unwrap());
            pb.enable_steady_tick(Duration::from_millis(100));
            pb.set_message("Bootstrapping DHT...");
            bootstrap(&mut routing_table, &service, ip).await;
            pb.finish();
        } else {
            // remove if it already exists, this can probably be removed though
            std::fs::remove_file(Path::new("routing_table.json")).unwrap();
        }
        // TODO Refactor out to ui module
        let refresh_progress = progress.add(ProgressBar::new_spinner());
        refresh_progress.enable_steady_tick(Duration::from_millis(100));
        refresh_progress.set_style(ProgressStyle::with_template("{spinner:.blue} {msg}").unwrap());
        refresh_progress.set_message("Refreshing routing table...");
        refresh(&mut routing_table, &service).await;
        refresh_progress.finish_with_message("Routing table refreshed");

        routing_table.ping_all_nodes(&service, &progress).await;
        log::info!("Done with pings");

        let remaining = routing_table
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes())
            .count();
        log::info!("Remaining nodes: {remaining}");

        // Linux mint magnet link + torrent file
        //let magnet_link = "magnet:?xt=urn:btih:CS5SSRQ4EJB2UKD43JUBJCHFPSPOWJNP".to_string();
        let torrent_info = std::fs::read("linux_mint.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent_info).unwrap();

        println!("sum: {:x?}", metainfo.info().files().next().unwrap());
        let find_peer_progress = progress.add(ProgressBar::new_spinner());
        find_peer_progress.enable_steady_tick(Duration::from_millis(100));
        find_peer_progress
            .set_style(ProgressStyle::with_template("{spinner:.blue} {msg}").unwrap());
        find_peer_progress.set_message("Finding peers...");

        let peers = find_peers(
            &service,
            &routing_table,
            metainfo.info().info_hash().as_ref(),
        )
        .await;

        find_peer_progress.finish_with_message(format!("Found {} peers", peers.len()));

        let info_hash = metainfo.info().info_hash();
        let torrent_manager = TorrentManager::new(metainfo.info().clone()).await;

        let connection_progress = progress.add(ProgressBar::new_spinner());
        connection_progress
            .set_style(ProgressStyle::with_template("{spinner:.blue} {msg}").unwrap());
        connection_progress.enable_steady_tick(Duration::from_millis(100));
        let total_peers = peers.len();
        let num_success = Rc::new(RefCell::new(0));
        let num_failures = Rc::new(RefCell::new(0));

        let connect_futures = peers.into_iter().enumerate().map(|(i, peer)| {
            let num_success_clone = num_success.clone();
            let num_failures_clone = num_failures.clone();
            let manager_clone = torrent_manager.clone();
            let connection_progress = connection_progress.clone();
            tokio_uring::spawn(async move {
                {
                    let success = num_success_clone.borrow();
                    let failures = num_failures_clone.borrow();
                    connection_progress.set_message(format!(
                        "Connecting to peer [{i}/{}], Success: {success}, Failures: {failures}",
                        total_peers
                    ));
                }

                let connect_res =
                    tokio::time::timeout(Duration::from_secs(5), manager_clone.add_peer(peer.addr))
                        .await;

                match connect_res {
                    Ok(Ok(_peer_con)) => {
                        *num_success_clone.borrow_mut() += 1;
                        log::info!("Connected to {}!", peer.addr);
                    }
                    Ok(Err(err)) => {
                        *num_failures_clone.borrow_mut() += 1;
                        log::error!("Failed to connect to peer {}, error: {err}", peer.addr);
                    }
                    Err(_) => {
                        *num_failures_clone.borrow_mut() += 1;
                        log::error!("Failed to connect to peer: {:?}, timedout", peer.addr);
                    }
                }
                {
                    let success = num_success_clone.borrow();
                    let failures = num_failures_clone.borrow();
                    connection_progress.set_message(format!(
                        "Connecting to peer [{i}/{}], Success: {success}, Failures: {failures}",
                        total_peers
                    ));
                }
            })
        });

        futures::future::join_all(connect_futures).await;
        connection_progress.finish_with_message(format!(
            "Connected to {}/{} peers",
            num_success.borrow(),
            total_peers
        ));
        let torrent_manager_clone = torrent_manager.clone();
        // Try to find more peers (all logic after peers have been found and added can be moved to
        // the torrrent manager)
        tokio_uring::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                refresh(&mut routing_table, &service).await;
                let peers = find_peers(&service, &routing_table, info_hash.as_ref()).await;
                for peer in peers {
                    let result = tokio::time::timeout(
                        Duration::from_secs(5),
                        torrent_manager_clone.add_peer(peer.addr),
                    )
                    .await;

                    if let Ok(Ok(peer_connection)) = result {
                        log::info!("Peer added!");
                        let _ = peer_connection.interested();

                        let mut state = torrent_manager_clone.torrent_state.borrow_mut();
                        if state.should_unchoke() && peer_connection.unchoke().is_ok() {
                            state.num_unchoked += 1;
                            for _ in 0..10 {
                                if let Some(index) = state.next_piece() {
                                    if peer_connection.state().peer_pieces[index as usize] {
                                        // TODO handle error
                                        let _ = peer_connection
                                            .request_piece(index, state.piece_length(index));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                let mut state = torrent_manager_clone.torrent_state.borrow_mut();
                // Attempt to unchoke a random peer
                if !state.should_unchoke() {
                    if let Some(unchoked) = state
                        .peer_connections
                        .iter()
                        .find(|peer| !peer.state().is_choking)
                    {
                        let _ = unchoked.choke();
                        state.num_unchoked -= 1;
                    }
                    if let Some(choked) = state
                        .peer_connections
                        .iter()
                        .find(|peer| peer.state().is_choking)
                    {
                        let _ = choked.unchoke();
                        for _ in 0..10 {
                            if let Some(index) = state.next_piece() {
                                if choked.state().peer_pieces[index as usize] {
                                    // TODO handle error
                                    let _ = choked.request_piece(index, state.piece_length(index));
                                    break;
                                }
                            }
                        }
                        state.num_unchoked += 1;
                    }
                }
            }
        });
        let download_progress = progress.add(ProgressBar::new(
            metainfo.info().files().next().unwrap().length(),
        ));
        download_progress.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap().progress_chars("#>-"));
        let download_progress_clone = download_progress.clone();
        torrent_manager.set_subpiece_callback(move |data| {
            download_progress_clone.inc(data.len() as u64);
        });
        // TODO announce peer (add support for incoming connections)
        download_progress.tick();
        torrent_manager.start().await.unwrap();
        download_progress.finish_with_message("File dowloaded!");
    });
}
