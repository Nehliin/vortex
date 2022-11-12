use std::{collections::BTreeMap, net::IpAddr, path::Path, time::Duration};

use bittorrent::TorrentManager;
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
    // TODO: somehow this fails randomly?
    let key = *btree_map.iter().next().unwrap().0;
    btree_map.remove(&key).unwrap()
}

async fn find_peers(
    service: &KrpcService,
    routing_table: &RoutingTable,
    magnet_link: String,
) -> Vec<Peer> {
    let magent_url = Magnet::new(&magnet_link).unwrap();

    let bytes = base32::decode(
        base32::Alphabet::RFC4648 { padding: false },
        magent_url.xt.as_ref().unwrap(),
    );

    let info_hash = NodeId::from(bytes.unwrap().as_slice());

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
                dbg!(&peers);
                break peers;
            }
        }
    }
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
            .flat_map(|bucket| bucket.nodes())
            .count();
        log::info!("remaining: {remaining}");

        // Linux mint magnet link + torrent file
        let magnet_link = "magnet:?xt=urn:btih:CS5SSRQ4EJB2UKD43JUBJCHFPSPOWJNP".to_string();
        let torrent_info = std::fs::read("linux_mint.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent_info).unwrap();

        let torrent_manager = TorrentManager::new(metainfo.info().clone(), 5);

        let peers = find_peers(&service, &routing_table, magnet_link).await;

        for peer in peers.into_iter() {
            let connect_res =
                tokio::time::timeout(Duration::from_secs(3), torrent_manager.add_peer(peer.addr))
                    .await;

            match connect_res {
                Ok(()) => {
                    log::info!("Connected to {}!", peer.addr);
                }
                Err(err) => log::error!("Failed to connect to peer: {:?}, error: {err}", peer.addr),
            }
        }
        // 1. announce peer (måste support inkommande connections då tillslut)
        // 2. välj X random pieces och hitta peers som har den o börja ladda ner genom intrest +
        //    unchoke + request
         
        // 3. (gå över till rarest)

    });
}
