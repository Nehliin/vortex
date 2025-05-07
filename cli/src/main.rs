use std::{
    net::SocketAddrV4,
    path::PathBuf,
    sync::mpsc::Sender,
    time::{Duration, Instant},
};

use mainline::{Dht, Id};
use metrics_exporter_prometheus::PrometheusBuilder;
use vortex_bittorrent::{Torrent, generate_peer_id};

struct TorrentPeerList {
    sender: Sender<SocketAddrV4>,
    // TODO: need to remove stuff when disconnecting
    ip_list: ahash::HashSet<SocketAddrV4>,
}

impl TorrentPeerList {
    fn insert_peer(&mut self, peer: SocketAddrV4) {
        if self.ip_list.contains(&peer) {
            return;
        }
        self.ip_list.insert(peer);
        self.sender.send(peer).unwrap();
    }
}

fn main() {
    let mut log_builder = env_logger::builder();
    log_builder.filter_level(log::LevelFilter::Info).init();

    let builder = PrometheusBuilder::new();
    builder.install().unwrap();
    // TODO Should start dht first
    let torrent_info =
        lava_torrent::torrent::v1::Torrent::read_from_file("linux-mint.torrent").unwrap();
    let (tx, rc) = std::sync::mpsc::channel();

    let info_hash = torrent_info.info_hash_bytes();
    let info_hash = Id::from_bytes(&info_hash).unwrap();

    std::thread::spawn(move || {
        let mut builder = Dht::builder();
        let mut torrent_peer_list = TorrentPeerList {
            sender: tx,
            ip_list: Default::default(),
        };
        let dht_boostrap_nodes = PathBuf::from("dht_boostrap_nodes");
        if dht_boostrap_nodes.exists() {
            let list = std::fs::read_to_string(&dht_boostrap_nodes).unwrap();
            let cached_nodes: Vec<String> = list.lines().map(|line| line.to_string()).collect();
            builder.extra_bootstrap(&cached_nodes);
        }

        let dht_client = builder.build().unwrap();
        log::info!("Bootstrapping!");
        dht_client.bootstrapped();
        log::info!("Bootstrapping done!");
        dht_client.announce_peer(info_hash, None).unwrap();

        loop {
            // query
            let all_peers = dht_client.get_peers(info_hash);
            for peers in all_peers {
                log::info!("Got {} peers", peers.len());
                for peer in peers {
                    torrent_peer_list.insert_peer(peer);
                }
            }
            let bootstrap_nodes = dht_client.to_bootstrap();
            let dht_bootstrap_nodes_contet = bootstrap_nodes.join("\n");
            std::fs::write("dht_boostrap_nodes", dht_bootstrap_nodes_contet.as_bytes()).unwrap();
            std::thread::sleep(Duration::from_secs(30));
        }
    });

    let id = generate_peer_id();
    let torrrent = Torrent::new(torrent_info, id);
    let start_time = Instant::now();
    torrrent.start(rc, "downloaded").unwrap();
    log::info!("DOWNLOADED in {}", start_time.elapsed().as_secs());
}
