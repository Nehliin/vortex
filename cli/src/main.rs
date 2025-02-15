use std::{
    net::{SocketAddr, SocketAddrV4},
    path::Path,
    sync::{mpsc::Sender, Arc},
    time::Instant,
};

use parking_lot::Mutex;
use vortex_bittorrent::{generate_peer_id, Torrent};
//use vortex_bittorrent::{PeerListHandle, TorrentManager};
use vortex_dht::PeerProvider;

struct TorrentPeerList {
    sender: Sender<SocketAddrV4>,
    // TODO: need to remove stuff when disconnecting
    ip_list: Vec<SocketAddr>,
}
// TODO: This will become the layer of indirection
// so that incoming peers will be sent to the right torrent manager
#[derive(Clone)]
struct PeerListProvider(Arc<Mutex<ahash::AHashMap<[u8; 20], TorrentPeerList>>>);

impl PeerProvider for PeerListProvider {
    fn get_peers(&self, info_hash: [u8; 20]) -> Option<Vec<std::net::SocketAddr>> {
        log::info!("Fetching peers");
        self.0
            .lock()
            .get(&info_hash)
            .map(|peer_list| peer_list.ip_list.clone())
    }

    fn insert_peer(&self, info_hash: [u8; 20], peer: std::net::SocketAddr) {
        if let Some(peer_list) = self.0.lock().get_mut(&info_hash) {
            log::info!("Inserting peer that was announced!");

            peer_list.ip_list.push(peer);
            match peer {
                SocketAddr::V4(socket_addr_v4) => peer_list.sender.send(socket_addr_v4).unwrap(),
                SocketAddr::V6(_) => log::warn!("Skipping ipv6 addr"),
            }
        }
    }
}

fn main() {
    //    let log_file = std::fs::File::create("log.txt").unwrap();
    let mut log_builder = env_logger::builder();
    log_builder
        // .target(env_logger::Target::Pipe(Box::new(log_file)))
        .filter_level(log::LevelFilter::Info)
        .filter_module("vortex_dht", log::LevelFilter::Warn)
        .init();

    // TODO Should start dht first
    let torrent_info =
        lava_torrent::torrent::v1::Torrent::read_from_file("tails-amd64-6.11-img.torrent").unwrap();
    let (tx, rc) = std::sync::mpsc::channel();
    let torrent_peer_list = TorrentPeerList {
        sender: tx,
        ip_list: Default::default(),
    };
    let info_hash = torrent_info.info_hash_bytes().try_into().unwrap();
    let peer_list_map = Arc::new(Mutex::new(
        [(info_hash, torrent_peer_list)].into_iter().collect(),
    ));
    let peer_list_provider = PeerListProvider(peer_list_map);

    std::thread::spawn(move || {
        tokio_uring::start(async move {
            let dht =
                vortex_dht::Dht::new("0.0.0.0:1337".parse().unwrap(), peer_list_provider.clone())
                    .await
                    .unwrap();

            let start_timer = Instant::now();
            dht.start().await.unwrap();
            log::info!("DHT startup time: {}", start_timer.elapsed().as_secs());
            let find_peers_timer = Instant::now();
            let mut peers_reciver = dht.find_peers(info_hash.as_ref());
            dht.save(Path::new("routing_table.json")).await.unwrap();
            while let Some(peers) = peers_reciver.recv().await {
                log::info!(
                    "DHT find peers time: {}",
                    find_peers_timer.elapsed().as_secs()
                );
                for peer_addr in peers {
                    log::info!("Attempting connection to {}", peer_addr);
                    peer_list_provider.insert_peer(info_hash, peer_addr);
                }
            }
        });
    });

    let id = generate_peer_id();
    let torrrent = Torrent::new(torrent_info, id);
    let start_time = Instant::now();
    torrrent.start(rc, "downloaded").unwrap();
    log::info!("DOWNLOADED in {}", start_time.elapsed().as_secs());
}
