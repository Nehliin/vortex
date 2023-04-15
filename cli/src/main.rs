use std::{path::Path, time::Instant};

use parking_lot::Mutex;
use vortex_bittorrent::{PeerListHandle, TorrentManager};
use vortex_dht::PeerProvider;

// TODO: This will become the layer of indirection
// so that incoming peers will be sent to the right torrent manager
struct PeerListProvider(Mutex<ahash::AHashMap<[u8; 20], PeerListHandle>>);

impl PeerProvider for PeerListProvider {
    fn get_peers(&self, info_hash: [u8; 20]) -> Option<Vec<std::net::SocketAddr>> {
        log::info!("Fetching peers");
        self.0
            .lock()
            .get(&info_hash)
            .map(|peer_list_handle| peer_list_handle.peers())
    }

    fn insert_peer(&self, info_hash: [u8; 20], peer: std::net::SocketAddr) {
        if let Some(peer_list_handle) = self.0.lock().get(&info_hash) {
            log::info!("Inserting peer that was announced!");
            peer_list_handle.insert(peer);
        }
    }
}

fn main() {
    //    let log_file = std::fs::File::create("log.txt").unwrap();
    let mut log_builder = env_logger::builder();
    log_builder
        // .target(env_logger::Target::Pipe(Box::new(log_file)))
        .filter_level(log::LevelFilter::Debug)
        .init();

    tokio_uring::start(async move {
        // TODO Should start dht first
        let torrent_info =
            lava_torrent::torrent::v1::Torrent::read_from_file("slackware.torrent").unwrap();
        let mut torrent_manager = TorrentManager::new("slackware.torrent").await;
        let info_hash = torrent_info.info_hash_bytes().try_into().unwrap();
        let peer_list_map = Mutex::new(
            [(info_hash, torrent_manager.peer_list_handle())]
                .into_iter()
                .collect(),
        );
        let peer_list_provider = PeerListProvider(peer_list_map);

        let dht = vortex_dht::Dht::new("0.0.0.0:1337".parse().unwrap(), peer_list_provider)
            .await
            .unwrap();

        let start_timer = Instant::now();
        dht.start().await.unwrap();
        log::info!("DHT startup time: {}", start_timer.elapsed().as_secs());
        let find_peers_timer = Instant::now();
        let mut peers_reciver = dht.find_peers(info_hash.as_ref());

        dht.save(Path::new("routing_table.json")).await.unwrap();
        let peer_list_handle = torrent_manager.peer_list_handle();
        tokio_uring::spawn(async move {
            while let Some(peers) = peers_reciver.recv().await {
                log::info!(
                    "DHT find peers time: {}",
                    find_peers_timer.elapsed().as_secs()
                );
                for peer_addr in peers {
                    log::info!("Attempting connection to {}", peer_addr);
                    peer_list_handle.insert(peer_addr);
                }
            }
        });

        torrent_manager.download_complete().await;
        log::info!("Download complete!");
    });
}
