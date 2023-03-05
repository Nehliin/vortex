use std::{cell::RefCell, path::Path, rc::Rc, time::Duration};

use vortex_bittorrent::{PeerListHandle, TorrentManager};
use vortex_dht::PeerProvider;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parking_lot::Mutex;

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
        let progress = MultiProgress::new();

        // TODO Should start dht first
        let torrent_info = std::fs::read("slackware.torrent").unwrap();
        let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent_info).unwrap();
        let torrent_manager = TorrentManager::new(metainfo.info().clone()).await;
        let peer_list_map = Mutex::new(
            [(
                metainfo.info().info_hash().try_into().unwrap(),
                torrent_manager.peer_list_handle(),
            )]
            .into_iter()
            .collect(),
        );
        let peer_list_provider = PeerListProvider(peer_list_map);

        let dht = vortex_dht::Dht::new("0.0.0.0:1337".parse().unwrap(), peer_list_provider)
            .await
            .unwrap();

        dht.start().await.unwrap();
        let info_hash = metainfo.info().info_hash();
        let mut peers_reciver = dht.find_peers(info_hash.as_ref());

        dht.save(Path::new("routing_table.json")).await.unwrap();
        let peers = peers_reciver.recv().await.unwrap();

        let connection_progress = progress.add(ProgressBar::new_spinner());
        connection_progress
            .set_style(ProgressStyle::with_template("{spinner:.blue} {msg}").unwrap());
        connection_progress.enable_steady_tick(Duration::from_millis(100));
        let total_peers = peers.len();
        let num_success = Rc::new(RefCell::new(0));
        let num_failures = Rc::new(RefCell::new(0));

        let connect_futures = peers.into_iter().enumerate().map(|(i, addr)| {
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
                    tokio::time::timeout(Duration::from_secs(5), manager_clone.add_peer(addr))
                        .await;

                match connect_res {
                    Ok(Ok(_peer_con)) => {
                        *num_success_clone.borrow_mut() += 1;
                        log::info!("Connected to {}!", addr);
                    }
                    Ok(Err(err)) => {
                        *num_failures_clone.borrow_mut() += 1;
                        log::error!("Failed to connect to peer {}, error: {err}", addr);
                    }
                    Err(_) => {
                        *num_failures_clone.borrow_mut() += 1;
                        log::error!("Failed to connect to peer: {:?}, timedout", addr);
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
        let download_progress = progress.add(ProgressBar::new(
            metainfo.info().files().next().unwrap().length(),
        ));
        download_progress.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})").unwrap().progress_chars("#>-"));
        let download_progress_clone = download_progress.clone();
        torrent_manager.set_subpiece_callback(move |data| {
            download_progress_clone.inc(data.len() as u64);
        });
        download_progress.tick();
        torrent_manager.start().await.unwrap();
        download_progress.finish_with_message("File dowloaded!");
    });
}
