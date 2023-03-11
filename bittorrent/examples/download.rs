use std::time::Instant;

use vortex_bittorrent::TorrentManager;

// Prerequisites:
// ./create_random_file.sh --path assets/test-file-1 --size 100000000
// ./transmission_containers.sh --name transmission-1
// ./seed_new_torrent.sh --name test-file-1 --path assets/test-file-1 --seed transmission-1
fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    let torrent = std::fs::read("assets/test-file-1.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(torrent).unwrap();
    tokio_uring::start(async move {
        let torrent_manager = TorrentManager::new(metainfo.info().clone()).await;
        let _peer_con = torrent_manager
            .add_peer("172.17.0.2:51413".parse().unwrap())
            .await
            .unwrap();
        log::info!("We are connected!!");

        println!(
            "file len: {}",
            metainfo.info().files().next().unwrap().length()
        );
        println!("pieces: {}", metainfo.info().pieces().count());

        // 1. 766s (with spawning write in separate task)
        // 2. 766s (without spawning write in separate task)
        let download_time = Instant::now();
        torrent_manager.start().await.unwrap();
        let elapsed = download_time.elapsed();
        log::info!("Download complete in: {}s", elapsed.as_secs());

        let expected = std::fs::read("assets/test-file-1").unwrap();
        let actual = std::fs::read("dowloaded/test-file-1").unwrap();
        assert_eq!(actual, expected);
    });
}