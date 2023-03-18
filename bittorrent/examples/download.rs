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

    let torrent =
        lava_torrent::torrent::v1::Torrent::read_from_file("assets/test-file-1.torrent").unwrap();
    tokio_uring::start(async move {
        let mut torrent_manager = TorrentManager::new("assets/test-file-1.torrent").await;
        let peer_list = torrent_manager.peer_list_handle();
        peer_list.insert("172.17.0.3:51413".parse().unwrap());

        log::info!("We are attempting a connection");

        println!("Total length: {}", torrent.length);
        println!("pieces: {}", torrent.pieces.len());

        // 1. 766s (with spawning write in separate task)
        // 2. 766s (without spawning write in separate task)
        let download_time = Instant::now();
        torrent_manager.download_complete().await;
        let elapsed = download_time.elapsed();
        log::info!("Download complete in: {}s", elapsed.as_secs());

        let expected = std::fs::read("assets/test-file-1").unwrap();
        let actual = std::fs::read("dowloaded/test-file-1").unwrap();
        assert_eq!(actual, expected);
    });
}
