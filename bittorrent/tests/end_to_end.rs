use std::time::Duration;

use bittorrent::TorrentManager;
use tokio_uring::net::TcpListener;

// Make these tests automatic
// ./build/examples/client_test -G -f log.txt final_test.torrent
#[test]
fn download_from_seeding() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
    let torrent = std::fs::read("final_test.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
    let torrent_manager = TorrentManager::new(metainfo.info().clone());
    tokio_uring::start(async move {
        let _peer_con = torrent_manager
            .add_peer("127.0.0.1:6881".parse().unwrap())
            .await
            .unwrap();
        log::info!("We are connected!!");

        println!(
            "file len: {}",
            metainfo.info().files().next().unwrap().length()
        );
        println!("pieces: {}", metainfo.info().pieces().count());

        torrent_manager.start().await.unwrap();

        let expected = std::fs::read("final_file_og.txt").unwrap();
        let actual = std::fs::read("final_file.txt").unwrap();
        assert_eq!(actual, expected);
    });
}

#[test]
fn accepts_incoming() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
    let torrent = std::fs::read("final_test.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(torrent).unwrap();
    // simulate seeding
    let torrent_manager = TorrentManager::new(metainfo.info().clone());
    let file = std::fs::read("final_file_og.txt").unwrap();
    torrent_manager
        .torrent_state
        .borrow_mut()
        .completed_pieces
        .fill(true);

    let listener = TcpListener::bind("127.0.0.1:1337".parse().unwrap()).unwrap();

    tokio_uring::start(async move {
        let mut attempt = 1;
        loop {
            if attempt > 2 {
                panic!("Should succeed after first failure");
            }
            if let Ok(handle) = torrent_manager.accept_incoming(&listener).await {
                log::info!("We are connected!!");
                tokio::time::sleep(Duration::from_secs(10)).await;
            } else {
                log::info!("Connection attempt {attempt} failed");
                // First will fail since libtorrent by default attempts to encrypt the 
                // connection
                attempt += 1;
            }
        }
    });
}
