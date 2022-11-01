use std::time::Duration;

use bittorrent::{peer_connection::PeerOrder, TorrentManager};

// Make these tests automatic
// ./build/examples/client_test -G -f log.txt --peer_fingerprint=9424471b03a5975de79a --list-settings test_torrent.torrent
#[test]
fn initial_end_to_end() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
    let torrent = std::fs::read("final_test.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
    let torrent_manager = TorrentManager::new(metainfo.info().clone(), 1);
    tokio_uring::start(async move {
        // Will never shutdown!
        let shutjown = torrent_manager
            .add_peer(
                "127.0.0.1:6881".parse().unwrap(),
                *b"7ee95578b1236c45daaa",
                *b"9424471b03a5975de79a",
            )
            .await;
        log::info!("We are connected!!");
        let handle = torrent_manager.peer(0).unwrap();
        handle.sender.send(PeerOrder::Interested).await.unwrap();

        println!(
            "file len: {}",
            metainfo.info().files().next().unwrap().length()
        );
        println!("pieces: {}", metainfo.info().pieces().count());

        torrent_manager.start().await;
        std::fs::write(
            "downloaded.txt",
            torrent_manager
                .torrent_state
                .lock()
                .pretended_file
                .as_slice(),
        )
        .unwrap();

        let expected = std::fs::read("final_file.txt").unwrap();
        let actual = std::fs::read("downloaded.txt").unwrap();
        assert_eq!(actual, expected);
    });
}
