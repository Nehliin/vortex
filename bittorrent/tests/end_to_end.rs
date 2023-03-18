use std::time::Duration;

use tokio_uring::net::TcpListener;
use vortex_bittorrent::TorrentManager;

// Make these tests automatic
// ./build/examples/client_test -G -f log.txt final_test.torrent
#[test]
#[ignore = "only for ci"]
fn download_from_seeding() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
    let torrent = lava_torrent::torrent::v1::Torrent::read_from_file("final_test.torrent").unwrap();
    tokio_uring::start(async move {
        let mut torrent_manager = TorrentManager::new("final_test.torrent").await;
        let peer_list_handle = torrent_manager.peer_list_handle();
        peer_list_handle.insert("127.0.0.1:6881".parse().unwrap());

        println!("Total length: {}", torrent.length);
        println!("pieces: {}", torrent.pieces.len());

        torrent_manager.download_complete().await;

        let expected = std::fs::read("final_file_og.txt").unwrap();
        let actual = std::fs::read("final_file.txt").unwrap();
        assert_eq!(actual, expected);
    });
}

/*#[test]
#[ignore = "only for ci"]
fn accepts_incoming() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init();
    let _torrent =
        lava_torrent::torrent::v1::Torrent::read_from_file("final_test.torrent").unwrap();
    // simulate seeding
    // TODO
    //let file = std::fs::read("final_file_og.txt").unwrap();
    /*torrent_manager
    .torrent_state
    .borrow_mut()
    .completed_pieces
    .fill(true);*/

    let listener = TcpListener::bind("127.0.0.1:1337".parse().unwrap()).unwrap();

    tokio_uring::start(async move {
        let torrent_manager = TorrentManager::new("final_test.torrent").await;
        let mut attempt = 1;
        loop {
            if attempt > 2 {
                panic!("Should succeed after first failure");
            }
            if let Ok(_handle) = torrent_manager.accept_incoming(&listener).await {
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
*/
