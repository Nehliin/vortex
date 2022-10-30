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
    let torrent = std::fs::read("test_torrent.torrent").unwrap();
    let metainfo = bip_metainfo::Metainfo::from_bytes(&torrent).unwrap();
    let torrent_manager = TorrentManager::new(metainfo.info().clone(), 1);
    tokio_uring::start(async move {
        // Will never shutdown!
        let shutdown = torrent_manager
            .add_peer(
                "127.0.0.1:6881".parse().unwrap(),
                *b"7ee95578b1236c45daaa",
                *b"9424471b03a5975de79a",
            )
            .await;
        log::info!("We are connected!!");
        let handle = torrent_manager.peer(0).unwrap();
        handle.sender.send(PeerOrder::Interested).await.unwrap();

        let piece_len = metainfo.info().piece_length();
        println!("pieces: {}", metainfo.info().pieces().count());
        /*        for (i, _) in metainfo.info().pieces().enumerate() {
            handle
                .sender
                .send(bittorrent::PeerOrder::RequestPiece {
                    index: i as i32,
                    total_len: piece_len as u32,
                })
                .await
                .unwrap();
        }*/
        // packet is too large
        handle
            .sender
            .send(PeerOrder::RequestPiece {
                index: 0,
                total_len: dbg!(piece_len) as u32,
            })
            .await
            .unwrap();
        // nästa steg: se till att torrent manager kollar hash samt skickar
        // ut have meddelande till samtliga peers
        // Sen ska den skicka ut ny order om att ladda ner piece
        // sen efter det kan man böra kolla piece selection algoritmer
        tokio::time::sleep(Duration::from_secs(70)).await;
        /*handle
        .sender
        .send(bittorrent::PeerOrder::RequestPiece {
            index: 0,
            total_len: dbg!(piece_len) as u32,
        })
        .await
        .unwrap();*/
        shutdown.await.unwrap();
    });
}
