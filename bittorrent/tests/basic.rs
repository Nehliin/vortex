use std::time::Instant;

use metrics_exporter_prometheus::PrometheusBuilder;
use vortex_bittorrent::{Command, State, Torrent, generate_peer_id};

#[test]
fn basic_seeded_download() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .init();
    let builder = PrometheusBuilder::new();
    builder.install().unwrap();
    let metadata =
        lava_torrent::torrent::v1::Torrent::read_from_file("../assets/test-file-1.torrent")
            .unwrap();
    let our_id = generate_peer_id();
    let mut torrent = Torrent::new(
        our_id,
        State::unstarted_from_metadata(metadata, "../downloaded".into()).unwrap(),
    );

    let download_time = Instant::now();
    let (tx, rc) = std::sync::mpsc::channel();

    tx.send(Command::ConnectToPeers(vec![
        "127.0.0.1:51413".parse().unwrap(),
    ]))
    .unwrap();

    // Spawn a thread to send Stop command after a timeout
    let tx_clone = tx.clone();
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_secs(60));
        // Send stop command if download takes too long
        let _ = tx_clone.send(Command::Stop);
    });

    torrent.start(rc).unwrap();
    let elapsed = download_time.elapsed();
    log::info!("Download complete in: {}s", elapsed.as_secs());
    let expected = std::fs::read("../assets/test-file-1").unwrap();
    let actual = std::fs::read("../downloaded/test-file-1/test-file-1").unwrap();
    assert_eq!(actual, expected);
}
