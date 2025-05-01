use std::time::Instant;

use metrics_exporter_prometheus::PrometheusBuilder;
use vortex_bittorrent::{generate_peer_id, Torrent};

#[test]
fn basic_seeded_download() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .init();
    let builder = PrometheusBuilder::new();
    builder.install().unwrap();
    let torrent =
        lava_torrent::torrent::v1::Torrent::read_from_file("../assets/test-file-1.torrent").unwrap();
    let our_id = generate_peer_id();
    let torrent = Torrent::new(torrent, our_id);

    let download_time = Instant::now();
    let (tx, rc) = std::sync::mpsc::channel();
    tx.send("127.0.0.1:51413".parse().unwrap()).unwrap();
    torrent.start(rc, "../downloaded").unwrap();
    let elapsed = download_time.elapsed();
    log::info!("Download complete in: {}s", elapsed.as_secs());
    let expected = std::fs::read("../assets/test-file-1").unwrap();
    let actual = std::fs::read("../downloaded/test-file-1/test-file-1").unwrap();
    assert_eq!(actual, expected);
}
