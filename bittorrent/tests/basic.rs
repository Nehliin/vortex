use std::time::{Duration, Instant};

use metrics_exporter_prometheus::PrometheusBuilder;
use vortex_bittorrent::{Command, State, Torrent, TorrentEvent, generate_peer_id};

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
    let mut command_q = heapless::spsc::Queue::new();
    let mut event_q = heapless::spsc::Queue::new();

    let (mut command_tx, command_rc) = command_q.split();
    let (event_tx, mut event_rc) = event_q.split();

    command_tx
        .enqueue(Command::ConnectToPeers(vec![
            "127.0.0.1:51413".parse().unwrap(),
        ]))
        .unwrap();

    std::thread::scope(move |s| {
        // Spawn a thread to send Stop command after a timeout
        s.spawn(move || {
            torrent.start(event_tx, command_rc).unwrap();
        });
        'outer: loop {
            if download_time.elapsed() >= Duration::from_secs(60) {
                // Should never take this long
                panic!("Download is too slow");
            }
            while let Some(event) = event_rc.dequeue() {
                match event {
                    TorrentEvent::TorrentComplete => {
                        let elapsed = download_time.elapsed();
                        log::info!("Download complete in: {}s", elapsed.as_secs());
                        let expected = std::fs::read("../assets/test-file-1").unwrap();
                        let actual =
                            std::fs::read("../downloaded/test-file-1/test-file-1").unwrap();
                        assert_eq!(actual, expected);
                        let _ = command_tx.enqueue(Command::Stop);
                        break 'outer;
                    }
                    TorrentEvent::MetadataComplete(_torrent) => {
                        log::info!("METADATA COMPLETE");
                    }
                    TorrentEvent::PeerMetrics {
                        conn_id: _,
                        throuhgput: _,
                        endgame: _,
                        snubbed: _,
                    } => {}
                    TorrentEvent::TorrentMetrics {
                        pieces_completed: _,
                        pieces_allocated: _,
                        num_connections: _,
                    } => {}
                    TorrentEvent::ListenerStarted { port: _ } => {}
                }
            }
        }
    });
}
