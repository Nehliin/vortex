mod common;

use std::{
    collections::HashMap,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

use common::{
    TempDir, calculate_file_hashes, create_test_torrent, init_test_environment,
    verify_downloaded_files,
};

const TIMEOUT: u64 = 60;

#[test]
fn basic_seeding() {
    init_test_environment();

    // Generate test files
    let test_files: HashMap<String, Vec<u8>> = [
        (
            "file2.txt".to_string(),
            b"BitTorrent Test Data!".repeat(200),
        ),
        ("subdir/file3.txt".to_string(), vec![42u8; 16384 * 5_000]),
    ]
    .into_iter()
    .collect();

    // Calculate expected file hashes for later verification
    let expected_hashes = calculate_file_hashes(&test_files);

    let torrent_name = format!("test_seeding_{}", rand::random::<u32>());

    // Create seeder directory with test files and build torrent metadata
    let (seeder_dir, metadata) = create_test_torrent(&test_files, &torrent_name, 16384);
    let tmp_dir_path = seeder_dir.path().clone();

    // Create downloader directory (empty initially)
    let downloader_dir = TempDir::new(&format!("{}_downloader", torrent_name));
    let downloader_path = downloader_dir.path().clone();

    // TODO: Fix this in a better way
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        // hacky clean up if a panic happens
        std::fs::remove_dir_all(&tmp_dir_path).unwrap();
        std::fs::remove_dir_all(&downloader_path).unwrap();
        prev_hook(info);
        std::process::abort();
    }));

    // Set up seeder state with completed files
    let seeder_id = PeerId::generate();
    let seeder_state = State::from_metadata_and_root(
        metadata.clone(),
        seeder_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create seeder state");
    let mut seeder_torrent = Torrent::new(seeder_id, seeder_state);

    // Set up downloader state (empty)
    let downloader_id = PeerId::generate();
    let downloader_state = State::from_metadata_and_root(
        metadata.clone(),
        downloader_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create downloader state");
    let mut downloader_torrent = Torrent::new(downloader_id, downloader_state);
    let (downloader_command_tx, downloader_command_rc) = std::sync::mpsc::sync_channel(64);
    let downloader_command_tx_clone = downloader_command_tx.clone();

    let (seeder_command_tx, seeder_command_rc) = std::sync::mpsc::sync_channel(64);

    let test_time = Instant::now();

    let seeder_shutting_down = Arc::new(AtomicBool::new(false));
    let seeder_shutting_down_clone = seeder_shutting_down.clone();

    let mut seeder_event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (seeder_event_tx, mut seeder_event_rc) = seeder_event_q.split();

    std::thread::scope(|s| {
        s.spawn(move || {
            let seeder_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            seeder_torrent
                .start(seeder_event_tx, seeder_command_rc, seeder_listener)
                .unwrap();
        });

        // Seeder thread
        let seeder_handle = s.spawn(move || {
            let mut saw_upload = false;

            loop {
                if test_time.elapsed() >= Duration::from_secs(TIMEOUT) {
                    panic!("Test timeout - seeding took too long");
                }

                while let Some(event) = seeder_event_rc.dequeue() {
                    match event {
                        TorrentEvent::TorrentMetrics { peer_metrics, .. } => {
                            for metrics in peer_metrics {
                                if metrics.upload_throughput > 0 {
                                    log::info!(
                                        "Seeder: Uploading at {} bytes/s",
                                        metrics.upload_throughput
                                    );
                                    saw_upload = true;
                                }
                            }
                        }
                        TorrentEvent::Running { port } => {
                            log::info!("Seeder listener started on port {}", port);
                            downloader_command_tx_clone
                                .send(Command::ConnectToPeers(vec![
                                    format!("127.0.0.1:{}", port).parse().unwrap(),
                                ]))
                                .unwrap();
                        }
                        TorrentEvent::TorrentComplete | TorrentEvent::MetadataComplete(_) => {}
                    }
                }

                if seeder_shutting_down.load(Ordering::Acquire) {
                    break;
                }
            }
            saw_upload
        });

        // Downloader thread
        let downloader_handle = s.spawn(move || {
            let mut downloader_event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
            let (downloader_event_tx, mut downloader_event_rc) = downloader_event_q.split();

            let downloader_listener = TcpListener::bind("127.0.0.1:0").unwrap();

            std::thread::scope(|downloader_scope| {
                downloader_scope.spawn(move || {
                    downloader_torrent
                        .start(
                            downloader_event_tx,
                            downloader_command_rc,
                            downloader_listener,
                        )
                        .unwrap();
                });

                loop {
                    if test_time.elapsed() >= Duration::from_secs(TIMEOUT) {
                        panic!("Test timeout - download took too long");
                    }

                    while let Some(event) = downloader_event_rc.dequeue() {
                        match event {
                            TorrentEvent::TorrentComplete => {
                                let elapsed = test_time.elapsed();
                                log::info!("Download complete in: {:.2}s", elapsed.as_secs_f64());
                                // Give time for the other peers to receive the event (sent on tick)
                                std::thread::sleep(Duration::from_secs(1));
                                let _ = downloader_command_tx.send(Command::Stop);
                                let _ = seeder_command_tx.send(Command::Stop);
                                seeder_shutting_down_clone.store(true, Ordering::Release);
                                return;
                            }
                            TorrentEvent::TorrentMetrics {
                                pieces_completed,
                                pieces_allocated,
                                ..
                            } => {
                                log::debug!(
                                    "Downloader progress: {}/{} pieces",
                                    pieces_completed,
                                    pieces_allocated
                                );
                            }
                            TorrentEvent::MetadataComplete(_) => {
                                log::info!("Downloader: Metadata complete");
                            }
                            TorrentEvent::Running { .. } => {}
                        }
                    }
                }
            })
        });

        // Wait for download to complete
        downloader_handle.join().unwrap();

        // Wait for seeder and check it saw upload activity
        let saw_upload = seeder_handle.join().unwrap();
        assert!(saw_upload, "Seeder never reported upload_throughput > 0");
    });

    // Verify downloaded files match original files
    verify_downloaded_files(
        &downloader_dir,
        &torrent_name,
        &expected_hashes,
        "downloader",
    );

    log::info!("All files verified successfully!");
}
