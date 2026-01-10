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

/// Test that verifies a 3-peer chain: Seeder → Middle → Leecher
///
/// This test ensures that:
/// - The middle peer can upload while downloading
/// - The leecher only downloads from the middle peer (never from the seeder)
/// - The leecher starts downloading after the middle peer has pieces
#[test]
fn chained_seeding() {
    init_test_environment();
    // ensure panics result in test failures
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev_hook(info);
        std::process::abort();
    }));
    // Generate test files
    let test_files: HashMap<String, Vec<u8>> = [
        (
            "file1.txt".to_string(),
            b"Chained Seeding Test!".repeat(150),
        ),
        (
            "file2.txt".to_string(),
            b"Middle peer uploads while downloading!".repeat(250),
        ),
        ("subdir/file3.txt".to_string(), vec![99u8; 20480]),
    ]
    .into_iter()
    .collect();

    // Calculate expected file hashes for later verification
    let expected_hashes = calculate_file_hashes(&test_files);

    let torrent_name = format!("test_chained_{}", rand::random::<u32>());

    // Create seeder directory with test files and build torrent metadata
    let (seeder_dir, metadata) = create_test_torrent(&test_files, &torrent_name, 16384);

    // Create middle and leecher directories (empty initially)
    let middle_dir = TempDir::new(&format!("{}_middle", torrent_name));
    let leecher_dir = TempDir::new(&format!("{}_leecher", torrent_name));

    let middle_dir_path = middle_dir.path().clone();
    let leecher_dir_path = leecher_dir.path().clone();
    let seeder_dir_path = seeder_dir.path().clone();
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev_hook(info);
        // hacky clean up if a panic happens
        std::fs::remove_dir_all(&middle_dir_path).unwrap();
        std::fs::remove_dir_all(&leecher_dir_path).unwrap();
        std::fs::remove_dir_all(&seeder_dir_path).unwrap();
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

    // Set up middle peer state (empty)
    let middle_id = PeerId::generate();
    let middle_state = State::from_metadata_and_root(
        metadata.clone(),
        middle_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create middle state");
    let mut middle_torrent = Torrent::new(middle_id, middle_state);

    // Set up leecher state (empty)
    let leecher_id = PeerId::generate();
    let leecher_state = State::from_metadata_and_root(
        metadata.clone(),
        leecher_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create leecher state");
    let mut leecher_torrent = Torrent::new(leecher_id, leecher_state);

    // Create command channels
    let (seeder_command_tx, seeder_command_rc) = std::sync::mpsc::sync_channel(64);

    let (middle_command_tx, middle_command_rc) = std::sync::mpsc::sync_channel(64);
    let middle_command_tx_clone1 = middle_command_tx.clone();
    let middle_command_tx_clone2 = middle_command_tx.clone();

    let (leecher_command_tx, leecher_command_rc) = std::sync::mpsc::sync_channel(64);
    let leecher_command_tx_clone = leecher_command_tx.clone();

    let test_time = Instant::now();

    let seeder_shutting_down = Arc::new(AtomicBool::new(false));
    let seeder_shutting_down_clone = seeder_shutting_down.clone();
    let middle_shutting_down = Arc::new(AtomicBool::new(false));
    let middle_shutting_down_clone = middle_shutting_down.clone();

    std::thread::scope(|s| {
        // Seeder thread (Peer 1)
        let seeder_handle = s.spawn(move || {
            let mut seeder_event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
            let (seeder_event_tx, mut seeder_event_rc) = seeder_event_q.split();

            let seeder_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            std::thread::scope(|seeder_scope| {
                seeder_scope.spawn(move || {
                    seeder_torrent
                        .start(seeder_event_tx, seeder_command_rc, seeder_listener)
                        .unwrap();
                });

                let mut saw_upload = false;

                loop {
                    if test_time.elapsed() >= Duration::from_secs(120) {
                        panic!("Test timeout - seeder took too long");
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
                            TorrentEvent::ListenerStarted { port } => {
                                log::info!("Seeder listener started on port {}", port);
                                // Connect middle peer to seeder
                                middle_command_tx_clone1
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
            })
        });

        // Middle peer thread (Peer 2)
        let middle_handle = s.spawn(move || {
            let mut middle_event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
            let (middle_event_tx, mut middle_event_rc) = middle_event_q.split();

            let middle_listener = TcpListener::bind("127.0.0.1:0").unwrap();

            std::thread::scope(|middle_scope| {
                middle_scope.spawn(move || {
                    middle_torrent
                        .start(middle_event_tx, middle_command_rc, middle_listener)
                        .unwrap();
                });

                let mut saw_upload = false;
                let mut completed = false;

                loop {
                    if test_time.elapsed() >= Duration::from_secs(120) {
                        panic!("Test timeout - middle peer took too long");
                    }

                    while let Some(event) = middle_event_rc.dequeue() {
                        match event {
                            TorrentEvent::TorrentComplete => {
                                log::info!("Middle peer: Download complete");
                                completed = true;
                            }
                            TorrentEvent::TorrentMetrics {
                                pieces_completed,
                                peer_metrics,
                                ..
                            } => {
                                log::debug!("Middle peer progress: {}", pieces_completed,);
                                for metrics in peer_metrics {
                                    if metrics.upload_throughput > 0 {
                                        log::info!(
                                            "Middle peer: Uploading at {} bytes/s (completed: {})",
                                            metrics.upload_throughput,
                                            completed
                                        );
                                        saw_upload = true;
                                    }
                                }
                            }
                            TorrentEvent::ListenerStarted { port } => {
                                log::info!("Middle peer listener started on port {}", port);
                                // Connect leecher to middle peer ONLY (not to seeder)
                                leecher_command_tx_clone
                                    .send(Command::ConnectToPeers(vec![
                                        format!("127.0.0.1:{}", port).parse().unwrap(),
                                    ]))
                                    .unwrap();
                            }
                            TorrentEvent::MetadataComplete(_) => {
                                log::info!("Middle peer: Metadata complete");
                            }
                        }
                    }

                    if middle_shutting_down.load(Ordering::Acquire) {
                        break;
                    }
                }
                saw_upload
            })
        });

        // Leecher thread (Peer 3)
        let leecher_handle = s.spawn(move || {
            let mut leecher_event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
            let (leecher_event_tx, mut leecher_event_rc) = leecher_event_q.split();

            let leecher_listener = TcpListener::bind("127.0.0.1:0").unwrap();

            std::thread::scope(|leecher_scope| {
                leecher_scope.spawn(move || {
                    leecher_torrent
                        .start(leecher_event_tx, leecher_command_rc, leecher_listener)
                        .unwrap();
                });

                loop {
                    if test_time.elapsed() >= Duration::from_secs(120) {
                        panic!("Test timeout - leecher took too long");
                    }

                    while let Some(event) = leecher_event_rc.dequeue() {
                        match event {
                            TorrentEvent::TorrentComplete => {
                                let elapsed = test_time.elapsed();
                                log::info!(
                                    "Leecher: Download complete in: {:.2}s",
                                    elapsed.as_secs_f64()
                                );
                                // Give time for the other peers to receive the event (sent on tick)
                                std::thread::sleep(Duration::from_secs(1));
                                // Stop all peers
                                let _ = leecher_command_tx.send(Command::Stop);
                                let _ = middle_command_tx_clone2.send(Command::Stop);
                                let _ = seeder_command_tx.send(Command::Stop);
                                middle_shutting_down_clone.store(true, Ordering::Release);
                                seeder_shutting_down_clone.store(true, Ordering::Release);
                                return;
                            }
                            TorrentEvent::TorrentMetrics {
                                pieces_completed, ..
                            } => {
                                log::debug!("Leecher progress: {} pieces", pieces_completed,);
                            }
                            TorrentEvent::MetadataComplete(_) => {
                                log::info!("Leecher: Metadata complete");
                            }
                            TorrentEvent::ListenerStarted { .. } => {}
                        }
                    }
                }
            })
        });

        // Wait for leecher to complete (which stops all peers)
        leecher_handle.join().unwrap();

        // Wait for middle peer and verify it uploaded
        let middle_saw_upload = middle_handle.join().unwrap();
        assert!(
            middle_saw_upload,
            "Middle peer never reported upload_throughput > 0"
        );

        // Wait for seeder and verify it uploaded
        let seeder_saw_upload = seeder_handle.join().unwrap();
        assert!(
            seeder_saw_upload,
            "Seeder never reported upload_throughput > 0"
        );
    });

    // Verify middle peer's downloaded files
    verify_downloaded_files(&middle_dir, &torrent_name, &expected_hashes, "middle peer");

    // Verify leecher's downloaded files
    verify_downloaded_files(&leecher_dir, &torrent_name, &expected_hashes, "leecher");

    log::info!("All files verified successfully!");
    log::info!("Chained seeding test passed: Seeder → Middle → Leecher");
}
