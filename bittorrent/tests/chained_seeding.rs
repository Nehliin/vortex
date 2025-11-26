use std::{
    collections::HashMap,
    net::TcpListener,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use lava_torrent::torrent::v1::TorrentBuilder;
use metrics_exporter_prometheus::PrometheusBuilder;
use sha1::{Digest, Sha1};
use vortex_bittorrent::{Command, State, Torrent, TorrentEvent, generate_peer_id};

/// Temporary directory that auto-cleans on drop
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let path = format!("/tmp/vortex_test_{}_{}", prefix, rand::random::<u32>());
        std::fs::create_dir_all(&path).unwrap();
        let path = std::fs::canonicalize(path).unwrap();
        Self { path }
    }

    fn add_file(&self, file_path: &str, data: &[u8]) {
        let file_path = self.path.join(file_path);
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(file_path, data).unwrap();
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Test that verifies a 3-peer chain: Seeder → Middle → Leecher
///
/// This test ensures that:
/// - The middle peer can upload while downloading
/// - The leecher only downloads from the middle peer (never from the seeder)
/// - The leecher starts downloading after the middle peer has pieces
#[test]
fn chained_seeding() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .init();
    let builder = PrometheusBuilder::new();
    if let Err(err) = builder.install() {
        log::error!("failed installing PrometheusBuilder: {err}");
    }

    // Generate test files
    let test_files: HashMap<String, Vec<u8>> = [
        ("file1.txt".to_string(), b"Chained Seeding Test!".repeat(150)),
        (
            "file2.txt".to_string(),
            b"Middle peer uploads while downloading!".repeat(250),
        ),
        ("subdir/file3.txt".to_string(), vec![99u8; 20480]),
    ]
    .into_iter()
    .collect();

    // Calculate expected file hashes for later verification
    let expected_hashes: HashMap<String, Vec<u8>> = test_files
        .iter()
        .map(|(name, data)| {
            let mut hasher = Sha1::new();
            hasher.update(data);
            (name.clone(), hasher.finalize().to_vec())
        })
        .collect();

    let torrent_name = format!("test_chained_{}", rand::random::<u32>());

    // Create seeder directory with test files
    let seeder_dir = TempDir::new(&format!("{}_seeder", torrent_name));
    for (file_path, data) in &test_files {
        seeder_dir.add_file(&format!("{}/{}", torrent_name, file_path), data);
    }

    // Build torrent metadata from the torrent subdirectory
    let metadata = TorrentBuilder::new(seeder_dir.path().join(&torrent_name), 16384)
        .set_name(torrent_name.clone())
        .build()
        .unwrap();

    // Create middle and leecher directories (empty initially)
    let middle_dir = TempDir::new(&format!("{}_middle", torrent_name));
    let leecher_dir = TempDir::new(&format!("{}_leecher", torrent_name));

    // Set up seeder state with completed files
    let seeder_id = generate_peer_id();
    let seeder_state = State::from_metadata_and_root(metadata.clone(), seeder_dir.path().clone())
        .expect("Failed to create seeder state");
    let mut seeder_torrent = Torrent::new(seeder_id, seeder_state);

    // Set up middle peer state (empty)
    let middle_id = generate_peer_id();
    let middle_state = State::from_metadata_and_root(metadata.clone(), middle_dir.path().clone())
        .expect("Failed to create middle state");
    let mut middle_torrent = Torrent::new(middle_id, middle_state);

    // Set up leecher state (empty)
    let leecher_id = generate_peer_id();
    let leecher_state = State::from_metadata_and_root(metadata.clone(), leecher_dir.path().clone())
        .expect("Failed to create leecher state");
    let mut leecher_torrent = Torrent::new(leecher_id, leecher_state);

    // Create command queues
    let mut seeder_command_q = heapless::spsc::Queue::new();
    let (mut seeder_command_tx, seeder_command_rc) = seeder_command_q.split();

    let mut middle_command_q = heapless::spsc::Queue::new();
    let (middle_command_tx, middle_command_rc) = middle_command_q.split();
    let middle_command_tx = Arc::new(Mutex::new(middle_command_tx));
    let middle_command_tx_clone1 = middle_command_tx.clone();
    let middle_command_tx_clone2 = middle_command_tx.clone();

    let mut leecher_command_q = heapless::spsc::Queue::new();
    let (leecher_command_tx, leecher_command_rc) = leecher_command_q.split();
    let leecher_command_tx = Arc::new(Mutex::new(leecher_command_tx));
    let leecher_command_tx_clone = leecher_command_tx.clone();

    let test_time = Instant::now();

    let seeder_shutting_down = Arc::new(AtomicBool::new(false));
    let seeder_shutting_down_clone = seeder_shutting_down.clone();
    let middle_shutting_down = Arc::new(AtomicBool::new(false));
    let middle_shutting_down_clone = middle_shutting_down.clone();

    std::thread::scope(|s| {
        // Seeder thread (Peer 1)
        let seeder_handle = s.spawn(move || {
            let mut seeder_event_q = heapless::spsc::Queue::new();
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
                                    .lock()
                                    .unwrap()
                                    .enqueue(Command::ConnectToPeers(vec![
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
            let mut middle_event_q = heapless::spsc::Queue::new();
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
                                pieces_allocated,
                                peer_metrics,
                                ..
                            } => {
                                log::debug!(
                                    "Middle peer progress: {}/{} pieces",
                                    pieces_completed,
                                    pieces_allocated
                                );
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
                                    .lock()
                                    .unwrap()
                                    .enqueue(Command::ConnectToPeers(vec![
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
            let mut leecher_event_q = heapless::spsc::Queue::new();
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
                                // Stop all peers
                                let _ = leecher_command_tx.lock().unwrap().enqueue(Command::Stop);
                                let _ = middle_command_tx_clone2
                                    .lock()
                                    .unwrap()
                                    .enqueue(Command::Stop);
                                let _ = seeder_command_tx.enqueue(Command::Stop);
                                middle_shutting_down_clone.store(true, Ordering::Release);
                                seeder_shutting_down_clone.store(true, Ordering::Release);
                                return;
                            }
                            TorrentEvent::TorrentMetrics {
                                pieces_completed,
                                pieces_allocated,
                                ..
                            } => {
                                log::debug!(
                                    "Leecher progress: {}/{} pieces",
                                    pieces_completed,
                                    pieces_allocated
                                );
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
    log::info!("Verifying middle peer's file hashes...");
    for (file_name, expected_hash) in &expected_hashes {
        let downloaded_path = middle_dir.path().join(&torrent_name).join(file_name);
        let downloaded_data = std::fs::read(&downloaded_path)
            .unwrap_or_else(|_| panic!("Failed to read middle peer file: {}", file_name));

        let mut hasher = Sha1::new();
        hasher.update(&downloaded_data);
        let actual_hash = hasher.finalize().to_vec();

        assert_eq!(
            actual_hash, *expected_hash,
            "Hash mismatch for middle peer file: {}",
            file_name
        );
        log::info!("Middle peer file {} hash verified", file_name);
    }

    // Verify leecher's downloaded files
    log::info!("Verifying leecher's file hashes...");
    for (file_name, expected_hash) in expected_hashes {
        let downloaded_path = leecher_dir.path().join(&torrent_name).join(&file_name);
        let downloaded_data = std::fs::read(&downloaded_path)
            .unwrap_or_else(|_| panic!("Failed to read leecher file: {}", file_name));

        let mut hasher = Sha1::new();
        hasher.update(&downloaded_data);
        let actual_hash = hasher.finalize().to_vec();

        assert_eq!(
            actual_hash, expected_hash,
            "Hash mismatch for leecher file: {}",
            file_name
        );
        log::info!("Leecher file {} hash verified", file_name);
    }

    log::info!("All files verified successfully!");
    log::info!("Chained seeding test passed: Seeder → Middle → Leecher");
}
