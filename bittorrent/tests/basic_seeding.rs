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

#[test]
fn basic_seeding() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .init();
    let builder = PrometheusBuilder::new();
    builder.install().unwrap();

    // Generate test files
    let test_files: HashMap<String, Vec<u8>> = [
        ("file1.txt".to_string(), b"Hello, World!".repeat(100)),
        (
            "file2.txt".to_string(),
            b"BitTorrent Test Data!".repeat(200),
        ),
        ("subdir/file3.txt".to_string(), vec![42u8; 16384]),
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

    let torrent_name = format!("test_seeding_{}", rand::random::<u32>());

    // Create seeder directory with test files
    // Note: FileStore expects files to be in root/torrent_name/, so we need to create them there
    let seeder_dir = TempDir::new(&format!("{}_seeder", torrent_name));
    for (file_path, data) in &test_files {
        seeder_dir.add_file(&format!("{}/{}", torrent_name, file_path), data);
    }

    // Build torrent metadata from the torrent subdirectory
    let metadata = TorrentBuilder::new(seeder_dir.path().join(&torrent_name), 16384)
        .set_name(torrent_name.clone())
        .build()
        .unwrap();

    // Create downloader directory (empty initially)
    let downloader_dir = TempDir::new(&format!("{}_downloader", torrent_name));

    // Set up seeder state with completed files
    let seeder_id = generate_peer_id();
    let seeder_state = State::from_metadata_and_root(metadata.clone(), seeder_dir.path().clone())
        .expect("Failed to create seeder state");
    let mut seeder_torrent = Torrent::new(seeder_id, seeder_state);

    // Set up downloader state (empty)
    let downloader_id = generate_peer_id();
    let downloader_state =
        State::from_metadata_and_root(metadata.clone(), downloader_dir.path().clone())
            .expect("Failed to create downloader state");
    let mut downloader_torrent = Torrent::new(downloader_id, downloader_state);
    let mut downloader_command_q = heapless::spsc::Queue::new();
    let (downloader_command_tx, downloader_command_rc) = downloader_command_q.split();
    // just to use it from both threads
    let downloader_command_tx = Arc::new(Mutex::new(downloader_command_tx));
    let downloader_command_tx_clone = downloader_command_tx.clone();

    let mut seeder_command_q = heapless::spsc::Queue::new();
    let (mut seeder_command_tx, seeder_command_rc) = seeder_command_q.split();

    let test_time = Instant::now();

    let seeder_shutting_down = Arc::new(AtomicBool::new(false));
    let seeder_shutting_down_clone = seeder_shutting_down.clone();

    std::thread::scope(|s| {
        // Seeder thread
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
                    if test_time.elapsed() >= Duration::from_secs(60) {
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
                            TorrentEvent::ListenerStarted { port } => {
                                log::info!("Seeder listener started on port {}", port);
                                downloader_command_tx_clone
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

                    // Keep seeding for a bit after we see upload activity
                    if seeder_shutting_down.load(Ordering::Acquire) {
                        break;
                    }
                }
                saw_upload
            })
        });

        // Downloader thread
        let downloader_handle = s.spawn(move || {
            let mut downloader_event_q = heapless::spsc::Queue::new();
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
                    if test_time.elapsed() >= Duration::from_secs(60) {
                        panic!("Test timeout - download took too long");
                    }

                    while let Some(event) = downloader_event_rc.dequeue() {
                        match event {
                            TorrentEvent::TorrentComplete => {
                                let elapsed = test_time.elapsed();
                                log::info!("Download complete in: {:.2}s", elapsed.as_secs_f64());
                                let _ =
                                    downloader_command_tx.lock().unwrap().enqueue(Command::Stop);
                                let _ = seeder_command_tx.enqueue(Command::Stop);
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
                            TorrentEvent::ListenerStarted { .. } => {}
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
    log::info!("Verifying downloaded file hashes...");
    for (file_name, expected_hash) in expected_hashes {
        let downloaded_path = downloader_dir.path().join(&torrent_name).join(&file_name);
        let downloaded_data = std::fs::read(&downloaded_path)
            .unwrap_or_else(|_| panic!("Failed to read downloaded file: {}", file_name));

        let mut hasher = Sha1::new();
        hasher.update(&downloaded_data);
        let actual_hash = hasher.finalize().to_vec();

        assert_eq!(
            actual_hash, expected_hash,
            "Hash mismatch for file: {}",
            file_name
        );
        log::info!("File {} hash verified", file_name);
    }

    log::info!("All files verified successfully!");
}
