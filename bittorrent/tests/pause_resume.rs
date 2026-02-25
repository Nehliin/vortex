mod common;

use std::{
    collections::HashMap,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

use common::{
    TempDir, calculate_file_hashes, create_test_torrent, init_test_environment,
    verify_downloaded_files,
};

const TIMEOUT: u64 = 120;

#[test]
fn pause_resume() {
    init_test_environment();

    // Generate test files, enough pieces to allow partial progress before pause
    let test_files: HashMap<String, Vec<u8>> = [
        (
            "file2.txt".to_string(),
            b"BitTorrent Test Data!".repeat(200),
        ),
        ("subdir/file3.txt".to_string(), vec![42u8; 16384 * 5_000]),
    ]
    .into_iter()
    .collect();

    let expected_hashes = calculate_file_hashes(&test_files);

    let torrent_name = format!("test_pause_resume_{}", rand::random::<u32>());

    let (seeder_dir, metadata) = create_test_torrent(&test_files, &torrent_name, 16384);
    let tmp_dir_path = seeder_dir.path().clone();

    let downloader_dir = TempDir::new(&format!("{}_downloader", torrent_name));
    let downloader_path = downloader_dir.path().clone();

    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        std::fs::remove_dir_all(&tmp_dir_path).unwrap();
        std::fs::remove_dir_all(&downloader_path).unwrap();
        prev_hook(info);
        std::process::abort();
    }));

    // Set up seeder
    let seeder_id = PeerId::generate();
    let seeder_state = State::from_metadata_and_root(
        metadata.clone(),
        seeder_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create seeder state");
    let mut seeder_torrent = Torrent::new(seeder_id, seeder_state);

    // Set up downloader
    let downloader_id = PeerId::generate();
    let downloader_state = State::from_metadata_and_root(
        metadata.clone(),
        downloader_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create downloader state");
    let mut downloader_torrent = Torrent::new(downloader_id, downloader_state);
    let (downloader_command_tx, downloader_command_rc) = std::sync::mpsc::sync_channel(64);
    let downloader_command_tx_for_seeder = downloader_command_tx.clone();

    let (seeder_command_tx, seeder_command_rc) = std::sync::mpsc::sync_channel(64);

    let test_time = Instant::now();

    let seeder_shutting_down = Arc::new(AtomicBool::new(false));
    let seeder_shutting_down_clone = seeder_shutting_down.clone();

    // Shared seeder port so the downloader can reconnect after resume
    let seeder_port = Arc::new(AtomicU16::new(0));
    let seeder_port_for_seeder = seeder_port.clone();

    let mut seeder_event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (seeder_event_tx, mut seeder_event_rc) = seeder_event_q.split();

    std::thread::scope(|s| {
        // Seeder event loop thread
        s.spawn(move || {
            let seeder_listener = TcpListener::bind("127.0.0.1:0").unwrap();
            seeder_torrent
                .start(seeder_event_tx, seeder_command_rc, seeder_listener)
                .unwrap();
        });

        // Seeder event handler thread
        let seeder_handle = s.spawn(move || {
            loop {
                if test_time.elapsed() >= Duration::from_secs(TIMEOUT) {
                    panic!("Test timeout in seeder event handler");
                }

                while let Some(event) = seeder_event_rc.dequeue() {
                    match event {
                        TorrentEvent::Running { port } => {
                            log::info!("Seeder running on port {}", port);
                            seeder_port_for_seeder.store(port, Ordering::Release);
                            // Connect the downloader to the seeder on first start
                            let _ = downloader_command_tx_for_seeder.send(Command::ConnectToPeers(
                                vec![format!("127.0.0.1:{}", port).parse().unwrap()],
                            ));
                        }
                        TorrentEvent::TorrentMetrics { .. } => {}
                        TorrentEvent::Paused => panic!("Seeder should never pause"),
                        TorrentEvent::TorrentComplete | TorrentEvent::MetadataComplete(_) => {}
                    }
                }

                if seeder_shutting_down.load(Ordering::Acquire) {
                    break;
                }
            }
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

                let mut pause_sent = false;
                let mut saw_paused = false;
                let mut resume_sent = false;
                let mut saw_resumed = false;

                loop {
                    if test_time.elapsed() >= Duration::from_secs(TIMEOUT) {
                        panic!("Test timeout - download took too long");
                    }

                    while let Some(event) = downloader_event_rc.dequeue() {
                        match event {
                            TorrentEvent::TorrentMetrics {
                                pieces_completed,
                                pieces_allocated,
                                ..
                            } => {
                                log::debug!(
                                    "Downloader progress: {}/{} pieces (pause_sent={}, saw_paused={}, resume_sent={}, saw_resumed={})",
                                    pieces_completed,
                                    pieces_allocated,
                                    pause_sent,
                                    saw_paused,
                                    resume_sent,
                                    saw_resumed,
                                );

                                // Once we have some progress, send Pause
                                if !pause_sent && pieces_completed >= 1 {
                                    log::info!(
                                        "Downloader has {} pieces, sending Pause",
                                        pieces_completed
                                    );
                                    downloader_command_tx.send(Command::Pause).unwrap();
                                    pause_sent = true;
                                }
                            }
                            TorrentEvent::Paused => {
                                log::info!("Downloader paused successfully");
                                assert!(
                                    pause_sent,
                                    "Received Paused event without sending Pause command"
                                );
                                saw_paused = true;

                                // Now resume
                                log::info!("Sending Resume to downloader");
                                downloader_command_tx.send(Command::Resume).unwrap();
                                resume_sent = true;
                            }
                            TorrentEvent::Running { port } => {
                                log::info!("Downloader running on port {}", port);
                                if resume_sent {
                                    saw_resumed = true;
                                    // Reconnect to the seeder after resume
                                    let sp = seeder_port.load(Ordering::Acquire);
                                    log::info!(
                                        "Downloader resumed, reconnecting to seeder on port {}",
                                        sp
                                    );
                                    downloader_command_tx
                                        .send(Command::ConnectToPeers(vec![
                                            format!("127.0.0.1:{}", sp).parse().unwrap(),
                                        ]))
                                        .unwrap();
                                }
                            }
                            TorrentEvent::TorrentComplete => {
                                let elapsed = test_time.elapsed();
                                log::info!(
                                    "Download complete in: {:.2}s",
                                    elapsed.as_secs_f64()
                                );
                                assert!(saw_paused, "Never received Paused event");
                                assert!(saw_resumed, "Never received Running event after resume");

                                std::thread::sleep(Duration::from_secs(1));
                                let _ = downloader_command_tx.send(Command::Stop);
                                let _ = seeder_command_tx.send(Command::Stop);
                                seeder_shutting_down_clone.store(true, Ordering::Release);
                                return;
                            }
                            TorrentEvent::MetadataComplete(_) => {
                                log::info!("Downloader: Metadata complete");
                            }
                        }
                    }
                }
            })
        });

        downloader_handle.join().unwrap();
        seeder_handle.join().unwrap();
    });

    verify_downloaded_files(
        &downloader_dir,
        &torrent_name,
        &expected_hashes,
        "downloader",
    );
}
