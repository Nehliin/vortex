mod common;

use std::{
    collections::HashMap,
    net::{SocketAddr, SocketAddrV4, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, Ordering},
    },
    time::{Duration, Instant},
};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

use common::{TempDir, create_test_torrent, init_test_environment};

const TIMEOUT: u64 = 120;
/// How long the banned peer is given to actually go away after the ban was sent
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A piece whose last subpiece arrived just before the connection was closed may
/// still finish hashing afterwards, so the download is only required to have
/// stopped making progress once it has been given this long to settle
const SETTLE_TIME: Duration = Duration::from_secs(3);
/// How long the download is watched after the disconnect to ensure it stays aborted
const ABORTED_OBSERVATION: Duration = Duration::from_secs(8);

/// What the downloader is currently waiting for
#[derive(Clone, Copy)]
enum Phase {
    /// Downloading from the seeder, waiting for enough progress to ban it mid download
    Downloading,
    /// The ban has been sent, waiting for the seeder to be disconnected
    Banned { since: Instant },
    /// The seeder is gone, making sure the download stays aborted
    Aborted {
        since: Instant,
        /// The completed piece count once the inflight pieces were given time to
        /// finish, `None` until the download has been left alone for [`SETTLE_TIME`]
        settled_progress: Option<usize>,
    },
}

/// Banning the only peer we are downloading from mid download disconnects it and
/// aborts the download instead of finishing it
#[test]
fn banned_peer_aborts_download() {
    init_test_environment();

    // Enough pieces that the download is guaranteed to still be in progress by
    // the time the first completed piece is reported
    let test_files: HashMap<String, Vec<u8>> = [
        (
            "file2.txt".to_string(),
            b"BitTorrent Test Data!".repeat(200),
        ),
        ("subdir/file3.txt".to_string(), vec![42u8; 16384 * 5_000]),
    ]
    .into_iter()
    .collect();

    let torrent_name = format!("test_ban_peer_{}", rand::random::<u32>());

    let (seeder_dir, metadata) = create_test_torrent(&test_files, &torrent_name, 16384);
    let num_pieces = metadata.pieces.len();
    let tmp_dir_path = seeder_dir.path().clone();

    let downloader_dir = TempDir::new(&format!("{}_downloader", torrent_name));
    let downloader_path = downloader_dir.path().clone();

    // TODO: Fix this in a better way
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev_hook(info);
        // hacky clean up if a panic happens
        let _ = std::fs::remove_dir_all(&tmp_dir_path);
        let _ = std::fs::remove_dir_all(&downloader_path);
        std::process::abort();
    }));

    // Set up seeder with the completed files
    let seeder_id = PeerId::generate();
    let seeder_state = State::from_metadata_and_root(
        seeder_id,
        metadata.clone(),
        seeder_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create seeder state");
    let mut seeder_torrent = Torrent::new(seeder_state);

    // Set up downloader (empty)
    let downloader_id = PeerId::generate();
    let downloader_state = State::from_metadata_and_root(
        downloader_id,
        metadata.clone(),
        downloader_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create downloader state");
    let mut downloader_torrent = Torrent::new(downloader_state);

    let (downloader_command_tx, downloader_command_rc) = std::sync::mpsc::sync_channel(64);
    let downloader_command_tx_for_seeder = downloader_command_tx.clone();
    let (seeder_command_tx, seeder_command_rc) = std::sync::mpsc::sync_channel(64);

    let test_time = Instant::now();

    let seeder_shutting_down = Arc::new(AtomicBool::new(false));
    let seeder_shutting_down_clone = seeder_shutting_down.clone();

    // Shared so the downloader knows which address to ban
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

        // Seeder event handler thread, it observes the ban from the other side of
        // the connection: the downloader should be gone and stay gone
        let seeder_handle = s.spawn(move || {
            let mut saw_downloader = false;
            let mut saw_disconnect = false;

            loop {
                if test_time.elapsed() >= Duration::from_secs(TIMEOUT) {
                    panic!("Test timeout in seeder event handler");
                }

                while let Some(event) = seeder_event_rc.dequeue() {
                    match event {
                        TorrentEvent::Running { port } => {
                            log::info!("Seeder running on port {}", port);
                            seeder_port_for_seeder.store(port, Ordering::Release);
                            downloader_command_tx_for_seeder
                                .send(Command::ConnectToPeers(vec![
                                    format!("127.0.0.1:{}", port).parse().unwrap(),
                                ]))
                                .unwrap();
                        }
                        TorrentEvent::TorrentMetrics { peer_metrics, .. } => {
                            if peer_metrics.is_empty() {
                                if saw_downloader {
                                    saw_disconnect = true;
                                }
                            } else {
                                assert!(
                                    !saw_disconnect,
                                    "The banned downloader reconnected to the seeder"
                                );
                                saw_downloader = true;
                            }
                        }
                        TorrentEvent::Paused => panic!("Seeder should never pause"),
                        _ => {}
                    }
                }

                if seeder_shutting_down.load(Ordering::Acquire) {
                    break;
                }
            }
            (saw_downloader, saw_disconnect)
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

                let mut phase = Phase::Downloading;

                loop {
                    if test_time.elapsed() >= Duration::from_secs(TIMEOUT) {
                        panic!("Test timeout - the download never reached the point of the ban");
                    }

                    while let Some(event) = downloader_event_rc.dequeue() {
                        match event {
                            TorrentEvent::TorrentComplete => match phase {
                                Phase::Downloading => panic!(
                                    "The download completed before the seeder could be banned mid download"
                                ),
                                _ => panic!("The ban should have aborted the download"),
                            },
                            TorrentEvent::TorrentMetrics {
                                progress,
                                peer_metrics,
                                ..
                            } => {
                                let completed = progress.as_ref().map_or(0, |p| p.total_completed());
                                let connected_peers = peer_metrics.len();
                                log::debug!(
                                    "Downloader progress: {completed}/{num_pieces} pieces, {connected_peers} connected peers"
                                );
                                match phase {
                                    // Ban the seeder as soon as the download is
                                    // demonstrably underway but far from done
                                    Phase::Downloading if connected_peers > 0 && completed > 0 => {
                                        assert!(
                                            completed < num_pieces,
                                            "The seeder should be banned mid download, not after it finished"
                                        );
                                        let port = seeder_port.load(Ordering::Acquire);
                                        assert_ne!(port, 0, "The seeder port should be known by now");
                                        let seeder_addr: SocketAddrV4 =
                                            format!("127.0.0.1:{port}").parse().unwrap();
                                        log::info!(
                                            "Banning the seeder at {seeder_addr} after {completed}/{num_pieces} pieces"
                                        );
                                        downloader_command_tx
                                            .send(Command::BanPeer {
                                                peer_addr: SocketAddr::V4(seeder_addr),
                                            })
                                            .unwrap();
                                        phase = Phase::Banned {
                                            since: Instant::now(),
                                        };
                                    }
                                    Phase::Downloading => {}
                                    Phase::Banned { since } => {
                                        if connected_peers == 0 {
                                            log::info!("The banned seeder was disconnected");
                                            // The ban has to outlive the disconnect, so
                                            // reconnecting to it must be refused as well
                                            let port = seeder_port.load(Ordering::Acquire);
                                            downloader_command_tx
                                                .send(Command::ConnectToPeers(vec![
                                                    format!("127.0.0.1:{port}").parse().unwrap(),
                                                ]))
                                                .unwrap();
                                            phase = Phase::Aborted {
                                                since: Instant::now(),
                                                settled_progress: None,
                                            };
                                        } else {
                                            assert!(
                                                since.elapsed() < DISCONNECT_TIMEOUT,
                                                "The banned seeder is still connected {}s after the ban",
                                                since.elapsed().as_secs()
                                            );
                                        }
                                    }
                                    Phase::Aborted {
                                        since,
                                        settled_progress,
                                    } => {
                                        assert_eq!(
                                            connected_peers, 0,
                                            "The banned seeder must not be connected to again"
                                        );
                                        assert!(
                                            completed < num_pieces,
                                            "The download must not complete without the banned seeder"
                                        );
                                        match settled_progress {
                                            Some(settled) => assert_eq!(
                                                completed, settled,
                                                "The download must not make progress after the only peer was banned"
                                            ),
                                            None if since.elapsed() >= SETTLE_TIME => {
                                                phase = Phase::Aborted {
                                                    since,
                                                    settled_progress: Some(completed),
                                                }
                                            }
                                            None => {}
                                        }
                                    }
                                }
                            }
                            TorrentEvent::MetadataComplete(_) => {
                                log::info!("Downloader: Metadata complete");
                            }
                            TorrentEvent::Paused => panic!("Downloader should never pause"),
                            _ => {}
                        }
                    }

                    if let Phase::Aborted {
                        since,
                        settled_progress,
                    } = phase
                        && since.elapsed() >= ABORTED_OBSERVATION
                    {
                        let settled_progress = settled_progress
                            .expect("The download should have reported metrics while aborted");
                        let _ = downloader_command_tx.send(Command::Stop);
                        let _ = seeder_command_tx.send(Command::Stop);
                        seeder_shutting_down_clone.store(true, Ordering::Release);
                        return settled_progress;
                    }
                }
            })
        });

        let final_progress = downloader_handle.join().unwrap();
        let (seeder_saw_downloader, seeder_saw_disconnect) = seeder_handle.join().unwrap();

        assert!(
            final_progress < num_pieces,
            "The download should have been left incomplete, got {final_progress}/{num_pieces} pieces"
        );
        assert!(
            seeder_saw_downloader,
            "The seeder never saw the downloader connect"
        );
        assert!(
            seeder_saw_disconnect,
            "The seeder never saw the banned downloader disconnect"
        );
        log::info!(
            "Download aborted at {final_progress}/{num_pieces} pieces after banning the seeder"
        );
    });
}
