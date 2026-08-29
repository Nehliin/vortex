mod common;

use std::{collections::HashMap, net::TcpListener, sync::mpsc::RecvTimeoutError, time::Duration};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

use common::{create_test_torrent, init_test_environment};

/// How long the event loop is given to shut down after being asked to. Only ever
/// waited out when the shutdown is broken, a working one takes well under a second
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(15);

/// A paused torrent iterates its command channel with the blocking iterator, so
/// `Stop` has to be enough on its own to get the event loop out of there. It used
/// to only return once the channel disconnected on top of that, which hangs any
/// client that keeps its command sender alive while joining the torrent thread.
#[test]
fn stop_while_paused_shuts_down() {
    init_test_environment();

    let test_files: HashMap<String, Vec<u8>> =
        [("file.txt".to_string(), b"BitTorrent Test Data!".repeat(200))]
            .into_iter()
            .collect();

    let torrent_name = format!("test_pause_shutdown_{}", rand::random::<u32>());
    let (torrent_dir, metadata) = create_test_torrent(&test_files, &torrent_name, 16384);

    let state = State::from_metadata_and_root(
        PeerId::generate(),
        metadata,
        torrent_dir.path().clone(),
        Config::default(),
    )
    .expect("Failed to create state");
    let mut torrent = Torrent::new(state);

    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(64);
    // Leaked so that the event loop can run on a detached thread: a shutdown that
    // never happens then fails the assertion below instead of blocking the test
    // forever on a join
    let event_q: &'static mut spsc::Queue<TorrentEvent, 512> =
        Box::leak(Box::new(spsc::Queue::new()));
    let (event_tx, mut event_rc) = event_q.split();

    let (stopped_tx, stopped_rc) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        torrent.start(event_tx, command_rc, listener).unwrap();
        stopped_tx.send(()).unwrap();
    });

    let mut pause_sent = false;
    loop {
        let Some(event) = event_rc.dequeue() else {
            continue;
        };
        match event {
            TorrentEvent::Running { port } => {
                log::info!("Running on port {port}, requesting pause");
                assert!(!pause_sent, "The torrent should never be resumed");
                command_tx.send(Command::Pause).unwrap();
                pause_sent = true;
            }
            TorrentEvent::Paused => {
                log::info!("Paused, requesting shutdown");
                command_tx.send(Command::Stop).unwrap();
                break;
            }
            _ => {}
        }
    }

    // `command_tx` is deliberately kept alive here, so the command channel never
    // disconnects and only the `Stop` itself can end the event loop
    match stopped_rc.recv_timeout(SHUTDOWN_TIMEOUT) {
        Ok(()) => log::info!("The event loop shut down while paused"),
        Err(RecvTimeoutError::Timeout) => {
            panic!("The event loop did not shut down after being stopped while paused")
        }
        Err(RecvTimeoutError::Disconnected) => panic!("The event loop thread died"),
    }
    drop(command_tx);
}
