//! Invalid queue sizes exercise setup failure without relying on MEMLOCK accounting.

use std::{error::Error as _, net::TcpListener};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

#[test]
fn ring_setup_failure_is_returned_to_the_caller() {
    let mut event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (event_tx, _event_rc) = event_q.split();
    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(1);
    // If setup unexpectedly succeeds, stop instead of waiting for peers.
    command_tx.send(Command::Stop).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();

    let state = State::unstarted(
        PeerId::generate(),
        [0; 20],
        std::env::temp_dir(),
        Config {
            sq_size: 0,
            ..Config::default()
        },
    );
    let mut torrent = Torrent::new(state);

    let error = torrent
        .start(event_tx, command_rc, listener)
        .expect_err("a zero submission queue must fail ring setup");
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(libc::EINVAL)
    );
}
