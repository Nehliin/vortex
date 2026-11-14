//! Keep process-wide resource limit changes isolated from other integration tests.

use std::{
    error::Error as _,
    net::TcpListener,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

fn start(max_connections: usize) -> Result<(), String> {
    let mut event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (event_tx, _event_rc) = event_q.split();
    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(1);
    // A missing preflight must fail the assertion rather than leave the test running.
    command_tx.send(Command::Stop).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let state = State::unstarted(
        PeerId::generate(),
        [0; 20],
        std::env::temp_dir(),
        Config {
            max_connections,
            ..Config::default()
        },
    );
    Torrent::new(state)
        .start(event_tx, command_rc, listener)
        .map_err(|err| err.to_string())
}

#[test]
fn insufficient_file_descriptor_limit_is_rejected_before_starting() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is valid writable storage for the duration of the call.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit.rlim_cur = 64;
    // SAFETY: `limit` is initialized; only this test process's soft limit changes.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);

    let error = start(128).expect_err("a limit of 64 cannot support 128 peer connections");
    assert!(error.contains("RLIMIT_NOFILE"), "{error}");
    assert!(error.contains("64"), "{error}");
    assert!(error.contains("ulimit -Sn"), "{error}");

    assert!(start(32).is_err(), "startup must leave descriptor headroom");
    start(16).expect("a smaller configured connection cap should fit");

    let files: Vec<_> = (0..24)
        .map(|_| std::fs::File::open("/dev/null").unwrap())
        .collect();
    assert!(start(16).is_err(), "already-open files must count too");
    drop(files);
    start(16).expect("closing files should restore the available budget");

    // The listener replaces this inspection FD; startup also needs a ring FD.
    let open_files = std::fs::read_dir("/proc/self/fd").unwrap().count();
    let available_connections = 64 - open_files - 32 - 1;
    start(available_connections).expect("an exactly sufficient limit should be accepted");
    assert!(start(available_connections + 1).is_err());

    assert!(start(usize::MAX).is_err(), "the budget must not overflow");

    let mut events: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (event_tx, _event_rc) = events.split();
    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(1);
    command_tx.send(Command::Stop).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let state = State::unstarted(
        PeerId::generate(),
        [0; 20],
        std::env::temp_dir(),
        Config::default(),
    );
    // Bind the listener before filling the last slots, so inspection hits EMFILE.
    let mut files = Vec::new();
    loop {
        match std::fs::File::open("/dev/null") {
            Ok(file) => files.push(file),
            Err(error) => {
                assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
                break;
            }
        }
    }
    let error = Torrent::new(state)
        .start(event_tx, command_rc, listener)
        .expect_err("fully exhausted descriptors must still produce useful guidance");
    drop(files);
    assert_eq!(
        error
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .raw_os_error(),
        Some(libc::EMFILE)
    );
    let message = error.to_string();
    assert!(message.contains("RLIMIT_NOFILE"), "{message}");
    assert!(message.contains("64"), "{message}");
    assert!(message.contains("ulimit -Sn"), "{message}");
    assert!(message.contains("ulimit -Hn"), "{message}");

    if limit.rlim_max >= 128 {
        limit.rlim_cur = 128;
        // SAFETY: initialized limits; only this test process's soft limit changes.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
        let source = std::fs::File::open("/dev/null").unwrap();
        let mut high_files = Vec::new();
        for _ in 0..40 {
            // SAFETY: the source FD is live; F_DUPFD_CLOEXEC returns a new owned FD.
            let fd = unsafe { libc::fcntl(source.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 64) };
            assert!(fd >= 64);
            // SAFETY: this descriptor was just created and has no other owner.
            high_files.push(unsafe { OwnedFd::from_raw_fd(fd) });
        }
        limit.rlim_cur = 64;
        // SAFETY: initialized limits; existing higher descriptors remain valid.
        assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
        start(16).expect("descriptors above the lowered limit do not occupy its available slots");
    }
}
