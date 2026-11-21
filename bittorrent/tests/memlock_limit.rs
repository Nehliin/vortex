//! Resource limits are process-wide, so this test has its own integration binary.

use std::{error::Error, net::TcpListener};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

fn start(config: Config) -> Result<(), Box<dyn Error>> {
    let mut events: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (event_tx, _event_rc) = events.split();
    let (command_tx, command_rc) = std::sync::mpsc::sync_channel(1);
    // Kernels may permit creation even at MEMLOCK=0; never wait indefinitely.
    command_tx.send(Command::Stop).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let state = State::unstarted(PeerId::generate(), [0; 20], std::env::temp_dir(), config);
    Torrent::new(state)
        .start(event_tx, command_rc, listener)
        .map_err(Into::into)
}

fn assert_budget_accepted(result: Result<(), Box<dyn Error>>) {
    if let Err(error) = result {
        // A passed policy check cannot guarantee the kernel has shared capacity left.
        assert_eq!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<std::io::Error>())
                .and_then(|source| source.raw_os_error()),
            Some(libc::ENOMEM),
            "unexpected rejection after satisfying the budget: {error}"
        );
    }
}

fn set_soft_limit(limit: &mut libc::rlimit, bytes: libc::rlim_t) {
    limit.rlim_cur = bytes;
    // SAFETY: `limit` is initialized; only this test process's soft limit changes.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, limit) }, 0);
}

#[test]
fn memlock_budget_is_checked_before_starting() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is writable storage for the duration of the call.
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) },
        0
    );
    set_soft_limit(&mut limit, 0);
    let strict = Config {
        memlock_strict: true,
        ..Config::default()
    };
    let error = start(strict)
        .expect_err("strict mode rejects a zero budget")
        .to_string();
    assert!(error.contains("RLIMIT_MEMLOCK"), "{error}");
    assert!(error.contains("headroom"), "{error}");
    assert!(error.contains("ulimit -Hl"), "{error}");

    // Verify the command offered to the user actually permits startup.
    let required_kib: libc::rlim_t = error
        .split_once("ulimit -Sl ")
        .unwrap()
        .1
        .split('`')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    set_soft_limit(&mut limit, required_kib * 1024);
    assert_budget_accepted(start(strict));
    set_soft_limit(&mut limit, required_kib * 1024 - 1);
    assert!(start(strict).is_err(), "headroom is required too");
    assert_budget_accepted(start(Config {
        memlock_headroom_percent: 0,
        ..strict
    }));

    let small = Config {
        sq_size: 64,
        cq_size: 64,
        ..strict
    };
    assert_budget_accepted(start(small));
    for config in [
        Config {
            cq_size: 65536,
            ..small
        },
        Config {
            sq_size: 32768,
            cq_size: 32768,
            ..small
        },
        Config {
            read_buffer_pool_size: 32768,
            ..small
        },
    ] {
        assert!(start(config).is_err(), "the larger allocation must not fit");
    }
}
