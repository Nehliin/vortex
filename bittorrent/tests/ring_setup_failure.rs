//! `Torrent::start` must report a failed io_uring setup to the caller rather
//! than unwinding the thread it runs on.
//!
//! This lives in its own integration test file on purpose. The test lowers
//! `RLIMIT_MEMLOCK`, which is process-wide, and cargo gives each file in
//! `tests/` its own binary and therefore its own process. Putting it next to
//! other tests would let the lowered limit leak into whatever ran in parallel.
//!
//! Unlike the other integration tests here, this one needs no seeded peer and
//! no `assets/` fixture: setup fails before the event loop starts, so an
//! unstarted state with a dummy info hash is enough.

use std::net::TcpListener;

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

/// Drop `RLIMIT_MEMLOCK` to zero for this process.
///
/// io_uring charges the memory backing a ring against this limit, so zeroing
/// it makes `io_uring_setup` fail with `ENOMEM`. That is the same failure a
/// client hits for real once it has created enough rings — the limit is a
/// byte budget shared across all of the user's processes, and at the default
/// `sq_size`/`cq_size` a single ring already costs a sizeable fraction of it.
///
/// Only the soft limit is lowered, and lowering never requires privileges.
fn zero_memlock_limit() {
    let limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is fully initialized and outlives the call, and
    // RLIMIT_MEMLOCK is a valid resource.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &limit) };
    assert_eq!(
        rc,
        0,
        "could not lower RLIMIT_MEMLOCK: {}",
        std::io::Error::last_os_error()
    );
}

#[test]
fn ring_setup_failure_is_returned_to_the_caller() {
    zero_memlock_limit();

    let mut event_q: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
    let (event_tx, _event_rc) = event_q.split();
    let (_command_tx, command_rc) = std::sync::mpsc::sync_channel::<Command>(1);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();

    let state = State::unstarted(
        PeerId::generate(),
        [0; 20],
        std::env::temp_dir(),
        Config::default(),
    );
    let mut torrent = Torrent::new(state);

    // Reaching this assertion at all is most of the point: `start` used to
    // unwrap the ring build, so an exhausted RLIMIT_MEMLOCK panicked here
    // instead of returning.
    let result = torrent.start(event_tx, command_rc, listener);

    assert!(
        result.is_err(),
        "expected io_uring setup to fail with RLIMIT_MEMLOCK at zero"
    );
}
