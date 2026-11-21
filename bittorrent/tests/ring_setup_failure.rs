//! Requires strace to exercise real ring-creation and registration failures.

use std::{error::Error as _, net::TcpListener, process::Command as ProcessCommand};

use heapless::spsc;
use vortex_bittorrent::{Command, Config, PeerId, State, Torrent, TorrentEvent};

const CHILD: &str = "VORTEX_TEST_RING_FAILURE_ERRNO";
const LOW_MEMLOCK: &str = "VORTEX_TEST_RING_FAILURE_LOW_MEMLOCK";

#[test]
fn ring_initialization_failures_preserve_errno_and_guidance() {
    if let Ok(errno) = std::env::var(CHILD) {
        let errno: i32 = errno.parse().unwrap();
        if std::env::var(LOW_MEMLOCK).as_deref() == Ok("1") {
            let mut limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: initialized writable output storage outlives the call.
            assert_eq!(
                unsafe { libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut limit) },
                0
            );
            limit.rlim_cur = 0;
            // SAFETY: initialized limits; only this child process's soft limit changes.
            assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &limit) }, 0);
        }
        let mut events: spsc::Queue<TorrentEvent, 512> = spsc::Queue::new();
        let (event_tx, _event_rc) = events.split();
        let (command_tx, command_rc) = std::sync::mpsc::sync_channel(1);
        // If injection stops working, fail an assertion instead of waiting for peers.
        command_tx.send(Command::Stop).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let state = State::unstarted(
            PeerId::generate(),
            [0; 20],
            std::env::temp_dir(),
            Config {
                max_connections: 1,
                sq_size: 8,
                cq_size: 8,
                read_buffer_pool_size: 8,
                ..Config::default()
            },
        );
        let error = Torrent::new(state)
            .start(event_tx, command_rc, listener)
            .expect_err("the injected allocation failure must reach the caller");
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<std::io::Error>()
                .unwrap()
                .raw_os_error(),
            Some(errno)
        );
        let message = error.to_string();
        assert_eq!(
            message.contains("RLIMIT_MEMLOCK"),
            errno == libc::ENOMEM,
            "{message}"
        );
        if errno == libc::ENOMEM {
            assert!(message.contains("ulimit -Sl"), "{message}");
        }
        return;
    }

    for (syscall, name, errno, low_memlock) in [
        ("io_uring_setup", "ENOMEM", libc::ENOMEM, false),
        ("io_uring_register", "ENOMEM", libc::ENOMEM, false),
        ("io_uring_setup", "EINVAL", libc::EINVAL, false),
        ("io_uring_register", "EINVAL", libc::EINVAL, false),
        ("io_uring_register", "EEXIST", libc::EEXIST, false),
        ("io_uring_setup", "ENOMEM", libc::ENOMEM, true),
    ] {
        let output = ProcessCommand::new("strace")
            .arg("-f")
            .arg("-e")
            .arg(format!("trace={syscall}"))
            .arg("-e")
            .arg(format!("inject={syscall}:error={name}:when=1"))
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "ring_initialization_failures_preserve_errno_and_guidance",
                "--nocapture",
            ])
            .env(CHILD, errno.to_string())
            .env(LOW_MEMLOCK, if low_memlock { "1" } else { "0" })
            .output()
            .expect("strace must be installed for the initialization fault-injection test");
        let trace = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "{syscall} regression failed:\n{}\n{trace}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(trace.contains("(INJECTED)"), "{syscall}: {trace}");
    }
}
