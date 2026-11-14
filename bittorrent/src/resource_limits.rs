use std::{fs, io};

#[cfg(any(target_env = "gnu", target_env = "uclibc"))]
use libc::__rlimit_resource_t as RlimitResource;
#[cfg(not(any(target_env = "gnu", target_env = "uclibc")))]
use libc::c_int as RlimitResource;

use crate::torrent::{Config, Error};

// Spare slots for temporary files, accepted sockets, and the embedding application.
const FD_HEADROOM: u128 = 32;

pub(crate) fn read_limit(resource: RlimitResource) -> io::Result<libc::rlimit> {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: the output points to initialized, writable storage that outlives the call.
    if unsafe { libc::getrlimit(resource, &mut limit) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(limit)
}

fn positive_sysconf(name: libc::c_int) -> io::Result<u128> {
    // SAFETY: sysconf takes no pointers and reports unsupported query names through errno.
    let value = unsafe { libc::sysconf(name) };
    if value == -1 {
        return Err(io::Error::last_os_error());
    }
    if value <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Expected a positive system configuration value",
        ));
    }
    Ok(value as u128)
}

fn page_size() -> io::Result<u128> {
    let size = positive_sysconf(libc::_SC_PAGESIZE)?;
    if !size.is_power_of_two() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "System page size is not a power of two",
        ));
    }
    Ok(size)
}

fn memlock_budget(storage: u128, percent: u32) -> Result<(u128, u128), Error> {
    let overflow = || Error::Configuration("MEMLOCK budget overflow".into());
    let headroom = storage
        .checked_mul(u128::from(percent))
        .ok_or_else(overflow)?
        .div_ceil(100);
    let required = storage.checked_add(headroom).ok_or_else(overflow)?;
    Ok((headroom, required))
}

fn memlock_inspection_failure(
    strict: bool,
    resource: &'static str,
    source: io::Error,
) -> Result<Option<String>, Error> {
    let error = Error::LimitInspection { resource, source };
    if strict {
        Err(error)
    } else {
        Ok(Some(format!(
            "{error}\nMEMLOCK check unavailable; attempting startup because memlock_strict=false."
        )))
    }
}

fn memlock_diagnostic(
    config: &Config,
    limit: io::Result<libc::rlimit>,
    page_size: io::Result<u128>,
) -> Result<Option<String>, Error> {
    let config = config.validate_ring_config()?;
    let limit = match limit {
        Ok(limit) => limit,
        Err(error) => {
            return memlock_inspection_failure(config.memlock_strict, "RLIMIT_MEMLOCK", error);
        }
    };
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return Ok(None);
    }
    let page_size = match page_size {
        Ok(size) => size,
        Err(error) => {
            return memlock_inspection_failure(config.memlock_strict, "system page size", error);
        }
    };
    let storage = config.estimated_ring_storage_bytes(page_size)?;
    let (headroom, required) = memlock_budget(storage, config.memlock_headroom_percent)?;
    if limit.rlim_cur as u128 >= required {
        return Ok(None);
    }
    let required_kib = required.div_ceil(1024);
    let shell_bytes = required_kib
        .checked_mul(1024)
        .ok_or_else(|| Error::Configuration("MEMLOCK shell setting overflow".into()))?;
    let guidance = if limit.rlim_max == libc::RLIM_INFINITY || shell_bytes <= limit.rlim_max as u128
    {
        format!(
            "Run `ulimit -Sl {required_kib}` in the launching shell, or reduce queue sizes/headroom."
        )
    } else {
        format!(
            "The shell setting of {required_kib} KiB exceeds the hard limit. Reduce queue sizes/headroom or arrange a higher hard limit."
        )
    };
    let mut message = format!(
        "Estimated ring storage plus headroom exceeds the RLIMIT_MEMLOCK soft limit.\n\
         Storage: {storage} bytes; headroom: {headroom} bytes ({}%); budget: {required} bytes.\n\
         Effective SQ: {}; CQ: {}; buffer-ring entries: {}.\n\
         Limits in bytes: soft {}; hard {} (`ulimit -Sl` / `ulimit -Hl` show KiB).\n\
         {guidance}\nKernel accounting varies; the soft limit is not remaining shared capacity.",
        config.memlock_headroom_percent,
        config.sq_size,
        config.cq_size,
        config.read_buffer_pool_size,
        limit_text(limit.rlim_cur),
        limit_text(limit.rlim_max),
    );
    if config.memlock_strict {
        Err(Error::ResourceBudget(message))
    } else {
        message.push_str("\nAttempting startup because memlock_strict=false.");
        Ok(Some(message))
    }
}

pub(crate) fn check_memlock_limit(config: &Config) -> Result<(), Error> {
    if let Some(warning) =
        memlock_diagnostic(config, read_limit(libc::RLIMIT_MEMLOCK), page_size())?
    {
        log::warn!("{warning}");
    }
    Ok(())
}

pub(crate) fn limit_text(limit: libc::rlim_t) -> String {
    if limit == libc::RLIM_INFINITY {
        "unlimited".to_owned()
    } else {
        limit.to_string()
    }
}

fn descriptor_inspection_error(source: io::Error, limit: &libc::rlimit) -> Error {
    match source.raw_os_error() {
        Some(libc::EMFILE) => {
            let guidance = if limit.rlim_max == libc::RLIM_INFINITY
                || limit.rlim_cur < limit.rlim_max
            {
                format!(
                    "Close unused files/sockets, or run `ulimit -Sn {}` in the launching shell. \
                     Check the hard limit with `ulimit -Hn`.",
                    limit_text(limit.rlim_max)
                )
            } else {
                "Close unused files/sockets, or arrange a higher hard limit. \
                 The soft limit already equals the hard limit (`ulimit -Hn`)."
                    .to_owned()
            };
            Error::FileDescriptorsExhausted {
                source,
                soft_limit: limit_text(limit.rlim_cur),
                hard_limit: limit_text(limit.rlim_max),
                guidance,
            }
        }
        Some(libc::ENFILE) => Error::SystemFileDescriptorsExhausted(source),
        _ => Error::Io(source),
    }
}

fn minimum_descriptor_limit(occupied: &[u32], additional: u128) -> Result<u128, Error> {
    let mut required = additional;
    for &fd in occupied {
        if u128::from(fd) >= required {
            break;
        }
        required = required
            .checked_add(1)
            .ok_or_else(|| Error::Configuration("File descriptor budget overflow".into()))?;
    }
    Ok(required)
}

fn check_descriptor_budget(
    max_connections: usize,
    limit: &libc::rlimit,
    descriptors: impl Iterator<Item = io::Result<u32>>,
) -> Result<(), Error> {
    let additional = (max_connections as u128)
        .checked_add(FD_HEADROOM)
        .ok_or_else(|| Error::Configuration("File descriptor budget overflow".into()))?;
    let mut occupied = descriptors
        .collect::<io::Result<Vec<_>>>()
        .map_err(|error| descriptor_inspection_error(error, limit))?;
    occupied.sort_unstable();
    occupied.dedup();
    let required = minimum_descriptor_limit(&occupied, additional)?;
    if limit.rlim_cur == libc::RLIM_INFINITY || limit.rlim_cur as u128 >= required {
        return Ok(());
    }

    let used = occupied
        .iter()
        .filter(|&&fd| u128::from(fd) < limit.rlim_cur as u128)
        .count() as u128;
    let available = (limit.rlim_cur as u128)
        .checked_sub(used)
        .ok_or_else(|| Error::Configuration("Invalid file descriptor snapshot".into()))?;
    let guidance = if limit.rlim_max == libc::RLIM_INFINITY || required <= limit.rlim_max as u128 {
        format!("Run `ulimit -Sn {required}` in the launching shell before retrying.")
    } else {
        format!(
            "This snapshot needs a limit of at least {required}, above the current hard limit. \
             Close files, reduce max_connections, or arrange a higher hard limit (`ulimit -Hn`)."
        )
    };
    Err(Error::ResourceBudget(format!(
        "Not enough file descriptor slots (RLIMIT_NOFILE).\n\
         After allowing for the new ring, {available} slots are available; \
         {additional} are needed ({max_connections} peers + {FD_HEADROOM} spare).\n\
         Soft limit: {}; hard limit: {}.\n{guidance}",
        limit_text(limit.rlim_cur),
        limit_text(limit.rlim_max),
    )))
}

/// A snapshot of this thread's descriptor table, not a reservation for future opens.
pub(crate) fn check_file_descriptor_limit(max_connections: usize) -> Result<(), Error> {
    let limit = read_limit(libc::RLIMIT_NOFILE).map_err(|source| Error::LimitInspection {
        resource: "RLIMIT_NOFILE",
        source,
    })?;
    if limit.rlim_cur == libc::RLIM_INFINITY {
        return Ok(());
    }
    let entries = fs::read_dir("/proc/thread-self/fd")
        .map_err(|error| descriptor_inspection_error(error, &limit))?;
    // The inspection FD stands in for the ring FD opened next; the listener is already present.
    let descriptors = entries.map(|entry| {
        entry?
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid descriptor name"))
    });
    check_descriptor_budget(max_connections, &limit, descriptors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn descriptor_budget_handles_holes_and_higher_existing_fds() {
        let occupied: Vec<_> = (0..4).chain(1000..1040).collect();
        let limit = libc::rlimit {
            rlim_cur: 64,
            rlim_max: 128,
        };
        check_descriptor_budget(16, &limit, occupied.iter().copied().map(Ok)).unwrap();
        assert_eq!(minimum_descriptor_limit(&occupied, 48).unwrap(), 52);

        let occupied: Vec<_> = (0..4).chain(64..100).collect();
        let error =
            check_descriptor_budget(64, &limit, occupied.iter().copied().map(Ok)).unwrap_err();
        assert!(error.to_string().contains("136"));
        assert!(!error.to_string().contains("ulimit -Sn"));
        let raised = libc::rlimit {
            rlim_cur: 64,
            rlim_max: 256,
        };
        let error =
            check_descriptor_budget(64, &raised, occupied.iter().copied().map(Ok)).unwrap_err();
        assert!(error.to_string().contains("ulimit -Sn 136"));
    }

    #[test]
    fn descriptor_budget_accepts_exact_and_unlimited_limits() {
        for (soft, expected) in [(51, false), (52, true), (libc::RLIM_INFINITY, true)] {
            let limit = libc::rlimit {
                rlim_cur: soft,
                rlim_max: libc::RLIM_INFINITY,
            };
            assert_eq!(
                check_descriptor_budget(16, &limit, (0..4).map(Ok)).is_ok(),
                expected
            );
        }
        assert_eq!(limit_text(libc::RLIM_INFINITY), "unlimited");
        assert!(minimum_descriptor_limit(&[0], u128::MAX).is_err());
    }

    #[test]
    fn inspection_errors_keep_their_cause_and_distinguish_exhaustion() {
        let limit = libc::rlimit {
            rlim_cur: 64,
            rlim_max: 64,
        };
        for errno in [libc::EMFILE, libc::ENFILE, libc::EIO] {
            let error = check_descriptor_budget(
                16,
                &limit,
                [Ok(0), Err(io::Error::from_raw_os_error(errno))].into_iter(),
            )
            .unwrap_err();
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<io::Error>()
                    .unwrap()
                    .raw_os_error(),
                Some(errno)
            );
            assert!(!error.to_string().contains("ulimit -Sn"));
            if errno == libc::ENFILE {
                assert!(error.to_string().contains("system-wide"));
            }
        }
    }

    #[test]
    fn limit_read_failure_preserves_errno() {
        assert_eq!(
            read_limit(RlimitResource::MAX).unwrap_err().raw_os_error(),
            Some(libc::EINVAL)
        );
    }

    #[test]
    fn sysconf_failure_preserves_errno() {
        assert_eq!(
            positive_sysconf(-1).unwrap_err().raw_os_error(),
            Some(libc::EINVAL)
        );
        assert!(page_size().unwrap().is_power_of_two());
    }

    #[test]
    fn memlock_policy_is_advisory_unless_strict_is_selected() {
        let config = Config::default();
        let limit = || {
            Ok(libc::rlimit {
                rlim_cur: 0,
                rlim_max: 8 * 1024 * 1024,
            })
        };
        let warning = memlock_diagnostic(&config, limit(), Ok(4096))
            .unwrap()
            .unwrap();
        assert!(warning.contains("ulimit -Sl 696"));
        assert!(warning.contains("memlock_strict=false"));
        let strict = Config {
            memlock_strict: true,
            ..config
        };
        assert!(memlock_diagnostic(&strict, limit(), Ok(4096)).is_err());
        for (soft, accepted) in [(712703, false), (712704, true)] {
            let limit = Ok(libc::rlimit {
                rlim_cur: soft,
                rlim_max: 8388608,
            });
            assert_eq!(
                memlock_diagnostic(&strict, limit, Ok(4096)).is_ok(),
                accepted
            );
        }
        let no_headroom = Config {
            memlock_headroom_percent: 0,
            ..strict
        };
        assert!(
            memlock_diagnostic(
                &no_headroom,
                Ok(libc::rlimit {
                    rlim_cur: 356352,
                    rlim_max: 8388608
                }),
                Ok(4096)
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(memlock_budget(3, 50).unwrap(), (2, 5));
        assert!(memlock_budget(u128::MAX, 1).is_err());
    }

    #[test]
    fn memlock_diagnostics_respect_hard_and_unlimited_limits() {
        let config = Config::default();
        let limit = |soft, hard| {
            Ok(libc::rlimit {
                rlim_cur: soft,
                rlim_max: hard,
            })
        };
        let warning = memlock_diagnostic(&config, limit(0, 0), Ok(4096))
            .unwrap()
            .unwrap();
        assert!(!warning.contains("ulimit -Sl 696"));
        assert!(warning.contains("exceeds the hard limit"));
        let warning = memlock_diagnostic(&config, limit(0, libc::RLIM_INFINITY), Ok(4096))
            .unwrap()
            .unwrap();
        assert!(warning.contains("hard unlimited"));
        assert!(warning.contains("ulimit -Sl 696"));
        assert!(
            memlock_diagnostic(
                &config,
                limit(libc::RLIM_INFINITY, libc::RLIM_INFINITY),
                Err(io::Error::from_raw_os_error(libc::EINVAL))
            )
            .unwrap()
            .is_none()
        );
        let invalid = Config {
            sq_size: 0,
            ..config
        };
        assert!(
            memlock_diagnostic(
                &invalid,
                limit(libc::RLIM_INFINITY, libc::RLIM_INFINITY),
                Ok(4096)
            )
            .is_err()
        );
        let fractional = Config {
            memlock_headroom_percent: 1,
            ..config
        };
        let warning = memlock_diagnostic(&fractional, limit(0, 359916), Ok(4096))
            .unwrap()
            .unwrap();
        assert!(warning.contains("exceeds the hard limit"));
        assert!(!warning.contains("ulimit -Sl 352"));
    }

    #[test]
    fn memlock_inspection_errors_warn_or_preserve_the_source_in_strict_mode() {
        for strict in [false, true] {
            let config = Config {
                memlock_strict: strict,
                ..Config::default()
            };
            for failed_limit in [true, false] {
                let limit = if failed_limit {
                    Err(io::Error::from_raw_os_error(libc::EPERM))
                } else {
                    Ok(libc::rlimit {
                        rlim_cur: 8192,
                        rlim_max: 8192,
                    })
                };
                let page = if failed_limit {
                    Ok(4096)
                } else {
                    Err(io::Error::from_raw_os_error(libc::EPERM))
                };
                let result = memlock_diagnostic(&config, limit, page);
                if strict {
                    let error = result.unwrap_err();
                    assert_eq!(
                        error
                            .source()
                            .unwrap()
                            .downcast_ref::<io::Error>()
                            .unwrap()
                            .raw_os_error(),
                        Some(libc::EPERM)
                    );
                } else {
                    assert!(result.unwrap().unwrap().contains("check unavailable"));
                }
            }
        }
    }

    #[test]
    fn descriptor_churn_does_not_break_snapshot_inspection() {
        use std::sync::{
            Barrier,
            atomic::{AtomicBool, Ordering},
        };
        let stop = AtomicBool::new(false);
        let ready = Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                ready.wait();
                let mut file = fs::File::open("/dev/null").unwrap();
                while !stop.load(Ordering::Relaxed) {
                    file = fs::File::open("/dev/null").unwrap();
                }
                drop(file);
            });
            ready.wait();
            let result = (0..32).try_for_each(|_| check_file_descriptor_limit(0));
            stop.store(true, Ordering::Relaxed);
            result.unwrap();
        });
    }
}
