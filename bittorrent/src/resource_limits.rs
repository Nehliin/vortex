use std::io;

#[cfg(any(target_env = "gnu", target_env = "uclibc"))]
use libc::__rlimit_resource_t as RlimitResource;
#[cfg(not(any(target_env = "gnu", target_env = "uclibc")))]
use libc::c_int as RlimitResource;

use crate::torrent::{Config, Error};

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

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
}
