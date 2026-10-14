//! `/proc/sys` writes for the calling thread's network namespace.
//!
//! Container runtimes often mount `/proc/sys` read-only; when a write fails
//! with `EROFS` the mount is flipped read-write for the batch and back
//! afterwards. Remounts are serialized process-wide so concurrent callers
//! never overlap a read-write window.

use std::io;

use parking_lot::Mutex;

/// One sysctl, keyed by its path below `/proc/sys` (e.g.
/// `net/ipv4/ip_forward`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sysctl {
    /// Path below `/proc/sys`.
    pub key: String,
    /// The value to write (a newline is appended).
    pub value: String,
}

impl Sysctl {
    /// A sysctl setting.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Sysctl {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// The filesystem operations sysctl writes need; replaced in tests.
pub trait ProcSys: Sync {
    /// Writes `data` to `path`.
    fn write_file(&self, path: &str, data: &[u8]) -> io::Result<()>;
    /// Remounts `/proc/sys` read-only or read-write.
    fn remount(&self, read_only: bool) -> io::Result<()>;
}

/// The host's `/proc/sys`.
pub struct HostProcSys;

impl ProcSys for HostProcSys {
    fn write_file(&self, path: &str, data: &[u8]) -> io::Result<()> {
        std::fs::write(path, data)
    }

    fn remount(&self, read_only: bool) -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            use nix::mount::{mount, MsFlags};
            let mut flags = MsFlags::MS_BIND | MsFlags::MS_REMOUNT;
            if read_only {
                flags |= MsFlags::MS_RDONLY;
            }
            mount(Some("none"), "/proc/sys", Some(""), flags, None::<&str>)
                .map_err(|e| io::Error::from_raw_os_error(e as i32))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = read_only;
            Err(io::Error::from(io::ErrorKind::Unsupported))
        }
    }
}

static PROC_SYS_MU: Mutex<()> = Mutex::new(());

/// Writes every sysctl in the calling thread's namespace.
pub fn set_sysctls(sysctls: &[Sysctl]) -> io::Result<()> {
    set_with(&HostProcSys, sysctls)
}

/// Writes every sysctl through `p`. Missing `net/ipv6/` keys are skipped
/// (IPv6 may be disabled); every other error is returned.
pub fn set_with(p: &dyn ProcSys, sysctls: &[Sysctl]) -> io::Result<()> {
    let _guard = PROC_SYS_MU.lock();
    let write_all = || -> io::Result<()> {
        for s in sysctls {
            let path = format!("/proc/sys/{}", s.key);
            match p.write_file(&path, format!("{}\n", s.value).as_bytes()) {
                Err(e) if e.kind() == io::ErrorKind::NotFound && s.key.starts_with("net/ipv6/") => {
                }
                Err(e) => {
                    return Err(io::Error::new(
                        e.kind(),
                        WrappedErrno {
                            errno: e.raw_os_error(),
                            msg: format!("while setting {} in the current netns: {e}", s.key),
                        },
                    ))
                }
                Ok(()) => {}
            }
        }
        Ok(())
    };
    match write_all() {
        Err(e) if errno_of(&e) == Some(libc::EROFS) => {}
        other => return other,
    }
    p.remount(false).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while remounting /proc/sys read-write: {e}"),
        )
    })?;
    let result = write_all();
    let _ = p.remount(true);
    result
}

/// An error message that remembers the errno it wraps.
#[derive(Debug)]
struct WrappedErrno {
    errno: Option<i32>,
    msg: String,
}

impl std::fmt::Display for WrappedErrno {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl std::error::Error for WrappedErrno {}

/// The errno of `e`, looking through [`WrappedErrno`].
pub fn errno_of(e: &io::Error) -> Option<i32> {
    e.raw_os_error().or_else(|| {
        e.get_ref()
            .and_then(|inner| inner.downcast_ref::<WrappedErrno>())
            .and_then(|w| w.errno)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    #[derive(Default)]
    struct FakeState {
        read_only: bool,
        values: HashMap<String, String>,
        writes: usize,
        remounted: i32,
        max_remounted: i32,
        remount_calls: usize,
    }

    #[derive(Default)]
    struct FakeProcSys(Mutex<FakeState>);

    impl FakeProcSys {
        fn new(read_only: bool, values: &[(&str, &str)]) -> Self {
            FakeProcSys(Mutex::new(FakeState {
                read_only,
                values: values
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                ..Default::default()
            }))
        }
    }

    impl ProcSys for FakeProcSys {
        fn write_file(&self, path: &str, data: &[u8]) -> io::Result<()> {
            let mut s = self.0.lock();
            if s.read_only {
                return Err(io::Error::from_raw_os_error(libc::EROFS));
            }
            s.values.insert(
                path.to_string(),
                String::from_utf8_lossy(data).trim().to_string(),
            );
            s.writes += 1;
            Ok(())
        }

        fn remount(&self, read_only: bool) -> io::Result<()> {
            let mut s = self.0.lock();
            s.remount_calls += 1;
            if read_only {
                s.read_only = true;
                s.remounted -= 1;
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
            s.read_only = false;
            s.remounted += 1;
            s.max_remounted = s.max_remounted.max(s.remounted);
            drop(s);
            std::thread::sleep(Duration::from_millis(1));
            Ok(())
        }
    }

    struct FnProcSys<W, R>(W, R);
    impl<W, R> ProcSys for FnProcSys<W, R>
    where
        W: Fn(&str, &[u8]) -> io::Result<()> + Sync,
        R: Fn(bool) -> io::Result<()> + Sync,
    {
        fn write_file(&self, path: &str, data: &[u8]) -> io::Result<()> {
            (self.0)(path, data)
        }
        fn remount(&self, read_only: bool) -> io::Result<()> {
            (self.1)(read_only)
        }
    }

    fn forwarding() -> Vec<Sysctl> {
        [
            "net/ipv4/conf/all/forwarding",
            "net/ipv4/conf/default/forwarding",
            "net/ipv6/conf/all/forwarding",
            "net/ipv6/conf/default/forwarding",
        ]
        .into_iter()
        .map(|k| Sysctl::new(k, "0"))
        .collect()
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs root with a writable /proc/sys (a privileged container or host)"]
    fn set_sysctl_reports_an_unrelated_error() {
        let err =
            set_sysctls(&[Sysctl::new("net/ipv4/workernet_no_such_sysctl", "0")]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound, "{err}");
        let st = nix::sys::statvfs::statvfs("/proc/sys").unwrap();
        assert!(
            !st.flags().contains(nix::sys::statvfs::FsFlags::ST_RDONLY),
            "/proc/sys was left read-only"
        );
    }

    #[test]
    fn set_sysctl_serializes_the_remount() {
        let fake = FakeProcSys::new(true, &[]);
        std::thread::scope(|s| {
            let handles: Vec<_> = (0..16)
                .map(|i| {
                    let fake = &fake;
                    s.spawn(move || {
                        set_with(fake, &[Sysctl::new(format!("net/ipv4/caller_{i}"), "0")])
                    })
                })
                .collect();
            for (i, h) in handles.into_iter().enumerate() {
                h.join()
                    .unwrap()
                    .unwrap_or_else(|e| panic!("caller {i}: {e}"));
            }
        });
        let s = fake.0.lock();
        assert_eq!(s.max_remounted, 1);
        assert!(s.read_only, "/proc/sys was left read-write");
    }

    #[test]
    fn set_sysctls_batches_remount() {
        let fake = FakeProcSys::new(
            true,
            &[
                ("/proc/sys/net/ipv4/ip_unprivileged_port_start", "1024"),
                ("/proc/sys/net/ipv4/conf/all/forwarding", "1"),
                ("/proc/sys/net/ipv4/conf/default/forwarding", "1"),
                ("/proc/sys/net/ipv6/conf/all/forwarding", "1"),
                ("/proc/sys/net/ipv6/conf/default/forwarding", "1"),
            ],
        );
        let mut sysctls = vec![Sysctl::new("net/ipv4/ip_unprivileged_port_start", "0")];
        sysctls.extend(forwarding());
        set_with(&fake, &sysctls).unwrap();
        let s = fake.0.lock();
        assert_eq!(s.remount_calls, 2, "one rw, one ro");
        for (k, v) in &s.values {
            assert_eq!(v, "0", "{k}");
        }
        assert_eq!(s.values.len(), 5);
    }

    #[test]
    fn set_sysctls_writes_every_family() {
        let fake = FakeProcSys::new(
            true,
            &[
                ("/proc/sys/net/ipv4/conf/all/forwarding", "1"),
                ("/proc/sys/net/ipv4/conf/default/forwarding", "1"),
                ("/proc/sys/net/ipv6/conf/all/forwarding", "1"),
                ("/proc/sys/net/ipv6/conf/default/forwarding", "1"),
            ],
        );
        set_with(&fake, &forwarding()).unwrap();
        for (k, v) in &fake.0.lock().values {
            assert_eq!(v, "0", "{k}");
        }
    }

    #[test]
    fn set_sysctls_skips_missing_ipv6_keys() {
        let values = Mutex::new(HashMap::new());
        let p = FnProcSys(
            |path: &str, data: &[u8]| {
                if path.starts_with("/proc/sys/net/ipv6/") {
                    return Err(io::Error::from_raw_os_error(libc::ENOENT));
                }
                values.lock().insert(
                    path.to_string(),
                    String::from_utf8_lossy(data).trim().to_string(),
                );
                Ok(())
            },
            |_| panic!("remounted /proc/sys for a missing key"),
        );
        set_with(&p, &forwarding()).unwrap();
        let v = values.lock();
        assert_eq!(v["/proc/sys/net/ipv4/conf/all/forwarding"], "0");
        assert_eq!(v["/proc/sys/net/ipv4/conf/default/forwarding"], "0");
    }

    #[test]
    fn set_sysctls_reports_a_missing_ipv4_key() {
        let p = FnProcSys(
            |_: &str, _: &[u8]| Err(io::Error::from_raw_os_error(libc::ENOENT)),
            |_| Ok(()),
        );
        let err = set_with(&p, &forwarding()).unwrap_err();
        assert_eq!(errno_of(&err), Some(libc::ENOENT));
    }

    #[test]
    fn set_sysctls_writes_keys_that_already_match() {
        let fake = FakeProcSys::new(
            false,
            &[("/proc/sys/net/ipv4/conf/all/accept_redirects", "0")],
        );
        set_with(
            &fake,
            &[Sysctl::new("net/ipv4/conf/all/accept_redirects", "0")],
        )
        .unwrap();
        assert_eq!(fake.0.lock().writes, 1);
    }
}
