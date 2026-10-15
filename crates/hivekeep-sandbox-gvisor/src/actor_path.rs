//! The paths the worker derives from the `ActorDirs` the keeper sends.

use std::io;
use std::path::{Path, PathBuf};

use hivekeep_proto::worker::v1::ActorDirs;

use crate::mounts;

/// A container's OCI bundle.
pub fn oci_bundle_path(actor_dirs: &ActorDirs, container: &str) -> PathBuf {
    Path::new(&actor_dirs.oci_bundle_dir).join(container)
}

/// runsc's `-root` for the Actor's sandbox.
pub fn runsc_state_dir(actor_dirs: &ActorDirs) -> PathBuf {
    Path::new(&actor_dirs.root_dir).join("runsc-state")
}

/// Where runsc writes `<container>.pid`.
pub fn pid_file_dir(actor_dirs: &ActorDirs) -> PathBuf {
    Path::new(&actor_dirs.root_dir).join("pidfiles")
}

/// Where runsc writes a container's pid.
pub fn pid_file_path(actor_dirs: &ActorDirs, container: &str) -> PathBuf {
    pid_file_dir(actor_dirs).join(format!("{container}.pid"))
}

/// Empties the runsc state and pid file directories for a new activation.
/// runsc can leave mounts behind in its state directory, which have to be
/// detached in this mount namespace before the directory can be removed.
pub fn reset_runsc_state_and_pid_file_dirs(actor_dirs: &ActorDirs) -> io::Result<()> {
    for dir in [runsc_state_dir(actor_dirs), pid_file_dir(actor_dirs)] {
        mounts::unmount_all_under(&dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while unmounting under {}: {e}", dir.display()),
            )
        })?;
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("while clearing {}: {e}", dir.display()),
                ))
            }
        }
        create_private_dir(&dir).map_err(|e| {
            io::Error::new(e.kind(), format!("while creating {}: {e}", dir.display()))
        })?;
    }
    Ok(())
}

/// `mkdir -p` with mode 0700.
pub fn create_private_dir(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_paths() {
        let d = ActorDirs {
            root_dir: "/node/actors/a".into(),
            oci_bundle_dir: "/node/actors/a/bundle".into(),
            ..Default::default()
        };
        assert_eq!(
            oci_bundle_path(&d, "app"),
            PathBuf::from("/node/actors/a/bundle/app")
        );
        assert_eq!(
            runsc_state_dir(&d),
            PathBuf::from("/node/actors/a/runsc-state")
        );
        assert_eq!(
            pid_file_path(&d, "_sandbox"),
            PathBuf::from("/node/actors/a/pidfiles/_sandbox.pid")
        );
    }

    #[test]
    fn reset_empties_both_dirs() {
        let root = tempfile::tempdir().unwrap();
        let d = ActorDirs {
            root_dir: root.path().to_str().unwrap().into(),
            ..Default::default()
        };
        for dir in [runsc_state_dir(&d), pid_file_dir(&d)] {
            std::fs::create_dir_all(dir.join("nested")).unwrap();
            std::fs::write(dir.join("stale"), b"x").unwrap();
        }
        reset_runsc_state_and_pid_file_dirs(&d).unwrap();
        for dir in [runsc_state_dir(&d), pid_file_dir(&d)] {
            assert!(dir.is_dir());
            assert_eq!(
                std::fs::read_dir(&dir).unwrap().count(),
                0,
                "{}",
                dir.display()
            );
        }
        // Again with nothing there.
        std::fs::remove_dir_all(runsc_state_dir(&d)).unwrap();
        reset_runsc_state_and_pid_file_dirs(&d).unwrap();
    }

    /// runsc leaves mounts in its state directory; removing one fails with
    /// EBUSY until it is detached.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs root (CAP_SYS_ADMIN) for bind mounts"]
    fn reset_detaches_mounts() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let root = tempfile::tempdir().unwrap();
        let d = ActorDirs {
            root_dir: root.path().to_str().unwrap().into(),
            ..Default::default()
        };
        std::fs::create_dir_all(runsc_state_dir(&d)).unwrap();
        let other = tempfile::tempdir().unwrap();
        let src = other.path().join("netns");
        let target = runsc_state_dir(&d).join("null-netns");
        for f in [&src, &target] {
            std::fs::write(f, b"").unwrap();
        }
        let c = |p: &Path| CString::new(p.as_os_str().as_bytes()).unwrap();
        let (csrc, ctarget) = (c(&src), c(&target));
        // SAFETY: valid NUL-terminated paths; null fstype and data are
        // allowed for a bind mount.
        let rc = unsafe {
            libc::mount(
                csrc.as_ptr(),
                ctarget.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND,
                std::ptr::null(),
            )
        };
        assert_eq!(rc, 0, "bind mount: {}", io::Error::last_os_error());

        let result = reset_runsc_state_and_pid_file_dirs(&d);
        // SAFETY: as above; a failure only means the reset already detached it.
        unsafe { libc::umount2(ctarget.as_ptr(), libc::MNT_DETACH) };
        result.unwrap();
        assert_eq!(std::fs::read_dir(runsc_state_dir(&d)).unwrap().count(), 0);
    }
}
