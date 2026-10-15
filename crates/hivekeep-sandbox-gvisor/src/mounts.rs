//! Detaching mounts this process made (or that runsc left) under a directory.

use std::io;
use std::path::{Path, PathBuf};

/// Lists the mount points at or below `dir` in a mountinfo table.
pub fn mount_points_in(mountinfo: &str, dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for line in mountinfo.lines() {
        // The fifth field is the mount point, with spaces and friends escaped
        // as octal.
        let Some(point) = line.split(' ').nth(4) else {
            continue;
        };
        let point = PathBuf::from(unescape_octal(point));
        if point.starts_with(dir) {
            out.push(point);
        }
    }
    out
}

fn unescape_octal(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\'
            && i + 3 < b.len()
            && b[i + 1..i + 4].iter().all(|c| (b'0'..=b'7').contains(c))
        {
            let v = u32::from(b[i + 1] - b'0') * 64
                + u32::from(b[i + 2] - b'0') * 8
                + u32::from(b[i + 3] - b'0');
            out.push(v as u8);
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Lazily detaches every mount at or below `dir`, deepest first, so nested
/// mounts go before their parents. A point that is already gone is fine.
#[cfg(target_os = "linux")]
pub fn unmount_all_under(dir: &Path) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let info = std::fs::read_to_string("/proc/self/mountinfo")?;
    let mut points = mount_points_in(&info, dir);
    points.sort_by_key(|p| std::cmp::Reverse(p.as_os_str().len()));
    let mut failures = Vec::new();
    for p in points {
        let Ok(c) = std::ffi::CString::new(p.as_os_str().as_bytes()) else {
            continue;
        };
        // SAFETY: c is a valid NUL-terminated path for the call's duration.
        let rc = unsafe { libc::umount2(c.as_ptr(), libc::MNT_DETACH) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINVAL) | Some(libc::ENOENT) => {}
                _ => failures.push(format!("while unmounting {}: {err}", p.display())),
            }
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(failures.join("; ")))
    }
}

/// Mounts only exist where the worker can make them.
#[cfg(not(target_os = "linux"))]
pub fn unmount_all_under(_dir: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_points_under_a_dir() {
        let info = "\
22 1 0:21 / /proc rw - proc proc rw
30 22 0:30 / /a/b rw - tmpfs t rw
31 30 0:31 / /a/b/c\\040d rw - tmpfs t rw
32 22 0:32 / /a/bc rw - tmpfs t rw
";
        assert_eq!(
            mount_points_in(info, Path::new("/a/b")),
            vec![PathBuf::from("/a/b"), PathBuf::from("/a/b/c d")]
        );
    }

    #[test]
    fn unmounting_nothing_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        unmount_all_under(dir.path()).unwrap();
    }
}
