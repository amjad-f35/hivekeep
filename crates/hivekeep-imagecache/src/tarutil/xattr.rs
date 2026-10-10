//! The extended attributes an archive round trip keeps.
//!
//! Kept: any `user.*` attribute (workload data the workload can set itself)
//! and the path-based `trusted.overlay.*` attributes an overlay upper
//! carries. The rest are file handles that go stale on another node or
//! belong to overlay features left off, and the kernel trusts all of them
//! when mounting an upper, so a crafted archive must not plant them.
//! Everything else, `security.capability` included, is dropped.
//!
//! Entries are addressed through their parent directory opened under the
//! confined root, never by joining a host path, and the final component by
//! a path under `/proc/self/fd/<parent>` with the non-following `l*` calls,
//! so a FIFO is never opened and a symlink never followed. Only Linux keeps
//! xattrs; elsewhere none are read or restored.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use cap_std::fs::Dir;

/// The `trusted.overlay.*` attributes kept.
const OVERLAY_XATTRS: [&str; 3] = [
    "trusted.overlay.opaque",
    "trusted.overlay.redirect",
    "trusted.overlay.impure",
];

/// Whether `attr` is archived and restored.
pub fn keep_xattr(attr: &str) -> bool {
    attr.starts_with("user.") || OVERLAY_XATTRS.contains(&attr)
}

#[cfg(target_os = "linux")]
fn proc_path(fd: libc::c_int, base: &std::ffi::CStr) -> io::Result<std::ffi::CString> {
    let mut p = format!("/proc/self/fd/{fd}/").into_bytes();
    p.extend_from_slice(base.to_bytes());
    std::ffi::CString::new(p)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))
}

/// The kept xattrs of `rel`. A filesystem without xattr support has none.
#[cfg(target_os = "linux")]
pub fn read(root: &Dir, rel: &Path) -> io::Result<BTreeMap<String, Vec<u8>>> {
    crate::confined::at_parent(root, rel, |fd, base| {
        let path = proc_path(fd, base)?;
        // SAFETY: a valid path; a null buffer asks for the size.
        let size = unsafe { libc::llistxattr(path.as_ptr(), std::ptr::null_mut(), 0) };
        if size < 0 {
            let e = io::Error::last_os_error();
            return match e.raw_os_error() {
                Some(libc::ENOTSUP) => Ok(BTreeMap::new()),
                _ => Err(e),
            };
        }
        let mut out = BTreeMap::new();
        if size == 0 {
            return Ok(out);
        }
        let mut names = vec![0u8; size as usize];
        // SAFETY: a valid path and a buffer of the given length.
        let n = unsafe { libc::llistxattr(path.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        for name in names[..n as usize]
            .split(|&b| b == 0)
            .filter(|n| !n.is_empty())
        {
            let Ok(attr) = std::str::from_utf8(name) else {
                continue;
            };
            if !keep_xattr(attr) {
                continue;
            }
            let cname = std::ffi::CString::new(name)
                .map_err(|_| io::Error::other("xattr name contains NUL"))?;
            // SAFETY: valid path and name; a null buffer asks for the size.
            let vs =
                unsafe { libc::lgetxattr(path.as_ptr(), cname.as_ptr(), std::ptr::null_mut(), 0) };
            if vs < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut val = vec![0u8; vs as usize];
            if vs > 0 {
                // SAFETY: valid path and name and a buffer of the given length.
                let got = unsafe {
                    libc::lgetxattr(
                        path.as_ptr(),
                        cname.as_ptr(),
                        val.as_mut_ptr().cast(),
                        val.len(),
                    )
                };
                if got < 0 {
                    return Err(io::Error::last_os_error());
                }
                val.truncate(got as usize);
            }
            out.insert(attr.to_string(), val);
        }
        Ok(out)
    })
}

/// Re-applies `attrs` (already filtered by [`keep_xattr`]) to `rel`.
/// Writing `trusted.*` needs `CAP_SYS_ADMIN`.
#[cfg(target_os = "linux")]
pub fn restore(root: &Dir, rel: &Path, attrs: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
    crate::confined::at_parent(root, rel, |fd, base| {
        let path = proc_path(fd, base)?;
        for (attr, val) in attrs {
            let cname = std::ffi::CString::new(attr.as_bytes())
                .map_err(|_| io::Error::other("xattr name contains NUL"))?;
            // SAFETY: valid path, name and value buffer.
            let r = unsafe {
                libc::lsetxattr(
                    path.as_ptr(),
                    cname.as_ptr(),
                    val.as_ptr().cast(),
                    val.len(),
                    0,
                )
            };
            if r != 0 {
                let e = io::Error::last_os_error();
                return Err(io::Error::new(
                    e.kind(),
                    format!("restoring xattr {attr:?}: {e}"),
                ));
            }
        }
        Ok(())
    })
}

/// Xattrs are only kept on Linux.
#[cfg(not(target_os = "linux"))]
pub fn read(_root: &Dir, _rel: &Path) -> io::Result<BTreeMap<String, Vec<u8>>> {
    Ok(BTreeMap::new())
}

/// Xattrs are only kept on Linux.
#[cfg(not(target_os = "linux"))]
pub fn restore(_root: &Dir, _rel: &Path, _attrs: &BTreeMap<String, Vec<u8>>) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_xattr_allowlist() {
        for (attr, want) in [
            ("user.custom", true),
            ("trusted.overlay.opaque", true),
            ("trusted.overlay.redirect", true),
            ("trusted.overlay.impure", true),
            ("trusted.overlay.origin", false),
            ("trusted.overlay.metacopy", false),
            ("trusted.other", false),
            ("security.capability", false),
        ] {
            assert_eq!(keep_xattr(attr), want, "{attr}");
        }
    }
}
