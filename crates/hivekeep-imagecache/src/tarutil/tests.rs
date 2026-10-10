use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime};

use tar::EntryType;

use super::*;
use crate::testutil::image::{build_tar, TarEntry};

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

fn mode_of(p: &Path) -> u32 {
    fs::symlink_metadata(p).unwrap().permissions().mode()
}

fn is_root() -> bool {
    // SAFETY: no preconditions.
    unsafe { libc::geteuid() == 0 }
}

fn write_tar(path: &Path, entries: &[TarEntry]) {
    fs::write(path, build_tar(entries)).unwrap();
}

fn round_trip(src: &Path) -> tempfile::TempDir {
    let out = tmp();
    let tar_path = out.path().join("out.tar");
    create(&tar_path, src).unwrap();
    let dst = tmp();
    extract(&tar_path, dst.path()).unwrap();
    dst
}

fn write_mode(p: &Path, body: &str, mode: u32) {
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, body).unwrap();
    fs::set_permissions(p, fs::Permissions::from_mode(mode)).unwrap();
}

/// Makes `dir/ro` writable again on drop, so temp cleanup can remove it.
struct Writable(std::path::PathBuf);

impl Drop for Writable {
    fn drop(&mut self) {
        let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700));
    }
}

#[test]
fn round_trip_tree() {
    let src = tmp();
    let s = src.path();
    write_mode(&s.join("a.txt"), "hello", 0o644);
    write_mode(&s.join("sub/b.txt"), "nested", 0o600);
    write_mode(&s.join("ro/c.txt"), "in-readonly", 0o444);
    fs::set_permissions(s.join("ro"), fs::Permissions::from_mode(0o500)).unwrap();
    let _w1 = Writable(s.join("ro"));
    fs::create_dir(s.join("empty")).unwrap();
    fs::set_permissions(s.join("empty"), fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink("a.txt", s.join("link")).unwrap();
    fs::hard_link(s.join("a.txt"), s.join("hard.txt")).unwrap();
    let mtime = filetime::FileTime::from_unix_time(1_600_000_000, 0);
    filetime::set_file_times(s.join("a.txt"), mtime, mtime).unwrap();

    let dst = round_trip(s);
    let d = dst.path();
    let _w2 = Writable(d.join("ro"));
    for (rel, want) in [
        ("a.txt", "hello"),
        ("sub/b.txt", "nested"),
        ("ro/c.txt", "in-readonly"),
        ("hard.txt", "hello"),
    ] {
        assert_eq!(fs::read_to_string(d.join(rel)).unwrap(), want, "{rel}");
    }
    for (rel, want) in [
        ("a.txt", 0o644),
        ("sub/b.txt", 0o600),
        ("ro/c.txt", 0o444),
        ("ro", 0o500),
        ("empty", 0o755),
    ] {
        assert_eq!(mode_of(&d.join(rel)) & 0o777, want, "{rel} mode");
    }
    assert_eq!(
        fs::metadata(d.join("a.txt")).unwrap().modified().unwrap(),
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_600_000_000)
    );
    assert_eq!(fs::read_link(d.join("link")).unwrap(), Path::new("a.txt"));
    let (a, h) = (
        fs::metadata(d.join("a.txt")).unwrap(),
        fs::metadata(d.join("hard.txt")).unwrap(),
    );
    assert_eq!((a.dev(), a.ino()), (h.dev(), h.ino()), "hard.txt is a copy");
}

#[test]
fn create_empty_dir() {
    let src = tmp();
    let dst = round_trip(src.path());
    assert_eq!(fs::read_dir(dst.path()).unwrap().count(), 0);
}

#[test]
fn round_trip_fifo() {
    let src = tmp();
    let p = std::ffi::CString::new(src.path().join("pipe").as_os_str().as_encoded_bytes()).unwrap();
    // SAFETY: a valid path.
    assert_eq!(unsafe { libc::mkfifo(p.as_ptr(), 0o640) }, 0);
    let dst = round_trip(src.path());
    let md = fs::symlink_metadata(dst.path().join("pipe")).unwrap();
    assert!(md.file_type().is_fifo());
    assert_eq!(md.permissions().mode() & 0o777, 0o640);
}

// A socket in the tree must not fail the archive.
#[test]
fn create_skips_sockets() {
    let src = tmp();
    fs::write(src.path().join("keep.txt"), "data").unwrap();
    let _l = std::os::unix::net::UnixListener::bind(src.path().join("agent.sock")).unwrap();
    let dst = round_trip(src.path());
    assert_eq!(
        fs::read_to_string(dst.path().join("keep.txt")).unwrap(),
        "data"
    );
    assert!(
        fs::symlink_metadata(dst.path().join("agent.sock")).is_err(),
        "socket was archived"
    );
}

#[test]
fn create_filtered_skips_subtree() {
    let src = tmp();
    for (rel, body) in [
        ("app/fs/data.txt", "keep"),
        ("app/work/tmp.bin", "drop"),
        ("app/work/#1/f", "drop nested"),
        ("beta/work-not/f", "keep (not an exact match)"),
    ] {
        write_mode(&src.path().join(rel), body, 0o644);
    }
    let out = tmp();
    let tar_path = out.path().join("filtered.tar");
    let skip = |rel: &str| {
        let parts: Vec<&str> = rel.split('/').collect();
        parts.len() == 2 && parts[1] == "work"
    };
    create_filtered(&tar_path, src.path(), &skip).unwrap();
    let dst = tmp();
    extract(&tar_path, dst.path()).unwrap();
    for rel in ["app/fs/data.txt", "beta/work-not/f"] {
        assert!(dst.path().join(rel).exists(), "{rel}");
    }
    assert!(!dst.path().join("app/work").exists());
}

// setuid, setgid and sticky survive a round trip.
#[test]
fn round_trip_special_mode_bits() {
    let src = tmp();
    fs::create_dir(src.path().join("shared")).unwrap();
    fs::create_dir(src.path().join("tmp")).unwrap();
    fs::write(src.path().join("tool"), "#!/bin/sh\n").unwrap();
    let want = [("shared", 0o2775), ("tmp", 0o1777), ("tool", 0o4755)];
    for (name, mode) in want {
        fs::set_permissions(src.path().join(name), fs::Permissions::from_mode(mode)).unwrap();
    }
    // Some filesystems refuse setgid on a directory whose group the user is
    // not in; compare against what the source really holds.
    let have: Vec<u32> = want
        .iter()
        .map(|(n, _)| mode_of(&src.path().join(n)) & 0o7777)
        .collect();
    let dst = round_trip(src.path());
    for ((name, _), have) in want.iter().zip(have) {
        assert_eq!(mode_of(&dst.path().join(name)) & 0o7777, have, "{name}");
    }
    assert_eq!(mode_of(&dst.path().join("tmp")) & 0o7777, 0o1777);
    assert_eq!(mode_of(&dst.path().join("tool")) & 0o7777, 0o4755);
}

#[test]
#[ignore = "needs root (CAP_CHOWN)"]
fn round_trip_ownership() {
    assert!(is_root());
    let src = tmp();
    let p = src.path().join("owned.txt");
    fs::write(&p, "data").unwrap();
    std::os::unix::fs::chown(&p, Some(1234), Some(5678)).unwrap();
    let dst = round_trip(src.path());
    let md = fs::metadata(dst.path().join("owned.txt")).unwrap();
    assert_eq!((md.uid(), md.gid()), (1234, 5678));
}

#[test]
#[ignore = "needs root (mknod)"]
fn round_trip_device_node() {
    assert!(is_root());
    let src = tmp();
    let p = std::ffi::CString::new(
        src.path()
            .join("deleted-file")
            .as_os_str()
            .as_encoded_bytes(),
    )
    .unwrap();
    // SAFETY: a valid path.
    assert_eq!(
        unsafe { libc::mknod(p.as_ptr(), libc::S_IFCHR | 0o600, 0) },
        0
    );
    let dst = round_trip(src.path());
    let md = fs::symlink_metadata(dst.path().join("deleted-file")).unwrap();
    assert!(md.file_type().is_char_device());
    assert_eq!(md.rdev(), 0);
}

#[cfg(target_os = "linux")]
fn lsetxattr(p: &Path, attr: &str, val: &[u8]) -> io::Result<()> {
    let (p, a) = (
        std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).unwrap(),
        std::ffi::CString::new(attr).unwrap(),
    );
    // SAFETY: valid path, name and value.
    if unsafe { libc::lsetxattr(p.as_ptr(), a.as_ptr(), val.as_ptr().cast(), val.len(), 0) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(target_os = "linux")]
fn lgetxattr(p: &Path, attr: &str) -> io::Result<Vec<u8>> {
    let (p, a) = (
        std::ffi::CString::new(p.as_os_str().as_encoded_bytes()).unwrap(),
        std::ffi::CString::new(attr).unwrap(),
    );
    let mut buf = vec![0u8; 256];
    // SAFETY: valid path, name and buffer.
    let n = unsafe { libc::lgetxattr(p.as_ptr(), a.as_ptr(), buf.as_mut_ptr().cast(), buf.len()) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    buf.truncate(n as usize);
    Ok(buf)
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "needs root (trusted.* xattrs)"]
fn round_trip_trusted_overlay_xattrs() {
    let src = tmp();
    fs::create_dir(src.path().join("replaced-dir")).unwrap();
    if let Err(e) = lsetxattr(
        &src.path().join("replaced-dir"),
        "trusted.overlay.opaque",
        b"y",
    ) {
        eprintln!("skipped: filesystem does not support trusted xattrs: {e}");
        return;
    }
    let dst = round_trip(src.path());
    assert_eq!(
        lgetxattr(&dst.path().join("replaced-dir"), "trusted.overlay.opaque").unwrap(),
        b"y"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn round_trip_user_xattrs() {
    let src = tmp();
    fs::create_dir(src.path().join("replaced-dir")).unwrap();
    if let Err(e) = lsetxattr(
        &src.path().join("replaced-dir"),
        "user.overlay.opaque",
        b"y",
    ) {
        eprintln!("skipped: filesystem does not support user xattrs: {e}");
        return;
    }
    fs::write(src.path().join("f.txt"), "x").unwrap();
    lsetxattr(&src.path().join("f.txt"), "user.custom", b"val").unwrap();
    let dst = round_trip(src.path());
    assert_eq!(
        lgetxattr(&dst.path().join("replaced-dir"), "user.overlay.opaque").unwrap(),
        b"y"
    );
    assert_eq!(
        lgetxattr(&dst.path().join("f.txt"), "user.custom").unwrap(),
        b"val"
    );
}

#[test]
fn extract_rejects_escapes() {
    let cases: Vec<(&str, Vec<TarEntry>)> = vec![
        (
            "parent traversal",
            vec![TarEntry::file("../escape.txt", "xxx")],
        ),
        (
            "nested parent traversal",
            vec![TarEntry::file("sub/../../escape.txt", "xxx")],
        ),
        (
            "write through symlink out of tree",
            vec![
                TarEntry::symlink("out", "/tmp"),
                TarEntry::file("out/escape.txt", "xxx"),
            ],
        ),
        (
            "hardlink outside the tree",
            vec![TarEntry::hardlink("link", "../../etc/passwd")],
        ),
    ];
    for (name, entries) in cases {
        let t = tmp();
        let tar_path = t.path().join("bad.tar");
        write_tar(&tar_path, &entries);
        let parent = tmp();
        let dst = parent.path().join("dst");
        fs::create_dir(&dst).unwrap();
        assert!(
            extract(&tar_path, &dst).is_err(),
            "{name}: extract succeeded"
        );
        assert!(
            !parent.path().join("escape.txt").exists(),
            "{name}: escaped"
        );
    }
}

#[test]
fn extract_absolute_name_stays_inside() {
    let t = tmp();
    let tar_path = t.path().join("abs.tar");
    write_tar(&tar_path, &[TarEntry::file("/abs.txt", "xx")]);
    let dst = tmp();
    extract(&tar_path, dst.path()).unwrap();
    assert!(dst.path().join("abs.txt").exists());
}

#[test]
fn extract_replaces_existing() {
    let src = tmp();
    fs::write(src.path().join("a.txt"), "new").unwrap();
    let out = tmp();
    let tar_path = out.path().join("out.tar");
    create(&tar_path, src.path()).unwrap();
    let dst = tmp();
    fs::write(dst.path().join("a.txt"), "stale").unwrap();
    extract(&tar_path, dst.path()).unwrap();
    assert_eq!(fs::read_to_string(dst.path().join("a.txt")).unwrap(), "new");
}

// The durable-dir case: the volume subdirectory already exists.
#[test]
fn extract_into_precreated_volume_dir() {
    let src = tmp();
    fs::create_dir_all(src.path().join("data")).unwrap();
    fs::write(src.path().join("data/counter"), "7").unwrap();
    let out = tmp();
    let tar_path = out.path().join("durable.tar");
    create(&tar_path, src.path()).unwrap();
    let dst = tmp();
    fs::create_dir_all(dst.path().join("data")).unwrap();
    extract(&tar_path, dst.path()).unwrap();
    assert_eq!(
        fs::read_to_string(dst.path().join("data/counter")).unwrap(),
        "7"
    );
}

// Only the 0:0 whiteout device is allowed.
#[test]
fn extract_rejects_device_nodes() {
    for e in [
        TarEntry::new("null", EntryType::Char).dev(1, 3),
        TarEntry::new("mem", EntryType::Char).dev(1, 1).mode(0o666),
        TarEntry::new("sda", EntryType::Block).dev(8, 0),
        TarEntry::new("blk0", EntryType::Block),
    ] {
        let t = tmp();
        let tar_path = t.path().join("dev.tar");
        write_tar(&tar_path, std::slice::from_ref(&e));
        let dst = tmp();
        assert!(extract(&tar_path, dst.path()).is_err(), "{}", e.name);
        assert!(
            fs::symlink_metadata(dst.path().join(&e.name)).is_err(),
            "{}",
            e.name
        );
    }
}

#[test]
#[ignore = "needs root (mknod)"]
fn create_skips_non_whiteout_devices() {
    assert!(is_root());
    let src = tmp();
    for (name, dev) in [("null", libc::makedev(1, 3)), ("whiteout", 0)] {
        let p =
            std::ffi::CString::new(src.path().join(name).as_os_str().as_encoded_bytes()).unwrap();
        // SAFETY: a valid path.
        assert_eq!(
            unsafe { libc::mknod(p.as_ptr(), libc::S_IFCHR | 0o666, dev) },
            0
        );
    }
    let dst = round_trip(src.path());
    assert!(fs::symlink_metadata(dst.path().join("null")).is_err());
    assert!(fs::symlink_metadata(dst.path().join("whiteout")).is_ok());
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "needs root (trusted.* and security.* xattrs)"]
fn extract_drops_disallowed_xattrs() {
    let t = tmp();
    let tar_path = t.path().join("xattr.tar");
    let mut e = TarEntry::file("f", "x").mode(0o755);
    e.pax = vec![
        (
            "SCHILY.xattr.security.capability".into(),
            b"\x01\x00\x00\x02\xff\xff\xff\xff\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00"
                .to_vec(),
        ),
        ("SCHILY.xattr.trusted.overlay.origin".into(), b"x".to_vec()),
        ("SCHILY.xattr.trusted.overlay.metacopy".into(), Vec::new()),
        ("SCHILY.xattr.trusted.other".into(), b"x".to_vec()),
        ("SCHILY.xattr.trusted.overlay.opaque".into(), b"y".to_vec()),
        ("SCHILY.xattr.user.custom".into(), b"val".to_vec()),
    ];
    write_tar(&tar_path, &[e]);
    let dst = tmp();
    if let Err(err) = extract(&tar_path, dst.path()) {
        eprintln!("skipped: {err}");
        return;
    }
    let p = dst.path().join("f");
    for attr in [
        "security.capability",
        "trusted.overlay.origin",
        "trusted.overlay.metacopy",
        "trusted.other",
    ] {
        assert_eq!(
            lgetxattr(&p, attr).unwrap_err().raw_os_error(),
            Some(libc::ENODATA),
            "{attr}"
        );
    }
    for attr in ["trusted.overlay.opaque", "user.custom"] {
        assert!(lgetxattr(&p, attr).is_ok(), "{attr}");
    }
}

// create_with_root carries src_dir's own metadata; create leaves the
// destination's alone.
#[test]
fn create_with_root_restores_root_meta() {
    let src = tmp();
    fs::write(src.path().join("f"), "x").unwrap();
    #[cfg(target_os = "linux")]
    let has_attr = lsetxattr(src.path(), "user.root-attr", b"v").is_ok();
    fs::set_permissions(src.path(), fs::Permissions::from_mode(0o2750)).unwrap();
    let src_mode = mode_of(src.path()) & 0o7777;
    for (name, with_root) in [("create_with_root", true), ("create", false)] {
        let out = tmp();
        let tar_path = out.path().join("root.tar");
        if with_root {
            create_with_root(&tar_path, src.path()).unwrap();
        } else {
            create(&tar_path, src.path()).unwrap();
        }
        let parent = tmp();
        let dst = parent.path().join("dst");
        fs::create_dir(&dst).unwrap();
        fs::set_permissions(&dst, fs::Permissions::from_mode(0o755)).unwrap();
        extract(&tar_path, &dst).unwrap();
        let want = if with_root { src_mode } else { 0o755 };
        assert_eq!(mode_of(&dst) & 0o7777, want, "{name}");
        #[cfg(target_os = "linux")]
        if has_attr {
            assert_eq!(
                lgetxattr(&dst, "user.root-attr").is_ok(),
                with_root,
                "{name}"
            );
        }
        assert_eq!(fs::read_to_string(dst.join("f")).unwrap(), "x", "{name}");
    }
}

// Only a directory "./" entry is root metadata.
#[test]
fn extract_ignores_non_dir_root_entry() {
    let t = tmp();
    let tar_path = t.path().join("root-symlink.tar");
    write_tar(&tar_path, &[TarEntry::symlink("./", "/")]);
    let dst = tmp();
    extract(&tar_path, dst.path()).unwrap();
    assert!(fs::symlink_metadata(dst.path()).unwrap().is_dir());
}

// A directory swapped for a symlink out of the tree mid-walk does not pull
// the outside directory's contents into the archive.
#[test]
fn create_does_not_follow_swapped_dir() {
    let base = tmp();
    let outside = base.path().join("outside");
    fs::create_dir(&outside).unwrap();
    fs::write(outside.join("secret"), "host data").unwrap();
    let src = base.path().join("src");
    fs::create_dir_all(src.join("d")).unwrap();
    fs::write(src.join("d/f"), "ok").unwrap();
    let (s, o) = (src.clone(), outside.clone());
    let swap = move |rel: &str| {
        if rel == "d" {
            fs::remove_dir_all(s.join("d")).unwrap();
            std::os::unix::fs::symlink(&o, s.join("d")).unwrap();
        }
        false
    };
    let out = tmp();
    let tar_path = out.path().join("swap.tar");
    let _ = create_filtered(&tar_path, &src, &swap);
    let f = fs::File::open(&tar_path).unwrap();
    let mut a = tar::Archive::new(f);
    for e in a.entries().unwrap().flatten() {
        assert!(
            !e.path_bytes().ends_with(b"secret"),
            "archive captured a file from outside the tree"
        );
    }
}

// Names and link targets longer than a header field round-trip.
#[test]
fn round_trip_long_names() {
    let src = tmp();
    let long_dir = "d".repeat(120);
    let long_file = format!("{long_dir}/{}", "f".repeat(130));
    write_mode(&src.path().join(&long_file), "deep", 0o644);
    let target = format!("{}/x", "t".repeat(150));
    std::os::unix::fs::symlink(&target, src.path().join("longlink")).unwrap();
    let dst = round_trip(src.path());
    assert_eq!(
        fs::read_to_string(dst.path().join(&long_file)).unwrap(),
        "deep"
    );
    assert_eq!(
        fs::read_link(dst.path().join("longlink")).unwrap(),
        Path::new(&target)
    );
}
