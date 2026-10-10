use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime};

use tar::EntryType;

use super::*;
use crate::removeall::remove_all_writable;
use crate::testutil::image::{build_tar, TarEntry};

fn unpack_into(dir: &Path, tar: &[u8]) -> Result<WhiteoutSet> {
    let root = confined::open_root(dir).unwrap();
    let mut r: &[u8] = tar;
    unpack_layer(&mut r, &root)
}

fn run_unpack(entries: &[TarEntry]) -> (tempfile::TempDir, Result<WhiteoutSet>) {
    let dir = tempfile::tempdir().unwrap();
    let res = unpack_into(dir.path(), &build_tar(entries));
    (dir, res)
}

fn same_file(a: &Path, b: &Path) -> bool {
    let (a, b) = (fs::metadata(a).unwrap(), fs::metadata(b).unwrap());
    a.dev() == b.dev() && a.ino() == b.ino()
}

fn read(p: &Path) -> String {
    fs::read_to_string(p).unwrap_or_default()
}

fn at(secs: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

#[test]
fn validate_tar_name_cases() {
    for (input, want_clean, want_skip, want_err) in [
        ("etc/passwd", "etc/passwd", false, false),
        (".", "", true, false),
        ("", "", true, false),
        ("etc/", "etc", false, false),
        ("/etc/passwd", "etc/passwd", false, false),
        ("//etc/passwd", "etc/passwd", false, false),
        ("../etc/passwd", "", false, true),
        ("..", "", false, true),
        ("a/../../escape", "", false, true),
        ("./a/./b", "a/b", false, false),
    ] {
        match validate_tar_name(input) {
            Err(_) => assert!(want_err, "{input}: unexpected error"),
            Ok(got) => {
                assert!(!want_err, "{input}: want an error");
                assert_eq!(got.is_none(), want_skip, "{input}: skip");
                assert_eq!(got.unwrap_or_default(), want_clean, "{input}: clean");
            }
        }
    }
}

#[test]
fn unpack_layer_happy_path() {
    let (dir, wh) = run_unpack(&[
        TarEntry::dir("."),
        TarEntry::dir("etc/"),
        TarEntry::file("etc/hostname", "demo\n"),
        TarEntry::dir("bin/"),
        TarEntry::file("bin/sh", "#!/sh\n").mode(0o755),
        TarEntry::hardlink("bin/bash", "bin/sh"),
        TarEntry::symlink("etc/host-link", "hostname"),
    ]);
    let wh = wh.unwrap();
    assert!(wh.whiteouts.is_empty() && wh.opaques.is_empty(), "{wh:?}");
    let d = dir.path();
    assert_eq!(read(&d.join("etc/hostname")), "demo\n");
    assert_eq!(
        fs::read_link(d.join("etc/host-link")).unwrap(),
        Path::new("hostname")
    );
    assert!(
        same_file(&d.join("bin/sh"), &d.join("bin/bash")),
        "bin/bash is not a hardlink to bin/sh"
    );
}

// Whiteout entries are recorded, never written into the layer tree.
#[test]
fn unpack_layer_whiteouts() {
    let (dir, wh) = run_unpack(&[
        TarEntry::dir("a/"),
        TarEntry::file("a/.wh.deleted", ""),
        TarEntry::dir("b/"),
        TarEntry::file("b/.wh..wh..opq", ""),
        TarEntry::file(".wh.toplevel", ""),
        TarEntry::file(".wh..wh.plnk", ""),
        TarEntry::file("a/kept", "kept"),
    ]);
    let wh = wh.unwrap();
    assert_eq!(wh.whiteouts, vec!["a/deleted", "toplevel"]);
    assert_eq!(wh.opaques, vec!["b"]);
    for p in [
        "a/.wh.deleted",
        "b/.wh..wh..opq",
        ".wh.toplevel",
        ".wh..wh.plnk",
    ] {
        assert!(
            fs::symlink_metadata(dir.path().join(p)).is_err(),
            "{p} was written into the tree"
        );
    }
    assert_eq!(read(&dir.path().join("a/kept")), "kept");
}

// Layer tars omit parents that exist only in lower layers.
#[test]
fn unpack_layer_missing_parent_dirs() {
    let (dir, res) = run_unpack(&[
        TarEntry::file("etc/nsswitch.conf", "hosts: files dns"),
        TarEntry::file("a/b/c/deep", "deep"),
        TarEntry::dir("usr/share/doc/"),
        TarEntry::file("lnk/target", "t"),
        TarEntry::symlink("other/dir/sym", "../../lnk/target"),
        TarEntry::hardlink("hard/link", "lnk/target"),
    ]);
    res.unwrap();
    let d = dir.path();
    assert_eq!(read(&d.join("etc/nsswitch.conf")), "hosts: files dns");
    assert_eq!(read(&d.join("a/b/c/deep")), "deep");
    assert!(d.join("usr/share/doc").is_dir());
    assert_eq!(
        fs::read_link(d.join("other/dir/sym")).unwrap(),
        Path::new("../../lnk/target")
    );
    assert!(same_file(&d.join("lnk/target"), &d.join("hard/link")));
}

// Implicit parents are recorded; declared ones are not, and candidates
// replaced by non-directories drop out.
#[test]
fn unpack_layer_implicit_dir_recording() {
    let (_dir, wh) = run_unpack(&[
        TarEntry::dir("declared/"),
        TarEntry::file("declared/file", "x"),
        TarEntry::file("etc/nsswitch.conf", "hosts: files"),
        TarEntry::file("a/b/c/deep", "d"),
        TarEntry::file("gone/.wh.victim", ""),
        TarEntry::file("late/child", "c"),
        TarEntry::dir("late/").mode(0o700),
        TarEntry::file("swap/inner", "i"),
        TarEntry::file("swap", "now a file"),
    ]);
    assert_eq!(
        wh.unwrap().implicit_dirs,
        vec!["a", "a/b", "a/b/c", "etc", "gone"]
    );
}

#[test]
fn unpack_layer_later_entry_wins_dir_then_symlink() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("var/"),
        TarEntry::dir("var/run/"),
        TarEntry::dir("run/"),
        TarEntry::file("run/sock", "sock"),
        TarEntry::symlink("var/run", "../run"),
    ]);
    res.unwrap();
    let md = fs::symlink_metadata(dir.path().join("var/run")).unwrap();
    assert!(md.file_type().is_symlink());
    assert_eq!(
        fs::read_link(dir.path().join("var/run")).unwrap(),
        Path::new("../run")
    );
}

#[test]
fn unpack_layer_later_entry_wins_file_overwrite() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("etc/"),
        TarEntry::file("etc/conf", "v1"),
        TarEntry::file("etc/conf", "v2"),
    ]);
    res.unwrap();
    assert_eq!(read(&dir.path().join("etc/conf")), "v2");
}

#[test]
fn unpack_layer_later_entry_wins_symlink_retargeted() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("etc/"),
        TarEntry::file("etc/x", "x"),
        TarEntry::file("etc/y", "y"),
        TarEntry::symlink("etc/link", "x"),
        TarEntry::symlink("etc/link", "y"),
    ]);
    res.unwrap();
    assert_eq!(
        fs::read_link(dir.path().join("etc/link")).unwrap(),
        Path::new("y")
    );
}

#[test]
fn unpack_layer_later_entry_wins_repeated_dir_entry_tolerated() {
    let (_dir, res) = run_unpack(&[TarEntry::dir("etc/"), TarEntry::dir("etc/")]);
    res.unwrap();
}

#[test]
fn unpack_layer_later_entry_wins_identical_symlink_is_noop() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("etc/"),
        TarEntry::file("etc/x", "x"),
        TarEntry::symlink("etc/link", "x"),
        TarEntry::symlink("etc/link", "x"),
    ]);
    res.unwrap();
    assert_eq!(
        fs::read_link(dir.path().join("etc/link")).unwrap(),
        Path::new("x")
    );
}

#[test]
fn unpack_layer_later_entry_wins_symlink_overwritten_by_file() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("etc/"),
        TarEntry::file("etc/x", "original"),
        TarEntry::symlink("etc/link", "x"),
        TarEntry::file("etc/link", "replacement"),
    ]);
    res.unwrap();
    let md = fs::symlink_metadata(dir.path().join("etc/link")).unwrap();
    assert!(md.is_file(), "etc/link is not a regular file");
    assert_eq!(read(&dir.path().join("etc/link")), "replacement");
    assert_eq!(read(&dir.path().join("etc/x")), "original");
}

#[test]
fn unpack_layer_later_entry_wins_file_overwritten_by_symlink() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("etc/"),
        TarEntry::file("etc/link", "original-file"),
        TarEntry::symlink("etc/link", "target-doesnt-exist"),
    ]);
    res.unwrap();
    assert!(fs::symlink_metadata(dir.path().join("etc/link"))
        .unwrap()
        .file_type()
        .is_symlink());
    assert_eq!(
        fs::read_link(dir.path().join("etc/link")).unwrap(),
        Path::new("target-doesnt-exist")
    );
}

#[test]
fn unpack_layer_later_entry_wins_hardlink_overwritten_by_file() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("bin/"),
        TarEntry::file("bin/sh", "sh-original"),
        TarEntry::hardlink("bin/bash", "bin/sh"),
        TarEntry::file("bin/bash", "bash-new"),
    ]);
    res.unwrap();
    assert_eq!(read(&dir.path().join("bin/bash")), "bash-new");
    assert_eq!(
        read(&dir.path().join("bin/sh")),
        "sh-original",
        "hardlink was not unlinked"
    );
}

// A read-only directory still gets its child written and keeps its mode,
// and the restored tree can still be deleted.
#[test]
fn unpack_layer_read_only_dir() {
    let (dir, res) = run_unpack(&[
        TarEntry::dir("app").mode(0o555),
        TarEntry::file("app/counter", "bin").mode(0o755),
    ]);
    res.unwrap();
    assert_eq!(read(&dir.path().join("app/counter")), "bin");
    let mode = fs::metadata(dir.path().join("app"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o555);
    remove_all_writable(&dir.path().join("app")).unwrap();
}

// Files and directories keep their tar mtimes; links never re-stamp
// their targets.
#[test]
fn unpack_layer_restores_mod_times() {
    let file_time = at(1_757_497_044);
    let dir_time = at(1_757_496_600);
    let sub_time = at(1_709_208_000);
    let stale = at(1_577_836_800);
    let link_time = at(1_767_225_600);
    let (dir, res) = run_unpack(&[
        TarEntry::dir("app/").mtime(stale),
        TarEntry::file("app/main.py", "print()\n").mtime(file_time),
        TarEntry::dir("app/sub/").mtime(sub_time),
        TarEntry::file("app/sub/mod.py", "x = 1\n").mtime(file_time),
        TarEntry::symlink("app/sym", "main.py").mtime(link_time),
        TarEntry::hardlink("app/hard", "app/main.py").mtime(link_time),
        TarEntry::dir("app/").mtime(dir_time),
        TarEntry::dir("ro/").mode(0o555).mtime(dir_time),
        TarEntry::file("ro/bin", "bin").mode(0o755).mtime(file_time),
    ]);
    res.unwrap();
    for (p, want) in [
        ("app/main.py", file_time),
        ("app/sub/mod.py", file_time),
        ("app/hard", file_time),
        ("app/sub", sub_time),
        ("app", dir_time),
        ("ro/bin", file_time),
        ("ro", dir_time),
    ] {
        let got = fs::symlink_metadata(dir.path().join(p))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(got, want, "{p} mtime");
    }
    assert_eq!(
        fs::metadata(dir.path().join("ro"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o555
    );
    remove_all_writable(&dir.path().join("ro")).unwrap();
}

#[test]
fn unpack_layer_path_traversal() {
    for name in ["../escape", "a/b/../../../escape", ".."] {
        let (_dir, res) = run_unpack(&[TarEntry::file(name, "x")]);
        let err = res.expect_err(name);
        assert!(
            err.to_string().contains("invalid tar entry"),
            "{name}: {err}"
        );
    }
}

// A symlink pointing out of the tree, then a write through it, is refused.
#[test]
fn unpack_layer_symlink_escape() {
    let parent = tempfile::tempdir().unwrap();
    let rootfs = parent.path().join("rootfs");
    let host = parent.path().join("host");
    fs::create_dir(&rootfs).unwrap();
    fs::create_dir(&host).unwrap();
    fs::write(host.join("passwd"), "original").unwrap();
    let tar = build_tar(&[
        TarEntry::symlink("etc", host.to_str().unwrap()),
        TarEntry::file("etc/passwd", "OWNED"),
    ]);
    assert!(unpack_into(&rootfs, &tar).is_err());
    assert_eq!(
        read(&host.join("passwd")),
        "original",
        "symlink escape was not prevented"
    );
}

#[test]
fn unpack_layer_hardlink_escape() {
    for target in ["../host/passwd", "a/../../host"] {
        let (_dir, res) = run_unpack(&[TarEntry::hardlink("etc/passwd", target)]);
        let err = res.expect_err(target);
        assert!(
            err.to_string().contains("invalid hardlink target"),
            "{target}: {err}"
        );
    }
}

#[test]
fn unpack_layer_skips_device_nodes() {
    let (dir, wh) = run_unpack(&[
        TarEntry::dir("dev/"),
        TarEntry::new("dev/console", EntryType::Char).dev(5, 1),
        TarEntry::new("dev/sda", EntryType::Block).dev(8, 0),
        TarEntry::new("dev/zero", EntryType::Char),
        TarEntry::new("undeclared/tty0", EntryType::Char).dev(4, 0),
        TarEntry::file("dev/replaced", "stale"),
        TarEntry::new("dev/replaced", EntryType::Char).dev(1, 3),
        TarEntry::dir("dev/subdir/"),
        TarEntry::dir("dev/subdir/nested/"),
        TarEntry::new("dev/subdir", EntryType::Char).dev(1, 5),
        TarEntry::file("etc/hostname", "x"),
    ]);
    let wh = wh.unwrap();
    for name in [
        "dev/console",
        "dev/sda",
        "dev/zero",
        "undeclared",
        "undeclared/tty0",
        "dev/replaced",
        "dev/subdir",
    ] {
        assert!(
            fs::symlink_metadata(dir.path().join(name)).is_err(),
            "{name} exists"
        );
    }
    assert_eq!(wh.implicit_dirs, vec!["etc"]);
    assert!(dir.path().join("etc/hostname").exists());
}

#[test]
fn unpack_layer_reject_fifo() {
    let (_dir, res) = run_unpack(&[TarEntry::new("weird", EntryType::Fifo)]);
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains(r#"unhandled tar entry typeflag "6" at "weird""#),
        "{err}"
    );
}

#[test]
fn unpack_layer_truncated_archive() {
    let full = build_tar(&[TarEntry::file("ok", "hello")]);
    assert!(full.len() >= 64);
    let truncated = &full[..full.len() - 64];
    let dir = tempfile::tempdir().unwrap();
    let err = unpack_into(dir.path(), truncated).unwrap_err().to_string();
    assert!(
        err.contains("while reading tar entry") || err.contains("unexpected EOF"),
        "{err}"
    );
}
