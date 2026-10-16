//! The containers' rootfs uppers on the host and their snapshot tars.
//!
//! Each tar holds one container's upperdir (`<name>/fs`) relative to it, plus
//! a root entry: overlayfs presents the upperdir's own mode, ownership and
//! xattrs as the container's `/`. The workdir is never archived; with
//! `index=off` a restored one would be inert, and staging recreates it.

use std::io;
use std::path::Path;

use hivekeep_proto::worker::v1 as pb;
use tokio_util::sync::CancellationToken;

use crate::actor_path::rootfs_upper_dir;
use crate::kata::overlay::mkdir_upper_work_dirs;
use crate::kata::paths::upper_work_dirs;
use crate::tarball;
use crate::volumes::is_single_element;

/// The snapshot file of one container's rootfs upper.
pub fn rootfs_upper_tar_file(container: &str) -> io::Result<String> {
    if !is_single_element(container) {
        return Err(io::Error::other(format!(
            "invalid container name {container:?}"
        )));
    }
    Ok(format!("rootfs-upper-{container}.tar"))
}

/// The names of the Actor's containers.
pub fn container_names(cs: &[pb::Container]) -> Vec<String> {
    cs.iter().map(|c| c.name.clone()).collect()
}

fn recreate(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(io::Error::new(
                e.kind(),
                format!("while clearing rootfs upper dir {}: {e}", dir.display()),
            ))
        }
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while creating rootfs upper dir {}: {e}", dir.display()),
            )
        })
}

/// Prepares the upper dir for a cold boot: pristine without `snapshot_dir`,
/// otherwise re-materialized from its tars. Any previous activation's
/// contents go first. Runs after the overlays using the dir are unmounted.
pub fn stage_rootfs_upper(
    d: &pb::ActorDirs,
    snapshot_dir: Option<&Path>,
    containers: &[String],
) -> io::Result<()> {
    match snapshot_dir {
        None => recreate(&rootfs_upper_dir(d)),
        Some(s) => untar_rootfs_upper(&rootfs_upper_dir(d), s, containers),
    }
}

/// Archives each container's upperdir under `dir` into `checkpoint_dir`. The
/// guest must be paused.
pub fn tar_rootfs_upper(
    dir: &Path,
    checkpoint_dir: &Path,
    containers: &[String],
    cancel: &CancellationToken,
) -> io::Result<()> {
    for c in containers {
        let name = rootfs_upper_tar_file(c)?;
        let (upper, _) = upper_work_dirs(dir, c);
        tarball::create_with_root(&checkpoint_dir.join(name), &upper, cancel).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while archiving rootfs upper {}: {e}", upper.display()),
            )
        })?;
    }
    Ok(())
}

/// Restores each container's upperdir from the snapshot into `dir`, which is
/// recreated from scratch: stale contents would corrupt the overlay state
/// the guest re-opens. Every directory above the upperdirs is created here,
/// never taken from a tar.
pub fn untar_rootfs_upper(
    dir: &Path,
    snapshot_dir: &Path,
    containers: &[String],
) -> io::Result<()> {
    for c in containers {
        rootfs_upper_tar_file(c)?;
    }
    recreate(dir)?;
    for c in containers {
        let name = rootfs_upper_tar_file(c)?;
        let (upper, _) = mkdir_upper_work_dirs(dir, c).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while creating rootfs upper for {c:?}: {e}"),
            )
        })?;
        tarball::extract(&snapshot_dir.join(name), &upper).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while restoring rootfs upper {}: {e}", upper.display()),
            )
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn upper_dir_with(files: &[(&str, &str)]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        for (rel, content) in files {
            let p = d.path().join(rel);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, content).unwrap();
        }
        d
    }

    fn names(n: &[&str]) -> Vec<String> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn round_trips() {
        let files = [
            ("app_ovl/fs/home/agent/notes.txt", "rootfs write"),
            ("app_ovl/work/index", ""),
            ("app_ovl/work/#1/tmp.bin", "in-flight copy-up temp"),
            ("sidecar_ovl/fs/var/log/s.log", "sidecar write"),
        ];
        let containers = names(&["app_ovl", "sidecar_ovl"]);
        let src = upper_dir_with(&files);
        std::fs::set_permissions(
            src.path().join("app_ovl/fs"),
            std::fs::Permissions::from_mode(0o750),
        )
        .unwrap();
        let ckpt = tempfile::tempdir().unwrap();
        tar_rootfs_upper(
            src.path(),
            ckpt.path(),
            &containers,
            &CancellationToken::new(),
        )
        .unwrap();
        let dst = upper_dir_with(&[("app_ovl/fs/stale.txt", "stale")]);
        untar_rootfs_upper(dst.path(), ckpt.path(), &containers).unwrap();
        for (rel, want) in files.iter().filter(|(r, _)| !r.contains("/work/")) {
            assert_eq!(
                std::fs::read_to_string(dst.path().join(rel)).unwrap(),
                *want,
                "{rel}"
            );
        }
        assert_eq!(
            std::fs::metadata(dst.path().join("app_ovl/fs"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
        assert!(
            std::fs::read_dir(dst.path().join("app_ovl/work"))
                .unwrap()
                .next()
                .is_none(),
            "the workdir came back"
        );
        assert!(
            !dst.path().join("app_ovl/fs/stale.txt").exists(),
            "stale content survived"
        );
    }

    #[test]
    fn stages_pristine_or_from_a_snapshot() {
        let containers = names(&["app_ovl"]);
        let src = upper_dir_with(&[("app_ovl/fs/home/agent/notes.txt", "rootfs write")]);
        let snap = tempfile::tempdir().unwrap();
        tar_rootfs_upper(
            src.path(),
            snap.path(),
            &containers,
            &CancellationToken::new(),
        )
        .unwrap();
        for from_snapshot in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let dirs = pb::ActorDirs {
                root_dir: root.path().to_string_lossy().into(),
                ..Default::default()
            };
            let upper = rootfs_upper_dir(&dirs);
            std::fs::create_dir_all(upper.join("app_ovl/fs")).unwrap();
            std::fs::write(upper.join("app_ovl/fs/stale.txt"), "stale").unwrap();
            stage_rootfs_upper(&dirs, from_snapshot.then_some(snap.path()), &containers).unwrap();
            if from_snapshot {
                assert_eq!(
                    std::fs::read_to_string(upper.join("app_ovl/fs/home/agent/notes.txt")).unwrap(),
                    "rootfs write"
                );
                assert!(!upper.join("app_ovl/fs/stale.txt").exists());
            } else {
                assert!(
                    std::fs::read_dir(&upper).unwrap().next().is_none(),
                    "not pristine"
                );
            }
        }
    }

    #[test]
    fn a_snapshot_cannot_plant_the_layout() {
        let victim = tempfile::tempdir().unwrap();
        let planted = upper_dir_with(&[]);
        for link in ["fs", "work"] {
            std::os::unix::fs::symlink(victim.path(), planted.path().join(link)).unwrap();
        }
        let snap = tempfile::tempdir().unwrap();
        tarball::create(
            &snap.path().join("rootfs-upper-app.tar"),
            planted.path(),
            false,
            &CancellationToken::new(),
        )
        .unwrap();
        let dst = tempfile::tempdir().unwrap();
        untar_rootfs_upper(dst.path(), snap.path(), &names(&["app"])).unwrap();
        for p in ["app", "app/fs", "app/work"] {
            assert!(
                std::fs::symlink_metadata(dst.path().join(p))
                    .unwrap()
                    .is_dir(),
                "{p}"
            );
        }
    }

    #[test]
    fn restored_layout_modes() {
        let src = upper_dir_with(&[("f", "x")]);
        let snap = tempfile::tempdir().unwrap();
        tarball::create(
            &snap.path().join("rootfs-upper-app.tar"),
            src.path(),
            false,
            &CancellationToken::new(),
        )
        .unwrap();
        let base = tempfile::tempdir().unwrap();
        let dst = base.path().join("upper");
        untar_rootfs_upper(&dst, snap.path(), &names(&["app"])).unwrap();
        for (rel, want) in [
            (".", 0o700),
            ("app", 0o700),
            ("app/fs", 0o755),
            ("app/work", 0o700),
        ] {
            let m = std::fs::metadata(dst.join(rel))
                .unwrap()
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(m, want, "{rel}");
        }
    }

    #[test]
    fn rejects_bad_names() {
        for c in ["", ".", "..", "a/b", "/abs", "../x"] {
            assert!(rootfs_upper_tar_file(c).is_err(), "{c:?}");
        }
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        assert!(untar_rootfs_upper(a.path(), b.path(), &names(&["../x"])).is_err());
    }

    #[test]
    fn a_missing_tar_is_an_error() {
        let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        assert!(untar_rootfs_upper(a.path(), b.path(), &names(&["app"])).is_err());
    }
}
