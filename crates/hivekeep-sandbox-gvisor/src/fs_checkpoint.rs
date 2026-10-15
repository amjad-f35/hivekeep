//! Filesystem checkpoints: which paths runsc saves, which files make up a
//! snapshot, and how a restore finds the bundles again.

use std::collections::HashSet;
use std::io;
use std::path::Path;

use hivekeep_core::snapshot::{FS_CHECKPOINT_SUBDIR, ROOTFS_CHECKPOINT_SUBDIR};
use hivekeep_proto::worker::v1 as pb;

use crate::error::{Error, Result};

/// The manifest runsc writes into each filesystem checkpoint bundle.
pub const FS_CHECKPOINT_MANIFEST: &str = "fscheckpoint.pb";

/// runsc targets (`rootfs/=<container>:/`) for every container's rootfs
/// overlay, bundled under `rootfs/`.
pub fn rootfs_fs_checkpoint_paths(containers: &[pb::Container]) -> Vec<String> {
    containers
        .iter()
        .map(|c| format!("{ROOTFS_CHECKPOINT_SUBDIR}/={}:/", c.name))
        .collect()
}

/// runsc targets (`fs/=<container>:<mount path>`) for every durable-dir
/// mount, bundled under `fs/`. A volume shared by several mounts is listed for
/// each: runsc backs it with one tmpfs and saves it once.
pub fn durable_fs_checkpoint_paths(containers: &[pb::Container]) -> Vec<String> {
    containers
        .iter()
        .flat_map(|c| {
            c.durable_dir_volume_mounts
                .iter()
                .map(move |m| format!("{FS_CHECKPOINT_SUBDIR}/={}:{}", c.name, m.mount_path))
        })
        .collect()
}

/// The snapshot files of the durable-dir checkpoint, those directly in `fs/`.
pub fn durable_snapshot_files(files: &[String]) -> Vec<String> {
    files
        .iter()
        .filter(|f| Path::new(f).parent() == Some(Path::new(FS_CHECKPOINT_SUBDIR)))
        .cloned()
        .collect()
}

/// The sorted relative names of the regular files directly in `dir`,
/// `dir/fs` and `dir/rootfs`: what the keeper ships as the snapshot.
pub fn list_snapshot_files(dir: &Path) -> io::Result<Vec<String>> {
    let mut files = regular_files(dir, None)?;
    for sub in [FS_CHECKPOINT_SUBDIR, ROOTFS_CHECKPOINT_SUBDIR] {
        match regular_files(&dir.join(sub), Some(sub)) {
            Ok(f) => files.extend(f),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    files.sort();
    Ok(files)
}

fn regular_files(dir: &Path, prefix: Option<&str>) -> io::Result<Vec<String>> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        if e.file_type()?.is_file() {
            let name = e.file_name().to_string_lossy().into_owned();
            out.push(match prefix {
                Some(p) => format!("{p}/{name}"),
                None => name,
            });
        }
    }
    Ok(out)
}

fn restores_rootfs(fidelity: pb::SnapshotFidelity) -> bool {
    matches!(
        fidelity,
        pb::SnapshotFidelity::Rootfs | pb::SnapshotFidelity::Memory
    )
}

/// `--fs-restore-image-path` arguments for the bundles in `checkpoint_dir`
/// that apply at `fidelity`: ROOTFS and MEMORY require the `rootfs/` bundle,
/// and every fidelity uses the `fs/` bundle when it is there.
pub fn fs_restore_args(
    checkpoint_dir: &Path,
    fidelity: pb::SnapshotFidelity,
) -> Result<Vec<String>> {
    let mut subdirs = Vec::new();
    if restores_rootfs(fidelity) {
        subdirs.push(ROOTFS_CHECKPOINT_SUBDIR);
    }
    subdirs.push(FS_CHECKPOINT_SUBDIR);
    let mut args = Vec::new();
    for sub in subdirs {
        let dir = checkpoint_dir.join(sub);
        let manifest = dir.join(FS_CHECKPOINT_MANIFEST);
        match std::fs::metadata(&manifest) {
            Ok(_) => {
                args.push("--fs-restore-image-path".to_string());
                args.push(dir.display().to_string());
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if sub == ROOTFS_CHECKPOINT_SUBDIR {
                    return Err(Error::msg(format!(
                        "{} snapshot has no rootfs filesystem checkpoint {}: {e}",
                        fidelity.as_str_name(),
                        manifest.display()
                    )));
                }
            }
            Err(e) => {
                return Err(Error::io(
                    format!(
                        "while checking filesystem checkpoint manifest {}",
                        manifest.display()
                    ),
                    e,
                ))
            }
        }
    }
    Ok(args)
}

/// The containers, in order, that `runsc wait -fsrestore` can wait on: those
/// whose filesystems are restored at `fidelity`. ROOTFS and MEMORY restore
/// every rootfs; VOLUMES only durable-dir volumes, which belong to the first
/// container to mount each.
pub fn fs_restore_containers(
    fidelity: pb::SnapshotFidelity,
    containers: &[pb::Container],
) -> Vec<String> {
    if restores_rootfs(fidelity) {
        return containers.iter().map(|c| c.name.clone()).collect();
    }
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for c in containers {
        let mut owns = false;
        for m in &c.durable_dir_volume_mounts {
            // Every mount is recorded, even once the container owns one.
            owns |= seen.insert(m.name.clone());
        }
        if owns {
            out.push(c.name.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::SnapshotFidelity as F;

    fn c(name: &str, mounts: &[(&str, &str)]) -> pb::Container {
        pb::Container {
            name: name.into(),
            durable_dir_volume_mounts: mounts
                .iter()
                .map(|(n, p)| pb::VolumeMount {
                    name: (*n).into(),
                    mount_path: (*p).into(),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn checkpoint_paths() {
        assert_eq!(
            rootfs_fs_checkpoint_paths(&[c("app", &[]), c("sidecar", &[])]),
            s(&["rootfs/=app:/", "rootfs/=sidecar:/"])
        );
        let cs = [
            c("app", &[("data", "/var/data"), ("cache", "/var/cache")]),
            c("sidecar", &[("logs", "/var/log/app")]),
        ];
        assert_eq!(
            durable_fs_checkpoint_paths(&cs),
            s(&[
                "fs/=app:/var/data",
                "fs/=app:/var/cache",
                "fs/=sidecar:/var/log/app"
            ])
        );
    }

    const ALL_FILES: &[&str] = &[
        "checkpoint.img",
        "fs/fscheckpoint.pb",
        "fs/multitar.img",
        "fs/pages.img",
        "fs/pages_meta.img",
        "pages.img",
        "pages_meta.img",
        "rootfs/fscheckpoint.pb",
        "rootfs/multitar.img",
        "rootfs/pages.img",
        "rootfs/pages_meta.img",
    ];

    #[test]
    fn durable_files_are_the_fs_subset() {
        assert_eq!(
            durable_snapshot_files(&s(ALL_FILES)),
            s(&[
                "fs/fscheckpoint.pb",
                "fs/multitar.img",
                "fs/pages.img",
                "fs/pages_meta.img"
            ])
        );
    }

    #[test]
    fn lists_snapshot_files() {
        let d = tempfile::tempdir().unwrap();
        for sub in ["fs", "rootfs", "ignored-dir"] {
            std::fs::create_dir_all(d.path().join(sub)).unwrap();
        }
        for rel in ALL_FILES.iter().chain(&["ignored-dir/x"]) {
            std::fs::write(d.path().join(rel), b"data").unwrap();
        }
        assert_eq!(list_snapshot_files(d.path()).unwrap(), s(ALL_FILES));
        // No subdirectories at all.
        let e = tempfile::tempdir().unwrap();
        std::fs::write(e.path().join("checkpoint.img"), b"").unwrap();
        assert_eq!(
            list_snapshot_files(e.path()).unwrap(),
            s(&["checkpoint.img"])
        );
    }

    fn manifest(dir: &Path, sub: &str) -> String {
        let d = dir.join(sub);
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join(FS_CHECKPOINT_MANIFEST), b"manifest").unwrap();
        d.display().to_string()
    }

    #[test]
    fn restore_args() {
        let none = tempfile::tempdir().unwrap();
        assert!(fs_restore_args(none.path(), F::Volumes).unwrap().is_empty());
        for f in [F::Rootfs, F::Memory] {
            assert!(fs_restore_args(none.path(), f).is_err());
        }

        let only_rootfs = tempfile::tempdir().unwrap();
        let r = manifest(only_rootfs.path(), "rootfs");
        for f in [F::Rootfs, F::Memory] {
            assert_eq!(
                fs_restore_args(only_rootfs.path(), f).unwrap(),
                vec!["--fs-restore-image-path".into(), r.clone()]
            );
        }
        assert!(fs_restore_args(only_rootfs.path(), F::Volumes)
            .unwrap()
            .is_empty());

        let only_fs = tempfile::tempdir().unwrap();
        let fsd = manifest(only_fs.path(), "fs");
        for f in [F::Rootfs, F::Memory] {
            let err = fs_restore_args(only_fs.path(), f).unwrap_err();
            assert!(err.to_string().contains("rootfs"), "{err}");
        }
        assert_eq!(
            fs_restore_args(only_fs.path(), F::Volumes).unwrap(),
            vec!["--fs-restore-image-path".into(), fsd]
        );

        let both = tempfile::tempdir().unwrap();
        let r = manifest(both.path(), "rootfs");
        let fsd = manifest(both.path(), "fs");
        for f in [F::Rootfs, F::Memory] {
            assert_eq!(
                fs_restore_args(both.path(), f).unwrap(),
                vec![
                    "--fs-restore-image-path".into(),
                    r.clone(),
                    "--fs-restore-image-path".into(),
                    fsd.clone()
                ]
            );
        }
        assert_eq!(
            fs_restore_args(both.path(), F::Volumes).unwrap(),
            vec!["--fs-restore-image-path".into(), fsd]
        );
    }

    #[test]
    fn restore_containers() {
        let cs = [
            c("init", &[]),
            c("app", &[("data", "/var/data")]),
            c("sidecar-shared-only", &[("data", "/mnt/data")]),
            c(
                "sidecar-with-own-vol",
                &[("data", "/mnt/data"), ("logs", "/var/log")],
            ),
        ];
        assert_eq!(
            fs_restore_containers(F::Volumes, &cs),
            s(&["app", "sidecar-with-own-vol"])
        );
        let all = s(&["init", "app", "sidecar-shared-only", "sidecar-with-own-vol"]);
        assert_eq!(fs_restore_containers(F::Rootfs, &cs), all);
        assert_eq!(fs_restore_containers(F::Memory, &cs), all);
    }
}
