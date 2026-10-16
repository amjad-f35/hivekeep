//! The Actor's volumes in the micro-VM: durable-dir volumes (host-backed and
//! snapshotted as per-volume tars), CSI volumes and system-info volumes, all
//! bound into the shared virtio-fs tree.

use std::io;
use std::path::{Path, PathBuf};

use hivekeep_proto::worker::v1 as pb;
use tokio_util::sync::CancellationToken;

use crate::guest_spec::{SHARE_CSI, SHARE_DURABLE, SHARE_SYSTEM_INFO};
use crate::kata::overlay;
use crate::kata::paths::shared_dir;
use crate::tarball;

/// Whether any container mounts a durable-dir volume.
pub fn has_durable_volumes(cs: &[pb::Container]) -> bool {
    cs.iter().any(|c| !c.durable_dir_volume_mounts.is_empty())
}

/// Whether any container mounts a CSI volume.
pub fn has_csi_volumes(cs: &[pb::Container]) -> bool {
    cs.iter().any(|c| !c.external_volume_mounts.is_empty())
}

/// Whether any container mounts a system-info volume.
pub fn has_system_info_volumes(cs: &[pb::Container]) -> bool {
    cs.iter().any(|c| !c.system_info_volume_mounts.is_empty())
}

/// The sorted, deduplicated durable-dir volume names.
pub fn durable_volume_names(cs: &[pb::Container]) -> Vec<String> {
    let mut names: Vec<String> = cs
        .iter()
        .flat_map(|c| c.durable_dir_volume_mounts.iter().map(|m| m.name.clone()))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Whether `name` is a single local path element.
pub fn is_single_element(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\0')
}

/// The snapshot file of one durable-dir volume: its contents plus a root
/// entry for the directory's own metadata. The directory itself always
/// comes from the keeper, never from the snapshot: the guest binds it by
/// path, so a planted symlink there would expose its target.
pub fn durable_tar_file(volume: &str) -> io::Result<String> {
    if !is_single_element(volume) {
        return Err(io::Error::other(format!(
            "invalid durable-dir volume name {volume:?}"
        )));
    }
    Ok(format!("durable-dir-{volume}.tar"))
}

/// Archives each volume under `dir` into `checkpoint_dir`, one tar each, and
/// returns the file names. The guest must be paused: the share is
/// write-through, so completed writes are on the host, but a running guest
/// could add more after the walk.
pub fn tar_durable_volumes(
    dir: &Path,
    checkpoint_dir: &Path,
    volumes: &[String],
    cancel: &CancellationToken,
) -> io::Result<Vec<String>> {
    let mut files = Vec::new();
    for v in volumes {
        let name = durable_tar_file(v)?;
        tarball::create_with_root(&checkpoint_dir.join(&name), &dir.join(v), cancel).map_err(
            |e| {
                io::Error::new(
                    e.kind(),
                    format!("while archiving durable-dir volume {v:?}: {e}"),
                )
            },
        )?;
        files.push(name);
    }
    Ok(files)
}

/// Restores each volume from the snapshot into its directory under `dir`. A
/// volume without a tar (added to the template since) stays empty. Runs
/// before the share's virtiofsd starts, so the guest never sees a volume
/// mid-restore.
pub fn untar_durable_volumes(
    dir: &Path,
    snapshot_dir: &Path,
    volumes: &[String],
) -> io::Result<()> {
    for v in volumes {
        let tar = snapshot_dir.join(durable_tar_file(v)?);
        if !tar.exists() {
            continue;
        }
        let vol = dir.join(v);
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&vol)?;
        }
        tarball::extract(&tar, &vol).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("while restoring durable-dir volume {v:?}: {e}"),
            )
        })?;
    }
    Ok(())
}

fn check_exists(src: &Path, what: &str) -> io::Result<()> {
    std::fs::metadata(src).map(drop).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while checking {what} dir {}: {e}", src.display()),
        )
    })
}

/// Binds the Actor's durable-dir volume directory into the share.
pub fn stage_durable_volumes(actor_uid: &str, src: &Path) -> io::Result<()> {
    check_exists(src, "durable-dir volumes")?;
    overlay::bind_into_share(src, actor_uid, SHARE_DURABLE).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while binding durable-dir volumes into the shared tree: {e}"),
        )
    })
}

/// Binds the Actor's CSI volume directory into the share.
pub fn stage_csi_volumes(actor_uid: &str, src: &Path) -> io::Result<()> {
    check_exists(src, "CSI volumes")?;
    overlay::bind_into_share(src, actor_uid, SHARE_CSI).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while binding CSI volumes into the shared tree: {e}"),
        )
    })
}

/// Binds the Actor's system-info directory into the share, read-only: the
/// keeper is its only writer and writes the host source directly.
pub fn stage_system_info_volumes(actor_uid: &str, src: &Path) -> io::Result<()> {
    check_exists(src, "system-info volumes")?;
    let dst: PathBuf = shared_dir(actor_uid).join(SHARE_SYSTEM_INFO);
    overlay::bind_read_only(src, &dst)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vm(name: &str, path: &str) -> pb::VolumeMount {
        pb::VolumeMount {
            name: name.into(),
            mount_path: path.into(),
        }
    }

    #[test]
    fn has_durable() {
        assert!(!has_durable_volumes(&[]));
        assert!(!has_durable_volumes(&[pb::Container {
            name: "app".into(),
            ..Default::default()
        }]));
        assert!(has_durable_volumes(&[
            pb::Container {
                name: "sidecar".into(),
                ..Default::default()
            },
            pb::Container {
                name: "app".into(),
                durable_dir_volume_mounts: vec![vm("data", "/home/counter")],
                ..Default::default()
            },
        ]));
    }

    #[test]
    fn has_csi() {
        assert!(!has_csi_volumes(&[]));
        assert!(!has_csi_volumes(&[pb::Container {
            name: "c1".into(),
            durable_dir_volume_mounts: vec![vm("data", "/data")],
            ..Default::default()
        }]));
        assert!(has_csi_volumes(&[pb::Container {
            name: "c1".into(),
            external_volume_mounts: vec![vm("csi-vol", "/csi")],
            ..Default::default()
        }]));
        assert!(has_csi_volumes(&[
            pb::Container {
                name: "c1".into(),
                ..Default::default()
            },
            pb::Container {
                name: "c2".into(),
                external_volume_mounts: vec![vm("csi-vol", "/csi")],
                ..Default::default()
            },
        ]));
    }

    #[test]
    fn durable_names_are_sorted_and_unique() {
        let cs = [
            pb::Container {
                name: "app".into(),
                durable_dir_volume_mounts: vec![vm("data", "/data"), vm("cache", "/cache")],
                ..Default::default()
            },
            pb::Container {
                name: "sidecar".into(),
                durable_dir_volume_mounts: vec![vm("data", "/shared")],
                ..Default::default()
            },
        ];
        assert_eq!(durable_volume_names(&cs), ["cache", "data"]);
    }

    fn mk(dir: &Path, vols: &[&str]) {
        for v in vols {
            std::fs::create_dir_all(dir.join(v)).unwrap();
        }
    }

    #[test]
    fn durable_volumes_round_trip() {
        use std::os::unix::fs::PermissionsExt;
        let src = tempfile::tempdir().unwrap();
        mk(src.path(), &["data", "cache"]);
        std::fs::write(src.path().join("data/a.txt"), "42").unwrap();
        std::fs::write(src.path().join("cache/a.txt"), "7").unwrap();
        std::fs::set_permissions(
            src.path().join("data"),
            std::fs::Permissions::from_mode(0o750),
        )
        .unwrap();
        let ckpt = tempfile::tempdir().unwrap();
        let vols = vec!["cache".to_string(), "data".to_string()];
        let files =
            tar_durable_volumes(src.path(), ckpt.path(), &vols, &CancellationToken::new()).unwrap();
        assert_eq!(files, ["durable-dir-cache.tar", "durable-dir-data.tar"]);
        let dst = tempfile::tempdir().unwrap();
        mk(dst.path(), &["data", "cache"]);
        untar_durable_volumes(dst.path(), ckpt.path(), &vols).unwrap();
        assert_eq!(
            std::fs::read_to_string(dst.path().join("data/a.txt")).unwrap(),
            "42"
        );
        assert_eq!(
            std::fs::read_to_string(dst.path().join("cache/a.txt")).unwrap(),
            "7"
        );
        assert_eq!(
            std::fs::metadata(dst.path().join("data"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );
    }

    #[test]
    fn a_snapshot_cannot_plant_the_volume_dir() {
        let victim = tempfile::tempdir().unwrap();
        let planted = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(victim.path(), planted.path().join("data")).unwrap();
        let snap = tempfile::tempdir().unwrap();
        // An entry named after the volume.
        tarball::create(
            &snap.path().join("durable-dir-data.tar"),
            planted.path(),
            false,
            &CancellationToken::new(),
        )
        .unwrap();
        let dst = tempfile::tempdir().unwrap();
        mk(dst.path(), &["data"]);
        untar_durable_volumes(dst.path(), snap.path(), &["data".to_string()]).unwrap();
        assert!(std::fs::symlink_metadata(dst.path().join("data"))
            .unwrap()
            .is_dir());
        assert!(std::fs::read_dir(victim.path()).unwrap().next().is_none());
    }

    #[test]
    fn a_missing_tar_restores_empty() {
        let dst = tempfile::tempdir().unwrap();
        mk(dst.path(), &["new"]);
        let snap = tempfile::tempdir().unwrap();
        untar_durable_volumes(dst.path(), snap.path(), &["new".to_string()]).unwrap();
        assert!(std::fs::read_dir(dst.path().join("new"))
            .unwrap()
            .next()
            .is_none());
    }

    #[test]
    fn rejects_bad_volume_names() {
        for v in ["", ".", "..", "a/b", "/abs", "../x"] {
            assert!(durable_tar_file(v).is_err(), "{v:?}");
        }
    }
}
