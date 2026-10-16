//! The snapshot fidelity ladder, lowest to highest, each level including the
//! ones below: VOLUMES holds the durable volumes, ROOTFS adds each
//! container's rootfs upper, MEMORY adds the guest. These predicates are the
//! one place that says which levels carry which layer.

use std::collections::HashMap;
use std::path::Path as FsPath;

use hivekeep_core::field::{ErrorList, Path};
use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::request_checks;

/// Whether snapshots at `f` carry the rootfs uppers.
pub fn includes_rootfs(f: i32) -> bool {
    f == pb::SnapshotFidelity::Rootfs as i32 || f == pb::SnapshotFidelity::Memory as i32
}

/// Whether snapshots at `f` carry the guest itself, so a restore resumes it
/// (and its CPU counters) rather than cold-booting.
pub fn includes_memory(f: i32) -> bool {
    f == pb::SnapshotFidelity::Memory as i32
}

/// Refuses a fidelity this runtime does not serve. Every level is served:
/// the rootfs upper is host-backed, so it ships with or without memory.
pub fn validate_fidelity(f: i32) -> Result<(), tonic::Status> {
    request_checks::validate_fidelity(f)
}

/// Requires the runtime assets to come from the Actor's own sandbox assets
/// directory, where the keeper staged them. The directory is derived from
/// the uid and the request's must match it, so a request cannot name a root
/// of its own choosing.
pub fn validate_runtime_asset_paths(
    actors_dir: &FsPath,
    actor_uid: &str,
    dirs: Option<&pb::ActorDirs>,
    paths: &HashMap<String, String>,
) -> Result<(), tonic::Status> {
    let root = dirs
        .map(|d| d.sandbox_assets_dir.as_str())
        .unwrap_or_default();
    let errs = request_checks::validate_sandbox_assets_dir(
        actors_dir,
        actor_uid,
        root,
        &Path::new(["actor_dirs", "sandbox_assets_dir"]),
    );
    if !errs.is_empty() {
        return errs.into_result().map_err(Into::into);
    }
    let mut errs = ErrorList::new();
    let mut names: Vec<_> = paths.keys().collect();
    names.sort();
    for name in names {
        errs.extend(request_checks::validate_runtime_asset_path(
            root,
            &paths[name],
            &Path::from("runtime_asset_paths").key(name),
        ));
    }
    errs.into_result().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hivekeep_sandbox_gvisor::node_path;

    #[test]
    fn the_ladder() {
        use pb::SnapshotFidelity as F;
        for (f, rootfs, memory) in [
            (F::Unspecified, false, false),
            (F::Volumes, false, false),
            (F::Rootfs, true, false),
            (F::Memory, true, true),
        ] {
            assert_eq!(includes_rootfs(f as i32), rootfs, "{f:?}");
            assert_eq!(includes_memory(f as i32), memory, "{f:?}");
        }
    }

    #[test]
    fn validates_fidelities() {
        use pb::SnapshotFidelity as F;
        for f in [F::Volumes, F::Rootfs, F::Memory] {
            assert!(validate_fidelity(f as i32).is_ok());
        }
        for f in [F::Unspecified as i32, 99] {
            let err = validate_fidelity(f).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(err.message().contains("fidelity"), "{}", err.message());
        }
    }

    #[test]
    fn runtime_assets_must_come_from_the_actors_own_dir() {
        let tmp = tempfile::tempdir().unwrap();
        // Canonical, so no symlink in the temp dir's own path trips the check.
        let actors = tmp.path().canonicalize().unwrap();
        let stage = |uid: &str| {
            let dir = node_path::actor_sandbox_assets_dir(&actors, uid);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("cloud-hypervisor"), "").unwrap();
            dir.to_string_lossy().to_string()
        };
        let (own, other) = (stage("actor-a"), stage("actor-b"));
        for (name, dir, asset, want_err) in [
            (
                "asset in the actor's own dir",
                own.clone(),
                format!("{own}/cloud-hypervisor"),
                false,
            ),
            (
                "asset outside the actor's dir",
                own.clone(),
                "/bin/sh".to_string(),
                true,
            ),
            (
                "another actor's dir named as the root",
                other.clone(),
                format!("{other}/cloud-hypervisor"),
                true,
            ),
            (
                "the root named as the root",
                "/".to_string(),
                "/bin/sh".to_string(),
                true,
            ),
        ] {
            let dirs = pb::ActorDirs {
                sandbox_assets_dir: dir,
                ..Default::default()
            };
            let paths = HashMap::from([("cloud-hypervisor".to_string(), asset)]);
            assert_eq!(
                validate_runtime_asset_paths(&actors, "actor-a", Some(&dirs), &paths).is_err(),
                want_err,
                "{name}"
            );
        }
    }
}
