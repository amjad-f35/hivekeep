//! Checks on worker requests that need more than the request itself: the
//! node's filesystem, or what this runtime supports.

use std::path::Path as FsPath;

use hivekeep_core::field::{ErrorList, FieldError, Path};
use hivekeep_core::names;
use hivekeep_core::validation::checks;
use hivekeep_proto::worker::v1 as pb;

use crate::node_path;

/// The snapshot fidelities gVisor serves.
pub const SUPPORTED_FIDELITIES: [pb::SnapshotFidelity; 3] = [
    pb::SnapshotFidelity::Volumes,
    pb::SnapshotFidelity::Rootfs,
    pb::SnapshotFidelity::Memory,
];

/// Rejects a fidelity this runtime cannot serve, including an unset one.
pub fn validate_fidelity(fidelity: i32) -> Result<(), tonic::Status> {
    check_fidelity(fidelity, &SUPPORTED_FIDELITIES, &Path::from("fidelity"))
        .into_result()
        .map_err(Into::into)
}

/// Requires `fidelity` to be one of `supported`; an unset fidelity is
/// reported as missing, anything else (including values outside the enum)
/// as not supported.
pub fn check_fidelity(fidelity: i32, supported: &[pb::SnapshotFidelity], path: &Path) -> ErrorList {
    if fidelity == pb::SnapshotFidelity::Unspecified as i32 {
        return FieldError::required(path, "").into();
    }
    if supported.iter().any(|f| *f as i32 == fidelity) {
        return ErrorList::new();
    }
    let names: Vec<&str> = supported.iter().map(|f| f.as_str_name()).collect();
    let shown = pb::SnapshotFidelity::try_from(fidelity)
        .map(|f| f.as_str_name().to_string())
        .unwrap_or_else(|_| fidelity.to_string());
    FieldError::not_supported(path, shown, &names).into()
}

/// Requires `dir` to be the Actor's own sandbox assets directory, derived from
/// its uid, so a request cannot point the worker at binaries staged for another
/// Actor or anywhere else on the node. The uid is checked first because it
/// becomes a path element.
pub fn validate_sandbox_assets_dir(
    actors_dir: &FsPath,
    actor_uid: &str,
    dir: &str,
    path: &Path,
) -> ErrorList {
    let msgs = names::check_dns_label(actor_uid);
    if !msgs.is_empty() {
        return FieldError::invalid(
            path,
            dir,
            format!(
                "actor_uid {actor_uid:?} is not a valid label: {}",
                msgs.join("; ")
            ),
        )
        .into();
    }
    let want = node_path::actor_sandbox_assets_dir(actors_dir, actor_uid);
    if FsPath::new(dir) != want {
        return FieldError::invalid(
            path,
            dir,
            format!(
                "must be the actor's own sandbox assets dir {}",
                want.display()
            ),
        )
        .into();
    }
    ErrorList::new()
}

/// Requires `p` to be a regular file strictly inside `root`, reached without
/// traversing a symlink.
pub fn validate_runtime_asset_path(root: &str, p: &str, path: &Path) -> ErrorList {
    if p.is_empty() {
        return FieldError::required(path, "").into();
    }
    let shape = checks::abs_clean_dir(path, p);
    if !shape.is_empty() || p == "/" {
        return FieldError::invalid(path, p, "must be an absolute, clean path").into();
    }
    let root = root.trim_end_matches('/');
    if !p.starts_with(&format!("{root}/")) {
        return FieldError::invalid(path, p, format!("must be inside {root}")).into();
    }
    match std::fs::canonicalize(p) {
        Err(e) => return FieldError::invalid(path, p, e.to_string()).into(),
        Ok(resolved) if resolved != FsPath::new(p) => {
            return FieldError::invalid(path, p, "must not traverse a symlink").into()
        }
        Ok(_) => {}
    }
    match std::fs::metadata(p) {
        Ok(m) if m.is_file() => ErrorList::new(),
        _ => FieldError::invalid(path, p, "must be a regular file").into(),
    }
}

/// Requires the runsc binary to come from the Actor's own sandbox assets
/// directory, where the keeper staged it. `actor_dirs` must already have
/// passed request validation.
pub fn validate_runsc_path(
    actors_dir: &FsPath,
    actor_uid: &str,
    actor_dirs: &pb::ActorDirs,
    runsc_path: &str,
) -> Result<(), tonic::Status> {
    let root = &actor_dirs.sandbox_assets_dir;
    let mut errs = validate_sandbox_assets_dir(
        actors_dir,
        actor_uid,
        root,
        &Path::new(["actor_dirs", "sandbox_assets_dir"]),
    );
    if errs.is_empty() {
        errs = validate_runtime_asset_path(root, runsc_path, &Path::from("runsc_path"));
    }
    errs.into_result().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fidelity() {
        for f in SUPPORTED_FIDELITIES {
            assert!(validate_fidelity(f as i32).is_ok(), "{f:?}");
        }
        for f in [0, 99] {
            let err = validate_fidelity(f).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(err.message().contains("fidelity"), "{}", err.message());
        }
        let err = validate_fidelity(99).unwrap_err();
        for want in [
            "SNAPSHOT_FIDELITY_VOLUMES",
            "SNAPSHOT_FIDELITY_ROOTFS",
            "SNAPSHOT_FIDELITY_MEMORY",
        ] {
            assert!(
                err.message().contains(want),
                "{} lacks {want}",
                err.message()
            );
        }
    }

    #[test]
    fn fidelity_against_a_supported_set() {
        use pb::SnapshotFidelity::{Memory, Rootfs, Unspecified, Volumes};
        let all = [Volumes, Rootfs, Memory];
        let no_rootfs = [Volumes, Memory];
        let path = Path::from("fidelity");
        for (name, f, supported, want_err) in [
            ("volumes", Volumes as i32, &all[..], false),
            ("memory", Memory as i32, &all[..], false),
            ("rootfs where supported", Rootfs as i32, &all[..], false),
            (
                "rootfs where unsupported",
                Rootfs as i32,
                &no_rootfs[..],
                true,
            ),
            ("unspecified", Unspecified as i32, &all[..], true),
            ("outside the enum", 99, &all[..], true),
        ] {
            let errs = check_fidelity(f, supported, &path);
            assert_eq!(!errs.is_empty(), want_err, "{name}: {errs:?}");
            for e in errs.iter() {
                assert_eq!(e.path.as_str(), "fidelity", "{name}");
            }
        }
    }

    #[test]
    fn runtime_asset_path_rules() {
        let tmp = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        // Canonical: on macOS the temp dir sits behind the /var symlink.
        let root = std::fs::canonicalize(tmp.path()).unwrap();
        let s = |p: &FsPath| p.to_str().unwrap().to_string();
        let asset = root.join("runsc-abc");
        std::fs::write(&asset, b"").unwrap();
        std::fs::create_dir(root.join("gvisor-abc")).unwrap();
        let nested = root.join("gvisor-abc").join("runsc");
        std::fs::write(&nested, b"").unwrap();
        let outside = std::fs::canonicalize(other.path()).unwrap().join("evil");
        std::fs::write(&outside, b"").unwrap();
        let link = root.join("runsc-link");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let linked_dir = root.join("gvisor-link");
        std::os::unix::fs::symlink(outside.parent().unwrap(), &linked_dir).unwrap();
        let r = s(&root);
        for (name, p, want_err) in [
            ("asset", s(&asset), false),
            ("release dir asset", s(&nested), false),
            ("empty", String::new(), true),
            ("relative", "runsc-abc".to_string(), true),
            ("unclean", format!("{r}/gvisor-abc/../runsc-abc"), true),
            ("outside root", s(&outside), true),
            ("root itself", r.clone(), true),
            ("directory", s(&root.join("gvisor-abc")), true),
            ("sibling with root prefix", format!("{r}-other/runsc"), true),
            ("host binary", "/bin/sh".to_string(), true),
            ("symlink out of root", s(&link), true),
            ("symlinked directory", s(&linked_dir.join("evil")), true),
            ("missing", s(&root.join("runsc-missing")), true),
        ] {
            let errs = validate_runtime_asset_path(&r, &p, &Path::from("runsc_path"));
            assert_eq!(!errs.is_empty(), want_err, "{name} ({p}): {errs:?}");
        }
    }

    #[test]
    fn sandbox_assets_dir_rules() {
        let actors = FsPath::new("/node/actors");
        let path = Path::new(["actor_dirs", "sandbox_assets_dir"]);
        for (name, uid, dir, want_err) in [
            (
                "the actor's own dir",
                "actor-a",
                "/node/actors/actor-a/sandbox-assets",
                false,
            ),
            (
                "another actor's dir",
                "actor-a",
                "/node/actors/actor-b/sandbox-assets",
                true,
            ),
            ("a dir elsewhere", "actor-a", "/usr/bin", true),
            ("the root", "actor-a", "/", true),
            (
                "a traversing uid",
                "../etc",
                "/node/etc/sandbox-assets",
                true,
            ),
            ("an empty uid", "", "/node/actors/sandbox-assets", true),
        ] {
            let errs = validate_sandbox_assets_dir(actors, uid, dir, &path);
            assert_eq!(!errs.is_empty(), want_err, "{name}: {errs:?}");
        }
    }

    #[test]
    fn runsc_path_requires_the_actors_own_dir() {
        let tmp = tempfile::tempdir().unwrap();
        // Canonical, so the symlink check sees the same path it is given.
        let actors = std::fs::canonicalize(tmp.path()).unwrap();
        let stage = |uid: &str| {
            let dir = node_path::actor_sandbox_assets_dir(&actors, uid);
            std::fs::create_dir_all(&dir).unwrap();
            let runsc = dir.join("runsc");
            std::fs::write(&runsc, b"").unwrap();
            runsc.to_str().unwrap().to_string()
        };
        let own = stage("actor-a");
        let other = stage("actor-b");
        let parent = |p: &str| {
            FsPath::new(p)
                .parent()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        };
        let dirs = |d: String| pb::ActorDirs {
            sandbox_assets_dir: d,
            ..Default::default()
        };
        for (name, dir, runsc, want_err) in [
            ("own dir", parent(&own), own.clone(), false),
            ("outside the dir", parent(&own), "/bin/sh".to_string(), true),
            ("another actor's dir", parent(&other), other.clone(), true),
            (
                "the root as the root",
                "/".to_string(),
                "/bin/sh".to_string(),
                true,
            ),
        ] {
            let got = validate_runsc_path(&actors, "actor-a", &dirs(dir), &runsc);
            assert_eq!(got.is_err(), want_err, "{name}: {got:?}");
            if let Err(e) = got {
                assert_eq!(e.code(), tonic::Code::InvalidArgument, "{name}");
            }
        }
    }

    #[test]
    fn runsc_path_names_the_field() {
        let dirs = pb::ActorDirs {
            sandbox_assets_dir: "/var/lib/hivekeep/actors/actor-a/sandbox-assets".into(),
            ..Default::default()
        };
        let err =
            validate_runsc_path(&node_path::actors_dir(), "actor-a", &dirs, "/bin/sh").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("runsc_path"), "{}", err.message());
    }
}
