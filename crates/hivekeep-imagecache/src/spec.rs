//! The per-bundle overlay spec: the contract between the keeper (which
//! cannot mount) and the worker runtimes (which mount the rootfs overlay
//! just before running the workload).
//!
//! The overlay's mountpoint, upperdir and workdir are always the bundle's
//! own `rootfs/`, `upper/` and `work/`, derived from the bundle path by the
//! consumer rather than trusted from the file. A bundle without a spec has a
//! plain rootfs directory and needs no mount.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub use hivekeep_node::image::{ImageVolumeOverlay, OverlaySpec, OVERLAY_SPEC_FILE_NAME};

use crate::confined;
use crate::error::{Error, Result};

/// A layer's unpacked tree, inside its pool directory.
pub const LAYER_FS_DIR_NAME: &str = "fs";

/// Where an image volume is composed inside a bundle; the OCI spec binds it
/// into the container.
pub fn image_volume_mount_path(bundle: &Path, volume: &str) -> PathBuf {
    bundle.join("volumes").join(volume)
}

/// Writes `spec` into `bundle`, atomically: the eviction root-set scan
/// must never see a partial spec, which could parse with layers missing and
/// leave them evictable while an actor uses them.
pub fn write_spec(bundle: &Path, spec: &mut OverlaySpec) -> Result<()> {
    spec.version = 1;
    let data = serde_json::to_vec_pretty(spec)
        .map_err(|e| Error::msg(format!("while encoding overlay spec: {e}")))?;
    let mut tmp = tempfile::Builder::new()
        .prefix(&format!(".{OVERLAY_SPEC_FILE_NAME}.tmp-"))
        .tempfile_in(bundle)
        .map_err(|e| Error::io("while creating overlay spec temp file", e))?;
    tmp.write_all(&data)
        .map_err(|e| Error::io("while writing overlay spec", e))?;
    tmp.persist(bundle.join(OVERLAY_SPEC_FILE_NAME))
        .map_err(|e| Error::io("while moving overlay spec into place", e.error))?;
    Ok(())
}

/// Reads `bundle`'s overlay spec; `None` when it has none.
pub fn read_spec(bundle: &Path) -> Result<Option<OverlaySpec>> {
    let path = bundle.join(OVERLAY_SPEC_FILE_NAME);
    let b = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(Error::io("while reading overlay spec", e)),
    };
    #[derive(serde::Deserialize)]
    struct Probe {
        #[serde(default)]
        version: u32,
        #[serde(default)]
        layers: Option<serde_json::Value>,
    }
    let probe: Probe = serde_json::from_slice(&b)
        .map_err(|e| Error::msg(format!("while decoding overlay spec: {e}")))?;
    if probe.version != 1 {
        return Err(Error::msg(format!(
            "overlay spec {:?} has version {}, want 1",
            path, probe.version
        )));
    }
    let mut value: serde_json::Value = serde_json::from_slice(&b)
        .map_err(|e| Error::msg(format!("while decoding overlay spec: {e}")))?;
    if probe.layers.is_none() || probe.layers.as_ref().is_some_and(|l| l.is_null()) {
        value["layers"] = serde_json::Value::Array(Vec::new());
    }
    let spec: OverlaySpec = serde_json::from_value(value)
        .map_err(|e| Error::msg(format!("while decoding overlay spec: {e}")))?;
    Ok(Some(spec))
}

/// The overlayfs lowerdirs for bottom-first `layers`: top-most first, each
/// the layer's `fs/` tree. An image may list one layer several times; the
/// pool stores it once and overlayfs rejects a repeated lowerdir, so only
/// the top-most occurrence (the only one that can affect the merged view)
/// is kept.
pub fn overlay_lower_dirs(layers: &[String]) -> Vec<PathBuf> {
    let mut seen = std::collections::HashSet::new();
    layers
        .iter()
        .rev()
        .map(|l| Path::new(l).join(LAYER_FS_DIR_NAME))
        .filter(|p| seen.insert(p.clone()))
        .collect()
}

/// Creates the absolute in-rootfs `extra_dirs` inside the (mounted) rootfs,
/// confined to it: a symlink planted by the image cannot redirect the write.
pub fn create_extra_dirs(rootfs: &Path, extra_dirs: &[String]) -> Result<()> {
    if extra_dirs.is_empty() {
        return Ok(());
    }
    let root = confined::open_root(rootfs)
        .map_err(|e| Error::io(format!("while opening rootfs {rootfs:?}"), e))?;
    for d in extra_dirs {
        let rel = d.trim_start_matches('/');
        if rel.is_empty() {
            continue;
        }
        let cleaned = confined::clean(rel.as_bytes());
        if !confined::is_local(&cleaned) {
            return Err(Error::msg(format!("extra dir {d:?} escapes the rootfs")));
        }
        let rel = String::from_utf8_lossy(&cleaned).into_owned();
        confined::mkdir_all(&root, Path::new(&rel), 0o755)
            .map_err(|e| Error::io(format!("while creating extra dir {rel:?}"), e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_spec_round_trip() {
        let bundle = tempfile::tempdir().unwrap();
        let mut spec = OverlaySpec {
            layers: vec![
                "/cache/layers/sha256/aaa".into(),
                "/cache/layers/sha256/bbb".into(),
            ],
            extra_dirs: vec!["/run/hivekeep".into()],
            ..Default::default()
        };
        write_spec(bundle.path(), &mut spec).unwrap();
        let out = read_spec(bundle.path()).unwrap().expect("a spec");
        assert_eq!(out.layers, spec.layers);
        assert_eq!(out.extra_dirs, spec.extra_dirs);
    }

    // A bundle without a spec reads as none: the consumer leaves its rootfs
    // untouched.
    #[test]
    fn read_spec_absent() {
        assert!(read_spec(tempfile::tempdir().unwrap().path())
            .unwrap()
            .is_none());
    }

    #[test]
    fn read_spec_unknown_version() {
        let bundle = tempfile::tempdir().unwrap();
        fs::write(
            bundle.path().join(OVERLAY_SPEC_FILE_NAME),
            br#"{"version":99}"#,
        )
        .unwrap();
        assert!(read_spec(bundle.path()).is_err());
    }

    #[test]
    fn overlay_lower_dirs_reverses_to_top_first() {
        let got = overlay_lower_dirs(&["/c/base".into(), "/c/top".into()]);
        assert_eq!(
            got,
            vec![PathBuf::from("/c/top/fs"), PathBuf::from("/c/base/fs")]
        );
    }

    // overlayfs rejects a repeated lowerdir with ELOOP, so duplicates
    // collapse to the topmost occurrence.
    #[test]
    fn overlay_lower_dirs_deduplicates_keeping_topmost() {
        let got = overlay_lower_dirs(&[
            "/c/base".into(),
            "/c/dup".into(),
            "/c/dup".into(),
            "/c/top".into(),
        ]);
        assert_eq!(
            got,
            vec![
                PathBuf::from("/c/top/fs"),
                PathBuf::from("/c/dup/fs"),
                PathBuf::from("/c/base/fs")
            ]
        );
    }

    #[test]
    fn create_extra_dirs_creates_target_inside_rootfs() {
        let root = tempfile::tempdir().unwrap();
        create_extra_dirs(root.path(), &["/run/hivekeep".into()]).unwrap();
        assert!(root.path().join("run/hivekeep").is_dir());
    }

    #[test]
    fn create_extra_dirs_refuses_symlink_escaping_the_rootfs() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("run")).unwrap();
        assert!(create_extra_dirs(root.path(), &["/run/hivekeep".into()]).is_err());
        assert_eq!(
            fs::read_dir(outside.path()).unwrap().count(),
            0,
            "write escaped the rootfs"
        );
    }

    #[test]
    fn create_extra_dirs_refuses_parent_traversal() {
        let root = tempfile::tempdir().unwrap();
        assert!(create_extra_dirs(root.path(), &["/../escape".into()]).is_err());
    }

    #[test]
    fn spec_image_digest_compat() {
        let dir = tempfile::tempdir().unwrap();
        let digest = format!("sha256:{}", "ef".repeat(32));
        let mut spec = OverlaySpec {
            image_digest: digest.clone(),
            layers: vec!["/pool/layers/sha256/aa".into()],
            ..Default::default()
        };
        write_spec(dir.path(), &mut spec).unwrap();
        assert_eq!(read_spec(dir.path()).unwrap().unwrap().image_digest, digest);
        // A spec without imageDigest still parses.
        fs::write(
            dir.path().join(OVERLAY_SPEC_FILE_NAME),
            serde_json::to_vec(
                &serde_json::json!({"version": 1, "layers": ["/pool/layers/sha256/bb"]}),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(read_spec(dir.path()).unwrap().unwrap().image_digest, "");
    }

    #[test]
    fn write_spec_leaves_no_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut spec = OverlaySpec {
            layers: vec!["/pool/layers/sha256/aa".into()],
            ..Default::default()
        };
        write_spec(dir.path(), &mut spec).unwrap();
        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(
            names,
            vec![std::ffi::OsString::from(OVERLAY_SPEC_FILE_NAME)]
        );
    }
}
