//! Repairing implicit-parent directory metadata in a composed rootfs.
//!
//! In overlayfs the top-most layer containing a directory supplies the
//! merged directory's attributes. When a layer tar omitted a parent entry,
//! the pool holds a fabricated root:root 0755 directory there, which would
//! shadow the real metadata a lower layer declared (`/tmp` losing 1777,
//! `/root` opening up from 0700). The cache unpacks layers standalone, so
//! the repair happens at compose time: after mounting, the affected
//! directories get the attributes of the top-most layer that declared them.
//! The writes copy up into the actor's private upper; the shared pool is
//! never modified.
//!
//! Not repaired: directory mtimes and xattrs, and a directory implicit in
//! every layer of the chain keeps the fabricated attributes.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use crate::confined;
use crate::error::{Error, Result};
use crate::spec::LAYER_FS_DIR_NAME;
use crate::unpack::{read_whiteouts, validate_tar_name};

/// Permission bits plus setuid, setgid and sticky.
const MODE_MASK: u32 = 0o7777;

/// One merged-view repair: set `path` (rootfs-relative) to `mode` and,
/// when known, the owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirFixup {
    /// The rootfs-relative directory.
    pub path: String,
    /// Permission and special bits.
    pub mode: u32,
    /// The owner, when the declaring layer's could be read.
    pub owner: Option<(u32, u32)>,
}

/// The repairs an image whose bottom-first layer chain is `layers` needs.
/// Layers without recorded implicit dirs contribute none, so layers from
/// before the recording stay compatible.
pub fn resolve_implicit_dir_fixups(layers: &[PathBuf]) -> Result<Vec<DirFixup>> {
    let mut implicit_at: Vec<HashSet<String>> = Vec::with_capacity(layers.len());
    let mut union = BTreeSet::new();
    for layer in layers {
        let meta = read_whiteouts(layer)?;
        let mut set = HashSet::new();
        for p in meta.implicit_dirs {
            match validate_tar_name(&p) {
                Err(e) => {
                    return Err(Error::msg(format!(
                        "invalid implicit dir path in {layer:?}: {e}"
                    )))
                }
                Ok(None) => {}
                Ok(Some(rel)) => {
                    union.insert(rel.clone());
                    set.insert(rel);
                }
            }
        }
        implicit_at.push(set);
    }
    let stat_dir = |i: usize, rel: &str| -> Option<fs::Metadata> {
        fs::symlink_metadata(layers[i].join(LAYER_FS_DIR_NAME).join(rel))
            .ok()
            .filter(|m| m.is_dir())
    };
    let mut fixups = Vec::new();
    for p in union {
        // Only repair when the top-most provider is implicit.
        let Some(top) = (0..layers.len()).rev().find(|&i| stat_dir(i, &p).is_some()) else {
            continue;
        };
        if !implicit_at[top].contains(&p) {
            continue;
        }
        for j in (0..top).rev() {
            if implicit_at[j].contains(&p) {
                continue;
            }
            if let Some(md) = stat_dir(j, &p) {
                fixups.push(DirFixup {
                    path: p.clone(),
                    mode: md.permissions().mode() & MODE_MASK,
                    owner: Some((md.uid(), md.gid())),
                });
                break;
            }
        }
    }
    Ok(fixups)
}

/// Applies `fixups` through the (mounted) rootfs, confined to it. Paths
/// hidden in the merged view (whited out, or shadowed by a non-directory)
/// are skipped.
pub fn apply_dir_fixups(rootfs: &Path, fixups: &[DirFixup]) -> Result<()> {
    if fixups.is_empty() {
        return Ok(());
    }
    let root = confined::open_root(rootfs)
        .map_err(|e| Error::io(format!("while opening rootfs {rootfs:?}"), e))?;
    for f in fixups {
        let rel = Path::new(&f.path);
        match root.symlink_metadata(rel) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(Error::io(format!("while checking {:?}", f.path), e)),
            Ok(md) if !md.is_dir() => continue,
            Ok(_) => {}
        }
        // Chown first: it can clear setuid/setgid, which the chmod restores.
        if let Some((uid, gid)) = f.owner {
            confined::lchown(&root, rel, uid, gid)
                .map_err(|e| Error::io(format!("while restoring owner of {:?}", f.path), e))?;
        }
        confined::chmod(&root, rel, f.mode)
            .map_err(|e| Error::io(format!("while restoring mode of {:?}", f.path), e))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unpack::{WhiteoutSet, LAYER_WHITEOUTS_FILE_NAME};

    /// A fake pool layer: `dirs` maps a relative path to its mode;
    /// `implicit` is the recorded implicit-dir list.
    fn make_layer(dirs: &[(&str, u32)], implicit: Option<&[&str]>) -> tempfile::TempDir {
        let layer = tempfile::tempdir().unwrap();
        let fs_dir = layer.path().join(LAYER_FS_DIR_NAME);
        fs::create_dir(&fs_dir).unwrap();
        for (rel, mode) in dirs {
            let p = fs_dir.join(rel);
            fs::create_dir_all(&p).unwrap();
            fs::set_permissions(&p, fs::Permissions::from_mode(*mode)).unwrap();
        }
        if let Some(implicit) = implicit {
            let wh = WhiteoutSet {
                version: 1,
                implicit_dirs: implicit.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            };
            fs::write(
                layer.path().join(LAYER_WHITEOUTS_FILE_NAME),
                serde_json::to_vec(&wh).unwrap(),
            )
            .unwrap();
        }
        layer
    }

    fn paths(ls: &[&tempfile::TempDir]) -> Vec<PathBuf> {
        ls.iter().map(|l| l.path().to_path_buf()).collect()
    }

    // The canonical shadowing case: the base declares tmp 1777, an upper
    // layer created it implicitly.
    #[test]
    fn resolve_and_apply_implicit_dir_fixups() {
        let base = make_layer(&[("tmp", 0o1777)], None);
        let upper = make_layer(&[("tmp", 0o755)], Some(&["tmp"]));
        let fixups = resolve_implicit_dir_fixups(&paths(&[&base, &upper])).unwrap();
        assert_eq!(fixups.len(), 1, "{fixups:?}");
        assert_eq!(fixups[0].path, "tmp");
        assert_eq!(fixups[0].mode, 0o1777, "sticky bit lost");
        assert!(fixups[0].owner.is_some());

        let rootfs = tempfile::tempdir().unwrap();
        fs::create_dir(rootfs.path().join("tmp")).unwrap();
        fs::set_permissions(rootfs.path().join("tmp"), fs::Permissions::from_mode(0o755)).unwrap();
        apply_dir_fixups(rootfs.path(), &fixups).unwrap();
        let mode = fs::symlink_metadata(rootfs.path().join("tmp"))
            .unwrap()
            .permissions()
            .mode()
            & MODE_MASK;
        assert_eq!(mode, 0o1777);
    }

    #[test]
    fn no_repair_when_topmost_provider_declared_the_dir() {
        let lower = make_layer(&[("d", 0o755)], Some(&["d"]));
        let top = make_layer(&[("d", 0o700)], None);
        assert!(resolve_implicit_dir_fixups(&paths(&[&lower, &top]))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn no_repair_when_implicit_in_every_layer() {
        let a = make_layer(&[("d", 0o755)], Some(&["d"]));
        let b = make_layer(&[("d", 0o755)], Some(&["d"]));
        assert!(resolve_implicit_dir_fixups(&paths(&[&a, &b]))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn no_repair_without_metadata() {
        let a = make_layer(&[("d", 0o700)], None);
        let b = make_layer(&[("d", 0o755)], None);
        assert!(resolve_implicit_dir_fixups(&paths(&[&a, &b]))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn escaping_metadata_path_is_rejected() {
        let a = make_layer(&[], Some(&["../escape"]));
        assert!(resolve_implicit_dir_fixups(&paths(&[&a])).is_err());
    }

    // Hidden or shadowed paths are skipped rather than failing or followed.
    #[test]
    fn apply_dir_fixups_skips_shadowed_paths() {
        let rootfs = tempfile::tempdir().unwrap();
        fs::write(rootfs.path().join("afile"), "").unwrap();
        let before = fs::metadata(rootfs.path().join("afile"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let fixups = [
            DirFixup {
                path: "missing".into(),
                mode: 0o700,
                owner: None,
            },
            DirFixup {
                path: "afile".into(),
                mode: 0o700,
                owner: None,
            },
        ];
        apply_dir_fixups(rootfs.path(), &fixups).unwrap();
        let after = fs::metadata(rootfs.path().join("afile"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(after, before, "non-directory was chmodded");
    }
}
