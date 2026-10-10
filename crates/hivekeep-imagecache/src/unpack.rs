//! Unpacking one uncompressed OCI layer tar into its pool directory.
//!
//! The unpacker runs as plain root with every capability dropped, so it
//! cannot create device nodes or set `trusted.*` xattrs. Whiteouts (`.wh.*`
//! entries) are therefore not written into the tree: they are recorded in
//! the layer's metadata for the privileged worker to materialize at compose
//! time. Every write goes through a confined directory handle, so path
//! traversal and symlink or hardlink escapes are refused.
//!
//! Cross-layer "later entry wins" is overlayfs's job; within one layer a
//! later entry replaces an earlier one at the same path.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use cap_std::fs::Dir;
use serde::{Deserialize, Serialize};
use tar::EntryType;

use crate::confined;
use crate::error::{Error, Result};

/// Prefix of an entry that deletes the same-named path from lower layers.
pub const WHITEOUT_PREFIX: &str = ".wh.";
/// Marks its directory opaque: lower-layer contents are hidden entirely.
pub const OPAQUE_MARKER: &str = ".wh..wh..opq";

/// The layer metadata file name, beside `fs/`.
pub const LAYER_WHITEOUTS_FILE_NAME: &str = "whiteouts.json";

/// Per-layer metadata captured at unpack time for the privileged consumer.
/// Paths are clean and relative to the layer's `fs/` root.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WhiteoutSet {
    /// The format version, 1.
    pub version: u32,
    /// Paths that must become 0:0 character devices in the lowerdir.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub whiteouts: Vec<String>,
    /// Directories that must carry `trusted.overlay.opaque=y`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub opaques: Vec<String>,
    /// Directories the layer never declared but that exist because a child
    /// (or a whiteout materialized later) needed a parent. Their root:root
    /// 0755 attributes are fabricated and, being in a higher layer, would
    /// shadow the real attributes a lower layer declared; the consumer
    /// repairs the merged view from this list.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub implicit_dirs: Vec<String>,
}

impl WhiteoutSet {
    fn empty() -> Self {
        WhiteoutSet {
            version: 1,
            ..Default::default()
        }
    }
}

/// Reads a layer's metadata; an absent file is an empty set.
pub fn read_whiteouts(layer_dir: &Path) -> Result<WhiteoutSet> {
    match fs::read(layer_dir.join(LAYER_WHITEOUTS_FILE_NAME)) {
        Ok(b) => serde_json::from_slice(&b)
            .map_err(|e| Error::msg(format!("while decoding layer whiteouts: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(WhiteoutSet::empty()),
        Err(e) => Err(Error::io("while reading layer whiteouts", e)),
    }
}

/// Validates a tar entry name: `Ok(None)` to skip entries naming the root,
/// the cleaned relative path otherwise.
pub fn validate_tar_name(name: &str) -> std::result::Result<Option<String>, String> {
    Ok(confined::validate_name(name.as_bytes())?.map(|p| p.to_string_lossy().into_owned()))
}

fn rel_str(p: &Path) -> String {
    p.to_string_lossy().into_owned()
}

fn mtime_of(header: &tar::Header) -> Option<SystemTime> {
    match header.mtime() {
        Ok(0) | Err(_) => None,
        Ok(secs) => Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs)),
    }
}

struct Unpacker<'a> {
    root: &'a Dir,
    wh: WhiteoutSet,
    dir_modes: BTreeMap<PathBuf, u32>,
    dir_mtimes: BTreeMap<PathBuf, Option<SystemTime>>,
    declared: BTreeSet<PathBuf>,
    implicit: BTreeSet<PathBuf>,
}

impl Unpacker<'_> {
    fn mark_ancestors(&mut self, rel: &Path) {
        for p in confined::ancestors(rel) {
            if !self.declared.contains(p) {
                self.implicit.insert(p.to_path_buf());
            }
        }
    }

    /// Removes whatever is at `rel` and forgets deferred restores for it
    /// and its subtree.
    fn replace_existing(&mut self, rel: &Path, before: &str) -> Result<()> {
        match confined::lexists(self.root, rel) {
            Ok(true) => {
                confined::remove_all(self.root, rel).map_err(|e| {
                    Error::msg(format!(
                        "while replacing existing path at {rel:?} before {before}: {e}"
                    ))
                })?;
                self.dir_modes.retain(|d, _| !d.starts_with(rel));
                self.dir_mtimes.retain(|d, _| !d.starts_with(rel));
                Ok(())
            }
            Ok(false) => Ok(()),
            Err(e) => Err(Error::msg(format!(
                "while checking existing path at {rel:?} before {before}: {e}"
            ))),
        }
    }

    fn entry<R: Read>(&mut self, entry: &mut tar::Entry<'_, R>) -> Result<()> {
        let raw_name = entry.path_bytes().into_owned();
        let rel = match confined::validate_name(&raw_name) {
            Ok(Some(rel)) => rel,
            Ok(None) => return Ok(()),
            Err(e) => return Err(Error::msg(format!("invalid tar entry: {e}"))),
        };
        let base = rel
            .file_name()
            .map(|b| b.as_bytes().to_vec())
            .unwrap_or_default();
        if base == OPAQUE_MARKER.as_bytes() {
            if let Some(dir) = confined::parent_of(&rel) {
                self.wh.opaques.push(rel_str(dir));
            }
            self.mark_ancestors(&rel);
            return Ok(());
        } else if base.starts_with(b".wh..wh.") {
            // AUFS bookkeeping (.wh..wh.plnk, ...): nothing for overlayfs.
            return Ok(());
        } else if let Some(deleted) = base.strip_prefix(WHITEOUT_PREFIX.as_bytes()) {
            let target = match confined::parent_of(&rel) {
                Some(p) => p.join(std::ffi::OsStr::from_bytes(deleted)),
                None => PathBuf::from(std::ffi::OsStr::from_bytes(deleted)),
            };
            self.wh.whiteouts.push(rel_str(&target));
            self.mark_ancestors(&rel);
            return Ok(());
        }

        let header = entry.header().clone();
        let mut kind = header.entry_type();
        // Old-style regular entries ending in a slash are directories.
        if header.as_old().linkflag[0] == 0 && raw_name.ends_with(b"/") {
            kind = EntryType::Directory;
        }
        // Device nodes are skipped: an image must never grant device access,
        // and the runtime provides /dev.
        if matches!(kind, EntryType::Char | EntryType::Block) {
            self.replace_existing(&rel, "device node")?;
            tracing::debug!(name = %rel.display(), "Skipping device node in image layer");
            return Ok(());
        }

        let mode = header.mode().unwrap_or(0o644) & 0o777;
        // Layer tars routinely omit parents that exist only in lower layers;
        // overlayfs merges the created ones with the lower directories.
        self.mark_ancestors(&rel);
        if let Some(parent) = confined::parent_of(&rel) {
            confined::mkdir_all(self.root, parent, 0o755).map_err(|e| {
                Error::msg(format!(
                    "while creating parent directories for {rel:?}: {e}"
                ))
            })?;
        }

        match kind {
            EntryType::Regular => {
                // Later entry wins: never write through a symlink, truncate a
                // hardlinked inode, or collide with a directory.
                self.replace_existing(&rel, "regular file")?;
                let mut out = confined::create_file(self.root, &rel, mode)
                    .map_err(|e| Error::msg(format!("while creating file {rel:?}: {e}")))?;
                let want = header.size().unwrap_or(0);
                let n = io::copy(entry, &mut out).map_err(|e| {
                    Error::msg(format!(
                        "while writing contents of {rel:?} from tar stream: {e}"
                    ))
                })?;
                if n != want {
                    return Err(Error::msg(format!(
                        "while writing contents of {rel:?} from tar stream: unexpected EOF"
                    )));
                }
                if let Some(t) = mtime_of(&header) {
                    confined::set_file_times(&out, t).map_err(|e| {
                        Error::msg(format!("while restoring mtime on {rel:?}: {e}"))
                    })?;
                }
            }
            EntryType::Directory => {
                // Owner-writable while extracting, so children can be written
                // even into a directory the image marks read-only; the real
                // mode is restored at the end.
                match confined::mkdir(self.root, &rel, mode | 0o700) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(e) => {
                        return Err(Error::msg(format!(
                            "while creating directory={rel:?}, mode={mode:o}: {e}"
                        )))
                    }
                }
                self.dir_modes.insert(rel.clone(), mode);
                self.dir_mtimes.insert(rel.clone(), mtime_of(&header));
                self.declared.insert(rel.clone());
                self.implicit.remove(&rel);
            }
            EntryType::Symlink => {
                let target = header
                    .link_name_bytes()
                    .map(|b| b.into_owned())
                    .unwrap_or_default();
                let target = PathBuf::from(std::ffi::OsStr::from_bytes(&target));
                if let Ok(md) = self.root.symlink_metadata(&rel) {
                    if md.file_type().is_symlink() {
                        if let Ok(cur) = self.root.read_link_contents(&rel) {
                            if cur == target {
                                return Ok(());
                            }
                        }
                    }
                }
                // Replaces the entry itself, never what a prior symlink
                // pointed at.
                self.replace_existing(&rel, "symlink")?;
                self.root.symlink_contents(&target, &rel).map_err(|e| {
                    Error::msg(format!(
                        "while creating symlink src={rel:?} target={target:?}: {e}"
                    ))
                })?;
                // The symlink's own mtime is not restored.
            }
            EntryType::Link => {
                let raw = header
                    .link_name_bytes()
                    .map(|b| b.into_owned())
                    .unwrap_or_default();
                let target = match confined::validate_name(&raw) {
                    Ok(Some(t)) => t,
                    Ok(None) => {
                        return Err(Error::msg(format!(
                            "invalid hardlink target for {rel:?}: empty"
                        )))
                    }
                    Err(e) => {
                        return Err(Error::msg(format!(
                            "invalid hardlink target for {rel:?}: {e}"
                        )))
                    }
                };
                self.replace_existing(&rel, "hardlink")?;
                self.root.hard_link(&target, self.root, &rel).map_err(|e| {
                    Error::msg(format!(
                        "while creating hardlink src={rel:?} target={target:?}: {e}"
                    ))
                })?;
            }
            other => {
                let flag = (other.as_byte() as char).to_string();
                tracing::error!(typeflag = %flag, name = %rel.display(), "Unhandled tar entry typeflag");
                return Err(Error::msg(format!(
                    "unhandled tar entry typeflag {flag:?} at {:?}",
                    rel_str(&rel)
                )));
            }
        }
        Ok(())
    }

    fn finish(mut self) -> Result<WhiteoutSet> {
        // Deepest first: a child's path is longer than its parent's, so a
        // parent's restrictive mode cannot block restoring its children.
        let mut dirs: Vec<PathBuf> = self.dir_modes.keys().cloned().collect();
        dirs.sort_by_key(|d| std::cmp::Reverse(d.as_os_str().len()));
        for d in dirs {
            // Times before mode: the directory must still be openable.
            if let Some(Some(t)) = self.dir_mtimes.get(&d) {
                confined::set_dir_times(self.root, &d, *t)
                    .map_err(|e| Error::msg(format!("while restoring mtime on {d:?}: {e}")))?;
            }
            let mode = self.dir_modes[&d];
            confined::chmod(self.root, &d, mode).map_err(|e| {
                Error::msg(format!(
                    "while restoring mode {mode:o} on directory {d:?}: {e}"
                ))
            })?;
        }

        // Keep implicit candidates that survived as directories, plus those
        // the finalize step will create for whiteout/opaque materialization.
        let mut finalize_dirs = BTreeSet::new();
        for w in &self.wh.whiteouts {
            for p in confined::ancestors(Path::new(w)) {
                finalize_dirs.insert(p.to_path_buf());
            }
        }
        for o in &self.wh.opaques {
            let o = Path::new(o);
            finalize_dirs.insert(o.to_path_buf());
            for p in confined::ancestors(o) {
                finalize_dirs.insert(p.to_path_buf());
            }
        }
        let mut implicit = Vec::new();
        for p in &self.implicit {
            if self.declared.contains(p) {
                continue;
            }
            let is_dir = self
                .root
                .symlink_metadata(p)
                .map(|m| m.is_dir())
                .unwrap_or(false);
            if is_dir || finalize_dirs.contains(p) {
                implicit.push(p.as_os_str().as_bytes().to_vec());
            }
        }
        implicit.sort();
        self.wh.implicit_dirs = implicit
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect();
        Ok(self.wh)
    }
}

/// Extracts one uncompressed layer tar into `root` and returns its
/// whiteout metadata.
pub fn unpack_layer(tar_data: &mut dyn Read, root: &Dir) -> Result<WhiteoutSet> {
    let mut u = Unpacker {
        root,
        wh: WhiteoutSet::empty(),
        dir_modes: BTreeMap::new(),
        dir_mtimes: BTreeMap::new(),
        declared: BTreeSet::new(),
        implicit: BTreeSet::new(),
    };
    let mut archive = tar::Archive::new(tar_data);
    {
        let entries = archive
            .entries()
            .map_err(|e| Error::msg(format!("while reading tar entry: {e}")))?;
        for entry in entries {
            let mut entry =
                entry.map_err(|e| Error::msg(format!("while reading tar entry: {e}")))?;
            u.entry(&mut entry)?;
        }
    }
    // The archive ends with two zero blocks; the reader stops after the
    // first. A partial second block is a truncated archive.
    let rest = archive.into_inner();
    let mut block = [0u8; 512];
    let mut got = 0;
    while got < block.len() {
        match rest.read(&mut block[got..]) {
            Ok(0) => break,
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::msg(format!("while reading tar entry: {e}"))),
        }
    }
    if got > 0 && got < block.len() {
        return Err(Error::msg("while reading tar entry: unexpected EOF"));
    }
    u.finish()
}

#[cfg(test)]
#[path = "unpack_tests.rs"]
mod tests;
