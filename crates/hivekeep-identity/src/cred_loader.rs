//! Credential and trust-bundle files cached against their file status, for
//! TLS configurations that look them up on every handshake.
//!
//! Each lookup stats the file and re-parses it only when it changed: a
//! different file identity (device and inode), modification time or size.
//! The kubelet rotates projected volumes by swapping a symlink to a fresh
//! directory, which always changes the identity; mtime and size also catch
//! in-place rewrites.
//!
//! The file can change between the stat and the read. The newer content is
//! then cached against the older status, so the next lookup sees a mismatch
//! and reads again: the cache lags a rotation by at most one lookup. Errors
//! keep the previous entry but are returned, so every lookup retries until a
//! parse succeeds.

use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;

use crate::cred_bundle::{load_trust_anchors, CredentialBundle};
use crate::error::{IdentityError, Result};

/// What a cached parse is keyed on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    identity: Option<(u64, u64)>,
}

impl FileStamp {
    fn of(meta: &Metadata) -> Self {
        FileStamp {
            len: meta.len(),
            modified: meta.modified().ok(),
            identity: identity(meta),
        }
    }
}

#[cfg(unix)]
fn identity(meta: &Metadata) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    Some((meta.dev(), meta.ino()))
}

#[cfg(not(unix))]
fn identity(_meta: &Metadata) -> Option<(u64, u64)> {
    None
}

/// A file parsed on first use and again whenever its status changes.
pub struct CachedFile<T> {
    path: PathBuf,
    what: &'static str,
    parse: fn(&Path) -> Result<T>,
    entry: Mutex<Option<(FileStamp, Arc<T>)>>,
}

impl<T> std::fmt::Debug for CachedFile<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFile")
            .field("path", &self.path)
            .field("what", &self.what)
            .finish_non_exhaustive()
    }
}

impl<T> CachedFile<T> {
    fn new(path: impl Into<PathBuf>, what: &'static str, parse: fn(&Path) -> Result<T>) -> Self {
        CachedFile {
            path: path.into(),
            what,
            parse,
            entry: Mutex::new(None),
        }
    }

    /// The file this reads.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The parsed file, re-read only if it changed since the last
    /// successful parse.
    pub fn get(&self) -> Result<Arc<T>> {
        let mut entry = self.entry.lock();
        // Follows symlinks, so a swapped projected-volume target shows up as
        // a new identity.
        let meta = std::fs::metadata(&self.path).map_err(|source| IdentityError::Io {
            path: format!("{} {}", self.what, self.path.display()),
            source,
        })?;
        let stamp = FileStamp::of(&meta);
        if let Some((cached, value)) = entry.as_ref() {
            if *cached == stamp {
                return Ok(value.clone());
            }
        }
        let value = Arc::new((self.parse)(&self.path)?);
        *entry = Some((stamp, value.clone()));
        Ok(value)
    }
}

/// A credential bundle (key and chain) cached against its file.
pub type CredentialBundleLoader = CachedFile<CredentialBundle>;

impl CachedFile<CredentialBundle> {
    /// A loader for the credential bundle at `path`. Nothing is read until
    /// the first [`get`](Self::get).
    pub fn credential_bundle(path: impl Into<PathBuf>) -> Self {
        Self::new(path, "credential bundle", |p| CredentialBundle::load(p))
    }
}

/// Trust anchors (DER certificates) cached against their bundle file.
pub type TrustAnchorLoader = CachedFile<Vec<Vec<u8>>>;

impl CachedFile<Vec<Vec<u8>>> {
    /// A loader for the trust bundle at `path`. Nothing is read until the
    /// first [`get`](Self::get).
    pub fn trust_anchors(path: impl Into<PathBuf>) -> Self {
        Self::new(path, "trust bundle", |p| load_trust_anchors(p))
    }
}
