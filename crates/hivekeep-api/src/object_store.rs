//! Deleting and copying external snapshots in object storage.
//!
//! The control plane never reads snapshot contents; it only collects a
//! prefix when its owner no longer needs it, and copies an Actor's snapshot
//! to a Tag's own prefix.
//!
//! - [`ControlPluginStore`]: the object-store control plugin over its Unix
//!   socket, the deployed shape.
//! - [`CloudObjectStore`]: the same cleanup and copy against GCS or S3 in
//!   process, on the backend the environment selects.
//! - [`LocalObjectStore`]: `<scheme>://<bucket>/<path>` on a local
//!   directory, for single-machine development.
//! - [`MemoryObjectStore`], for tests.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use hivekeep_objectstore::control_client::{DirectControlStore, PluginControlStore};
use parking_lot::Mutex;

/// Why an object-store call failed.
#[derive(Debug, thiserror::Error)]
#[error("object store: {0}")]
pub struct ObjectStoreError(pub String);

/// Snapshot operations the control plane needs.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    /// Deletes every object under `prefix`. A missing prefix is success.
    async fn cleanup_snapshot(&self, prefix: &str) -> Result<(), ObjectStoreError>;
    /// Copies every object under `src` to the same relative name under `dst`.
    async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), ObjectStoreError>;
}

fn with_slash(p: &str) -> String {
    if p.ends_with('/') {
        p.to_string()
    } else {
        format!("{p}/")
    }
}

/// An in-memory object store, keyed by full object URI.
#[derive(Debug, Default)]
pub struct MemoryObjectStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl MemoryObjectStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Writes one object.
    pub fn put(&self, uri: impl Into<String>, data: impl Into<Vec<u8>>) {
        self.objects.lock().insert(uri.into(), data.into());
    }

    /// Writes `names` under the snapshot prefix `snapshot_uri`.
    pub fn write_snapshot(&self, snapshot_uri: &str, names: &[&str]) {
        let p = with_slash(snapshot_uri);
        for n in names {
            self.put(format!("{p}{n}"), n.as_bytes().to_vec());
        }
    }

    /// Names of the objects under `prefix`, relative to it.
    pub fn list(&self, prefix: &str) -> Vec<String> {
        let p = with_slash(prefix);
        self.objects
            .lock()
            .keys()
            .filter_map(|k| k.strip_prefix(&p).map(str::to_string))
            .collect()
    }
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn cleanup_snapshot(&self, prefix: &str) -> Result<(), ObjectStoreError> {
        let p = with_slash(prefix);
        self.objects.lock().retain(|k, _| !k.starts_with(&p));
        Ok(())
    }

    async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), ObjectStoreError> {
        let (s, d) = (with_slash(src), with_slash(dst));
        let mut objs = self.objects.lock();
        let copies: Vec<(String, Vec<u8>)> = objs
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(&s)
                    .map(|rel| (format!("{d}{rel}"), v.clone()))
            })
            .collect();
        if copies.is_empty() {
            return Err(ObjectStoreError(format!("no objects under {src}")));
        }
        objs.extend(copies);
        Ok(())
    }
}

/// Stores `<scheme>://<bucket>/<path>` at `<root>/<bucket>/<path>`.
#[derive(Debug, Clone)]
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    /// A store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LocalObjectStore { root: root.into() }
    }

    fn path(&self, uri: &str) -> Result<PathBuf, ObjectStoreError> {
        let rest = uri
            .split_once("://")
            .map(|(_, r)| r)
            .ok_or_else(|| ObjectStoreError(format!("{uri:?} is not a URI")))?;
        let mut out = self.root.clone();
        for seg in rest.split('/').filter(|s| !s.is_empty()) {
            if seg == "." || seg == ".." {
                return Err(ObjectStoreError(format!("{uri:?} escapes the store")));
            }
            out.push(seg);
        }
        Ok(out)
    }
}

fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let to = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &to)?;
        } else {
            std::fs::copy(entry.path(), to)?;
        }
    }
    Ok(())
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
    async fn cleanup_snapshot(&self, prefix: &str) -> Result<(), ObjectStoreError> {
        let p = self.path(prefix)?;
        tokio::task::spawn_blocking(move || match std::fs::remove_dir_all(&p) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ObjectStoreError(format!("removing {}: {e}", p.display()))),
        })
        .await
        .map_err(|e| ObjectStoreError(e.to_string()))?
    }

    async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), ObjectStoreError> {
        let (s, d) = (self.path(src)?, self.path(dst)?);
        tokio::task::spawn_blocking(move || {
            copy_dir(&s, &d).map_err(|e| {
                ObjectStoreError(format!("copying {} to {}: {e}", s.display(), d.display()))
            })
        })
        .await
        .map_err(|e| ObjectStoreError(e.to_string()))?
    }
}

fn status_error(op: &str, uri: &str, s: tonic::Status) -> ObjectStoreError {
    ObjectStoreError(format!("{op} {uri}: {:?}: {}", s.code(), s.message()))
}

/// The object-store control plugin over its socket.
#[derive(Clone)]
pub struct ControlPluginStore(PluginControlStore);

impl ControlPluginStore {
    /// A client of the plugin at `socket`, connecting on first use.
    pub fn connect(socket: &Path) -> Result<Self, ObjectStoreError> {
        PluginControlStore::connect(socket)
            .map(ControlPluginStore)
            .map_err(ObjectStoreError)
    }

    /// Waits until the plugin reports it is serving.
    pub async fn wait_ready(&self) -> Result<(), ObjectStoreError> {
        self.0.wait_ready().await.map_err(|s| {
            ObjectStoreError(format!("control plugin is not serving: {}", s.message()))
        })
    }
}

#[async_trait]
impl ObjectStore for ControlPluginStore {
    async fn cleanup_snapshot(&self, prefix: &str) -> Result<(), ObjectStoreError> {
        self.0
            .cleanup_snapshot(prefix)
            .await
            .map_err(|s| status_error("cleaning up", prefix, s))
    }

    async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), ObjectStoreError> {
        self.0
            .copy_snapshot(src, dst)
            .await
            .map_err(|s| status_error("copying", src, s))
    }
}

/// Cleanup and copy against the cloud backend in process.
#[derive(Clone)]
pub struct CloudObjectStore(DirectControlStore);

impl CloudObjectStore {
    /// The backend `HK_STORAGE_BACKEND` selects (`s3`, else GCS), with the
    /// ambient credentials.
    pub fn from_env() -> Result<Self, ObjectStoreError> {
        DirectControlStore::from_env()
            .map(CloudObjectStore)
            .map_err(|e| ObjectStoreError(e.to_string()))
    }

    /// Cleanup and copy on `store`.
    pub fn new(store: std::sync::Arc<dyn hivekeep_objectstore::store::Store>) -> Self {
        CloudObjectStore(DirectControlStore::new(store))
    }
}

#[async_trait]
impl ObjectStore for CloudObjectStore {
    async fn cleanup_snapshot(&self, prefix: &str) -> Result<(), ObjectStoreError> {
        self.0
            .cleanup_snapshot(prefix)
            .await
            .map_err(|s| status_error("cleaning up", prefix, s))
    }

    async fn copy_snapshot(&self, src: &str, dst: &str) -> Result<(), ObjectStoreError> {
        self.0
            .copy_snapshot(src, dst)
            .await
            .map_err(|s| status_error("copying", src, s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_copy_and_cleanup() {
        let s = MemoryObjectStore::new();
        s.write_snapshot("gs://b/a/snap", &["manifest.json", "memory.zst"]);
        s.copy_snapshot("gs://b/a/snap", "gs://b/t/").await.unwrap();
        assert_eq!(s.list("gs://b/t"), vec!["manifest.json", "memory.zst"]);
        s.cleanup_snapshot("gs://b/a").await.unwrap();
        assert!(s.list("gs://b/a/snap").is_empty());
        assert!(s.copy_snapshot("gs://b/none", "gs://b/x").await.is_err());
    }

    #[tokio::test]
    async fn local_copy_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let s = LocalObjectStore::new(dir.path());
        let src = dir.path().join("b/a/snap");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(src.join("m"), "x").unwrap();
        s.copy_snapshot("gs://b/a/snap", "gs://b/t").await.unwrap();
        assert!(dir.path().join("b/t/m").exists());
        s.cleanup_snapshot("gs://b/a").await.unwrap();
        assert!(!src.exists());
        s.cleanup_snapshot("gs://b/missing").await.unwrap();
        assert!(s.cleanup_snapshot("gs://b/../x").await.is_err());
    }

    #[tokio::test]
    async fn cloud_store_copies_and_cleans_up() {
        let fake = std::sync::Arc::new(hivekeep_objectstore::store::fake::FakeStore::new());
        fake.put("b", "a/snap/manifest.json", "m");
        let s = CloudObjectStore::new(fake.clone());
        s.copy_snapshot("gs://b/a/snap", "gs://b/t/snap")
            .await
            .unwrap();
        assert_eq!(
            fake.objects(),
            vec!["b/a/snap/manifest.json", "b/t/snap/manifest.json"]
        );
        assert!(s.copy_snapshot("gs://b/none", "gs://b/x").await.is_err());
        s.cleanup_snapshot("gs://b/a/snap").await.unwrap();
        assert_eq!(fake.objects(), vec!["b/t/snap/manifest.json"]);
    }
}
