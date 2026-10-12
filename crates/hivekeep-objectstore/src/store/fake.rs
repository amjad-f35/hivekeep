//! In-memory [`Store`]s for tests: [`FakeStore`], which observes which
//! external snapshots a flow created and released, and the [`Store`] side
//! of [`MemObjects`].

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::snapshot::{SnapshotUri, StoragePrefix};
use parking_lot::Mutex;

use super::{bucket_prefix, Store};
use crate::error::{Error, Result};
use crate::testing::MemObjects;

#[async_trait]
impl Store for MemObjects {
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let lead = format!("{bucket}/");
        Ok(self
            .keys()
            .into_iter()
            .filter_map(|k| k.strip_prefix(&lead).map(str::to_string))
            .filter(|name| name.starts_with(prefix))
            .collect())
    }

    async fn delete(&self, bucket: &str, object: &str) -> Result<()> {
        self.remove(bucket, object);
        Ok(())
    }

    async fn copy(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
    ) -> Result<()> {
        let data = self.get(src_bucket, src_object).unwrap_or_default();
        self.insert(dst_bucket, dst_object, data);
        Ok(())
    }
}

/// A hook run before a [`FakeStore`] operation; an error fails it.
pub type Hook = Arc<dyn Fn(&str, &str) -> Option<Error> + Send + Sync>;

#[derive(Default)]
struct Hooks {
    on_list: Option<Hook>,
    on_delete: Option<Hook>,
    on_copy: Option<Hook>,
}

/// An in-memory [`Store`] for tests that observe which external snapshots a
/// flow created and released. Objects hold their own name as content, so a
/// copy can be traced to its source.
#[derive(Default)]
pub struct FakeStore {
    objects: Mutex<BTreeMap<String, String>>,
    hooks: Mutex<Hooks>,
}

impl FakeStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Runs `hook(bucket, prefix)` before every list.
    pub fn set_on_list(&self, hook: Hook) {
        self.hooks.lock().on_list = Some(hook);
    }

    /// Runs `hook(bucket, object)` before every delete.
    pub fn set_on_delete(&self, hook: Hook) {
        self.hooks.lock().on_delete = Some(hook);
    }

    /// Runs `hook(src_object, dst_object)` before every copy.
    pub fn set_on_copy(&self, hook: Hook) {
        self.hooks.lock().on_copy = Some(hook);
    }

    /// Removes every hook.
    pub fn clear_hooks(&self) {
        *self.hooks.lock() = Hooks::default();
    }

    /// Writes one object.
    pub fn put(&self, bucket: &str, object: &str, content: &str) {
        self.objects
            .lock()
            .insert(format!("{bucket}/{object}"), content.to_string());
    }

    /// Writes named objects into the external snapshot at `uri`.
    pub fn put_snapshot<S: AsRef<str>>(&self, uri: &SnapshotUri, names: &[S]) {
        self.write_snapshot(uri.as_str(), names)
            .expect("snapshot URI is valid");
    }

    /// [`put_snapshot`](Self::put_snapshot) from an unparsed URI.
    pub fn write_snapshot<S: AsRef<str>>(&self, snapshot_uri: &str, names: &[S]) -> Result<()> {
        let uri = SnapshotUri::parse(snapshot_uri).map_err(|e| {
            Error::other(format!(
                "while parsing the snapshot URI {snapshot_uri:?}: {e}"
            ))
        })?;
        let (bucket, prefix) = bucket_prefix(&uri.prefix())?;
        for name in names {
            let name = name.as_ref();
            self.put(&bucket, &format!("{prefix}{name}"), name);
        }
        Ok(())
    }

    /// The names, relative to `uri`, of the snapshot's objects.
    pub fn snapshot(&self, uri: &SnapshotUri) -> Vec<String> {
        self.prefix(&uri.prefix())
    }

    /// The names, relative to `prefix`, of every object below it.
    pub fn prefix(&self, storage_prefix: &StoragePrefix) -> Vec<String> {
        let (bucket, prefix) = bucket_prefix(storage_prefix).expect("valid prefix");
        self.list_sync(&bucket, &prefix)
            .into_iter()
            .map(|o| o.strip_prefix(&prefix).unwrap_or(&o).to_string())
            .collect()
    }

    /// Every object as `bucket/object`, sorted.
    pub fn objects(&self) -> Vec<String> {
        self.objects.lock().keys().cloned().collect()
    }

    fn list_sync(&self, bucket: &str, prefix: &str) -> Vec<String> {
        let lead = format!("{bucket}/");
        self.objects
            .lock()
            .keys()
            .filter_map(|k| k.strip_prefix(&lead))
            .filter(|o| o.starts_with(prefix))
            .map(str::to_string)
            .collect()
    }
}

#[async_trait]
impl Store for FakeStore {
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let hook = self.hooks.lock().on_list.clone();
        if let Some(e) = hook.and_then(|h| h(bucket, prefix)) {
            return Err(e);
        }
        Ok(self.list_sync(bucket, prefix))
    }

    async fn delete(&self, bucket: &str, object: &str) -> Result<()> {
        let hook = self.hooks.lock().on_delete.clone();
        if let Some(e) = hook.and_then(|h| h(bucket, object)) {
            return Err(e);
        }
        self.objects.lock().remove(&format!("{bucket}/{object}"));
        Ok(())
    }

    async fn copy(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
    ) -> Result<()> {
        let hook = self.hooks.lock().on_copy.clone();
        if let Some(e) = hook.and_then(|h| h(src_object, dst_object)) {
            return Err(e);
        }
        let mut objs = self.objects.lock();
        let content = objs
            .get(&format!("{src_bucket}/{src_object}"))
            .cloned()
            .ok_or_else(|| Error::other(format!("no such object: {src_bucket}/{src_object}")))?;
        objs.insert(format!("{dst_bucket}/{dst_object}"), content);
        Ok(())
    }
}
