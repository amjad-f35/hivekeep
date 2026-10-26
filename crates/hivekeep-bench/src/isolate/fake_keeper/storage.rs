//! Where the herder writes snapshot placeholders.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::snapshot::StoragePrefix;
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use parking_lot::Mutex;

/// Environment variable choosing the backend, as the keeper reads it.
pub const STORAGE_BACKEND_ENV: &str = "HK_STORAGE_BACKEND";

/// The part of object storage the herder uses.
#[async_trait]
pub trait ObjectWriter: Send + Sync + 'static {
    /// Writes `data` as `object` in `bucket`.
    async fn put_object(&self, bucket: &str, object: &str, data: Vec<u8>) -> Result<(), String>;
}

/// Splits a storage prefix into its bucket and the object-name prefix its
/// objects share. The prefix keeps its trailing separator so it cannot
/// match a sibling whose name it happens to prefix.
pub fn bucket_prefix(prefix: &StoragePrefix) -> Result<(String, String), String> {
    if prefix.is_zero() {
        return Err("empty storage prefix".into());
    }
    let url = url::Url::parse(prefix.as_str())
        .map_err(|e| format!("while parsing storage prefix {prefix}: {e}"))?;
    let bucket = url.host_str().unwrap_or_default();
    if bucket.is_empty() {
        return Err(format!("storage prefix {prefix} has no bucket"));
    }
    Ok((
        bucket.to_string(),
        format!("{}/", url.path().trim_start_matches('/')),
    ))
}

/// Which cloud an [`ObjectStoreWriter`] talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// S3 or an S3-compatible store, configured from `AWS_*` variables.
    /// `AWS_S3_USE_PATH_STYLE=true` selects path-style addressing.
    S3,
    /// Google Cloud Storage, configured from `GOOGLE_*` variables or the
    /// ambient credentials.
    Gcs,
}

impl Backend {
    /// The backend `HK_STORAGE_BACKEND` names: `s3`, or GCS otherwise.
    pub fn from_env() -> Backend {
        match std::env::var(STORAGE_BACKEND_ENV).as_deref() {
            Ok("s3") => Backend::S3,
            _ => Backend::Gcs,
        }
    }
}

/// Writes through the `object_store` clients, one per bucket.
pub struct ObjectStoreWriter {
    backend: Backend,
    clients: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ObjectStoreWriter {
    /// A writer for `backend`; clients are built per bucket on first use.
    pub fn new(backend: Backend) -> ObjectStoreWriter {
        ObjectStoreWriter {
            backend,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn client(&self, bucket: &str) -> Result<Arc<dyn ObjectStore>, String> {
        let mut clients = self.clients.lock();
        if let Some(c) = clients.get(bucket) {
            return Ok(c.clone());
        }
        let client: Arc<dyn ObjectStore> = match self.backend {
            Backend::S3 => {
                let path_style = std::env::var("AWS_S3_USE_PATH_STYLE").as_deref() == Ok("true");
                Arc::new(
                    AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .with_virtual_hosted_style_request(!path_style)
                        .build()
                        .map_err(|e| format!("S3 client for {bucket}: {e}"))?,
                )
            }
            Backend::Gcs => Arc::new(
                GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| format!("GCS client for {bucket}: {e}"))?,
            ),
        };
        clients.insert(bucket.to_string(), client.clone());
        Ok(client)
    }
}

#[async_trait]
impl ObjectWriter for ObjectStoreWriter {
    async fn put_object(&self, bucket: &str, object: &str, data: Vec<u8>) -> Result<(), String> {
        let client = self.client(bucket)?;
        client
            .put(&ObjectPath::from(object), PutPayload::from(data))
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_prefix_keeps_the_separator() {
        let p = StoragePrefix::parse("gs://bench-bucket/root/hives/team-a").unwrap();
        assert_eq!(
            bucket_prefix(&p).unwrap(),
            ("bench-bucket".to_string(), "root/hives/team-a/".to_string())
        );
        assert!(bucket_prefix(&StoragePrefix::default()).is_err());
    }
}
