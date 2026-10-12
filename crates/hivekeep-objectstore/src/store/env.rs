//! Choosing the control plane's [`Store`] from the environment.

use std::sync::Arc;

use super::gcs::GcsStore;
use super::s3::S3Store;
use super::Store;
use crate::error::Result;
use crate::gcs::{GcsConn, GcsOptions};
use crate::s3::S3Client;
use crate::storage::env::uses_s3;

/// The store [`uses_s3`] selects, with the ambient credentials. GCS gets
/// its own connection, since the transfer client's retry policy is scoped
/// to transfers.
pub fn new_store_from_env() -> Result<Arc<dyn Store>> {
    if uses_s3() {
        return Ok(Arc::new(S3Store::new(S3Client::from_env()?)));
    }
    let opts = GcsOptions::from_env(crate::gcp::default_credentials)
        .map_err(|e| e.context("creating GCS client"))?;
    let conn = GcsConn::new(
        (opts.transport)()?,
        opts.auth.clone(),
        opts.endpoint.clone(),
    );
    Ok(Arc::new(GcsStore::new(conn)))
}
