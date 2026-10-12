//! Choosing the storage backend from the environment.

use std::sync::Arc;

use super::ObjectStorage;
use crate::error::{Error, Result};
use crate::gcs::{GcsClient, GcsOptions};
use crate::s3::S3Client;

/// The variable selecting the backend: `s3`, or anything else for GCS.
pub const STORAGE_BACKEND_ENV: &str = "HK_STORAGE_BACKEND";

/// Whether the environment selects S3. Any other value selects GCS, the
/// default.
pub fn uses_s3() -> bool {
    std::env::var(STORAGE_BACKEND_ENV).as_deref() == Ok("s3")
}

/// The backend [`uses_s3`] selects, with the ambient credentials: the
/// standard AWS variables for S3; workload identity, a key file or the
/// emulator for GCS.
pub fn new_from_env() -> Result<Arc<dyn ObjectStorage>> {
    if uses_s3() {
        return Ok(Arc::new(S3Client::from_env()?));
    }
    let opts = GcsOptions::from_env(crate::gcp::default_credentials)
        .map_err(|e| e.context("creating GCS client"))?;
    let client = GcsClient::new(&opts).map_err(|e: Error| e.context("creating GCS client"))?;
    Ok(Arc::new(client))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::BackendKind;

    #[test]
    fn new_from_env_selects_the_backend() {
        let _env = crate::testing::ENV_LOCK.lock();
        // SAFETY: the lock serializes tests that touch the environment.
        unsafe {
            std::env::set_var("STORAGE_EMULATOR_HOST", "127.0.0.1:1");
            std::env::set_var("AWS_REGION", "us-east-1");
            std::env::set_var("AWS_ACCESS_KEY_ID", "ak");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "sk");
        }
        for (name, backend, path_style, want_s3, want_path_style) in [
            ("unset selects GCS", "", "", false, false),
            ("unknown value selects GCS", "gcs", "", false, false),
            ("s3", "s3", "", true, false),
            ("s3 with path style", "s3", "true", true, true),
            ("s3 path style needs exactly true", "s3", "1", true, false),
        ] {
            // SAFETY: as above.
            unsafe {
                std::env::set_var(STORAGE_BACKEND_ENV, backend);
                std::env::set_var("AWS_S3_USE_PATH_STYLE", path_style);
            }
            let got = new_from_env().unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(got.backend() == BackendKind::S3, want_s3, "{name}");
            if want_s3 {
                let s3 = S3Client::from_env().unwrap();
                assert_eq!(s3.path_style(), want_path_style, "{name}");
            }
        }
        // SAFETY: as above.
        unsafe {
            for k in [
                "STORAGE_EMULATOR_HOST",
                "AWS_REGION",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                STORAGE_BACKEND_ENV,
                "AWS_S3_USE_PATH_STYLE",
            ] {
                std::env::remove_var(k);
            }
        }
    }
}
