//! Plugging the store into the keeper: its [`ImageCache`] (pulls for
//! bundle preparation) and [`ImageGcStore`] (the watermark eviction loop).

use async_trait::async_trait;
use hivekeep_node::context::Ctx;
use hivekeep_node::image::{Image as KeeperImage, ImageCache};
use hivekeep_node::image_gc::{ImageEvictError, ImageEvictStats, ImageGcStore};
use tonic::Status;

use crate::error::{Error, ErrorKind};
use crate::gc::EvictStats;
use crate::store::{Image, Store};

/// The gRPC status for a pull failure. Registry throttling and outages are
/// Unavailable (retried by the control plane); bad references and lying
/// images are not retryable.
pub fn status_of(err: &Error) -> Status {
    let msg = err.to_string();
    if err.is(ErrorKind::Canceled) {
        return Status::cancelled(msg);
    }
    if err.is(ErrorKind::DeadlineExceeded) {
        return Status::deadline_exceeded(msg);
    }
    if err.is(ErrorKind::InvalidReference) {
        return Status::invalid_argument(msg);
    }
    if err.is(ErrorKind::DiffIdMismatch) {
        return Status::failed_precondition(msg);
    }
    match err.registry_status() {
        Some(401 | 403) => Status::permission_denied(msg),
        Some(404) => Status::not_found(msg),
        _ => Status::unavailable(msg),
    }
}

impl From<Image> for KeeperImage {
    fn from(i: Image) -> Self {
        KeeperImage {
            digest: i.digest.to_string(),
            config: (&i.config).into(),
            layer_dirs: i
                .layer_dirs
                .iter()
                .map(|d| d.to_string_lossy().into_owned())
                .collect(),
        }
    }
}

impl From<EvictStats> for ImageEvictStats {
    fn from(s: EvictStats) -> Self {
        ImageEvictStats {
            freed_bytes: s.freed_bytes,
            evicted_images: s.evicted_images,
            evicted_layers: s.evicted_layers,
            candidates: s.candidates,
            rooted_images: s.rooted_images,
            orphan_layers: s.orphan_layers,
            skipped_rooted: s.skipped_rooted,
            skipped_fresh: s.skipped_fresh,
        }
    }
}

#[async_trait]
impl ImageCache for Store {
    async fn ensure_image(&self, ctx: &Ctx, reference: &str) -> Result<KeeperImage, Status> {
        Store::ensure_image(self, ctx, reference)
            .await
            .map(KeeperImage::from)
            .map_err(|e| status_of(&e))
    }
}

#[async_trait]
impl ImageGcStore for Store {
    async fn cache_size(&self) -> Result<i64, String> {
        let s = self.clone();
        crate::spawn::blocking(move || s.cache_size())
            .await
            .map_err(|e| e.to_string())
    }

    async fn evict_unused(
        &self,
        target: i64,
        dry_run: bool,
    ) -> (ImageEvictStats, Option<ImageEvictError>) {
        let s = self.clone();
        let res =
            crate::spawn::blocking(move || Ok(s.evict_unused(&Ctx::background(), target, dry_run)))
                .await;
        match res {
            Ok((stats, None)) => (stats.into(), None),
            Ok((stats, Some(e))) if e.is(ErrorKind::IncompleteEnumeration) => (
                stats.into(),
                Some(ImageEvictError::IncompleteEnumeration(e.to_string())),
            ),
            Ok((stats, Some(e))) => (stats.into(), Some(ImageEvictError::Partial(e.to_string()))),
            Err(e) => (
                ImageEvictStats::default(),
                Some(ImageEvictError::Partial(e.to_string())),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::ImageConfig;
    use crate::store::tests::{backdate_store, fixture_store, hours, one_file_image};
    use crate::store::StoreOptions;
    use crate::testutil::registry::TestRegistry;
    use std::sync::Arc;

    #[tokio::test]
    async fn store_serves_the_keeper_traits() {
        let reg = TestRegistry::start().await;
        let r = one_file_image(&reg, "test/keeper:latest", "f", "x");
        let cfg = ImageConfig {
            env: vec!["A=b".into()],
            cmd: vec!["/app".into()],
            ..Default::default()
        };
        reg.push(
            "test/cfg:latest",
            cfg,
            &[crate::testutil::image::mem_layer(&[])],
        );
        let (_t, store) = fixture_store(StoreOptions::default());
        let cache: Arc<dyn ImageCache> = Arc::new(store.clone());
        let img = cache.ensure_image(&Ctx::background(), &r).await.unwrap();
        assert!(img.digest.starts_with("sha256:"));
        assert_eq!(img.layer_dirs.len(), 1);
        let img = cache
            .ensure_image(&Ctx::background(), &reg.reference("test/cfg:latest"))
            .await
            .unwrap();
        assert_eq!(img.config.env, vec!["A=b"]);
        assert_eq!(img.config.cmd, vec!["/app"]);
        let missing = cache
            .ensure_image(&Ctx::background(), &reg.reference("test/none:latest"))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);
        let bad = cache
            .ensure_image(&Ctx::background(), "NOT A REF")
            .await
            .unwrap_err();
        assert_eq!(bad.code(), tonic::Code::InvalidArgument);

        let gc: Arc<dyn ImageGcStore> = Arc::new(store.clone());
        assert!(gc.cache_size().await.unwrap() > 0);
        backdate_store(&store, hours(3));
        let (stats, err) = gc.evict_unused(i64::MAX, false).await;
        assert!(err.is_none(), "{err:?}");
        assert_eq!(stats.evicted_images, 2);
        assert_eq!(gc.cache_size().await.unwrap(), 0);
    }

    #[test]
    fn status_mapping() {
        let reg = |s| Error::new(ErrorKind::Registry(s), "x");
        assert_eq!(status_of(&reg(429)).code(), tonic::Code::Unavailable);
        assert_eq!(status_of(&reg(503)).code(), tonic::Code::Unavailable);
        assert_eq!(status_of(&reg(403)).code(), tonic::Code::PermissionDenied);
        assert_eq!(
            status_of(&Error::new(ErrorKind::DiffIdMismatch, "x")).code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            status_of(&Error::new(ErrorKind::Canceled, "x")).code(),
            tonic::Code::Cancelled
        );
        assert_eq!(
            status_of(&Error::msg("io")).code(),
            tonic::Code::Unavailable
        );
    }
}
