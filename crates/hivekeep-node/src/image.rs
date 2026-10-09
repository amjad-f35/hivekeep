//! The node's image layer cache as the keeper sees it: what an image
//! resolves to, the cache trait, and the overlay spec a bundle's rootfs is
//! mounted from.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::context::Ctx;

/// The overlay spec's file name in a bundle.
pub const OVERLAY_SPEC_FILE_NAME: &str = "rootfs-overlay.json";

/// The parts of an image config the keeper uses.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageConfig {
    /// `KEY=value` entries.
    pub env: Vec<String>,
    /// The image's entrypoint.
    pub entrypoint: Vec<String>,
    /// The image's command.
    pub cmd: Vec<String>,
}

/// A cached image.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Image {
    /// The manifest digest the reference resolved to.
    pub digest: String,
    /// The image config.
    pub config: ImageConfig,
    /// The cached layer directories, bottom-most first; each holds its tree
    /// under `fs/`.
    pub layer_dirs: Vec<String>,
}

/// The node's image layer cache.
#[async_trait]
pub trait ImageCache: Send + Sync + 'static {
    /// Pulls `reference` unless cached, and returns it.
    async fn ensure_image(&self, ctx: &Ctx, reference: &str) -> Result<Image, Status>;
}

/// An image cache that has none: every pull fails. For nodes and tests that
/// run no images.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoImageCache;

#[async_trait]
impl ImageCache for NoImageCache {
    async fn ensure_image(&self, _: &Ctx, reference: &str) -> Result<Image, Status> {
        Err(Status::failed_precondition(format!(
            "image {reference}: this keeper has no image cache configured"
        )))
    }
}

/// The spec the worker mounts a bundle's rootfs from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct OverlaySpec {
    /// The format version, 1.
    pub version: u32,
    /// The image's manifest digest; the cache's GC protects images named
    /// here while the bundle exists.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_digest: String,
    /// The cached layers, bottom-most first.
    pub layers: Vec<String>,
    /// In-rootfs directories to create after mounting, as mount targets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_dirs: Vec<String>,
    /// Image volumes to expose beside the rootfs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub image_volumes: Vec<ImageVolumeOverlay>,
}

/// One image volume's contents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ImageVolumeOverlay {
    /// The volume's name.
    pub name: String,
    /// The image's manifest digest.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub image_digest: String,
    /// The cached layers, bottom-most first.
    pub layers: Vec<String>,
}
