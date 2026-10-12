//! The asset plugin: content-addressed sandbox assets from object storage.

use std::path::Path;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use super::status::to_status;
use crate::assetfetch::{check_object_name, parse_uri, Stager};
use crate::proto::v1::asset_provider_server::AssetProvider;
use crate::proto::v1::{FetchAssetRequest, FetchAssetResponse};
use crate::storage::{object_size, ObjectStorage};

/// Serves AssetProvider on a storage client. It reads URIs of its
/// backend's scheme (`gs://` on GCS, `s3://` on S3) as `<bucket>/<object>`,
/// and refuses any other scheme without reading: a `gs://` bucket name read
/// on S3 names someone else's bucket, or none. The keeper then reads a
/// `gs://` asset anonymously.
///
/// Each asset is staged in a directory private to the plugin and copied to
/// the caller's file only once its size and sha256 check out, so the caller
/// never reads bytes it did not pin, such as a snapshot's.
pub struct AssetPlugin {
    client: Arc<dyn ObjectStorage>,
    scheme: String,
    stager: Stager,
}

impl AssetPlugin {
    /// A plugin serving `scheme` (`gs` or `s3`, `client`'s backend), writing
    /// caller files only below `root` and staging in `staging_dir`, which
    /// must not be below `root`, at most `staging_capacity` bytes at once.
    pub fn new(
        client: Arc<dyn ObjectStorage>,
        scheme: &str,
        root: &Path,
        staging_dir: &Path,
        staging_capacity: i64,
    ) -> Result<Self, String> {
        if scheme != "gs" && scheme != "s3" {
            return Err(format!("asset URI scheme {scheme:?} is not gs or s3"));
        }
        Ok(AssetPlugin {
            client,
            scheme: scheme.to_string(),
            stager: Stager::new(root, staging_dir, staging_capacity)?,
        })
    }

    /// Downloads and verifies the asset, then replaces the caller's file.
    pub async fn fetch(
        &self,
        cancel: &CancellationToken,
        req: &FetchAssetRequest,
    ) -> Result<(), Status> {
        let (bucket, object) = parse_asset_uri(&req.asset_uri, &self.scheme)?;
        let client = self.client.clone();
        let open = || {
            let (client, bucket, object) = (client.clone(), bucket.clone(), object.clone());
            Box::pin(async move {
                let rc = client.get_object(&bucket, &object).await?;
                let size = object_size(&rc).unwrap_or(-1);
                Ok((rc, size))
            }) as futures::future::BoxFuture<'static, _>
        };
        self.stager
            .fetch(cancel, req, &req.asset_uri, &open, &to_status)
            .await
    }
}

/// Accepts `<scheme>://<bucket>/<object>` whose object name is canonical and
/// not under a snapshot location, returning the bucket and object.
pub fn parse_asset_uri(uri: &str, scheme: &str) -> Result<(String, String), Status> {
    let u = parse_uri(uri)?;
    if u.scheme != scheme {
        return Err(Status::invalid_argument(format!(
            "asset URI {uri}: scheme {:?} is not served by this plugin, whose storage backend serves {scheme}://",
            u.scheme
        )));
    }
    let object = u.path.strip_prefix('/').unwrap_or(&u.path).to_string();
    if u.host.is_empty() || object.is_empty() {
        return Err(Status::invalid_argument(format!(
            "asset URI {uri} must name a bucket and an object"
        )));
    }
    check_object_name(uri, &format!("{}://{}", u.scheme, u.host), &object)?;
    Ok((u.host, object))
}

#[tonic::async_trait]
impl AssetProvider for AssetPlugin {
    async fn fetch_asset(
        &self,
        req: Request<FetchAssetRequest>,
    ) -> Result<Response<FetchAssetResponse>, Status> {
        self.fetch(&CancellationToken::new(), req.get_ref()).await?;
        Ok(Response::new(FetchAssetResponse {}))
    }
}
