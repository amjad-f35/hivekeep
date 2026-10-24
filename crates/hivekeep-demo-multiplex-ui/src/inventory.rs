//! Reading Workers and Actors from hivekeep-api.
//!
//! The dashboard dials an explicit endpoint (`HK_API_ENDPOINT`, e.g. a
//! `kubectl port-forward` to the `api` Service) over plaintext gRPC, with an
//! optional bearer token from `HK_API_TOKEN`. It does not open a
//! port-forward or mint ServiceAccount tokens by itself.

use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::Status;

/// Env var naming the hivekeep-api endpoint (`http://host:port`).
pub const API_ENDPOINT_ENV: &str = "HK_API_ENDPOINT";
/// Env var holding an optional bearer token.
pub const API_TOKEN_ENV: &str = "HK_API_TOKEN";
/// Deadline for each control-plane call.
pub const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// Page size used when listing.
const PAGE_SIZE: i32 = 500;

/// The control-plane reads the dashboard needs.
#[async_trait]
pub trait Inventory: Send + Sync {
    /// Every Worker.
    async fn list_workers(&self) -> Result<Vec<pb::Worker>, Status>;
    /// Every Actor across all hives.
    async fn list_actors(&self) -> Result<Vec<pb::Actor>, Status>;
}

/// Adds the bearer token, if any, to each call.
#[derive(Clone)]
pub struct BearerAuth {
    token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl tonic::service::Interceptor for BearerAuth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(t) = &self.token {
            req.metadata_mut().insert("authorization", t.clone());
        }
        Ok(req)
    }
}

type Client = pb::control_client::ControlClient<InterceptedService<Channel, BearerAuth>>;

/// [`Inventory`] over a gRPC connection to hivekeep-api.
pub struct GrpcInventory {
    client: Client,
}

impl GrpcInventory {
    /// Dials `endpoint` up front so a wrong address fails at startup rather
    /// than on every page refresh.
    pub async fn connect(endpoint: &str, token: Option<&str>) -> anyhow::Result<Self> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| anyhow::anyhow!("invalid {API_ENDPOINT_ENV} {endpoint:?}: {e}"))?
            .timeout(RPC_TIMEOUT)
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("connecting to hivekeep-api at {endpoint}: {e}"))?;
        let token = token
            .map(|t| format!("Bearer {t}").parse())
            .transpose()
            .map_err(|_| anyhow::anyhow!("{API_TOKEN_ENV} is not a valid header value"))?;
        Ok(Self {
            client: pb::control_client::ControlClient::with_interceptor(
                channel,
                BearerAuth { token },
            ),
        })
    }
}

#[async_trait]
impl Inventory for GrpcInventory {
    async fn list_workers(&self) -> Result<Vec<pb::Worker>, Status> {
        let mut out = Vec::new();
        let mut page_token = String::new();
        loop {
            let req = pb::ListWorkersRequest {
                options: Some(pb::ListOptions {
                    page_size: PAGE_SIZE,
                    page_token,
                }),
            };
            let resp = self.client.clone().list_workers(req).await?.into_inner();
            out.extend(resp.workers);
            if resp.next_page_token.is_empty() {
                return Ok(out);
            }
            page_token = resp.next_page_token;
        }
    }

    async fn list_actors(&self) -> Result<Vec<pb::Actor>, Status> {
        let mut out = Vec::new();
        let mut page_token = String::new();
        loop {
            let req = pb::ListActorsRequest {
                hive: String::new(),
                options: Some(pb::ListOptions {
                    page_size: PAGE_SIZE,
                    page_token,
                }),
            };
            let resp = self.client.clone().list_actors(req).await?.into_inner();
            out.extend(resp.actors);
            if resp.next_page_token.is_empty() {
                return Ok(out);
            }
            page_token = resp.next_page_token;
        }
    }
}
