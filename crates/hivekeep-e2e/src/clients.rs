//! The clients a suite talks to the cluster through: Kubernetes, and the
//! Hivekeep Control API reached over a port-forward to its Service.
//!
//! The API connection verifies hivekeep-api's service-DNS serving
//! certificate against the signer's live ClusterTrustBundles and
//! authenticates with a bearer token minted for the client ServiceAccount,
//! the same path an operator's CLI takes from outside the cluster.

use std::time::Duration;

use hivekeep_proto::api::v1::control_client::ControlClient;
use k8s_openapi::api::authentication::v1::{TokenRequest, TokenRequestSpec};
use k8s_openapi::api::core::v1::ServiceAccount;
use kube::api::{Api, PostParams};
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::env::{resource_name, system_namespace};
use crate::install;
use crate::portforward::{self, Forward};

/// Overrides the ServiceAccount the bearer token is minted from.
pub const CLIENT_SERVICE_ACCOUNT_ENV: &str = "HK_E2E_CLIENT_SERVICE_ACCOUNT";

/// Adds the bearer token and, when the install enabled preview gates, the
/// preview opt-in to every call.
#[derive(Clone)]
pub struct ApiAuth {
    token: Option<AsciiMetadataValue>,
    preview: bool,
}

impl ApiAuth {
    /// Sends `token` as a bearer token (none when `None`) and opts in to
    /// preview features when `preview` is set.
    pub fn new(token: Option<&str>, preview: bool) -> Result<Self, String> {
        let token = token
            .map(|t| {
                MetadataValue::try_from(format!("Bearer {t}"))
                    .map_err(|e| format!("bearer token is not valid metadata: {e}"))
            })
            .transpose()?;
        Ok(ApiAuth { token, preview })
    }
}

impl Interceptor for ApiAuth {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.token {
            req.metadata_mut().insert("authorization", token.clone());
        }
        if self.preview {
            req.metadata_mut().insert(
                hivekeep_core::preview::PREVIEW_HEADER,
                MetadataValue::from_static("true"),
            );
        }
        Ok(req)
    }
}

/// The Control API client type.
pub type ControlApi = ControlClient<InterceptedService<Channel, ApiAuth>>;

/// The shared clients of a suite run.
pub struct Clients {
    kube: kube::Client,
    api: ControlApi,
    _forward: Option<Forward>,
}

impl Clients {
    /// Wraps already-built clients (tests use this with in-process fakes).
    pub fn from_parts(kube: kube::Client, api: ControlApi) -> Self {
        Clients {
            kube,
            api,
            _forward: None,
        }
    }

    /// The Kubernetes client.
    pub fn kube(&self) -> kube::Client {
        self.kube.clone()
    }

    /// The Control API client (a cheap clone of the shared connection).
    pub fn api(&self) -> ControlApi {
        self.api.clone()
    }

    /// Connects to the cluster named by the run settings.
    pub async fn connect() -> Result<Clients, String> {
        let path = crate::settings::kubeconfig().ok_or_else(|| {
            format!(
                "neither {} nor KUBECONFIG is set",
                crate::settings::KUBECONFIG_ENV
            )
        })?;
        let kube = crate::kube_util::load_client(&path, crate::settings::kube_context().as_deref())
            .await?;
        let namespace = system_namespace();
        let forward = portforward::service_port_forward(
            &kube,
            &namespace,
            &resource_name(install::API_SERVICE),
            install::API_SERVICE_PORT,
        )
        .await?;
        let server_name = install::api_server_name(&namespace);
        let roots = crate::kube_util::live_signer_roots(&kube, install::SERVICE_DNS_SIGNER).await?;
        let token = mint_token(&kube, &namespace, &server_name).await?;
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(roots))
            .domain_name(server_name);
        let channel = Endpoint::from_shared(format!("https://{}", forward.addr()))
            .map_err(|e| e.to_string())?
            .tls_config(tls)
            .map_err(|e| format!("API TLS config: {e}"))?
            .connect_timeout(Duration::from_secs(30))
            .connect()
            .await
            .map_err(|e| format!("dialing hivekeep-api over the tunnel: {e}"))?;
        let auth = ApiAuth::new(Some(&token), crate::preview::any_enabled())?;
        Ok(Clients {
            kube,
            api: ControlClient::with_interceptor(channel, auth),
            _forward: Some(forward),
        })
    }
}

/// The ServiceAccount the harness mints its token from.
pub fn client_service_account() -> String {
    match std::env::var(CLIENT_SERVICE_ACCOUNT_ENV) {
        Ok(v) if !v.is_empty() => v,
        _ => resource_name(install::CLIENT_SERVICE_ACCOUNT),
    }
}

/// Mints an hour-long token for the client ServiceAccount, with the API's
/// DNS name as audience. Anyone Kubernetes lets mint for that account gets
/// the same identity.
async fn mint_token(
    kube: &kube::Client,
    namespace: &str,
    audience: &str,
) -> Result<String, String> {
    let api: Api<ServiceAccount> = Api::namespaced(kube.clone(), namespace);
    let request = TokenRequest {
        spec: Some(TokenRequestSpec {
            audiences: Some(vec![audience.to_string()]),
            expiration_seconds: Some(3600),
            ..Default::default()
        }),
        ..Default::default()
    };
    let sa = client_service_account();
    let issued = api
        .create_token_request(&sa, &PostParams::default(), &request)
        .await
        .map_err(|e| {
            format!("failed to request a hivekeep-api bearer token for {namespace}/{sa}: {e}")
        })?;
    let token = issued.status.and_then(|s| s.token).unwrap_or_default();
    if token.is_empty() {
        return Err(
            "failed to request a hivekeep-api bearer token: token response was empty".into(),
        );
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interceptor_adds_token_and_preview() {
        let mut auth = ApiAuth::new(Some("tok"), true).unwrap();
        let req = auth.call(Request::new(())).unwrap();
        assert_eq!(req.metadata().get("authorization").unwrap(), "Bearer tok");
        assert_eq!(
            req.metadata()
                .get(hivekeep_core::preview::PREVIEW_HEADER)
                .unwrap(),
            "true"
        );
        let mut bare = ApiAuth::new(None, false).unwrap();
        let req = bare.call(Request::new(())).unwrap();
        assert!(req.metadata().is_empty());
        assert!(ApiAuth::new(Some("bad\ntoken"), false).is_err());
    }
}
