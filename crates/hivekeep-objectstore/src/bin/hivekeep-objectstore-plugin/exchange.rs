//! Control mode's credential exchange: the handler for the configured
//! backend, and its TLS listener beside the plugin socket.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use hivekeep_identity::jwt::verifier::{JwksFileSource, KeySource, Verifier};
use hivekeep_objectstore::plugin::actorlookup::{ActorLookup, Config as LookupConfig};
use hivekeep_objectstore::plugin::exchange::{
    ExchangeServer, UnimplementedExchange, ACTOR_JWT_AUDIENCE,
};
use hivekeep_objectstore::plugin::gcsissuer::{GcsIssuer, IssuerOptions};
use hivekeep_objectstore::proto::exchange::v1::credential_exchange_server::{
    CredentialExchange, CredentialExchangeServer,
};
use hivekeep_objectstore::storage::env::uses_s3;
use tokio_util::sync::CancellationToken;

/// What control mode needs to serve the exchange.
#[derive(Debug, Clone, Default)]
pub struct ExchangeConfig {
    /// TCP address to serve on; empty leaves the exchange unserved.
    pub listen: String,
    /// The PEM credential bundle served, the pod's service DNS certificate.
    pub cred_bundle: PathBuf,
    /// The issuer of the actor JWTs accepted.
    pub issuer: String,
    /// The JWK set the JWTs are verified with.
    pub jwks_file: PathBuf,
    /// The service account snapshot transfers run as, on GCS.
    pub transfer_account: String,
    /// How hivekeep-api is reached.
    pub api: LookupConfig,
}

/// The exchange for the configured backend. GCS serves it; S3 answers
/// UNIMPLEMENTED, since its node plugin transfers with the pod's own
/// credentials and never calls it.
pub fn new_exchange_service(cfg: &ExchangeConfig) -> Result<Arc<dyn CredentialExchange>> {
    if uses_s3() {
        return Ok(Arc::new(UnimplementedExchange));
    }
    if cfg.issuer.is_empty() {
        bail!("--actor-jwt-issuer is required to serve the exchange on GCS");
    }
    if cfg.jwks_file.as_os_str().is_empty() {
        bail!("--actor-jwt-jwks-file is required to serve the exchange on GCS");
    }
    if cfg.transfer_account.is_empty() {
        bail!("--transfer-service-account is required to serve the exchange on GCS");
    }
    // Read the key set once now, so a wrong path fails the start rather
    // than the first exchange.
    let keys = JwksFileSource::new(&cfg.jwks_file);
    keys.keys().context("while reading the actor JWT key set")?;
    let verifier = Verifier::new(
        cfg.issuer.clone(),
        vec![ACTOR_JWT_AUDIENCE.into()],
        Arc::new(keys),
    )
    .with_actor_claims();
    let lookup = ActorLookup::new(cfg.api.clone()).map_err(anyhow::Error::msg)?;
    let issuer = GcsIssuer::new(&cfg.transfer_account, IssuerOptions::default())
        .context("while setting up the GCS credential issuer")?;
    Ok(Arc::new(ExchangeServer::new(
        Arc::new(verifier),
        Arc::new(lookup),
        Arc::new(issuer),
    )))
}

struct Shared(Arc<dyn CredentialExchange>);

#[tonic::async_trait]
impl CredentialExchange for Shared {
    async fn exchange_actor_jwt(
        &self,
        req: tonic::Request<hivekeep_objectstore::proto::exchange::v1::ExchangeActorJwtRequest>,
    ) -> Result<
        tonic::Response<hivekeep_objectstore::proto::exchange::v1::ExchangeActorJwtResponse>,
        tonic::Status,
    > {
        self.0.exchange_actor_jwt(req).await
    }
}

/// Starts the exchange on its own TLS listener and returns its task; it
/// drains when `stop` fires. The exchange runs TLS without client
/// certificates: the JWT is the credential.
pub async fn serve_exchange(
    cfg: &ExchangeConfig,
    stop: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    let svc = new_exchange_service(cfg)?;
    let tls = hivekeep_objectstore::tls::server_config(&cfg.cred_bundle)
        .map_err(|e| anyhow::anyhow!("while loading the exchange serving certificate: {e}"))?;
    let lis = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("while listening on {}", cfg.listen))?;
    let addr = lis.local_addr()?;
    let health = hivekeep_objectstore::pluginsocket::serving_health().await;
    let incoming = hivekeep_objectstore::tls::serve_incoming(lis, tls);
    tracing::info!(address = %addr, "Serving credential exchange");
    Ok(tokio::spawn(async move {
        let res = tonic::transport::Server::builder()
            .add_service(CredentialExchangeServer::new(Shared(svc)))
            .add_service(health)
            .serve_with_incoming_shutdown(incoming, stop.cancelled_owned())
            .await;
        // Anything but a drain leaves node plugins without an exchange while
        // the socket keeps the pod looking healthy, so the process exits.
        if let Err(e) = res {
            tracing::error!(err = %e, "Credential exchange failed");
            std::process::exit(1);
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_exchange_service_checks_its_config() {
        let _env = hivekeep_objectstore::testing::ENV_LOCK.lock();
        // SAFETY: the lock serializes environment changes.
        unsafe { std::env::set_var("HK_STORAGE_BACKEND", "s3") };
        assert!(
            new_exchange_service(&ExchangeConfig::default()).is_ok(),
            "S3 answers unimplemented"
        );
        unsafe { std::env::set_var("HK_STORAGE_BACKEND", "gcs") };
        let dir = tempfile::tempdir().unwrap();
        for (name, cfg, want) in [
            (
                "GCS needs the issuer",
                ExchangeConfig {
                    jwks_file: "jwks.json".into(),
                    ..Default::default()
                },
                "--actor-jwt-issuer",
            ),
            (
                "GCS needs the JWKS file",
                ExchangeConfig {
                    issuer: "https://idp.hivekeep-system.svc".into(),
                    ..Default::default()
                },
                "--actor-jwt-jwks-file",
            ),
            (
                "GCS needs the transfer account",
                ExchangeConfig {
                    issuer: "https://idp.hivekeep-system.svc".into(),
                    jwks_file: "jwks.json".into(),
                    ..Default::default()
                },
                "--transfer-service-account",
            ),
            (
                "GCS needs a readable JWKS file",
                ExchangeConfig {
                    issuer: "https://idp.hivekeep-system.svc".into(),
                    jwks_file: dir.path().join("absent.json"),
                    transfer_account: "xfer@proj.iam.gserviceaccount.com".into(),
                    ..Default::default()
                },
                "key set",
            ),
        ] {
            let e = new_exchange_service(&cfg)
                .err()
                .unwrap_or_else(|| panic!("{name}: accepted"));
            assert!(format!("{e:#}").contains(want), "{name}: {e:#}");
        }
        unsafe { std::env::remove_var("HK_STORAGE_BACKEND") };
    }

    #[test]
    fn missing_serving_bundle_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        assert!(hivekeep_objectstore::tls::server_config(&dir.path().join("absent.pem")).is_err());
    }
}
