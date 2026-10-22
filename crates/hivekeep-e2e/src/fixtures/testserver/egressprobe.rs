//! The `egressprobe` subcommand: drives the egress gateway from inside the
//! cluster with a credential of the caller's choosing and reports how far the
//! attempt got -- which hop refused it, or the certificate it was served.
//!
//! It runs as a pod rather than in the test process because the credentials
//! worth testing -- a pod's own workload identity, a leaf for an Actor that
//! does not exist -- are only obtainable, or only interesting, in-cluster.

use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response, StatusCode};
use hyper::body::Incoming;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use serde::{Deserialize, Serialize};

use hivekeep_identity::pem;

use super::egress_client::{Client, ClientConfig, EgressError};
use super::http::url_pairs;
use super::serve::{handler, json, text, Body, Handler};
use super::tls::crypto_provider;

/// The CONNECT authority. It must be a literal IP (the gateway takes the
/// destination from the original destination address and rejects names), and
/// the gateway routes every CONNECT to its interception listener whatever the
/// authority, so a documentation address that resolves nowhere is enough. The
/// name under test travels in the tunneled ClientHello instead.
pub const TUNNEL_DESTINATION: &str = "192.0.2.1:443";

/// A local failure before anything was dialed: a bad flag, an unreadable
/// credential bundle. Never a statement about the gateway.
pub const STAGE_CLIENT: &str = "client";
/// The front door's mutual TLS: the gateway refused the client certificate,
/// or presented one the probe would not accept.
pub const STAGE_GATEWAY_TLS: &str = "gateway_tls";
/// The CONNECT exchange: the certificate was accepted and the request
/// declined anyway, which is where authorization lives.
pub const STAGE_CONNECT: &str = "connect";
/// Any other failure opening the tunnel: DNS, TCP, a truncated response.
pub const STAGE_TUNNEL: &str = "tunnel";
/// The tunneled TLS handshake the gateway answers with a minted leaf; a
/// failure here is the minter's, not the front door's.
pub const STAGE_INNER_HANDSHAKE: &str = "inner_handshake";

/// The gateway coordinates a handshake needs.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    /// `host:port` of the CONNECT front door.
    pub gateway_address: String,
    /// PEM roots for the gateway's serving certificate.
    pub trust_bundle_path: String,
    /// Budget for one CONNECT plus inner handshake.
    pub handshake_timeout: Duration,
}

/// The `/handshake` response body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HandshakeResult {
    /// The SNI asked for.
    pub sni: String,
    /// The credential bundle presented, echoed so a result cannot be mistaken
    /// for one taken with another identity.
    pub credential: String,
    /// Whether the inner handshake completed. A denied SNI is a normal
    /// outcome, reported as `ok: false` with the reason.
    pub ok: bool,
    /// Where a failed handshake stopped; one of the `STAGE_*` constants.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stage: String,
    /// The gateway's answer to the CONNECT, when `stage` is `connect`.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub connect_status: u16,
    /// The failure.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    /// The chain the gateway presented, leaf first.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub chain_pem: String,
}

fn is_zero(v: &u16) -> bool {
    *v == 0
}

/// The stage an [`EgressError`] stopped at, read from its variant rather than
/// its message.
pub fn dial_stage(err: &EgressError) -> &'static str {
    match err {
        EgressError::Config(_) => STAGE_CLIENT,
        EgressError::GatewayHandshake(_) => STAGE_GATEWAY_TLS,
        EgressError::ConnectRejected { .. } => STAGE_CONNECT,
        EgressError::Tunnel(_) => STAGE_TUNNEL,
    }
}

/// The host half of a `host:port` address, split by hand so a malformed flag
/// surfaces at handshake time with the address in the message.
pub fn server_name(address: &str) -> &str {
    match address.rfind(':') {
        Some(i) => &address[..i],
        None => address,
    }
}

/// Accepts any certificate: verifying the minted leaf is the caller's
/// assertion, made on the chain this reports.
#[derive(Debug)]
struct AcceptAny(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Opens a tunnel and completes the inner handshake for `sni`, returning the
/// chain served, or the failing stage and why.
async fn fetch_chain(
    sni: &str,
    credential_bundle: &str,
    cfg: &ProbeConfig,
) -> Result<Vec<Vec<u8>>, (&'static str, String, u16)> {
    let client = Client::new(&ClientConfig {
        gateway_address: cfg.gateway_address.clone(),
        server_name: server_name(&cfg.gateway_address).to_string(),
        credential_bundle: credential_bundle.to_string(),
        trust_bundle_path: cfg.trust_bundle_path.clone(),
    })
    .map_err(|e| (STAGE_CLIENT, format!("building egress client: {e}"), 0))?;
    let tunnel = client.dial(TUNNEL_DESTINATION).await.map_err(|e| {
        let status = match &e {
            EgressError::ConnectRejected { status_code, .. } => *status_code,
            _ => 0,
        };
        (dial_stage(&e), format!("opening tunnel: {e}"), status)
    })?;
    let inner_err = |e: String| {
        (
            STAGE_INNER_HANDSHAKE,
            format!("inner TLS handshake for {sni:?}: {e}"),
            0,
        )
    };
    let provider = crypto_provider();
    let config = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| inner_err(e.to_string()))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        .with_no_client_auth();
    let name = ServerName::try_from(sni.to_string()).map_err(|e| inner_err(e.to_string()))?;
    let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
        .connect(name, tunnel)
        .await
        .map_err(|e| inner_err(e.to_string()))?;
    let chain: Vec<Vec<u8>> = tls
        .get_ref()
        .1
        .peer_certificates()
        .map(|c| c.iter().map(|d| d.to_vec()).collect())
        .unwrap_or_default();
    if chain.is_empty() {
        return Err((
            STAGE_INNER_HANDSHAKE,
            format!("handshake for {sni:?} completed with no peer certificates"),
            0,
        ));
    }
    Ok(chain)
}

async fn handshake(req: Request<Incoming>, cfg: ProbeConfig) -> Response<Body> {
    let params = url_pairs(req.uri().query().unwrap_or_default());
    let param = |key: &str| {
        params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    let sni = param("sni");
    if sni.is_empty() {
        return text(StatusCode::BAD_REQUEST, "missing sni query parameter\n");
    }
    // Required, not defaulted: the credential is the variable every caller
    // turns, and naming it per request lets one pod serve every case.
    let credential = param("credential-bundle");
    if credential.is_empty() {
        return text(
            StatusCode::BAD_REQUEST,
            "missing credential-bundle query parameter\n",
        );
    }
    let mut result = HandshakeResult {
        sni: sni.clone(),
        credential: credential.clone(),
        ..Default::default()
    };
    match tokio::time::timeout(cfg.handshake_timeout, fetch_chain(&sni, &credential, &cfg)).await {
        Ok(Ok(chain)) => {
            result.ok = true;
            result.chain_pem = pem::encode_certificates(chain.iter().map(Vec::as_slice));
        }
        Ok(Err((stage, error, status))) => {
            result.stage = stage.to_string();
            result.error = error;
            result.connect_status = status;
        }
        Err(_) => {
            result.stage = STAGE_TUNNEL.to_string();
            result.error = format!(
                "handshake for {sni:?} timed out after {:?}",
                cfg.handshake_timeout
            );
        }
    }
    json(&result)
}

/// The probe's routes: `/handshake?sni=&credential-bundle=` and `/healthz`.
pub fn egress_probe_handler(cfg: ProbeConfig) -> Handler {
    handler(move |req: Request<Incoming>| {
        let cfg = cfg.clone();
        async move {
            match req.uri().path() {
                "/handshake" => handshake(req, cfg).await,
                "/healthz" => text(StatusCode::OK, ""),
                _ => text(StatusCode::NOT_FOUND, "404 page not found\n"),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_name_is_the_host_half() {
        assert_eq!(
            server_name("gate-egress.hivekeep-system.svc:443"),
            "gate-egress.hivekeep-system.svc"
        );
        assert_eq!(server_name("no-port"), "no-port");
        assert_eq!(server_name("[::1]:443"), "[::1]");
    }

    #[test]
    fn stages_follow_the_error_variant() {
        assert_eq!(dial_stage(&EgressError::Config("x".into())), STAGE_CLIENT);
        assert_eq!(
            dial_stage(&EgressError::GatewayHandshake("x".into())),
            STAGE_GATEWAY_TLS
        );
        assert_eq!(
            dial_stage(&EgressError::ConnectRejected {
                status_code: 403,
                status: "403 Forbidden".into(),
                message: "denied".into()
            }),
            STAGE_CONNECT
        );
        assert_eq!(dial_stage(&EgressError::Tunnel("x".into())), STAGE_TUNNEL);
    }

    #[test]
    fn result_omits_empty_fields() {
        let ok = HandshakeResult {
            sni: "a".into(),
            credential: "c".into(),
            ok: true,
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_string(&ok).unwrap(),
            r#"{"sni":"a","credential":"c","ok":true}"#
        );
    }

    #[tokio::test]
    async fn unreadable_credentials_fail_at_the_client_stage() {
        let cfg = ProbeConfig {
            gateway_address: "127.0.0.1:1".into(),
            trust_bundle_path: "/nonexistent".into(),
            handshake_timeout: Duration::from_secs(2),
        };
        let (stage, _, status) = fetch_chain("example.com", "/nonexistent", &cfg)
            .await
            .unwrap_err();
        assert_eq!((stage, status), (STAGE_CLIENT, 0));
    }
}
