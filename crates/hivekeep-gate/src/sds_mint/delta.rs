//! One delta SDS stream: subscriptions are answered with freshly minted
//! leaves (refused names withdrawn); ACKs, NACKs, unsubscribes and
//! replays of resumed versions are answered with nothing.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use envoy_types::pb::envoy::config::core::v3::{data_source::Specifier, DataSource};
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::{
    secret, Secret, TlsCertificate,
};
use envoy_types::pb::envoy::service::discovery::v3::{
    DeltaDiscoveryRequest, DeltaDiscoveryResponse, Resource,
};
use envoy_types::pb::google::protobuf::{Any, Duration as PbDuration};
use futures::{Stream, StreamExt};
use hivekeep_identity::host_cert::{HostCertMinter, MintedCert};
use prost::Message;
use tokio::sync::mpsc;

use super::SECRET_TYPE_URL;

fn inline(b: &str) -> Option<DataSource> {
    Some(DataSource {
        specifier: Some(Specifier::InlineBytes(b.as_bytes().to_vec())),
        ..Default::default()
    })
}

/// The SDS secret for a minted leaf.
pub fn to_secret(name: &str, c: &MintedCert) -> Secret {
    Secret {
        name: name.to_string(),
        r#type: Some(secret::Type::TlsCertificate(TlsCertificate {
            certificate_chain: inline(&c.cert_chain_pem),
            private_key: inline(&c.private_key_pem),
            ..Default::default()
        })),
    }
}

/// A resource for `name`: versioned by serial, with a TTL a minute short
/// of the leaf's expiry (at least a second).
pub fn pack(name: &str, c: &MintedCert) -> Resource {
    let remaining = c
        .not_after
        .duration_since(SystemTime::now())
        .unwrap_or_default();
    let ttl = remaining
        .saturating_sub(Duration::from_secs(60))
        .max(Duration::from_secs(1));
    Resource {
        name: name.to_string(),
        version: c.serial.clone(),
        resource: Some(Any {
            type_url: SECRET_TYPE_URL.into(),
            value: to_secret(name, c).encode_to_vec(),
        }),
        ttl: Some(PbDuration {
            seconds: ttl.as_secs() as i64,
            nanos: ttl.subsec_nanos() as i32,
        }),
        ..Default::default()
    }
}

/// A protocol violation ending the stream.
#[derive(Debug, thiserror::Error)]
#[error("unexpected type_url {0:?} on the SDS stream")]
pub struct WrongTypeUrl(pub String);

/// Serves one stream: reads `requests` and sends answers on `out` until
/// the client ends the stream or `out` closes.
pub async fn serve_stream<S>(
    minter: Arc<HostCertMinter>,
    nonce: Arc<AtomicU64>,
    mut requests: S,
    out: mpsc::Sender<DeltaDiscoveryResponse>,
) -> Result<(), WrongTypeUrl>
where
    S: Stream<Item = Result<DeltaDiscoveryRequest, tonic::Status>> + Unpin,
{
    loop {
        let req = tokio::select! {
            r = requests.next() => r,
            _ = out.closed() => return Ok(()),
        };
        let Some(Ok(req)) = req else {
            return Ok(());
        };
        if !req.type_url.is_empty() && req.type_url != SECRET_TYPE_URL {
            return Err(WrongTypeUrl(req.type_url));
        }
        if let Some(ed) = &req.error_detail {
            tracing::error!(message = %ed.message, code = ed.code, nonce = %req.response_nonce, "Envoy NACKed an SDS response");
            continue;
        }
        if req.resource_names_subscribe.is_empty() {
            continue;
        }
        let mut resources = Vec::new();
        let mut removed = Vec::new();
        for name in &req.resource_names_subscribe {
            match minter.certificate(name, SystemTime::now()) {
                Ok(c) => resources.push(pack(name, &c)),
                Err(err) => {
                    tracing::warn!(host = %name, %err, "certificate request denied");
                    removed.push(name.clone());
                }
            }
        }
        let resp = DeltaDiscoveryResponse {
            type_url: SECRET_TYPE_URL.into(),
            resources,
            removed_resources: removed,
            nonce: (nonce.fetch_add(1, Ordering::SeqCst) + 1).to_string(),
            ..Default::default()
        };
        if out.send(resp).await.is_err() {
            return Ok(());
        }
    }
}

#[cfg(test)]
#[path = "delta_tests.rs"]
mod tests;
