//! SDS secrets: certificates and trust bundles Envoy reads from (and
//! watches on) disk.

use std::path::Path;

use envoy_types::pb::envoy::config::core::v3::WatchedDirectory;
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::{
    secret, CertificateValidationContext, Secret, TlsCertificate,
};

use super::consts::*;
use super::pb;
use super::XdsSettings;

/// The directory Envoy watches for rotations of `path`: a projected volume
/// swaps the whole directory atomically.
fn watched_directory(path: &str) -> WatchedDirectory {
    let dir = Path::new(path)
        .parent()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    WatchedDirectory {
        path: if dir.is_empty() { ".".to_string() } else { dir },
        ..Default::default()
    }
}

/// A certificate secret whose chain and key both live in the PEM bundle at
/// `path`.
pub(crate) fn build_cert_secret(name: &str, path: &str) -> Secret {
    Secret {
        name: name.to_string(),
        r#type: Some(secret::Type::TlsCertificate(TlsCertificate {
            certificate_chain: Some(pb::file_source(path)),
            private_key: Some(pb::file_source(path)),
            watched_directory: Some(watched_directory(path)),
            ..Default::default()
        })),
    }
}

/// A validation secret trusting the CAs in the PEM bundle at `path`.
pub(crate) fn build_trust_secret(name: &str, path: &str) -> Secret {
    Secret {
        name: name.to_string(),
        r#type: Some(secret::Type::ValidationContext(
            CertificateValidationContext {
                trusted_ca: Some(pb::file_source(path)),
                watched_directory: Some(watched_directory(path)),
                ..Default::default()
            },
        )),
    }
}

impl XdsSettings {
    /// The ingress serving certificate.
    pub(crate) fn build_tls_secret(&self) -> Secret {
        build_cert_secret(HTTPS_CERT_SECRET_NAME, &self.cert_path)
    }

    /// The CAs allowed to sign ingress client certificates.
    pub(crate) fn build_ingress_client_trust_secret(&self) -> Secret {
        build_trust_secret(
            INGRESS_CLIENT_TRUST_SECRET_NAME,
            &self.downstream_client_ca_path,
        )
    }

    /// The client certificate presented to workers.
    pub(crate) fn build_upstream_cert_secret(&self) -> Secret {
        build_cert_secret(
            UPSTREAM_CERT_SECRET_NAME,
            &self.upstream_credential_bundle_path,
        )
    }

    /// The trust bundle validating workers.
    pub(crate) fn build_upstream_trust_secret(&self) -> Secret {
        build_trust_secret(UPSTREAM_TRUST_SECRET_NAME, &self.upstream_trust_bundle_path)
    }
}
