//! TLS client configuration: trust roots from an instruction's PEM or from
//! the system (which honours `SSL_CERT_FILE` / `SSL_CERT_DIR`, how the
//! Actor's projected trust bundle is found), always on the ring provider.

use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;

/// Error text for a `rootCA` holding no usable certificate.
pub const INVALID_ROOT_CA: &str = "rootCA does not contain a valid certificate";

/// ALPN offering HTTP/2 then HTTP/1.1.
pub const ALPN_H2_AND_HTTP1: &[&[u8]] = &[b"h2", b"http/1.1"];
/// ALPN offering only HTTP/1.1.
pub const ALPN_HTTP1: &[&[u8]] = &[b"http/1.1"];

/// The crypto provider every TLS session uses.
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Installs ring as the process default, for libraries that build their own
/// configurations. Safe to call more than once.
pub fn install_default_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Trust roots parsed from PEM; at least one certificate must parse.
pub fn roots_from_pem(pem: &str) -> Result<RootCertStore, String> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(pem.as_bytes())
        .filter_map(Result::ok)
        .collect();
    let mut store = RootCertStore::empty();
    let (added, _) = store.add_parsable_certificates(certs);
    if added == 0 {
        return Err(INVALID_ROOT_CA.into());
    }
    Ok(store)
}

/// The system trust roots; unreadable entries are skipped.
///
/// `SSL_CERT_FILE` / `SSL_CERT_DIR` add to the image's public roots rather
/// than replace them: the Actor's projected gateway CA must be trusted for
/// intercepted origins while passthrough origins still verify publicly.
pub fn system_roots() -> RootCertStore {
    let mut store = RootCertStore::empty();
    let found = rustls_native_certs::load_native_certs();
    store.add_parsable_certificates(found.certs);
    if std::env::var_os("SSL_CERT_FILE").is_some() || std::env::var_os("SSL_CERT_DIR").is_some() {
        for path in [PUBLIC_BUNDLE, PUBLIC_BUNDLE_ALT] {
            if let Ok(iter) = CertificateDer::pem_file_iter(path) {
                store.add_parsable_certificates(iter.flatten());
            }
        }
    }
    store
}

/// Where Debian- and Alpine-based images keep the public CA bundle.
const PUBLIC_BUNDLE: &str = "/etc/ssl/certs/ca-certificates.crt";
/// Where Fedora/RHEL-based images keep it.
const PUBLIC_BUNDLE_ALT: &str = "/etc/pki/tls/certs/ca-bundle.crt";

/// Roots from `pem` when non-empty, else the system's.
pub fn roots(pem: &str) -> Result<RootCertStore, String> {
    if pem.is_empty() {
        Ok(system_roots())
    } else {
        roots_from_pem(pem)
    }
}

/// A verifying client configuration (TLS 1.2+) offering `alpn`.
pub fn client_config(roots: RootCertStore, alpn: &[&[u8]]) -> Arc<ClientConfig> {
    Arc::new(owned_client_config(roots, alpn))
}

/// [`client_config`] by value, for clients that take ownership.
pub fn owned_client_config(roots: RootCertStore, alpn: &[&[u8]]) -> ClientConfig {
    let mut config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .expect("ring supports the default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    config
}

/// Runs a TLS handshake over `stream`, verifying the peer as `server_name`.
pub async fn handshake(
    stream: TcpStream,
    server_name: &str,
    config: Arc<ClientConfig>,
) -> std::io::Result<TlsStream<TcpStream>> {
    let name = ServerName::try_from(server_name.to_string()).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("server name {server_name:?}: {e}"),
        )
    })?;
    TlsConnector::from(config).connect(name, stream).await
}

/// The issuer distinguished name of a DER certificate, most specific
/// component first (`CN=...,O=...`). Empty when the certificate does not
/// parse.
pub fn issuer_string(der: &[u8]) -> String {
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(der) else {
        return String::new();
    };
    let registry = x509_parser::objects::oid_registry();
    let rdns: Vec<String> = cert
        .issuer()
        .iter_rdn()
        .map(|rdn| {
            rdn.iter()
                .map(|attr| {
                    let key = x509_parser::objects::oid2abbrev(attr.attr_type(), registry)
                        .map(str::to_string)
                        .unwrap_or_else(|_| attr.attr_type().to_id_string());
                    let value = attr.as_str().map(str::to_string).unwrap_or_default();
                    format!("{key}={value}")
                })
                .collect::<Vec<_>>()
                .join("+")
        })
        .collect();
    rdns.into_iter().rev().collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_pem_is_refused() {
        assert_eq!(roots_from_pem("not-a-cert").unwrap_err(), INVALID_ROOT_CA);
        assert_eq!(roots_from_pem("").unwrap_err(), INVALID_ROOT_CA);
    }

    #[test]
    fn issuer_of_garbage_is_empty() {
        assert_eq!(issuer_string(b"nope"), "");
    }
}
