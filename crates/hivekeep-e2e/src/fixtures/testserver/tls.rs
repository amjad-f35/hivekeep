//! TLS for the origins that also serve HTTPS.

use std::sync::Arc;

use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

use super::serve::{bind_address, serve_listeners, Handler};

/// Where `http` and `websocket` serve HTTPS when given a certificate, beside
/// their cleartext `--listen`. A fixed port, which the server pod manifest
/// publishes.
pub const TLS_LISTEN_ADDRESS: &str = ":8443";

/// Why TLS could not be set up.
#[derive(Debug, thiserror::Error)]
pub enum TlsSetupError {
    /// Only one of the two flags was given.
    #[error("--tls-cert and --tls-key must be provided together")]
    Incomplete,
    /// The files could not be read or parsed.
    #[error("loading TLS certificate and key: {0}")]
    Load(String),
}

/// The process's one crypto provider.
pub fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_pair(
    cert_file: &str,
    key_file: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let cert_pem = std::fs::read(cert_file).map_err(|e| format!("{cert_file}: {e}"))?;
    let key_pem = std::fs::read(key_file).map_err(|e| format!("{key_file}: {e}"))?;
    let certs = rustls_pemfile::certs(&mut cert_pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("{cert_file}: {e}"))?;
    if certs.is_empty() {
        return Err(format!("{cert_file}: no certificates"));
    }
    let key = rustls_pemfile::private_key(&mut key_pem.as_slice())
        .map_err(|e| format!("{key_file}: {e}"))?
        .ok_or_else(|| format!("{key_file}: no private key"))?;
    Ok((certs, key))
}

/// Loads the origin certificate and key, offering only HTTP/1.1 over TLS.
/// `Ok(None)` when neither is given; bad credentials fail here, before any
/// listener starts.
pub fn configure_tls_server(
    cert_file: &str,
    key_file: &str,
) -> Result<Option<TlsAcceptor>, TlsSetupError> {
    if cert_file.is_empty() && key_file.is_empty() {
        return Ok(None);
    }
    if cert_file.is_empty() || key_file.is_empty() {
        return Err(TlsSetupError::Incomplete);
    }
    let (certs, key) = load_pair(cert_file, key_file).map_err(TlsSetupError::Load)?;
    let mut config = rustls::ServerConfig::builder_with_provider(crypto_provider())
        .with_protocol_versions(&[&rustls::version::TLS12, &rustls::version::TLS13])
        .map_err(|e| TlsSetupError::Load(e.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| TlsSetupError::Load(e.to_string()))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Some(TlsAcceptor::from(Arc::new(config))))
}

/// Serves `handler` in cleartext on `listen` and, when a certificate is
/// given, over TLS on [`TLS_LISTEN_ADDRESS`].
pub async fn serve_http(
    handler: Handler,
    listen: &str,
    cert_file: &str,
    key_file: &str,
) -> anyhow::Result<()> {
    let acceptor = configure_tls_server(cert_file, key_file)?;
    let cleartext = TcpListener::bind(bind_address(listen)).await?;
    let encrypted = match acceptor {
        Some(acceptor) => {
            let listener = TcpListener::bind(bind_address(TLS_LISTEN_ADDRESS)).await?;
            eprintln!("testserver: serving HTTPS on {TLS_LISTEN_ADDRESS}");
            Some((listener, acceptor))
        }
        None => None,
    };
    serve_listeners(handler, cleartext, encrypted).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_incomplete_or_unreadable_credentials() {
        for (name, cert, key) in [
            ("missing key", "cert.pem", ""),
            ("missing cert", "", "key.pem"),
            ("unreadable pair", "missing-cert.pem", "missing-key.pem"),
        ] {
            assert!(
                configure_tls_server(cert, key).is_err(),
                "{name}: configure_tls_server succeeded, want error"
            );
        }
    }

    #[test]
    fn no_credentials_means_cleartext_only() {
        assert!(configure_tls_server("", "").unwrap().is_none());
    }
}
