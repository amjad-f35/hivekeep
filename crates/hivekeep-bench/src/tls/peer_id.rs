//! The workload identity check on a peer certificate.

use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

/// Checks that the peer's leaf certificate (DER, first of `chain`) carries
/// the SPIFFE ID `want` as its first URI SAN. It complements chain
/// verification against the pod-identity trust bundle: the chain proves the
/// signer, this proves which workload.
pub fn verify_peer_id(chain: &[impl AsRef<[u8]>], want: &str) -> Result<(), String> {
    let leaf = chain.first().ok_or("peer presented no certificate")?;
    let (_, cert) = X509Certificate::from_der(leaf.as_ref())
        .map_err(|e| format!("parsing peer certificate: {e}"))?;
    let uris: Vec<String> = cert
        .subject_alternative_name()
        .map_err(|e| format!("parsing peer certificate SANs: {e}"))?
        .map(|ext| {
            ext.value
                .general_names
                .iter()
                .filter_map(|n| match n {
                    GeneralName::URI(u) => Some(u.to_string()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    match uris.first() {
        Some(first) if first == want => Ok(()),
        _ => Err(format!("peer SPIFFE ID {uris:?} is not {want:?}")),
    }
}
