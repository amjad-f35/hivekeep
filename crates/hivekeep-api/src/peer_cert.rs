//! Reading identities out of DER certificates.

use x509_parser::extensions::GeneralName;
use x509_parser::prelude::{FromDer, X509Certificate};

/// The URI SANs of `der`, in order. Empty if it does not parse.
pub fn uri_sans(der: &[u8]) -> Vec<String> {
    let Ok((_, cert)) = X509Certificate::from_der(der) else {
        return Vec::new();
    };
    let Ok(Some(san)) = cert.subject_alternative_name() else {
        return Vec::new();
    };
    san.value
        .general_names
        .iter()
        .filter_map(|n| match n {
            GeneralName::URI(u) => Some(u.to_string()),
            _ => None,
        })
        .collect()
}

/// The first URI SAN of `der`, the SPIFFE ID of a workload certificate.
pub fn first_uri_san(der: &[u8]) -> Option<String> {
    uri_sans(der).into_iter().next().filter(|s| !s.is_empty())
}

/// Whether `der` carries the serverAuth extended key usage.
pub fn has_server_auth(der: &[u8]) -> bool {
    let Ok((_, cert)) = X509Certificate::from_der(der) else {
        return false;
    };
    matches!(cert.extended_key_usage(), Ok(Some(eku)) if eku.value.server_auth)
}
