//! Authenticating the Actor behind an egress CONNECT from the tunnel's
//! client certificate, as Envoy forwards it (`x-forwarded-client-cert`)
//! or as an alternative dataplane attaches it (`source.certificate`).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hivekeep_core::resource::ActorRef;
use hivekeep_core::spiffe::actor_ref_from_worker_for_actor_spiffe_id;
use rustls::pki_types::{CertificateDer, UnixTime};

use crate::router::extproc::RequestMetadata;
use crate::tunnel::keeper_dial::uri_sans;

/// Attribute an alternative dataplane carries the client certificate in.
pub const CLIENT_CERTIFICATE_ATTRIBUTE: &str = "source.certificate";
/// The header Envoy forwards the client certificate in.
pub const FORWARDED_CLIENT_CERT_HEADER: &str = "x-forwarded-client-cert";
const XFCC_CHAIN_KEY: &str = "chain";

/// Loads the CAs Actor certificates chain to (reloaded per request so a
/// rotation applies at once).
pub type RootsLoader = Arc<dyn Fn() -> Result<rustls::RootCertStore, String> + Send + Sync>;

/// Splits on `sep` outside double quotes, dropping empty parts.
pub fn split_unquoted(s: &str, sep: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut cur = String::new();
    let (mut quoted, mut escaped) = (false, false);
    for c in s.chars() {
        if escaped {
            cur.push(c);
            escaped = false;
        } else if quoted && c == '\\' {
            cur.push(c);
            escaped = true;
        } else if c == '"' {
            quoted = !quoted;
            cur.push(c);
        } else if c == sep && !quoted {
            parts.push(std::mem::take(&mut cur));
        } else {
            cur.push(c);
        }
    }
    parts.push(cur);
    parts
        .into_iter()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

fn unquote(v: &str) -> String {
    if v.len() < 2 || !v.starts_with('"') || !v.ends_with('"') {
        return v.to_string();
    }
    let mut out = String::new();
    let mut escaped = false;
    for c in v[1..v.len() - 1].chars() {
        if escaped {
            out.push(c);
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else {
            out.push(c);
        }
    }
    out
}

fn xfcc_value(element: &str, key: &str) -> Option<String> {
    split_unquoted(element, ';').into_iter().find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        k.trim()
            .eq_ignore_ascii_case(key)
            .then(|| unquote(v.trim()))
    })
}

/// Percent-decodes (`+` stays `+`).
fn path_unescape(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = s.get(i + 1..i + 3).ok_or("truncated escape")?;
            out.push(u8::from_str_radix(hex, 16).map_err(|_| format!("invalid escape %{hex}"))?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|e| e.to_string())
}

/// The DER certificates of a PEM chain.
pub fn parse_certificate_chain_pem(pem: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    let mut chain = Vec::new();
    for item in rustls_pemfile::certs(&mut &pem[..]) {
        let der = item.map_err(|e| format!("parsing the client certificate chain: {e}"))?;
        x509_parser::parse_x509_certificate(&der)
            .map_err(|e| format!("parsing the client certificate chain: {e}"))?;
        chain.push(der.to_vec());
    }
    if chain.is_empty() {
        return Err("client certificate value carries no certificate".into());
    }
    Ok(chain)
}

/// The chain in a single-element XFCC header.
pub fn parse_xfcc_chain(header: &str) -> Result<Vec<Vec<u8>>, String> {
    let elements = split_unquoted(header, ',');
    if elements.len() != 1 {
        return Err(format!(
            "expected exactly one {FORWARDED_CLIENT_CERT_HEADER} element, got {}",
            elements.len()
        ));
    }
    let encoded = xfcc_value(&elements[0], XFCC_CHAIN_KEY).ok_or_else(|| {
        format!("{FORWARDED_CLIENT_CERT_HEADER} carries no {XFCC_CHAIN_KEY:?} value")
    })?;
    let pem = path_unescape(&encoded)
        .map_err(|e| format!("decoding the client certificate chain: {e}"))?;
    parse_certificate_chain_pem(pem.as_bytes())
}

fn to_system(t: x509_parser::time::ASN1Time) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(t.timestamp().max(0) as u64)
}

/// Verifies an Actor tunnel certificate chain and returns its Actor.
pub fn verify_actor_certificate(
    chain: &[Vec<u8>],
    roots: rustls::RootCertStore,
) -> Result<ActorRef, String> {
    let leaf_der = chain.first().ok_or("empty chain")?;
    let (_, leaf) = x509_parser::parse_x509_certificate(leaf_der).map_err(|e| e.to_string())?;
    let now = SystemTime::now();
    let (nb, na) = (
        to_system(leaf.validity().not_before),
        to_system(leaf.validity().not_after),
    );
    if now < nb || now >= na {
        return Err("actor certificate is outside its validity period".into());
    }
    if leaf.is_ca() {
        return Err("actor certificate is a CA certificate".into());
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .map_err(|e| format!("actor-identity CA: {e}"))?;
    let intermediates: Vec<CertificateDer<'_>> = chain[1..]
        .iter()
        .map(|d| CertificateDer::from(d.as_slice()))
        .collect();
    verifier
        .verify_client_cert(
            &CertificateDer::from(leaf_der.as_slice()),
            &intermediates,
            UnixTime::now(),
        )
        .map_err(|e| format!("actor certificate is not signed by the actor-identity CA: {e}"))?;
    let uris = uri_sans(leaf_der);
    if uris.len() != 1 {
        return Err(format!(
            "actor certificate has {} URI SANs, want 1",
            uris.len()
        ));
    }
    actor_ref_from_worker_for_actor_spiffe_id(&uris[0])
        .map_err(|e| format!("while parsing actor from SPIFFE ID: {e}"))
}

/// The Actor whose tunnel certificate the request carries.
pub fn authenticate(
    md: &RequestMetadata,
    roots: rustls::RootCertStore,
) -> Result<ActorRef, String> {
    let attr = md.attribute(CLIENT_CERTIFICATE_ATTRIBUTE);
    let chain = if !attr.is_empty() {
        parse_certificate_chain_pem(attr.as_bytes())?
    } else {
        let header = md.header(FORWARDED_CLIENT_CERT_HEADER);
        if header.is_empty() {
            return Err(format!(
                "request carries no {FORWARDED_CLIENT_CERT_HEADER} header"
            ));
        }
        parse_xfcc_chain(header)?
    };
    verify_actor_certificate(&chain, roots)
}
