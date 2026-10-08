//! Minimal PEM encoding and decoding, plus trust-bundle normalization.

use std::collections::BTreeSet;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;

use crate::error::{IdentityError, Result};

/// PEM label of certificates.
pub const CERTIFICATE: &str = "CERTIFICATE";
/// PEM label of PKCS#8 private keys.
pub const PRIVATE_KEY: &str = "PRIVATE KEY";

/// One PEM block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// The label, e.g. `"CERTIFICATE"`.
    pub label: String,
    /// The decoded contents.
    pub der: Vec<u8>,
}

/// Encodes one block with 64-column base64 lines.
pub fn encode(label: &str, der: &[u8]) -> String {
    let b64 = STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for chunk in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out
}

/// Encodes certificates as consecutive `CERTIFICATE` blocks.
pub fn encode_certificates<'a>(ders: impl IntoIterator<Item = &'a [u8]>) -> String {
    ders.into_iter().map(|d| encode(CERTIFICATE, d)).collect()
}

/// Decodes every block in `input`, ignoring text between blocks.
pub fn decode_all(input: &[u8]) -> Result<Vec<Block>> {
    let text =
        std::str::from_utf8(input).map_err(|_| IdentityError::Pem("input is not UTF-8".into()))?;
    let mut blocks = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("-----BEGIN ") {
        rest = &rest[start + "-----BEGIN ".len()..];
        let end_label = rest
            .find("-----")
            .ok_or_else(|| IdentityError::Pem("unterminated BEGIN line".into()))?;
        let label = rest[..end_label].to_string();
        rest = &rest[end_label + 5..];
        let footer = format!("-----END {label}-----");
        let end = rest
            .find(&footer)
            .ok_or_else(|| IdentityError::Pem(format!("missing END line for {label}")))?;
        let body = strip_headers(&rest[..end]);
        let der = STANDARD
            .decode(body)
            .map_err(|e| IdentityError::Pem(format!("{label} block: {e}")))?;
        blocks.push(Block { label, der });
        rest = &rest[end + footer.len()..];
    }
    Ok(blocks)
}

/// The base64 body of a block with whitespace and any leading RFC 1421
/// headers (`Name: value` lines, optionally ended by a blank line) removed.
/// Base64 never contains `:`, so a header line cannot be mistaken for data.
fn strip_headers(block: &str) -> String {
    let mut in_headers = true;
    let mut body = String::new();
    for line in block.lines() {
        if in_headers {
            let blank = line.trim().is_empty();
            if line.contains(':') || blank {
                continue;
            }
            in_headers = false;
        }
        body.extend(line.chars().filter(|c| !c.is_whitespace()));
    }
    body
}

/// The DER of every `CERTIFICATE` block in `input`.
pub fn decode_certificates(input: &[u8]) -> Result<Vec<Vec<u8>>> {
    Ok(decode_all(input)?
        .into_iter()
        .filter(|b| b.label == CERTIFICATE)
        .map(|b| b.der)
        .collect())
}

/// Normalizes a bundle of trust anchors: keeps only `CERTIFICATE` blocks,
/// drops duplicates, and emits them in an order fixed by `seed`.
///
/// Different consumers get different seeds, so no one consumer's reliance on
/// the first certificate goes unnoticed, while a given consumer's output
/// stays stable across rotations that do not change the set.
pub fn sanitize_certificate_bundle(input: &[u8], seed: u64) -> Result<String> {
    let unique: BTreeSet<Vec<u8>> = decode_certificates(input)?.into_iter().collect();
    if unique.is_empty() {
        return Err(IdentityError::TrustBundle(
            "bundle contains no CERTIFICATE blocks".into(),
        ));
    }
    let mut ders: Vec<Vec<u8>> = unique.into_iter().collect();
    shuffle(&mut ders, seed);
    Ok(encode_certificates(ders.iter().map(Vec::as_slice)))
}

/// Fisher-Yates driven by splitmix64, deterministic for a seed.
fn shuffle<T>(items: &mut [T], seed: u64) {
    let mut state = seed;
    let mut next = || {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    };
    for i in (1..items.len()).rev() {
        let j = (next() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let der: Vec<u8> = (0..200u8).collect();
        let pem = encode(CERTIFICATE, &der);
        assert!(pem.lines().all(|l| l.len() <= 64 || l.starts_with("-----")));
        let blocks = decode_all(format!("junk\n{pem}trailer").as_bytes()).unwrap();
        assert_eq!(
            blocks,
            vec![Block {
                label: CERTIFICATE.into(),
                der
            }]
        );
        assert!(decode_all(b"-----BEGIN X-----\nAAAA\n").is_err());
        assert!(decode_all(b"-----BEGIN X-----\n!!!!\n-----END X-----\n").is_err());
    }

    #[test]
    fn sanitize() {
        let a = encode(CERTIFICATE, b"a");
        let b = encode(CERTIFICATE, b"b");
        let c = encode(CERTIFICATE, b"c");
        let key = encode(PRIVATE_KEY, b"k");
        let input = format!("{a}{b}{key}{a}\n{c}");
        let out = sanitize_certificate_bundle(input.as_bytes(), 7).unwrap();
        let got = decode_certificates(out.as_bytes()).unwrap();
        assert_eq!(got.len(), 3);
        let set: BTreeSet<_> = got.iter().cloned().collect();
        assert_eq!(set, [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()].into());
        // Stable for a seed, whatever the input order.
        let reordered = format!("{c}{a}{b}");
        assert_eq!(
            sanitize_certificate_bundle(reordered.as_bytes(), 7).unwrap(),
            out
        );
        assert!(sanitize_certificate_bundle(key.as_bytes(), 7).is_err());
        assert!(sanitize_certificate_bundle(b"", 7).is_err());
    }

    #[test]
    fn headers_are_skipped() {
        let der: Vec<u8> = (0..100u8).collect();
        let body = STANDARD.encode(&der);
        for input in [
            format!("-----BEGIN CERTIFICATE-----\nComment: x\n\n{body}\n-----END CERTIFICATE-----\n"),
            format!("-----BEGIN CERTIFICATE-----\nProc-Type: 4,ENCRYPTED\nDEK-Info: AES\n\n{body}\n-----END CERTIFICATE-----\n"),
            format!("-----BEGIN CERTIFICATE-----\nComment: x\n{body}\n-----END CERTIFICATE-----\n"),
        ] {
            assert_eq!(decode_certificates(input.as_bytes()).unwrap(), vec![der.clone()], "{input}");
        }
    }
}
