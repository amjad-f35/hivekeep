//! Trust-bundle normalization for projection into Actors.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use common::self_signed_der;
use hivekeep_identity::pem::{decode_all, encode, sanitize_certificate_bundle, CERTIFICATE};

/// `der` as a PEM block carrying an RFC 1421 header.
fn with_headers(der: &[u8]) -> String {
    format!(
        "-----BEGIN CERTIFICATE-----\nComment: should be stripped\n\n{}\n-----END CERTIFICATE-----\n",
        STANDARD.encode(der)
    )
}

/// The output as a multiset of header-free re-encoded blocks.
fn block_counts(out: &str) -> BTreeMap<String, usize> {
    assert!(!out.contains("Comment"), "headers survived: {out}");
    let mut counts = BTreeMap::new();
    for b in decode_all(out.as_bytes()).unwrap() {
        *counts.entry(encode(&b.label, &b.der)).or_default() += 1;
    }
    counts
}

#[test]
fn keeps_only_certificates_strips_headers_dedupes() {
    let a = self_signed_der("a");
    let b = self_signed_der("b");
    let junk = encode("EC PRIVATE KEY", b"not-a-cert");
    let input = [
        "leading garbage\n".to_string(),
        with_headers(&a),
        junk,
        encode(CERTIFICATE, &b),
        encode(CERTIFICATE, &a),
    ]
    .concat();
    let got = sanitize_certificate_bundle(input.as_bytes(), 1).unwrap();
    let want: BTreeMap<String, usize> =
        [(encode(CERTIFICATE, &a), 1), (encode(CERTIFICATE, &b), 1)].into();
    assert_eq!(block_counts(&got), want);
}

fn four_certs() -> Vec<String> {
    ["a", "b", "c", "d"]
        .map(|cn| encode(CERTIFICATE, &self_signed_der(cn)))
        .to_vec()
}

#[test]
fn output_order_is_a_shuffle_that_varies_with_the_seed() {
    let input = four_certs().concat();
    let orders: BTreeSet<String> = (0..32u64)
        .map(|seed| sanitize_certificate_bundle(input.as_bytes(), seed).unwrap())
        .collect();
    // Four anchors have 24 orderings; 32 seeds on one ordering means the
    // shuffle is gone.
    assert!(orders.len() >= 2, "32 seeds produced a single ordering");
}

#[test]
fn same_certificates_and_seed_yield_the_same_output() {
    let certs = four_certs();
    let want = sanitize_certificate_bundle(certs.concat().as_bytes(), 42).unwrap();
    let reordered = [&certs[3], &certs[1], &certs[0], &certs[2], &certs[1]]
        .map(String::as_str)
        .concat();
    for _ in 0..8 {
        assert_eq!(
            sanitize_certificate_bundle(reordered.as_bytes(), 42).unwrap(),
            want
        );
    }
}

#[test]
fn errors_when_no_certificates_present() {
    for (name, input) in [
        ("empty", Vec::new()),
        (
            "junk only",
            encode("EC PRIVATE KEY", b"not-a-cert").into_bytes(),
        ),
        ("not pem", b"hello".to_vec()),
    ] {
        assert!(
            sanitize_certificate_bundle(&input, 1).is_err(),
            "{name}: no error"
        );
    }
}
