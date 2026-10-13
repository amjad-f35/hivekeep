//! Resolving allowlisted bundle names and combining their anchors.

use std::io::BufReader;
use std::sync::Arc;

use super::tests::pem;
use super::*;

/// A throwaway certificate block with a unique body.
fn cert() -> String {
    pem(format!("cert-{}", crate::rand::u64()).as_bytes())
}

/// A lister holding `bundles` by object name.
fn ctbs(bundles: &[(&str, String)]) -> ClusterTrustBundles {
    let map: std::collections::HashMap<String, String> = bundles
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(move |name: &str| Ok(map.get(name).cloned()))
}

/// The DER of every certificate block, sorted so bundles compare
/// independent of the sanitizer's shuffle.
fn parse_bundle(b: &[u8]) -> Vec<Vec<u8>> {
    let mut ders: Vec<Vec<u8>> = rustls_pemfile::certs(&mut BufReader::new(b))
        .flatten()
        .map(|c| c.as_ref().to_vec())
        .collect();
    ders.sort();
    ders
}

#[test]
fn raw_resolves_allowlisted_name_unsanitized() {
    let raw = format!("garbage\n{}", cert());
    let s = Source::new(ctbs(&[(EGRESS_CTB, raw.clone())]), Vec::new());
    assert_eq!(s.raw(EGRESS_NAME).unwrap(), raw.into_bytes());
}

#[test]
fn raw_unknown_bundle_name_fails_naming_it() {
    // Present in the cluster, but the allowlist still refuses it.
    let s = Source::new(ctbs(&[("my-own-bundle", cert())]), Vec::new());
    let err = s.raw("my-own-bundle").unwrap_err();
    assert!(
        err.contains(r#"unknown trust bundle "my-own-bundle""#),
        "{err}"
    );
}

#[test]
fn raw_missing_bundle_fails_naming_it() {
    let s = Source::new(ctbs(&[]), Vec::new());
    let err = s.raw(EGRESS_NAME).unwrap_err();
    assert!(
        err.contains(EGRESS_CTB) && err.contains("not found"),
        "{err}"
    );
}

struct Combined {
    s: Source,
    shared: String,
    mitm_only: String,
    roots_only: String,
}

fn combined_fixture() -> Combined {
    let (shared, mitm_only, roots_only) = (cert(), cert(), cert());
    let mitm_raw = format!("{shared}{mitm_only}");
    let roots_raw = format!("{roots_only}{shared}{roots_only}");
    Combined {
        s: Source::new(ctbs(&[(EGRESS_CTB, mitm_raw)]), roots_raw.into_bytes()),
        shared,
        mitm_only,
        roots_only,
    }
}

#[test]
fn combined_unions_and_deduplicates() {
    let f = combined_fixture();
    let got = f.s.combined(&[EGRESS_NAME, SYSTEM_ROOTS_NAME]).unwrap();
    let want = parse_bundle(format!("{}{}{}", f.shared, f.mitm_only, f.roots_only).as_bytes());
    assert_eq!(parse_bundle(&got), want);
}

#[test]
fn combined_is_stable_and_tracks_every_bundle() {
    let f = combined_fixture();
    let names = [EGRESS_NAME, SYSTEM_ROOTS_NAME];
    let first = f.s.combined(&names).unwrap();
    let again = f.s.combined(&[SYSTEM_ROOTS_NAME, EGRESS_NAME]).unwrap();
    assert_eq!(first, again, "same anchors, same output");

    let rotated = Source {
        get_ctb: ctbs(&[(EGRESS_CTB, cert())]),
        system_roots: f.s.system_roots.clone(),
        shuffle_seed: f.s.shuffle_seed,
    };
    assert_ne!(
        first,
        rotated.combined(&names).unwrap(),
        "output ignores a change to one bundle"
    );
}

#[test]
fn combined_single_bundle() {
    let f = combined_fixture();
    let got = f.s.combined(&[SYSTEM_ROOTS_NAME]).unwrap();
    assert_eq!(
        parse_bundle(&got),
        parse_bundle(format!("{}{}", f.shared, f.roots_only).as_bytes())
    );
}

#[test]
fn combined_any_unresolvable_name_fails() {
    let f = combined_fixture();
    let err = f.s.combined(&[EGRESS_NAME, "my-own-bundle"]).unwrap_err();
    assert!(err.contains("unknown trust bundle"), "{err}");
}

#[test]
fn combined_no_names_fails() {
    let f = combined_fixture();
    assert!(f.s.combined::<&str>(&[]).is_err());
}
