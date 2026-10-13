//! Trust bundles actors may project into system-info volumes, by
//! well-known name.

use std::io::BufReader;
use std::sync::Arc;

use base64::Engine;

/// The egress gateway's CA bundle: the anchors of the per-name leaves the
/// egress gateway mints.
pub const EGRESS_NAME: &str = "egress-mitm.hivekeep.io";
/// The cluster trust bundle object read for [`EGRESS_NAME`].
pub const EGRESS_CTB: &str = "egress-mitm.hivekeep.io:mitm:primary-bundle";
/// A selection of WebPKI roots.
pub const SYSTEM_ROOTS_NAME: &str = "system-roots.hivekeep.io";

/// The allowlisted bundle a cluster trust bundle object backs, if any.
pub fn bundle_backed_by(object_name: &str) -> Option<&'static str> {
    match object_name {
        EGRESS_CTB => Some(EGRESS_NAME),
        _ => None,
    }
}

/// Reads cluster trust bundle objects by name; `Ok(None)` when absent.
pub type ClusterTrustBundles = Arc<dyn Fn(&str) -> Result<Option<String>, String> + Send + Sync>;

/// Resolves allowlisted bundle names to their contents.
#[derive(Clone)]
pub struct Source {
    get_ctb: ClusterTrustBundles,
    system_roots: Vec<u8>,
    shuffle_seed: u64,
}

impl Source {
    /// A source reading cluster bundles with `get_ctb`, with
    /// `system_roots_pem` as the WebPKI roots.
    pub fn new(get_ctb: ClusterTrustBundles, system_roots_pem: Vec<u8>) -> Self {
        Source {
            get_ctb,
            system_roots: system_roots_pem,
            shuffle_seed: crate::rand::u64(),
        }
    }

    fn raw(&self, name: &str) -> Result<Vec<u8>, String> {
        match name {
            SYSTEM_ROOTS_NAME => Ok(self.system_roots.clone()),
            EGRESS_NAME => match (self.get_ctb)(EGRESS_CTB) {
                Ok(Some(b)) => Ok(b.into_bytes()),
                Ok(None) => Err(format!(
                    "trust bundle {name:?}: ClusterTrustBundle {EGRESS_CTB:?} not found"
                )),
                Err(e) => Err(format!(
                    "trust bundle {name:?}: while reading ClusterTrustBundle {EGRESS_CTB:?}: {e}"
                )),
            },
            other => Err(format!("unknown trust bundle {other:?}")),
        }
    }

    /// The sanitized union of the named bundles' anchors, deduplicated.
    pub fn combined<S: AsRef<str>>(&self, names: &[S]) -> Result<Vec<u8>, String> {
        if names.is_empty() {
            return Err("no trust bundle names given".into());
        }
        let mut all = Vec::new();
        for n in names {
            all.extend(self.raw(n.as_ref())?);
            all.push(b'\n');
        }
        sanitize_certificate_bundle(&all, self.shuffle_seed)
    }
}

/// Keeps only the certificate blocks of a PEM bundle, deduplicated and
/// re-encoded, in an order fixed by `seed` but independent of the input's:
/// sorted, then shuffled, so consumers that try anchors in order spread
/// across them.
pub fn sanitize_certificate_bundle(input: &[u8], seed: u64) -> Result<Vec<u8>, String> {
    let mut ders: Vec<Vec<u8>> = Vec::new();
    for c in rustls_pemfile::certs(&mut BufReader::new(input)).flatten() {
        let der = c.as_ref().to_vec();
        if !ders.contains(&der) {
            ders.push(der);
        }
    }
    if ders.is_empty() {
        return Err("bundle contains no CERTIFICATE PEM blocks".into());
    }
    ders.sort();
    crate::rand::Seeded::new(seed).shuffle(&mut ders);
    let mut out = Vec::new();
    for der in ders {
        out.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
        let b64 = base64::engine::general_purpose::STANDARD.encode(&der);
        for chunk in b64.as_bytes().chunks(64) {
            out.extend_from_slice(chunk);
            out.push(b'\n');
        }
        out.extend_from_slice(b"-----END CERTIFICATE-----\n");
    }
    Ok(out)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn pem(body: &[u8]) -> String {
        format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----\n",
            base64::engine::general_purpose::STANDARD.encode(body)
        )
    }

    #[test]
    fn dedups_and_drops_other_blocks() {
        let input = format!(
            "{}{}-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n{}",
            pem(b"one"),
            pem(b"two"),
            pem(b"one")
        );
        let out = sanitize_certificate_bundle(input.as_bytes(), 1).unwrap();
        let back: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(&out[..]))
            .flatten()
            .collect();
        assert_eq!(back.len(), 2);
        assert!(!String::from_utf8(out.clone()).unwrap().contains("PRIVATE"));
        assert_eq!(
            out,
            sanitize_certificate_bundle(input.as_bytes(), 1).unwrap(),
            "stable for a seed"
        );
        assert!(sanitize_certificate_bundle(b"nothing", 1).is_err());
    }

    #[test]
    fn resolves_known_names_only() {
        let ctb = pem(b"egress");
        let src = Source::new(
            Arc::new(move |name: &str| {
                assert_eq!(name, EGRESS_CTB);
                Ok(Some(ctb.clone()))
            }),
            pem(b"roots").into_bytes(),
        );
        let both = src.combined(&[EGRESS_NAME, SYSTEM_ROOTS_NAME]).unwrap();
        assert_eq!(
            rustls_pemfile::certs(&mut BufReader::new(&both[..])).count(),
            2
        );
        assert!(src.combined(&["unknown.example"]).is_err());
        assert!(src.combined::<&str>(&[]).is_err());
        let missing = Source::new(Arc::new(|_: &str| Ok(None)), Vec::new());
        assert!(missing
            .combined(&[EGRESS_NAME])
            .unwrap_err()
            .contains("not found"));
    }
}

#[cfg(test)]
mod behavior_tests;
