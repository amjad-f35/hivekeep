//! Trust bundles projected into Actors by name.

use crate::error::{IdentityError, Result};
use crate::pem::sanitize_certificate_bundle;

/// Roots of the egress gateway's interception CA.
pub const EGRESS_BUNDLE: &str = "egress-mitm.hivekeep.io";
/// ClusterTrustBundle that publishes [`EGRESS_BUNDLE`].
pub const EGRESS_CLUSTER_TRUST_BUNDLE: &str = "egress-mitm.hivekeep.io:mitm:primary-bundle";
/// The node's system roots.
pub const SYSTEM_ROOTS_BUNDLE: &str = "system-roots.hivekeep.io";

/// Looks up a ClusterTrustBundle's PEM by object name: `Ok(None)` when it
/// does not exist.
pub type ClusterTrustBundleLookup = dyn Fn(&str) -> Result<Option<String>> + Send + Sync;

/// Resolves trust bundle names to PEM.
pub struct TrustBundleSource {
    lookup: Box<ClusterTrustBundleLookup>,
    system_roots: String,
    seed: u64,
}

impl TrustBundleSource {
    /// A source reading ClusterTrustBundles through `lookup`, serving
    /// `system_roots_pem` for [`SYSTEM_ROOTS_BUNDLE`]. `seed` fixes the
    /// order of combined bundles for this consumer.
    pub fn new(
        lookup: impl Fn(&str) -> Result<Option<String>> + Send + Sync + 'static,
        system_roots_pem: impl Into<String>,
        seed: u64,
    ) -> Self {
        TrustBundleSource {
            lookup: Box::new(lookup),
            system_roots: system_roots_pem.into(),
            seed,
        }
    }

    fn raw(&self, name: &str) -> Result<String> {
        match name {
            SYSTEM_ROOTS_BUNDLE => Ok(self.system_roots.clone()),
            EGRESS_BUNDLE => (self.lookup)(EGRESS_CLUSTER_TRUST_BUNDLE)
                .map_err(|e| {
                    IdentityError::TrustBundle(format!(
                        "trust bundle {name:?}: reading ClusterTrustBundle {EGRESS_CLUSTER_TRUST_BUNDLE:?}: {e}"
                    ))
                })?
                .ok_or_else(|| {
                    IdentityError::TrustBundle(format!(
                        "trust bundle {name:?}: ClusterTrustBundle {EGRESS_CLUSTER_TRUST_BUNDLE:?} not found"
                    ))
                }),
            other => Err(IdentityError::TrustBundle(format!("unknown trust bundle {other:?}"))),
        }
    }

    /// The named bundles merged into one deduplicated PEM bundle.
    pub fn combined<S: AsRef<str>>(&self, names: &[S]) -> Result<String> {
        if names.is_empty() {
            return Err(IdentityError::TrustBundle(
                "no trust bundle names given".into(),
            ));
        }
        let mut all = String::new();
        for name in names {
            all.push_str(&self.raw(name.as_ref())?);
            all.push('\n');
        }
        sanitize_certificate_bundle(all.as_bytes(), self.seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pem::{decode_certificates, encode, CERTIFICATE};

    fn source(egress: Option<&'static str>) -> TrustBundleSource {
        let system = format!(
            "{}{}",
            encode(CERTIFICATE, b"sys-1"),
            encode(CERTIFICATE, b"shared")
        );
        TrustBundleSource::new(
            move |name| {
                assert_eq!(name, EGRESS_CLUSTER_TRUST_BUNDLE);
                Ok(egress.map(|e| {
                    format!(
                        "{}{}",
                        encode(CERTIFICATE, e.as_bytes()),
                        encode(CERTIFICATE, b"shared")
                    )
                }))
            },
            system,
            42,
        )
    }

    #[test]
    fn combines_and_dedups() {
        let s = source(Some("mitm"));
        let out = s.combined(&[SYSTEM_ROOTS_BUNDLE, EGRESS_BUNDLE]).unwrap();
        assert_eq!(decode_certificates(out.as_bytes()).unwrap().len(), 3);
        assert_eq!(
            decode_certificates(s.combined(&[EGRESS_BUNDLE]).unwrap().as_bytes())
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn errors() {
        assert!(source(None).combined(&[EGRESS_BUNDLE]).is_err());
        assert!(source(Some("m")).combined(&["nope.hivekeep.io"]).is_err());
        assert!(source(Some("m")).combined::<&str>(&[]).is_err());
    }

    fn cert_pem() -> String {
        let der = rcgen::generate_simple_self_signed(vec!["test".to_string()])
            .unwrap()
            .cert
            .der()
            .to_vec();
        encode(CERTIFICATE, &der)
    }

    fn with_egress(egress: Option<String>, system: String) -> TrustBundleSource {
        TrustBundleSource::new(
            move |name| {
                Ok((name == EGRESS_CLUSTER_TRUST_BUNDLE)
                    .then(|| egress.clone())
                    .flatten())
            },
            system,
            42,
        )
    }

    /// The DER of every certificate, sorted, so bundles compare
    /// independently of the shuffle.
    fn anchors(bundle: &str) -> Vec<Vec<u8>> {
        let mut ders = decode_certificates(bundle.as_bytes()).unwrap();
        ders.sort();
        ders
    }

    #[test]
    fn raw_resolves_allowlisted_name_unsanitized() {
        let raw = format!("garbage\n{}", cert_pem());
        let s = with_egress(Some(raw.clone()), String::new());
        assert_eq!(s.raw(EGRESS_BUNDLE).unwrap(), raw);
    }

    #[test]
    fn raw_unknown_name_fails_naming_it() {
        // Even a bundle the cluster has is refused unless allowlisted.
        let pem = cert_pem();
        let s = TrustBundleSource::new(move |_| Ok(Some(pem.clone())), String::new(), 42);
        let err = s.raw("my-own-bundle").unwrap_err().to_string();
        assert!(
            err.contains(r#"unknown trust bundle "my-own-bundle""#),
            "{err}"
        );
    }

    #[test]
    fn raw_missing_bundle_fails_naming_it() {
        let err = with_egress(None, String::new())
            .raw(EGRESS_BUNDLE)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(EGRESS_CLUSTER_TRUST_BUNDLE) && err.contains("not found"),
            "{err}"
        );
    }

    #[test]
    fn combined_unions_dedups_and_is_stable() {
        let (shared, mitm_only, system_only) = (cert_pem(), cert_pem(), cert_pem());
        let mitm = format!("{shared}{mitm_only}");
        let system = format!("{system_only}{shared}{system_only}");
        let s = with_egress(Some(mitm.clone()), system.clone());

        // Unions and deduplicates across bundles.
        let both = s.combined(&[EGRESS_BUNDLE, SYSTEM_ROOTS_BUNDLE]).unwrap();
        assert_eq!(
            anchors(&both),
            anchors(&format!("{shared}{mitm_only}{system_only}"))
        );

        // Stable for the same anchors, whatever the order of names.
        assert_eq!(
            s.combined(&[SYSTEM_ROOTS_BUNDLE, EGRESS_BUNDLE]).unwrap(),
            both
        );
        // And tracks a change to one bundle.
        let rotated = with_egress(Some(cert_pem()), system);
        assert_ne!(
            rotated
                .combined(&[EGRESS_BUNDLE, SYSTEM_ROOTS_BUNDLE])
                .unwrap(),
            both
        );

        // A single bundle.
        assert_eq!(
            anchors(&s.combined(&[SYSTEM_ROOTS_BUNDLE]).unwrap()),
            anchors(&format!("{shared}{system_only}"))
        );

        // Any unresolvable name fails; so do no names.
        let err = s
            .combined(&[EGRESS_BUNDLE, "my-own-bundle"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown trust bundle"), "{err}");
        assert!(s.combined::<&str>(&[]).is_err());
    }
}
