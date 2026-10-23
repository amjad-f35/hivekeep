//! The probe fixture: an ActorTemplate running `hivekeep-e2e-probe`, which
//! reports what an Actor can see and reach.

use hivekeep_proto::api::v1 as pb;

use crate::clients::Clients;
use crate::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use crate::harness::T;
use crate::trustbundle::ensure_egress_trust_bundle;

/// Name of the probe fixture's WorkerPool and ActorTemplate.
pub const PROBE_NAME: &str = "probe";

/// The manifests [`deploy_probe`] deploys.
pub const PROBE_MANIFESTS: FixtureManifests = FixtureManifests {
    pool: "fixtures/probe/probe.yaml.tmpl",
    template: "fixtures/probe/probe-template.yaml.tmpl",
};

/// The allowlisted name the keeper resolves the egress MITM bundle by.
pub const EGRESS_TRUST_BUNDLE_NAME: &str = "egress-mitm.hivekeep.io";
/// The allowlisted name of the WebPKI roots the keeper ships.
pub const SYSTEM_ROOTS_BUNDLE_NAME: &str = "system-roots.hivekeep.io";

/// Adjusts what [`deploy_probe`] installs.
#[derive(Debug, Clone, Default)]
pub struct ProbeOptions {
    /// Bundles projected into the probe's `trust-bundle.pem`.
    pub trust_bundles: Vec<String>,
}

impl ProbeOptions {
    /// Projects the egress trust bundle, ensuring the cluster-scoped bundle
    /// exists first. Only suites that test the bundle should ask: an Actor
    /// whose template names a missing bundle does not start.
    pub fn with_trust_bundle(mut self) -> Self {
        self.trust_bundles.push(EGRESS_TRUST_BUNDLE_NAME.into());
        self
    }

    /// Adds the shipped WebPKI roots to the projected file, as the egress
    /// demo combines them with the egress bundle.
    pub fn with_system_roots(mut self) -> Self {
        self.trust_bundles.push(SYSTEM_ROOTS_BUNDLE_NAME.into());
        self
    }
}

/// Installs the probe fixture for the sandbox class under test, removed
/// when the test ends. Returns its hive (also the pool's namespace) and the
/// golden-snapshotted template.
pub async fn deploy_probe(
    t: &T,
    clients: &Clients,
    bucket: &str,
    name: &str,
    opts: ProbeOptions,
) -> (String, pb::ActorTemplate) {
    if opts
        .trust_bundles
        .iter()
        .any(|b| b == EGRESS_TRUST_BUNDLE_NAME)
    {
        // Every Actor from this template, the golden boot included, fails
        // closed while the bundle is missing.
        ensure_egress_trust_bundle(t, clients).await;
    }
    let (hive, mut templates) = deploy_hive_fixture(
        t,
        clients,
        PROBE_MANIFESTS,
        bucket,
        name,
        &opts.trust_bundles,
        FixtureOptions::default(),
    )
    .await;
    (hive, templates.remove(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{fixture_path, render_str};
    use crate::protojson::decode_actor_templates;
    use crate::sandbox::{hive_template_substitutions, SandboxClass};
    use crate::trustbundle::EGRESS_TRUST_BUNDLE_OBJECT_NAME;

    /// The probe template projects exactly the bundles a suite asks for,
    /// into one file. Strict decoding proves the fragment landed at the
    /// right depth.
    #[test]
    fn probe_template_trust_bundle() {
        let cases: Vec<(&str, ProbeOptions, Vec<&str>)> = vec![
            ("default", ProbeOptions::default(), vec![]),
            (
                "WithTrustBundle",
                ProbeOptions::default().with_trust_bundle(),
                vec!["egress-mitm.hivekeep.io"],
            ),
            (
                "WithSystemRoots",
                ProbeOptions::default().with_system_roots(),
                vec!["system-roots.hivekeep.io"],
            ),
            (
                "both",
                ProbeOptions::default()
                    .with_trust_bundle()
                    .with_system_roots(),
                vec!["egress-mitm.hivekeep.io", "system-roots.hivekeep.io"],
            ),
        ];
        let raw = std::fs::read_to_string(fixture_path(PROBE_MANIFESTS.template)).unwrap();
        for (name, opts, want) in cases {
            let subs = hive_template_substitutions(
                SandboxClass::Gvisor,
                "test-bucket",
                "render",
                &opts.trust_bundles,
            );
            let templates = decode_actor_templates(&render_str(&raw, &subs))
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(templates.len(), 1, "{name}");
            let mut sources = Vec::new();
            for vol in &templates[0].volumes {
                if let Some(si) = &vol.system_info {
                    for ds in &si.data_sources {
                        if let Some(tb) = &ds.trust_bundle {
                            sources.push(tb.clone());
                        }
                    }
                }
            }
            if want.is_empty() {
                assert!(
                    sources.is_empty(),
                    "{name}: trustBundle data sources = {sources:?}, want none"
                );
                continue;
            }
            assert_eq!(
                sources.len(),
                1,
                "{name}: want one source projecting every name into one file"
            );
            assert_eq!(sources[0].names, want, "{name}");
            assert!(
                !sources[0].path.is_empty(),
                "{name}: projection has no path"
            );
        }
        // The egress name must select the bundle the controller publishes.
        assert!(
            EGRESS_TRUST_BUNDLE_OBJECT_NAME.starts_with(&format!("{EGRESS_TRUST_BUNDLE_NAME}:"))
        );
    }
}
