//! An Actor has egress while it boots: the boot-fetch probe fetches an
//! in-cluster origin through the MITM gate before it listens and exits if
//! it cannot, so its template gets a golden snapshot only if the worker
//! tunneled the golden Actor's egress before the wakeup probe and the gate
//! admitted the Actor while it was still resuming. The template's
//! `goldenEgressPolicy` gives the golden Actor its policy before the
//! controller resumes it. An Actor restored from that snapshot must fetch
//! again as itself before its wakeup probe passes, covering the restore
//! path. `goldenEgressPolicy` is a preview feature, so the test skips
//! unless the gate is enabled.

use hivekeep_core::preview::Gate;
use hivekeep_e2e::egresspolicy::{egress_allow_https, ensure_egress_policy};
use hivekeep_e2e::env::check_env;
use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::preview::require_preview;
use hivekeep_e2e::probe::EGRESS_TRUST_BUNDLE_NAME;
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::serverpod::deploy_server_pod;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::trustbundle::ensure_egress_trust_bundle;
use hivekeep_proto::api::v1 as pb;

use crate::common::{origin_spec, remove_actor, wait_for_actor_state};

const BOOT_FETCH_MANIFESTS: FixtureManifests = FixtureManifests {
    pool: "fixtures/probe/probe.yaml.tmpl",
    template: "fixtures/probe/probe-bootfetch-template.yaml.tmpl",
};

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn golden_boot_egress() {
    e2e_test("TestGoldenBootEgress", |t| async move {
        require_preview(&t, Gate::GoldenEgressPolicy);
        let env = check_env(&["HK_E2E_BUCKET_NAME"])
            .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let clients = clients();
        ensure_egress_trust_bundle(&t, &clients).await;
        let origin = deploy_server_pod(&t, &origin_spec("bootfetch-origin")).await;
        let host = origin.svc_name();

        // The deploy returns once every template has its golden snapshot,
        // which is the assertion.
        let opts = FixtureOptions::default()
            .with_template_value("BOOT_FETCH_HOST", &host)
            .with_template_value("BOOT_FETCH_URL", &format!("https://{host}/healthz"));
        let (hive, _) = deploy_hive_fixture(
            &t,
            &clients,
            BOOT_FETCH_MANIFESTS,
            &env["HK_E2E_BUCKET_NAME"],
            "egressmitm-bootfetch",
            &[EGRESS_TRUST_BUNDLE_NAME.to_string()],
            opts,
        )
        .await;

        let (c, h, host) = (clients.clone(), hive.clone(), host.clone());
        t.run("restore", |t| async move {
            let id = "bootfetch-restore";
            let r = pb::ObjectRef {
                hive: h.clone(),
                name: id.into(),
            };
            let actor = pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: h.clone(),
                    name: id.into(),
                    ..Default::default()
                }),
                actor_template: Some(pb::ObjectRef {
                    hive: h.clone(),
                    name: "probe-bootfetch".into(),
                }),
                ..Default::default()
            };
            if let Err(e) = c
                .api()
                .create_actor(pb::CreateActorRequest { actor: Some(actor) })
                .await
            {
                t.fatal(format!("CreateActor {id:?}: {e}"));
            }
            let (cc, rr) = (c.clone(), r.clone());
            t.cleanup(move |t| async move {
                if let Err(e) = remove_actor(&cc, &rr).await {
                    t.log(format!("cleanup: DeleteActor {id:?} failed: {e}"));
                }
            });
            // Before the resume, so the gate never caches a missing policy.
            let fq = format!("{host}.cluster.local");
            ensure_egress_policy(&t, &c, &r, vec![egress_allow_https(&[&host, &fq])]).await;
            if let Err(e) =
                resume_actor_await_capacity(&t, &c, pb::ResumeActorRequest { actor: Some(r) }).await
            {
                t.fatal(format!("ResumeActor {id:?}: {e}"));
            }
            wait_for_actor_state(&t, &c, &h, id, pb::ActorState::Running).await;
        })
        .await;
    });
}
