//! Checks that an ActorTemplate's `securityContext.capabilities` is in force
//! inside the sandbox as the kernel reports it, not merely written into the
//! runtime spec: the keeper does not spawn containers itself, so a correct
//! spec and a sandbox that applies it are separate claims. Runs against
//! whichever sandbox class `HK_E2E_SANDBOX_CLASS` selects.

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::actors::create_and_resume_actor;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::env::check_env;
use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::suite::{clients, e2e_test};
use serde::Deserialize;

/// The keeper's default capability set, written out rather than imported
/// so changing the default is a deliberate two-place edit and this suite
/// fails if it drifts silently.
const DEFAULT_CAPABILITIES: &[&str] = &["KILL", "NET_BIND_SERVICE", "AUDIT_WRITE"];

/// The probe's `/capabilities` payload.
#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct CapabilitiesResponse {
    bounding: Vec<String>,
    effective: Vec<String>,
    permitted: Vec<String>,
    inheritable: Vec<String>,
    ambient: Vec<String>,
    no_new_privs: bool,
    error: String,
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_capabilities() {
    e2e_test("TestActorCapabilities", |t| async move {
        let env = check_env(&["HK_E2E_BUCKET_NAME"])
            .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let clients = clients();
        let hive = deploy_fixture(&t, &clients, &env["HK_E2E_BUCKET_NAME"]).await;

        // `want` is the exact bounding set.
        let cases: [(&str, &str, &[&str]); 3] = [
            (
                "no securityContext keeps the default set",
                "caps-default",
                DEFAULT_CAPABILITIES,
            ),
            // Everything default was dropped, and only the named one returned.
            (
                "drop ALL plus add yields exactly the added capability",
                "caps-exact",
                &["NET_BIND_SERVICE"],
            ),
            // The golden bake already proved port 80 binds without
            // NET_BIND_SERVICE; this checks the restored Actor answers with
            // an empty set.
            ("drop ALL still binds port 80", "caps-none", &[]),
        ];
        for (name, template, want) in cases {
            let (clients, hive) = (clients.clone(), hive.clone());
            t.run(name, |t| async move {
                let actor = format!("{template}-actor");
                create_and_resume_actor(&t, &clients, &hive, &actor, template).await;
                let rc = RouterClient::new(&t, &clients)
                    .await
                    .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")));
                let got = probe_capabilities(&t, &rc, &hive, &actor).await;
                if !got.error.is_empty() {
                    t.fatal(format!(
                        "probe reported an error reading its capabilities: {}",
                        got.error
                    ));
                }
                // Bounding is the ceiling no set can exceed: asserting it
                // exactly proves a dropped capability is gone, not idle.
                assert_same_capabilities(&t, "bounding", &got.bounding, want);
                assert_same_capabilities(&t, "effective", &got.effective, want);
                assert_same_capabilities(&t, "permitted", &got.permitted, want);
                // Inheritable would let a process that drops to an
                // unprivileged uid regain a capability on exec; ambient is
                // unsupported. Both must reach the guest empty.
                if !got.inheritable.is_empty() {
                    t.error(format!("inheritable = {:?}, want empty", got.inheritable));
                }
                if !got.ambient.is_empty() {
                    t.error(format!(
                        "ambient = {:?}, want empty (ambient capabilities are not supported)",
                        got.ambient
                    ));
                }
                // Neither runtime sets no_new_privileges.
                if got.no_new_privs {
                    t.error("no_new_privs = 1, want 0");
                }
            })
            .await;
        }
    });
}

/// Compares two capability sets irrespective of order.
fn assert_same_capabilities(t: &T, set: &str, got: &[String], want: &[&str]) {
    let mut g: Vec<&str> = got.iter().map(String::as_str).collect();
    let mut w = want.to_vec();
    g.sort_unstable();
    w.sort_unstable();
    if g != w {
        t.error(format!("{set} capability set = {g:?}, want {w:?}"));
    }
}

/// Installs the fixture for the sandbox class under test and returns its
/// hive. Every template is golden-snapshotted on return, so a template whose
/// container cannot start (a needed capability dropped, say) fails here with
/// its error rather than timing out per case.
async fn deploy_fixture(t: &T, clients: &Clients, bucket: &str) -> String {
    let manifests = FixtureManifests {
        pool: "fixtures/capabilities/capabilities.yaml.tmpl",
        template: "fixtures/capabilities/capabilities-templates.yaml.tmpl",
    };
    deploy_hive_fixture(
        t,
        clients,
        manifests,
        bucket,
        "capabilities",
        &[],
        FixtureOptions::default(),
    )
    .await
    .0
}

async fn probe_capabilities(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    id: &str,
) -> CapabilitiesResponse {
    let resp = rc
        .get(&ActorRef::new(hive, id), "/capabilities")
        .await
        .unwrap_or_else(|e| t.fatal(format!("GET /capabilities for {id:?}: {e}")));
    if resp.status != 200 {
        t.fatal(format!(
            "GET /capabilities for {id:?}: status {}, body {:?}",
            resp.status,
            resp.text()
        ));
    }
    serde_json::from_slice(&resp.body)
        .unwrap_or_else(|e| t.fatal(format!("decoding /capabilities for {id:?}: {e}")))
}
