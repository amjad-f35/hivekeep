//! Per-Actor identity inside the sandbox: what an Actor restored from a
//! shared golden snapshot sees about itself, and how its projected files
//! behave across live refresh and a suspend/resume cycle.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::actors::{self, oref, wait_for_actor_state};
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::env::check_env;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::probe::{deploy_probe, ProbeOptions};
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::suite::e2e_test;
use hivekeep_e2e::trustbundle::{add_egress_ca, same_certificates};
use hivekeep_proto::api::v1 as pb;
use serde::Deserialize;

const BUCKET_ENV: &str = "HK_E2E_BUCKET_NAME";
const PROBE_TEMPLATE: &str = "probe";

/// The probe's `/whoami` reply.
#[derive(Debug, Default, Deserialize)]
struct Whoami {
    #[serde(default)]
    file: String,
    #[serde(default)]
    hive: String,
    #[serde(default)]
    uid: String,
    #[serde(default)]
    trust: String,
    #[serde(default)]
    hostname: String,
    /// The Actor id read through a descriptor the probe opened at startup
    /// and holds across checkpoints: the snapshot carries an open guest
    /// handle on a system-info file, which restore must re-bind to the
    /// regenerated file.
    #[serde(default)]
    held: String,
    /// The probe's file read errors, if any.
    #[serde(default)]
    error: String,
}

/// The regression gate for per-Actor identity: two Actors restored from one
/// golden snapshot must each observe their OWN id, and explicitly not the
/// golden Actor's. One is then suspended and resumed: the keeper regenerates
/// the system-info files in between, so suspend-time guest state (the
/// startup-held descriptor and every inode indexed before) must re-bind to
/// the regenerated files at the same paths. The micro-VM lane enforces that
/// hardest: a path that moved leaves the guest reference faulty, which the
/// held-descriptor assertion catches.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn actor_identity_after_restore_is_own_id_not_golden() {
    e2e_test(
        "TestActorIdentity_AfterRestore_IsOwnID_NotGolden",
        |t| async move {
            let env = check_env(&[BUCKET_ENV])
                .unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
            let clients = hivekeep_e2e::suite::clients();
            // This run's CA goes in before the fixture deploys, so the
            // assertions know the bundle's exact contents.
            let want_trust = add_egress_ca(&t, &clients, "hivekeep-e2e-probe-trust").await;
            let (hive, tmpl) = deploy_probe(
                &t,
                &clients,
                &env[BUCKET_ENV],
                "identity",
                ProbeOptions::default().with_trust_bundle(),
            )
            .await;
            // The golden Actor is named after the template's UID, so a naming
            // change weakens this check to a no-op rather than false-failing it.
            let golden = tmpl.metadata.map(|m| m.uid).unwrap_or_default();

            let ids = ["probe-alpha", "probe-beta"];
            for id in ids {
                create_and_resume_actor(&t, &clients, &hive, id).await;
            }
            let rc = RouterClient::new(&t, &clients)
                .await
                .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")));

            let mut seen: BTreeMap<String, String> = BTreeMap::new();
            let mut seen_uids: BTreeMap<String, String> = BTreeMap::new();
            for id in ids {
                let got = whoami(&t, &rc, &hive, id).await;
                if got.file != id {
                    t.error(format!("actor {id:?}: /run/hivekeep/actor-id = {:?}, want {id:?} (probe read error: {:?})", got.file, got.error));
                }
                if got.file == golden {
                    t.error(format!("actor {id:?}: identity is the GOLDEN snapshot id {golden:?}; restore leaked shared state"));
                }
                if let Some(other) = seen.get(&got.file) {
                    t.error(format!("actor {id:?} and {other:?} both report identity {:?}; actors are not distinct", got.file));
                }
                seen.insert(got.file.clone(), id.to_string());
                // The descriptor held since before the golden snapshot must
                // survive restore and read the restored Actor's own id.
                if got.held != id {
                    t.error(format!("actor {id:?}: id via startup-held fd = {:?}, want {id:?} (probe read error: {:?})", got.held, got.error));
                }
                if got.hive != hive {
                    t.error(format!("actor {id:?}: /run/hivekeep/hive = {:?}, want {hive:?} (probe read error: {:?})", got.hive, got.error));
                }
                // Exactly the pool's CAs, as published and sanitized.
                if !same_certificates(&got.trust, &want_trust) {
                    t.error(format!(
                    "actor {id:?}: /run/hivekeep/trust-bundle.pem = {:?}, want the certificates in {want_trust:?} (probe read error: {:?})",
                    got.trust, got.error
                ));
                }
                // The projected UID matches the control plane and is distinct
                // per Actor even though both share a golden snapshot.
                let actor = clients
                    .api()
                    .get_actor(pb::GetActorRequest {
                        actor: Some(oref(&hive, id)),
                    })
                    .await
                    .unwrap_or_else(|e| t.fatal(format!("GetActor {id:?}: {e}")))
                    .into_inner();
                let want_uid = actor.metadata.map(|m| m.uid).unwrap_or_default();
                if got.uid != want_uid {
                    t.error(format!("actor {id:?}: /run/hivekeep/actor-uid = {:?}, want {want_uid:?} (probe read error: {:?})", got.uid, got.error));
                }
                if let Some(other) = seen_uids.get(&got.uid) {
                    t.error(format!(
                        "actor {id:?} and {other:?} both report uid {:?}; actors are not distinct",
                        got.uid
                    ));
                }
                seen_uids.insert(got.uid.clone(), id.to_string());
                // Resolved from the /etc/hosts the runtime writes, not DNS:
                // images commonly ship an empty one.
                for (host, want) in [
                    ("localhost".to_string(), "127.0.0.1"),
                    (got.hostname.clone(), "169.254.17.2"),
                ] {
                    match lookup(&rc, &hive, id, &host).await {
                        Ok(addrs) if addrs.iter().any(|a| a == want) => {}
                        other => t.error(format!(
                            "actor {id:?}: lookup {host:?} = {other:?}; want {want}"
                        )),
                    }
                }
            }

            // Live refresh: rotate the pool while both run; each must see the new
            // sanitized contents at the same path.
            let live_trust = add_egress_ca(&t, &clients, "hivekeep-e2e-probe-trust-live").await;
            for id in ids {
                wait_for_trust(&t, &rc, &hive, id, &live_trust, Duration::from_secs(120)).await;
            }

            // A full suspend/resume of one Actor. The bundle is rotated first and
            // the suspend does not wait for the live rewrite: whichever side of
            // the suspend it lands on, the resumed Actor must see it.
            let rotated_trust =
                add_egress_ca(&t, &clients, "hivekeep-e2e-probe-trust-rotated").await;
            let id = ids[0];
            let r = oref(&hive, id);
            // The Actor may edit its /etc files, and the edits outlive a suspend
            // like any other rootfs write.
            let etc_files = ["/etc/hosts", "/etc/resolv.conf"];
            for path in etc_files {
                match probe_file(&rc, &hive, id, "/writefile", path).await {
                    Ok(out) if out.get("error").is_none_or(|e| e.is_empty()) => {}
                    other => t.fatal(format!("actor {id:?}: editing {path}: {other:?}")),
                }
            }
            if let Err(e) = clients
                .api()
                .suspend_actor(pb::SuspendActorRequest {
                    actor: Some(r.clone()),
                    ..Default::default()
                })
                .await
            {
                t.fatal(format!("SuspendActor {id:?}: {e}"));
            }
            wait_for_actor_state(&t, &clients, &hive, id, pb::ActorState::Suspended).await;
            if let Err(e) =
                resume_actor_await_capacity(&t, &clients, pb::ResumeActorRequest { actor: Some(r) })
                    .await
            {
                t.fatal(format!("ResumeActor {id:?} (after suspend): {e}"));
            }
            wait_for_actor_state(&t, &clients, &hive, id, pb::ActorState::Running).await;

            let got = whoami(&t, &rc, &hive, id).await;
            if got.file != id {
                t.error(format!("after suspend/resume: /run/hivekeep/actor-id = {:?}, want {id:?} (probe read error: {:?})", got.file, got.error));
            }
            if got.held != id {
                t.error(format!("after suspend/resume: id via startup-held fd = {:?}, want {id:?} (probe read error: {:?})", got.held, got.error));
            }
            if got.hive != hive {
                t.error(format!("after suspend/resume: /run/hivekeep/hive = {:?}, want {hive:?} (probe read error: {:?})", got.hive, got.error));
            }
            // Against the value recorded before the cycle, so a UID change is
            // not masked by a fresh lookup.
            let want_uid = seen_uids
                .iter()
                .find(|(_, a)| a.as_str() == id)
                .map(|(u, _)| u.clone())
                .unwrap_or_else(|| t.fatal(format!("no UID recorded for actor {id:?}")));
            if got.uid != want_uid {
                t.error(format!("after suspend/resume: /run/hivekeep/actor-uid = {:?}, want {want_uid:?} (probe read error: {:?})", got.uid, got.error));
            }
            for path in etc_files {
                match probe_file(&rc, &hive, id, "/readfile", path).await {
                    Ok(out)
                        if out.get("content").map(String::as_str) == Some("written by probe") => {}
                    other => t.error(format!(
                        "after suspend/resume: {path} = {other:?}; want the actor's edit"
                    )),
                }
            }
            wait_for_trust(&t, &rc, &hive, id, &rotated_trust, Duration::from_secs(10)).await;
            // The other Actor never cycled: the rotation must reach it live,
            // undisturbed by a sibling of the same bundle suspending and resuming.
            wait_for_trust(
                &t,
                &rc,
                &hive,
                ids[1],
                &rotated_trust,
                Duration::from_secs(120),
            )
            .await;
        },
    );
}

fn esc(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Polls until the projected trust bundle equals `want`; live refresh has
/// no completion signal to wait on.
async fn wait_for_trust(
    t: &T,
    rc: &RouterClient,
    hive: &str,
    id: &str,
    want: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    let mut got = Whoami::default();
    let mut last_err = None;
    while Instant::now() < deadline {
        match try_whoami(rc, hive, id).await {
            Ok(w) => {
                if same_certificates(&w.trust, want) {
                    return;
                }
                got = w;
                last_err = None;
            }
            Err(e) => last_err = Some(e),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    t.fatal(format!(
        "actor {id:?}: timed out waiting for the live-refreshed trust bundle: /run/hivekeep/trust-bundle.pem = {:?}, want {want:?} (probe read error: {:?}, poll error: {last_err:?})",
        got.trust, got.error
    ))
}

async fn whoami(t: &T, rc: &RouterClient, hive: &str, id: &str) -> Whoami {
    try_whoami(rc, hive, id)
        .await
        .unwrap_or_else(|e| t.fatal(e))
}

/// Asks the probe to resolve `host` from inside the Actor.
async fn lookup(
    rc: &RouterClient,
    hive: &str,
    id: &str,
    host: &str,
) -> Result<Vec<String>, String> {
    #[derive(Deserialize)]
    struct Lookup {
        #[serde(default)]
        addrs: Vec<String>,
        #[serde(default)]
        error: String,
    }
    let resp = rc
        .get(
            &ActorRef::new(hive, id),
            &format!("/lookup?host={}", esc(host)),
        )
        .await?;
    let out: Lookup =
        serde_json::from_slice(&resp.body).map_err(|e| format!("decoding /lookup: {e}"))?;
    if !out.error.is_empty() {
        return Err(out.error);
    }
    Ok(out.addrs)
}

/// Runs the probe's /readfile or /writefile on `path`.
async fn probe_file(
    rc: &RouterClient,
    hive: &str,
    id: &str,
    op: &str,
    path: &str,
) -> Result<BTreeMap<String, String>, String> {
    let resp = rc
        .get(
            &ActorRef::new(hive, id),
            &format!("{op}?path={}", esc(path)),
        )
        .await?;
    serde_json::from_slice(&resp.body).map_err(|e| format!("decoding {op}: {e}"))
}

async fn try_whoami(rc: &RouterClient, hive: &str, id: &str) -> Result<Whoami, String> {
    let resp = rc
        .get(&ActorRef::new(hive, id), "/whoami")
        .await
        .map_err(|e| format!("GET /whoami for {id:?}: {e}"))?;
    if resp.status != 200 {
        return Err(format!(
            "GET /whoami for {id:?}: status {}, body {:?}",
            resp.status,
            resp.text()
        ));
    }
    serde_json::from_slice(&resp.body).map_err(|e| format!("decoding /whoami for {id:?}: {e}"))
}

/// Creates the probe Actor and resumes it from the golden snapshot (the
/// restore path), without waiting for RUNNING.
async fn create_and_resume_actor(t: &T, clients: &Clients, hive: &str, id: &str) {
    actors::create_and_resume_actor(t, clients, hive, id, PROBE_TEMPLATE).await;
}
