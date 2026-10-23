//! End-to-end checks of Hivekeep's isolation boundaries. Every assertion is
//! negative (something must not be possible) and paired with a positive
//! control showing the probe could have seen the violation: a negative test
//! that stops exercising its boundary keeps passing.
//!
//! This covers the boundary between two hives whose Actors are identical
//! but for the hive they run in. The victim runs, writes state and vacates
//! its worker; the attacker, in the other hive, then reuses that worker and
//! must find none of it. A single-worker pool
//! (fixtures/security/hive-a-pool.yaml.tmpl) forces the reuse, and the test
//! checks that it happened.

use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::actors::{self, get_actor, oref, wait_for_actor_state};
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::env::check_env;
use hivekeep_e2e::fixture::{deploy_hive_fixture, FixtureManifests, FixtureOptions};
use hivekeep_e2e::harness::T;
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::suite::e2e_test;
use hivekeep_proto::api::v1 as pb;
use serde::Deserialize;
use tonic::Code;

const BUCKET_ENV: &str = "HK_E2E_BUCKET_NAME";
/// The ActorTemplate both hives deploy.
const TEMPLATE_NAME: &str = "probe";
/// Where hive-template.yaml.tmpl mounts the durable-dir volume, and that
/// volume's name (the last segment of its mount point on the worker). Keep
/// in step with the manifest.
const DATA_MOUNT: &str = "/data";
const DURABLE_VOLUME: &str = "data";
/// The fixed body the probe's /writefile writes; Actors are told apart by
/// path, not content.
const WRITTEN_BY_PROBE: &str = "written by probe";
/// Per-Actor state on the worker host: `<base>/actors/<uid>/...`, mirroring
/// the node agent's layout (default base dir).
const ACTORS_DIR: &str = "/var/lib/hivekeep/actors";

fn marker_a() -> String {
    format!("{DATA_MOUNT}/hive-a-marker")
}
fn marker_b() -> String {
    format!("{DATA_MOUNT}/hive-b-marker")
}
/// A path no Actor creates: reading it proves /readfile reports a missing
/// file as an error, which every negative assertion relies on.
fn never_written() -> String {
    format!("{DATA_MOUNT}/never-written")
}

/// The probe's /readfile and /writefile response.
#[derive(Debug, Default, Deserialize)]
struct FileResponse {
    #[serde(default)]
    #[allow(dead_code)]
    path: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    ok: String,
    #[serde(default)]
    error: String,
}

struct Hives {
    a: String,
    b: String,
}

/// The hive boundary holds across worker reuse for an Actor's stored data
/// and its snapshots. The victim (hive B) runs, writes its durable marker
/// and is suspended; the attacker (hive A) then starts on the worker the
/// victim left and must find none of its state. The fixture is deployed
/// once: deploying waits for each hive's golden snapshot, which on the
/// micro-VM lane is a cold boot plus a checkpoint.
#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn cross_hive_isolation() {
    e2e_test("TestCrossHiveIsolation", |t| async move {
        let env =
            check_env(&[BUCKET_ENV]).unwrap_or_else(|e| t.fatal(format!("CheckEnv failed: {e}")));
        let clients = hivekeep_e2e::suite::clients();
        let hives = deploy_hives(&t, &clients, &env[BUCKET_ENV]).await;
        let rc = RouterClient::new(&t, &clients)
            .await
            .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")));

        // Victim: UID and worker are recorded while it runs; the attacker
        // probes host paths keyed by that UID and must land on that worker.
        const VICTIM: &str = "victim";
        create_and_resume_actor(&t, &clients, &hives.b, VICTIM).await;
        let victim = ActorRef::new(&hives.b, VICTIM);
        let victim_uid = actor_uid(&t, &clients, &hives.b, VICTIM).await;
        let victim_worker = assigned_worker(&t, &clients, &hives.b, VICTIM).await;
        write_file(&t, &rc, &victim, &marker_b()).await;
        // Control: the victim reads its own marker back, so the attacker's
        // failure to read it below means the boundary held.
        let got = read_file(&t, &rc, &victim, &marker_b()).await;
        if got.content != WRITTEN_BY_PROBE {
            t.fatal(format!(
                "control: victim {victim} reading its OWN {} = {:?} (error {:?}), want {WRITTEN_BY_PROBE:?}: the probe cannot see its own durable data, so nothing below is meaningful",
                marker_b(),
                got.content,
                got.error
            ));
        }

        // Suspend the victim to free the worker. The durable marker stays on
        // the worker; the snapshot goes to object storage.
        if let Err(e) = clients
            .api()
            .suspend_actor(pb::SuspendActorRequest {
                actor: Some(oref(&hives.b, VICTIM)),
                ..Default::default()
            })
            .await
        {
            t.fatal(format!("SuspendActor {}/{VICTIM}: {e}", hives.b));
        }
        wait_for_actor_state(&t, &clients, &hives.b, VICTIM, pb::ActorState::Suspended).await;

        // Attacker: hive A's Actor takes the freed worker.
        const ATTACKER: &str = "attacker";
        create_and_resume_actor(&t, &clients, &hives.a, ATTACKER).await;
        let attacker = ActorRef::new(&hives.a, ATTACKER);
        require_reused_worker(&t, &clients, &hives.a, ATTACKER, &victim_worker).await;

        // Controls for every negative assertion below, fatal: the probe
        // cannot tell a denial from a missing file.
        let got = read_file(&t, &rc, &attacker, &never_written()).await;
        if got.error.is_empty() {
            t.fatal(format!(
                "control: attacker {attacker} read {}, which nothing ever created, and reported no error (content {:?}): /readfile does not report misses as errors, so every negative assertion here would pass vacuously",
                never_written(),
                got.content
            ));
        }
        write_file(&t, &rc, &attacker, &marker_a()).await;
        let got = read_file(&t, &rc, &attacker, &marker_a()).await;
        if got.content != WRITTEN_BY_PROBE {
            t.fatal(format!(
                "control: attacker {attacker} reading its OWN {} = {:?} (error {:?}), want {WRITTEN_BY_PROBE:?}: the probe cannot see its own durable data",
                marker_a(),
                got.content,
                got.error
            ));
        }

        let (rc, hives, attacker, victim_uid) = (&rc, &hives, &attacker, &victim_uid);
        // Durable mount: the attacker's /data must be its own.
        t.run("VictimDurableMarkerNotVisibleThroughAttackerMount", |t| async move {
            let got = read_file(&t, rc, attacker, &marker_b()).await;
            if got.error.is_empty() {
                t.error(format!(
                    "attacker {attacker} read {}, the victim's durable marker in hive {}, through its own mount: content {:?}; durable-dir volumes are shared across the worker-reuse boundary",
                    marker_b(),
                    hives.b,
                    got.content
                ));
            }
        })
        .await;

        // Host state: the sandbox must not expose the host directories the
        // mount is built from.
        t.run("VictimHostStateNotReachableFromAttackerSandbox", |t| async move {
            let marker_file = marker_b().rsplit('/').next().unwrap_or_default().to_string();
            let probes = [
                ("the victim's durable marker on the host", actor_host_path(victim_uid, &["durable-dir", DURABLE_VOLUME, &marker_file])),
                ("the victim's projected actor identity", actor_host_path(victim_uid, &["system-info", "system-info", "actor-id"])),
                ("the victim's sandbox asset record", actor_host_path(victim_uid, &["sandbox-assets.json"])),
            ];
            for (what, path) in probes {
                let got = read_file(&t, rc, attacker, &path).await;
                if got.error.is_empty() {
                    t.error(format!(
                        "attacker {attacker} read {path} ({what}): content {:?}; the sandbox exposes the worker's per-actor host state across the reuse boundary",
                        got.content
                    ));
                }
            }
            // A write is the more serious direction: reaching the victim's
            // durable directory means an Actor can corrupt another hive's data.
            let injected = actor_host_path(victim_uid, &["durable-dir", DURABLE_VOLUME, "injected"]);
            let got = probe_file_op(&t, rc, attacker, "/writefile", &injected).await;
            if got.error.is_empty() {
                t.error(format!(
                    "attacker {attacker} wrote {injected}, inside the victim's durable directory (hive {}) on the worker; an Actor can corrupt another hive's stored data",
                    hives.b
                ));
            }
        })
        .await;

        // Tag scope. Runs last: it suspends the attacker to snapshot it.
        let clients = &clients;
        t.run("TagScopedToHiveCannotSeedAnotherHive", |t| async move {
            tag_scope_boundary(&t, clients, rc, hives, ATTACKER).await
        })
        .await;
    });
}

fn actor_host_path(uid: &str, elems: &[&str]) -> String {
    let mut p = format!("{ACTORS_DIR}/{uid}");
    for e in elems {
        p.push('/');
        p.push_str(e);
    }
    p
}

/// An Actor cannot be seeded from a snapshot taken in another hive: a
/// snapshot carries the source's memory, root filesystem and durable data.
/// The API enforces this through the Tag's scope at CreateActor; the
/// negative case matches the message, not just the code. `source` is a
/// running Actor in hive A holding marker A; it is suspended here to produce
/// the snapshot the tag captures.
async fn tag_scope_boundary(
    t: &T,
    clients: &Clients,
    rc: &RouterClient,
    hives: &Hives,
    source: &str,
) {
    let suspended = clients
        .api()
        .suspend_actor(pb::SuspendActorRequest {
            actor: Some(oref(&hives.a, source)),
            options: Some(pb::SuspendOptions {
                mode: pb::DurabilityMode::Synchronous as i32,
            }),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("SuspendActor {}/{source}: {e}", hives.a)))
        .into_inner();
    let status = suspended.actor.and_then(|a| a.status).unwrap_or_default();
    if durable_snapshot_uri(&status).is_empty() {
        t.fatal(format!("suspended actor {}/{source} has no external snapshot, so there is nothing for a tag to capture", hives.a));
    }

    let tag_ref = oref(&hives.a, "hive-a-tag");
    let mut api = clients.api();
    let tr = tag_ref.clone();
    t.cleanup(move |_| async move {
        let _ = api
            .delete_tag(pb::DeleteTagRequest {
                tag: Some(tr),
                ..Default::default()
            })
            .await;
    });
    // Tags outlive the fixture namespace; clear one an interrupted run left.
    let _ = clients
        .api()
        .delete_tag(pb::DeleteTagRequest {
            tag: Some(tag_ref.clone()),
            ..Default::default()
        })
        .await;
    let tag = clients
        .api()
        .create_tag(pb::CreateTagRequest {
            tag: Some(pb::Tag {
                metadata: Some(pb::ResourceMetadata {
                    hive: hives.a.clone(),
                    name: tag_ref.name.clone(),
                    ..Default::default()
                }),
                scope: pb::TagScope::Hive as i32,
                source_actor: Some(oref(&hives.a, source)),
                ..Default::default()
            }),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("CreateTag {}/{}: {e}", hives.a, tag_ref.name)))
        .into_inner();
    if snapshot_uri(tag.status.as_ref().and_then(|s| s.snapshot.as_ref())).is_empty() {
        t.fatal(format!(
            "tag {}/{} has no snapshot uri, so it could seed an Actor in NO hive and the negative case below would be vacuous",
            hives.a, tag_ref.name
        ));
    }

    // The boundary: the other hive may not name this tag.
    let cross = oref(&hives.b, "cross-hive-clone");
    let mut api = clients.api();
    let c = cross.clone();
    t.cleanup(move |_| async move {
        let _ = api
            .delete_actor(pb::DeleteActorRequest {
                actor: Some(c),
                ..Default::default()
            })
            .await;
    });
    let result = clients
        .api()
        .create_actor(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: hives.b.clone(),
                    name: cross.name.clone(),
                    ..Default::default()
                }),
                actor_template: Some(oref(&hives.b, TEMPLATE_NAME)),
                source_tag: Some(tag_ref.clone()),
                ..Default::default()
            }),
        })
        .await;
    match result {
        Ok(_) => t.error(format!(
            "CreateActor {}/{} seeded from tag {}/{} (scope HIVE) succeeded; an Actor can be seeded from another hive's snapshot",
            hives.b, cross.name, hives.a, tag_ref.name
        )),
        Err(e) if e.code() != Code::FailedPrecondition => {
            t.error(format!("CreateActor {}/{} seeded from tag {}/{}: got {e}, want FailedPrecondition", hives.b, cross.name, hives.a, tag_ref.name))
        }
        Err(e) if !e.message().contains("not published outside its Hive") => t.error(format!(
            "CreateActor {}/{} seeded from tag {}/{} was rejected by a different precondition: {:?}; want the tag-scope rejection",
            hives.b,
            cross.name,
            hives.a,
            tag_ref.name,
            e.message()
        )),
        Err(_) => {}
    }

    // Positive control: the same tag seeds an Actor in its own hive and the
    // clone carries the source's durable data.
    const CLONE: &str = "same-hive-clone";
    let clone_ref = oref(&hives.a, CLONE);
    let mut api = clients.api();
    let c = clone_ref.clone();
    t.cleanup(move |_| async move {
        let _ = api
            .suspend_actor(pb::SuspendActorRequest {
                actor: Some(c.clone()),
                ..Default::default()
            })
            .await;
        let _ = api
            .delete_actor(pb::DeleteActorRequest {
                actor: Some(c),
                ..Default::default()
            })
            .await;
    });
    if let Err(e) = clients
        .api()
        .create_actor(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: hives.a.clone(),
                    name: CLONE.into(),
                    ..Default::default()
                }),
                actor_template: Some(oref(&hives.a, TEMPLATE_NAME)),
                source_tag: Some(tag_ref.clone()),
                ..Default::default()
            }),
        })
        .await
    {
        t.fatal(format!(
            "control: CreateActor {}/{CLONE} from tag {} in its own hive: {e}; the tag is unusable, so the cross-hive rejection above proves nothing",
            hives.a, tag_ref.name
        ));
    }
    if let Err(e) = resume_actor_await_capacity(
        t,
        clients,
        pb::ResumeActorRequest {
            actor: Some(clone_ref.clone()),
        },
    )
    .await
    {
        t.fatal(format!("control: ResumeActor {}/{CLONE}: {e}", hives.a));
    }
    wait_for_actor_state(t, clients, &hives.a, CLONE, pb::ActorState::Running).await;
    let got = read_file(t, rc, &ActorRef::new(&hives.a, CLONE), &marker_a()).await;
    if got.content != WRITTEN_BY_PROBE {
        t.fatal(format!(
            "control: actor {}/{CLONE} restored from tag {} reads {} = {:?} (error {:?}), want {WRITTEN_BY_PROBE:?}; the tag carries no durable data, so the cross-hive rejection above proves nothing",
            hives.a,
            tag_ref.name,
            marker_a(),
            got.content,
            got.error
        ));
    }
}

/// Installs both hives. A goes first: it brings the only WorkerPool, and
/// B's golden snapshot needs a worker to boot on.
async fn deploy_hives(t: &T, clients: &Clients, bucket: &str) -> Hives {
    let (a, _) = deploy_hive_fixture(
        t,
        clients,
        FixtureManifests {
            pool: "fixtures/security/hive-a-pool.yaml.tmpl",
            template: "fixtures/security/hive-template.yaml.tmpl",
        },
        bucket,
        "sec-a",
        &[],
        FixtureOptions::default(),
    )
    .await;
    let (b, _) = deploy_hive_fixture(
        t,
        clients,
        FixtureManifests {
            pool: "fixtures/security/hive-b-namespace.yaml.tmpl",
            template: "fixtures/security/hive-template.yaml.tmpl",
        },
        bucket,
        "sec-b",
        &[],
        FixtureOptions::default(),
    )
    .await;
    if a == b {
        t.fatal(format!(
            "both fixtures rendered to hive {a:?}; the fixture no longer isolates anything"
        ));
    }
    Hives { a, b }
}

/// Fails unless the Actor landed on `prior`. The fixture runs one worker for
/// both hives, so any other worker means the reuse boundary is not
/// exercised.
async fn require_reused_worker(t: &T, clients: &Clients, hive: &str, name: &str, prior: &str) {
    let got = assigned_worker(t, clients, hive, name).await;
    if got != prior {
        t.fatal(format!(
            "actor {hive}/{name} landed on worker {got:?}, not the worker {prior:?} the victim vacated; the fixture deploys one worker for both hives, so the reuse boundary under test is not being exercised"
        ));
    }
    t.log(format!("actor {hive}/{name} reused worker {prior}"));
}

async fn assigned_worker(t: &T, clients: &Clients, hive: &str, name: &str) -> String {
    let actor = get_actor(t, clients, hive, name).await;
    let worker = actor
        .status
        .and_then(|s| s.worker_assignment)
        .and_then(|w| w.worker)
        .map(|w| w.name)
        .unwrap_or_default();
    if worker.is_empty() {
        t.fatal(format!(
            "actor {hive}/{name} has no worker assignment while RUNNING"
        ));
    }
    worker
}

async fn actor_uid(t: &T, clients: &Clients, hive: &str, name: &str) -> String {
    let uid = get_actor(t, clients, hive, name)
        .await
        .metadata
        .map(|m| m.uid)
        .unwrap_or_default();
    if uid.is_empty() {
        t.fatal(format!("actor {hive}/{name} has no uid"));
    }
    uid
}

/// Writes the probe's fixed body at `path`, failing if it did not land, so
/// a later negative assertion cannot pass on missing state.
async fn write_file(t: &T, rc: &RouterClient, actor: &ActorRef, path: &str) {
    let got = probe_file_op(t, rc, actor, "/writefile", path).await;
    if got.ok != "true" {
        t.fatal(format!("actor {actor} writing {path}: {:?}", got.error));
    }
}

async fn read_file(t: &T, rc: &RouterClient, actor: &ActorRef, path: &str) -> FileResponse {
    probe_file_op(t, rc, actor, "/readfile", path).await
}

/// Calls one of the probe's file endpoints. A transport or HTTP failure is
/// fatal: the probe never ran the operation, which is not the same as the
/// Actor being denied.
async fn probe_file_op(
    t: &T,
    rc: &RouterClient,
    actor: &ActorRef,
    endpoint: &str,
    path: &str,
) -> FileResponse {
    let query: String = url::form_urlencoded::byte_serialize(path.as_bytes()).collect();
    let resp = rc
        .get(actor, &format!("{endpoint}?path={query}"))
        .await
        .unwrap_or_else(|e| t.fatal(format!("GET {endpoint} for {actor}: {e}")));
    if resp.status != 200 {
        t.fatal(format!(
            "GET {endpoint} for {actor}: status {}, body {:?}",
            resp.status,
            resp.text()
        ));
    }
    serde_json::from_slice(&resp.body)
        .unwrap_or_else(|e| t.fatal(format!("decoding {endpoint} for {actor}: {e}")))
}

fn durable_snapshot_uri(status: &pb::ActorStatus) -> String {
    status
        .snapshots
        .iter()
        .filter(|s| !snapshot_uri(Some(s)).is_empty())
        .max_by_key(|s| s.generation)
        .map(|s| snapshot_uri(Some(s)))
        .unwrap_or_default()
}

fn snapshot_uri(snap: Option<&pb::Snapshot>) -> String {
    match snap.and_then(|s| s.durable_snapshot.as_ref()) {
        Some(st) if st.state == pb::SnapshotStorageState::Completed as i32 => st
            .object
            .as_ref()
            .map(|o| o.snapshot_uri.clone())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Creates the Actor from the suite's template, resumes it and waits for
/// RUNNING.
async fn create_and_resume_actor(t: &T, clients: &Clients, hive: &str, name: &str) {
    actors::create_and_resume_actor(t, clients, hive, name, TEMPLATE_NAME).await;
    wait_for_actor_state(t, clients, hive, name, pb::ActorState::Running).await;
}
