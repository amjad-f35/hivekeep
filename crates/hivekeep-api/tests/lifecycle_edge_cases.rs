//! Edge cases: crashes, deletes, reverts, tags, templates,
//! workers, reconciliation, metrics, event records and startup checks.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use hivekeep_api::authz::SystemPrincipals;
use hivekeep_api::metrics::{KnownPool, OpLabels};
use hivekeep_api::template_reconciler::TemplateReconciler;
use hivekeep_api::worker_workflow::WorkerWorkflow;
use hivekeep_core::attr;
use hivekeep_core::resource::{ActorTemplateRef, TagRef};
use hivekeep_proto::api::v1 as pb;
use hivekeep_store::{
    ActorStore, ActorTemplateStore, DeletePreconditions, Precondition, StoreError, TagStore,
    WorkerAssignmentStore, WorkerStore,
};
use pb::SnapshotStorageState::{Completed, Pending};
use tonic::{Code, Status};

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

// ---------- authz / startup ----------

#[test]
fn new_rejects_invalid_system_principals() {
    let ok = SystemPrincipals {
        controller: "spiffe://cluster.local/ns/hk/sa/controller".into(),
        egress_gateway: "spiffe://cluster.local/ns/hk/sa/egress".into(),
        ingress_router: "spiffe://cluster.local/ns/hk/sa/router".into(),
        keeper: "spiffe://cluster.local/ns/hk/sa/keeper".into(),
        api_server_sidecar: "spiffe://cluster.local/ns/hk/sa/api".into(),
    };
    ok.validate().unwrap();
    type Mutation = fn(&mut SystemPrincipals);
    let cases: [(Mutation, &str); 7] = [
        (|p| p.controller.clear(), "invalid controller SPIFFE ID"),
        (
            |p| p.egress_gateway.clear(),
            "invalid egress_gateway SPIFFE ID",
        ),
        (
            |p| p.ingress_router.clear(),
            "invalid ingress_router SPIFFE ID",
        ),
        (|p| p.keeper.clear(), "invalid keeper SPIFFE ID"),
        (
            |p| p.api_server_sidecar.clear(),
            "invalid api_server_sidecar SPIFFE ID",
        ),
        (
            |p| p.controller = "hivekeep-controller".into(),
            "invalid controller SPIFFE ID",
        ),
        (
            |p| p.egress_gateway = "https://cluster.local/ns/hk/sa/egress".into(),
            "invalid egress_gateway SPIFFE ID",
        ),
    ];
    for (mutate, want) in cases {
        let mut p = ok.clone();
        mutate(&mut p);
        let e = p.validate().unwrap_err();
        assert!(e.contains(want), "{e}");
    }
}

#[test]
fn connect_store_requires_postgres_read_write_connection_string() {
    let e = hivekeep_api::server::pg_config("", "", "", "", "hivekeep", 0).unwrap_err();
    assert!(e.contains("--postgres-read-write-connection-string is required"));
    let c = hivekeep_api::server::pg_config("postgres://x", "", "rw", "", "hivekeep", 5).unwrap();
    assert_eq!(
        (c.owner_url, c.read_write_role.as_deref(), c.max_connections),
        (None, Some("rw"), Some(5))
    );
}

fn write_bundle(dir: &std::path::Path) -> (String, String) {
    use hivekeep_identity::ca::{Ca, CaPool, CertificateAuthority, LeafSpec};
    use hivekeep_identity::key::{KeyAlgorithm, PrivateKey};
    let ca = Ca::generate("srv", KeyAlgorithm::EcdsaP256, Duration::from_secs(86400)).unwrap();
    let root = ca.root_pem();
    let pool = CaPool::new(vec![ca], "");
    let key = PrivateKey::generate(KeyAlgorithm::EcdsaP256).unwrap();
    let now = std::time::SystemTime::now();
    let mut spec = LeafSpec::new(
        now - Duration::from_secs(60),
        now + Duration::from_secs(3600),
    );
    spec.dns_names.push("hivekeep-api".into());
    spec.server_auth = true;
    let chain = pool.sign(&spec, &key.public_key()).unwrap();
    let bundle = format!(
        "{}{}",
        key.to_pem(),
        hivekeep_identity::pem::encode_certificates(chain.iter().map(Vec::as_slice))
    );
    let b = dir.join("server.pem");
    std::fs::write(&b, bundle).unwrap();
    let c = dir.join("ca.pem");
    std::fs::write(&c, root).unwrap();
    (b.to_string_lossy().into(), c.to_string_lossy().into())
}

#[test]
fn build_server_tls_config_without_ca_certs_allows_certless_clients() {
    let dir = tempfile::tempdir().unwrap();
    let (bundle, _) = write_bundle(dir.path());
    hivekeep_api::server::server_tls(&bundle, "").unwrap();
}

#[test]
fn build_server_tls_config_rejects_unreadable_ca_certs() {
    let dir = tempfile::tempdir().unwrap();
    let (bundle, ca) = write_bundle(dir.path());
    hivekeep_api::server::server_tls(&bundle, &ca).unwrap();
    let e =
        hivekeep_api::server::server_tls(&bundle, dir.path().join("missing.pem").to_str().unwrap())
            .unwrap_err();
    assert!(e.contains("load pod-identity CA"), "{e}");
}

// ---------- actors ----------

#[tokio::test]
async fn update_actor_template_metadata() {
    let h = Harness::new().await;
    let t = h.create_template().await;
    let r = ActorTemplateRef::new(HIVE, "tmpl1");
    let renamed = h
        .store
        .update_actor_template(&r, Precondition::of(&t), &mut |t| {
            t.metadata.as_mut().unwrap().name = "other".into();
            Ok(())
        })
        .await;
    assert!(
        matches!(renamed, Err(StoreError::ImmutableField(_))),
        "{renamed:?}"
    );
    let updated = h
        .store
        .update_actor_template(&r, Precondition::of(&t), &mut |t| {
            t.metadata.as_mut().unwrap().version = 99;
            t.status = Some(Default::default());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        updated.metadata.as_ref().unwrap().version,
        t.metadata.as_ref().unwrap().version + 1,
        "server metadata is recomputed"
    );
}

#[test]
fn validate_actor_status_crash() {
    use hivekeep_core::field::Path;
    use hivekeep_core::validation::{api as v, Operation};
    let base = pb::Actor {
        metadata: Some(pb::ResourceMetadata {
            hive: "h".into(),
            name: "a".into(),
            uid: uuid::Uuid::new_v4().to_string(),
            version: 1,
            ..Default::default()
        }),
        actor_template: Some(oref("h", "t")),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Crashed as i32,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut ok = base.clone();
    ok.status.as_mut().unwrap().crash = Some(pb::ActorCrash {
        message: "x".repeat(4096),
        crash_time: None,
    });
    assert!(
        v::validate_actor_update(&Operation::update(), &Path::from("actor"), &ok, &base, true)
            .is_empty()
    );
    let mut long = base.clone();
    long.status.as_mut().unwrap().crash = Some(pb::ActorCrash {
        message: "x".repeat(4097),
        crash_time: None,
    });
    assert!(!v::validate_actor_update(
        &Operation::update(),
        &Path::from("actor"),
        &long,
        &base,
        true
    )
    .is_empty());
}

#[tokio::test]
async fn update_actor_delete_recreate_race() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_actor",
        hook(async move {
            let old = mem.get_actor(&ar("a1")).await.unwrap();
            mem.delete_actor(&ar("a1"), DeletePreconditions::none())
                .await
                .unwrap();
            let mut fresh = old.clone();
            fresh.metadata.as_mut().unwrap().uid.clear();
            mem.create_actor(&fresh).await.unwrap();
        }),
    );
    let mut u = a.clone();
    u.worker_selector = Some(pb::Selector {
        match_labels: [("k".to_string(), "v".to_string())].into(),
    });
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest { actor: Some(u) }))
            .await,
        Code::Aborted,
        "conflict",
    );
    assert!(
        h.get_actor("a1").await.worker_selector.is_none(),
        "not applied to the new incarnation"
    );
}

#[tokio::test]
async fn update_actor_concurrent_disjoint_updates() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_actor",
        hook(async move {
            let cur = mem.get_actor(&ar("a1")).await.unwrap();
            mem.update_actor(&ar("a1"), Precondition::of(&cur), &mut |a| {
                a.status.as_mut().unwrap().assigned_node = "elsewhere".into();
                Ok(())
            })
            .await
            .unwrap();
        }),
    );
    let mut u = a.clone();
    u.worker_selector = Some(pb::Selector {
        match_labels: [("k".to_string(), "v".to_string())].into(),
    });
    assert_code(
        h.c()
            .update_actor(h.req(pb::UpdateActorRequest { actor: Some(u) }))
            .await,
        Code::Aborted,
        "conflict",
    );
}

#[tokio::test]
async fn create_actor_does_not_inherit_golden_egress_policy() {
    let h = Harness::with(Options {
        gates: hivekeep_core::preview::PreviewGates::all(),
        preview_client: true,
        ..Default::default()
    })
    .await;
    h.c()
        .create_actor_template(h.req(pb::CreateActorTemplateRequest {
            actor_template: Some(pb::ActorTemplate {
                metadata: Some(md(HIVE, "tmpl1")),
                containers: vec![container()],
                resources: Some(pb::ResourceRequirements {
                    limits: Some(pb::Resources {
                        cpu_millicores: 1000,
                        memory_bytes: 512 << 20,
                    }),
                }),
                golden_egress_policy: Some(pb::EgressPolicyTemplate {
                    rules: vec![pb::EgressRule {
                        https: Some(pb::HttpsRule {
                            hostnames: vec!["x.example".into()],
                            ports: None,
                            effects: None,
                        }),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            }),
        }))
        .await
        .unwrap();
    h.create_actor("a1", "tmpl1").await;
    assert!(matches!(
        h.store.get_egress_policy(&ar("a1")).await,
        Err(StoreError::NotFound)
    ));
}

#[tokio::test]
async fn create_actor_template_per_template_snapshot_location_preview() {
    for (gate, opt_in, location, want) in [
        (false, false, "gs://my-bucket/snapshots", Code::Ok),
        (false, true, "gs://my-bucket/snapshots", Code::Ok),
        (
            true,
            false,
            "gs://my-bucket/snapshots",
            Code::InvalidArgument,
        ),
        (true, false, "", Code::Ok),
        (true, true, "gs://my-bucket/snapshots", Code::Ok),
        (true, true, "", Code::Ok),
    ] {
        let gates = if gate {
            hivekeep_core::preview::PreviewGates::none()
                .with(hivekeep_core::preview::Gate::PerTemplateSnapshotLocation)
        } else {
            hivekeep_core::preview::PreviewGates::none()
        };
        let h = Harness::with(Options {
            gates,
            preview_client: opt_in,
            ..Default::default()
        })
        .await;
        let r = h
            .c()
            .create_actor_template(h.req(pb::CreateActorTemplateRequest {
                actor_template: Some(pb::ActorTemplate {
                    metadata: Some(md(HIVE, "t")),
                    containers: vec![container()],
                    resources: Some(pb::ResourceRequirements {
                        limits: Some(pb::Resources {
                            cpu_millicores: 1000,
                            memory_bytes: 1 << 30,
                        }),
                    }),
                    snapshot_config: Some(pb::SnapshotConfig {
                        storage_location: location.into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }))
            .await;
        let got = r.err().map(|e| e.code()).unwrap_or(Code::Ok);
        assert_eq!(
            got, want,
            "gate={gate} opt_in={opt_in} location={location:?}"
        );
    }
}

#[tokio::test]
async fn crash_actor_release_failure_leaves_worker_reclaimable() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let uid = a.metadata.as_ref().unwrap().uid.clone();
    h.faults.fail_next("release_actor_from_worker", || {
        StoreError::Unavailable("db down".into())
    });
    let e = hivekeep_api::workflow::crash::crash_actor(
        h.store.as_ref(),
        &h.metrics,
        &ar("a1"),
        "resume",
        "boom",
    )
    .await
    .unwrap_err();
    assert_eq!(e.code(), Code::Unavailable);
    let a = h.get_actor("a1").await;
    assert_eq!(
        state(&a),
        pb::ActorState::Running,
        "not crashed while the worker is still held"
    );
    assert!(a.status.unwrap().worker_assignment.is_some());
    assert_eq!(h.store.find_worker_hosting_actor(&uid).await.unwrap(), "w1");
    hivekeep_api::workflow::crash::crash_actor(
        h.store.as_ref(),
        &h.metrics,
        &ar("a1"),
        "resume",
        "boom",
    )
    .await
    .unwrap();
    assert!(
        h.store.find_worker_hosting_actor(&uid).await.is_err(),
        "the retry reclaims the worker"
    );
}

#[test]
fn crash_actor_record_and_counter_agree() {
    let (_, events) = logs::capture(async {
        let h = Harness::new().await;
        h.running_actor("a1").await;
        for _ in 0..2 {
            hivekeep_api::workflow::crash::crash_actor(
                h.store.as_ref(),
                &h.metrics,
                &ar("a1"),
                "resume",
                "boom",
            )
            .await
            .unwrap();
        }
        assert_eq!(h.crash_count(), 1.0);
    });
    let crashes = events
        .iter()
        .filter(|e| {
            e.target == logs::ACTOR_EVENTS && e.message == hivekeep_api::events::CRASHED.body
        })
        .count();
    assert_eq!(crashes, 1, "one record per counted crash");
}

// ---------- deletes ----------

#[tokio::test]
async fn delete_actor_missing_snapshot_bucket() {
    for st in [pb::ActorState::Suspended, pb::ActorState::Deleting] {
        let h = Harness::new().await;
        h.create_template().await;
        let a = h.create_actor("a1", "tmpl1").await;
        let uri = h.actor_uri(&a, "snapshot");
        h.set_actor_status("a1", move |s| {
            s.state = st as i32;
            let state = if st == pb::ActorState::Suspended {
                Completed
            } else {
                Pending
            };
            s.snapshots = vec![actor_snapshot(1, "snapshot", &uri, state, "")];
            s.last_assigned_generation = 1;
        })
        .await;
        let d = h.delete("a1").await.unwrap();
        assert_eq!(d.metadata.unwrap().uid, a.metadata.as_ref().unwrap().uid);
        assert_eq!(
            h.objects.cleanups.lock().len(),
            1,
            "one collection of the owner prefix"
        );
        assert_code(
            h.c()
                .get_actor(h.req(pb::GetActorRequest {
                    actor: Some(oref(HIVE, "a1")),
                }))
                .await,
            Code::NotFound,
            "Actor test-hive/a1 not found",
        );
    }
}

#[tokio::test]
async fn ensure_marked_deleting_all_states() {
    use pb::ActorState as S;
    for st in [
        S::Suspended,
        S::Resuming,
        S::Running,
        S::Suspending,
        S::Crashed,
        S::Reverting,
        S::Deleting,
    ] {
        let h = Harness::new().await;
        h.create_template().await;
        h.create_actor("a1", "tmpl1").await;
        h.set_actor_status("a1", move |s| s.state = st as i32).await;
        h.faults.fail_always("delete_actor", || {
            StoreError::Unavailable("stop here".into())
        });
        assert!(h.delete("a1").await.is_err());
        let marked = h.get_actor("a1").await;
        assert_eq!(state(&marked), S::Deleting, "{st:?}");
        assert!(marked.metadata.as_ref().unwrap().delete_time.is_some());
        assert!(h.delete("a1").await.is_err());
        assert_eq!(
            h.get_actor("a1").await.metadata.unwrap().version,
            marked.metadata.unwrap().version,
            "a second mark writes nothing"
        );
    }
}

#[tokio::test]
async fn delete_actor_collects_in_flight_snapshot_without_template() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let uri = h.actor_uri(&a, "abandoned");
    h.objects.write_snapshot(&uri, &SNAPSHOT_OBJECTS);
    h.set_actor_status("a1", {
        let uri = uri.clone();
        move |s| {
            s.state = pb::ActorState::Crashed as i32;
            s.snapshots = vec![actor_snapshot(1, "abandoned", &uri, Pending, "")];
            s.last_assigned_generation = 1;
        }
    })
    .await;
    h.memory
        .delete_actor_template(
            &ActorTemplateRef::new(HIVE, "tmpl1"),
            DeletePreconditions::none(),
        )
        .await
        .unwrap();
    h.delete("a1").await.unwrap();
    assert!(h.objects.list(&uri).is_empty());
}

#[tokio::test]
async fn ensure_external_snapshots_released_delete_prefix_failure() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let uri = uri_of(latest_durable(&s).unwrap());
    *h.objects.fail_cleanup.lock() = true;
    assert!(h.delete("a1").await.is_err());
    assert!(
        !h.objects.list(&uri).is_empty(),
        "nothing is lost on a failed collection"
    );
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Deleting);
    *h.objects.fail_cleanup.lock() = false;
    h.delete("a1").await.unwrap();
    assert!(h.objects.list(&uri).is_empty());
}

#[tokio::test]
async fn delete_actor_workflow_retry_keeps_admission() {
    let h = Harness::new().await;
    h.create_template().await;
    let a = h.create_actor("a1", "tmpl1").await;
    let uri = h.actor_uri(&a, "abandoned");
    let observed = h
        .set_actor_status("a1", move |s| {
            s.state = pb::ActorState::Crashed as i32;
            s.snapshots = vec![actor_snapshot(1, "abandoned", &uri, Pending, "")];
            s.last_assigned_generation = 1;
        })
        .await;
    let md_ = observed.metadata.clone().unwrap();
    let del = |uid: &str, version: i64| pb::DeleteActorRequest {
        actor: Some(oref(HIVE, "a1")),
        options: Some(pb::DeleteOptions {
            uid: uid.into(),
            version,
        }),
    };
    *h.objects.fail_cleanup.lock() = true;
    assert!(h
        .c()
        .delete_actor(h.req(del(&md_.uid, md_.version)))
        .await
        .is_err());
    let admitted = h.get_actor("a1").await;
    assert!(admitted.metadata.as_ref().unwrap().delete_time.is_some());
    assert_eq!(state(&admitted), pb::ActorState::Deleting);
    assert!(h.c().delete_actor(h.req(del(&md_.uid, 0))).await.is_err());
    assert_eq!(
        h.get_actor("a1").await.metadata.unwrap().version,
        admitted.metadata.as_ref().unwrap().version
    );
    *h.objects.fail_cleanup.lock() = false;
    assert_code(
        h.c().delete_actor(h.req(del(&md_.uid, md_.version))).await,
        Code::Aborted,
        "",
    );
    let deleted = h
        .c()
        .delete_actor(h.req(del(&md_.uid, 0)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        deleted.metadata.unwrap().delete_time,
        admitted.metadata.unwrap().delete_time
    );
    assert_code(h.delete("a1").await, Code::NotFound, "");
}

// ---------- volumes and detaches ----------

fn ext_volumes(names: &[&str]) -> (Vec<pb::Container>, Vec<pb::Volume>) {
    let mut c = container();
    c.volume_mounts = names
        .iter()
        .map(|n| pb::VolumeMount {
            name: n.to_string(),
            mount_path: format!("/{n}"),
        })
        .collect();
    let vols = names
        .iter()
        .map(|n| pb::Volume {
            name: n.to_string(),
            external: Some(pb::ExternalVolumeSource {
                template: Some(pb::ExternalVolumeTemplate {
                    capacity_bytes: 1 << 30,
                    storage_class_name: "standard".into(),
                }),
            }),
            ..Default::default()
        })
        .collect();
    (vec![c], vols)
}

async fn volume_harness(names: &[&str]) -> Harness {
    let h = Harness::preview().await;
    let (c, v) = ext_volumes(names);
    h.create_template_with("tmpl1", "pool1", c, v).await;
    h.create_worker("w1", NODE, "pool1", 2).await;
    h.create_actor("a1", "tmpl1").await;
    h
}

#[tokio::test]
async fn resume_actor_volume_attach_failure_delete_actor() {
    let h = volume_harness(&["data"]).await;
    *h.volumes.fail_attach.lock() = Some(Status::internal("attach failed"));
    assert!(h.resume("a1").await.is_err());
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Resuming);
    h.delete("a1").await.unwrap();
    assert_eq!(h.volumes.deleted.lock().len(), 1);
    h.wait_worker_free("w1").await;
}

#[tokio::test]
async fn resume_actor_multi_volume_partial_attach_failure_retry() {
    let h = volume_harness(&["vol1", "vol2"]).await;
    h.volumes.fail_attach_matching.lock().push("vol2".into());
    assert!(h.resume("a1").await.is_err());
    assert_eq!(
        h.volumes.attached.lock().len(),
        1,
        "vol1 attached before vol2 failed"
    );
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Resuming);
    assert!(
        a.status.unwrap().worker_assignment.is_some(),
        "the worker stays held"
    );
    h.volumes.fail_attach_matching.lock().clear();
    let r = h.resume("a1").await.unwrap();
    assert_eq!(state(r.actor.as_ref().unwrap()), pb::ActorState::Running);
    let attached = h.volumes.attached.lock().clone();
    assert!(
        attached.iter().any(|(id, _)| id.contains("vol2")),
        "vol2 attaches on the retry: {attached:?}"
    );
    assert!(h.ks().run.is_some() || h.ks().restore.is_some());
}

#[tokio::test]
async fn suspend_actor_async_volume_detach_failure_retry_success() {
    let h = volume_harness(&["data"]).await;
    h.resume("a1").await.unwrap();
    *h.volumes.fail_detach.lock() = Some(Status::internal("busy"));
    assert!(h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .is_err());
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Suspending);
    *h.volumes.fail_detach.lock() = None;
    let s = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    assert_eq!(state(&s), pb::ActorState::Suspended);
    assert!(latest_local(&s).is_some());
    h.delete("a1").await.unwrap();
}

#[tokio::test]
async fn resume_actor_restore_crash_delete_detaches() {
    let h = volume_harness(&["data"]).await;
    h.resume("a1").await.unwrap();
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.ks().fail_restore = Some(Status::internal("bad image"));
    assert!(h.resume("a1").await.is_err());
    let a = h.get_actor("a1").await;
    assert_eq!(state(&a), pb::ActorState::Crashed);
    assert_eq!(
        a.status.unwrap().assigned_node,
        NODE,
        "a crash keeps the node on record"
    );
    h.volumes.detached.lock().clear();
    h.delete("a1").await.unwrap();
    assert_eq!(
        h.volumes.detached.lock().len(),
        1,
        "delete unpublishes the volume from that node"
    );
}

#[tokio::test]
async fn suspend_actor_worker_row_gone_still_detaches() {
    let h = volume_harness(&["data"]).await;
    h.resume("a1").await.unwrap();
    h.memory
        .delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    h.volumes.detached.lock().clear();
    let s = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    assert_eq!(h.volumes.detached.lock().as_slice()[0].1, NODE);
    assert!(s.status.unwrap().assigned_node.is_empty());
}

#[tokio::test]
async fn revert_actor_crashed_detaches_assigned_node() {
    let h = volume_harness(&["data"]).await;
    h.resume("a1").await.unwrap();
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Crashed);
    h.volumes.detached.lock().clear();
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
    assert!(r.status.unwrap().assigned_node.is_empty());
    assert_eq!(h.volumes.detached.lock().as_slice()[0].1, NODE);
}

#[tokio::test]
async fn revert_actor_worker_no_longer_hosts_detaches() {
    let h = volume_harness(&["data"]).await;
    let a = h.resume("a1").await.unwrap().actor.unwrap();
    h.store
        .release_actor_from_worker("w1", &a.metadata.unwrap().uid)
        .await
        .unwrap();
    h.volumes.detached.lock().clear();
    let r = h.revert("a1").await.unwrap();
    assert_eq!(
        h.ks().terminate.clone().unwrap().worker_pod_uid,
        "",
        "the workload termination is skipped"
    );
    assert_eq!(h.volumes.detached.lock().len(), 1);
    assert!(r.status.unwrap().assigned_node.is_empty());
}

#[tokio::test]
async fn create_actor_volumes_rejects_empty_plugin_id() {
    struct Empty;
    #[async_trait::async_trait]
    impl hivekeep_api::volumes::VolumePlugin for Empty {
        async fn create_volume(
            &self,
            _: &str,
            _: i64,
            _: &std::collections::HashMap<String, String>,
        ) -> Result<hivekeep_api::volumes::CreatedVolume, Status> {
            Ok(Default::default())
        }
        async fn attach_volume(
            &self,
            _: &str,
            _: &str,
        ) -> Result<std::collections::HashMap<String, String>, Status> {
            Ok(Default::default())
        }
        async fn detach_volume(&self, _: &str, _: &str) -> Result<(), Status> {
            Ok(())
        }
        async fn delete_volume(&self, _: &str) -> Result<(), Status> {
            Ok(())
        }
    }
    let p = hivekeep_api::volumes::StaticVolumes::new();
    p.add_plugin(MOCK_DRIVER, Arc::new(Empty));
    p.add_storage_class(
        "standard",
        hivekeep_api::volumes::StorageClass {
            provisioner: MOCK_DRIVER.into(),
            parameters: Default::default(),
        },
    );
    let (_, vols) = ext_volumes(&["data"]);
    let t = pb::ActorTemplate {
        volumes: vols,
        ..Default::default()
    };
    let init = hivekeep_api::volumes::initial_external_volumes(&p, &t)
        .await
        .unwrap();
    let (out, err) = hivekeep_api::volumes::create_actor_volumes(&p, "uid", &t, &init).await;
    assert!(tonic::Status::from(err.unwrap())
        .message()
        .contains("returned no ID"));
    assert!(
        out[0].storage_volume_id.is_empty(),
        "not recorded as provisioned"
    );
}

#[tokio::test]
async fn detach_from_full_node_reentry_records_after_conflict() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.resume("a1").await.unwrap();
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    h.create_actor("a2", "tmpl1").await;
    h.wait_worker_free("w1").await;
    h.resume("a2").await.unwrap();
    h.dialer.set("node2", h.keeper_addr.clone());
    h.create_worker("w2", "node2", "pool1", 1).await;
    h.faults
        .fail_next("update_actor", || StoreError::VersionConflict);
    assert_code(h.resume("a1").await, Code::Aborted, "");
    assert_eq!(
        h.get_actor("a1").await.status.unwrap().assigned_node,
        NODE,
        "the detach was not recorded"
    );
    h.ks().detach = None;
    let r = h.resume("a1").await.unwrap();
    assert!(h.ks().detach.is_some(), "the retry detaches again");
    assert_eq!(
        r.actor
            .unwrap()
            .status
            .unwrap()
            .worker_assignment
            .unwrap()
            .node_name,
        "node2"
    );
}

// ---------- reverts ----------

#[tokio::test]
async fn revert_actor_paused_snapshot_already_uploaded() {
    let h = Harness::new().await;
    let a = h.running_actor("a1").await;
    let first = h
        .suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let replaced = uri_of(latest_durable(&first).unwrap());
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    let paused = h
        .suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let local = latest_local(&paused).unwrap().clone();
    h.objects.write_snapshot(&uri_of(&local), &SNAPSHOT_OBJECTS);
    let upload = pb::SnapshotReport {
        uuid: local.uuid.clone(),
        generation: local.generation,
        fidelity: local.fidelity,
        object: Some(pb::ObjectSnapshot {
            snapshot_uri: uri_of(&local),
        }),
        ..Default::default()
    };
    h.control
        .workflow()
        .apply_reported_snapshot_state(
            &ar("a1"),
            &a.metadata.as_ref().unwrap().uid,
            NODE,
            &pb::ActorSnapshotState {
                latest_durable: Some(upload),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // Detached: no node, but the latest generation's record is still local?
    let reported = h.get_actor("a1").await;
    assert!(reported.status.as_ref().unwrap().assigned_node.is_empty());
    // Mark it local again so revert is accepted, as when the keeper still
    // holds the copy.
    h.set_actor_status("a1", |s| {
        let g = s.last_assigned_generation;
        s.snapshots
            .iter_mut()
            .find(|x| x.generation == g)
            .unwrap()
            .locality = NODE.into();
    })
    .await;
    h.reset_keeper();
    let r = h.revert("a1").await.unwrap();
    assert_eq!(
        uri_of(latest_durable(&r).unwrap()),
        uri_of(&local),
        "returned to the upload"
    );
    assert!(
        h.objects.list(&replaced).is_empty(),
        "the replaced durable snapshot is collected"
    );
}

#[tokio::test]
async fn revert_actor_missing_template() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.memory
        .delete_actor_template(
            &ActorTemplateRef::new(HIVE, "tmpl1"),
            DeletePreconditions::none(),
        )
        .await
        .unwrap();
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
}

#[tokio::test]
async fn revert_actor_no_snapshot_to_revert_to() {
    let h = Harness::new().await;
    h.create_template().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    h.create_actor("a1", "tmpl1").await;
    h.ks().fail_run = Some(Status::internal("x"));
    let _ = h.resume("a1").await;
    h.ks().fail_run = None;
    let r = h.revert("a1").await.unwrap();
    assert!(r.status.as_ref().unwrap().snapshots.is_empty());
    h.wait_worker_free("w1").await;
    h.resume("a1").await.unwrap();
    assert!(h.ks().run.is_some(), "resumes as a fresh actor");
}

#[tokio::test]
async fn ensure_reverted_finalized_no_object_store() {
    let h = Harness::with(Options {
        no_object_store: true,
        ..Default::default()
    })
    .await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Asynchronous)
        .await
        .unwrap();
    let r = h.revert("a1").await.unwrap();
    assert_eq!(state(&r), pb::ActorState::Suspended);
}

// ---------- tags ----------

async fn tagged_actor(h: &Harness) -> pb::Tag {
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.c()
        .create_tag(h.req(pb::CreateTagRequest {
            tag: Some(pb::Tag {
                metadata: Some(md(HIVE, "t1")),
                source_actor: Some(oref(HIVE, "a1")),
                scope: pb::TagScope::Hive as i32,
                status: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn update_tag() {
    let h = Harness::new().await;
    let t = tagged_actor(&h).await;
    let mut u = t.clone();
    u.scope = pb::TagScope::Published as i32;
    let got = h
        .c()
        .update_tag(h.req(pb::UpdateTagRequest { tag: Some(u) }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(got.scope, pb::TagScope::Published as i32);
    assert_eq!(
        got.metadata.as_ref().unwrap().version,
        t.metadata.as_ref().unwrap().version + 1
    );
}

#[tokio::test]
async fn update_tag_unset_scope_does_not_unpublish() {
    let h = Harness::new().await;
    let t = tagged_actor(&h).await;
    let mut u = t.clone();
    u.scope = 0;
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(u) }))
            .await,
        Code::InvalidArgument,
        "",
    );
    assert_eq!(
        h.store
            .get_tag(&TagRef::new(HIVE, "t1"))
            .await
            .unwrap()
            .scope,
        pb::TagScope::Hive as i32
    );
}

#[tokio::test]
async fn update_tag_delete_recreate_race() {
    let h = Harness::new().await;
    let t = tagged_actor(&h).await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_tag",
        hook(async move {
            let old = mem.get_tag(&TagRef::new(HIVE, "t1")).await.unwrap();
            mem.delete_tag(&TagRef::new(HIVE, "t1"), DeletePreconditions::none())
                .await
                .unwrap();
            let mut fresh = old.clone();
            fresh.metadata.as_mut().unwrap().uid.clear();
            mem.create_tag(&fresh).await.unwrap();
        }),
    );
    let mut u = t.clone();
    u.scope = pb::TagScope::Published as i32;
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(u) }))
            .await,
        Code::Aborted,
        "not found with uid",
    );
}

#[tokio::test]
async fn update_tag_concurrent_update() {
    let h = Harness::new().await;
    let t = tagged_actor(&h).await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_tag",
        hook(async move {
            let cur = mem.get_tag(&TagRef::new(HIVE, "t1")).await.unwrap();
            mem.update_tag(&TagRef::new(HIVE, "t1"), Precondition::of(&cur), &mut |t| {
                t.scope = pb::TagScope::Published as i32;
                Ok(())
            })
            .await
            .unwrap();
        }),
    );
    let mut u = t.clone();
    u.scope = pb::TagScope::Published as i32;
    assert_code(
        h.c()
            .update_tag(h.req(pb::UpdateTagRequest { tag: Some(u) }))
            .await,
        Code::Aborted,
        "",
    );
}

#[tokio::test]
async fn tag_actor_snapshot_actor_repointed_to_another_template() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let built_on = h
        .store
        .get_actor_template(&ActorTemplateRef::new(HIVE, "tmpl1"))
        .await
        .unwrap();
    let other = h
        .create_template_with("tmpl2", "pool1", vec![container()], vec![])
        .await;
    let mut a = h.get_actor("a1").await;
    a.actor_template = Some(oref(HIVE, "tmpl2"));
    h.c()
        .update_actor(h.req(pb::UpdateActorRequest { actor: Some(a) }))
        .await
        .unwrap();
    let t = h
        .c()
        .create_tag(h.req(pb::CreateTagRequest {
            tag: Some(pb::Tag {
                metadata: Some(md(HIVE, "v1")),
                source_actor: Some(oref(HIVE, "a1")),
                scope: pb::TagScope::Hive as i32,
                status: None,
            }),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_ne!(
        built_on.metadata.as_ref().unwrap().uid,
        other.metadata.as_ref().unwrap().uid
    );
    assert_eq!(
        t.status.unwrap().actor_template_uid,
        built_on.metadata.unwrap().uid,
        "the template the snapshot was built under"
    );
}

#[tokio::test]
async fn tag_actor_snapshot_recreate_after_record_failure() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    h.faults
        .fail_next("update_tag", || StoreError::Unavailable("db down".into()));
    let req = pb::CreateTagRequest {
        tag: Some(pb::Tag {
            metadata: Some(md(HIVE, "t1")),
            source_actor: Some(oref(HIVE, "a1")),
            scope: pb::TagScope::Hive as i32,
            status: None,
        }),
    };
    assert!(h.c().create_tag(h.req(req.clone())).await.is_err());
    let left = h.store.get_tag(&TagRef::new(HIVE, "t1")).await.unwrap();
    assert!(
        left.status.unwrap().snapshot.is_none(),
        "no snapshot recorded, nothing copied"
    );
    let before = h.objects.cleanups.lock().len();
    h.c()
        .delete_tag(h.req(pb::DeleteTagRequest {
            tag: Some(oref(HIVE, "t1")),
            options: None,
        }))
        .await
        .unwrap();
    assert_eq!(
        h.objects.cleanups.lock().len(),
        before,
        "deleting it touches no object storage"
    );
    h.c().create_tag(h.req(req)).await.unwrap();
}

#[tokio::test]
async fn tag_actor_snapshot_races_delete() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.suspend("a1", pb::DurabilityMode::Synchronous)
        .await
        .unwrap();
    let control = h.control.clone();
    let result = Arc::new(parking_lot::Mutex::new(None));
    let r2 = result.clone();
    h.faults.before(
        "update_tag",
        hook(async move {
            use hivekeep_proto::api::v1::control_server::Control;
            let req = hivekeep_api::context::request_as(
                pb::DeleteTagRequest {
                    tag: Some(oref(HIVE, "t1")),
                    options: None,
                },
                Some(hivekeep_api::context::Principal::jwt("tester", ISSUER)),
                None,
            );
            *r2.lock() = Some(Control::delete_tag(&control, req).await.map(|_| ()));
        }),
    );
    h.c()
        .create_tag(h.req(pb::CreateTagRequest {
            tag: Some(pb::Tag {
                metadata: Some(md(HIVE, "t1")),
                source_actor: Some(oref(HIVE, "a1")),
                scope: pb::TagScope::Hive as i32,
                status: None,
            }),
        }))
        .await
        .unwrap();
    let r = result.lock().take().unwrap();
    assert_eq!(
        r.unwrap_err().code(),
        Code::Aborted,
        "a delete of a tag being created is aborted"
    );
}

// ---------- template reconciler ----------

#[tokio::test]
async fn checkpoint_stale_observation_conflicts() {
    let h = Harness::new().await;
    let t = h.create_template().await;
    let rec = TemplateReconciler::new(h.store.clone(), h.control.clone(), Duration::from_secs(1));
    rec.checkpoint(&t, |s| {
        s.snapshot_time = Some(std::time::SystemTime::now().into())
    })
    .await
    .unwrap();
    let e = rec
        .checkpoint(&t, |s| s.snapshot_time = None)
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Aborted, "the stale observation is rejected");
}

#[tokio::test]
async fn resync_follows_pagination() {
    let h = Harness::new().await;
    for i in 0..105 {
        h.create_template_with(&format!("t{i}"), "pool1", vec![container()], vec![])
            .await;
    }
    let done = h
        .create_template_with("done", "pool1", vec![container()], vec![])
        .await;
    h.record_golden(&done).await;
    let rec = TemplateReconciler::new(h.store.clone(), h.control.clone(), Duration::from_secs(1));
    let refs = rec.actionable_templates().await.unwrap();
    assert_eq!(
        refs.len(),
        105,
        "every page is walked and terminal templates are skipped"
    );
}

fn probed() -> pb::Container {
    let mut c = container();
    c.wakeup_probe = Some(pb::WakeupProbe {
        http_get: Some(pb::HttpGetAction {
            path: "/".into(),
            port: 8080,
        }),
        timeout_seconds: 30,
    });
    c
}

async fn reconcile_until_done(
    h: &Harness,
    rec: &TemplateReconciler,
    r: &ActorTemplateRef,
) -> pb::ActorTemplate {
    for _ in 0..20 {
        let _ = rec.reconcile_one(r).await;
        let t = h.store.get_actor_template(r).await.unwrap();
        if hivekeep_api::template_reconciler::golden_done(
            t.status
                .as_ref()
                .and_then(|s| s.golden_snapshot_status.as_ref()),
        ) {
            return t;
        }
    }
    panic!("never finished");
}

#[tokio::test]
async fn reconcile_one_golden_tag_recovery() {
    for scenario in [
        "copy failure",
        "actor deletion failure",
        "tag deletion failure",
        "incomplete tag",
    ] {
        let h = Harness::new().await;
        let t = h
            .create_template_with("tmpl1", "pool1", vec![probed()], vec![])
            .await;
        let uid = t.metadata.as_ref().unwrap().uid.clone();
        h.create_worker("w1", NODE, "pool1", 2).await;
        let rec =
            TemplateReconciler::new(h.store.clone(), h.control.clone(), Duration::from_secs(1));
        let r = ActorTemplateRef::new(HIVE, "tmpl1");
        match scenario {
            "copy failure" => *h.objects.fail_copy.lock() = true,
            "actor deletion failure" => h.faults.fail_next("delete_actor", || {
                StoreError::Unavailable("storage unavailable".into())
            }),
            "incomplete tag" | "tag deletion failure" => {
                h.store
                    .create_tag(&pb::Tag {
                        metadata: Some(md(HIVE, &uid)),
                        source_actor: Some(oref(HIVE, &uid)),
                        scope: pb::TagScope::Published as i32,
                        status: Some(pb::TagStatus {
                            snapshot: None,
                            actor_template_uid: uid.clone(),
                        }),
                    })
                    .await
                    .unwrap();
                if scenario == "tag deletion failure" {
                    h.faults.fail_next("delete_tag", || {
                        StoreError::Unavailable("storage unavailable".into())
                    });
                }
            }
            _ => unreachable!(),
        }
        // The first failing pass records nothing terminal.
        let first = rec.reconcile_one(&r).await;
        if scenario != "incomplete tag" {
            assert!(first.is_err(), "{scenario}");
            let gs = h
                .store
                .get_actor_template(&r)
                .await
                .unwrap()
                .status
                .unwrap()
                .golden_snapshot_status;
            assert!(
                gs.and_then(|g| g.golden_tag).is_none(),
                "{scenario}: not marked ready before cleanup"
            );
        }
        *h.objects.fail_copy.lock() = false;
        let done = reconcile_until_done(&h, &rec, &r).await;
        assert_eq!(
            done.status
                .unwrap()
                .golden_snapshot_status
                .unwrap()
                .golden_tag,
            Some(oref(HIVE, &uid)),
            "{scenario}"
        );
        assert!(
            h.store.get_actor(&ar(&uid)).await.is_err(),
            "{scenario}: the golden actor is gone"
        );
    }
}

#[tokio::test]
async fn reconcile_one_golden_egress_policy_recovery() {
    for scenario in ["create fails", "lookup fails"] {
        let h = Harness::with(Options {
            gates: hivekeep_core::preview::PreviewGates::all(),
            preview_client: true,
            ..Default::default()
        })
        .await;
        let rules = vec![pb::EgressRule {
            https: Some(pb::HttpsRule {
                hostnames: vec!["api.example.com".into()],
                ports: None,
                effects: None,
            }),
            ..Default::default()
        }];
        h.c()
            .create_actor_template(h.req(pb::CreateActorTemplateRequest {
                actor_template: Some(pb::ActorTemplate {
                    metadata: Some(md(HIVE, "tmpl1")),
                    containers: vec![probed()],
                    worker_selector: Some(pb::Selector {
                        match_labels: [("pool".to_string(), "pool1".to_string())].into(),
                    }),
                    resources: Some(pb::ResourceRequirements {
                        limits: Some(pb::Resources {
                            cpu_millicores: 1000,
                            memory_bytes: 512 << 20,
                        }),
                    }),
                    golden_egress_policy: Some(pb::EgressPolicyTemplate { rules }),
                    ..Default::default()
                }),
            }))
            .await
            .unwrap();
        h.create_worker("w1", NODE, "pool1", 2).await;
        let rec =
            TemplateReconciler::new(h.store.clone(), h.control.clone(), Duration::from_secs(1));
        let r = ActorTemplateRef::new(HIVE, "tmpl1");
        match scenario {
            "create fails" => h.faults.fail_next("create_egress_policy", || {
                StoreError::Unavailable("policy service unavailable".into())
            }),
            _ => {
                // The policy exists (an earlier pass's lost response), then the lookup fails once.
                h.faults.fail_next("get_egress_policy", || {
                    StoreError::Unavailable("policy service unavailable".into())
                });
            }
        }
        if scenario == "lookup fails" {
            let _ = rec.reconcile_one(&r).await; // creates the actor and policy, then resumes
        }
        let first = rec.reconcile_one(&r).await;
        if scenario == "create fails" {
            assert!(first.is_err());
            assert_eq!(h.ks().run, None, "not resumed without its policy");
        }
        let done = reconcile_until_done(&h, &rec, &r).await;
        assert!(
            done.status
                .unwrap()
                .golden_snapshot_status
                .unwrap()
                .golden_tag
                .is_some(),
            "{scenario}"
        );
    }
}

// ---------- workers and reconciliation ----------

#[tokio::test]
async fn worker_assignment_reconciler_releases_worker_raised_before_start() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.register_worker("w1", NODE, 4).await;
    h.wait_for_cache("w1", |w| w.status.as_ref().unwrap().registered_epoch == 2)
        .await;
    let stop = tokio_util::sync::CancellationToken::new();
    hivekeep_api::assignment_reconciler::AssignmentReconciler::new(
        h.store.clone(),
        h.worker_workflow.clone(),
    )
    .start(&h.cache, stop.clone());
    for _ in 0..400 {
        if state(&h.get_actor("a1").await) == pb::ActorState::Crashed {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Crashed);
    stop.cancel();
}

#[tokio::test]
async fn reconcile_assignments_keeps_actor_rebound_since_list() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.register_worker("w1", NODE, 4).await;
    let mem = h.memory.clone();
    h.faults.before(
        "acquire_lease",
        hook(async move {
            let a = mem.get_actor(&ar("a1")).await.unwrap();
            mem.update_actor(&ar("a1"), Precondition::of(&a), &mut |a| {
                a.status
                    .as_mut()
                    .unwrap()
                    .worker_assignment
                    .as_mut()
                    .unwrap()
                    .worker_epoch = 2;
                Ok(())
            })
            .await
            .unwrap();
        }),
    );
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Running);
    assert_eq!(
        h.store
            .list_worker_assignments("w1", &Default::default())
            .await
            .unwrap()
            .items
            .len(),
        1
    );
}

#[tokio::test]
async fn reconcile_assignments_retries_record_after_concurrent_bind() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.register_worker("w1", NODE, 4).await;
    let mem = h.memory.clone();
    let late = ghost_assignment();
    let l2 = late.clone();
    h.faults.before(
        "update_worker",
        hook(async move {
            mem.bind_actor_to_worker("w1", &l2, None).await.unwrap();
        }),
    );
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    let w = h.store.get_worker("w1").await.unwrap();
    assert_eq!(w.status.as_ref().unwrap().observed_epoch, 2);
    assert!(
        h.store
            .get_worker_assignment("w1", &late.actor_uid)
            .await
            .is_ok(),
        "the later bind is kept"
    );
}

fn ghost_assignment() -> pb::ActorAssignment {
    pb::ActorAssignment {
        actor: Some(oref(HIVE, "ghost")),
        actor_uid: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn reconcile_assignments_leaves_replaced_worker() {
    let h = Harness::new().await;
    h.ensure_base().await;
    h.register_worker("w1", NODE, 4).await;
    let mem = h.memory.clone();
    h.faults.before(
        "list_worker_assignments",
        hook(async move {
            let old = mem.get_worker("w1").await.unwrap();
            mem.delete_worker("w1", DeletePreconditions::none())
                .await
                .unwrap();
            let mut fresh = old.clone();
            fresh.metadata.as_mut().unwrap().uid.clear();
            fresh.status.as_mut().unwrap().observed_epoch = 0;
            mem.create_worker(&fresh).await.unwrap();
        }),
    );
    h.worker_workflow.reconcile_assignments("w1").await.unwrap();
    assert_eq!(
        h.store
            .get_worker("w1")
            .await
            .unwrap()
            .status
            .unwrap()
            .observed_epoch,
        0,
        "the new incarnation is untouched"
    );
}

#[tokio::test]
async fn delete_worker_workflow_failed_release_keeps_worker() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.faults
        .fail_next("update_actor", || StoreError::Unavailable("db down".into()));
    assert!(h
        .c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None
        }))
        .await
        .is_err());
    assert!(
        h.store.get_worker("w1").await.is_ok(),
        "the worker stays findable"
    );
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    assert_eq!(state(&h.get_actor("a1").await), pb::ActorState::Crashed);
}

#[tokio::test]
async fn delete_worker_workflow_actor_deleted_during_release() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    let mem = h.memory.clone();
    h.faults.before(
        "update_actor",
        hook(async move {
            mem.delete_actor(&ar("a1"), DeletePreconditions::none())
                .await
                .unwrap();
        }),
    );
    h.c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap();
    assert!(h.store.get_worker("w1").await.is_err());
}

#[tokio::test]
async fn delete_worker_workflow_drained_worker_is_still_admitted() {
    let h = Harness::new().await;
    h.create_worker("w1", NODE, "pool1", 1).await;
    let drained = h
        .c()
        .drain_worker(h.req(pb::DrainWorkerRequest {
            worker: Some(wref("w1")),
        }))
        .await
        .unwrap()
        .into_inner()
        .worker
        .unwrap();
    let d = h
        .c()
        .delete_worker(h.req(pb::DeleteWorkerRequest {
            worker: Some(wref("w1")),
            options: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(d.metadata.as_ref().unwrap().delete_time.is_some());
    assert_eq!(
        d.metadata.unwrap().version,
        drained.metadata.unwrap().version + 1
    );
}

#[tokio::test]
async fn delete_worker_workflow_hung_reclaim_does_not_wedge_delete() {
    let h = Harness::new().await;
    h.running_actor("a1").await;
    h.create_actor("a2", "tmpl1").await;
    h.resume("a2").await.unwrap();
    h.ks().terminate_delay = Some(Duration::from_secs(2));
    let wf = WorkerWorkflow::new(
        h.store.clone(),
        Some(h.dialer.clone()),
        h.metrics.clone(),
        true,
    )
    .with_reclaim_timeout(Duration::from_millis(100));
    let start = std::time::Instant::now();
    wf.delete_worker("w1", DeletePreconditions::none())
        .await
        .unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "one timed-out reclaim, the next skipped"
    );
    assert_eq!(
        h.ks().terminate_calls,
        0,
        "the hung terminate never answered"
    );
    for a in ["a1", "a2"] {
        assert_eq!(state(&h.get_actor(a).await), pb::ActorState::Crashed);
    }
}

// ---------- metrics ----------

#[test]
fn lifecycle_op_attrs_omit_unknown_scope_and_template_before_load() {
    let l = OpLabels::of(None, "", "");
    assert!(
        l.template_name.is_empty()
            && l.template_hive.is_empty()
            && l.pool.is_empty()
            && l.snapshot_fidelity.is_empty()
    );
    let a = pb::Actor {
        actor_template: Some(oref("h", "t")),
        ..Default::default()
    };
    let l = OpLabels::of(Some(&a), "", "");
    assert_eq!(l.template_name, "t");
    assert!(
        l.pool.is_empty() && l.pool_namespace.is_empty(),
        "an unassigned actor names no pool"
    );
}

fn pool_worker(pool: &str, state: pb::WorkerState, class: &str, alloc: i32) -> pb::Worker {
    pb::Worker {
        worker_namespace: "ns".into(),
        worker_pool: pool.into(),
        status: Some(pb::WorkerStatus {
            state: state as i32,
            capacity: Some(pb::WorkerResources {
                actors: 2,
                ..Default::default()
            }),
            allocated: Some(pb::WorkerResources {
                actors: alloc,
                ..Default::default()
            }),
            sandbox_classes: if class.is_empty() {
                vec![]
            } else {
                vec![gvisor_class("v1", "a")]
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn worker_count_metrics() {
    let m = hivekeep_api::metrics::Metrics::new();
    m.set_known_pools(vec![KnownPool {
        namespace: "ns".into(),
        pool: "empty".into(),
        sandbox_class: String::new(),
    }]);
    let ws = [
        pool_worker("p", pb::WorkerState::Active, "gvisor", 0),
        pool_worker("p", pb::WorkerState::Active, "gvisor", 1),
        pool_worker("p", pb::WorkerState::Active, "gvisor", 2),
        pool_worker("p", pb::WorkerState::Unavailable, "", 0),
    ];
    m.observe_workers(ws.iter());
    let text = m.encode();
    let count = |pool: &str, state: &str, class: &str| -> f64 {
        text.lines()
            .filter(|l| l.starts_with("hivekeep_workerpool_workers{"))
            .filter(|l| {
                l.contains(&format!("hk_workerpool_name=\"{pool}\""))
                    && l.contains(&format!("hk_worker_state=\"{state}\""))
                    && l.contains(&format!("hk_sandbox_class=\"{class}\""))
            })
            .filter_map(|l| l.rsplit(' ').next().and_then(|v| v.parse::<f64>().ok()))
            .sum()
    };
    // Seeds zero for known pools.
    for st in hivekeep_api::metrics::WORKER_STATES {
        assert!(text.contains(&format!("hk_sandbox_class=\"gvisor\",hk_worker_state=\"{st}\",hk_workerpool_name=\"empty\",hk_workerpool_namespace=\"ns\"}} 0")), "{st}\n{text}");
    }
    // A worker that has not registered a class is unknown, not the pool's.
    assert_eq!(count("p", "unschedulable", "unknown"), 1.0);
    // Sums to the pool size.
    let total: f64 = hivekeep_api::metrics::WORKER_STATES
        .iter()
        .map(|s| count("p", s, "gvisor") + count("p", s, "unknown"))
        .sum();
    assert_eq!(total, 4.0);
    assert_eq!(
        (
            count("p", "idle", "gvisor"),
            count("p", "partial", "gvisor"),
            count("p", "at_capacity", "gvisor")
        ),
        (1.0, 1.0, 1.0)
    );
}

#[test]
fn worker_count_skips_when_cache_not_ready() {
    rt().block_on(async {
        let m = hivekeep_api::metrics::Metrics::new();
        m.observe_workers([&pool_worker("p", pb::WorkerState::Active, "gvisor", 0)]);
        // Only the worker gauge: the state-change counter is process-wide
        // and other tests move it concurrently.
        let workers = |m: &hivekeep_api::metrics::Metrics| -> String {
            m.encode()
                .lines()
                .filter(|l| l.contains("hivekeep_workerpool_workers"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let before = workers(&m);
        let store: Arc<dyn hivekeep_store::Store> = Arc::new(hivekeep_store::MemoryStore::new());
        let cache = hivekeep_api::worker_cache::WorkerCache::new(store, Duration::from_secs(60));
        m.observe_cache(&cache);
        assert_eq!(workers(&m), before, "the observation is skipped");
    });
}

// ---------- event records ----------

fn actor_events(events: &[logs::Event]) -> Vec<(String, String)> {
    events
        .iter()
        .filter(|e| e.target == logs::ACTOR_EVENTS)
        .map(|e| {
            (
                e.field(attr::ACTOR_OPERATION_NAME_KEY)
                    .unwrap_or("")
                    .to_string(),
                e.field(attr::ACTOR_STATE_KEY).unwrap_or("").to_string(),
            )
        })
        .collect()
}

#[test]
fn actor_state_change_records() {
    let (_, events) = logs::capture(async {
        let h = Harness::new().await;
        h.running_actor("a1").await;
        h.suspend("a1", pb::DurabilityMode::Synchronous)
            .await
            .unwrap();
        h.revert("a1").await.ok();
        h.delete("a1").await.unwrap();
    });
    let got = actor_events(&events);
    let want: Vec<(String, String)> = [
        ("create", "suspended"),
        ("resume", "resuming"),
        ("resume", "running"),
        ("suspend", "suspending"),
        ("suspend", "suspended"),
        ("delete", "deleting"),
        ("delete", "deleted"),
    ]
    .iter()
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect();
    assert_eq!(got, want);
}

#[test]
fn actor_created_and_deleted_records() {
    let (_, events) = logs::capture(async {
        let h = Harness::new().await;
        h.create_template().await;
        let a = h.create_actor("a1", "tmpl1").await;
        h.delete("a1").await.unwrap();
        a
    });
    let recs: Vec<_> = events
        .iter()
        .filter(|e| e.target == logs::ACTOR_EVENTS)
        .collect();
    assert_eq!(
        recs.first().unwrap().field(attr::ACTOR_OPERATION_NAME_KEY),
        Some("create")
    );
    assert_eq!(
        recs.first().unwrap().field(attr::ACTOR_NAME_KEY),
        Some("a1")
    );
    assert_eq!(
        recs.last().unwrap().field(attr::ACTOR_STATE_KEY),
        Some("deleted")
    );
    assert!(!recs
        .last()
        .unwrap()
        .field(attr::ACTOR_UID_KEY)
        .unwrap_or("")
        .is_empty());
}

#[test]
fn actor_state_change_record_skipped_on_conflict() {
    let (_, events) = logs::capture(async {
        let h = Harness::new().await;
        h.running_actor("a1").await;
        h.faults
            .fail_always("update_actor", || StoreError::VersionConflict);
        assert!(h
            .suspend("a1", pb::DurabilityMode::Synchronous)
            .await
            .is_err());
    });
    assert!(
        !actor_events(&events).iter().any(|(op, _)| op == "suspend"),
        "no record for a lost commit"
    );
}

#[test]
fn node_refusal_log_names_credential() {
    let (_, events) = logs::capture(async {
        let h = Harness::new().await;
        let a = h.running_actor("a1").await;
        let uid = a.metadata.unwrap().uid;
        let _ = h
            .ws()
            .mint_worker_actor_jwt(h.keeper_req(
                "node2",
                pb::MintWorkerActorJwtRequest {
                    actor: Some(oref(HIVE, "a1")),
                    actor_uid: uid.clone(),
                    audiences: vec!["x".into()],
                },
            ))
            .await;
        let key = rcgen::KeyPair::generate().unwrap();
        let csr = rcgen::CertificateParams::new(vec![])
            .unwrap()
            .serialize_request(&key)
            .unwrap()
            .der()
            .to_vec();
        let _ = h
            .ws()
            .mint_worker_actor_certificate(h.keeper_req(
                "node2",
                pb::MintWorkerActorCertificateRequest {
                    actor: Some(oref(HIVE, "a1")),
                    actor_uid: uid,
                    certificate_signing_request: csr,
                    worker: Some(wref("w1")),
                },
            ))
            .await;
    });
    let refusals: Vec<_> = events
        .iter()
        .filter(|e| e.message.contains("refusing a worker-for-actor"))
        .map(|e| e.message.clone())
        .collect();
    assert!(refusals.iter().any(|m| m.contains("JWT")), "{refusals:?}");
    assert!(
        refusals.iter().any(|m| m.contains("certificate")),
        "{refusals:?}"
    );
}
