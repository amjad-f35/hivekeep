//! Shared pieces of the demo suite: per-test counter templates, waiting on
//! Actor states, picking snapshots out of an Actor's status, calling the
//! counter through the gate router and checking its counters.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hivekeep_core::preview::Gate;
use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::SnapshotUri;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::namespace::Namespace;
use hivekeep_e2e::retry::{resume_actor_await_capacity, retry_aborted};
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::template::{
    create_counter_template, template_ref, HiveTemplateOptions, TemplateEdit,
};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::Api;
use tonic::Status;

/// The hive the whole suite shares; per-test suffixes keep names unique.
pub const DEMO_HIVE: &str = "demo";

/// The counter binary inside the demo image.
pub const COUNTER_BINARY: &str = "/app/counter";

/// The extra durable-dir volume of [`TemplateKind::TwoDurableDirs`].
pub const SECOND_DURABLE_DIR_VOLUME: &str = "data2";
/// Where the counter container mounts it.
pub const SECOND_DURABLE_DIR_MOUNT_PATH: &str = "/home/counter2";

/// The default wait for an Actor to reach a state.
pub const STATE_WAIT: Duration = Duration::from_secs(60);

/// A reference to `name` in the demo hive.
pub fn demo_ref(name: &str) -> pb::ObjectRef {
    pb::ObjectRef {
        hive: DEMO_HIVE.into(),
        name: name.into(),
    }
}

/// The core reference to `name` in the demo hive.
pub fn demo_actor(name: &str) -> ActorRef {
    ActorRef::new(DEMO_HIVE, name)
}

/// Reads a required environment variable or fails the test.
pub fn require_env(t: &T, key: &str) -> String {
    match hivekeep_e2e::env::check_env(&[key]) {
        Ok(mut m) => m.remove(key).unwrap_or_default(),
        Err(e) => t.fatal(format!("CheckEnv failed: {e}")),
    }
}

/// The shapes of counter template the suite builds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateKind {
    /// The counter as installed.
    Plain,
    /// A second durable dir the counter also writes on every request.
    TwoDurableDirs,
    /// The counter's file counter kept on a provisioned external volume.
    ExternalVolume,
}

async fn create_template_internal(
    t: &T,
    clients: &Clients,
    ns: &Namespace,
    base: &str,
    fidelity: pb::SnapshotFidelity,
    modify: Option<TemplateEdit>,
) -> pb::ActorTemplate {
    let bucket = require_env(t, "HK_E2E_BUCKET_NAME");
    let name = format!("{base}-{}", ns.name);
    create_counter_template(
        t,
        clients,
        &ns.name,
        HiveTemplateOptions {
            hive: DEMO_HIVE.into(),
            name: name.clone(),
            pool_name: base.into(),
            // One Actor at a time, plus a spare for the worker-deletion tests.
            pool_replicas: 2,
            labels: BTreeMap::from([("demo".to_string(), ns.name.clone())]),
            snapshot_config: Some(pb::SnapshotConfig {
                storage_location: format!("gs://{bucket}/hivekeep-demo-{name}"),
                preferred_fidelity: fidelity as i32,
                ..Default::default()
            }),
            modify,
        },
    )
    .await
}

/// Creates a per-test pool and counter template of `kind`.
pub async fn create_actor_template(
    t: &T,
    clients: &Clients,
    ns: &Namespace,
    kind: TemplateKind,
    fidelity: pb::SnapshotFidelity,
) -> pb::ActorTemplate {
    match kind {
        TemplateKind::Plain => {
            create_template_internal(t, clients, ns, "counter", fidelity, None).await
        }
        TemplateKind::TwoDurableDirs => {
            let modify: TemplateEdit = Box::new(|at: &mut pb::ActorTemplate| {
                for c in at.containers.iter_mut().filter(|c| c.name == "counter") {
                    c.command = vec![
                        COUNTER_BINARY.into(),
                        format!("--second-file-counter-directory={SECOND_DURABLE_DIR_MOUNT_PATH}"),
                    ];
                    c.volume_mounts.push(pb::VolumeMount {
                        name: SECOND_DURABLE_DIR_VOLUME.into(),
                        mount_path: SECOND_DURABLE_DIR_MOUNT_PATH.into(),
                    });
                }
                at.volumes.push(pb::Volume {
                    name: SECOND_DURABLE_DIR_VOLUME.into(),
                    durable_dir: Some(pb::DurableDirVolumeSource {}),
                    ..Default::default()
                });
            });
            create_template_internal(
                t,
                clients,
                ns,
                "counter-two-durabledirs",
                fidelity,
                Some(modify),
            )
            .await
        }
        TemplateKind::ExternalVolume => {
            let sc = hivekeep_e2e::settings::storage_class();
            if !has_storage_class(clients, &sc).await {
                t.fatal(format!(
                    "StorageClass {sc:?} not found in cluster; set {} or install the CSI driver",
                    hivekeep_e2e::settings::STORAGE_CLASS_ENV
                ));
            }
            let modify: TemplateEdit = Box::new(move |at: &mut pb::ActorTemplate| {
                for c in at.containers.iter_mut().filter(|c| c.name == "counter") {
                    c.command = vec![
                        COUNTER_BINARY.into(),
                        "--file-counter-directory=/external-data".into(),
                    ];
                    if !c.volume_mounts.iter().any(|vm| vm.name == "external-data") {
                        c.volume_mounts.push(pb::VolumeMount {
                            name: "external-data".into(),
                            mount_path: "/external-data".into(),
                        });
                    }
                }
                if !at.volumes.iter().any(|v| v.name == "external-data") {
                    at.volumes.push(pb::Volume {
                        name: "external-data".into(),
                        external: Some(pb::ExternalVolumeSource {
                            template: Some(pb::ExternalVolumeTemplate {
                                capacity_bytes: 1 << 30,
                                storage_class_name: sc,
                            }),
                        }),
                        ..Default::default()
                    });
                }
            });
            create_template_internal(t, clients, ns, "counter-ext-vol", fidelity, Some(modify))
                .await
        }
    }
}

async fn has_storage_class(clients: &Clients, name: &str) -> bool {
    Api::<StorageClass>::all(clients.kube())
        .get(name)
        .await
        .is_ok()
}

/// Creates the Actor `name` in the demo hive from `at`.
pub async fn create_actor(
    t: &T,
    clients: &Clients,
    name: &str,
    at: &pb::ActorTemplate,
) -> pb::Actor {
    clients
        .api()
        .create_actor(pb::CreateActorRequest {
            actor: Some(pb::Actor {
                metadata: Some(pb::ResourceMetadata {
                    hive: DEMO_HIVE.into(),
                    name: name.into(),
                    ..Default::default()
                }),
                actor_template: Some(template_ref(at)),
                ..Default::default()
            }),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to create Actor {name:?}: {e}")))
        .into_inner()
}

/// Gets `actor`.
pub async fn get_actor(clients: &Clients, actor: &pb::ObjectRef) -> Result<pb::Actor, Status> {
    clients
        .api()
        .get_actor(pb::GetActorRequest {
            actor: Some(actor.clone()),
        })
        .await
        .map(|r| r.into_inner())
}

/// Deletes `actor`, returning the RPC result.
pub async fn delete_actor(clients: &Clients, actor: &pb::ObjectRef) -> Result<pb::Actor, Status> {
    clients
        .api()
        .delete_actor(pb::DeleteActorRequest {
            actor: Some(actor.clone()),
            ..Default::default()
        })
        .await
        .map(|r| r.into_inner())
}

/// Deletes `actor` (best effort, 2 minute budget) when the test ends.
pub fn delete_on_cleanup(t: &T, clients: &Arc<Clients>, actor: pb::ObjectRef) {
    let clients = clients.clone();
    t.cleanup(move |_| async move {
        let _ =
            tokio::time::timeout(Duration::from_secs(120), delete_actor(&clients, &actor)).await;
    });
}

/// Suspends `actor` with `mode`.
pub async fn suspend(
    clients: &Clients,
    actor: &pb::ObjectRef,
    mode: pb::DurabilityMode,
) -> Result<pb::SuspendActorResponse, Status> {
    clients
        .api()
        .suspend_actor(pb::SuspendActorRequest {
            actor: Some(actor.clone()),
            options: Some(pb::SuspendOptions { mode: mode as i32 }),
        })
        .await
        .map(|r| r.into_inner())
}

/// [`suspend`], retried through `Aborted`.
pub async fn suspend_retrying(
    t: &T,
    clients: &Clients,
    actor: &pb::ObjectRef,
    mode: pb::DurabilityMode,
) -> Result<pb::SuspendActorResponse, Status> {
    retry_aborted(t, "SuspendActor", || suspend(clients, actor, mode)).await
}

/// Reverts `actor`.
pub async fn revert(
    clients: &Clients,
    actor: &pb::ObjectRef,
) -> Result<pb::RevertActorResponse, Status> {
    clients
        .api()
        .revert_actor(pb::RevertActorRequest {
            actor: Some(actor.clone()),
        })
        .await
        .map(|r| r.into_inner())
}

/// [`revert`], retried through `Aborted`.
pub async fn revert_retrying(
    t: &T,
    clients: &Clients,
    actor: &pb::ObjectRef,
) -> Result<pb::RevertActorResponse, Status> {
    retry_aborted(t, "RevertActor", || revert(clients, actor)).await
}

/// Resumes `actor`, waiting out a saturated pool.
pub async fn resume(
    t: &T,
    clients: &Clients,
    actor: &pb::ObjectRef,
) -> Result<pb::ResumeActorResponse, Status> {
    resume_actor_await_capacity(
        t,
        clients,
        pb::ResumeActorRequest {
            actor: Some(actor.clone()),
        },
    )
    .await
}

/// The status of `actor`, or an empty one.
pub fn status(actor: &pb::Actor) -> pb::ActorStatus {
    actor.status.clone().unwrap_or_default()
}

/// The state of `st`.
pub fn state(st: &pb::ActorStatus) -> pb::ActorState {
    pb::ActorState::try_from(st.state).unwrap_or(pb::ActorState::Unspecified)
}

/// Waits up to [`STATE_WAIT`] for the demo Actor `name` to reach `want`.
pub async fn wait_for_actor_state(t: &T, clients: &Clients, name: &str, want: pb::ActorState) {
    wait_for_actor_state_with_timeout(t, clients, name, want, STATE_WAIT).await
}

/// Waits up to `timeout` for the demo Actor `name` to reach `want`.
pub async fn wait_for_actor_state_with_timeout(
    t: &T,
    clients: &Clients,
    name: &str,
    want: pb::ActorState,
    timeout: Duration,
) {
    t.log(format!(
        "Waiting for Actor {name:?} to be {}...",
        want.as_str_name()
    ));
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(a) = get_actor(clients, &demo_ref(name)).await {
            if state(&status(&a)) == want {
                t.log(format!(
                    "Actor {name:?} reached state {}",
                    want.as_str_name()
                ));
                return;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    t.fatal(format!(
        "timed out waiting for actor {name:?} to reach state {}",
        want.as_str_name()
    ))
}

/// Waits for the demo Actor `name` to be SUSPENDED while holding a
/// node-local snapshot on the node it is pinned to.
pub async fn wait_for_actor_paused(t: &T, clients: &Clients, name: &str) {
    wait_for_actor_state(t, clients, name, pb::ActorState::Suspended).await;
    let a = get_actor(clients, &demo_ref(name))
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetActor {name:?} after pause: {e}")));
    let st = status(&a);
    if !has_local_snapshot(&st) {
        t.fatal(format!(
            "Actor {name:?} is SUSPENDED after pause but holds no local snapshot: {st:?}"
        ));
    }
    if st.assigned_node.is_empty() {
        t.fatal(format!(
            "Actor {name:?} is paused but not pinned to a node: {st:?}"
        ));
    }
}

/// The newest completed durable snapshot of `st`.
pub fn durable_snapshot(st: &pb::ActorStatus) -> Option<&pb::Snapshot> {
    st.snapshots
        .iter()
        .filter(|s| {
            s.durable_snapshot.as_ref().is_some_and(|d| {
                d.state == pb::SnapshotStorageState::Completed as i32
                    && d.object
                        .as_ref()
                        .is_some_and(|o| !o.snapshot_uri.is_empty())
            })
        })
        .max_by_key(|s| s.generation)
}

/// The URI of a snapshot's durable copy, or `""`.
pub fn snapshot_uri(s: Option<&pb::Snapshot>) -> String {
    s.and_then(|s| s.durable_snapshot.as_ref())
        .and_then(|d| d.object.as_ref())
        .map(|o| o.snapshot_uri.clone())
        .unwrap_or_default()
}

/// The URI of the newest completed durable snapshot of `st`, or `""`.
pub fn durable_snapshot_uri(st: &pb::ActorStatus) -> String {
    snapshot_uri(durable_snapshot(st))
}

/// The runtime stamp of a snapshot.
pub fn stamp(s: Option<&pb::Snapshot>) -> Option<pb::ActorSandboxRuntime> {
    s.and_then(|s| s.sandbox_runtime.clone())
}

/// The newest node-local snapshot of `st`.
pub fn local_snapshot(st: &pb::ActorStatus) -> Option<&pb::Snapshot> {
    st.snapshots
        .iter()
        .filter(|s| !s.locality.is_empty() && !s.uuid.is_empty())
        .max_by_key(|s| s.generation)
}

/// Whether `st` holds a node-local snapshot.
pub fn has_local_snapshot(st: &pb::ActorStatus) -> bool {
    local_snapshot(st).is_some()
}

/// The runtime the worker assignment of `st` records.
pub fn assignment_runtime(st: &pb::ActorStatus) -> Option<pb::ActorSandboxRuntime> {
    st.worker_assignment
        .as_ref()
        .and_then(|w| w.sandbox_runtime.clone())
}

/// Fails (non-fatally) when two stamps differ.
pub fn check_same_stamp(
    t: &T,
    what: &str,
    want: &Option<pb::ActorSandboxRuntime>,
    got: &Option<pb::ActorSandboxRuntime>,
) -> bool {
    if want != got {
        t.error(format!("{what} mismatch: want {want:?}, got {got:?}"));
        return false;
    }
    true
}

/// [`check_same_stamp`], but fatal.
pub fn require_same_stamp(
    t: &T,
    what: &str,
    want: &Option<pb::ActorSandboxRuntime>,
    got: &Option<pb::ActorSandboxRuntime>,
) {
    if want != got {
        t.fatal(format!("{what} mismatch: want {want:?}, got {got:?}"));
    }
}

/// The snapshot URI of a Tag, or `""`.
pub fn tag_snapshot_uri(tag: &pb::Tag) -> String {
    snapshot_uri(tag.status.as_ref().and_then(|s| s.snapshot.as_ref()))
}

/// Reads `at`'s golden Tag and returns the runtime its snapshot is stamped
/// with.
pub async fn golden_snapshot_runtime(
    t: &T,
    clients: &Clients,
    at: &pb::ActorTemplate,
) -> pb::ActorSandboxRuntime {
    let current = clients
        .api()
        .get_actor_template(pb::GetActorTemplateRequest {
            actor_template: Some(template_ref(at)),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetActorTemplate: {e}")))
        .into_inner();
    let golden = current
        .status
        .and_then(|s| s.golden_snapshot_status)
        .and_then(|g| g.golden_tag)
        .unwrap_or_default();
    if golden.name.is_empty() {
        t.fatal(format!(
            "ActorTemplate {:?} has no golden tag",
            template_ref(at)
        ));
    }
    let tag = clients
        .api()
        .get_tag(pb::GetTagRequest {
            tag: Some(golden.clone()),
        })
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "GetTag(golden {}/{}): {e}",
                golden.hive, golden.name
            ))
        })
        .into_inner();
    let snap = tag.status.and_then(|s| s.snapshot);
    match snap.as_ref().and_then(|s| s.sandbox_runtime.clone()) {
        Some(rt) => rt,
        None => t.fatal(format!(
            "golden snapshot {snap:?} carries no sandbox_runtime"
        )),
    }
}

/// The value of the version attribute `key` of `rt`, or `""`.
pub fn runtime_attribute(rt: &pb::ActorSandboxRuntime, key: &str) -> String {
    rt.version
        .iter()
        .flat_map(|v| v.attributes.iter())
        .find(|a| a.key == key)
        .map(|a| a.value.clone())
        .unwrap_or_default()
}

/// Where hivekeep-api stores `tmpl`'s snapshots: the template's storage
/// location under the per-template location preview gate, otherwise the
/// install's `SNAPSHOT_LOCATION`.
pub fn want_snapshot_location(t: &T, tmpl: &pb::ActorTemplate) -> String {
    let location = tmpl
        .snapshot_config
        .as_ref()
        .map(|s| s.storage_location.clone())
        .unwrap_or_default();
    if !location.is_empty() && hivekeep_e2e::preview::is_enabled(Gate::PerTemplateSnapshotLocation)
    {
        return location;
    }
    require_env(t, "HK_E2E_SNAPSHOT_LOCATION")
}

/// Checks that `snapshot_uri` is stored under `location`, comparing owner
/// prefixes so the location may be spelled with or without a trailing slash.
pub fn assert_snapshot_under_location(t: &T, snapshot_uri: &str, location: &str) {
    let uri = SnapshotUri::parse(snapshot_uri)
        .unwrap_or_else(|e| t.fatal(format!("parsing snapshot URI {snapshot_uri:?}: {e}")));
    let Some(owner) = uri.owner() else {
        t.fatal(format!("snapshot URI {snapshot_uri:?} names no owner"))
    };
    let want = owner
        .prefix(location)
        .unwrap_or_else(|e| t.fatal(format!("snapshot owner prefix under {location:?}: {e}")));
    let got = uri.owner_prefix();
    if got != want {
        t.error(format!(
            "snapshot {snapshot_uri:?} is stored under {got}, want {want} (PerTemplateSnapshotLocation preview gate enabled: {})",
            hivekeep_e2e::preview::is_enabled(Gate::PerTemplateSnapshotLocation)
        ));
    }
}

/// Checks the memory and first-file counters of a counter response.
pub fn validate_counter_response(t: &T, resp: &str, stage: &str, want_memory: i64, want_file: i64) {
    if !resp.contains(&format!("preserved memory count: {want_memory}")) {
        t.error(format!(
            "[{stage}] expected memory count {want_memory}, got response: {resp}"
        ));
    }
    if !resp.contains(&format!("preserved file counter: {want_file}")) {
        t.error(format!(
            "[{stage}] expected file count {want_file}, got response: {resp}"
        ));
    }
}

/// Checks the counter kept in the second durable dir.
pub fn validate_second_file_counter(t: &T, resp: &str, stage: &str, want: i64) {
    if !resp.contains(&format!("preserved second file counter: {want}")) {
        t.error(format!(
            "[{stage}] expected second file count {want}, got response: {resp}"
        ));
    }
}

/// Checks the counter kept on the root filesystem, the one layer that tells
/// ROOTFS apart from VOLUMES.
pub fn validate_rootfs_counter(t: &T, resp: &str, stage: &str, want: i64) {
    if !resp.contains(&format!("preserved rootfs counter: {want}")) {
        t.error(format!(
            "[{stage}] expected rootfs counter {want}, got response: {resp}"
        ));
    }
}

/// POSTs `/` to the Actor through the router, retrying for 30 s.
pub async fn call_actor(t: &T, clients: &Clients, actor: &ActorRef) -> Result<String, String> {
    call_actor_path(t, clients, actor, http::Method::POST, "/").await
}

/// Sends `method path` to the Actor through the router, retrying for 30 s
/// until it answers 200.
pub async fn call_actor_path(
    t: &T,
    clients: &Clients,
    actor: &ActorRef,
    method: http::Method,
    path: &str,
) -> Result<String, String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = String::new();
    while Instant::now() < deadline {
        match call_actor_path_once(t, clients, actor, method.clone(), path).await {
            Ok(body) => return Ok(body),
            Err(e) => last = e,
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(format!("timed out waiting for actor response: {last}"))
}

async fn call_actor_path_once(
    t: &T,
    clients: &Clients,
    actor: &ActorRef,
    method: http::Method,
    path: &str,
) -> Result<String, String> {
    let router = RouterClient::new(t, clients)
        .await
        .map_err(|e| format!("connecting to the gate router: {e}"))?;
    let resp = tokio::time::timeout(
        Duration::from_secs(15),
        router.request(
            method,
            actor,
            path,
            bytes::Bytes::new(),
            None,
            &http::HeaderMap::new(),
        ),
    )
    .await
    .map_err(|_| "failed to do request: timed out".to_string())?
    .map_err(|e| format!("failed to do request: {e}"))?;
    if resp.status != 200 {
        return Err(format!(
            "unexpected status code: {}, body: {}",
            resp.status,
            resp.text()
        ));
    }
    Ok(resp.text())
}

/// Lists every Worker.
pub async fn list_workers(clients: &Clients) -> Result<Vec<pb::Worker>, Status> {
    clients
        .api()
        .list_workers(pb::ListWorkersRequest::default())
        .await
        .map(|r| r.into_inner().workers)
}

/// Deletes a pod.
pub async fn delete_pod(clients: &Clients, namespace: &str, name: &str) -> Result<(), kube::Error> {
    Api::<k8s_openapi::api::core::v1::Pod>::namespaced(clients.kube(), namespace)
        .delete(name, &kube::api::DeleteParams::default())
        .await
        .map(|_| ())
}
