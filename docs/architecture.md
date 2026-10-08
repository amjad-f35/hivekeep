# Architecture

This page describes how Hivekeep is put together: what each component does,
what happens when an Actor is resumed or suspended, how state is modeled,
and the scheduling and fencing rules that keep it consistent. For terms, see
the [glossary](glossary.md).

## Components

```text
                     clients (hivectl, SDKs, services)
                       |  gRPC + mTLS/JWT              |  HTTPS / CONNECT over mTLS
                       v                               v
 +------------------------------------+     +------------------------------+
 | hivekeep-api                       |<----| gate-router                  |
 |  Control, WorkerService            |     |  Envoy + hivekeep-gate router|
 |  scheduler, workflows, authz       |     |  (--mode=ingress): ext_proc, |
 |  + objectstore-plugin (control)    |     |  xDS, request parking        |
 +------------------------------------+     +------------------------------+
   |  store            | Keeper (mTLS)                    | forwards to worker:443
   v                   v                                  v
 PostgreSQL   +------------------------- node ------------------------------+
              | hivekeep-keeper (DaemonSet)        worker pod (WorkerPool)  |
              |  Keeper + WorkerSupport  <-unix->  hivekeep-sandbox-gvisor  |
              |  snapshot tracker, caches          or -microvm              |
              |  + objectstore-plugin (node)       N Actors, one sandbox    |
              +------------------------------------each--------------------+
                       |  snapshots                           | Actor egress
                       v                                      v
               object storage (GCS / S3)       gate-egress: Envoy + hivekeep-gate
                                               router (--mode=egress), sdsmint,
                                               credential provider
 hivekeep-controller: WorkerPools -> worker Deployments, worker registry sync
 hivekeep-podcert-controller: pod-identity / service-DNS / postgres certificates
 hivekeep-idp: OIDC discovery + JWKS for Actor JWTs
```

| Component | Binary | Runs as | Purpose |
|-----------|--------|---------|---------|
| Control plane | `hivekeep-api` | Deployment `hivekeep-api` in `hivekeep-system` (Service `hivekeep-api`, port 443, headless) | Serves the `Control` API to clients and the `WorkerService` API to keepers. Owns all state through `hivekeep-store`. Places Actors and drives their lifecycle. |
| Object-store control plugin | `hivekeep-objectstore-plugin` | Sidecar in the `hivekeep-api` pod | Copies and deletes snapshot prefixes for the control plane. Serves the credential exchange on port 8445. |
| Node agent | `hivekeep-keeper` | DaemonSet `keeper`, port 7443 | Serves the `Keeper` service (Run, Checkpoint, Restore, PromoteSnapshot, DetachActor, Terminate) to the control plane, and `WorkerSupport` to the node's workers over a Unix socket. Stages bundles and runtimes, and keeps and uploads snapshots. |
| Object-store node plugin | `hivekeep-objectstore-plugin` | Sidecar in the keeper pod | Moves snapshot files and runtime assets between the node and object storage. |
| Sandbox workers | `hivekeep-sandbox-gvisor`, `hivekeep-sandbox-microvm` | Pods of a WorkerPool's Deployment | Run many Actors per pod, each in its own sandbox. Serve `hivekeep.worker.v1.Worker` to the keeper on a Unix socket. |
| Controller | `hivekeep-controller` | Deployment `hivekeep-controller` | Turns WorkerPools into worker Deployments, protects SandboxConfigs, maintains worker NetworkPolicies and the egress MITM ClusterTrustBundle, and syncs worker pods into the Worker registry. |
| Ingress router | `hivekeep-gate router --mode=ingress` + Envoy | Deployment `gate-router` | Terminates client mTLS, resumes the target Actor, and forwards to its worker. Also serves Envoy's xDS. |
| Egress gateway | `hivekeep-gate router --mode=egress` + Envoy + `hivekeep-gate sdsmint` | Deployment `gate-egress` | Authenticates Actor egress, enforces the egress policy, intercepts HTTPS with minted certificates, and injects credentials. |
| Credential provider | `hivekeep-credential-provider` | Deployment `k8s-credential-provider` (optional) | Resolves `hk-secret://k8s.io/...` references to Secret values for the egress gateway. |
| Pod certificates | `hivekeep-podcert-controller` | Deployment `podcertificate-controller` | Signs PodCertificateRequests for `podidentity.podcert.hivekeep.io/identity`, `servicedns.podcert.hivekeep.io/identity` and `postgres.podcert.hivekeep.io/identity`. |
| Actor JWT issuer | `hivekeep-idp` | Deployment `hivekeep-idp-server` | Publishes the OIDC discovery document and JWKS of the Actor JWT pool. |
| Database | PostgreSQL 13+ | Bundled StatefulSet `postgres`, or external | Holds all control-plane state. |

The gate's sandbox side lives in `hivekeep_gate::tunnel`. It is the
per-Actor tunnel that carries ingress into the sandbox and egress out to the
egress gateway, plus a DNS relay that enforces the egress policy. It is meant
to run inside each worker pod. See the README status section for how far
that wiring has got.

## Data model

All API resources are protobuf messages in `hivekeep.api.v1`. Each one
carries `ResourceMetadata` (`hive`, `name`, server-assigned `uid`, a
`version` bumped on every write, timestamps, deletion time).

| Resource | Scope | Notes |
|----------|-------|-------|
| `Hive` | global | The isolation boundary for Actors, templates and Tags. Cannot be deleted while it holds Actors or Tags. |
| `ActorTemplate` | Hive | Containers (digest-pinned images), volumes, resource limits, snapshot config, worker selector. Immutable: delete and recreate to change one. |
| `Actor` | Hive | References a template (changeable only while SUSPENDED), optionally a source Tag. The status holds the state, worker assignment, snapshots, crash and external volumes. |
| `EgressPolicy` | Actor singleton `default` | Allow rules for the Actor's outbound traffic. |
| `Tag` | Hive | A stable name for one external snapshot. Owns a private copy. Its scope is `TAG_SCOPE_HIVE` or `TAG_SCOPE_PUBLISHED`. |
| `AccessPolicy` | global or Hive singleton `default` | Role bindings (`owner`, `editor`, `viewer`). |
| `Worker` | global | One worker pod: namespace, pool, pod name and UID, node, SPIFFE ID, labels. The status holds the state, capacity, allocation, epochs and sandbox runtimes. |
| `ActorAssignment` | Worker sub-resource | The binding of one Actor to one Worker, with the resources it books. |

These Kubernetes CRDs live in the `hivekeep.io/v1` group and are owned by
`hivekeep-k8s`:

- `WorkerPool`: replicas, worker image, pod template overrides, and exactly
  one sandbox class (`gvisor` or `microvm`), optionally pinned to a
  SandboxConfig through `configRef`.
- `SandboxConfig`: the versions of a sandbox runtime's binaries and assets.
  The newest config annotated as the class default is used when a pool
  names none.
- `CSIDriverConfig`: how workers reach a CSI driver for external volumes.

The Actor lifecycle is a state machine (`hivekeep_core::actor_state`):

```text
              BeginResume            FinishResume
  SUSPENDED ------------> RESUMING ------------> RUNNING
    ^  ^                                           |  |
    |  | FinishSuspend                BeginSuspend |  | BeginRevert
    |  +---------------- SUSPENDING <--------------+  v
    |                                             REVERTING
    +------------------- FinishRevert ----------------+

  Crash:        any live state -> CRASHED   (CRASHED -BeginRevert-> REVERTING)
  MarkDeleting: any state      -> DELETING  (terminal)
```

New Actors start SUSPENDED. If a workflow is retried and re-applies the
event that produced the current state, the transition is a no-op, so
retries can fast-forward.

## Request flow: resume

A resume is triggered by `ResumeActor` (for example `hivectl resume actor`),
or by the ingress router when a request arrives for a suspended Actor.

```text
client --mTLS--> gate-router (Envoy) --ext_proc--> hivekeep-gate router
                                                   | ResumeActor (one flight per Actor)
                                                   v
hivekeep-api: lease Actor -> provision volumes -> schedule + bind Worker (RESUMING)
            -> attach volumes -> Keeper.Run | Keeper.Restore  ---mTLS--> keeper
keeper: fetch snapshot (local copy, cache, or object store) + prepare bundle
        (image layer cache, runtime assets) -> Worker.RunWorkload | RestoreWorkload
worker: runsc restore / VM restore -> wakeup probe answers 200
hivekeep-api: commit RUNNING, stamped with the Worker's epoch
gate-router: route to worker pod IP:443 (tunnel) with the Actor port --> Actor
```

1. **Lease.** Every operation on an Actor takes a lease on it in the store.
   A concurrent operation fails with `ABORTED`, and the router retries it.
2. **Volumes.** External (CSI) volumes are provisioned once per Actor, then
   attached on the chosen node.
3. **Placement.** The scheduler picks a Worker (see
   [Scheduling](#scheduling)). The store re-checks room and eligibility
   under the Worker's row lock while it binds the `ActorAssignment` and
   moves the Actor to RESUMING.
4. **Run or restore.** An Actor with no snapshot cold-boots through
   `Keeper.Run`. Otherwise `Keeper.Restore` restores the newest snapshot.
   That can use a node-local copy if the Actor is on the same node, the
   shared snapshot cache, or a download from object storage. A SUSPENDED
   Actor whose newest snapshot exists only on one node is placed on that
   node. If that node is full or gone, the Actor is detached first, so its
   snapshot becomes durable and it can resume anywhere. If no reachable
   Worker offers a runtime compatible with a memory snapshot's stamp, the
   scheduler may drop that requirement. The Actor then restores without
   its memory state: it cold-boots on its rootfs and volume layers.
5. **Wakeup probe.** If a container declares a `wakeup_probe`, the worker
   polls it after every boot or restore. A probe that does not answer 200
   within its timeout crashes the Actor.
6. **Commit.** The Actor becomes RUNNING. The commit is fenced on the epoch
   of the Worker process that took the workload (see [Epochs](#epochs)).

While the resume is in flight, the router holds concurrent requests for the
same Actor in one flight. Requests that hit a transient condition wait in
the parking lot (see [request-parking.md](request-parking.md)).

## Request flow: suspend

A suspend is triggered by `SuspendActor`, by `hivectl suspend actor`, or by a
worker through `WorkerService.RequestActorSuspend`.

```text
hivekeep-api: lease -> SUSPENDING (new Snapshot record, generation N, PENDING upload
              under the Actor's own prefix) -> Keeper.Checkpoint
keeper -> Worker.CheckpointWorkload -> snapshot files staged on the node
       -> local commit; upload now (sync) or after --snapshot-upload-delay (async)
hivekeep-api: detach volumes -> SUSPENDED (assignment cleared, Worker slot freed)
              -> release snapshots the new one replaced
keeper (background): upload -> ReportActorSnapshotState -> COMPLETED;
              after --snapshot-detach-delay, DetachActor removes local copies
```

- `DURABILITY_MODE_ASYNCHRONOUS` (the default, `hivectl suspend --mode=async`)
  returns once the node-local copy exists.
  `DURABILITY_MODE_SYNCHRONOUS` (`--mode=sync`) waits until the copy in
  object storage is complete.
- The keeper's snapshot tracker fences concurrent checkpoints by
  generation, retries uploads with timeouts that grow with snapshot size,
  and reports each snapshot's state to the control plane. A SUSPENDED Actor
  whose node fills up or goes away is detached, so it can resume anywhere.
- `RevertActor` throws away the current execution and returns the Actor to
  SUSPENDED at its newest durable snapshot. It is the recovery verb for
  CRASHED Actors, and it needs no live worker.

## Snapshot fidelities

`SnapshotConfig.preferred_fidelity` on the template picks what a suspend
captures. The default is `SNAPSHOT_FIDELITY_MEMORY`. Each level includes
the ones below it:

| Fidelity | Captures | Resume behaviour |
|----------|----------|------------------|
| `SNAPSHOT_FIDELITY_VOLUMES` | `durable_dir` volumes | Cold boot with the volumes restored |
| `SNAPSHOT_FIDELITY_ROOTFS` | the above, plus each container's root filesystem changes | Cold boot with the rootfs and volumes restored |
| `SNAPSHOT_FIDELITY_MEMORY` | the above, plus process (or guest) memory | Processes continue where they stopped |

Both workers accept all three levels. Memory snapshots carry the runtime
stamp they were taken under (a schema version plus attributes such as
`cpu_features`). They can only be restored on a Worker that offers a
compatible runtime. In the object store, snapshots live under their owner's
prefix:

```text
<location>/hives/<hive>/actors/<actor-uid>/snapshots/<snapshot-name>
<location>/hives/<hive>/tags/<tag-uid>
```

Because ownership can be read from the URI, a collector only ever deletes
under its own prefix. See [storage.md](storage.md).

## Golden snapshots and Tags

For each ActorTemplate (unless `GOLDEN_SNAPSHOT_MODE_DISABLED`), the template
reconciler builds a golden snapshot. It creates a golden Actor named after
the template's UID, resumes it, lets it warm up, suspends it synchronously,
tags the snapshot as a published golden Tag, and deletes the Actor. New
Actors of that template then start from the golden snapshot instead of a
cold boot. Every pass works out its next step from what it observes, so
each step is safe to repeat.

`CreateTag` copies a SUSPENDED Actor's external snapshot into the Tag's own
prefix, so later suspends or deletes of the Actor cannot collect it.
`CreateActor` with a `source_tag` starts a new Actor from that copy.

## Scheduling

`hivekeep_api::scheduling::Scheduler` places Actors on Workers:

- **Eligibility.** The Worker is ACTIVE, matches the template's and the
  Actor's `worker_selector`, offers the template's sandbox class, and
  (for a restore) offers a runtime compatible with the snapshot's stamp.
- **Room.** The Worker has a free Actor slot, and CPU and memory for the
  template's limits plus the runtime's per-Actor memory overhead
  (`RuntimeOverhead`: a fixed part plus a part per GiB of limit).
- **Choice.** Two eligible Workers are sampled at random and the less
  loaded one wins. Load is the higher of slot utilization and compute
  utilization.

The scheduler reads a watch-fed Worker cache, so the store repeats the
eligibility and room checks under the Worker's row lock when it binds. A
failed bind is retried. If no Worker has room, the resume fails with a
capacity error, which the router can park on.

Workers enter the registry through the controller's worker sync. It
registers each worker pod (named after its pod UID) once the pod has an IP.
It marks the Worker ACTIVE or UNAVAILABLE as pod readiness changes, drains
and deregisters it when the pod goes away, and deletes terminal or orphaned
worker pods.

## Epochs

Each worker process gets a fresh **epoch**:

1. On start, the worker calls `AcquireWorkerEpoch` through the keeper's
   `WorkerSupport`, which forwards it to `WorkerService`. The control plane
   records it as `issued_epoch`.
2. The worker calls `RegisterWorker` with that epoch, its IPs and its
   capacity, which sets `registered_epoch`. It then reports its sandbox
   runtimes with `SetWorkerSandboxRuntimes`.
3. Each assignment records the Worker's epoch at placement time
   (`WorkerAssignment.worker_epoch`), and the commit to RUNNING is fenced on
   it.
4. When a new process registers (registered epoch above the observed one),
   the assignment reconciler crashes and releases the Actors the previous
   process held. Their sandboxes died with it.

This keeps a restarted worker process from being mistaken for the one that
held an Actor's memory.

## Concurrency and leases

Operations on one resource are serialized by store leases
(`hivekeep_api::leases`). In PostgreSQL these are TTL leases that renew in
the background. When a lease is lost, the leased context is cancelled, and
an assignment in progress records a cancelled error instead of committing.
Every write carries the resource's `version`, and updates that take a
precondition (egress and access policies) need a matching `uid` and
`version`.

Worker changes are published through a transactional outbox
(`worker_events`, kept for 15 minutes). That outbox feeds the API replicas'
Worker caches.

## System infra isolation

`hivekeep-setup deploy hivekeep-system --cordon-system-infra-components`
keeps the system components off the worker nodes. It needs two node pools,
each tainted `NoSchedule` with its label:

- `hivekeep.io/workloadType=hivekeep-system-infra` runs `hivekeep-api`,
  `hivekeep-idp-server`, `hivekeep-controller`, `gate-router`,
  `gate-egress`, `podcertificate-controller` and `k8s-credential-provider`.
  Replicas are spread across its nodes on a best-effort basis.
- `hivekeep.io/workloadType=hivekeep-postgres` is a single node for the
  bundled PostgreSQL StatefulSet. The installer refuses to cordon if no node
  carries this label.

If a pool is missing, its pods stay Pending. This is implemented by the
`cordon-system-infra-components` kustomize component in
`crates/hivekeep-setup/manifests/install/components/`.

## Where to read the code

| Concern | Location |
|---------|----------|
| Lifecycle workflows | `crates/hivekeep-api/src/workflow/` |
| Scheduling | `crates/hivekeep-api/src/scheduling.rs`, `worker_cache.rs` |
| Epoch reconciliation | `crates/hivekeep-api/src/assignment_reconciler.rs`, `worker_workflow.rs` |
| Golden snapshots | `crates/hivekeep-api/src/template_reconciler.rs` |
| Keeper RPCs | `crates/hivekeep-keeper/src/keeper/` |
| Snapshot tracker | `crates/hivekeep-keeper/src/snapshot_tracker/` |
| gVisor lifecycle | `crates/hivekeep-sandbox-gvisor/src/lifecycle/` |
| Micro-VM boot, checkpoint, restore | `crates/hivekeep-sandbox-microvm/src/{boot,checkpoint,restore}.rs` |
| Router and egress gateway | `crates/hivekeep-gate/src/router/` |
| State machine | `crates/hivekeep-core/src/actor_state.rs` |
