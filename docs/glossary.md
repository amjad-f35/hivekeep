# Glossary

**AccessPolicy.** A set of role bindings. There is one global policy (named
`default`) and at most one per Hive. See [authorization.md](authorization.md).

**Actor.** One sandboxed, stateful workload instance, built from an
ActorTemplate and living in a Hive. It is either running on a Worker or
suspended in a snapshot. Its states are RESUMING, RUNNING, SUSPENDING,
SUSPENDED, CRASHED, REVERTING and DELETING.

**ActorAssignment.** The binding of an Actor to a Worker, stored as a
sub-resource of the Worker. It records the resources the Actor books.

**ActorTemplate.** Says how to build Actors: containers (digest-pinned
images), volumes, resource limits, snapshot config and a worker selector.
It is immutable.

**Bootstrap owner.** A principal named by `hivekeep-api
--authz-bootstrap-owners`. It is always a global owner, so it can create or
repair the global AccessPolicy.

**CA pool.** A JSON file (usually in a Secret) holding an active signing CA
plus the trusted roots. Used for Actor certificates (`actor-id-ca-pool`),
egress interception (`egress-mitm-ca-pool`) and the pod certificate signers.
Create one with `hivectl admin make-ca-pool`.

**Credential provider.** A gRPC service (`hivekeep.credprovider.v1`) that
resolves `hk-secret://` references for the egress gateway. The bundled one,
`hivekeep-credential-provider`, reads Kubernetes Secrets.

**Durable-dir volume.** A directory that persists across resumes and is
captured in every snapshot.

**Epoch.** A number issued to each worker process at start. Assignments
record it, so the Actors of a restarted process can be detected and
released.

**External volume.** A CSI volume provisioned for each Actor from a template
(`ExternalVolumeTemplate`). It lives and dies with the Actor. Requires the
`ExternalVolumes` preview gate.

**Fidelity.** How much a snapshot captures: `VOLUMES`, `ROOTFS` or `MEMORY`.

**Gate.** Hivekeep's network layer, `hivekeep-gate`. It has the ingress
router (`gate-router`), the egress gateway (`gate-egress`), the SDS
certificate minter (`sdsmint`) and the sandbox tunnel with its DNS relay.

**Golden snapshot.** A warmed-up snapshot that the template reconciler
builds for each ActorTemplate and publishes as a Tag. New Actors start from
it.

**Hive.** The isolation and authorization boundary. Actors, ActorTemplates
and Tags belong to exactly one Hive.

**Image volume.** The read-only contents of a digest-pinned OCI image,
mounted into an Actor.

**JWT authority pool.** A rotating set of signing keys for Actor JWTs
(`actor-id-jwt-pool`). `hivekeep-idp` publishes its keys. It is managed with
`hivectl admin make-jwt-pool | add-jwt-key | activate-jwt-key |
remove-jwt-key`.

**Keeper.** The node agent, `hivekeep-keeper`. It drives the workers on its
node for the control plane and moves snapshots.

**Lease.** A store-level lock that serializes operations on one resource.

**Parking.** The router holding a request for an Actor whose resume hit a
transient condition, instead of failing it. See
[request-parking.md](request-parking.md).

**Pod identity.** A pod certificate from the
`podidentity.podcert.hivekeep.io/identity` signer. It carries the pod's
ServiceAccount SPIFFE ID and a private extension describing the pod.
Components use it to authenticate each other.

**Preview gate.** An opt-in for an unstable feature: `ExternalVolumes`,
`GoldenEgressPolicy` or `PerTemplateSnapshotLocation` (or `ALL`). It is
enabled with `--preview` on the server. Clients opt in per request with the
`hk-preview-allowed` header (`hivectl --preview`).

**Principal.** The authenticated caller. For a JWT it is the token subject.
For mTLS it is the first URI SAN of the client certificate. Bindings name
principals as `user:<id>`.

**Sandbox class.** A family of isolation runtimes. Today these are `gvisor`
and `microvm`.

**Sandbox runtime.** A specific version of a sandbox class's binaries
offered by a Worker, described by a compatibility schema version and
attributes. Snapshots are stamped with the runtime they were taken under.

**SandboxConfig.** A cluster-scoped CRD that lists the versions and assets
of a sandbox runtime.

**Service DNS certificate.** A serving certificate from the
`servicedns.podcert.hivekeep.io/identity` signer, naming the Services that
select the pod (for example `hivekeep-api.hivekeep-system.svc`).

**Snapshot.** A captured Actor state with a monotonically increasing
generation. It is node-local first, then durable in object storage.

**SPIFFE ID.** A URI identity. Actors get
`spiffe://<actor-trust-domain>/actor/<hive>/<actor>`, and components get
`spiffe://cluster.local/ns/<namespace>/sa/<service-account>`.

**System-info volume.** A read-only volume of generated per-Actor files
(Actor name, Hive, UID, trust bundles), rewritten on every start.

**Tag.** A stable, Hive-owned name for one external snapshot, backed by its
own copy.

**Trust bundle.** A named set of CA certificates that can be projected into
Actors. Examples are `egress-mitm.hivekeep.io` and
`system-roots.hivekeep.io`.

**Worker.** One worker pod registered with the control plane. It hosts
many Actors in sandboxes of one class.

**WorkerPool.** A CRD that the controller turns into a Deployment of worker
pods.
