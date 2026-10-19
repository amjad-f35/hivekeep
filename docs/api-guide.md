# API guide

`hivekeep-api` serves two gRPC services from
`crates/hivekeep-proto/proto/hivekeep/api/v1/api.proto` (package
`hivekeep.api.v1`) on `--grpc-listen-addr` (default `0.0.0.0:443`):

- **`Control`**, the client-facing API for Hives, ActorTemplates, Actors,
  Tags, egress and access policies, and Workers.
- **`WorkerService`**, which only keepers may call. They use it to report
  on the workers of their own node.

Rust clients use `hivekeep_proto::api::v1::control_client::ControlClient`.
Clients in other languages can generate stubs from the same `.proto`. The
command-line client is `hivectl` (see [below](#hivectl)).

## Conventions

- **Addressing.** Hive-scoped resources are addressed by an `ObjectRef`
  `{hive, name}`. Global resources (Hive, Worker, the global AccessPolicy)
  leave `hive` empty. Names are DNS labels.
- **Metadata.** The server owns `uid`, `version`, `create_time`,
  `update_time` and `delete_time`. A recreated resource gets a new `uid`.
  `version` goes up on every write.
- **Preconditions.** `DeleteOptions{version, uid}` makes a delete
  conditional. Updates to egress and access policies require
  `metadata.uid` and `metadata.version` to match the stored policy.
- **Listing.** `ListOptions{page_size, page_token}` controls paging. The
  page size defaults to 1000, and the end is reached when
  `next_page_token` is empty. Changes during a listing can cause items to be
  missed or repeated.
- **Validation.** Every field rule appears in the proto as a `Rules:` line,
  for example `required`, `dns-label`, `max items 256`, `immutable` or
  `needs preview gate ...`. A violation returns `INVALID_ARGUMENT` with the
  field path.
- **Concurrency.** Operations on one Actor are serialized. A conflicting
  operation returns `ABORTED`, and the client should retry.
- **Preview features.** Fields gated behind a preview gate need the gate
  enabled on the server (`--preview`) and the request header
  `hk-preview-allowed: true` (`hivectl --preview`).
- **Authentication.** Send `authorization: Bearer <JWT>`, or present a
  client certificate. See [authentication.md](authentication.md).

Common status codes: `NOT_FOUND`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`
(wrong state, for example deleting a Hive that still holds Actors),
`ABORTED` (concurrent write), `RESOURCE_EXHAUSTED` (no Worker has room),
`PERMISSION_DENIED`, `UNAUTHENTICATED` and `UNAVAILABLE`.

## Control

### Hives

| RPC | Effect |
|-----|--------|
| `CreateHive`, `GetHive`, `ListHives` | Manage the global list of Hives. |
| `DeleteHive` | Fails with `FAILED_PRECONDITION` while the Hive holds Actors or Tags. |

### ActorTemplates

| RPC | Effect |
|-----|--------|
| `CreateActorTemplate` | Creates an immutable template. Unless golden snapshots are disabled, it starts the golden snapshot build. Progress shows in `status.golden_snapshot_status`. |
| `GetActorTemplate`, `ListActorTemplates` | Read. |
| `DeleteActorTemplate` | Deletes the template together with its golden Actor and golden Tag. |

A template's main fields are `containers` (up to 10; image pinned by
digest, command, args, env, `wakeup_probe`, `volume_mounts`,
`security_context.capabilities`, per-container `resources`), `volumes` (up
to 32; `durable_dir`, `image`, `system_info` or `external`),
`resources.limits` (`cpu_millicores`, `memory_bytes`), `snapshot_config`
(`preferred_fidelity`, `golden_snapshot_config.mode`, `storage_location`)
and `worker_selector`.

### Actors

| RPC | Effect |
|-----|--------|
| `CreateActor` | Creates an Actor from `actor_template`, or from `source_tag`. New Actors are SUSPENDED. |
| `GetActor`, `ListActors` | Read. `ListActors` takes an optional `hive`. |
| `UpdateActor` | Changes `actor_template` (only while SUSPENDED) or `worker_selector`. Changes apply on the next resume. |
| `ResumeActor` | Places the Actor on a Worker and restores it, or cold-boots it if it has no snapshot. `resumed` is false if it was already running. |
| `SuspendActor` | Checkpoints a RUNNING Actor and frees its Worker. `options.mode` is `DURABILITY_MODE_SYNCHRONOUS` (wait for object storage) or `DURABILITY_MODE_ASYNCHRONOUS` (return once node-local). Suspending a SUSPENDED Actor returns it unchanged. |
| `RevertActor` | Discards the current execution and returns to SUSPENDED at the newest durable snapshot. Accepted for RUNNING and CRASHED Actors, and for SUSPENDED Actors whose newest snapshot is only on a node. |
| `DeleteActor` | Terminates any workload, detaches volumes, releases the Worker, collects snapshots and removes the record. |
| `MintActorJwt` | Issues a JWT asserting the Actor's identity, for the given audiences. Only the egress gateway role may call it. |
| `MintActorCertificate` | Issues an X.509 certificate whose URI SAN is the Actor's SPIFFE ID. Only the egress gateway role may call it. |

### Egress policies

`GetActorEgressPolicy`, `CreateActorEgressPolicy`,
`UpdateActorEgressPolicy` and `DeleteActorEgressPolicy` manage an Actor's
single `default` EgressPolicy. See [egress.md](egress.md) for the rule
semantics.

### Tags

| RPC | Effect |
|-----|--------|
| `CreateTag` | Copies a SUSPENDED Actor's external snapshot into the Tag's own prefix, uploading it first if it is still node-local. `scope` is `TAG_SCOPE_HIVE` or `TAG_SCOPE_PUBLISHED`. Not idempotent: if a create fails, the Tag stays pending, and you must delete it before retrying. |
| `GetTag`, `ListTags` | Read. |
| `UpdateTag` | Changes the scope (publish or unpublish). The snapshot never changes. |
| `DeleteTag` | Deletes the Tag and its snapshot copy. |

### Access policies

`GetGlobalAccessPolicy`, `CreateGlobalAccessPolicy` and
`UpdateGlobalAccessPolicy` manage the deployment-wide policy. It cannot be
deleted: empty its bindings instead. `GetHiveAccessPolicy`,
`CreateHiveAccessPolicy`, `UpdateHiveAccessPolicy` and
`DeleteHiveAccessPolicy` manage each Hive's policy. See
[authorization.md](authorization.md).

### Workers

The controller manages Worker records. Operators mostly use the read and
drain RPCs.

| RPC | Effect |
|-----|--------|
| `ListWorkers`, `GetWorker` | Read. Worker names are assigned by the control plane and should be treated as opaque. |
| `CreateWorker`, `UpdateWorker`, `DeleteWorker` | Register, update or remove a worker pod record (controller). Nothing cascades on delete. |
| `DrainWorker` | Moves the Worker to DRAINING. No new Actors are placed on it. This is one-way and idempotent. |
| `MarkWorkerAvailable`, `MarkWorkerUnavailable` | Toggle between ACTIVE and UNAVAILABLE. Returns `FAILED_PRECONDITION` for a DRAINING Worker. |
| `ListWorkerActorAssignments` | Lists the Actors bound to a Worker. |

## WorkerService

Keepers call `WorkerService` over mTLS with their pod-identity certificate,
mostly to relay calls from the workers on their node (through the keeper's
`WorkerSupport` socket). Any Worker or Actor that is not on the caller's
node is reported as `NOT_FOUND`.

| RPC | Purpose |
|-----|---------|
| `AcquireWorkerEpoch` | Hands out a fresh epoch for a starting worker process. |
| `RegisterWorker` | Activates an acquired epoch and records the worker's IPs and capacity. |
| `SetWorkerSandboxRuntimes` | Records the sandbox runtimes the worker can run (default runtime and restorable runtimes per class). |
| `MintWorkerActorCertificate` | Issues a tunnel certificate for an Actor the calling worker hosts. |
| `MintWorkerActorJwt` | Issues a JWT for an Actor the calling worker hosts (used for snapshot transfer credentials). |
| `RequestActorSuspend` | Asks the control plane to suspend an Actor the calling worker hosts. |
| `ReportActorSnapshotState` | Reports the keeper's local, uploading and durable snapshots for an Actor. |
| `ReportActorCrashed` | Reports that an Actor's sandbox died on its own. |

## Internal services

These services are not for clients. They are listed here so the whole
surface is in one place.

- `hivekeep.keeper.v1.Keeper` is served by the keeper on port 7443 and
  called only by `hivekeep-api`. Its RPCs are `Run`, `Checkpoint`,
  `Restore`, `PromoteSnapshot`, `DetachActor` and `Terminate`. At most one
  runs per Actor at a time.
- `hivekeep.keeper.v1.WorkerSupport` is served by the keeper on
  `<base-dir>/worker-support.sock` for its workers. It mirrors the
  WorkerService calls above, plus `GetActorEgressPolicy`.
- `hivekeep.worker.v1.Worker` is served by each worker pod on
  `<base-dir>/workers/<pod-uid>/worker.sock`. Its RPCs are `RunWorkload`,
  `CheckpointWorkload`, `RestoreWorkload`, `TerminateWorkload`,
  `GetWorkloadStats` and `GetActiveWorkloadStats`.

## hivectl

`hivectl` uses `hivectl <verb> <resource>` commands. Global options:

| Flag | Env | Meaning |
|------|-----|---------|
| `--endpoint` | `HK_ENDPOINT` | `host:port` or `http(s)://` URI of `hivekeep-api` |
| `--ca-file` | `HK_CA_FILE` | PEM bundle the server certificate must chain to (default: system roots) |
| `--tls-server-name` | `HK_TLS_SERVER_NAME` | Expected server name (default `hivekeep-api.hivekeep-system.svc`) |
| `--plaintext` | | No TLS, development only. No token is sent. |
| `--token-file` | `HK_TOKEN_FILE` | Bearer token file (re-read on every call), or `-` for stdin |
| `-o table\|json\|yaml` | | Output format |
| `-v` / `--verbose[=N]` | | Log RPCs and timings |
| `--trace` | | Send a W3C trace context and print the trace ID |
| `--preview` | | Send `hk-preview-allowed` |
| `--kubeconfig`, `--context` | | For `logs`, `top` and `admin`, which go through `kubectl` |

`HK_NAMESPACE`, `HK_API_SERVICE_NAME` and `HK_GATE_SERVICE_NAME` override
the default install names that `hivectl` derives server names from.

Commands:

```sh
hivectl get actor|actor-template|hive|tag|worker [NAME...] [-H HIVE | -A]
hivectl get worker [-n NAMESPACE] [-H HIVE] [-l SELECTOR] [--sandbox-class gvisor|microvm]
hivectl get worker-assignments WORKER
hivectl get egress-policy ACTOR -H HIVE
hivectl get access-policy (-H HIVE | --global)

hivectl create hive NAME
hivectl create actor NAME -H HIVE --template [HIVE/]TEMPLATE [--tag [HIVE/]TAG]
hivectl create actor-template -f template.yaml
hivectl create tag NAME -H HIVE --actor ACTOR [--scope hive|published]
hivectl create egress-policy ACTOR -H HIVE -f policy.yaml
hivectl create access-policy (-H HIVE | --global) -f policy.yaml

hivectl update actor NAME -H HIVE [--template [HIVE/]T] [--worker-selector k=v,...]
hivectl update tag NAME -H HIVE --scope hive|published
hivectl update egress-policy ACTOR -H HIVE -f policy.yaml
hivectl update access-policy (-H HIVE | --global) -f policy.yaml

hivectl resume actor NAME -H HIVE
hivectl suspend actor NAME -H HIVE [--mode async|sync]
hivectl revert actor NAME -H HIVE
hivectl delete actor|actor-template|tag NAME -H HIVE
hivectl delete hive NAME
hivectl delete egress-policy ACTOR -H HIVE [--uid U] [--version V]
hivectl delete access-policy -H HIVE [--uid U] [--version V]

hivectl logs actor NAME -H HIVE [-f] [-c CONTAINER|_sandbox] [--source all|containers|lifecycle]
hivectl top worker
hivectl drain worker NAME
hivectl mark-available worker NAME
hivectl mark-unavailable worker NAME

hivectl proxy actor NAME [[LOCAL:]REMOTE ...] -H HIVE [--address 127.0.0.1] \
  --gate HOST:PORT --gate-ca-file CA.pem --client-cert C.pem --client-key K.pem [--gate-server-name N]

hivectl admin make-ca-pool --name SECRET [--secret-namespace NS] [--ca-id ID] [--key-type ECDSAP256|ED25519] [--validity 8760h]
hivectl admin make-jwt-pool --name SECRET [--secret-namespace NS] [--alg ES256|RS256] [--key-id ID]
hivectl admin list-jwt-keys --name SECRET
hivectl admin add-jwt-key --name SECRET [--alg ES256|RS256] [--key-id ID]
hivectl admin activate-jwt-key|remove-jwt-key --name SECRET --key-id ID
```

Manifests (`-f`) hold one resource in protobuf JSON form (YAML or JSON),
exactly as `get ... -o yaml` prints it. Unknown fields are rejected.
`hivectl proxy` resumes the Actor if needed. With no mappings, it forwards
local port 8000 to Actor port 80. The gate flags can also come from
`HK_GATE_ENDPOINT`, `HK_GATE_CA_FILE`, `HK_GATE_SERVER_NAME`,
`HK_GATE_CLIENT_CERT` and `HK_GATE_CLIENT_KEY`.

Example template manifest:

```yaml
metadata:
  hive: team-a
  name: notebook
containers:
- name: main
  image: registry.example/notebook@sha256:<digest>
  wakeupProbe:
    httpGet: {path: /healthz, port: 8888}
    timeoutSeconds: 30
  volumeMounts:
  - {name: work, mountPath: /home/user}
volumes:
- name: work
  durableDir: {}
resources:
  limits: {cpuMillicores: "1000", memoryBytes: "2147483648"}
snapshotConfig:
  preferredFidelity: SNAPSHOT_FIDELITY_MEMORY
```
