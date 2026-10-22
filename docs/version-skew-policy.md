# Version skew policy

During an upgrade, Hivekeep components run at different versions. This
page says which combinations must work, so upgrades can be rolled out one
component at a time without stopping running Actors. It applies from the
first tagged release on. Before 1.0, a release's notes may suspend it for
a specific change, and must say so explicitly.

## Versions

Releases are tagged `vMAJOR.MINOR.PATCH[-PRERELEASE]`
(`hivekeep-setup publish release-images` refuses any other form). Every
image and binary of a release carries the same version. In this policy,
*N* is a minor version and *N-1* is the minor version before it.

## Components and their interfaces

| Interface | Server | Clients |
|-----------|--------|---------|
| `hivekeep.api.v1.Control` | `hivekeep-api` | `hivectl`, the controller, the gate, SDKs |
| `hivekeep.api.v1.WorkerService` | `hivekeep-api` | keepers |
| `hivekeep.keeper.v1.Keeper` | keeper | `hivekeep-api` |
| `hivekeep.keeper.v1.WorkerSupport` | keeper | workers |
| `hivekeep.worker.v1.Worker` | workers | keeper |
| `hivekeep.objectstore.v1`, `hivekeep.credprovider.v1` | plugins, credential provider | keeper, `hivekeep-api`, gate |
| PostgreSQL schema | the database | `hivekeep-api` replicas |
| Node files (`/var/lib/hivekeep`) | keeper | workers |
| Snapshots | object storage, node | keeper, workers |
| CRDs | Kubernetes API | controller, keeper, installer |

## Supported skew

- **Control plane and keepers.** `hivekeep-api` at N works with keepers at
  N and N-1, and a keeper at N works with `hivekeep-api` at N and N-1.
  Either side may be upgraded first.
- **Keeper and workers.** A keeper at N works with worker pods at N and
  N-1 on the same node. Workers are upgraded by rolling their WorkerPool,
  which may take a long time on a busy pool.
- **API replicas.** During a rollout, replicas at N and N-1 may share the
  database and serve traffic together.
- **Clients.** `hivectl` and other Control clients at N-1 work against
  `hivekeep-api` at N. Newer clients against an older server may find
  fields the server ignores.
- **Gate.** The router and egress gateway at N-1 or N work with
  `hivekeep-api` at N.
- **Snapshots.** A snapshot taken at N-1 can be restored at N. When a memory
  snapshot's runtime stamp is not offered by any Worker, the scheduler can
  restore it without its memory state (see
  [architecture.md](architecture.md)). Snapshots are never rewritten in
  place.

Skipping a minor version (N-2 to N) is not supported. Upgrade through each
minor version in turn.

## What this requires of a change

- **Protobuf.** Field numbers and names never change, and removed fields are
  reserved. New fields must have a safe zero value, so an N-1 peer that
  drops them behaves correctly. `hivekeep-apitool validate` keeps the public
  API consistent with its style rules.
- **Preview features.** A field behind a preview gate may change between
  minor versions. Graduating it removes the gate.
- **Database.** Migrations are append-only and must leave a schema that the
  N-1 binary can still use. Destructive steps (dropping a column or table)
  wait one minor version after the last reader is gone. The migrator
  tolerates applied migrations it does not know about, so an N-1 replica
  can start against a schema that N has migrated.
- **CRDs.** New fields are optional. A version of a CRD is never removed
  while a supported release still reads or writes it.
- **Node files.** The keeper's state files and the sandbox runtimes file
  can be read by the previous version of their reader, or are versioned so
  that an older reader rejects them clearly.
- **Snapshots.** Formats carry a version (the sparse format's magic and
  version). Readers accept the previous format for at least one minor
  version.
- **Worker pod contract.** Flags, environment variables, mounts and labels
  that the controller puts on worker pods keep working for the previous
  worker image.

## Recommended upgrade order

1. `podcertificate-controller` (certificates must keep being issued)
2. Keepers (DaemonSet)
3. `hivekeep-api` and `hivekeep-idp-server`
4. CRDs and `hivekeep-controller`
5. The gate (`gate-router`, `gate-egress`) and the credential provider
6. SandboxConfigs (new runtime versions; pools pick them up as they roll)
7. WorkerPools (new worker image)

[upgrade.md](upgrade.md) turns this order into a procedure.

## Not covered

Development settings (`--memory-store`, `--authz-allow-all`,
`--insecure-dev`, static keeper endpoints) and the demo workloads are
outside this policy.
