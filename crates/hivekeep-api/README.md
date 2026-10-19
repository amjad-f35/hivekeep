# hivekeep-api

The Hivekeep control plane (binary `hivekeep-api`).

- Serves `hivekeep.api.v1.Control` to clients and `WorkerService` to
  keepers on `--grpc-listen-addr` (`0.0.0.0:443`), with metrics on `:9090`
  and health on `:9091`.
- Authenticates bearer JWTs (`authn`) or mTLS SPIFFE IDs. Authorizes
  against the global and Hive access policies and the system roles
  (`authz`).
- Places Actors with a power-of-two-choices scheduler over a watch-fed
  Worker cache (`scheduling`, `worker_cache`).
- Drives the lifecycle workflows (resume, suspend, revert, delete, Tags,
  template deletion) under per-resource leases (`workflow`, `leases`),
  calling the keeper on the Actor's node over mTLS (`kube_dialer`,
  `keeper_tls`).
- Runs reconcilers for golden snapshots (`template_reconciler`) and for
  epoch-based assignment cleanup (`assignment_reconciler`).
- Copies and deletes snapshot prefixes through the object-store control
  plugin (`object_store`), and manages per-Actor CSI volumes through a
  pluggable provider (`volumes`): with the `ExternalVolumes` gate, CSI
  controller drivers opened from their `CSIDriverConfig`, with storage
  classes from `--storage-classes-file` or the cluster (`csi_volumes`).

```sh
cargo run -p hivekeep-api -- --help
cargo test -p hivekeep-api
```

See [docs/architecture.md](../../docs/architecture.md) and
[docs/configuration.md](../../docs/configuration.md).
