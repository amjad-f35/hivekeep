# hivekeep-controller

The cluster controller (binary `hivekeep-controller`).

Reconcilers (`src/controllers/`):

- **WorkerPool**: resolves each sandbox class to a SandboxConfig (an
  explicit `configRef` or the newest class default), protects the configs
  in use with a finalizer, and applies the worker Deployment with
  server-side apply.
- **SandboxConfig protection**: releases a deleting SandboxConfig only once
  no pool names it.
- **NetworkPolicy**: lets only the gate router reach worker pods.
- **Egress MITM trust**: publishes the roots of the `egress-mitm-ca-pool`
  Secret as the ClusterTrustBundle
  `egress-mitm.hivekeep.io:mitm:primary-bundle`.

The worker registry sync (`src/workersync/`) mirrors worker pods into
`hivekeep-api`'s Worker registry. It registers, marks available or
unavailable, drains and deregisters Workers, and deletes terminal or
orphaned pods.

It exports `hivekeep_workerpool_desired_workers` and
`hivekeep_workerpool_ready_workers`.

```sh
cargo test -p hivekeep-controller    # runs against the in-memory fake API server
```
