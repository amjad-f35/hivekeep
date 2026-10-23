# hivekeep-e2e

The end-to-end harness, fixtures and suites, run against a live cluster.

- Library (`src/`): a test context with cleanups, skip and fatal; cluster
  and Control API clients; fixture rendering and deployment;
  per-test namespaces; and helpers for the gate, the egress trust bundle
  and the metrics pipeline.
- Fixture binaries: `hivekeep-e2e-probe` (an in-sandbox probe) and
  `hivekeep-e2e-testserver` (a multi-protocol origin). Manifests live in
  `fixtures/`.
- Suites (`tests/`): demo lifecycle, networking, authz, security, identity,
  ingress auth, egress authz, egress MITM, credential injection, snapshot
  credentials, combined volumes, metrics, multi-actor, NetworkPolicy,
  parking, SandboxConfig and sizing.

Every cluster test is `#[ignore]`d and also requires `HK_E2E=1`, so plain
`cargo test` stays offline.

```sh
make e2e-kind        # or: HK_E2E=1 cargo test -p hivekeep-e2e -- --ignored
```

See [docs/development.md](../../docs/development.md#end-to-end-suites).
