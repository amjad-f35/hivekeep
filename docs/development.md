# Development

## Toolchain

- Rust 1.98 or newer (edition 2021). The workspace builds on Linux and macOS
  (Apple Silicon included). Linux-only code is behind
  `#[cfg(target_os = "linux")]`.
- `protoc`: the proto build scripts use `PROTOC` if it is set, otherwise the
  pinned copy from `make protoc` (`bin/protoc/25.3/bin/protoc`), otherwise
  whatever is on `PATH`.
- Pinned tools (`scripts/tools/versions.env`): `kind`, `cargo-nextest`,
  `cargo-deny` and `shellcheck`. Install them into `bin/tools` with
  `make tools`, and run them with `scripts/tools/run.sh`.
- For the local cluster: Docker with `buildx`, and `kubectl`.
- Python 3.10 or newer for the Python tooling (benchmark runner and locust
  files, automation, analysis, `scripts/threats`, `scripts/metrics`). The
  scripts pick the newest `python3.N` on `PATH`; set `HK_PYTHON` to choose
  one. macOS ships 3.9 as `python3`, so install a newer one (for example
  `brew install python@3.12`).

## Repository layout

```text
crates/        every Rust crate (see the README table)
  hivekeep-proto/proto/hivekeep/{api,keeper,worker,config}/v1/*.proto
  hivekeep-store/migrations/          PostgreSQL migrations
  hivekeep-setup/manifests/           everything the installer applies
  hivekeep-k8s/manifests/             generated CRDs and admission policies
  hivekeep-core/registry/events.yaml  the Actor event vocabulary
demos/         demo walkthroughs, Dockerfiles, helper scripts
scripts/       lib/, kind/, install/, e2e/, test/, verify/, update/, tools/,
               csi/, microvm/, microvm-assets/, gcp/, release/, threats/, metrics/
images/        container build support
docs/          this documentation
Makefile       entry points (run `make help`)
```

## Conventions

- **One concern per module.** A module should depend only on modules that
  could exist before it. Most crates document their layering at the top of
  `src/lib.rs`.
- **Errors.** Libraries use `thiserror` enums, and binaries use `anyhow`.
  gRPC errors map to precise status codes. Callers rely on `ABORTED`,
  `FAILED_PRECONDITION`, `RESOURCE_EXHAUSTED` and `NOT_FOUND`.
- **Async.** Use `tokio`. Long RPC handlers in the keeper and workers run
  detached from caller cancellation and observe their own deadlines.
- **gRPC.** Use `tonic` and `prost`. Codegen lives in
  `hivekeep-proto/build.rs` (and in the crates that own private protos).
  The public API in `api.proto` must pass `hivekeep-apitool validate`. Run
  `cargo run -p hivekeep-devtools --bin hivekeep-apitool -- validate`, and
  add `--update` to refresh `crates/hivekeep-devtools/exemptions.json`.
- **TLS.** Use `rustls` with the `ring` provider. Every binary installs it
  at startup, because `aws-lc-rs` is also linked (for JWT verification).
- **Kubernetes.** Use `kube` with `k8s-openapi` (API level `v1_36`).
  Controller and installer tests run against `hivekeep_k8s::fake::FakeApiServer`,
  an in-memory API server behind a real `kube::Client` (feature `testing`).
- **Placeholders.** Every production-relevant default (port, path, domain,
  image reference, OID, metric prefix, stubbed backend) gets a row in
  `PLACEHOLDERS.md`.

## Building

```sh
make build                    # all targets, whole workspace
make build PKG=hivekeep-keeper
make build-release            # release binaries
make doc                      # rustdoc, no deps
```

`CARGO_FLAGS` defaults to `--locked`. When several builds share one
checkout, give each its own `CARGO_TARGET_DIR`.

## Tests

| Kind | How to run | Notes |
|------|------------|-------|
| Unit and integration | `make test` or `make test PKG=<crate>` | Offline; runs on macOS and Linux |
| PostgreSQL | `HK_TEST_DATABASE_URL=postgres://... cargo test -p hivekeep-store -- --include-ignored` | Ignored without the env var |
| Root-only (Linux) | `make test-root` (`scripts/test/root.sh`) | Builds as you, then runs `--ignored` tests whose ignore reason starts with "needs root" under `sudo`. `ROOT_TEST_FILTER`, `ROOT_TEST_SKIP` and `ROOT_JUNIT_FILE` tune it. |
| End to end | `make e2e` (against `KUBECTL_CONTEXT`) or `make e2e-kind` | See below |
| apiserver-backed | needs `KUBEBUILDER_ASSETS` (etcd and kube-apiserver) | Used by `hivekeep-e2e`'s `testenv` |
| Python | `make test-python` (`scripts/verify/python.sh`) | Needs Python 3.10+ and network on first run. Builds a venv at `target/python-venv` (override with `HK_PYTHON_VENV`) from the benchmark requirements plus pytest, then runs each suite in its own pytest process: `scripts/threats`, `scripts/metrics`, and in `crates/hivekeep-bench` `analysis`, `automation`, `locust/unit_tests`, `nighthawk-ingress/tests` and `workloads/manifests/sweperf`. Extra arguments go to pytest. `make python-test` is an alias. |

Tests that need root, Linux, a cluster, PostgreSQL or a cloud bucket are
`#[ignore]`d, with the reason saying what they need.

### End-to-end suites

The suites live in `crates/hivekeep-e2e/tests/`: demo lifecycle,
networking, authz, security, identity, ingress auth, egress authz, egress
MITM, credential injection, snapshot credentials, combined volumes,
metrics, multi-actor, NetworkPolicy, parking, SandboxConfig and sizing.
Every cluster test is `#[ignore]`d and also needs `HK_E2E=1`.

```sh
make kind-up
scripts/install/hivekeep-kind.sh deploy hivekeep-system --e2e-tuning \
  --credential-provider='{"name":"k8s.io"}' \
  --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client
make install-kind-demo DEMO=counter
make install-kind-demo DEMO=egress
make e2e-kind                     # scripts/e2e/run-kind.sh
make e2e-cleanup                  # delete namespaces kept by failed runs
```

`scripts/e2e/run.sh` passes extra arguments through to `cargo test`
(arguments after `--` go to the test binary). The environment knobs are
`HK_E2E_PACKAGE`, `E2E_TEST_THREADS` (8), `E2E_TIMEOUT` (30m),
`E2E_JUNIT_FILE` (required when `CI=true`), `HK_E2E_STORAGE_CLASS`
(`csi-nfs-sc`), `HK_PREVIEW` and `KUBECTL_CONTEXT`. The lane selector
`HK_E2E_SANDBOX_CLASS=microvm` switches the sandbox backend. Fixture images come from `HK_E2E_IMAGE_REGISTRY` and
`HK_E2E_IMAGE_TAG`. `PLACEHOLDERS.md` lists every e2e setting.

## Code quality

```sh
make fmt          # scripts/update/fmt.sh (+ proto-fmt.sh with clang-format)
make fmt-check    # scripts/verify/fmt.sh
make lint         # clippy --all-targets -D warnings
make shellcheck   # scripts/ and demos/
make verify       # scripts/verify/all.sh, which runs every check and reports all failures
make update       # scripts/update/all.sh, which runs every generator
```

`scripts/verify/` contains:

| Script | Checks |
|--------|--------|
| `fmt.sh`, `clippy.sh` | rustfmt and clippy |
| `shellcheck.sh` | Shell scripts |
| `migrations.sh [base-ref]` | Migration naming and numbering, no transaction control, no edits to shipped migrations |
| `api-deps.sh` | `hivekeep-api` does not link object-store cloud clients |
| `licenses.sh` (+ `deny.toml`) | Dependency license allowlist |
| `metrics.sh` | Event registry under `crates/hivekeep-core/registry` with OpenTelemetry Weaver (skipped when absent) |
| `threats.sh` | `docs/threats.json` matches `docs/threat-model.md` |
| `proto-fmt.sh` | clang-format of `.proto` files |
| `python.sh` | Every Python test suite (what `make test-python` runs) |

After you edit the threat model, run `scripts/update/threats.sh`.

## Local cluster

```sh
make kind-up                 # KIND_CLUSTER_NAME, KIND_REGISTRY_PORT (5001), IP_FAMILY=ipv4|ipv6|dual
make install-kind
make install-kind-demo DEMO=counter
make csi-nfs                 # or csi-hostpath
make microvm-demo-kind       # counter-microvm end to end (needs KVM in the Kind node)
make kind-down
```

`make demo-images REGISTRY=<registry> TAG=<tag>` builds the demo workload
images. Developer settings for non-Kind clusters go in
`.hivekeep-dev-env.sh` (copy `scripts/dev-env.sh.example`).

## Running a component locally

Most binaries print their flags with `--help`. For quick experiments:

- `hivekeep-api --memory-store --authz-allow-all --keeper-endpoints node=host:port ...`
  runs without PostgreSQL or Kubernetes keeper discovery. An empty
  `--grpc-server-cred-bundle` serves plaintext.
- `hivekeep-keeper --insecure-dev --cluster-view-file view.json --object-store=local --object-store-dir /tmp/objs`
  runs without TLS, the Kubernetes API or a cloud bucket.
- `hivekeep-sandbox-gvisor --standalone-epoch 1 --no-tunnel` runs a worker
  without keeper registration or the node's identity bundles.
- `hivekeep-bench` has `fake-keeper` and `fake-workersync` for
  benchmarking the control plane on its own (`--fake-data-plane` at
  install time).

Every one of these switches is for development only.
