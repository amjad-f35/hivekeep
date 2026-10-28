# Hivekeep

Hivekeep is a zero-trust sandbox runtime for Kubernetes. It runs many
long-lived, stateful **Actors** (agents, notebooks, interactive sandboxes) on
a much smaller pool of warm **Workers**. An idle Actor costs no compute: it is
checkpointed into a snapshot and its Worker slot goes to someone else. The
next request for it resumes it from that snapshot, often on a different
Worker, with its processes, files and memory intact.

What it provides:

- **Multiplexing on warm workers.** One Worker pod hosts many Actors. The
  control plane places each resume on the less loaded of two eligible
  Workers and releases the slot on suspend.
- **Snapshot and restore.** Suspends capture volumes, the container root
  filesystem, or the whole process memory (the snapshot *fidelity*).
  Snapshots are kept on the node first and uploaded to object storage
  (GCS or S3) in the background. Tags give a snapshot a stable name, and
  every template gets a pre-warmed *golden* snapshot.
- **Two isolation backends.** `hivekeep-sandbox-gvisor` runs each Actor in a
  gVisor sandbox (`runsc`). `hivekeep-sandbox-microvm` runs each Actor in its
  own Cloud Hypervisor micro-VM, driven through the Kata guest agent.
- **Per-Actor identity.** Every Actor has a SPIFFE ID
  (`spiffe://<trust-domain>/actor/<hive>/<actor>`), and the control plane
  can mint X.509 certificates and JWTs for it.
- **Credential-free sandboxes.** All Actor traffic goes through the
  **gate**. Ingress is mTLS-only and resumes suspended Actors on demand.
  Egress is deny-by-default per Actor, and the egress gateway can put
  credentials into outbound requests so that secrets never enter the
  sandbox.

## Status

Hivekeep is **pre-1.0** (workspace version `0.1.0`). APIs, CRDs, flags and
on-disk formats can still change without a deprecation period. Do not run
it for untrusted tenants yet. Some known gaps in the current tree:

- Every binary starts telemetry through `hivekeep-telemetry` (JSON logs
  with trace correlation, Prometheus metrics, OTLP export when an endpoint
  is configured; see [docs/observability.md](docs/observability.md)). The
  end-to-end metrics suite has not yet been run against a live collector.

Values that must be set or confirmed before production are listed in
[`PLACEHOLDERS.md`](PLACEHOLDERS.md).

## Workspace layout

Rust workspace, edition 2021, MSRV 1.98. Every crate lives under `crates/`.

| Crate | Role |
|-------|------|
| `hivekeep-proto` | Protobuf and gRPC definitions: `hivekeep.api.v1` (Control, WorkerService), `hivekeep.keeper.v1` (Keeper, WorkerSupport), `hivekeep.worker.v1` (Worker), `hivekeep.config.v1` |
| `hivekeep-core` | Shared domain logic: names, references, validation, defaults, the Actor state machine, snapshot addressing, preview gates, runtime compatibility, telemetry attribute keys |
| `hivekeep-store` | The `Store` contract with in-memory and PostgreSQL backends, migrations, leases, the worker event outbox |
| `hivekeep-identity` | Keys, CA pools, SPIFFE certificate minting, JWT authority pools and verification, trust bundles |
| `hivekeep-api` | `hivekeep-api`, the control plane: Control and WorkerService, scheduling, lifecycle workflows, authn/authz |
| `hivekeep-controller` | `hivekeep-controller`: WorkerPool, SandboxConfig protection, NetworkPolicy and egress-trust reconcilers, worker registry sync |
| `hivekeep-k8s` | The `hivekeep.io/v1` CRDs (WorkerPool, SandboxConfig, CSIDriverConfig), informers, an in-memory fake API server, Kubernetes helpers |
| `hivekeep-node` | Node-side seams: contexts, clocks, work queues, node paths, image/object-store/cluster-view traits, kubelet credential providers |
| `hivekeep-keeper` | `hivekeep-keeper`, the node agent (DaemonSet): Keeper and WorkerSupport services, snapshot tracker, caches |
| `hivekeep-imagecache` | Node-local OCI layer cache: registry pulls, verified unpacking, overlay composition, eviction |
| `hivekeep-objectstore` | GCS and S3 snapshot transfer, the sparse zstd snapshot format, and the `hivekeep-objectstore-plugin` sidecar |
| `hivekeep-sandbox-gvisor` | `hivekeep-sandbox-gvisor`, the gVisor worker |
| `hivekeep-sandbox-microvm` | `hivekeep-sandbox-microvm`, the Cloud Hypervisor micro-VM worker |
| `hivekeep-gate` | `hivekeep-gate`: the ingress router and egress gateway (Envoy ext_proc and xDS), the SDS certificate minter, and the sandbox tunnel library |
| `hivekeep-credential-provider` | `hivekeep-credential-provider`: resolves `hk-secret://k8s.io/...` references to Kubernetes Secret values for egress credential injection |
| `hivekeep-podcert-controller` | `hivekeep-podcert-controller`: signs PodCertificateRequests for the pod-identity, service-DNS and PostgreSQL signers |
| `hivekeep-idp` | `hivekeep-idp`: OIDC discovery and JWKS for verifying Actor JWTs |
| `hivekeep-ctl` | `hivectl`, the command-line client |
| `hivekeep-setup` | `hivekeep-setup`, the installer (Kubernetes manifests, secrets, demos, GCP scaffolding) |
| `hivekeep-telemetry` | Logging, OpenTelemetry bootstrap, gRPC interceptors, `hivekeep-otlp-relay` |
| `hivekeep-e2e` | End-to-end harness, fixtures (`hivekeep-e2e-probe`, `hivekeep-e2e-testserver`) and suites |
| `hivekeep-bench` | Load and benchmark tools (glutton, locust worker, egress bench, fake data plane) |
| `hivekeep-devtools` | `hivekeep-apitool` (API lint) and `hivekeep-junittool` (CI report checks) |
| `hivekeep-demo-*` | Demo workloads: `counter`, `egress`, `fspersist`, `sandbox`, `multiplex-ui` |

The other top-level directories are `demos/` (demo walkthroughs and
Dockerfiles), `scripts/` (Kind, install, e2e, verify and update helpers),
`images/`, and `docs/`.

## Quickstart on Kind

You need Docker, `kubectl`, a Rust toolchain and `protoc`. Run `make protoc`
to fetch the pinned `protoc`, and `make tools` to install the pinned `kind`,
`cargo-nextest`, `cargo-deny` and `shellcheck` into `bin/tools`.

```sh
make kind-up                  # Kind cluster plus a local registry on localhost:5001
make install-kind             # hivekeep-setup deploy hivekeep-system, Kind profile
make install-kind-demo DEMO=counter
```

`make install-kind` wraps `scripts/install/hivekeep-kind.sh`. That script
runs `hivekeep-setup` with the Kind profile, builds the images and pushes
them to the local registry, deploys the bundled Kubernetes Secrets
credential provider, and adds the
`spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client`
identity to the ingress allowlist.

To talk to the control plane from your machine, install `hivectl` and
forward the API Service:

```sh
cargo install --path crates/hivekeep-ctl
kubectl -n hivekeep-system port-forward svc/hivekeep-api 8443:443 &
kubectl -n hivekeep-system create token hivekeep-client \
  --audience hivekeep-api.hivekeep-system.svc > /tmp/hk-token
export HK_ENDPOINT=https://localhost:8443 HK_TOKEN_FILE=/tmp/hk-token
export HK_CA_FILE=<PEM of the servicedns.podcert.hivekeep.io trust bundle>

hivectl get actor-templates -A
hivectl create actor my-counter -H hivekeep-demo-counter --template counter
hivectl resume actor my-counter -H hivekeep-demo-counter
hivectl get actor my-counter -H hivekeep-demo-counter
hivectl suspend actor my-counter -H hivekeep-demo-counter --mode=sync
hivectl delete actor my-counter -H hivekeep-demo-counter
```

By default, the install makes the `hivekeep-client` ServiceAccount a
bootstrap owner, so its token can do everything. That default is for
development only: see [docs/authorization.md](docs/authorization.md).
`hivectl proxy actor` reaches Actors through the gate. It needs a client
certificate for an allowlisted SPIFFE ID (see
[docs/networking.md](docs/networking.md)). Remove everything with
`make kind-down`.

## Build, test, lint

```sh
make build                 # cargo build --locked --workspace --all-targets
make test                  # cargo test --locked --workspace
make test PKG=hivekeep-api # scope any cargo target to one crate
make lint                  # clippy, warnings as errors
make fmt / make fmt-check  # rustfmt (and clang-format for .proto, if installed)
make verify                # every check in scripts/verify
make test-root             # Linux only: root-only #[ignore] tests under sudo
make e2e-kind              # end-to-end suites against the Kind cluster
make help                  # everything else
```

The workspace builds and unit-tests on macOS too. Linux-only code is behind
`#[cfg(target_os = "linux")]`. Tests that need PostgreSQL read
`HK_TEST_DATABASE_URL` and are `#[ignore]`d by default. See
[docs/development.md](docs/development.md).

## Documentation

Start with [docs/architecture.md](docs/architecture.md) and the
[glossary](docs/glossary.md). Operators should then read
[operator-install.md](docs/operator-install.md),
[configuration.md](docs/configuration.md) and [security.md](docs/security.md).
API users should read [api-guide.md](docs/api-guide.md). The
[docs index](docs/README.md) lists every page.

To contribute, see [CONTRIBUTING.md](CONTRIBUTING.md). To report a
vulnerability, see [SECURITY.md](SECURITY.md).

## License

Apache-2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
