# Installing Hivekeep

`hivekeep-setup` (crate `hivekeep-setup`) installs and removes Hivekeep. It
renders the manifests in `crates/hivekeep-setup/manifests/`, creates the
Secrets and config the control plane needs, builds or pins images, and
waits for each rollout. Run it from a checkout with
`cargo run --release -p hivekeep-setup -- <command>`, or use the wrapper
`scripts/install/hivekeep.sh <command>` (set `HK_SETUP_BIN` to use a
prebuilt binary).

## Prerequisites

- A Kubernetes cluster with the `PodCertificateRequest`,
  `ClusterTrustBundle` and `ClusterTrustBundleProjection` feature gates (and
  their APIs) enabled. `scripts/kind/up.sh` turns them on for Kind.
- Nodes for workers. gVisor workers need a Linux node with cgroup v2.
  Micro-VM workers also need `/dev/kvm`: the keeper registers it with the
  kubelet's device manager as the extended resource `hivekeep.io/kvm`
  (device plugin API v1beta1, over `/var/lib/kubelet/device-plugins/`), so
  the keeper DaemonSet must mount that directory and the node's `/dev` at
  `/host/dev`.
- An object-storage bucket (GCS or S3) for snapshots, unless you use the
  Kind profile's in-cluster bucket.
- A registry to push images to (`HK_BUILD_REPO`), or a set of pre-built
  images (`--image-repo`, `--image-tag`).
- `kubectl`, Docker with `buildx` (for builds from source), and `protoc`.

## Install the system

```sh
hivekeep-setup deploy hivekeep-system \
  --context my-cluster \
  --snapshot-location gs://my-bucket/hivekeep/ \
  --api-authz-bootstrap-owners user-or-sa-principal \
  --ingress-allowed-spiffe-ids spiffe://cluster.local/ns/clients/sa/frontend \
  --credential-provider '{"name":"k8s.io"}'
```

The command runs these steps in order:

1. Install the CRDs (`hivekeep.io/v1` WorkerPool, SandboxConfig,
   CSIDriverConfig) and RBAC, and create the `hivekeep-system` namespace and
   its NetworkPolicies.
2. Create the CA pools for the pod certificate controller (podidentity,
   servicedns, postgres), then deploy `podcertificate-controller` and wait
   for its ClusterTrustBundles.
3. Create the Actor identity material if it is missing: the
   `actor-id-ca-pool`, the actor CA trust bundle, the `actor-id-jwt-pool`
   (with its JWKS published), and `egress-mitm-ca-pool`. Existing pools
   are kept.
4. Create the `hivekeep-api` config, authentication config and environment
   ConfigMap and Secret.
5. Deploy PostgreSQL (unless an external database is configured), then
   `hivekeep-api` with its object-store plugin sidecar,
   `hivekeep-idp-server`, `hivekeep-controller`, the gate (`gate-router`,
   `gate-egress`), the keeper DaemonSet, the SandboxConfig admission policy,
   and the default gVisor SandboxConfig.
6. Write an install record (see [Records](#records)).

You can also run the steps one at a time:

| Command | Deploys |
|---------|---------|
| `deploy keeper` | The keeper DaemonSet |
| `deploy api-server` (alias `apiserver`) | `hivekeep-api` and `hivekeep-idp-server` |
| `deploy hivekeep-controller` (alias `controller`) | The controller and its CRDs |
| `deploy gate` | The router and egress gateway |
| `deploy podcertificate-controller` (alias `podcert`) | The pod certificate controller |
| `deploy sandboxconfig` | The SandboxConfig admission policy and the default gVisor SandboxConfig |
| `deploy postgres` | The single-replica PostgreSQL StatefulSet |
| `deploy demo <name>` | A bundled demo (`counter`, `counter-microvm`, `egress`, `egress-microvm`, `sandbox`, `multi-template`, `parking`, `jupyter`, `claude-code-multiplex`) |
| `deploy benchmarks` | The benchmark workloads and the locust stack |

`hivekeep-setup create <resource>` re-creates one Secret or config on an
existing cluster. The resources are `jwt-authority-pool`,
`actor-id-ca-pool`, `actor-id-ca-certs`, `egress-mitm-ca-pool`,
`podcertificate-controller-cas`, `api-server-env-vars`,
`api-authentication-config` and `api-config`.

`hivekeep-setup setup csi nfs|hostpath|both|none` installs the CSI drivers
used by external volumes. You can also install them with `deploy
hivekeep-system --setup-csi=<driver>`.

## Micro-VM workers

```sh
scripts/install-microvm-deps.sh --install    # assemble/stage assets, apply the microvm SandboxConfig
hivekeep-setup deploy demo counter-microvm
```

`scripts/microvm-assets/assemble.sh` downloads the Kata guest assets and
Cloud Hypervisor, stamps their versions and lists their SHA-256 sums.
`stage-to-gcs.sh` and `stage-to-rustfs.sh` upload them for the keeper to
fetch. `scripts/microvm/demo-kind.sh` does the whole bring-up on Kind.

## Worker pools

Workers are declared as WorkerPools:

```yaml
apiVersion: hivekeep.io/v1
kind: WorkerPool
metadata:
  name: general
  namespace: team-a-workers
spec:
  replicas: 3
  workerImage: <registry>/hivekeep-sandbox-gvisor:<tag>
  sandboxClasses:
  - name: gvisor          # optional configRef: {name: <SandboxConfig>}
  template:
    nodeSelector: {pool: sandbox}
    resources:
      limits: {cpu: "16", memory: 64Gi}
```

The controller creates a Deployment, a NetworkPolicy and a default worker
ServiceAccount for the pool. Worker pods are registered as Workers once
they have an IP and become ACTIVE when they are Ready. A pool waits to
roll out until every sandbox class resolves to a SandboxConfig. The class
condition shows why it is waiting.

## Configuration channels

Each setting can come from four places. Later ones override earlier ones:

1. the built-in default;
2. the environment (for example `HK_GATE_DATAPLANE`);
3. a config file (`--config` or `HK_CONFIG`);
4. a command-line flag (for example `--gate-dataplane`).

The config file has a fixed header and nested keys:

```yaml
apiVersion: install.hivekeep.io/v1alpha1
kind: HivekeepInstall
context: my-cluster
storage:
  snapshotLocation: gs://my-bucket/hivekeep/
gate:
  dataplane: envoy
api:
  logLevel: info
  authz:
    bootstrapOwners: system:serviceaccount:ops:admin
```

Unknown or duplicate keys are rejected. Each run prints a configuration
report that shows where every value came from, with secrets withheld.
`--no-report-defaults` limits the report to values that were set. Outside
the Kind profile, `.hivekeep-dev-env.sh` at the repository root is read too
(see `scripts/dev-env.sh.example`). Pass `--no-dev-env` or `NO_DEV_ENV=1`
to skip it.

Frequently used global settings:

| Flag | Env | Purpose |
|------|-----|---------|
| `--kind` | `HK_INSTALL_KIND` | Kind overlays, local registry, host-architecture builds |
| `--context`, `--kubeconfig` | `KUBECTL_CONTEXT`, `KUBECONFIG` | Target cluster |
| `--namespace` | `HK_NAMESPACE` | System namespace (default `hivekeep-system`) |
| `--build-repo`, `--build-platforms` | `HK_BUILD_REPO`, `HK_BUILD_PLATFORMS` | Build images from source and push them here |
| `--image-repo`, `--image-tag` | `HK_IMAGE_REPO`, `HK_IMAGE_TAG` | Use pre-built images instead |
| `--snapshot-location` | `SNAPSHOT_LOCATION` | Cluster-wide snapshot base URI |
| `--gate-dataplane` | `HK_GATE_DATAPLANE` | `envoy` (default and only value) |
| `--credential-provider` | `HK_CREDENTIAL_PROVIDER` | JSON naming the egress credential provider, for example `{"name":"k8s.io"}` |
| `--ingress-allowed-spiffe-ids` | `HK_INGRESS_ALLOWED_SPIFFE_IDS` | Router client allowlist |
| `--api-authz-bootstrap-owners` | `HK_API_AUTHZ_BOOTSTRAP_OWNERS` | Bootstrap owners (default: the `hivekeep-client` ServiceAccount) |
| `--api-expected-jwt-issuer` | `EXPECTED_JWT_ISSUER` | ServiceAccount token issuer to trust |
| `--actor-jwt-issuer`, `--actor-jwt-algorithm` | `ACTOR_JWT_ISSUER`, `ACTOR_JWT_ALGORITHM` | Actor JWT `iss` (default `https://idp.<namespace>.svc`) and key type |
| `--preview` | `HK_PREVIEW` | Preview gates injected into every component |
| `--cluster-size` | `HK_INSTALL_CLUSTER_SIZE` | `size0` or `size10` resource profile |
| `--cordon-system-infra-components` | `HK_INSTALL_CORDON_SYSTEM_INFRA_COMPONENTS` | Keep system pods on dedicated nodes |
| `--rollout-timeout` | `HK_INSTALL_ROLLOUT_TIMEOUT` | How long to wait for each rollout |
| `--e2e-tuning` | `HK_INSTALL_E2E_TUNING` | Short snapshot delays, for the e2e suites |
| `--fake-data-plane` | `HK_INSTALL_FAKE_DATA_PLANE` | Fake keeper and worker sync, for control-plane benchmarks |

## Records

After each run, `hivekeep-setup` writes an install record under the user
cache directory (or `--record-dir`, `HK_RECORD_DIR`). It holds the settings
and the version that were applied. A successful run replaces the previous
record. Failed runs are kept alongside it.

## GCP scaffolding

`hivekeep-setup gcp` prepares a GCP project:

```sh
hivekeep-setup gcp bootstrap                 # everything below, in order
hivekeep-setup gcp enable apis
hivekeep-setup gcp create cluster            # GKE with nested virtualization and pod-cert projection
hivekeep-setup gcp create bucket             # snapshot and asset buckets
hivekeep-setup gcp create iam                # snapshot transfer service account and grants
hivekeep-setup gcp create cloudsql           # private-IP Cloud SQL with IAM auth
hivekeep-setup gcp create dashboards         # Cloud Monitoring dashboards
```

`scripts/gcp/teardown.sh` reverses the bootstrap, one step at a time or all
at once with `--all`.

## Removing Hivekeep

```sh
hivekeep-setup delete demo <name>
hivekeep-setup delete gate
hivekeep-setup delete hivekeep-system
hivekeep-setup delete all            # every demo, then the system
```

Deleting the system releases SandboxConfig finalizers and removes the
ClusterTrustBundles it created. Snapshot buckets are left alone.

## Publishing

`hivekeep-setup publish worker-images`, `publish release-images` (needs
`HK_VERSION` or a release `git describe`, and `--overwrite` to replace
existing images) and `publish release-binaries` (`hivectl` binaries plus
checksums into `bin/release`) build release artifacts.
