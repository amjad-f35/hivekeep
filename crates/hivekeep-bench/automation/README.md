# Benchmark automation

Scheduled, repeatable benchmark runs of a branch. A CronJob on an
**orchestration cluster** builds, installs, runs and tears down against a
separate **test cluster**, once per entry in [tests.yaml](tests.yaml). Each
runner uploads its own results: the locust runner (`locust/runner.py`) for
`type: locust`, and the Nighthawk runner (`nighthawk-ingress/runner.py`) for
`type: nighthawk-ingress`, the gate router capacity benchmark described in
[nighthawk-ingress/README.md](../nighthawk-ingress/README.md).

## How a run goes

1. The CronJob starts a pod: the orchestrator plus a Docker-in-Docker
   sidecar.
2. The orchestrator waits for Docker, shallow-clones `--repo` at `--branch`
   and records the commit.
3. For each entry it installs the entry's target cluster settings
   (`/etc/orchestrator/target-clusters/<name>.sh`) as the clone's
   `.hivekeep-dev-env.sh` and fetches the cluster's credentials.
4. It builds and pushes the runner image of each test type in use, tagged
   with the commit: `${HK_BUILD_REPO}/hivekeep-bench-locust:<commit>`
   and `${HK_BUILD_REPO}/hivekeep-bench-nighthawk-ingress:<commit>`.
5. It installs Hivekeep with `scripts/install/hivekeep.sh deploy
   hivekeep-system` (plus the entry's `installArgs`) and deploys the
   workloads with `crates/hivekeep-bench/workloads/deploy.sh --deploy`. The
   install sets `HK_API_AUTHZ_BOOTSTRAP_OWNERS` to the `hivekeep-client`
   and `benchmarking/benchmark-runner` ServiceAccounts, so the runner's calls
   pass authorization, and `HK_INGRESS_ALLOWED_SPIFFE_IDS` to the runner for
   the gate router. A `microvm` entry also gets
   `scripts/install-microvm-deps.sh --install` between the two. A
   `nighthawk-ingress` entry has the `gate-router` Deployment pinned to its
   `envoyCpu` right after the install (CPU requests = limits on both
   containers and Envoy's `--concurrency`), and the orchestrator waits for
   that rollout before the workloads go in.
6. It submits the entry's Job (`manifests/runner-job.yaml.tmpl` or
   `manifests/nighthawk-ingress-runner-job.yaml.tmpl`). A locust Job also
   gets `--metadata`: the target cluster's name, location and project,
   merged with `BENCHMARK_METADATA` (a JSON object) when set and with any
   `--metadata` of the entry's own `flags`, whose keys win.
7. It waits for the Job, prints its log, deletes it, and tears down the
   workloads, the micro-VM dependencies and Hivekeep.

## Sandbox class

`sandboxClass: gvisor | microvm` (default `gvisor`) sets the benchmark
WorkerPool's sandbox class and its worker image
(`hivekeep-sandbox-gvisor` or `hivekeep-sandbox-microvm`). A `microvm` test
cluster needs KVM-capable nodes, and the orchestrator's identity must be able
to write the micro-VM assets to the cluster's asset bucket.

## Setup

```bash
crates/hivekeep-bench/automation/setup.sh
```

The script takes the repository's `.hivekeep-dev-env.sh` as the target
cluster's settings and snapshots it to `scratch/target-clusters/<name>.sh`.
It asks for the target cluster's name (what `targetCluster` selects in
tests.yaml) and the GCP project of the orchestrator image registry, builds
and pushes the orchestrator image to
`gcr.io/<project>/hivekeep/hivekeep-benchmark-orchestrator:<short-commit>`
(with `-dirty` when this directory has uncommitted changes), and renders
`scratch/cronjob.yaml`, `scratch/test-list.yaml` and
`scratch/target-clusters.yaml`.

Fill in `--repo`, `--branch` and `--dest` in `scratch/cronjob.yaml`, then:

```bash
kubectl --context=<orchestration-cluster> apply \
  -f scratch/cronjob.yaml -f scratch/test-list.yaml -f scratch/target-clusters.yaml
```

Run it now instead of waiting for the schedule (`0 3 * * *`):

```bash
kubectl --context=<orchestration-cluster> -n hivekeep-benchmark \
  create job --from=cronjob/hivekeep-benchmark manual-$(date +%s)
```

## Test cluster

The test cluster needs the pod certificate APIs (Kubernetes 1.36 with the
`certificates.k8s.io/v1beta1` PodCertificateRequest and ClusterTrustBundle
APIs enabled, or 1.37+), Workload Identity, and an OpenTelemetry collector.
The scripts under `scripts/gcp/` create a suitable GKE cluster. The
orchestration cluster runs one pod at a time and needs only Workload
Identity.

## Permissions

Both ServiceAccounts come from the manifests (`cronjob.yaml` for the
orchestrator, the runner Job templates for the runner). Grant roles to each
one's Workload Identity principal:

```
principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/<NAMESPACE>/sa/<KSA>
```

- **Orchestrator** (`hivekeep-benchmark/hivekeep-benchmark-orchestrator`):
  `roles/container.admin` on the test cluster's project, since the install
  creates CRDs, ClusterRoles and Namespaces (`container.developer` lacks
  those), and `roles/artifactregistry.writer` on `HK_BUILD_REPO`.
- **Runner** (`benchmarking/benchmark-runner` on the test cluster):
  `roles/storage.objectCreator` on the results bucket.

## Changing the tests

`tests.yaml` reaches the orchestrator through a ConfigMap mounted at
`/etc/orchestrator/tests.yaml`, so changing the list needs no image rebuild:
re-render and re-apply `scratch/test-list.yaml`.

## Tests

```bash
cd crates/hivekeep-bench/automation && python3 -m unittest
```

They need PyYAML (`requirements.txt`) and read the install's NetworkPolicies
from `crates/hivekeep-setup/manifests/install/`. The Rust test
`crates/hivekeep-bench/tests/isolate_tuning.rs` checks that `ISOLATE_TUNING`
in `util.py` names exactly the fake keeper's tuning variables.
