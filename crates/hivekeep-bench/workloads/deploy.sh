#!/usr/bin/env bash
# Deploys or removes the benchmark workloads: the benchmark-worker WorkerPool
# and the benchmark ActorTemplates in the benchmark-workloads hive, which the
# load classes create their Actors from. Run by
# scripts/benchmarking/deploy_locust.sh; usable on its own (see --help).
#
# Environment:
#   BUCKET_NAME or HK_BENCH_SNAPSHOT_URI  where the templates' snapshots go
#                         (HK_BENCH_SNAPSHOT_URI wins; default gs://$BUCKET_NAME)
#   HK_BUILD_REPO         registry the glutton image is pushed to and the
#                         worker image is pulled from
#   HK_BUILD_PLATFORMS    platforms to build for (default linux/amd64)
#   GLUTTON_IMAGE         use this glutton image instead of building one
#   WORKER_IMAGE          worker image (default
#                         $HK_BUILD_REPO/hivekeep-sandbox-<class>:<version>)
#   HK_ENV_IMAGE          sandbox guest image for the hk-env template
#   SWEPERF_IMAGE_REPO    registry path of the sweperf trajectory images
#   STRESS_IMAGE          image for the usermem/kernelmem templates
#   WORKLOAD_TEMPLATES    space-separated templates to deploy
#   HIVECTL               hivectl binary (default: on PATH, else built here);
#                         its connection settings come from HK_ENDPOINT etc.
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"
cd "${HK_ROOT}"
if declare -F hk_load_dev_env >/dev/null; then
  hk_load_dev_env
fi

BENCH_DIR="crates/hivekeep-bench"
MANIFEST_DIR="${BENCH_DIR}/workloads/manifests"
POOL_MANIFEST="${MANIFEST_DIR}/workloads.yaml.tmpl"
SWEPERF_TEMPLATE="${MANIFEST_DIR}/sweperf/sweperf-template.yaml.tmpl"
SWEPERF_TASKS="${MANIFEST_DIR}/sweperf/tasks.tsv"
HIVE="benchmark-workloads"

# The default template set. usermem and kernelmem are opt-in; glutton-storage
# joins when a StorageClass is given, since it needs a CSI driver; hk-env
# joins when HK_ENV_IMAGE is set.
read -r -a TEMPLATES <<<"${WORKLOAD_TEMPLATES:-sleep glutton glutton-durdir-data glutton-durdir-full}"

WORKER_COUNT=1
SANDBOX_CLASS="gvisor"
# ActorTemplate memory limit. Small by default: a bigger guest drags more page
# cache into every memory snapshot.
ACTOR_MEMORY="256Mi"
# ActorTemplate CPU limit; the scheduler books it against worker capacity.
ACTOR_CPU="1"
# Worker pod memory request and limit; empty leaves the pod unsized.
WORKER_MEMORY=""
STORAGE_CLASS_NAME="${STORAGE_CLASS_NAME:-}"
HK_ENV_IMAGE="${HK_ENV_IMAGE:-}"
HK_ENV_COMMAND="${HK_ENV_COMMAND:-[\"/app/hk-env-guest\", \"--listen=:80\"]}"
STRESS_IMAGE="${STRESS_IMAGE:-containerstack/alpine-stress:latest}"
SWEPERF_IMAGE_REPO="${SWEPERF_IMAGE_REPO:-}"
OTLP_ENDPOINT=""
WAIT_TIMEOUT_SECS=300
# Fake keeper tuning, as FAKE_KEEPER_<FLAG>=VALUE entries.
FAKE_KEEPER_ENV=()

usage() {
  cat <<'TXT'
Usage: workloads/deploy.sh --deploy|--delete [options]
  --worker-count N            WorkerPool replicas (default 1)
  --sandbox-class CLASS       gvisor | microvm (default gvisor)
  --actor-memory SIZE         ActorTemplate memory limit (default 256Mi)
  --actor-cpu CPU             ActorTemplate CPU limit (default 1)
  --worker-memory SIZE        memory request and limit of each worker pod
  --storage-class-name NAME   StorageClass for glutton-storage; setting it
                              deploys that template (env STORAGE_CLASS_NAME)
  --otlp-endpoint URL         where instrumented Actors send telemetry
                              (default: the hivekeep-otel-config ConfigMap)
  --wait-timeout SECONDS      how long to wait for workers and templates (300)
  --fake-keeper-env NAME=VALUE
                              on a fake data plane, set a fake-keeper flag,
                              e.g. FAKE_KEEPER_DELAY=500ms; repeatable
  -h, --help                  this help
TXT
}

# resolve_otlp_endpoint reads the endpoint the control plane itself uses, so
# Actor telemetry follows whatever collector the cluster runs. A missing
# ConfigMap means the system is not installed, which is an error.
resolve_otlp_endpoint() {
  [[ -n "${OTLP_ENDPOINT}" ]] && return 0
  OTLP_ENDPOINT="$(kubectl get configmap hivekeep-otel-config --namespace=hivekeep-system \
    -o jsonpath='{.data.OTEL_EXPORTER_OTLP_ENDPOINT}' 2>/dev/null || true)"
  [[ -n "${OTLP_ENDPOINT}" ]] \
    || hk_die "cannot read OTEL_EXPORTER_OTLP_ENDPOINT from the hivekeep-otel-config ConfigMap in hivekeep-system; install Hivekeep first or pass --otlp-endpoint"
}

# workspace_version is the version the installer tags images with.
workspace_version() {
  sed -n '/^\[workspace.package\]/,/^\[/s/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -n1
}

# ensure_hivectl finds hivectl once; it runs on every poll below.
ensure_hivectl() {
  if [[ -n "${HIVECTL:-}" ]]; then
    return 0
  fi
  # type -P looks at PATH only, not at the wrapper function below.
  if HIVECTL="$(type -P hivectl)"; then
    return 0
  fi
  hk_log "building hivectl"
  cargo build --release --locked -p hivekeep-ctl --bin hivectl
  HIVECTL="${CARGO_TARGET_DIR:-target}/release/hivectl"
}

hivectl() { "${HIVECTL}" "$@"; }

# build_glutton builds and pushes the glutton image and pins GLUTTON_IMAGE to
# the pushed digest, unless GLUTTON_IMAGE is already set.
build_glutton() {
  [[ -n "${GLUTTON_IMAGE:-}" ]] && return 0
  hk_require_env HK_BUILD_REPO
  local ref meta
  ref="${HK_BUILD_REPO}/hivekeep-bench-glutton:$(workspace_version)"
  meta="$(mktemp)"
  hk_log "building ${ref}"
  docker buildx build --push --platform "${HK_BUILD_PLATFORMS:-linux/amd64}" \
    --metadata-file "${meta}" -f "${BENCH_DIR}/images/glutton/Dockerfile" -t "${ref}" .
  GLUTTON_IMAGE="${ref%:*}@$(jq -r '."containerimage.digest"' "${meta}")"
  rm -f "${meta}"
}

# cpu_millicores turns 250m or 1.5 into millicores.
cpu_millicores() {
  local q="$1"
  if [[ "${q}" =~ ^([0-9]+)m$ ]]; then
    echo "${BASH_REMATCH[1]}"
  elif [[ "${q}" =~ ^([0-9]+)(\.([0-9]{1,3}))?$ ]]; then
    local frac="${BASH_REMATCH[3]}000"
    echo $((10#${BASH_REMATCH[1]} * 1000 + 10#${frac:0:3}))
  else
    hk_die "--actor-cpu ${q} is not a CPU quantity like 250m or 1.5"
  fi
}

# memory_bytes turns 256Mi or 1Gi into bytes.
memory_bytes() {
  local q="$1"
  if [[ "${q}" =~ ^([0-9]+)(Ki|Mi|Gi|Ti)?$ ]]; then
    local shift=0
    case "${BASH_REMATCH[2]}" in
      Ki) shift=10 ;;
      Mi) shift=20 ;;
      Gi) shift=30 ;;
      Ti) shift=40 ;;
    esac
    echo $((10#${BASH_REMATCH[1]} << shift))
  else
    hk_die "--actor-memory ${q} is not a memory quantity like 256Mi or 1Gi"
  fi
}

# substitute fills the ${...} placeholders of a manifest.
substitute() {
  local manifest="$1"
  local worker_resources=""
  # One flow-style line, so an unset WORKER_MEMORY leaves a blank line.
  if [[ -n "${WORKER_MEMORY}" ]]; then
    worker_resources="resources: {requests: {memory: \"${WORKER_MEMORY}\"}, limits: {memory: \"${WORKER_MEMORY}\"}}"
  fi
  sed -e "s|\${SNAPSHOT_BASE_URI}|${SNAPSHOT_BASE_URI}|g" \
      -e "s|\${WORKER_COUNT}|${WORKER_COUNT}|g" \
      -e "s|\${SANDBOX_CLASS}|${SANDBOX_CLASS}|g" \
      -e "s|\${WORKER_IMAGE}|${WORKER_IMAGE:-}|g" \
      -e "s|\${WORKER_RESOURCES}|${worker_resources}|g" \
      -e "s|\${OTLP_ENDPOINT}|${OTLP_ENDPOINT}|g" \
      -e "s|\${ACTOR_MEMORY_BYTES}|${ACTOR_MEMORY_BYTES:-0}|g" \
      -e "s|\${ACTOR_CPU_MILLICORES}|${ACTOR_CPU_MILLICORES:-0}|g" \
      -e "s|\${GLUTTON_IMAGE}|${GLUTTON_IMAGE:-}|g" \
      -e "s|\${HK_ENV_IMAGE}|${HK_ENV_IMAGE}|g" \
      -e "s|\${HK_ENV_COMMAND}|${HK_ENV_COMMAND}|g" \
      -e "s|\${STRESS_IMAGE}|${STRESS_IMAGE}|g" \
      -e "s|\${STORAGE_CLASS_NAME}|${STORAGE_CLASS_NAME}|g" \
      "${manifest}"
}

# sweperf_task prints "steps image" for a sweperf task, or fails.
sweperf_task() {
  local want="$1" task steps image
  while IFS=$'\t' read -r task steps image; do
    [[ -z "${task}" || "${task}" == \#* ]] && continue
    if [[ "${task}" == "${want}" ]]; then
      if [[ -z "${image}" ]]; then
        [[ -n "${SWEPERF_IMAGE_REPO}" ]] || hk_die "sweperf-${want} needs SWEPERF_IMAGE_REPO (or an image in ${SWEPERF_TASKS})"
        image="${SWEPERF_IMAGE_REPO}/sweperf:${task}"
      fi
      echo "${steps} ${image}"
      return 0
    fi
  done <"${SWEPERF_TASKS}"
  hk_die "no sweperf task ${want} in ${SWEPERF_TASKS}"
}

# render_template prints the filled ActorTemplate manifest for NAME.
render_template() {
  local name="$1"
  case "${name}" in
    sweperf-*)
      local task="${name#sweperf-}" found steps image
      # A separate assignment, so a failed lookup stops the script.
      found="$(sweperf_task "${task}")" || exit 1
      read -r steps image <<<"${found}"
      substitute "${SWEPERF_TEMPLATE}" \
        | sed -e "s|\${SWEPERF_TASK}|${task}|g" \
              -e "s|\${SWEPERF_CONTAINER}|${task%%-*}|g" \
              -e "s|\${SWEPERF_STEPS}|${steps}|g" \
              -e "s|\${SWEPERF_IMAGE}|${image}|g"
      ;;
    *)
      local manifest="${MANIFEST_DIR}/${name}-template.yaml.tmpl"
      [[ -f "${manifest}" ]] || hk_die "no manifest for template ${name}: ${manifest} not found"
      substitute "${manifest}"
      ;;
  esac
}

# wait_template_ready polls until a template's golden snapshot exists,
# failing at once when the template reports an error.
wait_template_ready() {
  local template="$1"
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECS))
  local json tag err
  while ((SECONDS < deadline)); do
    if json="$(hivectl get actor-template "${template}" -H "${HIVE}" -o json 2>/dev/null)"; then
      tag="$(jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty' <<<"${json}")"
      [[ -n "${tag}" ]] && return 0
      err="$(jq -r '.status.goldenSnapshotStatus.errorMessage // empty' <<<"${json}")"
      [[ -n "${err}" ]] && hk_die "actor template ${HIVE}/${template} failed: ${err}"
    fi
    sleep 5
  done
  hk_die "timed out waiting for the golden snapshot of ${HIVE}/${template}"
}

# fake_data_plane reports whether the install serves WorkerPools with fake
# Workers.
fake_data_plane() {
  [[ -n "$(kubectl get deployment/fake-workersync --namespace=hivekeep-system -o name --ignore-not-found)" ]]
}

# tune_fake_keeper writes FAKE_KEEPER_ENV, and only that, to the
# fake-keeper-tuning ConfigMap the fake keeper reads its flags from, and
# restarts the fake keeper when the ConfigMap changed.
tune_fake_keeper() {
  local before after entry ds
  before="$(kubectl get configmap fake-keeper-tuning --namespace=hivekeep-system -o jsonpath='{.data}' --ignore-not-found)"
  local literals=()
  for entry in "${FAKE_KEEPER_ENV[@]+"${FAKE_KEEPER_ENV[@]}"}"; do
    literals+=("--from-literal=${entry}")
  done
  kubectl create configmap fake-keeper-tuning --namespace=hivekeep-system "${literals[@]+"${literals[@]}"}" \
    --dry-run=client -o yaml | kubectl apply -f -
  after="$(kubectl get configmap fake-keeper-tuning --namespace=hivekeep-system -o jsonpath='{.data}')"
  [[ "${before}" == "${after}" ]] && return 0
  hk_log "restarting the fake keeper with its new tuning"
  for ds in $(kubectl get daemonset --namespace=hivekeep-system -l app=keeper -o name); do
    kubectl rollout restart --namespace=hivekeep-system "${ds}"
    kubectl rollout status --namespace=hivekeep-system "${ds}" --timeout="${WAIT_TIMEOUT_SECS}s"
  done
}

deploy() {
  hk_require_cmd kubectl jq docker
  local template
  # Render every template before anything on the cluster changes, so a
  # missing manifest or image fails first.
  ACTOR_CPU_MILLICORES="$(cpu_millicores "${ACTOR_CPU}")"
  ACTOR_MEMORY_BYTES="$(memory_bytes "${ACTOR_MEMORY}")"
  build_glutton
  for template in "${TEMPLATES[@]}"; do
    render_template "${template}" >/dev/null
  done
  if [[ -z "${WORKER_IMAGE:-}" ]]; then
    hk_require_env HK_BUILD_REPO
    WORKER_IMAGE="${HK_BUILD_REPO}/hivekeep-sandbox-${SANDBOX_CLASS}:$(workspace_version)"
  fi
  resolve_otlp_endpoint
  # Before the pool, so its fake Workers are served by the tuned fake keeper.
  if fake_data_plane; then
    tune_fake_keeper
  elif ((${#FAKE_KEEPER_ENV[@]} > 0)); then
    hk_die "--fake-keeper-env needs Hivekeep installed with the fake data plane"
  fi
  hk_log "deploying workloads (workers=${WORKER_COUNT}, actor_memory=${ACTOR_MEMORY}, actor_cpu=${ACTOR_CPU}, worker_memory=${WORKER_MEMORY:-unset}, storage_class=${STORAGE_CLASS_NAME:-none}, otlp=${OTLP_ENDPOINT})"
  substitute "${POOL_MANIFEST}" | kubectl apply -f -
  hk_log "waiting for the worker pool (timeout ${WAIT_TIMEOUT_SECS}s)"
  if fake_data_plane; then
    # Fake Workers have no Deployment; fake-workersync reports the ready
    # count in the pool's status.
    if ((WORKER_COUNT > 0)); then
      kubectl wait --for=jsonpath='{.status.readyReplicas}'="${WORKER_COUNT}" workerpool/benchmark-worker \
        --namespace="${HIVE}" --timeout="${WAIT_TIMEOUT_SECS}s"
    fi
  else
    kubectl wait --for=create deployment/benchmark-worker --namespace="${HIVE}" --timeout="${WAIT_TIMEOUT_SECS}s"
    kubectl rollout status deployment/benchmark-worker --namespace="${HIVE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  fi

  # A template's hive must exist when it is created.
  hivectl create hive "${HIVE}" >/dev/null 2>&1 || hivectl get hive "${HIVE}" >/dev/null

  # Templates are immutable, and per-run values (OTLP endpoint, sandbox
  # class) change between runs, so each is deleted and created anew. The
  # benchmark deletes its Actors between runs, so nothing depends on them.
  for template in "${TEMPLATES[@]}"; do
    hivectl delete actor-template "${template}" -H "${HIVE}" >/dev/null 2>&1 || true
    render_template "${template}" | hivectl create actor-template -f -
  done
  for template in "${TEMPLATES[@]}"; do
    hk_log "waiting for the ${HIVE}/${template} golden snapshot"
    wait_template_ready "${template}"
  done
}

delete() {
  hk_require_cmd kubectl
  local template
  hk_log "deleting workloads"
  for template in "${TEMPLATES[@]}"; do
    hivectl delete actor-template "${template}" -H "${HIVE}" >/dev/null 2>&1 || true
  done
  hivectl delete hive "${HIVE}" >/dev/null 2>&1 \
    || hk_log "hive ${HIVE} not deleted (absent or not empty)"
  WORKER_IMAGE="${WORKER_IMAGE:-unused}"
  substitute "${POOL_MANIFEST}" | kubectl delete --ignore-not-found -f -
}

[[ $# -gt 0 ]] || { usage >&2; exit 1; }

action=""
while [[ $# -gt 0 ]]; do
  arg="$1"
  value=""
  if [[ "${arg}" == --*=* ]]; then
    value="${arg#*=}"
    arg="${arg%%=*}"
  fi
  case "${arg}" in
    --deploy) action=deploy ;;
    --delete) action=delete ;;
    -h | --help) usage; exit 0 ;;
    --worker-count | --sandbox-class | --otlp-endpoint | --actor-memory | --actor-cpu | --worker-memory | --storage-class-name | --wait-timeout | --fake-keeper-env)
      if [[ -z "${value}" ]]; then
        [[ $# -ge 2 ]] || hk_die "${arg} needs a value"
        value="$2"
        shift
      fi
      case "${arg}" in
        --worker-count) WORKER_COUNT="${value}" ;;
        --sandbox-class) SANDBOX_CLASS="${value}" ;;
        --otlp-endpoint) OTLP_ENDPOINT="${value}" ;;
        --actor-memory) ACTOR_MEMORY="${value}" ;;
        --actor-cpu) ACTOR_CPU="${value}" ;;
        --worker-memory) WORKER_MEMORY="${value}" ;;
        --storage-class-name) STORAGE_CLASS_NAME="${value}" ;;
        --wait-timeout) WAIT_TIMEOUT_SECS="${value}" ;;
        --fake-keeper-env) FAKE_KEEPER_ENV+=("${value}") ;;
      esac
      ;;
    *) usage >&2; hk_die "unknown option $1" ;;
  esac
  shift
done

[[ -n "${action}" ]] || { usage >&2; exit 1; }
case "${SANDBOX_CLASS}" in
  gvisor | microvm) ;;
  *) hk_die "--sandbox-class must be gvisor or microvm, got '${SANDBOX_CLASS}'" ;;
esac
[[ "${WORKER_COUNT}" =~ ^[0-9]+$ ]] || hk_die "--worker-count must be a whole number, got '${WORKER_COUNT}'"
[[ "${WAIT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] || hk_die "--wait-timeout must be whole seconds, got '${WAIT_TIMEOUT_SECS}'"
for entry in "${FAKE_KEEPER_ENV[@]+"${FAKE_KEEPER_ENV[@]}"}"; do
  [[ "${entry}" =~ ^FAKE_KEEPER_[A-Z_]+=.+$ ]] \
    || hk_die "--fake-keeper-env must be FAKE_KEEPER_<FLAG>=VALUE, e.g. FAKE_KEEPER_DELAY=500ms, got '${entry}'"
done

# The optional templates join the default set when configured; delete always
# includes them, since deleting an absent template is a no-op.
if [[ -z "${WORKLOAD_TEMPLATES:-}" ]]; then
  if [[ -n "${STORAGE_CLASS_NAME}" || "${action}" == delete ]]; then
    TEMPLATES+=(glutton-storage)
  fi
  if [[ -n "${HK_ENV_IMAGE}" || "${action}" == delete ]]; then
    TEMPLATES+=(hk-env)
  fi
fi
if [[ "${action}" == deploy ]]; then
  [[ " ${TEMPLATES[*]} " != *" glutton-storage "* || -n "${STORAGE_CLASS_NAME}" ]] \
    || hk_die "the glutton-storage template needs --storage-class-name"
  [[ " ${TEMPLATES[*]} " != *" hk-env "* || -n "${HK_ENV_IMAGE}" ]] \
    || hk_die "the hk-env template needs HK_ENV_IMAGE"
  if [[ -n "${HK_BENCH_SNAPSHOT_URI:-}" ]]; then
    SNAPSHOT_BASE_URI="${HK_BENCH_SNAPSHOT_URI%/}"
  else
    hk_require_env BUCKET_NAME
    SNAPSHOT_BASE_URI="gs://${BUCKET_NAME}"
  fi
else
  SNAPSHOT_BASE_URI="${HK_BENCH_SNAPSHOT_URI:-gs://${BUCKET_NAME:-unused}}"
fi

ensure_hivectl
"${action}"
