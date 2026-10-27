#!/usr/bin/env bash
# One development run of the gate router capacity benchmark (about 8-10 min).
#
# Runs only the benchmark against a cluster you prepared (Hivekeep and the
# benchmark workers, see README.md): pins the router if its CPU differs,
# builds the runner image from the working tree (uncommitted changes
# included), submits the Job, follows it, and prints capacity.json.
#
#   crates/hivekeep-bench/nighthawk-ingress/run-dev.sh --envoy-cpu 2
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"
# shellcheck source=../../../scripts/lib/venv.sh
. "${HK_ROOT}/scripts/lib/venv.sh"
cd "${HK_ROOT}"

BENCH_DIR="crates/hivekeep-bench"
ENVOY_CPU=2
ACTORS=100
TAIL_LATENCY_SLO_MS=25
HIVE="ingress-benchmark"
DEST=""
VENV="${HK_BENCH_VENV:-${HOME}/.venvs/hivekeep-bench}"
NAMESPACE="benchmarking"
SYSTEM_NAMESPACE="hivekeep-system"

usage() {
  cat <<EOF
Usage: $0 [options]
  --envoy-cpu N             router CPU pin, the independent variable (default ${ENVOY_CPU})
  --actors N                Actor fleet size; needs at least one Running worker (default ${ACTORS})
  --tail-latency-slo-ms N   SLO bound in ms; 0 disables it (default ${TAIL_LATENCY_SLO_MS})
  --hive NAME               hive of the benchmark Actors (default ${HIVE})
  --dest gs://...           results root (default gs://\$BUCKET_NAME/nighthawk-ingress-results)
EOF
  exit "${1:-1}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --envoy-cpu) ENVOY_CPU="$2"; shift 2 ;;
    --actors) ACTORS="$2"; shift 2 ;;
    --tail-latency-slo-ms) TAIL_LATENCY_SLO_MS="$2"; shift 2 ;;
    --hive) HIVE="$2"; shift 2 ;;
    --dest) DEST="$2"; shift 2 ;;
    -h | --help) usage 0 ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

# --- preflight; every failure says how to fix it -----------------------------
[[ -f "${HK_DEV_ENV_FILE}" ]] || hk_die "${HK_DEV_ENV_FILE} not found; start from scripts/dev-env.sh.example"
hk_load_dev_env
hk_require_env PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION HK_BUILD_REPO BUCKET_NAME
hk_require_cmd docker kubectl gcloud
DEST="${DEST:-gs://${BUCKET_NAME}/nighthawk-ingress-results}"

docker info >/dev/null 2>&1 || hk_die "the Docker daemon is not running (the runner image is built locally)"

hk_kubectl get deployment gate-router -n "${SYSTEM_NAMESPACE}" >/dev/null 2>&1 \
  || hk_die "Hivekeep is not installed on ${CLUSTER_NAME}; run:
  scripts/install/hivekeep.sh deploy hivekeep-system --credential-provider='{\"name\":\"k8s.io\"}' --ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner"

# A worker hosts many Actors, so the fleet may outnumber the workers.
RUNNING_WORKERS="$(hk_kubectl get pods -n benchmark-workloads --no-headers 2>/dev/null | grep -c ' Running ' || true)"
((RUNNING_WORKERS >= 1)) || hk_die "no worker is Running in benchmark-workloads; run:
  ${BENCH_DIR}/workloads/deploy.sh --deploy --worker-count ${ACTORS} --sandbox-class gvisor"

# Every value below is used as shown, defaults included; read it before the
# router is pinned.
cat <<EOF
>>> run config:
      cluster:              ${CLUSTER_NAME} (${CLUSTER_LOCATION})
      envoy_cpu:            ${ENVOY_CPU}
      actors:               ${ACTORS}
      workers running:      ${RUNNING_WORKERS}
      tail_latency_slo_ms:  ${TAIL_LATENCY_SLO_MS}
      hive:                 ${HIVE}
      dest:                 ${DEST}
EOF

# The automation's modules render the Job and patch the router; they need
# PyYAML.
hk_venv_ensure "${VENV}"
hk_venv_install "${VENV}" "${BENCH_DIR}/automation/requirements.txt"
PY="${VENV}/bin/python3"
export PYTHONPATH="${BENCH_DIR}/automation"

# --- pin the router when its CPU differs --------------------------------------
CURRENT_CPU="$(hk_kubectl get deployment gate-router -n "${SYSTEM_NAMESPACE}" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].resources.limits.cpu}')"
if [[ "${CURRENT_CPU}" != "${ENVOY_CPU}" ]]; then
  echo ">>> pinning the router: envoy cpu '${CURRENT_CPU:-unset}' -> ${ENVOY_CPU}"
  ENVOY_CPU="${ENVOY_CPU}" "${PY}" -c '
import os
from testtypes import nighthawk_ingress
nighthawk_ingress.pre_test({"nighthawk-ingress": {"envoyCpu": int(os.environ["ENVOY_CPU"])}})'
fi

# --- runner image from the working tree ----------------------------------------
SHA7="$(git rev-parse --short=7 HEAD)"
DIRTY=""
[[ -n "$(git status --porcelain)" ]] && DIRTY="-dirty"
TAG="quick-${SHA7}${DIRTY}-$(date +%H%M%S)"
IMAGE="${HK_BUILD_REPO}/hivekeep-bench-nighthawk-ingress:${TAG}"
echo ">>> building ${IMAGE} from the working tree"
# The clusters are amd64; an arm64 host would otherwise build an image the
# nodes cannot run.
docker build --platform linux/amd64 -f "${BENCH_DIR}/nighthawk-ingress/Dockerfile" -t "${IMAGE}" .
docker push "${IMAGE}"

# --- render and submit the Job --------------------------------------------------
NAME="ingress_routercap_envoy_${ENVOY_CPU}cpu"
JOB="runner-ingress-routercap-${ENVOY_CPU}cpu-quick-$(date +%H%M%S)"
export IMAGE JOB NAME DEST TAG ENVOY_CPU ACTORS TAIL_LATENCY_SLO_MS HIVE
"${PY}" - <<'PYEOF' | hk_kubectl apply -f -
import os

import orchestrator
from testtypes import nighthawk_ingress

test = {
    "name": os.environ["NAME"],
    "type": "nighthawk-ingress",
    "targetCluster": "dev",
    "duration": "30m",
    "workerCount": int(os.environ["ACTORS"]),
    "nighthawk-ingress": {
        "envoyCpu": int(os.environ["ENVOY_CPU"]),
        "hive": os.environ["HIVE"],
        "tailLatencySloMs": float(os.environ["TAIL_LATENCY_SLO_MS"]),
        "apiClientAuth": os.environ.get("HK_API_CLIENT_AUTH", "cert"),
    },
}
orchestrator.validate_and_normalize_tests([test])
subs = {
    "JOB_NAME": os.environ["JOB"],
    "IMAGE": os.environ["IMAGE"],
    "TAG": os.environ["TAG"],
    "NAME": test["name"],
    "DEST": os.environ["DEST"],
    **nighthawk_ingress.job_subs(test),
}
print(orchestrator.render_template(nighthawk_ingress.job_tmpl("crates/hivekeep-bench/automation/manifests"), subs))
PYEOF

# --- wait, streaming the runner's own lines --------------------------------------
# [service] and [adaptive] lines are Nighthawk's raw dumps; they are in the
# uploaded logs.txt and too noisy here.
echo ">>> waiting for job/${JOB} (progress only; the full log lands in logs.txt)"
(until hk_kubectl logs -f "job/${JOB}" -n "${NAMESPACE}" 2>/dev/null \
  | grep --line-buffered -vE '^\[(service|adaptive)\] '; do sleep 5; done) &
LOGS_PID=$!
STATUS="timeout"
for _ in $(seq 1 150); do # 150 x 10 s = 25 min
  S="$(hk_kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null)"
  F="$(hk_kubectl get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null)"
  [[ "${S}" == "1" ]] && { STATUS="complete"; break; }
  [[ "${F}" == "1" ]] && { STATUS="failed"; break; }
  sleep 10
done
kill "${LOGS_PID}" 2>/dev/null || true

# --- verdict -----------------------------------------------------------------------
if [[ "${STATUS}" == "complete" ]]; then
  CAPACITY="$(gcloud storage ls "${DEST}/runs/${NAME}/**/run_tag=${TAG}/capacity.json" 2>/dev/null | tail -1)"
  if [[ -n "${CAPACITY}" ]]; then
    echo ">>> ${CAPACITY}"
    gcloud storage cat "${CAPACITY}"
  fi
  hk_kubectl delete job "${JOB}" -n "${NAMESPACE}" >/dev/null
  echo ">>> ${NAME}: complete (tag ${TAG})"
else
  cat >&2 <<EOF
>>> ${NAME}: ${STATUS}; job/${JOB} kept for debugging:
      kubectl logs job/${JOB} -n ${NAMESPACE}
    (warm-up 503s with TLS errors mean an expired router pod certificate; fix:
      kubectl -n ${SYSTEM_NAMESPACE} rollout restart deployment/gate-router)
EOF
  exit 1
fi
