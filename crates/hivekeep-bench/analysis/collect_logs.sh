#!/usr/bin/env bash
# Dumps the node-side logs phase_report.py reads: every keeper pod (the
# keeper's timing breakdowns) and every worker pod (the sandbox runtimes
# write theirs to the worker's stdout). Point --since-time (RFC 3339) or
# --since at the start of the run so the report covers exactly that run.
#
# kubectl logs returns only a container's current log file; once the kubelet
# rotates it under load the earlier records are gone, so collect soon after
# the run. A container that restarted mid-run is dumped twice, its previous
# log as <pod>.previous.log. On GKE, Cloud Logging keeps every record past
# rotation and teardown, and phase_report.py reads `gcloud logging read
# --format json` output directly.
#
# Reads .hivekeep-dev-env.sh unless NO_DEV_ENV is set, and honors
# KUBECTL_CONTEXT, like the other scripts.
#
# Usage: collect_logs.sh --dest DIR [--since 30m | --since-time 2026-09-24T18:00:00Z]
#                        [--namespace hivekeep-system] [--worker-namespace benchmark-workloads]
set -uo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"
hk_load_dev_env

DEST=""
SINCE="1h"
SINCE_TIME=""
NAMESPACE="hivekeep-system"
WORKER_NAMESPACE="benchmark-workloads"

usage() {
  echo "usage: $0 --dest DIR [--since 30m | --since-time RFC3339] [--namespace ${NAMESPACE}] [--worker-namespace ${WORKER_NAMESPACE}]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  [[ $# -ge 2 ]] || usage
  case "$1" in
    --dest) DEST="$2" ;;
    --since) SINCE="$2" ;;
    --since-time) SINCE_TIME="$2" ;;
    --namespace) NAMESPACE="$2" ;;
    --worker-namespace) WORKER_NAMESPACE="$2" ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
  shift 2
done
[[ -n "${DEST}" ]] || usage
mkdir -p "${DEST}"

if [[ -n "${SINCE_TIME}" ]]; then
  WINDOW=(--since-time="${SINCE_TIME}")
else
  WINDOW=(--since="${SINCE}")
fi

# collect NAMESPACE SELECTOR: dumps every matching pod. A pod that cannot be
# read (still starting, evicted) costs only itself, so each kubectl call
# warns and moves on.
collect() {
  local ns="$1" selector="$2" pods pod name restarts out
  if ! pods="$(hk_kubectl get pods -n "${ns}" -l "${selector}" -o name)"; then
    hk_warn "could not list pods in ${ns} (${selector}); skipping"
    return 0
  fi
  for pod in ${pods}; do
    name="${pod#pod/}"
    out="${DEST}/${ns}-${name}.log"
    echo "collecting ${ns}/${name} (${WINDOW[*]})"
    hk_kubectl logs -n "${ns}" "${name}" "${WINDOW[@]}" --timestamps=false >"${out}" \
      || { hk_warn "kubectl logs ${ns}/${name} failed; skipping"; rm -f "${out}"; }
    # --previous is valid only after a restart.
    restarts="$(hk_kubectl get pod -n "${ns}" "${name}" \
      -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
    if [[ "${restarts:-0}" -gt 0 ]]; then
      out="${DEST}/${ns}-${name}.previous.log"
      echo "collecting ${ns}/${name} previous container (${restarts} restarts)"
      hk_kubectl logs -n "${ns}" "${name}" --previous "${WINDOW[@]}" --timestamps=false >"${out}" \
        || { hk_warn "kubectl logs --previous ${ns}/${name} failed; skipping"; rm -f "${out}"; }
    fi
  done
}

collect "${NAMESPACE}" "app=keeper"
# Worker pods carry the pool label whatever the pool is called.
collect "${WORKER_NAMESPACE}" "hivekeep.io/worker-pool"

echo "logs in ${DEST}; next:"
echo "  python3 crates/hivekeep-bench/analysis/phase_report.py ${DEST}/*.log"
