#!/usr/bin/env bash
# Deploys or removes the locust benchmark stack, in order:
#   --deploy  workloads (WorkerPool + benchmark ActorTemplates), then the
#             locust image build/push, then locust itself
#   --delete  locust, then the workloads
# hivekeep-setup's `deploy benchmarks` / `delete benchmarks` call this.
#
# The steps are delegated to the benchmark suite's own scripts under
# HK_BENCHMARKING_DIR (default <repo>/crates/hivekeep-bench):
#   workloads/deploy.sh, locust/build_and_push.sh, locust/deploy.sh
# --with-monitoring also applies (or, on --delete, removes) the optional
# Prometheus + Grafana stack in monitoring.yaml from the same directory.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

BENCH_DIR="${HK_BENCHMARKING_DIR:-${HK_ROOT}/crates/hivekeep-bench}"
worker_count=1
sandbox_class=gvisor
skip_build=false
with_monitoring=false
forward=()

usage() {
  cat <<'TXT'
Usage: scripts/benchmarking/deploy_locust.sh --deploy|--delete [options]
  --worker-count N          WorkerPool replicas (default 1)
  --sandbox-class CLASS     gvisor | microvm (default gvisor; microvm needs
                            scripts/install-microvm-deps.sh --install first)
  --otlp-endpoint URL       where instrumented Actors send telemetry
  --actor-memory SIZE       benchmark ActorTemplate memory limit
  --actor-cpu CPU           benchmark ActorTemplate CPU limit
  --worker-memory SIZE      memory request/limit of each worker pod
  --storage-class-name SC   StorageClass for the storage benchmark's external volume
  --wait-timeout SECONDS    how long to wait for workers to be ready
  --skip-build              reuse the existing locust image
  --with-monitoring         also deploy/delete the Prometheus + Grafana stack
                            (monitoring.yaml in the benchmark directory)
TXT
}

# opt NAME VALUE: forward a workloads option.
opt() { forward+=("$1" "$2"); }

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
    --skip-build) skip_build=true ;;
    --with-monitoring) with_monitoring=true ;;
    -h | --help) usage; exit 0 ;;
    --worker-count | --sandbox-class | --otlp-endpoint | --actor-memory | --actor-cpu | --worker-memory | --storage-class-name | --wait-timeout)
      if [[ -z "${value}" ]]; then
        [[ $# -ge 2 ]] || hk_die "${arg} needs a value"
        value="$2"
        shift
      fi
      case "${arg}" in
        --worker-count) worker_count="${value}" ;;
        --sandbox-class) sandbox_class="${value}" ;;
        --wait-timeout)
          [[ "${value}" =~ ^[0-9]+$ ]] || hk_die "--wait-timeout must be whole seconds, got '${value}'"
          opt "${arg}" "${value}"
          ;;
        *) opt "${arg}" "${value}" ;;
      esac
      ;;
    *) usage >&2; hk_die "unknown option $1" ;;
  esac
  shift
done

case "${sandbox_class}" in
  gvisor | microvm) ;;
  *) hk_die "--sandbox-class must be gvisor or microvm, got '${sandbox_class}'" ;;
esac
[[ -n "${action}" ]] || { usage >&2; exit 1; }

for s in workloads/deploy.sh locust/build_and_push.sh locust/deploy.sh; do
  [[ -x "${BENCH_DIR}/${s}" ]] || hk_die "benchmark step ${BENCH_DIR}/${s} is missing; set HK_BENCHMARKING_DIR to the benchmark suite"
done
if [[ "${with_monitoring}" == true ]]; then
  [[ -f "${BENCH_DIR}/monitoring.yaml" ]] || hk_die "${BENCH_DIR}/monitoring.yaml is missing"
fi

if [[ "${action}" == deploy ]]; then
  hk_log "deploying benchmark workloads (workers=${worker_count}, class=${sandbox_class})"
  "${BENCH_DIR}/workloads/deploy.sh" --deploy --worker-count "${worker_count}" --sandbox-class "${sandbox_class}" \
    "${forward[@]+"${forward[@]}"}"
  if [[ "${skip_build}" == false ]]; then
    hk_log "building and pushing the locust image"
    "${BENCH_DIR}/locust/build_and_push.sh"
  fi
  hk_log "deploying locust"
  "${BENCH_DIR}/locust/deploy.sh" --deploy
  if [[ "${with_monitoring}" == true ]]; then
    hk_log "deploying the monitoring stack (kubectl -n benchmarking port-forward svc/grafana 3000:3000)"
    kubectl apply -f "${BENCH_DIR}/monitoring.yaml"
  fi
else
  if [[ "${with_monitoring}" == true ]]; then
    hk_log "deleting the monitoring stack"
    # By name rather than `delete -f`, which would also remove the
    # benchmarking namespace that locust/deploy.sh owns.
    kubectl -n benchmarking delete --ignore-not-found \
      deployment/prometheus deployment/grafana service/prometheus service/grafana \
      configmap/prometheus-config configmap/grafana-provisioning configmap/grafana-dashboards \
      serviceaccount/prometheus
    kubectl delete --ignore-not-found \
      clusterrolebinding/hivekeep-benchmarking-prometheus clusterrole/hivekeep-benchmarking-prometheus
  fi
  hk_log "deleting locust"
  "${BENCH_DIR}/locust/deploy.sh" --delete
  hk_log "deleting benchmark workloads"
  "${BENCH_DIR}/workloads/deploy.sh" --delete
fi
