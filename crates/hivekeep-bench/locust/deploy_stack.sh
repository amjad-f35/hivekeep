#!/usr/bin/env bash
# Brings the whole interactive benchmark up or down: the benchmark workloads
# (workloads/deploy.sh), the locust image (build_and_push.sh) and the locust
# deployment (deploy.sh).
#
# The workload options are passed through to workloads/deploy.sh only when
# given, so its own defaults apply otherwise.
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"

BENCH_DIR="${HK_ROOT}/crates/hivekeep-bench"
SKIP_BUILD=0
workload_args=()

usage() {
  cat <<'TXT'
Usage: locust/deploy_stack.sh --deploy|--delete [options]
  --worker-count N          WorkerPool replicas (default 1)
  --sandbox-class CLASS     gvisor | microvm (default gvisor)
  --otlp-endpoint URL       where instrumented actor containers send telemetry
  --actor-memory SIZE       memory limit of the benchmark ActorTemplates
  --actor-cpu CPU           CPU limit of the benchmark ActorTemplates
  --worker-memory SIZE      memory request and limit of each worker pod
  --storage-class-name SC   StorageClass of the glutton-storage volume; setting
                            it deploys glutton-storage
  --wait-timeout SECONDS    how long to wait for the workers to become ready
  --skip-build              deploy the existing locust image
  -h, --help                this help
The workload options are passed to workloads/deploy.sh.
TXT
}

[[ $# -gt 0 ]] || { usage >&2; exit 1; }

action=""
sandbox_class=gvisor
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
    --skip-build) SKIP_BUILD=1 ;;
    -h | --help) usage; exit 0 ;;
    --worker-count | --sandbox-class | --otlp-endpoint | --actor-memory | --actor-cpu | \
      --worker-memory | --storage-class-name | --wait-timeout)
      if [[ -z "${value}" ]]; then
        [[ $# -ge 2 ]] || hk_die "${arg} needs a value"
        value="$2"
        shift
      fi
      # An empty value would override workloads/deploy.sh's default with
      # nothing, so it is dropped instead.
      [[ -n "${value}" ]] && workload_args+=("${arg}" "${value}")
      [[ "${arg}" == --sandbox-class ]] && sandbox_class="${value}"
      if [[ "${arg}" == --wait-timeout && ! "${value}" =~ ^[0-9]+$ ]]; then
        hk_die "--wait-timeout must be a whole number of seconds, got '${value}'"
      fi
      ;;
    *) usage >&2; hk_die "unknown option $1" ;;
  esac
  shift
done

case "${sandbox_class}" in
  gvisor | microvm) ;;
  *) hk_die "--sandbox-class must be gvisor or microvm, got '${sandbox_class}'" ;;
esac

case "${action}" in
  deploy)
    hk_log "deploying the benchmark workloads"
    "${BENCH_DIR}/workloads/deploy.sh" --deploy ${workload_args[@]+"${workload_args[@]}"}
    if [[ "${SKIP_BUILD}" -eq 0 ]]; then
      hk_log "building and pushing the locust image"
      "${BENCH_DIR}/locust/build_and_push.sh"
    else
      hk_log "skipping the locust image build (--skip-build)"
    fi
    hk_log "deploying locust"
    "${BENCH_DIR}/locust/deploy.sh" --deploy
    ;;
  delete)
    hk_log "deleting locust"
    "${BENCH_DIR}/locust/deploy.sh" --delete
    hk_log "deleting the benchmark workloads"
    "${BENCH_DIR}/workloads/deploy.sh" --delete
    ;;
  *) usage >&2; exit 1 ;;
esac
