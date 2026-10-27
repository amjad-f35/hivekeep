#!/usr/bin/env bash
# Deploys (default) or deletes the egress benchmark's Kubernetes side: the
# egress-origin server and the driver-gateway in the egress-bench namespace.
# The namespace is created on deploy and never deleted here.
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="egress-bench"
WAIT_TIMEOUT_SECS=120

usage() {
  cat <<EOF
Usage: $0 [--deploy | --delete] [options]
  --deploy                deploy egress-origin and driver-gateway (default)
  --delete                delete them
  --context CONTEXT       kubectl context (default: \$KUBECTL_CONTEXT, else the current one)
  --wait-timeout SECONDS  rollout wait (default ${WAIT_TIMEOUT_SECS})
  -h, --help              this help
EOF
}

action=deploy
while [[ $# -gt 0 ]]; do
  arg="$1" value=""
  if [[ "${arg}" == --*=* ]]; then
    value="${arg#*=}"
    arg="${arg%%=*}"
  fi
  case "${arg}" in
    --deploy) action=deploy ;;
    --delete) action=delete ;;
    --context | --wait-timeout)
      if [[ -z "${value}" ]]; then
        [[ $# -ge 2 ]] || hk_die "${arg} needs a value"
        value="$2"
        shift
      fi
      if [[ "${arg}" == --context ]]; then KUBECTL_CONTEXT="${value}"; else WAIT_TIMEOUT_SECS="${value}"; fi
      ;;
    -h | --help) usage; exit 0 ;;
    *) usage >&2; hk_die "unknown option $1" ;;
  esac
  shift
done
[[ "${WAIT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] \
  || hk_die "--wait-timeout must be whole seconds like 120, got '${WAIT_TIMEOUT_SECS}'"

case "${action}" in
  deploy)
    hk_kubectl apply -f "${DIR}/namespace.yaml" -f "${DIR}/egress-origin.yaml" -f "${DIR}/driver-gateway.yaml"
    for d in egress-origin driver-gateway; do
      hk_log "waiting for deployment/${d} (timeout ${WAIT_TIMEOUT_SECS}s)"
      hk_kubectl -n "${NAMESPACE}" rollout status "deployment/${d}" --timeout="${WAIT_TIMEOUT_SECS}s"
    done
    ;;
  delete)
    hk_kubectl delete --ignore-not-found -f "${DIR}/driver-gateway.yaml" -f "${DIR}/egress-origin.yaml"
    ;;
esac
