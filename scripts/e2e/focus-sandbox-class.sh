#!/usr/bin/env bash
# Gives one sandbox class's demo WorkerPools the node to themselves: scales
# the other class's pools to zero (remembering their replica counts in an
# annotation) and restores this class's pools, waiting until they are ready.
#   scripts/e2e/focus-sandbox-class.sh gvisor|microvm
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-kind}"
annotation="e2e.hivekeep.io/replicas"
gvisor=(hivekeep-demo-counter/counter hivekeep-demo-egress/egress)
microvm=(hivekeep-demo-counter-microvm/counter-microvm hivekeep-demo-egress-microvm/egress-microvm)

case "${1:-}" in
  gvisor) keep=("${gvisor[@]}"); drop=("${microvm[@]}") ;;
  microvm) keep=("${microvm[@]}"); drop=("${gvisor[@]}") ;;
  *) echo "usage: $0 gvisor|microvm" >&2; exit 1 ;;
esac

# pool NAMESPACE/NAME VERB [ARGS...]
pool() {
  hk_kubectl -n "${1%/*}" "$2" "workerpool/${1#*/}" "${@:3}"
}

for p in "${drop[@]}"; do
  replicas="$(pool "${p}" get -o jsonpath='{.spec.replicas}')"
  if [[ "${replicas}" != 0 ]]; then
    pool "${p}" annotate --overwrite "${annotation}=${replicas}"
    pool "${p}" scale --replicas=0
  fi
done

for p in "${keep[@]}"; do
  saved="$(pool "${p}" get -o jsonpath="{.metadata.annotations.${annotation//./\\.}}")"
  if [[ -n "${saved}" ]]; then
    pool "${p}" scale --replicas="${saved}"
    pool "${p}" annotate "${annotation}-"
  fi
  replicas="$(pool "${p}" get -o jsonpath='{.spec.replicas}')"
  pool "${p}" wait --for=jsonpath='{.status.readyReplicas}'="${replicas}" --timeout=5m
done
