#!/usr/bin/env bash
# Deletes every namespace a failed e2e run left behind (failed suites keep
# theirs so worker logs can be read). Removes ALL e2e namespaces in the
# cluster, so do not run it while a suite is running.
#   KUBECTL_CONTEXT or KIND_CLUSTER_NAME selects the cluster.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

if [[ -z "${KUBECTL_CONTEXT:-}" && -n "${KIND_CLUSTER_NAME:-}" ]]; then
  export KUBECTL_CONTEXT="kind-${KIND_CLUSTER_NAME}"
fi
# The label the e2e framework stamps on namespaces it creates.
selector="${HK_E2E_NAMESPACE_LABEL:-hivekeep.io/e2e}"

leftover="$(hk_kubectl get namespaces -l "${selector}" -o name)"
if [[ -z "${leftover}" ]]; then
  echo "No leftover e2e namespaces."
  exit 0
fi
echo "Deleting leftover e2e namespaces:"
echo "${leftover}"
hk_kubectl delete namespaces -l "${selector}" --ignore-not-found
