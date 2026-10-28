#!/usr/bin/env bash
# Deletes the Kind cluster made by scripts/kind/up.sh, and the local registry
# container if this tooling created it.
#   KIND_CLUSTER_NAME  cluster name (default: kind)
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
REGISTRY_NAME="${HK_KIND_REGISTRY_NAME:-kind-registry}"
hk_require_cmd docker

# NFS hard mounts on a node whose in-cluster NFS server is gone retry forever
# and wedge the node, so stop the other pods and unmount while the server is
# still up.
for node in $(hk_kind get nodes --name "${KIND_CLUSTER_NAME}" 2>/dev/null); do
  # shellcheck disable=SC2016 # expanded inside the node
  docker exec "${node}" sh -c '
    grep -qE "^[^ ]+ [^ ]+ nfs4? " /proc/mounts || exit 0
    systemctl stop kubelet
    server="$(crictl pods -q --label app=nfs-server)"
    for pod in $(crictl pods -q); do
      case "${server}" in *"${pod}"*) ;; *) crictl stopp "${pod}" >/dev/null ;; esac
    done
    awk "\$3 ~ /^nfs4?\$/ {print \$2}" /proc/mounts | sort -r | xargs -r umount -l
  ' || true
done

hk_log "deleting cluster ${KIND_CLUSTER_NAME}"
hk_kind delete cluster --name "${KIND_CLUSTER_NAME}"

if docker inspect "${REGISTRY_NAME}" >/dev/null 2>&1; then
  owner="$(docker inspect --format '{{index .Config.Labels "created-by"}}' "${REGISTRY_NAME}" 2>/dev/null || true)"
  if [[ "${owner}" == hivekeep ]]; then
    hk_log "removing registry container ${REGISTRY_NAME}"
    docker rm -f "${REGISTRY_NAME}" >/dev/null || true
  else
    hk_log "leaving registry ${REGISTRY_NAME} running (not created by these scripts)"
  fi
fi
