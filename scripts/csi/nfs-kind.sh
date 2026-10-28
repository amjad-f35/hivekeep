#!/usr/bin/env bash
# Installs an in-cluster NFS server and the NFS CSI driver on the Kind
# cluster (`hivekeep-setup setup csi --setup-csi=nfs`). The Kind node uses the
# host kernel, so the host must have the nfsd module loaded.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

if [[ -f /proc/filesystems ]] && ! grep -q nfsd /proc/filesystems; then
  hk_die "the host kernel has no NFS server support; run 'sudo modprobe nfsd' and retry"
fi
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"

"${HK_ROOT}/scripts/csi/sync-third-party.sh"
"${HK_ROOT}/scripts/install/hivekeep-kind.sh" setup csi --setup-csi=nfs "$@"
