#!/usr/bin/env bash
# Installs the hostpath CSI driver on the Kind cluster for the external-volume
# demos and e2e tests (`hivekeep-setup setup csi --setup-csi=hostpath`, which
# also clears stale mounts on the node). The vendored driver bundle is synced
# into the installer's manifests first.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"

"${HK_ROOT}/scripts/csi/sync-third-party.sh"
"${HK_ROOT}/scripts/install/hivekeep-kind.sh" setup csi --setup-csi=hostpath "$@"
