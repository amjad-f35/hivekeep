#!/usr/bin/env bash
# scripts/microvm/demo.sh against the local Kind cluster: local registry,
# rustfs staging, context kind-<cluster>.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

export NO_DEV_ENV=true
export HK_INSTALL_KIND=true
export HK_BUILD_REPO="${HK_BUILD_REPO:-localhost:${KIND_REGISTRY_PORT:-5001}}"
export HK_BUILD_PLATFORMS="${HK_BUILD_PLATFORMS:-linux/$(hk_host_arch)}"
export BUCKET_NAME="${BUCKET_NAME:-hivekeep-snapshots}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KIND_CLUSTER_NAME
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"
unset GCE_REGION CLUSTER_LOCATION NETWORK SUBNETWORK MEMORYSTORE_INSTANCE PROJECT_ID

exec "${HK_ROOT}/scripts/microvm/demo.sh" "$@"
