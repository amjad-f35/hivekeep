#!/usr/bin/env bash
# Runs the installer against the local Kind cluster: Kind profile on, dev env
# file ignored, images pushed to the local registry, context kind-<cluster>.
#   scripts/install/hivekeep-kind.sh deploy hivekeep-system --credential-provider='{"name":"k8s.io"}'
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

export NO_DEV_ENV=true
export HK_INSTALL_KIND=true
export HK_BUILD_REPO="${HK_BUILD_REPO:-localhost:${KIND_REGISTRY_PORT:-5001}}"
export HK_BUILD_PLATFORMS="${HK_BUILD_PLATFORMS:-linux/$(hk_host_arch)}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KIND_CLUSTER_NAME
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"
# Cloud settings a developer may have exported must not reach a local install.
unset GCE_REGION CLUSTER_LOCATION NETWORK SUBNETWORK MEMORYSTORE_INSTANCE PROJECT_ID

hk_setup "$@"
