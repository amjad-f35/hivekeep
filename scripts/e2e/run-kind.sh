#!/usr/bin/env bash
# scripts/e2e/run.sh against the local Kind cluster, with the same image repo
# and snapshot bucket the Kind install uses. Arguments are forwarded.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

export NO_DEV_ENV=true
export HK_BUILD_REPO="${HK_BUILD_REPO:-localhost:${KIND_REGISTRY_PORT:-5001}}"
export HK_BUILD_PLATFORMS="${HK_BUILD_PLATFORMS:-linux/$(hk_host_arch)}"
export BUCKET_NAME=hivekeep-snapshots
export SNAPSHOT_LOCATION="gs://${BUCKET_NAME}/"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME}}"

exec "${HK_ROOT}/scripts/e2e/run.sh" "$@"
