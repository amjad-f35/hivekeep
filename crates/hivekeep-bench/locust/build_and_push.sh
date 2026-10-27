#!/usr/bin/env bash
# Builds the locust benchmark image (locust/Dockerfile: the master's
# locustfiles, the headless runner and locust-worker) and pushes it.
#
# Environment:
#   LOCUST_IMAGE         image reference to build (default
#                        $HK_BUILD_REPO/hivekeep-bench-locust:<version>)
#   HK_BUILD_REPO        registry, when LOCUST_IMAGE is unset
#   HK_BUILD_PLATFORMS   platforms to build for (default linux/amd64; must
#                        match the cluster's nodes, not the build host)
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"
cd "${HK_ROOT}"
if declare -F hk_load_dev_env >/dev/null; then
  hk_load_dev_env
fi
hk_require_cmd docker

version="$(sed -n '/^\[workspace.package\]/,/^\[/s/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -n1)"
if [[ -z "${LOCUST_IMAGE:-}" ]]; then
  hk_require_env HK_BUILD_REPO
  LOCUST_IMAGE="${HK_BUILD_REPO}/hivekeep-bench-locust:${version}"
fi
platforms="${HK_BUILD_PLATFORMS:-linux/amd64}"

hk_log "building ${LOCUST_IMAGE} (${platforms})"
# The build context is the repository root: the image compiles the workspace
# and reads the protos and analysis/phase_report.py.
docker buildx build --push --platform "${platforms}" \
  -f crates/hivekeep-bench/locust/Dockerfile -t "${LOCUST_IMAGE}" .
hk_log "pushed ${LOCUST_IMAGE}"
