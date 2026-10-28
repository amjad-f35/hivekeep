#!/usr/bin/env bash
# Copies the vendored CSI driver bundles (scripts/third_party/csi-driver-*)
# into the installer's manifests directory, where `hivekeep-setup setup csi`
# looks for them (crates/hivekeep-setup/manifests/csi/third_party/), or into
# HK_MANIFESTS_DIR/csi/third_party when that is set.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

dest="${HK_MANIFESTS_DIR:-${HK_ROOT}/crates/hivekeep-setup/manifests}/csi/third_party"
mkdir -p "${dest}"
for bundle in csi-driver-host-path csi-driver-nfs; do
  rm -rf "${dest:?}/${bundle}"
  cp -R "${HK_ROOT}/scripts/third_party/${bundle}" "${dest}/${bundle}"
  hk_log "copied ${bundle} to ${dest}/${bundle}"
done
