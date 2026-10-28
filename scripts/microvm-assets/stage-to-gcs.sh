#!/usr/bin/env bash
# Uploads an assembled micro-VM asset set to gs://$BUCKET/kata-assets/, where
# keepers fetch it through the node object-store plugin. Keep assets outside
# any snapshot location: the plugin refuses to serve those as assets.
#   OUT         asset directory (default bin/microvm-assets/amd64)
#   BUCKET      asset bucket (required; BUCKET_NAME-assets by convention)
#   PROJECT_ID  passed to gcloud as --project when set
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

OUT="${OUT:-${HK_ROOT}/bin/microvm-assets/amd64}"
BUCKET="${BUCKET:?set BUCKET to the asset bucket}"
hk_require_cmd gcloud
project=()
if [[ -n "${PROJECT_ID:-}" ]]; then
  project=(--project="${PROJECT_ID}")
fi

for f in cloud-hypervisor virtiofsd vmlinux rootfs.img; do
  hk_log "uploading ${f}"
  gcloud storage cp "${project[@]+"${project[@]}"}" "${OUT}/${f}" "gs://${BUCKET}/kata-assets/${f}"
done
gcloud storage ls "${project[@]+"${project[@]}"}" "gs://${BUCKET}/kata-assets/"
