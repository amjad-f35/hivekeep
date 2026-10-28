#!/usr/bin/env bash
# Installs or removes the cluster-wide micro-VM (kata guest on Cloud
# Hypervisor) dependencies. The default install only ships the gVisor
# SandboxConfig; this adds the `microvm` one.
#
#   scripts/install-microvm-deps.sh --install
#       assemble the runtime assets (skipped when bin/microvm-assets/<arch>
#       already holds a current set), stage them under kata-assets/ in the
#       asset bucket (rustfs on Kind, GCS otherwise) and apply the
#       SandboxConfig, which pins every asset's sha256.
#   scripts/install-microvm-deps.sh --delete
#       remove the SandboxConfig, unless a WorkerPool still uses it (then it
#       is left in place and the pools are listed). Bucket contents stay.
#
# hivekeep-setup calls this for micro-VM demos and benchmarks.
#
# Environment (most come from .hivekeep-dev-env.sh unless NO_DEV_ENV is set):
#   BUCKET_NAME      snapshot bucket (default hivekeep-snapshots). On Kind the
#                    assets share it; elsewhere they go to BUCKET_NAME-assets.
#   KUBECTL_CONTEXT  kube context (default: current context)
#   PROJECT_ID       GCP project for the GCS upload
#   ARCH             asset architecture (default: from HK_BUILD_PLATFORMS, else host)
#   OUT              asset directory (default bin/microvm-assets/<arch>)
#   HK_INSTALL_KIND  true to stage into the in-cluster rustfs
set -euo pipefail
# shellcheck source=lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/common.sh"
cd "${HK_ROOT}"
hk_load_dev_env

BUCKET_NAME="${BUCKET_NAME:-hivekeep-snapshots}"
HK_INSTALL_KIND="${HK_INSTALL_KIND:-false}"
TEMPLATE="${HK_MICROVM_SANDBOXCONFIG:-crates/hivekeep-setup/manifests/microvm/sandboxconfig-microvm.yaml.tmpl}"
ASSETS=(cloud-hypervisor virtiofsd vmlinux rootfs.img)

usage() {
  echo "usage: $0 --install | --delete"
}

action=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install) action=install ;;
    --delete) action=delete ;;
    -h | --help) usage; exit 0 ;;
    *) usage >&2; exit 1 ;;
  esac
  shift
done
[[ -n "${action}" ]] || { usage >&2; exit 1; }
hk_require_cmd kubectl

# Without a context or a current-context kubectl dials localhost:8080 and the
# failure would only show after the assets were staged.
if [[ -z "${KUBECTL_CONTEXT:-}" ]] && ! kubectl config current-context >/dev/null 2>&1; then
  hk_die "no kube context: set KUBECTL_CONTEXT or run 'kubectl config use-context <name>'"
fi

if [[ "${HK_INSTALL_KIND}" == true ]]; then
  BUCKET_SCHEME=s3
  ASSET_BUCKET_NAME="${BUCKET_NAME}"
else
  BUCKET_SCHEME=gs
  ASSET_BUCKET_NAME="${BUCKET_NAME}-assets"
fi

if [[ "${action}" == delete ]]; then
  # A pool using the config holds its protection finalizer; deleting would
  # leave it Terminating (and unrecreatable), so name the pools instead.
  users="$(hk_kubectl get workerpools --all-namespaces \
    -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name} {.status.sandboxClasses[*].configRef.name}{"\n"}{end}' 2>/dev/null |
    awk '{ for (i = 2; i <= NF; i++) if ($i == "microvm") { print $1; break } }' || true)"
  if [[ -n "${users}" ]]; then
    hk_warn "the microvm SandboxConfig stays: these WorkerPools still use it:"
    while IFS= read -r pool; do echo "  ${pool}" >&2; done <<<"${users}"
    exit 0
  fi
  hk_log "deleting the microvm SandboxConfig"
  if ! hk_kubectl delete --ignore-not-found --timeout=60s sandboxconfig microvm; then
    hk_die "the microvm SandboxConfig did not finish deleting; check that hivekeep-controller is running (it releases the finalizer)"
  fi
  hk_log "done; assets under ${BUCKET_SCHEME}://${ASSET_BUCKET_NAME}/kata-assets/ were left in place"
  exit 0
fi

if [[ -z "${ARCH:-}" ]]; then
  if [[ -n "${HK_BUILD_PLATFORMS:-}" ]]; then
    first="${HK_BUILD_PLATFORMS%%,*}"
    ARCH="${first##*/}"
  else
    ARCH="$(hk_host_arch)"
  fi
fi
OUT="${OUT:-${HK_ROOT}/bin/microvm-assets/${ARCH}}"

# Asset filenames never change when a version pin moves, so compare the
# version stamp assemble.sh leaves behind rather than trusting presence.
need_assemble=false
for f in "${ASSETS[@]}"; do
  [[ -f "${OUT}/${f}" ]] || need_assemble=true
done
if [[ "${need_assemble}" == false ]]; then
  want="$(ARCH="${ARCH}" scripts/microvm-assets/assemble.sh --print-stamp)"
  if [[ "$(cat "${OUT}/.asset-versions" 2>/dev/null || true)" != "${want}" ]]; then
    hk_log "assets in ${OUT} are from other versions; re-assembling"
    need_assemble=true
  fi
fi
if [[ "${need_assemble}" == true ]]; then
  hk_log "assembling micro-VM assets into ${OUT} (ARCH=${ARCH})"
  ARCH="${ARCH}" OUT="${OUT}" scripts/microvm-assets/assemble.sh
else
  hk_log "current assets already in ${OUT}"
fi

if [[ "${HK_INSTALL_KIND}" == true ]]; then
  hk_log "staging assets into rustfs bucket ${BUCKET_NAME}"
  OUT="${OUT}" BUCKET="${BUCKET_NAME}" scripts/microvm-assets/stage-to-rustfs.sh
else
  hk_log "uploading assets to gs://${ASSET_BUCKET_NAME}/kata-assets/"
  OUT="${OUT}" BUCKET="${ASSET_BUCKET_NAME}" scripts/microvm-assets/stage-to-gcs.sh
fi

hk_log "applying the microvm SandboxConfig"
sed -e "s|\${BUCKET_SCHEME}|${BUCKET_SCHEME}|g" \
  -e "s|\${ASSET_BUCKET_NAME}|${ASSET_BUCKET_NAME}|g" \
  "${TEMPLATE}" | hk_kubectl apply -f -
hk_log "done"
