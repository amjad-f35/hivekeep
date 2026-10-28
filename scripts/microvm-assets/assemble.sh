#!/usr/bin/env bash
# Assembles the four runtime assets micro-VM workers fetch at run time:
#   cloud-hypervisor  virtiofsd  vmlinux  rootfs.img
# All four are downloaded (kata-static release + Cloud Hypervisor release),
# so their bytes are reproducible and their sha256 sums can be pinned in
# crates/hivekeep-setup/manifests/microvm/sandboxconfig-microvm.yaml.tmpl.
#
# The guest rootfs is attached over virtio-pmem with DAX, so it must be a
# multiple of 2 MiB with page-aligned partitions; kata's container image is
# built that way. virtiofsd must be 1.14.0 or newer: older releases hang the
# Cloud Hypervisor restore handshake.
#
# Environment:
#   ARCH      arm64 | amd64 (default arm64)
#   KATA_VER  kata-static release (default 4.3.1)
#   CH_VER    Cloud Hypervisor release (default v53.0)
#   OUT       output directory (default bin/microvm-assets/<arch>)
#   KATA_RELEASES_URL, CH_RELEASES_URL  download bases (mirrors)
#
# Every run re-downloads. OUT/.asset-versions is removed before the first
# write and written only after a complete run, so its presence means the
# directory holds exactly the stamped versions. `--print-stamp` prints the
# stamp for the current settings and exits.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

ARCH="${ARCH:-arm64}"
KATA_VER="${KATA_VER:-4.3.1}"
CH_VER="${CH_VER:-v53.0}"
# What KATA_VER bundles; declared so --print-stamp needs no download, and
# checked against the extracted binary below.
VIRTIOFSD_VER="1.14.0"
OUT="${OUT:-${HK_ROOT}/bin/microvm-assets/${ARCH}}"
KATA_RELEASES_URL="${KATA_RELEASES_URL:-https://github.com/kata-containers/kata-containers/releases/download}"
CH_RELEASES_URL="${CH_RELEASES_URL:-https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download}"
STAMP_FILE=".asset-versions"

case "${ARCH}" in
  arm64) CH_ASSET=cloud-hypervisor-static-aarch64 ;;
  amd64) CH_ASSET=cloud-hypervisor-static ;;
  *) hk_die "unsupported ARCH=${ARCH}" ;;
esac

stamp() {
  printf 'arch=%s\nkata=%s\ncloud-hypervisor=%s\nvirtiofsd=%s\n' "${ARCH}" "${KATA_VER}" "${CH_VER}" "${VIRTIOFSD_VER}"
}

if [[ "${1:-}" == --print-stamp ]]; then
  stamp
  exit 0
fi
hk_require_cmd curl tar

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
mkdir -p "${OUT}"
rm -f "${OUT:?}/${STAMP_FILE}"

hk_log "downloading kata-static ${KATA_VER} (${ARCH})"
curl -fSL -o "${work}/kata-static.tar.zst" "${KATA_RELEASES_URL}/${KATA_VER}/kata-static-${KATA_VER}-${ARCH}.tar.zst"
mkdir -p "${work}/kata"
tar --zstd -xf "${work}/kata-static.tar.zst" -C "${work}/kata"
kroot="${work}/kata/opt/kata"

resolve() { readlink -f "$1" 2>/dev/null || realpath "$1"; }
cp "$(resolve "${kroot}/share/kata-containers/vmlinux.container")" "${OUT}/vmlinux"
cp "$(resolve "${kroot}/share/kata-containers/kata-containers.img")" "${OUT}/rootfs.img"
cp "${kroot}/libexec/virtiofsd" "${OUT}/virtiofsd"
chmod +x "${OUT}/virtiofsd"

hk_log "downloading cloud-hypervisor ${CH_VER} (${CH_ASSET})"
curl -fSL -o "${OUT}/cloud-hypervisor" "${CH_RELEASES_URL}/${CH_VER}/${CH_ASSET}"
chmod +x "${OUT}/cloud-hypervisor"

for f in cloud-hypervisor virtiofsd vmlinux rootfs.img; do
  [[ -f "${OUT}/${f}" ]] || hk_die "missing ${OUT}/${f}"
done

# Only checkable where the binary runs; a cross-arch or macOS assembly skips it.
if got="$("${OUT}/virtiofsd" --version 2>/dev/null | head -1 | awk '{print $2}')" && [[ -n "${got}" ]]; then
  [[ "${got}" == "${VIRTIOFSD_VER}" ]] ||
    hk_die "kata ${KATA_VER} bundles virtiofsd ${got}, not ${VIRTIOFSD_VER}; update VIRTIOFSD_VER"
fi

stamp >"${OUT}/${STAMP_FILE}"
hk_log "assets in ${OUT}; sha256 for the SandboxConfig's ${ARCH} block:"
for f in cloud-hypervisor virtiofsd vmlinux rootfs.img; do
  printf '%s  %s\n' "$(hk_sha256 "${OUT}/${f}")" "${f}"
done
