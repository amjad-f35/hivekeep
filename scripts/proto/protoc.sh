#!/usr/bin/env bash
# Runs a pinned, checksum-verified protoc, downloading it into bin/protoc/ on
# first use. The proto build scripts (tonic/prost build.rs) need protoc; point
# them here with `export PROTOC="$(scripts/proto/protoc.sh --print-path)"`,
# which the Makefile does.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

VERSION="${PROTOC_VERSION:-25.3}"
declare_sha() {
  case "$1" in
    linux-x86_64) echo f853e691868d0557425ea290bf7ba6384eef2fa9b04c323afab49a770ba9da80 ;;
    linux-aarch_64) echo 9eae1f20f70cccc912d1c318c3929b86aebf5afd4b0f32c196ef682c222ed5ae ;;
    osx-x86_64) echo 247e003b8e115405172eacc50bd19825209d85940728e766f0848eee7c80e2a1 ;;
    osx-aarch_64) echo d0fcd6d3b3ef6f22f1c47cc30a80c06727e1eccdddcaf0f4a3be47c070ffd3fe ;;
    *) return 1 ;;
  esac
}

os="$(uname -s)"
arch="$(hk_host_arch)"
case "${os}/${arch}" in
  Darwin/amd64) platform=osx-x86_64 ;;
  Darwin/arm64) platform=osx-aarch_64 ;;
  Linux/amd64) platform=linux-x86_64 ;;
  Linux/arm64) platform=linux-aarch_64 ;;
  *) hk_die "no pinned protoc for ${os}/${arch}" ;;
esac

dir="${HK_ROOT}/bin/protoc/${VERSION}"
bin="${dir}/bin/protoc"
if [[ ! -x "${bin}" ]]; then
  hk_require_cmd curl unzip
  # A version other than the pinned one has no known checksum.
  want="$(declare_sha "${platform}")"
  [[ "${VERSION}" == 25.3 ]] || want="${PROTOC_SHA256:?set PROTOC_SHA256 for protoc ${VERSION}}"
  mkdir -p "${dir}"
  zip="${dir}/protoc.zip"
  curl -fsSL -o "${zip}" "${PROTOC_RELEASES_URL:-https://github.com/protocolbuffers/protobuf/releases/download}/v${VERSION}/protoc-${VERSION}-${platform}.zip"
  got="$(hk_sha256 "${zip}")"
  if [[ "${got}" != "${want}" ]]; then
    rm -f "${zip}"
    hk_die "protoc checksum mismatch: want ${want}, got ${got}"
  fi
  unzip -q -o "${zip}" -d "${dir}"
  rm -f "${zip}"
  chmod +x "${bin}"
fi

if [[ "${1:-}" == --print-path ]]; then
  echo "${bin}"
  exit 0
fi
exec "${bin}" "$@"
