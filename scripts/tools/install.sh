#!/usr/bin/env bash
# Installs pinned developer tools into bin/tools/ under the repository root.
#   scripts/tools/install.sh [tool ...]    (default: every tool)
# Tools: kind, cargo-nextest, cargo-deny, shellcheck. Versions come from
# scripts/tools/versions.env; override one with e.g. KIND_VERSION=v0.32.0.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

versions="${HK_ROOT}/scripts/tools/versions.env"
# Values already in the environment win over the pinned file.
while IFS='=' read -r key value; do
  [[ -z "${key}" || "${key}" == \#* ]] && continue
  if [[ -z "${!key:-}" ]]; then
    printf -v "${key}" '%s' "${value}"
  fi
done <"${versions}"

BIN_DIR="${HK_ROOT}/bin/tools"
mkdir -p "${BIN_DIR}"

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(hk_host_arch)"

install_kind() {
  local dest="${BIN_DIR}/kind"
  if [[ -x "${dest}" ]] && "${dest}" version 2>/dev/null | grep -q "${KIND_VERSION}"; then
    return 0
  fi
  hk_require_cmd curl
  hk_log "installing kind ${KIND_VERSION}"
  curl -fsSL -o "${dest}.tmp" "${KIND_DOWNLOAD_BASE:-https://kind.sigs.k8s.io/dl}/${KIND_VERSION}/kind-${os}-${arch}"
  chmod +x "${dest}.tmp"
  mv "${dest}.tmp" "${dest}"
}

install_cargo_tool() {
  local crate="$1" version="$2" bin="$3"
  if [[ -x "${BIN_DIR}/${bin}" ]] && "${BIN_DIR}/${bin}" --version 2>/dev/null | grep -q "${version}"; then
    return 0
  fi
  hk_require_cmd cargo
  hk_log "installing ${crate} ${version}"
  cargo install --locked --quiet --root "${HK_ROOT}/bin/cargo-tools" --version "${version}" "${crate}"
  ln -sf "${HK_ROOT}/bin/cargo-tools/bin/${bin}" "${BIN_DIR}/${bin}"
}

install_shellcheck() {
  local dest="${BIN_DIR}/shellcheck"
  if [[ -x "${dest}" ]] && "${dest}" --version 2>/dev/null | grep -q "${SHELLCHECK_VERSION#v}"; then
    return 0
  fi
  if [[ "${os}" == darwin ]]; then
    hk_die "install shellcheck ${SHELLCHECK_VERSION} with your package manager on macOS"
  fi
  hk_require_cmd curl tar xz
  local sc_arch=x86_64
  [[ "${arch}" == arm64 ]] && sc_arch=aarch64
  local tmp
  tmp="$(mktemp -d)"
  hk_log "installing shellcheck ${SHELLCHECK_VERSION}"
  curl -fsSL -o "${tmp}/sc.tar.xz" \
    "${SHELLCHECK_DOWNLOAD_BASE:-https://github.com/koalaman/shellcheck/releases/download}/${SHELLCHECK_VERSION}/shellcheck-${SHELLCHECK_VERSION}.linux.${sc_arch}.tar.xz"
  tar -xJf "${tmp}/sc.tar.xz" -C "${tmp}"
  mv "${tmp}/shellcheck-${SHELLCHECK_VERSION}/shellcheck" "${dest}"
  rm -rf "${tmp}"
}

tools=("$@")
if [[ ${#tools[@]} -eq 0 ]]; then
  tools=(kind cargo-nextest cargo-deny shellcheck)
fi
for t in "${tools[@]}"; do
  case "${t}" in
    kind) install_kind ;;
    cargo-nextest) install_cargo_tool cargo-nextest "${CARGO_NEXTEST_VERSION}" cargo-nextest ;;
    cargo-deny) install_cargo_tool cargo-deny "${CARGO_DENY_VERSION}" cargo-deny ;;
    shellcheck) install_shellcheck ;;
    *) hk_die "unknown tool ${t}" ;;
  esac
done
