# shellcheck shell=bash
# Shared helpers for the scripts under scripts/. Source it with:
#   . "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
# (adjust the relative path to the caller's depth).

# hk_root is the repository root: the directory holding the workspace
# Cargo.toml, found from this file's location so it works without git.
hk_root() {
  (cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
}
HK_ROOT="${HK_ROOT:-$(hk_root)}"
export HK_ROOT

# Developer settings file, read by the installer and by these scripts.
HK_DEV_ENV_FILE="${HK_ROOT}/.hivekeep-dev-env.sh"

# hk_log prints a tagged progress line; the tag is the calling script's name.
hk_log() {
  local tag
  tag="$(basename "${0%.sh}")"
  if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    printf '\033[1;36m[%s]\033[0m %s\n' "${tag}" "$*"
  else
    printf '[%s] %s\n' "${tag}" "$*"
  fi
}

hk_warn() { printf 'warning: %s\n' "$*" >&2; }

hk_die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

# hk_require_cmd fails naming every missing command at once.
hk_require_cmd() {
  local missing=()
  local c
  for c in "$@"; do
    command -v "${c}" >/dev/null 2>&1 || missing+=("${c}")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    hk_die "required command(s) not on PATH: ${missing[*]}"
  fi
}

# hk_require_env fails naming the first unset variable.
hk_require_env() {
  local v
  for v in "$@"; do
    if [[ -z "${!v:-}" ]]; then
      hk_die "${v} is not set; export it or put it in ${HK_DEV_ENV_FILE}"
    fi
  done
}

# hk_load_dev_env sources the developer settings file unless NO_DEV_ENV is
# set (the Kind wrappers set it so cloud settings never leak into a local
# install).
hk_load_dev_env() {
  if [[ -z "${NO_DEV_ENV:-}" && -r "${HK_DEV_ENV_FILE}" ]]; then
    # shellcheck disable=SC1090 # path is computed
    . "${HK_DEV_ENV_FILE}"
  fi
}

# hk_kubectl runs kubectl against KUBECTL_CONTEXT when it is set.
hk_kubectl() {
  if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
    kubectl --context="${KUBECTL_CONTEXT}" "$@"
  else
    kubectl "$@"
  fi
}

# hk_hivectl runs the hivectl CLI: HIVECTL if set, else hivectl on PATH, else
# a cargo build of it. KUBECTL_CONTEXT is passed through as --context.
hk_hivectl() {
  local ctx_args=()
  if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
    ctx_args=(--context "${KUBECTL_CONTEXT}")
  fi
  if [[ -n "${HIVECTL:-}" ]]; then
    "${HIVECTL}" "${ctx_args[@]+"${ctx_args[@]}"}" "$@"
  elif command -v hivectl >/dev/null 2>&1; then
    hivectl "${ctx_args[@]+"${ctx_args[@]}"}" "$@"
  else
    cargo run --quiet --manifest-path "${HK_ROOT}/Cargo.toml" -p hivekeep-ctl --bin hivectl -- \
      "${ctx_args[@]+"${ctx_args[@]}"}" "$@"
  fi
}

# hk_setup runs the installer: HK_SETUP_BIN if set, else a release build of
# hivekeep-setup from this workspace.
hk_setup() {
  if [[ -n "${HK_SETUP_BIN:-}" ]]; then
    "${HK_SETUP_BIN}" "$@"
  else
    cargo run --quiet --release --manifest-path "${HK_ROOT}/Cargo.toml" -p hivekeep-setup --bin hivekeep-setup -- "$@"
  fi
}

# hk_host_arch prints the host CPU architecture in container-platform terms
# (amd64 or arm64).
hk_host_arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    *) uname -m ;;
  esac
}

# hk_kind runs kind: KIND if set, else kind on PATH, else the pinned copy
# installed by scripts/tools/install.sh.
hk_kind() {
  if [[ -n "${KIND:-}" ]]; then
    "${KIND}" "$@"
  else
    "${HK_ROOT}/scripts/tools/run.sh" kind "$@"
  fi
}

# hk_sha256 prints the SHA-256 of a file on Linux or macOS.
hk_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}
