#!/usr/bin/env bash
# Runs a pinned developer tool, installing it first if needed.
#   scripts/tools/run.sh [--print-bin-path] <tool> [args...]
# A tool already on PATH is used as-is unless HK_PINNED_TOOLS=1.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

print_path=false
if [[ "${1:-}" == --print-bin-path ]]; then
  print_path=true
  shift
fi
[[ $# -ge 1 ]] || hk_die "usage: $0 [--print-bin-path] <tool> [args...]"
tool="$1"
shift

bin=""
if [[ "${HK_PINNED_TOOLS:-}" != 1 ]] && command -v "${tool}" >/dev/null 2>&1; then
  bin="$(command -v "${tool}")"
else
  bin="${HK_ROOT}/bin/tools/${tool}"
  if [[ ! -x "${bin}" ]]; then
    "${HK_ROOT}/scripts/tools/install.sh" "${tool}" >&2
  fi
fi

if [[ "${print_path}" == true ]]; then
  echo "${bin}"
  exit 0
fi
exec "${bin}" "$@"
