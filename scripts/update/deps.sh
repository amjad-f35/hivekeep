#!/usr/bin/env bash
# Updates Cargo.lock within the version requirements of every manifest
# (pass crate names to update only those, e.g. scripts/update/deps.sh tokio).
# Not part of update/all.sh's normal flow: run it deliberately.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
if [[ $# -gt 0 ]]; then
  args=()
  for c in "$@"; do args+=(-p "${c}"); done
  cargo update "${args[@]}"
else
  cargo update
fi
