#!/usr/bin/env bash
# Runs every generator/formatter in scripts/update/ in name order.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
export LC_ALL=C
for f in scripts/update/*.sh; do
  case "${f}" in */all.sh | */deps.sh) continue ;; esac
  echo "==> ${f}"
  "${f}" "$@"
done
