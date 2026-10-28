#!/usr/bin/env bash
# Runs every check in scripts/verify/ (in name order), continuing past
# failures and reporting all of them at the end. Arguments go to each check.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
export LC_ALL=C

failed=()
for f in scripts/verify/*.sh; do
  [[ "${f}" == */all.sh ]] && continue
  echo "==> ${f}"
  if ! "${f}" "$@"; then
    failed+=("${f}")
  fi
done
if [[ ${#failed[@]} -gt 0 ]]; then
  printf 'FAILED: %s\n' "${failed[@]}" >&2
  exit 1
fi
echo "All checks passed."
