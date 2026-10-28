#!/usr/bin/env bash
# Runs shellcheck over every shell script in scripts/ and demos/. Uses a
# local shellcheck, else the pinned one from scripts/tools, else the pinned
# container image.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

files=()
while IFS= read -r f; do
  files+=("${f}")
done < <(find scripts demos -type f \( -name '*.sh' -o -name '*.sh.example' \) -not -path '*/third_party/*' | sort)
[[ ${#files[@]} -gt 0 ]] || exit 0

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck -x "${files[@]}"
elif [[ "$(uname -s)" == Linux ]]; then
  "${HK_ROOT}/scripts/tools/run.sh" shellcheck -x "${files[@]}"
else
  hk_require_cmd docker
  docker run --rm -v "${HK_ROOT}:/src:ro" -w /src \
    "${HK_SHELLCHECK_IMAGE:-koalaman/shellcheck:v0.11.0}" -x "${files[@]}"
fi
