#!/usr/bin/env bash
# Formats every .proto file in place with clang-format. Long lines are not
# reflowed: they are mostly comments, and reflowing them churns generated code.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
hk_require_cmd clang-format
files=()
while IFS= read -r f; do files+=("${f}"); done < <(find crates -name '*.proto' -not -path '*/target/*' -not -path '*/third_party/*' | sort)
[[ ${#files[@]} -gt 0 ]] || exit 0
clang-format -i --style="{BasedOnStyle: LLVM, ColumnLimit: 0}" "${files[@]}"
