#!/usr/bin/env bash
# hivekeep-api moves snapshots through the object-store plugin sidecar, so it
# must not link an object-store client itself. Fails if its normal
# dependency tree contains one of the banned crates.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

banned=(hivekeep-objectstore aws-sdk-s3 google-cloud-storage cloud-storage object_store)
deps="$(cargo tree -p hivekeep-api -e normal --prefix none --format '{p}' 2>/dev/null | awk '{print $1}' | sort -u)"
[[ -n "${deps}" ]] || hk_die "could not list hivekeep-api dependencies"
status=0
for c in "${banned[@]}"; do
  if grep -qx -- "${c}" <<<"${deps}"; then
    echo "hivekeep-api must not depend on ${c}" >&2
    status=1
  fi
done
exit "${status}"
