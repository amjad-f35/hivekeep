#!/usr/bin/env bash
# Fails when docs/threats.json is stale relative to docs/threat-model.md.
# Skips (successfully) when the threat model is not in the tree.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

md="${HK_THREAT_MODEL:-docs/threat-model.md}"
json="${HK_THREATS_JSON:-docs/threats.json}"
if [[ ! -f "${md}" ]]; then
  echo "no ${md}; skipping"
  exit 0
fi
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
python3 scripts/threats/export_threats.py --md "${md}" --out "${tmp}/threats.json" >/dev/null
if ! diff -u "${json}" "${tmp}/threats.json"; then
  hk_die "${json} is out of date; run scripts/update/threats.sh"
fi
