#!/usr/bin/env bash
# Regenerates docs/threats.json from the tables in docs/threat-model.md.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
md="${HK_THREAT_MODEL:-docs/threat-model.md}"
if [[ ! -f "${md}" ]]; then
  echo "no ${md}; nothing to do"
  exit 0
fi
python3 scripts/threats/export_threats.py --md "${md}" --out "${HK_THREATS_JSON:-docs/threats.json}"
