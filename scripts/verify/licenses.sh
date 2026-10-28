#!/usr/bin/env bash
# Checks every crate dependency's license against the allowlist in
# scripts/verify/deny.toml (cargo-deny), and every Python requirements file
# under scripts/ and demos/ against a copyleft denylist (pip-licenses).
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
# shellcheck source=../lib/venv.sh
. "${HK_ROOT}/scripts/lib/venv.sh"

deny="$("${HK_ROOT}/scripts/tools/run.sh" --print-bin-path cargo-deny)"
"${deny}" deny --workspace --all-features check \
  --config "${HK_DENY_CONFIG:-${HK_ROOT}/scripts/verify/deny.toml}" licenses

disallowed="AGPL;GPLv2;GPLv3;GPL-2.0;GPL-3.0;GNU General Public License;GNU Affero General Public License;Server Side Public License;SSPL;Commons Clause"
status=0
while IFS= read -r req; do
  venv="$(dirname "${req}")/.venv"
  echo "==> ${req}"
  (
    hk_venv_ensure "${venv}"
    hk_venv_install "${venv}" "${req}"
    "${venv}/bin/pip" install --quiet pip-licenses
    "${venv}/bin/pip-licenses" --fail-on="${disallowed}"
  ) || status=1
done < <(find scripts demos -name requirements.txt -not -path '*/.venv/*' | sort)
exit "${status}"
