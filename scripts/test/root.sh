#!/usr/bin/env bash
# Runs the tests that need root (Linux only). Those are `#[ignore]`d with a
# reason starting "needs root"; this builds the workspace test binaries as the
# invoking user (so target/ never gets root-owned files) and then runs them
# under sudo with --ignored, filtered to the root tests.
#
#   scripts/test/root.sh [extra test-binary args...]
#
#   ROOT_TEST_FILTER   name filter passed to each binary (default: none)
#   ROOT_TEST_SKIP     comma-separated --skip patterns for ignored tests that
#                      need something other than root (default covers the
#                      database- and object-store-backed ones)
#   ROOT_JUNIT_FILE    also write a JUnit report (via cargo-nextest); required when CI=true
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
# shellcheck source=../lib/junit.sh
. "${HK_ROOT}/scripts/lib/junit.sh"
cd "${HK_ROOT}"

[[ "$(uname -s)" == Linux ]] || hk_die "root tests only exist on Linux"
hk_require_cmd cargo python3

skip_csv="${ROOT_TEST_SKIP:-postgres,database,db::,s3_upload_against_real_server,another_mount,third_party,hostpath,nfs_setup}"
skips=()
IFS=',' read -r -a skip_list <<<"${skip_csv}"
for s in "${skip_list[@]}"; do
  [[ -n "${s}" ]] && skips+=(--skip "${s}")
done
filter=()
if [[ -n "${ROOT_TEST_FILTER:-}" ]]; then
  filter=("${ROOT_TEST_FILTER}")
fi

hk_require_junit_in_ci ROOT_JUNIT_FILE root
if [[ -n "${ROOT_JUNIT_FILE:-}" ]]; then
  mkdir -p "$(dirname "${ROOT_JUNIT_FILE}")"
  hk_junit_register "${ROOT_JUNIT_FILE}"
  nextest="$("${HK_ROOT}/scripts/tools/run.sh" --print-bin-path cargo-nextest)"
  archive="$(mktemp -d)/root-tests.tar.zst"
  "${nextest}" nextest archive --workspace --archive-file "${archive}"
  cfg="$(hk_nextest_config)"
  status=0
  sudo -E env "PATH=${PATH}" "${nextest}" nextest run --archive-file "${archive}" \
    --workspace-remap "${HK_ROOT}" --tool-config-file "hk:${cfg}" --profile hk \
    --run-ignored only -- "${filter[@]+"${filter[@]}"}" "${skips[@]}" "$@" || status=$?
  sudo chown "$(id -u):$(id -g)" -R "${CARGO_TARGET_DIR:-${HK_ROOT}/target}/nextest" 2>/dev/null || true
  cp "${CARGO_TARGET_DIR:-${HK_ROOT}/target}/nextest/hk/junit.xml" "${ROOT_JUNIT_FILE}" || true
  exit "${status}"
fi

# Build as the user, collecting the test executables cargo reports.
mapfile -t bins < <(cargo test --workspace --no-run --message-format=json 2>/dev/null |
  python3 -c 'import json,sys
for line in sys.stdin:
    try: m = json.loads(line)
    except ValueError: continue
    if m.get("reason") == "compiler-artifact" and m.get("profile", {}).get("test") and m.get("executable"):
        print(m["executable"])')
[[ ${#bins[@]} -gt 0 ]] || hk_die "no test binaries were built"

status=0
for b in "${bins[@]}"; do
  echo "==> ${b##*/}"
  if [[ "$(id -u)" -eq 0 ]]; then
    "${b}" --ignored "${filter[@]+"${filter[@]}"}" "${skips[@]}" "$@" || status=1
  else
    sudo -E "${b}" --ignored "${filter[@]+"${filter[@]}"}" "${skips[@]}" "$@" || status=1
  fi
done
exit "${status}"
