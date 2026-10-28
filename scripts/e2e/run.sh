#!/usr/bin/env bash
# Runs the end-to-end tests against the cluster in KUBECTL_CONTEXT.
#
#   scripts/e2e/run.sh [cargo-test-args...] [-- test-binary-args...]
#
# The e2e tests are `#[ignore]`d integration tests of HK_E2E_PACKAGE (default
# hivekeep-e2e); this runs them with --ignored and HK_E2E=1 so they talk to
# the cluster. The cluster must be installed with --e2e-tuning
# (HK_INSTALL_E2E_TUNING=true) so the keeper uses short snapshot delays.
#
# Environment:
#   HK_E2E_PACKAGE        crate holding the suites (default hivekeep-e2e)
#   E2E_TEST_THREADS      concurrent tests (default 8)
#   E2E_TIMEOUT           whole-run timeout, e.g. 30m (default 30m)
#   E2E_JUNIT_FILE        write a JUnit report here (uses cargo-nextest);
#                         required when CI=true
#   HK_E2E_STORAGE_CLASS  StorageClass for external-volume tests (default csi-nfs-sc)
#   HK_PREVIEW            preview gates the cluster was installed with
#   KUBECTL_CONTEXT       context the suites target
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
# shellcheck source=../lib/junit.sh
. "${HK_ROOT}/scripts/lib/junit.sh"
cd "${HK_ROOT}"
hk_load_dev_env

if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
  sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

pkg="${HK_E2E_PACKAGE:-hivekeep-e2e}"
if [[ ! -d "crates/${pkg}" ]]; then
  hk_die "no e2e crate at crates/${pkg}; set HK_E2E_PACKAGE to the crate holding the suites"
fi

cargo_args=()
test_args=()
seen_sep=false
for a in "$@"; do
  if [[ "${a}" == -- && "${seen_sep}" == false ]]; then
    seen_sep=true
    continue
  fi
  if [[ "${seen_sep}" == true ]]; then
    test_args+=("${a}")
  else
    cargo_args+=("${a}")
  fi
done

export HK_E2E=1
export HK_E2E_STORAGE_CLASS="${HK_E2E_STORAGE_CLASS:-csi-nfs-sc}"
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
  export HK_E2E_KUBE_CONTEXT="${KUBECTL_CONTEXT}"
fi
threads="${E2E_TEST_THREADS:-8}"
timeout_spec="${E2E_TIMEOUT:-30m}"

timeout_cmd=()
if command -v timeout >/dev/null 2>&1; then
  timeout_cmd=(timeout "${timeout_spec}")
elif command -v gtimeout >/dev/null 2>&1; then
  timeout_cmd=(gtimeout "${timeout_spec}")
fi

hk_require_junit_in_ci E2E_JUNIT_FILE e2e
if [[ -n "${E2E_JUNIT_FILE:-}" ]]; then
  mkdir -p "$(dirname "${E2E_JUNIT_FILE}")"
  hk_junit_register "${E2E_JUNIT_FILE}"
  cfg="$(hk_nextest_config)"
  status=0
  "${timeout_cmd[@]+"${timeout_cmd[@]}"}" "${HK_ROOT}/scripts/tools/run.sh" cargo-nextest nextest run \
    --tool-config-file "hk:${cfg}" --profile hk -p "${pkg}" --run-ignored only \
    --test-threads "${threads}" "${cargo_args[@]+"${cargo_args[@]}"}" \
    -- "${test_args[@]+"${test_args[@]}"}" || status=$?
  cp "${CARGO_TARGET_DIR:-${HK_ROOT}/target}/nextest/hk/junit.xml" "${E2E_JUNIT_FILE}" || true
  rm -f "${cfg}"
  exit "${status}"
fi

exec "${timeout_cmd[@]+"${timeout_cmd[@]}"}" cargo test -p "${pkg}" "${cargo_args[@]+"${cargo_args[@]}"}" -- \
  --ignored --test-threads "${threads}" "${test_args[@]+"${test_args[@]}"}"
