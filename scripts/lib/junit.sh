# shellcheck shell=bash
# Helpers for test runners that optionally write a JUnit report.

# hk_junit_register claims JUnit path $1 in the run manifest before the run,
# so a run killed part-way is reported as missing its file.
hk_junit_register() {
  cargo run --quiet --manifest-path "${HK_ROOT}/Cargo.toml" -p hivekeep-devtools --bin hivekeep-junittool -- register "$1"
}

# hk_nextest_config writes a nextest config whose `hk` profile emits JUnit
# and prints its path.
hk_nextest_config() {
  local cfg
  cfg="$(mktemp)"
  cat >"${cfg}" <<'TOML'
[profile.hk]
fail-fast = false
[profile.hk.junit]
path = "junit.xml"
store-success-output = false
TOML
  echo "${cfg}"
}

# hk_require_junit_in_ci fails when CI=true and variable $1 is unset: CI
# checks that every run produced a report.
hk_require_junit_in_ci() {
  if [[ "${CI:-}" == true && -z "${!1:-}" ]]; then
    hk_die "$1 must be set when CI=true (a path unique to this run, e.g. \${ARTIFACTS}/$2.xml)"
  fi
}
