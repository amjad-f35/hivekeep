#!/usr/bin/env bash
# Checks the metric registry two ways:
#
#   1. against the code: every Prometheus family the components register
#      (`hivekeep_*`, `controller_runtime_*`) is declared in
#      `metrics.yaml`, every declared metric is registered somewhere, and
#      every metric the end-to-end suite asserts on is declared;
#   2. with OpenTelemetry Weaver (local binary of the pinned version, else
#      the pinned image), unless HK_METRICS_SKIP_WEAVER=1.
#
# Skips when there is no registry.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

registry="${HK_METRICS_REGISTRY:-crates/hivekeep-core/registry}"
version="${WEAVER_VERSION:-v0.25.1}"
image="${WEAVER_IMAGE:-docker.io/otel/weaver:v0.25.1@sha256:9ad46ca9cd4fa5974b121f886aa3e9946a8ef8ea905001a96c018d21f9db87ca}"
if [[ ! -d "${registry}" ]]; then
  echo "no metric registry at ${registry}; skipping"
  exit 0
fi

# --- 1. registry against code -------------------------------------------

metrics_file="${registry}/metrics.yaml"
[[ -f "${metrics_file}" ]] || hk_die "no ${metrics_file}: declare the metric instruments there"

# The Prometheus name of every declared metric, one per line, derived as
# the file's header describes. Also prints the dotted OTel name as a
# second column.
declared() {
  awk '
    function flush() {
      if (name == "") return
      prom = name; gsub(/\./, "_", prom)
      if (unit == "s") prom = prom "_seconds"
      else if (unit == "By") prom = prom "_bytes"
      if (inst == "counter") prom = prom "_total"
      print prom, name
      name = ""; unit = ""; inst = ""
    }
    /^  - id: / { flush() }
    /^    metric_name: / { name = $2 }
    /^    instrument: / { inst = $2 }
    /^    unit: / { unit = $2; gsub(/"/, "", unit) }
    END { flush() }
  ' "${metrics_file}"
}

# Families registered in production code: name literals in files that build
# Prometheus instruments, and `{METRIC_PREFIX}_...` names formatted at run
# time. Tests, test harnesses, the end-to-end suite and the benchmark tools
# are left out; the benchmarks report their own `glutton_*`/`locust_*`
# series.
registered() {
  local files
  files="$(grep -rlE 'Opts::new\(|HistogramOpts::new\(' crates/*/src --include='*.rs' |
    grep -vE '(_tests?\.rs|/tests?/|/tests_[^/]*\.rs|test_support|_harness\.rs|/fake_cluster\.rs|/testutil\.rs)$' |
    grep -vE '^crates/hivekeep-(e2e|bench|bench-tunnel)/' || true)"
  [[ -n "${files}" ]] || return 0
  # shellcheck disable=SC2086
  {
    grep -hoE '"(hivekeep|controller_runtime)_[a-z0-9_]+"' ${files} | tr -d '"'
    grep -hoE '\{METRIC_PREFIX\}_[a-z0-9_]+' ${files} | sed 's/{METRIC_PREFIX}/hivekeep/'
  } | sort -u
}

decl="$(declared)"
decl_prom="$(awk '{print $1}' <<<"${decl}" | sort -u)"
code="$(registered)"
failed=0

while read -r name; do
  [[ -n "${name}" ]] || continue
  if ! grep -qxF "${name}" <<<"${decl_prom}"; then
    echo "registered in code but not declared in ${metrics_file}: ${name}" >&2
    failed=1
  fi
done <<<"${code}"

while read -r prom dotted; do
  [[ -n "${prom}" ]] || continue
  if grep -qxF "${prom}" <<<"${code}"; then
    continue
  fi
  # An OpenTelemetry instrument is named by its dotted name in code.
  if grep -rqF "\"${dotted}\"" crates/*/src --include='*.rs'; then
    continue
  fi
  echo "declared in ${metrics_file} but registered nowhere: ${prom} (${dotted})" >&2
  failed=1
done <<<"${decl}"

# Every name prefix the end-to-end suite asserts on resolves to a declared
# metric.
e2e_file="crates/hivekeep-e2e/src/collector_metrics.rs"
if [[ -f "${e2e_file}" ]]; then
  e2e="$(awk '/PLATFORM_METRIC_PREFIXES: &\[&str\] = &\[/,/^\];/' "${e2e_file}" | grep -oE '"hivekeep_[a-z0-9_]+"' | tr -d '"')"
  e2e+=$'\n'"$(grep -oE 'LIFECYCLE_EVENT_METRIC: &str = "[a-z0-9_]+"' "${e2e_file}" | grep -oE '"[a-z0-9_]+"' | tr -d '"')"
  while read -r prefix; do
    [[ -n "${prefix}" ]] || continue
    if ! grep -q "^${prefix}" <<<"${decl_prom}"; then
      echo "the e2e suite asserts on ${prefix}, which no declared metric provides" >&2
      failed=1
    fi
  done <<<"${e2e}"
fi

((failed == 0)) || hk_die "the metric registry and the code disagree (see above)"
echo "metric registry matches the code: $(wc -l <<<"${decl_prom}" | tr -d ' ') metrics"

# --- 2. Weaver ----------------------------------------------------------

if [[ "${HK_METRICS_SKIP_WEAVER:-}" == "1" ]]; then
  echo "HK_METRICS_SKIP_WEAVER=1; skipping weaver registry check"
  exit 0
fi

weaver_run() {
  if command -v weaver >/dev/null 2>&1; then
    local have
    have="v$(weaver --version | awk '{print $NF}')"
    # A different version may judge differently; refuse rather than mislead.
    [[ "${have}" == "${version}" ]] || hk_die "local weaver is ${have}, checks need ${version} (or remove it to use the image)"
    weaver "$@"
  else
    hk_require_cmd docker
    docker run --rm --user "$(id -u):$(id -g)" -v "${HK_ROOT}:/workspace:ro" -w /workspace "${image}" "$@"
  fi
}
# --future turns planned checks (e.g. duplicate metric names) into errors.
weaver_run registry check --registry "${registry}" --future ||
  hk_die "the metric registry under ${registry} is invalid"
