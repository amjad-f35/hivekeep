#!/usr/bin/env bash
# Runs every Python unit-test suite in the repository with pytest, inside a
# Python 3.10+ virtual environment (HK_PYTHON_VENV, default
# target/python-venv). The venv is created on first use and gets the
# benchmark suite's requirements plus pytest; a stamp of the requirement
# files skips the install when nothing changed.
#
# Commands the code under test shells out to are stubbed to fail, so an
# escaped mock can never reach a real cluster or account.
#
# Each suite runs in its own pytest process: several suites have top-level
# modules of the same name (runner.py, for one) that would shadow each other
# in a shared interpreter. Extra arguments are passed to every pytest run.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
# shellcheck source=../lib/venv.sh
. "${HK_ROOT}/scripts/lib/venv.sh"
cd "${HK_ROOT}"

venv="${HK_PYTHON_VENV:-${HK_ROOT}/target/python-venv}"
bench=crates/hivekeep-bench
requirements=(
  "${bench}/locust/requirements.txt"
  "${bench}/automation/requirements.txt"
  "${bench}/nighthawk-ingress/requirements.txt"
)
suites=(
  scripts/threats
  scripts/metrics
  "${bench}/analysis"
  "${bench}/automation"
  "${bench}/locust/unit_tests"
  "${bench}/nighthawk-ingress/tests"
  "${bench}/workloads/manifests/sweperf"
)

hk_venv_ensure "${venv}"
stamp="${venv}/.hk-requirements"
want="$(cat "${requirements[@]}" | (sha256sum 2>/dev/null || shasum -a 256) | cut -d' ' -f1)"
if [[ "$(cat "${stamp}" 2>/dev/null)" != "${want}" ]]; then
  hk_log "installing test requirements into ${venv}"
  for req in "${requirements[@]}"; do
    hk_venv_install "${venv}" "${req}"
  done
  "${venv}/bin/pip" install --quiet pytest
  printf '%s\n' "${want}" >"${stamp}"
fi
py="${venv}/bin/python3"
hk_log "using $("${py}" --version)"

stubs="$(mktemp -d)"
trap 'rm -rf "${stubs}"' EXIT
for cmd in gh gcloud gsutil kubectl docker helm kind; do
  printf '#!/bin/sh\necho "%s: refusing to run from a unit test" >&2\nexit 1\n' "${cmd}" >"${stubs}/${cmd}"
  chmod +x "${stubs}/${cmd}"
done
export PATH="${stubs}:${PATH}" KUBECONFIG=/nonexistent

failed=()
for dir in "${suites[@]}"; do
  echo "==> ${dir}"
  if ! (cd "${dir}" && "${py}" -m pytest -q -p no:cacheprovider "$@" .); then
    failed+=("${dir}")
  fi
done
if [[ ${#failed[@]} -gt 0 ]]; then
  hk_die "Python tests failed in: ${failed[*]}"
fi
hk_log "all ${#suites[@]} Python suites passed"
