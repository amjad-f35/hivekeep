#!/usr/bin/env bash
# Deploys or removes the interactive locust benchmark in the benchmarking
# namespace: the locust master, with the web UI and /loadgen-config, and
# locust-worker as its one worker (see manifests/locust.yaml).
#
# Environment:
#   LOCUST_IMAGE               image (default
#                              $HK_BUILD_REPO/hivekeep-bench-locust:<version>)
#   LOCUST_IMAGE_PULL_POLICY   pull policy (default Always)
#   OTLP_ENDPOINT              where both containers send traces (default:
#                              none)
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"
cd "${HK_ROOT}"
if declare -F hk_load_dev_env >/dev/null; then
  hk_load_dev_env
fi

SCRIPT_DIR="crates/hivekeep-bench/locust"
MANIFEST="${SCRIPT_DIR}/manifests/locust.yaml"
NAMESPACE=benchmarking

version="$(sed -n '/^\[workspace.package\]/,/^\[/s/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -n1)"
LOCUST_IMAGE_PULL_POLICY="${LOCUST_IMAGE_PULL_POLICY:-Always}"
OTLP_ENDPOINT="${OTLP_ENDPOINT:-}"

BENCHMARK_USER_CLASS=glutton
# A local agent-session script YAML to run instead of a built-in one. It is
# uploaded as the agentsession-script ConfigMap, mounted in the worker at
# AGENTSESSION_SCRIPT_MOUNT, and the master serves that path as the default
# of --agentsession-script-file.
AGENTSESSION_SCRIPT=""
AGENTSESSION_SCRIPT_MOUNT=/etc/agentsession/script.yaml

usage() {
  cat <<'TXT'
Usage: locust/deploy.sh --deploy|--delete [options]
  --user-class NAME            load class to run, lowercase; runs tests/NAME.py
                               (default glutton): glutton spawn durdir hkenv
                               resumepaused sweperf agentsession
  --agentsession-script FILE   run this script YAML in the agentsession class
                               instead of a built-in one; validated locally,
                               then mounted into the worker
  -h, --help                   this help
TXT
}

render() {
  sed -e "s|\${LOCUST_IMAGE}|${LOCUST_IMAGE}|g" \
      -e "s|\${LOCUST_IMAGE_PULL_POLICY}|${LOCUST_IMAGE_PULL_POLICY}|g" \
      -e "s|\${BENCHMARK_USER_CLASS}|${BENCHMARK_USER_CLASS}|g" \
      -e "s|\${OTLP_ENDPOINT}|${OTLP_ENDPOINT}|g" \
      -e "s|\${AGENTSESSION_SCRIPT_FILE}|${AGENTSESSION_SCRIPT_FILE}|g" \
      -e "s|\${AGENTSESSION_SCRIPT_SHA}|${AGENTSESSION_SCRIPT_SHA}|g" \
      "${MANIFEST}"
}

deploy() {
  hk_log "ensuring the ${NAMESPACE} namespace"
  kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
  # hivekeep-system's NetworkPolicies admit API calls only from namespaces
  # carrying this label.
  kubectl label namespace "${NAMESPACE}" hivekeep.io/api-access=true --overwrite
  if [[ -n "${AGENTSESSION_SCRIPT}" ]]; then
    hk_log "validating agent-session script ${AGENTSESSION_SCRIPT}"
    cargo run --quiet --release --locked -p hivekeep-bench --bin locust-worker -- \
      --check-agentsession-script "${AGENTSESSION_SCRIPT}"
    kubectl -n "${NAMESPACE}" create configmap agentsession-script \
      --from-file="script.yaml=${AGENTSESSION_SCRIPT}" --dry-run=client -o yaml | kubectl apply -f -
  fi
  hk_log "deploying locust (image=${LOCUST_IMAGE}, class=${BENCHMARK_USER_CLASS}, script=${AGENTSESSION_SCRIPT:-built-in})"
  render | kubectl apply -f -
}

delete() {
  hk_log "deleting locust"
  render | kubectl delete --ignore-not-found -f -
  kubectl -n "${NAMESPACE}" delete configmap agentsession-script --ignore-not-found
}

[[ $# -gt 0 ]] || { usage >&2; exit 1; }

action=""
while [[ $# -gt 0 ]]; do
  arg="$1"
  value=""
  if [[ "${arg}" == --*=* ]]; then
    value="${arg#*=}"
    arg="${arg%%=*}"
  fi
  case "${arg}" in
    --deploy) action=deploy ;;
    --delete) action=delete ;;
    -h | --help) usage; exit 0 ;;
    --user-class | --agentsession-script)
      if [[ -z "${value}" ]]; then
        [[ $# -ge 2 ]] || hk_die "${arg} needs a value"
        value="$2"
        shift
      fi
      if [[ "${arg}" == --user-class ]]; then
        BENCHMARK_USER_CLASS="$(printf '%s' "${value}" | tr '[:upper:]' '[:lower:]')"
      else
        AGENTSESSION_SCRIPT="${value}"
      fi
      ;;
    *) usage >&2; hk_die "unknown option $1" ;;
  esac
  shift
done
[[ -n "${action}" ]] || { usage >&2; exit 1; }
hk_require_cmd kubectl

[[ -f "${SCRIPT_DIR}/tests/${BENCHMARK_USER_CLASS}.py" ]] \
  || hk_die "no tests/${BENCHMARK_USER_CLASS}.py; --user-class must name a locustfile"
if [[ -z "${LOCUST_IMAGE:-}" ]]; then
  if [[ "${action}" == deploy ]]; then
    hk_require_env HK_BUILD_REPO
  fi
  LOCUST_IMAGE="${HK_BUILD_REPO:-unused}/hivekeep-bench-locust:${version}"
fi

# Empty leaves the master's --agentsession-script-file unset, so the worker
# runs the built-in script --agentsession-script names. The checksum goes
# into the worker's env: a different script changes the pod template and so
# rolls the pod, which an otherwise identical manifest would not.
AGENTSESSION_SCRIPT_FILE=""
AGENTSESSION_SCRIPT_SHA=""
if [[ -n "${AGENTSESSION_SCRIPT}" ]]; then
  [[ "${BENCHMARK_USER_CLASS}" == agentsession ]] || hk_die "--agentsession-script needs --user-class agentsession"
  [[ -f "${AGENTSESSION_SCRIPT}" ]] || hk_die "--agentsession-script ${AGENTSESSION_SCRIPT}: no such file"
  # Absolute: validation runs from the repository root.
  AGENTSESSION_SCRIPT="$(cd "$(dirname "${AGENTSESSION_SCRIPT}")" && pwd)/$(basename "${AGENTSESSION_SCRIPT}")"
  AGENTSESSION_SCRIPT_FILE="${AGENTSESSION_SCRIPT_MOUNT}"
  AGENTSESSION_SCRIPT_SHA="$( (sha256sum "${AGENTSESSION_SCRIPT}" 2>/dev/null || shasum -a 256 "${AGENTSESSION_SCRIPT}") | cut -c1-16)"
fi

"${action}"
