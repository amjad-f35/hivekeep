#!/usr/bin/env bash
# Deploys (default) or deletes the egress benchmark's Hivekeep side: the
# load-driver WorkerPool, the load-driver ActorTemplate in the egress-bench
# hive, the `driver` Actor with its egress policy, resumed.
#
# Environment (or .hivekeep-dev-env.sh):
#   HK_BENCH_SNAPSHOT_URI or BUCKET_NAME   where the template's snapshots go
#                                          (default gs://$BUCKET_NAME)
#   WORKER_IMAGE    worker image (default $HK_BUILD_REPO/hivekeep-sandbox-gvisor:<version>)
#   HIVECTL         hivectl binary (default: on PATH, else built here)
set -euo pipefail
# shellcheck source=../../../scripts/lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../../scripts/lib/common.sh"
cd "${HK_ROOT}"
hk_load_dev_env

DIR="crates/hivekeep-bench/nighthawk-egress"
HIVE="egress-bench"
TEMPLATE_NAME="load-driver"
ACTOR_NAME="driver"
WAIT_TIMEOUT_SECS=300

usage() {
  cat <<EOF
Usage: $0 [--deploy | --delete] [options]
  --deploy                deploy the pool, template and ${ACTOR_NAME} Actor (default)
  --delete                delete the Actor, template, hive and pool
  --snapshot-uri URI      snapshot base URI (default \$HK_BENCH_SNAPSHOT_URI, else gs://\$BUCKET_NAME)
  --worker-image IMAGE    worker image (default \$WORKER_IMAGE, else \$HK_BUILD_REPO/hivekeep-sandbox-gvisor:<version>)
  --context CONTEXT       kubectl context (default: \$KUBECTL_CONTEXT, else the current one)
  --wait-timeout SECONDS  wait for the pool and the golden snapshot (default ${WAIT_TIMEOUT_SECS})
  -h, --help              this help
EOF
}

SNAPSHOT_BASE_URI="${HK_BENCH_SNAPSHOT_URI:-${BUCKET_NAME:+gs://${BUCKET_NAME}}}"
action=deploy
while [[ $# -gt 0 ]]; do
  arg="$1" value=""
  if [[ "${arg}" == --*=* ]]; then
    value="${arg#*=}"
    arg="${arg%%=*}"
  fi
  case "${arg}" in
    --deploy) action=deploy ;;
    --delete) action=delete ;;
    --snapshot-uri | --worker-image | --context | --wait-timeout)
      if [[ -z "${value}" ]]; then
        [[ $# -ge 2 ]] || hk_die "${arg} needs a value"
        value="$2"
        shift
      fi
      case "${arg}" in
        --snapshot-uri) SNAPSHOT_BASE_URI="${value}" ;;
        --worker-image) WORKER_IMAGE="${value}" ;;
        --context) KUBECTL_CONTEXT="${value}" ;;
        --wait-timeout) WAIT_TIMEOUT_SECS="${value}" ;;
      esac
      ;;
    -h | --help) usage; exit 0 ;;
    *) usage >&2; hk_die "unknown option $1" ;;
  esac
  shift
done
[[ "${WAIT_TIMEOUT_SECS}" =~ ^[0-9]+$ ]] \
  || hk_die "--wait-timeout must be whole seconds like 300, got '${WAIT_TIMEOUT_SECS}'"

workspace_version() {
  sed -n '/^\[workspace.package\]/,/^\[/s/^version *= *"\(.*\)"/\1/p' Cargo.toml | head -n1
}

# render_pool prints the WorkerPool with its image filled in.
render_pool() {
  sed -e "s|\${WORKER_IMAGE}|${WORKER_IMAGE:-unused}|g" "${DIR}/load-driver-pool.yaml.tmpl"
}

wait_template_ready() {
  hk_log "waiting for the ${HIVE}/${TEMPLATE_NAME} golden snapshot (timeout ${WAIT_TIMEOUT_SECS}s)"
  local deadline=$((SECONDS + WAIT_TIMEOUT_SECS)) json err tag
  while true; do
    json="$(hk_hivectl get actor-template "${TEMPLATE_NAME}" -H "${HIVE}" -o json)"
    err="$(jq -r '.status.goldenSnapshotStatus.errorMessage // empty' <<<"${json}")"
    [[ -z "${err}" ]] || hk_die "actor template ${HIVE}/${TEMPLATE_NAME} golden snapshot failed: ${err}"
    tag="$(jq -r '.status.goldenSnapshotStatus.goldenTag.name // empty' <<<"${json}")"
    if [[ -n "${tag}" ]]; then
      hk_log "${HIVE}/${TEMPLATE_NAME} ready (golden tag ${tag})"
      return 0
    fi
    if ((SECONDS >= deadline)); then
      echo "${json}" >&2
      hk_die "timed out waiting for the ${HIVE}/${TEMPLATE_NAME} golden snapshot"
    fi
    sleep 2
  done
}

remove_actor_and_template() {
  hk_hivectl suspend actor "${ACTOR_NAME}" -H "${HIVE}" >/dev/null 2>&1 || true
  hk_hivectl delete actor "${ACTOR_NAME}" -H "${HIVE}" >/dev/null 2>&1 || true
  hk_hivectl delete actor-template "${TEMPLATE_NAME}" -H "${HIVE}" >/dev/null 2>&1 || true
}

deploy() {
  hk_require_cmd kubectl jq
  [[ -n "${SNAPSHOT_BASE_URI}" ]] || hk_die "no snapshot location: set HK_BENCH_SNAPSHOT_URI or BUCKET_NAME, or pass --snapshot-uri"
  if [[ -z "${WORKER_IMAGE:-}" ]]; then
    hk_require_env HK_BUILD_REPO
    WORKER_IMAGE="${HK_BUILD_REPO}/hivekeep-sandbox-gvisor:$(workspace_version)"
  fi

  hk_kubectl apply -f "${DIR}/namespace.yaml"
  render_pool | hk_kubectl apply -f -
  hk_log "waiting for the ${TEMPLATE_NAME} worker pool (timeout ${WAIT_TIMEOUT_SECS}s)"
  hk_kubectl wait --for=create "deployment/${TEMPLATE_NAME}" --namespace="${HIVE}" --timeout="${WAIT_TIMEOUT_SECS}s"
  hk_kubectl rollout status "deployment/${TEMPLATE_NAME}" --namespace="${HIVE}" --timeout="${WAIT_TIMEOUT_SECS}s"

  hk_hivectl create hive "${HIVE}" >/dev/null 2>&1 || hk_hivectl get hive "${HIVE}" >/dev/null

  # Templates are immutable: start from a clean slate on every deploy.
  remove_actor_and_template
  hk_log "creating actor template ${HIVE}/${TEMPLATE_NAME}"
  sed "s|\${SNAPSHOT_BASE_URI}|${SNAPSHOT_BASE_URI%/}|g" "${DIR}/load-driver-template.yaml.tmpl" \
    | hk_hivectl create actor-template -f -
  wait_template_ready

  hk_log "creating actor ${HIVE}/${ACTOR_NAME} with its egress policy"
  hk_hivectl create actor "${ACTOR_NAME}" -H "${HIVE}" --template "${TEMPLATE_NAME}"
  hk_hivectl create egress-policy "${ACTOR_NAME}" -H "${HIVE}" -f "${DIR}/egress-policy.yaml"
  hk_hivectl resume actor "${ACTOR_NAME}" -H "${HIVE}"
}

delete() {
  hk_log "deleting actor ${HIVE}/${ACTOR_NAME} and template ${HIVE}/${TEMPLATE_NAME}"
  remove_actor_and_template
  hk_hivectl delete hive "${HIVE}" >/dev/null 2>&1 || hk_log "hive ${HIVE} not deleted (absent or not empty)"
  render_pool | hk_kubectl delete --ignore-not-found -f -
}

"${action}"
