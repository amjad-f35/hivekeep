#!/usr/bin/env bash
# Prepares the scheduled benchmark for an orchestration cluster.
#
#  1. Takes the repository's .hivekeep-dev-env.sh as the target cluster's
#     settings and snapshots it to scratch/target-clusters/<name>.sh. Edit
#     .hivekeep-dev-env.sh and re-run to refresh a snapshot, or re-run with
#     another name to add a target cluster.
#  2. Builds and pushes the orchestrator image to the orchestrator project's
#     registry, the one value the settings file cannot supply.
#  3. Renders three manifests into scratch/, each re-appliable on its own:
#       scratch/cronjob.yaml          Namespace, ServiceAccount, CronJob
#       scratch/test-list.yaml        hivekeep-benchmark-tests ConfigMap
#       scratch/target-clusters.yaml  hivekeep-benchmark-target-clusters ConfigMap
#
# Re-running overwrites those three files and the prompted cluster's
# snapshot; other snapshots in scratch/target-clusters/ are kept.
set -o errexit -o nounset -o pipefail

AUTOMATION_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${AUTOMATION_DIR}/../../.." && pwd)"
SCRATCH_DIR="${AUTOMATION_DIR}/scratch"
TARGET_CLUSTERS_DIR="${SCRATCH_DIR}/target-clusters"
DEV_ENV="${ROOT}/.hivekeep-dev-env.sh"
NAMESPACE="hivekeep-benchmark"

# prompt VAR DESCRIPTION [DEFAULT]: reads VAR, insisting on a value when
# there is no default.
prompt() {
  local var_name="$1" description="$2" default="${3:-}" value=""
  if [[ -n "${default}" ]]; then
    read -r -p "${description} [${default}]: " value
    value="${value:-${default}}"
  else
    while [[ -z "${value}" ]]; do
      read -r -p "${description}: " value
    done
  fi
  printf -v "${var_name}" '%s' "${value}"
}

echo "=== Hivekeep benchmark orchestrator setup ==="
echo
echo "${DEV_ENV} is taken as the target cluster's settings; adjust it before"
echo "continuing if the cluster you are adding needs other values."
echo

if [[ ! -f "${DEV_ENV}" ]]; then
  echo "ERROR: ${DEV_ENV} not found; start from scripts/dev-env.sh.example." >&2
  exit 1
fi

prompt TARGET_CLUSTER_NAME "Target cluster name (tests.yaml entries pick it with targetCluster)" "dev"
prompt ORCHESTRATOR_PROJECT_ID "GCP project of the orchestrator image registry"

ORCHESTRATOR_REPO="gcr.io/${ORCHESTRATOR_PROJECT_ID}/hivekeep"
IMAGE_TAG="$(git -C "${ROOT}" rev-parse --short HEAD)"
if ! git -C "${ROOT}" diff --quiet HEAD -- "${AUTOMATION_DIR}"; then
  IMAGE_TAG="${IMAGE_TAG}-dirty"
fi
ORCHESTRATOR_IMAGE="${ORCHESTRATOR_REPO}/hivekeep-benchmark-orchestrator:${IMAGE_TAG}"

echo
echo "Snapshotting ${DEV_ENV} as target cluster '${TARGET_CLUSTER_NAME}'"
echo "and building ${ORCHESTRATOR_IMAGE}"
echo

mkdir -p "${TARGET_CLUSTERS_DIR}"
# The orchestrator copies this back into its clone as .hivekeep-dev-env.sh
# for every entry with "targetCluster: ${TARGET_CLUSTER_NAME}".
cp "${DEV_ENV}" "${TARGET_CLUSTERS_DIR}/${TARGET_CLUSTER_NAME}.sh"
echo "Snapshotted ${DEV_ENV} -> ${TARGET_CLUSTERS_DIR}/${TARGET_CLUSTER_NAME}.sh"

echo
echo "Building ${ORCHESTRATOR_IMAGE}..."
# Pinned to amd64, the orchestration cluster's architecture, even on an
# arm64 host.
docker build --platform=linux/amd64 -t "${ORCHESTRATOR_IMAGE}" "${AUTOMATION_DIR}"
docker push "${ORCHESTRATOR_IMAGE}"

export ORCHESTRATOR_IMAGE
# shellcheck disable=SC2016 # envsubst takes the literal variable name
envsubst '${ORCHESTRATOR_IMAGE}' \
  <"${AUTOMATION_DIR}/manifests/cronjob.yaml.tmpl" \
  >"${SCRATCH_DIR}/cronjob.yaml"
echo "Wrote ${SCRATCH_DIR}/cronjob.yaml"

# Re-apply this one alone to change the test list.
kubectl create configmap hivekeep-benchmark-tests \
  --namespace="${NAMESPACE}" \
  --from-file=tests.yaml="${AUTOMATION_DIR}/tests.yaml" \
  --dry-run=client -o yaml \
  >"${SCRATCH_DIR}/test-list.yaml"
echo "Wrote ${SCRATCH_DIR}/test-list.yaml"

# One key per snapshot; re-apply this one alone to change cluster settings.
target_args=()
for f in "${TARGET_CLUSTERS_DIR}"/*.sh; do
  target_args+=(--from-file="$(basename "${f}")=${f}")
done
kubectl create configmap hivekeep-benchmark-target-clusters \
  --namespace="${NAMESPACE}" \
  "${target_args[@]}" \
  --dry-run=client -o yaml \
  >"${SCRATCH_DIR}/target-clusters.yaml"
echo "Wrote ${SCRATCH_DIR}/target-clusters.yaml"

cat <<EOF

=== Next steps ===
1. Fill in --repo / --branch / --dest in ${SCRATCH_DIR}/cronjob.yaml.
2. To change a target cluster's settings, edit
   ${TARGET_CLUSTERS_DIR}/${TARGET_CLUSTER_NAME}.sh and re-run, or edit
   ${SCRATCH_DIR}/target-clusters.yaml directly.
3. Apply everything to the orchestration cluster:
     kubectl --context=<orchestration-cluster> apply \\
       -f ${SCRATCH_DIR}/cronjob.yaml \\
       -f ${SCRATCH_DIR}/test-list.yaml \\
       -f ${SCRATCH_DIR}/target-clusters.yaml

Only the test list (no image rebuild):
  kubectl --context=<orchestration-cluster> apply -f ${SCRATCH_DIR}/test-list.yaml
Only the target cluster settings:
  kubectl --context=<orchestration-cluster> apply -f ${SCRATCH_DIR}/target-clusters.yaml
EOF
