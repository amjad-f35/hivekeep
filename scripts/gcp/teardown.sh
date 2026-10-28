#!/usr/bin/env bash
# Tears down the GCP resources `hivekeep-setup gcp bootstrap` created, step by
# step or all at once (reverse order of setup). Every step only talks to GCP,
# so it works on a half-deleted cluster. Settings come from the environment
# or .hivekeep-dev-env.sh; each step checks only the variables it needs.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
hk_load_dev_env
hk_require_cmd gcloud

NS="${HK_NAMESPACE:-hivekeep-system}"
KEEPER_SA="${HK_KEEPER_SERVICE_ACCOUNT:-keeper}"
API_SA="${HK_API_SERVICE_ACCOUNT:-hivekeep-api}"

usage() {
  cat <<'TXT'
Usage: scripts/gcp/teardown.sh <step> [<step> ...]
  --revoke-gke-node-permissions      remove the nodes' image/object read grants
  --revoke-keeper-permissions        remove the keeper's project-level grants
  --delete-iam-policy-bindings       remove bucket grants (keeper, hivekeep-api, transfer account)
  --delete-transfer-service-account  delete the snapshot transfer service account
  --delete-snapshot-bucket           empty and delete the snapshot and asset buckets
  --delete-node-pool                 delete NODE_POOL_NAME
  --delete-cluster                   delete the GKE cluster
  --delete-dashboards                delete the Hivekeep monitoring dashboards
  --all                              everything above, in reverse setup order
TXT
  exit 1
}

transfer_sa() {
  echo "${SNAPSHOT_TRANSFER_GSA_NAME:-hivekeep-snapshot-transfer}@${PROJECT_ID}.iam.gserviceaccount.com"
}
wi_principal() {
  echo "principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/${NS}/sa/$1"
}
remove_project_binding() {
  gcloud projects remove-iam-policy-binding "${PROJECT_ID}" --member="$1" --role="$2" --condition=None --quiet >/dev/null || true
}

revoke_gke_node_permissions() {
  hk_require_env PROJECT_ID PROJECT_NUMBER
  hk_log "revoking node image/object read grants"
  local m="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
  remove_project_binding "${m}" roles/storage.objectViewer
  remove_project_binding "${m}" roles/artifactregistry.reader
}

revoke_keeper_permissions() {
  hk_require_env PROJECT_ID PROJECT_NUMBER
  hk_log "revoking keeper project grants"
  local m
  m="$(wi_principal "${KEEPER_SA}")"
  remove_project_binding "${m}" roles/storage.objectAdmin
  remove_project_binding "${m}" roles/artifactregistry.reader
}

delete_dashboards() {
  hk_require_env PROJECT_ID
  hk_log "deleting dashboards"
  local names=("Hivekeep Snapshot Size & QPS" "Hivekeep Routing & E2E Latency" "Hivekeep gRPC Server — latency / QPS / errors")
  local n d
  for n in "${names[@]}"; do
    for d in $(gcloud monitoring dashboards list --project="${PROJECT_ID}" --filter="displayName=\"${n}\"" --format='value(name)' 2>/dev/null); do
      gcloud monitoring dashboards delete "${d}" --project="${PROJECT_ID}" --quiet || true
    done
  done
}

delete_iam_policy_bindings() {
  hk_require_env PROJECT_ID PROJECT_NUMBER BUCKET_NAME
  hk_log "removing bucket grants"
  local subject role
  for subject in "${KEEPER_SA}" "${API_SA}"; do
    for role in roles/storage.objectAdmin roles/storage.bucketViewer; do
      gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" --member="$(wi_principal "${subject}")" --role="${role}" --quiet >/dev/null || true
    done
  done
  for role in roles/storage.objectAdmin roles/storage.bucketViewer; do
    gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" --member="serviceAccount:$(transfer_sa)" --role="${role}" --quiet >/dev/null || true
  done
  gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}-assets" --member="$(wi_principal "${KEEPER_SA}")" --role=roles/storage.objectViewer --quiet >/dev/null || true
}

delete_transfer_service_account() {
  hk_require_env PROJECT_ID
  hk_log "deleting $(transfer_sa)"
  gcloud iam service-accounts delete "$(transfer_sa)" --project="${PROJECT_ID}" --quiet || true
}

delete_snapshot_bucket() {
  hk_require_env PROJECT_ID BUCKET_NAME
  local b
  for b in "${BUCKET_NAME}" "${BUCKET_NAME}-assets"; do
    hk_log "deleting gs://${b}"
    gcloud storage rm --recursive "gs://${b}/**" --project="${PROJECT_ID}" --quiet || true
    gcloud storage buckets delete "gs://${b}" --project="${PROJECT_ID}" --quiet || true
  done
}

delete_node_pool() {
  hk_require_env PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION NODE_POOL_NAME
  hk_log "deleting node pool ${NODE_POOL_NAME}"
  gcloud container node-pools delete "${NODE_POOL_NAME}" --cluster="${CLUSTER_NAME}" --location="${CLUSTER_LOCATION}" --project="${PROJECT_ID}" --quiet || true
}

delete_cluster() {
  hk_require_env PROJECT_ID CLUSTER_NAME CLUSTER_LOCATION
  hk_log "deleting cluster ${CLUSTER_NAME}"
  gcloud container clusters delete "${CLUSTER_NAME}" --location="${CLUSTER_LOCATION}" --project="${PROJECT_ID}" --quiet || true
}

[[ $# -gt 0 ]] || usage
while [[ $# -gt 0 ]]; do
  case "$1" in
    --revoke-gke-node-permissions) revoke_gke_node_permissions ;;
    --revoke-keeper-permissions) revoke_keeper_permissions ;;
    --delete-iam-policy-bindings) delete_iam_policy_bindings ;;
    --delete-transfer-service-account) delete_transfer_service_account ;;
    --delete-snapshot-bucket) delete_snapshot_bucket ;;
    --delete-node-pool) delete_node_pool ;;
    --delete-cluster) delete_cluster ;;
    --delete-dashboards) delete_dashboards ;;
    --all)
      delete_dashboards
      delete_iam_policy_bindings
      delete_transfer_service_account
      revoke_keeper_permissions
      revoke_gke_node_permissions
      delete_snapshot_bucket
      # The cluster takes its pools with it; the pool step is optional here.
      if [[ -n "${NODE_POOL_NAME:-}" ]]; then delete_node_pool; fi
      delete_cluster
      ;;
    *) usage ;;
  esac
  shift
done
