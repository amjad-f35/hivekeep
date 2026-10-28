#!/usr/bin/env bash
# Uploads an assembled micro-VM asset set into the Kind cluster's rustfs
# bucket under kata-assets/. The S3 client runs in a throwaway container that
# shares the Kind node's network namespace, so the rustfs ClusterIP is
# routable without a port-forward and no local aws CLI is needed.
#   OUT               asset directory (default bin/microvm-assets/<host arch>)
#   BUCKET            bucket (default hivekeep-snapshots)
#   NAMESPACE         rustfs namespace (default hivekeep-system)
#   KUBECTL_CONTEXT   kube context; KIND_CLUSTER_NAME is derived from it
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY  rustfs credentials (Kind defaults)
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

OUT="${OUT:-${HK_ROOT}/bin/microvm-assets/$(hk_host_arch)}"
BUCKET="${BUCKET:-hivekeep-snapshots}"
NAMESPACE="${NAMESPACE:-hivekeep-system}"
ctx="${KUBECTL_CONTEXT:-}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-${ctx#kind-}}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
# Same pinned client as the bucket-init Job in the Kind overlay.
AWS_CLI_IMAGE="${HK_AWS_CLI_IMAGE:-amazon/aws-cli:2.38.5@sha256:c8156a0e74e13929a42d7e60f0d4e14e7d28d980cd21822505d4bff80e5460c1}"
ASSETS=(cloud-hypervisor virtiofsd vmlinux rootfs.img)
hk_require_cmd docker kubectl

for f in "${ASSETS[@]}"; do
  [[ -f "${OUT}/${f}" ]] || hk_die "missing ${OUT}/${f}; run scripts/microvm-assets/assemble.sh first"
done

hk_log "waiting for rustfs and its bucket in ${NAMESPACE}"
hk_kubectl -n "${NAMESPACE}" rollout status deploy/rustfs --timeout=300s
hk_kubectl -n "${NAMESPACE}" wait --for=condition=Complete job/rustfs-bucket-init --timeout=300s

node="$(hk_kind get nodes --name "${KIND_CLUSTER_NAME}" | head -n1)"
[[ -n "${node}" ]] || hk_die "no nodes in kind cluster ${KIND_CLUSTER_NAME}"
endpoint="http://$(hk_kubectl -n "${NAMESPACE}" get svc rustfs -o jsonpath='{.spec.clusterIP}'):9000"

s3() {
  # No -t: a TTY would mangle the binary stream on stdin.
  docker run --rm -i --network "container:${node}" \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-rustfsadmin}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-rustfsadmin}" \
    -e AWS_REGION="${AWS_REGION:-us-east-1}" \
    -e AWS_ENDPOINT_URL="${endpoint}" \
    "${AWS_CLI_IMAGE}" "$@"
}

for f in "${ASSETS[@]}"; do
  hk_log "uploading ${f} to s3://${BUCKET}/kata-assets/"
  s3 s3 cp - "s3://${BUCKET}/kata-assets/${f}" <"${OUT}/${f}"
done
s3 s3 ls "s3://${BUCKET}/kata-assets/" </dev/null
