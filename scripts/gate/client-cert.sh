#!/usr/bin/env bash
# Mints a client certificate for the gate router's CONNECT listener, issued
# by the pod identity signer to the hivekeep-gate-client ServiceAccount, and
# writes the key and chain for `hivectl proxy --client-cert/--client-key`.
#
# The caller needs create/get/delete on podcertificaterequests in the system
# namespace (Role hivekeep-gate-client-minter), and the router's
# --ingress-allowed-spiffe-ids must list
# spiffe://cluster.local/<ns>/sa/hivekeep-gate-client.
#
# Usage: scripts/gate/client-cert.sh [OUT_DIR]   (default ./.hivekeep-gate-client)
#   HK_SYSTEM_NS      system namespace (default hivekeep-system)
#   HK_CLIENT_SA      ServiceAccount to mint for (default hivekeep-gate-client)
#   HK_CERT_TIMEOUT   seconds to wait for issuance (default 120)
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

OUT="${1:-.hivekeep-gate-client}"
NS="${HK_SYSTEM_NS:-hivekeep-system}"
SA="${HK_CLIENT_SA:-hivekeep-gate-client}"
TIMEOUT="${HK_CERT_TIMEOUT:-120}"
SIGNER="podidentity.podcert.hivekeep.io/identity"
REQUESTER="hivectl"

hk_require_cmd openssl kubectl base64

mkdir -p "${OUT}"
umask 077
openssl ecparam -name prime256v1 -genkey -noout -out "${OUT}/key.pem"
# The signer only reads the stub request's public key and checks its
# self-signature; the subject is ignored.
stub="$(openssl req -new -key "${OUT}/key.pem" -subj "/" -outform DER | base64 | tr -d '\n')"
uuid() { cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen | tr 'A-Z' 'a-z'; }

name="$(kubectl create -o jsonpath='{.metadata.name}' -f - <<YAML
apiVersion: certificates.k8s.io/v1beta1
kind: PodCertificateRequest
metadata:
  generateName: ${REQUESTER}-
  namespace: ${NS}
spec:
  signerName: ${SIGNER}
  podName: ${REQUESTER}
  podUID: $(uuid)
  serviceAccountName: ${SA}
  serviceAccountUID: $(uuid)
  nodeName: ${REQUESTER}
  nodeUID: $(uuid)
  maxExpirationSeconds: 86400
  stubPKCS10Request: ${stub}
YAML
)"
trap 'kubectl -n "${NS}" delete podcertificaterequest "${name}" --ignore-not-found >/dev/null 2>&1 || true' EXIT

hk_log "waiting for ${NS}/${name} to be issued"
deadline=$((SECONDS + TIMEOUT))
while :; do
  chain="$(kubectl -n "${NS}" get podcertificaterequest "${name}" -o jsonpath='{.status.certificateChain}')"
  [[ -n "${chain}" ]] && break
  denied="$(kubectl -n "${NS}" get podcertificaterequest "${name}" \
    -o jsonpath='{.status.conditions[?(@.type!="Issued")].message}')"
  [[ -z "${denied}" ]] || hk_die "certificate request refused: ${denied}"
  (( SECONDS < deadline )) || hk_die "timed out waiting for ${NS}/${name}"
  sleep 1
done
printf '%s\n' "${chain}" > "${OUT}/chain.pem"
hk_log "wrote ${OUT}/chain.pem and ${OUT}/key.pem for spiffe://cluster.local/ns/${NS}/sa/${SA}"
