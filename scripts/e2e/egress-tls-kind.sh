#!/usr/bin/env bash
# E2E setup only: makes the gate egress gateway trust local test origins
# signed by the cluster's service-DNS CA, in addition to the public roots.
# Run after installing the egress gateway and before the local-origin TLS
# tests. The CA signing key is never mounted on the gateway.
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"

export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-${KIND_CLUSTER_NAME:-kind}}"
ns="${HK_NAMESPACE:-hivekeep-system}"
deploy="${HK_GATE_EGRESS_DEPLOYMENT:-gate-egress}"
k() { hk_kubectl -n "${ns}" "$@"; }

gateway="$(k get deployment "${deploy}" -o jsonpath='{.spec.template.spec.containers[?(@.name=="envoy")].name}')"
[[ -n "${gateway}" ]] || hk_die "install the Envoy egress gateway first"
image="$(k get deployment "${deploy}" -o jsonpath="{.spec.template.spec.containers[?(@.name==\"${gateway}\")].image}")"
sdsmint="$(k get deployment "${deploy}" -o jsonpath='{.spec.template.spec.initContainers[?(@.name=="sdsmint")].name}')"
if [[ "${sdsmint}" != sdsmint ]]; then
  hk_die "the Envoy egress gateway is missing its sdsmint certificate service"
fi

# A separate init container rebuilds the bundle on every start, so restarts
# never append to an existing one.
k patch deployment "${deploy}" --type=strategic --patch "$(
  cat <<YAML
spec:
  template:
    spec:
      initContainers:
      - name: e2e-egress-upstream-roots
        image: ${image}
        command:
        - sh
        - -ec
        - cat /etc/ssl/certs/ca-certificates.crt /run/servicedns.podcert.hivekeep.io/trust-bundle.pem > /run/e2e-egress-roots/ca-certificates.crt
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities:
            drop: [ALL]
        volumeMounts:
        - name: servicedns
          mountPath: /run/servicedns.podcert.hivekeep.io
          readOnly: true
        - name: e2e-egress-upstream-roots
          mountPath: /run/e2e-egress-roots
      containers:
      - name: ${gateway}
        volumeMounts:
        - name: e2e-egress-upstream-roots
          mountPath: /etc/ssl/certs/ca-certificates.crt
          subPath: ca-certificates.crt
          readOnly: true
      volumes:
      - name: e2e-egress-upstream-roots
        emptyDir: {}
YAML
)"
k rollout status "deployment/${deploy}" --timeout=120s
