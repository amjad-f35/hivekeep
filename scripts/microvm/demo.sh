#!/usr/bin/env bash
# Brings up the counter-microvm demo end to end: the control plane, the
# micro-VM dependencies (assets + SandboxConfig) and the demo itself, then
# prints how to drive it. Uses the dev env file / cloud settings; for a local
# Kind cluster run scripts/microvm/demo-kind.sh instead.
#
# The control plane needs a credential provider choice: export
# HK_CREDENTIAL_PROVIDER (e.g. '{"name":"k8s.io"}') first. For the proxy step
# the install's HK_INGRESS_ALLOWED_SPIFFE_IDS must include
# spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client.
#
#   HK_BUILD_REPO    image registry for source builds (required)
#   HK_INSTALL_KIND  true for the Kind path
#   KUBECTL_CONTEXT, BUCKET_NAME, PROJECT_ID, ARCH, OUT: see install-microvm-deps.sh
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"
hk_load_dev_env

[[ $# -eq 0 ]] || hk_die "unexpected argument $1"
HK_INSTALL_KIND="${HK_INSTALL_KIND:-false}"
[[ -n "${HK_BUILD_REPO:-}" || "${HK_INSTALL_KIND}" == true ]] ||
  hk_die "HK_BUILD_REPO is required (or use scripts/microvm/demo-kind.sh)"

installer=scripts/install/hivekeep.sh
if [[ "${HK_INSTALL_KIND}" == true ]]; then
  installer=scripts/install/hivekeep-kind.sh
fi

hk_log "deploying the control plane"
"${installer}" deploy hivekeep-system

# The SandboxConfig CRD comes with the control plane, so this goes second.
hk_log "installing micro-VM dependencies"
scripts/install-microvm-deps.sh --install

hk_log "deploying the counter-microvm demo"
"${installer}" deploy demo counter-microvm

ctx=""
if [[ -n "${KUBECTL_CONTEXT:-}" ]]; then
  ctx=" --context ${KUBECTL_CONTEXT}"
fi
cat <<MSG

Demo ready. Next:
  hivectl${ctx} get actor-templates -H hivekeep-demo-counter-microvm
  hivectl${ctx} create actor my-counter-1 -H hivekeep-demo-counter-microvm --template counter-microvm
  hivectl${ctx} proxy actor my-counter-1 -H hivekeep-demo-counter-microvm &
  curl -X POST http://localhost:8000
Bump the counter a few times, suspend the actor
(hivectl${ctx} suspend actor my-counter-1 -H hivekeep-demo-counter-microvm), curl again,
and the in-memory count carries on from the guest memory snapshot.
MSG
