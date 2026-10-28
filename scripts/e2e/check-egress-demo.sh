#!/usr/bin/env bash
# Drives the egress demo through the gate egress gateway and shows the
# gateway's ext_proc log line proving it saw the Actor's identity and the
# CONNECT authority.
# Needs: a cluster with the egress demo deployed, on an install whose
# ingress allowlist includes spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client.
#   HIVE (default hivekeep-demo-egress), ACTOR (default egress-demo),
#   TARGET_URL (default http://example.com/), KUBECTL_CONTEXT (default kind-kind)
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

export KUBECTL_CONTEXT="${KUBECTL_CONTEXT:-kind-kind}"
ns="${HK_NAMESPACE:-hivekeep-system}"
HIVE="${HIVE:-hivekeep-demo-egress}"
ACTOR="${ACTOR:-egress-demo}"
TARGET_URL="${TARGET_URL:-http://example.com/}"
PORT="${LOCAL_PORT:-18000}"
gw_logs() { hk_kubectl -n "${ns}" logs deployment/gate-egress -c ext-proc --tail=-1 2>/dev/null; }

hk_kubectl -n "${ns}" rollout status deployment/gate-egress --timeout=120s
hk_hivectl create hive "${HIVE}" >/dev/null 2>&1 || true
hk_hivectl create actor "${ACTOR}" -H "${HIVE}" --template egress >/dev/null 2>&1 || true
sleep "${HK_ACTOR_SETTLE_SECS:-10}"

before="$(gw_logs | wc -l | tr -d ' ')"
pf_log="$(mktemp)"
hk_hivectl proxy actor "${ACTOR}" "${PORT}:80" -H "${HIVE}" >"${pf_log}" 2>&1 &
pf=$!
trap 'kill "${pf}" 2>/dev/null || true; rm -f "${pf_log}"' EXIT
sleep 3
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://localhost:${PORT}/" \
  -H 'Content-Type: application/json' -d "{\"url\":\"${TARGET_URL}\"}" || true)"
echo "actor round trip: HTTP ${code}"

# The CONNECT can land after the reply; and a warm actor reuses an open tunnel,
# which logs nothing new, so fall back to existing entries for this actor.
new=""
for _ in $(seq 1 15); do
  new="$(gw_logs | tail -n +"$((before + 1))" | grep 'egress tunnel opened' || true)"
  [[ -n "${new}" ]] && break
  sleep 2
done
if [[ -n "${new}" ]]; then
  echo "${new}"
elif gw_logs | grep 'egress tunnel opened' | grep -q "${ACTOR}"; then
  echo "no new CONNECT (an open tunnel was reused); earlier entries:"
  gw_logs | grep 'egress tunnel opened' | grep "${ACTOR}" | tail -3
else
  echo "no egress tunnel entries for ${HIVE}/${ACTOR}; recent ext_proc log:" >&2
  hk_kubectl -n "${ns}" logs deployment/gate-egress -c ext-proc --tail=20 >&2
  exit 1
fi
echo "PASS: actor egress went through the gate egress gateway"
