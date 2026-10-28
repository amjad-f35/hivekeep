#!/usr/bin/env bash
# Live-cluster check of the gate router's graceful drain:
#   1. pins the counter demo pool to one worker and occupies it, so a request
#      for a second Actor parks on the router;
#   2. deletes the router pod while that request is parked, then frees the
#      worker: the parked request must still complete (200) through the
#      Terminating pod;
#   3. asserts /readyz turns 503 while /healthz stays 200, the pod exits
#      inside the drain window (not at the grace-period SIGKILL), and the
#      router logged its drain sequence in order.
# It deletes the shared router pod, which breaks other port-forwards, so it
# runs on its own rather than inside the parallel e2e suites.
#
# Needs the control plane and the counter demo installed, with no Actors
# running on the counter pool (it is scaled to 1 and restored afterwards).
set -euo pipefail
# shellcheck source=../lib/common.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/common.sh"
cd "${HK_ROOT}"

ROUTER_NS="${HK_NAMESPACE:-hivekeep-system}"
ROUTER="gate-router"
DEMO_NS=hivekeep-demo-counter
HIVE=hivekeep-demo-counter
POOL=counter
suffix="$(date +%s)"
BUSY="busy-${suffix}"
PARKED="parked-${suffix}"
HTTP_PORT="${LOCAL_HTTP_PORT:-18080}"
HEALTH_PORT="${LOCAL_HEALTH_PORT:-19091}"
LOG_FILE="$(mktemp "${TMPDIR:-/tmp}/gate-drain-log.XXXXXX")"
CURL_OUT="$(mktemp "${TMPDIR:-/tmp}/gate-drain-out.XXXXXX")"

step() { echo; echo "[gate-drain] $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

BG=()
ORIG_REPLICAS=""
cleanup() {
  local code=$?
  set +e
  for pid in ${BG[@]+"${BG[@]}"}; do kill "${pid}" 2>/dev/null; done
  step "cleanup"
  for a in "${PARKED}" "${BUSY}"; do
    hk_hivectl suspend actor "${a}" -H "${HIVE}" >/dev/null 2>&1
    hk_hivectl delete actor "${a}" -H "${HIVE}" >/dev/null 2>&1
  done
  if [[ -n "${ORIG_REPLICAS}" ]]; then
    hk_kubectl patch workerpool -n "${DEMO_NS}" "${POOL}" --type merge \
      -p "{\"spec\":{\"replicas\":${ORIG_REPLICAS}}}" >/dev/null 2>&1
    hk_kubectl rollout status "deploy/${POOL}" -n "${DEMO_NS}" --timeout=180s >/dev/null 2>&1
  fi
  exit "${code}"
}
trap cleanup EXIT

step "preflight"
hk_hivectl get actor-template "${POOL}" -H "${HIVE}" -o json 2>/dev/null | grep -q '"goldenTag"' ||
  fail "template ${HIVE}/${POOL} has no golden snapshot; deploy the counter demo first"
if hk_hivectl get workers 2>/dev/null | awk 'NR>1 && $4=="ASSIGNED"' | grep -q .; then
  fail "workers are ASSIGNED; suspend running actors first (the pool is scaled to 1)"
fi
for port in "${HTTP_PORT}" "${HEALTH_PORT}"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/${port}") 2>/dev/null; then
    fail "local port ${port} is in use; free it or set LOCAL_HTTP_PORT/LOCAL_HEALTH_PORT"
  fi
done

step "pinning ${DEMO_NS}/${POOL} to one worker"
ORIG_REPLICAS="$(hk_kubectl get workerpool -n "${DEMO_NS}" "${POOL}" -o jsonpath='{.spec.replicas}')"
hk_kubectl patch workerpool -n "${DEMO_NS}" "${POOL}" --type merge -p '{"spec":{"replicas":1}}' >/dev/null
hk_kubectl rollout status "deploy/${POOL}" -n "${DEMO_NS}" --timeout=180s >/dev/null

step "creating ${BUSY} and ${PARKED}"
hk_hivectl create actor "${BUSY}" -H "${HIVE}" --template "${POOL}" >/dev/null
hk_hivectl create actor "${PARKED}" -H "${HIVE}" --template "${POOL}" >/dev/null
for i in $(seq 1 20); do
  hk_hivectl resume actor "${BUSY}" -H "${HIVE}" >/dev/null 2>&1 && break
  [[ "${i}" == 20 ]] && fail "could not resume ${BUSY}"
  sleep 3
done

pod="$(hk_kubectl get pods -n "${ROUTER_NS}" -l "app=${ROUTER}" --no-headers | awk '$3=="Running"{print $1}')"
(($(wc -w <<<"${pod}") == 1)) || fail "expected one running router pod, got: ${pod:-none}"
step "router pod under test: ${pod}"

hk_kubectl logs -n "${ROUTER_NS}" "${pod}" -c "${ROUTER}" -f >"${LOG_FILE}" 2>&1 &
BG+=($!)
hk_kubectl port-forward -n "${ROUTER_NS}" "svc/${ROUTER}" "${HTTP_PORT}:80" >/dev/null 2>&1 &
BG+=($!)
hk_kubectl port-forward -n "${ROUTER_NS}" "${pod}" "${HEALTH_PORT}:9091" >/dev/null 2>&1 &
BG+=($!)

readyz() { curl -s -o /dev/null -w '%{http_code}' "localhost:${HEALTH_PORT}/readyz" || true; }
healthz() { curl -s -o /dev/null -w '%{http_code}' "localhost:${HEALTH_PORT}/healthz" || true; }
for i in $(seq 1 40); do
  [[ "$(readyz)" == 200 ]] && break
  [[ "${i}" == 40 ]] && fail "steady-state /readyz is not 200"
  sleep 0.5
done
[[ "$(healthz)" == 200 ]] || fail "steady-state /healthz is not 200"

step "sending the request that parks"
(curl -s --max-time 30 -w '\nHTTP=%{http_code}\n' -H "hk-target-actor: ${HIVE}/${PARKED}" \
  "http://localhost:${HTTP_PORT}/" >"${CURL_OUT}" 2>&1) &
curl_pid=$!
sleep 1

step "deleting the router pod with the request parked"
deleted_at="$(date +%s)"
hk_kubectl delete pod -n "${ROUTER_NS}" "${pod}" --wait=false >/dev/null
until [[ -n "$(hk_kubectl get pod -n "${ROUTER_NS}" "${pod}" -o jsonpath='{.metadata.deletionTimestamp}' 2>/dev/null)" ]]; do
  sleep 0.2
done
[[ "$(readyz)" == 503 ]] || fail "/readyz is not 503 while Terminating"
[[ "$(healthz)" == 200 ]] || fail "/healthz is not 200 while Terminating"

step "freeing the worker"
hk_hivectl suspend actor "${BUSY}" -H "${HIVE}" >/dev/null
wait "${curl_pid}" || true
grep -q "HTTP=200" "${CURL_OUT}" || fail "parked request did not return 200: $(cat "${CURL_OUT}")"
grep -q "hello from" "${CURL_OUT}" || fail "parked request body is not the counter's: $(cat "${CURL_OUT}")"

step "waiting for the pod to go"
while hk_kubectl get pod -n "${ROUTER_NS}" "${pod}" >/dev/null 2>&1; do sleep 1; done
elapsed=$(($(date +%s) - deleted_at))
((elapsed >= ${HK_DRAIN_MIN_SECS:-10})) || fail "terminated after ${elapsed}s, before the drain delay could run"
((elapsed <= ${HK_DRAIN_MAX_SECS:-55})) || fail "terminated after ${elapsed}s, at the grace period (SIGKILL path)"

for marker in "shutdown signal received; draining" "draining dataplane" "starting ext_proc drain" "drain-complete marker written"; do
  grep -q "${marker}" "${LOG_FILE}" || fail "router log lacks \"${marker}\" (see ${LOG_FILE})"
done
hk_kubectl rollout status "deploy/${ROUTER}" -n "${ROUTER_NS}" --timeout=120s >/dev/null
echo
echo "PASS: parked request served while Terminating; readiness flipped; exited after ${elapsed}s; drain sequence logged."
