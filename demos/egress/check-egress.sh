#!/usr/bin/env bash
# Checks the egress demo end to end on a running cluster:
#   allowed  - a running Actor's plain-HTTP egress is captured, tunneled to
#              gate-egress over mTLS + CONNECT with the Actor's certificate,
#              authorised against hivekeep-api and delivered (HTTP 200).
#   refused  - a pod holding a valid pod certificate but no Actor certificate
#              cannot open a tunnel: gate-egress only trusts the Actor CA, so
#              the TLS handshake fails before any CONNECT is answered.
#
# Needs: kubectl, hivectl and curl on PATH, and `hivekeep-setup deploy demo
# egress` already applied. The install's --ingress-allowed-spiffe-ids must
# include spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client.
#
# Usage:
#   demos/egress/check-egress.sh            run the checks
#   demos/egress/check-egress.sh --cleanup  remove what the checks created
#
# Environment (all optional):
#   HK_KUBE_CONTEXT   kube context (default kind-kind)
#   HK_SYSTEM_NS      Hivekeep system namespace (default hivekeep-system)
#   HK_DEMO_HIVE      hive holding the template (default hivekeep-demo-egress)
#   HK_DEMO_ACTOR     Actor to create (default egress-demo)
#   HK_DEMO_TEMPLATE  template name (default egress)
#   HK_TARGET_NS      namespace for the HTTP target (default egress-target)
#   HK_TARGET_PORT    target Service port (default 80)
#   HK_PROXY_PORT     local port for `hivectl proxy` (default 18099)
#   KEEP=1            keep everything after a run

set -euo pipefail

CONTEXT="${HK_KUBE_CONTEXT:-kind-kind}"
SYSTEM_NS="${HK_SYSTEM_NS:-hivekeep-system}"
HIVE="${HK_DEMO_HIVE:-hivekeep-demo-egress}"
ACTOR="${HK_DEMO_ACTOR:-egress-demo}"
TEMPLATE="${HK_DEMO_TEMPLATE:-egress}"
TARGET_NS="${HK_TARGET_NS:-egress-target}"
TARGET_PORT="${HK_TARGET_PORT:-80}"
PROXY_PORT="${HK_PROXY_PORT:-18099}"
PROBE_POD="egress-identity-probe"

WORK_DIR="$(mktemp -d)"
FAILURES=0

kc() { kubectl --context "${CONTEXT}" "$@"; }
hc() { hivectl --context "${CONTEXT}" "$@"; }

section() { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
note() { printf '   %s\n' "$*"; }
ok() { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
bad() {
  printf '\033[1;31mFAIL\033[0m %s\n' "$*"
  FAILURES=$((FAILURES + 1))
}

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "required tool not found: $1" >&2
    exit 1
  }
}

remove_all() {
  section "cleanup"
  kc -n "${SYSTEM_NS}" delete pod "${PROBE_POD}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  hc suspend actor "${ACTOR}" -H "${HIVE}" >/dev/null 2>&1 || true
  hc delete actor "${ACTOR}" -H "${HIVE}" >/dev/null 2>&1 || true
  kc delete namespace "${TARGET_NS}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  note "done"
}

on_exit() {
  rm -rf "${WORK_DIR}"
  if [[ "${KEEP:-}" != "1" ]]; then
    remove_all
  fi
}

need kubectl
need hivectl
if [[ "${1:-}" == "--cleanup" ]]; then
  remove_all
  rm -rf "${WORK_DIR}"
  exit 0
fi
need curl
trap on_exit EXIT

section "preflight: gate-egress is up"
kc -n "${SYSTEM_NS}" rollout status deployment/gate-egress --timeout=120s
containers="$(kc -n "${SYSTEM_NS}" get deployment/gate-egress -o jsonpath='{.spec.template.spec.containers[*].name}')"
case " ${containers} " in
  *" envoy "*) DATAPLANE=envoy; LOG_MARKER='\[egress\]' ;;
  *)
    echo "cannot tell the egress dataplane from containers: ${containers}" >&2
    exit 1
    ;;
esac
note "dataplane: ${DATAPLANE}"

section "HTTP target"
kc create namespace "${TARGET_NS}" >/dev/null 2>&1 || true
kc -n "${TARGET_NS}" create deployment whoami --image=traefik/whoami >/dev/null 2>&1 || true
kc -n "${TARGET_NS}" expose deployment whoami --port="${TARGET_PORT}" --target-port=80 >/dev/null 2>&1 || true
kc -n "${TARGET_NS}" rollout status deployment/whoami --timeout=120s
TARGET_IP="$(kc -n "${TARGET_NS}" get svc whoami -o jsonpath='{.spec.clusterIP}')"
note "target: ${TARGET_IP}:${TARGET_PORT}"

section "Actor ${HIVE}/${ACTOR} with an allow-all egress policy"
hc create hive "${HIVE}" >/dev/null 2>&1 || true
hc create actor "${ACTOR}" -H "${HIVE}" --template "${TEMPLATE}" >/dev/null 2>&1 || true
hc resume actor "${ACTOR}" -H "${HIVE}" >/dev/null 2>&1 || true
printf 'rules:\n- http: {hostnames: ["*"], ports: {all: {}}}\n- https: {hostnames: ["*"]}\n' |
  hc create egress-policy "${ACTOR}" -H "${HIVE}" -f - >/dev/null 2>&1 || true

# Reads the whole listing before matching: an early-exiting grep would
# break the pipe under pipefail while hivectl is still writing.
actor_running() {
  local listing
  listing="$(hc get actors -H "${HIVE}" 2>/dev/null || true)"
  grep -E "(^|[[:space:]])${ACTOR}[[:space:]].*ACTOR_STATE_RUNNING" <<<"${listing}" >/dev/null
}
for _ in $(seq 1 30); do
  actor_running && break
  sleep 3
done
actor_running || {
  echo "Actor ${ACTOR} did not reach ACTOR_STATE_RUNNING" >&2
  exit 1
}

gateway_log() {
  kc -n "${SYSTEM_NS}" logs deployment/gate-egress -c "${DATAPLANE}" --tail=-1 2>/dev/null |
    grep -E "${LOG_MARKER}" || true
}
gateway_log_count() { gateway_log | wc -l | tr -d ' '; }
# Lines logged after the first $1 matching lines.
gateway_log_since() { gateway_log | tail -n +"$(($1 + 1))"; }
# Waits up to 10s for a new line matching $2; prints it.
await_gateway_log() {
  local line
  for _ in $(seq 1 10); do
    line="$(gateway_log_since "$1" | grep -E "$2" | tail -n 1 || true)"
    if [[ -n "${line}" ]]; then
      printf '%s\n' "${line}"
      return 0
    fi
    sleep 1
  done
  return 1
}

section "allowed: Actor egress goes through the gateway (expect 200)"
before="$(gateway_log_count)"
hc proxy actor "${ACTOR}" "${PROXY_PORT}:80" -H "${HIVE}" >"${WORK_DIR}/proxy.log" 2>&1 &
proxy_pid=$!
sleep 4
code="$(curl -s -o "${WORK_DIR}/body.txt" -w '%{http_code}' -X POST "http://127.0.0.1:${PROXY_PORT}/" \
  -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:${TARGET_PORT}/\"}" || true)"
kill "${proxy_pid}" >/dev/null 2>&1 || true
wait "${proxy_pid}" 2>/dev/null || true
note "round trip: HTTP ${code}"

gateway_ip="$(kc -n "${SYSTEM_NS}" get pod -l app=gate-egress -o jsonpath='{.items[0].status.podIP}')"
if [[ "${code}" == "200" ]]; then
  ok "the Actor fetched the target"
else
  bad "expected HTTP 200, got ${code}"
fi
if grep -F "RemoteAddr: ${gateway_ip}" "${WORK_DIR}/body.txt" >/dev/null 2>&1; then
  ok "the target saw gate-egress (${gateway_ip}) as its client"
else
  note "target saw $(grep -oE 'RemoteAddr: [0-9.]+' "${WORK_DIR}/body.txt" 2>/dev/null || echo 'no RemoteAddr'), gateway is ${gateway_ip}"
fi
# Envoy names the peer by its certificate SAN, which the Actor cannot forge.
allowed_pattern="worker-for-actor/${HIVE}/${ACTOR}.*code=200"
if line="$(await_gateway_log "${before}" "${allowed_pattern}")"; then
  ok "gate-egress logged the CONNECT: ${line}"
else
  bad "${DATAPLANE} did not log the allowed CONNECT"
fi

section "refused: a pod certificate is not an Actor certificate"
kc apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: ${PROBE_POD}
  namespace: ${SYSTEM_NS}
spec:
  containers:
  - name: curl
    image: curlimages/curl:latest
    command: ["sleep", "600"]
    volumeMounts:
    - {name: podidentity, mountPath: /run/podidentity.podcert.hivekeep.io, readOnly: true}
    - {name: servicedns, mountPath: /run/servicedns.podcert.hivekeep.io, readOnly: true}
  volumes:
  - name: podidentity
    projected:
      sources:
      - podCertificate:
          signerName: podidentity.podcert.hivekeep.io/identity
          keyType: ECDSAP256
          credentialBundlePath: credential-bundle.pem
  - name: servicedns
    projected:
      sources:
      - clusterTrustBundle:
          signerName: servicedns.podcert.hivekeep.io/identity
          labelSelector: {matchLabels: {podcert.hivekeep.io/canarying: live}}
          path: trust-bundle.pem
YAML
kc -n "${SYSTEM_NS}" wait --for=condition=Ready "pod/${PROBE_POD}" --timeout=60s >/dev/null

before="$(gateway_log_count)"
# %{http_connect} is the proxy's CONNECT status and stays 000 when no tunnel
# opens; curl itself fails on a refused handshake, so record both.
probe="$(kc -n "${SYSTEM_NS}" exec "${PROBE_POD}" -- sh -c "curl -s -o /dev/null -w '%{http_connect}' \
  --proxy-cacert /run/servicedns.podcert.hivekeep.io/trust-bundle.pem \
  --proxy-cert /run/podidentity.podcert.hivekeep.io/credential-bundle.pem \
  --proxy-key /run/podidentity.podcert.hivekeep.io/credential-bundle.pem \
  --proxytunnel -x https://gate-egress.${SYSTEM_NS}.svc:443 http://${TARGET_IP}:${TARGET_PORT}/; echo \" exit=\$?\"" || true)"
connect_code="${probe%% *}"
note "pod-certificate CONNECT: http_connect=${connect_code:-000}${probe#"${connect_code}"}"
if [[ "${connect_code}" != "200" ]]; then
  ok "a pod certificate cannot open an egress tunnel"
else
  bad "gate-egress accepted a non-Actor client certificate (CONNECT 200)"
fi
# A refused handshake never becomes an HTTP request, so normally nothing is
# logged; show whatever was, to help diagnose a failure.
line="$(gateway_log_since "${before}" | tail -n 1)"
if [[ -n "${line}" ]]; then
  note "gate-egress logged since the probe: ${line}"
else
  note "nothing new in the gateway log: the handshake was refused before HTTP"
fi

echo
if [[ "${FAILURES}" -eq 0 ]]; then
  printf '\033[1;32mALL CHECKS PASSED\033[0m egress tunnelling and Actor authentication work\n'
else
  printf '\033[1;31m%d CHECK(S) FAILED\033[0m\n' "${FAILURES}"
  exit 1
fi
