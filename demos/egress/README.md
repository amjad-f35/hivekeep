# Egress demo

This demo follows an Actor's outbound traffic from the sandbox, through the
gate's egress gateway, to its destination — and shows that the gateway decides
what may leave based on *which Actor* is asking, not on anything the Actor
itself writes into the request.

The workload (`crates/hivekeep-demo-egress`, binary `/app/egress` in the image
`REGISTRY_PLACEHOLDER/hivekeep-demo-egress`) is a small HTTP server. You POST it
a JSON instruction, it performs the outbound work and reports back what it saw.
The Actor thinks it is talking straight to the destination; in reality every
connection it opens is captured and carried to the gateway.

## The path a request takes

```text
  worker pod
  ┌──────────────────────────────────────────────────────┐
  │ Actor (sandboxed)                                     │
  │   GET http://<destination>/      plain HTTP           │
  │        │                                              │
  │        ▼  redirected by the worker's packet filter    │
  │   gate tunnel: mTLS with the Actor's own certificate, │
  │   then a bare CONNECT to the original destination     │
  └────────┼─────────────────────────────────────────────┘
           ▼
  gate-egress (hivekeep-system)
  ┌──────────────────────────────────────────────────────┐
  │ • accepts only client certificates from the Actor CA  │
  │ • asks hivekeep-api whether that Actor exists and is  │
  │   running (matched on the UID inside the certificate) │
  │ • checks the request against the Actor's EgressPolicy │
  │ • forwards to the CONNECT authority                   │
  └────────┼─────────────────────────────────────────────┘
           ▼
      destination
```

Key properties:

- **Transparent capture.** The Actor dials plain TCP; the worker redirects it
  into the tunnel. No proxy settings in the workload.
- **Identity from the certificate only.** hivekeep-api mints each Actor a
  certificate off the Actor identity CA with an identity extension naming its
  hive, name and UID. The tunnel presents it and sends a CONNECT with no extra
  headers. Recreating an Actor under the same name yields a new UID, so an old
  certificate stops working.
- **Deny by default.** Without an `EgressPolicy` the Actor gets no tunnel.
  Requests the gateway can read (cleartext HTTP, or HTTPS it terminates with
  its own CA) are matched rule by rule against their `Host` and the dialed
  address; the first match wins. TLS to a name no `https` rule covers needs a
  `tls_passthrough` rule.

## Dataplane

The egress gateway runs on Envoy, selected when installing hivekeep-system
(`--gate-dataplane=envoy`, the default), not per demo:

| | Envoy |
| --- | --- |
| Routing | dynamic forward proxy |
| Actor check | co-located gate `ext-proc` container |
| Access log | text lines starting `[egress]` with the peer SAN |

## Endpoints

All take `POST` with a JSON body (at most 64 KiB) and answer JSON. Anything
the instruction asks for that fails on the way out is a `502`; a malformed
instruction is a `400`.

| Path | Instruction | What it does |
| --- | --- | --- |
| `/` | `{"url", "rootCA"?, "http1"?}` | GET the URL, relay status, body (≤1 MiB), protocol, issuer of the server certificate and whether TLS was verified. `rootCA` or `http1` switch to a one-off client that trusts only that root (or the system roots) and does not follow redirects. |
| `/post` | `{"url", "rootCA"?}` | POST 64 bytes, sending the headers immediately and the body 15 s later — longer than the gateway caches policy decisions. |
| `/slow-headers` | `{"url"}` (http only) | GET whose header block is completed only after 5 s. |
| `/mux` | `{"url"}` (http only) | Open 250 connections, send all their headers, then all their bodies; every request must answer 200. Used to count the HTTP/2 tunnels the gateway opens. |
| `/websocket` | `{"url": "ws(s)://…", "rootCA"?, "messages": [...]}` | Handshake (refusals keep status, protocol and up to 4 KiB of body), then send each message (≤16, ≤64 KiB each) and record each reply. |
| `/grpc` | `{"target": "host:port", "message", "streamCount"?, "bidiCount"?}` | Cleartext HTTP/2 calls to the Echo service; see below. |
| `/connect` | `{"proxy": "host:port", "target": "host:port", "tls"?, "rootCA"?}` | Send one CONNECT to a forward proxy (optionally over TLS) and report the answer. A refusal is still a `200` report. |

`GET /readyz` answers `ok` and is the template's wake-up probe.

### gRPC

```json
{"target":"10.0.0.7:50051","message":"hello","streamCount":2,"bidiCount":2}
```

returns

```json
{
  "message": "hello",
  "stream": [{"message":"hello","index":0},{"message":"hello","index":1}],
  "bidi":   [{"message":"hello-0","index":0},{"message":"hello-1","index":1}],
  "code":   "OK"
}
```

Each shape catches a different fault in the path. Unary `Echo` always runs; its
status travels in trailers after the body, so `code` proves trailers made it
and that nothing fell back to HTTP/1.1. `EchoStream` holds one connection open
across many frames. `EchoBidi` sends each message only after reading the reply
to the previous one, then half-closes while replies may still arrive — a relay
that serialises the two directions hangs here. A failed RPC answers `502` and
still names its gRPC code. The service definition lives in
`crates/hivekeep-demo-egress/proto/`.

## Prerequisites

- A Kind cluster with Hivekeep installed (`hivekeep-setup deploy hivekeep-system`),
  including
  `--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client`.
- `kubectl`, `curl` and `hivectl` on `PATH`.
- `HK_API_ENDPOINT` pointing at hivekeep-api for the installer's demo steps.

## Deploy

```bash
hivekeep-setup deploy demo egress
```

This applies the worker pool, creates the hive `hivekeep-demo-egress` and the
`egress` ActorTemplate in it, and waits for the template's golden snapshot:

```bash
hivectl get actor-template egress -H hivekeep-demo-egress
```

## Automated check

```bash
demos/egress/check-egress.sh             # run
demos/egress/check-egress.sh --cleanup   # remove what it created
```

The script confirms the Envoy dataplane is deployed, starts an in-cluster
HTTP target, creates and resumes an Actor with an allow-all policy, and then
checks:

- **allowed path** — the Actor reaches the target with `200`, the target sees
  the gateway's pod IP as the client, and the gateway logs the CONNECT with
  the Actor's SAN;
- **identity check** — a pod with a valid *pod* certificate but no Actor
  certificate cannot open a tunnel; the handshake is refused before any
  CONNECT is answered.

Set `KEEP=1` to leave resources in place after a run.

## Walk through it by hand

```bash
# A target to fetch.
kubectl create namespace egress-target
kubectl -n egress-target create deployment whoami --image=traefik/whoami
kubectl -n egress-target expose deployment whoami --port=80
TARGET_IP=$(kubectl -n egress-target get svc whoami -o jsonpath='{.spec.clusterIP}')

# An Actor from the template, resumed.
hivectl create actor egress-demo -H hivekeep-demo-egress --template egress
hivectl resume actor egress-demo -H hivekeep-demo-egress

# Allow everything: cleartext HTTP on any port, HTTPS on 443 (intercepted).
hivectl create egress-policy egress-demo -H hivekeep-demo-egress -f - <<'POLICY'
rules:
- http: {hostnames: ["*"], ports: {all: {}}}
- https: {hostnames: ["*"]}
POLICY

# Reach the Actor through the gate and have it fetch the target. A fetch
# attempted before the policy existed is cached as a denial for about 10s.
hivectl proxy actor egress-demo 8000:80 -H hivekeep-demo-egress &
curl -s -X POST http://localhost:8000/ -H 'Content-Type: application/json' \
  -d "{\"url\":\"http://${TARGET_IP}:80/\"}"
```

The `whoami` body reports `RemoteAddr: <gate-egress pod IP>`: the request left
through the gateway. To see the gateway's side:

```bash
kubectl -n hivekeep-system logs deploy/gate-egress -c envoy | grep '\[egress\]'
kubectl -n hivekeep-system logs deploy/gate-egress -c ext-proc | grep -i 'egress'
```

## Remove

```bash
demos/egress/check-egress.sh --cleanup
hivekeep-setup delete demo egress
```
