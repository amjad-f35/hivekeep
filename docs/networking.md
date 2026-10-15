# Networking

All traffic to and from Actors goes through the **gate**
(`crates/hivekeep-gate`). This page covers ingress, the sandbox tunnel, DNS
and network namespaces. The outbound policy is in [egress.md](egress.md).

## Ingress

```text
client --mTLS--> Service gate-router :443 (-> Envoy :8443, HTTPS)
                                     :8444 (-> Envoy :8444, CONNECT over TLS)
Envoy --ext_proc (127.0.0.1:50051)--> hivekeep-gate router --mode=ingress
   resolve target Actor, ResumeActor (flight + parking), return worker address
Envoy --original-dst cluster, mTLS--> worker pod IP :443 (tunnel) --> Actor port
```

The `gate-router` Deployment runs two containers. One is the `hivekeep-gate
router --mode=ingress` process, which serves ext_proc and Envoy's xDS (ADS
on `--port-xds`, default 18000). The other is the Envoy dataplane, selected
at install time with `--gate-dataplane=envoy` (the default).

**Client authentication.** Envoy requires a client certificate signed by a
CA in `--ingress-client-ca-file`. Its SPIFFE ID must exactly match one of
`--ingress-allowed-spiffe-ids`. If the list is empty, no traffic is served.
Set the list at install time with `hivekeep-setup
--ingress-allowed-spiffe-ids=...` (`HK_INGRESS_ALLOWED_SPIFFE_IDS`). The
install does not issue client certificates. An operator issues them for
the allowlisted identities, for example as pod certificates for a
ServiceAccount with the `podidentity.podcert.hivekeep.io/identity` signer.

**Choosing the Actor.** The target Actor is named `<hive>/<actor>` in one of
two places:

- the `hk-target-actor` request header, for plain HTTPS requests (the router
  strips it before forwarding); or
- the outer CONNECT request, for the CONNECT-over-TLS listener on 8444. The
  CONNECT authority's port selects the Actor port, and defaults to 80.

**Routing.** The router resumes the Actor if needed (see
[request-parking.md](request-parking.md)). It then gives Envoy the worker
pod's IP from the Actor's assignment, the Actor port, and the worker's
SPIFFE ID. Envoy dials the worker tunnel on port 443 over mTLS
(`--upstream-credential-bundle`, `--upstream-trust-bundle`) and verifies
that SPIFFE ID. With `--upstream-connect=false` (Envoy only), the router
uses plain HTTPS to the tunnel instead of a CONNECT tunnel.

`hivectl proxy actor` opens local listeners and carries each connection as
a CONNECT through the 8444 listener. It accepts any number of
`[local:]remote` port mappings.

**Worker isolation.** `hivekeep-controller` applies a NetworkPolicy to each
WorkerPool's pods that allows ingress only from the gate router. The
`hivekeep-system` namespace starts from `default-deny-ingress`, with one
allow policy per component (`hivekeep-api`, `postgres`, `keeper`,
`gate-router`, `hivekeep-idp-server`, `gate-egress`, observability).

## The sandbox tunnel

The tunnel (`hivekeep_gate::tunnel`) is the sandbox side of the gate.
Each worker pod hosts one, with a per-Actor *activation* that exists
while the Actor runs:

| Listener | Default | Purpose |
|----------|---------|---------|
| `--tunnel-listen-address` | `:443` | Ingress HTTPS from the router |
| `--tunnel-connect-listen-address` | `:8443` | Ingress mTLS CONNECT from the router |
| `--tunnel-egress-listen-address` | `0.0.0.0:15001` | Transparently redirected Actor egress |

- **Ingress.** Only `--tunnel-client-identity` (the router's SPIFFE ID) is
  accepted. A request is dialed into the Actor's network namespace only
  while the Actor's activation is live. The upstream transport mirrors the
  client's protocol (HTTP/1.1, HTTP/2, gRPC).
- **Egress.** Each redirected connection's original destination is read
  with `SO_ORIGINAL_DST`. The connection is then carried as an HTTP/1.1 or
  HTTP/2 CONNECT over mTLS to the egress gateway, which is verified against
  `--tunnel-egress-trust-bundle`. The client certificate is the Actor's own
  certificate, obtained through the node's keeper (`--tunnel-keeper-identity`).
  When an activation ends, its listener and tunnels are torn down, and a
  later activation for the same Actor inherits nothing from it.

The controller passes these flags to worker containers. Embedding the
tunnel in the worker binaries is still in progress (see the README status
section).

## DNS

Each Actor gets a generated `resolv.conf` that points at a per-Actor DNS
relay in the tunnel. The relay serves UDP and TCP, rewrites query IDs (a
64k free map), bounds concurrent queries, and only relays queries for names
the Actor's egress policy could reach. Everything else gets an error
answer. Decisions are cached per name. The mode is set with
`--dns-egress-policy`. The only accepted value is `enforce-in-tunnel`,
which `hivekeep-controller --dns-egress-policy` validates.

## Network namespaces

Both workers build the Actor's namespaces with
`hivekeep_sandbox_gvisor::actor_net` and serve them from the gate's tunnel
(egress proxy and DNS relay) in the Actor's gateway namespace. Namespaces
are created over netlink (veth, addresses, routes) and protected by an
nftables firewall in the gateway namespace: a `forward` chain that drops
everything, a `disallowed-egress` chain that rejects link-local
destinations and non-TCP traffic leaving the Actor's /30, and a NAT
`prerouting` chain that redirects all other TCP to the tunnel's egress port
(`--tunnel-egress-listen-address`). Kernel settings that would let an
Actor route, accept redirects or reach `127.0.0.0/8` are pinned before any
link exists. Namespace switches happen on a short-lived OS thread, so no
async runtime thread ever changes namespace.

- **gVisor.** The sandbox runs in `/run/netns/worker-actor:<actor-uid>`,
  whose `eth0` (`169.254.17.2/30`) is one end of a veth pair. The other end
  (`gwside`, `169.254.17.1/30`), the DNS relay and the egress listener live
  in `/run/netns/worker-actor:<actor-uid>-gw`, because gVisor claims every
  interface of its own namespace.
- **Micro-VM.** Each VM gets a tap device (backing virtio-net) inside its
  Actor namespace, which also holds the tunnel's sockets. The guest is configured through the Kata agent
  with fixed MAC addresses (`02:a8:1e:00:00:01` gateway,
  `02:a8:1e:00:00:02` guest) on the link-local subnet `169.254.17.0/30`.
  Because the addresses are fixed, a memory snapshot restored on another
  pod still finds its gateway.

## Ports at a glance

| Component | Port | Purpose |
|-----------|------|---------|
| `hivekeep-api` | 443 | gRPC (Control, WorkerService) |
| `hivekeep-api` | 8445 | Object-store credential exchange (plugin sidecar) |
| `hivekeep-api`, keeper, router | 9090 / 9091 | Prometheus metrics / health |
| `hivekeep-keeper` | 7443 | Keeper gRPC (mTLS) |
| `gate-router` Service | 443 -> 8443, 8444 | HTTPS ingress, CONNECT over TLS |
| `gate-router` (pod) | 50051 (loopback), 18000, 4040 (loopback) | ext_proc, xDS, `/statusz` |
| `gate-egress` Service | 443 | Egress CONNECT from tunnels |
| `hivekeep-credential-provider` | 50051, 4040 (loopback) | gRPC, `/statusz` |
| `hivekeep-idp` | 8443 | OIDC discovery and JWKS |
| worker pods | 443, 8443, 15001, 8080 | Tunnel ingress, CONNECT ingress, egress redirect, health |
