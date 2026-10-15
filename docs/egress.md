# Egress

An Actor's outbound traffic never goes straight to the network. The
sandbox tunnel redirects every TCP connection the Actor opens and carries it
over an mTLS CONNECT to the egress gateway (`gate-egress`). The gateway
authenticates the Actor from the tunnel's client certificate, checks the
destination against the Actor's EgressPolicy, and then forwards, inspects
or refuses the connection. UDP is always blocked. DNS lookups go to the
sandbox's DNS relay, which applies the same policy.

```text
Actor --TCP--> tunnel (worker pod, :15001) --mTLS CONNECT--> gate-egress Envoy
     --ext_proc--> hivekeep-gate router --mode=egress
                     | authenticate Actor (XFCC / source certificate)
                     | fetch EgressPolicy (cache, --egress-policy-cache-ttl)
                     | decide: http / https (terminate + inspect) / tls_passthrough
                     | inject credential headers
                     v
                   upstream
```

## The policy

Each Actor has at most one EgressPolicy, named `default`. With no policy,
or an empty one, nothing is allowed. A policy holds up to 256 rules, and
each rule allows exactly one protocol:

| Rule | Matches | Ports | Notes |
|------|---------|-------|-------|
| `http` | each cleartext request, on its authority host and port | default 80 | may carry `effects` |
| `https` | each request after the gateway terminates TLS | default 443 | the Actor must trust the gateway's interception CA; may carry `effects` |
| `tls_passthrough` | once per connection, on SNI and port | required | forwarded without decryption; connections without SNI never match |

Hostnames are patterns: an exact name, `*.example.com` (exactly one extra
leftmost label), or `*` (every name). IP literals only match `*`. Ports are
either `all: {}` or up to 16 `numbers`. When several rules match, the most
specific hostname pattern wins, then the port. Ties go to policy order. A
rule that fails to compile is dropped, which can only narrow what is
allowed.

The gateway refuses WebSocket upgrades ("WebSocket egress is not
supported") and nested CONNECT requests ("CONNECT egress is not
supported").

```yaml
# hivectl create egress-policy my-agent -H team-a -f policy.yaml
rules:
- https:
    hostnames: ["api.github.com"]
    effects:
      replaceHeaders:
      - header: authorization
        prefix: "Bearer "
        credentialUri: hk-secret://k8s.io/default/team-a-secrets/github/token
- https:
    hostnames: ["*.googleapis.com"]
- tlsPassthrough:
    hostnames: ["registry.example.com"]
    ports: {numbers: [443]}
- http:
    hostnames: ["mirror.internal.example"]
    ports: {numbers: [80, 8080]}
```

A template can give its golden Actor an egress policy through
`golden_egress_policy` (preview gate `GoldenEgressPolicy`). That lets the
warm-up phase reach, for example, a package mirror.

## Credential injection

An `http` or `https` rule can rewrite up to 16 request headers. The Actor
sends any placeholder value, and the gateway drops it and forwards the
credential in its place. A credential comes from one of two sources:

- **`credential_uri`**, a secret reference
  `hk-secret://<provider-class>/<provider-name>/<path>`. The gateway resolves
  it through the credential provider configured with
  `--credential-provider-name`, `--credential-provider-address`,
  `--credential-provider-ca-file`, `--credential-provider-client-cert` and
  `--credential-provider-server-name`. If no address is configured,
  injection is off.
- **`actor_jwt`**, a JWT minted for the calling Actor with the given
  `audiences` and an `expiration_seconds` between 300 and 3600. Tokens are
  cached and reused until a third of their lifetime remains, and concurrent
  requests share one mint.

### The bundled Kubernetes Secrets provider

`hivekeep-credential-provider` serves the `k8s.io` class:

```text
hk-secret://k8s.io/default/<namespace>/<secret>/<key>
```

The grammar is strict: no port, user info, query, fragment or
percent-encoding, and exactly three path segments after `default`. Install
the provider with `hivekeep-setup deploy hivekeep-system
--credential-provider='{"name":"k8s.io"}'` (the Kind Makefile target does
this). It uses the manifests in
`crates/hivekeep-setup/manifests/egress-credential-injection/`.

The provider only answers the egress gateway (`--injector-identity`). It
decides which Hive may read which namespace from a YAML file
(`--namespace-policy-file`, shipped as the ConfigMap
`k8s-credential-provider-namespace-policy`):

```yaml
policies:
- hive: team-a
  allowedNamespaces: [team-a-secrets]
  secretSelector:            # optional: every label must match
    matchLabels:
      team-a.example/egress: "true"
```

Access is denied by default: a Hive not listed here can resolve nothing.
Unknown fields are rejected, so a typo cannot silently widen a grant. A
missing Secret and a Secret outside the grant both return the same
PermissionDenied. The provider's `/statusz` page (loopback
`127.0.0.1:4040`) shows the version, flags and loaded grants, never values.

## HTTPS interception and the trust bundle

For `https` rules the gateway terminates TLS with a leaf certificate for
the requested SNI. `hivekeep-gate sdsmint` mints these leaves from the CA
pool in the `egress-mitm-ca-pool` Secret and serves them to Envoy over SDS
(`--leaf-cert-ttl`, default 15 minutes).

For Actors to trust these leaves:

1. `hivekeep-controller` publishes the pool's roots as the
   ClusterTrustBundle `egress-mitm.hivekeep.io:mitm:primary-bundle` (signer
   `egress-mitm.hivekeep.io/mitm`). If the pool Secret becomes unreadable,
   the last good bundle is kept. If the pool is deleted, the bundle is
   removed.
2. The template mounts a `system_info` volume with a `trust_bundle` data
   source naming `egress-mitm.hivekeep.io`, plus
   `system-roots.hivekeep.io` if the Actor also needs public roots:

   ```yaml
   volumes:
   - name: certs
     systemInfo:
       dataSources:
       - trustBundle:
           names: [egress-mitm.hivekeep.io, system-roots.hivekeep.io]
           path: ca-certificates.crt
   ```

3. The keeper writes the PEM on every start and refreshes it for running
   Actors when the bundle changes. Point the workload at the file, for
   example with `SSL_CERT_FILE`.

To rotate the CA, add a new CA to the pool, wait for the ClusterTrustBundle
and the Actors' files to pick it up, and only then make it the active
signer. `hivekeep-setup create egress-mitm-ca-pool` creates the initial
pool.

## DNS

The sandbox's resolver is a per-Actor DNS relay in the tunnel. It answers
from upstream only for names the Actor's policy could reach, using cached
per-name decisions. The only supported mode is
`--dns-egress-policy=enforce-in-tunnel`, which the controller validates and
passes to worker pods.

## Operations

- After an EgressPolicy changes, it takes effect within
  `--egress-policy-cache-ttl` (default 10s; 0 disables the cache).
- Deleting an Actor's EgressPolicy is the fastest way to cut off all of
  its egress.
- `hivekeep-setup --experimental-additional-egress-extproc-service=NS/SVC:PORT`
  (`HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE`, Envoy dataplane only) adds a
  second ext_proc authorization filter to the egress dataplane, for
  site-specific inspection.
