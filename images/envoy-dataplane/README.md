# Envoy dataplane image

Build context of `hivekeep-envoy-dataplane`, the Envoy that the gate's
`envoy` router (`gate-router.yaml`) and the egress gateway
(`gate-egress.yaml`) run. It is the upstream Envoy image plus one dynamic
module:

```text
Dockerfile
dynamic-modules/              separate cargo workspace (not in the repo workspace)
  egress-policy-core/         SDK-free policy parsing, hashing, interning, chain choice
  egress-policy/              cdylib libenvoy_hivekeep_egress_policy.so for Envoy
```

The bootstrap is not baked in: each Deployment mounts its own from a
ConfigMap at `/etc/envoy/envoy.yaml` (`gate-router-envoy-config`, and the
egress one). The router's bootstrap points ADS at the router on
`127.0.0.1:18000`; the admin API stays on loopback `:9901` and only
`/ready` and `/stats/prometheus` are exposed on `:9902`.

## The egress-policy module

The egress gateway sees an actor connection twice: as a CONNECT request on
the outer listener, and as the tunnelled TLS on an internal listener. The
gate's ext_proc answer to the CONNECT carries the actor's SNI rules as JSON
in dynamic metadata (`io.hivekeep.policy.egress` / `policy_json`).

- `envoy_hivekeep_egress_policy_store` (HTTP filter, outer leg, after
  ext_proc) parses the rules, interns them in a per-worker-thread table keyed
  by a 64-bit rapidhash of their content, and writes the hash to
  `io.hivekeep.policy.egress.hash.not_shared`. A `set_filter_state` filter
  then copies it to `io.hivekeep.policy.egress.hash` shared with the
  upstream, because the module API cannot share filter state itself yet.
  Identical rule sets share one entry, and the last stream using an entry
  removes it. A hash collision between different rule sets fails the
  CONNECT with 400.
- `envoy_hivekeep_egress_policy` (listener filter, inner leg, after
  tls_inspector) writes `io.hivekeep.egress.filter_chain`: `mitm` or
  `passthrough` for TLS whose SNI matches a rule of that mode (first match
  wins), `cleartext` for non-TLS, and `denied` for anything else. No chain is
  named `denied`, so Envoy closes those connections.

Stats: `hk_egress.policy_store_size` (gauge), `hk_egress.parse_errors` and
`hk_egress.policy_shared` (counters).

## Building and testing

```sh
docker buildx build -t hivekeep-envoy-dataplane images/envoy-dataplane
```

The SDK is a git dependency on the Envoy repository and needs libclang, so
the module workspace is built only in the image. The policy logic has no SDK
dependency; test it on its own with
`cargo test --manifest-path images/envoy-dataplane/dynamic-modules/Cargo.toml -p hivekeep-egress-policy-core`
(this still fetches the SDK to resolve the workspace), or run the whole
module suite, SDK mocks included, inside the build image.
