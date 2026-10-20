# hivekeep-credential-provider

A gRPC credential provider (`hivekeep.credprovider.v1`, binary
`hivekeep-credential-provider`). It resolves
`hk-secret://k8s.io/default/<namespace>/<secret>/<key>` references to
Kubernetes Secret values for the egress gateway's credential injection.
It is the only component on that path with access to Kubernetes Secrets.

- Answers only the injector identity (`--injector-identity`, by default the
  egress gateway's SPIFFE ID) over mTLS.
- A Hive may only read the namespaces granted in `--namespace-policy-file`,
  optionally narrowed by `secretSelector.matchLabels`. Access is denied by
  default, and unknown fields are rejected.
- A missing Secret and a Secret outside the grant return the same
  PermissionDenied.
- `/healthz`, `/readyz`, and `/statusz` on loopback (`127.0.0.1:4040`).
  The status page never shows Secret values.
- Prometheus `/metrics` on `--metrics-address` (`:9090`):
  `hivekeep_credprovider_fetch_secret_total{code}`,
  `hivekeep_credprovider_fetch_secret_duration_seconds{code}` and
  `hivekeep_credprovider_build_info{version}`.
- Kubernetes API requests are limited client side to `--kube-api-qps`
  (`50`) with bursts of `--kube-api-burst` (`100`).

The manifests are in
`crates/hivekeep-setup/manifests/egress-credential-injection/`.

```sh
cargo test -p hivekeep-credential-provider
```
