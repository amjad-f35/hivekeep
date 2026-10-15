# hivekeep-gate

The gate, Hivekeep's network layer (binary `hivekeep-gate`).

- `hivekeep-gate router --mode=ingress`: the ext_proc server and Envoy xDS
  control plane behind `gate-router`. It resolves the target Actor
  (`hk-target-actor` or the CONNECT authority), resumes it with
  single-flight and request parking, and routes to the worker tunnel over
  mTLS.
- `hivekeep-gate router --mode=egress`: the ext_proc server behind
  `gate-egress`. It authenticates Actors from their tunnel certificates,
  evaluates their EgressPolicy, and injects credentials (`hk-secret://`
  references through a credential provider, or minted Actor JWTs).
- `hivekeep-gate sdsmint`: serves short-lived leaf certificates from the
  egress MITM CA pool to Envoy over delta SDS.
- `tunnel` (library): the per-Actor sandbox tunnel (ingress server,
  transparent egress proxy) and the policy-enforcing DNS relay, meant to
  run inside worker pods. `netns` has the Linux network-namespace
  helpers.

`--gate-dataplane` selects the dataplane; `envoy` (the default) is the
only one supported.

```sh
cargo run -p hivekeep-gate -- router --help
cargo test -p hivekeep-gate
```

See [docs/networking.md](../../docs/networking.md),
[docs/egress.md](../../docs/egress.md) and
[docs/request-parking.md](../../docs/request-parking.md).
