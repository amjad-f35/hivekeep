# hivekeep-identity

Keys, certificates and tokens for Actors and the components that act for
them.

- X.509: `CaPool` (an active CA plus trusted roots, as a JSON state file
  reloaded every minute). It mints Actor and worker-for-actor SPIFFE
  certificates from verified CSRs (`actor_cert`), short-lived certificates
  for intercepted hosts (`host_cert`), and pod workload certificates
  (`pod_cert`). Private extensions carry Actor and pod identity
  (`extension`).
- JWT: rotating `JwtPool` signing authorities, claims, JWKS publication, a
  verifier with cached and rate-limited key refresh, and OIDC discovery
  documents (`jwt`, `oidc`).
- Trust: PEM normalization, named trust bundles (`egress-mitm.hivekeep.io`,
  `system-roots.hivekeep.io`), and credential bundle files cached against
  file status for per-handshake TLS lookups (`cred_bundle`, `cred_loader`).

JWT verification uses `aws-lc-rs`. TLS elsewhere uses `ring`.

```sh
cargo test -p hivekeep-identity
```
