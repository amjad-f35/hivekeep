# hivekeep-idp

The Actor JWT identity provider (binary `hivekeep-idp`, deployed as
`hivekeep-idp-server`). It serves the OpenID Connect discovery document
(`/.well-known/openid-configuration`) and the JWK set (`/openid/v1/jwks`)
that relying parties use to verify Actor JWTs. Both are built from the same
`actor-id-jwt-pool` that `hivekeep-api` signs with, and they follow key
rotation.

Flags: `--issuer` (default `https://idp.<namespace>.svc`),
`--actor-id-jwt-pool`, `--listen-address` (`:8443`), `--server-cred-bundle`
(re-read when it changes), `--drain-grace`.

```sh
cargo test -p hivekeep-idp
```
