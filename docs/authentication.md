# Authentication

Every call to `hivekeep-api` must present exactly one of two identities.

1. **A client certificate (mTLS).** If the TLS handshake carried a client
   certificate, the principal is the first URI SAN of the leaf, usually a
   SPIFFE ID. System components (controller, gate, keepers, API sidecars)
   authenticate this way with pod-identity certificates.
2. **A bearer JWT.** Otherwise the server reads `authorization: Bearer
   <token>`. It picks the provider whose `issuer` matches the token's `iss`,
   then verifies the signature, expiry and audience against that provider's
   keys. The principal is the token's `sub`.

A call without either fails with `UNAUTHENTICATED`. Principals are named in
access policies as `user:<principal>`, for example
`user:system:serviceaccount:team-a:ci` or
`user:spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-controller`.

## Configuring JWT providers

`--authentication-config` (env `HK_API_AUTHENTICATION_CONFIG`) points to a
YAML or JSON file. Unknown fields are rejected:

```yaml
jwtProviders:
- name: kubernetes                     # used in logs
  issuer: https://kubernetes.default.svc.cluster.local
  audiences: [hivekeep-api.hivekeep-system.svc]
  certificateAuthorityFile: /var/run/secrets/kubernetes.io/serviceaccount/ca.crt
  discoveryTokenFile: /var/run/secrets/kubernetes.io/serviceaccount/token
```

- At least one provider is required. Issuers must be unique.
- Keys come from the issuer's OIDC discovery document and JWKS over HTTPS,
  and are refreshed in the background. `certificateAuthorityFile` trusts a
  private CA for that fetch. `discoveryTokenFile` adds a bearer token to it,
  which an in-cluster Kubernetes issuer needs.
- A token must name one of `audiences`.

`hivekeep-setup create api-authentication-config` (part of `deploy
hivekeep-system`) writes this file for the cluster's own ServiceAccount
issuer, with the audience `hivekeep-api.hivekeep-system.svc`. It finds the
issuer through OIDC discovery, unless `--api-expected-jwt-issuer`
(`EXPECTED_JWT_ISSUER`) names one. So any ServiceAccount token minted for
that audience can authenticate:

```sh
kubectl -n team-a create token ci --audience hivekeep-api.hivekeep-system.svc
```

Point `jwtProviders` at your own identity provider to accept human users.

## Server TLS

`--grpc-server-cred-bundle` is a PEM file holding the private key followed
by the certificate chain. In the install, this is a Service DNS certificate
for `hivekeep-api.hivekeep-system.svc` from the
`servicedns.podcert.hivekeep.io/identity` signer. Clients verify it against
that signer's trust bundle. `hivectl` expects the name
`hivekeep-api.hivekeep-system.svc` by default. If the bundle is empty, the
server falls back to plaintext. That mode exists for local development and
should never be used in production.

Client certificates are verified against `--pod-identity-ca-certs`.

## Component-to-component authentication

| Caller -> callee | Mechanism |
|------------------|-----------|
| `hivekeep-api` -> keeper | mTLS with `--keeper-client-cred-bundle`. The API verifies the keeper's serving certificate (serverAuth, the SPIFFE ID of the `hivekeep-keeper` ServiceAccount) and pins the pod UID it looked up for the node. |
| keeper -> `hivekeep-api` | mTLS with the keeper's pod-identity bundle (`/run/podidentity.podcert.hivekeep.io/credential-bundle.pem`). The keeper verifies the server against `--api-ca-file` and `--api-server-name`. |
| keeper `Keeper` service | Accepts only `--api-allowed-spiffe-id` (default `spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-api`). |
| worker -> keeper (micro-VM) | mTLS over the `WorkerSupport` Unix socket. The worker checks the keeper's SPIFFE ID and that the keeper is on the same node. |
| gate router -> `hivekeep-api` | mTLS. The API maps the router's and the egress gateway's SPIFFE IDs (`--ingress-router-spiffe-id`, `--egress-gateway-spiffe-id`) to their system roles. |
| egress gateway -> credential provider | mTLS. The provider accepts only `--injector-identity` (default: the egress gateway's SPIFFE ID). |
| clients -> ingress router | mTLS. The client CA comes from `--ingress-client-ca-file`, and the SPIFFE ID must be on `--ingress-allowed-spiffe-ids`. |
| `hivekeep-api` -> PostgreSQL | TLS. Bundled installs use client certificates from the postgres signer. See [postgres.md](postgres.md). |

## Actor identity

Actors do not authenticate to `hivekeep-api`. What they have is an
identity that other parties can check:

- **Certificates.** Minted from `--actor-id-ca-pool`, with URI SAN
  `spiffe://hivekeep-actor.local/actor/<hive>/<actor>`.
- **JWTs.** Signed by `--actor-id-jwt-pool`, with issuer
  `--actor-jwt-issuer` (env `HK_API_ACTOR_JWT_ISSUER`; must be `https`
  with no query or fragment). `hivekeep-idp` serves
  `/.well-known/openid-configuration` and `/openid/v1/jwks` for that issuer
  from the same pool. The pool's keys are rotated with
  `hivectl admin add-jwt-key`, then `activate-jwt-key`, then
  `remove-jwt-key`. Activate a key only after relying parties have fetched
  the new key set. Remove the old key only after the last token it signed
  has expired.

The egress gateway can put an Actor JWT into outbound requests (see
[egress.md](egress.md)). The Actor never holds the signing key and never
sees the token.
