# Security design

Hivekeep assumes that the code inside an Actor is hostile. It might be an
LLM agent under prompt injection, a user's notebook, or a deliberately
malicious tenant. The platform has to keep that code from reaching other
Actors, the node, the control plane, and any credentials it was not given.
This page describes the mechanisms that do this. The attacker-by-attacker
analysis is in [threat-model.md](threat-model.md).

## Principles

1. **Deny by default.** No Actor egress without a rule. No ingress without
   an allowlisted client identity. No API access without a role binding.
   A NetworkPolicy keeps everything except the ingress router away from
   worker pods.
2. **Every hop authenticates.** Components talk over mutual TLS with pod
   certificates, and each side pins the SPIFFE ID it expects. Unix sockets
   between the keeper and workers are also mutually authenticated where the
   peer can act for an Actor.
3. **Least authority, scoped to one Actor.** Credentials for an Actor's
   snapshot, identity and egress are minted for that Actor only, and only
   while it is on the node that asks.
4. **Sandboxes hold no secrets.** Credentials the Actor's traffic needs are
   added by the egress gateway on the way out. The sandbox only ever sees a
   placeholder.
5. **Fail closed.** Unparseable policy entries are dropped, which only
   narrows an allow list. Unknown fields in policy files are rejected.
   Identifier validation runs before authorization.

## Isolation

| Layer | Mechanism |
|-------|-----------|
| Actor from Actor | Each Actor runs in its own sandbox: a gVisor sandbox (`hivekeep-sandbox-gvisor`, user-space kernel) or a Cloud Hypervisor micro-VM (`hivekeep-sandbox-microvm`, hardware virtualization). Each has its own network namespace and its own cgroup leaf. |
| Actor from node | gVisor: the sentry intercepts syscalls. Micro-VM: a separate guest kernel. Container capabilities start from a fixed default set, and templates may only change it within validated bounds (`ALL` is accepted only in `drop`). |
| Actor from Kubernetes | The Actor's OCI spec is built by Hivekeep from the template, not from a Kubernetes pod spec, so no ServiceAccount token is projected into it. Egress is tunnelled to the egress gateway, which only forwards to destinations the policy allows. |
| Tenant from tenant | Hives scope every API resource. Access policies bind roles per Hive. Snapshot object paths are prefixed by Hive and owner UID. |
| System from workers | `--cordon-system-infra-components` keeps control-plane pods on their own tainted nodes (see [architecture.md](architecture.md#system-infra-isolation)). |

Container images must be pinned by digest. Image layers are verified
against their diffIDs as they are unpacked, and unpacking is confined to
the cache directory (`hivekeep-imagecache`).

## Identity

- **Components.** `hivekeep-podcert-controller` answers PodCertificateRequests
  for three signers. `podidentity.podcert.hivekeep.io/identity` issues
  certificates with ServiceAccount SPIFFE IDs and a pod-identity extension.
  `servicedns.podcert.hivekeep.io/identity` issues serving certificates for
  the Services that select the pod. `postgres.podcert.hivekeep.io/identity`
  issues database client certificates, limited to the bundled logins. Leaf
  certificates last at most 24 hours.
- **Actors.** The control plane mints Actor certificates (URI SAN
  `spiffe://hivekeep-actor.local/actor/<hive>/<actor>`) from the
  `actor-id-ca-pool`, and Actor JWTs from the `actor-id-jwt-pool`. Relying
  parties verify JWTs against `hivekeep-idp`'s JWKS.
- **Workers acting for an Actor.** `WorkerService.MintWorkerActorCertificate`
  and `MintWorkerActorJwt` issue worker-for-actor credentials. The calling
  keeper must be on the node that currently hosts the Actor.

See [authentication.md](authentication.md).

## Authorization

Every Control RPC is checked against the global and Hive access policies
(`owner`, `editor`, `viewer`). System components hold fixed roles bound to
their mTLS SPIFFE IDs: controller, egress gateway, ingress router, keeper,
and the API server's sidecars. `WorkerService` callers must be keepers. They
can only see Workers and Actors on their own node, and get NOT_FOUND for
anything else. The `Keeper` service accepts only the control plane's SPIFFE
ID (`--api-allowed-spiffe-id`). See [authorization.md](authorization.md).

## Network

- **Ingress.** The ingress router requires a client certificate from the
  configured CA, with a SPIFFE ID on `--ingress-allowed-spiffe-ids`. If the
  list is empty, nothing is served. The router reaches the worker's tunnel
  over mTLS and verifies the worker's SPIFFE ID.
- **Egress.** Each Actor has at most one EgressPolicy, and without one,
  nothing is allowed. Rules allow HTTP, HTTPS (terminated and inspected by
  the gateway) or TLS passthrough (matched on SNI), per hostname pattern
  and port. All protocols are TCP. UDP is always blocked. DNS answers are
  filtered by the same policy in the sandbox's DNS relay.
- **Interception.** For `https` rules, the egress gateway presents a
  certificate minted by `hivekeep-gate sdsmint` from the
  `egress-mitm-ca-pool`, valid for 15 minutes by default. The controller
  publishes the pool's roots as the ClusterTrustBundle
  `egress-mitm.hivekeep.io:mitm:primary-bundle`, and Actors trust it by
  projecting the `egress-mitm.hivekeep.io` trust bundle.

See [egress.md](egress.md) and [networking.md](networking.md).

## Storage

- Snapshot objects live under `<location>/hives/<hive>/actors/<uid>/...` or
  `.../tags/<uid>`. Collectors only delete under their own owner's prefix.
- For GCS, the node object-store plugin exchanges an Actor's transfer JWT
  for a downscoped token. The control-plane plugin checks the JWT, looks up
  the Actor (refusing a recreated one with a new UID), checks that it is
  bound to the calling node, and issues credentials limited by a Credential
  Access Boundary. The Actor gets read-write on its own prefix and
  read-only on completed Tags. S3 transfers use the plugin's ambient
  credentials.
- Runtime assets are downloaded with their SHA-256 checked and staged
  atomically. The snapshot manifest is uploaded last, so a partial upload
  is never mistaken for a complete one.
- PostgreSQL is reached with separate owner (migrations) and read-write
  (serving) roles. The bundled install authenticates with client
  certificates from the postgres signer.

See [storage.md](storage.md) and [postgres.md](postgres.md).

## Secrets handling

- `EnvVar.value` and other sensitive fields are marked `debug_redact` in the
  protos. The object-store plugin's Debug output never prints JWTs, asset
  URIs or tokens.
- The credential provider returns the same PermissionDenied for a missing
  Secret as for one outside the Hive's grant, so callers cannot probe for
  names. Its status page lists grants, never values.
- Unauthenticated status pages (`/statusz` on the router and the credential
  provider) bind to loopback by default.

## Audit and response

`hivekeep-api` records Actor events (state changes, crashes and deletes)
with the Actor, Hive and template attached. Workers and the keeper attach
the same attribution to sandbox logs and usage records (see
[observability.md](observability.md)). To contain an Actor, delete its
EgressPolicy (it loses all egress immediately, once the gateway's policy
cache of up to `--egress-policy-cache-ttl` expires), suspend or delete it,
or drain its Worker with `hivectl drain worker`.

## Hardening checklist

- Replace every value in [PLACEHOLDERS.md](../PLACEHOLDERS.md), in
  particular the X.509 extension OID arc, the `hivekeep.io` domain and the
  Actor trust domain.
- Set `--api-authz-bootstrap-owners` to your operators' principals. Do not
  leave it as the shared `hivekeep-client` ServiceAccount.
- Never run `hivekeep-api` with `--authz-allow-all`, `--memory-store`, or
  without `--grpc-server-cred-bundle` outside development. Never run the
  keeper with `--insecure-dev`, or the gate with
  `--credential-provider-insecure`.
- Use `--cordon-system-infra-components` on shared clusters.
- Keep the ingress allowlist to the identities that really need to reach
  Actors.
