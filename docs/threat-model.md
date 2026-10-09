# Threat model

This page lists the attacks Hivekeep is designed to resist, the invariants
that resist them, and the work still needed where those invariants are not
fully in place. The tables below are exported to
[`threats.json`](threats.json) by `scripts/update/threats.sh`, and
`scripts/verify/threats.sh` fails if the two drift apart.

Priorities: **Critical** means a sandbox escape, a cross-tenant breach or a
control-plane takeover. **High** means credential or data exposure within
one tenant, or a broad denial of service. **Medium** and **Low** cover
narrower or harder-to-exploit issues.

## Assets

- Actor state: running memory, root filesystems, volumes, snapshots in
  object storage, Tags.
- Credentials: the CA pools (`actor-id-ca-pool`, `egress-mitm-ca-pool`,
  the pod certificate CA pools), the JWT authority pool, credential
  provider Secrets, object-store credentials, database credentials.
- The control plane's authority: the `Control` and `WorkerService` APIs,
  the `Keeper` service on every node, the store.
- Nodes and the Kubernetes API.

## Trust boundaries

```text
 [internet / clients] --mTLS--> gate-router --mTLS--> worker tunnel --> [Actor sandbox]
 [clients] --JWT or mTLS--> hivekeep-api --mTLS--> keeper --unix--> worker --> [Actor sandbox]
 [Actor sandbox] --tunnel--> gate-egress --policy--> [external services]
 keeper / objectstore plugin --scoped creds--> [object storage]
 hivekeep-api --role-separated TLS--> [PostgreSQL]
```

The sandbox boundary is the most important one. Everything inside an Actor
is untrusted. Worker pods and keepers are trusted only for the Actors that
are currently on their node.

## Attacks from inside an Actor

| Threat ID | Priority | Threat | Mitigating Invariants | Suggested Concrete Mitigations | Notes |
|---|---|---|---|---|---|
| T-01 | Critical | Code in an Actor escapes its sandbox to the worker pod or node. | Every Actor runs in its own gVisor sandbox or micro-VM; capabilities can only be changed within validated bounds; images are digest-pinned. | Track gVisor and Cloud Hypervisor security releases through SandboxConfig versions; run workers on dedicated, tainted nodes; keep `--cordon-system-infra-components` on. | The isolation strength comes from the chosen backend; the micro-VM class adds a hardware boundary. |
| T-02 | Critical | An Actor reaches another Actor on the same worker over the network. | Each Actor has its own network namespace; its egress only leaves through the tunnel to the egress gateway; the router reaches Actors only through the worker tunnel with mTLS. | Add an e2e check that two Actors on one worker cannot connect to each other's addresses. | Depends on the tunnel running in worker pods; see the README status section. |
| T-03 | Critical | An Actor reaches the keeper, the worker's Unix sockets or other node-local services. | The sandbox mount set is built only from the template's declared volumes and fixed system mounts; sockets that carry Actor credentials (keeper dial from the worker and tunnel) are mutually authenticated with pod certificates and same-node checks. | Periodically audit the sandbox mount set produced by the OCI spec builders in `hivekeep-imagecache::ocispec`. | |
| T-04 | Critical | An Actor reads or overwrites another Actor's snapshots. | Snapshot objects sit under `hives/<hive>/actors/<uid>`; for GCS, node credentials are downscoped per Actor (read-write own prefix, read-only completed Tags) and issued only for Actors bound to the calling node. | Add per-Actor scoping for the S3 backend, which currently uses the plugin's ambient credentials. | |
| T-05 | Critical | A tampered or corrupt snapshot is restored into an Actor. | Snapshot manifests are written last as commit markers; the sparse format reader validates its framing; restore needs a compatible runtime stamp. | Add content digests or a MAC over snapshot manifests and verify them on restore. | Integrity against a party with bucket write access currently relies on bucket access control. |
| T-06 | High | An Actor uses its egress to reach cloud metadata, the Kubernetes API or internal Services. | Egress is deny-by-default per Actor; UDP is always blocked; destinations must match a hostname-pattern rule and port; addresses match only `*`; DNS answers are filtered by the same policy. | Document a recommended deny list for `*` rules; alert on policies containing `*` with all ports. | |
| T-07 | High | A credential the Actor's traffic needs leaks from the sandbox (for example, through prompt injection). | Injected credentials never enter the sandbox: the Actor sends a placeholder and the egress gateway replaces the header for matching rules only. | Prefer `actor_jwt` injection with short expirations over static secrets where the upstream accepts JWTs. | Credentials the template itself puts in env vars or files are not protected by this. |
| T-08 | High | An Actor obtains identity credentials for a different Actor. | Actor certificates and JWTs are minted only by the control plane: through Control for the egress gateway role, or through WorkerService for a keeper on the Actor's current node. | Add a regression e2e test that minting for an Actor on another node returns NOT_FOUND. | |
| T-09 | High | An Actor that keeps getting resumed on new Workers uses the moves to spread laterally. | A restore starts a fresh sandbox; a restarted worker's previous Actors are crashed by epoch fencing; RUNNING commits are fenced on the placement epoch. | Rate-limit `RequestActorSuspend` from workers per Actor. | |
| T-10 | Medium | A malicious image or workload exhausts node CPU, memory, disk or file descriptors. | CPU and memory limits are enforced per Actor through cgroup leaves; the scheduler books limits plus runtime overhead; the image cache evicts at high and low watermarks. | Add per-Actor disk quotas for durable-dir and rootfs layers; limit image size at pull time. | The glutton workload in `hivekeep-bench` exercises these limits. |
| T-11 | Medium | An Actor learns cluster topology from what it can see inside the sandbox. | The sandbox gets a generated `resolv.conf` and `hosts`; system-info volumes expose only the Actor's own name, Hive, UID and allow-listed trust bundles. | Review the guest-visible environment for each sandbox class before 1.0. | |

## Attacks from API callers and tenants

| Threat ID | Priority | Threat | Mitigating Invariants | Suggested Concrete Mitigations | Notes |
|---|---|---|---|---|---|
| T-12 | Critical | A caller in one Hive reads or changes resources in another Hive. | Every Control RPC derives its authorization check from validated identifiers before running; Hive roles are evaluated against that Hive's policy only; the store rejects cross-hive references. | Keep the authz e2e suite (`crates/hivekeep-e2e/tests/authz/`) in the release gate. | |
| T-13 | Critical | Anyone on the internal network calls `hivekeep-api` or the keeper without credentials. | `hivekeep-api` requires a trusted JWT or a client certificate on every call; the keeper accepts only the control plane's SPIFFE ID; NetworkPolicies limit who can reach system pods. | Refuse to start `hivekeep-api` without `--grpc-server-cred-bundle` unless an explicit development flag is set. | `--authz-allow-all`, `--memory-store` and the keeper's `--insecure-dev` are development-only. |
| T-14 | High | A template author abuses template fields to escalate (for example by adding capabilities, picking a storage location or mounting host paths). | Templates are validated field by field; capabilities are bounded; `storage_location` is honored only with the `PerTemplateSnapshotLocation` preview gate; volume sources are limited to durable-dir, image, system-info and CSI external volumes. | Restrict template creation to Hive editors (the default) and review editors regularly. | |
| T-15 | High | A shared bootstrap owner grants every holder of its token global ownership. | Bootstrap owners must be configured explicitly; the installer warns when the shared `hivekeep-client` ServiceAccount is one. | Set `--api-authz-bootstrap-owners` to operator principals and manage the rest through access policies. | |
| T-16 | Medium | A flood of requests or Actors exhausts the API, the router or cluster capacity. | Resume requests share one flight per Actor; the parking lot has a fixed capacity and sheds with 503; per-pool capacity bounds placement. | Add per-principal rate limits on Control and per-Hive Actor quotas. | |
| T-17 | Medium | An egress policy grants more than intended through broad patterns. | Patterns are exact, `*.suffix` or `*`; the most specific match wins; rules that fail to compile are dropped (narrowing only); at most 256 rules. | Provide a dry-run evaluator in `hivectl` that shows which rule would decide a destination. | |

## Attacks from a compromised node or component

| Threat ID | Priority | Threat | Mitigating Invariants | Suggested Concrete Mitigations | Notes |
|---|---|---|---|---|---|
| T-18 | High | A compromised keeper reads snapshots of Actors that are not on its node. | WorkerService answers NOT_FOUND for Workers and Actors on other nodes; the credential exchange binds issuance to the Actor's current node. | Shorten the lifetime of downscoped tokens and alert on exchange refusals. | |
| T-19 | High | A compromised node escalates to the control plane or other nodes. | Keepers authenticate as their DaemonSet ServiceAccount; the control plane checks pod UID and node when dialing keepers; keeper roles are limited to node-scoped WorkerService calls and egress policy reads. | Bind the keeper ServiceAccount's Kubernetes RBAC to read-only, node-scoped access. | |
| T-20 | High | A compromised gate component misuses its control-plane role. | The egress gateway's role is limited to reading Actors and egress policies and minting Actor credentials; the router's role is limited to listing and resuming Actors. | Run the egress gateway's ext_proc without Kubernetes access (the egress mode already needs none). | |
| T-21 | High | Signing keys (CA pools, JWT pool) are stolen from Secrets. | Pools live in Secrets in `hivekeep-system`; JWT keys rotate through add, activate and remove; leaf certificates are short-lived. | Store pools in an external KMS-backed signer; restrict Secret access in `hivekeep-system` to the components that mount them. | |
| T-22 | Medium | Traffic between components is intercepted or spoofed. | Component traffic uses mTLS with pinned SPIFFE IDs or Service DNS names; the router verifies the worker's SPIFFE ID recorded on the assignment. | Enable dual-stack NetworkPolicies alongside mTLS. | |
| T-23 | Medium | A stale egress policy keeps applying after it changes. | The gateway caches policies for at most `--egress-policy-cache-ttl` (10s by default); the tunnel's policy source is a short-lived, deduplicated cache. | Push policy invalidations from the control plane instead of relying on the TTL. | |

## Operational threats

| Threat ID | Priority | Threat | Mitigating Invariants | Suggested Concrete Mitigations | Notes |
|---|---|---|---|---|---|
| T-24 | Medium | Insiders with database or bucket access read Actor state. | Database roles are split between owner and read-write; bundled PostgreSQL uses certificate logins; snapshot paths are owner-scoped. | Encrypt snapshots with per-Actor keys before upload. | |
| T-25 | Medium | Malicious activity goes unrecorded or unnoticed. | The control plane records Actor state changes, crashes and deletes with attribution; worker logs carry Actor, Hive and template labels; Prometheus metrics cover lifecycle errors and crashes. | Wire `hivekeep-telemetry` into the service binaries for OTLP export, and ship an alerting rule set. | |
| T-26 | Medium | A detected malicious Actor cannot be contained quickly. | Deleting the EgressPolicy removes all egress; suspend, revert and delete work from any state; Workers can be drained. | Add a single "quarantine" verb that strips egress and suspends. | |
| T-27 | Low | Production installs keep development placeholders (domain, OID arc, trust domain). | Every such value is listed in `PLACEHOLDERS.md`. | Fail the release image build when placeholder values remain. | |
