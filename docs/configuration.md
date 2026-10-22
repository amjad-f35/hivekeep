# Configuration

Hivekeep components are configured with command-line flags. Some flags also
read an environment variable. The installer sets the flags through the
manifests in `crates/hivekeep-setup/manifests/install/`, and its own
settings are layered as described in
[operator-install.md](operator-install.md#configuration-channels). Every
binary lists its flags with `--help`.

**Production values.** Many defaults work for development and tests but
must be replaced, or consciously confirmed, before a production deployment.
Examples are the Kubernetes API group and domain (`hivekeep.io`), the X.509
extension OID arc, the Actor SPIFFE trust domain, image registries
(`REGISTRY_PLACEHOLDER`), service names, ports, on-node paths and metric
prefixes. [`PLACEHOLDERS.md`](../PLACEHOLDERS.md) lists each one with its
exact location and what to put there. Treat that file as the checklist for
a production install.

## hivekeep-api

| Flag | Env | Default |
|------|-----|---------|
| `--config-file` | `HK_API_CONFIG_FILE` | required: `ApiConfig` as `.pb`, `.textproto` or `.textpb` (for example `logging: { level: "info" }`) |
| `--authentication-config` | `HK_API_AUTHENTICATION_CONFIG` | required (see [authentication.md](authentication.md)) |
| `--grpc-listen-addr` / `--metrics-listen-addr` / `--health-listen-addr` | | `0.0.0.0:443` / `:9090` / `:9091` |
| `--grpc-server-cred-bundle` | | empty, which means plaintext (development only) |
| `--postgres-*` | `HK_API_POSTGRES_*` | see [postgres.md](postgres.md) |
| `--memory-store` | | off |
| `--authz-bootstrap-owners`, `--authz-allow-all` | | none, off |
| `--controller-spiffe-id`, `--egress-gateway-spiffe-id`, `--ingress-router-spiffe-id`, `--api-server-sidecar-spiffe-id` | | derived from `POD_NAMESPACE` |
| `--actor-id-ca-pool`, `--actor-id-jwt-pool` | | required: CA pool and JWT pool files |
| `--actor-jwt-issuer` | `HK_API_ACTOR_JWT_ISSUER` | required |
| `--default-egress-gateway-address` | | the `host:port` sent to workers on every Run or Restore |
| `--pod-identity-ca-certs`, `--keeper-client-cred-bundle` | | needed to dial keepers |
| `--keeper-service-account`, `--keeper-selector`, `--keeper-port` | | `hivekeep-keeper`, keeper pod selector, `7443` |
| `--keeper-endpoints` | | static `node=endpoint` routes (development) |
| `--objectstore`, `--objectstore-plugin-socket`, `--objectstore-local-root` | | see [storage.md](storage.md) |
| `--snapshot-location` | `HK_API_SNAPSHOT_LOCATION` | cluster-wide snapshot base URI |
| `--storage-classes-file` | | JSON storage classes for external volumes; others are read from the cluster's StorageClasses |
| `--csi-client-cred-bundle`, `--csi-ca-file` | | Pod identity bundle and service DNS trust bundle for CSI controller mTLS (`/run/podidentity.podcert.hivekeep.io/credential-bundle.pem`, `/run/servicedns.podcert.hivekeep.io/trust-bundle.pem`) |
| `--template-resync-interval`, `--worker-relist-interval` | | `20s`, `5m` |
| `--drain-delay`, `--drain-timeout` | | `13s`, `15s` |
| `--preview` | | gates to enable, or `ALL` |

## hivekeep-keeper

| Flag | Env | Default |
|------|-----|---------|
| `--port` | | `7443` |
| `--base-dir`, `--snapshots-dir` | | `/var/lib/hivekeep`, `<base-dir>/snapshots` |
| `--grpc-server-cred-bundle`, `--client-ca-certs` | | the pod-identity bundle under `/run/podidentity.podcert.hivekeep.io/` |
| `--api-allowed-spiffe-id` | | `spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-api` |
| `--api-address`, `--api-ca-file`, `--api-server-name` | | `https://hivekeep-api.hivekeep-system.svc:443`, the service-DNS trust bundle, `hivekeep-api.hivekeep-system.svc` |
| `--pod-identity-oid` | | the pod-identity extension OID (see `PLACEHOLDERS.md`) |
| `--node-name` | `HK_NODE_NAME` | the node in the keeper's pod-identity certificate |
| `--object-store` | | `plugin` (or `local` when `--object-store-dir` is set) |
| `--snapshot-*`, `--image-cache-*`, `--sandbox-asset-*` | | see [storage.md](storage.md) |
| `--egress-ca-bundle-file`, `--system-roots-file` | | none, `/etc/ssl/certs/ca-certificates.crt` |
| `--actor-stats-poll-interval`, `--sandbox-sync-period` | | `1m`, `30s` |
| `--drain-delay`, `--drain-timeout` | | `0s`, `5m` |
| `--insecure-dev`, `--cluster-view-file` | | development only |

## hivekeep-gate

`hivekeep-gate router` takes `--mode=ingress|egress` (required) plus the
flags in [networking.md](networking.md), [egress.md](egress.md) and
[request-parking.md](request-parking.md). `--hivekeep-api-address` defaults
to `k8s:///hivekeep-api.hivekeep-system.svc:443`, a target that is resolved
from EndpointSlices and balanced client-side.

`hivekeep-gate sdsmint` takes `--uds-path`, `--ca-pool-path` and
`--leaf-cert-ttl` (default `15m`).

## hivekeep-controller

The flags cover the control-plane connection, the OpenTelemetry settings
passed to worker pods (`--otel-*`), the service accounts of the keeper and
router, `--dns-egress-policy`, `--worker-sync-concurrency` (2),
`--metrics-addr` (`0.0.0.0:8080`), `--health-addr` (`0.0.0.0:8081`),
`--log-json` and `--preview`.

## Workers

| Flag | Env | Purpose |
|------|-----|---------|
| `--pod-uid`, `--pod-ips` | `HK_POD_UID`, `HK_POD_IPS` | Identity of the worker pod (from the Downward API) |
| `--max-actors` | | Admission ceiling (1000) |
| `--usage-sample-interval` | | Usage record cadence (1m, at least 50s) |
| `--readiness-listen-address` | | `0.0.0.0:8080` |
| `--pool-namespace`, `--pool-name` | `POD_NAMESPACE`, `WORKER_POOL_NAME` | Attribution of usage records |
| `--runsc-flags` | `HK_RUNSC_FLAGS` | gVisor: JSON runsc flag overrides, reloaded when the file changes |
| `--tunnel-listen-address`, `--tunnel-connect-listen-address` | | Actor ingress HTTPS (`:443`) and mTLS CONNECT (`:8443`) |
| `--tunnel-credential-bundle`, `--tunnel-trust-bundle` | | Pod credential and trust bundles; also used for the keeper connection |
| `--tunnel-client-identity` | | SPIFFE ID allowed to call Actor ingress (the gate router) |
| `--tunnel-keeper-identity` (alias `--tunnel-broker-identity`) | | SPIFFE ID the node's keeper must present |
| `--tunnel-egress-listen-address`, `--tunnel-egress-trust-bundle` | | Captured egress port (`0.0.0.0:15001`) and the egress gateway's trust bundle |
| `--dns-egress-policy` | | Must be `enforce-in-tunnel` |
| `--no-tunnel` | | Development: Actors keep their own namespaces but egress, DNS and ingress go unserved |
| `--keeper-socket`, `--pod-identity-oid`, `--capacity-dir` | | Keeper worker-support socket, pod identity extension, projected capacity (`/run/worker-capacity`) |
| `--preview` | | Preview gates |
| `--cloud-hypervisor-binary` | | Micro-VM worker |
| `--standalone-epoch` | `HK_STANDALONE_EPOCH` (gVisor) | Serve with a fixed epoch instead of registering with the keeper (development) |

## Other binaries

- **`hivekeep-credential-provider`**: `--listen-address` (`:50051`),
  `--server-cred-bundle`, `--client-ca-file`, `--injector-identity`,
  `--namespace-policy-file`, `--status-address` (`127.0.0.1:4040`),
  `--health-address`, `--metrics-address` (`:9090`, Prometheus
  `/metrics`), `--drain-grace` (`5s`), and `--kube-api-qps` /
  `--kube-api-burst` (`50` / `100`, a client-side token bucket on every
  Kubernetes API request).
- **`hivekeep-idp`**: `--issuer`, `--actor-id-jwt-pool`, `--listen-address`
  (`:8443`), `--server-cred-bundle`, `--drain-grace`.
- **`hivekeep-podcert-controller`**: the CA pool paths of its three signers
  (`--service-dns-ca-pool`, `--pod-identity-ca-pool`, `--postgres-ca-pool`),
  `--postgres-client-namespace`, `--workers-per-signer`,
  `--kube-api-qps` / `--kube-api-burst` (client-side rate limit; QPS `0`
  disables it, burst `0` allows one second's worth), and the replica
  identity used for sharding (`--sharding-pod-*`,
  `--sharding-application-name`).
- **`hivekeep-objectstore-plugin`**: runs as the node plugin, the control
  plugin, or a health check. Run `--help` for its socket, exchange listener
  and byte-budget flags.
- **`hivekeep-otlp-relay`**: `--socket`, plus the shared logging flags.

Every binary also accepts the shared logging and export settings
(`--log-level`, `--log-format`, `--otlp-endpoint` where the component has
no older flag of the same name; see
[observability.md](observability.md#logs)).

## Common environment variables

| Variable | Used by | Meaning |
|----------|---------|---------|
| `HK_LOG_LEVEL`, `HK_LOG_FORMAT` | all | Defaults for `--log-level` (`error`..`debug` or a filter directive) and `--log-format` (`json`, `text`) |
| `RUST_LOG` | all | Log filter used when no level is set explicitly |
| `HK_TELEMETRY_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_*` | all | Turn on OTLP export of the process's own traces and metrics (off by default) and tune it |
| `HK_ACTOR_LOG_GCE_LABELS` | micro-VM worker | `true` spells forwarded Actor log labels as `logging.googleapis.com/labels` |
| `POD_NAMESPACE` | api, idp, workers | System namespace for derived defaults |
| `HK_STORAGE_BACKEND` | object-store plugin, `hivekeep-api --objectstore=direct`, keeper `--object-store=direct` | `s3` selects S3; anything else selects GCS |
| `GOOGLE_APPLICATION_CREDENTIALS`, `AWS_*` | object storage | Cloud credentials (see [storage.md](storage.md)) |
| `HK_PREVIEW` | installer | Preview gates for every component |
| `HK_ENDPOINT`, `HK_CA_FILE`, `HK_TOKEN_FILE`, `HK_TLS_SERVER_NAME` | `hivectl` | Connection settings |
| `HK_BUILD_COMMIT`, `HK_BUILD_DATE` | build | Build identity stamped into `hivekeep-telemetry` |

## Preview gates

| Gate | Unlocks |
|------|---------|
| `ExternalVolumes` | `Volume.external` (per-Actor CSI volumes) |
| `GoldenEgressPolicy` | `ActorTemplate.golden_egress_policy` |
| `PerTemplateSnapshotLocation` | `SnapshotConfig.storage_location` |

Enable gates on the server with `--preview=<gate>[,<gate>]` or
`--preview=ALL`. Installs set them on every component with `hivekeep-setup
--preview`. Clients opt in per request with the `hk-preview-allowed`
header.
