# Observability

Every Hivekeep binary starts its logging, tracing and metrics through one
call, `hivekeep_telemetry::init`. Services write structured JSON logs to
stdout and serve Prometheus metrics and health probes over HTTP; with an
OTLP endpoint configured they also push traces and metrics (and,
optionally, logs) to an OpenTelemetry collector. Attribute names live in the
`hk.` namespace (`hivekeep_core::attr`), so logs and metrics from different
components can be joined on the same keys: `hk.hive`, `hk.actor.name`,
`hk.actor.uid`, `hk.template.hive`, `hk.template.name`,
`hk.workerpool.namespace`, `hk.workerpool.name`, `hk.sandbox.class`,
`hk.snapshot.fidelity` and so on.

## Endpoints

| Component | Metrics | Health |
|-----------|---------|--------|
| `hivekeep-api` | `--metrics-listen-addr` (`0.0.0.0:9090`) `/metrics` | `--health-listen-addr` (`0.0.0.0:9091`) `/healthz`, `/readyz` |
| `hivekeep-keeper` | `--metrics-listen-addr` (`0.0.0.0:9090`) | `--health-listen-addr` (`0.0.0.0:9091`) |
| `hivekeep-gate router` | `--metrics-listen-addr` (`:9090`) | `--health-listen-addr` (`:9091`); readiness also checks dependencies every `--health-interval` (1s) |
| `hivekeep-controller` | `--metrics-addr` (`0.0.0.0:8080`) | `--health-addr` (`0.0.0.0:8081`) |
| `hivekeep-credential-provider` | `--metrics-address` `/metrics` | `--health-address` (`:9091`) |
| workers | none | `--readiness-listen-address` (`0.0.0.0:8080`) |

`/readyz` turns false when a process starts draining, so load balancers stop
sending traffic to it before it exits.

## Metrics

Control plane (`hivekeep-api`):

| Metric | Type | Labels |
|--------|------|--------|
| `hivekeep_actor_lifecycle_operation_duration_seconds` | histogram | `hk_actor_operation_name` (create, resume, suspend, delete, revert), `hk_template_name`, `hk_template_hive`, `hk_workerpool_namespace`, `hk_workerpool_name`, `hk_sandbox_class`, `hk_snapshot_kind`, `hk_snapshot_fidelity`, `error_type` |
| `hivekeep_scheduler_assignment_duration_seconds` | histogram | `outcome` (`assigned`, `no_capacity`, ...), `hk_workerpool_namespace`, `hk_workerpool_name`, `hk_sandbox_class`, `error_type` |
| `hivekeep_actor_crashes_total` | counter | `hk_actor_operation_name`, `hk_template_hive`, `hk_template_name`, `hk_workerpool_namespace`, `hk_workerpool_name`, `hk_sandbox_class` |
| `hivekeep_actor_state_changes_total` | counter | `hk_actor_state`: one per `hk.actor.state_changed` / `hk.actor.crashed` event |
| `hivekeep_workerpool_workers` | gauge | `hk_workerpool_namespace`, `hk_workerpool_name`, `hk_worker_state`, `hk_sandbox_class` |

Label names are the `hk.*` attribute keys with dots spelled as
underscores, so a series reads the same scraped or pushed.

Keeper:

| Metric | Type | Labels |
|--------|------|--------|
| `hivekeep_actor_restore_duration_seconds` | histogram | `hk_snapshot_phase` and `hk_template_hive`, `hk_template_name`, `hk_snapshot_fidelity`, `hk_snapshot_kind`, `hk_sandbox_class` (phases overlap, so each is a separate observation) |
| `hivekeep_actor_checkpoint_duration_seconds` | histogram | same as restore |
| `hivekeep_keeper_snapshot_size_bytes` | histogram | `file_name`, `hk_template_hive`, `hk_template_name` |
| `hivekeep_keeper_snapshot_upload_duration_seconds` | histogram | `hk_snapshot_fidelity`, `error_type` |
| `hivekeep_keeper_snapshot_detach_duration_seconds` | histogram | `hk_snapshot_fidelity`, `error_type` |
| `hivekeep_keeper_snapshot_upload_overdue_duration_seconds` | histogram | none |
| `hivekeep_keeper_snapshot_detach_overdue_duration_seconds` | histogram | none |
| `hivekeep_keeper_snapshot_upload_overdue_actors` | gauge | `hk_snapshot_upload_overdue_band` |
| `hivekeep_keeper_snapshot_undurable_age_max_seconds` | gauge | none |
| `hivekeep_actor_stats_sampled_actors` | gauge | template, sandbox class, `stats_source`, `worker_pool_namespace`, `worker_pool_name` |
| `hivekeep_actor_stats_memory_usage_bytes` | gauge | same as above |
| `hivekeep_actor_stats_memory_working_set_bytes` | gauge | same as above |
| `hivekeep_actor_stats_cpu_time_seconds_total` | counter | same as above |
| `hivekeep_filecache_size_bytes` | gauge | `cache` |
| `hivekeep_filecache_evictions_total` | counter | `cache` |
| `hivekeep_filecache_gc_passes_total` | counter | `cache`, `outcome` |
| `hivekeep_imagecache_requests_total` | counter | outcome, bounded error type |

Gate router:

| Metric | Type | Labels |
|--------|------|--------|
| `hivekeep_gate_router_route_duration_seconds` | histogram | template hive and name, router outcome, resume kind |
| `hivekeep_gate_router_parking_active` | gauge | none |
| `hivekeep_gate_router_parking_wait_duration_seconds` | histogram | `outcome` |
| `hivekeep_gate_router_parking_rejected_total` | counter | outcome |

Controller:

| Metric | Type | Labels |
|--------|------|--------|
| `hivekeep_workerpool_desired_workers` | gauge | `hk_workerpool_namespace`, `hk_workerpool_name` |
| `hivekeep_workerpool_ready_workers` | gauge | `hk_workerpool_namespace`, `hk_workerpool_name` |
| `controller_runtime_reconcile_total` | counter | `controller`, `result` (`success`, `requeue_after`, `error`) |
| `controller_runtime_reconcile_errors_total` | counter | `controller` |
| `controller_runtime_reconcile_time_seconds` | histogram | `controller` |

Credential provider: `hivekeep_credprovider_fetch_secret_total` and
`hivekeep_credprovider_fetch_secret_duration_seconds` (by gRPC `code`),
and `hivekeep_credprovider_build_info`.

Every metric above is declared in
`crates/hivekeep-core/registry/metrics.yaml`. `scripts/verify/metrics.sh`
fails when a component registers a family the registry does not declare,
when a declared metric is registered nowhere, or when the end-to-end suite
asserts on a name the registry lacks; it then runs Weaver on the registry
(`HK_METRICS_SKIP_WEAVER=1` skips that step).

Envoy's own statistics are on its admin and metrics ports (`9902` on the
router, `15090` on the egress gateway).

Questions these metrics answer:

- *Are resumes slow?* Use `hivekeep_actor_lifecycle_operation_duration_seconds{hk_actor_operation_name="resume"}`
  broken down by `hk_snapshot_fidelity`, then the keeper's restore phases.
- *Is the pool too small?* Look at `hivekeep_scheduler_assignment_duration_seconds{outcome="no_capacity"}`,
  `hivekeep_gate_router_parking_rejected_total` and
  `hivekeep_gate_router_parking_active`.
- *Is durability falling behind?* Check `hivekeep_keeper_snapshot_undurable_age_max_seconds`
  and `hivekeep_keeper_snapshot_upload_overdue_actors`.

## Logs

Every binary shares the logging flags of `hivekeep_telemetry::TelemetryArgs`:

| Flag | Environment | Default | Meaning |
|------|-------------|---------|---------|
| `--log-level` | `HK_LOG_LEVEL` | `info` (or `RUST_LOG` when set) | `error`, `warn`, `info`, `debug`, or a filter such as `info,hivekeep_gate=debug` |
| `--log-format` | `HK_LOG_FORMAT` | `json` for services, `text` for CLIs | `json` or `text` |
| `--otlp-endpoint` | `HK_TELEMETRY_OTLP_ENDPOINT` | unset | OTLP gRPC collector; see below |

Services log one JSON object per line on stdout, with `trace_id` and
`span_id` when the line is written inside a traced call, and with protobuf
fields marked `debug_redact` masked. Command-line tools (`hivectl`,
`hivekeep-setup`, the dev tools) write text to stderr, at `warn` unless
`-v` or the variables above say otherwise. Components whose flags predate
the shared set keep them: the keeper, `hivekeep-api` (its config file's
`logging.level`), the workers and the credential provider read their own
`--log-level` and honour `HK_LOG_FORMAT`; `--log-json` on the controllers
is still accepted.

Every gRPC server logs one `Handle RPC` line per call through
`GrpcLogLayer`: the method, the request (unary calls only, redacted), the
gRPC code and message, and the handler's elapsed time, which is also
returned to the caller as `x-server-elapsed-us` metadata. `hivekeep-api`
additionally refuses requests carrying fields its descriptors do not know
(`InvalidArgument`).

### Actor events

`hivekeep-api` writes one record per committed Actor state change, plus one
per crash. The micro-VM worker writes periodic usage records. The
vocabulary is pinned in `crates/hivekeep-core/registry/events.yaml`, and a
test keeps the code and the file in step:

| Event | Meaning | Key attributes |
|-------|---------|----------------|
| `hk.actor.state_changed` | An Actor reached a new state | `hk.hive`, `hk.actor.name`, `hk.actor.uid`, `hk.template.hive`, `hk.template.name`, `hk.actor.operation.name`, `hk.actor.state` |
| `hk.actor.crashed` | An Actor moved to CRASHED | identity plus the crash message |
| `hk.actor.usage_sampled` | One usage sample (CPU, memory) | identity plus `hk.stats.*`, `hk.actor.epoch` |

Never sample or filter `hk.actor.state_changed`. The latest record for an
`hk.actor.uid` gives that Actor's current state and when it began.

### Actor output

The workers forward each container's stdout and stderr as JSON log lines
labelled with `hk.hive`, `hk.actor.name` and `hk.actor.container.name`,
and add lifecycle records. `hivectl logs actor` picks one Actor's lines out
of its worker pod's stream and follows it across workers with `-f`. Pass
`-c _sandbox` for the sandbox's own diagnostics, and `--source lifecycle`
for lifecycle records only.

## Tracing and OTLP

OTLP export is off by default. It turns on when any of these is set:
`--otlp-endpoint` / `HK_TELEMETRY_OTLP_ENDPOINT`, the standard
`OTEL_EXPORTER_OTLP_ENDPOINT` (or a per-signal
`OTEL_EXPORTER_OTLP_{TRACES,METRICS,LOGS}_ENDPOINT`), or, for the workers,
the keeper's relay socket (`/var/lib/hivekeep/keeper-otlp.sock`, served by
`hivekeep-otlp-relay`). An explicit `OTEL_TRACES_EXPORTER`,
`OTEL_METRICS_EXPORTER` or `OTEL_LOGS_EXPORTER` is always honoured. The
install manifests set `OTEL_EXPORTER_OTLP_ENDPOINT` from the
`hivekeep-otel-config` ConfigMap, so in a cluster every component pushes.

With export on:

- Traces: spans from `tracing` are exported in batches; sampling is
  parent-based with a 10% root ratio unless `OTEL_TRACES_SAMPLER` /
  `OTEL_TRACES_SAMPLER_ARG` say otherwise.
- Metrics: the components that serve their own Prometheus registry (API,
  controller, keeper, gate router) push that registry over OTLP through a
  bridge, next to any OpenTelemetry instruments, so each series reaches the
  collector exactly once. `OTEL_METRICS_EXPORTER=otlp` stops serving
  OpenTelemetry instruments on `/metrics`; `prometheus` keeps pull only.
- Logs: off unless `OTEL_LOGS_EXPORTER=otlp` (or `otlp,console`).
- Resources carry `service.name` and anything in `OTEL_RESOURCE_ATTRIBUTES`
  (the manifests add `k8s.node.name` for node components).

`hivectl --trace` sends a W3C `traceparent` with every call and prints the
trace ID. The gate router can configure Envoy to report spans to an OTLP
gRPC collector (`--otlp-collector-address`). The installer exposes the
cluster's endpoint (`hivekeep-setup --otlp-endpoint`, `HK_OTLP_ENDPOINT`;
distinct from `HK_TELEMETRY_OTLP_ENDPOINT`, which only sets where a process
sends its own telemetry), and the controller passes `OTEL_*` settings
through to worker pods.

The Kind profile installs an OpenTelemetry Collector and Prometheus in the
cluster (`crates/hivekeep-setup/manifests/install/kind/`).
