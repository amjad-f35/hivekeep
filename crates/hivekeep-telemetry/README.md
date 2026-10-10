# hivekeep-telemetry

Process telemetry for Hivekeep binaries:

- Build identity (`version`; `HK_BUILD_COMMIT` and `HK_BUILD_DATE` at
  compile time).
- A JSON tracing layer with OpenTelemetry trace correlation, and redaction
  of protobuf fields marked `debug_redact` (`contextlogging`, `protoredact`).
- OpenTelemetry bootstrap for traces, metrics (Prometheus and OTLP, plus a
  bridge pushing a binary's own Prometheus registry over OTLP) and logs, configured from the standard `OTEL_*` variables (`boot`, `init`).
- `/healthz`, `/readyz` and `/metrics` servers with timed dependency checks.
- gRPC handler wrappers for logging, deadline ceilings and unknown-field
  rejection (`interceptors`).
- The node OTLP relay (`otlprelay`), which lets workers export through the
  keeper's socket. The binary is `hivekeep-otlp-relay`.

Every Hivekeep binary starts with `hivekeep_telemetry::init`, flattening
`TelemetryArgs` (`--log-level`, `--log-format`, `--otlp-endpoint`) into its
CLI. OTLP export stays off until an endpoint or relay is configured.

```sh
cargo test -p hivekeep-telemetry
```
