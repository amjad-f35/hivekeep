# hivekeep-bench

Load and benchmark tools:

- `glutton`: a sandboxed workload that consumes RAM, disk, CPU and file
  descriptors on request.
- `locust-worker`: a load generator that speaks the locust protocol. Its
  user classes drive Actors through their lifecycle, and drive traffic
  through the gate.
- `egress-bench`: the egress gateway benchmark (a traffic Actor and a
  target).
- `fake-keeper` and `fake-workersync`: a fake data plane for benchmarking
  the control plane on its own (install with `--fake-data-plane`).

`hivekeep-setup deploy benchmarks` deploys the benchmark stack, using
`scripts/benchmarking/deploy_locust.sh`.
The script runs `workloads/deploy.sh`, `locust/build_and_push.sh` and
`locust/deploy.sh` from this crate (override the directory with
`HK_BENCHMARKING_DIR`). `locust/deploy_stack.sh` does the same three steps
directly.

## Optional Prometheus + Grafana

The locust web UI is enough for most runs. For longer runs, or to line load
up against container memory and CPU, apply the monitoring stack:

```bash
kubectl apply -f crates/hivekeep-bench/monitoring.yaml
# or: scripts/benchmarking/deploy_locust.sh --deploy --with-monitoring
kubectl -n benchmarking port-forward svc/grafana 3000:3000
```

It scrapes the locust master (8000) and worker (8001), automation runner
Jobs, the optional postgres-exporter sidecar in `hivekeep-system`, and
kubelet cAdvisor, and provisions a "Hivekeep benchmark load" dashboard.

## Python tests

The locust files, automation and analysis scripts are Python 3.10+. Run
their unit tests with `make test-python` from the repository root.
