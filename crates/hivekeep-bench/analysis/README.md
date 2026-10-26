# Suspend/resume phase analysis

Turns the node side's timing logs into the percentiles a benchmark run needs,
without making the phases part of the metrics API. The
`hk.actor.restore.duration` and `hk.actor.checkpoint.duration` histograms
stop at `worker_restore`, `worker_checkpoint`, `persist` and `download` on
purpose: what happens inside those is a sandbox runtime's implementation
detail, and metrics are a contract. The finer breakdown is logged as
structured records instead, and this directory is what aggregates them.

Two record kinds, each written by both layers, join per operation:

| message | written by | duration fields |
|---|---|---|
| `Restore timing breakdown` | keeper, both sandbox runtimes | `hk.actor.restore.duration.<phase>` (keeper), `worker.actor.restore.duration.<phase>` (runtime) |
| `Checkpoint timing breakdown` | keeper, both sandbox runtimes | `hk.actor.checkpoint.duration.<phase>` (keeper), `worker.actor.checkpoint.duration.<phase>` (runtime) |

Each record carries the Actor's identity (`hk.actor.uid`, name, hive,
template) and the snapshot's kind and fidelity, which the histograms may not
carry, so records join per Actor and per operation. Durations are seconds as
floats, like the histograms. A phase that did not run is absent, not zero. A
failed keeper operation still writes its record, with `error.type` set to the
gRPC code; the report leaves those out of the percentiles.

The reader accepts a record flat (message under `msg` or `message`, fields
beside it) or as the tracing JSON formatter writes it (message and the
record, a JSON string field named `timing` or `record`, under `fields`).

## Making a measurement

1. Run a suspend/resume-heavy load, e.g. the glutton or sweperf class.
2. Dump the node logs for the run while the pods still exist. The automation
   deletes the workload and system pods right after each test and does not
   call this script, so run it before that teardown, or on a cluster you
   manage yourself:

   ```bash
   crates/hivekeep-bench/analysis/collect_logs.sh --dest /tmp/run1 --since-time 2026-09-24T18:00:00Z
   ```

   The script reads `.hivekeep-dev-env.sh` unless `NO_DEV_ENV` is set and
   honors `KUBECTL_CONTEXT`. Make `--since-time` (or `--since 30m`) cover the
   run and nothing before it. `--namespace` (default `hivekeep-system`)
   selects the keeper pods and `--worker-namespace` (default
   `benchmark-workloads`) the worker pods. `kubectl logs` returns only a
   container's current log file, so on a long or busy run rotation can drop
   early records: collect soon after. A container that restarted is dumped
   twice (`<pod>.previous.log`).

   On GKE the same records are in Cloud Logging, which keeps them past
   rotation and teardown, and an export is read directly:

   ```bash
   gcloud logging read '(jsonPayload.fields.message="Checkpoint timing breakdown" OR jsonPayload.fields.message="Restore timing breakdown") AND resource.labels.cluster_name="<cluster>" AND timestamp>="2026-09-24T18:00:00Z"' \
     --project <project> --format json > /tmp/run1/export.json
   ```

3. Aggregate the dumps, or the export:

   ```bash
   python3 crates/hivekeep-bench/analysis/phase_report.py /tmp/run1/*.log --csv /tmp/run1/report
   python3 crates/hivekeep-bench/analysis/phase_report.py /tmp/run1/export.json --csv /tmp/run1/report
   ```

   One source per run: dumps and an export of the same pods hold the same
   records, and both together would count each twice.

## Reading the report

**Phase percentiles.** Per layer, operation, sandbox class, snapshot kind,
scope and phase: count, p50, p90, p95 and max. A `local` restore reads a
node-local snapshot and a `latest` one a durable snapshot from object
storage, so they are separate rows, as are gVisor and micro-VM operations. A
restore from a template's golden snapshot reports `latest`. The keeper rows
split a checkpoint into `sandbox_assets`, `worker_checkpoint`, `persist` and
`cleanup`, and a restore into `volume_mount`, `manifest_fetch`,
`sandbox_assets`, `download`, `oci_unpack` and `worker_restore`. The worker
rows split the `worker_*` phase with the runtime's own phases: for micro-VM
`prep` / `pause` / `snapshot` / `durable_dir` / `rootfs_upper` / `teardown`
on a checkpoint and `prep` / ... / `vm_restore` / `wakeup_probe` on a
restore; for gVisor `pause` / `checkpoint` / `fs_checkpoint` / `resume` /
`teardown` and `prep` / `net_setup` / ... / `sandbox_restore` /
`app_restore` / `wakeup_probe` / `activate` (the full lists are in each
runtime crate's `phaselog`). Worker records carry no sandbox class or kind of
their own; a paired one takes both from its keeper record, so both layers
split into the same rows.

Mind the concurrency: the keeper's restore phases overlap (the download runs
beside the asset fetch and unpack), the three micro-VM checkpoint captures
run concurrently on the paused guest (the paused window costs their max), and
the worker restore phases are sequential. For both checkpoint layers the
report adds an `unattributed` row, the total less what the logged phases
account for (counting the concurrent captures once): the time the
instrumentation does not name yet.

**Waterfalls.** The slowest operations, with the worker record of the same
Actor nested under the keeper's `worker_*` phase and that phase's gap to the
worker total (the call and queueing between the layers). The worker record
paired is the one written inside that phase's window (for a checkpoint,
before `persist` began), and each worker record pairs with at most one
operation, so an operation whose own record is missing prints without one
rather than with another cycle's. Failed operations are left out of both
sections. A tail outlier that always blows up in one phase is systematic;
different phases each time point to the environment.

`--csv` also writes `phase_percentiles.csv` for comparing runs.

The parser is tolerant: it finds the JSON object on any line and matches on
the message, so raw `kubectl logs` dumps, even prefixed, work.

## Automated runs

The locust runner collects the breakdown itself at the end of a headless
run, reading the keeper and worker pod logs through the Kubernetes API while
the pods still exist, and appends the percentiles to the run's `stats.jsonl`
(`stats_rows()` gives the shape: one row per layer / operation / class /
kind / scope / phase, metric `phase_<layer>_<op>_<class>_<kind>_<scope>_<phase>`,
measurements as strings). The runner's ServiceAccount needs `list` on pods
and `get` on `pods/log` in `hivekeep-system` and `benchmark-workloads`
(`automation/manifests/runner-job.yaml.tmpl`).

## Tests

```bash
python3 -m unittest discover -s crates/hivekeep-bench/analysis
```
