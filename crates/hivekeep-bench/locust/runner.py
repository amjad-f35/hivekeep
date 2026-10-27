"""Runs one headless locust test and publishes what it measured.

The runner starts locust with the flags it is given, turns locust's stats CSV
into JSONL rows, adds the cluster, server-side and phase summaries next to
them, and copies every artifact to a GCS bucket or a local directory under
<dest>/runs/<name>/run_date=.../run_ts=.../run_tag=<tag>/.

Most load classes run in the Hivekeep load generator, the Rust
`locust-worker` binary baked into the image. For those the runner starts
locust as a master that waits for one worker, then starts locust-worker
next to it. The few classes still written in Python (PYTHON_TESTS) run
inside locust itself.

-f takes one path or a comma-separated list, which locust loads as one test:
the test file first, then any load shape, as in
`/app/tests/glutton.py,/app/shapes/ladder_shape.py`. A shape declares no user
class, so test_file() picks the test out of the list.
"""

import argparse
import csv
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, TextIO

from cluster_facts import (
    EMPTY_FACTS,
    append_trial_summary,
    diff_worker_pods,
    get_cluster_hardware_facts,
    read_evicted_pods,
    snapshot_worker_pods,
)
from common.loadgen_config import build_config_json
from server_telemetry import extract_and_record_server_telemetry
from phase_breakdown import append_phase_breakdown
from ladder_steps import append_ladder_steps

# The load generator binary, copied into the image by locust/Dockerfile.
WORKER_BINARY = "/app/locust-worker"

# Loopback port of the headless /loadgen-config server
# (common/loadgen_config.py), from which the worker reads the values that
# change during a run. 5557 (worker protocol) and 8089 (web UI) are taken.
LOADGEN_CONFIG_PORT = 5560

# The in-cluster Prometheus the benchmark monitoring stack installs.
# --prometheus-url overrides it, e.g. for a port-forward.
DEFAULT_PROMETHEUS_URL = "http://prometheus.benchmarking.svc.cluster.local:9090"

# Columns of traces.txt, tab separated. Readers index them by position.
TRACE_COLUMNS = ("time", "name", "duration_ms", "latency_source", "trace_id", "err")

# The line a Python user class logs for each sampled span
# (common/grpc_tracing.py, tests/counter_demo.py):
#   Traced {name}[ (failed)]: trace_id={32hex}, duration_ms={float} ({src})
PY_TRACE_RE = re.compile(
    r"Traced\s+(?P<name>\S+)(?:\s+\(failed\))?:\s*"
    r"trace_id=(?P<trace_id>[0-9a-f]{32}),\s*"
    r"duration_ms=(?P<duration>[0-9.]+)\s+"
    r"\((?P<source>\w+)\)"
)


def json_object(value: str) -> dict[str, Any]:
    """argparse type for --metadata: a JSON object, checked before the run."""
    try:
        obj = json.loads(value)
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"not valid JSON: {e}") from e
    if not isinstance(obj, dict):
        raise argparse.ArgumentTypeError("must be a JSON object")
    return obj


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-f", required=True, dest="file", help="Locust test file (-f)")
    p.add_argument("-t", required=True, dest="duration", help="Run duration (-t)")
    p.add_argument(
        "-u", required=True, type=int, dest="users", help="Number of users (-u)"
    )
    p.add_argument("--tag", required=True, help="Tag for this run")
    p.add_argument(
        "--name", required=True, help="Name for this run; used as locust --csv prefix"
    )
    p.add_argument(
        "--dest",
        required=True,
        help="Root destination (gs://bucket/path or local path)",
    )
    p.add_argument(
        "--allow-empty-stats",
        action="store_true",
        help=(
            "Exit 0 even when locust wrote no measurement rows, for a test "
            "whose result lives outside the locust statistics (one that sends "
            "no request on purpose, say)"
        ),
    )
    # Start-up flags of locust-worker. Neither locust nor the runtime config
    # knows them, so they are parsed here, kept out of locust_extra (locust
    # would reject them) and appended to the worker command in run_test().
    p.add_argument(
        "--actors-per-user",
        type=int,
        default=None,
        help=(
            "Actors each GluttonUser creates and cycles through round-robin "
            "(iteration i uses actor i%%N). Passed to locust-worker as "
            "--actors-per-user; unset keeps the worker's default of 1."
        ),
    )
    p.add_argument(
        "--total-actors",
        type=int,
        default=None,
        help="Actors to create in the batch (passed to locust-worker).",
    )
    p.add_argument(
        "--spawn-concurrency",
        type=int,
        default=None,
        help="Actors created concurrently (passed to locust-worker).",
    )
    p.add_argument(
        "--actor-deadline",
        type=float,
        default=None,
        help="Per-actor timeout in seconds (passed to locust-worker).",
    )
    p.add_argument(
        "--cluster-facts",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "After the run, read node capacity and the worker pod count from "
            "the Kubernetes API for the density frontiers, and worker pod "
            "restarts and evictions around the run for oom_events. "
            "--no-cluster-facts skips every Kubernetes API read"
        ),
    )
    p.add_argument(
        "--metadata",
        type=json_object,
        default={},
        help=(
            "A JSON object describing the run, e.g. which cluster it ran on. "
            "Each key lands in trial_summary as metadata_<key>"
        ),
    )
    p.add_argument(
        "--prometheus-url",
        default=DEFAULT_PROMETHEUS_URL,
        help=(
            "Prometheus to read the server-side telemetry from after the run. "
            "If it cannot be reached the affected fields are null; the run "
            "does not fail"
        ),
    )
    p.add_argument(
        "--keeper-lag-s",
        type=int,
        default=70,
        help=(
            "Seconds to wait after the run so the keeper's last metric export "
            "is scraped; the snapshot and active-actor windows are read half "
            "this late. It must cover the keeper's export interval plus one "
            "Prometheus scrape; the default fits a 60s export and a 10s scrape"
        ),
    )
    p.add_argument(
        "--phase-breakdown",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "After the run, read the keeper and worker pod logs and append "
            "suspend/resume phase percentiles to stats.jsonl (see "
            "analysis/README.md). --no-phase-breakdown skips the log reads"
        ),
    )
    args, extra = p.parse_known_args()
    args.locust_extra = extra
    return args


# Test files whose user class is written in Python and runs inside locust.
# Everything else is a class of locust-worker. New load is written for the
# worker, so this set only shrinks.
PYTHON_TESTS = frozenset({
    "hive_api.py",
    "counter_demo.py",
    "sleep.py",
    "usermem.py",
    "kernelmem.py",
})


def test_file(files: str) -> str:
    """The one path of `files` (the -f value) that holds the user class.

    Load shapes live in the shapes/ directory and declare no user class, so
    they are dropped first; the run's class name and needs_worker() must not
    come from a shape. Worker classes have no PYTHON_TESTS entry, so that set
    cannot find the test by itself. What remains is the test, whatever order
    the paths were given in.
    """
    paths = [p for p in files.split(",") if p]
    tests = [p for p in paths if os.path.basename(os.path.dirname(p)) != "shapes"]
    if tests:
        return tests[0]
    return paths[0] if paths else files


def needs_worker(files: str) -> bool:
    return os.path.basename(test_file(files)) not in PYTHON_TESTS


def pair_flags(locust_extra: list[str]) -> list[str]:
    """`locust_extra` with each flag joined to its value, for the log.

    tests.yaml lists a flag and its value as two entries; one line per pair
    keeps the log readable side by side with tests.yaml.
    """
    pairs: list[str] = []
    i = 0
    while i < len(locust_extra):
        item = locust_extra[i]
        nxt = locust_extra[i + 1] if i + 1 < len(locust_extra) else None
        if item.startswith("--") and nxt is not None and not nxt.startswith("--"):
            pairs.append(f"{item} {nxt}")
            i += 2
            continue
        pairs.append(item)
        i += 1
    return pairs


def tee(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()


def log_run_config(args: argparse.Namespace, dest_prefix: str, work_dir: Path, logs: TextIO) -> None:
    """Writes what this run executes at the top of logs.txt, so a reader
    needs neither tests.yaml nor the orchestrator's command line."""
    lines = [
        "==== Run config ====",
        f"  name:           {args.name}",
        f"  tag:            {args.tag}",
        f"  files:          {args.file}",
        f"  test_file:      {test_file(args.file)}",
        f"  duration:       {args.duration}",
        f"  users:          {args.users}",
        f"  uses_worker:    {needs_worker(args.file)}",
        f"  empty stats ok: {args.allow_empty_stats}",
        f"  dest_prefix:    {dest_prefix}",
        f"  work_dir:       {work_dir}",
        # One flag and its value per line: a run passes many flags, and a
        # single line of them cannot be compared with tests.yaml.
        "  extra flags:",
    ]
    lines += [f"    {flag}" for flag in pair_flags(args.locust_extra)] or ["    (none)"]
    lines.append("====================")
    for line in lines:
        tee(logs, line)


def _worker_trace_record(line: str) -> dict[str, str] | None:
    """A sampled span from one locust-worker log line.

    The worker logs JSON through tracing-subscriber: the event's fields sit
    under "fields", its text under "fields.message", and the time at the
    top level as "timestamp".
    """
    if "trace_id" not in line:
        return None
    try:
        obj = json.loads(line)
    except ValueError:
        return None
    if not isinstance(obj, dict):
        return None
    fields = obj.get("fields")
    if not isinstance(fields, dict):
        fields = obj
    message = str(fields.get("message", fields.get("msg", "")))
    if not message.startswith("traced span") or "trace_id" not in fields:
        return None
    duration = fields.get("duration_ms")
    return {
        "time": str(obj.get("timestamp", obj.get("time", ""))),
        "name": str(fields.get("name", "")),
        "duration_ms": "" if duration is None else f"{float(duration):.3f}",
        "latency_source": str(fields.get("source", "")),
        "trace_id": str(fields["trace_id"]),
        "err": str(fields.get("err", "")),
    }


def extract_trace_record(prefix: str, line: str) -> dict[str, str] | None:
    """The trace record (TRACE_COLUMNS keys) a log line describes, if any.

    Worker lines are JSON events whose message starts with "traced span";
    locust lines are the free-form "Traced ...:" lines of the Python classes.
    A failed span carries its error in `err`.
    """
    if prefix == "worker":
        return _worker_trace_record(line)
    m = PY_TRACE_RE.search(line)
    if not m:
        return None
    return {
        "time": "",
        "name": m.group("name"),
        "duration_ms": m.group("duration"),
        "latency_source": m.group("source"),
        "trace_id": m.group("trace_id"),
        "err": "failed" if "(failed)" in line else "",
    }


def pump_stream(prefix: str, stream: IO[str], logs: TextIO, traces: TextIO) -> None:
    """Copies each line of `stream`, tagged with `prefix`, to stdout and the
    log, and every trace record found in it to `traces` as a TSV row."""
    for line in stream:
        line = line.rstrip("\n")
        tagged = f"[{prefix}] {line}"
        sys.stdout.write(tagged + "\n")
        sys.stdout.flush()
        logs.write(tagged + "\n")
        logs.flush()
        record = extract_trace_record(prefix, line)
        if record is not None:
            traces.write("\t".join(record[c] for c in TRACE_COLUMNS) + "\n")
            traces.flush()


def worker_command(args: argparse.Namespace) -> list[str]:
    """The locust-worker command line for this run."""
    cmd = [WORKER_BINARY, "--user-class", Path(test_file(args.file)).stem]
    cfg_json = build_config_json(args.locust_extra)
    if cfg_json:
        cmd += ["--config-json", cfg_json]
    if args.actors_per_user is not None:
        cmd += ["--actors-per-user", str(args.actors_per_user)]
    if args.total_actors is not None:
        cmd += ["--total-actors", str(args.total_actors)]
    if args.spawn_concurrency is not None:
        cmd += ["--spawn-concurrency", str(args.spawn_concurrency)]
    if args.actor_deadline is not None:
        cmd += ["--actor-deadline", str(args.actor_deadline)]
    # Re-read the config at every spawn message, so a value that changes
    # mid-run (a ladder step's sample rate, say) reaches the worker when it
    # changes. The worker's --master-host default is the loopback address the
    # server listens on.
    cmd += ["--master-web-port", str(LOADGEN_CONFIG_PORT)]
    return cmd


def run_test(args: argparse.Namespace, csv_prefix: Path, logs: TextIO, traces: TextIO) -> int:
    """Runs locust, and locust-worker when the class needs it, and returns
    locust's exit code.

    Each process's output goes to logs.txt tagged `[locust]` or `[worker]`;
    the sampled spans found in it go to traces.txt.
    """
    with_worker = needs_worker(args.file)

    locust_cmd = [
        sys.executable, "-m", "locust",
        "--headless",
        "-f", args.file,
        "-t", args.duration,
        "-u", str(args.users),
        "--csv", str(csv_prefix),
        # A stats_history.csv row for every operation, not only Aggregated,
        # so each one's latency can be plotted over the run. The readers in
        # this directory keep to the Aggregated rows.
        "--csv-full-history",
    ]
    if with_worker:
        # A master that waits for its one worker before it starts the test.
        locust_cmd += ["--master", "--expect-workers", "1"]
        # Serve /loadgen-config. --config-json hands the worker its values
        # once, at start; a run whose values change (a load shape) needs the
        # endpoint as well.
        locust_cmd += ["--loadgen-config-port", str(LOADGEN_CONFIG_PORT)]
    locust_cmd += list(args.locust_extra)

    tee(logs, f"Running: {' '.join(locust_cmd)}")
    locust_proc = subprocess.Popen(
        locust_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        text=True,
    )

    pumps = [threading.Thread(
        target=pump_stream,
        args=("locust", locust_proc.stdout, logs, traces),
        daemon=True,
    )]

    worker_proc = None
    if with_worker:
        worker_cmd = worker_command(args)
        tee(logs, f"Running: {' '.join(worker_cmd)}")
        worker_proc = subprocess.Popen(
            worker_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
        )
        pumps.append(threading.Thread(
            target=pump_stream,
            args=("worker", worker_proc.stdout, logs, traces),
            daemon=True,
        ))

    for t in pumps:
        t.start()

    locust_exit = locust_proc.wait()
    tee(logs, f"Locust exited with code {locust_exit}")

    if worker_proc is not None:
        # The test window is over. On SIGTERM the worker suspends and deletes
        # every actor it created; give that time before killing it.
        tee(logs, "Stopping locust-worker...")
        worker_proc.send_signal(signal.SIGTERM)
        try:
            worker_proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            tee(logs, "locust-worker did not exit within 90s; killing it")
            worker_proc.kill()
            worker_proc.wait()
        tee(logs, f"locust-worker exited with code {worker_proc.returncode}")

    for t in pumps:
        t.join(timeout=5)

    return locust_exit


def stats_to_jsonl(stats_csv: Path, jsonl_path: Path, timestamp: str, tag: str, test_name: str) -> int:
    rows_written = 0
    with open(stats_csv) as f, open(jsonl_path, "w") as out:
        reader = csv.DictReader(f)
        for row in reader:
            type_val = row.pop("Type", "") or ""
            name_val = row.pop("Name", "") or ""
            if name_val == "Aggregated":
                continue
            measurements = {}
            for k, v in row.items():
                if k is None:
                    continue
                if k.endswith("%"):
                    # Percentile columns: "50%" -> "p50", "99.99%" -> "p99_99".
                    key = "p" + k[:-1].replace(".", "_")
                else:
                    # Keys stay lowercase alphanumerics and underscores.
                    key = k.lower().replace("/", "_per_")
                    key = re.sub(r"[^a-z0-9]+", "_", key).strip("_")
                measurements[key] = v
            entry = {
                "timestamp": timestamp,
                "tag": tag,
                "test_name": test_name,
                "metric": f"{type_val}_{name_val}",
                "measurements": measurements,
            }
            out.write(json.dumps(entry) + "\n")
            rows_written += 1
    return rows_written


def upload_to_gcs(local_path: Path, gcs_uri: str) -> None:
    # Imported lazily: a local destination needs no cloud client.
    from google.cloud import storage

    bucket_name, _, blob_path = gcs_uri[len("gs://"):].partition("/")
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_filename(
        str(local_path)
    )


def upload(src: Path, dest: str) -> None:
    if dest.startswith("gs://"):
        upload_to_gcs(src, dest)
    else:
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dest_path)


def collect_cluster_facts(
    args: argparse.Namespace, logs: TextIO
) -> dict[str, Any]:
    """Cluster facts, or empty facts when discovery is off."""
    if not args.cluster_facts:
        tee(logs, "Skipping cluster hardware discovery (--no-cluster-facts)")
        return dict(EMPTY_FACTS)
    return get_cluster_hardware_facts(logs)


def read_worker_pods(
    args: argparse.Namespace, logs: TextIO
) -> dict[str, dict[str, Any]] | None:
    """Worker pod state, or None when discovery is off or the read fails."""
    if not args.cluster_facts:
        return None
    try:
        return snapshot_worker_pods(logs)
    except Exception as e:
        tee(logs, f"Warning: Failed to read worker pods: {e}")
        return None


def read_evictions(
    args: argparse.Namespace, since_ts: int, logs: TextIO
) -> set[str] | None:
    """Pods evicted since since_ts, or None when discovery is off or fails."""
    if not args.cluster_facts:
        return None
    try:
        return read_evicted_pods(since_ts, logs)
    except Exception as e:
        tee(logs, f"Warning: Failed to read evictions: {e}")
        return None


def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    # For the local work directory name.
    path_ts = now.strftime("%Y%m%dT%H%M%SZ")
    # RFC 3339, for the JSONL timestamp column.
    data_ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Partition values of the result layout.
    run_date = now.strftime("%Y-%m-%d")
    run_ts = int(now.timestamp())

    work_dir = Path(f"/tmp/{path_ts}-locust-runner")
    work_dir.mkdir(parents=True, exist_ok=True)
    csv_prefix = work_dir / args.name
    stats_csv = work_dir / f"{args.name}_stats.csv"
    jsonl_path = work_dir / f"{args.name}.jsonl"
    logs_path = work_dir / f"{args.name}_logs.txt"
    traces_path = work_dir / f"{args.name}_traces.txt"
    status_path = work_dir / f"{args.name}_status.json"
    server_summary_json = work_dir / f"{args.name}_server_summary.json"

    prefix = (
        f"{args.dest.rstrip('/')}/runs/{args.name}"
        f"/run_date={run_date}/run_ts={run_ts}/run_tag={args.tag}"
    )

    with open(logs_path, "w") as logs, open(traces_path, "w") as traces:
        traces.write("\t".join(TRACE_COLUMNS) + "\n")
        traces.flush()
        log_run_config(args, prefix, work_dir, logs)
        # Worker pods before and after, for OOMKilled restarts and evictions.
        pods_before = read_worker_pods(args, logs)
        exit_code = run_test(args, csv_prefix, logs, traces)
        run_end_ts = int(datetime.now(timezone.utc).timestamp())
        pods_after = read_worker_pods(args, logs)
        # Read at once: Evicted events expire after about an hour.
        evicted_uids = read_evictions(args, run_ts, logs)

        stats_generated = False
        if stats_csv.exists():
            try:
                rows = stats_to_jsonl(
                    stats_csv, jsonl_path, data_ts, args.tag, args.name
                )
                if rows == 0:
                    tee(
                        logs,
                        f"Stats CSV {stats_csv} had no measurement rows; "
                        f"treating as not produced",
                    )
                    if jsonl_path.exists():
                        jsonl_path.unlink()
                else:
                    stats_generated = jsonl_path.exists()
            except Exception as e:
                tee(logs, f"Failed to generate JSONL from {stats_csv}: {e}")
                if jsonl_path.exists():
                    jsonl_path.unlink()
        else:
            tee(logs, f"Stats CSV {stats_csv} not produced; skipping JSONL")

        # Everything below only adds rows. Each step catches its own failure
        # so none of them can cost the locust measurements above.
        stats_history_csv = work_dir / f"{args.name}_stats_history.csv"
        # Empty facts first, so a failed discovery still leaves the frontier
        # summary something to read.
        facts = dict(EMPTY_FACTS)
        try:
            facts = collect_cluster_facts(args, logs)
        except Exception as e:
            tee(logs, f"Warning: Failed to read cluster facts: {e}")

        # The frontiers divide by user counts, which come from the CSV.
        if stats_generated:
            try:
                append_trial_summary(
                    jsonl_path,
                    stats_csv,
                    stats_history_csv,
                    args,
                    data_ts,
                    facts,
                    logs,
                    run_start=run_ts,
                    run_end=run_end_ts,
                )
            except Exception as e:
                tee(logs, f"Warning: Failed to record cluster facts: {e}")

        # Server telemetry is a query over the run's time window and needs no
        # CSV: a run too overloaded to write one is exactly the run whose
        # packing, PSI and snapshot figures matter.
        try:
            extract_and_record_server_telemetry(
                prom_url=args.prometheus_url,
                start_ts=run_ts,
                end_ts=run_end_ts,
                stats_history_csv=stats_history_csv,
                output_json_path=server_summary_json,
                jsonl_path=jsonl_path,
                data_ts=data_ts,
                tag=args.tag,
                test_name=args.name,
                logs=logs,
                lag_s=args.keeper_lag_s,
                worker_pods=diff_worker_pods(pods_before, pods_after, evicted_uids),
            )
        except Exception as e:
            tee(logs, f"Warning: Failed to harvest server telemetry: {e}")

        # Per-step rows of a ladder run. After the server telemetry, whose
        # wait gives Prometheus time to scrape the last step.
        try:
            n = append_ladder_steps(
                jsonl_path, logs_path, args.prometheus_url, data_ts,
                args.tag, args.name, logs,
            )
            if n:
                tee(logs, f"Appended {n} ladder step rows to {jsonl_path}")
        except Exception as e:
            tee(logs, f"Warning: Failed to summarize ladder steps: {e}")

        # The phase breakdown reads pod logs, so it runs now, before the
        # orchestrator deletes the pods. Its window reaches back past this
        # run's start by a minute of clock slack.
        if stats_generated and args.phase_breakdown:
            try:
                append_phase_breakdown(
                    jsonl_path,
                    stats_csv,
                    int(time.time() - now.timestamp()) + 60,
                    data_ts,
                    args.tag,
                    args.name,
                    logs,
                )
            except Exception as e:
                tee(logs, f"Warning: Failed to record phase breakdown: {e}")
        elif stats_generated:
            tee(logs, "Skipping phase breakdown (--no-phase-breakdown)")

    status_path.write_text(
        json.dumps(
            {"locust_exit_code": exit_code, "stats_generated": stats_generated}
        )
    )

    files: list[tuple[Path, str]] = [
        (status_path, "status.json"),
        (logs_path, "logs.txt"),
        (traces_path, "traces.txt"),
        (jsonl_path, "stats.jsonl"),
        (stats_csv, "stats.csv"),
        (work_dir / f"{args.name}_exceptions.csv", "exceptions.csv"),
        (work_dir / f"{args.name}_failures.csv", "failures.csv"),
        (work_dir / f"{args.name}_stats_history.csv", "stats_history.csv"),
        (server_summary_json, "server_summary.json"),
    ]
    for src, basename in files:
        if not src.exists():
            print(f"Skipping {src}: not produced", flush=True)
            continue
        dest = f"{prefix}/{basename}"
        upload(src, dest)
        print(f"Uploaded {src} -> {dest}", flush=True)

    if not stats_generated and not args.allow_empty_stats:
        sys.exit(1)


if __name__ == "__main__":
    main()
