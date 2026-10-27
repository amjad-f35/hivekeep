"""One stats.jsonl row per step of a ladder run (shapes/ladder_shape.py).

The other rows of stats.jsonl span the whole run and so mix all the steps.
This module finds each step's start and end in the runner's log and queries
Prometheus over the step's steady part: from SETTLE seconds in (the worker
may take up to 10 s to pick up a new target rate) to TAIL seconds before
the end. Each step becomes a `ladder_step_NNN` row.

Like the server telemetry it only adds rows: runner.py logs a failure here
and keeps the locust measurements.
"""

import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, TextIO

# Seconds trimmed from each end of a step.
SETTLE = 30
TAIL = 5

STEP_RE = re.compile(
    r"\[locust\] \[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+\].*"
    r"Ladder step (\d+)/(\d+): users=(\d+).*target_rps=([\d.]+|unchanged)"
)
DONE_RE = re.compile(r"\[locust\] \[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d),\d+\].*Ladder complete")


def _ts(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp())


def steps(lines: Iterable[str]) -> list[dict[str, Any]]:
    """The ladder's steps from runner log lines, each with its start and end
    as Unix seconds. A step ends where the next starts; the last ends at
    "Ladder complete", or is None if the log has no such line. A step logged
    with target_rps=unchanged keeps the previous step's rate, None before
    any step sets one."""
    out: list[dict[str, Any]] = []
    done = None
    for line in lines:
        if m := STEP_RE.search(line):
            rps = out[-1]["target_rps"] if out else None
            if m[5] != "unchanged":
                rps = float(m[5])
            out.append({"step": int(m[2]), "users": int(m[4]),
                        "target_rps": rps, "start": _ts(m[1])})
        elif m := DONE_RE.search(line):
            done = _ts(m[1])
    for i, s in enumerate(out):
        s["end"] = out[i + 1]["start"] if i + 1 < len(out) else done
    return out


# Seconds a query may take, so an unreachable Prometheus fails the summary
# instead of hanging the runner.
QUERY_TIMEOUT_S = 30


def _get_json(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=QUERY_TIMEOUT_S) as r:
        return json.load(r)


# Unlike server_telemetry.py's helper, a failed query raises here: a step's
# row must fail rather than carry a gap.
def query(prom: str, q: str, t: int) -> list[dict[str, Any]]:
    url = f"{prom.rstrip('/')}/api/v1/query?" + urllib.parse.urlencode({"query": q, "time": t})
    data = _get_json(url)
    if data.get("status") != "success":
        raise RuntimeError(f"{q}: {data}")
    return data["data"]["result"]


def scalar(prom: str, q: str, t: int) -> float | None:
    """The query's first value, or None if it has none or it is NaN."""
    r = query(prom, q, t)
    if not r:
        return None
    v = float(r[0]["value"][1])
    return None if v != v else v


def by_label(prom: str, q: str, t: int, label: str) -> dict[str, float]:
    return {r["metric"].get(label, ""): float(r["value"][1]) for r in query(prom, q, t)}


# summarize() and memory() carry the cold-resume ladder's own queries (its
# request names, the control-plane containers and the database). A ladder
# measuring something else needs its own pair.
def summarize(prom: str, s: dict[str, Any]) -> dict[str, Any]:
    """One step's throughput, latency, CPU and Postgres figures over its
    steady window."""
    a, b = s["start"] + SETTLE, s["end"] - TAIL
    secs = b - a
    d = f"{secs}s"
    req = 'locust_requests_total{name="%s",status="%s"}'
    hist = 'histogram_quantile(%s, sum by (le) (increase(%s_bucket{name="%s"%s}[%s])))'
    mean = ('sum(increase(%s_sum{name="%s"%s}[%s]))'
            '/sum(increase(%s_count{name="%s"%s}[%s]))')
    cpu = 'sum(increase(container_cpu_usage_seconds_total{namespace="hivekeep-system",container="%s"}[%s]))/%d'
    thr = ('sum(increase(container_cpu_cfs_throttled_periods_total{namespace="hivekeep-system",container="hivekeep-api"}[%s]))'
           '/sum(increase(container_cpu_cfs_periods_total{namespace="hivekeep-system",container="hivekeep-api"}[%s]))')
    pg_conns = 'avg_over_time(sum(pg_stat_activity_count{datname="hivekeeppg"%s})[%s:15s])'
    client, server = "locust_request_duration_milliseconds", "locust_server_duration_milliseconds"
    ok = ',status="success"'

    def q(expr: str) -> float | None:
        return scalar(prom, expr, b)

    def latency(metric: str, name: str, extra: str, quantile: float) -> float | None:
        return q(hist % (quantile, metric, name, extra, d))

    def mean_ms(metric: str, name: str, extra: str) -> float | None:
        return q(mean % (metric, name, extra, d, metric, name, extra, d))

    per_replica = by_label(
        prom,
        f'sum by (pod) (increase(container_cpu_usage_seconds_total{{namespace="hivekeep-system",container="hivekeep-api"}}[{d}]))/{secs}',
        b, "pod")
    out = {
        "step": s["step"],
        "users": s["users"],
        "target_rps": s["target_rps"],
        "window": [a, b],
        "resume_rps": q(f"sum(increase({req % ('ResumeActorCold', 'success')}[{d}]))/{secs}"),
        "pause_rps": q(f"sum(increase({req % ('SuspendActorAsync', 'success')}[{d}]))/{secs}"),
        "failures": by_label(prom, f'sum by (name) (increase(locust_requests_total{{status="failure"}}[{d}]))', b, "name"),
        "resume_client_p50_ms": latency(client, "ResumeActorCold", ok, 0.5),
        "resume_client_p99_ms": latency(client, "ResumeActorCold", ok, 0.99),
        "resume_server_p50_ms": latency(server, "ResumeActorCold", "", 0.5),
        "resume_server_p90_ms": latency(server, "ResumeActorCold", "", 0.9),
        "resume_server_p95_ms": latency(server, "ResumeActorCold", "", 0.95),
        "resume_server_p99_ms": latency(server, "ResumeActorCold", "", 0.99),
        "pause_client_p50_ms": latency(client, "SuspendActorAsync", ok, 0.5),
        "pause_client_p99_ms": latency(client, "SuspendActorAsync", ok, 0.99),
        "resume_client_mean_ms": mean_ms(client, "ResumeActorCold", ok),
        "resume_server_mean_ms": mean_ms(server, "ResumeActorCold", ""),
        "pause_client_mean_ms": mean_ms(client, "SuspendActorAsync", ok),
        "pause_server_mean_ms": mean_ms(server, "SuspendActorAsync", ""),
        "api_cores": q(cpu % ("hivekeep-api", d, secs)),
        # Each replica's cores, busiest first.
        "api_cores_per_replica": sorted(per_replica.values(), reverse=True),
        "api_throttled_ratio": q(thr % (d, d)),
        "postgres_cores": q(cpu % ("postgres", d, secs)),
        # On the fake data plane this is the fake keeper, which runs in the
        # keeper DaemonSet under the same container name.
        "keeper_cores": q(cpu % ("keeper", d, secs)),
        "pg_active_conns": q(pg_conns % (',state="active"', d)),
        "pg_conns": q(pg_conns % ("", d)),
        "pg_commits_per_s": q(f'sum(increase(pg_stat_database_xact_commit{{datname="hivekeeppg"}}[{d}]))/{secs}'),
        "pg_lock_wait_conns": q(pg_conns % (',state="active",wait_event_type="Lock"', d)),
        "pg_walwrite_wait_conns": q(pg_conns % (',state="active",wait_event="WALWrite"', d)),
    }
    out.update(memory(prom, d, secs, b))
    return out


def memory(prom: str, d: str, secs: int, b: int) -> dict[str, Any]:
    """Memory over the step's window: peak working set and RSS in MiB, the
    share of time stalled on memory (PSI), and Postgres's buffer cache hits."""
    ws = 'container_memory_working_set_bytes{namespace="hivekeep-system",container="%s"}'
    rss = 'container_memory_rss{namespace="hivekeep-system",container="%s"}'
    psi = 'rate(container_pressure_memory_waiting_seconds_total{namespace="hivekeep-system",container="%s"}[%s])'
    blks = 'sum(increase(pg_stat_database_blks_%s{datname="hivekeeppg"}[%s]))'

    def mib(q: str) -> float | None:
        v = scalar(prom, q, b)
        return None if v is None else v / (1024 * 1024)

    hit, read = scalar(prom, blks % ("hit", d), b), scalar(prom, blks % ("read", d), b)
    return {
        # The largest replica's peak, and all replicas' mean together.
        "api_mem_ws_max_mib": mib(f'max(max_over_time({ws % "hivekeep-api"}[{d}]))'),
        "api_mem_ws_total_mib": mib(f'sum(avg_over_time({ws % "hivekeep-api"}[{d}]))'),
        "api_mem_rss_max_mib": mib(f'max(max_over_time({rss % "hivekeep-api"}[{d}]))'),
        "api_mem_psi": scalar(prom, f'max({psi % ("hivekeep-api", d)})', b),
        "postgres_mem_ws_mib": mib(f'max(max_over_time({ws % "postgres"}[{d}]))'),
        "postgres_mem_rss_mib": mib(f'max(max_over_time({rss % "postgres"}[{d}]))'),
        "postgres_mem_psi": scalar(prom, f'max({psi % ("postgres", d)})', b),
        "pg_cache_hit_ratio": None if hit is None or read is None or hit + read == 0 else hit / (hit + read),
        "pg_blks_read_per_s": None if read is None else read / secs,
    }


def stats_record(summary: dict[str, Any], timestamp: str, tag: str,
                 test_name: str) -> dict[str, Any]:
    """A step's summary as a stats.jsonl row, its measurements flattened to
    scalars like every other row's."""
    nested = ("step", "window", "failures", "api_cores_per_replica")
    m = {k: v for k, v in summary.items() if k not in nested}
    m["window_start"], m["window_end"] = summary["window"]
    m["api_cores_busiest_replica"] = max(summary["api_cores_per_replica"], default=None)
    for name, n in summary["failures"].items():
        m[f"failures_{name}"] = n
    return {
        "timestamp": timestamp,
        "tag": tag,
        "test_name": test_name,
        "metric": f"ladder_step_{summary['step']:03d}",
        "measurements": m,
    }


def append_ladder_steps(jsonl_path: Path, log_path: Path, prom_url: str,
                        timestamp: str, tag: str, test_name: str,
                        logs: TextIO) -> int:
    """Append a row per measurable ladder step in the runner log at
    log_path to jsonl_path, and return how many. A run that is no ladder
    has none. Every step is queried before any row is written, so a failed
    query leaves no partial ladder."""
    with open(log_path, errors="replace") as f:
        ladder = steps(f)
    rows = []
    for s in ladder:
        if s["end"] is None:
            _log(logs, f"Ladder step {s['step']} has no end in the log; skipped")
            continue
        if s["end"] - s["start"] <= SETTLE + TAIL:
            _log(logs, f"Ladder step {s['step']} lasts {s['end'] - s['start']}s, too short "
                       f"for a steady window after trimming {SETTLE + TAIL}s; skipped")
            continue
        rows.append(stats_record(summarize(prom_url, s), timestamp, tag, test_name))
    if rows:
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
    return len(rows)


def _log(logs: TextIO, msg: str) -> None:
    print(msg, flush=True)
    logs.write(msg + "\n")
    logs.flush()
