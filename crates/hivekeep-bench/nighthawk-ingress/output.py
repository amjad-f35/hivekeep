"""Turns Nighthawk's AdaptiveLoadSessionOutput into the run's artifacts:

  results.json   parse_output_textproto(): the session as JSON, field for
                 field.
  stats.jsonl    stage_rows() then stats_records(): one record per stage,
                 shaped {timestamp, tag, test_name, metric, measurements}
                 like the locust runner's.
  capacity.json  capacity_summary(): the one-record verdict.

Measurement keys come in three kinds: metric_nighthawk.builtin_* (Nighthawk's
built-in metrics as reported), plain counters and *_ms conversions, and the
derived verdicts (slo_max_rps, failed_thresholds, binding_threshold; the last
two name thresholds by their short metric names).
"""

import json
import re

# The statistic holding client-measured end-to-end request latency.
LATENCY_STAT_ID = "benchmark_http_client.request_to_response"

# The first line of a textproto field: `name {` or `name: value`.
_FIELD_LINE = re.compile(r"^\s*[A-Za-z_][A-Za-z0-9_]*\s*[:{]")

PERCENTILE_TARGETS = {"p50": 0.5, "p90": 0.9, "p95": 0.95, "p99": 0.99}

COUNTERS_OF_INTEREST = (
    "benchmark.http_2xx",
    "benchmark.http_3xx",
    "benchmark.http_4xx",
    "benchmark.http_5xx",
    "benchmark.pool_overflow",
    "benchmark.pool_connection_failure",
    "benchmark.stream_resets",
    "upstream_rq_total",
)

# How Nighthawk defines its built-in metrics:
#   attempted-rps = specified / duration
#   achieved-rps  = sent / duration   (errors included, not successes)
#   send-rate     = sent / specified
#   success-rate  = http_2xx / sent
BUILTIN_PREFIX = "metric_nighthawk.builtin_"


def _builtin_key(metric_name: str) -> str:
    """'attempted-rps' -> 'metric_nighthawk.builtin_attempted_rps'."""
    return BUILTIN_PREFIX + metric_name.replace("-", "_")


def parse_output_textproto(text: str, desc_path: str) -> dict:
    """The output file as a dict. Only this function needs protobuf; the rest
    of the module works on plain dicts, so its tests run without the runner
    image's dependencies."""
    from google.protobuf import json_format
    from google.protobuf import text_format

    import spec as spec_mod

    # The client writes marker lines before the message; skip to the first
    # field.
    lines = text.splitlines()
    start = 0
    while start < len(lines) and not _FIELD_LINE.match(lines[start]):
        start += 1
    pool = spec_mod.load_pool(desc_path)
    msg = spec_mod.message_class(pool, spec_mod.OUTPUT_MESSAGE)()
    text_format.Parse("\n".join(lines[start:]), msg, descriptor_pool=pool)
    return json_format.MessageToDict(msg, preserving_proto_field_name=True, descriptor_pool=pool)


def _duration_seconds(value) -> float:
    """json_format writes a Duration as e.g. '30.000001s'."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value).rstrip("s") or 0)


def _global_result(nighthawk_output: dict) -> dict:
    results = nighthawk_output.get("results", [])
    for r in results:
        if r.get("name") == "global":
            return r
    return results[0] if results else {}


def _counters(result: dict) -> dict:
    return {c.get("name", ""): int(c.get("value", 0)) for c in result.get("counters", [])}


def _latency_ms(result: dict) -> dict:
    """Mean, min, max and percentile latencies, in ms, of the end-to-end
    statistic."""
    out: dict = {}
    for stat in result.get("statistics", []):
        if stat.get("id") != LATENCY_STAT_ID:
            continue
        out["latency_mean_ms"] = _duration_seconds(stat.get("mean")) * 1000.0
        out["latency_min_ms"] = _duration_seconds(stat.get("min")) * 1000.0
        out["latency_max_ms"] = _duration_seconds(stat.get("max")) * 1000.0
        percentiles = stat.get("percentiles", [])
        for label, target in PERCENTILE_TARGETS.items():
            # The histogram's points fall unevenly: take the first at or
            # above the target.
            for p in percentiles:
                if float(p.get("percentile", 0)) >= target:
                    out[f"{label}_ms"] = _duration_seconds(p.get("duration")) * 1000.0
                    break
        break
    return out


def _stage_row(stage: str, benchmark_result: dict) -> dict:
    nh_output = benchmark_result.get("nighthawk_service_output", {})
    options = nh_output.get("options", {})
    result = _global_result(nh_output)
    counters = _counters(result)
    duration_s = _duration_seconds(result.get("execution_duration"))
    per_worker_rps = int(options.get("requests_per_second", 0))
    concurrency = int(options.get("concurrency", "1") or "1")

    row = {
        "stage": stage,
        "start_time": benchmark_result.get("start_time", ""),
        "end_time": benchmark_result.get("end_time", ""),
        "duration_s": duration_s,
        "per_worker_rps": per_worker_rps,
        "concurrency": concurrency,
    }
    # threshold_score is +1 for a pass, -1 for a violation, and absent for an
    # informational metric.
    failed = []
    for ev in benchmark_result.get("metric_evaluations", []):
        name = ev.get("metric_id", "").split("/")[-1]
        if name:
            row[_builtin_key(name)] = ev.get("metric_value", 0.0)
        if float(ev.get("threshold_score", 0)) < 0:
            failed.append(name)
    # Where an evaluation is missing, compute it as Nighthawk would.
    sent = counters.get("upstream_rq_total", 0)
    http_2xx = counters.get("benchmark.http_2xx", 0)
    attempted, achieved = _builtin_key("attempted-rps"), _builtin_key("achieved-rps")
    row.setdefault(attempted, per_worker_rps * concurrency)
    row.setdefault(achieved, (sent / duration_s) if duration_s else 0.0)
    row.setdefault(_builtin_key("send-rate"), (row[achieved] / row[attempted]) if row[attempted] else 0.0)
    row.setdefault(_builtin_key("success-rate"), (http_2xx / sent) if sent else 0.0)
    for name in COUNTERS_OF_INTEREST:
        row[name.replace("benchmark.", "")] = counters.get(name, 0)
    row.update(_latency_ms(result))
    row["failed_thresholds"] = sorted(failed)
    return row


def stage_rows(output_dict: dict) -> list[dict]:
    rows = [
        _stage_row(f"adjusting_{i:03d}", br)
        for i, br in enumerate(output_dict.get("adjusting_stage_results", []))
    ]
    testing = output_dict.get("testing_stage_result")
    if testing:
        rows.append(_stage_row("testing", testing))
    return rows


def stats_records(rows: list[dict], tag: str, test_name: str) -> list[dict]:
    """One stats.jsonl record per stage, in the locust runner's shape."""
    return [
        {
            "timestamp": row.get("start_time", ""),
            "tag": tag,
            "test_name": test_name,
            "metric": f"nighthawk_{row['stage']}",
            "measurements": {k: v for k, v in row.items() if k not in ("stage", "start_time")},
        }
        for row in rows
    ]


def session_succeeded(output_dict: dict) -> bool:
    """session_status is a google.rpc.Status: no code, or 0, is OK."""
    return int(output_dict.get("session_status", {}).get("code", 0)) == 0


def capacity_summary(
    output_dict: dict,
    *,
    envoy_cpu: int,
    actors: int,
    client_concurrency: int | None = None,
    tail_latency_slo_ms: float | None = None,
) -> dict:
    rows = stage_rows(output_dict)
    testing = next((r for r in rows if r["stage"] == "testing"), None)
    binding = sorted({name for r in rows for name in r.get("failed_thresholds", [])})
    # The search converges on the failure boundary, so the testing stage may
    # be marginal; the clean stage with the highest attempted rate is the
    # conservative answer.
    clean = [r for r in rows if not r.get("failed_thresholds")]
    best_clean = max(clean, key=lambda r: r[_builtin_key("attempted-rps")], default=None)
    summary = {
        "envoy_cpu": envoy_cpu,
        "actors": actors,
        "client_concurrency": client_concurrency,
        "tail_latency_slo_ms": tail_latency_slo_ms,
        "binding_threshold": binding,
        # A clean stage's achieved-rps is within successRateThreshold of its
        # successful throughput, so no separate 2xx/s figure is kept.
        "slo_max_rps": best_clean[_builtin_key("achieved-rps")] if best_clean else None,
        "slo_max_stage": best_clean["stage"] if best_clean else None,
        "converged": session_succeeded(output_dict) and testing is not None,
        "adjusting_stages": sum(1 for r in rows if r["stage"] != "testing"),
    }
    if testing:
        for name in ("attempted-rps", "achieved-rps", "send-rate", "success-rate"):
            summary[_builtin_key(name)] = testing.get(_builtin_key(name))
        for name in ("p50_ms", "p95_ms", "p99_ms"):
            summary[name] = testing.get(name)
    return summary


def write_jsonl(records: list[dict], path) -> None:
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
