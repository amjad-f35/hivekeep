"""Turns the suspend/resume timing-breakdown log records into a report.

Input is the keeper and worker pod logs, either as `kubectl logs` dumps (one
JSON object per line; anything else is skipped) or as a Cloud Logging export
(`gcloud logging read --format json`: one JSON array of entries, each record
under jsonPayload). Two record kinds are read, each written by both layers:

  "Restore timing breakdown"     keeper (hk.actor.restore.duration.<phase>)
                                 worker (worker.actor.restore.duration.<phase>)
  "Checkpoint timing breakdown"  keeper (hk.actor.checkpoint.duration.<phase>)
                                 worker (worker.actor.checkpoint.duration.<phase>)

A record may be flat (the message under `msg` or `message`, the fields beside
it) or as the tracing JSON formatter writes it: the message and a JSON-encoded
`timing` or `record` field under `fields`, the time under `timestamp`.

The report shows where SuspendActor and ResumeActor time goes: per-phase
percentiles for each layer, and the slowest operations as waterfalls with the
worker record nested under the keeper record of the same operation.

Usage:
    phase_report.py logs/*.log [--csv DEST_DIR] [--slowest N]

Give it one source per run, the dumps or an export, never both: the same
record would count twice.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

KEEPER = "keeper"
WORKER = "worker"

# Duration-key prefixes per (layer, operation).
BREAKDOWN_PREFIXES = {
    (KEEPER, "restore"): "hk.actor.restore.duration.",
    (KEEPER, "checkpoint"): "hk.actor.checkpoint.duration.",
    (WORKER, "restore"): "worker.actor.restore.duration.",
    (WORKER, "checkpoint"): "worker.actor.checkpoint.duration.",
}
BREAKDOWN_MSGS = {"Restore timing breakdown", "Checkpoint timing breakdown"}

# Where the tracing JSON formatter puts the record.
RECORD_FIELDS = ("timing", "record")

ACTOR_UID_KEY = "hk.actor.uid"
SCOPE_KEY = "hk.snapshot.scope"
KIND_KEY = "hk.snapshot.kind"
SANDBOX_CLASS_KEY = "hk.sandbox.class"
TEMPLATE_KEY = "hk.template.name"
ERROR_TYPE_KEY = "error.type"

# Worker records carry neither the sandbox class nor the snapshot kind: both
# sandbox runtimes write the same messages, and the keeper record of the same
# operation says which it was. join_layers copies them across.
MICROVM = "microvm"
GVISOR = "gvisor"

# Every record reports TOTAL; the report derives UNATTRIBUTED, the part of the
# total no logged phase accounts for.
TOTAL = "total"
UNATTRIBUTED = "unattributed"

# Display order of the known phases. Absent phases do not print; unknown ones
# print after these, by name.
PHASE_ORDER = [
    # keeper restore
    "volume_mount", "manifest_fetch", "sandbox_assets", "download", "oci_unpack", "worker_restore",
    # worker restore, micro-VM and gVisor, each in its own sequence
    "prep", "egress_prepare", "net_setup", "bundles", "upper_join", "lowers",
    "sandbox_create", "sandbox_restore", "app_rootfs", "app_create", "app_restore",
    "tap", "vmm_launch", "vm_restore", "resume", "wakeup_probe", "activate",
    # keeper and worker checkpoint
    "pause", "checkpoint", "snapshot", "durable_dir", "rootfs_upper",
    "fs_checkpoint", "worker_checkpoint", "teardown", "persist", "cleanup",
    UNATTRIBUTED, TOTAL,
]
_PHASE_RANK = {name: i for i, name in enumerate(PHASE_ORDER)}

# The keeper phase that wraps the worker's call.
INNER_PHASE = {"restore": "worker_restore", "checkpoint": "worker_checkpoint"}

# The micro-VM checkpoint captures run concurrently on the paused guest, so
# the paused window costs their maximum, not their sum. gVisor's checkpoint
# phases are sequential.
CONCURRENT_CAPTURES = ("snapshot", "durable_dir", "rootfs_upper")

# Slack on the window the worker record of an operation must fall in: the
# keeper does a little untimed work between the worker call and its own
# record.
JOIN_SLACK_S = 1.0


@dataclass
class Breakdown:
    """One parsed timing-breakdown record."""
    source: str  # keeper | worker
    op: str  # restore | checkpoint
    time: str
    ts: float | None  # epoch seconds, None when the time does not parse
    actor_uid: str
    template: str
    sandbox_class: str
    scope: str
    kind: str
    phases: dict[str, float]  # phase -> seconds
    failed: bool


@dataclass
class Parsed:
    breakdowns: list[Breakdown] = field(default_factory=list)
    lines_seen: int = 0
    lines_matched: int = 0
    bad_values: int = 0  # duration fields whose value was not a number


_TIME_RE = re.compile(r"^(.*T\d\d:\d\d:\d\d)(\.\d+)?(Z|[+-]\d\d:?\d\d)?$")


def parse_time(s: str) -> float | None:
    """RFC 3339 to epoch seconds. Fractions beyond microseconds (loggers
    write nanoseconds) are cut, since fromisoformat rejects them. None when
    the time does not parse: the record still counts, it just cannot be
    paired by time."""
    m = _TIME_RE.match(s.strip()) if s else None
    if not m:
        return None
    base, frac, tz = m.groups()
    frac = (frac or "")[:7]
    tz = "+00:00" if tz in (None, "Z") else tz
    try:
        return datetime.fromisoformat(f"{base}{frac}{tz}").timestamp()
    except ValueError:
        return None


def unattributed(source: str, op: str, sandbox_class: str, phases: dict[str, float]) -> float | None:
    """The total minus what the logged phases account for, where that is
    well defined: the keeper checkpoint phases are sequential; a micro-VM
    worker checkpoint is prep, pause, the concurrent captures (their max)
    and teardown; a gVisor worker checkpoint is all sequential. The keeper
    restore phases overlap on purpose (the download runs beside the asset
    fetch and the unpack), and both runtimes' restore phases add up to their
    total by construction, so restores get no residual; nor does a worker
    checkpoint whose runtime is unknown. A negative result means a bug in
    the emitter or here, and is left negative so it shows."""
    total = phases.get(TOTAL)
    if total is None:
        return None
    if (source, op) == (KEEPER, "checkpoint"):
        spent = sum(phases.get(p, 0.0) for p in ("sandbox_assets", "worker_checkpoint", "persist", "cleanup"))
    elif (source, op, sandbox_class) == (WORKER, "checkpoint", MICROVM):
        spent = (phases.get("prep", 0.0) + phases.get("pause", 0.0)
                 + max(phases.get(p, 0.0) for p in CONCURRENT_CAPTURES)
                 + phases.get("teardown", 0.0))
    elif (source, op, sandbox_class) == (WORKER, "checkpoint", GVISOR):
        spent = sum(v for k, v in phases.items() if k != TOTAL)
    else:
        return None
    return total - spent


def normalize(obj: dict) -> dict | None:
    """The flat record (message under `msg`, fields beside it, time under
    `time`) inside any of the shapes the logs come in, or None when obj is
    not a timing breakdown."""
    # A Cloud Logging entry nests the logged object under jsonPayload and
    # moves its time to the entry's timestamp.
    payload = obj.get("jsonPayload")
    if isinstance(payload, dict):
        entry, obj = obj, dict(payload)
        obj.setdefault("time", entry.get("timestamp", ""))
    fields = obj.get("fields")
    if isinstance(fields, dict) and fields.get("message") in BREAKDOWN_MSGS:
        # The tracing JSON formatter: the record is a JSON string field.
        rec: dict = {}
        for name in RECORD_FIELDS:
            raw = fields.get(name)
            if isinstance(raw, str):
                try:
                    decoded = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(decoded, dict):
                    rec = decoded
                    break
            elif isinstance(raw, dict):
                rec = raw
                break
        return {**rec, "msg": fields["message"], "time": obj.get("timestamp", obj.get("time", ""))}
    msg = obj.get("msg", obj.get("message", ""))
    if msg not in BREAKDOWN_MSGS:
        return None
    flat = dict(obj)
    flat["msg"] = msg
    flat.setdefault("time", obj.get("timestamp", ""))
    return flat


def parse_line(obj: dict, out: Parsed) -> None:
    rec = normalize(obj)
    if rec is None:
        return
    for (source, op), prefix in BREAKDOWN_PREFIXES.items():
        phases: dict[str, float] = {}
        for k, v in rec.items():
            if not k.startswith(prefix):
                continue
            try:
                phases[k[len(prefix):]] = float(v)
            except (TypeError, ValueError):
                # One bad value costs that phase, not the report.
                out.bad_values += 1
        if not phases:
            continue
        time_s = rec.get("time", "")
        out.breakdowns.append(Breakdown(
            source=source,
            op=op,
            time=time_s,
            ts=parse_time(time_s),
            actor_uid=rec.get(ACTOR_UID_KEY, ""),
            template=rec.get(TEMPLATE_KEY, ""),
            sandbox_class=rec.get(SANDBOX_CLASS_KEY, ""),
            scope=rec.get(SCOPE_KEY, ""),
            kind=rec.get(KIND_KEY, ""),
            phases=phases,
            failed=ERROR_TYPE_KEY in rec,
        ))
        out.lines_matched += 1
        return


def parse_lines(lines: Iterable[str], out: Parsed | None = None) -> Parsed:
    """Parses one pod log line by line into out (a new Parsed by default).
    Also the entry point for callers already holding the log text, such as
    the locust runner reading pods through the Kubernetes API."""
    out = Parsed() if out is None else out
    for line in lines:
        # Dumps may prefix each line (pod name, timestamp); the record starts
        # at the first brace.
        brace = line.find("{")
        if brace < 0:
            continue
        out.lines_seen += 1
        try:
            obj = json.loads(line[brace:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            parse_line(obj, out)
    return out


def parse_files(paths: list[str]) -> Parsed:
    out = Parsed()
    for path in paths:
        f = sys.stdin if path == "-" else open(path, encoding="utf-8", errors="replace")
        with f:
            text = f.read()
        if text.lstrip().startswith("["):
            # A Cloud Logging export is one pretty-printed array, which cannot
            # be read line by line.
            try:
                entries = json.loads(text)
            except json.JSONDecodeError as e:
                # A dump whose lines start with a bracket, or a truncated
                # export: say so and read it line by line rather than drop it.
                print(f"warn: {path}: starts with '[' but is not a JSON array ({e}); "
                      f"reading it line by line", file=sys.stderr)
            else:
                if isinstance(entries, list):
                    for obj in entries:
                        if isinstance(obj, dict):
                            out.lines_seen += 1
                            parse_line(obj, out)
                    continue
        parse_lines(text.splitlines(), out)
    return out


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1]


def phase_sort_key(name: str) -> tuple[int, str]:
    return (_PHASE_RANK.get(name, len(PHASE_ORDER)), name)


def fmt_s(seconds: float) -> str:
    return f"{seconds * 1000:8.1f}"


def report_phases(breakdowns: list[Breakdown], writer) -> list[dict]:
    """Percentiles per (layer, op, sandbox class, kind, scope, phase). A local
    and a latest restore, or a gVisor and a micro-VM checkpoint, are
    different distributions and are never pooled. Returns the CSV rows."""
    groups: dict[tuple, list[float]] = defaultdict(list)
    for b in breakdowns:
        if b.failed:
            continue
        for name, seconds in b.phases.items():
            groups[(b.source, b.op, b.sandbox_class or "-", b.kind or "-", b.scope or "-", name)].append(seconds)

    rows = []
    writer("== Phase percentiles (ms) ==")
    writer(f"{'layer':7} {'op':11} {'class':8} {'kind':7} {'scope':15} {'phase':16} {'n':>5} "
           f"{'p50':>8} {'p90':>8} {'p95':>8} {'max':>8}")
    for key in sorted(groups, key=lambda k: (k[:5], phase_sort_key(k[5]))):
        vals = sorted(groups[key])
        source, op, sandbox_class, kind, scope, name = key
        p50, p90, p95 = (percentile(vals, q) for q in (50, 90, 95))
        rows.append({
            "layer": source, "op": op, "class": sandbox_class, "kind": kind, "scope": scope, "phase": name,
            "count": len(vals),
            "p50_ms": p50 * 1000, "p90_ms": p90 * 1000, "p95_ms": p95 * 1000, "max_ms": max(vals) * 1000,
        })
        writer(f"{source:7} {op:11} {sandbox_class:8} {kind:7} {scope:15} {name:16} {len(vals):5d} "
               f"{fmt_s(p50)} {fmt_s(p90)} {fmt_s(p95)} {fmt_s(max(vals))}")
    failed = sum(1 for b in breakdowns if b.failed)
    if failed:
        writer(f"(excluded {failed} failed operation records)")
    return rows


def worker_window(op: Breakdown) -> tuple[float, float] | None:
    """When, around the keeper record's time, the worker record of the same
    operation was written. The worker writes it as its call returns, at the
    end of the keeper's worker_* phase: for a checkpoint that is before
    persist and cleanup, for a restore it is the last phase. An operation
    that never reached the worker has no window."""
    total = op.phases.get(TOTAL)
    if op.ts is None or total is None or INNER_PHASE[op.op] not in op.phases:
        return None
    if op.op == "checkpoint":
        after = op.phases.get("persist", 0.0) + op.phases.get("cleanup", 0.0)
        return (op.ts - total - JOIN_SLACK_S, op.ts - after + JOIN_SLACK_S)
    return (op.ts - op.phases[INNER_PHASE[op.op]] - JOIN_SLACK_S, op.ts + JOIN_SLACK_S)


def pair_records(breakdowns: list[Breakdown]) -> dict[int, Breakdown]:
    """Pairs each keeper record with the worker record of the same operation:
    same Actor and op, written inside the keeper's worker_* window, and not
    claimed by another operation. Returns id(keeper record) -> worker record.

    One cycle writes one of each. A worker record outside the window belongs
    to another cycle whose keeper record is missing (pod gone, log rotated);
    claiming it would show a gap that never happened, so such an operation
    prints unpaired. Pairing in time order with a consumed set keeps fast
    cycles of one Actor from sharing or swapping records. A paired worker
    record takes the snapshot kind and sandbox class its layer does not log,
    so both layers split alike and the residual uses the right runtime."""
    by_actor: dict[tuple, list[Breakdown]] = defaultdict(list)
    for b in breakdowns:
        if b.source == WORKER and b.ts is not None:
            by_actor[(b.actor_uid, b.op)].append(b)
    for v in by_actor.values():
        v.sort(key=lambda b: b.ts)

    pairs: dict[int, Breakdown] = {}
    consumed: set[int] = set()
    keeper = sorted((b for b in breakdowns if b.source == KEEPER and b.ts is not None), key=lambda b: b.ts)
    for op in keeper:
        window = worker_window(op)
        if window is None:
            continue
        lo, hi = window
        candidates = [w for w in by_actor.get((op.actor_uid, op.op), [])
                      if id(w) not in consumed and lo <= w.ts <= hi]
        if not candidates:
            continue
        inner = candidates[-1]
        consumed.add(id(inner))
        pairs[id(op)] = inner
        inner.kind = inner.kind or op.kind
        inner.sandbox_class = inner.sandbox_class or op.sandbox_class
    return pairs


def join_layers(breakdowns: list[Breakdown]) -> dict[int, Breakdown]:
    """Pairs the layers, then adds each record's unattributed residual, which
    for a worker checkpoint depends on the runtime the pairing found."""
    pairs = pair_records(breakdowns)
    for b in breakdowns:
        residual = unattributed(b.source, b.op, b.sandbox_class, b.phases)
        if residual is not None:
            b.phases[UNATTRIBUTED] = residual
    return pairs


def report_waterfalls(breakdowns: list[Breakdown], writer, slowest: int,
                      pairs: dict[int, Breakdown] | None = None) -> None:
    """The slowest operations, the worker record nested under the keeper's
    worker_* phase with that phase's gap to the worker total (the call and
    queueing between the layers)."""
    if pairs is None:
        pairs = join_layers(breakdowns)
    keeper = [b for b in breakdowns if b.source == KEEPER and not b.failed]
    keeper.sort(key=lambda b: b.phases.get(TOTAL, 0), reverse=True)

    writer("")
    writer(f"== Slowest {slowest} operations (waterfall, ms) ==")
    for b in keeper[:slowest]:
        inner = pairs.get(id(b))
        total = b.phases.get(TOTAL, 0)
        writer(f"{b.op} actor={b.actor_uid} template={b.template} "
               f"class={b.sandbox_class or '-'} scope={b.scope or '-'} kind={b.kind or '-'} "
               f"total={total * 1000:.1f}")
        for name in sorted(b.phases, key=phase_sort_key):
            if name == TOTAL:
                continue
            writer(f"  keeper {name:15} {fmt_s(b.phases[name])}")
            if name == INNER_PHASE.get(b.op) and inner:
                for iname in sorted(inner.phases, key=phase_sort_key):
                    if iname != TOTAL:
                        writer(f"    worker {iname:13} {fmt_s(inner.phases[iname])}")
                if TOTAL in inner.phases:
                    writer(f"    (gap)  {'rpc/queueing':13} {fmt_s(b.phases[name] - inner.phases[TOTAL])}")


def stats_rows(rows: list[dict], timestamp: str, tag: str, test_name: str) -> list[dict]:
    """report_phases() rows in the locust runner's stats.jsonl shape: one
    object per metric with the run's timestamp, tag and test name, and a
    flat map of string values. The metric name carries the row's dimensions
    so a dashboard selects a phase as it selects a request type; the
    dimensions are fields of the map too, since phase names contain
    underscores and the name cannot be split apart again."""
    out = []
    for r in rows:
        dims = {k: (r[k] if r[k] and r[k] != "-" else "none") for k in ("layer", "op", "class", "kind", "scope", "phase")}
        metric = "_".join(("phase", *dims.values()))
        measurements = {**dims, **{k: r[k] for k in ("count", "p50_ms", "p90_ms", "p95_ms", "max_ms")}}
        out.append({
            "timestamp": timestamp,
            "tag": tag,
            "test_name": test_name,
            "metric": re.sub(r"[^a-z0-9_]+", "_", metric.lower()),
            "measurements": {k: str(v) for k, v in measurements.items()},
        })
    return out


def write_csv(dest: Path, name: str, rows: list[dict]) -> None:
    if not rows:
        return
    dest.mkdir(parents=True, exist_ok=True)
    with open(dest / name, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("logs", nargs="+",
                    help="kubectl logs dumps (JSON lines) or Cloud Logging exports "
                         "(a JSON array, as gcloud logging read --format json writes); '-' for stdin")
    ap.add_argument("--csv", type=Path, default=None, help="also write phase_percentiles.csv here")
    ap.add_argument("--slowest", type=int, default=3, help="how many of the slowest operations to show")
    args = ap.parse_args()

    parsed = parse_files(args.logs)
    print(f"parsed {parsed.lines_matched} timing breakdown records out of {parsed.lines_seen} JSON log entries")
    if parsed.bad_values:
        print(f"(skipped {parsed.bad_values} non-numeric duration values)")
    if not parsed.breakdowns:
        print("no matching records; are these the keeper and worker pod logs?", file=sys.stderr)
        return 1

    # Pair first: a paired worker record takes its kind and class from the
    # keeper record, so both layers' rows split the same way.
    pairs = join_layers(parsed.breakdowns)
    print()
    rows = report_phases(parsed.breakdowns, print)
    report_waterfalls(parsed.breakdowns, print, args.slowest, pairs)
    if args.csv:
        write_csv(args.csv, "phase_percentiles.csv", rows)
        print(f"\nCSV written to {args.csv}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
