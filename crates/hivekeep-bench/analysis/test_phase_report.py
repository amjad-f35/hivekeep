"""Tests for phase_report.py; no cluster needed:

    python3 -m unittest discover -s crates/hivekeep-bench/analysis
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import phase_report  # noqa: E402


def parse(*records):
    out = phase_report.Parsed()
    for rec in records:
        phase_report.parse_line(json.loads(json.dumps(rec)), out)
    return out


def render(fn, *args, **kwargs):
    buf = io.StringIO()
    fn(*args, writer=lambda line: buf.write(line + "\n"), **kwargs)
    return buf.getvalue()


KEEPER_RESTORE = {
    "time": "2026-09-23T10:00:05.500000000Z", "level": "INFO",
    "msg": "Restore timing breakdown",
    "hk.actor.uid": "uid-1", "hk.template.name": "sweperf-astropy-7336",
    "hk.snapshot.scope": "full", "hk.snapshot.kind": "latest",
    "hk.actor.restore.duration.download": 2.4,
    "hk.actor.restore.duration.worker_restore": 1.1,
    "hk.actor.restore.duration.total": 3.9,
}

WORKER_RESTORE = {
    "time": "2026-09-23T10:00:05.400000000Z", "level": "INFO",
    "msg": "Restore timing breakdown",
    "hk.actor.uid": "uid-1", "hk.template.name": "sweperf-astropy-7336",
    "hk.snapshot.scope": "full",
    "worker.actor.restore.duration.vm_restore": 0.8,
    "worker.actor.restore.duration.wakeup_probe": 0.2,
    "worker.actor.restore.duration.total": 1.05,
}

KEEPER_CHECKPOINT = {
    "time": "2026-09-23T10:01:00.000000000Z", "level": "INFO",
    "msg": "Checkpoint timing breakdown",
    "hk.actor.uid": "uid-1", "hk.template.name": "sweperf-astropy-7336",
    "hk.snapshot.scope": "full", "hk.snapshot.kind": "latest",
    "hk.actor.checkpoint.duration.sandbox_assets": 0.01,
    "hk.actor.checkpoint.duration.worker_checkpoint": 1.14,
    "hk.actor.checkpoint.duration.persist": 4.48,
    "hk.actor.checkpoint.duration.total": 6.0,
}

WORKER_CHECKPOINT = {
    "time": "2026-09-23T10:00:55.000000000Z", "level": "INFO",
    "msg": "Checkpoint timing breakdown",
    "hk.actor.uid": "uid-1", "hk.template.name": "sweperf-astropy-7336",
    "hk.snapshot.scope": "full",
    "worker.actor.checkpoint.duration.prep": 0.04,
    "worker.actor.checkpoint.duration.pause": 0.01,
    "worker.actor.checkpoint.duration.snapshot": 0.5,
    "worker.actor.checkpoint.duration.rootfs_upper": 0.9,
    "worker.actor.checkpoint.duration.teardown": 0.2,
    "worker.actor.checkpoint.duration.total": 1.2,
}

# A worker restore record of the same Actor an hour earlier: another cycle,
# whose keeper record is not in the dump.
WORKER_RESTORE_STALE = dict(WORKER_RESTORE, time="2026-09-23T09:00:05.400000000Z")

# One Cloud Logging export entry, cut to what matters: the record under
# jsonPayload, its time moved to the entry's timestamp, dotted keys as logged.
CLOUD_LOGGING_ENTRY = {
    "insertId": "m9xljnzmzppup1vj",
    "jsonPayload": {
        "hk.actor.checkpoint.duration.worker_checkpoint": 1.024788329,
        "hk.actor.checkpoint.duration.persist": 4.18451256,
        "hk.actor.checkpoint.duration.sandbox_assets": 2.4358e-05,
        "hk.actor.checkpoint.duration.total": 5.385861508,
        "hk.actor.name": "sb-d790f7ed", "hk.actor.uid": "cc7a2ac7",
        "hk.hive": "benchmark", "hk.sandbox.class": "microvm",
        "hk.snapshot.kind": "latest", "hk.snapshot.scope": "full",
        "hk.template.hive": "benchmark-workloads", "hk.template.name": "glutton",
        "level": "INFO", "msg": "Checkpoint timing breakdown",
    },
    "logName": "projects/p/logs/stdout",
    "resource": {"labels": {"container_name": "keeper", "namespace_name": "hivekeep-system"},
                 "type": "k8s_container"},
    "severity": "INFO",
    "timestamp": "2026-09-28T17:49:47.665836237Z",
}

FAILED = {
    "time": "2026-09-23T10:02:00Z", "level": "INFO",
    "msg": "Restore timing breakdown",
    "hk.actor.uid": "uid-2", "hk.snapshot.scope": "full",
    "error.type": "DeadlineExceeded",
    "hk.actor.restore.duration.download": 30.0,
    "hk.actor.restore.duration.total": 30.0,
}


class ParseTest(unittest.TestCase):
    def test_parses_both_layers_of_the_same_msg(self):
        out = parse(KEEPER_RESTORE, WORKER_RESTORE)
        self.assertEqual([(b.source, b.op) for b in out.breakdowns],
                         [("keeper", "restore"), ("worker", "restore")])
        self.assertEqual(out.breakdowns[0].phases["download"], 2.4)
        self.assertEqual(out.breakdowns[1].phases["vm_restore"], 0.8)
        self.assertEqual(out.breakdowns[1].actor_uid, "uid-1")

    def test_nanosecond_timestamps_parse(self):
        out = parse(KEEPER_RESTORE, FAILED)
        self.assertIsNotNone(out.breakdowns[0].ts)
        self.assertIsNotNone(out.breakdowns[1].ts)
        self.assertAlmostEqual(out.breakdowns[0].ts % 1, 0.5, places=6)
        self.assertIsNone(phase_report.parse_time("yesterday"))

    def test_worker_checkpoint_parses(self):
        b = parse(WORKER_CHECKPOINT).breakdowns[0]
        self.assertEqual((b.source, b.op), ("worker", "checkpoint"))
        self.assertEqual(b.phases["rootfs_upper"], 0.9)

    def test_cloud_logging_entry_parses_with_the_entry_timestamp(self):
        b = parse(CLOUD_LOGGING_ENTRY).breakdowns[0]
        self.assertEqual((b.source, b.op), ("keeper", "checkpoint"))
        self.assertEqual(b.time, "2026-09-28T17:49:47.665836237Z")
        self.assertIsNotNone(b.ts)
        self.assertEqual((b.sandbox_class, b.kind, b.scope), ("microvm", "latest", "full"))
        self.assertAlmostEqual(b.phases["persist"], 4.18451256)

    def test_cloud_logging_export_file_is_a_json_array(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "export.json")
            second = dict(CLOUD_LOGGING_ENTRY, timestamp="2026-09-28T17:50:47.000000000Z")
            with open(path, "w") as f:
                json.dump([CLOUD_LOGGING_ENTRY, second], f, indent=2)
            out = phase_report.parse_files([path])
        self.assertEqual((out.lines_seen, len(out.breakdowns)), (2, 2))

    def test_worker_records_take_class_and_kind_from_the_paired_keeper_record(self):
        keeper = dict(KEEPER_RESTORE, **{"hk.sandbox.class": "gvisor"})
        out = parse(WORKER_RESTORE, keeper)
        self.assertEqual(out.breakdowns[0].sandbox_class, "")  # not on the worker record
        phase_report.join_layers(out.breakdowns)
        self.assertEqual((out.breakdowns[0].sandbox_class, out.breakdowns[0].kind), ("gvisor", "latest"))

    def test_an_unpaired_worker_record_keeps_no_class(self):
        out = parse(WORKER_RESTORE)
        phase_report.join_layers(out.breakdowns)
        self.assertEqual(out.breakdowns[0].sandbox_class, "")
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        self.assertEqual({r["class"] for r in rows}, {"-"})
    def test_non_numeric_duration_is_skipped_not_fatal(self):
        rec = dict(KEEPER_RESTORE, **{"hk.actor.restore.duration.download": None,
                                      "hk.actor.restore.duration.oci_unpack": "fast"})
        out = parse(rec)
        self.assertEqual(out.bad_values, 2)
        self.assertEqual(out.breakdowns[0].phases["total"], 3.9)
        self.assertNotIn("download", out.breakdowns[0].phases)

    def test_malformed_export_warns_instead_of_vanishing(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "export.json")
            with open(path, "w") as f:
                f.write('[{"jsonPayload": {"msg": "Checkpoint timing breakdown"')  # truncated
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                out = phase_report.parse_files([path])
        self.assertEqual(out.breakdowns, [])
        self.assertIn("export.json", err.getvalue())
        self.assertIn("not a JSON array", err.getvalue())

    def test_bracket_prefixed_dump_is_read_line_by_line(self):
        # A dump whose lines start with "[INFO]" also starts with "[", but
        # it is not an export; its records must not be lost.
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "pod.log")
            with open(path, "w") as f:
                f.write("[INFO] " + json.dumps(KEEPER_RESTORE) + "\n"
                        "[2026-09-23 10:00:05] " + json.dumps(WORKER_RESTORE) + "\n")
            err = io.StringIO()
            with contextlib.redirect_stderr(err):
                out = phase_report.parse_files([path])
        self.assertEqual(len(out.breakdowns), 2)
        self.assertIn("reading it line by line", err.getvalue())

    def test_parse_lines_feeds_an_existing_parse(self):
        out = phase_report.parse_lines([json.dumps(KEEPER_RESTORE), "not json", ""])
        self.assertEqual((out.lines_seen, len(out.breakdowns)), (1, 1))
        phase_report.parse_lines(["pod/x " + json.dumps(WORKER_RESTORE)], out)
        self.assertEqual(len(out.breakdowns), 2)

    def test_prefixed_kubectl_lines_still_parse(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "pod.log")
            with open(path, "w") as f:
                f.write("pod/keeper-abc " + json.dumps(KEEPER_RESTORE) + "\nnot json\n{\"msg\": \"other\"}\n")
            out = phase_report.parse_files([path])
        self.assertEqual(len(out.breakdowns), 1)
        self.assertEqual(out.lines_seen, 2)


class UnattributedTest(unittest.TestCase):
    def test_keeper_checkpoint_residual_is_total_minus_sequential_phases(self):
        rec = dict(KEEPER_CHECKPOINT, **{"hk.actor.checkpoint.duration.cleanup": 0.35})
        out = parse(rec)
        phase_report.join_layers(out.breakdowns)
        self.assertAlmostEqual(out.breakdowns[0].phases["unattributed"], 6.0 - (0.01 + 1.14 + 4.48 + 0.35))
    def test_microvm_worker_checkpoint_counts_the_concurrent_captures_once(self):
        # prep + pause + max(snapshot, rootfs_upper) + teardown; the two
        # captures overlapped, so only the slower one is spent wall time.
        phases = {k[len("worker.actor.checkpoint.duration."):]: v
                  for k, v in WORKER_CHECKPOINT.items() if k.startswith("worker.actor.checkpoint.duration.")}
        got = phase_report.unattributed("worker", "checkpoint", "microvm", phases)
        self.assertAlmostEqual(got, 1.2 - (0.04 + 0.01 + 0.9 + 0.2))

    def test_gvisor_worker_checkpoint_phases_are_sequential(self):
        phases = {"prep": 0.01, "pause": 0.02, "checkpoint": 0.15, "durable_dir": 0.03,
                  "resume": 0.02, "teardown": 0.05, "total": 0.30}
        got = phase_report.unattributed("worker", "checkpoint", "gvisor", phases)
        self.assertAlmostEqual(got, 0.30 - (0.01 + 0.02 + 0.15 + 0.03 + 0.02 + 0.05))

    def test_worker_checkpoint_of_unknown_runtime_gets_no_residual(self):
        out = parse(WORKER_CHECKPOINT)  # no keeper partner, so no class
        phase_report.join_layers(out.breakdowns)
        self.assertNotIn("unattributed", out.breakdowns[0].phases)

    def test_gvisor_pair_splits_both_layers_by_class(self):
        keeper = dict(KEEPER_CHECKPOINT, **{"hk.sandbox.class": "gvisor"})
        worker = {
            "time": "2026-09-23T10:00:55.000000000Z", "msg": "Checkpoint timing breakdown",
            "hk.actor.uid": "uid-1", "hk.template.name": "sweperf-astropy-7336", "hk.snapshot.scope": "full",
            "worker.actor.checkpoint.duration.pause": 0.02,
            "worker.actor.checkpoint.duration.checkpoint": 0.15,
            "worker.actor.checkpoint.duration.teardown": 0.05,
            "worker.actor.checkpoint.duration.total": 0.25,
        }
        out = parse(worker, keeper)
        phase_report.join_layers(out.breakdowns)
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        worker_rows = {r["phase"]: r for r in rows if r["layer"] == "worker"}
        self.assertEqual({r["class"] for r in worker_rows.values()}, {"gvisor"})
        self.assertEqual({r["kind"] for r in worker_rows.values()}, {"latest"})
        self.assertAlmostEqual(worker_rows["unattributed"]["p50_ms"], (0.25 - 0.22) * 1000)
        # gVisor's phases sort in their own order, before the shared tail.
        order = [r["phase"] for r in rows if r["layer"] == "worker"]
        self.assertEqual(order, ["pause", "checkpoint", "teardown", "unattributed", "total"])
    def test_restore_layers_get_no_residual(self):
        out = parse(KEEPER_RESTORE, WORKER_RESTORE)
        phase_report.join_layers(out.breakdowns)
        for b in out.breakdowns:
            self.assertNotIn("unattributed", b.phases)
    def test_negative_residual_is_not_clamped(self):
        # Cannot happen on a well-formed record; if it does, it must show.
        phases = {"sandbox_assets": 0.01, "worker_checkpoint": 1.14, "persist": 4.48, "total": 1.0}
        self.assertAlmostEqual(phase_report.unattributed("keeper", "checkpoint", "microvm", phases),
                               1.0 - (0.01 + 1.14 + 4.48))

class ReportTest(unittest.TestCase):
    def test_failed_records_are_flagged_and_excluded_from_percentiles(self):
        out = parse(KEEPER_RESTORE, FAILED)
        self.assertEqual([b.failed for b in out.breakdowns], [False, True])
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        downloads = [r for r in rows if r["phase"] == "download"]
        self.assertEqual(len(downloads), 1)
        self.assertEqual(downloads[0]["count"], 1)  # the failed 30s never entered

    def test_percentiles_split_by_sandbox_class_and_kind(self):
        local = dict(KEEPER_RESTORE, **{"time": "2026-09-23T09:59:00.000000000Z",
                                        "hk.snapshot.kind": "local", "hk.sandbox.class": "gvisor",
                                        "hk.actor.restore.duration.download": 9.0,
                                        "hk.actor.restore.duration.total": 10.0})
        latest = dict(KEEPER_RESTORE, **{"hk.sandbox.class": "gvisor"})
        rows = phase_report.report_phases(parse(local, latest).breakdowns, lambda _: None)
        downloads = {(r["class"], r["kind"]): r["p50_ms"] for r in rows if r["phase"] == "download"}
        self.assertEqual(downloads, {("gvisor", "local"): 9000.0, ("gvisor", "latest"): 2400.0})

    def test_paired_worker_record_inherits_the_keeper_kind(self):
        out = parse(WORKER_RESTORE, KEEPER_RESTORE)
        pairs = phase_report.pair_records(out.breakdowns)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(out.breakdowns[0].kind, "latest")
        rows = phase_report.report_phases(out.breakdowns, lambda _: None)
        self.assertEqual({r["kind"] for r in rows if r["layer"] == "worker"}, {"latest"})

    def test_checkpoint_pairing_window_ends_where_persist_starts(self):
        # The worker record is written when worker_checkpoint ends, before the
        # 4.48 s persist; a record from inside the persist window belongs to
        # a later cycle of a rapidly cycling actor.
        during_persist = dict(WORKER_CHECKPOINT, time="2026-09-23T10:00:59.000000000Z")
        out = parse(WORKER_CHECKPOINT, during_persist, KEEPER_CHECKPOINT)
        pairs = phase_report.pair_records(out.breakdowns)
        keeper = next(b for b in out.breakdowns if b.source == "keeper")
        self.assertIs(pairs[id(keeper)], out.breakdowns[0])

    def test_checkpoint_pairing_window_excludes_cleanup(self):
        # Cleanup runs after the worker record is written too: a record from
        # inside the 0.35 s cleanup window belongs to a later cycle.
        checkpoint = dict(KEEPER_CHECKPOINT, **{"hk.actor.checkpoint.duration.cleanup": 0.35})
        during_cleanup = dict(WORKER_CHECKPOINT, time="2026-09-23T10:00:56.400000000Z")
        out = parse(WORKER_CHECKPOINT, during_cleanup, checkpoint)
        pairs = phase_report.pair_records(out.breakdowns)
        keeper = next(b for b in out.breakdowns if b.source == "keeper")
        self.assertIs(pairs[id(keeper)], out.breakdowns[0])

    def test_an_worker_record_pairs_with_at_most_one_operation(self):
        second = dict(KEEPER_RESTORE, time="2026-09-23T10:00:05.900000000Z")
        out = parse(WORKER_RESTORE, KEEPER_RESTORE, second)
        pairs = phase_report.pair_records(out.breakdowns)
        self.assertEqual(len(pairs), 1)
        first = next(b for b in out.breakdowns if b.source == "keeper")
        self.assertIn(id(first), pairs)

    def test_waterfall_ignores_an_worker_record_from_another_cycle(self):
        out = parse(WORKER_RESTORE_STALE, KEEPER_RESTORE)
        text = render(phase_report.report_waterfalls, out.breakdowns, slowest=1)
        self.assertIn("keeper worker_restore", text)
        self.assertNotIn("worker vm_restore", text)
        self.assertNotIn("(gap)", text)

    def test_stats_rows_take_the_runner_jsonl_shape(self):
        rows = phase_report.report_phases(parse(KEEPER_RESTORE).breakdowns, lambda _: None)
        entries = phase_report.stats_rows(rows, "2026-09-23T10:00:00Z", "abc123", "glutton_mem_1gi")
        download = next(e for e in entries if e["metric"].endswith("_download"))
        self.assertEqual(download["metric"], "phase_keeper_restore_none_latest_full_download")
        self.assertEqual((download["timestamp"], download["tag"], download["test_name"]),
                         ("2026-09-23T10:00:00Z", "abc123", "glutton_mem_1gi"))
        # String values and the dimensions as fields, like the runner's rows.
        self.assertEqual(download["measurements"], {
            "layer": "keeper", "op": "restore", "class": "none", "kind": "latest",
            "scope": "full", "phase": "download",
            "count": "1", "p50_ms": "2400.0", "p90_ms": "2400.0", "p95_ms": "2400.0", "max_ms": "2400.0"})

    def test_waterfall_nests_worker_and_gap_under_keeper(self):
        out = parse(WORKER_RESTORE, KEEPER_RESTORE)
        text = render(phase_report.report_waterfalls, out.breakdowns, slowest=1)
        lines = [line.strip() for line in text.splitlines()]
        self.assertTrue(any(line.startswith("keeper worker_restore") for line in lines), text)
        self.assertTrue(any(line.startswith("worker vm_restore") for line in lines), text)
        gap = [line for line in lines if line.startswith("(gap)")]
        self.assertEqual(len(gap), 1, text)
        self.assertIn("50.0", gap[0])  # 1.1s keeper worker_restore - 1.05s worker total


# The same keeper restore record as the tracing JSON formatter writes it: the
# message and the record (a JSON string) under fields, the time under
# timestamp.
TRACING_KEEPER_RESTORE = {
    "timestamp": "2026-09-23T10:00:05.500000Z", "level": "INFO", "target": "hivekeep_keeper::keeper::restore",
    "fields": {
        "message": "Restore timing breakdown",
        "timing": json.dumps({k: v for k, v in KEEPER_RESTORE.items() if k not in ("time", "level", "msg")}),
    },
}


class TracingFormatTest(unittest.TestCase):
    def test_tracing_record_parses_like_the_flat_one(self):
        flat = parse(KEEPER_RESTORE).breakdowns[0]
        nested = parse(TRACING_KEEPER_RESTORE).breakdowns[0]
        self.assertEqual((nested.source, nested.op), ("keeper", "restore"))
        self.assertEqual(nested.phases, flat.phases)
        self.assertEqual((nested.actor_uid, nested.kind, nested.scope), ("uid-1", "latest", "full"))
        self.assertAlmostEqual(nested.ts, flat.ts, places=6)

    def test_record_field_and_message_key(self):
        worker = {
            "timestamp": "2026-09-23T10:00:05.400000Z",
            "fields": {"message": "Restore timing breakdown",
                       "record": json.dumps({k: v for k, v in WORKER_RESTORE.items() if k not in ("time", "level", "msg")})},
        }
        flat_message = dict(WORKER_RESTORE)
        flat_message["message"] = flat_message.pop("msg")
        out = parse(worker, flat_message, TRACING_KEEPER_RESTORE)
        self.assertEqual([b.source for b in out.breakdowns], ["worker", "worker", "keeper"])
        self.assertEqual(len(phase_report.pair_records(out.breakdowns)), 1)

    def test_other_tracing_events_are_skipped(self):
        out = parse({"timestamp": "2026-09-23T10:00:05Z", "fields": {"message": "restored actor"}},
                    {"fields": {"message": "Restore timing breakdown", "timing": "not json"}})
        self.assertEqual(out.breakdowns, [])


if __name__ == "__main__":
    unittest.main()
