"""Unit tests for runner.py's command lines and trace extraction.

Run with: python3 crates/hivekeep-bench/locust/unit_tests/test_runner.py
"""

import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import runner

ARGV = ["runner.py", "-f", "/app/tests/glutton.py,/app/shapes/ladder_shape.py",
        "-t", "1m", "-u", "10", "--tag", "unit", "--name", "unit-run",
        "--dest", "/tmp/unit"]


def parse(*extra):
    with mock.patch.object(sys, "argv", ARGV + list(extra)):
        return runner.parse_args()


class TestFileTest(unittest.TestCase):
    def test_shapes_are_not_the_test(self):
        files = "/app/shapes/ladder_shape.py,/app/tests/resumepaused.py"
        self.assertEqual(runner.test_file(files), "/app/tests/resumepaused.py")
        self.assertTrue(runner.needs_worker(files))

    def test_python_classes_run_in_locust(self):
        self.assertFalse(runner.needs_worker("/app/tests/counter_demo.py"))
        self.assertFalse(runner.needs_worker("/app/tests/hive_api.py"))
        self.assertTrue(runner.needs_worker("/app/tests/hkenv.py"))


class WorkerCommandTest(unittest.TestCase):
    def test_class_flags_and_config(self):
        args = parse("--actors-per-user", "3", "--trace-probability", "0.5",
                     "--spawn-rate", "2")
        cmd = runner.worker_command(args)
        self.assertEqual(cmd[:3], [runner.WORKER_BINARY, "--user-class", "glutton"])
        cfg = json.loads(cmd[cmd.index("--config-json") + 1])
        # Only worker knobs reach the config; locust's own flags do not.
        self.assertEqual(cfg, {"trace_probability": 0.5})
        self.assertEqual(cmd[cmd.index("--actors-per-user") + 1], "3")
        self.assertEqual(cmd[-2:], ["--master-web-port", str(runner.LOADGEN_CONFIG_PORT)])

    def test_no_knobs_no_config(self):
        cmd = runner.worker_command(parse())
        self.assertNotIn("--config-json", cmd)
        self.assertNotIn("--total-actors", cmd)


class TraceRecordTest(unittest.TestCase):
    def test_worker_json_event(self):
        line = json.dumps({
            "timestamp": "2026-10-01T10:00:00.000001Z", "level": "INFO",
            "fields": {"message": "traced span (failed)", "name": "ResumeActor",
                       "trace_id": "ab" * 16, "duration_ms": 12.5,
                       "source": "server", "err": "unavailable"},
            "target": "hivekeep_bench::loadgen::telemetry",
        })
        self.assertEqual(runner.extract_trace_record("worker", line), {
            "time": "2026-10-01T10:00:00.000001Z", "name": "ResumeActor",
            "duration_ms": "12.500", "latency_source": "server",
            "trace_id": "ab" * 16, "err": "unavailable",
        })

    def test_worker_lines_without_a_span(self):
        for line in ("not json trace_id", json.dumps({"fields": {"message": "hello", "trace_id": "x"}}),
                     json.dumps({"fields": {"message": "traced span"}}), "[1, 2]"):
            self.assertIsNone(runner.extract_trace_record("worker", line), line)

    def test_python_line(self):
        line = ("[2026-10-01 10:00:00,000] host/INFO/common.grpc_tracing: Traced "
                f"SuspendActor (failed): trace_id={'cd' * 16}, duration_ms=3.25 (client)")
        self.assertEqual(runner.extract_trace_record("locust", line), {
            "time": "", "name": "SuspendActor", "duration_ms": "3.25",
            "latency_source": "client", "trace_id": "cd" * 16, "err": "failed",
        })


class PairFlagsTest(unittest.TestCase):
    def test_pairs(self):
        self.assertEqual(
            runner.pair_flags(["--a", "1", "--b", "--c", "x", "y"]),
            ["--a 1", "--b", "--c x", "y"],
        )


if __name__ == "__main__":
    unittest.main()
