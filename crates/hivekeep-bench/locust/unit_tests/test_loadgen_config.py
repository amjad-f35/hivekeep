"""Unit tests for common/loadgen_config.py."""

import json
import sys
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from common import loadgen_config


class TestWorkerConfig(unittest.TestCase):
    def test_build_config_json_lifecycle_mode_none(self):
        raw = loadgen_config.build_config_json([
            "--lifecycle-mode=none",
            "--agentsession-script=personal-assistant-light",
        ])
        parsed = json.loads(raw)
        self.assertEqual(parsed["lifecycle_mode"], "none")
        self.assertEqual(
            parsed["agentsession_script"], "personal-assistant-light"
        )

    def test_build_config_json_snapshot_steps(self):
        raw = loadgen_config.build_config_json([
            "--lifecycle-mode",
            "sync",
            "--agentsession-snapshot-steps",
            "3,10,20,30,40,50,61",
        ])
        parsed = json.loads(raw)
        self.assertEqual(parsed["lifecycle_mode"], "sync")
        self.assertEqual(
            parsed["agentsession_snapshot_steps"], "3,10,20,30,40,50,61"
        )

    def test_build_config_json_phase_in_and_retry_budget(self):
        raw = loadgen_config.build_config_json([
            "--agentsession-phase-in",
            "300",
            "--agentsession-resume-retry-budget",
            "0",
        ])
        parsed = json.loads(raw)
        self.assertEqual(parsed["agentsession_phase_in"], 300.0)
        self.assertEqual(parsed["agentsession_resume_retry_budget"], 0.0)

    def test_build_config_json_empty_and_unknown_flags(self):
        self.assertEqual(loadgen_config.build_config_json([]), "")
        self.assertEqual(
            loadgen_config.build_config_json(["--spawn-rate", "5", "-u", "3"]), ""
        )

    def test_headless_server_serves_the_current_options(self):
        options = SimpleNamespace(trace_probability=0.1, target_rps=5.0)
        env = SimpleNamespace(parsed_options=options)
        server = loadgen_config.serve_config_headless(env, 0)
        try:
            base = f"http://127.0.0.1:{server.server_address[1]}"
            with urllib.request.urlopen(base + "/loadgen-config") as r:
                got = json.load(r)
            self.assertEqual(set(got), {loadgen_config.key(f) for f in loadgen_config.WORKER_FLAGS})
            self.assertEqual(got["trace_probability"], 0.1)
            self.assertIsNone(got["lifecycle_mode"])

            # A later change, as a ladder step makes, is served at once.
            options.target_rps = 9.0
            with urllib.request.urlopen(base + "/loadgen-config?x=1") as r:
                self.assertEqual(json.load(r)["target_rps"], 9.0)

            with self.assertRaises(urllib.error.HTTPError) as e:
                urllib.request.urlopen(base + "/boom")
            self.assertEqual(e.exception.code, 404)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
