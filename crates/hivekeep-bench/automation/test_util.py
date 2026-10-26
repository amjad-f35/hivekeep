"""Tests for util.py: python3 -m unittest test_util (from this directory)."""

import unittest
from unittest import mock

import yaml

import util


class ParseDurationSecondsTest(unittest.TestCase):
    def test_units(self):
        self.assertEqual(util.parse_duration_seconds("30"), 30)
        self.assertEqual(util.parse_duration_seconds("30s"), 30)
        self.assertEqual(util.parse_duration_seconds("5m"), 300)
        self.assertEqual(util.parse_duration_seconds("2h"), 7200)

    def test_whitespace(self):
        self.assertEqual(util.parse_duration_seconds(" 10 m "), 600)

    def test_invalid(self):
        for bad in ("10 parsecs", "", "m", "1d", "-5s"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                util.parse_duration_seconds(bad)


class BuildAndPushTest(unittest.TestCase):
    @mock.patch("util.run")
    def test_builds_amd64_and_pushes(self, run):
        image = util.build_and_push("gcr.io/p/repo/img:tag", "path/Dockerfile")
        self.assertEqual(image, "gcr.io/p/repo/img:tag")
        build, push = (c.args[0] for c in run.call_args_list)
        self.assertEqual(build[:2], ["docker", "build"])
        self.assertEqual(build[build.index("--platform") + 1], "linux/amd64")
        self.assertEqual(build[build.index("-t") + 1], "gcr.io/p/repo/img:tag")
        self.assertEqual(build[build.index("-f") + 1], "path/Dockerfile")
        self.assertEqual(push, ["docker", "push", "gcr.io/p/repo/img:tag"])


class IsolateTest(unittest.TestCase):
    def test_absent(self):
        test = {"name": "t"}
        self.assertIsNone(util.isolate_block(test))
        self.assertEqual(util.isolate_install_args(test), [])
        self.assertEqual(util.isolate_workload_args(test), [])

    def test_empty_block_selects_the_mode_with_defaults(self):
        test = {"isolate": {}}
        self.assertEqual(util.isolate_install_args(test), ["--fake-data-plane"])
        self.assertEqual(util.isolate_workload_args(test), [])

    def test_bare_key_selects_the_mode(self):
        test = yaml.safe_load("name: t\nisolate:\n")
        self.assertEqual(util.isolate_install_args(test), ["--fake-data-plane"])
        self.assertEqual(util.isolate_workload_args(test), [])

    def test_tuning_goes_to_deploy_sh_not_the_install(self):
        test = {"isolate": {"delay": "500ms", "delayRestore": "2s", "mintActorCertificate": False}}
        self.assertEqual(util.isolate_install_args(test), ["--fake-data-plane"])
        self.assertEqual(
            util.isolate_workload_args(test),
            [
                "--fake-keeper-env", "FAKE_KEEPER_DELAY=500ms",
                "--fake-keeper-env", "FAKE_KEEPER_DELAY_RESTORE=2s",
                "--fake-keeper-env", "FAKE_KEEPER_MINT_ACTOR_CERTIFICATE=false",
            ],
        )

    def test_durations(self):
        for delay in ("0", "0s", "1.5s", "1m30s", "250ms", "100us"):
            with self.subTest(delay=delay):
                self.assertEqual(
                    util.isolate_workload_args({"isolate": {"delayCheckpoint": delay}}),
                    ["--fake-keeper-env", f"FAKE_KEEPER_DELAY_CHECKPOINT={delay}"],
                )

    def test_invalid(self):
        for block in (
            # Every fake keeper node serves the pool; there is no node count.
            {"nodes": 4},
            {"run": "nightly"},
            {"delay": "fast"},
            {"delay": "500"},
            {"delay": "-1s"},
            {"delay": 0},
            {"delayRun": "soon"},
            {"mintActorCertificate": "no"},
            ["nodes"],
        ):
            with self.subTest(block=block), self.assertRaises(ValueError):
                util.isolate_block({"isolate": block})


if __name__ == "__main__":
    unittest.main()
