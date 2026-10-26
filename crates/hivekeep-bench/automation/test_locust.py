"""Tests for testtypes/locust.py: python3 -m unittest test_locust (from this
directory)."""

import os
import unittest
from unittest import mock

import yaml

import controlplane
import orchestrator
from testtypes import locust

HERE = os.path.dirname(os.path.abspath(__file__))


def entry(**fields):
    return {
        "name": "t",
        "file": "/app/tests/resumepaused.py,/app/shapes/ladder_shape.py",
        "duration": "21m",
        "users": 200,
        **fields,
    }


CONTROL_PLANE = {"apiServer": {"cpu": 2}}


class ValidateTest(unittest.TestCase):
    def test_without_control_plane(self):
        locust.validate(entry())
        self.assertIsNone(locust.control_plane_config(entry()))

    def test_with_control_plane(self):
        test = entry(runnerCpu="8", runnerMemory="2Gi", controlPlane=CONTROL_PLANE)
        locust.validate(test)
        self.assertEqual(locust.control_plane_config(test), controlplane.config(CONTROL_PLANE))

    def test_invalid_control_plane_names_the_test(self):
        with self.assertRaisesRegex(ValueError, "locust test 't': needs controlPlane.apiServer.cpu"):
            locust.validate(entry(controlPlane={"apiServer": {}}))

    def test_missing_required(self):
        for field in locust.REQUIRED_FIELDS:
            test = entry()
            del test[field]
            with self.subTest(field=field), self.assertRaises(ValueError):
                locust.validate(test)


class PreTestTest(unittest.TestCase):
    def shaped(self, test):
        calls = []
        patcher = mock.patch.object(controlplane, "shape", calls.append)
        patcher.start()
        self.addCleanup(patcher.stop)
        locust.pre_test(test)
        return calls

    def test_without_control_plane_leaves_the_cluster(self):
        self.assertEqual(self.shaped(entry()), [])

    def test_shapes_the_control_plane(self):
        self.assertEqual(self.shaped(entry(controlPlane=CONTROL_PLANE)), [controlplane.config(CONTROL_PLANE)])


class JobTest(unittest.TestCase):
    def render(self, test):
        subs = {"JOB_NAME": "j", "NAME": "t", "TAG": "c", "IMAGE": "i", "DEST": "d", **locust.job_subs(test)}
        tmpl = locust.job_tmpl(os.path.join(HERE, "manifests"))
        text = orchestrator.render_template(tmpl, subs, test.get("flags", []))
        self.assertNotIn("${", text)
        jobs = [d for d in yaml.safe_load_all(text) if d and d["kind"] == "Job"]
        self.assertEqual(len(jobs), 1)
        return jobs[0]["spec"]["template"]["spec"]

    def test_defaults_render(self):
        pod = self.render(entry())
        self.assertNotIn("affinity", pod)
        self.assertEqual(pod["containers"][0]["resources"], {"requests": {"cpu": "500m", "memory": "512Mi"}})
        # The load generator's metrics port, for scraping.
        self.assertIn({"name": "loadgen-metrics", "containerPort": 8001}, pod["containers"][0]["ports"])

    def test_sized_runner_renders(self):
        test = entry(runnerCpu="8", runnerMemory="2Gi", controlPlane=CONTROL_PLANE, flags=["--actors", "200"])
        pod = self.render(test)
        self.assertEqual(pod["containers"][0]["resources"]["requests"], {"cpu": "8", "memory": "2Gi"})
        self.assertEqual(pod["containers"][0]["args"][-2:], ["--actors", "200"])

    def test_mounts_match_the_load_generator(self):
        # The Rust load generator reads its CA and client bundle from these
        # directories (src/loadgen/client.rs).
        mounts = {m["mountPath"] for m in self.render(entry())["containers"][0]["volumeMounts"]}
        self.assertEqual(mounts, {"/run/servicedns.podcert.hivekeep.io", "/run/podidentity.podcert.hivekeep.io"})


class TestsYamlTest(unittest.TestCase):
    def test_entries_validate(self):
        with open(os.path.join(HERE, "tests.yaml")) as f:
            tests = yaml.safe_load(f)["tests"]
        orchestrator.validate_and_normalize_tests(tests)


if __name__ == "__main__":
    unittest.main()
