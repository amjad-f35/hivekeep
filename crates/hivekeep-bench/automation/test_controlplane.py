"""Tests for controlplane.py: python3 -m unittest test_controlplane (from this
directory)."""

import os
import unittest
from unittest import mock

import yaml

import controlplane

HERE = os.path.dirname(os.path.abspath(__file__))
NETWORK_POLICIES = os.path.join(
    HERE, "..", "..", "hivekeep-setup", "manifests", "install", "hivekeep-system-networkpolicies.yaml"
)


def stub(test, **fakes):
    for name, fake in fakes.items():
        patcher = mock.patch.object(controlplane, name, fake)
        patcher.start()
        test.addCleanup(patcher.stop)


API = {"apiServer": {"cpu": 2}}


class ConfigTest(unittest.TestCase):
    def test_defaults(self):
        cfg = controlplane.config(API)
        # Only what the block sets overrides the install.
        self.assertEqual(cfg["apiServer"], {"cpu": 2})
        self.assertEqual(cfg["postgres"], {"stats": True, "args": []})


class ValidateTest(unittest.TestCase):
    def test_valid_knobs(self):
        controlplane.validate(API)
        controlplane.validate(
            {"apiServer": {"cpu": "1500m", "memory": "8Gi", "replicas": 2, "poolMaxConns": 16},
             "postgres": {"stats": False}})
        controlplane.validate({**API, "postgres": {"args": ["commit_delay=100", "max_wal_size=16GB"]}})

    def test_invalid(self):
        def api(**knobs):
            return {"apiServer": {"cpu": 2, **knobs}}

        def pg(**knobs):
            return {**API, "postgres": knobs}

        bad = [
            {},
            "2",
            {"apiServer": "2"},
            {"apiServer": {}},
            {**API, "hivekeepApi": {}},
            {**API, "postgres": "on"},
            api(cpu=0),
            api(cpu="two"),
            api(cpu=True),
            api(cpus=2),
            api(replicas=0),
            api(replicas=True),
            api(poolMaxConns="64"),
            api(memory="4 GiB"),
            pg(stats="yes"),
            pg(statz=True),
            pg(args="commit_delay=100"),
            pg(args=["commit_delay"]),
            pg(args=["commit_delay = 100"]),
            pg(args=["-c commit_delay=100"]),
            pg(args=[100]),
            # The settings ride on the restart that stats makes.
            pg(stats=False, args=["commit_delay=100"]),
        ]
        for block in bad:
            with self.subTest(block=block), self.assertRaises(ValueError):
                controlplane.validate(block)

    def test_errors_name_the_knob(self):
        with self.assertRaisesRegex(ValueError, "controlPlane.apiServer.cpu"):
            controlplane.validate({"apiServer": {}})
        with self.assertRaisesRegex(ValueError, r"unknown controlPlane.postgres knob\(s\) \['statz'\]"):
            controlplane.validate({**API, "postgres": {"statz": True}})


class ApiServerPatchTest(unittest.TestCase):
    def patch(self, **knobs):
        return controlplane.api_server_patch(controlplane.config({"apiServer": {"cpu": 2, **knobs}})["apiServer"])

    def test_sets_the_knobs_given(self):
        patch = self.patch(memory="4Gi", replicas=1)
        self.assertEqual(patch["spec"]["replicas"], 1)
        pod = patch["spec"]["template"]["spec"]
        self.assertEqual(pod["containers"][0]["name"], "hivekeep-api")
        resources = pod["containers"][0]["resources"]
        self.assertEqual(resources["requests"], {"cpu": "2", "memory": "4Gi"})
        self.assertEqual(resources["limits"], resources["requests"])
        # Placement is the install's.
        self.assertNotIn("affinity", pod)

    def test_unset_knobs_keep_the_install(self):
        patch = self.patch()
        self.assertNotIn("replicas", patch["spec"])
        resources = patch["spec"]["template"]["spec"]["containers"][0]["resources"]
        self.assertEqual(resources, {"requests": {"cpu": "2"}, "limits": {"cpu": "2"}})


class ShapeTest(unittest.TestCase):
    # Postgres restarts once, then hivekeep-api rolls once and reads the
    # pool size.
    def test_order(self):
        calls = []
        stub(self, **{
            name: (lambda *_, name=name: calls.append(name))
            for name in ("enable_postgres_stats", "set_pool_max_conns", "resize_api_server", "run_no_check")
        })
        controlplane.shape(controlplane.config({"apiServer": {"cpu": 2, "poolMaxConns": 40}}))
        self.assertEqual(
            [c for c in calls if c != "run_no_check"],
            ["enable_postgres_stats", "set_pool_max_conns", "resize_api_server"],
        )

    def test_unset_pool_size_keeps_the_install(self):
        calls = []
        stub(self,
             enable_postgres_stats=lambda *_: None,
             set_pool_max_conns=calls.append,
             resize_api_server=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.shape(controlplane.config(API))
        self.assertEqual(calls, [])

    def test_passes_the_settings(self):
        got = []
        stub(self,
             enable_postgres_stats=got.append,
             resize_api_server=lambda *_: None,
             run_no_check=lambda *_: None)
        controlplane.shape(controlplane.config({**API, "postgres": {"args": ["commit_delay=100"]}}))
        self.assertEqual(got, [["commit_delay=100"]])


class PostgresExporterTest(unittest.TestCase):
    """The exporter's port must be one hivekeep-system's observability
    NetworkPolicy admits; any other name leaves the scrape timing out."""

    def admitted_ports(self):
        with open(NETWORK_POLICIES) as f:
            for doc in yaml.safe_load_all(f):
                if doc and doc["metadata"]["name"] == "hivekeep-system-observability":
                    return {p["port"] for rule in doc["spec"]["ingress"] for p in rule["ports"]}
        self.fail("no hivekeep-system-observability NetworkPolicy")

    def test_exporter_port_is_admitted(self):
        patches = []
        stub(self,
             _patch_and_wait=lambda kind, obj, patch: patches.append(patch),
             run=lambda *_, **__: None)
        controlplane.enable_postgres_stats([])
        containers = patches[0]["spec"]["template"]["spec"]["containers"]
        exporter = next(c for c in containers if c["name"] == "postgres-exporter")
        admitted = self.admitted_ports()
        for port in exporter["ports"]:
            self.assertIn(port["name"], admitted)


class PostgresArgsTest(unittest.TestCase):
    def test_base_args_first_then_settings(self):
        self.assertEqual(
            controlplane.postgres_args(["commit_delay=100", "wal_buffers=64MB"]),
            [
                "-c", "config_file=/etc/postgresql/postgresql.conf",
                "-c", "shared_preload_libraries=pg_stat_statements",
                "-c", "commit_delay=100",
                "-c", "wal_buffers=64MB",
            ],
        )


class PoolMaxConnsTest(unittest.TestCase):
    def test_patch(self):
        self.assertEqual(
            controlplane.pool_max_conns_patch(40),
            {"data": {"HK_API_POSTGRES_POOL_MAX_CONNS": "40"}},
        )


if __name__ == "__main__":
    unittest.main()
