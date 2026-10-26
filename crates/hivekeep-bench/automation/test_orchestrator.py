"""Tests for orchestrator.py: python3 -m unittest test_orchestrator (from this
directory)."""

import json
import os
import unittest
from unittest import mock

import yaml

import orchestrator
from testtypes import locust
from testtypes import nighthawk_ingress

HERE = os.path.dirname(os.path.abspath(__file__))
NETWORK_POLICIES = os.path.join(
    HERE, "..", "..", "hivekeep-setup", "manifests", "install", "hivekeep-system-networkpolicies.yaml"
)
WORKLOADS = "crates/hivekeep-bench/workloads/deploy.sh"


class DeployWorkloadsFakeKeeperTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    def test_fake_keeper_tuning_goes_to_deploy_sh(self, run):
        orchestrator.deploy_workloads(fake_keeper_args=["--fake-keeper-env", "FAKE_KEEPER_DELAY=500ms"])
        cmd = run.call_args.args[0]
        i = cmd.index("--fake-keeper-env")
        self.assertEqual(cmd[i + 1], "FAKE_KEEPER_DELAY=500ms")


class InstallArgsTest(unittest.TestCase):
    def test_install_args_only(self):
        self.assertEqual(
            orchestrator.install_args({"installArgs": ["--cluster-size=size10"]}),
            ["--cluster-size=size10"],
        )

    def test_isolate_installs_the_fake_data_plane(self):
        self.assertEqual(
            orchestrator.install_args({"installArgs": ["--cluster-size=size10"], "isolate": {"delay": "500ms"}}),
            # The delay tunes the fake keeper through deploy.sh, not the install.
            ["--cluster-size=size10", "--fake-data-plane"],
        )

    def test_neither(self):
        self.assertEqual(orchestrator.install_args({}), [])

    def test_bare_isolate_key(self):
        self.assertEqual(orchestrator.install_args({"isolate": None}), ["--fake-data-plane"])


class DeployHivekeepTest(unittest.TestCase):
    # The runner authenticates with its pod certificate, so hivekeep-api
    # denies it unless the install makes it an owner, and the gate router
    # refuses to deploy without an allowed client SPIFFE ID.
    @mock.patch("orchestrator.run")
    def test_runner_and_client_are_bootstrap_owners(self, run):
        orchestrator.deploy_hivekeep(["--credential-provider={}"])
        cmd = run.call_args.args[0]
        self.assertEqual(
            cmd, ["scripts/install/hivekeep.sh", "deploy", "hivekeep-system", "--credential-provider={}"]
        )
        env = run.call_args.kwargs["env"]
        self.assertEqual(
            env["HK_API_AUTHZ_BOOTSTRAP_OWNERS"].split(","),
            [
                "system:serviceaccount:hivekeep-system:hivekeep-client",
                "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner",
            ],
        )
        self.assertEqual(
            env["HK_INGRESS_ALLOWED_SPIFFE_IDS"],
            "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner",
        )

    @mock.patch("orchestrator.run")
    def test_keeps_the_rest_of_the_environment(self, run):
        with mock.patch.dict(os.environ, {"HK_BUILD_REPO": "example"}):
            orchestrator.deploy_hivekeep()
        self.assertEqual(run.call_args.kwargs["env"]["HK_BUILD_REPO"], "example")

    @mock.patch("orchestrator.run_no_check")
    def test_teardown_hivekeep(self, run_no_check):
        orchestrator.teardown_hivekeep()
        run_no_check.assert_called_once_with(["scripts/install/hivekeep.sh", "delete", "hivekeep-system"])


class DeployWorkloadsTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    def test_defaults_omit_optional_flags(self, run):
        orchestrator.deploy_workloads()
        run.assert_called_once_with([WORKLOADS, "--deploy", "--worker-count", "1", "--sandbox-class", "gvisor"])

    @mock.patch("orchestrator.run")
    def test_worker_memory(self, run):
        orchestrator.deploy_workloads(worker_memory="12Gi")
        cmd = run.call_args.args[0]
        self.assertEqual(cmd[cmd.index("--worker-memory") + 1], "12Gi")

    @mock.patch("orchestrator.run")
    def test_whole_core_actor_cpu_is_a_string(self, run):
        orchestrator.deploy_workloads(actor_cpu=1)
        cmd = run.call_args.args[0]
        self.assertEqual(cmd[cmd.index("--actor-cpu") + 1], "1")

    @mock.patch("orchestrator.run")
    def test_all_options(self, run):
        orchestrator.deploy_workloads(
            worker_count=3,
            sandbox_class="microvm",
            actor_memory="1536Mi",
            wait_timeout_secs=600,
            worker_memory="5Gi",
            actor_cpu="250m",
        )
        run.assert_called_once_with([
            WORKLOADS, "--deploy",
            "--worker-count", "3",
            "--sandbox-class", "microvm",
            "--actor-memory", "1536Mi",
            "--actor-cpu", "250m",
            "--worker-memory", "5Gi",
            "--wait-timeout", "600",
        ])


class RunnerSizingTest(unittest.TestCase):
    TMPL = os.path.join(HERE, "manifests", "runner-job.yaml.tmpl")
    SUBS = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}

    def render(self, test):
        subs = {**self.SUBS, **locust.job_subs(test)}
        text = orchestrator.render_template(self.TMPL, subs)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        return job["spec"]["template"]["spec"]["containers"][0]["resources"]

    def test_defaults(self):
        res = self.render({"file": "f", "duration": "1m", "users": 1})
        self.assertEqual(res, {"requests": {"cpu": "500m", "memory": "512Mi"}})

    def test_runner_cpu_and_memory(self):
        res = self.render({"file": "f", "duration": "1m", "users": 1000, "runnerCpu": "4", "runnerMemory": "8Gi"})
        self.assertEqual(res["requests"], {"cpu": "4", "memory": "8Gi"})
        self.assertNotIn("limits", res)

    def test_no_placeholder_survives(self):
        subs = {**self.SUBS, **locust.job_subs({"file": "f", "duration": "1m", "users": 1})}
        self.assertNotIn("${", orchestrator.render_template(self.TMPL, subs))


class NighthawkJobTest(unittest.TestCase):
    TEST = {"name": "n", "type": "nighthawk-ingress", "targetCluster": "dev", "duration": "30m",
            "workerCount": 50, "nighthawk-ingress": {"envoyCpu": 2, "tailLatencySloMs": 25}}

    def test_no_placeholder_survives(self):
        nighthawk_ingress.validate(self.TEST)
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d",
                **nighthawk_ingress.job_subs(self.TEST)}
        text = orchestrator.render_template(nighthawk_ingress.job_tmpl(os.path.join(HERE, "manifests")), subs)
        self.assertNotIn("${", text)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        args = job["spec"]["template"]["spec"]["containers"][0]["args"]
        self.assertEqual(args[args.index("--hive") + 1], "ingress-benchmark")
        self.assertEqual(args[args.index("--actors") + 1], "50")

    def test_router_patch_pins_both_containers(self):
        containers = nighthawk_ingress.router_patch(4)["spec"]["template"]["spec"]["containers"]
        self.assertEqual([c["name"] for c in containers], ["gate-router", "envoy"])
        for c in containers:
            self.assertEqual(c["resources"], {"requests": {"cpu": "4"}, "limits": {"cpu": "4"}})
        self.assertEqual(containers[1]["command"][-2:], ["--concurrency", "4"])

    def test_invalid(self):
        for bad in (
            {**self.TEST, "workerCount": 0},
            {**self.TEST, "nighthawk-ingress": {}},
            {**self.TEST, "nighthawk-ingress": {"envoyCpu": 2, "envoyCPU": 2}},
            {**self.TEST, "nighthawk-ingress": {"envoyCpu": 2, "hive": "Not_A_Label"}},
            {**self.TEST, "nighthawk-ingress": {"envoyCpu": 2, "apiClientAuth": "password"}},
            {**self.TEST, "nighthawk-ingress": {"envoyCpu": 2, "measuringPeriod": "soon"}},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                nighthawk_ingress.validate(bad)


class RunnerNamespaceTest(unittest.TestCase):
    """Every runner namespace carries the label hivekeep-system's NetworkPolicy
    admits API clients by; without it the runner cannot reach hivekeep-api."""

    def api_client_labels(self):
        with open(NETWORK_POLICIES) as f:
            for doc in yaml.safe_load_all(f):
                if not doc or doc["metadata"]["name"] != "hivekeep-api":
                    continue
                for rule in doc["spec"]["ingress"]:
                    for peer in rule.get("from", []):
                        if "namespaceSelector" in peer:
                            return peer["namespaceSelector"]["matchLabels"]
        self.fail("the hivekeep-api NetworkPolicy admits no namespace by label")

    def test_namespace_has_api_access_label(self):
        want = self.api_client_labels()
        for name in ("runner-job.yaml.tmpl", "nighthawk-ingress-runner-job.yaml.tmpl"):
            with self.subTest(template=name):
                with open(os.path.join(HERE, "manifests", name)) as f:
                    text = f.read()
                ns = next(yaml.safe_load(d) for d in text.split("\n---\n") if "kind: Namespace" in d)
                self.assertEqual(ns["metadata"]["name"], "benchmarking")
                for k, v in want.items():
                    self.assertEqual(ns["metadata"].get("labels", {}).get(k), v)


class JobNameTest(unittest.TestCase):
    COMMIT = "ac41c06deadbeef"

    def test_short_name_keeps_full_test_name(self):
        self.assertRegex(orchestrator.job_name("Eng Review", self.COMMIT), r"^runner-eng-review-ac41c06-[0-9a-f]{6}$")

    def test_long_name_fits_a_label_and_keeps_suffix(self):
        test = "very-long-benchmark-name-that-overflows-the-kubernetes-label-limit"
        name = orchestrator.job_name(test, self.COMMIT)
        self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
        self.assertRegex(name, r"-ac41c06-[0-9a-f]{6}$")
        self.assertTrue(name.startswith("runner-very-long-benchmark-name"))

    def test_truncation_never_leaves_a_double_hyphen(self):
        for i in range(1, 80):
            test = "-".join(["ab"] * i)
            name = orchestrator.job_name(test, self.COMMIT)
            self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
            self.assertNotIn("--", name, test)
            self.assertRegex(name, r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

    def test_two_runs_of_the_same_long_test_differ(self):
        test = "x" * 100
        self.assertNotEqual(orchestrator.job_name(test, self.COMMIT), orchestrator.job_name(test, self.COMMIT))


CLUSTER_ENV = {"CLUSTER_NAME": "c", "CLUSTER_LOCATION": "l", "PROJECT_ID": "p"}


class RunMetadataTest(unittest.TestCase):
    def test_cluster_env(self):
        with mock.patch.dict(os.environ, CLUSTER_ENV, clear=True):
            self.assertEqual(
                orchestrator.run_metadata(),
                {"cluster_name": "c", "cluster_location": "l", "project_id": "p"},
            )

    def test_benchmark_metadata_is_merged(self):
        with mock.patch.dict(os.environ, {**CLUSTER_ENV, "BENCHMARK_METADATA": '{"build_id": "1"}'}, clear=True):
            self.assertEqual(orchestrator.run_metadata()["build_id"], "1")

    def test_bad_benchmark_metadata(self):
        for bad in ("not json", "[1]", '"text"'):
            env = {**CLUSTER_ENV, "BENCHMARK_METADATA": bad}
            with self.subTest(bad=bad), mock.patch.dict(os.environ, env, clear=True), \
                    self.assertRaisesRegex(ValueError, "BENCHMARK_METADATA"):
                orchestrator.run_metadata()

    def submitted_args(self, ttype, flags=("--x",)):
        """The extra args run_test hands to render_template for a type."""

        class Rendered(Exception):
            pass

        fake = mock.Mock(job_tmpl=mock.Mock(return_value="t"), job_subs=mock.Mock(return_value={}))
        test = {"name": "n", "type": ttype, "duration": "1m", "flags": list(flags)}
        with mock.patch.dict(orchestrator.TYPES, {ttype: fake}), \
                mock.patch.dict(os.environ, CLUSTER_ENV, clear=True), \
                mock.patch.object(orchestrator, "render_template", side_effect=Rendered) as render, \
                self.assertRaises(Rendered):
            orchestrator.run_test(test, "img", "dest", "abcdef0123")
        return render.call_args.args[2]

    def test_locust_job_gets_metadata(self):
        args = self.submitted_args("locust")
        self.assertEqual(args[:2], ["--x", "--metadata"])
        self.assertEqual(json.loads(args[2])["cluster_name"], "c")

    def test_other_types_do_not(self):
        self.assertEqual(self.submitted_args("nighthawk-ingress"), ["--x"])

    def test_test_metadata_is_merged_not_overridden(self):
        for own in (["--metadata", '{"cluster_name": "mine", "suite": "s"}'],
                    ['--metadata={"cluster_name": "mine", "suite": "s"}']):
            with self.subTest(own=own):
                args = self.submitted_args("locust", ["--x", *own, "--y"])
                self.assertEqual(args.count("--metadata"), 1)
                self.assertFalse([a for a in args if a.startswith("--metadata=")])
                self.assertEqual(args[:3], ["--x", "--y", "--metadata"])
                md = json.loads(args[3])
                # The entry's keys win; run keys it does not set are kept.
                self.assertEqual(md["cluster_name"], "mine")
                self.assertEqual(md["suite"], "s")
                self.assertEqual(md["project_id"], "p")

    def test_bad_test_metadata_fails_clearly(self):
        for own in (["--metadata", "not json"], ["--metadata=[1]"], ["--metadata"]):
            with self.subTest(own=own), self.assertRaisesRegex(ValueError, "test 'n': --metadata"):
                orchestrator.split_metadata_flag({"name": "n", "flags": own})
        # Caught by validation, before any cluster work.
        test = {"name": "n", "type": "locust", "targetCluster": "t", "file": "f", "duration": "1m",
                "users": 1, "flags": ["--metadata", "{bad"]}
        with self.assertRaisesRegex(ValueError, "not valid JSON"):
            orchestrator.validate_and_normalize_tests([test])

    def test_metadata_survives_yaml(self):
        md = json.dumps({"cluster_name": "c: d", "q": "\"'"})
        subs = {**RunnerSizingTest.SUBS, **locust.job_subs({"file": "f", "duration": "1m", "users": 1})}
        text = orchestrator.render_template(RunnerSizingTest.TMPL, subs, ["--metadata", md])
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        self.assertEqual(job["spec"]["template"]["spec"]["containers"][0]["args"][-1], md)


class ValidateTestsTest(unittest.TestCase):
    BASE = {"name": "n", "type": "locust", "targetCluster": "dev", "file": "f", "duration": "1m", "users": 1}

    def test_shared_fields(self):
        orchestrator.validate_and_normalize_tests([self.BASE])
        for bad in (
            {k: v for k, v in self.BASE.items() if k != "targetCluster"},
            {k: v for k, v in self.BASE.items() if k != "type"},
            {**self.BASE, "type": "k6"},
            {**self.BASE, "sandboxClass": "firecracker"},
            {**self.BASE, "isolate": {"delay": "fast"}},
        ):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                orchestrator.validate_and_normalize_tests([bad])


if __name__ == "__main__":
    unittest.main()
