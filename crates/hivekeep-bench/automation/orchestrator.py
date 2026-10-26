"""Hivekeep benchmark orchestrator.

Runs the type-independent part of every benchmark; each test type's own part
lives in a testtypes/ module (see that package for the hooks). In order:

  1. Wait for the Docker sidecar and clone the branch under test once.
  2. Check every tests.yaml entry before touching a cluster [validate].
  3. For each entry:
     a. Load the target cluster's settings. Cluster credentials and the
        runner image [build_image] are cached per target cluster.
     b. Sweep leftovers, install Hivekeep, let the type shape the cluster
        [pre_test], deploy the benchmark workloads (with the micro-VM
        dependencies first when the entry's sandbox class needs them).
     c. Render the type's Job [job_tmpl, job_subs], submit it, wait, print
        its log, delete it.
     d. Tear the workloads and Hivekeep down again, so no entry sees another
        one's state, and drop the settings file so the next entry cannot
        inherit this cluster.
  4. Exit non-zero if any entry did not complete.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml

from testtypes import TYPES
from util import (
    isolate_block,
    isolate_install_args,
    isolate_workload_args,
    parse_duration_seconds,
    run,
    run_no_check,
)

REPO_DIR = "/workspace/hivekeep"
TARGET_CLUSTER_DIR = "/etc/orchestrator/target-clusters"
# The settings file the installer and the benchmark scripts read at the
# repository root.
ENV_FILE_NAME = ".hivekeep-dev-env.sh"
MANIFESTS_DIR = "/opt/automation/manifests"
NAMESPACE = "benchmarking"
RUNNER_APP_LABEL = "hivekeep-benchmark-runner"

INSTALLER = "scripts/install/hivekeep.sh"
SYSTEM_NAMESPACE = "hivekeep-system"
MICROVM_DEPS = "scripts/install-microvm-deps.sh"
WORKLOADS_DEPLOY = "crates/hivekeep-bench/workloads/deploy.sh"

TEST_TYPES = tuple(TYPES)
SANDBOX_CLASSES = ("gvisor", "microvm")

# Settings a target cluster file must define.
REQUIRED_CLUSTER_ENV = ("PROJECT_ID", "CLUSTER_NAME", "CLUSTER_LOCATION", "HK_BUILD_REPO")

# The process environment at startup. Each target cluster's settings are
# applied on top of this, never on top of the previous cluster's, so a value
# one cluster sets cannot leak into the next.
_ORIG_ENV = dict(os.environ)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo", required=True, help="Git URL of the repository to benchmark")
    p.add_argument("--branch", default="main", help="branch to benchmark")
    p.add_argument("--dest", required=True, help="results root, handed to each runner as --dest")
    p.add_argument("--tests", default="/etc/orchestrator/tests.yaml", help="the tests file")
    p.add_argument("--target-cluster-dir", default=TARGET_CLUSTER_DIR,
                   help="directory of target cluster settings files (<name>.sh)")
    p.add_argument("--manifests-dir", default=MANIFESTS_DIR, help="directory of the runner Job templates")
    p.add_argument("--junit-output", help="write a JUnit XML summary of the results here")
    return p.parse_args()


def source_env(path: str) -> None:
    """Merges the variables a shell settings file exports into os.environ."""
    result = subprocess.run(
        ["bash", "-c", f'set -a; source "{path}"; env'],
        check=True, capture_output=True, text=True,
    )
    for line in result.stdout.splitlines():
        k, sep, v = line.partition("=")
        if sep:
            os.environ[k] = v


def apply_target_cluster(target_cluster: str, target_cluster_dir: str = TARGET_CLUSTER_DIR) -> None:
    """Installs <dir>/<name>.sh as the clone's settings file, where the
    installer and the deploy scripts read it, and loads it into this
    process for the orchestrator's own gcloud, docker and kubectl calls."""
    src = Path(target_cluster_dir) / f"{target_cluster}.sh"
    if not src.exists():
        raise FileNotFoundError(f"target cluster {target_cluster!r} not found at {src}")
    os.environ.clear()
    os.environ.update(_ORIG_ENV)
    dst = Path(REPO_DIR) / ENV_FILE_NAME
    shutil.copy(src, dst)
    source_env(str(dst))
    for k in REQUIRED_CLUSTER_ENV:
        if not os.environ.get(k):
            raise RuntimeError(f"{k} not set after sourcing target cluster {target_cluster!r}")


def clear_target_cluster() -> None:
    (Path(REPO_DIR) / ENV_FILE_NAME).unlink(missing_ok=True)


def registry_host(build_repo: str) -> str:
    return build_repo.split("/", 1)[0]


def gcloud_setup_for_target_cluster() -> None:
    """Docker credentials for the cluster's registry, and kubectl pointed at
    the cluster."""
    run(["gcloud", "auth", "configure-docker", registry_host(os.environ["HK_BUILD_REPO"]), "--quiet"])
    run(["gcloud", "container", "clusters", "get-credentials", os.environ["CLUSTER_NAME"],
         "--location", os.environ["CLUSTER_LOCATION"], "--project", os.environ["PROJECT_ID"]])


def test_type(test: dict[str, Any]) -> str:
    return test["type"]


def _json_object(raw: str, what: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"{what} is not valid JSON: {e}") from e
    if not isinstance(value, dict):
        raise ValueError(f"{what} must be a JSON object")
    return value


def run_metadata() -> dict[str, Any]:
    """Facts about the run handed to the locust runner as --metadata: the
    target cluster's name, location and project, plus BENCHMARK_METADATA (a
    JSON object), which survives the per-cluster reset because it is part
    of the startup environment."""
    md: dict[str, Any] = {
        k.lower(): os.environ[k]
        for k in ("CLUSTER_NAME", "CLUSTER_LOCATION", "PROJECT_ID")
        if os.environ.get(k)
    }
    raw = os.environ.get("BENCHMARK_METADATA")
    if raw:
        md.update(_json_object(raw, "BENCHMARK_METADATA"))
    return md


def split_metadata_flag(test: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    """Splits an entry's own --metadata out of its flags.

    Takes `--metadata <json>` and `--metadata=<json>`, any number of times;
    later keys win. A missing value or one that is not a JSON object raises
    ValueError instead of being dropped."""
    name = test.get("name")
    rest: list[Any] = []
    md: dict[str, Any] = {}
    flags = list(test.get("flags", []))
    i = 0
    while i < len(flags):
        flag = str(flags[i])
        if flag == "--metadata":
            if i + 1 >= len(flags):
                raise ValueError(f"test {name!r}: --metadata needs a JSON value")
            raw, i = str(flags[i + 1]), i + 2
        elif flag.startswith("--metadata="):
            raw, i = flag.split("=", 1)[1], i + 1
        else:
            rest.append(flags[i])
            i += 1
            continue
        md.update(_json_object(raw, f"test {name!r}: --metadata"))
    return rest, md


def wait_for_docker(timeout: int = 120) -> None:
    print("Waiting for the Docker sidecar...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if subprocess.run(["docker", "info"], capture_output=True).returncode == 0:
            print("Docker ready.", flush=True)
            return
        time.sleep(2)
    raise RuntimeError("the Docker sidecar did not become ready in time")


def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")


# A Job's name is copied into the job-name label of the Job and its pods, and
# label values hold at most 63 bytes.
MAX_JOB_NAME_LEN = 63


def job_name(name: str, commit: str) -> str:
    """A unique Job name for one run of a test that fits in a label value.
    The commit and a random suffix are always kept; a long test name is cut
    to make room, so two runs of one test never collide."""
    suffix = f"-{commit[:7]}-{uuid.uuid4().hex[:6]}"
    prefix = f"runner-{sanitize(name)}"
    return prefix[: MAX_JOB_NAME_LEN - len(suffix)].rstrip("-") + suffix


def render_template(path: str, subs: dict[str, Any], extra_args: Iterable[str] = ()) -> str:
    """Fills ${KEY} placeholders, then appends extra_args to the Job's first
    container. The args are added through YAML, not text, so values with
    quotes or colons (JSON metadata) survive."""
    text = Path(path).read_text()
    for k, v in subs.items():
        text = text.replace("${" + k + "}", str(v))
    extra = [str(a) for a in extra_args]
    if not extra:
        return text
    docs = list(yaml.safe_load_all(text))
    for doc in docs:
        if doc and doc.get("kind") == "Job":
            doc["spec"]["template"]["spec"]["containers"][0]["args"].extend(extra)
    return yaml.safe_dump_all(docs)


def _kubectl_json(args: list[str]) -> dict[str, Any] | None:
    r = subprocess.run(["kubectl", *args, "-o", "json"], capture_output=True, text=True, check=False)
    if r.returncode != 0:
        print(f"kubectl {' '.join(args)} failed: {r.stderr}", flush=True)
        return None
    return json.loads(r.stdout)


def wait_for_no_active_runners(timeout: int = 300) -> None:
    """Waits until no earlier runner Job is still going; two runs at once
    would measure each other."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        jobs = _kubectl_json(["get", "jobs", "-n", NAMESPACE, "-l", f"app={RUNNER_APP_LABEL}"])
        if jobs is None:
            time.sleep(5)
            continue
        active = [
            j["metadata"]["name"]
            for j in jobs.get("items", [])
            if j.get("status", {}).get("succeeded", 0) == 0 and j.get("status", {}).get("failed", 0) == 0
        ]
        if not active:
            return
        print(f"Waiting for runner Jobs still in progress: {active}", flush=True)
        time.sleep(10)
    raise RuntimeError(f"runner Jobs still active after {timeout}s; aborting")


def wait_for_job(name: str, timeout_seconds: int) -> str:
    """complete, failed or timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        job = _kubectl_json(["get", "job", name, "-n", NAMESPACE])
        if job is None:
            time.sleep(5)
            continue
        status = job.get("status", {})
        if status.get("succeeded", 0) >= 1:
            return "complete"
        if status.get("failed", 0) >= 1:
            return "failed"
        time.sleep(10)
    return "timeout"


def validate_and_normalize_tests(tests: list[dict[str, Any]]) -> None:
    """Raises ValueError on the first malformed entry, before any cluster
    work: the fields every type shares here, the rest in the type's own
    validate hook."""
    for t in tests:
        name = t.get("name")
        if not t.get("targetCluster"):
            raise ValueError(f"test {name!r} missing required 'targetCluster' field")
        sandbox_class = t.get("sandboxClass", "gvisor")
        if sandbox_class not in SANDBOX_CLASSES:
            raise ValueError(
                f"test {name!r} has invalid sandboxClass {sandbox_class!r} (want one of {list(SANDBOX_CLASSES)})"
            )
        if "type" not in t:
            raise ValueError(f"test {name!r} missing required 'type' field (one of {list(TEST_TYPES)})")
        ttype = test_type(t)
        if ttype not in TEST_TYPES:
            raise ValueError(f"test {name!r} has invalid type {ttype!r} (want one of {list(TEST_TYPES)})")
        TYPES[ttype].validate(t)
        if ttype == "locust":
            # A malformed --metadata fails now, not in the middle of a sweep.
            split_metadata_flag(t)
        isolate_block(t)


def install_args(test: dict[str, Any]) -> list[str]:
    """The `deploy hivekeep-system` arguments for an entry: its installArgs,
    then the fake data plane flag its isolate block asks for. Teardown needs
    no counterpart: deleting hivekeep-system removes the fake data plane
    either way."""
    return [*(str(a) for a in test.get("installArgs", [])), *isolate_install_args(test)]


# hivekeep-api enforces authorization, and the runner calls it with its pod
# certificate under BENCHMARK_RUNNER_PRINCIPAL. Every entry reinstalls
# Hivekeep, so naming the runner a bootstrap owner of each install grants it
# owner rights for exactly that install's life. The hivectl client principal
# stays an owner for the workload and teardown steps. The gate router also
# needs an allowlist of client SPIFFE IDs on every install.
BENCHMARK_RUNNER_PRINCIPAL = "spiffe://cluster.local/ns/benchmarking/sa/benchmark-runner"
CLIENT_PRINCIPAL = f"system:serviceaccount:{SYSTEM_NAMESPACE}:hivekeep-client"


def deploy_hivekeep(args: Iterable[str] = ()) -> None:
    env = {
        **os.environ,
        "HK_API_AUTHZ_BOOTSTRAP_OWNERS": f"{CLIENT_PRINCIPAL},{BENCHMARK_RUNNER_PRINCIPAL}",
        "HK_INGRESS_ALLOWED_SPIFFE_IDS": BENCHMARK_RUNNER_PRINCIPAL,
    }
    run([INSTALLER, "deploy", SYSTEM_NAMESPACE, *(str(a) for a in args)], env=env)


def teardown_hivekeep() -> None:
    run_no_check([INSTALLER, "delete", SYSTEM_NAMESPACE])


def install_microvm_deps() -> None:
    """Stages the micro-VM runtime assets and applies the cluster-wide
    microvm SandboxConfig. Needs the CRDs, so it runs after deploy_hivekeep."""
    run([MICROVM_DEPS, "--install"])


def teardown_microvm_deps() -> None:
    """Removes the microvm SandboxConfig. Runs before teardown_hivekeep,
    which deletes the CRD this needs."""
    run_no_check([MICROVM_DEPS, "--delete"])


def deploy_workloads(
    worker_count: int = 1,
    sandbox_class: str = "gvisor",
    actor_memory: str = "",
    wait_timeout_secs: int | str = "",
    worker_memory: str = "",
    fake_keeper_args: Iterable[str] = (),
    actor_cpu: str | int = "",
) -> None:
    """Runs workloads/deploy.sh --deploy. An empty option keeps deploy.sh's
    own default (256Mi Actor memory, 1 Actor CPU, unsized worker pods, a
    300 s wait). deploy.sh itself waits for every template's golden
    snapshot."""
    cmd = [WORKLOADS_DEPLOY, "--deploy", "--worker-count", str(worker_count), "--sandbox-class", sandbox_class]
    if actor_memory:
        cmd += ["--actor-memory", actor_memory]
    # str(): a whole number of cores (actorCPU: 1) loads as an int.
    if actor_cpu:
        cmd += ["--actor-cpu", str(actor_cpu)]
    if worker_memory:
        cmd += ["--worker-memory", worker_memory]
    if wait_timeout_secs != "":
        cmd += ["--wait-timeout", str(wait_timeout_secs)]
    cmd += list(fake_keeper_args)
    run(cmd)


def teardown_workloads() -> None:
    run_no_check([WORKLOADS_DEPLOY, "--delete"])


def run_test(
    test: dict[str, Any],
    image: str,
    dest: str,
    commit: str,
    manifests_dir: str = MANIFESTS_DIR,
) -> str:
    """Submits the entry's runner Job, waits for it, prints its log and
    deletes it. Returns complete, failed or timeout."""
    name = test["name"]
    job = job_name(name, commit)
    subs = {"JOB_NAME": job, "IMAGE": image, "TAG": commit, "NAME": name, "DEST": dest}
    mod = TYPES[test_type(test)]
    tmpl = mod.job_tmpl(manifests_dir)
    subs.update(mod.job_subs(test))
    flags = list(test.get("flags", []))
    # Only the locust runner takes --metadata, and it keeps the last one it
    # sees, so the entry's own metadata is merged into one flag, its keys
    # winning over the run's.
    if test_type(test) == "locust":
        flags, own = split_metadata_flag(test)
        flags += ["--metadata", json.dumps({**run_metadata(), **own})]
    manifest = render_template(tmpl, subs, flags)
    wait_for_no_active_runners()
    print(f"Submitting Job {job}", flush=True)
    subprocess.run(["kubectl", "apply", "-f", "-"], input=manifest, text=True, check=True)
    # The entry's duration plus half an hour for setup and upload.
    result = wait_for_job(job, parse_duration_seconds(test["duration"]) + 1800)
    print(f"Job {job} result: {result}", flush=True)
    run_no_check(["kubectl", "logs", f"job/{job}", "-n", NAMESPACE, "--tail=500"])
    run_no_check(["kubectl", "delete", "job", job, "-n", NAMESPACE])
    return result


def write_junit_xml(
    path: str,
    results: list[tuple[str, str, float, str | None]],
    classname: str = "hivekeep.benchmarks",
) -> None:
    failures = sum(1 for _, status, _, _ in results if status != "complete")
    suite = ET.Element(
        "testsuite",
        name="hivekeep-benchmarks",
        tests=str(len(results)),
        failures=str(failures),
        errors="0",
        time=f"{sum(d for _, _, d, _ in results):.2f}",
    )
    for name, status, duration, failure_msg in results:
        case = ET.SubElement(suite, "testcase", name=name, classname=classname, time=f"{duration:.2f}")
        if status != "complete":
            msg = failure_msg or f"Test failed with status: {status}"
            ET.SubElement(case, "failure", message=msg).text = msg
    tree = ET.ElementTree(suite)
    ET.indent(tree, space="  ", level=0)
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tree.write(str(out), encoding="utf-8", xml_declaration=True)


def teardown_all() -> None:
    """Workloads, then the micro-VM SandboxConfig (it needs the CRD the
    Hivekeep teardown removes), then Hivekeep. Every step tolerates absence,
    so this is cheap on a clean cluster."""
    teardown_workloads()
    teardown_microvm_deps()
    teardown_hivekeep()


def run_entry(test: dict[str, Any], image: str, args: argparse.Namespace, commit: str) -> tuple[str, float, str | None]:
    """Installs, runs and tears down one entry; returns its status, wall
    time and failure message."""
    ttype = test_type(test)
    sandbox_class = test.get("sandboxClass", "gvisor")
    status, failure_msg = "error", None
    start = time.time()
    try:
        deploy_hivekeep(install_args(test))
        TYPES[ttype].pre_test(test)
        if sandbox_class == "microvm":
            install_microvm_deps()
        deploy_workloads(
            test.get("workerCount", 1),
            sandbox_class,
            test.get("actorMemory", ""),
            test.get("workerWaitTimeout", ""),
            test.get("workerMemory", ""),
            isolate_workload_args(test),
            test.get("actorCPU", ""),
        )
        try:
            status = run_test(test, image, args.dest, commit, args.manifests_dir)
            if status != "complete":
                failure_msg = f"Test finished with status: {status}"
        except Exception as e:
            print(f"Test {test['name']} crashed: {e}", flush=True)
            failure_msg = str(e)
    except Exception as e:
        print(f"Test {test['name']} setup failed: {e}", flush=True)
        failure_msg = str(e)
    finally:
        # Always, so the next entry (and the next scheduled run) starts clean.
        teardown_all()
    return status, time.time() - start, failure_msg


def main() -> None:
    args = parse_args()
    wait_for_docker()
    run(["git", "clone", "--depth", "1", "--branch", args.branch, args.repo, REPO_DIR])
    os.chdir(REPO_DIR)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    print(f"Benchmarking commit {commit}", flush=True)

    tests = yaml.safe_load(Path(args.tests).read_text())["tests"]
    print(f"Running {len(tests)} test(s)", flush=True)
    try:
        validate_and_normalize_tests(tests)
        # Only BENCHMARK_METADATA can be checked yet; no cluster is loaded.
        run_metadata()
    except ValueError as e:
        sys.exit(str(e))

    # Credentials and runner images are reused while consecutive entries
    # target the same cluster. Images are built per type on first use, so a
    # locust-only tests file never builds the Nighthawk image.
    last_target = None
    images: dict[str, str] = {}
    results: list[tuple[str, str, float, str | None]] = []
    for i, test in enumerate(tests):
        target = test["targetCluster"]
        print(f"\n=== test {i + 1}/{len(tests)}: {test['name']} (targetCluster={target}) ===", flush=True)
        try:
            apply_target_cluster(target, args.target_cluster_dir)
        except Exception as e:
            print(f"Failed to apply target cluster {target!r}: {e}", flush=True)
            results.append((test["name"], "config-error", 0.0, str(e)))
            continue
        try:
            if target != last_target:
                gcloud_setup_for_target_cluster()
                images = {}
                last_target = target
            ttype = test_type(test)
            if ttype not in images:
                images[ttype] = TYPES[ttype].build_image(commit)
            # A run that crashed mid-test may have left its install behind.
            teardown_all()
            status, duration, failure_msg = run_entry(test, images[ttype], args, commit)
            results.append((test["name"], status, duration, failure_msg))
        finally:
            clear_target_cluster()

    print("\n=== summary ===", flush=True)
    for name, status, duration, _ in results:
        print(f"  {name}: {status} ({duration:.2f}s)", flush=True)
    if args.junit_output:
        print(f"Writing JUnit XML report to {args.junit_output}", flush=True)
        write_junit_xml(args.junit_output, results)
    sys.exit(1 if any(status != "complete" for _, status, _, _ in results) else 0)


if __name__ == "__main__":
    main()
