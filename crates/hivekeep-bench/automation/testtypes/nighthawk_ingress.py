"""Hooks for `type: nighthawk-ingress` entries: gate router capacity at one
Envoy CPU size.

pre_test pins the gate router to the size under test; the runner Job then
drives Nighthawk's adaptive search against it (see
crates/hivekeep-bench/nighthawk-ingress/README.md).
"""

import json
import os
import re
from typing import Any

from util import build_and_push, parse_duration_seconds, run

TEST_TYPE = "nighthawk-ingress"

NAMESPACE = "hivekeep-system"
ROUTER_DEPLOYMENT = "gate-router"
ROUTER_CONTAINER = "gate-router"
ENVOY_CONTAINER = "envoy"

RUNNER_IMAGE = "hivekeep-bench-nighthawk-ingress"
RUNNER_DOCKERFILE = "crates/hivekeep-bench/nighthawk-ingress/Dockerfile"

# Defaults of the `nighthawk-ingress:` block. envoyCpu has none: it is the
# benchmark's independent variable. The README's knob table documents these.
DEFAULTS = {
    # The hive the benchmark Actors live in: one per experiment, never per
    # run, since nothing deletes hives on its own.
    "hive": "ingress-benchmark",
    # Client sizing, kept apart from envoyCpu so the client never binds first.
    "clientConcurrency": 16,
    "connections": 1000,
    "maxPendingRequests": 10000,
    "initialRps": 500,
    "exponentialFactor": 2.0,
    "measuringPeriod": "10s",
    "convergenceDeadline": "600s",
    "testingStageDuration": "60s",
    "successRateThreshold": 0.999,
    "sendRateThreshold": 0.9,
    # Bound on latency mean+2 stdev, in ms; 0 disables it.
    "tailLatencySloMs": 0,
    # How the runner authenticates to hivekeep-api: "cert" (its pod
    # identity certificate) or "token" (a projected ServiceAccount token).
    "apiClientAuth": "cert",
}

DNS_LABEL = re.compile(r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?")


def config(test: dict[str, Any]) -> dict[str, Any]:
    """The entry's `nighthawk-ingress:` block over DEFAULTS."""
    return {**DEFAULTS, **test.get("nighthawk-ingress", {})}


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "nighthawk-ingress-runner-job.yaml.tmpl")


def validate(test: dict[str, Any]) -> None:
    """Raises ValueError on a malformed entry. workerCount is required: the
    benchmark warms one Actor per worker, so it is also the fleet size."""
    name = test.get("name")
    if "duration" not in test:
        raise ValueError(f"nighthawk-ingress test {name!r} missing 'duration'")
    # A misspelt (or retired) knob must not quietly run with the default.
    allowed = set(DEFAULTS) | {"envoyCpu"}
    unknown = set(test.get("nighthawk-ingress", {})) - allowed
    if unknown:
        raise ValueError(
            f"nighthawk-ingress test {name!r} has unknown knob(s) {sorted(unknown)}; allowed: {sorted(allowed)}"
        )
    nh = config(test)
    envoy_cpu = nh.get("envoyCpu")
    if not isinstance(envoy_cpu, int) or isinstance(envoy_cpu, bool) or envoy_cpu < 1:
        raise ValueError(f"nighthawk-ingress test {name!r} needs nighthawk-ingress.envoyCpu (int >= 1)")
    concurrency = nh["clientConcurrency"]
    if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency < 1:
        raise ValueError(
            f"nighthawk-ingress test {name!r} has invalid nighthawk-ingress.clientConcurrency {concurrency!r}"
        )
    workers = test.get("workerCount")
    if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
        raise ValueError(
            f"nighthawk-ingress test {name!r} needs workerCount (int >= 1): the benchmark warms one "
            f"Actor per worker, so it is also the Actor fleet size"
        )
    for field in ("measuringPeriod", "convergenceDeadline", "testingStageDuration"):
        parse_duration_seconds(str(nh[field]))
    if not DNS_LABEL.fullmatch(str(nh["hive"])):
        raise ValueError(f"nighthawk-ingress test {name!r} hive {nh['hive']!r} must be a DNS label")
    if nh["apiClientAuth"] not in ("cert", "token"):
        raise ValueError(
            f"nighthawk-ingress test {name!r} apiClientAuth must be cert or token, got {nh['apiClientAuth']!r}"
        )


def build_image(commit: str) -> str:
    return build_and_push(f"{os.environ['HK_BUILD_REPO']}/{RUNNER_IMAGE}:{commit}", RUNNER_DOCKERFILE)


def router_patch(envoy_cpu: int) -> dict[str, Any]:
    """Pins both router containers to requests = limits = envoy_cpu
    (Guaranteed QoS) and runs Envoy with that many worker threads. Envoy's
    command is restated whole, which also drops any debug flags the install
    set."""
    cpu = str(envoy_cpu)
    pinned = {"requests": {"cpu": cpu}, "limits": {"cpu": cpu}}
    containers = [
        {"name": ROUTER_CONTAINER, "resources": pinned},
        {
            "name": ENVOY_CONTAINER,
            "command": ["/usr/local/bin/envoy", "-c", "/etc/envoy/envoy.yaml", "--concurrency", cpu],
            "resources": pinned,
        },
    ]
    return {"spec": {"template": {"spec": {"containers": containers}}}}


def pre_test(test: dict[str, Any]) -> None:
    """Pins the router and waits for the rollout. Nothing undoes it: every
    test reinstalls Hivekeep."""
    patch = router_patch(config(test)["envoyCpu"])
    run(["kubectl", "-n", NAMESPACE, "patch", "deployment", ROUTER_DEPLOYMENT,
         "--type=strategic", "-p", json.dumps(patch)])
    run(["kubectl", "-n", NAMESPACE, "rollout", "status", f"deployment/{ROUTER_DEPLOYMENT}", "--timeout=300s"])


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    nh = config(test)
    return {
        "ENVOY_CPU": nh["envoyCpu"],
        "HIVE": nh["hive"],
        # One running Actor per worker.
        "ACTORS": test["workerCount"],
        "CLIENT_CONCURRENCY": nh["clientConcurrency"],
        "CONNECTIONS": nh["connections"],
        "MAX_PENDING": nh["maxPendingRequests"],
        "INITIAL_RPS": nh["initialRps"],
        "EXP_FACTOR": nh["exponentialFactor"],
        "MEASURING_PERIOD": nh["measuringPeriod"],
        "CONVERGENCE_DEADLINE": nh["convergenceDeadline"],
        "TESTING_STAGE_DURATION": nh["testingStageDuration"],
        "SUCCESS_RATE": nh["successRateThreshold"],
        "SEND_RATE": nh["sendRateThreshold"],
        "TAIL_LATENCY_SLO_MS": nh["tailLatencySloMs"],
        # One core per event loop plus one for the Python runner.
        "RUNNER_CPU": nh["clientConcurrency"] + 1,
        "API_CLIENT_AUTH": nh["apiClientAuth"],
    }
