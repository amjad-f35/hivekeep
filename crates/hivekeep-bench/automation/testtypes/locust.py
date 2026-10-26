"""Hooks for `type: locust` entries.

The runner Job runs the locust runner (crates/hivekeep-bench/locust/runner.py),
which starts the locust master and the Rust load-generator worker and uploads
its own results.

An optional `controlPlane:` block sizes hivekeep-api and Postgres for a
capacity run (see controlplane.py); without one, pre_test leaves the cluster
as installed.
"""

import os
from typing import Any

import controlplane
from util import build_and_push

TEST_TYPE = "locust"

RUNNER_IMAGE = "hivekeep-bench-locust"
RUNNER_DOCKERFILE = "crates/hivekeep-bench/locust/Dockerfile"

# Runner pod requests when the entry sets none: enough for the glutton and
# sweperf suites at their usual user counts. Classes run at thousands of
# users (agentsession holds a large ingest payload per in-flight call) need
# runnerCpu / runnerMemory raised in tests.yaml.
DEFAULT_RUNNER_CPU = "500m"
DEFAULT_RUNNER_MEMORY = "512Mi"

REQUIRED_FIELDS = ("file", "duration", "users")


def control_plane_config(test: dict[str, Any]) -> dict[str, Any] | None:
    """The entry's `controlPlane:` block over its defaults, or None."""
    if "controlPlane" not in test:
        return None
    return controlplane.config(test["controlPlane"])


def validate(test: dict[str, Any]) -> None:
    name = test.get("name")
    for field in REQUIRED_FIELDS:
        if field not in test:
            raise ValueError(f"locust test {name!r} missing {field!r}")
    if "controlPlane" in test:
        try:
            controlplane.validate(test["controlPlane"])
        except ValueError as e:
            raise ValueError(f"locust test {name!r}: {e}") from None


def build_image(commit: str) -> str:
    """Builds and pushes the runner image into HK_BUILD_REPO, which must be a
    registry the test cluster can pull from."""
    return build_and_push(f"{os.environ['HK_BUILD_REPO']}/{RUNNER_IMAGE}:{commit}", RUNNER_DOCKERFILE)


def pre_test(test: dict[str, Any]) -> None:
    cfg = control_plane_config(test)
    if cfg is not None:
        controlplane.shape(cfg)


def job_tmpl(manifests_dir: str) -> str:
    return os.path.join(manifests_dir, "runner-job.yaml.tmpl")


def job_subs(test: dict[str, Any]) -> dict[str, Any]:
    return {
        "TEST_FILE": test["file"],
        "DURATION": test["duration"],
        "USERS": test["users"],
        "RUNNER_CPU": test.get("runnerCpu", DEFAULT_RUNNER_CPU),
        "RUNNER_MEMORY": test.get("runnerMemory", DEFAULT_RUNNER_MEMORY),
    }
