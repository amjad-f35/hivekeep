"""Small helpers the orchestrator and the test-type modules share."""

import re
import shlex
import subprocess
import time


def _echo(cmd: list[str]) -> None:
    print(f"$ {' '.join(shlex.quote(c) for c in cmd)}", flush=True)


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Runs cmd, echoing it and its wall time; raises on a non-zero exit."""
    _echo(cmd)
    start = time.monotonic()
    try:
        return subprocess.run(cmd, check=True, **kwargs)
    finally:
        print(f"  (took {time.monotonic() - start:.1f}s)", flush=True)


def run_no_check(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Like run, but a failing command is only reported, never raised."""
    _echo(cmd)
    start = time.monotonic()
    try:
        return subprocess.run(cmd, check=False, **kwargs)
    finally:
        print(f"  (took {time.monotonic() - start:.1f}s)", flush=True)


def build_and_push(image: str, dockerfile: str) -> str:
    """Builds image from the repository root for linux/amd64, the test
    clusters' architecture, pushes it, and returns its reference."""
    run(["docker", "build", "--platform", "linux/amd64", "-t", image, "-f", dockerfile, "."])
    run(["docker", "push", image])
    return image


_DURATION = re.compile(r"(\d+)\s*([smh]?)")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}


def parse_duration_seconds(s: str) -> int:
    """Parses the tests.yaml duration grammar, <int>[s|m|h] (seconds when no
    unit is given), into whole seconds."""
    m = _DURATION.fullmatch(s.strip())
    if not m:
        raise ValueError(f"unrecognized duration: {s}")
    return int(m.group(1)) * _UNIT_SECONDS[m.group(2) or "s"]


# Each key a tests.yaml `isolate:` block may hold, and the fake keeper tuning
# variable it sets (passed on through workloads/deploy.sh --fake-keeper-env).
# The variable names are FAKE_KEEPER_<FLAG>; the fake keeper ignores a name it
# does not know, so this table is checked against the fake keeper's own list
# by crates/hivekeep-bench/tests/isolate_tuning.rs.
ISOLATE_TUNING = {
    "delay": "FAKE_KEEPER_DELAY",
    "delayRun": "FAKE_KEEPER_DELAY_RUN",
    "delayRestore": "FAKE_KEEPER_DELAY_RESTORE",
    "delayCheckpoint": "FAKE_KEEPER_DELAY_CHECKPOINT",
    "delayPromoteSnapshot": "FAKE_KEEPER_DELAY_PROMOTE_SNAPSHOT",
    "delayTerminate": "FAKE_KEEPER_DELAY_TERMINATE",
    "mintActorCertificate": "FAKE_KEEPER_MINT_ACTOR_CERTIFICATE",
}

# A non-negative duration the fake keeper accepts for its delays: a bare 0,
# or one or more <number><unit> parts such as 1m30s or 1.5s.
ISOLATE_DELAY_PATTERN = re.compile(r"0|([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+")


def isolate_block(test: dict) -> dict | None:
    """The entry's `isolate:` block, or None when it has none.

    A bare `isolate:` (which YAML loads as None) selects the fake data plane
    with every default. An unknown key, or a value the fake keeper would
    reject at startup, raises ValueError here, before any cluster work,
    rather than leaving a crash-looping fake keeper behind."""
    if "isolate" not in test:
        return None
    block = test["isolate"] if test["isolate"] is not None else {}
    if not isinstance(block, dict):
        raise ValueError(f"isolate must be a mapping, got {block!r}")
    unknown = sorted(set(block) - set(ISOLATE_TUNING))
    if unknown:
        raise ValueError(f"isolate has unknown keys {unknown} (want some of {sorted(ISOLATE_TUNING)})")
    for key, value in block.items():
        if key == "mintActorCertificate":
            if not isinstance(value, bool):
                raise ValueError(f"isolate.{key} must be true or false, got {value!r}")
        elif not isinstance(value, str) or not ISOLATE_DELAY_PATTERN.fullmatch(value):
            raise ValueError(f"isolate.{key} must be a duration string such as 500ms, got {value!r}")
    return block


def isolate_install_args(test: dict) -> list[str]:
    """The installer flags an `isolate:` block needs: the installer only picks
    the data plane, the tuning goes to workloads/deploy.sh."""
    return [] if isolate_block(test) is None else ["--fake-data-plane"]


def isolate_workload_args(test: dict) -> list[str]:
    """The workloads/deploy.sh flags that tune the fake keeper for an
    `isolate:` block, in ISOLATE_TUNING order; none without a block."""
    block = isolate_block(test) or {}
    args: list[str] = []
    for key, env in ISOLATE_TUNING.items():
        if key not in block:
            continue
        value = block[key]
        if isinstance(value, bool):
            value = "true" if value else "false"
        args += ["--fake-keeper-env", f"{env}={value}"]
    return args
