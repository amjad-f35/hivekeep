"""Control-plane shaping for capacity runs.

A test entry's `controlPlane:` block has a required `apiServer:` section
(knobs in API_SERVER_KNOBS) and an optional `postgres:` section (defaults in
POSTGRES_DEFAULTS). A test type checks the block with validate() and applies
it with shape() from its pre_test hook.

Placement is left to the install: --cordon-system-infra-components in the
entry's installArgs keeps hivekeep-api and Postgres on their own tainted
node pools, away from the load.
"""

import json
import re
from typing import Any

from util import run, run_no_check

NAMESPACE = "hivekeep-system"
API_DEPLOYMENT = "hivekeep-api"
API_CONTAINER = "hivekeep-api"
POSTGRES_STATEFULSET = "postgres"
POSTGRES_POD = "postgres-0"
POSTGRES_DB = "hivekeeppg"

# The `apiServer:` knobs. None has a default here: the install owns
# hivekeep-api's shape, and a run overrides only what it names.
API_SERVER_KNOBS = {
    # Required: the run's independent variable. CPU request and limit of
    # each replica.
    "cpu",
    # Memory request and limit of each replica.
    "memory",
    "replicas",
    # Postgres pool size per replica, set through the API's env ConfigMap.
    "poolMaxConns",
}

# The `postgres:` knobs and their defaults.
POSTGRES_DEFAULTS = {
    # Load pg_stat_statements and run postgres_exporter beside Postgres so
    # Prometheus can scrape query statistics.
    "stats": True,
    # Extra server settings as name=value, passed as -c flags in the restart
    # that `stats` makes.
    "args": [],
}

# A Postgres setting: a lowercase name, '=', and a value without whitespace,
# which the -c flag takes verbatim.
POSTGRES_ARG_PATTERN = re.compile(r"[a-z_]+=\S+")

# A non-negative Kubernetes quantity: a decimal number with an optional
# binary-SI suffix (Ki..Ei), decimal-SI suffix (m..E) or decimal exponent.
QUANTITY_PATTERN = re.compile(r"([0-9]+(\.[0-9]*)?|\.[0-9]+)([KMGTPE]i|[mkMGTPE]|[eE][+-]?[0-9]+)?")


def config(block: dict[str, Any]) -> dict[str, Any]:
    """The block with `postgres:` merged over its defaults. `apiServer:` keeps
    only the knobs the block sets."""
    return {
        "apiServer": dict(block.get("apiServer", {})),
        "postgres": {**POSTGRES_DEFAULTS, **block.get("postgres", {})},
    }


def _check_knobs(path: str, section: Any, allowed: set[str]) -> None:
    if not isinstance(section, dict):
        raise ValueError(f"{path} must be a mapping")
    unknown = set(section) - allowed
    if unknown:
        raise ValueError(f"unknown {path} knob(s) {sorted(unknown)}; allowed: {sorted(allowed)}")


def _is_positive_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and v >= 1


def _is_quantity(v: Any) -> bool:
    """A quantity as tests.yaml writes it: 2, 0.5, "1500m", "4Gi"."""
    if isinstance(v, bool):
        return False
    if isinstance(v, (int, float)):
        return v > 0
    return isinstance(v, str) and bool(QUANTITY_PATTERN.fullmatch(v))


def validate(block: Any) -> None:
    """Raises ValueError unless block is a well-formed `controlPlane:` block.
    Messages name the offending knob by its full path."""
    if not isinstance(block, dict):
        raise ValueError("controlPlane must be a mapping")
    _check_knobs("controlPlane", block, {"apiServer", "postgres"})
    if "apiServer" not in block:
        raise ValueError("needs controlPlane.apiServer")
    _check_knobs("controlPlane.apiServer", block["apiServer"], API_SERVER_KNOBS)
    _check_knobs("controlPlane.postgres", block.get("postgres", {}), set(POSTGRES_DEFAULTS))
    cfg = config(block)
    api, pg = cfg["apiServer"], cfg["postgres"]
    if "cpu" not in api or not _is_quantity(api["cpu"]):
        raise ValueError("needs controlPlane.apiServer.cpu, as in 2 or 1500m")
    if "memory" in api and not _is_quantity(api["memory"]):
        raise ValueError("invalid controlPlane.apiServer.memory")
    for knob in ("replicas", "poolMaxConns"):
        if knob in api and not _is_positive_int(api[knob]):
            raise ValueError(f"controlPlane.apiServer.{knob} must be an int >= 1")
    if not isinstance(pg["stats"], bool):
        raise ValueError("controlPlane.postgres.stats must be a bool")
    args = pg["args"]
    if not isinstance(args, list) or not all(
        isinstance(a, str) and POSTGRES_ARG_PATTERN.fullmatch(a) for a in args
    ):
        raise ValueError(
            "controlPlane.postgres.args must be a list of name=value settings, as in commit_delay=100"
        )
    if args and not pg["stats"]:
        raise ValueError("controlPlane.postgres.args needs stats: true, whose Postgres restart applies them")


def shape(cfg: dict[str, Any]) -> None:
    """Applies the block: Postgres statistics first (one restart), then the
    pool size, then hivekeep-api's resources, whose rollout picks the pool
    size up. Nothing is undone afterwards: every test reinstalls Hivekeep."""
    api, pg = cfg["apiServer"], cfg["postgres"]
    if pg["stats"]:
        enable_postgres_stats(pg["args"])
    if "poolMaxConns" in api:
        set_pool_max_conns(api["poolMaxConns"])
    resize_api_server(api)
    # Record where the API and Postgres landed: steps compare across runs
    # only when they sat on the same nodes.
    run_no_check(
        ["kubectl", "-n", NAMESPACE, "get", "pods", "-o", "wide",
         "-l", f"app in ({API_DEPLOYMENT},postgres)"]
    )


POSTGRES_EXPORTER_IMAGE = "quay.io/prometheuscommunity/postgres-exporter:v0.17.1"


def postgres_args(settings: list[str]) -> list[str]:
    """The postgres container's args: the install's config_file (a strategic
    merge replaces args whole, so it must be restated), pg_stat_statements,
    then one -c flag per setting, which overrides postgresql.conf."""
    args = [
        "-c", "config_file=/etc/postgresql/postgresql.conf",
        "-c", "shared_preload_libraries=pg_stat_statements",
    ]
    for setting in settings:
        args += ["-c", setting]
    return args


def postgres_stats_patch(settings: list[str]) -> dict[str, Any]:
    """The StatefulSet patch that loads pg_stat_statements, applies settings,
    and adds a postgres_exporter sidecar reaching the server over the pod's
    unix socket."""
    exporter = {
        "name": "postgres-exporter",
        "image": POSTGRES_EXPORTER_IMAGE,
        "args": ["--collector.stat_statements"],
        "env": [{
            "name": "DATA_SOURCE_NAME",
            "value": f"host=/var/run/postgresql user=postgres dbname={POSTGRES_DB} sslmode=disable",
        }],
        # hivekeep-system admits scrapes only to named ports such as metrics.
        "ports": [{"name": "metrics", "containerPort": 9187}],
        "volumeMounts": [{"name": "socket", "mountPath": "/var/run/postgresql"}],
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}},
    }
    containers = [{"name": "postgres", "args": postgres_args(settings)}, exporter]
    return {"spec": {"template": {"spec": {"containers": containers}}}}


def enable_postgres_stats(settings: list[str]) -> None:
    """One patch, hence one Postgres restart, before hivekeep-api rolls."""
    _patch_and_wait("statefulset", POSTGRES_STATEFULSET, postgres_stats_patch(settings))
    run(
        ["kubectl", "-n", NAMESPACE, "exec", POSTGRES_POD, "-c", "postgres", "--",
         "psql", "-U", "postgres", "-d", POSTGRES_DB, "-v", "ON_ERROR_STOP=1",
         "-c", "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"]
    )


# The installer's ConfigMap of hivekeep-api environment variables. The API
# reads the pool size from it at start.
ENV_CONFIGMAP = "hivekeep-api-envvars"
POOL_MAX_CONNS_KEY = "HK_API_POSTGRES_POOL_MAX_CONNS"


def pool_max_conns_patch(n: int) -> dict[str, Any]:
    return {"data": {POOL_MAX_CONNS_KEY: str(n)}}


def set_pool_max_conns(n: int) -> None:
    """Sets the read/write pool size; the resize_api_server rollout that
    follows makes the API read it."""
    run(
        ["kubectl", "-n", NAMESPACE, "patch", "configmap", ENV_CONFIGMAP,
         "--type", "merge", "-p", json.dumps(pool_max_conns_patch(n))]
    )


def api_server_patch(api: dict[str, Any]) -> dict[str, Any]:
    """A strategic merge patch setting only the knobs api names, so the rest
    of the Deployment keeps the install's values. Requests equal limits."""
    resources = {"cpu": str(api["cpu"])}
    if "memory" in api:
        resources["memory"] = api["memory"]
    pod_spec = {
        "containers": [{
            "name": API_CONTAINER,
            "resources": {"requests": resources, "limits": dict(resources)},
        }]
    }
    spec: dict[str, Any] = {"template": {"spec": pod_spec}}
    if "replicas" in api:
        spec["replicas"] = api["replicas"]
    return {"spec": spec}


def resize_api_server(api: dict[str, Any]) -> None:
    _patch_and_wait("deployment", API_DEPLOYMENT, api_server_patch(api))


def _patch_and_wait(kind: str, name: str, patch: dict[str, Any]) -> None:
    run(["kubectl", "-n", NAMESPACE, "patch", kind, name, "--type=strategic", "-p", json.dumps(patch)])
    run(["kubectl", "-n", NAMESPACE, "rollout", "status", f"{kind}/{name}", "--timeout=600s"])
