"""The runtime config the master hands to locust-worker.

The locust wire protocol carries only the user count and spawn rate, so the
worker reads every other knob over HTTP: a JSON object keyed by locust flag
name in snake_case (`--min-wait-time` -> `min_wait_time`), served by the
master at /loadgen-config. The worker lays each payload over its class's
defaults, refuses one the class cannot run on, and ignores keys the class
does not read.

The flags themselves are registered by the module that owns each one:
  * --trace-probability                 common.trace.init_tracing
  * --min/max-wait-time, --min/max-live-time
                                        common.wait_time.init_wait_time
  * --resume-mode                       common.resume_mode
  * --lifecycle-mode                    common.lifecycle_mode
  * --durdir-*                          common.durdir_config
  * --mem-target/--mem-churn/--mem-read common.memload_config
  * --cpu-cores/--cpu-duty-cycle        common.cpuload_config
  * --max-pings-per-wake                common.ping_config
  * --target-rps                        common.rate_config
  * --total-actors/--spawn-concurrency/--actor-deadline
                                        common.spawn_config
  * --sweperf-*                         common.sweperf_config
  * --agentsession-*                    common.agentsession_config

This module serves them in three ways:
  * init_loadgen_config() makes sure the owners registered their flags and,
    on a master with a web UI, adds /loadgen-config to it, so the values an
    operator types into the form reach the worker.
  * build_config_json() turns an argv list into the payload, for the
    worker's --config-json in a headless run.
  * serve_config_headless() serves the same payload from a small loopback
    server, for a headless run whose values change while it runs.

WORKER_FLAGS is every key the payload can carry. A flag added here needs a
matching field in the worker class that reads it.
"""

import argparse
import json
import logging
import threading
from collections.abc import Iterable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger(__name__)

CONFIG_PATH = "/loadgen-config"

WORKER_FLAGS = {
    "--trace-probability": float,
    "--min-wait-time": float,
    "--max-wait-time": float,
    "--min-live-time": float,
    "--max-live-time": float,
    "--durdir-file-size-bytes": int,
    "--resume-mode": str,
    "--lifecycle-mode": str,
    "--durdir-read-mode": str,
    "--durdir-template": str,
    "--mem-target": str,
    "--mem-churn": str,
    "--mem-read": str,
    "--cpu-cores": int,
    "--cpu-duty-cycle": float,
    "--max-pings-per-wake": int,
    "--sweperf-template": str,
    "--sweperf-total-steps": int,
    "--sweperf-num-cycles": int,
    "--sweperf-poll-interval-ms": int,
    "--agentsession-script": str,
    "--agentsession-script-file": str,
    "--agentsession-think-scale": float,
    "--agentsession-snapshot-steps": str,
    "--agentsession-phase-in": float,
    "--agentsession-resume-retry-budget": float,
    "--total-actors": int,
    "--spawn-concurrency": int,
    "--actor-deadline": float,
    "--target-rps": float,
}


def key(flag: str) -> str:
    """The payload key of a flag: its name without dashes, in snake_case."""
    return flag.lstrip("-").replace("-", "_")


def build_config_json(argv: Iterable[str]) -> str:
    """The --config-json payload for the worker, parsed from `argv`.

    Arguments that are not worker flags are ignored, and flags left unset
    are left out so the worker keeps its own defaults. Empty when no worker
    flag is set.
    """
    p = argparse.ArgumentParser(add_help=False)
    for flag, type_func in WORKER_FLAGS.items():
        p.add_argument(flag, type=type_func)
    parsed, _ = p.parse_known_args(list(argv))
    cfg = {
        key(f): getattr(parsed, key(f))
        for f in WORKER_FLAGS
        if getattr(parsed, key(f)) is not None
    }
    return json.dumps(cfg) if cfg else ""


def payload(parsed_options) -> dict[str, float | int | str | None]:
    """Every worker flag's current value; null for one this master lacks."""
    return {key(f): getattr(parsed_options, key(f), None) for f in WORKER_FLAGS}


def serve_config_headless(environment, port: int, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    """Serves /loadgen-config on `host`:`port` for a headless run.

    A headless master has no web UI to add the route to, so the runner
    normally passes the values once, as --config-json. A ladder changes the
    sample rate and target rate mid-run (shapes/ladder_shape.py), and the
    worker sees such a change only through this endpoint. Each request reads
    the parsed options afresh, so it returns the running step's values.

    The worker shares the pod's loopback interface with the master, so the
    endpoint is not exposed on the network.
    """

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - the name is the library's
            if self.path.split("?", 1)[0] != CONFIG_PATH:
                self.send_error(404)
                return
            body = json.dumps(payload(environment.parsed_options)).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args) -> None:
            # A line per fetch would be a line per spawn message.
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info(f"Serving {CONFIG_PATH} on {host}:{port} for locust-worker")
    return server


def init_loadgen_config() -> None:
    """Registers every worker flag and exposes their current values to
    locust-worker at /loadgen-config."""
    from locust import events
    from locust.argument_parser import LocustArgumentParser
    from locust.env import Environment

    # Imported for their parser listeners.
    from common import (  # noqa: F401
        agentsession_config,
        cpuload_config,
        durdir_config,
        lifecycle_mode,
        memload_config,
        ping_config,
        rate_config,
        resume_mode,
        spawn_config,
        sweperf_config,
    )
    from common.trace import init_tracing
    from common.wait_time import init_wait_time

    init_tracing()
    init_wait_time()

    @events.init_command_line_parser.add_listener
    def on_init_parser(parser: LocustArgumentParser) -> None:
        parser.add_argument(
            "--loadgen-config-port",
            type=int,
            default=0,
            env_var="LOCUST_LOADGEN_CONFIG_PORT",
            help=(
                "In a headless run, serve /loadgen-config on this loopback "
                "port. runner.py sets it, and passes the same port to "
                "locust-worker as --master-web-port, so values that change "
                "mid-run reach the worker."
            ),
        )

    @events.init.add_listener
    def on_init(environment: Environment, **kwargs) -> None:
        if environment.web_ui is None:
            # Headless: there is no web app to add the route to.
            port = getattr(environment.parsed_options, "loadgen_config_port", 0)
            if port:
                serve_config_headless(environment, port)
            return

        @environment.web_ui.app.route(CONFIG_PATH)
        def loadgen_config() -> dict[str, float | int | str | None]:
            return payload(environment.parsed_options)

        logger.info(f"Registered {CONFIG_PATH} on the web UI for locust-worker")
