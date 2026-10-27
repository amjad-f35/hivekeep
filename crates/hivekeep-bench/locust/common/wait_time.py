"""Wait-time and live-time flags shared by every user class."""

import random

from locust import User, events
from locust.argument_parser import LocustArgumentParser

_initialized = False


def init_wait_time() -> None:
    """Registers the wait-time and live-time flags, once."""
    global _initialized
    if _initialized:
        return

    @events.init_command_line_parser.add_listener
    def on_init_parser(parser: LocustArgumentParser) -> None:
        parser.add_argument(
            "--min-wait-time",
            type=float,
            default=0.0,
            env_var="LOCUST_MIN_WAIT_TIME",
            help="Shortest wait in seconds between a user's tasks",
            include_in_web_ui=True,
        )
        parser.add_argument(
            "--max-wait-time",
            type=float,
            default=0.5,
            env_var="LOCUST_MAX_WAIT_TIME",
            help="Longest wait in seconds between a user's tasks",
            include_in_web_ui=True,
        )
        # The live window: how long a GluttonUser keeps its actor resumed
        # between the first ping and the suspend. The wait time above is the
        # other gap, from the suspend to the next resume. 0-0 suspends right
        # after the ping.
        parser.add_argument(
            "--min-live-time",
            type=float,
            default=0.0,
            env_var="LOCUST_MIN_LIVE_TIME",
            help="Shortest time in seconds a GluttonUser actor stays resumed after its first ping (locust-worker only)",
            include_in_web_ui=True,
        )
        parser.add_argument(
            "--max-live-time",
            type=float,
            default=0.0,
            env_var="LOCUST_MAX_LIVE_TIME",
            help="Longest time in seconds a GluttonUser actor stays resumed after its first ping (locust-worker only)",
            include_in_web_ui=True,
        )

    _initialized = True


def dynamic_wait_time(user_instance: User) -> float:
    opts = user_instance.environment.parsed_options
    min_wait = getattr(opts, "min_wait_time", None)
    max_wait = getattr(opts, "max_wait_time", None)
    return random.uniform(
        0.0 if min_wait is None else min_wait,
        0.5 if max_wait is None else max_wait,
    )
