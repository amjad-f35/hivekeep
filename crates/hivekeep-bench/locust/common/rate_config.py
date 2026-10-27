"""--target-rps, for the worker classes that pace themselves (resumepaused)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_rate_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--target-rps",
        type=float,
        default=0.0,
        env_var="LOCUST_TARGET_RPS",
        help=(
            "Requests per second the whole worker paces to, for classes that "
            "pace (resumepaused). 0 means no cap. A --ladder step written "
            "with @target_rps changes it mid-run"
        ),
        include_in_web_ui=True,
    )
