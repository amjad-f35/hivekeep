"""Per-wake ping loop of the GluttonUser class."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_ping_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Ping Loop")
    group.add_argument(
        "--max-pings-per-wake",
        type=int,
        default=1,
        help="Most pings a GluttonUser sends in one resume/suspend cycle. "
        "Pings after the first are 0.2-1.0s apart and stop early when the "
        "live window (--min/--max-live-time) closes, so values above 1 need a "
        "live window long enough for the gaps. At least 1 (default: 1).",
    )
