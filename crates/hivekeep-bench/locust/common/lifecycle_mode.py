"""--lifecycle-mode: how an actor is put away between steps."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_lifecycle_mode_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Lifecycle Mode")
    group.add_argument(
        "--lifecycle-mode",
        type=str,
        default="async",
        choices=["sync", "async", "none"],
        help="'sync': SuspendActor returns once the snapshot is uploaded; "
        "'async': SuspendActor returns at once and the upload runs behind it; "
        "'none': actors stay resident between steps (default: async)",
    )
