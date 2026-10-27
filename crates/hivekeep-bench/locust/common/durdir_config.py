"""Flags of the durable-directory benchmark (DurdirUser)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_durdir_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("DurDir Benchmark")
    group.add_argument(
        "--durdir-file-size-bytes",
        type=int,
        default=8388608,
        help="Size of the file written and read back each cycle (default: 8388608, 8 MiB)",
    )
    group.add_argument(
        "--durdir-read-mode",
        type=str,
        default="data",
        choices=["data", "digest"],
        help="'data' (default) returns the whole file and the client checks it; "
        "'digest' returns only its size and sha256, to keep the network out of it",
    )
    group.add_argument(
        "--durdir-template",
        type=str,
        default="glutton-durdir-data",
        help="ActorTemplate to run (default: glutton-durdir-data)",
    )
