"""Flags of the CPU-load variant of GluttonUser."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_cpuload_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("CPU Benchmark")
    group.add_argument(
        "--cpu-cores",
        type=int,
        default=0,
        help="Busy threads each GluttonUser starts in its actor through the "
        "glutton UseCPU call before the first suspend, so every suspend and "
        "resume runs against a steady CPU draw (default: 0, off). Capped at "
        "the actor's CPU count.",
    )
    group.add_argument(
        "--cpu-duty-cycle",
        type=float,
        default=0.0,
        help="Share of one core in [0, 1] each --cpu-cores thread burns "
        "(0.1 with --cpu-cores=1 draws 0.1 vCPU). Needs --cpu-cores.",
    )
