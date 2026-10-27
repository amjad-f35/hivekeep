"""Flags of the memory-load variant of GluttonUser."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_memload_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Memory Benchmark")
    group.add_argument(
        "--mem-target",
        type=str,
        default="",
        help="Resident memory each GluttonUser fills in its actor through "
        "the glutton WriteRAM call before the first suspend, with an optional "
        "unit ('1Gi'), so snapshots carry a realistic working set (default: "
        "empty, off). Give the template's actor memory limit some headroom "
        "above it.",
    )
    group.add_argument(
        "--mem-churn",
        type=str,
        default="",
        help="How much of the working set each GluttonUser rewrites every "
        "cycle (a WriteRAM rotate: the dirty window walks through the array "
        "and wraps), with an optional unit ('64Mi'), so successive snapshots "
        "see changing memory (default: empty, off). Needs --mem-target.",
    )
    group.add_argument(
        "--mem-read",
        type=str,
        default="",
        help="How much of the working set each GluttonUser touches (ReadRAM, "
        "one byte per page) right after every resume, with an optional unit "
        "('1Gi') or 'all'. With a demand-paged restore its latency is the "
        "cost of faulting the snapshot back in (default: empty, off). Needs "
        "--mem-target.",
    )
