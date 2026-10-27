"""Flags of the SWE-perf replay benchmark (SweperfUser)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_sweperf_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--sweperf-template",
        type=str,
        default="sweperf-astropy-7336",
        env_var="LOCUST_SWEPERF_TEMPLATE",
        help="ActorTemplate of the replayed task (default: sweperf-astropy-7336)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--sweperf-total-steps",
        type=int,
        default=21,
        env_var="LOCUST_SWEPERF_TOTAL_STEPS",
        help="Steps in the task's replay trace (default: 21, astropy-7336's)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--sweperf-num-cycles",
        type=int,
        default=4,
        env_var="LOCUST_SWEPERF_NUM_CYCLES",
        help="Suspend/resume cycles the steps are split across (default: 4)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--sweperf-poll-interval-ms",
        type=int,
        default=100,
        env_var="LOCUST_SWEPERF_POLL_INTERVAL_MS",
        help="Milliseconds between /status polls while a cycle's job runs (default: 100)",
        include_in_web_ui=True,
    )
