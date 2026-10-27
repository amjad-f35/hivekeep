"""Flags of the spawn benchmark (SpawnUser)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_spawn_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--total-actors",
        type=int,
        default=0,
        env_var="LOCUST_TOTAL_ACTORS",
        help="Actors in the batch; 0 keeps locust-worker's --total-actors (default 100)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--spawn-concurrency",
        type=int,
        default=0,
        env_var="LOCUST_SPAWN_CONCURRENCY",
        help="Actors created at once; 0 keeps locust-worker's --spawn-concurrency (default 1)",
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--actor-deadline",
        type=float,
        default=0.0,
        env_var="LOCUST_ACTOR_DEADLINE",
        help="Seconds each actor may take; 0 keeps locust-worker's --actor-deadline (default 120s)",
        include_in_web_ui=True,
    )
