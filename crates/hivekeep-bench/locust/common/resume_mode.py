"""--resume-mode: how an actor is woken."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_resume_mode_arguments(parser: LocustArgumentParser) -> None:
    group = parser.add_argument_group("Resume Mode")
    group.add_argument(
        "--resume-mode",
        type=str,
        default="explicit",
        choices=["explicit", "implicit"],
        help="'explicit' calls ResumeActor before sending traffic; 'implicit' "
        "sends the traffic and lets the gate router wake the actor "
        "(default: explicit)",
    )
