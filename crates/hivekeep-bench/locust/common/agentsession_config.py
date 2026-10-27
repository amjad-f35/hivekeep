"""Flags of the scripted agent-session benchmark (AgentSessionUser)."""

from locust import events
from locust.argument_parser import LocustArgumentParser


@events.init_command_line_parser.add_listener
def add_agentsession_arguments(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--agentsession-script",
        type=str,
        default="coding-session",
        env_var="LOCUST_AGENTSESSION_SCRIPT",
        help=(
            "Built-in script to run: coding-session, personal-assistant or "
            "personal-assistant-light (default: coding-session)"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--agentsession-script-file",
        type=str,
        default="",
        env_var="LOCUST_AGENTSESSION_SCRIPT_FILE",
        help=(
            "Path, on the worker, of a script YAML to run instead of a "
            "built-in one. locust/deploy.sh --agentsession-script FILE mounts "
            "FILE there and sets this (default: unset)"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--agentsession-think-scale",
        type=float,
        default=1.0,
        env_var="LOCUST_AGENTSESSION_THINK_SCALE",
        help=(
            "Multiplier on the script's think times, the gaps the actor spends "
            "suspended between steps: 0.5 halves them, 2.0 doubles them "
            "(default: 1.0)"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--agentsession-phase-in",
        type=float,
        default=0.0,
        env_var="LOCUST_AGENTSESSION_PHASE_IN",
        help=(
            "Seconds to spread session starts over: each user first waits a "
            "uniformly random part of it, so a fleet spawned at once does not "
            "hit every later step in lockstep (default: 0, all start at once)"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--agentsession-resume-retry-budget",
        type=float,
        default=60.0,
        env_var="LOCUST_AGENTSESSION_RESUME_RETRY_BUDGET",
        help=(
            "Seconds a step keeps retrying a wake the fleet had no room for "
            "(ResumeActor ResourceExhausted or Unavailable, router 503/504/429) "
            "before it skips to its next think gap. The wait counts in the "
            "TimeToWake row. 0 tries once (default: 60)"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--agentsession-snapshot-steps",
        type=str,
        default="",
        env_var="LOCUST_AGENTSESSION_SNAPSHOT_STEPS",
        help=(
            "Comma-separated 1-based steps after which the actor is suspended "
            "(per --lifecycle-mode). Empty (default) suspends after every step "
            "unless --lifecycle-mode is 'none'."
        ),
        include_in_web_ui=True,
    )
