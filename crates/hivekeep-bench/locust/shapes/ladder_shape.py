"""A ladder of load steps run as one locust test.

A ladder holds a user count (and optionally a sample rate or a target
request rate) for a while, then moves to the next step. Running every step
in one test spares each step its own deploy, cold caches and baseline.

`--ladder` takes the steps as a comma-separated list of
`users:duration[:trace_probability][@target_rps]`:

    --ladder 5:3m,15:3m,30:3m          sweep the user count
    --ladder 15:3m:0,15:3m:0.1,15:3m:1 sweep the sample rate
    --ladder 0:5m                      measure the idle floor
    --ladder 200:3m@1000,200:3m@2000   sweep the target rate at 200 users

List this file after the test in the test's `file`:

    file: /app/tests/glutton.py,/app/shapes/ladder_shape.py

With a shape loaded locust ignores `-t`; the run ends after the last step.

A step that names a trace probability sets the sample rate when it starts,
and the rate holds until a later step names another; before the first such
step `--trace-probability` applies. A step that names a target rate works
the same way, starting from `--target-rps`; `0` lifts the cap. Only classes
that pace read the rate (resumepaused), and it caps the whole worker's
requests per second, so there must be enough users to reach it: each user
waits for its whole iteration (for resumepaused, a resume and a pause).

Both values are written into the master's parsed options:

  * a Python user class picks the sample rate up through common.trace's
    sampler;
  * locust-worker reads both from /loadgen-config. A step that changes the
    user count sends a spawn message, on which the worker re-reads the
    config; a step that keeps the count is seen at the worker's next poll,
    within 10 s, so leave the start of such steps out of the measurement.

Run ladders on a locust-worker class. Python and gRPC contend with each other
at high user counts, and the latency measured is then the load generator's.
"""

import logging
import re

from locust import LoadTestShape, events
from locust.argument_parser import LocustArgumentParser

logger = logging.getLogger(__name__)

# users:duration[:trace_probability][@target_rps]
_STEP_RE = re.compile(
    r"^(?P<users>\d+):(?P<duration>\d+)(?P<unit>[smh]?)"
    r"(?::(?P<probability>[0-9.]+))?"
    r"(?:@(?P<rps>[0-9.]+))?$"
)

_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600}

# A step of the ladder holds one number of users, for one period, at one
# sample rate and one target request rate. `probability` and `rps` are None
# when the step does not change them.
Step = tuple[int, int, float | None, float | None]


def parse_ladder(spec: str) -> list[Step]:
    """The steps of a --ladder value. Any step that does not parse raises
    ValueError: a ladder quietly running the wrong steps would produce a
    table indistinguishable from a right one."""
    steps: list[Step] = []
    for raw in spec.split(","):
        item = raw.strip()
        if not item:
            continue
        m = _STEP_RE.match(item)
        if not m:
            raise ValueError(
                f"unrecognized ladder step {item!r}; "
                f"the form is users:duration[:trace_probability][@target_rps], "
                f"as in 15:3m, 15:3m:0.1, or 200:3m@1000"
            )
        seconds = int(m.group("duration")) * _UNIT_SECONDS[m.group("unit") or "s"]
        if seconds <= 0:
            raise ValueError(f"ladder step {item!r} has no duration")
        probability = _parse_float(item, m.group("probability"), "trace probability")
        if probability is not None and not 0.0 <= probability <= 1.0:
            raise ValueError(
                f"ladder step {item!r} has a trace probability outside 0.0 to 1.0"
            )
        rps = _parse_float(item, m.group("rps"), "target rate")
        steps.append((int(m.group("users")), seconds, probability, rps))
    if not steps:
        raise ValueError("--ladder has no steps")
    return steps


def _parse_float(item: str, value: str | None, what: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"ladder step {item!r} has a {what} that is not a number")


@events.init_command_line_parser.add_listener
def _(parser: LocustArgumentParser) -> None:
    parser.add_argument(
        "--ladder",
        type=str,
        default="",
        env_var="LOCUST_LADDER",
        help=(
            "Run a ladder of steps in one test, as "
            "users:duration[:trace_probability][@target_rps],... For "
            "example 5:3m,15:3m,30:3m"
        ),
        include_in_web_ui=True,
    )
    parser.add_argument(
        "--ladder-spawn-rate",
        type=float,
        default=5.0,
        env_var="LOCUST_LADDER_SPAWN_RATE",
        help="Users per second at each step change of a --ladder run",
        include_in_web_ui=True,
    )


def _set_trace_probability(environment, probability: float) -> None:
    """Puts `probability` in force for both kinds of user class.

    locust-worker reads the parsed options through /loadgen-config; a Python
    class samples through common.trace's sampler in this process.
    """
    environment.parsed_options.trace_probability = probability
    from common.trace import set_trace_probability

    set_trace_probability(probability)


def _set_target_rps(environment, rps: float) -> None:
    """Puts `rps` into the parsed options, which /loadgen-config serves."""
    environment.parsed_options.target_rps = rps


class LadderShape(LoadTestShape):
    """Holds each `--ladder` step in turn, then ends the run.

    The file declares no user class, so it is listed in -f after a test.
    Without --ladder the shape does nothing: the first tick() returns None
    and locust stops at once, so never load it on its own.
    """

    # Filled in at the first tick: the parsed options do not exist yet when
    # locust builds the shape.
    _steps: list[Step] | None = None
    _boundaries: list[float] = []
    _current: int = -1

    def _load(self) -> bool:
        if self._steps is not None:
            return bool(self._steps)
        spec = getattr(self.runner.environment.parsed_options, "ladder", "")
        self._steps = parse_ladder(spec) if spec else []
        elapsed = 0.0
        self._boundaries = []
        for _, seconds, _, _ in self._steps:
            elapsed += seconds
            self._boundaries.append(elapsed)
        if self._steps:
            logger.info(
                "Ladder: %d steps, %.0fs in total",
                len(self._steps),
                self._boundaries[-1],
            )
        return bool(self._steps)

    def tick(self) -> tuple[int, float] | None:
        if not self._load():
            return None
        assert self._steps is not None

        run_time = self.get_run_time()
        index = next(
            (i for i, end in enumerate(self._boundaries) if run_time < end),
            None,
        )
        if index is None:
            logger.info("Ladder complete after %.0fs", run_time)
            return None

        users, _, probability, rps = self._steps[index]
        spawn_rate = self.runner.environment.parsed_options.ladder_spawn_rate

        if index != self._current:
            self._current = index
            if probability is not None:
                _set_trace_probability(self.runner.environment, probability)
            if rps is not None:
                _set_target_rps(self.runner.environment, rps)
            logger.info(
                "Ladder step %d/%d: users=%d trace_probability=%s target_rps=%s",
                index + 1,
                len(self._steps),
                users,
                "unchanged" if probability is None else probability,
                "unchanged" if rps is None else rps,
            )

        return (users, spawn_rate)
