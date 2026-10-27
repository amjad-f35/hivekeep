"""OpenTelemetry tracing for the master and the Python user classes.

--trace-probability sets the share of iterations sampled. The master also
serves it to locust-worker through /loadgen-config, so one flag samples
both kinds of user class.
"""

import logging
from collections.abc import Sequence

from locust import events
from locust.argument_parser import LocustArgumentParser
from locust.env import Environment
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import set_global_textmap
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import Sampler, SamplingResult, TraceIdRatioBased
from opentelemetry.trace import Link, SpanKind, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace.span import TraceState
from opentelemetry.util.types import Attributes

logger = logging.getLogger(__name__)

SERVICE_NAME_VALUE = "hivekeep-locust"


class UpdatableSampler(Sampler):
    """A trace-id ratio sampler whose ratio can change while it is in use."""

    def __init__(self, initial_probability: float = 0.0) -> None:
        self.sampler = TraceIdRatioBased(initial_probability)
        self.probability = initial_probability

    def update_probability(self, probability: float) -> None:
        self.sampler = TraceIdRatioBased(probability)
        self.probability = probability

    def should_sample(
        self,
        parent_context: Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes: Attributes = None,
        links: Sequence[Link] | None = None,
        trace_state: TraceState | None = None,
    ) -> SamplingResult:
        return self.sampler.should_sample(
            parent_context, trace_id, name, kind, attributes, links, trace_state
        )

    def get_description(self) -> str:
        return f"UpdatableSampler(probability={self.probability})"


_initialized = False
_sampler = UpdatableSampler(0.0)


def _span_exporter():
    """The OTLP exporter, when the image has it; spans are dropped otherwise."""
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("OTLP exporter not installed; sampled spans are not exported")
        return None
    return BatchSpanProcessor(OTLPSpanExporter())


def init_tracing() -> None:
    global _initialized
    if _initialized:
        logger.info("Tracing already initialized, skipping.")
        return

    @events.init_command_line_parser.add_listener
    def _(parser: LocustArgumentParser) -> None:
        parser.add_argument(
            "--trace-probability",
            type=float,
            default=0.0,
            env_var="LOCUST_TRACE_PROBABILITY",
            help="Share of iterations traced (0.0 to 1.0)",
            include_in_web_ui=True,
        )

    @events.init.add_listener
    def on_locust_init(environment: Environment, **kwargs) -> None:
        probability = getattr(environment.parsed_options, "trace_probability", 0.0)
        _sampler.update_probability(probability)

        provider = TracerProvider(
            sampler=_sampler,
            resource=Resource(attributes={SERVICE_NAME: SERVICE_NAME_VALUE}),
        )
        # Installed even at probability 0, so raising it later exports.
        processor = _span_exporter()
        if processor is not None:
            provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        set_global_textmap(TraceContextTextMapPropagator())
        logger.info(f"Tracing initialized for {SERVICE_NAME_VALUE} with initial probability {probability}")

    @events.test_start.add_listener
    def on_test_start(environment: Environment, **kwargs) -> None:
        probability = getattr(environment.parsed_options, "trace_probability", 0.0)
        _sampler.update_probability(probability)
        logger.info(f"Test started, updated trace probability to {probability}")

    _initialized = True


def set_trace_probability(probability: float) -> None:
    """Changes this process's sample rate mid-run.

    The provider keeps one sampler for the life of the process, so a ladder
    step (shapes/ladder_shape.py) changes that sampler's ratio rather than
    installing a new provider.
    """
    _sampler.update_probability(probability)
    logger.info(f"Trace probability set to {probability}")


def get_tracer(name: str) -> Tracer:
    return trace.get_tracer(name)
