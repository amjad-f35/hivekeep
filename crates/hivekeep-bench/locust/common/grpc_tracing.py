"""Tracing and locust reporting around a unary gRPC call."""

import logging
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager

import gevent
import grpc
from locust import events
from opentelemetry.propagate import inject

from common.trace import get_tracer

logger = logging.getLogger(__name__)
_tracer = get_tracer(__name__)

# Trailer in which Hivekeep servers report their handler time, in microseconds.
SERVER_ELAPSED_TRAILER = "x-server-elapsed-us"


class TracedMetadata(list):
    """Call metadata with room for the finished Call.

    To report the server's own latency, call the stub as
    ``stub.X.with_call(req, metadata=metadata)`` and store the returned Call
    in ``metadata.call``; traced_grpc then reports the elapsed trailer
    instead of the time measured here.
    """

    def __init__(self, items: Iterable[tuple[str, str]]) -> None:
        super().__init__(items)
        self.call: grpc.Call | None = None


def read_server_elapsed_ms(call: grpc.Call | None) -> float | None:
    """The server-reported handler time in ms, or None without the trailer."""
    if call is None:
        return None
    try:
        trailers = call.trailing_metadata()
    except Exception:
        return None
    for key, value in trailers or ():
        if key == SERVER_ELAPSED_TRAILER:
            try:
                return int(value) / 1000.0
            except (TypeError, ValueError):
                return None
    return None


@contextmanager
def traced_grpc(name: str, user_class: str) -> Iterator[TracedMetadata]:
    """Wraps one unary call in a span and a locust request event.

    Yields the call's metadata, with the W3C trace context already in it.
    Pass it as ``metadata=``, and set ``metadata.call`` to report the
    server's time, which gevent scheduling on this side cannot inflate. On
    exit the request event fires (success or failure) and a sampled span's
    trace id is logged. Exceptions propagate, so each caller keeps its own
    policy (log, give up, StopUser).

        with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
            _, metadata.call = stub.ResumeActor.with_call(request, metadata=metadata)
    """
    start_time = time.time()
    with _tracer.start_as_current_span(name) as span:
        headers = {}
        inject(headers)
        metadata = TracedMetadata(headers.items())
        exception = None
        try:
            yield metadata
        except Exception as e:
            exception = e
            raise
        finally:
            client_ms = (time.time() - start_time) * 1000
            server_ms = read_server_elapsed_ms(metadata.call)
            reported_ms = server_ms if server_ms is not None else client_ms
            if server_ms is not None:
                span.set_attribute("server.elapsed_ms", server_ms)
            events.request.fire(
                request_type="grpc",
                name=name,
                response_time=reported_ms,
                response_length=0,
                exception=exception,
                user_class=user_class,
            )
            ctx = span.get_span_context()
            if ctx.trace_flags.sampled:
                suffix = " (failed)" if exception else ""
                source = "server" if server_ms is not None else "client"
                # Logged from another greenlet: a stderr write yields to the
                # hub, and doing it inline would pad the span and the tail
                # latency under load.
                gevent.spawn(
                    logger.info,
                    f"Traced {name}{suffix}: trace_id={ctx.trace_id:032x}, "
                    f"duration_ms={reported_ms:.2f} ({source})",
                )
