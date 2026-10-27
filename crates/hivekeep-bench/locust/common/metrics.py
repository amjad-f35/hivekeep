"""Prometheus metrics of the Python user classes, on :8000.

The series match the ones locust-worker serves on :8001, so one dashboard
reads both.
"""

import logging

from locust import events
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import start_http_server

logger = logging.getLogger(__name__)

METRICS_PORT = 8000

_initialized = False
request_counter = None
request_latency = None
locust_users = None


def init_metrics() -> None:
    global _initialized, request_counter, request_latency, locust_users
    if _initialized:
        logger.info("Metrics already initialized, skipping.")
        return

    logger.info("Initializing Prometheus metrics...")
    try:
        start_http_server(METRICS_PORT)
    except Exception as e:
        logger.warning(f"Metrics server already started or failed: {e}")

    provider = MeterProvider(metric_readers=[PrometheusMetricReader()])
    metrics.set_meter_provider(provider)
    meter = metrics.get_meter("hivekeep_locust")

    request_counter = meter.create_counter(
        name="locust_requests_total",
        description="Total number of requests",
    )
    request_latency = meter.create_histogram(
        name="locust_request_duration_milliseconds",
        description="Request latency in milliseconds",
    )
    locust_users = meter.create_up_down_counter(
        name="locust_users",
        description="Number of active locust users",
    )

    _initialized = True
    logger.info("Metrics initialized.")


@events.request.add_listener
def on_request(
    request_type: str,
    name: str,
    response_time: float,
    response_length: int,
    exception: Exception | None,
    context: dict | None = None,
    **kwargs,
) -> None:
    if not _initialized:
        return
    if context is None:
        context = kwargs.get("context", {})
    user_class = kwargs.get("user_class", context.get("user_class", "unknown"))
    attributes = {
        "method": request_type,
        "name": name,
        "status": "success" if exception is None else "failure",
        "user_class": user_class,
    }
    request_counter.add(1, attributes)
    request_latency.record((response_time or 0), attributes)


def update_user_count(delta: int, user_class: str) -> None:
    if not _initialized:
        logger.warning("Metrics not initialized, cannot update user count.")
        return
    locust_users.add(delta, {"user_class": user_class})
