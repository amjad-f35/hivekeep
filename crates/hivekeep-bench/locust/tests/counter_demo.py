"""CounterUser: the counter demo end to end.

Each iteration resumes the user's counter actor, increments it with a POST
through the gate router, and suspends it synchronously.
"""

from locust import User, events, task

from common.grpc_setup import init_grpc_gevent

# Before any channel exists.
init_grpc_gevent()

import logging  # noqa: E402
import time  # noqa: E402
import uuid  # noqa: E402

import requests  # noqa: E402
from opentelemetry.propagate import inject  # noqa: E402

from common import api_pb2, api_pb2_grpc  # noqa: E402
from common.api_channel import API_HOST, CA_FILE, CRED_BUNDLE, api_channel  # noqa: E402
from common.grpc_tracing import traced_grpc  # noqa: E402
from common.hive import HIVE, ensure_hive  # noqa: E402
from common.metrics import init_metrics, update_user_count  # noqa: E402
from common.trace import get_tracer, init_tracing  # noqa: E402
from common.wait_time import dynamic_wait_time, init_wait_time  # noqa: E402

logger = logging.getLogger(__name__)

init_tracing()
init_metrics()
init_wait_time()

tracer = get_tracer(__name__)

ROUTER_URL = "https://gate-router.hivekeep-system.svc"
# The header the gate router routes on: <hive>/<actor>.
TARGET_HEADER = "hk-target-actor"
COUNTER_TEMPLATE = ("hivekeep-demo-counter", "counter")


class CounterUser(User):
    wait_time = dynamic_wait_time

    # `host` is what the web UI and --host show and may be changed at test
    # start; the API target is kept apart so that cannot redirect it.
    host = API_HOST
    api_host = API_HOST

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = api_channel(self.api_host)
        self.stub = api_pb2_grpc.ControlStub(self.channel)

        try:
            ensure_hive(self.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure hive {HIVE}: {e}")

        self.actor_name = f"sb-{uuid.uuid4()}"
        self.actor_ref = api_pb2.ObjectRef(hive=HIVE, name=self.actor_name)
        try:
            with traced_grpc("CreateActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.CreateActor.with_call(
                    api_pb2.CreateActorRequest(
                        actor=api_pb2.Actor(
                            metadata=api_pb2.ResourceMetadata(hive=HIVE, name=self.actor_name),
                            actor_template=api_pb2.ObjectRef(
                                hive=COUNTER_TEMPLATE[0], name=COUNTER_TEMPLATE[1]
                            ),
                        )
                    ),
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_name}: {e}")

        # One mTLS session to the router per user.
        self.http_session = requests.Session()
        self.http_session.verify = CA_FILE
        self.http_session.cert = CRED_BUNDLE
        self.run_url = f"{ROUTER_URL}/"

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        self._suspend()
        self.channel.close()
        try:
            self.http_session.close()
        except Exception as e:
            logger.warning(f"Failed to close http session: {e}")

    def _suspend(self) -> None:
        try:
            with traced_grpc("SuspendActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.SuspendActor.with_call(
                    api_pb2.SuspendActorRequest(
                        actor=self.actor_ref,
                        options=api_pb2.SuspendOptions(mode=api_pb2.DURABILITY_MODE_SYNCHRONOUS),
                    ),
                    metadata=metadata,
                )
        except Exception as e:
            logger.error(f"Failed to suspend actor {self.actor_name}: {e}")

    def _fire(self, start: float, length: int, exception: Exception | None) -> float:
        duration = (time.time() - start) * 1000
        events.request.fire(
            request_type="http",
            name="RunCounter",
            response_time=duration,
            response_length=length,
            exception=exception,
            user_class=self.__class__.__name__,
        )
        return duration

    @task
    def run_and_suspend(self) -> None:
        # Errors are already in the statistics; the later steps still run.
        try:
            with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.ResumeActor.with_call(
                    api_pb2.ResumeActorRequest(actor=self.actor_ref),
                    metadata=metadata,
                )
        except Exception:
            pass

        start = time.time()
        with tracer.start_as_current_span("RunCounter") as span:
            headers = {TARGET_HEADER: f"{HIVE}/{self.actor_name}"}
            inject(headers)
            ctx = span.get_span_context()
            try:
                response = self.http_session.post(self.run_url, headers=headers)
                response.raise_for_status()
                duration = self._fire(start, len(response.content), None)
                if ctx.trace_flags.sampled:
                    logger.info(f"Traced RunCounter: trace_id={ctx.trace_id:032x}, duration_ms={duration:.2f} (client)")
            except Exception as e:
                duration = self._fire(start, 0, e)
                logger.error(f"RunCounter failed: {e}")
                if ctx.trace_flags.sampled:
                    logger.info(f"Traced RunCounter (failed): trace_id={ctx.trace_id:032x}, duration_ms={duration:.2f} (client)")

        self._suspend()
