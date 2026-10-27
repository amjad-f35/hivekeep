"""HiveApiUser: hivekeep-api's actor calls alone, with no traffic through
the gate router.

Each user creates a counter-demo actor, then loops GetActor, ResumeActor and
a synchronous SuspendActor.
"""

import logging
import uuid

from locust import User, task

from common import api_pb2, api_pb2_grpc
from common.api_channel import API_HOST, api_channel
from common.grpc_tracing import traced_grpc
from common.hive import HIVE, ensure_hive
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing
from common.wait_time import dynamic_wait_time, init_wait_time

logger = logging.getLogger(__name__)

# The counter demo's Hive and ActorTemplate (hivectl's demo install).
COUNTER_TEMPLATE = ("hivekeep-demo-counter", "counter")

init_tracing()
init_metrics()
init_wait_time()


class HiveApiUser(User):
    wait_time = dynamic_wait_time
    host = API_HOST

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = api_channel(self.host)
        self.stub = api_pb2_grpc.ControlStub(self.channel)

        try:
            ensure_hive(self.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure hive {HIVE}: {e}")

        self.actor_name = str(uuid.uuid4())
        self.actor_ref = api_pb2.ObjectRef(hive=HIVE, name=self.actor_name)
        try:
            self.stub.CreateActor(
                api_pb2.CreateActorRequest(
                    actor=api_pb2.Actor(
                        metadata=api_pb2.ResourceMetadata(hive=HIVE, name=self.actor_name),
                        actor_template=api_pb2.ObjectRef(
                            hive=COUNTER_TEMPLATE[0], name=COUNTER_TEMPLATE[1]
                        ),
                    )
                )
            )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_name}: {e}")

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        try:
            self.stub.SuspendActor(api_pb2.SuspendActorRequest(actor=self.actor_ref))
        except Exception as e:
            logger.error(f"Failed to suspend actor {self.actor_name}: {e}")
        self.channel.close()

    def _call(self, name: str, method, request) -> None:
        # A failed call is already a failed request in the statistics.
        try:
            with traced_grpc(name, self.__class__.__name__) as metadata:
                _, metadata.call = method.with_call(request, metadata=metadata)
        except Exception:
            pass

    @task
    def invoke_target(self) -> None:
        self._call("GetActor", self.stub.GetActor,
                   api_pb2.GetActorRequest(actor=self.actor_ref))
        self._call("ResumeActor", self.stub.ResumeActor,
                   api_pb2.ResumeActorRequest(actor=self.actor_ref))
        self._call("SuspendActor", self.stub.SuspendActor,
                   api_pb2.SuspendActorRequest(
                       actor=self.actor_ref,
                       options=api_pb2.SuspendOptions(mode=api_pb2.DURABILITY_MODE_SYNCHRONOUS),
                   ))
