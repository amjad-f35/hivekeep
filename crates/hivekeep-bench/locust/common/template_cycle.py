"""A Python user class that suspends and resumes one actor in a loop.

Each user creates an actor from `template_name` in the benchmark-workloads
Hive, then repeats: wait half a second, SuspendActor (synchronous), wait
half a second, ResumeActor. On stop it suspends and deletes the actor.
"""

import logging
import time
import uuid

from locust import User, task
from locust.exception import StopUser

from common import api_pb2, api_pb2_grpc
from common.api_channel import API_HOST, api_channel
from common.grpc_tracing import traced_grpc
from common.hive import HIVE, ensure_hive
from common.metrics import init_metrics, update_user_count
from common.trace import init_tracing
from common.wait_time import dynamic_wait_time, init_wait_time

logger = logging.getLogger(__name__)

# The Hive the benchmark ActorTemplates are installed in (workloads/deploy.sh).
TEMPLATE_HIVE = "benchmark-workloads"

init_tracing()
init_metrics()
init_wait_time()


class TemplateCycleUser(User):
    abstract = True
    wait_time = dynamic_wait_time
    host = API_HOST
    template_name = ""

    def on_start(self) -> None:
        update_user_count(1, self.__class__.__name__)
        self.channel = api_channel(self.host)
        self.stub = api_pb2_grpc.ControlStub(self.channel)

        try:
            ensure_hive(self.stub, self.__class__.__name__)
        except Exception as e:
            logger.error(f"Failed to ensure hive {HIVE}: {e}")
            self.channel.close()
            raise StopUser()

        self.actor_name = f"sb-{uuid.uuid4()}"
        self.actor_ref = api_pb2.ObjectRef(hive=HIVE, name=self.actor_name)
        try:
            self.stub.CreateActor(
                api_pb2.CreateActorRequest(
                    actor=api_pb2.Actor(
                        metadata=api_pb2.ResourceMetadata(
                            hive=HIVE, name=self.actor_name
                        ),
                        actor_template=api_pb2.ObjectRef(
                            hive=TEMPLATE_HIVE,
                            name=self.template_name,
                        ),
                    )
                )
            )
        except Exception as e:
            logger.error(f"Failed to create actor {self.actor_name}: {e}")
            self.channel.close()
            raise StopUser()

    def on_stop(self) -> None:
        update_user_count(-1, self.__class__.__name__)
        try:
            self.stub.SuspendActor(api_pb2.SuspendActorRequest(actor=self.actor_ref))
        except Exception as e:
            logger.warning(f"Failed to suspend actor {self.actor_name} during teardown: {e}")
        try:
            self.stub.DeleteActor(api_pb2.DeleteActorRequest(actor=self.actor_ref))
        except Exception as e:
            logger.warning(f"Failed to delete actor {self.actor_name}: {e}")
        self.channel.close()

    @task
    def workload_cycle(self) -> None:
        time.sleep(0.5)
        try:
            with traced_grpc("SuspendActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.SuspendActor.with_call(
                    api_pb2.SuspendActorRequest(
                        actor=self.actor_ref,
                        options=api_pb2.SuspendOptions(mode=api_pb2.DURABILITY_MODE_SYNCHRONOUS),
                    ),
                    metadata=metadata,
                )
        except Exception:
            # Already reported as a failed request.
            pass

        time.sleep(0.5)
        try:
            with traced_grpc("ResumeActor", self.__class__.__name__) as metadata:
                _, metadata.call = self.stub.ResumeActor.with_call(
                    api_pb2.ResumeActorRequest(actor=self.actor_ref),
                    metadata=metadata,
                )
        except Exception:
            pass
