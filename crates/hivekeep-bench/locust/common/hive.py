"""Creates the benchmark's Hive, idempotently, for the Python user classes.

CreateHive answers ALREADY_EXISTS once the Hive is there, which the second
and later callers take as success, so concurrent users need no coordination.
The call is traced like every other API call, so it shows in the locust
statistics too.
"""

import grpc

from common import api_pb2
from common.grpc_tracing import traced_grpc

# The Hive the actors live in; locust-worker's --hive defaults to the same.
HIVE = "benchmark"


def ensure_hive(stub, user_class: str, name: str = HIVE) -> None:
    with traced_grpc("CreateHive", user_class) as metadata:
        try:
            _, metadata.call = stub.CreateHive.with_call(
                api_pb2.CreateHiveRequest(
                    hive=api_pb2.Hive(
                        metadata=api_pb2.ResourceMetadata(name=name)
                    )
                ),
                metadata=metadata,
            )
        except grpc.RpcError as e:
            if e.code() != grpc.StatusCode.ALREADY_EXISTS:
                raise
            # The raised error is also the Call, so the server-elapsed
            # trailer of the expected ALREADY_EXISTS can still be read.
            if isinstance(e, grpc.Call):
                metadata.call = e
