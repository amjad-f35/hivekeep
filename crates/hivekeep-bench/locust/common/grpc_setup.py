"""gRPC under gevent, for the Python user classes."""

_initialized = False


def init_grpc_gevent() -> None:
    """Routes gRPC's poller through the gevent hub.

    locust patches the standard library for gevent, but gRPC's C core runs
    its own threads, which never yield to the hub: one call in flight would
    stall every other user. This hands gRPC's polling to gevent.

    Call it after locust is imported (which patches) and before the first
    channel. The underlying init is not idempotent, so repeat calls, from
    several locustfiles loaded into one process, are ignored here.
    """
    global _initialized
    if _initialized:
        return
    from grpc.experimental import gevent as grpc_gevent
    grpc_gevent.init_gevent()
    _initialized = True
