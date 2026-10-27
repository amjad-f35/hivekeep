"""Declares SpawnUser for the locust master; the load itself runs in locust-worker.

Creates a batch of actors (--total-actors, --spawn-concurrency) and
reports how long the batch took to become ready (TimeToAllReady). Pair it
with shapes/spawn_shape.py, which ends the run once that row exists.

The master only needs the class by name, to hand its users to the worker
and to label the worker's statistics rows. A Python worker process sets
LOCUST_NO_SPAWN_USER=1 so it never takes users of this class.
"""

import os

if os.environ.get("LOCUST_NO_SPAWN_USER") != "1":
    from locust import User, task

    from common.api_channel import API_HOST
    from common.loadgen_config import init_loadgen_config

    # Serves /loadgen-config, so values typed into the web UI form reach
    # the worker. Without a web UI it only registers the flags.
    init_loadgen_config()

    class SpawnUser(User):
        host = API_HOST

        @task
        def run_on_the_worker(self) -> None:
            # Never runs: locust requires every User to declare a task, and
            # only locust-worker takes users of this class.
            pass
