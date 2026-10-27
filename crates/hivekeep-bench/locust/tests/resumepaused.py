"""Declares ResumePausedUser for the locust master; the load itself runs in locust-worker.

Each user creates one actor and makes it cold on its first iteration,
then resumes and pauses it on every later one without waiting. Pace the
cold resumes with --target-rps, or with @target_rps steps of a --ladder.

The master only needs the class by name, to hand its users to the worker
and to label the worker's statistics rows. A Python worker process sets
LOCUST_NO_RESUMEPAUSED_USER=1 so it never takes users of this class.
"""

import os

if os.environ.get("LOCUST_NO_RESUMEPAUSED_USER") != "1":
    from locust import User, task

    from common.api_channel import API_HOST
    from common.loadgen_config import init_loadgen_config

    # Serves /loadgen-config, so values typed into the web UI form reach
    # the worker. Without a web UI it only registers the flags.
    init_loadgen_config()

    class ResumePausedUser(User):
        host = API_HOST

        @task
        def run_on_the_worker(self) -> None:
            # Never runs: locust requires every User to declare a task, and
            # only locust-worker takes users of this class.
            pass
