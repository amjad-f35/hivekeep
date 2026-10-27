"""Load shape of the spawn benchmark.

One user, until the worker reports the batch's TimeToAllReady row or -t
runs out.
"""

import logging

from locust import LoadTestShape

logger = logging.getLogger(__name__)


class SpawnShape(LoadTestShape):
    def tick(self) -> tuple[int, float] | None:
        if self.runner is not None:
            # .get(), not [...]: indexing entries inserts an empty row for a
            # missing key, which would then show in the statistics.
            entry = self.runner.environment.stats.entries.get(
                ("TimeToAllReady", "summary")
            )
            if entry is not None and entry.num_requests >= 1:
                return None
            run_time = getattr(self.runner.environment.parsed_options, "run_time", None)
            if run_time and self.get_run_time() >= run_time:
                logger.info("Spawn test reached run_time deadline (%.0fs); stopping", run_time)
                return None
        return (1, 1.0)
