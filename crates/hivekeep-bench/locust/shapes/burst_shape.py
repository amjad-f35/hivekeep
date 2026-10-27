"""Bursts of users: ramp to a small peak, then let the users run out on
their own before the next burst."""

from locust import LoadTestShape

class BurstShape(LoadTestShape):
    # Overall duration for one burst cycle
    cycle_length = 30
    spawn_duration = 10

    # 3 clients peak per burst, spawned at 1/sec
    peak_users = 3
    spawn_rate = 1

    def tick(self) -> tuple[int, int]:
        run_time = self.get_run_time()
        t = run_time % self.cycle_length

        if t < self.spawn_duration:
            # Spawning: climb steadily to the peak.
            target = min(int((t + 1) * self.spawn_rate), self.peak_users)
            return (target, self.spawn_rate)
        else:
            # Idle: keep whoever is still running instead of stopping them,
            # so users end on their own.
            active_users = self.runner.user_count
            return (active_users, self.spawn_rate)
