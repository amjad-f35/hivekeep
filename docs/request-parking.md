# Request parking

A request can reach the ingress router for an Actor that cannot be resumed
yet. The pool may be full, the Actor may still be suspending, or the control
plane may be restarting. Instead of failing the request at once, the router
can **park** it, meaning it holds the request while the resume is retried,
and then serve it once the Actor is running. Parking is part of the ingress
mode of `hivekeep-gate router` (`crates/hivekeep-gate/src/router/ingress/`).

## How it works

1. **One flight per Actor.** Concurrent requests for the same Actor share a
   single `ResumeActor` call, called a *flight*, and they all get its
   outcome. A running Actor resumes as a no-op, so the fast path costs one
   cheap call.
2. **Retry with backoff.** If the flight hits a retryable condition, it
   retries with exponential backoff until its budget runs out. With parking
   enabled, the retryable conditions are a concurrent operation (`ABORTED`),
   no capacity (`RESOURCE_EXHAUSTED`), a transient Actor state
   (`FAILED_PRECONDITION`) and an unavailable control plane
   (`UNAVAILABLE`). The budget limits how long the flight keeps retrying,
   not how long any one attempt runs, so a resume that finishes after the
   budget is still served.
3. **Parking slots.** A request only takes a parking slot once its flight
   has had to retry. Requests answered on the first attempt never park. If
   every slot is taken, a request that needs one is shed with 503
   `actor "<hive>/<name>" unavailable: router at capacity`.
4. **Client cancellation.** If the client goes away, its request leaves the
   lot (408). If the client's deadline passes, the request ends with 504.

With parking disabled (`--parked-request-max=0`), the router still retries
concurrent-operation conflicts (with a 15 s budget) but fails every other
condition at once.

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--parked-request-budget` | `5s` | Longest a flight keeps retrying while requests are parked |
| `--parked-request-max` | `1024` | Lot capacity; `0` disables parking |
| `--parked-request-retry-interval` | `100ms` | First retry delay |
| `--parked-request-retry-factor` | `1.1` | Delay multiplier, at least 1 |
| `--parked-request-retry-jitter` | `0.1` | Random fraction in [0, 1) added to each delay |
| `--extproc-max-requests` | `0` (derived) | Envoy's ext_proc circuit breaker. `0` derives twice `--parked-request-max`, at least 1024. |
| `--drain-timeout` | `0s` (derived) | ext_proc drain deadline. `0` derives one long enough for a parked request to use its full budget. |
| `--route-timeout` | `5m` | Envoy's timeout on the workload route, which does not include the resume |

## What clients see

| Resume outcome | HTTP status |
|----------------|-------------|
| Actor does not exist (`NOT_FOUND`) | 404 |
| `FAILED_PRECONDITION`, `ABORTED`, `RESOURCE_EXHAUSTED` once the budget is spent | 503, with the control plane's reason |
| `UNAVAILABLE` | 503 |
| Lot full | 503 `router at capacity` |
| `DEADLINE_EXCEEDED`, or the client's deadline passed | 504 |
| Client went away | 408 |
| `PERMISSION_DENIED` | 403 |
| `UNAUTHENTICATED` | 401 |
| anything else | 500 |

## Observing it

| Metric | Type | Labels |
|--------|------|--------|
| `hivekeep_gate_router_parking_active` | gauge | none (requests parked right now) |
| `hivekeep_gate_router_parking_wait_duration_seconds` | histogram | `outcome` |
| `hivekeep_gate_router_parking_rejected_total` | counter | outcome (`no_capacity` or `unavailable`) |
| `hivekeep_gate_router_route_duration_seconds` | histogram | template hive and name, outcome, resume kind (already running, started, joined) |

The router's `/statusz` page (`--status-address 127.0.0.1`,
`--status-port 4040`) shows the lot's state and recent routing decisions:

```sh
kubectl -n hivekeep-system port-forward deployment/gate-router 4040:4040
curl -s localhost:4040/statusz
```

The `parking` demo (`hivekeep-setup deploy demo parking`) fills a small
pool on purpose so you can watch requests park and drain.
