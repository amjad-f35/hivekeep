# Resource model

Hivekeep accounts for compute three times: what an Actor asks for, what a
Worker supplies, and what a Worker's Actors already hold. All three use the
same `Resources` shape (`cpu_millicores`, `memory_bytes`), so one can be
subtracted from another. A zero dimension means "none of that resource".

## What an Actor asks for

An ActorTemplate's `resources.limits` sets each Actor's size:

```yaml
resources:
  limits:
    cpuMillicores: "2000"     # 1000 = one core, at most 999999
    memoryBytes: "4294967296"
```

Containers may also declare their own `resources.limits`, and the sandbox's
envelope must fit them. Limits are the only figure Hivekeep uses: there
are no separate requests, and the scheduler books the full limit.

How each backend enforces the size:

- **gVisor.** runsc applies the limits to the Actor's host cgroup leaf. CPU
  is a CFS quota over a 100 ms period. Memory is enforced by throttling
  (`memory.high`) and a stall watch, not by the OOM killer. A sandbox that
  stays stuck at its limit is detected and handled. Checkpoints run in a
  separate cgroup, so their own memory does not count against the Actor.
- **Micro-VM.** The VM gets whole vCPUs (the limit rounded up) and guest RAM
  sized from the memory limit, with a floor of 128 MiB and 64 MiB reserved
  for the guest itself (`vm_boot.rs`). Container cgroups inside the guest come from the containers'
  declared resources.

## What a Worker supplies

A Worker's `status.capacity` holds `cpu_millicores`, `memory_bytes` and
`actors`, the number of Actor slots. Workers report it when they register:

- CPU and memory come from the worker pod's own limits, through the
  Downward API (mounted at `/run/worker-capacity` for the micro-VM worker).
  Set them in the WorkerPool's `spec.template.resources`.
- `actors` is the slot count reported at registration. Independently, a
  worker refuses to admit more than `--max-actors` (default 1000) at once.

Each sandbox runtime the Worker offers also declares a per-Actor memory
overhead (`RuntimeOverhead`): a fixed `memory_bytes`, plus
`memory_bytes_per_gib` for each GiB of the Actor's limit. This covers the
gVisor sentry or the VMM and guest kernel.

## Placement arithmetic

When an Actor is placed, its Worker's `status.allocated` grows by the
Actor's limits plus the runtime overhead, and by one Actor slot. It shrinks
again when the Actor is suspended, deleted or released. A Worker has room
when capacity minus allocated covers the request in every dimension the
Actor asks for. Among eligible Workers with room, the scheduler samples two
and picks the one whose load (the higher of slot and compute utilization)
is lower. The store repeats the room check under the Worker's row lock when
it binds the Actor.

The ratio of `actors` to compute decides how densely a pool packs:

- To pack by memory, give Workers a large slot count and let memory limit
  placement.
- To pack by count (for example, many tiny Actors), set `--max-actors`
  below what memory would allow.

Only RUNNING (and RESUMING and SUSPENDING) Actors hold an allocation, so a
pool can hold many more Actors in total than it has slots. Suspended Actors
cost object storage and, until they are detached, node disk.

## Node disk

Disk is not part of placement. The per-node figures to watch are:

- local snapshots, until they are uploaded and then detached
  (`--snapshot-upload-delay`, `--snapshot-detach-delay`);
- the image layer cache (watermarks `--image-cache-high-percent`,
  `--image-cache-low-percent`);
- the snapshot and sandbox asset caches (`--*-cache-max-bytes`);
- durable-dir and rootfs upper directories of the Actors on the node.

See [storage.md](storage.md) for the flags.

## Observing usage

- `hivectl get workers` shows capacity and allocation per Worker.
- `hivectl top worker` shows live CPU and memory from the Kubernetes
  metrics API.
- The keeper samples each running Actor every `--actor-stats-poll-interval`
  (1m) into `hivekeep_actor_stats_*`. Workers sample every
  `--usage-sample-interval` (1m, at least 50s) for usage records.
- `hivekeep_workerpool_workers` and `hivekeep_scheduler_assignment_duration_seconds{outcome="no_capacity"}`
  show whether pools are large enough.
