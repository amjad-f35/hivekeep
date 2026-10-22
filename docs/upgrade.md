# Upgrading

This procedure moves a cluster from one minor version to the next while
Actors keep running. It follows the order in
[version-skew-policy.md](version-skew-policy.md). Read the release notes
first: they list any step that needs manual action.

## Before you start

1. Back up the database. An external or Cloud SQL database uses its own
   tooling. For the bundled StatefulSet, take a `pg_dump` from `postgres-0`.
2. Check that the system is healthy:

   ```sh
   kubectl -n hivekeep-system get deploy,ds,sts
   hivectl get workers
   ```

   Every Worker should be ACTIVE. `hivekeep_keeper_snapshot_undurable_age_max_seconds`
   should be low, because snapshots that are not yet durable are pinned to
   their nodes.
3. Use the same configuration as the last install. The install record and
   your `--config` file hold it. Pin the new images with `--image-repo` and
   `--image-tag`, or build from the release tag.

## Steps

Each step is one `hivekeep-setup` command. Wait for its rollout to finish
before the next. The installer waits for you, bounded by
`--rollout-timeout`.

### 1. Pod certificate controller

```sh
hivekeep-setup deploy podcertificate-controller
```

Existing certificates stay valid while it restarts.

### 2. Keepers

```sh
hivekeep-setup deploy keeper
```

The DaemonSet rolls one node at a time. On SIGTERM, a keeper stops
accepting new RPCs after `--drain-delay` and waits up to `--drain-timeout`
(5m) for in-flight checkpoints and restores. The snapshot tracker resumes
uploads and reports from its state file after the restart. Running Actors
are not affected, because their sandboxes live in the worker pods.

### 3. Control plane

```sh
hivekeep-setup deploy api-server
```

This applies pending migrations at startup over the owner connection.
`hivekeep-api` keeps serving for `--drain-delay` (13s) after SIGTERM, then
drains for up to `--drain-timeout` (15s). Leases held by a terminating
replica are released or expire, and operations they guarded are retried
by their callers.

### 4. CRDs and controller

```sh
hivekeep-setup deploy hivekeep-controller
```

### 5. Gate

```sh
hivekeep-setup deploy gate
```

The router keeps serving for `--drain-delay` (13s), drains its ext_proc
server (long enough for parked requests to finish), and then drains Envoy
to zero connections through the admin interface. It signals the dataplane's
preStop hook with `--drain-complete-file`.
`scripts/e2e/check-gate-drain.sh` checks this behavior on a live cluster.

### 6. SandboxConfigs

Apply the new SandboxConfig version, or run `scripts/install-microvm-deps.sh
--install` for micro-VMs. Pools that name no `configRef` pick up the newest
class default as they roll. The keeper stages the new runtime assets in the
background. Snapshots taken under the old runtime stay restorable on
Workers that still offer it, and fall back as described in
[architecture.md](architecture.md) otherwise.

### 7. WorkerPools

Update `spec.workerImage` on each WorkerPool, one pool at a time. When a
worker pod terminates, its Actors get the pod's grace period to save state.
Actors on a worker that dies without checkpointing are crashed by epoch
fencing and can be reverted to their newest durable snapshot.

To watch a pool's progress:

```sh
kubectl -n <pool-namespace> rollout status deploy/<pool-deployment>
hivectl get workers -l <pool-label-selector>
```

## Rolling back

- **Gate, controller, keeper, pod certificate controller.** Re-run the same
  `deploy` command with the previous `--image-tag`.
- **Control plane.** Redeploy the previous version. It tolerates migrations
  it does not know about, so you do not need to restore the database unless
  the release notes say so.
- **Worker pools.** Set `spec.workerImage` back. Actors suspended on the
  new runtime may need a compatible Worker to restore their memory state.
  Until one is available, they cold-boot from their rootfs and volume
  layers.
