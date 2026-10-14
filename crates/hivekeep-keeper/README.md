# hivekeep-keeper

The node agent (binary `hivekeep-keeper`), deployed as the `keeper`
DaemonSet.

- Serves `hivekeep.keeper.v1.Keeper` on port 7443 (mTLS, callable only by
  `hivekeep-api`): `Run`, `Checkpoint`, `Restore`, `PromoteSnapshot`,
  `DetachActor` and `Terminate`, at most one at a time per Actor.
- Serves `WorkerSupport` to the node's workers on
  `<base-dir>/worker-support.sock`, forwarding epoch, registration,
  runtime, credential, suspend and crash calls to `WorkerService`.
- Prepares OCI bundles from the image layer cache, stages sandbox runtime
  assets, and writes each worker's sandbox runtimes file.
- Keeps node-local snapshots and makes them durable in the background
  (`snapshot_tracker`): generation fencing, upload and detach workers, and
  state reports with backoff.
- Runs the shared snapshot-file and asset caches, image cache eviction,
  system-info volumes and trust bundle refresh, and per-Actor usage
  sampling.
- Mounts Actors' external volumes through CSI node drivers found by their
  `CSIDriverConfig` (`csi_volumes`).
- Advertises `/dev/kvm` as the extended resource `hivekeep.io/kvm` through
  the kubelet device plugin API v1beta1 (`device_plugin::kubelet`),
  re-registering whenever the kubelet restarts. The device plugin
  protocol definitions are under `proto/deviceplugin/` (third-party,
  Apache-2.0, see `proto/deviceplugin/LICENSE`).

```sh
cargo run -p hivekeep-keeper -- --help
cargo test -p hivekeep-keeper
```

See [docs/storage.md](../../docs/storage.md).
