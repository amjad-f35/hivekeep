# hivekeep-sandbox-gvisor

The gVisor sandbox worker (binary `hivekeep-sandbox-gvisor`). One worker
pod hosts many Actors, each in its own gVisor sandbox driven through the
`runsc` CLI. The worker serves `hivekeep.worker.v1.Worker` to the keeper on
`<base-dir>/workers/<pod-uid>/worker.sock`.

- Run, checkpoint, restore and terminate workloads at every snapshot
  fidelity (`lifecycle`, `fs_checkpoint`, `checkpoint_record`).
- Per-Actor cgroup leaves, checkpoint cgroup isolation, and memory limits
  enforced by throttling with a stall watch (`cgroup`, `ckpt_cgroup`,
  `memlimit`, `memory_watch`).
- runsc flags from a JSON file, reloaded when it changes (`flags`,
  `--runsc-flags`). The defaults are in `data/internal_flags.json`.
- Crash detection, usage sampling, and graceful SIGTERM drain.
- Per-Actor network namespaces served by the gate's tunnel (`actor_net`,
  `netns_tunnel`): netlink and nftables setup, the DNS relay and egress
  listeners in the Actor's gateway namespace. Shared with the micro-VM
  worker, as are the node flags (`node_flags`).
- Registration with the keeper over its `WorkerSupport` socket
  (`keeper_dial`, `register`, `runtimes_file`, `crash_report`), also used
  by the micro-VM worker. `--standalone-epoch` skips it for development.

`cdi_inject` is built and tested but not yet wired in.

```sh
cargo test -p hivekeep-sandbox-gvisor      # root/Linux tests: make test-root
```
