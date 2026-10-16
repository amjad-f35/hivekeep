# hivekeep-sandbox-microvm

The micro-VM sandbox worker (binary `hivekeep-sandbox-microvm`). One worker
pod hosts many Actors, each in its own Cloud Hypervisor micro-VM booted
with a Kata guest image. The worker drives the VMM over its REST API
(`ch`) and the guest through the Kata agent, using ttrpc over the VMM's
hybrid vsock (`kata`, `ttrpc`, `agentpb`).

- Cold boot, memory restore and rootfs or volume restore (`boot`,
  `restore`); checkpoint with memory image merging (`checkpoint`,
  `ch::merge`).
- Host-side rootfs staging and virtiofsd (`kata::overlay`). Fixed-address
  guest networking over a tap in each Actor's own network namespace
  (`guest_net::NetnsTaps`), served by the gate's tunnel.
- Registration through the keeper's `WorkerSupport` socket over mTLS
  (`keeper_dial`, `register`, shared with the gVisor worker). Capacity is read from the Downward API
  mounted at `/run/worker-capacity`.
- Runs as PID 1 with signal forwarding and reaping (`pid1`). Usage
  sampling, crash reports, and graceful drain.

Needs `/dev/kvm` on the node. Assets come from the micro-VM SandboxConfig
(`scripts/install-microvm-deps.sh`).

```sh
cargo test -p hivekeep-sandbox-microvm
```
