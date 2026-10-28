# Hivekeep demos

Small, self-contained workloads that show one Hivekeep behaviour each. Every
demo is deployed and removed by the installer:

```sh
cargo run --release -p hivekeep-setup -- deploy demo <name>
cargo run --release -p hivekeep-setup -- delete demo <name>
```

(`scripts/install/hivekeep-kind.sh deploy demo <name>` does the same against
the local Kind cluster.) The manifests the installer applies live in
`crates/hivekeep-setup/manifests/demos/`; this directory holds the workload
code, container builds and walkthroughs.

| Demo | Shows | Workload |
|------|-------|----------|
| [counter](counter/) | what memory, volume and rootfs state survives suspend/resume; extra ports | `crates/hivekeep-demo-counter` |
| [egress](egress/) | per-Actor egress policy through the gate | `crates/hivekeep-demo-egress` |
| [sandbox](sandbox/) | a stateful command sandbox driven from a local prompt | `crates/hivekeep-demo-sandbox` |
| [multi-template](multi-template/) | two templates, two binaries, one WorkerPool | counter + `crates/hivekeep-demo-fspersist` |
| [parking](parking/) | the gate router holding requests while the pool is full | counter |
| [jupyter](jupyter/) | an unmodified notebook image suspending and resuming transparently | upstream Jupyter image |
| [claude-code-multiplex](claude-code-multiplex/) | several coding agents taking turns on one pool | `crates/hivekeep-demo-multiplex-ui` + workload image |

All demos need a cluster with Hivekeep installed, `hivectl` on `PATH`
(`cargo install --path crates/hivekeep-ctl`), and a snapshot bucket
(`BUCKET_NAME`; the Kind profile uses the in-cluster `hivekeep-snapshots`).
To reach Actors from your machine, the install's ingress allowlist must
include `spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-client`.

> These are demos. None of them authenticates its callers; the sandbox demo in
> particular executes whatever it is sent. Do not expose them to untrusted
> networks.
