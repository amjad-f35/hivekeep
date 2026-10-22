# hivekeep-setup

The installer (binary `hivekeep-setup`).

```sh
cargo run --release -p hivekeep-setup -- --help
cargo run --release -p hivekeep-setup -- deploy hivekeep-system
cargo run --release -p hivekeep-setup -- deploy demo counter
```

- `deploy`: `hivekeep-system`, `keeper`, `api-server`, `hivekeep-controller`,
  `gate`, `podcertificate-controller`, `sandboxconfig`, `postgres`,
  `benchmarks`, and `demo <name>`.
- `delete`: `hivekeep-system`, `gate`, `all`, `benchmarks`, and
  `demo <name>`.
- `create`: re-creates one Secret or config.
- `setup csi`: NFS and/or hostpath CSI drivers.
- `publish`: worker images, release images and `hivectl` release binaries.
- `gcp`: project bootstrap, APIs, GKE cluster, buckets, IAM, Cloud SQL and
  dashboards.

Settings are layered (default, then environment, then `--config` file, then
flag), and each run prints a report of where every value came from. The
manifests it applies live in `manifests/` (`install/`,
`egress-credential-injection/`, `microvm/`, `csi/`, `demos/`). Cloud
Monitoring dashboards live in `dashboards/`.

See [docs/operator-install.md](../../docs/operator-install.md).
