# Storage

Hivekeep keeps state in four places:

- **object storage**, for durable snapshots and runtime assets;
- **the node**, for local snapshots, caches and Actor directories under the
  keeper's base directory;
- **CSI volumes**, for external volumes;
- **PostgreSQL**, for control-plane records (see [postgres.md](postgres.md)).

## Snapshots

A suspend writes the snapshot on the Actor's node first. The keeper's
snapshot tracker then makes it durable in object storage:

- **Upload.** In async mode, the upload waits for `--snapshot-upload-delay`
  (default 5m) after the oldest snapshot that is not yet durable. In sync
  mode, or when `PromoteSnapshot` is called, it starts at once. Each
  attempt's timeout is `--snapshot-upload-timeout` (2m) plus an allowance
  based on the snapshot's allocated size at
  `--snapshot-upload-est-throughput-mib` (100 MiB/s), and doubles after
  each failure. `--snapshot-upload-workers`, `--snapshot-detach-workers`
  and `--snapshot-report-workers` limit how many run at once.
- **Report.** The keeper reports each Actor's local, uploading and durable
  snapshots through `ReportActorSnapshotState`, with backoff. If a report
  keeps failing, it gives up after `--snapshot-report-give-up-after` (10m).
- **Detach.** An Actor that stays SUSPENDED for `--snapshot-detach-delay`
  (default 30m) is detached: its newest snapshot is made durable and its
  node files are removed, so it can resume on any node.
- **Generations.** Each snapshot gets the next generation number, so a
  late result from an older checkpoint can never replace a newer one. A
  Snapshot record moves from PENDING to COMPLETED when its durable copy is
  complete.
- **Commit marker.** The manifest is uploaded last. A snapshot without a
  manifest is not usable.
- **Restarts.** After a node reboot, local snapshots from before the reboot
  are pruned, because the node boot ID is recorded with them.

### Object layout

`--snapshot-location` (env `HK_API_SNAPSHOT_LOCATION`) on `hivekeep-api` is
the base URI, made of a scheme, a bucket and a path. With the
`PerTemplateSnapshotLocation` preview gate, a template can set its own
`snapshot_config.storage_location`. Below the base:

```text
<location>/hives/<hive>/actors/<actor-uid>/snapshots/<snapshot-name>/...
<location>/hives/<hive>/tags/<tag-uid>/...
```

Inside a snapshot, durable-dir volume files are under `fs/` and rootfs
files under `rootfs/`. Memory images are the backend's own files.

### Format

Large, mostly empty files (memory images, disk images) are stored in the
sparse-extent format (magic `HKSPARSE`, version 2). Only the populated
extents are kept, compressed as independent zstd frames so parts can be
uploaded and downloaded in parallel. A stream without the magic is read as
plain zstd. On restore, holes are punched back into the target file.

### Backends

| Backend | Selected by | Credentials |
|---------|-------------|-------------|
| GCS | `HK_STORAGE_BACKEND` unset or anything except `s3` | GCE metadata server, or a service-account key file in `GOOGLE_APPLICATION_CREDENTIALS`. External-account (workload identity federation) files are not supported. |
| S3 (and S3-compatible) | `HK_STORAGE_BACKEND=s3` | `AWS_*` environment keys, web identity (STS), or IMDSv2. `AWS_REGION` / `AWS_DEFAULT_REGION` and `AWS_ENDPOINT_URL` are honored. Shared config and credentials files are not read. |

The Kind profile uses an in-cluster S3-compatible server (`rustfs`) with the
bucket `hivekeep-snapshots`.

### Who talks to object storage

- **Keeper.** `--object-store=plugin` (the deployed shape) moves data
  through the `hivekeep-objectstore-plugin` node sidecar on
  `--objectstore-plugin-socket` (default `/run/objectstore-plugin/node.sock`).
  `direct` talks to GCS or S3 in-process, and `local` uses
  `--object-store-dir` (development only).
- **Control plane.** `hivekeep-api --objectstore=plugin|direct|local|none`
  copies (for Tags) and deletes snapshot prefixes, through the control
  plugin sidecar on `--objectstore-plugin-socket`, or in-process.
- **Scoped credentials (GCS).** For each transfer, the node plugin
  exchanges the Actor's transfer JWT (minted through
  `MintWorkerActorJwt`) at the control plugin's credential exchange
  (`hivekeep-api:8445`, audience `objectstore.hivekeep.io`). The control
  plugin issues a downscoped token: read-write on the Actor's own prefix,
  read-only on completed Tags. It refuses Actors that are not bound to the
  calling node, and recreated Actors.

### Shared snapshot cache

Golden and Tag snapshots are restored by many Actors. The keeper caches
their files on the node (`--snapshot-cache-dir`, default
`<base-dir>/snapshot-cache`, off with `--no-snapshot-cache`) and links or
copies them into each restore. Eviction is controlled by
`--snapshot-cache-max-bytes`, `--snapshot-cache-min-age` (10m),
`--snapshot-cache-gc-period` (5m) and `--snapshot-cache-gc-dry-run`.

## Sandbox runtime assets

A SandboxConfig lists, per version and architecture, the files a sandbox
runtime needs (for example the `runsc` binary, or the Cloud Hypervisor
binary with the guest kernel and image), each with a URL and SHA-256. The
keeper stages them into a content-addressed asset cache
(`--sandbox-asset-cache-dir`, default `<base-dir>/sandbox-asset-cache`, on
the same mount as the actors directory, so Actors get hard links). Each
download is checked against its SHA-256 and swapped in atomically. When the
object store cannot read a public `gs://` asset, the keeper falls back to an
anonymous download (`--anonymous-gcs-assets`, default `true`).
Each worker's default runtime is staged ahead of time.
`scripts/microvm-assets/assemble.sh` builds the micro-VM asset set, and
`scripts/install-microvm-deps.sh --install` applies the micro-VM
SandboxConfig.

Eviction is controlled by `--sandbox-asset-cache-max-bytes`,
`--sandbox-asset-cache-min-age` (1h), `--sandbox-asset-cache-gc-period`
(5m) and `--sandbox-asset-fetch-timeout` (5m).

## Image cache

`hivekeep-imagecache` pulls OCI images once per node and keeps their
unpacked layers in a content-addressed pool (`--image-cache-dir`, default
`<base-dir>/image-cache`). The pool must be on the volume shared with
worker pods, because the layers are used directly as overlay lower
directories. Each layer is checked against its diffID while it is
unpacked. Whiteouts and implicit directories are recorded as metadata, and
the worker applies them when it composes the rootfs.

- Registry credentials come from a kubelet credential provider
  configuration (`--image-credential-provider-config`,
  `--image-credential-provider-bin-dir`).
- `--localhost-registry-replacement` (for example `kind-registry:5000`)
  rewrites loopback registries for local development.
- Eviction starts when the volume passes `--image-cache-high-percent` (85)
  and frees down to `--image-cache-low-percent` (80). `--image-cache-max-bytes`
  caps the cache, and `--image-cache-min-age` (2m) protects fresh layers.
  `--image-cache-gc-period` (5m) and `--image-cache-gc-dry-run` control the
  loop.
- `hivekeep-validate-image-cache` checks a batch of images for pull and
  unpack correctness, writing results to CSV.

## Node storage

Everything on a node lives under the keeper's `--base-dir`, default
`/var/lib/hivekeep`. The same host path must be mounted into the keeper and
every worker pod:

| Path | Contents |
|------|----------|
| `actors/<actor-uid>/` | An Actor's bundles, rootfs upper directory, durable-dir volumes and runtime state |
| `snapshots/<actor-uid>/` | Staged local snapshots (`--snapshots-dir` can move these to another mount) |
| `workers/<pod-uid>/worker.sock` | A worker pod's Worker API socket |
| `workers/<pod-uid>/sandbox-runtimes.json` | The runtimes the keeper staged for that worker |
| `worker-support.sock` | The keeper's WorkerSupport socket |
| `image-cache/` | OCI layer pool |
| `snapshot-cache/`, `sandbox-asset-cache/` | Shared caches |
| `snapshot-scratch/`, `sandbox-asset-scratch/` | Short-lived transfer directories, wiped at startup |
| `sandbox-runtime-index.json` | Runtimes that workers on this node have advertised |

Removing an Actor's directories never follows a path into a mounted volume.

## External (CSI) volumes

A template volume with an `external.template` source
(`capacity_bytes`, `storage_class_name`; preview gate `ExternalVolumes`)
gets one CSI volume per Actor. The volume is created on the Actor's first
resume, attached on the node before each restore, detached on suspend,
revert and delete, and deleted with the Actor. Its ID and context are
recorded in `status.external_volumes`.

- Storage classes name the driver (`provisioner`) and its parameters.
  `hivekeep-api --storage-classes-file` loads them from JSON; a class not
  in the file is read from the cluster's `storage.k8s.io/v1`
  StorageClasses:

  ```json
  {"csi-nfs-sc": {"provisioner": "nfs.csi.k8s.io", "parameters": {"server": "nfs-server.example", "share": "/"}}}
  ```

- The cluster-scoped `CSIDriverConfig` CRD (named after the driver) tells
  Hivekeep how to reach a driver: the driver name, the controller endpoint
  (`tcp://` or `dns://`), optional mutual TLS with the pod identity, and a
  node socket override (default
  `unix:///var/lib/kubelet/plugins/<driver>/csi.sock`).
- `hivekeep-setup setup csi nfs|hostpath|both|none` installs the NFS
  and/or hostpath drivers (hostpath is single-node Kind only), with their
  controllers exposed over mTLS proxies. `make csi-hostpath` and
  `make csi-nfs` do the same on Kind.

### How the CSI calls are made

Both sides speak CSI v1 through the client in `hivekeep-node` (`csi`
module). A driver is opened the first time one of its volumes is used:
its `CSIDriverConfig` is read, the driver is dialled, `GetPluginInfo` must
report the requested name, and its capabilities are queried. The opened
driver is then cached for the life of the process; a failed open is
retried on the next use.

- **Control plane** (`hivekeep-api`, only with the `ExternalVolumes`
  gate): `CreateVolume` (named `hivekeep-<actor uid>-<volume>`, one
  node-writer filesystem mount, the class parameters),
  `ControllerPublishVolume`/`ControllerUnpublishVolume` to attach to and
  detach from the Actor's node, and `DeleteVolume`. With TLS enabled the
  API presents `--csi-client-cred-bundle` and verifies the driver's
  certificate (for `tls.serverName`) against `--csi-ca-file`; both files
  are re-read when they rotate, without a restart. Only pod-identity TLS
  is supported.
- **Node** (the keeper): `NodeStageVolume` into
  `<base>/staging/<volume id>`, then `NodePublishVolume` at the Actor's
  external volume mount point; on teardown `NodeUnpublishVolume` and
  `NodeUnstageVolume`. The node socket is reached without TLS.

Attachment and staging are optional in CSI. They are skipped for a driver
whose capabilities do not list `PUBLISH_UNPUBLISH_VOLUME` or
`STAGE_UNSTAGE_VOLUME`, and switched off the first time the driver answers
them with `Unimplemented`. If the capability query fails or lists nothing,
both are assumed supported. A `NOT_FOUND` from unpublish, detach or delete
means the work is already done.

Without the `ExternalVolumes` gate, or without a Kubernetes API (the
keeper logs a warning and knows no drivers), templates with external
volumes are refused.
