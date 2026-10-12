# hivekeep-objectstore

Object storage for snapshots and runtime assets.

- `storage`: the sparse-extent snapshot format (magic `HKSPARSE`, v2),
  parallel zstd, budgeted range-parallel uploads, ranged downloads, and
  hole-punching restore.
- `gcs` and `s3`: hand-written clients over `reqwest`, with GCS compose and
  rewrite, S3 multipart and UploadPartCopy, and SigV4. `HK_STORAGE_BACKEND=s3`
  selects S3. Anything else selects GCS.
- `store`: listing, copying and deleting whole snapshot prefixes.
- `plugin`: the `hivekeep.objectstore.v1` plugin API on a Unix socket
  (NodeProvider, ControlProvider, AssetProvider), and the credential
  exchange that issues per-Actor downscoped GCS credentials to node
  plugins.
- `keeper_store` and `control_client`: the keeper's and the control plane's
  clients, either over the plugin socket or in process.

The binary `hivekeep-objectstore-plugin` runs as the keeper's node sidecar,
as the API pod's control sidecar, or as a health check.

```sh
cargo test -p hivekeep-objectstore
```
