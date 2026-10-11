# hivekeep-imagecache

The node-local OCI image layer cache used by the keeper.

- A small registry client of its own (`registry`, `auth`, `backoff`), with
  per-registry backoff, Basic and Bearer challenges, digest-verified
  manifests, and cancellable blob streams.
- Hardened layer unpacking (`unpack`, `confined`): diffID verification,
  whiteout and opaque-dir capture, confinement to the cache root.
- `Store`: pulls each layer once into a pinned, content-addressed pool,
  backfills sizes, and evicts least-recently-used layers with two-phase
  retirement and startup recovery.
- Worker-side composition (`bundle`, Linux only): overlay rootfs and image
  volume mounts. Runtime-neutral OCI spec builders (`ocispec`), with gVisor
  and micro-VM shaping.
- Metric `hivekeep_imagecache_requests_total`.

The binary `hivekeep-validate-image-cache` pulls and unpacks a batch of
images and writes the results to CSV.

```sh
cargo test -p hivekeep-imagecache
```
