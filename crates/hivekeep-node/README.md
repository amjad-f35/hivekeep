# hivekeep-node

Pieces shared by the keeper, the backends that plug into it, and the
controllers that reuse its async plumbing. Keeping them here means
`hivekeep-k8s`, `hivekeep-imagecache`, `hivekeep-objectstore` and
`hivekeep-podcert-controller` do not depend on the keeper.

- `context` (cancellation plus deadlines, gRPC timeout parsing), `clock`
  (with a mock), `workqueue` (deduplicating, rate-limited keys).
- `paths`: the on-node layout under `/var/lib/hivekeep`.
- `credential_provider`: kubelet credential provider config and exec
  plugins for registry credentials.
- Traits: `ImageCache` and the overlay spec (`image`), `ImageGcStore`
  (`image_gc`), the keeper's `ObjectStore` (`objectstore`), the
  SandboxConfig model (`sandbox_config`), and `ClusterView`
  (`cluster_view`, with a JSON `StaticClusterView` for development).
- `csi`: a Container Storage Interface (v1) client for external volumes,
  used by the keeper (node service: stage and publish) and `hivekeep-api`
  (controller service: create, attach, detach, delete). It follows each
  driver's capabilities, speaks mTLS with the rotating pod identity to
  controller services, and opens drivers by name from a
  `CsiDriverConfigGetter`. `csi::fake` (feature `testing`) is an
  in-process driver for tests. The CSI protocol definitions are under
  `proto/csi/` (third-party, Apache-2.0, see `proto/csi/LICENSE`).
