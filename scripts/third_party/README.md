# Third-party CSI driver bundles

Kubernetes manifests of the hostpath and NFS CSI drivers, taken from their
upstream `deploy/` trees and kept under their original Apache-2.0 license.
Local changes:

- `csi-driver-host-path/deploy/csi-hostpath-testing.yaml`: the controller is
  fronted by an Envoy mTLS proxy whose client certificate check admits only
  hivekeep-api, with serving and trust material from Hivekeep pod
  certificates (`servicedns.podcert.hivekeep.io`, `podidentity.podcert.hivekeep.io`).
- `csi-driver-host-path/deploy/csi-snapshot-metadata-sidecar.patch`: kept for
  reference; not applied by the kustomization.
- `csi-driver-nfs/deploy/example/storageclass-nfs.yaml`: renamed to
  `csi-nfs-sc` and pointed at the in-cluster development NFS server.

`hivekeep-setup setup csi` reads these from
`crates/hivekeep-setup/manifests/csi/third_party/`; run
`scripts/csi/sync-third-party.sh` to copy them there.
