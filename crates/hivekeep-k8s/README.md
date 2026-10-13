# hivekeep-k8s

Kubernetes types and helpers for Hivekeep.

- The `hivekeep.io/v1` CRDs: `WorkerPool`, `SandboxConfig` and
  `CSIDriverConfig`. The crate generates their CustomResourceDefinitions
  (`crds()`, `crds_yaml()`) and checks them against the schema, CEL and
  admission-policy rules shipped in `manifests/`.
- `Informer<K>`: a watch-backed cache with handlers.
- Helpers for the install layout, API error classification,
  `certificates.k8s.io` version discovery, ClusterTrustBundles,
  PodCertificateRequest-based pod identities for processes outside pods,
  and EndpointSlice-based endpoint resolution.
- `KubeClusterView`: the keeper's view of worker pods, pools and
  SandboxConfigs on its node.
- `KubeCsiDriverConfigs`: CSI driver settings read from `CSIDriverConfig`
  resources, for the keeper's and the API's CSI clients.
- `client_rate_limit`: a token-bucket tower layer and
  `rate_limited_client()` enforcing `--kube-api-qps`/`--kube-api-burst` on
  every request a `kube::Client` sends.
- `fake::FakeApiServer` (feature `testing`): an in-memory API server behind
  a real `kube::Client`, with CRUD, watch, server-side apply, finalizers and
  reactors. Other crates use it in their tests.

`k8s-openapi` is pinned to API level `v1_36`.
