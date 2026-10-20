# hivekeep-podcert-controller

Answers Kubernetes PodCertificateRequests for Hivekeep's three signers,
each backed by its own CA pool (binary `hivekeep-podcert-controller`):

| Signer | Issues |
|--------|--------|
| `podidentity.podcert.hivekeep.io/identity` | Certificates carrying the pod's ServiceAccount SPIFFE ID and a pod-identity extension |
| `servicedns.podcert.hivekeep.io/identity` | Serving certificates for `<svc>.<ns>.svc`, for each Service that selects the pod |
| `postgres.podcert.hivekeep.io/identity` | PostgreSQL client-login certificates for the control plane's ServiceAccount, limited to the bundled logins |

Each signer keeps a `<prefix>primary-bundle` ClusterTrustBundle up to date.
Replicas shard requests by rendezvous hashing over Leases. Leaf
certificates are backdated 2 minutes, refreshed within 30 minutes of
expiry, and capped at 24 hours. `--kube-api-qps`/`--kube-api-burst` limit
Kubernetes API requests client side (off by default).

```sh
cargo test -p hivekeep-podcert-controller
```
