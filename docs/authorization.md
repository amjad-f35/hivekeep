# Authorization

`hivekeep-api` checks every Control RPC against role bindings. These come
from the deployment-wide (global) AccessPolicy and from the AccessPolicy of
the Hive the target belongs to. The model is in
`crates/hivekeep-api/src/authz/`.

## Roles

| Role | Granted by | Meaning |
|------|-----------|---------|
| `owner` | global or Hive policy | Full control. A global owner owns every Hive. |
| `editor` | Hive policy only | Read and write within the Hive, but no access management. |
| `viewer` | global or Hive policy | Read only. A global viewer can view every Hive. |

Roles are cumulative: owner implies editor, and editor implies viewer.
Members are written `user:<principal>`. The principal is the JWT `sub` or
the client certificate's URI SAN (see [authentication.md](authentication.md)).
`user:*` and member strings containing control characters are rejected. One
policy may hold at most 1500 members across at most 100 bindings.

## What each role may do

| Object | owner | editor | viewer |
|--------|-------|--------|--------|
| Deployment (global) | create Hives; create or update the global policy | | list Hives, templates, Tags, Actors; get the global policy (global viewer) |
| Hive | delete it; create, update or delete its policy | create Actors, templates and Tags | get it; get its policy; list its Actors, templates, Tags |
| ActorTemplate | (as editor) | delete | get; use it to create Actors |
| Tag | (as editor) | update scope; delete; create Actors from it | get |
| Actor | (as editor) | update; delete; suspend; resume; revert; create, update or delete its egress policy | get; get its egress policy |
| Worker | global owner: update, delete, drain, create | | global viewer: get, list, list assignments |

The `owner` column inherits everything in the `editor` column, and
`editor` inherits everything in `viewer`.

## System roles

These roles are granted only to mTLS principals whose SPIFFE IDs match the
server's flags. The defaults are derived from `POD_NAMESPACE` (default
`hivekeep-system`):

| Role | Flag (default) | Allowed |
|------|----------------|---------|
| Controller | `--controller-spiffe-id` (`.../sa/hivekeep-controller`) | create, get, list, update, delete and drain Workers |
| Egress gateway | `--egress-gateway-spiffe-id` (`.../sa/hivekeep-gate-egress`) | list and get Actors, get egress policies, `MintActorJwt`, `MintActorCertificate` |
| Ingress router | `--ingress-router-spiffe-id` (`.../sa/hivekeep-gate-router`) | list Actors, `ResumeActor` |
| API sidecar | `--api-server-sidecar-spiffe-id` (`.../sa/hivekeep-api`) | get Actors |
| Keeper | the `--keeper-service-account` SPIFFE ID | get egress policies; all `WorkerService` calls for its own node |

Minting Actor credentials through Control is reserved for the egress
gateway. No tenant role can mint them.

## Bootstrapping

Until a global policy exists, nobody holds a global role. The principals
in `--authz-bootstrap-owners` are always global owners, whatever the stored
policy says, so they can create the first policy and repair a broken one.

```sh
# as a bootstrap owner
cat > global.yaml <<'EOF'
bindings:
- role: owner
  members: ["user:alice@example.com"]
- role: viewer
  members: ["user:system:serviceaccount:monitoring:dashboards"]
EOF
hivectl create access-policy --global -f global.yaml

cat > team-a.yaml <<'EOF'
bindings:
- role: editor
  members: ["user:system:serviceaccount:team-a:ci"]
EOF
hivectl create hive team-a
hivectl create access-policy -H team-a -f team-a.yaml
```

To replace a policy, pass its current `metadata.uid` and `metadata.version`.
`hivectl get access-policy ... -o yaml` prints both, ready for
`hivectl update access-policy -f`. The global policy cannot be deleted:
update it with no bindings instead.

> **Install default.** `hivekeep-setup` sets
> `--api-authz-bootstrap-owners` (env `HK_API_AUTHZ_BOOTSTRAP_OWNERS`) to
> `system:serviceaccount:hivekeep-system:hivekeep-client` unless you
> override it, so the demos and e2e suites work out of the box. Anyone who
> can mint a token for that ServiceAccount is then a global owner. On a
> shared or production cluster, set it to your operators' own principals.

## Development switch

`--authz-allow-all` lets every authenticated request through. It is for
local development only.

## Failure behavior

- A denied check returns `PERMISSION_DENIED` and names the member, the
  relation and the object.
- If a policy cannot be read, the call returns `UNAVAILABLE`. The server
  never falls back to allowing it.
- Identifiers are validated before the check is derived, so a malformed
  name can never select the wrong object.
