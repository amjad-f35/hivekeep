# hivekeep-core

Domain logic shared by the control plane, the keeper, the workers and the
CLI. It does no I/O.

- Formats and identities: resource names (`names`), references
  (`resource`), URIs (`uri`), SPIFFE IDs (`spiffe`), image references
  (`image_ref`), egress hostname patterns (`hostname`).
- Snapshot addressing (`snapshot`): the owner-prefixed object layout
  `<location>/hives/<hive>/{actors,tags}/<uid>/...`.
- The Actor lifecycle state machine (`actor_state`).
- Placement arithmetic (`compute`), sandbox classes (`sandbox_class`),
  and runtime compatibility (`hardware`, `cpu_features`).
- Preview gates (`preview`) and access-policy members (`access`).
- Validation of every API, keeper and worker message (`validation`), and
  defaults (`defaults`).
- Telemetry attribute keys in the `hk.` namespace (`attr`), Actor log
  forwarding (`actor_log`) and the Actor event vocabulary (`actor_event`,
  pinned by `registry/events.yaml`).

```sh
cargo test -p hivekeep-core
```
