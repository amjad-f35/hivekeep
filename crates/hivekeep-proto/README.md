# hivekeep-proto

The protobuf messages and gRPC services that Hivekeep components use to
talk to each other.

| Package | Module | Services |
|---------|--------|----------|
| `hivekeep.api.v1` (`proto/hivekeep/api/v1/api.proto`) | `api::v1` | `Control` (clients), `WorkerService` (keepers) |
| `hivekeep.keeper.v1` (`proto/hivekeep/keeper/v1/keeper.proto`) | `keeper::v1` | `Keeper` (served by the keeper to the control plane), `WorkerSupport` (served by the keeper to workers) |
| `hivekeep.worker.v1` (`proto/hivekeep/worker/v1/worker.proto`) | `worker::v1` | `Worker` (served by sandbox workers to the keeper) |
| `hivekeep.config.v1` (`proto/hivekeep/config/v1/config.proto`) | `config::v1` | the `hivekeep-api` config file |

`build.rs` generates clients and servers with `tonic-prost-build` and embeds
a descriptor set (`FILE_DESCRIPTOR_SET`) for reflection and protojson. It
needs `protoc`: it uses `PROTOC` if set, then the Homebrew or system
copies. Run `make protoc` from the repo root to fetch the pinned one.

Field rules are documented as `Rules:` lines on each field. Never change or
reuse field numbers. Public API changes must pass `hivekeep-apitool
validate` (see `hivekeep-devtools`). The API itself is described in
[docs/api-guide.md](../../docs/api-guide.md).
