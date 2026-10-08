# hivekeep-store

The persistence layer of `hivekeep-api`. The contract is the `Store` trait,
split into one trait per resource family under `api`. There are two
interchangeable backends:

- `MemoryStore`, with full semantics in process, for tests and
  single-replica development (`hivekeep-api --memory-store`).
- `PgStore`, on PostgreSQL 13+. Migrations are embedded from `migrations/`.
  It provides TTL leases, a transactional Worker event outbox for watches,
  role-separated connections, and a separate health-probe connection.
  `src/postgres/setup.sql` is the one-time role and schema bootstrap.

Resources are stored as encoded protobuf messages, and writes are guarded
by `version` and `uid` preconditions.

```sh
cargo test -p hivekeep-store                       # contract suite in memory
HK_TEST_DATABASE_URL=postgres://... cargo test -p hivekeep-store -- --include-ignored
```

See [docs/postgres.md](../../docs/postgres.md).
