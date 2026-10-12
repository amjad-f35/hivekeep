# PostgreSQL

All control-plane state lives in PostgreSQL through `hivekeep-store`'s
`PgStore`. PostgreSQL 13 or newer is required, and the server version is
checked at startup. Every resource is stored as its encoded protobuf
message, with the columns needed for keys, foreign keys and listing.

`hivekeep-api --memory-store` replaces the database with an in-process store
that supports a single replica and loses everything on restart. It is for
development only.

## Connection settings

| Flag | Env | Default | Meaning |
|------|-----|---------|---------|
| `--postgres-read-write-connection-string` | `HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING` | (required) | URL used to serve requests |
| `--postgres-owner-connection-string` | `HK_API_POSTGRES_OWNER_CONNECTION_STRING` | read-write URL | URL used to create the schema and run migrations |
| `--postgres-read-write-role` | `HK_API_POSTGRES_READ_WRITE_ROLE` | | Role assumed by read-write connections |
| `--postgres-owner-role` | `HK_API_POSTGRES_OWNER_ROLE` | | Role assumed by owner connections |
| `--postgres-schema` | `HK_API_POSTGRES_SCHEMA` | `hivekeep` | Schema holding the tables |
| `--postgres-pool-max-conns` | `HK_API_POSTGRES_POOL_MAX_CONNS` | `0` (driver default) | Cap on read-write connections |

Keep credentials out of the URL where you can. Bundled installs use client
certificates instead of passwords (see below). If the database is not
reachable at startup, `hivekeep-api` retries 30 times, 2 seconds apart,
before it gives up.

The store keeps separate connection pools for requests, watches and health
probes. The health probe has its own connection, so `/readyz` reflects the
database even when the request pool is saturated.

## Roles and schema bootstrap

Hivekeep separates the role that owns the schema from the role that serves
requests. `crates/hivekeep-store/src/postgres/setup.sql` is a one-time,
idempotent bootstrap an administrator runs with `psql` before the first
start. It creates both roles, a login user for each, and the schema. It
refuses to adopt existing roles or a schema whose attributes conflict.

```sh
psql "$ADMIN_URL" \
  -v hivekeep_schema=hivekeep \
  -v hivekeep_owner_role=hivekeep_owner -v hivekeep_owner_user=hivekeep_owner_login \
  -v hivekeep_owner_password= \
  -v hivekeep_readwrite_role=hivekeep_rw -v hivekeep_readwrite_user=hivekeep_rw_login \
  -v hivekeep_readwrite_password= \
  -f crates/hivekeep-store/src/postgres/setup.sql
```

An empty password leaves login by password disabled, for installs that
authenticate by certificate. The bundled install runs this setup for you
through `kubectl exec`.

## Migrations

Migrations are embedded from `crates/hivekeep-store/migrations/`
(currently `0001_initial.sql`). They are applied at startup over the owner
connection, and the run is serialized across replicas. Applied migrations
that this build does not know about are tolerated, so a replica can be
rolled back after a newer one has moved the schema forward.

Rules for new migrations, enforced by `scripts/verify/migrations.sh`:

- Name them `NNNN_name.sql`, numbered without gaps.
- Don't manage transactions inside a migration (each one already runs in
  its own), and don't use `IF NOT EXISTS` to hide drift.
- Never edit or delete a migration that has shipped. Run
  `scripts/verify/migrations.sh <base-ref>` to check against a base branch.
- Every change must stay readable by the previous release (see
  [version-skew-policy.md](version-skew-policy.md)).

## Tables

| Table | Holds |
|-------|-------|
| `hives`, `actors`, `actor_templates`, `tags` | Hive-scoped resources (foreign keys to `hives`) |
| `actor_egress_policies` | Each Actor's EgressPolicy |
| `global_access_policy`, `hive_access_policies` | Access policies |
| `workers`, `worker_assignments` | The Worker registry and Actor bindings, which are bound and released under row locks |
| `worker_events`, `worker_events_trim` | Transactional outbox of Worker changes, polled by every API replica's Worker watch and trimmed after 15 minutes |
| `leases` | TTL leases that serialize operations on one resource, renewed in the background |
| `_sqlx_migrations` | Migration ledger |

## Deployment options

- **Bundled.** `hivekeep-setup deploy hivekeep-system` (or `deploy
  postgres`) brings up a single-replica PostgreSQL StatefulSet named
  `postgres` in `hivekeep-system`. `hivekeep-api` logs in with client
  certificates from the `postgres.podcert.hivekeep.io/identity` signer,
  which only issues the bundled login usernames to the control plane's
  ServiceAccount. Use `--api-postgres-storage-class` for the volume, and
  `--cluster-size=size10` for the larger profile. This option is
  experimental: it has no replication and no backups.
- **External.** Set `HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING` (and
  usually the owner URL) before `deploy hivekeep-system`, and the bundled
  StatefulSet is skipped. Use `--api-postgres-server-ca-file` to trust a
  private server CA.
- **Cloud SQL.** Set `HK_API_POSTGRES_CLOUDSQL_INSTANCE`,
  `HK_API_POSTGRES_CLOUDSQL_GSA`, `HK_API_POSTGRES_CLOUDSQL_IAM_AUTH` and
  `HK_API_POSTGRES_CLOUDSQL_IP_TYPE`. The installer then adds the Cloud SQL
  proxy sidecar to `hivekeep-api` and builds a passwordless DSN.
  `hivekeep-setup gcp create cloudsql` provisions a private-IP instance
  with IAM authentication.

## Testing against PostgreSQL

The store's contract suite runs in memory by default. To run it, and the
PostgreSQL-only tests, against a real server:

```sh
HK_TEST_DATABASE_URL=postgres://user@localhost/hivekeep_test \
  cargo test -p hivekeep-store -- --include-ignored
```
