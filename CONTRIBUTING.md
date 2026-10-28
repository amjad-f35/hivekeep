# Contributing to Hivekeep

Thanks for helping. Bug reports, fixes, tests and docs are all welcome.

## Before you start

- For anything larger than a small fix, open an issue first and describe the
  problem and the change you have in mind. A short design note saves a lot of
  review time on changes to the API, the CRDs, the database schema or
  on-disk formats.
- Security problems go through [SECURITY.md](SECURITY.md), not public issues.

## Making a change

1. Set up the toolchain: Rust 1.98 or newer, `protoc` (`make protoc`), and
   optionally the pinned tools (`make tools`).
2. Keep changes focused. One concern per commit, and one concern per module
   where you can.
3. Add or update tests next to the code you change. Tests that need root, a
   live cluster, PostgreSQL or a cloud bucket are `#[ignore]`d. Say in the
   ignore reason what they need (see [docs/development.md](docs/development.md)).
4. Run the checks before you push:

   ```sh
   make fmt
   make lint
   make test
   make verify
   ```

5. If you change behavior, update the matching page under `docs/`. If you
   add a production-relevant default (a port, path, domain, image reference
   or endpoint), add a row to [PLACEHOLDERS.md](PLACEHOLDERS.md).

## Conventions

- Errors are typed with `thiserror` in libraries. `anyhow` is for binaries
  only.
- Async code uses `tokio`, and gRPC uses `tonic` and `prost`.
- Linux-only code sits behind `#[cfg(target_os = "linux")]`, so the
  workspace still builds and tests on macOS.
- Binaries install the `ring` rustls provider at startup.
- Public API changes in `crates/hivekeep-proto/proto/hivekeep/api/v1/api.proto`
  must pass `hivekeep-apitool validate`. Keep field numbers stable.
- Database migrations in `crates/hivekeep-store/migrations` are
  append-only. `scripts/verify/migrations.sh <base-ref>` rejects edits to
  migrations that already exist.
- Shell scripts must pass `make shellcheck`.

## Pull requests

Describe what changed and why, how you tested it, and anything a reviewer
should look at closely. A maintainer reviews every change before it merges.

By contributing, you agree that your contribution is licensed under the
Apache License 2.0 (see [LICENSE](LICENSE)).
