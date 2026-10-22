# hivekeep-devtools

Developer tooling.

- `hivekeep-apitool validate [--update] [--proto PATH] [--exemptions PATH]`
  lints `hivekeep.api.v1` against the project's API rules: standard method
  shapes, naming, resource metadata and status fields, no oneofs, and
  documented elements. Accepted exceptions are kept in `exemptions.json`.
- `hivekeep-junittool register | verify | report` makes CI fail closed. It
  records which JUnit files a run must produce, checks that each one
  reported tests, and summarizes failures from libtest JSON event streams
  (`cargo test -- -Z unstable-options --format json`) and cargo-nextest
  JUnit reports, annotating each with its panic location under the
  workspace root.

```sh
cargo run -p hivekeep-devtools --bin hivekeep-apitool -- validate
```
