//! Developer tooling for Hivekeep.
//!
//! - [`apitool`] lints the public API definition (`hivekeep.api.v1`) against the
//!   project's API style rules, with a checked-in exemptions file.
//! - [`junittool`] makes CI fail closed: it records which JUnit files a run is
//!   expected to write, verifies each one reported tests, and summarizes failures
//!   from libtest JSON event streams and cargo-nextest JUnit reports.

pub mod apitool;
