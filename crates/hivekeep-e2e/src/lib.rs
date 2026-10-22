//! End-to-end harness for a live Hivekeep cluster.
//!
//! The library half holds what every suite shares: a small test context
//! ([`harness::T`]) with LIFO cleanups and skip/fatal, cluster clients,
//! fixture rendering and deployment, and helpers for the gate, the egress
//! trust bundle and the metrics pipeline. The suites live under `tests/`, one
//! file per suite; every cluster test is `#[ignore]`d and also requires
//! `HK_E2E=1`, reading the kubeconfig from `HK_E2E_KUBECONFIG` (else
//! `KUBECONFIG`). So `cargo test` stays offline and
//! `HK_E2E=1 HK_E2E_KUBECONFIG=... cargo test -p hivekeep-e2e -- --ignored`
//! runs them.
//!
//! The fixture servers the suites deploy are in [`fixtures`], shipped as the
//! `hivekeep-e2e-testserver` and `hivekeep-e2e-probe` binaries.

pub mod backend;
pub mod crd;
pub mod fixtures;
pub mod harness;
pub mod proto;
pub mod roottest;
pub mod testcert;
