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
pub mod clients;
pub mod cmd;
pub mod collector_metrics;
pub mod crd;
pub mod credprovider;
pub mod env;
pub mod fixture;
pub mod fixtures;
pub mod harness;
pub mod images;
pub mod install;
pub mod kube_util;
pub mod manifest;
pub mod portforward;
pub mod preflight;
pub mod pretty;
pub mod preview;
pub mod proto;
pub mod protojson;
pub mod retry;
pub mod roottest;
pub mod sandbox;
pub mod settings;
pub mod statusz;
pub mod template;
pub mod testcert;
pub mod testenv;
