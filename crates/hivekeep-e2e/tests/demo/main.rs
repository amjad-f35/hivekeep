//! The demo suite: Actor lifecycle end to end against per-test copies of the
//! counter demo (create, pause, suspend, resume, revert, delete), golden
//! snapshots, template updates and worker termination.
//!
//! Every test needs a live cluster with the counter demo installed and is
//! `#[ignore]`d; run with `HK_E2E=1 HK_E2E_KUBECONFIG=... cargo test -p hivekeep-e2e
//! --test demo -- --ignored`. Suite setup (clients, preflight, end-of-run
//! namespace handling) happens in `hivekeep_e2e::suite::e2e_test`.

mod golden;
mod helpers;
mod lifecycle;
mod termination;
mod updatetemplate;
