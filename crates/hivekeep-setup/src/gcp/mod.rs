//! Provisioning the Google Cloud project a Hivekeep install runs on: APIs,
//! the GKE cluster, snapshot and asset buckets, the snapshot transfer
//! service account, IAM grants, an optional Cloud SQL store, and monitoring
//! dashboards. Everything is idempotent and goes through `gcloud`, behind
//! [`runner::Runner`] so it can be tested against a recording fake.
//!
//! Mounted as `hivekeep-setup gcp ...` through [`command`] and [`run`].

pub mod bucket;
pub mod cli;
pub mod cloudsql;
pub mod cluster;
pub mod config;
pub mod dashboards;
pub mod policy;
pub mod project;
pub mod runner;
pub mod transfer;

pub use cli::{command, run, run_with};
pub use config::GcpConfig;

/// A provisioning failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum GcpError {
    /// A missing or inconsistent flag, reported before anything changes.
    #[error("{0}")]
    Usage(String),
    /// gcloud ran and failed.
    #[error("{what}: {stderr}")]
    Command { what: String, stderr: String },
    #[error("{0}")]
    Other(String),
}
