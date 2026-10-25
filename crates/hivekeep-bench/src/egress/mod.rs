//! The egress-gateway scale benchmark.
//!
//! Its programs, each a subcommand of `egress-bench`:
//! - [`actor`]: runs inside an Actor, holds one kept-alive tunnel per endpoint
//!   open through the egress gateway at a fixed pace, and counts what
//!   happened, served to a driver over a small HTTP control API.
//! - [`target`]: the endpoint every tunnel leads to, answering each request
//!   with a small fixed body over HTTP and HTTPS.
//! - [`targetcert`]: the name-constrained CA and serving certificate the
//!   target uses over HTTPS.

pub mod json;

pub mod actor;
pub mod target;
pub mod targetcert;
