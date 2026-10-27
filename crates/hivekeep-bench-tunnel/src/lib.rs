//! A benchmark of the worker tunnel's own CPU and memory cost.
//!
//! The benchmark is one program with a subcommand per process: a load
//! generator, the tunnel hosted by the worker's own code, the worker pod
//! around it (a fake keeper, cgroups, the Actors' stand-ins), and a fake
//! egress gateway. This library holds the parts those subcommands share.

pub mod admin;
pub mod durfmt;
pub mod error;
pub mod fake_keeper;
pub mod httpio;
pub mod load;
pub mod pki;
pub mod procstats;
pub mod report;
pub mod sandbox_net;
pub mod sink;
pub mod supervisor;
pub mod workerhost;

pub use error::{Error, Result};
