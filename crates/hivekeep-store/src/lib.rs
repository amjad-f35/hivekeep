//! Persistence layer of the Hivekeep control plane.
//!
//! The contract is the [`Store`] trait (split into one trait per resource
//! family under [`api`]), with two backends:
//!
//! * [`memory::MemoryStore`]: complete, serializable semantics in process,
//!   for tests and single-replica development.
//! * [`postgres::PgStore`]: the production backend on PostgreSQL 13+.
//!
//! Both store resources as their encoded protobuf messages and apply the same
//! write rules ([`resource`]), so they are interchangeable behind
//! `Arc<dyn Store>`.

pub mod api;
pub mod error;
pub mod lease;
pub mod list;
pub mod memory;
pub mod page_token;
pub mod postgres;
pub mod precondition;
pub mod resource;
pub mod watch;

pub use api::*;
pub use error::{BoxError, Result, StoreError};
pub use lease::Lease;
pub use list::{ListOptions, ListResponse, DEFAULT_PAGE_SIZE};
pub use memory::MemoryStore;
pub use postgres::{PgConfig, PgStore};
pub use precondition::{DeletePreconditions, Precondition};
pub use resource::Resource;
pub use watch::{WorkerEvent, WorkerEventType, WorkerWatch};
