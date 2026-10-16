//! The guest data plane API (`hivekeep.guest.v1`): process execution and
//! file I/O inside a sandbox.

#![allow(missing_docs, clippy::all, clippy::pedantic)]

/// Version 1.
pub mod v1 {
    tonic::include_proto!("hivekeep.guest.v1");
}
