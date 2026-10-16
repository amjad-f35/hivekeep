//! Messages of the kata guest agent's protocol (packages `grpc` and `types`
//! of the agent's protobuf definitions).

#![allow(missing_docs, clippy::all, clippy::pedantic)]

/// The agent service's messages.
pub mod grpc {
    include!(concat!(env!("OUT_DIR"), "/grpc.rs"));
}

/// Shared network types.
pub mod types {
    include!(concat!(env!("OUT_DIR"), "/types.rs"));
}
