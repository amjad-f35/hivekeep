//! The credential provider plugin API (`hivekeep.credprovider.v1`).

#![allow(clippy::doc_lazy_continuation)]

/// Version 1 of the credential provider API.
pub mod v1 {
    tonic::include_proto!("hivekeep.credprovider.v1");
}

/// Encoded `FileDescriptorSet` of the API, for gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("credprovider_descriptor");
