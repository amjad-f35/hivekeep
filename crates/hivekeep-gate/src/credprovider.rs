//! The credential provider plugin API (`hivekeep.credprovider.v1`): secret
//! backends implement `CredentialProvider` so the egress gateway can fetch
//! credentials on an Actor's behalf without Hivekeep storing them.

#![allow(clippy::doc_lazy_continuation)]

/// Version 1 of the credential provider API.
pub mod v1 {
    tonic::include_proto!("hivekeep.credprovider.v1");
}
