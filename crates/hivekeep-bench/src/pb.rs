//! Protobuf messages and gRPC services owned by the benchmark tooling.
//!
//! | package                | module          | contents                                  |
//! |------------------------|-----------------|-------------------------------------------|
//! | `hivekeep.glutton.v1`  | [`glutton::v1`] | the glutton workload's `Glutton` service  |
//! | `hivekeep.guest.v1`    | [`guest::v1`]   | the sandbox guest's process and file APIs |

#![allow(
    clippy::doc_lazy_continuation,
    clippy::large_enum_variant,
    clippy::enum_variant_names,
    missing_docs
)]

/// The glutton benchmark workload.
pub mod glutton {
    /// Version 1 of the glutton API.
    pub mod v1 {
        tonic::include_proto!("hivekeep.glutton.v1");
    }
}

/// The sandbox guest data plane (process execution and file I/O), driven by
/// the guest-environment load class.
pub mod guest {
    /// Version 1 of the guest API.
    pub mod v1 {
        tonic::include_proto!("hivekeep.guest.v1");
    }
}

/// Encoded `FileDescriptorSet` of the protos above, for gRPC reflection.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/bench_descriptor.bin"));

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn descriptor_set_carries_both_packages() {
        let set = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
        let pkgs: Vec<_> = set.file.iter().filter_map(|f| f.package.clone()).collect();
        assert!(pkgs.iter().any(|p| p == "hivekeep.glutton.v1"));
        assert!(pkgs.iter().any(|p| p == "hivekeep.guest.v1"));
    }

    #[test]
    fn use_cpu_cap_keeps_field_four() {
        let req = glutton::v1::UseCpuRequest {
            cap_at_max_parallelism: Some(false),
            ..Default::default()
        };
        assert_eq!(req.encode_to_vec(), vec![0x20, 0x00]);
    }
}
