use std::env;
use std::path::Path;

/// Where to look for protoc when `PROTOC` is not set.
const FALLBACK_PROTOC: &[&str] = &[
    "/opt/homebrew/bin/protoc",
    "/usr/local/bin/protoc",
    "/usr/bin/protoc",
];

const KATA: &[&str] = &[
    "proto/kata/agent.proto",
    "proto/kata/oci.proto",
    "proto/kata/types.proto",
    "proto/kata/csi.proto",
];
const GUEST: &str = "proto/hivekeep/guest/v1/guest.proto";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("PROTOC").is_none() {
        if let Some(found) = FALLBACK_PROTOC.iter().find(|p| Path::new(p).exists()) {
            // SAFETY: build scripts are single-threaded at this point.
            unsafe { env::set_var("PROTOC", found) };
        }
    }
    println!("cargo:rerun-if-env-changed=PROTOC");
    for p in KATA.iter().chain([&GUEST]) {
        println!("cargo:rerun-if-changed={p}");
    }
    // The kata agent speaks ttrpc, not gRPC, so only its messages are
    // generated; the client is hand-written over the ttrpc framing.
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .compile_protos(KATA, &["proto/kata"])?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[GUEST], &["proto"])?;
    Ok(())
}
