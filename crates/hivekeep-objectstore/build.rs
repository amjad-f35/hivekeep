use std::env;

const PROTOS: &[&str] = &[
    "proto/hivekeep/objectstore/v1/objectstore.proto",
    "proto/hivekeep/objectstore/exchange/v1/exchange.proto",
];

/// Where to look for protoc when `PROTOC` is not set.
const FALLBACK_PROTOC: &[&str] = &[
    "/opt/homebrew/bin/protoc",
    "/usr/local/bin/protoc",
    "/usr/bin/protoc",
];

/// Messages carrying a credential or a URI that may hold one; their Debug is
/// written by hand so it never prints those fields.
const REDACTED: &[&str] = &[
    "hivekeep.objectstore.v1.FetchSnapshotRequest",
    "hivekeep.objectstore.v1.UploadSnapshotRequest",
    "hivekeep.objectstore.v1.FetchAssetRequest",
    "hivekeep.objectstore.exchange.v1.ExchangeActorJwtRequest",
    "hivekeep.objectstore.exchange.v1.ExchangeActorJwtResponse",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if env::var_os("PROTOC").is_none() {
        if let Some(found) = FALLBACK_PROTOC
            .iter()
            .find(|p| std::path::Path::new(p).exists())
        {
            // SAFETY: build scripts are single-threaded at this point.
            unsafe { env::set_var("PROTOC", found) };
        }
    }
    println!("cargo:rerun-if-env-changed=PROTOC");
    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .skip_debug(REDACTED.iter().copied())
        .compile_protos(PROTOS, &["proto"])?;
    Ok(())
}
