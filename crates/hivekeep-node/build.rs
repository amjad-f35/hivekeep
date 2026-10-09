use std::env;

/// The third-party protocol definitions this crate speaks.
const PROTOS: &[&str] = &["proto/csi/csi.proto"];

/// Where to look for protoc when `PROTOC` is not set.
const FALLBACK_PROTOC: &[&str] = &[
    "/opt/homebrew/bin/protoc",
    "/usr/local/bin/protoc",
    "/usr/bin/protoc",
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
    // Servers are generated too, for the fake driver tests run against;
    // every RPC a fake leaves out answers Unimplemented, as a real driver
    // lacking that capability does.
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .generate_default_stubs(true)
        .compile_protos(PROTOS, &["proto"])?;
    Ok(())
}
