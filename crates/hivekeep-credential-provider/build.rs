use std::env;

const PROTOS: &[&str] = &["proto/hivekeep/credprovider/v1/credprovider.proto"];

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
    let out_dir = std::path::PathBuf::from(env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(out_dir.join("credprovider_descriptor.bin"))
        .compile_protos(PROTOS, &["proto"])?;
    Ok(())
}
