//! The OCI and Docker v2 documents a pull reads: image manifests, indexes
//! (multi-platform manifest lists) and image config files.

use serde::{Deserialize, Serialize};

use crate::digest::Digest;
use crate::error::{Error, Result};

/// OCI image manifest.
pub const MEDIA_OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
/// OCI image index.
pub const MEDIA_OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
/// Docker v2 schema 2 manifest.
pub const MEDIA_DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
/// Docker v2 manifest list.
pub const MEDIA_DOCKER_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
/// OCI image config.
pub const MEDIA_OCI_CONFIG: &str = "application/vnd.oci.image.config.v1+json";
/// gzip-compressed OCI layer.
pub const MEDIA_OCI_LAYER_GZIP: &str = "application/vnd.oci.image.layer.v1.tar+gzip";
/// zstd-compressed OCI layer.
pub const MEDIA_OCI_LAYER_ZSTD: &str = "application/vnd.oci.image.layer.v1.tar+zstd";
/// Uncompressed OCI layer.
pub const MEDIA_OCI_LAYER_TAR: &str = "application/vnd.oci.image.layer.v1.tar";
/// gzip-compressed Docker layer.
pub const MEDIA_DOCKER_LAYER_GZIP: &str = "application/vnd.docker.image.rootfs.diff.tar.gzip";

/// Every manifest type a pull accepts, for the `Accept` header.
pub const ACCEPTED_MANIFESTS: &[&str] = &[
    MEDIA_OCI_MANIFEST,
    MEDIA_DOCKER_MANIFEST,
    MEDIA_OCI_INDEX,
    MEDIA_DOCKER_LIST,
];

/// Whether `media_type` is a multi-platform index.
pub fn is_index(media_type: &str) -> bool {
    media_type == MEDIA_OCI_INDEX || media_type == MEDIA_DOCKER_LIST
}

/// How a layer blob is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Not compressed.
    None,
    /// gzip.
    Gzip,
    /// zstd.
    Zstd,
}

/// The compression a layer media type declares.
pub fn layer_compression(media_type: &str) -> Result<Compression> {
    match media_type {
        MEDIA_OCI_LAYER_GZIP | MEDIA_DOCKER_LAYER_GZIP | "" => Ok(Compression::Gzip),
        MEDIA_OCI_LAYER_ZSTD => Ok(Compression::Zstd),
        MEDIA_OCI_LAYER_TAR => Ok(Compression::None),
        other => Err(Error::msg(format!(
            "unsupported layer media type {other:?}"
        ))),
    }
}

/// A content descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
    /// The content's media type.
    #[serde(default)]
    pub media_type: String,
    /// The content's size in bytes.
    #[serde(default)]
    pub size: i64,
    /// The content's digest.
    pub digest: Digest,
    /// For index entries, the platform the manifest is for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

/// An image manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    /// Always 2.
    pub schema_version: i64,
    /// The manifest's media type.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub media_type: String,
    /// The image config blob.
    pub config: Descriptor,
    /// The layer blobs, bottom-most first.
    #[serde(default)]
    pub layers: Vec<Descriptor>,
}

/// A multi-platform index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    /// Always 2.
    pub schema_version: i64,
    /// The index's media type.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub media_type: String,
    /// One manifest per platform.
    #[serde(default)]
    pub manifests: Vec<Descriptor>,
}

/// A target platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Platform {
    /// CPU architecture, e.g. `amd64`.
    #[serde(default)]
    pub architecture: String,
    /// Operating system, e.g. `linux`.
    #[serde(default)]
    pub os: String,
    /// CPU variant, e.g. `v8`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub variant: String,
}

impl Platform {
    /// `linux` on the architecture this binary was built for.
    pub fn host_linux() -> Self {
        let arch = match std::env::consts::ARCH {
            "x86_64" => "amd64",
            "aarch64" => "arm64",
            "x86" => "386",
            "powerpc64" => "ppc64le",
            other => other,
        };
        Platform {
            architecture: arch.to_string(),
            os: "linux".to_string(),
            variant: String::new(),
        }
    }

    /// Whether an index entry for `candidate` serves this platform. An
    /// unset variant here matches any.
    pub fn matches(&self, candidate: &Platform) -> bool {
        self.architecture == candidate.architecture
            && self.os == candidate.os
            && (self.variant.is_empty() || self.variant == candidate.variant)
    }
}

/// The parts of an image config a container runtime reads. Unknown fields
/// are kept, so a stored config round-trips.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ImageConfig {
    /// `KEY=value` environment entries.
    #[serde(
        rename = "Env",
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "nullable_vec"
    )]
    pub env: Vec<String>,
    /// The entrypoint.
    #[serde(
        rename = "Entrypoint",
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "nullable_vec"
    )]
    pub entrypoint: Vec<String>,
    /// The command.
    #[serde(
        rename = "Cmd",
        default,
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "nullable_vec"
    )]
    pub cmd: Vec<String>,
    /// The working directory.
    #[serde(
        rename = "WorkingDir",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub working_dir: String,
    /// The user to run as.
    #[serde(rename = "User", default, skip_serializing_if = "String::is_empty")]
    pub user: String,
    /// Everything else.
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

fn nullable_vec<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<String>, D::Error> {
    Ok(Option::<Vec<String>>::deserialize(d)?.unwrap_or_default())
}

/// The root filesystem section of a config file.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct RootFs {
    /// Always `layers`.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// The uncompressed layer digests, bottom-most first.
    #[serde(default)]
    pub diff_ids: Vec<Digest>,
}

/// An image config file.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ConfigFile {
    /// CPU architecture.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub architecture: String,
    /// Operating system.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub os: String,
    /// The runtime config.
    #[serde(default)]
    pub config: ImageConfig,
    /// The layer list.
    #[serde(default)]
    pub rootfs: RootFs,
}

impl From<&ImageConfig> for hivekeep_node::image::ImageConfig {
    fn from(c: &ImageConfig) -> Self {
        hivekeep_node::image::ImageConfig {
            env: c.env.clone(),
            entrypoint: c.entrypoint.clone(),
            cmd: c.cmd.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_round_trips_unknown_fields() {
        let raw = r#"{"Env":["A=b"],"Cmd":null,"Labels":{"x":"y"},"StopSignal":"SIGTERM"}"#;
        let c: ImageConfig = serde_json::from_str(raw).unwrap();
        assert_eq!(c.env, vec!["A=b"]);
        assert!(c.cmd.is_empty());
        let back = serde_json::to_value(&c).unwrap();
        assert_eq!(back["Labels"]["x"], "y");
        assert_eq!(back["StopSignal"], "SIGTERM");
    }

    #[test]
    fn platform_matching() {
        let want = Platform {
            architecture: "arm64".into(),
            os: "linux".into(),
            variant: String::new(),
        };
        let v8 = Platform {
            variant: "v8".into(),
            ..want.clone()
        };
        assert!(want.matches(&v8));
        assert!(!v8.matches(&want));
        assert!(layer_compression("application/x-foo").is_err());
        assert_eq!(
            layer_compression(MEDIA_OCI_LAYER_ZSTD).unwrap(),
            Compression::Zstd
        );
    }
}
