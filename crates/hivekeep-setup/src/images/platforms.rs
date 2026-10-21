//! Which platforms an image build targets.
//!
//! Every Hivekeep image is built for the same set, so a Dockerfile image
//! installed beside the component images runs on the same nodes: the
//! explicit `HK_BUILD_PLATFORMS`, else `defaultPlatforms` from the build
//! config (`.hivekeep-build.yaml` at the repository root, or the file or
//! directory `HK_BUILD_CONFIG_PATH` names), else `linux/amd64`.

use super::ImageError;
use std::path::{Path, PathBuf};

/// Environment variable naming the build config file or its directory.
pub const BUILD_CONFIG_PATH_ENV: &str = "HK_BUILD_CONFIG_PATH";
/// The build config's file name.
pub const BUILD_CONFIG_FILE: &str = ".hivekeep-build.yaml";
const DEFAULT_PLATFORM: &str = "linux/amd64";

/// The `--platform` value for a build from `root`.
pub fn build_platforms(root: &Path, explicit: &str) -> Result<String, ImageError> {
    let config_path = std::env::var(BUILD_CONFIG_PATH_ENV).unwrap_or_default();
    build_platforms_with(root, explicit, &config_path)
}

/// [`build_platforms`] with the config path given rather than read from the
/// environment.
pub fn build_platforms_with(
    root: &Path,
    explicit: &str,
    config_path: &str,
) -> Result<String, ImageError> {
    if !explicit.is_empty() {
        return Ok(explicit.to_string());
    }
    let mut path: PathBuf = root.join(BUILD_CONFIG_FILE);
    if !config_path.is_empty() {
        // Builds run from the root, so a relative path is relative to it.
        let p = if Path::new(config_path).is_absolute() {
            PathBuf::from(config_path)
        } else {
            root.join(config_path)
        };
        path = if p.is_dir() {
            p.join(BUILD_CONFIG_FILE)
        } else {
            p
        };
    }
    let raw = match std::fs::read_to_string(&path) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(DEFAULT_PLATFORM.into()),
        Err(e) => {
            return Err(ImageError::Other(format!(
                "while reading the build config: {e}"
            )))
        }
    };
    #[derive(serde::Deserialize, Default)]
    struct BuildConfig {
        #[serde(default, rename = "defaultPlatforms")]
        default_platforms: Vec<String>,
    }
    let parsed: Option<BuildConfig> = serde_yaml::from_str(&raw)
        .map_err(|e| ImageError::Other(format!("while parsing {}: {e}", path.display())))?;
    let platforms = parsed.unwrap_or_default().default_platforms;
    if platforms.is_empty() {
        return Ok(DEFAULT_PLATFORM.into());
    }
    Ok(platforms.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_selection() {
        const TWO: &str = "defaultPlatforms:\n  - linux/amd64\n  - linux/arm64\n";
        struct Case {
            name: &'static str,
            yaml: &'static str,
            file: &'static str,
            config_path: &'static str,
            explicit: &'static str,
            want: &'static str,
        }
        let cases = [
            Case {
                name: "explicit platforms win",
                yaml: TWO,
                file: "",
                config_path: "",
                explicit: "linux/s390x",
                want: "linux/s390x",
            },
            Case {
                name: "build config",
                yaml: TWO,
                file: "",
                config_path: "",
                explicit: "",
                want: "linux/amd64,linux/arm64",
            },
            Case {
                name: "build config without platforms",
                yaml: "builder: default\n",
                file: "",
                config_path: "",
                explicit: "",
                want: "linux/amd64",
            },
            Case {
                name: "no build config",
                yaml: "",
                file: "",
                config_path: "",
                explicit: "",
                want: "linux/amd64",
            },
            Case {
                name: "config path file",
                yaml: TWO,
                file: "conf/build.yaml",
                config_path: "conf/build.yaml",
                explicit: "",
                want: "linux/amd64,linux/arm64",
            },
            Case {
                name: "config path directory",
                yaml: TWO,
                file: "conf/.hivekeep-build.yaml",
                config_path: "conf",
                explicit: "",
                want: "linux/amd64,linux/arm64",
            },
        ];
        for c in cases {
            let root = tempfile::tempdir().unwrap();
            if !c.yaml.is_empty() {
                let file = root.path().join(if c.file.is_empty() {
                    BUILD_CONFIG_FILE
                } else {
                    c.file
                });
                std::fs::create_dir_all(file.parent().unwrap()).unwrap();
                std::fs::write(file, c.yaml).unwrap();
            }
            let got = build_platforms_with(root.path(), c.explicit, c.config_path).unwrap();
            assert_eq!(got, c.want, "{}", c.name);
        }
    }
}
