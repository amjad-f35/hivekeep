//! The kubelet's CredentialProviderConfig, and the plugins it names.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use super::matcher::matches_image;
use super::plugin::Plugin;

/// The credential provider API version plugins must speak.
pub const SUPPORTED_API_VERSION: &str = "credentialprovider.kubelet.k8s.io/v1";
const CONFIG_API_VERSION: &str = "kubelet.config.k8s.io/v1";
const CONFIG_KIND: &str = "CredentialProviderConfig";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigFile {
    kind: String,
    api_version: String,
    #[serde(default)]
    providers: Vec<ProviderConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderConfig {
    // Defaulted so a nameless entry is skipped as unusable rather than
    // failing the whole config.
    #[serde(default)]
    name: String,
    #[serde(default)]
    match_images: Vec<String>,
    default_cache_duration: Option<String>,
    #[serde(default)]
    api_version: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: Vec<EnvVar>,
    #[serde(default)]
    token_attributes: Option<serde_yaml::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvVar {
    name: String,
    value: String,
}

/// Parses a duration the way Kubernetes writes one: "12h", "1h30m", "90s",
/// "0".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(Duration::ZERO);
    }
    if s.starts_with('-') {
        return Err(format!("negative duration {s:?}"));
    }
    let mut total = Duration::ZERO;
    let mut rest = s;
    if rest.is_empty() {
        return Err("empty duration".into());
    }
    while !rest.is_empty() {
        let n = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(|| format!("missing unit in duration {s:?}"))?;
        let value: f64 = rest[..n]
            .parse()
            .map_err(|_| format!("invalid duration {s:?}"))?;
        rest = &rest[n..];
        let u = rest
            .find(|c: char| c.is_ascii_digit() || c == '.')
            .unwrap_or(rest.len());
        let unit = &rest[..u];
        rest = &rest[u..];
        let secs = match unit {
            "ns" => value / 1e9,
            "us" | "µs" => value / 1e6,
            "ms" => value / 1e3,
            "s" => value,
            "m" => value * 60.0,
            "h" => value * 3600.0,
            _ => return Err(format!("unknown unit {unit:?} in duration {s:?}")),
        };
        total += Duration::from_secs_f64(secs);
    }
    Ok(total)
}

/// Loads the plugins `config_path` names, resolving each under `bin_dir`.
/// An unusable or duplicate provider is skipped with a warning.
pub fn load_config(config_path: &Path, bin_dir: &Path) -> Result<Vec<Plugin>, String> {
    let raw = std::fs::read_to_string(config_path)
        .map_err(|e| format!("while reading credential provider config: {e}"))?;
    parse_config(&raw, config_path, bin_dir)
}

pub(super) fn parse_config(
    raw: &str,
    config_path: &Path,
    bin_dir: &Path,
) -> Result<Vec<Plugin>, String> {
    let cfg: ConfigFile = serde_yaml::from_str(raw).map_err(|e| {
        format!(
            "while parsing credential provider config {}: {e}",
            config_path.display()
        )
    })?;
    if cfg.kind != CONFIG_KIND || cfg.api_version != CONFIG_API_VERSION {
        return Err(format!(
            "credential provider config {} must be {CONFIG_API_VERSION} {CONFIG_KIND}, got {:?} {:?}",
            config_path.display(),
            cfg.api_version,
            cfg.kind
        ));
    }
    let mut out: Vec<Plugin> = Vec::new();
    for p in cfg.providers {
        match new_plugin(p, bin_dir) {
            Ok(plugin) => {
                if out.iter().any(|o| o.name == plugin.name) {
                    tracing::warn!(config = %config_path.display(), provider = %plugin.name, "Skipping duplicate image credential provider");
                    continue;
                }
                out.push(plugin);
            }
            Err(err) => {
                tracing::warn!(config = %config_path.display(), %err, "Skipping unusable image credential provider")
            }
        }
    }
    if out.is_empty() {
        tracing::error!(config = %config_path.display(), "No usable image credential providers; image pulls will be anonymous");
    }
    Ok(out)
}

fn new_plugin(p: ProviderConfig, bin_dir: &Path) -> Result<Plugin, String> {
    if p.name.is_empty() {
        return Err("provider name is required".into());
    }
    if p.name.contains('/') {
        return Err(format!("provider name {:?} must not contain \"/\"", p.name));
    }
    if p.match_images.is_empty() {
        return Err(format!("provider {:?}: matchImages is required", p.name));
    }
    for m in &p.match_images {
        matches_image(m, "example.registry.io/image")
            .map_err(|e| format!("provider {:?}: invalid matchImages entry: {e}", p.name))?;
    }
    let dur = p
        .default_cache_duration
        .as_deref()
        .ok_or_else(|| format!("provider {:?}: defaultCacheDuration is required", p.name))?;
    let default_cache_duration = parse_duration(dur)
        .map_err(|e| format!("provider {:?}: defaultCacheDuration: {e}", p.name))?;
    if p.api_version != SUPPORTED_API_VERSION {
        return Err(format!(
            "provider {:?}: apiVersion {:?} is not supported (want {SUPPORTED_API_VERSION:?})",
            p.name, p.api_version
        ));
    }
    if p.token_attributes.is_some() {
        return Err(format!(
            "provider {:?}: tokenAttributes is not supported",
            p.name
        ));
    }
    Ok(Plugin::new(
        p.name.clone(),
        PathBuf::from(bin_dir).join(&p.name),
        p.args,
        p.env.into_iter().map(|e| (e.name, e.value)).collect(),
        p.match_images,
        default_cache_duration,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = r#"
kind: CredentialProviderConfig
apiVersion: kubelet.config.k8s.io/v1
providers:
  - name: cloud-auth
    matchImages: ["*.gcr.io", "*.pkg.dev"]
    defaultCacheDuration: "12h"
    apiVersion: credentialprovider.kubelet.k8s.io/v1
    args: ["get-credentials"]
    env:
      - name: MODE
        value: node
  - name: cloud-auth
    matchImages: ["*.gcr.io"]
    defaultCacheDuration: "1h"
    apiVersion: credentialprovider.kubelet.k8s.io/v1
  - name: old
    matchImages: ["quay.io"]
    defaultCacheDuration: "1h"
    apiVersion: credentialprovider.kubelet.k8s.io/v1alpha1
"#;

    #[test]
    fn loads_usable_providers_and_skips_the_rest() {
        let plugins = parse_config(GOOD, Path::new("cfg.yaml"), Path::new("/bin")).unwrap();
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].name, "cloud-auth");
        assert_eq!(plugins[0].path, PathBuf::from("/bin/cloud-auth"));
    }

    #[test]
    fn refuses_other_kinds_and_unknown_fields() {
        let wrong = "kind: KubeletConfiguration\napiVersion: kubelet.config.k8s.io/v1\n";
        assert!(parse_config(wrong, Path::new("c"), Path::new("/b")).is_err());
        let unknown =
            "kind: CredentialProviderConfig\napiVersion: kubelet.config.k8s.io/v1\nbogus: 1\n";
        assert!(parse_config(unknown, Path::new("c"), Path::new("/b")).is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(
            parse_duration("12h").unwrap(),
            Duration::from_secs(12 * 3600)
        );
        assert_eq!(parse_duration("1h30m").unwrap(), Duration::from_secs(5400));
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert!(parse_duration("-1s").is_err());
        assert!(parse_duration("5").is_err());
        assert!(parse_duration("3d").is_err());
    }
}
