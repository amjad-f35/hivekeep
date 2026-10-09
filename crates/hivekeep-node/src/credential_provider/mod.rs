//! Image pull credentials from the kubelet's credential provider plugins.
//!
//! The kubelet already knows how to authenticate to its node's registries
//! through exec plugins the node ships. Pointing the keeper at the same
//! config and binaries lets it pull with no cloud SDK compiled in, on any
//! node that configures a provider.

mod config;
mod matcher;
mod plugin;

use std::collections::HashSet;
use std::path::Path;

pub use config::{load_config, parse_duration, SUPPORTED_API_VERSION};
pub use matcher::{
    glob, matches_image, matching_auth_keys, registry_of, repository_of, sort_auth_keys,
};
pub use plugin::{AuthConfig, Plugin};

use crate::clock::SharedClock;
use crate::context::Ctx;

/// Resolves registry credentials for an image from the configured plugins.
pub struct Keychain {
    plugins: Vec<Plugin>,
}

impl Keychain {
    /// The keychain of the providers `config_path` names, under `bin_dir`.
    pub fn new(config_path: &Path, bin_dir: &Path) -> Result<Self, String> {
        let plugins = load_config(config_path, bin_dir)?;
        tracing::info!(
            config = %config_path.display(), bin_dir = %bin_dir.display(),
            providers = ?plugins.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            "Loaded image credential providers"
        );
        Ok(Keychain { plugins })
    }

    /// Like [`Keychain::new`], timing cache expiry with `clock`.
    pub fn with_clock(
        config_path: &Path,
        bin_dir: &Path,
        clock: SharedClock,
    ) -> Result<Self, String> {
        let mut k = Self::new(config_path, bin_dir)?;
        for p in &mut k.plugins {
            p.set_clock(clock.clone());
        }
        Ok(k)
    }

    /// The configured plugins, in config order.
    pub fn plugins(&self) -> &[Plugin] {
        &self.plugins
    }

    /// The credential to pull `image` with: the first candidate, or `None`
    /// to pull anonymously.
    pub async fn resolve(&self, ctx: &Ctx, image: &str) -> Result<Option<AuthConfig>, String> {
        Ok(self.candidates(ctx, image).await?.into_iter().next())
    }

    /// Every candidate credential for `image`, most specific first, without
    /// duplicates. A plugin that fails is skipped. No candidates means pull
    /// anonymously. Plugins are asked about the image's repository, without
    /// tag or digest. Fails only when `ctx` ends or `image` is malformed.
    pub async fn candidates(&self, ctx: &Ctx, image: &str) -> Result<Vec<AuthConfig>, String> {
        let image = repository_of(image);
        let mut by_key: Vec<(String, AuthConfig)> = Vec::new();
        for p in &self.plugins {
            if !p.claims(image)? {
                continue;
            }
            let res = p.provide(ctx, image).await;
            if let Some(err) = ctx.err() {
                return Err(err.to_string());
            }
            let (auth, cached) = match res {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(provider = %p.name, image, %err, "Image credential provider failed; skipping it");
                    continue;
                }
            };
            let keys = match matching_auth_keys(auth.keys(), image) {
                Ok(keys) => keys,
                Err(err) => {
                    tracing::warn!(provider = %p.name, image, %err, "Image credential provider returned an invalid auth key; skipping it");
                    continue;
                }
            };
            if keys.is_empty() {
                tracing::debug!(provider = %p.name, image, "Image credential provider returned no credentials");
                continue;
            }
            tracing::info!(provider = %p.name, image, cached, "Resolved image credentials from credential provider");
            for k in keys {
                let a = auth[&k].clone();
                by_key.push((k, a));
            }
        }
        // Most specific key first; stable within a key, in plugin order.
        let mut keys: Vec<String> = by_key.iter().map(|(k, _)| k.clone()).collect();
        sort_auth_keys(&mut keys);
        keys.dedup();
        by_key.sort_by_key(|(k, _)| keys.iter().position(|x| x == k));
        let mut seen = HashSet::new();
        Ok(by_key
            .into_iter()
            .map(|(_, a)| a)
            .filter(|a| seen.insert(a.clone()))
            .collect())
    }
}

#[cfg(test)]
mod behavior_tests;
