//! Expanding a demo template with the environment's values.

use crate::steps::{Env, Result, StepError};
use std::collections::BTreeMap;

/// Substituted into every demo template with the snapshot bucket.
pub const BUCKET_NAME_PLACEHOLDER: &str = "BUCKET_NAME";

/// The optional external-volume hooks in the counter templates. Every path
/// except `deploy demo counter --with-external-volume` drops them, which
/// removes the lines entirely.
pub const EXTERNAL_VOLUME_PLACEHOLDERS: [&str; 3] = [
    "VALIDATE_EXISTING_FILE_PATH_ARG",
    "EXTERNAL_VOLUME_MOUNTS",
    "EXTERNAL_VOLUMES",
];

/// Expands the template at `rel_path` (relative to the manifests directory)
/// with the configured bucket name and `extra`, dropping lines that name a
/// placeholder in `drop`.
pub fn render(
    env: &Env,
    rel_path: &str,
    extra: &BTreeMap<String, String>,
    drop: &[&str],
) -> Result<Vec<u8>> {
    let mut values = BTreeMap::new();
    values.insert(
        BUCKET_NAME_PLACEHOLDER.to_string(),
        env.cfg.bucket_name.clone(),
    );
    values.extend(extra.iter().map(|(k, v)| (k.clone(), v.clone())));
    let path = env.cfg.manifests_path(&[rel_path]);
    crate::render::template(&path, &values, drop).map_err(|e| StepError::msg(e.to_string()))
}
