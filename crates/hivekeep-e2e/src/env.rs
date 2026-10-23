//! Environment lookups shared by the suites: required variables, and where
//! the install under test lives.

use std::collections::BTreeMap;

use crate::install;

/// Names the namespace the control plane under test was installed into.
pub const SYSTEM_NAMESPACE_ENV: &str = "HK_E2E_SYSTEM_NAMESPACE";

/// The prefix the install under test puts on Hivekeep's resource names. A
/// packaging layer that renames resources prefixes them, and the harness
/// addresses several of those resources by name.
pub const RESOURCE_PREFIX_ENV: &str = "HK_E2E_RESOURCE_PREFIX";

/// A required variable is unset or empty.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("environment variable {0} is not set")]
pub struct MissingEnv(pub String);

/// Returns the values of `keys`, or the first one that is unset or empty.
pub fn check_env(keys: &[&str]) -> Result<BTreeMap<String, String>, MissingEnv> {
    check_env_with(keys, |k| std::env::var(k).ok())
}

fn check_env_with(
    keys: &[&str],
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<BTreeMap<String, String>, MissingEnv> {
    let mut out = BTreeMap::new();
    for key in keys {
        match lookup(key) {
            Some(v) if !v.is_empty() => {
                out.insert((*key).to_string(), v);
            }
            _ => return Err(MissingEnv((*key).to_string())),
        }
    }
    Ok(out)
}

/// The namespace the control plane under test runs in, falling back to the
/// canonical install namespace.
pub fn system_namespace() -> String {
    match std::env::var(SYSTEM_NAMESPACE_ENV) {
        Ok(ns) if !ns.is_empty() => ns,
        _ => install::SYSTEM_NAMESPACE.to_string(),
    }
}

/// `name` as the install under test renders it.
pub fn resource_name(name: &str) -> String {
    format!(
        "{}{name}",
        std::env::var(RESOURCE_PREFIX_ENV).unwrap_or_default()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_env_cases() {
        let vars: BTreeMap<&str, &str> = [
            ("TEST_ENV_FOO", "value_foo"),
            ("TEST_ENV_BAR", "value_bar"),
            ("TEST_ENV_EMPTY", ""),
        ]
        .into();
        let lookup = |k: &str| vars.get(k).map(|v| v.to_string());
        let ok = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };

        // all env vars present
        assert_eq!(
            check_env_with(&["TEST_ENV_FOO", "TEST_ENV_BAR"], lookup),
            Ok(ok(&[
                ("TEST_ENV_FOO", "value_foo"),
                ("TEST_ENV_BAR", "value_bar")
            ]))
        );
        // missing env var
        assert_eq!(
            check_env_with(&["TEST_ENV_FOO", "TEST_ENV_NONEXISTENT"], lookup),
            Err(MissingEnv("TEST_ENV_NONEXISTENT".into()))
        );
        // env var is empty string
        assert_eq!(
            check_env_with(&["TEST_ENV_FOO", "TEST_ENV_EMPTY"], lookup),
            Err(MissingEnv("TEST_ENV_EMPTY".into()))
        );
        // no keys requested
        assert_eq!(check_env_with(&[], lookup), Ok(BTreeMap::new()));
    }
}
