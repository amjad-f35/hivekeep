//! Environment lookup behind a trait, so configuration resolution can be
//! exercised with a fixed set of variables instead of the process
//! environment.

use std::collections::HashMap;

/// A source of environment variables.
pub trait Env {
    /// The value of `key`, or `None` when it is not set.
    fn lookup(&self, key: &str) -> Option<String>;

    /// The value of `key`, or the empty string when it is not set.
    fn get(&self, key: &str) -> String {
        self.lookup(key).unwrap_or_default()
    }

    /// The value of `key` with surrounding whitespace removed.
    fn trimmed(&self, key: &str) -> String {
        self.get(key).trim().to_string()
    }
}

/// The process environment.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessEnv;

impl Env for ProcessEnv {
    fn lookup(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }
}

/// A fixed set of variables, for tests and for callers that resolve
/// configuration from somewhere other than the process environment.
#[derive(Debug, Clone, Default)]
pub struct MapEnv(pub HashMap<String, String>);

impl MapEnv {
    /// An environment holding exactly `pairs`.
    pub fn new<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        MapEnv(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    /// Sets `key` to `value`.
    pub fn set(&mut self, key: &str, value: &str) {
        self.0.insert(key.to_string(), value.to_string());
    }
}

impl Env for MapEnv {
    fn lookup(&self, key: &str) -> Option<String> {
        self.0.get(key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_env_distinguishes_unset_from_empty() {
        let env = MapEnv::new([("A", ""), ("B", "  x ")]);
        assert_eq!(env.lookup("A"), Some(String::new()));
        assert_eq!(env.lookup("C"), None);
        assert_eq!(env.trimmed("B"), "x");
        assert_eq!(env.get("C"), "");
    }
}
