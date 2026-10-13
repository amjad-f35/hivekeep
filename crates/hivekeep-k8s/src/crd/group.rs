//! The `hivekeep.io/v1` API group.

/// API group of the Hivekeep custom resources.
pub const GROUP: &str = "hivekeep.io";
/// The only served version.
pub const VERSION: &str = "v1";
/// `group/version`.
pub const API_VERSION: &str = "hivekeep.io/v1";

/// Whether `key` (a label or annotation key) is in the reserved
/// `hivekeep.io` domain or one of its subdomains.
pub fn is_reserved_key(key: &str) -> bool {
    key.starts_with("hivekeep.io/") || key.contains(".hivekeep.io/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_keys() {
        assert!(is_reserved_key("hivekeep.io/worker-pool"));
        assert!(is_reserved_key("sandboxconfig.hivekeep.io/x"));
        assert!(!is_reserved_key("example.com/hivekeep.io"));
        assert!(!is_reserved_key("hivekeep.iox/a"));
    }
}
