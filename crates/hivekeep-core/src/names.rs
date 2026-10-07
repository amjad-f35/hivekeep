//! Name and identifier formats shared by every Hivekeep resource.
//!
//! Each `check_*` function returns the list of reasons a value is malformed
//! (empty when it is fine), so callers can attach them to a field path. The
//! `is_*` predicates are convenience wrappers for code that only needs a
//! yes/no answer.

use std::sync::LazyLock;

use regex::Regex;

/// Maximum length of a DNS-1123 label.
pub const DNS_LABEL_MAX_LEN: usize = 63;
/// Maximum length of a DNS-1123 subdomain.
pub const DNS_SUBDOMAIN_MAX_LEN: usize = 253;
/// Maximum length of a label value and of the name part of a label key.
pub const LABEL_VALUE_MAX_LEN: usize = 63;

/// Regular expression (without anchors) of a valid resource name: a
/// DNS-1123 label.
pub const RESOURCE_NAME_PATTERN: &str = "[a-z0-9]([-a-z0-9]*[a-z0-9])?";

const LABEL_FMT: &str = "[a-z0-9]([-a-z0-9]*[a-z0-9])?";
const QUALIFIED_NAME_FMT: &str = "([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]";

static LABEL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(&format!("^{LABEL_FMT}$")).unwrap());
static SUBDOMAIN_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!(r"^{LABEL_FMT}(\.{LABEL_FMT})*$")).unwrap());
static QUALIFIED_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!("^{QUALIFIED_NAME_FMT}$")).unwrap());

fn max_len_msg(max: usize) -> String {
    format!("must be no more than {max} bytes")
}

/// Checks a DNS-1123 label: lowercase alphanumerics and '-', starting and
/// ending with an alphanumeric, at most 63 bytes.
pub fn check_dns_label(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS_LABEL_MAX_LEN {
        errs.push(max_len_msg(DNS_LABEL_MAX_LEN));
    }
    if !LABEL_RE.is_match(value) {
        if SUBDOMAIN_RE.is_match(value) {
            errs.push("must not contain dots".to_string());
        } else {
            errs.push(
                "must consist of lower case alphanumeric characters or '-', and must start and end with an \
                 alphanumeric character (e.g. 'my-name' or '123-abc')"
                    .to_string(),
            );
        }
    }
    errs
}

/// Checks a DNS-1123 subdomain: dot-separated DNS labels, at most 253 bytes.
pub fn check_dns_subdomain(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > DNS_SUBDOMAIN_MAX_LEN {
        errs.push(max_len_msg(DNS_SUBDOMAIN_MAX_LEN));
    }
    if !SUBDOMAIN_RE.is_match(value) {
        errs.push(
            "must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an \
             alphanumeric character (e.g. 'example.com')"
                .to_string(),
        );
    }
    errs
}

/// Checks a canonical UUID: 36 bytes, lowercase hex in 8-4-4-4-12 groups.
pub fn check_uuid(value: &str) -> Vec<String> {
    let bytes = value.as_bytes();
    let ok = bytes.len() == 36
        && bytes.iter().enumerate().all(|(i, &c)| match i {
            8 | 13 | 18 | 23 => c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(&c),
        });
    if ok {
        Vec::new()
    } else {
        vec!["must be a lowercase UUID in 8-4-4-4-12 format".to_string()]
    }
}

/// Checks a label key: an optional DNS-subdomain prefix and '/', then a name
/// of at most 63 alphanumerics, '-', '_' or '.', starting and ending with an
/// alphanumeric.
pub fn check_label_key(value: &str) -> Vec<String> {
    let (prefix, name) = match value.split_once('/') {
        Some((p, n)) => (Some(p), n),
        None => (None, value),
    };
    let mut errs = Vec::new();
    if let Some(prefix) = prefix {
        if prefix.is_empty() {
            errs.push("prefix part must be non-empty".to_string());
        } else {
            errs.extend(
                check_dns_subdomain(prefix)
                    .into_iter()
                    .map(|m| format!("prefix part {m}")),
            );
        }
    }
    if name.contains('/') {
        errs.push(
            "must consist of an optional DNS subdomain prefix and '/', followed by a name"
                .to_string(),
        );
        return errs;
    }
    if name.is_empty() {
        errs.push("name part must be non-empty".to_string());
    } else {
        if name.len() > LABEL_VALUE_MAX_LEN {
            errs.push(format!("name part {}", max_len_msg(LABEL_VALUE_MAX_LEN)));
        }
        if !QUALIFIED_NAME_RE.is_match(name) {
            errs.push(
                "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end \
                 with an alphanumeric character"
                    .to_string(),
            );
        }
    }
    errs
}

/// Checks a label value: empty, or at most 63 alphanumerics, '-', '_' or
/// '.', starting and ending with an alphanumeric.
pub fn check_label_value(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > LABEL_VALUE_MAX_LEN {
        errs.push(max_len_msg(LABEL_VALUE_MAX_LEN));
    }
    if !value.is_empty() && !QUALIFIED_NAME_RE.is_match(value) {
        errs.push(
            "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', \
             and must start and end with an alphanumeric character"
                .to_string(),
        );
    }
    errs
}

/// Reports whether `name` is a valid resource name (a DNS-1123 label).
/// Empty is not valid.
pub fn is_valid_resource_name(name: &str) -> bool {
    check_dns_label(name).is_empty()
}

/// Reports whether `value` is a valid DNS-1123 subdomain.
pub fn is_dns_subdomain(value: &str) -> bool {
    check_dns_subdomain(value).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dns_label() {
        for ok in ["a", "abc", "a-b", "0", "a1", &"a".repeat(63)] {
            assert!(check_dns_label(ok).is_empty(), "{ok}");
        }
        for bad in ["", "-a", "a-", "A", "a_b", "a.b", "a b", &"a".repeat(64)] {
            assert!(!check_dns_label(bad).is_empty(), "{bad}");
        }
        assert_eq!(check_dns_label("a.b"), vec!["must not contain dots"]);
    }

    #[test]
    fn dns_subdomain() {
        assert!(check_dns_subdomain("a.b-c.d").is_empty());
        assert!(!check_dns_subdomain("a..b").is_empty());
        assert!(!check_dns_subdomain(".a").is_empty());
        assert!(!check_dns_subdomain(&"a".repeat(254)).is_empty());
    }

    #[test]
    fn uuid() {
        assert!(check_uuid("123e4567-e89b-12d3-a456-426614174000").is_empty());
        assert!(!check_uuid("123E4567-E89B-12D3-A456-426614174000").is_empty());
        assert!(!check_uuid("123e4567e89b12d3a456426614174000").is_empty());
        assert!(!check_uuid("123e4567-e89b-12d3-a456-42661417400g").is_empty());
    }

    #[test]
    fn label_key_and_value() {
        for ok in ["tier", "example.com/tier", "a_b.c-d", "A"] {
            assert!(check_label_key(ok).is_empty(), "{ok}");
        }
        for bad in ["", "/tier", "bad key!", "a/b/c", "Example.com/x", "-a"] {
            assert!(!check_label_key(bad).is_empty(), "{bad}");
        }
        for ok in ["", "1", "Aa_b.c-d"] {
            assert!(check_label_value(ok).is_empty(), "{ok}");
        }
        for bad in ["not valid!", "-a", &"a".repeat(64)] {
            assert!(!check_label_value(bad).is_empty(), "{bad}");
        }
    }
}
