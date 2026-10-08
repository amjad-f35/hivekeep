//! Helpers shared by the validation tests.
#![allow(dead_code)]

use hivekeep_core::field::{ErrorKind, ErrorList};

/// An expected error: kind, path and originating rule.
pub type Want = (ErrorKind, &'static str, Option<&'static str>);

pub const PINNED: &str =
    "example.com/app:v1@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
pub const UID: &str = "12345678-1234-1234-1234-123456789abc";

pub fn required(path: &'static str) -> Want {
    (ErrorKind::Required, path, None)
}
pub fn forbidden(path: &'static str) -> Want {
    (ErrorKind::Forbidden, path, None)
}
pub fn invalid(path: &'static str) -> Want {
    (ErrorKind::Invalid, path, None)
}
pub fn invalid_by(path: &'static str, origin: &'static str) -> Want {
    (ErrorKind::Invalid, path, Some(origin))
}
pub fn dns_label(path: &'static str) -> Want {
    invalid_by(path, "format=dns-label")
}
pub fn duplicate(path: &'static str) -> Want {
    (ErrorKind::Duplicate, path, None)
}
pub fn too_many(path: &'static str) -> Want {
    (ErrorKind::TooMany, path, Some("maxItems"))
}
pub fn too_long(path: &'static str) -> Want {
    (ErrorKind::TooLong, path, Some("maxLength"))
}
pub fn not_supported(path: &'static str) -> Want {
    (ErrorKind::NotSupported, path, None)
}

/// Asserts that `got` holds exactly the `want` errors, in any order.
#[track_caller]
pub fn assert_errs(case: &str, got: ErrorList, want: &[Want]) {
    let mut g: Vec<(ErrorKind, String, Option<&str>)> = got
        .iter()
        .map(|e| (e.kind, e.path.as_str().to_string(), e.origin))
        .collect();
    let mut w: Vec<(ErrorKind, String, Option<&str>)> = want
        .iter()
        .map(|(k, p, o)| (*k, p.to_string(), *o))
        .collect();
    g.sort();
    w.sort();
    assert_eq!(g, w, "case {case:?}: got {}", got.message());
}

/// Builds a value from a default and a list of in-place edits.
pub fn with<T>(mut v: T, f: impl FnOnce(&mut T)) -> T {
    f(&mut v);
    v
}
