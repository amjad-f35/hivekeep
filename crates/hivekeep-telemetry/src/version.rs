//! Build identity for Hivekeep binaries.
//!
//! The commit and build date are taken from `HK_BUILD_COMMIT` and
//! `HK_BUILD_DATE` at compile time, so a release pipeline sets them in the
//! environment of `cargo build`. A plain local build reports `unknown` for
//! both and the crate version from `Cargo.toml`.

/// The crate version the binary was built from.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The source revision, or `unknown` when the build did not record one.
pub const COMMIT: &str = match option_env!("HK_BUILD_COMMIT") {
    Some(c) => c,
    None => "unknown",
};

/// The build timestamp, or `unknown` when the build did not record one.
pub const BUILD_DATE: &str = match option_env!("HK_BUILD_DATE") {
    Some(d) => d,
    None => "unknown",
};

/// A single-line version summary that reads well after a `<binary> version`
/// prefix: `0.1.0 commit=abc built=2026-01-01 linux/x86_64`.
pub fn string() -> String {
    format!(
        "{VERSION} commit={COMMIT} built={BUILD_DATE} {}/{}",
        std::env::consts::OS,
        std::env::consts::ARCH
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_names_every_part() {
        let s = string();
        assert!(s.starts_with(VERSION));
        assert!(s.contains(&format!("commit={COMMIT}")));
        assert!(s.contains(&format!("built={BUILD_DATE}")));
        assert!(s.ends_with(&format!(
            "{}/{}",
            std::env::consts::OS,
            std::env::consts::ARCH
        )));
    }
}
