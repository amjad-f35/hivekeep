//! Checks on the command line that clap cannot express.

/// The sorted names of the flags whose values are empty.
pub fn missing_flags<'a>(flags: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<String> {
    let mut missing: Vec<String> = flags
        .into_iter()
        .filter(|(_, value)| value.is_empty())
        .map(|(name, _)| name.to_string())
        .collect();
    missing.sort();
    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_flags_cases() {
        let none = missing_flags([
            ("--issuer", "https://idp.hivekeep-system.svc"),
            ("--actor-id-jwt-pool", "/run/actor-id-jwt-pool/pool.json"),
        ]);
        assert!(none.is_empty(), "{none:?}");
        assert_eq!(
            missing_flags([
                ("--actor-id-jwt-pool", ""),
                ("--issuer", ""),
                ("--server-cred-bundle", "/run/bundle.pem"),
            ]),
            vec!["--actor-id-jwt-pool", "--issuer"]
        );
    }
}
