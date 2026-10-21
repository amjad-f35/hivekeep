//! The version this install reports.

use super::Env;

impl Env {
    /// The build version stamped into the components.
    ///
    /// With `--image-repo` nothing is built, so the checkout's version would
    /// describe the wrong thing: the image tag is the version then.
    /// `HK_VERSION` wins over both.
    pub fn hivekeep_version(&self) -> String {
        if self.cfg.images.is_prebuilt() {
            if let Ok(v) = std::env::var(crate::builder::VERSION_ENV) {
                if !v.is_empty() {
                    return v;
                }
            }
            // The tag may carry its digest (v1@sha256:...).
            return self
                .cfg
                .images
                .tag
                .split('@')
                .next()
                .unwrap_or("")
                .to_string();
        }
        crate::builder::build_version(&self.cfg.root)
    }
}

#[cfg(test)]
// Tests build a default Config and adjust the one field under test.
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::images::Source;

    #[test]
    fn hivekeep_version_prebuilt() {
        let _g = crate::builder::VERSION_ENV_LOCK.lock();
        std::env::remove_var(crate::builder::VERSION_ENV);
        let cases = [
            ("tag is the version", Source { repo: "example.com/hivekeep".into(), tag: "v0.0.0-503-4b3423c0".into() }, Some("v0.0.0-503-4b3423c0")),
            (
                "a tag carrying its digest is the version without it",
                Source {
                    repo: "example.com/hivekeep".into(),
                    tag: "v0.0.0@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
                },
                Some("v0.0.0"),
            ),
            // Built from source, the builder's version applies; it is not a
            // checkout here, so it is whatever the builder falls back to.
            ("built from source falls back to the builder", Source::default(), None),
        ];
        let dir = tempfile::tempdir().unwrap();
        for (name, source, want) in cases {
            let mut cfg = Config::default();
            cfg.root = dir.path().to_path_buf();
            cfg.images = source;
            let e = Env::local(cfg);
            let got = e.hivekeep_version();
            match want {
                Some(w) => assert_eq!(got, w, "{name}"),
                None => assert_eq!(got, crate::builder::build_version(dir.path()), "{name}"),
            }
        }
    }
}
