//! Configuration errors that name the channel each offending value came
//! from, so the reader knows which one to edit.

use super::resolve::Value;
use super::setting::Setting;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// No channel supplied a setting that has no default. Every channel is
    /// named: the reader may know only one of them.
    #[error("at least one of {} must be set, got none", setting.channels())]
    Required { setting: Box<Setting> },

    /// A value not valid for its setting.
    #[error("{} must be {want}, got {:?} (from {})", value.setting.key, value.display(), value.from.describe(&value.setting))]
    Invalid { value: Box<Value>, want: String },

    /// Two values that cannot hold together.
    #[error(
        "{}={:?} (from {}) conflicts with {}={:?} (from {}): {why}",
        a.setting.key, a.display(), a.from.describe(&a.setting),
        b.setting.key, b.display(), b.from.describe(&b.setting)
    )]
    Conflict {
        a: Box<Value>,
        b: Box<Value>,
        why: String,
    },

    #[error("{0}")]
    Other(String),
}

impl ConfigError {
    pub fn invalid(value: &Value, want: impl Into<String>) -> Self {
        Self::Invalid {
            value: Box::new(value.clone()),
            want: want.into(),
        }
    }

    pub fn conflict(a: &Value, b: &Value, why: impl Into<String>) -> Self {
        Self::Conflict {
            a: Box::new(a.clone()),
            b: Box::new(b.clone()),
            why: why.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::registry::{lookup, registry};
    use crate::config::resolve::{resolve, Origin, ResolveOptions};
    use crate::config::testutil::{env_map, load_options, Options, TestEnv};
    use crate::config::Config;

    #[test]
    fn required_error_names_every_channel() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let got = ConfigError::Required {
            setting: Box::new(lookup("fixture.nested.deep.key").unwrap()),
        }
        .to_string();
        assert_eq!(
            got,
            "at least one of FIXTURE_NESTED_DEEP_KEY, config.fixture.nested.deep.key or --fixture-nested-deep-key must be set, got none"
        );
    }

    #[test]
    fn required_error_covers_every_setting() {
        for s in registry() {
            let got = ConfigError::Required {
                setting: Box::new(s.clone()),
            }
            .to_string();
            let mut want = vec![s.env.clone(), format!("config.{}", s.key)];
            if !s.flag.is_empty() {
                want.push(format!("--{}", s.flag));
            }
            for w in want {
                assert!(got.contains(&w), "{}: {got} lacks {w}", s.key);
            }
        }
    }

    #[test]
    fn invalid_error_names_the_supplying_channel() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let s = lookup("fixture.mode").unwrap();
        for (origin, want_in) in [
            (Origin::Env, "FIXTURE_MODE"),
            (Origin::File, "config.fixture.mode"),
            (Origin::Flag, "--fixture-mode"),
        ] {
            let v = Value {
                setting: s.clone(),
                raw: "gamma".into(),
                from: origin,
                supplied: false,
            };
            let got = ConfigError::invalid(&v, "alpha or beta").to_string();
            for w in ["fixture.mode", "alpha or beta", "gamma", want_in] {
                assert!(got.contains(w), "{got} lacks {w}");
            }
        }
    }

    #[test]
    fn conflict_error_names_both_sides() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let a = Value {
            setting: lookup("fixture.enabled").unwrap(),
            raw: "true".into(),
            from: Origin::File,
            supplied: false,
        };
        let b = Value {
            setting: lookup("fixture.mode").unwrap(),
            raw: "beta".into(),
            from: Origin::Env,
            supplied: false,
        };
        let got = ConfigError::conflict(&a, &b, "the feature requires mode alpha").to_string();
        for w in [
            "fixture.enabled",
            "true",
            "config.fixture.enabled",
            "fixture.mode",
            "beta",
            "FIXTURE_MODE",
            "the feature requires mode alpha",
        ] {
            assert!(got.contains(w), "{got} lacks {w}");
        }
    }

    #[test]
    fn error_messages_withhold_secret_values() {
        let secret = Setting::new("scratch.secret", "SCRATCH_SECRET", "scratch-secret")
            .secret()
            .usage("a credential");
        let v = Value {
            setting: secret,
            raw: "postgres://u:hunter2@h/db".into(),
            from: Origin::Env,
            supplied: true,
        };
        let other = Value {
            setting: Setting::new("scratch.other", "SCRATCH_OTHER", "scratch-other"),
            raw: "x".into(),
            from: Origin::Flag,
            supplied: false,
        };
        for (name, got, want_in) in [
            (
                "invalid",
                ConfigError::invalid(&v, "a DSN").to_string(),
                vec!["scratch.secret", "SCRATCH_SECRET", "a DSN"],
            ),
            (
                "conflict",
                ConfigError::conflict(&v, &other, "they disagree").to_string(),
                vec!["scratch.secret", "SCRATCH_SECRET", "they disagree"],
            ),
            ("display", v.display(), vec![]),
        ] {
            assert!(!got.contains("hunter2"), "{name}: {got}");
            for w in want_in {
                assert!(got.contains(w), "{name}: {got} lacks {w}");
            }
        }
    }

    #[test]
    fn resolved_require() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        for (name, env, key, want_err) in [
            (
                "supplied",
                vec![("FIXTURE_NAME", "n")],
                "fixture.name",
                false,
            ),
            ("absent and no default", vec![], "fixture.name", true),
            ("absent but defaulted", vec![], "fixture.mode", false),
        ] {
            let env = env_map(&env);
            let r = resolve(
                None,
                ResolveOptions {
                    env: Some(&env),
                    file: None,
                },
            )
            .unwrap();
            match (r.require(key), want_err) {
                (Ok(()), false) => {}
                (Err(ConfigError::Required { .. }), true) => {}
                (got, _) => panic!("{name}: {got:?}"),
            }
        }
    }

    #[test]
    fn validation_errors_name_the_supplying_channel() {
        let mut t = TestEnv::new();
        let cases: Vec<(&str, (&str, &str), Vec<&str>)> = vec![
            (
                "extproc service is malformed",
                ("HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE", "nope"),
                vec![
                    "gate.egress.additionalExtprocService",
                    "<namespace>/<service>:<port>",
                    "\"nope\"",
                    "HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
                ],
            ),
            (
                "extproc port is out of range",
                ("HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE", "ns/svc:99999"),
                vec![
                    "gate.egress.additionalExtprocService",
                    "1-65535",
                    "HK_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
                ],
            ),
            (
                "image tag without a repository",
                ("HK_IMAGE_TAG", "v1"),
                vec![
                    "images.tag",
                    "HK_IMAGE_TAG",
                    "images.repo",
                    "the tag names nothing",
                ],
            ),
            (
                "image repository without a tag",
                ("HK_IMAGE_REPO", "example.com/hivekeep"),
                vec!["images.repo", "HK_IMAGE_REPO", "images.tag", "needs a tag"],
            ),
        ];
        for (name, (k, v), want_in) in cases {
            t.load_env();
            t.set_env(k, v);
            let err = load_options(&Options::default())
                .expect_err(name)
                .to_string();
            for w in want_in {
                assert!(err.contains(w), "{name}: {err} lacks {w}");
            }
            assert!(!err.starts_with("--"), "{name}: {err}");
        }
    }

    #[test]
    fn credential_provider_error_names_the_file_channel() {
        let err = Config::default()
            .credential_provider()
            .unwrap_err()
            .to_string();
        for w in [
            "--credential-provider",
            "HK_CREDENTIAL_PROVIDER",
            "config.gate.egress.credentialProvider",
        ] {
            assert!(err.contains(w), "{err} lacks {w}");
        }
    }
}
