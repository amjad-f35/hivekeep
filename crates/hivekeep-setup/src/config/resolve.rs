//! Layering the channels into one resolved value per setting, keeping the
//! channel each came from so errors and reports can point at it.

use super::errors::ConfigError;
use super::file::File;
use super::registry::all;
use super::setting::{Parsed, Setting};
use std::collections::BTreeMap;
use std::time::Duration;

/// The channel a resolved value came from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Origin {
    /// The declared default; no channel supplied it.
    #[default]
    Default,
    Env,
    File,
    Flag,
    /// A value a Kind install derived for itself because no channel supplied
    /// one. Overridden through any of the three channels.
    KindProfile,
}

impl Origin {
    /// Names the origin the way the operator refers to it, so a message
    /// points at the thing to edit.
    pub fn describe(self, s: &Setting) -> String {
        match self {
            Self::Env => s.env.clone(),
            Self::File => format!("config.{}", s.key),
            Self::Flag => format!("--{}", s.flag),
            Self::KindProfile => "the --kind profile".into(),
            Self::Default => "default".into(),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::File => "file",
            Self::Flag => "flag",
            Self::KindProfile => "kind",
            Self::Default => "default",
        }
    }
}

impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One setting's resolved value and the channel that supplied it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Value {
    pub setting: Setting,
    pub raw: String,
    pub from: Origin,
    /// False when `raw` is the declared default: distinguishes an empty value
    /// a channel provided from one nobody set.
    pub supplied: bool,
}

impl Value {
    /// The value as shown to an operator: a supplied secret is `<set>`, so
    /// the reader learns which channel to change but not the credential.
    pub fn display(&self) -> String {
        if self.setting.secret && self.supplied {
            "<set>".into()
        } else {
            self.raw.clone()
        }
    }
}

/// Flags the command line set explicitly, by flag name, as raw strings.
/// Flags left at their defaults are absent: a default is not a supplied value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Flags(pub BTreeMap<String, String>);

impl Flags {
    pub fn changed(&self, flag: &str) -> Option<&str> {
        if flag.is_empty() {
            return None;
        }
        self.0.get(flag).map(String::as_str)
    }

    pub fn set(&mut self, flag: &str, raw: &str) {
        self.0.insert(flag.to_string(), raw.to_string());
    }
}

/// Inputs to resolution that are not themselves settings.
#[derive(Clone, Copy, Debug, Default)]
pub struct ResolveOptions<'a> {
    /// The parsed configuration document, if `--config` named one.
    pub file: Option<&'a File>,
    /// The environment to read.
    pub env: Option<&'a BTreeMap<String, String>>,
}

/// Every setting after the channels have been layered.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Resolved {
    values: BTreeMap<String, Value>,
}

/// Layers the channels, lowest to highest: default < environment < file <
/// flag. The file outranks the environment because it is the artifact an
/// operator owns and reviews; ambient state must not silently outrank it.
pub fn resolve(flags: Option<&Flags>, opts: ResolveOptions<'_>) -> Result<Resolved, ConfigError> {
    let mut r = Resolved::default();
    for s in all() {
        let mut v = Value {
            setting: s.clone(),
            raw: s.default.clone(),
            from: Origin::Default,
            supplied: false,
        };
        if let Some(raw) = opts.env.and_then(|e| e.get(&s.env)) {
            v = Value {
                setting: s.clone(),
                raw: raw.clone(),
                from: Origin::Env,
                supplied: true,
            };
        }
        if let Some(raw) = opts.file.and_then(|f| f.get(&s.key)) {
            v = Value {
                setting: s.clone(),
                raw: raw.to_string(),
                from: Origin::File,
                supplied: true,
            };
        }
        if let Some(raw) = flags.and_then(|f| f.changed(&s.flag)) {
            if s.bindable() {
                v = Value {
                    setting: s.clone(),
                    raw: raw.to_string(),
                    from: Origin::Flag,
                    supplied: true,
                };
            }
        }
        // An optional setting nothing supplied has no value to parse; whether
        // that is acceptable is require's question.
        if (v.supplied || !v.raw.is_empty()) && s.parse(&v.raw).is_err() {
            return Err(ConfigError::Invalid {
                value: Box::new(v),
                want: s.kind.description().into(),
            });
        }
        r.values.insert(s.key.clone(), v);
    }
    Ok(r)
}

impl Resolved {
    /// Every setting on its declared default.
    pub fn defaults_only() -> Self {
        resolve(None, ResolveOptions::default()).expect("declared defaults parse")
    }

    /// Overrides a value with one the installer derived. Writing it back here
    /// is what puts it in the report and the record. Unknown keys are
    /// ignored.
    pub fn set(&mut self, key: &str, raw: &str, from: Origin) {
        if let Some(v) = self.values.get_mut(key) {
            v.raw = raw.to_string();
            v.from = from;
            v.supplied = true;
        }
    }

    /// The resolved value; `None` for a key outside the registry, which is a
    /// programming error rather than a user one.
    pub fn value(&self, key: &str) -> Option<&Value> {
        self.values.get(key)
    }

    /// [`Self::value`], defaulting an unknown key to an empty value.
    pub fn value_or_default(&self, key: &str) -> Value {
        self.values.get(key).cloned().unwrap_or_default()
    }

    pub fn string(&self, key: &str) -> String {
        self.values
            .get(key)
            .map(|v| v.raw.clone())
            .unwrap_or_default()
    }

    fn parsed(&self, key: &str) -> Option<Parsed> {
        let v = self.values.get(key)?;
        v.setting.parse(&v.raw).ok()
    }

    pub fn bool(&self, key: &str) -> bool {
        matches!(self.parsed(key), Some(Parsed::Bool(true)))
    }

    pub fn int(&self, key: &str) -> i64 {
        match self.parsed(key) {
            Some(Parsed::Int(n)) => n,
            _ => 0,
        }
    }

    pub fn duration(&self, key: &str) -> Duration {
        match self.parsed(key) {
            Some(Parsed::Duration(d)) => d,
            _ => Duration::ZERO,
        }
    }

    /// Whether a channel set the key, as opposed to it holding its default.
    pub fn supplied(&self, key: &str) -> bool {
        self.values.get(key).is_some_and(|v| v.supplied)
    }

    /// Fails when the setting is empty: no channel supplied it and it has no
    /// default.
    pub fn require(&self, key: &str) -> Result<(), ConfigError> {
        match self.values.get(key) {
            None => Err(ConfigError::Other(format!("unknown setting {key:?}"))),
            Some(v) if v.raw.is_empty() => Err(ConfigError::Required {
                setting: Box::new(v.setting.clone()),
            }),
            Some(_) => Ok(()),
        }
    }

    /// Every setting a channel supplied, sorted by key.
    pub fn origins(&self) -> Vec<Value> {
        self.values
            .values()
            .filter(|v| v.supplied)
            .cloned()
            .collect()
    }

    /// Every value, sorted by key.
    pub fn all_sorted(&self) -> Vec<Value> {
        self.values.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::registry::lookup;
    use crate::config::setting::ValueKind;
    use crate::config::testutil::{env_map, scratch_setting, TestEnv};

    fn flags_with(pairs: &[(&str, &str)]) -> Flags {
        let mut f = Flags::default();
        for (k, v) in pairs {
            f.set(k, v);
        }
        f
    }

    fn file_with(pairs: &[(&str, &str)]) -> File {
        File::from_values(
            "test.yaml",
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn resolve_precedence() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let (key, env, flag) = ("fixture.mode", "FIXTURE_MODE", "fixture-mode");
        type Case<'a> = (
            &'a str,
            Vec<(&'a str, &'a str)>,
            Option<Vec<(&'a str, &'a str)>>,
            Vec<(&'a str, &'a str)>,
            &'a str,
            Origin,
        );
        let cases: Vec<Case> = vec![
            (
                "nothing supplied falls back to the default",
                vec![],
                None,
                vec![],
                "alpha",
                Origin::Default,
            ),
            (
                "environment alone",
                vec![(env, "beta")],
                None,
                vec![],
                "beta",
                Origin::Env,
            ),
            (
                "file alone",
                vec![],
                Some(vec![(key, "beta")]),
                vec![],
                "beta",
                Origin::File,
            ),
            (
                "flag alone",
                vec![],
                None,
                vec![(flag, "beta")],
                "beta",
                Origin::Flag,
            ),
            (
                "file beats environment",
                vec![(env, "beta")],
                Some(vec![(key, "alpha")]),
                vec![],
                "alpha",
                Origin::File,
            ),
            (
                "flag beats file",
                vec![],
                Some(vec![(key, "alpha")]),
                vec![(flag, "beta")],
                "beta",
                Origin::Flag,
            ),
            (
                "flag beats environment and file together",
                vec![(env, "alpha")],
                Some(vec![(key, "alpha")]),
                vec![(flag, "beta")],
                "beta",
                Origin::Flag,
            ),
        ];
        for (name, e, f, fl, want_raw, want_from) in cases {
            let e = env_map(&e);
            let file = f.map(|f| file_with(&f));
            let r = resolve(
                Some(&flags_with(&fl)),
                ResolveOptions {
                    file: file.as_ref(),
                    env: Some(&e),
                },
            )
            .unwrap();
            let v = r.value(key).unwrap();
            assert_eq!((v.raw.as_str(), v.from), (want_raw, want_from), "{name}");
        }
    }

    #[test]
    fn resolve_supplied_distinguishes_empty_from_absent() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        for (name, env, want) in [
            ("absent", vec![], false),
            ("present but empty", vec![("FIXTURE_NAME", "")], true),
            (
                "present with a value",
                vec![("FIXTURE_NAME", "p:r:i")],
                true,
            ),
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
            assert_eq!(r.supplied("fixture.name"), want, "{name}");
        }
    }

    #[test]
    fn resolve_accepts_an_unset_optional_setting() {
        for kind in [
            ValueKind::Int,
            ValueKind::Duration,
            ValueKind::String,
            ValueKind::Bool,
        ] {
            let mut t = TestEnv::new();
            t.use_settings(vec![Setting::new(
                "scratch.optional",
                "SCRATCH_OPTIONAL",
                "scratch-optional",
            )
            .kind(kind)
            .usage("no default")]);
            let r = resolve(None, ResolveOptions::default()).unwrap();
            assert!(!r.supplied("scratch.optional"), "{kind:?}");
        }
        // An optional setting owned by one command does not fail the others.
        let mut t = TestEnv::new();
        t.use_settings(vec![
            Setting::new(
                "scratch.unrelated",
                "SCRATCH_UNRELATED",
                "scratch-unrelated",
            )
            .default_value("fine")
            .usage("other"),
            Setting::new("scratch.workers", "SCRATCH_WORKERS", "scratch-workers")
                .kind(ValueKind::Int)
                .commands(&["deploy benchmarks"])
                .usage("optional"),
        ]);
        assert_eq!(
            resolve(None, ResolveOptions::default())
                .unwrap()
                .string("scratch.unrelated"),
            "fine"
        );
    }

    #[test]
    fn resolve_honors_an_explicitly_empty_string() {
        for decl in [scratch_setting(), scratch_setting().default_value("alpha")] {
            for channel in ["environment", "file", "flag"] {
                let mut t = TestEnv::new();
                t.use_settings(vec![decl.clone()]);
                let env = env_map(&[("SCRATCH_STR", "")]);
                let file = file_with(&[("scratch.str", "")]);
                let flags = flags_with(&[("scratch-str", "")]);
                let r = match channel {
                    "environment" => resolve(
                        None,
                        ResolveOptions {
                            env: Some(&env),
                            file: None,
                        },
                    ),
                    "file" => resolve(
                        None,
                        ResolveOptions {
                            file: Some(&file),
                            env: None,
                        },
                    ),
                    _ => resolve(Some(&flags), ResolveOptions::default()),
                }
                .unwrap();
                assert_eq!(r.string("scratch.str"), "", "{}/{channel}", decl.default);
                assert!(r.supplied("scratch.str"), "{}/{channel}", decl.default);
            }
        }
    }

    #[test]
    fn adding_a_default_does_not_change_what_empty_means() {
        let resolve_with = |def: &str| {
            let mut t = TestEnv::new();
            t.use_settings(vec![scratch_setting().default_value(def)]);
            let env = env_map(&[("SCRATCH_STR", "")]);
            let r = resolve(
                None,
                ResolveOptions {
                    env: Some(&env),
                    file: None,
                },
            )
            .unwrap();
            (r.string("scratch.str"), r.supplied("scratch.str"))
        };
        assert_eq!(resolve_with(""), resolve_with("alpha"));
    }

    #[test]
    fn resolve_typed_accessors() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let env = env_map(&[
            ("FIXTURE_ENABLED", "true"),
            ("FIXTURE_COUNT", "6"),
            ("FIXTURE_TIMEOUT", "90s"),
            ("FIXTURE_NAME", "custom"),
        ]);
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        assert!(r.bool("fixture.enabled"));
        assert_eq!(r.int("fixture.count"), 6);
        assert_eq!(r.duration("fixture.timeout"), Duration::from_secs(90));
        assert_eq!(r.string("fixture.name"), "custom");
    }

    #[test]
    fn resolve_defaults() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let r = resolve(None, ResolveOptions::default()).unwrap();
        for (key, want) in [
            ("fixture.mode", "alpha"),
            ("fixture.count", "1"),
            ("fixture.enabled", "false"),
            ("fixture.timeout", "1m0s"),
        ] {
            assert_eq!(r.string(key), want);
            assert!(!r.supplied(key), "{key}");
        }
    }

    #[test]
    fn resolve_rejects_malformed_value() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let env = env_map(&[("FIXTURE_TIMEOUT", "ninety")]);
        let err = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap_err()
        .to_string();
        for w in ["fixture.timeout", "FIXTURE_TIMEOUT", "ninety"] {
            assert!(err.contains(w), "{err} lacks {w}");
        }
    }

    #[test]
    fn resolve_origins_lists_only_supplied_settings() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let env = env_map(&[("FIXTURE_NAME", "n")]);
        let r = resolve(
            Some(&flags_with(&[("fixture-mode", "beta")])),
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let got: BTreeMap<String, Origin> = r
            .origins()
            .into_iter()
            .map(|v| (v.setting.key, v.from))
            .collect();
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got["fixture.name"], Origin::Env);
        assert_eq!(got["fixture.mode"], Origin::Flag);
    }

    #[test]
    fn origin_describe_names_the_editable_thing() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let s = lookup("fixture.mode").unwrap();
        for (o, want) in [
            (Origin::Env, "FIXTURE_MODE"),
            (Origin::File, "config.fixture.mode"),
            (Origin::Flag, "--fixture-mode"),
            (Origin::Default, "default"),
        ] {
            assert_eq!(o.describe(&s), want);
        }
    }
}
