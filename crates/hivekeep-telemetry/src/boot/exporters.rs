//! The rules every `OTEL_*_EXPORTER` variable shares.

use std::collections::BTreeSet;
use std::fmt;

use crate::env::Env;

/// Selects the empty set; it is not an exporter.
pub const EXPORTER_NONE: &str = "none";
/// The specification's pull exporter, known only to a component that
/// serves its own `/metrics`.
pub const EXPORTER_PROMETHEUS: &str = "prometheus";
/// The OTLP push exporter.
pub const EXPORTER_OTLP: &str = "otlp";
/// Writing records to stdout as JSON log lines.
pub const EXPORTER_CONSOLE: &str = "console";

/// The set of exporters an `OTEL_*_EXPORTER` variable selects. The empty set
/// is `none`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Exporters(BTreeSet<String>);

impl Exporters {
    /// The empty set.
    pub fn none() -> Self {
        Exporters::default()
    }

    /// A set holding `names`.
    pub fn of<'a>(names: impl IntoIterator<Item = &'a str>) -> Self {
        Exporters(names.into_iter().map(str::to_string).collect())
    }

    /// Whether `name` is selected.
    pub fn has(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    /// Whether nothing is selected.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Removes `name`.
    pub fn remove(&mut self, name: &str) {
        self.0.remove(name);
    }
}

impl fmt::Display for Exporters {
    /// The names in order, or `none`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return f.write_str(EXPORTER_NONE);
        }
        let names: Vec<&str> = self.0.iter().map(String::as_str).collect();
        f.write_str(&names.join(","))
    }
}

/// A problem with an exporter variable's value. The set to use is returned
/// alongside it either way.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ExportersWarning(pub String);

/// Applies the shared rules to a comma-separated list, as the spec allows.
/// Names are trimmed, lowercased, and counted once; empty and unknown items
/// are skipped, and the rest apply. `none` with an exporter is rejected, and
/// a value that is empty, rejected, or names no known exporter gives a copy
/// of `def`.
pub fn resolve_exporters(
    env: &str,
    value: &str,
    known: &[&str],
    def: &Exporters,
) -> (Exporters, Option<ExportersWarning>) {
    // Unset and empty read the same: templated manifests can render empty
    // variables.
    if value.trim().is_empty() {
        return (def.clone(), None);
    }
    let mut got = Exporters::none();
    let mut none = false;
    let mut skipped = Vec::new();
    for item in value.split(',') {
        let name = item.trim().to_lowercase();
        if name == EXPORTER_NONE {
            none = true;
        } else if known.contains(&name.as_str()) {
            got.0.insert(name);
        } else {
            skipped.push(name);
        }
    }
    let skip_warn = (!skipped.is_empty()).then(|| {
        ExportersWarning(format!(
            "{env}: skipped empty or unknown exporters {skipped:?}"
        ))
    });
    if none && !got.is_empty() {
        return (
            def.clone(),
            Some(ExportersWarning(format!(
                "{env} {value:?} lists none with an exporter"
            ))),
        );
    }
    if none {
        return (Exporters::none(), skip_warn);
    }
    if got.is_empty() {
        return (
            def.clone(),
            Some(ExportersWarning(format!(
                "{env} {value:?} names no known exporter"
            ))),
        );
    }
    (got, skip_warn)
}

/// Reads `name` from `env` and applies [`resolve_exporters`], logging any
/// problem with the value rather than failing startup over a telemetry
/// setting.
pub fn resolve_exporters_env(
    env: &dyn Env,
    name: &str,
    known: &[&str],
    def: &Exporters,
) -> Exporters {
    let value = env.get(name);
    let (got, warning) = resolve_exporters(name, &value, known, def);
    if let Some(w) = warning {
        tracing::warn!(env = name, value = %value, using = %got, err = %w, "Invalid exporter environment");
    }
    got
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contextlogging::tests::capture;
    use crate::env::MapEnv;

    #[test]
    fn resolve_exporters_table() {
        let known = ["otlp", "console"];
        let otlp = Exporters::of(["otlp"]);
        let both = Exporters::of(["otlp", "console"]);
        let console = Exporters::of(["console"]);
        let none = Exporters::none();
        let cases: &[(&str, &str, &Exporters, &Exporters, bool)] = &[
            ("unset or empty keeps the default", "", &otlp, &otlp, false),
            (
                "whitespace only keeps the default",
                "   ",
                &otlp,
                &otlp,
                false,
            ),
            ("one name", "otlp", &none, &otlp, false),
            (
                "case and spaces do not matter",
                "  OTLP\t",
                &none,
                &otlp,
                false,
            ),
            ("a list", " Console , OTLP ", &none, &both, false),
            (
                "a repeated name counts once",
                "otlp,otlp",
                &none,
                &otlp,
                false,
            ),
            (
                "an empty item is skipped with a warning",
                "otlp,",
                &none,
                &otlp,
                true,
            ),
            ("none alone is the empty set", "none", &otlp, &none, false),
            (
                "an unknown item is skipped and the rest apply",
                "otlp,zipkin",
                &none,
                &otlp,
                true,
            ),
            (
                "unknown items next to none leave none",
                "none,zipkin",
                &otlp,
                &none,
                true,
            ),
            (
                "none with an exporter keeps the default",
                "none,otlp",
                &console,
                &console,
                true,
            ),
            (
                "none after an exporter keeps the default",
                "otlp,none",
                &console,
                &console,
                true,
            ),
            (
                "no known item keeps the default",
                "zipkin",
                &otlp,
                &otlp,
                true,
            ),
            ("only empty items keep the default", ",", &otlp, &otlp, true),
        ];
        for (name, value, def, want, want_err) in cases {
            let (got, err) = resolve_exporters("OTEL_TEST_EXPORTER", value, &known, def);
            assert_eq!(err.is_some(), *want_err, "{name}: err = {err:?}");
            assert_eq!(&got, *want, "{name}");
        }
    }

    #[test]
    fn resolve_exporters_env_warns() {
        const ENV: &str = "OTEL_TEST_EXPORTER";
        let known = ["otlp", "console"];
        for (value, want_using) in [
            ("otlp,zipkin", "otlp"),
            ("none,otlp", "console"),
            ("", ""),
            ("otlp", ""),
        ] {
            let env = MapEnv::new([(ENV, value)]);
            let out = capture(|| {
                resolve_exporters_env(&env, ENV, &known, &Exporters::of(["console"]));
            });
            let warns: Vec<&str> = out
                .lines()
                .filter(|l| l.contains("\"level\":\"WARN\""))
                .collect();
            if want_using.is_empty() {
                assert!(warns.is_empty(), "{value:?}: got warnings {warns:?}");
            } else {
                assert_eq!(warns.len(), 1, "{value:?}: {out}");
                assert!(
                    warns[0].contains(&format!("\"using\":\"{want_using}\"")),
                    "{value:?}: {out}"
                );
            }
        }
    }

    #[test]
    fn resolve_exporters_copies_default() {
        for value in ["", "zipkin", "none,otlp"] {
            let def = Exporters::of(["otlp"]);
            let (mut got, _) = resolve_exporters("OTEL_TEST_EXPORTER", value, &["otlp"], &def);
            got.remove("otlp");
            assert!(
                def.has("otlp"),
                "{value:?}: changing the result changed def"
            );
        }
    }

    #[test]
    fn exporters_string() {
        for (want, set) in [
            ("none", Exporters::none()),
            ("otlp", Exporters::of(["otlp"])),
            ("console,otlp", Exporters::of(["otlp", "console"])),
            ("a,b,c,d", Exporters::of(["d", "c", "b", "a"])),
        ] {
            assert_eq!(set.to_string(), want);
        }
    }
}
