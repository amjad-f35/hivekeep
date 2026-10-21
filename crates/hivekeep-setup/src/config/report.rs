//! The configuration report printed before an install touches the cluster.
//!
//! A value from a file or an exported variable is invisible in the command
//! the operator typed, so every channel is named on a run that succeeds -- an
//! install that works with the wrong value never produces an error.

use super::resolve::{Resolved, Value};
use std::io::Write;

/// Shapes the report.
#[derive(Clone, Copy, Debug, Default)]
pub struct ReportOptions {
    /// List only the settings something supplied.
    pub omit_defaults: bool,
}

/// [`Value::display`] for the table: an empty value shows as `""`, so a
/// setting set to empty is not a blank column.
fn reported(v: &Value) -> String {
    match v.display() {
        s if s.is_empty() => "\"\"".into(),
        s => s,
    }
}

impl Resolved {
    /// Writes a summary line and one aligned row per setting.
    pub fn report(&self, w: &mut dyn Write, opts: ReportOptions) {
        let supplied = self.origins();
        let all = self.all_sorted();
        let _ = writeln!(
            w,
            "configuration: {} set, {} default",
            supplied.len(),
            all.len() - supplied.len()
        );
        let rows = if opts.omit_defaults { supplied } else { all };
        let key_width = rows.iter().map(|v| v.setting.key.len()).max().unwrap_or(0);
        let val_width = rows
            .iter()
            .map(|v| reported(v).chars().count())
            .max()
            .unwrap_or(0);
        for v in &rows {
            let _ = writeln!(
                w,
                "  {:<key_width$}  {:<val_width$}  (from {})",
                v.setting.key,
                reported(v),
                v.from.describe(&v.setting)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::all;
    use crate::config::file::File;
    use crate::config::resolve::{resolve, Flags, ResolveOptions};
    use crate::config::testutil::{env_map, TestEnv};
    use std::collections::BTreeMap;

    fn render(r: &Resolved, opts: ReportOptions) -> String {
        let mut buf = Vec::new();
        r.report(&mut buf, opts);
        String::from_utf8(buf).unwrap()
    }

    #[test]
    fn report_names_every_channel() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let mut flags = Flags::default();
        flags.set("fixture-name", "from-flag");
        let file = File::from_values(
            "install.yaml",
            BTreeMap::from([("fixture.count".into(), "9".into())]),
        );
        let env = env_map(&[("FIXTURE_MODE", "beta")]);
        let r = resolve(
            Some(&flags),
            ResolveOptions {
                env: Some(&env),
                file: Some(&file),
            },
        )
        .unwrap();
        let got = render(&r, ReportOptions::default());
        for want in [
            "fixture.name",
            "from-flag",
            "--fixture-name",
            "fixture.mode",
            "beta",
            "FIXTURE_MODE",
            "fixture.count",
            "9",
            "config.fixture.count",
        ] {
            assert!(got.contains(want), "missing {want}:\n{got}");
        }
    }

    #[test]
    fn report_counts_supplied_and_defaulted() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let env = env_map(&[("FIXTURE_NAME", "just-one")]);
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let full = render(&r, ReportOptions::default());
        let short = render(
            &r,
            ReportOptions {
                omit_defaults: true,
            },
        );
        let want = format!("configuration: 1 set, {} default", all().len() - 1);
        for b in [&full, &short] {
            assert_eq!(b.lines().next().unwrap(), want);
        }
        let lines: Vec<&str> = short.trim_end().lines().collect();
        assert_eq!(lines.len(), 2, "{short}");
        assert_eq!(full.matches('\n').count(), all().len() + 1);
        assert!(lines[1].contains("just-one"));
    }

    #[test]
    fn report_never_prints_a_secret_value() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let mut env = BTreeMap::new();
        for s in all() {
            if s.secret {
                env.insert(s.env.clone(), format!("SENSITIVE-{}", s.key));
            }
        }
        assert!(!env.is_empty());
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let got = render(&r, ReportOptions::default());
        assert!(!got.contains("SENSITIVE"), "{got}");
        for k in env.keys() {
            assert!(got.contains(k.as_str()), "omits the channel {k}");
        }
        assert!(got.contains("<set>"));
    }

    #[test]
    fn value_display() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let secret = crate::config::lookup("fixture.secret").unwrap();
        let plain = crate::config::lookup("fixture.name").unwrap();
        let v = |s: &crate::config::Setting, raw: &str, supplied: bool| Value {
            setting: s.clone(),
            raw: raw.into(),
            supplied,
            ..Default::default()
        };
        assert_eq!(v(&plain, "a-value", false).display(), "a-value");
        assert_eq!(v(&secret, "postgres://u:p@h/d", true).display(), "<set>");
        assert_eq!(v(&secret, "", false).display(), "");
        assert_eq!(v(&plain, "", false).display(), "");
    }

    #[test]
    fn report_lists_every_setting_by_default() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let env = env_map(&[("FIXTURE_NAME", "chosen")]);
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let short = render(
            &r,
            ReportOptions {
                omit_defaults: true,
            },
        );
        let long = render(&r, ReportOptions::default());
        assert_eq!(long.matches('\n').count(), all().len() + 1);
        assert!(short.matches('\n').count() < long.matches('\n').count());
        assert!(short.contains("chosen") && long.contains("chosen"));
        for want in ["fixture.mode", "alpha", "(from default)"] {
            assert!(long.contains(want), "missing {want}");
        }
    }

    #[test]
    fn report_keeps_secrets_withheld_when_listing_everything() {
        let mut t = TestEnv::new();
        t.use_fixtures();
        let env = env_map(&[("FIXTURE_SECRET", "SENSITIVE")]);
        let r = resolve(
            None,
            ResolveOptions {
                env: Some(&env),
                file: None,
            },
        )
        .unwrap();
        let got = render(&r, ReportOptions::default());
        assert!(!got.contains("SENSITIVE") && got.contains("<set>"), "{got}");

        let r = resolve(None, ResolveOptions::default()).unwrap();
        let got = render(&r, ReportOptions::default());
        assert!(got.contains("fixture.secret"));
        assert!(!got.contains("<set>"), "{got}");
    }
}
