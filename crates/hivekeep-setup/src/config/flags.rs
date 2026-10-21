//! Projecting the registry onto clap: one flag per bindable setting, and
//! reading back only the flags the command line actually set.

use super::registry::all;
use super::resolve::Flags;
use super::setting::{Setting, ValueKind};
use clap::builder::BoolishValueParser;
use clap::parser::ValueSource;
use clap::{Arg, ArgAction, ArgMatches, Command};

/// The clap argument for a setting. It is registered with its declared
/// default so help shows it; a default is never mistaken for a supplied
/// value, because [`Flags::from_matches`] reads only command-line values.
///
/// Only bools are typed. Ints and durations take strings so that
/// [`Setting::parse`] is the one thing deciding what they accept, on every
/// channel alike.
fn arg_for(s: &Setting) -> Arg {
    let mut arg = Arg::new(s.flag.clone())
        .long(s.flag.clone())
        .help(s.usage.clone());
    if s.kind == ValueKind::Bool {
        // `--kind` and `--kind=false` both work: an explicit false is a
        // deliberate override of an inherited variable.
        arg = arg
            .num_args(0..=1)
            .require_equals(true)
            .default_missing_value("true")
            .value_parser(BoolishValueParser::new())
            .action(ArgAction::Set);
        if !s.default.is_empty() {
            arg = arg.default_value(s.default.clone());
        }
        return arg;
    }
    arg = arg
        .num_args(1)
        .action(ArgAction::Set)
        .allow_hyphen_values(true);
    if !s.default.is_empty() {
        arg = arg.default_value(s.default.clone());
    }
    arg
}

/// Adds the settings that belong to no single command, as global flags on
/// the root. Secrets never get a flag, whatever their declaration says: a
/// command line is visible in `ps` and kept in shell history.
pub fn bind_flags(mut cmd: Command) -> Command {
    for s in all() {
        if s.bindable() && s.commands.is_empty() && !has_arg(&cmd, &s.flag) {
            cmd = cmd.arg(arg_for(&s).global(true));
        }
    }
    cmd
}

/// Adds the settings scoped to one command path (as typed: `deploy
/// benchmarks`) to that command. A setting naming several paths is bound on
/// each.
pub fn bind_command_flags(path: &str, mut cmd: Command) -> Command {
    for s in all() {
        if s.bindable() && s.commands.iter().any(|c| c == path) && !has_arg(&cmd, &s.flag) {
            cmd = cmd.arg(arg_for(&s));
        }
    }
    cmd
}

fn has_arg(cmd: &Command, id: &str) -> bool {
    cmd.get_arguments().any(|a| a.get_id().as_str() == id)
}

impl Flags {
    /// The flags the command line set, as raw strings. Values clap filled in
    /// from defaults are left out. Bools read back as `"true"` or `"false"`.
    pub fn from_matches(matches: &ArgMatches) -> Flags {
        let mut out = Flags::default();
        for id in matches.ids() {
            let name = id.as_str();
            if matches.value_source(name) != Some(ValueSource::CommandLine) {
                continue;
            }
            if let Ok(Some(b)) = matches.try_get_one::<bool>(name) {
                out.set(name, if *b { "true" } else { "false" });
            } else if let Ok(Some(v)) = matches.try_get_one::<String>(name) {
                out.set(name, v);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::file::File;
    use crate::config::registry::registry;
    use crate::config::resolve::{resolve, Origin, ResolveOptions};
    use crate::config::testutil::{env_map, test_setting, TestEnv};
    use std::collections::BTreeMap;

    fn parse(cmd: Command, args: &[&str]) -> ArgMatches {
        let mut argv = vec!["test"];
        argv.extend_from_slice(args);
        cmd.try_get_matches_from(argv).unwrap()
    }

    fn arg_names(cmd: &Command) -> Vec<String> {
        cmd.get_arguments()
            .map(|a| a.get_id().to_string())
            .collect()
    }

    #[test]
    fn bind_flags_registers_every_setting() {
        let cmd = bind_flags(Command::new("test"));
        let names = arg_names(&cmd);
        for s in registry() {
            if !s.flag.is_empty() {
                assert!(names.contains(&s.flag), "--{} not registered", s.flag);
            }
        }
    }

    #[test]
    fn bind_flags_registers_declared_defaults() {
        let cmd = bind_flags(Command::new("test"));
        let m = parse(cmd.clone(), &[]);
        for s in registry() {
            if s.default.is_empty() || s.flag.is_empty() {
                continue;
            }
            let arg = cmd
                .get_arguments()
                .find(|a| a.get_id().as_str() == s.flag)
                .unwrap();
            let defaults: Vec<String> = arg
                .get_default_values()
                .iter()
                .map(|v| v.to_string_lossy().into_owned())
                .collect();
            assert_eq!(defaults, vec![s.default.clone()], "--{}", s.flag);
            assert_ne!(
                m.value_source(&s.flag),
                Some(ValueSource::CommandLine),
                "--{} changed",
                s.flag
            );
        }
        assert!(
            Flags::from_matches(&m).0.is_empty(),
            "defaults read back as supplied"
        );
    }

    #[test]
    fn declared_defaults_appear_in_help() {
        let mut cmd = bind_flags(Command::new("test"));
        let help = cmd.render_long_help().to_string();
        let zero = |k: ValueKind| match k {
            ValueKind::Bool => "false",
            ValueKind::Int => "0",
            ValueKind::Duration => "0s",
            ValueKind::String => "",
        };
        for s in registry() {
            if s.default.is_empty() || s.default == zero(s.kind) {
                continue;
            }
            assert!(
                help.contains(&format!("[default: {}]", s.default)),
                "--{} default {} not in help",
                s.flag,
                s.default
            );
        }
    }

    #[test]
    fn from_matches_reads_only_the_command_line() {
        let m = parse(
            bind_flags(Command::new("test")),
            &["--kind=false", "--namespace", "x", "--e2e-tuning"],
        );
        let f = Flags::from_matches(&m);
        assert_eq!(f.changed("kind"), Some("false"));
        assert_eq!(f.changed("e2e-tuning"), Some("true"));
        assert_eq!(f.changed("namespace"), Some("x"));
        assert_eq!(f.changed("cluster-size"), None);
        // An explicitly empty value is still a value.
        let m = parse(bind_flags(Command::new("test")), &["--preview="]);
        assert_eq!(Flags::from_matches(&m).changed("preview"), Some(""));
    }

    #[test]
    fn global_flags_reach_subcommands() {
        let cmd = bind_flags(Command::new("test").subcommand(Command::new("deploy")));
        let m = cmd
            .try_get_matches_from(["test", "deploy", "--namespace", "x"])
            .unwrap();
        let (_, sub) = m.subcommand().unwrap();
        assert_eq!(Flags::from_matches(sub).changed("namespace"), Some("x"));
    }

    #[test]
    fn scoped_setting_resolves_from_every_channel() {
        let mut t = TestEnv::new();
        t.use_settings(vec![test_setting(
            "test.scoped",
            "HK_TEST_SCOPED",
            "test-scoped",
            &["deploy thing"],
        )]);
        type Case<'a> = (
            &'a str,
            Vec<(&'a str, &'a str)>,
            bool,
            Option<&'a str>,
            &'a str,
            Origin,
        );
        let cases: Vec<Case> = vec![
            ("default", vec![], false, None, "", Origin::Default),
            (
                "environment",
                vec![("HK_TEST_SCOPED", "fromenv")],
                false,
                None,
                "fromenv",
                Origin::Env,
            ),
            ("file", vec![], true, None, "fromfile", Origin::File),
            (
                "flag",
                vec![],
                false,
                Some("fromflag"),
                "fromflag",
                Origin::Flag,
            ),
        ];
        for (name, env, file, flag, want, want_from) in cases {
            let file = file.then(|| {
                File::from_values(
                    "f",
                    BTreeMap::from([("test.scoped".into(), "fromfile".into())]),
                )
            });
            let cmd = bind_command_flags("deploy thing", Command::new("thing"));
            let mut argv = vec![];
            let arg;
            if let Some(v) = flag {
                arg = format!("--test-scoped={v}");
                argv.push(arg.as_str());
            }
            let flags = Flags::from_matches(&parse(cmd, &argv));
            let env = env_map(&env);
            let r = resolve(
                Some(&flags),
                ResolveOptions {
                    env: Some(&env),
                    file: file.as_ref(),
                },
            )
            .unwrap();
            assert_eq!(r.string("test.scoped"), want, "{name}");
            assert_eq!(r.value("test.scoped").unwrap().from, want_from, "{name}");
        }
    }

    #[test]
    fn bind_command_flags_binds_only_the_named_path() {
        let mut t = TestEnv::new();
        t.use_settings(vec![
            test_setting("test.one", "HK_TEST_ONE", "test-one", &["deploy one"]),
            test_setting(
                "test.both",
                "HK_TEST_BOTH",
                "test-both",
                &["deploy one", "delete one"],
            ),
        ]);
        for (path, want, skip) in [
            ("deploy one", vec!["test-one", "test-both"], vec![]),
            ("delete one", vec!["test-both"], vec!["test-one"]),
            ("deploy other", vec![], vec!["test-one", "test-both"]),
        ] {
            let names = arg_names(&bind_command_flags(path, Command::new("x")));
            for n in want {
                assert!(names.iter().any(|x| x == n), "--{n} not bound to {path}");
            }
            for n in skip {
                assert!(!names.iter().any(|x| x == n), "--{n} bound to {path}");
            }
        }
    }

    #[test]
    fn bind_flags_skips_scoped_settings() {
        let mut t = TestEnv::new();
        t.use_settings(vec![
            test_setting(
                "test.scoped",
                "HK_TEST_SCOPED",
                "test-scoped",
                &["deploy thing"],
            ),
            test_setting("test.global", "HK_TEST_GLOBAL", "test-global", &[]),
        ]);
        let names = arg_names(&bind_flags(Command::new("x")));
        assert!(!names.iter().any(|n| n == "test-scoped"));
        assert!(names.iter().any(|n| n == "test-global"));
    }

    #[test]
    fn binding_refuses_secrets_that_declare_a_flag() {
        let mut t = TestEnv::new();
        t.use_settings(vec![
            Setting::new("test.secret", "HK_TEST_SECRET", "test-secret")
                .secret()
                .usage("a secret"),
            Setting::new(
                "test.scopedSecret",
                "HK_TEST_SCOPED_SECRET",
                "test-scoped-secret",
            )
            .secret()
            .usage("scoped")
            .commands(&["deploy thing"]),
        ]);
        assert!(!arg_names(&bind_flags(Command::new("x")))
            .iter()
            .any(|n| n == "test-secret"));
        assert!(
            !arg_names(&bind_command_flags("deploy thing", Command::new("x")))
                .iter()
                .any(|n| n == "test-scoped-secret")
        );
    }

    #[test]
    fn int_acceptance_is_the_same_on_every_channel() {
        let mut t = TestEnv::new();
        t.use_settings(vec![Setting::new(
            "scratch.count",
            "SCRATCH_COUNT",
            "scratch-count",
        )
        .kind(ValueKind::Int)
        .default_value("1")
        .usage("an int")]);
        for raw in ["3x", "2.5", "0x10", "1e3", "12 34"] {
            let env = env_map(&[("SCRATCH_COUNT", raw)]);
            let env_err = resolve(
                None,
                ResolveOptions {
                    env: Some(&env),
                    file: None,
                },
            )
            .is_err();
            let file =
                File::from_values("f", BTreeMap::from([("scratch.count".into(), raw.into())]));
            let file_err = resolve(
                None,
                ResolveOptions {
                    file: Some(&file),
                    env: None,
                },
            )
            .is_err();
            let arg = format!("--scratch-count={raw}");
            let flags = Flags::from_matches(&parse(bind_flags(Command::new("x")), &[arg.as_str()]));
            let flag_err = resolve(Some(&flags), ResolveOptions::default()).is_err();
            assert!(
                env_err && file_err && flag_err,
                "{raw}: env {env_err} file {file_err} flag {flag_err}"
            );
        }
    }
}
