//! The command line as a user meets it: flag parsing, argument counts,
//! where help and errors go, and the checks made before connecting.

mod common;

use clap::Parser;
use common::*;
use hivekeep_ctl::app::{self, failure_report, parse_failure_report};
use hivekeep_ctl::cli::Cli;
use hivekeep_ctl::io::Io;

/// What one `hivectl` invocation printed, as the binary would print it.
struct Outcome {
    out: String,
    err: String,
    failed: bool,
}

fn argv(args: &[&str]) -> Vec<String> {
    std::iter::once("hivectl")
        .chain(args.iter().copied())
        .map(String::from)
        .collect()
}

/// Runs `hivectl args` with no endpoint configured, the way `main` does:
/// clap's own messages go where clap sends them, and a failed run prints
/// [`failure_report`] on stderr.
async fn run(args: &[&str]) -> Outcome {
    let argv = argv(args);
    match Cli::try_parse_from(&argv) {
        Err(e) => {
            let text = parse_failure_report(&e, &argv);
            if e.use_stderr() {
                Outcome {
                    out: String::new(),
                    err: text,
                    failed: true,
                }
            } else {
                Outcome {
                    out: text,
                    err: String::new(),
                    failed: false,
                }
            }
        }
        Ok(mut cli) => {
            cli.global.endpoint = None;
            let (io, out, err) = Io::captured();
            let result = app::run(cli, io).await;
            let mut err = err.contents();
            if let Err(e) = &result {
                err.push_str(&failure_report(e, &argv));
            }
            Outcome {
                out: out.contents(),
                err,
                failed: result.is_err(),
            }
        }
    }
}

fn parses(args: &[&str]) -> bool {
    Cli::try_parse_from(argv(args)).is_ok()
}

#[test]
fn version_flag() {
    let err = Cli::try_parse_from(argv(&["--version"])).unwrap_err();
    assert_eq!(err.kind(), clap::error::ErrorKind::DisplayVersion);
    assert!(!err.use_stderr());
    let text = err.render().to_string();
    assert!(text.starts_with("hivectl "), "{text}");
    // -v is verbosity, never the version.
    assert!(Cli::try_parse_from(argv(&["-v", "get", "hives"])).is_ok());
}

#[test]
fn verbose_flag_counting() {
    for (name, flags, want) in [
        ("default", &[][..], 0),
        ("single short", &["-v"], 1),
        ("stacked short 2", &["-vv"], 2),
        ("stacked short 3", &["-vvv"], 3),
        ("stacked short 5", &["-vvvvv"], 5),
        ("repeated short", &["-v", "-v", "-v"], 3),
        ("single long", &["--verbose"], 1),
        ("repeated long", &["--verbose", "--verbose"], 2),
        ("mixed short and long", &["-vv", "--verbose"], 3),
        ("explicit value", &["--verbose=4"], 4),
    ] {
        let mut args = flags.to_vec();
        args.extend(["get", "hives"]);
        let cli = Cli::try_parse_from(argv(&args)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(cli.global.verbosity(), want, "{name}");
    }
}

#[tokio::test]
async fn preview_flag() {
    for (name, flags, want) in [
        ("unset", &[][..], None),
        ("set", &["--preview"], Some("true")),
    ] {
        let fake = Fake::default();
        let addr = serve(fake.clone()).await;
        let mut args = vec!["get", "hives"];
        args.extend_from_slice(flags);
        hivectl(addr, &args).await.ok();
        let call = fake.state().calls_to("ListHives").pop().unwrap();
        assert_eq!(
            call.metadata.get("hk-preview-allowed").map(String::as_str),
            want,
            "{name}"
        );
    }
}

#[test]
fn command_args() {
    for (name, args, ok) in [
        // Actors.
        ("actor list", &["get", "actors", "-A"][..], true),
        ("actor get", &["get", "actors", "actor-1", "-H", "h"], true),
        (
            "actor get multiple",
            &["get", "actors", "actor-1", "actor-2", "-H", "h"],
            true,
        ),
        // Hives.
        ("hive list", &["get", "hives"], true),
        ("hive get", &["get", "hives", "team-a"], true),
        (
            "hive get multiple",
            &["get", "hives", "team-a", "team-b"],
            true,
        ),
        // Workers.
        ("worker list", &["get", "workers"], true),
        ("worker get", &["get", "workers", "worker-1"], true),
        (
            "worker get multiple",
            &["get", "workers", "worker-1", "worker-2"],
            true,
        ),
        ("top list", &["top", "workers"], true),
        (
            "top rejects argument",
            &["top", "workers", "worker-1"],
            false,
        ),
        // Actor templates.
        ("template list", &["get", "actor-templates", "-A"], true),
        (
            "template get",
            &["get", "actor-templates", "counter", "-H", "h"],
            true,
        ),
        (
            "template get multiple",
            &[
                "get",
                "actor-templates",
                "counter",
                "counter-microvm",
                "-H",
                "h",
            ],
            true,
        ),
        // Egress policies.
        (
            "egress get",
            &["get", "egress-policy", "c1", "-H", "team-a"],
            true,
        ),
        (
            "egress get requires actor",
            &["get", "egress-policy", "-H", "team-a"],
            false,
        ),
        (
            "egress get rejects multiple",
            &["get", "egress-policy", "c1", "c2", "-H", "team-a"],
            false,
        ),
        (
            "egress create",
            &[
                "create",
                "egress-policy",
                "c1",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            true,
        ),
        (
            "egress create requires actor",
            &[
                "create",
                "egress-policy",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            false,
        ),
        (
            "egress create rejects multiple",
            &[
                "create",
                "egress-policy",
                "c1",
                "c2",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            false,
        ),
        (
            "egress update",
            &[
                "update",
                "egress-policy",
                "c1",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            true,
        ),
        (
            "egress update requires actor",
            &[
                "update",
                "egress-policy",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            false,
        ),
        (
            "egress update rejects multiple",
            &[
                "update",
                "egress-policy",
                "c1",
                "c2",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            false,
        ),
        (
            "egress delete",
            &["delete", "egress-policy", "c1", "-H", "team-a"],
            true,
        ),
        (
            "egress delete requires actor",
            &["delete", "egress-policy", "-H", "team-a"],
            false,
        ),
        (
            "egress delete rejects multiple",
            &["delete", "egress-policy", "c1", "c2", "-H", "team-a"],
            false,
        ),
    ] {
        assert_eq!(parses(args), ok, "{name}: {args:?}");
    }
}

#[test]
fn suspend_mode_flag_default() {
    let cli = Cli::try_parse_from(argv(&["suspend", "actor", "a", "-H", "h"])).unwrap();
    match cli.command {
        hivekeep_ctl::cli::Command::Suspend(hivekeep_ctl::cli::SuspendCommand::Actor {
            mode,
            ..
        }) => assert_eq!(mode, hivekeep_ctl::refs::SUSPEND_MODE_ASYNC),
        other => panic!("{other:?}"),
    }
}

#[test]
fn make_ca_pool_default_key_type_is_ecdsa() {
    let cli = Cli::try_parse_from(argv(&["admin", "make-ca-pool", "--name", "ca"])).unwrap();
    match cli.command {
        hivekeep_ctl::cli::Command::Admin(hivekeep_ctl::cli::AdminCommand::MakeCaPool {
            key_type,
            ..
        }) => assert_eq!(
            key_type.parse::<hivekeep_ctl::ca_pool::CaKeyType>(),
            Ok(hivekeep_ctl::ca_pool::CaKeyType::EcdsaP256)
        ),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn get_scope_before_connecting() {
    for (resource, plural) in [
        ("actors", "actors"),
        ("actor-templates", "actor templates"),
        ("tags", "tags"),
    ] {
        let by_name = format!("-H/--hive is required when getting {plural} by name");
        let all_by_name = format!("-A/--all-hives cannot be used when getting {plural} by name");
        for (name, extra, want) in [
            ("get without hive", &["counter"][..], by_name.as_str()),
            (
                "get with all hives",
                &["counter", "-A"],
                all_by_name.as_str(),
            ),
            (
                "get with both scopes",
                &["counter", "-H", "demo", "-A"],
                all_by_name.as_str(),
            ),
            (
                "list without scope",
                &[],
                "-H/--hive <name> or -A/--all-hives is required",
            ),
            (
                "list with both scopes",
                &["-H", "demo", "-A"],
                "-H/--hive and -A/--all-hives are mutually exclusive",
            ),
            // Valid scopes get as far as connecting, where the missing
            // endpoint fails.
            (
                "get in hive",
                &["counter", "-H", "demo"],
                "no hivekeep-api endpoint configured",
            ),
            (
                "get multiple in hive",
                &["counter", "other", "-H", "demo"],
                "no hivekeep-api endpoint configured",
            ),
            (
                "list in hive",
                &["-H", "demo"],
                "no hivekeep-api endpoint configured",
            ),
            (
                "list all hives",
                &["-A"],
                "no hivekeep-api endpoint configured",
            ),
        ] {
            let mut args = vec!["get", resource];
            args.extend_from_slice(extra);
            let o = run(&args).await;
            assert!(o.failed, "{resource} {name}");
            assert!(o.err.contains(want), "{resource} {name}: {}", o.err);
        }
    }
}

#[tokio::test]
async fn help_output_goes_to_stdout() {
    for (name, args, want) in [
        (
            "root --help",
            &["--help"][..],
            "Usage: hivectl [OPTIONS] <COMMAND>",
        ),
        (
            "verb --help",
            &["get", "--help"],
            "Usage: hivectl get [OPTIONS] <COMMAND>",
        ),
        (
            "leaf -h",
            &["create", "actor", "-h"],
            "Usage: hivectl create actor",
        ),
        (
            "help subcommand",
            &["help", "create", "actor"],
            "Usage: hivectl create actor",
        ),
    ] {
        let o = run(args).await;
        assert!(!o.failed, "{name}");
        assert_eq!(o.err, "", "{name}");
        assert!(o.out.contains(want), "{name}: {}", o.out);
    }
}

#[tokio::test]
async fn usage_errors_go_to_stderr_with_usage() {
    const GATE: [&str; 8] = [
        "--gate",
        "gate:443",
        "--gate-ca-file",
        "ca.pem",
        "--client-cert",
        "c.pem",
        "--client-key",
        "k.pem",
    ];
    let proxy_bad_port: Vec<&str> = ["proxy", "actor", "my-actor", "badport", "-H", "space-1"]
        .into_iter()
        .chain(GATE)
        .collect();
    let cases: Vec<(&str, Vec<&str>, Vec<&str>, &str)> = vec![
        // Flag values and parsing.
        (
            "get actor -H missing argument",
            vec!["get", "actor", "-H"],
            vec!["a value is required for '--hive <HIVE>'"],
            "Usage: hivectl get actor",
        ),
        (
            "get actor --hive missing argument",
            vec!["get", "actor", "--hive"],
            vec!["a value is required for '--hive <HIVE>'"],
            "Usage: hivectl get actor",
        ),
        (
            "create actor -H missing argument",
            vec!["create", "actor", "-H"],
            vec!["a value is required for '--hive <HIVE>'"],
            "Usage: hivectl create actor",
        ),
        (
            "create actor-template -f missing argument",
            vec!["create", "actor-template", "-f"],
            vec!["a value is required for '--filename <FILENAME>'"],
            "Usage: hivectl create actor-template",
        ),
        (
            "global -o missing argument",
            vec!["get", "hives", "-o"],
            vec!["a value is required for '--output <OUTPUT>'"],
            "Usage: hivectl get hive",
        ),
        (
            "swallowed flag after -H",
            vec!["create", "actor", "my-actor", "-H", "--template", "counter"],
            vec!["--hive"],
            "Usage: hivectl create actor",
        ),
        (
            "invalid output format",
            vec!["get", "hives", "-o", "xml"],
            vec![
                "invalid value 'xml' for '--output <OUTPUT>'",
                "invalid output format \"xml\". Must be one of: table, json, yaml",
            ],
            "Usage: hivectl get hive",
        ),
        (
            "unknown short flag on leaf",
            vec!["get", "hives", "-x"],
            vec!["unexpected argument '-x' found"],
            "Usage: hivectl get hive",
        ),
        (
            "unknown long flag on leaf",
            vec!["get", "hives", "--bogus"],
            vec!["unexpected argument '--bogus' found"],
            "Usage: hivectl get hive",
        ),
        // Missing positional arguments and required flags, all reported at
        // once.
        (
            "create actor with only -H",
            vec!["create", "actor", "-H", "my-hive"],
            vec![
                "required arguments were not provided",
                "--template <TEMPLATE>",
                "<NAME>",
            ],
            "Usage: hivectl create actor",
        ),
        (
            "create actor with nothing",
            vec!["create", "actor"],
            vec![
                "required arguments were not provided",
                "--hive <HIVE>",
                "--template <TEMPLATE>",
                "<NAME>",
            ],
            "Usage: hivectl create actor",
        ),
        (
            "create actor with name only",
            vec!["create", "actor", "my-actor"],
            vec!["--hive <HIVE>", "--template <TEMPLATE>"],
            "Usage: hivectl create actor",
        ),
        (
            "create actor with name and -H",
            vec!["create", "actor", "my-actor", "-H", "my-hive"],
            vec!["--template <TEMPLATE>"],
            "Usage: hivectl create actor",
        ),
        (
            "create actor with both flags but no name",
            vec!["create", "actor", "-H", "my-hive", "--template", "counter"],
            vec!["<NAME>"],
            "Usage: hivectl create actor",
        ),
        (
            "create actor with too many args",
            vec![
                "create",
                "actor",
                "a1",
                "a2",
                "-H",
                "my-hive",
                "--template",
                "counter",
            ],
            vec!["unexpected argument 'a2' found"],
            "Usage: hivectl create actor",
        ),
        (
            "update actor with nothing",
            vec!["update", "actor"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl update actor",
        ),
        (
            "update actor missing -H",
            vec!["update", "actor", "my-actor", "--template", "counter"],
            vec!["--hive <HIVE>"],
            "Usage: hivectl update actor",
        ),
        (
            "delete actor with nothing",
            vec!["delete", "actor"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl delete actor",
        ),
        (
            "delete actor missing -H",
            vec!["delete", "actor", "my-actor"],
            vec!["--hive <HIVE>"],
            "Usage: hivectl delete actor",
        ),
        (
            "revert actor with nothing",
            vec!["revert", "actor"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl revert actor",
        ),
        (
            "logs actor with nothing",
            vec!["logs", "actor"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl logs actor",
        ),
        (
            "proxy actor with nothing",
            vec!["proxy", "actor"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl proxy actor",
        ),
        (
            "proxy actor missing -H",
            vec!["proxy", "actor", "my-actor"],
            vec!["--hive <HIVE>"],
            "Usage: hivectl proxy actor",
        ),
        (
            "proxy actor invalid port mapping",
            proxy_bad_port,
            vec!["invalid port \"badport\""],
            "Usage: hivectl proxy actor",
        ),
        (
            "create actor-template missing -f",
            vec!["create", "actor-template"],
            vec!["--filename <FILENAME>"],
            "Usage: hivectl create actor-template",
        ),
        (
            "create actor-template unexpected positional arg",
            vec!["create", "actor-template", "my-tpl", "-f", "tpl.yaml"],
            vec!["unexpected argument 'my-tpl' found"],
            "Usage: hivectl create actor-template",
        ),
        (
            "delete actor-template with nothing",
            vec!["delete", "actor-template"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl delete actor-template",
        ),
        (
            "create hive missing name",
            vec!["create", "hive"],
            vec!["<NAME>"],
            "Usage: hivectl create hive",
        ),
        (
            "get egress-policy missing all",
            vec!["get", "egress-policy"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl get egress-policy",
        ),
        (
            "get egress-policy rejects multiple args",
            vec!["get", "egress-policy", "c1", "c2", "-H", "team-a"],
            vec!["unexpected argument 'c2' found"],
            "Usage: hivectl get egress-policy",
        ),
        (
            "create egress-policy missing all",
            vec!["create", "egress-policy"],
            vec!["--hive <HIVE>", "--filename <FILENAME>", "<NAME>"],
            "Usage: hivectl create egress-policy",
        ),
        (
            "create egress-policy rejects multiple args",
            vec![
                "create",
                "egress-policy",
                "c1",
                "c2",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            vec!["unexpected argument 'c2' found"],
            "Usage: hivectl create egress-policy",
        ),
        (
            "update egress-policy missing all",
            vec!["update", "egress-policy"],
            vec!["--hive <HIVE>", "--filename <FILENAME>", "<NAME>"],
            "Usage: hivectl update egress-policy",
        ),
        (
            "update egress-policy rejects multiple args",
            vec![
                "update",
                "egress-policy",
                "c1",
                "c2",
                "-H",
                "team-a",
                "-f",
                "policy.yaml",
            ],
            vec!["unexpected argument 'c2' found"],
            "Usage: hivectl update egress-policy",
        ),
        (
            "delete egress-policy missing all",
            vec!["delete", "egress-policy"],
            vec!["--hive <HIVE>", "<NAME>"],
            "Usage: hivectl delete egress-policy",
        ),
        (
            "delete egress-policy rejects multiple args",
            vec!["delete", "egress-policy", "c1", "c2", "-H", "team-a"],
            vec!["unexpected argument 'c2' found"],
            "Usage: hivectl delete egress-policy",
        ),
        (
            "create tag missing all",
            vec!["create", "tag"],
            vec!["--hive <HIVE>", "--actor <ACTOR>", "<NAME>"],
            "Usage: hivectl create tag",
        ),
        (
            "update tag missing scope",
            vec!["update", "tag", "t1", "-H", "space-1"],
            vec!["--scope <SCOPE>"],
            "Usage: hivectl update tag",
        ),
        (
            "top workers unexpected positional arg",
            vec!["top", "workers", "w1"],
            vec!["unexpected argument 'w1' found"],
            "Usage: hivectl top worker",
        ),
        (
            "admin make-ca-pool missing --name",
            vec!["admin", "make-ca-pool"],
            vec!["--name <NAME>"],
            "Usage: hivectl admin make-ca-pool",
        ),
        (
            "admin make-ca-pool unexpected positional arg",
            vec!["admin", "make-ca-pool", "my-secret", "--name", "my-secret"],
            vec!["unexpected argument 'my-secret' found"],
            "Usage: hivectl admin make-ca-pool",
        ),
        (
            "admin activate-jwt-key missing both required flags",
            vec!["admin", "activate-jwt-key"],
            vec!["--name <NAME>", "--key-id <KEY_ID>"],
            "Usage: hivectl admin activate-jwt-key",
        ),
        // Checks made after parsing, before connecting.
        (
            "get actor without -H or -A",
            vec!["get", "actor"],
            vec!["Error: -H/--hive <name> or -A/--all-hives is required"],
            "Usage: hivectl get actor",
        ),
        (
            "get actor with both -H and -A",
            vec!["get", "actor", "-H", "space-1", "-A"],
            vec!["Error: -H/--hive and -A/--all-hives are mutually exclusive"],
            "Usage: hivectl get actor",
        ),
        (
            "get actor by name without -H",
            vec!["get", "actor", "my-actor"],
            vec!["Error: -H/--hive is required when getting actors by name"],
            "Usage: hivectl get actor",
        ),
        (
            "get actor by name with -A",
            vec!["get", "actor", "my-actor", "-A"],
            vec!["Error: -A/--all-hives cannot be used when getting actors by name"],
            "Usage: hivectl get actor",
        ),
        (
            "get actor-template without -H or -A",
            vec!["get", "actor-template"],
            vec!["Error: -H/--hive <name> or -A/--all-hives is required"],
            "Usage: hivectl get actor-template",
        ),
        (
            "get tag without -H or -A",
            vec!["get", "tag"],
            vec!["Error: -H/--hive <name> or -A/--all-hives is required"],
            "Usage: hivectl get tag",
        ),
        (
            "get tag by name with -A",
            vec!["get", "tag", "t1", "-A"],
            vec!["Error: -A/--all-hives cannot be used when getting tags by name"],
            "Usage: hivectl get tag",
        ),
        (
            "get workers by name with filter flag",
            vec!["get", "workers", "w1", "-n", "default"],
            vec!["Error: -n/--namespace cannot be used when getting workers by name"],
            "Usage: hivectl get worker",
        ),
        (
            "get workers with invalid label selector",
            vec!["get", "workers", "-l", "===bad"],
            vec!["Error: invalid label selector \"===bad\""],
            "Usage: hivectl get worker",
        ),
        (
            "top workers with invalid label selector",
            vec!["top", "workers", "-l", "===bad"],
            vec!["Error: invalid label selector \"===bad\""],
            "Usage: hivectl top worker",
        ),
        (
            "logs actor with invalid --source",
            vec![
                "logs",
                "actor",
                "my-actor",
                "-H",
                "space-1",
                "--source=bogus",
            ],
            vec!["Error: invalid --source \"bogus\""],
            "Usage: hivectl logs actor",
        ),
        (
            "update actor without --template or --worker-selector",
            vec!["update", "actor", "my-actor", "-H", "space-1"],
            vec!["Error: at least one of --template or --worker-selector is required"],
            "Usage: hivectl update actor",
        ),
        (
            "update actor with malformed --template",
            vec![
                "update",
                "actor",
                "my-actor",
                "-H",
                "space-1",
                "--template",
                "a/b/c",
            ],
            vec!["Error: malformed reference \"a/b/c\" (expected <name> or <hive>/<name>)"],
            "Usage: hivectl update actor",
        ),
        (
            "update actor with invalid --worker-selector",
            vec![
                "update",
                "actor",
                "my-actor",
                "-H",
                "space-1",
                "--worker-selector",
                "===bad",
            ],
            vec!["Error: invalid worker selector \"===bad\""],
            "Usage: hivectl update actor",
        ),
        (
            "create tag with invalid --scope",
            vec![
                "create",
                "tag",
                "t1",
                "-H",
                "space-1",
                "--actor",
                "a1",
                "--scope=bogus",
            ],
            vec!["Error: invalid scope \"bogus\"; must be hive or published"],
            "Usage: hivectl create tag",
        ),
        (
            "delete egress-policy with invalid --version",
            vec![
                "delete",
                "egress-policy",
                "a1",
                "-H",
                "space-1",
                "--version=0",
            ],
            vec!["Error: --version must be at least 1, got 0"],
            "Usage: hivectl delete egress-policy",
        ),
        (
            "admin make-ca-pool with invalid --key-type",
            vec!["admin", "make-ca-pool", "--name", "s", "--key-type", "RSA"],
            vec!["Error: unknown key type \"RSA\""],
            "Usage: hivectl admin make-ca-pool",
        ),
        // Bare verbs and unknown subcommands.
        (
            "bare root command",
            vec![],
            vec![],
            "Usage: hivectl [OPTIONS] <COMMAND>",
        ),
        (
            "unknown root subcommand",
            vec!["bogus"],
            vec!["unrecognized subcommand 'bogus'"],
            "Usage: hivectl [OPTIONS] <COMMAND>",
        ),
        (
            "bare get command",
            vec!["get"],
            vec![],
            "Usage: hivectl get [OPTIONS] <COMMAND>",
        ),
        (
            "bare create command",
            vec!["create"],
            vec![],
            "Usage: hivectl create [OPTIONS] <COMMAND>",
        ),
        (
            "create plural actors suggests singular actor",
            vec![
                "create",
                "actors",
                "my-actor",
                "-H",
                "space-1",
                "--template",
                "counter",
            ],
            vec![
                "unrecognized subcommand 'actors'",
                "similar subcommands exist",
                "'actor'",
            ],
            "Usage: hivectl create [OPTIONS] <COMMAND>",
        ),
        (
            "logs without actor subcommand",
            vec!["logs", "my-actor", "-H", "space-1"],
            vec!["unrecognized subcommand 'my-actor'"],
            "Usage: hivectl logs [OPTIONS] <COMMAND>",
        ),
        (
            "logs with unknown subcommand and -h still reports unknown subcommand",
            vec!["logs", "asdasd", "-h"],
            vec!["unrecognized subcommand 'asdasd'"],
            "Usage: hivectl logs [OPTIONS] <COMMAND>",
        ),
        (
            "flag passed to verb without resource type",
            vec!["get", "-H", "space-1"],
            vec!["unexpected argument '-H' found"],
            "Usage: hivectl get [OPTIONS] <COMMAND>",
        ),
    ];
    for (name, args, wants, usage) in cases {
        let o = run(&args).await;
        assert!(o.failed, "{name}: succeeded");
        assert_eq!(o.out, "", "{name}: stdout");
        for want in wants {
            assert!(
                o.err.contains(want),
                "{name}: stderr lacks {want:?}:\n{}",
                o.err
            );
        }
        assert!(
            o.err.contains("\n\nUsage: "),
            "{name}: no usage:\n{}",
            o.err
        );
        assert!(
            o.err.contains(usage),
            "{name}: stderr lacks {usage:?}:\n{}",
            o.err
        );
    }
}

#[tokio::test]
async fn runtime_errors_do_not_print_usage() {
    let o = run(&[
        "create",
        "actor-template",
        "-f",
        "/nonexistent/path/to/manifest.yaml",
    ])
    .await;
    assert!(o.failed);
    assert_eq!(o.out, "");
    assert!(o.err.starts_with("Error: "), "{}", o.err);
    assert!(!o.err.contains("Usage:"), "{}", o.err);
}
