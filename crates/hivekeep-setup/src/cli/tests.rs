use super::*;

fn try_parse(args: &[&str]) -> Result<ArgMatches, clap::Error> {
    let mut argv = vec!["hivekeep-setup"];
    argv.extend_from_slice(args);
    root().try_get_matches_from(argv)
}

/// Parses and validates the way [`execute`] does before it loads anything.
fn parse_and_validate(args: &[&str]) -> Result<(), String> {
    let m = try_parse(args).map_err(|e| e.to_string())?;
    let (path, sub) = leaf(&m);
    validate_args(&path, sub).map_err(|e| e.to_string())
}

/// A driver nothing can install is rejected while arguments are validated,
/// the last step before configuration is loaded and credentials fetched.
#[tokio::test]
async fn an_unknown_csi_driver_is_rejected_before_anything_runs() {
    for args in [
        vec!["deploy", "hivekeep-system", "--setup-csi=hostpaht"],
        vec!["setup", "csi", "nfsv4"],
    ] {
        let err = parse_and_validate(&args).expect_err("an unknown-driver error");
        assert!(err.contains("unknown CSI driver"), "{args:?}: {err}");
        // execute stops at the same point: no configuration report, no
        // connection, just the error.
        let mut argv = vec!["hivekeep-setup"];
        argv.extend_from_slice(&args);
        let err = execute(argv, CancellationToken::new()).await.unwrap_err();
        assert!(matches!(err, CliError::Step(_)), "{args:?}: {err:?}");
    }
}

#[test]
fn accepted_csi_drivers_pass_argument_validation() {
    let accepted = ["nfs", "hostpath", "both", "none", "true", "false", ""];
    for d in accepted {
        parse_and_validate(&["setup", "csi", d]).unwrap_or_else(|e| panic!("setup csi {d:?}: {e}"));
        let flag = format!("--setup-csi={d}");
        parse_and_validate(&["deploy", "hivekeep-system", &flag])
            .unwrap_or_else(|e| panic!("{flag}: {e}"));
    }
    parse_and_validate(&["setup", "csi"]).expect("setup csi with no argument");
    // The bare flag means none.
    let m = try_parse(&["deploy", "hivekeep-system", "--setup-csi"]).unwrap();
    let (_, sub) = leaf(&m);
    assert_eq!(
        sub.get_one::<String>("setup-csi").map(String::as_str),
        Some("none")
    );
}

#[test]
fn csi_commands_still_reject_stray_arguments() {
    for args in [
        vec!["deploy", "hivekeep-system", "nfs"],
        vec!["setup", "csi", "nfs", "hostpath"],
    ] {
        assert!(
            try_parse(&args).is_err(),
            "{args:?} accepted a stray argument"
        );
    }
}

/// An upgrade or rollback moves the control plane one component at a time,
/// so the components `deploy hivekeep-system` installs have their own
/// subcommands.
#[test]
fn component_deploy_subcommands() {
    let r = root();
    for (arg, want) in [
        ("podcertificate-controller", "podcertificate-controller"),
        ("podcert", "podcertificate-controller"),
        ("sandboxconfig", "sandboxconfig"),
        ("apiserver", "api-server"),
        ("controller", "hivekeep-controller"),
    ] {
        let path = find(&r, &["deploy", arg]).unwrap_or_else(|| panic!("deploy {arg}"));
        assert_eq!(path.last().map(String::as_str), Some(want), "deploy {arg}");
        assert!(
            try_parse(&["deploy", arg, "extra"]).is_err(),
            "deploy {arg} accepted a stray argument"
        );
    }
}

/// Resolves a typed path in the tree and checks it names itself.
fn canonical(words: &str) -> String {
    let w: Vec<&str> = words.split_whitespace().collect();
    let path = find(&root(), &w)
        .unwrap_or_else(|| panic!("no command {words:?}"))
        .join(" ");
    assert_eq!(path, words);
    path
}

#[test]
fn only_system_deploys_record_their_run() {
    for (path, want) in [
        ("deploy hivekeep-system", true),
        ("deploy gate", true),
        ("deploy api-server", true),
        ("deploy benchmarks", false),
        ("deploy demo counter", false),
        ("delete hivekeep-system", false),
        ("delete benchmarks", false),
        ("setup csi", false),
        ("publish", false),
    ] {
        assert_eq!(records_run(&canonical(path)), want, "{path}");
    }
}

#[test]
fn only_publish_skips_the_cluster() {
    for (path, want) in [
        ("publish release-images", false),
        ("publish release-binaries", false),
        ("publish worker-images", false),
        ("deploy hivekeep-system", true),
        ("delete hivekeep-system", true),
        ("setup csi", true),
    ] {
        assert_eq!(needs_cluster(&canonical(path)), want, "{path}");
    }
}

/// The merged registry -- installer settings plus every command's own --
/// is valid once the whole tree is built.
#[test]
fn the_full_registry_validates() {
    let _ = root();
    config::validate().unwrap();
}

/// Every command-scoped setting names a command that exists and is bound
/// there as a flag; a misspelt path would silently drop the flag.
#[test]
fn scoped_settings_are_bound_on_existing_commands() {
    let r = root();
    let paths = all_paths(&r);
    for s in config::all() {
        for c in &s.commands {
            assert!(
                paths.contains(c),
                "{} names command {c:?}, which does not exist",
                s.key
            );
            let words: Vec<&str> = c.split_whitespace().collect();
            let mut cmd = &r;
            for w in &words {
                cmd = cmd.find_subcommand(w).unwrap();
            }
            assert!(
                cmd.get_arguments().any(|a| a.get_id().as_str() == s.flag),
                "{} is not bound as --{} on {c:?}",
                s.key,
                s.flag
            );
        }
    }
}

/// Every demo has a deploy and a delete command.
#[test]
fn every_demo_has_commands() {
    let r = root();
    for d in crate::demos::all() {
        let arg = crate::demos::demo_arg(d.as_ref());
        assert!(
            find(&r, &["deploy", "demo", arg]).is_some(),
            "deploy demo {arg}"
        );
        assert!(
            find(&r, &["delete", "demo", arg]).is_some(),
            "delete demo {arg}"
        );
    }
}

/// Help needs neither configuration nor a cluster.
#[tokio::test]
async fn help_needs_no_cluster() {
    execute(
        [
            "hivekeep-setup",
            "--kind",
            "deploy",
            "demo",
            "counter",
            "--help",
        ],
        CancellationToken::new(),
    )
    .await
    .unwrap();
}
