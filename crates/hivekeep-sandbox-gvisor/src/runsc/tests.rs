use super::*;
use crate::flags::{parse_flag_config, FlagConfig};
use parking_lot::Mutex;

const TEST_FLAGS: &str = r#"{
  "globalFlags": ["-global1", "-global2"],
  "commands": {
    "restore": {
      "globalFlags": ["-restore-global"],
      "subcommandFlags": ["-restore-sub1", "-restore-sub2"]
    },
    "start": { "globalFlags": ["-start-global"] },
    "list": { "subcommandFlags": ["-list-sub"] }
  }
}"#;

fn test_dirs() -> pb::ActorDirs {
    pb::ActorDirs {
        root_dir: "/node/actors/test-actor-123".into(),
        oci_bundle_dir: "/node/actors/test-actor-123/bundle".into(),
        ..Default::default()
    }
}

fn cmd_with(flags: FlagConfig) -> RunscCommand {
    RunscCommand::bare(
        "/usr/bin/runsc",
        "test-actor-123",
        &test_dirs(),
        FlagStore::with_config(flags),
    )
}

fn cmd() -> RunscCommand {
    cmd_with(parse_flag_config(TEST_FLAGS.as_bytes()).unwrap())
}

fn v(s: &[&str]) -> Vec<String> {
    s.iter().map(|s| s.to_string()).collect()
}

const ROOT: &str = "/node/actors/test-actor-123/runsc-state";

#[test]
fn build_args() {
    let c = cmd();
    let cases: Vec<(&str, &str, Vec<String>, Vec<String>)> = vec![
        (
            "kill",
            "",
            v(&["my-container", "SIGTERM"]),
            v(&[
                "-global1",
                "-global2",
                "-root",
                ROOT,
                "kill",
                "my-container",
                "SIGTERM",
            ]),
        ),
        (
            "wait",
            "my-container",
            vec![],
            v(&[
                "-global1",
                "-global2",
                "-root",
                ROOT,
                "wait",
                "my-container",
            ]),
        ),
        (
            "wait",
            "my-container",
            v(&["-restore"]),
            v(&[
                "-global1",
                "-global2",
                "-root",
                ROOT,
                "wait",
                "-restore",
                "my-container",
            ]),
        ),
        (
            "wait",
            "my-container",
            v(&["-fsrestore"]),
            v(&[
                "-global1",
                "-global2",
                "-root",
                ROOT,
                "wait",
                "-fsrestore",
                "my-container",
            ]),
        ),
        (
            "list",
            "",
            vec![],
            v(&["-global1", "-global2", "-root", ROOT, "list", "-list-sub"]),
        ),
        (
            "start",
            "app",
            vec![],
            v(&[
                "-global1",
                "-global2",
                "-start-global",
                "-root",
                ROOT,
                "start",
                "app",
            ]),
        ),
        (
            "restore",
            "app",
            v(&["-bundle", "/bundle"]),
            v(&[
                "-global1",
                "-global2",
                "-restore-global",
                "-root",
                ROOT,
                "restore",
                "-restore-sub1",
                "-restore-sub2",
                "-bundle",
                "/bundle",
                "app",
            ]),
        ),
    ];
    for (command, container, extra, want) in cases {
        assert_eq!(
            c.build_args(command, container, &extra),
            want,
            "{command} {extra:?}"
        );
    }
}

#[test]
fn build_args_with_no_flags() {
    let c = cmd_with(FlagConfig::default());
    assert_eq!(
        c.build_args("pause", "contId", &[]),
        v(&["-root", ROOT, "pause", "contId"])
    );
}

#[test]
fn checkpoint_args() {
    let dir = "/node/actors/test-actor-123/checkpoints/snap-1";
    let fs = v(&["rootfs/=app:/", "fs/=app:/var/data"]);
    assert_eq!(
        cmd().checkpoint_args(SANDBOX_ID, dir, &fs),
        v(&[
            "-global1",
            "-global2",
            "-root",
            ROOT,
            "checkpoint",
            "-image-path",
            dir,
            "-fs-checkpoint-paths",
            "rootfs/=app:/",
            "-fs-checkpoint-paths",
            "fs/=app:/var/data",
            SANDBOX_ID,
        ])
    );
    assert_eq!(
        cmd().fs_checkpoint_args(SANDBOX_ID, dir, &fs),
        v(&[
            "-global1",
            "-global2",
            "-root",
            ROOT,
            "fscheckpoint",
            "-image-path",
            dir,
            "-path",
            "rootfs/=app:/",
            "-path",
            "fs/=app:/var/data",
            SANDBOX_ID,
        ])
    );
}

/// Only the sandbox gets -no-root-container: runsc refuses it otherwise.
#[test]
fn create_args() {
    let bundle = "/node/actors/test-actor-123/bundle/";
    let pids = "/node/actors/test-actor-123/pidfiles/";
    assert_eq!(
        cmd().create_args(SANDBOX_ID, &v(&["-fs-restore-image-path", "/snap/fs"])),
        v(&[
            "-global1",
            "-global2",
            "-root",
            ROOT,
            "create",
            "-no-root-container",
            "-bundle",
            &format!("{bundle}{SANDBOX_ID}"),
            "-pid-file",
            &format!("{pids}{SANDBOX_ID}.pid"),
            "-fs-restore-image-path",
            "/snap/fs",
            SANDBOX_ID,
        ])
    );
    assert_eq!(
        cmd().create_args("app", &[]),
        v(&[
            "-global1",
            "-global2",
            "-root",
            ROOT,
            "create",
            "-bundle",
            &format!("{bundle}app"),
            "-pid-file",
            &format!("{pids}app.pid"),
            "app",
        ])
    );
}

#[test]
fn restore_args() {
    let dir = "/node/actors/test-actor-123/checkpoints/snap-1";
    let bundle = "/node/actors/test-actor-123/bundle/";
    let pids = "/node/actors/test-actor-123/pidfiles/";
    let head = [
        "-global1",
        "-global2",
        "-restore-global",
        "-root",
        ROOT,
        "restore",
        "-restore-sub1",
        "-restore-sub2",
    ];
    let mut want = v(&head);
    want.extend(v(&[
        "-no-root-container",
        "-bundle",
        &format!("{bundle}{SANDBOX_ID}"),
        "-image-path",
        dir,
        "-pid-file",
        &format!("{pids}{SANDBOX_ID}.pid"),
        SANDBOX_ID,
    ]));
    assert_eq!(cmd().restore_args(SANDBOX_ID, dir), want);
    let mut want = v(&head);
    want.extend(v(&[
        "-bundle",
        &format!("{bundle}app"),
        "-image-path",
        dir,
        "-pid-file",
        &format!("{pids}app.pid"),
        "app",
    ]));
    assert_eq!(cmd().restore_args("app", dir), want);
}

/// write_spec builds a container's spec from the request and a sandbox spec
/// with no process or rootfs, making only the sandbox's bundle.
#[test]
fn write_spec() {
    let root = tempfile::tempdir().unwrap();
    let dirs = pb::ActorDirs {
        root_dir: root.path().display().to_string(),
        oci_bundle_dir: root.path().join("bundles").display().to_string(),
        ..Default::default()
    };
    let mut c = RunscCommand::bare("", "uid-a", &dirs, FlagStore::empty());
    c.containers = vec![pb::Container {
        name: "app".into(),
        container_spec: Some(pb::ContainerSpec {
            args: v(&["/app", "serve"]),
            ..Default::default()
        }),
        ..Default::default()
    }];
    assert!(
        c.write_spec("app").is_err(),
        "the keeper makes the application bundle"
    );
    std::fs::create_dir_all(oci_bundle_path(&dirs, "app")).unwrap();
    for name in ["app", SANDBOX_ID] {
        c.write_spec(name).unwrap();
        let b = std::fs::read(oci_bundle_path(&dirs, name).join("config.json")).unwrap();
        let spec: oci::Spec = serde_json::from_slice(&b).unwrap();
        assert_eq!(spec.annotations["io.kubernetes.cri.container-name"], name);
        if name == SANDBOX_ID {
            assert!(spec.process.is_none() && spec.root.is_none() && spec.mounts.is_none());
        } else {
            assert_eq!(spec.process.unwrap().args, v(&["/app", "serve"]));
        }
    }
    assert!(c.write_spec("missing").is_err());
}

#[test]
fn durable_volume_names_are_sorted_and_unique() {
    let m = |n: &str| pb::VolumeMount {
        name: n.into(),
        mount_path: format!("/{n}"),
    };
    let spec = pb::WorkloadSpec {
        containers: vec![
            pb::Container {
                durable_dir_volume_mounts: vec![m("b"), m("a")],
                ..Default::default()
            },
            pb::Container {
                durable_dir_volume_mounts: vec![m("a")],
                ..Default::default()
            },
        ],
    };
    assert_eq!(durable_volume_names(Some(&spec)), v(&["a", "b"]));
    assert!(durable_volume_names(None).is_empty());
}

/// Records argv instead of running anything.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<Vec<String>>>,
    list_output: Vec<u8>,
}

#[async_trait]
impl CommandRunner for Recorder {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        _: &Output,
        _: bool,
        _: &CancellationToken,
    ) -> Result<Vec<u8>> {
        let mut call = vec![program.to_string()];
        call.extend(args.iter().cloned());
        self.calls.lock().push(call);
        Ok(self.list_output.clone())
    }
}

#[tokio::test]
async fn runtime_runs_the_built_argv() {
    let rec = Arc::new(Recorder {
        list_output: b"_sandbox\napp\n".to_vec(),
        ..Default::default()
    });
    let rt = Runsc::new(cmd(), rec.clone());
    let cancel = CancellationToken::new();
    rt.kill(&cancel, "app", "SIGTERM").await.unwrap();
    assert_eq!(rt.list(&cancel).await.unwrap(), v(&["_sandbox", "app"]));
    rt.wait_restore(&cancel, SANDBOX_ID).await.unwrap();
    let calls = rec.calls.lock().clone();
    assert_eq!(
        calls[0],
        v(&[
            "/usr/bin/runsc",
            "-global1",
            "-global2",
            "-root",
            ROOT,
            "kill",
            "app",
            "SIGTERM"
        ])
    );
    assert_eq!(calls[1].last().unwrap(), "-list-sub");
    assert_eq!(calls[2][calls[2].len() - 2..], v(&["-restore", SANDBOX_ID]));
}

#[tokio::test]
async fn process_runner_reports_failures_and_cancellation() {
    let cancel = CancellationToken::new();
    let r = ProcessRunner;
    let out = r
        .run(
            "sh",
            &v(&["-c", "echo hi"]),
            &Output::Inherit,
            true,
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(out, b"hi\n");
    assert!(r
        .run(
            "sh",
            &v(&["-c", "exit 3"]),
            &Output::Inherit,
            false,
            &cancel
        )
        .await
        .is_err());
    let c2 = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        c2.cancel();
    });
    let err = r
        .run("sleep", &v(&["10"]), &Output::Inherit, false, &cancel)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Cancelled), "{err}");
}

/// runsc CLI output is logged as the Actor's sandbox: a JSON line keeps its
/// level, message and time, a plain line is logged verbatim, and neither
/// can forge the attribution.
#[tokio::test(flavor = "current_thread")]
async fn sandbox_log_attribution() {
    use hivekeep_core::resource::ActorRef;

    let (logs, _g) = crate::test_log::capture();
    let attribution = ActorAttribution {
        actor: ActorRef::new("default", "act-1"),
        uid: "uid-1".into(),
        template_hive: "tmpl-ns".into(),
        template_name: "tmpl-1".into(),
    };
    const JSON_MESSAGE: &str = "container.go:123] cannot start container";
    const LOG_TIME: &str = "2026-10-08T03:40:00Z";
    let json_line = format!(
        r#"{{"msg":"{JSON_MESSAGE}","level":"warning","time":"{LOG_TIME}","actor":"forged","hk.actor.name":"forged"}}"#
    );
    const PLAIN_LINE: &str =
        "I1008 03:40:00.000000 1234 main.go:123] Args: [runsc --alsologtostderr create _sandbox]";
    let script = format!("printf '%s\\n' '{json_line}'; printf '%s\\n' '{PLAIN_LINE}' >&2");
    let out = Output::Actor {
        attribution,
        container: SANDBOX_ID.into(),
    };
    ProcessRunner
        .run(
            "sh",
            &v(&["-c", &script]),
            &out,
            false,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

    let json = logs.with_message(JSON_MESSAGE);
    assert_eq!(json.len(), 1, "{:?}", logs.records());
    assert_eq!(json[0].level, tracing::Level::WARN);
    assert_eq!(json[0].get("runsc_time"), LOG_TIME);
    let plain = logs.with_message(PLAIN_LINE);
    assert_eq!(plain.len(), 1, "{:?}", logs.records());
    assert_eq!(plain[0].level, tracing::Level::INFO);
    for r in [&json[0], &plain[0]] {
        assert_eq!(r.get("actor"), "act-1", "the forged value must not survive");
        assert_eq!(r.get("hive"), "default");
        assert_eq!(r.get("actor_uid"), "uid-1");
        assert_eq!(r.get("template_name"), "tmpl-1");
        assert_eq!(r.get("container"), SANDBOX_ID);
        assert!(!r.fields.contains_key("hk.actor.name"));
    }
}

#[test]
fn runsc_lines_parse() {
    let l = parse_runsc_line(r#"{"msg":"m","level":"error"}"#);
    assert_eq!(
        (l.level, l.message.as_str(), l.time),
        (tracing::Level::ERROR, "m", None)
    );
    // JSON without a message is a plain line.
    let l = parse_runsc_line(r#"{"level":"warning"}"#);
    assert_eq!(l.level, tracing::Level::INFO);
    assert_eq!(l.message, r#"{"level":"warning"}"#);
    assert_eq!(parse_runsc_line("plain").message, "plain");
}
