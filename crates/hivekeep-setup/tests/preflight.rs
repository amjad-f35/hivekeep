//! The installer's preflight behaviour, observed by running the real binary
//! against stub tools and a stub API server: Kind prerequisites, the
//! control-plane check before demos and benchmarks, and help needing no
//! cluster.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CONTROLLER_GET: &str =
    "GET /apis/apps/v1/namespaces/hivekeep-system/deployments/hivekeep-controller";

/// Discovery of apps/v1, which the client consults before addressing a
/// Deployment.
const APPS_V1: &str = r#"{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"apps/v1","resources":[{"name":"deployments","singularName":"deployment","namespaced":true,"kind":"Deployment","verbs":["get","list"]}]}"#;

struct Run {
    out: String,
    code: i32,
}

/// Runs the installer with `docker` stubbed to exit with `docker_status`.
fn run_installer(kubeconfig: &Path, docker_status: &str, args: &[&str]) -> Run {
    let bin = tempfile::tempdir().unwrap();
    let docker = bin.path().join("docker");
    std::fs::write(&docker, "#!/bin/sh\nexit \"$PREFLIGHT_DOCKER_STATUS\"\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&docker, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let home = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_hivekeep-setup"))
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env_clear()
        .env(
            "PATH",
            format!(
                "{}:{}",
                bin.path().display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .env("HOME", home.path())
        .env("KUBECONFIG", kubeconfig)
        .env("NO_DEV_ENV", "true")
        .env("HK_NO_REPORT_DEFAULTS", "1")
        .env("HK_RECORD_DIR", home.path().join("records"))
        .env("PREFLIGHT_DOCKER_STATUS", docker_status)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // The installer must fail promptly rather than retry into a timeout.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("installer did not finish promptly");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let out = child.wait_with_output().unwrap();
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    Run {
        out: text,
        code: out.status.code().unwrap_or(-1),
    }
}

/// A kubeconfig with no contexts, or one pointing kind-kind at `server`.
fn kubeconfig(dir: &Path, server: Option<&str>) -> PathBuf {
    let path = dir.join("config");
    let body = match server {
        None => "apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []\n".to_string(),
        Some(s) => format!(
            "apiVersion: v1\nkind: Config\nclusters:\n- name: local\n  cluster:\n    server: {s}\ncontexts:\n- name: kind-kind\n  context:\n    cluster: local\n    user: local\nusers:\n- name: local\n  user: {{}}\ncurrent-context: kind-kind\n"
        ),
    };
    std::fs::write(&path, body).unwrap();
    path
}

type Responder = dyn Fn(&str, &str) -> (u16, String) + Send + Sync;

/// A one-request-per-connection HTTP/1.1 server recording `METHOD path`.
struct Server {
    url: String,
    requests: Arc<Mutex<Vec<String>>>,
}

fn serve(respond: Box<Responder>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let log = requests.clone();
    let respond: Arc<Responder> = Arc::from(respond);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let log = log.clone();
            let respond = respond.clone();
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                if reader.read_line(&mut line).is_err() || line.is_empty() {
                    return;
                }
                let mut parts = line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts
                    .next()
                    .unwrap_or("")
                    .split('?')
                    .next()
                    .unwrap_or("")
                    .to_string();
                let mut len = 0usize;
                loop {
                    let mut h = String::new();
                    if reader.read_line(&mut h).is_err() || h == "\r\n" || h.is_empty() {
                        break;
                    }
                    if let Some(v) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0; len];
                let _ = reader.read_exact(&mut body);
                log.lock().unwrap().push(format!("{method} {path}"));
                let (code, text) = respond(&method, &path);
                let _ = write!(
                    stream,
                    "HTTP/1.1 {code} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                    text.len()
                );
            });
        }
    });
    Server { url, requests }
}

fn is_discovery(request: &str) -> bool {
    let path = request.split_whitespace().nth(1).unwrap_or("");
    path == "/api"
        || path == "/apis"
        || path.starts_with("/api/v1") && path.matches('/').count() == 2
        || {
            let segs: Vec<&str> = path.trim_start_matches('/').split('/').collect();
            segs.first() == Some(&"apis") && segs.len() <= 3
        }
}

fn status(code: u16, reason: &str, message: &str) -> (u16, String) {
    (
        code,
        format!(
            r#"{{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"{reason}","code":{code},"message":"{message}"}}"#
        ),
    )
}

#[test]
fn kind_installer_missing_context() {
    let d = tempfile::tempdir().unwrap();
    let r = run_installer(
        &kubeconfig(d.path(), None),
        "0",
        &["--kind", "deploy", "demo", "counter"],
    );
    assert!(
        r.code != 0 && r.out.contains("kind create cluster") && r.out.contains("export kubeconfig"),
        "exit={}, want missing-context recovery guidance\n{}",
        r.code,
        r.out
    );
}

#[test]
fn kind_installer_docker_unavailable() {
    let d = tempfile::tempdir().unwrap();
    let r = run_installer(
        &kubeconfig(d.path(), Some("http://127.0.0.1:1")),
        "1",
        &["--kind", "deploy", "demo", "counter"],
    );
    assert!(
        r.code != 0 && r.out.contains("Docker") && r.out.contains("docker info"),
        "exit={}, want Docker readiness guidance\n{}",
        r.code,
        r.out
    );
}

#[test]
fn installer_requires_control_plane_before_demo_or_benchmark() {
    for args in [
        vec!["deploy", "demo", "counter"],
        vec!["deploy", "benchmarks"],
    ] {
        let server = serve(Box::new(|_, path| {
            if path == "/apis/apps/v1" {
                return (200, APPS_V1.to_string());
            }
            status(
                404,
                "NotFound",
                "deployments.apps hivekeep-controller not found",
            )
        }));
        let d = tempfile::tempdir().unwrap();
        let r = run_installer(&kubeconfig(d.path(), Some(&server.url)), "0", &args);
        assert!(
            r.code != 0
                && r.out.contains("deploy hivekeep-system")
                && r.out.contains("credential-provider"),
            "{args:?}: exit={}, want control-plane install guidance\n{}",
            r.code,
            r.out
        );
        let requests: Vec<String> = server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| !is_discovery(r))
            .cloned()
            .collect();
        assert_eq!(
            requests,
            vec![CONTROLLER_GET.to_string()],
            "{args:?}: only the check before refusing"
        );
    }
}

#[test]
fn kind_installer_help_needs_no_cluster() {
    let d = tempfile::tempdir().unwrap();
    let r = run_installer(
        &kubeconfig(d.path(), None),
        "1",
        &["--kind", "deploy", "demo", "counter", "--help"],
    );
    assert!(
        r.code == 0 && r.out.contains("Usage:"),
        "exit={}, want help\n{}",
        r.code,
        r.out
    );
}

#[test]
fn installer_control_plane_check_preserves_errors_and_allows_installed_system() {
    for installed in [false, true] {
        let server = serve(Box::new(move |_, path| {
            if path == "/apis/apps/v1" {
                return (200, APPS_V1.to_string());
            }
            if installed && path.ends_with("/deployments/hivekeep-controller") {
                return (
                    200,
                    r#"{"kind":"Deployment","apiVersion":"apps/v1","metadata":{"name":"hivekeep-controller","namespace":"hivekeep-system"}}"#.into(),
                );
            }
            status(403, "Forbidden", "fixture access denied")
        }));
        let d = tempfile::tempdir().unwrap();
        let r = run_installer(
            &kubeconfig(d.path(), Some(&server.url)),
            "0",
            &["deploy", "demo", "counter"],
        );
        let want = if installed {
            "demo-counter_deploy"
        } else {
            "fixture access denied"
        };
        assert!(
            r.code != 0 && r.out.contains(want) && !r.out.contains("Deploy the control plane first"),
            "installed={installed}: exit={}, want the original API error without install guidance\n{}",
            r.code,
            r.out
        );
        let all = server.requests.lock().unwrap().clone();
        let requests: Vec<&String> = all.iter().filter(|r| !is_discovery(r)).collect();
        assert_eq!(
            requests.first().map(|s| s.as_str()),
            Some(CONTROLLER_GET),
            "installed={installed}: {all:?}"
        );
        if !installed {
            assert_eq!(
                requests.len(),
                1,
                "a permission error must stop the install: {all:?}"
            );
        }
        for req in &all {
            assert!(req.starts_with("GET "), "unexpected mutation: {req}");
        }
    }
}

#[test]
fn kind_installer_uses_resolved_context_and_cluster_name() {
    let d = tempfile::tempdir().unwrap();
    let doc = d.path().join("install.yaml");
    std::fs::write(
        &doc,
        "apiVersion: install.hivekeep.io/v1alpha1\nkind: HivekeepInstall\ncontext: selected-by-config\nkindCluster:\n  name: local-dev\n",
    )
    .unwrap();
    let r = run_installer(
        &kubeconfig(d.path(), Some("http://127.0.0.1:1")),
        "0",
        &[
            "--kind",
            "--config",
            doc.to_str().unwrap(),
            "deploy",
            "demo",
            "counter",
        ],
    );
    for want in [
        r#"context "selected-by-config""#,
        r#"export kubeconfig --name="local-dev""#,
        r#"kind create cluster --name="local-dev""#,
    ] {
        assert!(
            r.code != 0 && r.out.contains(want),
            "exit={}, want {want:?}\n{}",
            r.code,
            r.out
        );
    }
}

#[test]
fn kind_installer_missing_or_malformed_kubeconfig() {
    for malformed in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("config");
        let mut want = "kind create cluster";
        if malformed {
            std::fs::write(&path, "contexts: [\n").unwrap();
            want = "while reading kubeconfig";
        }
        let r = run_installer(&path, "0", &["--kind", "deploy", "demo", "counter"]);
        assert!(
            r.code != 0 && r.out.contains(want),
            "malformed={malformed}: exit={}, want {want:?}\n{}",
            r.code,
            r.out
        );
        if malformed {
            assert!(
                !r.out.contains("kind create cluster"),
                "malformed kubeconfig misreported as missing\n{}",
                r.out
            );
        }
    }
}
