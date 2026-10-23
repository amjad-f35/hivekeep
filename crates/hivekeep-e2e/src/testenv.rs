//! A throwaway Kubernetes control plane (etcd plus kube-apiserver) for
//! tests that need a real API server but no nodes, with the Hivekeep CRDs
//! installed.
//!
//! The binaries come from `KUBEBUILDER_ASSETS` (a directory holding `etcd`
//! and `kube-apiserver`, as the kubebuilder test-asset tooling lays them out); tests
//! using this are `#[ignore]`d and skip themselves when it is unset.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{Api, PostParams};

/// Names the directory holding the control-plane binaries.
pub const ASSETS_ENV: &str = "KUBEBUILDER_ASSETS";
/// The CRDs installed into every environment, relative to the workspace
/// root.
pub const CRD_DIR: &str = "crates/hivekeep-setup/manifests/install/generated";

const ADMIN_TOKEN: &str = "hk-testenv-admin";

/// A running control plane; dropping it stops both processes and removes
/// their state.
pub struct TestEnv {
    /// A client authenticated as a cluster admin.
    pub client: kube::Client,
    /// The API server URL.
    pub url: String,
    etcd: Child,
    apiserver: Child,
    _dir: tempfile::TempDir,
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        for child in [&mut self.apiserver, &mut self.etcd] {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// The asset directory, if configured.
pub fn assets_dir() -> Option<PathBuf> {
    std::env::var_os(ASSETS_ENV)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

fn free_port() -> Result<u16, String> {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .map_err(|e| e.to_string())
}

fn write(path: &Path, data: &[u8]) -> Result<(), String> {
    std::fs::write(path, data).map_err(|e| format!("writing {}: {e}", path.display()))
}

/// The flags kube-apiserver runs with, given its state directory and ports.
pub fn apiserver_args(dir: &Path, etcd_url: &str, secure_port: u16) -> Vec<String> {
    let p = |f: &str| dir.join(f).to_string_lossy().into_owned();
    vec![
        format!("--etcd-servers={etcd_url}"),
        "--bind-address=127.0.0.1".into(),
        format!("--secure-port={secure_port}"),
        format!("--cert-dir={}", p("certs")),
        format!("--tls-cert-file={}", p("serving.crt")),
        format!("--tls-private-key-file={}", p("serving.key")),
        format!("--token-auth-file={}", p("tokens.csv")),
        format!("--service-account-key-file={}", p("sa.pub")),
        format!("--service-account-signing-key-file={}", p("sa.key")),
        "--service-account-issuer=https://kubernetes.default.svc".into(),
        "--service-cluster-ip-range=10.0.0.0/24".into(),
        "--authorization-mode=RBAC".into(),
        "--allow-privileged=true".into(),
    ]
}

/// Starts etcd and kube-apiserver from [`assets_dir`] and installs the
/// CRDs under [`CRD_DIR`].
pub async fn start() -> Result<TestEnv, String> {
    let assets = assets_dir().ok_or_else(|| format!("{ASSETS_ENV} is not set"))?;
    let dir = tempfile::tempdir().map_err(|e| e.to_string())?;
    let (client_port, peer_port, secure_port) = (free_port()?, free_port()?, free_port()?);
    let etcd_url = format!("http://127.0.0.1:{client_port}");
    let etcd = Command::new(assets.join("etcd"))
        .args([
            format!("--data-dir={}", dir.path().join("etcd").display()),
            format!("--listen-client-urls={etcd_url}"),
            format!("--advertise-client-urls={etcd_url}"),
            format!("--listen-peer-urls=http://127.0.0.1:{peer_port}"),
            "--unsafe-no-fsync=true".into(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("starting etcd: {e}"))?;

    let serving_key = rcgen::KeyPair::generate().map_err(|e| e.to_string())?;
    let mut params =
        rcgen::CertificateParams::new(vec!["localhost".to_string()]).map_err(|e| e.to_string())?;
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress("127.0.0.1".parse().expect("ip")));
    let serving = params
        .self_signed(&serving_key)
        .map_err(|e| e.to_string())?;
    write(&dir.path().join("serving.crt"), serving.pem().as_bytes())?;
    write(
        &dir.path().join("serving.key"),
        serving_key.serialize_pem().as_bytes(),
    )?;
    let sa_key = rcgen::KeyPair::generate().map_err(|e| e.to_string())?;
    write(
        &dir.path().join("sa.key"),
        sa_key.serialize_pem().as_bytes(),
    )?;
    write(
        &dir.path().join("sa.pub"),
        sa_key.public_key_pem().as_bytes(),
    )?;
    write(
        &dir.path().join("tokens.csv"),
        format!("{ADMIN_TOKEN},admin,admin,system:masters\n").as_bytes(),
    )?;

    let apiserver = Command::new(assets.join("kube-apiserver"))
        .args(apiserver_args(dir.path(), &etcd_url, secure_port))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("starting kube-apiserver: {e}"))?;

    let url = format!("https://127.0.0.1:{secure_port}");
    let mut config = kube::Config::new(url.parse().map_err(|e| format!("{e}"))?);
    config.root_cert = Some(vec![serving.der().to_vec()]);
    config.auth_info.token = Some(ADMIN_TOKEN.to_string().into());
    let client = kube::Client::try_from(config).map_err(|e| e.to_string())?;
    let mut env = TestEnv {
        client,
        url,
        etcd,
        apiserver,
        _dir: dir,
    };

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if env.client.apiserver_version().await.is_ok() {
            break;
        }
        if let Ok(Some(status)) = env.apiserver.try_wait() {
            return Err(format!("kube-apiserver exited early: {status}"));
        }
        if Instant::now() > deadline {
            return Err("kube-apiserver did not become ready within 60s".into());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    install_crds(&env.client).await?;
    Ok(env)
}

/// Creates every CustomResourceDefinition in [`CRD_DIR`].
pub async fn install_crds(client: &kube::Client) -> Result<(), String> {
    let root = crate::cmd::find_repo_root()?;
    let api: Api<CustomResourceDefinition> = Api::all(client.clone());
    let mut entries: Vec<_> = std::fs::read_dir(root.join(CRD_DIR))
        .map_err(|e| e.to_string())?
        .flatten()
        .map(|e| e.path())
        .collect();
    entries.sort();
    for path in entries {
        let text = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        for doc in crate::manifest::split_documents(&text) {
            let value: serde_json::Value =
                serde_yaml_ng::from_str(doc).map_err(|e| format!("{}: {e}", path.display()))?;
            if value.get("kind").and_then(|k| k.as_str()) != Some("CustomResourceDefinition") {
                continue;
            }
            let crd: CustomResourceDefinition =
                serde_json::from_value(value).map_err(|e| format!("{}: {e}", path.display()))?;
            api.create(&PostParams::default(), &crd)
                .await
                .map_err(|e| format!("installing {}: {e}", path.display()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crd_directory_holds_crds() {
        let root = crate::cmd::find_repo_root().unwrap();
        let n = std::fs::read_dir(root.join(CRD_DIR))
            .unwrap()
            .flatten()
            .filter(|e| {
                std::fs::read_to_string(e.path())
                    .unwrap_or_default()
                    .contains("kind: CustomResourceDefinition")
            })
            .count();
        assert!(n >= 3, "found {n} CRD files");
        let args = apiserver_args(Path::new("/s"), "http://127.0.0.1:1", 2);
        assert!(args.contains(&"--secure-port=2".to_string()));
        assert!(args.iter().any(|a| a == "--token-auth-file=/s/tokens.csv"));
    }

    #[tokio::test]
    #[ignore = "needs etcd and kube-apiserver binaries: set KUBEBUILDER_ASSETS"]
    async fn starts_with_crds() {
        if assets_dir().is_none() {
            eprintln!("skipping: {ASSETS_ENV} is not set");
            return;
        }
        let env = start().await.unwrap();
        let crds: Api<CustomResourceDefinition> = Api::all(env.client.clone());
        crds.get("workerpools.hivekeep.io").await.unwrap();
    }
}
