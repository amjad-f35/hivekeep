//! Controller mutual TLS: handshakes against a fake driver, configuration
//! resolution, and trust bundle rotation.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tonic::Status;

use super::driver_config::{CsiDriverConfig, CsiDriverTls, StaticCsiDriverConfigs};
use super::fake::{FakeCsiDriver, FakeCsiServer, FAKE_DRIVER_NAME};
use super::open::{open_csi_plugin_with, OpenOptions, Role};
use super::plugin::CsiPlugin;
use super::test_certs::{write_creds, TestCa};
use super::tls::{resolve_tls_config, ControllerTls, TlsPaths};

fn driver_config(endpoint: &str, tls: Option<CsiDriverTls>) -> CsiDriverConfig {
    CsiDriverConfig {
        driver_name: FAKE_DRIVER_NAME.into(),
        controller_endpoint: endpoint.into(),
        node_socket_override: String::new(),
        tls,
    }
}

fn pod_identity_tls(server_name: &str) -> CsiDriverTls {
    CsiDriverTls {
        enabled: true,
        use_pod_identity: true,
        server_name: server_name.into(),
    }
}

async fn start_tls_server(server_ca: &TestCa, dns_name: &str, client_ca: &TestCa) -> FakeCsiServer {
    FakeCsiDriver::new()
        .serve_tls(server_ca.server_config(dns_name, client_ca))
        .await
        .unwrap()
}

/// Opens the controller plugin at `endpoint`; opening makes a
/// GetPluginInfo call, so success means the handshake and a call worked.
async fn dial_plugin(
    endpoint: &str,
    server_name: &str,
    paths: &TlsPaths,
) -> Result<CsiPlugin, Status> {
    let getter = StaticCsiDriverConfigs(vec![driver_config(
        endpoint,
        Some(pod_identity_tls(server_name)),
    )]);
    open_csi_plugin_with(
        &getter,
        FAKE_DRIVER_NAME,
        Role::Controller,
        OpenOptions {
            tls_paths: Some(paths.clone()),
            ..Default::default()
        },
    )
    .await
}

#[tokio::test]
async fn mtls_succeeds() {
    let ca = TestCa::new();
    let server = start_tls_server(&ca, "localhost", &ca).await;
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca.client_bundle(), &ca.cert_pem());
    dial_plugin(&server.endpoint, "localhost", &paths)
        .await
        .unwrap();
}

#[tokio::test]
async fn mtls_rejects_untrusted_server_ca() {
    let (server_ca, client_ca) = (TestCa::new(), TestCa::new());
    let server = start_tls_server(&server_ca, "localhost", &client_ca).await;
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(
        dir.path(),
        &client_ca.client_bundle(),
        &client_ca.cert_pem(),
    );
    assert!(
        dial_plugin(&server.endpoint, "localhost", &paths)
            .await
            .is_err(),
        "accepted a server certificate from an untrusted CA"
    );
}

#[tokio::test]
async fn mtls_rejects_server_name_mismatch() {
    let ca = TestCa::new();
    let server = start_tls_server(&ca, "localhost", &ca).await;
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca.client_bundle(), &ca.cert_pem());
    assert!(
        dial_plugin(&server.endpoint, "wrong.example", &paths)
            .await
            .is_err(),
        "accepted a server certificate issued for another name"
    );
}

#[tokio::test]
async fn mtls_rejects_untrusted_client_cert() {
    // The server only accepts clients of another CA: proves the connection
    // is mutually authenticated, not one-way TLS.
    let (ca, other_ca) = (TestCa::new(), TestCa::new());
    let server = start_tls_server(&ca, "localhost", &other_ca).await;
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca.client_bundle(), &ca.cert_pem());
    assert!(
        dial_plugin(&server.endpoint, "localhost", &paths)
            .await
            .is_err(),
        "succeeded with a client certificate the server should reject"
    );
}

#[test]
fn resolve_tls_config_disabled() {
    let cases = [
        ("nil config", None),
        ("no TLS block", Some(driver_config("", None))),
        (
            "TLS disabled",
            Some(driver_config(
                "",
                Some(CsiDriverTls {
                    enabled: false,
                    ..Default::default()
                }),
            )),
        ),
    ];
    for (name, cfg) in cases {
        let got = resolve_tls_config(cfg.as_ref(), &TlsPaths::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(got.is_none(), "{name}: want no TLS");
    }
}

#[test]
fn resolve_tls_config_rejects_manual_certs() {
    let cfg = driver_config(
        "",
        Some(CsiDriverTls {
            enabled: true,
            use_pod_identity: false,
            server_name: "x".into(),
        }),
    );
    assert!(resolve_tls_config(Some(&cfg), &TlsPaths::default()).is_err());
}

#[test]
fn resolve_tls_config_rejects_empty_server_name() {
    let cfg = driver_config("", Some(pod_identity_tls("")));
    let err = resolve_tls_config(Some(&cfg), &TlsPaths::default()).unwrap_err();
    assert!(
        err.contains("tls.serverName is required when TLS is enabled"),
        "{err}"
    );
}

#[test]
fn resolve_tls_config_fields() {
    const SERVER_NAME: &str = "my-service.default.svc";
    let ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca.client_bundle(), &ca.cert_pem());
    let got: ControllerTls = resolve_tls_config(
        Some(&driver_config("", Some(pod_identity_tls(SERVER_NAME)))),
        &paths,
    )
    .unwrap()
    .unwrap();
    assert_eq!(got.server_name(), SERVER_NAME);
    assert_eq!(ControllerTls::MIN_VERSION, rustls::ProtocolVersion::TLSv1_3);
    let cfg = got.client_config().unwrap();
    assert_eq!(cfg.alpn_protocols, vec![b"h2".to_vec()]);
    // The credential bundle loader stands behind the client certificate.
    assert!(cfg.client_auth_cert_resolver.has_certs());
    assert_eq!(got.ca_cache().path(), paths.ca_cert);
    // A missing trust bundle fails resolution up front.
    let missing = TlsPaths {
        ca_cert: dir.path().join("missing.pem"),
        ..paths
    };
    assert!(resolve_tls_config(
        Some(&driver_config("", Some(pod_identity_tls(SERVER_NAME)))),
        &missing
    )
    .is_err());
}

#[tokio::test]
async fn mtls_picks_up_ca_rotation() {
    let (ca1, ca2) = (TestCa::new(), TestCa::new());
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca1.client_bundle(), &ca1.cert_pem());
    let server1 = start_tls_server(&ca1, "localhost", &ca1).await;
    dial_plugin(&server1.endpoint, "localhost", &paths)
        .await
        .expect("connection with the initial CA");

    let server2 = start_tls_server(&ca2, "localhost", &ca2).await;
    std::fs::write(&paths.ca_cert, ca2.cert_pem()).unwrap();
    std::fs::write(&paths.client_cert, ca2.client_bundle()).unwrap();
    dial_plugin(&server2.endpoint, "localhost", &paths)
        .await
        .expect("connection after CA rotation");
}

#[tokio::test]
async fn one_plugin_follows_rotation_between_connections() {
    // The same plugin (and TLS config) reconnects after the driver and the
    // files on disk move to a new CA. A forwarding proxy keeps the
    // driver's address fixed while the backend changes.
    let (ca1, ca2) = (TestCa::new(), TestCa::new());
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca1.client_bundle(), &ca1.cert_pem());
    let tls = resolve_tls_config(
        Some(&driver_config("", Some(pod_identity_tls("localhost")))),
        &paths,
    )
    .unwrap()
    .unwrap();
    let driver = FakeCsiDriver::new();
    let server1 = driver
        .serve_tls(ca1.server_config("localhost", &ca1))
        .await
        .unwrap();
    let backend = Arc::new(parking_lot::Mutex::new(
        server1.endpoint.trim_start_matches("tcp://").to_string(),
    ));
    let proxy = forward(backend.clone()).await;
    let plugin = CsiPlugin::new(
        super::client::CsiClient::new(&format!("tcp://{proxy}"), Some(&tls)).unwrap(),
    );
    assert_eq!(plugin.driver_name().await.unwrap(), FAKE_DRIVER_NAME);
    drop(server1);

    let server2 = driver
        .serve_tls(ca2.server_config("localhost", &ca2))
        .await
        .unwrap();
    *backend.lock() = server2.endpoint.trim_start_matches("tcp://").to_string();
    std::fs::write(&paths.ca_cert, ca2.cert_pem()).unwrap();
    std::fs::write(&paths.client_cert, ca2.client_bundle()).unwrap();
    bump_mtime(&paths.ca_cert);
    bump_mtime(&paths.client_cert);
    let mut ok = false;
    for _ in 0..100 {
        if plugin.driver_name().await.is_ok() {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        ok,
        "the plugin never reconnected with the rotated credentials"
    );
}

/// A loopback TCP proxy to whatever `backend` holds at connect time.
async fn forward(backend: Arc<parking_lot::Mutex<String>>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut a, _)) = listener.accept().await {
            let target = backend.lock().clone();
            tokio::spawn(async move {
                if let Ok(mut b) = tokio::net::TcpStream::connect(&target).await {
                    let _ = tokio::io::copy_bidirectional(&mut a, &mut b).await;
                }
            });
        }
    });
    addr
}

fn bump_mtime(path: &std::path::Path) {
    let f = std::fs::File::options().write(true).open(path).unwrap();
    f.set_modified(SystemTime::now() + Duration::from_secs(2))
        .unwrap();
}

#[test]
fn ca_pool_cache_hit_and_file_change() {
    let ca = TestCa::new();
    let dir = tempfile::tempdir().unwrap();
    let paths = write_creds(dir.path(), &ca.client_bundle(), &ca.cert_pem());
    let tls = resolve_tls_config(
        Some(&driver_config("", Some(pod_identity_tls("x")))),
        &paths,
    )
    .unwrap()
    .unwrap();
    let cache = tls.ca_cache();
    let pool1 = cache.get().unwrap();
    let pool2 = cache.get().unwrap();
    assert!(
        Arc::ptr_eq(&pool1, &pool2),
        "expected the cached pool on an unchanged file"
    );

    std::fs::write(&paths.ca_cert, ca.cert_pem()).unwrap();
    // The cache keys on mtime and size; make the change observable on
    // filesystems with coarse timestamps.
    bump_mtime(&paths.ca_cert);
    let pool3 = cache.get().unwrap();
    assert!(
        !Arc::ptr_eq(&pool1, &pool3),
        "expected a new pool after the file changed"
    );
}
