//! mTLS admission of the Keeper and WorkerSupport services, over real
//! handshakes with certificates shaped like the node's pod certificates
//! (SPIFFE URI SAN, client and server auth, pod identity extension).
//!
//! The fixtures under `testdata/tls` were issued by a throwaway test CA
//! (and a second, rogue one) and are valid for a century.

use hivekeep_proto::keeper::v1 as pb;
use hivekeep_proto::keeper::v1::keeper_client::KeeperClient;
use hivekeep_proto::keeper::v1::keeper_server::KeeperServer;
use hivekeep_proto::keeper::v1::worker_support_client::WorkerSupportClient;
use hivekeep_proto::keeper::v1::worker_support_server::WorkerSupportServer;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity, Server};
use tonic::Code;

use super::*;
use crate::service_stubs::{loopback, StubKeeper, StubWorkerSupport};

const CA: &str = include_str!("testdata/tls/ca.pem");
const KEEPER_NODE_A: &str = include_str!("testdata/tls/keeper_node_a.pem");
const API_NODE_B: &str = include_str!("testdata/tls/api_node_b.pem");
const TENANT_DEFAULT_NODE_A: &str = include_str!("testdata/tls/tenant_default_node_a.pem");
const TENANT_API_NODE_A: &str = include_str!("testdata/tls/tenant_api_node_a.pem");
const SYSTEM_GATE_ROUTER_NODE_A: &str = include_str!("testdata/tls/system_gate_router_node_a.pem");
const SYSTEM_KEEPER_NODE_B: &str = include_str!("testdata/tls/system_keeper_node_b.pem");
const API_NO_URI: &str = include_str!("testdata/tls/api_no_uri.pem");
const API_TWO_URIS: &str = include_str!("testdata/tls/api_two_uris.pem");
const ROGUE_API: &str = include_str!("testdata/tls/rogue_api.pem");
const WORKERS_DEFAULT_NODE_A: &str = include_str!("testdata/tls/workers_default_node_a.pem");
const WORKERS_DEFAULT_NODE_B: &str = include_str!("testdata/tls/workers_default_node_b.pem");

const API_ID: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-api";

/// A channel to `addr` verifying the keeper's certificate against the test
/// CA and presenting `client` (a key and certificate PEM), if any.
/// The keeper links two rustls providers, so the binary picks ring at
/// startup; tests do the same.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn channel(addr: std::net::SocketAddr, client: Option<&str>) -> Result<Channel, String> {
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(CA))
        .domain_name("keeper.test");
    if let Some(c) = client {
        tls = tls.identity(Identity::from_pem(c, c));
    }
    Endpoint::from_shared(format!("https://{addr}"))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
        .map_err(|e| format!("{e:?}"))
}

/// Serves the Keeper service the way the binary does, admitting only the
/// control plane's SPIFFE ID, and calls Run as `client`.
async fn call_keeper(client: Option<&str>) -> Result<(), tonic::Status> {
    let (incoming, addr) = loopback().await;
    let cfg = server_tls_config(KEEPER_NODE_A.as_bytes(), CA.as_bytes()).unwrap();
    let svc =
        KeeperServer::with_interceptor(StubKeeper::default(), require_spiffe_id(API_ID.into()));
    let server = tokio::spawn(
        Server::builder()
            .tls_config(cfg)
            .unwrap()
            .add_service(svc)
            .serve_with_incoming(incoming),
    );
    let res = match channel(addr, client).await {
        Ok(ch) => KeeperClient::new(ch)
            .run(pb::RunRequest::default())
            .await
            .map(|_| ()),
        Err(e) => Err(tonic::Status::unavailable(e)),
    };
    server.abort();
    res
}

/// Serves WorkerSupport the way the binary does, admitting only workers on
/// the keeper's node, and calls it as `client`.
async fn call_worker_support(client: &str) -> Result<(), tonic::Status> {
    let (incoming, addr) = loopback().await;
    let cfg = server_tls_config(KEEPER_NODE_A.as_bytes(), CA.as_bytes()).unwrap();
    let node = bundle_pod_identity(KEEPER_NODE_A.as_bytes(), DEFAULT_POD_IDENTITY_OID).unwrap();
    let svc = WorkerSupportServer::with_interceptor(
        StubWorkerSupport::default(),
        require_same_node(node, DEFAULT_POD_IDENTITY_OID.into()),
    );
    let server = tokio::spawn(
        Server::builder()
            .tls_config(cfg)
            .unwrap()
            .add_service(svc)
            .serve_with_incoming(incoming),
    );
    let ch = channel(addr, Some(client)).await.unwrap();
    let res = WorkerSupportClient::new(ch)
        .register_worker(pb::RegisterWorkerRequest::default())
        .await
        .map(|_| ());
    server.abort();
    res
}

fn want_refused(res: Result<(), tonic::Status>, code: Code, want: &str) {
    let err = res.expect_err("call admitted, want it refused");
    assert_eq!(err.code(), code, "{err:?}");
    assert!(
        err.message().contains(want),
        "error {:?} does not mention {want:?}",
        err.message()
    );
}

#[tokio::test]
async fn keeper_tls_admits_only_the_control_plane() {
    install_crypto_provider();
    call_keeper(Some(API_NODE_B))
        .await
        .expect("the control plane is admitted");

    // Any pod can get a certificate from the same CA; none of these may
    // drive actor lifecycles.
    for (name, client) in [
        ("workload pod in another namespace", TENANT_DEFAULT_NODE_A),
        (
            "control plane SA name in another namespace",
            TENANT_API_NODE_A,
        ),
        ("other system component", SYSTEM_GATE_ROUTER_NODE_A),
        ("keeper on another node", SYSTEM_KEEPER_NODE_B),
    ] {
        let err = call_keeper(Some(client)).await.expect_err(name);
        assert_eq!(err.code(), Code::PermissionDenied, "{name}: {err:?}");
        assert!(
            err.message().contains("is not authorized"),
            "{name}: {err:?}"
        );
    }

    // No URI SAN, and the control plane's ID plus a second one.
    want_refused(
        call_keeper(Some(API_NO_URI)).await,
        Code::PermissionDenied,
        "no valid SPIFFE ID",
    );
    want_refused(
        call_keeper(Some(API_TWO_URIS)).await,
        Code::PermissionDenied,
        "no valid SPIFFE ID",
    );

    // Without a client certificate the handshake itself fails, before any
    // SPIFFE check could run.
    let err = call_keeper(None)
        .await
        .expect_err("admitted without a certificate");
    assert_ne!(err.code(), Code::PermissionDenied, "{err:?}");
    assert_ne!(err.code(), Code::Ok);
}

#[tokio::test]
async fn keeper_tls_rejects_an_untrusted_ca() {
    install_crypto_provider();
    // The right SPIFFE ID from the wrong issuer: the chain check applies.
    let err = call_keeper(Some(ROGUE_API))
        .await
        .expect_err("a certificate from an untrusted CA was admitted");
    assert_ne!(
        err.code(),
        Code::PermissionDenied,
        "rejected after the handshake: {err:?}"
    );
}

#[tokio::test]
async fn worker_support_tls_enforces_same_node() {
    install_crypto_provider();
    call_worker_support(WORKERS_DEFAULT_NODE_A)
        .await
        .expect("a worker on the keeper's node is admitted");
    want_refused(
        call_worker_support(WORKERS_DEFAULT_NODE_B).await,
        Code::PermissionDenied,
        "is not on node",
    );
}

#[test]
fn server_tls_config_rejects_unreadable_ca_certs() {
    // The binary reads the bundle and hands its bytes over; a missing file
    // fails the read, and a bundle without certificates the config.
    let d = tempfile::tempdir().unwrap();
    assert!(std::fs::read(d.path().join("absent.pem")).is_err());
    assert!(server_tls_config(KEEPER_NODE_A.as_bytes(), b"").is_err());
    assert!(server_tls_config(KEEPER_NODE_A.as_bytes(), b"not a pem").is_err());
    assert!(server_tls_config(KEEPER_NODE_A.as_bytes(), CA.as_bytes()).is_ok());
}
