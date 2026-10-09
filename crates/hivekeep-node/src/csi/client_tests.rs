//! The client against the fake driver over TCP and a unix socket.

use super::client::CsiClient;
use super::fake::{FakeCsiDriver, FAKE_DRIVER_NAME};
use super::pb;

#[tokio::test]
async fn new_csi_client_tcp() {
    let driver = FakeCsiDriver::new();
    let server = driver.serve_tcp().await.unwrap();
    assert!(server.endpoint.starts_with("tcp://127.0.0.1:"));
    let client = CsiClient::new(&server.endpoint, None).unwrap();
    let info = client
        .identity()
        .get_plugin_info(pb::GetPluginInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.name, FAKE_DRIVER_NAME);
}

#[tokio::test]
async fn client_identity() {
    let dir = tempfile::tempdir().unwrap();
    let driver = FakeCsiDriver::new();
    let server = driver
        .serve_unix(&dir.path().join("csi.sock"))
        .await
        .unwrap();
    let client = CsiClient::new(&server.endpoint, None).unwrap();

    let info = client
        .identity()
        .get_plugin_info(pb::GetPluginInfoRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(info.name, FAKE_DRIVER_NAME);

    let caps = client
        .identity()
        .get_plugin_capabilities(pb::GetPluginCapabilitiesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(
        !caps.capabilities.is_empty(),
        "expected capabilities, got none"
    );

    client.identity().probe(pb::ProbeRequest {}).await.unwrap();
    assert_eq!(
        driver.calls(),
        ["GetPluginInfo", "GetPluginCapabilities", "Probe"]
    );
}

#[tokio::test]
async fn unimplemented_rpcs_answer_unimplemented() {
    let dir = tempfile::tempdir().unwrap();
    let driver = FakeCsiDriver::new();
    let server = driver
        .serve_unix(&dir.path().join("csi.sock"))
        .await
        .unwrap();
    let client = CsiClient::new(&server.endpoint, None).unwrap();
    let err = client
        .controller()
        .list_volumes(pb::ListVolumesRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
async fn calls_fail_while_nothing_listens_and_recover_once_it_does() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("csi.sock");
    let client = CsiClient::new(&format!("unix://{}", sock.display()), None).unwrap();
    let err = client
        .identity()
        .probe(pb::ProbeRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unavailable);
    let driver = FakeCsiDriver::new();
    let _server = driver.serve_unix(&sock).await.unwrap();
    client.identity().probe(pb::ProbeRequest {}).await.unwrap();
}
