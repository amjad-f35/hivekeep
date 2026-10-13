//! Registration against a fake kubelet, and the device plugin API it then
//! calls.

use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use super::*;
use crate::device_plugin::pb::device_plugin_client::DevicePluginClient;
use crate::device_plugin::pb::registration_server::{Registration, RegistrationServer};
use crate::device_plugin::{DEVICE_COUNT, RESOURCE_KVM};

/// A kubelet's Registration service, recording requests and failing the
/// first `fail_first` of them.
#[derive(Clone, Default)]
struct FakeKubelet {
    requests: Arc<Mutex<Vec<pb::RegisterRequest>>>,
    fail_first: Arc<Mutex<usize>>,
}

#[tonic::async_trait]
impl Registration for FakeKubelet {
    async fn register(
        &self,
        req: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::Empty>, Status> {
        self.requests.lock().push(req.into_inner());
        let mut fail = self.fail_first.lock();
        if *fail > 0 {
            *fail -= 1;
            return Err(Status::invalid_argument("resource name already registered"));
        }
        Ok(Response::new(pb::Empty {}))
    }
}

/// Serves `kubelet` on `dir/kubelet.sock` until the token is cancelled.
fn serve_kubelet(dir: &Path, kubelet: FakeKubelet) -> CancellationToken {
    let sock = dir.join(KUBELET_SOCKET_NAME);
    let _ = std::fs::remove_file(&sock);
    let listener = tokio::net::UnixListener::bind(&sock).unwrap();
    let stop = CancellationToken::new();
    let s = stop.clone();
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(RegistrationServer::new(kubelet))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                s.cancelled_owned(),
            )
            .await;
    });
    stop
}

fn kvm() -> HostDevice {
    HostDevice {
        resource_name: RESOURCE_KVM.into(),
        path: "/dev/kvm".into(),
    }
}

fn registrar(dir: &Path) -> KubeletRegistrar {
    KubeletRegistrar::new(dir)
        .with_retry_interval(Duration::from_millis(20))
        .with_watch_interval(Duration::from_millis(20))
}

async fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..250 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn registers_with_the_kubelet_and_serves_the_device_api() {
    let dir = tempfile::tempdir().unwrap();
    let kubelet = FakeKubelet::default();
    let _k = serve_kubelet(dir.path(), kubelet.clone());
    let ctx = Ctx::background().child();
    let (r, c) = (registrar(dir.path()), ctx.clone());
    let run = tokio::spawn(async move { r.run(&c, &kvm()).await });

    wait_for("registration", || !kubelet.requests.lock().is_empty()).await;
    let req = kubelet.requests.lock()[0].clone();
    assert_eq!(req.version, "v1beta1");
    assert_eq!(req.endpoint, "hivekeep.io-kvm.sock");
    assert_eq!(req.resource_name, "hivekeep.io/kvm");
    assert!(req.options.is_none());

    let mut client = DevicePluginClient::new(unix_channel(&dir.path().join(&req.endpoint)));
    let opts = client
        .get_device_plugin_options(pb::Empty {})
        .await
        .unwrap()
        .into_inner();
    assert!(!opts.pre_start_required && !opts.get_preferred_allocation_available);

    let mut stream = client
        .list_and_watch(pb::Empty {})
        .await
        .unwrap()
        .into_inner();
    let first = stream.message().await.unwrap().unwrap();
    assert_eq!(first.devices.len(), DEVICE_COUNT);
    assert!(first.devices.iter().all(|d| d.health == HEALTHY));
    assert_eq!(first.devices[0].id, "kvm-0");
    // The list never changes, so the stream stays open with nothing more.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), stream.message())
            .await
            .is_err()
    );

    let resp = client
        .allocate(pb::AllocateRequest {
            container_requests: vec![
                pb::ContainerAllocateRequest {
                    devices_ids: vec!["kvm-0".into()],
                },
                pb::ContainerAllocateRequest {
                    devices_ids: vec!["kvm-1".into()],
                },
            ],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.container_responses.len(), 2);
    for c in &resp.container_responses {
        assert_eq!(
            c.devices,
            vec![pb::DeviceSpec {
                container_path: "/dev/kvm".into(),
                host_path: "/dev/kvm".into(),
                permissions: "rw".into()
            }]
        );
    }
    client
        .pre_start_container(pb::PreStartContainerRequest {
            devices_ids: vec!["kvm-0".into()],
        })
        .await
        .unwrap();
    let err = client
        .get_preferred_allocation(pb::PreferredAllocationRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);

    // Stopping ends the registration and removes the socket.
    ctx.cancel();
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!dir.path().join(&req.endpoint).exists());
    // An open ListAndWatch ends with the plugin.
    let ended = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap();
    assert!(!matches!(ended, Ok(Some(_))));
}

#[tokio::test]
async fn re_registers_after_the_kubelet_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let kubelet = FakeKubelet::default();
    let first = serve_kubelet(dir.path(), kubelet.clone());
    let ctx = Ctx::background().child();
    let (r, c) = (registrar(dir.path()), ctx.clone());
    let run = tokio::spawn(async move { r.run(&c, &kvm()).await });
    wait_for("first registration", || kubelet.requests.lock().len() == 1).await;

    // Nothing changes while the kubelet keeps running.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(kubelet.requests.lock().len(), 1);

    // A restarted kubelet recreates its socket and has forgotten us.
    first.cancel();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _second = serve_kubelet(dir.path(), kubelet.clone());
    wait_for("re-registration", || kubelet.requests.lock().len() == 2).await;

    // The plugin serves again on its (recreated) socket.
    let mut client =
        DevicePluginClient::new(unix_channel(&dir.path().join("hivekeep.io-kvm.sock")));
    client
        .get_device_plugin_options(pb::Empty {})
        .await
        .unwrap();
    ctx.cancel();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn retries_until_the_kubelet_accepts() {
    let dir = tempfile::tempdir().unwrap();
    let kubelet = FakeKubelet::default();
    *kubelet.fail_first.lock() = 2;
    let _k = serve_kubelet(dir.path(), kubelet.clone());
    let ctx = Ctx::background().child();
    let (r, c) = (registrar(dir.path()), ctx.clone());
    let run = tokio::spawn(async move { r.run(&c, &kvm()).await });
    wait_for("third registration", || kubelet.requests.lock().len() == 3).await;
    // Accepted on the third try: no more attempts.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(kubelet.requests.lock().len(), 3);
    ctx.cancel();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn retries_while_no_kubelet_listens() {
    let dir = tempfile::tempdir().unwrap();
    let ctx = Ctx::background().child();
    let (r, c) = (registrar(dir.path()), ctx.clone());
    let run = tokio::spawn(async move { r.run(&c, &kvm()).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let kubelet = FakeKubelet::default();
    let _k = serve_kubelet(dir.path(), kubelet.clone());
    wait_for("registration once the kubelet is up", || {
        !kubelet.requests.lock().is_empty()
    })
    .await;
    ctx.cancel();
    run.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_stale_plugin_socket_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("hivekeep.io-kvm.sock"), b"stale").unwrap();
    let kubelet = FakeKubelet::default();
    let _k = serve_kubelet(dir.path(), kubelet.clone());
    let ctx = Ctx::background().child();
    let (r, c) = (registrar(dir.path()), ctx.clone());
    let run = tokio::spawn(async move { r.run(&c, &kvm()).await });
    wait_for("registration", || !kubelet.requests.lock().is_empty()).await;
    let mut client =
        DevicePluginClient::new(unix_channel(&dir.path().join("hivekeep.io-kvm.sock")));
    client
        .get_device_plugin_options(pb::Empty {})
        .await
        .unwrap();
    ctx.cancel();
    run.await.unwrap().unwrap();
}
