//! The router Deployment's flags and Service.

use super::support::*;
use crate::router::config::RouterConfig;
use crate::router::status::listen_status;

/// The router accepts the flags in its manifest, and the client CA it is told
/// about is mounted in Envoy, which is what enforces it. The manifest carries
/// no allowlist; the installer appends one, so the test does too.
#[test]
fn router_ingress_auth_manifest_is_valid() {
    let (mut rest, _) = router_manifest_args();
    rest.push("--ingress-allowed-spiffe-ids=spiffe://cluster.local/ns/test/sa/client".into());
    let cfg = RouterConfig::from(parse_router_args(&rest));
    if let Err(e) = cfg.validate() {
        panic!("the router would refuse the manifest's flags: {e}");
    }
    let pod = pod_spec(ROUTER_MANIFEST, ROUTER_DEPLOYMENT);
    let mounts: Vec<&str> = list(container(&pod, "envoy"), "volumeMounts")
        .into_iter()
        .map(|m| s(m, "mountPath"))
        .collect();
    let ca = &cfg.ingress_auth.client_ca_file;
    assert!(
        mounts.iter().any(|dir| ca.starts_with(&format!("{dir}/"))),
        "--ingress-client-ca-file={ca} is not under any of the envoy container's mounts {mounts:?}"
    );
}

#[tokio::test]
async fn router_manifest_binds_status_to_loopback() {
    let (_, a) = router_manifest_args();
    // Port 0 binds any free port; only the address is under test.
    let lis = listen_status(&a.status_address, 0).await.unwrap();
    let ip = lis.local_addr().unwrap().ip();
    assert!(
        ip.is_loopback(),
        "--status-address {:?} in {ROUTER_MANIFEST} binds {ip}, which other pods can reach",
        a.status_address
    );
}

#[test]
fn router_service_does_not_expose_status() {
    let (_, a) = router_manifest_args();
    let status_port = i64::from(a.status_port);
    let svc = object(ROUTER_MANIFEST, "Service", ROUTER_DEPLOYMENT);
    for port in list(child(&svc, "spec"), "ports") {
        let target = child(port, "targetPort");
        let exposes = s(port, "name") == "status"
            || port.get("port").and_then(serde_json::Value::as_i64) == Some(status_port)
            || target.as_i64() == Some(status_port)
            || target.as_str() == Some("status")
            || target.as_str().and_then(|t| t.parse::<i64>().ok()) == Some(status_port);
        assert!(!exposes, "router Service exposes the status server: {port}");
    }
}
