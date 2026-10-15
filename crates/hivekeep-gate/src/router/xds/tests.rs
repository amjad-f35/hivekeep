use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use envoy_types::pb::envoy::config::accesslog::v3::access_log;
use envoy_types::pb::envoy::config::cluster::v3::{
    cluster::ClusterDiscoveryType, cluster::DiscoveryType, cluster::LbConfig, Cluster,
};
use envoy_types::pb::envoy::config::core::v3::{
    address, config_source, data_source, header_value_option::HeaderAppendAction, transport_socket,
    Node, TransportSocket,
};
use envoy_types::pb::envoy::config::listener::v3::{filter, Listener};
use envoy_types::pb::envoy::config::route::v3::{
    route, route_match, RouteAction, RouteConfiguration,
};
use envoy_types::pb::envoy::extensions::access_loggers::stream::v3::{
    stdout_access_log, StdoutAccessLog,
};
use envoy_types::pb::envoy::extensions::filters::common::set_filter_state::v3::{
    filter_state_value, FilterStateValue,
};
use envoy_types::pb::envoy::extensions::filters::http::set_filter_state::v3::Config as HttpSetFilterState;
use envoy_types::pb::envoy::extensions::filters::network::http_connection_manager::v3::{
    http_connection_manager::CodecType, http_filter, HttpConnectionManager, HttpFilter,
};
use envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::{
    common_tls_context, secret, subject_alt_name_matcher::SanType, CommonTlsContext,
    DownstreamTlsContext, Secret, TlsCertificate, UpstreamTlsContext,
};
use envoy_types::pb::envoy::extensions::upstreams::http::v3::{
    http_protocol_options::UpstreamProtocolOptions, HttpProtocolOptions,
};
use envoy_types::pb::envoy::r#type::matcher::v3::string_matcher::MatchPattern;
use envoy_types::pb::envoy::service::discovery::v3::DiscoveryRequest;
use envoy_types::pb::envoy::service::secret::v3::secret_discovery_service_client::SecretDiscoveryServiceClient;
use envoy_types::pb::google::protobuf::Any;
use prost::{Message, Name};
use tokio_util::sync::CancellationToken;

use super::filters::{actor_routing_filter_state_filter, SharedWithUpstream};
use super::routes::build_connect_routes;
use super::snapshot::{CLUSTER_TYPE, SECRET_TYPE};
use super::*;
use crate::router::extproc::{CONNECT_AUTHORITY_FILTER_STATE_KEY, TARGET_ACTOR_FILTER_STATE_KEY};
use crate::router::ingress::{
    DEFAULT_PARKED_REQUEST_MAX, ORIGINAL_DST_ADDRESS_KEY, ORIGINAL_DST_METADATA_KEY,
};
use crate::tunnel::ingress::TARGET_PORT_HEADER;

pub(super) const TEST_CERT_PATH: &str = "/run/servicedns.podcert.hivekeep.io/credential-bundle.pem";
pub(super) const TEST_CA_PATH: &str = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem";
pub(super) const TEST_CLIENT_ID: &str = "spiffe://cluster.local/ns/demo/sa/client";

pub(super) fn set_ingress_tls(server: &XdsServer) {
    server.set_tls_config(8443, TEST_CERT_PATH);
    server.set_downstream_client_auth(TEST_CA_PATH, &[TEST_CLIENT_ID.to_string()]);
}

pub(super) fn unpack<T: Message + Name + Default>(any: &Any) -> T {
    pb::unpack(any).unwrap()
}

/// Publishes a snapshot and returns it, checked for consistency.
pub(super) fn publish(server: &XdsServer) -> Arc<Snapshot> {
    server.update_snapshot().expect("update_snapshot");
    let snap = server.snapshot().expect("a snapshot for the node");
    snap.consistent().expect("consistent snapshot");
    snap
}

pub(super) fn snapshot_listeners(server: &XdsServer) -> BTreeMap<String, Listener> {
    publish(server).decoded::<Listener>()
}

pub(super) fn snapshot_secrets(server: &XdsServer) -> BTreeMap<String, Secret> {
    publish(server).decoded::<Secret>()
}

pub(super) fn filter_any(f: &envoy_types::pb::envoy::config::listener::v3::Filter) -> &Any {
    match &f.config_type {
        Some(filter::ConfigType::TypedConfig(any)) => any,
        other => panic!("filter {} has no typed config: {other:?}", f.name),
    }
}

pub(super) fn http_filter_any(f: &HttpFilter) -> &Any {
    match &f.config_type {
        Some(http_filter::ConfigType::TypedConfig(any)) => any,
        other => panic!("HTTP filter {} has no typed config: {other:?}", f.name),
    }
}

pub(super) fn socket_any(ts: &TransportSocket) -> &Any {
    match &ts.config_type {
        Some(transport_socket::ConfigType::TypedConfig(any)) => any,
        other => panic!(
            "transport socket {} has no typed config: {other:?}",
            ts.name
        ),
    }
}

pub(super) fn hcm_of(l: &Listener) -> HttpConnectionManager {
    unpack(filter_any(&l.filter_chains[0].filters[0]))
}

pub(super) fn set_filter_state(f: &HttpFilter) -> HttpSetFilterState {
    unpack(http_filter_any(f))
}

pub(super) fn object_key(v: &FilterStateValue) -> &str {
    match &v.key {
        Some(filter_state_value::Key::ObjectKey(k)) => k,
        None => "",
    }
}

pub(super) fn format_string(v: &FilterStateValue) -> String {
    match &v.value {
        Some(filter_state_value::Value::FormatString(f)) => inline(f),
        other => panic!("filter state value without a format string: {other:?}"),
    }
}

pub(super) fn inline(
    f: &envoy_types::pb::envoy::config::core::v3::SubstitutionFormatString,
) -> String {
    use envoy_types::pb::envoy::config::core::v3::substitution_format_string::Format;
    match &f.format {
        Some(Format::TextFormatSource(ds)) => match &ds.specifier {
            Some(data_source::Specifier::InlineString(s)) => s.clone(),
            other => panic!("format source is not inline: {other:?}"),
        },
        other => panic!("not a text format source: {other:?}"),
    }
}

pub(super) fn filename(
    ds: &Option<envoy_types::pb::envoy::config::core::v3::DataSource>,
) -> String {
    match ds.as_ref().and_then(|d| d.specifier.as_ref()) {
        Some(data_source::Specifier::Filename(f)) => f.clone(),
        _ => String::new(),
    }
}

pub(super) fn tls_certificate(s: &Secret) -> &TlsCertificate {
    match &s.r#type {
        Some(secret::Type::TlsCertificate(c)) => c,
        other => panic!("secret {} is not a certificate: {other:?}", s.name),
    }
}

pub(super) fn validation_context(
    s: &Secret,
) -> &envoy_types::pb::envoy::extensions::transport_sockets::tls::v3::CertificateValidationContext {
    match &s.r#type {
        Some(secret::Type::ValidationContext(c)) => c,
        other => panic!("secret {} is not a validation context: {other:?}", s.name),
    }
}

pub(super) fn combined(
    common: &CommonTlsContext,
) -> &common_tls_context::CombinedCertificateValidationContext {
    match &common.validation_context_type {
        Some(common_tls_context::ValidationContextType::CombinedValidationContext(c)) => c,
        other => panic!("no combined validation context: {other:?}"),
    }
}

pub(super) fn uses_ads(
    cfg: &Option<envoy_types::pb::envoy::config::core::v3::ConfigSource>,
) -> bool {
    matches!(
        cfg.as_ref()
            .and_then(|c| c.config_source_specifier.as_ref()),
        Some(config_source::ConfigSourceSpecifier::Ads(_))
    )
}

pub(super) fn socket(
    addr: &Option<envoy_types::pb::envoy::config::core::v3::Address>,
) -> &envoy_types::pb::envoy::config::core::v3::SocketAddress {
    match addr.as_ref().and_then(|a| a.address.as_ref()) {
        Some(address::Address::SocketAddress(sa)) => sa,
        other => panic!("not a socket address: {other:?}"),
    }
}

pub(super) fn port_value(sa: &envoy_types::pb::envoy::config::core::v3::SocketAddress) -> u32 {
    use envoy_types::pb::envoy::config::core::v3::socket_address::PortSpecifier;
    match sa.port_specifier {
        Some(PortSpecifier::PortValue(p)) => p,
        _ => 0,
    }
}

pub(super) fn endpoint_address(
    c: &Cluster,
) -> &Option<envoy_types::pb::envoy::config::core::v3::Address> {
    use envoy_types::pb::envoy::config::endpoint::v3::lb_endpoint::HostIdentifier;
    match &c.load_assignment.as_ref().unwrap().endpoints[0].lb_endpoints[0].host_identifier {
        Some(HostIdentifier::Endpoint(e)) => &e.address,
        other => panic!("no endpoint: {other:?}"),
    }
}

pub(super) fn cluster_type(c: &Cluster) -> Option<DiscoveryType> {
    match c.cluster_discovery_type {
        Some(ClusterDiscoveryType::Type(t)) => DiscoveryType::try_from(t).ok(),
        _ => None,
    }
}

pub(super) fn protocol_options(c: &Cluster) -> HttpProtocolOptions {
    unpack(
        c.typed_extension_protocol_options
            .get(HTTP_PROTOCOL_OPTIONS_NAME)
            .expect("protocol options"),
    )
}

pub(super) fn route_action(r: &envoy_types::pb::envoy::config::route::v3::Route) -> &RouteAction {
    match &r.action {
        Some(route::Action::Route(a)) => a,
        other => panic!("not a route action: {other:?}"),
    }
}

pub(super) fn action_cluster(a: &RouteAction) -> &str {
    use envoy_types::pb::envoy::config::route::v3::route_action::ClusterSpecifier;
    match &a.cluster_specifier {
        Some(ClusterSpecifier::Cluster(c)) => c,
        _ => "",
    }
}

pub(super) fn downstream_tls_context(l: &Listener) -> DownstreamTlsContext {
    assert_eq!(
        l.filter_chains.len(),
        1,
        "listener {} has {} filter chains",
        l.name,
        l.filter_chains.len()
    );
    unpack(socket_any(
        l.filter_chains[0]
            .transport_socket
            .as_ref()
            .expect("transport socket"),
    ))
}

#[test]
fn actor_routing_filter_state_filter_cases() {
    let actor = "%CEL(request.headers['hk-target-actor'])%";
    for (name, capture, want) in [
        (
            "ordinary ingress",
            false,
            BTreeMap::from([(TARGET_ACTOR_FILTER_STATE_KEY, actor.to_string())]),
        ),
        (
            "CONNECT termination",
            true,
            BTreeMap::from([
                (TARGET_ACTOR_FILTER_STATE_KEY, actor.to_string()),
                (
                    CONNECT_AUTHORITY_FILTER_STATE_KEY,
                    "%REQ(:authority)%".to_string(),
                ),
                (
                    CONNECT_POOL_FILTER_STATE_KEY,
                    format!("{actor}|%REQ(:authority)%"),
                ),
            ]),
        ),
    ] {
        let config = set_filter_state(&actor_routing_filter_state_filter(capture));
        assert_eq!(config.on_request_headers.len(), want.len(), "{name}");
        for value in &config.on_request_headers {
            let key = object_key(value);
            let format = format_string(value);
            assert_eq!(Some(&format), want.get(key), "{name}: {key}");
            if key != CONNECT_AUTHORITY_FILTER_STATE_KEY && key != CONNECT_POOL_FILTER_STATE_KEY {
                assert!(
                    !format.to_lowercase().contains(":authority"),
                    "{name}: {key} reads the authority"
                );
            }
            let want_factory = if key == CONNECT_POOL_FILTER_STATE_KEY {
                "envoy.hashable_string"
            } else {
                "envoy.string"
            };
            assert_eq!(value.factory_key, want_factory, "{name}: {key}");
        }
    }
}

fn assert_dual_stack_ingress(l: &Listener, want_port: u32) {
    let sa = socket(&l.address);
    assert_eq!(sa.address, "0.0.0.0");
    assert_eq!(port_value(sa), want_port);
    assert_eq!(l.additional_addresses.len(), 1);
    let asa = socket(&l.additional_addresses[0].address);
    assert_eq!(asa.address, "::");
    assert!(!asa.ipv4_compat, "the IPv6 twin must not accept IPv4");
    assert_eq!(port_value(asa), want_port);
}

fn assert_hcm_websocket_upgrade(l: &Listener) {
    assert_eq!(l.filter_chains[0].filters[0].name, HCM_FILTER_NAME);
    let hcm = hcm_of(l);
    assert!(
        hcm.upgrade_configs
            .iter()
            .any(|u| u.upgrade_type == "websocket"),
        "no websocket upgrade on {}",
        l.name
    );
}

#[test]
fn update_snapshot() {
    let server = XdsServer::new(18000);
    server.set_config(50052, "10.0.0.1");
    set_ingress_tls(&server);
    let snap = publish(&server);

    let clusters = snap.decoded::<Cluster>();
    assert_eq!(clusters.len(), 2, "{:?}", clusters.keys());
    let c = &clusters["hk-cluster"];
    assert_eq!(c.name, "hk-cluster");
    let ep = socket(endpoint_address(c));
    assert_eq!(ep.address, "10.0.0.1");
    assert_eq!(port_value(ep), 50052);
    let od = &clusters[ORIGINAL_DST_CLUSTER_NAME];
    assert_eq!(cluster_type(od), Some(DiscoveryType::OriginalDst));

    let routes = snap.decoded::<RouteConfiguration>();
    assert_eq!(routes.len(), 1);
    let rc = &routes[ROUTE_NAME];
    assert_eq!(rc.name, ROUTE_NAME);
    assert_eq!(rc.virtual_hosts.len(), 1);
    let vh = &rc.virtual_hosts[0];
    assert_eq!(vh.domains, vec!["*"]);
    assert_eq!(vh.routes.len(), 1);
    let m = vh.routes[0].r#match.as_ref().unwrap();
    assert_eq!(
        m.path_specifier,
        Some(route_match::PathSpecifier::Prefix("/".into()))
    );

    let listeners = snap.decoded::<Listener>();
    assert_eq!(listeners.len(), 1);
    let l = &listeners[INGRESS_HTTPS_LISTENER];
    assert_dual_stack_ingress(l, 8443);
    assert_hcm_websocket_upgrade(l);
}

#[test]
fn update_snapshot_with_https() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    server.set_tls_config(8443, TEST_CERT_PATH);
    server.set_downstream_client_auth(TEST_CA_PATH, &[TEST_CLIENT_ID.to_string()]);
    let snap = publish(&server);

    let listeners = snap.decoded::<Listener>();
    assert_eq!(listeners.len(), 1);
    let l = &listeners[INGRESS_HTTPS_LISTENER];
    assert_dual_stack_ingress(l, 8443);
    let ts = l.filter_chains[0].transport_socket.as_ref().unwrap();
    assert_eq!(ts.name, "envoy.transport_sockets.tls");
    let dtc: DownstreamTlsContext = unpack(socket_any(ts));
    let common = dtc.common_tls_context.unwrap();
    assert!(
        common.tls_certificates.is_empty(),
        "the serving cert must arrive over SDS"
    );
    let sds = &common.tls_certificate_sds_secret_configs;
    assert_eq!(sds.len(), 1);
    assert_eq!(sds[0].name, HTTPS_CERT_SECRET_NAME);
    assert!(
        uses_ads(&sds[0].sds_config),
        "SDS config must use the ADS config source"
    );

    let secrets = snap.decoded::<Secret>();
    assert_eq!(secrets.len(), 2);
    let cert = tls_certificate(&secrets[HTTPS_CERT_SECRET_NAME]);
    assert_eq!(filename(&cert.certificate_chain), TEST_CERT_PATH);
    assert_eq!(filename(&cert.private_key), TEST_CERT_PATH);
    assert_eq!(
        cert.watched_directory.as_ref().unwrap().path,
        "/run/servicedns.podcert.hivekeep.io"
    );
}

#[test]
fn update_snapshot_connect_disabled_by_default() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    set_ingress_tls(&server);
    let snap = publish(&server);
    let clusters = snap.decoded::<Cluster>();
    assert!(!clusters.contains_key(MAIN_INTERNAL_NAME));
    assert_eq!(clusters.len(), 2);
    let listeners = snap.decoded::<Listener>();
    for name in [CONNECT_TERMINATE_TLS_LISTENER, MAIN_INTERNAL_NAME] {
        assert!(
            !listeners.contains_key(name),
            "{name} built without a CONNECT port"
        );
    }
    assert_eq!(listeners.len(), 1);
}

#[test]
fn update_snapshot_with_connect() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    server.set_connect_tls_port(8444);
    server.set_tls_config(0, TEST_CERT_PATH);
    server.set_downstream_client_auth(TEST_CA_PATH, &[TEST_CLIENT_ID.to_string()]);
    let snap = publish(&server);
    assert!(snap.decoded::<Cluster>().contains_key(MAIN_INTERNAL_NAME));
    let listeners = snap.decoded::<Listener>();
    assert!(
        !listeners.contains_key(INGRESS_HTTPS_LISTENER),
        "plain HTTPS listener must not be built when only httpsPort is left disabled"
    );
    let l = &listeners[CONNECT_TERMINATE_TLS_LISTENER];
    assert_dual_stack_ingress(l, 8444);
    assert_hcm_websocket_upgrade(l);
    assert_eq!(
        l.filter_chains[0].transport_socket.as_ref().unwrap().name,
        "envoy.transport_sockets.tls"
    );
    assert!(listeners.contains_key(MAIN_INTERNAL_NAME));
    assert!(
        snap.decoded::<Secret>()
            .contains_key(HTTPS_CERT_SECRET_NAME),
        "cert secret missing even though connect_terminate_tls needs it"
    );
}

#[test]
fn access_logs_omit_query_string() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    server.set_connect_tls_port(8444);
    set_ingress_tls(&server);
    let mut hcms = 0;
    for (name, l) in snapshot_listeners(&server) {
        for fc in &l.filter_chains {
            for f in fc.filters.iter().filter(|f| f.name == HCM_FILTER_NAME) {
                hcms += 1;
                let hcm: HttpConnectionManager = unpack(filter_any(f));
                assert!(!hcm.access_log.is_empty(), "{name} has no access log");
                for al in &hcm.access_log {
                    let Some(access_log::ConfigType::TypedConfig(any)) = &al.config_type else {
                        panic!("untyped access log")
                    };
                    let stdout: StdoutAccessLog = unpack(any);
                    let Some(stdout_access_log::AccessLogFormat::LogFormat(fmt)) =
                        &stdout.access_log_format
                    else {
                        panic!("no log format")
                    };
                    assert!(
                        inline(fmt).contains("%PATH(NQ:"),
                        "{name} logs the query string"
                    );
                }
            }
        }
    }
    assert!(hcms > 0, "no HTTP connection managers found");
}

#[tokio::test]
async fn serve_shutdown() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cancel = CancellationToken::new();
    let task = {
        let (server, cancel) = (server.clone(), cancel.clone());
        tokio::spawn(async move { server.serve(lis, cancel).await })
    };
    cancel.cancel();
    let res = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("serve did not stop within 2s");
    res.unwrap().expect("serve returned an error");
}

#[tokio::test]
async fn serves_secret_over_sds() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    server.set_tls_config(8443, TEST_CERT_PATH);
    server.set_downstream_client_auth(TEST_CA_PATH, &[TEST_CLIENT_ID.to_string()]);
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    {
        let server = server.clone();
        tokio::spawn(async move { server.serve(lis, cancel).await });
    }

    let mut client = SecretDiscoveryServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    tx.send(DiscoveryRequest {
        node: Some(Node {
            id: NODE_ID.to_string(),
            ..Default::default()
        }),
        type_url: SECRET_TYPE.to_string(),
        resource_names: vec![
            HTTPS_CERT_SECRET_NAME.to_string(),
            INGRESS_CLIENT_TRUST_SECRET_NAME.to_string(),
        ],
        ..Default::default()
    })
    .await
    .unwrap();
    let mut stream = client
        .stream_secrets(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let resp = tokio::time::timeout(Duration::from_secs(10), stream.message())
        .await
        .expect("no SDS response")
        .unwrap()
        .unwrap();
    let secret = resp
        .resources
        .iter()
        .map(unpack::<Secret>)
        .find(|s| s.name == HTTPS_CERT_SECRET_NAME)
        .expect("serving cert secret missing from the SDS response");
    let cert = tls_certificate(&secret);
    assert_eq!(filename(&cert.certificate_chain), TEST_CERT_PATH);
    assert_eq!(filename(&cert.private_key), TEST_CERT_PATH);
    assert_eq!(
        cert.watched_directory.as_ref().unwrap().path,
        "/run/servicedns.podcert.hivekeep.io"
    );
}

const DATA_DIR_NAME: &str = "..data";
const NEW_DATA_DIR_NAME: &str = "..data_tmp";

fn make_serving_bundle(cn: &str) -> Vec<u8> {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, cn);
    params.subject_alt_names = vec![rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap())];
    params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let cert = params.self_signed(&key).unwrap();
    format!("{}{}", key.serialize_pem(), cert.pem()).into_bytes()
}

fn read_serving_cn(path: &Path) -> String {
    let data = std::fs::read(path).unwrap();
    let der = rustls_pemfile::certs(&mut data.as_slice())
        .next()
        .expect("no certificate in bundle")
        .unwrap();
    assert!(
        rustls_pemfile::private_key(&mut data.as_slice())
            .unwrap()
            .is_some(),
        "no key in bundle"
    );
    let (_, cert) = x509_parser::parse_x509_certificate(&der).unwrap();
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    cn
}

fn write_projected_volume(dir: &Path, ts_dir: &str, bundle: &[u8]) {
    std::fs::create_dir(dir.join(ts_dir)).unwrap();
    std::fs::write(dir.join(ts_dir).join("credential-bundle.pem"), bundle).unwrap();
    std::os::unix::fs::symlink(ts_dir, dir.join(DATA_DIR_NAME)).unwrap();
    std::os::unix::fs::symlink(
        Path::new(DATA_DIR_NAME).join("credential-bundle.pem"),
        dir.join("credential-bundle.pem"),
    )
    .unwrap();
}

fn rotate_projected_volume(dir: &Path, new_ts_dir: &str, old_ts_dir: &str, bundle: &[u8]) {
    std::fs::create_dir(dir.join(new_ts_dir)).unwrap();
    std::fs::write(dir.join(new_ts_dir).join("credential-bundle.pem"), bundle).unwrap();
    std::os::unix::fs::symlink(new_ts_dir, dir.join(NEW_DATA_DIR_NAME)).unwrap();
    std::fs::rename(dir.join(NEW_DATA_DIR_NAME), dir.join(DATA_DIR_NAME)).unwrap();
    std::fs::remove_dir_all(dir.join(old_ts_dir)).unwrap();
}

/// Rotating a projected volume swaps the `..data` link inside the watched
/// directory, and the configured path then reads the new bundle.
fn assert_rotation_followed(
    secret: &Secret,
    dir: &Path,
    cert_a: &str,
    cert_b: &str,
    bundle_b: &[u8],
) {
    const TS_DIR_A: &str = "..2026_07_25_00_00_00.0000000001";
    const TS_DIR_B: &str = "..2026_07_25_00_00_00.0000000002";
    let cert = tls_certificate(secret);
    let chain = filename(&cert.certificate_chain);
    assert_eq!(read_serving_cn(Path::new(&chain)), cert_a);
    let swap = Path::new(&cert.watched_directory.as_ref().unwrap().path).join(DATA_DIR_NAME);
    let before = std::fs::read_link(&swap).expect("the watched directory holds no ..data link");
    rotate_projected_volume(dir, TS_DIR_B, TS_DIR_A, bundle_b);
    let after = std::fs::read_link(&swap).unwrap();
    assert_ne!(
        after, before,
        "rotation did not swap the watched ..data link"
    );
    assert_eq!(read_serving_cn(Path::new(&chain)), cert_b);
}

#[test]
fn tls_secret_projected_volume_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("credential-bundle.pem");
    write_projected_volume(
        dir.path(),
        "..2026_07_25_00_00_00.0000000001",
        &make_serving_bundle("serving-cert-a"),
    );
    let server = XdsServer::new(18000);
    server.set_tls_config(8443, cert_path.to_str().unwrap());
    let secret = server.settings().build_tls_secret();
    assert_rotation_followed(
        &secret,
        dir.path(),
        "serving-cert-a",
        "serving-cert-b",
        &make_serving_bundle("serving-cert-b"),
    );
}

#[test]
fn upstream_transport_socket_delivers_certs_over_sds() {
    const CRED: &str = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";
    const TRUST: &str = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem";
    let server = XdsServer::new(18000);
    server.set_upstream_tls(CRED, TRUST);
    let ts = server
        .settings()
        .build_upstream_transport_socket()
        .expect("upstream transport socket");
    let utc: UpstreamTlsContext = unpack(socket_any(&ts));
    let common = utc.common_tls_context.unwrap();
    assert!(
        common.tls_certificates.is_empty(),
        "the client cert must arrive over SDS"
    );
    let sds = &common.tls_certificate_sds_secret_configs;
    assert_eq!(sds.len(), 1);
    assert_eq!(sds[0].name, UPSTREAM_CERT_SECRET_NAME);
    assert!(
        uses_ads(&sds[0].sds_config),
        "client cert SDS config must use ADS"
    );
    assert!(
        !matches!(
            common.validation_context_type,
            Some(common_tls_context::ValidationContextType::ValidationContext(_))
        ),
        "no inline validation context; the trusted CA must arrive over SDS"
    );
    let combined = combined(&common);
    let default_ctx = combined.default_validation_context.as_ref().unwrap();
    assert_eq!(filename(&default_ctx.trusted_ca), "");
    let trust = combined
        .validation_context_sds_secret_config
        .as_ref()
        .unwrap();
    assert_eq!(trust.name, UPSTREAM_TRUST_SECRET_NAME);
    assert!(
        uses_ads(&trust.sds_config),
        "trust bundle SDS config must use ADS"
    );
    let sans = &default_ctx.match_typed_subject_alt_names;
    assert_eq!(sans.len(), 1);
    assert_eq!(sans[0].san_type, SanType::Uri as i32);
    assert_eq!(
        sans[0].matcher.as_ref().unwrap().match_pattern,
        Some(MatchPattern::Prefix("spiffe://cluster.local/".into()))
    );
}

fn sets_upstream_sans(f: &HttpFilter) -> Option<FilterStateValue> {
    if f.name != HTTP_SET_FILTER_STATE_NAME {
        return None;
    }
    set_filter_state(f)
        .on_request_headers
        .into_iter()
        .find(|v| object_key(v) == "envoy.network.upstream_subject_alt_names")
}

#[test]
fn worker_identity_filter() {
    for upstream_connect in [false, true] {
        let server = XdsServer::new(18000);
        server.set_config(50053, "127.0.0.1");
        server.set_connect_tls_port(8444);
        set_ingress_tls(&server);
        server.set_upstream_tls("/run/bundle.pem", "/run/trust.pem");
        server.set_upstream_connect(upstream_connect);
        let listeners = snapshot_listeners(&server);
        for name in [INGRESS_HTTPS_LISTENER, MAIN_INTERNAL_NAME] {
            let filters = hcm_of(&listeners[name]).http_filters;
            let n = filters.len();
            assert!(
                n >= 2 && filters[n - 1].name == HTTP_ROUTER_FILTER_NAME,
                "{name}: router filter not last"
            );
            let v = sets_upstream_sans(&filters[n - 2]).unwrap_or_else(|| {
                panic!(
                    "{name} (connect={upstream_connect}): no upstream SAN filter before the router"
                )
            });
            assert_eq!(
                format_string(&v),
                "%DYNAMIC_METADATA(envoy.filters.listener.original_dst:worker_spiffe_id)%"
            );
            assert_eq!(v.shared_with_upstream, SharedWithUpstream::Once as i32);
            assert_eq!(v.factory_key, "");
        }
    }

    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    set_ingress_tls(&server);
    server.set_upstream_tls("/run/bundle.pem", "");
    for f in &hcm_of(&snapshot_listeners(&server)[INGRESS_HTTPS_LISTENER]).http_filters {
        assert!(
            sets_upstream_sans(f).is_none(),
            "upstream SANs set although the tunnel's certificate is not verified"
        );
    }
}

#[test]
fn update_snapshot_upstream_secrets() {
    const CRED: &str = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";
    const TRUST: &str = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem";
    const DIR: &str = "/run/podidentity.podcert.hivekeep.io";
    let server_with = |cred: Option<(&str, &str)>| {
        let server = XdsServer::new(18000);
        server.set_config(50053, "127.0.0.1");
        set_ingress_tls(&server);
        if let Some((c, t)) = cred {
            server.set_upstream_tls(c, t);
        }
        server
    };

    // Published when configured.
    let secrets = snapshot_secrets(&server_with(Some((CRED, TRUST))));
    let cert = tls_certificate(&secrets[UPSTREAM_CERT_SECRET_NAME]);
    assert_eq!(filename(&cert.certificate_chain), CRED);
    assert_eq!(filename(&cert.private_key), CRED);
    assert_eq!(cert.watched_directory.as_ref().unwrap().path, DIR);
    let trust = validation_context(&secrets[UPSTREAM_TRUST_SECRET_NAME]);
    assert_eq!(filename(&trust.trusted_ca), TRUST);
    assert_eq!(trust.watched_directory.as_ref().unwrap().path, DIR);

    // Absent when upstream mTLS is disabled.
    let secrets = snapshot_secrets(&server_with(None));
    for name in [UPSTREAM_CERT_SECRET_NAME, UPSTREAM_TRUST_SECRET_NAME] {
        assert!(
            !secrets.contains_key(name),
            "{name} published without upstream mTLS"
        );
    }

    // The trust secret is omitted without a trust bundle.
    let secrets = snapshot_secrets(&server_with(Some((CRED, ""))));
    assert!(!secrets.contains_key(UPSTREAM_TRUST_SECRET_NAME));
    assert!(secrets.contains_key(UPSTREAM_CERT_SECRET_NAME));
}

#[test]
fn upstream_cert_secret_projected_volume_rotation() {
    let dir = tempfile::tempdir().unwrap();
    let cred_path = dir.path().join("credential-bundle.pem");
    write_projected_volume(
        dir.path(),
        "..2026_07_25_00_00_00.0000000001",
        &make_serving_bundle("upstream-client-cert-a"),
    );
    let server = XdsServer::new(18000);
    server.set_upstream_tls(cred_path.to_str().unwrap(), "");
    let secret = server.settings().build_upstream_cert_secret();
    assert_rotation_followed(
        &secret,
        dir.path(),
        "upstream-client-cert-a",
        "upstream-client-cert-b",
        &make_serving_bundle("upstream-client-cert-b"),
    );
}

fn ext_proc_max_requests(x: &XdsServer) -> u32 {
    x.settings()
        .build_cluster()
        .circuit_breakers
        .unwrap()
        .thresholds[0]
        .max_requests
        .unwrap()
        .value
}

#[test]
fn ext_proc_circuit_breaker() {
    // The default covers the parking lot plus headroom.
    let got = ext_proc_max_requests(&XdsServer::new(0));
    assert_eq!(got, DEFAULT_EXT_PROC_MAX_REQUESTS);
    assert!(got as usize >= DEFAULT_PARKED_REQUEST_MAX);

    // The setter overrides it.
    let x = XdsServer::new(0);
    x.set_ext_proc_max_requests(4096);
    assert_eq!(ext_proc_max_requests(&x), 4096);

    // Zero keeps the default.
    let x = XdsServer::new(0);
    x.set_ext_proc_max_requests(0);
    assert_eq!(ext_proc_max_requests(&x), DEFAULT_EXT_PROC_MAX_REQUESTS);
}

fn workload_route_action(x: &XdsServer) -> RouteAction {
    let rc = x.settings().build_routes();
    assert_eq!(rc.virtual_hosts.len(), 1);
    assert_eq!(rc.virtual_hosts[0].routes.len(), 1);
    let action = route_action(&rc.virtual_hosts[0].routes[0]).clone();
    assert_eq!(action_cluster(&action), ORIGINAL_DST_CLUSTER_NAME);
    action
}

fn route_timeouts(x: &XdsServer) -> (Duration, Option<Duration>) {
    let a = workload_route_action(x);
    (
        pb::to_std(a.timeout.as_ref().unwrap()),
        a.idle_timeout.as_ref().map(pb::to_std),
    )
}

#[test]
fn route_timeout() {
    // Default.
    assert_eq!(route_timeouts(&XdsServer::new(0)).0, DEFAULT_ROUTE_TIMEOUT);

    // The setter overrides it.
    let x = XdsServer::new(0);
    x.set_route_timeout(Duration::from_secs(30));
    assert_eq!(route_timeouts(&x).0, Duration::from_secs(30));

    // Zero keeps the default (a negative duration is not representable).
    let x = XdsServer::new(0);
    x.set_route_timeout(Duration::ZERO);
    assert_eq!(route_timeouts(&x).0, DEFAULT_ROUTE_TIMEOUT);

    // The idle timeout stays beyond a longer route timeout.
    let x = XdsServer::new(0);
    x.set_route_timeout(Duration::from_secs(30 * 60));
    assert_eq!(
        route_timeouts(&x).1,
        Some(Duration::from_secs(30 * 60) + ROUTE_IDLE_TIMEOUT_MARGIN)
    );

    // The idle timeout keeps Envoy's default when the route timeout is shorter.
    for d in [Duration::from_secs(10), Duration::from_secs(60)] {
        let x = XdsServer::new(0);
        x.set_route_timeout(d);
        assert_eq!(
            route_timeouts(&x).1,
            Some(ENVOY_DEFAULT_STREAM_IDLE_TIMEOUT),
            "{d:?}"
        );
    }

    // The idle timeout never fires before the route timeout.
    for d in [
        Duration::ZERO,
        Duration::from_secs(10),
        Duration::from_secs(60),
        DEFAULT_ROUTE_TIMEOUT,
        Duration::from_secs(30 * 60),
    ] {
        let x = XdsServer::new(0);
        x.set_route_timeout(d);
        let (route, idle) = route_timeouts(&x);
        let idle = idle.expect("idle timeout unset");
        assert!(idle > route, "idle {idle:?} <= route {route:?} for {d:?}");
    }
}

#[test]
fn build_original_dst_cluster_uses_metadata_key() {
    use envoy_types::pb::envoy::r#type::metadata::v3::metadata_key::path_segment::Segment;
    let c = XdsServer::new(18000)
        .settings()
        .build_original_dst_cluster();
    let Some(LbConfig::OriginalDstLbConfig(lb)) = c.lb_config else {
        panic!("no original_dst lb config")
    };
    assert!(
        !lb.use_http_header,
        "use_http_header only applies to HTTP traffic; dynamic metadata is what ext_proc sets"
    );
    let key = lb.metadata_key.unwrap();
    assert_eq!(key.key, ORIGINAL_DST_METADATA_KEY);
    assert_eq!(key.path.len(), 1);
    assert_eq!(
        key.path[0].segment,
        Some(Segment::Key(ORIGINAL_DST_ADDRESS_KEY.to_string()))
    );
}

#[test]
fn actor_cluster_protocol_options() {
    let x = XdsServer::new(18000);
    assert!(x
        .settings()
        .build_original_dst_cluster()
        .typed_extension_protocol_options
        .is_empty());
    x.set_upstream_tls("/run/bundle.pem", "/run/trust.pem");
    let cluster = x.settings().build_original_dst_cluster();
    let ts = cluster
        .transport_socket
        .as_ref()
        .expect("no transport socket with upstream TLS");
    let utc: UpstreamTlsContext = unpack(socket_any(ts));
    assert!(
        utc.common_tls_context.unwrap().alpn_protocols.is_empty(),
        "ALPN must follow the downstream protocol"
    );
    let Some(UpstreamProtocolOptions::UseDownstreamProtocolConfig(downstream)) =
        protocol_options(&cluster).upstream_protocol_options
    else {
        panic!("no use_downstream_protocol_config")
    };
    assert!(downstream.http2_protocol_options.is_some(), "use_downstream_protocol_config must enable HTTP/2 so gRPC keeps trailers on the tunnel leg");
}

#[test]
fn build_routes_writes_target_port_header() {
    let rc = XdsServer::new(18000).settings().build_routes();
    let headers = &rc.virtual_hosts[0].routes[0].request_headers_to_add;
    assert_eq!(headers.len(), 1);
    let header = headers[0].header.as_ref().unwrap();
    assert_eq!(header.key, TARGET_PORT_HEADER);
    assert_eq!(
        header.value,
        "%DYNAMIC_METADATA(envoy.filters.listener.original_dst:port)%"
    );
    assert_eq!(
        headers[0].append_action,
        HeaderAppendAction::OverwriteIfExistsOrAdd as i32
    );
}

#[test]
fn build_connect_routes_disables_timeout() {
    let rc = build_connect_routes();
    let timeout = route_action(&rc.virtual_hosts[0].routes[0])
        .timeout
        .as_ref()
        .expect("timeout unset: Envoy would default it to 15s");
    assert_eq!(pb::to_std(timeout), Duration::ZERO);
}

fn otlp_endpoint(x: &XdsServer) -> (String, u32) {
    let c = x.settings().build_otlp_collector_cluster();
    let sa = socket(endpoint_address(&c));
    (sa.address.clone(), port_value(sa))
}

#[test]
fn set_otlp_collector() {
    for (name, addr, host, port) in [
        (
            "HostPort",
            "collector.otel-system.svc:4317",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "HostOnlyDefaultsPort",
            "collector.otel-system.svc",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "HttpURL",
            "http://collector.otel-system.svc:4317",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "HttpURLNoPort",
            "http://collector.otel-system.svc",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "HttpURLTrailingSlash",
            "http://collector.otel-system.svc:4317/",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "HttpURLWithPath",
            "http://collector.otel-system.svc:4317/v1/traces",
            "collector.otel-system.svc",
            4317,
        ),
        (
            "NonDefaultPort",
            "http://collector.otel-system.svc:14317",
            "collector.otel-system.svc",
            14317,
        ),
        ("IPv6", "[::1]:4317", "::1", 4317),
        ("IPv6URL", "http://[::1]:4317", "::1", 4317),
    ] {
        let x = XdsServer::new(0);
        x.set_otlp_collector(addr)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let s = x.settings();
        assert_eq!((s.otlp_host.as_str(), s.otlp_port), (host, port), "{name}");
        assert_eq!(otlp_endpoint(&x), (host.to_string(), port), "{name}");
    }
}

#[test]
fn set_otlp_collector_rejects() {
    for (name, addr) in [
        ("Https", "https://collector.otel-system.svc:4317"),
        ("UnknownScheme", "grpc://collector.otel-system.svc:4317"),
        ("NoHost", "http://:4317"),
        ("NonNumericPort", "collector.otel-system.svc:grpc"),
    ] {
        assert!(
            XdsServer::new(0).set_otlp_collector(addr).is_err(),
            "{name}: {addr} accepted"
        );
    }
}

#[test]
fn set_otlp_collector_empty_disables_tracing() {
    let x = XdsServer::new(0);
    set_ingress_tls(&x);
    x.set_otlp_collector("").unwrap();
    assert!(x.settings().build_tracing().is_none());
    let snap = publish(&x);
    assert!(
        !snap
            .resources(CLUSTER_TYPE)
            .unwrap()
            .contains_key(OTLP_CLUSTER_NAME),
        "OTLP cluster built without a collector"
    );
}

#[test]
fn build_tracing_random_sampling_from_policy() {
    const COLLECTOR: &str = "collector.otel-system.svc:4317";
    for (name, collector, percent, want_tracing, want_percent) in [
        (
            "percent mirrors the resolved policy",
            COLLECTOR,
            Some(1.0),
            true,
            1.0,
        ),
        ("full sampling", COLLECTOR, Some(100.0), true, 100.0),
        (
            "setter never called defaults to zero",
            COLLECTOR,
            None,
            true,
            0.0,
        ),
        (
            "no collector yields no tracing block",
            "",
            Some(100.0),
            false,
            0.0,
        ),
    ] {
        let x = XdsServer::new(0);
        x.set_otlp_collector(collector).unwrap();
        if let Some(p) = percent {
            x.set_trace_root_sampling_percent(p);
        }
        let tr = x.settings().build_tracing();
        assert_eq!(tr.is_some(), want_tracing, "{name}");
        if let Some(tr) = tr {
            assert_eq!(tr.random_sampling.unwrap().value, want_percent, "{name}");
        }
    }
}

#[test]
fn snapshot_versions_unique_across_restarts() {
    let mut seen = std::collections::HashSet::new();
    for _restart in 0..2 {
        let x = XdsServer::new(0);
        set_ingress_tls(&x);
        for _ in 0..3 {
            let v = publish(&x).version().to_string();
            assert!(seen.insert(v.clone()), "version {v} reused");
        }
    }
}

#[test]
fn alpn() {
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    set_ingress_tls(&server);
    server.set_connect_tls_port(8444);
    let listeners = snapshot_listeners(&server);
    let https = &listeners[INGRESS_HTTPS_LISTENER];
    assert_eq!(
        downstream_tls_context(https)
            .common_tls_context
            .unwrap()
            .alpn_protocols,
        vec!["h2", "http/1.1"]
    );
    let connect = downstream_tls_context(&listeners[CONNECT_TERMINATE_TLS_LISTENER]);
    assert!(
        connect
            .common_tls_context
            .unwrap()
            .alpn_protocols
            .is_empty(),
        "the CONNECT listener must not advertise ALPN"
    );
    assert_eq!(hcm_of(https).codec_type, CodecType::Auto as i32);
}

#[test]
fn update_snapshot_downstream_client_auth() {
    const DIR: &str = "/run/podidentity.podcert.hivekeep.io";
    let allowed = vec![
        "spiffe://cluster.local/ns/demo/sa/client".to_string(),
        "spiffe://cluster.local/ns/other/sa/client".to_string(),
    ];
    let server = XdsServer::new(18000);
    server.set_config(50053, "127.0.0.1");
    server.set_connect_tls_port(8444);
    server.set_tls_config(8443, TEST_CERT_PATH);
    server.set_downstream_client_auth(TEST_CA_PATH, &allowed);
    let listeners = snapshot_listeners(&server);
    for name in [INGRESS_HTTPS_LISTENER, CONNECT_TERMINATE_TLS_LISTENER] {
        let dtc = downstream_tls_context(&listeners[name]);
        assert!(
            dtc.require_client_certificate.unwrap().value,
            "{name} does not require client certificates"
        );
        let common = dtc.common_tls_context.unwrap();
        let combined = combined(&common);
        let trust = combined
            .validation_context_sds_secret_config
            .as_ref()
            .unwrap();
        assert_eq!(trust.name, INGRESS_CLIENT_TRUST_SECRET_NAME);
        assert!(uses_ads(&trust.sds_config));
        let matchers = &combined
            .default_validation_context
            .as_ref()
            .unwrap()
            .match_typed_subject_alt_names;
        assert_eq!(matchers.len(), allowed.len());
        for (m, want) in matchers.iter().zip(&allowed) {
            assert_eq!(m.san_type, SanType::Uri as i32);
            assert_eq!(
                m.matcher.as_ref().unwrap().match_pattern,
                Some(MatchPattern::Exact(want.clone()))
            );
        }
    }
    let secrets = snapshot_secrets(&server);
    let trust = validation_context(&secrets[INGRESS_CLIENT_TRUST_SECRET_NAME]);
    assert_eq!(filename(&trust.trusted_ca), TEST_CA_PATH);
    assert_eq!(trust.watched_directory.as_ref().unwrap().path, DIR);
}

#[test]
fn update_snapshot_tls_listener_without_client_auth() {
    for (name, https_port, connect_tls_port, ca_path, allowed, want_err) in [
        (
            "HTTPS without allowlist",
            8443,
            0,
            TEST_CA_PATH,
            vec![],
            false,
        ),
        (
            "CONNECT-TLS without allowlist",
            0,
            8444,
            TEST_CA_PATH,
            vec![],
            false,
        ),
        (
            "HTTPS without CA",
            8443,
            0,
            "",
            vec![TEST_CLIENT_ID.to_string()],
            true,
        ),
        (
            "CONNECT-TLS without CA",
            0,
            8444,
            "",
            vec![TEST_CLIENT_ID.to_string()],
            true,
        ),
    ] {
        let server = XdsServer::new(18000);
        server.set_config(50053, "127.0.0.1");
        server.set_connect_tls_port(connect_tls_port);
        server.set_tls_config(https_port, TEST_CERT_PATH);
        server.set_downstream_client_auth(ca_path, &allowed);
        if want_err {
            assert!(
                server.update_snapshot().is_err(),
                "{name}: a TLS listener without a client CA was accepted"
            );
            continue;
        }
        let listeners = snapshot_listeners(&server);
        for l in [INGRESS_HTTPS_LISTENER, CONNECT_TERMINATE_TLS_LISTENER] {
            assert!(
                !listeners.contains_key(l),
                "{name}: {l} built without an allowlist"
            );
        }
    }
}

#[test]
fn xds_listen_address_is_loopback() {
    use crate::router::dataplane::xds_listen_address;
    for port in [0u16, 18000] {
        let addr: std::net::SocketAddr = xds_listen_address(port).parse().unwrap();
        assert!(addr.ip().is_loopback(), "{addr} is not loopback");
        assert_eq!(addr.port(), port);
    }
    let lis = std::net::TcpListener::bind(xds_listen_address(0)).unwrap();
    assert!(lis.local_addr().unwrap().ip().is_loopback());
}

#[test]
fn snapshot_consistency_catches_missing_routes() {
    let settings = XdsSettings {
        https_port: 8443,
        cert_path: TEST_CERT_PATH.into(),
        downstream_client_ca_path: TEST_CA_PATH.into(),
        downstream_allowed_spiffe_ids: vec![TEST_CLIENT_ID.into()],
        ..Default::default()
    };
    let snap = settings.build_snapshot("v1").unwrap();
    let listeners = snap.decoded::<Listener>().into_values().collect();
    let broken = Snapshot::new("v1", Vec::new(), Vec::new(), listeners, Vec::new()).unwrap();
    assert!(
        broken.consistent().is_err(),
        "a listener referencing a missing route passed"
    );
}

#[tokio::test]
async fn ads_ignores_acks_and_pushes_updates() {
    use envoy_types::pb::envoy::service::discovery::v3::aggregated_discovery_service_client::AggregatedDiscoveryServiceClient;
    let server = XdsServer::new(18000);
    set_ingress_tls(&server);
    let lis = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let _guard = cancel.clone().drop_guard();
    {
        let server = server.clone();
        tokio::spawn(async move { server.serve(lis, cancel).await });
    }
    let mut client = AggregatedDiscoveryServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let request = |version: &str, nonce: &str| DiscoveryRequest {
        node: Some(Node {
            id: NODE_ID.to_string(),
            ..Default::default()
        }),
        type_url: CLUSTER_TYPE.to_string(),
        version_info: version.to_string(),
        response_nonce: nonce.to_string(),
        ..Default::default()
    };
    tx.send(request("", "")).await.unwrap();
    let mut stream = client
        .stream_aggregated_resources(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.resources.len(), 2);

    tx.send(request(&first.version_info, &first.nonce))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(200), stream.message())
            .await
            .is_err(),
        "an ACK was answered"
    );

    server.set_upstream_connect(true);
    server.set_upstream_tls("/run/bundle.pem", "");
    server.update_snapshot().unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), stream.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_ne!(second.version_info, first.version_info);
    assert_eq!(second.resources.len(), 4);
}
