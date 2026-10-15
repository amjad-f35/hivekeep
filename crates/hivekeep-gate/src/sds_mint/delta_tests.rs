use std::time::Duration;

use envoy_types::pb::google::rpc::Status as RpcStatus;
use hivekeep_identity::ca::{Ca, CaPool};
use hivekeep_identity::host_cert::check_host;
use hivekeep_identity::key::KeyAlgorithm;
use tokio_stream::wrappers::ReceiverStream;

use super::*;

pub(crate) fn test_minter(ttl: Duration) -> Arc<HostCertMinter> {
    let ca = Ca::generate("mitm", KeyAlgorithm::EcdsaP256, Duration::from_secs(3600)).unwrap();
    let pool = CaPool::new(vec![ca], "mitm");
    Arc::new(HostCertMinter::new(Arc::new(pool), ttl).unwrap())
}

struct Stream {
    requests: mpsc::Sender<Result<DeltaDiscoveryRequest, tonic::Status>>,
    sent: mpsc::Receiver<DeltaDiscoveryResponse>,
    done: tokio::task::JoinHandle<Result<(), WrongTypeUrl>>,
}

fn start() -> Stream {
    let (req_tx, req_rx) = mpsc::channel(8);
    let (out_tx, out_rx) = mpsc::channel(8);
    let done = tokio::spawn(serve_stream(
        test_minter(super::super::DEFAULT_TTL),
        Arc::default(),
        ReceiverStream::new(req_rx),
        out_tx,
    ));
    Stream {
        requests: req_tx,
        sent: out_rx,
        done,
    }
}

impl Stream {
    async fn send(&self, r: DeltaDiscoveryRequest) {
        self.requests.send(Ok(r)).await.unwrap();
    }
    async fn next(&mut self) -> DeltaDiscoveryResponse {
        tokio::time::timeout(Duration::from_secs(60), self.sent.recv())
            .await
            .expect("timed out waiting for a response")
            .unwrap()
    }
    async fn quiet(&mut self, what: &str) {
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(self.sent.try_recv().is_err(), "sent a response {what}");
    }
}

fn subscribe(names: &[&str]) -> DeltaDiscoveryRequest {
    DeltaDiscoveryRequest {
        type_url: SECRET_TYPE_URL.into(),
        resource_names_subscribe: names.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

fn secret_of(r: &Resource) -> Secret {
    Secret::decode(r.resource.as_ref().unwrap().value.as_slice()).unwrap()
}

fn chain_and_key(s: &Secret) -> (Vec<u8>, Vec<u8>) {
    let Some(secret::Type::TlsCertificate(tc)) = &s.r#type else {
        panic!("not a TLS certificate")
    };
    let get = |d: &Option<DataSource>| match d.as_ref().and_then(|d| d.specifier.as_ref()) {
        Some(Specifier::InlineBytes(b)) => b.clone(),
        _ => panic!("not inline"),
    };
    (get(&tc.certificate_chain), get(&tc.private_key))
}

fn leaf_of(r: &Resource) -> Vec<u8> {
    let (chain, _) = chain_and_key(&secret_of(r));
    let first = rustls_pemfile::certs(&mut chain.as_slice())
        .next()
        .unwrap()
        .unwrap()
        .to_vec();
    first
}

#[tokio::test]
async fn mints_subscribed_name() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    let resp = s.next().await;
    assert_eq!(resp.type_url, SECRET_TYPE_URL);
    assert!(!resp.nonce.is_empty(), "Envoy needs a nonce to ACK");
    assert_eq!(resp.resources.len(), 1);
    let r = &resp.resources[0];
    assert_eq!(r.name, "a.example");
    assert!(
        !r.version.is_empty(),
        "delta xDS needs a version per resource"
    );
    let secret = secret_of(r);
    assert_eq!(secret.name, "a.example");
    let (chain, key) = chain_and_key(&secret);
    let certs: Vec<_> = rustls_pemfile::certs(&mut chain.as_slice())
        .collect::<Result<_, _>>()
        .unwrap();
    let key = rustls_pemfile::private_key(&mut key.as_slice())
        .unwrap()
        .unwrap();
    rustls::crypto::ring::sign::any_supported_type(&key).expect("key parses");
    assert!(!certs.is_empty());
}

#[tokio::test]
async fn stamps_resource_ttl() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    let r = s.next().await.resources.remove(0);
    let ttl = r
        .ttl
        .as_ref()
        .expect("a resource without ttl would be held past notAfter");
    let leaf = leaf_of(&r);
    let (_, cert) = x509_parser::parse_x509_certificate(&leaf).unwrap();
    let remaining = cert.validity().not_after.timestamp() - chrono::Utc::now().timestamp();
    assert!(
        ttl.seconds < remaining,
        "ttl {} must be below the leaf's remaining {}",
        ttl.seconds,
        remaining
    );
}

#[tokio::test]
async fn withdraws_refused_name() {
    let mut s = start();
    s.send(subscribe(&["ok.allowed", "*.evil.test"])).await;
    let resp = s.next().await;
    assert_eq!(
        resp.resources
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["ok.allowed"]
    );
    assert_eq!(resp.removed_resources, ["*.evil.test"]);
}

#[tokio::test]
async fn bare_ack_sends_nothing() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    let first = s.next().await;
    s.send(DeltaDiscoveryRequest {
        type_url: SECRET_TYPE_URL.into(),
        response_nonce: first.nonce,
        ..Default::default()
    })
    .await;
    s.quiet("in reply to a bare ACK").await;
}

#[tokio::test(start_paused = true)]
async fn never_pushes_unprompted() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    s.next().await;
    tokio::time::sleep(2 * super::super::DEFAULT_TTL).await;
    s.quiet("after the only subscribed leaf expired").await;
}

#[tokio::test]
async fn ignores_unsubscribe() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    s.next().await;
    s.send(DeltaDiscoveryRequest {
        type_url: SECRET_TYPE_URL.into(),
        resource_names_unsubscribe: vec!["a.example".into()],
        ..Default::default()
    })
    .await;
    s.quiet("in reply to an unsubscribe").await;
    s.send(subscribe(&["a.example"])).await;
    assert_eq!(s.next().await.resources[0].name, "a.example");
}

#[tokio::test]
async fn replay_only_request_is_silent() {
    let mut s = start();
    s.send(DeltaDiscoveryRequest {
        type_url: SECRET_TYPE_URL.into(),
        initial_resource_versions: [("resumed.example".to_string(), "old-version".to_string())]
            .into(),
        ..Default::default()
    })
    .await;
    s.quiet("in reply to a replay-only request").await;
}

#[tokio::test]
async fn rejects_wrong_type_url() {
    let s = start();
    s.send(DeltaDiscoveryRequest {
        type_url: "type.googleapis.com/envoy.config.cluster.v3.Cluster".into(),
        resource_names_subscribe: vec!["a.example".into()],
        ..Default::default()
    })
    .await;
    let r = tokio::time::timeout(Duration::from_secs(60), s.done)
        .await
        .unwrap()
        .unwrap();
    assert!(r.is_err(), "accepted a non-SDS type_url");
}

#[tokio::test]
async fn survives_nack() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    let first = s.next().await;
    s.send(DeltaDiscoveryRequest {
        type_url: SECRET_TYPE_URL.into(),
        response_nonce: first.nonce,
        error_detail: Some(RpcStatus {
            code: 3,
            message: "bad certificate".into(),
            details: vec![],
        }),
        ..Default::default()
    })
    .await;
    s.send(subscribe(&["b.example"])).await;
    assert_eq!(
        s.next()
            .await
            .resources
            .iter()
            .map(|r| r.name.as_str())
            .collect::<Vec<_>>(),
        ["b.example"]
    );
}

#[tokio::test]
async fn resubscribe_is_minted_again() {
    let mut s = start();
    s.send(subscribe(&["a.example"])).await;
    let first = s.next().await;
    s.send(subscribe(&["a.example"])).await;
    let second = s.next().await;
    assert_eq!(second.resources.len(), 1);
    assert_eq!(second.resources[0].name, "a.example");
    let leaf = leaf_of(&second.resources[0]);
    let (_, cert) = x509_parser::parse_x509_certificate(&leaf).unwrap();
    let names: Vec<String> = cert
        .subject_alternative_name()
        .unwrap()
        .unwrap()
        .value
        .general_names
        .iter()
        .filter_map(|n| match n {
            x509_parser::extensions::GeneralName::DNSName(d) => Some(d.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(names, ["a.example"]);
    assert_ne!(first.resources[0].version, second.resources[0].version);
}

#[test]
fn minter_mints_per_call() {
    let m = test_minter(Duration::from_secs(60));
    let now = SystemTime::now();
    let first = m.certificate("a.example", now).unwrap();
    let second = m.certificate("a.example", now).unwrap();
    assert_ne!(first.serial, second.serial);
    let other = m.certificate("b.example", now).unwrap();
    assert_ne!(
        other.serial, first.serial,
        "different hosts were served the same certificate"
    );
}

#[test]
fn minter_refuses_non_hostnames() {
    let m = test_minter(Duration::from_secs(60));
    m.certificate("anything.at.all.test", SystemTime::now())
        .unwrap();
    assert!(
        m.certificate("*.evil.test", SystemTime::now()).is_err(),
        "a wildcard SNI was minted"
    );
}

#[test]
fn minter_is_concurrency_safe() {
    let m = test_minter(Duration::from_secs(60));
    std::thread::scope(|s| {
        for i in 0..64 {
            let m = &m;
            s.spawn(move || {
                m.certificate(&format!("h{}.example", i % 16), SystemTime::now())
                    .unwrap()
            });
        }
    });
}

#[test]
fn check_host_syntax() {
    let long_label = format!("{}.example.com", "a".repeat(63));
    for h in [
        "example.com",
        "a.example.com",
        "a.b.c.d.example.com",
        "EXAMPLE.COM",
        "anything.test",
        "localhost",
        "xn--80ak6aa92e.com",
        "a.example.com.",
        "my-host.example.com",
        "_dmarc.example.com",
        long_label.as_str(),
        "192.0.2.1",
        "127.0.0.1",
        "2001:db8::1",
        "::1",
        "::ffff:192.0.2.1",
    ] {
        assert!(check_host(h).is_ok(), "{h:?} should be allowed");
    }
    let too_long_label = format!("{}.example.com", "a".repeat(64));
    let too_long = format!("{}example.com", "a.".repeat(200));
    for h in [
        "",
        ".",
        "*.example.com",
        "a.example.com/../x",
        "..example.com",
        ".example.com",
        "a.example.com\nfoo",
        "-example.com",
        "example-.com",
        "[::1]",
        "fe80::1%eth0",
        "192.0.2.1:8443",
        too_long_label.as_str(),
        too_long.as_str(),
    ] {
        assert!(check_host(h).is_err(), "{h:?} should be refused");
    }
}
