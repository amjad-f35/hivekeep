use std::path::Path;
use std::time::{Duration, SystemTime};

use hivekeep_k8s::install_defaults::{egress_spiffe_id, SYSTEM_NAMESPACE};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio_rustls::TlsConnector;

use super::*;

fn ring() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A self-signed CA issuing test certificates.
struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    der: Vec<u8>,
    pem: String,
}

impl TestCa {
    fn new(cn: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        let cert = params.self_signed(&key).unwrap();
        TestCa {
            der: cert.der().to_vec(),
            pem: cert.pem(),
            issuer: Issuer::new(params, key),
        }
    }

    fn issue(&self, dns: &[&str], uris: &[&str]) -> Leaf {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.distinguished_name.push(DnType::CommonName, "leaf");
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.subject_alt_names = dns
            .iter()
            .map(|d| SanType::DnsName((*d).try_into().unwrap()))
            .chain(uris.iter().map(|u| SanType::URI((*u).try_into().unwrap())))
            .collect();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        Leaf {
            der: cert.der().to_vec(),
            pem: cert.pem(),
            key,
        }
    }
}

struct Leaf {
    der: Vec<u8>,
    pem: String,
    key: KeyPair,
}

fn injector() -> String {
    egress_spiffe_id(SYSTEM_NAMESPACE)
}

#[test]
fn verify_client_san_cases() {
    let ca = TestCa::new("ca");
    let id = injector();
    let cases: [(&str, Option<Leaf>, bool); 5] = [
        ("matching SAN", Some(ca.issue(&[], &[&id])), true),
        (
            "matching SAN among several",
            Some(ca.issue(&[], &["spiffe://cluster.local/ns/other/sa/x", &id])),
            true,
        ),
        (
            "wrong SAN",
            Some(ca.issue(
                &[],
                &["spiffe://cluster.local/ns/hivekeep-system/sa/impostor"],
            )),
            false,
        ),
        ("no URI SANs", Some(ca.issue(&["x.test"], &[])), false),
        ("no peer certificate", None, false),
    ];
    for (name, leaf, ok) in cases {
        let got = verify_client_san(leaf.as_ref().map(|l| l.der.as_slice()), &id);
        assert_eq!(got.is_ok(), ok, "{name}: {got:?}");
    }
}

fn write_with_mtime(path: &Path, data: &[u8], mtime: SystemTime) {
    std::fs::write(path, data).unwrap();
    std::fs::File::options()
        .write(true)
        .open(path)
        .unwrap()
        .set_modified(mtime)
        .unwrap();
}

/// One handshake against `tls`, presenting `client`. Reports the server's
/// failure first, as that is where the client certificate is judged.
async fn handshake(tls: &MutualTls, server_ca: &TestCa, client: &Leaf) -> Result<(), String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(server_ca.der.clone()))
        .unwrap();
    let mut cfg = ClientConfig::builder_with_provider(ring())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            vec![CertificateDer::from(client.der.clone())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client.key.serialize_der())),
        )
        .unwrap();
    cfg.alpn_protocols = vec![b"h2".to_vec()];
    let connector = TlsConnector::from(Arc::new(cfg));

    // On success the server sends one byte and keeps the stream open until
    // the client has read it; on rejection the client reads the alert.
    let server = async {
        use tokio::io::AsyncWriteExt;
        let (tcp, _) = listener.accept().await.unwrap();
        let mut s = tls.accept(tcp).await?;
        s.write_all(b"x").await?;
        s.flush().await?;
        Ok::<_, io::Error>(s)
    };
    let client = async {
        use tokio::io::AsyncReadExt;
        let tcp = TcpStream::connect(addr).await.unwrap();
        let name = ServerName::try_from("credprovider.test").unwrap();
        let mut stream = connector.connect(name, tcp).await?;
        let mut buf = [0u8; 1];
        match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
            Ok(Ok(1)) => Ok(()),
            Ok(Ok(_)) => Err(io::Error::other("connection closed")),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(io::Error::other("timed out")),
        }
    };
    let (server, client) = tokio::join!(server, client);
    server.map_err(|e| format!("server: {e}"))?;
    client.map_err(|e| format!("client: {e}"))
}

/// Drives real handshakes, then republishes the client CA file and checks
/// that a certificate from the new CA verifies on a new connection without
/// rebuilding anything, while the chain and SAN checks still hold.
#[tokio::test]
async fn reloads_client_ca() {
    let dir = tempfile::tempdir().unwrap();
    let server_ca = TestCa::new("server-ca");
    let server_leaf = server_ca.issue(&["credprovider.test"], &[]);
    let bundle_path = dir.path().join("server-bundle.pem");
    std::fs::write(
        &bundle_path,
        format!("{}{}", server_leaf.pem, server_leaf.key.serialize_pem()),
    )
    .unwrap();

    let client_ca1 = TestCa::new("client-ca-1");
    let client_ca2 = TestCa::new("client-ca-2");
    let ca_path = dir.path().join("client-ca.pem");
    write_with_mtime(&ca_path, client_ca1.pem.as_bytes(), SystemTime::now());

    let tls = MutualTls::new(
        bundle_path.to_str().unwrap(),
        ca_path.to_str().unwrap(),
        &injector(),
        ring(),
    )
    .unwrap();

    let id = injector();
    let from_ca1 = client_ca1.issue(&[], &[&id]);
    let from_ca2 = client_ca2.issue(&[], &[&id]);
    let wrong_san = client_ca1.issue(
        &[],
        &["spiffe://cluster.local/ns/hivekeep-system/sa/impostor"],
    );

    handshake(&tls, &server_ca, &from_ca1)
        .await
        .expect("CA1 client before rotation");
    assert!(
        handshake(&tls, &server_ca, &from_ca2).await.is_err(),
        "CA2 client accepted before rotation"
    );
    assert!(
        handshake(&tls, &server_ca, &wrong_san).await.is_err(),
        "wrong-SAN client accepted"
    );

    write_with_mtime(
        &ca_path,
        client_ca2.pem.as_bytes(),
        SystemTime::now() + Duration::from_secs(1),
    );

    handshake(&tls, &server_ca, &from_ca2)
        .await
        .expect("CA2 client after rotation");
    assert!(
        handshake(&tls, &server_ca, &from_ca1).await.is_err(),
        "CA1 client accepted after rotation"
    );
}

#[test]
fn new_requires_a_client_ca() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent.pem");
    assert!(MutualTls::new("bundle.pem", missing.to_str().unwrap(), &injector(), ring()).is_err());
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "").unwrap();
    assert!(MutualTls::new("bundle.pem", empty.to_str().unwrap(), &injector(), ring()).is_err());
}
