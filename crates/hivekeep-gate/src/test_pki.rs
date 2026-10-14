//! Test PKI: a throwaway CA issuing leaf certificates, and rustls configs
//! built from them.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use rcgen::{
    BasicConstraints, CertificateParams, CustomExtension, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, SanType, SerialNumber,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;

/// The ring provider every test config uses.
pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// A self-signed test CA.
pub struct TestCa {
    params: CertificateParams,
    key: KeyPair,
    /// The root certificate, DER.
    pub der: Vec<u8>,
}

/// A leaf certificate and its key.
#[derive(Clone)]
pub struct Leaf {
    /// The leaf, DER.
    pub der: Vec<u8>,
    /// The chain after the leaf, DER.
    pub chain: Vec<Vec<u8>>,
    /// The PKCS#8 key, DER.
    pub key_der: Vec<u8>,
}

/// What to put in a leaf.
#[derive(Default, Clone)]
pub struct LeafOpts {
    /// URI SAN (SPIFFE ID).
    pub uri: Option<String>,
    /// DNS SANs.
    pub dns: Vec<String>,
    /// Client auth EKU.
    pub client: bool,
    /// Server auth EKU.
    pub server: bool,
    /// Private extensions.
    pub extensions: Vec<CustomExtension>,
    /// Validity relative to now (default one hour).
    pub lifetime: Option<Duration>,
    /// Backdating (default one minute).
    pub backdate: Option<Duration>,
    /// Extra URI SANs (any scheme).
    pub uris: Vec<String>,
    /// Marks the leaf as a CA.
    pub is_ca: bool,
    /// Explicit validity window, overriding lifetime and backdate.
    pub validity: Option<(SystemTime, SystemTime)>,
}

fn serial() -> SerialNumber {
    SerialNumber::from_slice(uuid::Uuid::new_v4().as_bytes())
}

impl TestCa {
    /// A fresh CA valid for an hour.
    pub fn new() -> TestCa {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::default();
        params.distinguished_name.push(
            DnType::CommonName,
            format!("test CA {}", uuid::Uuid::new_v4()),
        );
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let now = SystemTime::now();
        params.not_before = (now - Duration::from_secs(60)).into();
        params.not_after = (now + Duration::from_secs(3600)).into();
        params.serial_number = Some(serial());
        let der = params.self_signed(&key).unwrap().der().to_vec();
        TestCa { params, key, der }
    }

    /// A copy sharing the same key and root.
    pub fn clone_of(other: &TestCa) -> TestCa {
        let key = KeyPair::try_from(other.key.serialize_der()).unwrap();
        TestCa {
            params: other.params.clone(),
            key,
            der: other.der.clone(),
        }
    }

    /// The root, PEM.
    pub fn pem(&self) -> String {
        hivekeep_identity::pem::encode(hivekeep_identity::pem::CERTIFICATE, &self.der)
    }

    /// Issues a leaf.
    pub fn issue(&self, opts: &LeafOpts) -> Leaf {
        let key = KeyPair::generate().unwrap();
        self.issue_for(&key, opts)
    }

    /// Issues a leaf for an existing key.
    pub fn issue_for(&self, key: &KeyPair, opts: &LeafOpts) -> Leaf {
        let mut p = CertificateParams::default();
        p.distinguished_name.push(DnType::CommonName, "leaf");
        if let Some(uri) = &opts.uri {
            p.subject_alt_names
                .push(SanType::URI(uri.clone().try_into().unwrap()));
        }
        for d in &opts.dns {
            p.subject_alt_names
                .push(SanType::DnsName(d.clone().try_into().unwrap()));
        }
        for u in &opts.uris {
            p.subject_alt_names
                .push(SanType::URI(u.clone().try_into().unwrap()));
        }
        if opts.is_ca {
            p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        }
        if opts.client {
            p.extended_key_usages
                .push(ExtendedKeyUsagePurpose::ClientAuth);
        }
        if opts.server {
            p.extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
        }
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        p.custom_extensions = opts.extensions.clone();
        let now = SystemTime::now();
        let backdate = opts.backdate.unwrap_or(Duration::from_secs(60));
        p.not_before = (now - backdate).into();
        let lifetime = opts.lifetime.unwrap_or(Duration::from_secs(3600));
        p.not_after = (now + lifetime).into();
        if let Some((nb, na)) = opts.validity {
            p.not_before = nb.into();
            p.not_after = na.into();
        }
        p.serial_number = Some(serial());
        let issuer = Issuer::from_params(&self.params, &self.key);
        let der = p.signed_by(key, &issuer).unwrap().der().to_vec();
        Leaf {
            der,
            chain: vec![self.der.clone()],
            key_der: key.serialize_der(),
        }
    }

    /// Issues a leaf for a public key known only by its SPKI, returning DER.
    pub fn issue_public(&self, key: &impl rcgen::PublicKeyData, opts: &LeafOpts) -> Vec<u8> {
        let mut p = CertificateParams::default();
        p.distinguished_name.push(DnType::CommonName, "leaf");
        if let Some(uri) = &opts.uri {
            p.subject_alt_names
                .push(SanType::URI(uri.clone().try_into().unwrap()));
        }
        if opts.client {
            p.extended_key_usages
                .push(ExtendedKeyUsagePurpose::ClientAuth);
        }
        if opts.server {
            p.extended_key_usages
                .push(ExtendedKeyUsagePurpose::ServerAuth);
        }
        p.custom_extensions = opts.extensions.clone();
        let now = SystemTime::now();
        p.not_before = (now - opts.backdate.unwrap_or(Duration::from_secs(60))).into();
        p.not_after = (now + opts.lifetime.unwrap_or(Duration::from_secs(3600))).into();
        p.serial_number = Some(serial());
        let issuer = Issuer::from_params(&self.params, &self.key);
        p.signed_by(key, &issuer).unwrap().der().to_vec()
    }

    /// A server leaf for `dns`.
    pub fn server(&self, dns: &str) -> Leaf {
        self.issue(&LeafOpts {
            dns: vec![dns.into()],
            server: true,
            ..Default::default()
        })
    }

    /// A root store trusting this CA.
    pub fn roots(&self) -> rustls::RootCertStore {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from(self.der.clone())).unwrap();
        roots
    }
}

impl Default for TestCa {
    fn default() -> Self {
        Self::new()
    }
}

impl Leaf {
    /// The leaf and chain as rustls certificates.
    pub fn certs(&self) -> Vec<CertificateDer<'static>> {
        std::iter::once(&self.der)
            .chain(&self.chain)
            .map(|d| CertificateDer::from(d.clone()))
            .collect()
    }

    /// The key as a rustls key.
    pub fn key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone()))
    }

    /// The pair as a signing-ready rustls key.
    pub fn certified(&self) -> Arc<CertifiedKey> {
        let signer = rustls::crypto::ring::sign::any_supported_type(&self.key()).unwrap();
        Arc::new(CertifiedKey::new(self.certs(), signer))
    }

    /// A credential bundle file body: chain then key, PEM.
    pub fn bundle_pem(&self) -> String {
        use hivekeep_identity::pem;
        let mut out = String::new();
        for d in std::iter::once(&self.der).chain(&self.chain) {
            out.push_str(&pem::encode(pem::CERTIFICATE, d));
        }
        out.push_str(&pem::encode("PRIVATE KEY", &self.key_der));
        out
    }
}

/// A TLS server config presenting `leaf`, requiring client certificates
/// from `client_ca` when given.
pub fn server_config(
    leaf: &Leaf,
    client_ca: Option<&TestCa>,
    alpn: &[&[u8]],
    versions: &[&'static rustls::SupportedProtocolVersion],
) -> Arc<rustls::ServerConfig> {
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(versions)
        .unwrap();
    let builder = match client_ca {
        Some(ca) => builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(ca.roots()),
                provider(),
            )
            .build()
            .unwrap(),
        ),
        None => builder.with_no_client_auth(),
    };
    let mut cfg = builder.with_single_cert(leaf.certs(), leaf.key()).unwrap();
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(cfg)
}

/// Every TLS version rustls supports.
pub const ALL_VERSIONS: &[&rustls::SupportedProtocolVersion] = rustls::ALL_VERSIONS;

/// Accepts any server certificate (tests that exercise client auth only).
#[derive(Debug)]
pub struct NoVerify;

impl rustls::client::danger::ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            m,
            c,
            d,
            &provider().signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        m: &[u8],
        c: &CertificateDer<'_>,
        d: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            m,
            c,
            d,
            &provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A client config presenting `leaf` and trusting any server.
pub fn insecure_client(leaf: Option<&Leaf>, alpn: &[&[u8]]) -> Arc<rustls::ClientConfig> {
    let b = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify));
    let mut cfg = match leaf {
        Some(l) => b.with_client_auth_cert(l.certs(), l.key()).unwrap(),
        None => b.with_no_client_auth(),
    };
    cfg.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    Arc::new(cfg)
}
