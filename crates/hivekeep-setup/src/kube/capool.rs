//! Reading the root certificates out of a CA pool Secret.

use super::{Client, KubeError};
use hivekeep_identity::ca::CaPool;

/// PEM-encodes DER certificates, one block each.
pub fn pem_encode_certs<'a>(ders: impl IntoIterator<Item = &'a [u8]>) -> Vec<u8> {
    use base64::Engine;
    let mut out = String::new();
    for der in ders {
        out.push_str("-----BEGIN CERTIFICATE-----\n");
        let b64 = base64::engine::general_purpose::STANDARD.encode(der);
        for chunk in b64.as_bytes().chunks(64) {
            out.push_str(std::str::from_utf8(chunk).expect("base64 is ASCII"));
            out.push('\n');
        }
        out.push_str("-----END CERTIFICATE-----\n");
    }
    out.into_bytes()
}

/// The PEM roots of a serialised CA pool.
pub fn pool_root_pem(pool_bytes: &[u8], namespace: &str, name: &str) -> Result<Vec<u8>, KubeError> {
    let pool = CaPool::from_json(pool_bytes).map_err(|e| {
        KubeError::Decode(format!("while decoding CA pool {namespace}/{name}: {e}"))
    })?;
    if pool.cas().is_empty() {
        return Err(KubeError::Other(format!(
            "CA pool {namespace}/{name} contains no CAs"
        )));
    }
    for ca in pool.cas() {
        if ca.root_der().is_empty() {
            return Err(KubeError::Other(format!(
                "CA {:?} in pool {namespace}/{name} has no root certificate",
                ca.id()
            )));
        }
    }
    Ok(pem_encode_certs(pool.cas().iter().map(|c| c.root_der())))
}

impl Client {
    /// The PEM-encoded root of every CA in the pool stored in the Secret's
    /// `pool` key.
    pub async fn ca_pool_root_pem(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Vec<u8>, KubeError> {
        let secret = self.get_secret(namespace, name).await?.ok_or_else(|| {
            KubeError::Other(format!("CA pool secret {namespace}/{name} not found"))
        })?;
        let pool = secret
            .data
            .as_ref()
            .and_then(|d| d.get("pool"))
            .ok_or_else(|| {
                KubeError::Other(format!(
                    "CA pool secret {namespace}/{name} has no \"pool\" key"
                ))
            })?;
        pool_root_pem(&pool.0, namespace, name)
    }
}
