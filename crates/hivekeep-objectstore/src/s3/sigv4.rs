//! AWS Signature Version 4 for S3 requests.

use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::http::{encode_component, Request};

/// Static credentials for one signature.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    /// The access key id.
    pub access_key_id: String,
    /// The secret key.
    pub secret_access_key: String,
    /// The session token of temporary credentials.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut m = <Hmac<Sha256> as KeyInit>::new_from_slice(key).expect("HMAC takes any key length");
    m.update(data);
    m.finalize().into_bytes().to_vec()
}

/// Hex SHA-256.
pub fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// The canonical query string: pairs sorted, both sides strictly encoded.
pub fn canonical_query(req: &Request) -> String {
    let mut pairs: Vec<(String, String)> = req
        .url
        .query_pairs()
        .map(|(k, v)| (encode_component(&k), encode_component(&v)))
        .collect();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Signs `req` in place for `region` at `now`.
pub fn sign(req: &mut Request, creds: &Credentials, region: &str, now: DateTime<Utc>) {
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload = sha256_hex(&req.body);
    let host = match req.url.port() {
        Some(p) => format!("{}:{p}", req.url.host_str().unwrap_or_default()),
        None => req.url.host_str().unwrap_or_default().to_string(),
    };
    let r = std::mem::replace(req, Request::new(req.method.clone(), req.url.clone()));
    let mut r = r
        .header("host", &host)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload);
    if let Some(t) = &creds.session_token {
        r = r.header("x-amz-security-token", t);
    }
    let mut names: Vec<String> = r
        .headers
        .keys()
        .map(|k| k.as_str().to_ascii_lowercase())
        .filter(|k| k == "host" || k.starts_with("x-amz-") || k == "content-type" || k == "range")
        .collect();
    names.sort();
    names.dedup();
    let canonical_headers: String = names
        .iter()
        .map(|n| {
            let v = r.header_str(n).unwrap_or_default().trim().to_string();
            format!("{n}:{v}\n")
        })
        .collect();
    let signed = names.join(";");
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        r.method.as_str(),
        if r.url.path().is_empty() {
            "/"
        } else {
            r.url.path()
        },
        canonical_query(&r),
        canonical_headers,
        signed,
        payload
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let k = hmac(
        format!("AWS4{}", creds.secret_access_key).as_bytes(),
        date.as_bytes(),
    );
    let k = hmac(&k, region.as_bytes());
    let k = hmac(&k, b"s3");
    let k = hmac(&k, b"aws4_request");
    let signature = hex::encode(hmac(&k, to_sign.as_bytes()));
    r = r.header(
        "authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={signature}",
            creds.access_key_id
        ),
    );
    *req = r;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use http::Method;

    // The GET Object example of the SigV4 documentation.
    #[test]
    fn matches_the_published_example() {
        let mut req = Request::new(
            Method::GET,
            url::Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap(),
        )
        .header("range", "bytes=0-9");
        let creds = Credentials {
            access_key_id: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            session_token: None,
        };
        let now = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();
        sign(&mut req, &creds, "us-east-1", now);
        assert_eq!(
            req.header_str("authorization").unwrap(),
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }
}
