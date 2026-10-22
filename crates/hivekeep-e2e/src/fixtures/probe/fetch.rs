//! `/fetch`: an HTTP(S) GET through the Actor's egress path.
//!
//! Query parameters:
//!
//! - `url=<URL>`: required.
//! - `roots=bundle|system|pem`: the TLS trust anchors. `bundle` (the
//!   default) loads the projected trust bundle; `system` uses the image's
//!   system roots; `pem` trusts only the certificates in `ca`.
//! - `ca=<PEM>`: what `roots=pem` trusts. Required by `roots=pem` and refused
//!   with any other mode.
//! - `header=<name>:<value>`: repeatable; set on the request, so a suite can
//!   pre-seed a header and see whether the gateway overwrites it.
//! - `dial=<host:port>`: connect here instead of the URL's host, which still
//!   supplies the SNI and the Host header. A suite uses it to prove the
//!   gateway dials the name the SNI claims, not the address the Actor dialed.
//!
//! The reply is a JSON object with the origin's `status` and the first
//! 64 KiB of its `body`. Failures land in `error`, not the HTTP status: a TLS
//! verification failure is a result to assert on, not a broken probe.
//! `tls_handshake` is `complete`, `closed` or `failed` whenever TLS was tried.
//!
//! Redirects are not followed: a cross-scheme redirect would hop between the
//! gateway's cleartext and TLS legs and flip the very behaviour (credential
//! injection) some suites assert on.

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue, HOST};
use http::HeaderMap;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use url::{Host, Position, Url};

use super::roots;
use super::TRUST_FILE;

/// Caps the body `/fetch` echoes back, so a large origin response cannot
/// balloon the reply. Comfortably holds the header-echo documents the suites
/// assert on.
pub const MAX_FETCH_BODY: usize = 64 << 10;

/// The whole-fetch budget.
const FETCH_TIMEOUT: Duration = Duration::from_secs(20);

/// Parses repeated `header=<name>:<value>` parameters. The value is taken
/// verbatim after the first colon.
pub fn parse_fetch_headers<S: AsRef<str>>(params: &[S]) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    for p in params {
        let p = p.as_ref();
        let bad = || format!("header parameter {p:?} is not <name>:<value>");
        let (name, value) = p.split_once(':').ok_or_else(bad)?;
        if name.is_empty() {
            return Err(bad());
        }
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| format!("header parameter {p:?}: {e}"))?;
        let value =
            HeaderValue::from_str(value).map_err(|e| format!("header parameter {p:?}: {e}"))?;
        headers.append(name, value);
    }
    Ok(headers)
}

/// What one GET produced.
#[derive(Debug)]
pub struct Response {
    /// The status code.
    pub status: u16,
    /// Up to the requested number of body bytes.
    pub body: Vec<u8>,
    /// Why the body could not be read in full, if it could not.
    pub body_error: Option<String>,
}

/// How a TLS handshake ended.
fn handshake_result(err: Option<&io::Error>) -> &'static str {
    match err {
        None => "complete",
        Some(e)
            if matches!(
                e.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            "closed"
        }
        Some(_) => "failed",
    }
}

async fn send<S>(
    stream: S,
    url: &Url,
    headers: &HeaderMap,
    max_body: usize,
) -> Result<Response, String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake::<_, Empty<Bytes>>(TokioIo::new(stream))
            .await
            .map_err(|e| e.to_string())?;
    // The connection is used once; it ends with this task.
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut req = http::Request::get(&url[Position::BeforePath..Position::AfterQuery])
        .body(Empty::<Bytes>::new())
        .map_err(|e| e.to_string())?;
    let authority = &url[Position::BeforeHost..Position::AfterPort];
    req.headers_mut().insert(
        HOST,
        HeaderValue::from_str(authority).map_err(|e| e.to_string())?,
    );
    for (name, value) in headers {
        req.headers_mut().append(name.clone(), value.clone());
    }
    let res = sender.send_request(req).await.map_err(|e| e.to_string());
    let res = match res {
        Ok(r) => r,
        Err(e) => {
            driver.abort();
            return Err(e);
        }
    };
    let status = res.status().as_u16();
    let mut body = res.into_body();
    let mut out = Vec::new();
    let mut body_error = None;
    while out.len() < max_body {
        match body.frame().await {
            None => break,
            Some(Ok(frame)) => {
                if let Ok(data) = frame.into_data() {
                    let room = max_body - out.len();
                    out.extend_from_slice(&data[..data.len().min(room)]);
                }
            }
            Some(Err(e)) => {
                body_error = Some(e.to_string());
                break;
            }
        }
    }
    driver.abort();
    Ok(Response {
        status,
        body: out,
        body_error,
    })
}

/// Issues one GET of `url`, connecting to `dial` when given, trusting `tls`
/// for an `https` URL. `handshake` records how a TLS handshake ended, even
/// when the caller abandons the future on a timeout.
pub async fn get(
    url: &str,
    dial: Option<&str>,
    tls: Arc<ClientConfig>,
    headers: &HeaderMap,
    max_body: usize,
    handshake: &mut Option<&'static str>,
) -> Result<Response, String> {
    let wrap = |e: String| format!("Get {url:?}: {e}");
    let parsed = Url::parse(url).map_err(|e| wrap(e.to_string()))?;
    let https = match parsed.scheme() {
        "http" => false,
        "https" => true,
        other => return Err(wrap(format!("unsupported protocol scheme {other:?}"))),
    };
    let host = parsed
        .host()
        .ok_or_else(|| wrap("no host in URL".into()))?
        .to_owned();
    let port = parsed
        .port_or_known_default()
        .unwrap_or(if https { 443 } else { 80 });
    let target = match dial {
        Some(d) => d.to_string(),
        None => match &host {
            Host::Ipv6(ip) => format!("[{ip}]:{port}"),
            Host::Ipv4(ip) => format!("{ip}:{port}"),
            Host::Domain(d) => format!("{d}:{port}"),
        },
    };
    let tcp = TcpStream::connect(&target)
        .await
        .map_err(|e| wrap(format!("dial tcp {target}: {e}")))?;
    if !https {
        return send(tcp, &parsed, headers, max_body).await.map_err(wrap);
    }
    let server_name = match host {
        Host::Ipv4(ip) => ServerName::IpAddress(std::net::IpAddr::V4(ip).into()),
        Host::Ipv6(ip) => ServerName::IpAddress(std::net::IpAddr::V6(ip).into()),
        Host::Domain(d) => ServerName::try_from(d).map_err(|e| wrap(e.to_string()))?,
    };
    let connector = tokio_rustls::TlsConnector::from(tls);
    match connector.connect(server_name, tcp).await {
        Ok(stream) => {
            *handshake = Some(handshake_result(None));
            send(stream, &parsed, headers, max_body).await.map_err(wrap)
        }
        Err(e) => {
            *handshake = Some(handshake_result(Some(&e)));
            Err(wrap(format!("tls: {e}")))
        }
    }
}

fn error_reply(
    mut resp: BTreeMap<String, String>,
    error: impl Into<String>,
) -> BTreeMap<String, String> {
    resp.insert("error".into(), error.into());
    resp
}

/// Serves `/fetch` for the decoded query parameters, in order.
pub async fn fetch(query: &[(String, String)]) -> BTreeMap<String, String> {
    fetch_with_trust_file(query, TRUST_FILE).await
}

/// [`fetch`] with the projected trust bundle at `trust_file`.
pub async fn fetch_with_trust_file(
    query: &[(String, String)],
    trust_file: &str,
) -> BTreeMap<String, String> {
    let resp = BTreeMap::new();
    let first = |key: &str| {
        query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .unwrap_or("")
    };
    let url = first("url");
    if url.is_empty() {
        return error_reply(resp, "missing url parameter");
    }
    let roots_mode = first("roots");
    if !matches!(roots_mode, "" | "bundle" | "system" | "pem") {
        // Fail closed on typos: an unknown value read as "bundle" would turn
        // a suite's negative control into a misleading positive fetch.
        return error_reply(
            resp,
            format!("unknown roots value {roots_mode:?} (want bundle, system or pem)"),
        );
    }
    // Same reasoning: an ignored ca, or roots=pem with nothing to trust, is
    // a suite bug, not a result.
    let ca = first("ca");
    if (roots_mode == "pem") != !ca.is_empty() {
        return error_reply(
            resp,
            "the ca parameter goes with roots=pem, and roots=pem needs it",
        );
    }
    let header_params: Vec<&str> = query
        .iter()
        .filter(|(k, _)| k == "header")
        .map(|(_, v)| v.as_str())
        .collect();
    let headers = match parse_fetch_headers(&header_params) {
        Ok(h) => h,
        Err(e) => return error_reply(resp, e),
    };
    let dial = first("dial");
    if !dial.is_empty() {
        if let Err(e) = split_host_port(dial) {
            return error_reply(resp, format!("dial parameter: {e}"));
        }
    }
    let store = match roots_mode {
        "system" => roots::system_roots(),
        "pem" => match roots::roots_from_pem(ca.as_bytes()) {
            Some(s) => s,
            None => return error_reply(resp, "no certificates parsed from the ca parameter"),
        },
        _ => {
            let bundle = match std::fs::read(trust_file) {
                Ok(b) => b,
                Err(e) => return error_reply(resp, format!("reading trust bundle: {e}")),
            };
            match roots::roots_from_pem(&bundle) {
                Some(s) => s,
                None => {
                    return error_reply(resp, format!("no certificates parsed from {trust_file}"))
                }
            }
        }
    };
    let tls = match roots::client_config(store) {
        Ok(c) => c,
        Err(e) => return error_reply(resp, e),
    };

    let mut resp = resp;
    let mut handshake = None;
    let dial = (!dial.is_empty()).then_some(dial);
    let result = tokio::time::timeout(
        FETCH_TIMEOUT,
        get(url, dial, tls, &headers, MAX_FETCH_BODY, &mut handshake),
    )
    .await;
    if let Some(h) = handshake {
        resp.insert("tls_handshake".into(), h.into());
    }
    match result {
        Err(_) => error_reply(
            resp,
            format!("Get {url:?}: timed out after {FETCH_TIMEOUT:?}"),
        ),
        Ok(Err(e)) => error_reply(resp, e),
        Ok(Ok(res)) => {
            resp.insert("status".into(), res.status.to_string());
            if let Some(e) = res.body_error {
                resp.insert("error".into(), format!("reading response body: {e}"));
            }
            resp.insert(
                "body".into(),
                String::from_utf8_lossy(&res.body).into_owned(),
            );
            resp
        }
    }
}

/// Checks that `addr` is `host:port` (with brackets for an IPv6 host).
fn split_host_port(addr: &str) -> Result<(&str, &str), String> {
    let (host, port) = if let Some(rest) = addr.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .ok_or_else(|| format!("address {addr}: missing ']' in address"))?;
        let port = after
            .strip_prefix(':')
            .ok_or_else(|| format!("address {addr}: missing port in address"))?;
        (host, port)
    } else {
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| format!("address {addr}: missing port in address"))?;
        if host.contains(':') {
            return Err(format!("address {addr}: too many colons in address"));
        }
        (host, port)
    };
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::super::test_support::{self, Origin};
    use super::*;

    /// A header-parsing case: name, parameters, expected values per header.
    type HeaderCase<'a> = (&'a str, &'a [&'a str], &'a [(&'a str, &'a [&'a str])]);
    /// A bad-ca case: name, query parameters, expected error fragment.
    type CaCase<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a str);

    fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Fetches `origin` with system roots (there is no projected bundle
    /// outside a cluster) and the given header parameters.
    async fn do_fetch(origin: &str, header_params: &[&str]) -> BTreeMap<String, String> {
        let mut query = q(&[("url", origin), ("roots", "system")]);
        for h in header_params {
            query.push(("header".into(), h.to_string()));
        }
        fetch(&query).await
    }

    fn get<'a>(m: &'a BTreeMap<String, String>, k: &str) -> &'a str {
        m.get(k).map(String::as_str).unwrap_or("")
    }

    #[test]
    fn parse_fetch_headers_cases() {
        let ok: &[HeaderCase] = &[
            ("no parameters", &[], &[]),
            (
                "name and value",
                &["Authorization:Bearer x"],
                &[("authorization", &["Bearer x"])],
            ),
            // Only the first colon separates; the value keeps the rest.
            (
                "value containing a colon",
                &["X-Test:a:b"],
                &[("x-test", &["a:b"])],
            ),
            (
                "repeated name keeps every value",
                &["x-test:1", "X-Test:2"],
                &[("x-test", &["1", "2"])],
            ),
        ];
        for (name, params, want) in ok {
            let got = parse_fetch_headers(params).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(got.keys_len(), want.len(), "{name}: {got:?}");
            for (key, values) in *want {
                let got_values: Vec<&str> = got
                    .get_all(*key)
                    .iter()
                    .map(|v| v.to_str().unwrap())
                    .collect();
                assert_eq!(&got_values, values, "{name}");
            }
        }
        for (name, params) in [("no colon", ["no-colon"]), ("empty name", [":empty-name"])] {
            assert!(
                parse_fetch_headers(&params).is_err(),
                "{name}: want an error"
            );
        }
    }

    // Credential-injection assertions live in the body an origin echoes
    // back, and suites pre-seed headers through ?header=.
    #[tokio::test]
    async fn fetch_returns_body_and_sets_headers() {
        let origin = Origin::http(|req| {
            let auth = req
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            test_support::text(200, format!("auth={auth}"))
        })
        .await;
        let resp = do_fetch(&origin.url(), &["Authorization:Bearer seeded"]).await;
        assert_eq!(get(&resp, "error"), "", "{resp:?}");
        assert_eq!(get(&resp, "status"), "200");
        assert_eq!(get(&resp, "body"), "auth=Bearer seeded");
    }

    // A followed cross-scheme redirect would hop between the gateway's legs.
    #[tokio::test]
    async fn fetch_does_not_follow_redirects() {
        let followed = Arc::new(AtomicBool::new(false));
        let seen = followed.clone();
        let origin = Origin::http(move |req| {
            if req.uri().path() == "/target" {
                seen.store(true, Ordering::SeqCst);
            }
            let mut res = test_support::text(302, String::new());
            res.headers_mut()
                .insert("location", HeaderValue::from_static("/target"));
            res
        })
        .await;
        let resp = do_fetch(&origin.url(), &[]).await;
        assert_eq!(get(&resp, "error"), "", "{resp:?}");
        assert_eq!(get(&resp, "status"), "302");
        assert!(
            !followed.load(Ordering::SeqCst),
            "fetch followed the redirect"
        );
    }

    // A malformed ?header= fails the fetch before anything is sent.
    #[tokio::test]
    async fn fetch_rejects_malformed_header() {
        let origin = Origin::http(|_| test_support::text(200, String::new())).await;
        let resp = do_fetch(&origin.url(), &["no-colon"]).await;
        assert!(
            get(&resp, "error").contains("not <name>:<value>"),
            "{resp:?}"
        );
        assert_eq!(get(&resp, "status"), "");
        assert_eq!(
            origin.hits(),
            0,
            "fetch sent the request despite a malformed header parameter"
        );
    }

    // ?dial= moves only the connection: the URL's host still names the origin.
    #[tokio::test]
    async fn fetch_dials_the_given_address() {
        let origin = Origin::http(|req| {
            let host = req
                .headers()
                .get(HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            test_support::text(200, format!("host={host}"))
        })
        .await;
        let resp = fetch(&q(&[
            ("url", "http://origin.invalid/"),
            ("roots", "system"),
            ("dial", &origin.addr.to_string()),
        ]))
        .await;
        assert_eq!(get(&resp, "error"), "", "{resp:?}");
        assert_eq!(get(&resp, "status"), "200");
        assert_eq!(get(&resp, "body"), "host=origin.invalid");
    }

    // A ?dial= without a port fails the fetch before anything is sent.
    #[tokio::test]
    async fn fetch_rejects_malformed_dial() {
        let origin = Origin::http(|_| test_support::text(200, String::new())).await;
        let resp = fetch(&q(&[
            ("url", &origin.url()),
            ("roots", "system"),
            ("dial", "127.0.0.1"),
        ]))
        .await;
        assert!(get(&resp, "error").contains("dial parameter"), "{resp:?}");
        assert_eq!(get(&resp, "status"), "");
        assert_eq!(origin.hits(), 0);
    }

    // roots=pem trusts exactly the certificates in ca.
    #[tokio::test]
    async fn fetch_trusts_only_the_given_ca() {
        let origin = Origin::https(|_| test_support::text(200, "ok".into())).await;
        let origin_ca = origin.ca_pem.clone().unwrap();
        let unrelated_ca = test_support::unrelated_ca_pem();
        let cases = [
            ("origin's CA", origin_ca.clone(), true),
            (
                "origin's CA among others",
                format!("{unrelated_ca}{origin_ca}"),
                true,
            ),
            ("unrelated CA", unrelated_ca.clone(), false),
        ];
        for (name, ca, want_ok) in cases {
            let resp = fetch(&q(&[("url", &origin.url()), ("roots", "pem"), ("ca", &ca)])).await;
            if want_ok {
                assert!(
                    get(&resp, "error").is_empty() && get(&resp, "status") == "200",
                    "{name}: {resp:?}"
                );
                assert_eq!(get(&resp, "tls_handshake"), "complete", "{name}");
            } else {
                assert_eq!(get(&resp, "tls_handshake"), "failed", "{name}: {resp:?}");
                assert!(
                    get(&resp, "error").contains("certificate"),
                    "{name}: {resp:?}"
                );
            }
        }
    }

    // A server that closes on the ClientHello is a closure, not a failure.
    #[tokio::test]
    async fn fetch_reports_client_hello_closure() {
        let addr = test_support::close_on_client_hello().await;
        let resp = do_fetch(&format!("https://{addr}/"), &[]).await;
        assert!(
            get(&resp, "tls_handshake") == "closed"
                && get(&resp, "status").is_empty()
                && !get(&resp, "error").is_empty(),
            "fetch = {resp:?}, want connection closed during TLS with no HTTP response"
        );
    }

    // roots=pem and ca come together, and ca must hold a certificate.
    #[tokio::test]
    async fn fetch_rejects_mismatched_ca() {
        let origin = Origin::http(|_| test_support::text(200, String::new())).await;
        let cases: [CaCase; 3] = [
            ("roots=pem without ca", &[("roots", "pem")], "ca parameter"),
            (
                "ca without roots=pem",
                &[("roots", "system"), ("ca", "x")],
                "ca parameter",
            ),
            (
                "ca without a certificate",
                &[("roots", "pem"), ("ca", "not a certificate")],
                "no certificates parsed",
            ),
        ];
        for (name, params, want) in cases {
            let mut query = q(params);
            query.push(("url".into(), origin.url()));
            let resp = fetch(&query).await;
            assert!(get(&resp, "error").contains(want), "{name}: {resp:?}");
            assert_eq!(get(&resp, "status"), "", "{name}");
        }
        assert_eq!(
            origin.hits(),
            0,
            "fetch sent the request despite a bad ca parameter"
        );
    }

    #[tokio::test]
    async fn fetch_truncates_body() {
        let origin =
            Origin::http(|_| test_support::text(200, "x".repeat(MAX_FETCH_BODY + 1))).await;
        let resp = do_fetch(&origin.url(), &[]).await;
        assert_eq!(get(&resp, "body").len(), MAX_FETCH_BODY);
    }

    #[tokio::test]
    async fn fetch_refuses_unknown_roots_and_missing_url() {
        let resp = fetch(&q(&[("url", "http://x/"), ("roots", "bundel")])).await;
        assert!(
            get(&resp, "error").contains("unknown roots value"),
            "{resp:?}"
        );
        let resp = fetch(&q(&[])).await;
        assert_eq!(get(&resp, "error"), "missing url parameter");
        let resp =
            fetch_with_trust_file(&q(&[("url", "http://x/")]), "/nonexistent/trust.pem").await;
        assert!(
            get(&resp, "error").starts_with("reading trust bundle"),
            "{resp:?}"
        );
    }

    #[test]
    fn split_host_port_cases() {
        assert!(split_host_port("10.0.0.1:443").is_ok());
        assert!(split_host_port("[::1]:443").is_ok());
        assert!(split_host_port("name.svc:80").is_ok());
        assert!(split_host_port("127.0.0.1").is_err());
        assert!(split_host_port("::1:80").is_err());
        assert!(split_host_port("[::1]").is_err());
    }
}
