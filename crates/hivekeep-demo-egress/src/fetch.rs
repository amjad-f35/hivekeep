//! Outbound GETs for the `/` endpoint.
//!
//! The server holds one long-lived [`Fetch`] implementation (in the cluster
//! an [`HttpFetcher`] on the system roots, which follows redirects). An
//! instruction that asks for its own trust root or for HTTP/1.1 gets a
//! fresh, request-scoped client instead, built from scratch so it cannot
//! inherit a proxy or a relaxed verifier, and which reports redirects
//! rather than following them.

use std::error::Error as StdError;

use async_trait::async_trait;
use reqwest::redirect::Policy;

use crate::limits::{MAX_RESPONSE_BODY, REQUEST_TIMEOUT};
use crate::tls;
use crate::wire::FetchRequest;

/// One outbound GET.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Outbound {
    pub url: String,
    /// Trace context copied from the instruction, if any.
    pub traceparent: Option<String>,
}

/// What came back from the origin.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Fetched {
    pub status: u16,
    /// At most [`MAX_RESPONSE_BODY`] bytes.
    pub body: Vec<u8>,
    /// Issuer of the origin's certificate, empty over plain HTTP.
    pub server_cert: String,
    pub protocol: String,
    /// The response arrived over a verified TLS session.
    pub tls: bool,
}

/// Performs outbound GETs.
#[async_trait]
pub trait Fetch: Send + Sync {
    async fn get(&self, request: Outbound) -> Result<Fetched, String>;
}

/// Renders an error with its causes, which is where the useful detail of a
/// connection or TLS failure lives.
pub fn error_chain(err: &dyn StdError) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    out
}

/// [`Fetch`] over a reqwest client.
#[derive(Clone, Debug)]
pub struct HttpFetcher {
    client: reqwest::Client,
}

struct Options<'a> {
    root_ca: &'a str,
    http1: bool,
    follow_redirects: bool,
    honour_proxy_env: bool,
}

impl HttpFetcher {
    fn build(opts: Options<'_>) -> Result<Self, String> {
        let roots = tls::roots(opts.root_ca)?;
        let alpn = if opts.http1 {
            tls::ALPN_HTTP1
        } else {
            tls::ALPN_H2_AND_HTTP1
        };
        let mut builder = reqwest::Client::builder()
            .use_preconfigured_tls(tls::owned_client_config(roots, alpn))
            .tls_info(true)
            .timeout(REQUEST_TIMEOUT)
            .redirect(if opts.follow_redirects {
                Policy::limited(10)
            } else {
                Policy::none()
            });
        if !opts.honour_proxy_env {
            builder = builder.no_proxy();
        }
        if opts.http1 {
            builder = builder.http1_only();
        }
        let client = builder.build().map_err(|e| error_chain(&e))?;
        Ok(Self { client })
    }

    /// The server's default client: system roots, redirects followed.
    pub fn system() -> Result<Self, String> {
        Self::build(Options {
            root_ca: "",
            http1: false,
            follow_redirects: true,
            honour_proxy_env: true,
        })
    }

    /// A default-style client trusting only the roots in `pem`.
    pub fn with_roots(pem: &str) -> Result<Self, String> {
        Self::build(Options {
            root_ca: pem,
            http1: false,
            follow_redirects: true,
            honour_proxy_env: true,
        })
    }

    /// The request-scoped client `input` asks for, or `None` when the
    /// server's own client should be used.
    pub fn for_request(input: &FetchRequest) -> Result<Option<Self>, String> {
        if input.root_ca.is_empty() && !input.http1 {
            return Ok(None);
        }
        Self::build(Options {
            root_ca: &input.root_ca,
            http1: input.http1,
            follow_redirects: false,
            honour_proxy_env: false,
        })
        .map(Some)
    }
}

fn protocol_name(version: reqwest::Version) -> &'static str {
    match version {
        reqwest::Version::HTTP_09 => "HTTP/0.9",
        reqwest::Version::HTTP_10 => "HTTP/1.0",
        reqwest::Version::HTTP_2 => "HTTP/2.0",
        reqwest::Version::HTTP_3 => "HTTP/3.0",
        _ => "HTTP/1.1",
    }
}

#[async_trait]
impl Fetch for HttpFetcher {
    async fn get(&self, request: Outbound) -> Result<Fetched, String> {
        let mut outbound = self.client.get(&request.url);
        if let Some(tp) = &request.traceparent {
            outbound = outbound.header("traceparent", tp);
        }
        let mut response = outbound
            .send()
            .await
            .map_err(|e| format!("request failed: {}", error_chain(&e)))?;
        let status = response.status().as_u16();
        let protocol = protocol_name(response.version()).to_string();
        let peer = response
            .extensions()
            .get::<reqwest::tls::TlsInfo>()
            .and_then(|i| i.peer_certificate().map(<[u8]>::to_vec));
        let tls = response.url().scheme() == "https" && peer.is_some();
        let server_cert = peer.as_deref().map(tls::issuer_string).unwrap_or_default();

        let mut body = Vec::new();
        while body.len() < MAX_RESPONSE_BODY {
            match response.chunk().await {
                Ok(Some(chunk)) => body.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(e) => return Err(format!("reading response: {}", error_chain(&e))),
            }
        }
        body.truncate(MAX_RESPONSE_BODY);
        Ok(Fetched {
            status,
            body,
            server_cert,
            protocol,
            tls,
        })
    }
}
