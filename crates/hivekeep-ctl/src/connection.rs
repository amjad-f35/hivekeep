//! Connecting to `hivekeep-api`: endpoint, TLS, bearer token, preview opt-in
//! and trace propagation.
//!
//! The server's certificate is verified before any token is attached: a
//! bearer token is never sent over an unauthenticated channel.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use hivekeep_core::preview::PREVIEW_HEADER;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::error::{CliError, Result};
use crate::install;
use crate::io::Sink;
use crate::verbosity::{round_ms, Logger, BASIC, FLOW};

/// How long to wait for the connection to be established.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Where the bearer token comes from.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TokenSource {
    /// Requests carry no token.
    #[default]
    None,
    /// The token is read from this file before every request, so a rotated
    /// token is picked up.
    File(PathBuf),
    /// A token read once, from stdin.
    Static(String),
}

impl TokenSource {
    /// The source for `--token-file`: `-` reads `stdin` once, anything else
    /// names a file.
    pub fn from_flag(flag: Option<&str>, stdin: &mut dyn Read) -> Result<Self> {
        match flag {
            None | Some("") => Ok(TokenSource::None),
            Some("-") => {
                let mut s = String::new();
                stdin
                    .read_to_string(&mut s)
                    .map_err(|e| CliError::failed(format!("read bearer token from stdin: {e}")))?;
                let token = s.trim();
                if token.is_empty() {
                    return Err(CliError::failed(
                        "read bearer token from stdin: bearer token is empty",
                    ));
                }
                Ok(TokenSource::Static(token.to_string()))
            }
            Some(path) => Ok(TokenSource::File(PathBuf::from(path))),
        }
    }

    /// The current token, if any.
    pub fn token(&self) -> std::result::Result<Option<String>, String> {
        match self {
            TokenSource::None => Ok(None),
            TokenSource::Static(t) => Ok(Some(t.clone())),
            TokenSource::File(path) => read_token_file(path).map(Some),
        }
    }
}

fn read_token_file(path: &Path) -> std::result::Result<String, String> {
    let data = std::fs::read_to_string(path).map_err(|e| {
        format!(
            "read bearer token file {:?}: {e}",
            path.display().to_string()
        )
    })?;
    let token = data.trim();
    if token.is_empty() {
        return Err(format!(
            "bearer token file {:?} is empty",
            path.display().to_string()
        ));
    }
    Ok(token.to_string())
}

/// Everything needed to reach `hivekeep-api`.
#[derive(Debug, Clone, Default)]
pub struct ConnectionConfig {
    /// `host:port` or a full `http(s)://` URI.
    pub endpoint: Option<String>,
    /// PEM bundle the server's certificate must chain to; the system roots
    /// when unset.
    pub ca_file: Option<PathBuf>,
    /// Name the server's certificate must carry; the in-cluster Service DNS
    /// name when unset.
    pub server_name: Option<String>,
    /// Connect without TLS. Only for local development: no token is sent.
    pub plaintext: bool,
    /// The bearer token.
    pub token: TokenSource,
    /// Opt in to the server's preview features.
    pub preview: bool,
    /// Propagate a trace context and print its trace ID.
    pub trace: bool,
}

/// Adds the per-request metadata: bearer token, preview opt-in and trace
/// context.
#[derive(Debug, Clone, Default)]
pub struct RequestDecorator {
    token: TokenSource,
    preview: bool,
    traceparent: Option<String>,
}

impl RequestDecorator {
    /// A decorator adding `token`, the preview header when `preview`, and
    /// `traceparent` when given.
    pub fn new(token: TokenSource, preview: bool, traceparent: Option<String>) -> Self {
        RequestDecorator {
            token,
            preview,
            traceparent,
        }
    }
}

impl Interceptor for RequestDecorator {
    fn call(&mut self, mut req: Request<()>) -> std::result::Result<Request<()>, Status> {
        let md = req.metadata_mut();
        if let Some(token) = self.token.token().map_err(Status::unauthenticated)? {
            let value = MetadataValue::try_from(format!("Bearer {token}"))
                .map_err(|_| Status::unauthenticated("bearer token is not a valid header value"))?;
            md.insert("authorization", value);
        }
        if self.preview {
            md.insert(PREVIEW_HEADER, MetadataValue::from_static("true"));
        }
        if let Some(tp) = &self.traceparent {
            if let Ok(v) = MetadataValue::try_from(tp.as_str()) {
                md.insert("traceparent", v);
            }
        }
        Ok(req)
    }
}

/// A fresh W3C trace context and its trace ID.
pub fn new_trace_context() -> (String, String) {
    let trace_id = uuid::Uuid::new_v4().simple().to_string();
    let span_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_string();
    (format!("00-{trace_id}-{span_id}-01"), trace_id)
}

/// Normalizes an endpoint to a URI with a scheme matching the transport.
pub fn endpoint_uri(endpoint: &str, plaintext: bool) -> Result<String> {
    let e = endpoint.trim();
    if e.is_empty() {
        return Err(CliError::usage("--endpoint must not be empty"));
    }
    match (e.split_once("://"), plaintext) {
        (Some(("http", _)), false) => Err(CliError::usage(
            "an http:// endpoint is plaintext; pass --plaintext to use it",
        )),
        (Some(("https", _)), true) => Err(CliError::usage(
            "an https:// endpoint cannot be used with --plaintext",
        )),
        (Some(("http" | "https", _)), _) => Ok(e.to_string()),
        (Some((scheme, _)), _) => Err(CliError::usage(format!(
            "unsupported endpoint scheme {scheme:?}; use host:port, http:// or https://"
        ))),
        (None, true) => Ok(format!("http://{e}")),
        (None, false) => Ok(format!("https://{e}")),
    }
}

/// Warns on `err` when requests will carry no token, so the caller acts
/// with whatever an anonymous principal is granted rather than as itself.
pub fn warn_anonymous(err: &Sink, token: &TokenSource) {
    if *token == TokenSource::None {
        err.line(
            "Warning: no --token-file given; requests carry no bearer token and are \
             treated as anonymous. Pass --token-file to act as yourself.",
        );
    }
}

impl ConnectionConfig {
    /// The endpoint to dial, or a usage error naming how to set one.
    fn endpoint(&self) -> Result<String> {
        let e = self.endpoint.as_deref().ok_or_else(|| {
            CliError::usage(
                "no hivekeep-api endpoint configured: pass --endpoint <host:port> or set HK_ENDPOINT \
                 (for an in-cluster API, port-forward its Service first)",
            )
        })?;
        endpoint_uri(e, self.plaintext)
    }

    /// Builds the endpoint, with TLS unless `plaintext`.
    pub fn tonic_endpoint(&self) -> Result<Endpoint> {
        if self.plaintext && self.token != TokenSource::None {
            return Err(CliError::usage(
                "refusing to send a bearer token over a plaintext connection; drop --plaintext or --token-file",
            ));
        }
        let uri = self.endpoint()?;
        let mut ep = Endpoint::from_shared(uri.clone())
            .map_err(|e| CliError::usage(format!("invalid endpoint {uri:?}: {e}")))?
            .connect_timeout(CONNECT_TIMEOUT);
        if !self.plaintext {
            let server_name = self
                .server_name
                .clone()
                .unwrap_or_else(install::api_server_name);
            let mut tls = ClientTlsConfig::new().domain_name(server_name);
            tls = match &self.ca_file {
                Some(path) => {
                    let pem = std::fs::read(path).map_err(|e| {
                        CliError::failed(format!("read CA file {}: {e}", path.display()))
                    })?;
                    tls.ca_certificate(Certificate::from_pem(pem))
                }
                None => tls.with_native_roots(),
            };
            ep = ep
                .tls_config(tls)
                .map_err(|e| CliError::failed(format!("configure TLS: {e}")))?;
        }
        Ok(ep)
    }

    /// The request decorator for this configuration, printing the trace ID
    /// to `err` when tracing.
    pub fn decorator(&self, err: &Sink) -> RequestDecorator {
        let traceparent = self.trace.then(|| {
            let (tp, trace_id) = new_trace_context();
            err.line(&format!("Tracing enabled. Trace ID: {trace_id}"));
            tp
        });
        RequestDecorator::new(self.token.clone(), self.preview, traceparent)
    }

    /// Dials the server, logging progress.
    pub async fn connect(&self, log: &Logger) -> Result<Channel> {
        let ep = self.tonic_endpoint()?;
        if log.enabled(FLOW) {
            log.log(
                FLOW,
                format!(
                    "Connecting to hivekeep-api (endpoint={:?}, plaintext={}, ca-file={:?}, token={}, trace={})",
                    self.endpoint.clone().unwrap_or_default(),
                    self.plaintext,
                    self.ca_file.as_ref().map(|p| p.display().to_string()).unwrap_or_default(),
                    match &self.token {
                        TokenSource::None => "none".to_string(),
                        TokenSource::Static(_) => "stdin".to_string(),
                        TokenSource::File(p) => format!("{:?}", p.display().to_string()),
                    },
                    self.trace,
                ),
            );
        } else {
            log.log(
                BASIC,
                format!(
                    "Connecting to hivekeep-api at {}",
                    self.endpoint.clone().unwrap_or_default()
                ),
            );
        }
        let start = Instant::now();
        let result = ep.connect().await;
        let elapsed = start.elapsed();
        match result {
            Ok(ch) => {
                log.completed(
                    elapsed,
                    format!("Connected to hivekeep-api in {}", round_ms(elapsed)),
                );
                Ok(ch)
            }
            Err(e) => {
                let detail = error_chain(&e);
                log.log(
                    BASIC,
                    format!(
                        "Connection to hivekeep-api failed after {}: {detail}",
                        round_ms(elapsed)
                    ),
                );
                Err(CliError::failed(format!(
                    "failed to connect to hivekeep-api: {detail}"
                )))
            }
        }
    }
}

/// An error and its sources, joined with `: `.
pub fn error_chain(e: &dyn std::error::Error) -> String {
    let mut s = e.to_string();
    let mut src = e.source();
    while let Some(cause) = src {
        let c = cause.to_string();
        if !s.contains(&c) {
            s.push_str(": ");
            s.push_str(&c);
        }
        src = cause.source();
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        assert_eq!(
            endpoint_uri("localhost:8080", true).unwrap(),
            "http://localhost:8080"
        );
        assert_eq!(
            endpoint_uri("hivekeep-api:443", false).unwrap(),
            "https://hivekeep-api:443"
        );
        assert_eq!(
            endpoint_uri("https://hivekeep-api:443", false).unwrap(),
            "https://hivekeep-api:443"
        );
        assert!(endpoint_uri("http://api", false).unwrap_err().is_usage());
        assert!(endpoint_uri("https://api", true).unwrap_err().is_usage());
        assert!(endpoint_uri("unix:///x", false).unwrap_err().is_usage());
        assert!(endpoint_uri("", false).unwrap_err().is_usage());
    }

    #[test]
    fn missing_endpoint_is_a_usage_error() {
        let err = ConnectionConfig::default().tonic_endpoint().unwrap_err();
        assert!(
            err.is_usage() && err.message().contains("HK_ENDPOINT"),
            "{err}"
        );
    }

    #[test]
    fn token_never_goes_over_plaintext() {
        let cfg = ConnectionConfig {
            endpoint: Some("localhost:1".into()),
            plaintext: true,
            token: TokenSource::Static("t".into()),
            ..Default::default()
        };
        assert!(cfg
            .tonic_endpoint()
            .unwrap_err()
            .message()
            .contains("plaintext"));
    }

    #[test]
    fn token_sources() {
        let mut empty: &[u8] = b"";
        assert_eq!(
            TokenSource::from_flag(None, &mut empty).unwrap(),
            TokenSource::None
        );
        let mut stdin: &[u8] = b"  tok-1\n";
        assert_eq!(
            TokenSource::from_flag(Some("-"), &mut stdin).unwrap(),
            TokenSource::Static("tok-1".into())
        );
        let mut blank: &[u8] = b" \n";
        assert!(TokenSource::from_flag(Some("-"), &mut blank).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        let src = TokenSource::File(path.clone());
        assert!(src.token().unwrap_err().contains("read bearer token file"));
        std::fs::write(&path, "\n").unwrap();
        assert!(src.token().unwrap_err().contains("is empty"));
        std::fs::write(&path, "tok-2\n").unwrap();
        assert_eq!(src.token().unwrap(), Some("tok-2".into()));
        // A rotated token is picked up on the next request.
        std::fs::write(&path, "tok-3").unwrap();
        assert_eq!(src.token().unwrap(), Some("tok-3".into()));
    }

    #[test]
    fn decorator_adds_metadata() {
        let mut d = RequestDecorator::new(
            TokenSource::Static("secret".into()),
            true,
            Some("00-abc-def-01".into()),
        );
        let req = d.call(Request::new(())).unwrap();
        let md = req.metadata();
        assert_eq!(md.get("authorization").unwrap(), "Bearer secret");
        assert_eq!(md.get(PREVIEW_HEADER).unwrap(), "true");
        assert_eq!(md.get("traceparent").unwrap(), "00-abc-def-01");

        let mut plain = RequestDecorator::default();
        let req = plain.call(Request::new(())).unwrap();
        assert!(req.metadata().is_empty());
    }

    #[test]
    fn decorator_fails_on_unreadable_token() {
        let mut d =
            RequestDecorator::new(TokenSource::File("/nonexistent/token".into()), false, None);
        let err = d.call(Request::new(())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn trace_context_shape() {
        let (tp, id) = new_trace_context();
        assert_eq!(id.len(), 32);
        let parts: Vec<_> = tp.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[1], id);
        assert_eq!(parts[2].len(), 16);
    }

    #[test]
    fn anonymous_warning() {
        let (sink, buf) = Sink::buffer();
        warn_anonymous(&sink, &TokenSource::File("t".into()));
        assert_eq!(buf.contents(), "");
        warn_anonymous(&sink, &TokenSource::None);
        assert!(buf.contents().contains("no --token-file given"));
    }
}
