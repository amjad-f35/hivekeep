//! Prometheus metrics, served as text on `/metrics` of the metrics
//! address: secret fetches by outcome and latency, and the build.

use std::sync::Arc;
use std::time::{Duration, Instant};

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, Opts, Registry, TextEncoder,
};
use tonic::{Request, Response, Status};

use crate::http::{not_found, response, HttpResponse};
use crate::pb::v1::credential_provider_server::CredentialProvider;
use crate::pb::v1::{FetchSecretRequest, FetchSecretResponse};

/// The provider's metrics.
#[derive(Clone)]
pub struct ProviderMetrics {
    registry: Registry,
    fetches: IntCounterVec,
    latency: HistogramVec,
}

/// The gRPC status code name of `code`, as metric labels use it.
fn code_label(code: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match code {
        Ok => "OK",
        Cancelled => "Canceled",
        Unknown => "Unknown",
        InvalidArgument => "InvalidArgument",
        DeadlineExceeded => "DeadlineExceeded",
        NotFound => "NotFound",
        AlreadyExists => "AlreadyExists",
        PermissionDenied => "PermissionDenied",
        ResourceExhausted => "ResourceExhausted",
        FailedPrecondition => "FailedPrecondition",
        Aborted => "Aborted",
        OutOfRange => "OutOfRange",
        Unimplemented => "Unimplemented",
        Internal => "Internal",
        Unavailable => "Unavailable",
        DataLoss => "DataLoss",
        Unauthenticated => "Unauthenticated",
    }
}

impl ProviderMetrics {
    /// Fresh metrics in their own registry.
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        let fetches = IntCounterVec::new(
            Opts::new(
                "hivekeep_credprovider_fetch_secret_total",
                "FetchSecret calls by gRPC status code.",
            ),
            &["code"],
        )?;
        let latency = HistogramVec::new(
            HistogramOpts::new(
                "hivekeep_credprovider_fetch_secret_duration_seconds",
                "FetchSecret latency by gRPC status code.",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ]),
            &["code"],
        )?;
        let build = IntGauge::with_opts(
            Opts::new(
                "hivekeep_credprovider_build_info",
                "Always 1; labelled with the running version.",
            )
            .const_label("version", env!("CARGO_PKG_VERSION")),
        )?;
        build.set(1);
        registry.register(Box::new(fetches.clone()))?;
        registry.register(Box::new(latency.clone()))?;
        registry.register(Box::new(build))?;
        Ok(ProviderMetrics {
            registry,
            fetches,
            latency,
        })
    }

    /// The registry, for other collectors.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Records one FetchSecret call.
    pub fn observe(&self, code: tonic::Code, elapsed: Duration) {
        let label = code_label(code);
        self.fetches.with_label_values(&[label]).inc();
        self.latency
            .with_label_values(&[label])
            .observe(elapsed.as_secs_f64());
    }

    /// The `/metrics` handler.
    pub fn handle(&self, req: &http::request::Parts) -> HttpResponse {
        if req.uri.path() != "/metrics" {
            return not_found();
        }
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        match encoder.encode(&self.registry.gather(), &mut buf) {
            Ok(()) => response(http::StatusCode::OK, encoder.format_type(), buf),
            Err(e) => response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "text/plain; charset=utf-8",
                format!("{e}\n"),
            ),
        }
    }
}

/// A provider whose calls are counted and timed.
#[derive(Clone)]
pub struct Metered<P> {
    inner: P,
    metrics: Arc<ProviderMetrics>,
}

impl<P> Metered<P> {
    /// Wraps `inner`, recording into `metrics`.
    pub fn new(inner: P, metrics: Arc<ProviderMetrics>) -> Self {
        Metered { inner, metrics }
    }
}

#[tonic::async_trait]
impl<P: CredentialProvider> CredentialProvider for Metered<P> {
    async fn fetch_secret(
        &self,
        request: Request<FetchSecretRequest>,
    ) -> Result<Response<FetchSecretResponse>, Status> {
        let start = Instant::now();
        let r = self.inner.fetch_secret(request).await;
        let code = match &r {
            Ok(_) => tonic::Code::Ok,
            Err(s) => s.code(),
        };
        self.metrics.observe(code, start.elapsed());
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Answer(Option<Status>);

    #[tonic::async_trait]
    impl CredentialProvider for Answer {
        async fn fetch_secret(
            &self,
            _: Request<FetchSecretRequest>,
        ) -> Result<Response<FetchSecretResponse>, Status> {
            match &self.0 {
                Some(s) => Err(s.clone()),
                None => Ok(Response::new(FetchSecretResponse::default())),
            }
        }
    }

    fn scrape(m: &ProviderMetrics, path: &str) -> (http::StatusCode, String) {
        let (parts, ()) = http::Request::get(path).body(()).unwrap().into_parts();
        let resp = m.handle(&parts);
        let status = resp.status();
        let body = futures::executor::block_on(http_body_util::BodyExt::collect(resp.into_body()))
            .unwrap()
            .to_bytes();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn fetches_are_counted_by_code() {
        let m = Arc::new(ProviderMetrics::new().unwrap());
        let ok = Metered::new(Answer(None), m.clone());
        let denied = Metered::new(Answer(Some(Status::permission_denied("no"))), m.clone());
        ok.fetch_secret(Request::new(FetchSecretRequest::default()))
            .await
            .unwrap();
        ok.fetch_secret(Request::new(FetchSecretRequest::default()))
            .await
            .unwrap();
        denied
            .fetch_secret(Request::new(FetchSecretRequest::default()))
            .await
            .unwrap_err();

        let (status, body) = scrape(&m, "/metrics");
        assert_eq!(status, http::StatusCode::OK);
        assert!(
            body.contains(r#"hivekeep_credprovider_fetch_secret_total{code="OK"} 2"#),
            "{body}"
        );
        assert!(
            body.contains(r#"hivekeep_credprovider_fetch_secret_total{code="PermissionDenied"} 1"#),
            "{body}"
        );
        assert!(
            body.contains(
                r#"hivekeep_credprovider_fetch_secret_duration_seconds_count{code="OK"} 2"#
            ),
            "{body}"
        );
        assert!(
            body.contains("hivekeep_credprovider_build_info{version="),
            "{body}"
        );
        assert_eq!(scrape(&m, "/other").0, http::StatusCode::NOT_FOUND);
    }
}
