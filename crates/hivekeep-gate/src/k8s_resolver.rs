//! Resolving `k8s:///<namespace>/<service>[:port]` targets to the ready
//! endpoints of a Kubernetes Service, from its EndpointSlices, so gRPC
//! clients can balance across pods and follow them as they change.
//!
//! The resolver publishes the full, sorted, de-duplicated address list to a
//! [`ResolverSink`] whenever the Service's slices change or a re-resolution
//! is requested, and stops publishing once closed.

use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointSlice};
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;

/// The target scheme.
pub const SCHEME: &str = "k8s";
/// Label linking an EndpointSlice to its Service.
pub const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";
/// Port used when the target names none.
const DEFAULT_PORT: &str = "443";

/// A parsed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The Service's namespace.
    pub namespace: String,
    /// The Service's name.
    pub service: String,
    /// The port to dial (as written).
    pub port: String,
}

/// A target that does not name a Service.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid target {0:?}, expected k8s:///<namespace>/<service>[:port]")]
pub struct InvalidTarget(pub String);

/// Parses a target from its URL authority (`host`) and `path`.
///
/// Accepted forms: `/<ns>/<svc>[:port]`, host `<ns>` with path
/// `/<svc>[:port]`, and DNS forms `<svc>.<ns>[.svc[.cluster.local]][:port]`.
/// The namespace defaults to `default`, the port to 443.
pub fn parse_target_parts(host: &str, path: &str) -> Result<Target, InvalidTarget> {
    let mut ep = path.trim_start_matches('/').to_string();
    if !host.is_empty() && host != "localhost" {
        ep = format!("{host}/{ep}");
    }
    let (mut ns, rest) = match ep.split_once('/') {
        Some((ns, rest)) => (ns.to_string(), rest.to_string()),
        None => ("default".to_string(), ep.clone()),
    };
    let (mut svc, port) = match rest.rsplit_once(':') {
        Some((s, p)) if !s.contains(':') => (s.to_string(), p.to_string()),
        _ => (rest.clone(), DEFAULT_PORT.to_string()),
    };
    if let Some((s, rest)) = svc.split_once('.') {
        ns = rest.split('.').next().unwrap_or_default().to_string();
        svc = s.to_string();
    }
    if ns.is_empty() || svc.is_empty() {
        return Err(InvalidTarget(format!("{SCHEME}://{host}{path}")));
    }
    Ok(Target {
        namespace: ns,
        service: svc,
        port,
    })
}

/// Parses a full target such as `k8s:///hivekeep-system/hivekeep-api:443`.
pub fn parse_target(target: &str) -> Result<Target, InvalidTarget> {
    let u = url::Url::parse(target).map_err(|_| InvalidTarget(target.to_string()))?;
    if u.scheme() != SCHEME {
        return Err(InvalidTarget(target.to_string()));
    }
    parse_target_parts(u.host_str().unwrap_or_default(), u.path())
}

/// Where resolved addresses go.
pub trait ResolverSink: Send + Sync + 'static {
    /// The current addresses (`ip:port`, sorted).
    fn update_state(&self, addrs: Vec<String>) -> Result<(), String>;
    /// A resolution failure.
    fn report_error(&self, err: String);
}

/// A sink publishing addresses on a watch channel.
pub struct WatchSink(pub watch::Sender<Vec<String>>);

impl ResolverSink for WatchSink {
    fn update_state(&self, addrs: Vec<String>) -> Result<(), String> {
        self.0.send_replace(addrs);
        Ok(())
    }

    fn report_error(&self, err: String) {
        tracing::error!(%err, "k8s resolver error");
    }
}

/// Supplies the EndpointSlices of one Service: each item is the full
/// current set (or a watch error).
pub trait SliceSource: Send + Sync + 'static {
    /// Watches the slices labeled for `service` in `namespace`.
    fn watch(
        &self,
        namespace: &str,
        service: &str,
    ) -> BoxStream<'static, Result<Vec<EndpointSlice>, String>>;
}

/// Slices from the Kubernetes API (watch + local cache).
pub struct KubeSliceSource(pub kube::Client);

impl SliceSource for KubeSliceSource {
    fn watch(
        &self,
        namespace: &str,
        service: &str,
    ) -> BoxStream<'static, Result<Vec<EndpointSlice>, String>> {
        use kube::runtime::{reflector, watcher};
        let api: kube::Api<EndpointSlice> = kube::Api::namespaced(self.0.clone(), namespace);
        let cfg = watcher::Config::default().labels(&format!("{SERVICE_NAME_LABEL}={service}"));
        let (reader, writer) = reflector::store();
        let (ns, svc) = (namespace.to_string(), service.to_string());
        reflector::reflector(writer, watcher::watcher(api, cfg))
            .filter_map(move |ev| {
                let out = match ev {
                    Ok(watcher::Event::Init) | Ok(watcher::Event::InitApply(_)) => None,
                    Ok(_) => Some(Ok(reader.state().iter().map(|s| (**s).clone()).collect())),
                    Err(e) => Some(Err(format!(
                        "k8s resolver: watch error for {ns}/{svc}: {e}"
                    ))),
                };
                std::future::ready(out)
            })
            .boxed()
    }
}

/// Whether an endpoint should receive traffic: serving, which defaults to
/// ready, which defaults to true.
pub fn is_healthy(ep: &Endpoint) -> bool {
    let c = ep.conditions.as_ref();
    let ready = c.and_then(|c| c.ready).unwrap_or(true);
    c.and_then(|c| c.serving).unwrap_or(ready)
}

/// The dialable addresses of `slices`: healthy endpoints on the target
/// port (or the slice's first port, or 443), sorted and de-duplicated.
pub fn addresses(slices: &[EndpointSlice], port: &str) -> Vec<String> {
    let target: i32 = port.parse().unwrap_or(0);
    let mut out = Vec::new();
    for slice in slices {
        let mut p = target;
        if p == 0 {
            p = slice
                .ports
                .iter()
                .flatten()
                .next()
                .and_then(|p| p.port)
                .unwrap_or(0);
        }
        if p == 0 {
            p = 443;
        }
        for ep in slice.endpoints.iter().flatten().filter(|e| is_healthy(e)) {
            for ip in &ep.addresses {
                out.push(if ip.contains(':') {
                    format!("[{ip}]:{p}")
                } else {
                    format!("{ip}:{p}")
                });
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// A running resolver.
pub struct K8sResolver {
    cancel: CancellationToken,
    resolve_now: Arc<Notify>,
}

impl K8sResolver {
    /// Resolves `target` with slices from `source`, publishing to `sink`.
    pub fn build(
        source: &dyn SliceSource,
        target: &str,
        sink: Arc<dyn ResolverSink>,
    ) -> Result<K8sResolver, String> {
        let t = parse_target(target).map_err(|e| format!("k8s resolver: {e}"))?;
        let mut slices = source.watch(&t.namespace, &t.service);
        let cancel = CancellationToken::new();
        let resolve_now = Arc::new(Notify::new());
        let (c, rn) = (cancel.clone(), resolve_now.clone());
        tokio::spawn(async move {
            let mut latest: Option<Vec<EndpointSlice>> = None;
            loop {
                tokio::select! {
                    biased;
                    _ = c.cancelled() => return,
                    item = slices.next() => match item {
                        Some(Ok(s)) => latest = Some(s),
                        Some(Err(e)) => {
                            sink.report_error(e);
                            continue;
                        }
                        None => return,
                    },
                    _ = rn.notified() => {}
                }
                if c.is_cancelled() {
                    return;
                }
                if let Some(s) = &latest {
                    if let Err(e) = sink.update_state(addresses(s, &t.port)) {
                        sink.report_error(e);
                    }
                }
            }
        });
        Ok(K8sResolver {
            cancel,
            resolve_now,
        })
    }

    /// Resolves `target` against the Kubernetes API.
    pub fn build_kube(
        client: kube::Client,
        target: &str,
        sink: Arc<dyn ResolverSink>,
    ) -> Result<K8sResolver, String> {
        Self::build(&KubeSliceSource(client), target, sink)
    }

    /// Re-publishes the current addresses.
    pub fn resolve_now(&self) {
        self.resolve_now.notify_one();
    }

    /// Stops watching; nothing is published afterwards.
    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl Drop for K8sResolver {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
#[path = "k8s_resolver_tests.rs"]
mod tests;
