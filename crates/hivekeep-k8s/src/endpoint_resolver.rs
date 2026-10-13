//! Resolves a Kubernetes Service to the addresses of its ready endpoints by
//! watching its EndpointSlices, for client-side load balancing.
//!
//! Targets look like `k8s:///<namespace>/<service>[:port]` or
//! `k8s:///<service>.<namespace>[.svc...][:port]`. The resolver reports the
//! sorted, de-duplicated `ip:port` set of every serving endpoint to a
//! [`ResolverSink`] whenever the slices change; it promises eventual
//! convergence, not that each report is final.

use std::sync::Arc;

use k8s_openapi::api::discovery::v1::{Endpoint, EndpointSlice};
use kube::api::Api;
use kube::runtime::watcher;
use kube::Client;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::informer::Informer;

/// The target scheme.
pub const SCHEME: &str = "k8s";
/// Port used when neither the target nor the slice names one.
const DEFAULT_PORT: u16 = 443;
/// The label EndpointSlices carry naming their Service.
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// A parsed target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    /// The Service's namespace.
    pub namespace: String,
    /// The Service name.
    pub service: String,
    /// The port, as written (default "443").
    pub port: String,
}

/// Parses a resolver target. The scheme is optional.
pub fn parse_target(target: &str) -> Result<Target, String> {
    let invalid =
        || format!("invalid target {target:?}, expected k8s:///<namespace>/<service>[:port]");
    let rest = target.strip_prefix("k8s:").unwrap_or(target);
    let mut ep = match rest.strip_prefix("//") {
        Some(after) => {
            let (host, path) = after.split_once('/').unwrap_or((after, ""));
            if host.is_empty() || host == "localhost" {
                path.to_string()
            } else {
                format!("{host}/{path}")
            }
        }
        None => rest.trim_start_matches('/').to_string(),
    };
    let mut namespace = "default".to_string();
    if let Some((ns, svc)) = ep.split_once('/') {
        namespace = ns.to_string();
        ep = svc.to_string();
    }
    let (mut service, port) = match ep.rsplit_once(':') {
        Some((h, p)) if !p.is_empty() && !h.is_empty() => (h.to_string(), p.to_string()),
        _ => (ep.clone(), DEFAULT_PORT.to_string()),
    };
    let parts: Vec<&str> = service.split('.').collect();
    if parts.len() > 1 {
        namespace = parts[1].to_string();
        service = parts[0].to_string();
    }
    if namespace.is_empty() || service.is_empty() {
        return Err(invalid());
    }
    Ok(Target {
        namespace,
        service,
        port,
    })
}

/// Receives resolution results.
pub trait ResolverSink: Send + Sync + 'static {
    /// The current address set.
    fn update_addresses(&self, addrs: Vec<String>);
    /// A watch or sync failure.
    fn report_error(&self, err: String);
}

/// Ready or serving: a terminating endpoint that still serves keeps
/// receiving traffic, so connections are not dropped early. Unknown
/// conditions read as ready.
fn is_healthy(ep: &Endpoint) -> bool {
    let c = ep.conditions.clone().unwrap_or_default();
    let ready = c.ready.unwrap_or(true);
    match c.serving {
        Some(serving) => serving,
        None => ready,
    }
}

/// The addresses of `slices` for `port` ("0" or unparsable: the slice's
/// first port, else 443).
pub fn addresses(slices: &[Arc<EndpointSlice>], port: &str) -> Vec<String> {
    let target_port: u16 = port.parse().unwrap_or(0);
    let mut out = Vec::new();
    for s in slices {
        let mut p = target_port;
        if p == 0 {
            p = s
                .ports
                .as_ref()
                .and_then(|ps| ps.first())
                .and_then(|p| p.port)
                .and_then(|p| u16::try_from(p).ok())
                .unwrap_or(0);
        }
        if p == 0 {
            p = DEFAULT_PORT;
        }
        for ep in s.endpoints.iter().flatten().filter(|e| is_healthy(e)) {
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

/// A running resolution of one target.
pub struct EndpointResolver {
    trigger: Arc<Notify>,
    cancel: CancellationToken,
}

impl EndpointResolver {
    /// Starts watching `target`'s EndpointSlices, reporting to `sink`.
    pub fn start(
        client: Client,
        target: &str,
        sink: Arc<dyn ResolverSink>,
    ) -> Result<Self, String> {
        let t = parse_target(target).map_err(|e| format!("endpoint resolver: {e}"))?;
        let api: Api<EndpointSlice> = Api::namespaced(client, &t.namespace);
        let informer = Informer::new(
            api,
            watcher::Config::default().labels(&format!("{SERVICE_NAME_LABEL}={}", t.service)),
        );
        let trigger = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        {
            let trigger = trigger.clone();
            informer.add_handler(move |_| trigger.notify_one());
        }
        {
            let sink = sink.clone();
            let cancel = cancel.clone();
            let (ns, svc) = (t.namespace.clone(), t.service.clone());
            informer.add_error_handler(move |err| {
                if !cancel.is_cancelled() {
                    sink.report_error(format!(
                        "endpoint resolver: watch error for {ns}/{svc}: {err}"
                    ));
                }
            });
        }
        let run = informer.clone().run();
        let worker = {
            let (informer, trigger, cancel, port) = (
                informer.clone(),
                trigger.clone(),
                cancel.clone(),
                t.port.clone(),
            );
            async move {
                informer.wait_synced().await;
                trigger.notify_one();
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = trigger.notified() => {}
                    }
                    if cancel.is_cancelled() {
                        return;
                    }
                    sink.update_addresses(addresses(&informer.list(), &port));
                }
            }
        };
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = c.cancelled() => {}
                _ = run => {}
            }
        });
        tokio::spawn(worker);
        Ok(EndpointResolver { trigger, cancel })
    }

    /// Asks for a fresh report.
    pub fn resolve_now(&self) {
        if !self.cancel.is_cancelled() {
            self.trigger.notify_one();
        }
    }

    /// Stops resolving; no reports follow.
    pub fn close(&self) {
        self.cancel.cancel();
    }
}

impl Drop for EndpointResolver {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kube::api::PostParams;
    use parking_lot::Mutex;
    use serde_json::json;
    use tokio::sync::mpsc;

    use super::*;
    use crate::fake::FakeApiServer;

    #[test]
    fn parse_targets() {
        for (name, target, ns, svc, port) in [
            (
                "slash format namespace/service:port",
                "k8s:///hivekeep-system/api:443",
                "hivekeep-system",
                "api",
                "443",
            ),
            (
                "host format",
                "k8s://hivekeep-system/api:443",
                "hivekeep-system",
                "api",
                "443",
            ),
            (
                "fqdn format api.hivekeep-system.svc:443",
                "k8s:///api.hivekeep-system.svc:443",
                "hivekeep-system",
                "api",
                "443",
            ),
            (
                "full cluster fqdn format",
                "k8s:///api.hivekeep-system.svc.cluster.local:443",
                "hivekeep-system",
                "api",
                "443",
            ),
            (
                "simple service without port",
                "k8s:///api",
                "default",
                "api",
                "443",
            ),
        ] {
            let got = parse_target(target).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(
                got,
                Target {
                    namespace: ns.into(),
                    service: svc.into(),
                    port: port.into()
                },
                "{name}"
            );
        }
        assert!(parse_target("k8s:///ns/").is_err());
    }

    struct Sink {
        states: mpsc::UnboundedSender<Vec<String>>,
        errors: Mutex<Vec<String>>,
    }

    impl ResolverSink for Sink {
        fn update_addresses(&self, addrs: Vec<String>) {
            let _ = self.states.send(addrs);
        }
        fn report_error(&self, err: String) {
            self.errors.lock().push(err);
        }
    }

    fn slice(name: &str, ip: &str) -> EndpointSlice {
        serde_json::from_value(json!({
            "metadata": {"name": name, "namespace": "hivekeep-system", "labels": {"kubernetes.io/service-name": "api"}},
            "addressType": "IPv4",
            "ports": [{"port": 443}],
            "endpoints": [{"addresses": [ip], "conditions": {"ready": true}}]
        }))
        .unwrap()
    }

    async fn wait_for(rx: &mut mpsc::UnboundedReceiver<Vec<String>>, want: &[&str]) {
        let want: Vec<String> = want.iter().map(|s| s.to_string()).collect();
        let mut last = Vec::new();
        let r = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(s) = rx.recv().await {
                last = s.clone();
                if s == want {
                    return;
                }
            }
        })
        .await;
        assert!(
            r.is_ok(),
            "addresses never converged: last = {last:?}, want {want:?}"
        );
    }

    #[tokio::test]
    async fn endpoint_slice_updates() {
        let fake = FakeApiServer::new();
        fake.insert(&slice("api-slice-1", "10.0.0.1"));
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = Arc::new(Sink {
            states: tx,
            errors: Mutex::new(Vec::new()),
        });
        let r = EndpointResolver::start(
            fake.client(),
            "k8s:///hivekeep-system/api:443",
            sink.clone(),
        )
        .unwrap();
        wait_for(&mut rx, &["10.0.0.1:443"]).await;
        Api::<EndpointSlice>::namespaced(fake.client(), "hivekeep-system")
            .create(&PostParams::default(), &slice("api-slice-2", "10.0.0.2"))
            .await
            .unwrap();
        wait_for(&mut rx, &["10.0.0.1:443", "10.0.0.2:443"]).await;
        r.close();
        assert!(sink.errors.lock().is_empty());
    }

    #[tokio::test]
    async fn no_reports_after_close() {
        let fake = FakeApiServer::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = Arc::new(Sink {
            states: tx,
            errors: Mutex::new(Vec::new()),
        });
        let r = EndpointResolver::start(
            fake.client(),
            "k8s:///hivekeep-system/api:443",
            sink.clone(),
        )
        .unwrap();
        r.close();
        tokio::time::sleep(Duration::from_millis(20)).await;
        while rx.try_recv().is_ok() {}
        r.resolve_now();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(rx.try_recv().is_err(), "update after close");
        assert!(sink.errors.lock().is_empty(), "error after close");
    }

    #[test]
    fn health() {
        let ep = |ready: Option<bool>, serving: Option<bool>| Endpoint {
            conditions: Some(k8s_openapi::api::discovery::v1::EndpointConditions {
                ready,
                serving,
                terminating: None,
            }),
            ..Default::default()
        };
        assert!(is_healthy(&ep(None, None)));
        assert!(
            is_healthy(&ep(Some(false), Some(true))),
            "terminating but serving"
        );
        assert!(!is_healthy(&ep(Some(false), None)));
        assert!(!is_healthy(&ep(Some(true), Some(false))));
    }
}
