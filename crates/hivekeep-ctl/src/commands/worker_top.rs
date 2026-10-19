//! `top worker`: live CPU and memory of worker pods, from the cluster's
//! metrics API.

use std::collections::HashMap;
use std::future::Future;

use serde::Deserialize;

use crate::cli::WorkerFilter;
use crate::client::Api;
use crate::commands::worker::{list_filtered, validate_filter};
use crate::context::Ctx;
use crate::error::Result;
use crate::io::Sink;
use crate::kubectl::Kubectl;
use crate::label_selector::LabelSelector;
use crate::output::Printer;
use crate::print::worker::{occupancy, sandbox_class};
use crate::print::worker_top::{self, WorkerTopItem};
use crate::quantity::Quantity;
use crate::verbosity::{Logger, BASIC, FLOW};

/// Shown when a pod's usage is not known.
pub const UNAVAILABLE: &str = "metrics unavailable";

/// The container whose usage represents a worker pod.
pub const WORKER_CONTAINER: &str = "worker";

/// Usage of one container.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ContainerMetrics {
    /// Container name.
    pub name: String,
    /// Usage by resource name (`cpu`, `memory`).
    #[serde(default)]
    pub usage: HashMap<String, String>,
}

/// Usage of one pod.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PodMetrics {
    /// Pod namespace.
    pub namespace: String,
    /// Pod name.
    pub name: String,
    /// Per-container usage.
    pub containers: Vec<ContainerMetrics>,
}

/// A source of pod metrics.
pub trait MetricsSource {
    /// Pod metrics in `namespace`, or in every namespace when empty.
    fn list(
        &self,
        namespace: &str,
    ) -> impl Future<Output = std::result::Result<Vec<PodMetrics>, String>> + Send;
}

#[derive(Deserialize)]
struct MetricsList {
    #[serde(default)]
    items: Vec<MetricsItem>,
}

#[derive(Deserialize)]
struct MetricsItem {
    metadata: MetricsMeta,
    #[serde(default)]
    containers: Vec<ContainerMetrics>,
}

#[derive(Deserialize)]
struct MetricsMeta {
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    name: String,
}

/// Parses a `PodMetricsList` from the metrics API.
pub fn parse_metrics_list(json: &[u8]) -> std::result::Result<Vec<PodMetrics>, String> {
    let list: MetricsList =
        serde_json::from_slice(json).map_err(|e| format!("decode pod metrics: {e}"))?;
    Ok(list
        .items
        .into_iter()
        .map(|i| PodMetrics {
            namespace: i.metadata.namespace,
            name: i.metadata.name,
            containers: i.containers,
        })
        .collect())
}

/// Reads pod metrics through `kubectl get --raw`.
pub struct KubectlMetrics(pub Kubectl);

impl MetricsSource for KubectlMetrics {
    async fn list(&self, namespace: &str) -> std::result::Result<Vec<PodMetrics>, String> {
        let path = if namespace.is_empty() {
            "/apis/metrics.k8s.io/v1beta1/pods".to_string()
        } else {
            format!("/apis/metrics.k8s.io/v1beta1/namespaces/{namespace}/pods")
        };
        let kubectl = self.0.clone();
        let out = tokio::task::spawn_blocking(move || kubectl.run(&["get", "--raw", &path], None))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        parse_metrics_list(&out)
    }
}

/// The CPU and memory columns for a pod: the worker container's usage, or
/// the sum over all containers when there is none of that name.
pub fn container_usage(pm: &PodMetrics, log: &Logger) -> (String, String) {
    let q = |c: &ContainerMetrics, key: &str| {
        c.usage
            .get(key)
            .and_then(|v| Quantity::parse(v).ok())
            .unwrap_or_default()
    };
    let (cpu, mem) = if let Some(c) = pm.containers.iter().find(|c| c.name == WORKER_CONTAINER) {
        (q(c, "cpu"), q(c, "memory"))
    } else if !pm.containers.is_empty() {
        log.log(
            FLOW,
            format!(
                "Container {WORKER_CONTAINER:?} not found in pod metrics for {}/{}; summing all {} container(s)",
                pm.namespace,
                pm.name,
                pm.containers.len()
            ),
        );
        pm.containers.iter().fold(
            (Quantity::default(), Quantity::default()),
            |(cpu, mem), c| (cpu + q(c, "cpu"), mem + q(c, "memory")),
        )
    } else {
        return (UNAVAILABLE.into(), UNAVAILABLE.into());
    };
    (
        format!("{}m", cpu.milli_value()),
        format!("{}Mi", mem.value() / (1024 * 1024)),
    )
}

/// Builds the usage rows for the filtered Workers.
pub async fn top_items<M: MetricsSource>(
    api: &Api,
    metrics: &M,
    filter: &WorkerFilter,
    selector: Option<&LabelSelector>,
    log: &Logger,
) -> Result<Vec<WorkerTopItem>> {
    let workers = list_filtered(api, filter, selector).await?;
    let namespace = filter.namespace.clone().unwrap_or_default();
    let by_pod: Option<HashMap<String, PodMetrics>> = match metrics.list(&namespace).await {
        Ok(list) => Some(
            list.into_iter()
                .map(|pm| (format!("{}/{}", pm.namespace, pm.name), pm))
                .collect(),
        ),
        Err(e) => {
            log.log(
                BASIC,
                format!("Failed to list pod metrics (CPU/memory will be unavailable): {e}"),
            );
            None
        }
    };
    Ok(workers
        .iter()
        .map(|w| {
            let key = format!("{}/{}", w.worker_namespace, w.worker_pod);
            let (cpu, memory) = match by_pod.as_ref().map(|m| m.get(&key)) {
                Some(Some(pm)) => container_usage(pm, log),
                Some(None) => {
                    log.log(
                        BASIC,
                        format!("No pod metrics returned for worker pod {key}"),
                    );
                    (UNAVAILABLE.into(), UNAVAILABLE.into())
                }
                None => (UNAVAILABLE.into(), UNAVAILABLE.into()),
            };
            WorkerTopItem {
                pod: w.worker_pod.clone(),
                pool: w.worker_pool.clone(),
                class: sandbox_class(w),
                status: occupancy(w),
                cpu,
                memory,
                namespace: w.worker_namespace.clone(),
            }
        })
        .collect())
}

/// Prints the usage rows.
pub async fn run_with<M: MetricsSource>(
    api: &Api,
    metrics: &M,
    filter: &WorkerFilter,
    printer: &Printer,
    out: &Sink,
    log: &Logger,
) -> Result<()> {
    let selector = validate_filter(&[], filter)?;
    let items = top_items(api, metrics, filter, selector.as_ref(), log).await?;
    worker_top::print(printer, &mut out.clone(), items)
}

/// `top worker`.
pub async fn run(ctx: &mut Ctx, filter: &WorkerFilter) -> Result<()> {
    validate_filter(&[], filter)?;
    let api = ctx.api().await?;
    let metrics = KubectlMetrics(ctx.kubectl());
    run_with(&api, &metrics, filter, &ctx.printer, &ctx.out(), &ctx.log).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn container(name: &str, cpu: &str, mem: &str) -> ContainerMetrics {
        ContainerMetrics {
            name: name.into(),
            usage: [
                ("cpu".to_string(), cpu.to_string()),
                ("memory".to_string(), mem.to_string()),
            ]
            .into(),
        }
    }

    #[test]
    fn worker_container_wins() {
        let pm = PodMetrics {
            namespace: "ns".into(),
            name: "p".into(),
            containers: vec![
                container("sidecar", "1", "1Gi"),
                container("worker", "250m", "512Mi"),
            ],
        };
        assert_eq!(
            container_usage(&pm, &Logger::silent()),
            ("250m".into(), "512Mi".into())
        );
    }

    #[test]
    fn other_containers_are_summed() {
        let (sink, buf) = Sink::buffer();
        let pm = PodMetrics {
            namespace: "ns".into(),
            name: "p".into(),
            containers: vec![
                container("a", "100m", "64Mi"),
                container("b", "150m", "64Mi"),
            ],
        };
        assert_eq!(
            container_usage(&pm, &Logger::new(FLOW, sink)),
            ("250m".into(), "128Mi".into())
        );
        assert!(buf.contents().contains("summing all 2 container(s)"));
    }

    #[test]
    fn no_containers_is_unavailable() {
        let pm = PodMetrics::default();
        assert_eq!(
            container_usage(&pm, &Logger::silent()),
            (UNAVAILABLE.into(), UNAVAILABLE.into())
        );
    }

    #[test]
    fn metrics_list_json() {
        let json = br#"{"kind":"PodMetricsList","items":[{"metadata":{"name":"p1","namespace":"ns"},"containers":[{"name":"worker","usage":{"cpu":"123456789n","memory":"12345Ki"}}]}]}"#;
        let list = parse_metrics_list(json).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "p1");
        assert_eq!(
            container_usage(&list[0], &Logger::silent()),
            ("124m".into(), "12Mi".into())
        );
        assert!(parse_metrics_list(b"nope").is_err());
    }
}
