//! The workload's Prometheus instruments, on a registry the service owns.

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry,
};

/// Every instrument the service reports.
#[derive(Clone)]
pub struct Metrics {
    /// Registry the instruments live on.
    pub registry: Registry,
    pub(crate) ram_write_bytes: IntCounter,
    pub(crate) ram_read_bytes: IntCounter,
    pub(crate) disk_write_bytes: IntCounter,
    pub(crate) disk_read_bytes: IntCounter,
    pub(crate) pings_received: IntCounter,
    pub(crate) gossip_sent: IntCounterVec,
    pub(crate) gossip_latency: HistogramVec,
    pub(crate) fds_open: IntGauge,
    pub(crate) gossip_delay: IntGaugeVec,
    pub(crate) cpu_workers: IntGauge,
}

/// Bucket bounds of the gossip latency histogram, in seconds.
pub const GOSSIP_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

fn counter(registry: &Registry, name: &str, help: &str) -> prometheus::Result<IntCounter> {
    let c = IntCounter::new(name, help)?;
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

fn gauge(registry: &Registry, name: &str, help: &str) -> prometheus::Result<IntGauge> {
    let g = IntGauge::new(name, help)?;
    registry.register(Box::new(g.clone()))?;
    Ok(g)
}

impl Metrics {
    /// Builds and registers every instrument on a fresh registry.
    pub fn new() -> prometheus::Result<Metrics> {
        let registry = Registry::new();
        let gossip_sent = IntCounterVec::new(
            Opts::new(
                "glutton_gossip_requests_sent_total",
                "Number of gossip Ping requests sent per peer.",
            ),
            &["host", "outcome"],
        )?;
        registry.register(Box::new(gossip_sent.clone()))?;
        let gossip_latency = HistogramVec::new(
            HistogramOpts::new(
                "glutton_gossip_latency_seconds",
                "Latency of gossip Ping requests per peer.",
            )
            .buckets(GOSSIP_LATENCY_BUCKETS.to_vec()),
            &["host", "outcome"],
        )?;
        registry.register(Box::new(gossip_latency.clone()))?;
        let gossip_delay = IntGaugeVec::new(
            Opts::new(
                "glutton_gossip_delay_ms",
                "Configured gossip delay per peer, in milliseconds.",
            ),
            &["host"],
        )?;
        registry.register(Box::new(gossip_delay.clone()))?;
        Ok(Metrics {
            ram_write_bytes: counter(
                &registry,
                "glutton_ram_write_bytes_total",
                "Total bytes written to RAM via WriteRAM over the process lifetime.",
            )?,
            ram_read_bytes: counter(
                &registry,
                "glutton_ram_read_bytes_total",
                "Total bytes walked by ReadRAM over the process lifetime.",
            )?,
            disk_write_bytes: counter(
                &registry,
                "glutton_disk_write_bytes_total",
                "Total bytes written to disk via WriteDisk and Ingest over the process lifetime.",
            )?,
            disk_read_bytes: counter(
                &registry,
                "glutton_disk_read_bytes_total",
                "Total bytes read from disk via ReadDisk over the process lifetime.",
            )?,
            pings_received: counter(
                &registry,
                "glutton_ping_requests_total",
                "Number of Ping requests received.",
            )?,
            fds_open: gauge(
                &registry,
                "glutton_fds_open",
                "File descriptors currently held open by OpenFD.",
            )?,
            cpu_workers: gauge(
                &registry,
                "glutton_cpu_workers",
                "Number of threads currently spinning under UseCPU.",
            )?,
            gossip_sent,
            gossip_latency,
            gossip_delay,
            registry,
        })
    }

    pub(crate) fn gossip_latency_for(&self, host: &str, outcome: &str) -> Histogram {
        self.gossip_latency.with_label_values(&[host, outcome])
    }
}
