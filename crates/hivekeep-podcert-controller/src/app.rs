//! Wires the signers, their controllers and the replica hasher together.

use std::sync::Arc;

use anyhow::Context as _;
use hivekeep_identity::ca::CertificateAuthority;
use hivekeep_identity::state_file::RefreshingCaPool;
use hivekeep_k8s::cluster_trust_bundle::ClusterTrustBundleClient;
use hivekeep_node::clock::{SharedClock, SystemClock};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Client, Config};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::cli::Args;
use crate::pod_certificate::PcrClient;
use crate::pod_identity_signer::PodIdentitySigner;
use crate::postgres_signer::PostgresSigner;
use crate::rendezvous::LeaseHasher;
use crate::service_dns_signer::ServiceDnsSigner;
use crate::signer::controller::Controller;
use crate::signer::SignerImpl;

/// A Kubernetes client from the in-cluster configuration or a kubeconfig.
pub async fn kube_client(args: &Args) -> anyhow::Result<Client> {
    let config = if args.in_cluster {
        Config::incluster().context("creating in-cluster config")?
    } else {
        let path = match &args.kubeconfig {
            Some(p) => p.clone(),
            None => std::env::var_os("HOME")
                .map(|h| std::path::Path::new(&h).join(".kube").join("config"))
                .context("no --kubeconfig and no HOME")?,
        };
        let kubeconfig = Kubeconfig::read_from(&path)
            .with_context(|| format!("reading kubeconfig {}", path.display()))?;
        Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default())
            .await
            .context("reading kubeconfig")?
    };
    match client_rate_limit(args.kube_api_qps, args.kube_api_burst) {
        Some((qps, burst)) => {
            tracing::info!(qps, burst, "Limiting Kubernetes API requests");
            hivekeep_k8s::client_rate_limit::rate_limited_client(config, qps, burst)
                .map_err(anyhow::Error::msg)
                .context("creating Kubernetes client")
        }
        None => {
            if args.kube_api_burst > 0 {
                tracing::warn!(
                    burst = args.kube_api_burst,
                    "--kube-api-burst has no effect without --kube-api-qps"
                );
            }
            Client::try_from(config).context("creating Kubernetes client")
        }
    }
}

/// The client-side rate limit for `--kube-api-qps`/`--kube-api-burst`:
/// none when the QPS is 0, and a burst of at least one second's worth of
/// requests when the burst is 0.
pub fn client_rate_limit(qps: f32, burst: i64) -> Option<(f32, u32)> {
    if qps.is_nan() || qps <= 0.0 {
        return None;
    }
    let burst = if burst > 0 {
        u32::try_from(burst).unwrap_or(u32::MAX)
    } else {
        // Truncation is fine: QPS flags are small.
        (qps.ceil() as u32).max(1)
    };
    Some((qps, burst))
}

fn ca_pool(path: &std::path::Path, signer: &str) -> anyhow::Result<Arc<dyn CertificateAuthority>> {
    let pool = RefreshingCaPool::open(path)
        .with_context(|| format!("loading {signer} CA pool state from {}", path.display()))?;
    Ok(Arc::new(pool))
}

/// Runs every signer until `shutdown`.
pub async fn run(args: Args, client: Client, shutdown: CancellationToken) -> anyhow::Result<()> {
    let clock: SharedClock = Arc::new(SystemClock);
    let pcrs = Arc::new(
        PcrClient::new(client.clone())
            .await
            .context("discovering PodCertificateRequest API")?,
    );
    let trust_bundles = Arc::new(
        ClusterTrustBundleClient::new(client.clone(), None)
            .await
            .context("discovering ClusterTrustBundle API")?,
    );
    let hasher = Arc::new(LeaseHasher::new(
        client.clone(),
        &args.sharding_pod_namespace,
        &args.sharding_application_name,
        &args.sharding_pod_name,
        &args.sharding_pod_uid,
        clock.clone(),
    ));

    let signers: Vec<Arc<dyn SignerImpl>> = vec![
        Arc::new(ServiceDnsSigner::new(
            client.clone(),
            ca_pool(&args.service_dns_ca_pool, crate::service_dns_signer::NAME)?,
            pcrs.clone(),
            clock.clone(),
        )),
        Arc::new(PodIdentitySigner::new(
            ca_pool(&args.pod_identity_ca_pool, crate::pod_identity_signer::NAME)?,
            pcrs.clone(),
            clock.clone(),
        )),
        Arc::new(PostgresSigner::new(
            args.postgres_client_namespace.clone(),
            ca_pool(&args.postgres_ca_pool, crate::postgres_signer::NAME)?,
            pcrs.clone(),
            clock.clone(),
        )),
    ];

    let mut tasks = JoinSet::new();
    {
        let hasher = hasher.clone();
        let stop = shutdown.clone();
        tasks.spawn(async move { hasher.run(stop).await });
    }
    // Controllers register their handlers before the shared cache starts,
    // so the initial list reaches every one of them.
    for signer in signers {
        let controller =
            Controller::new(signer, hasher.clone(), pcrs.clone(), trust_bundles.clone());
        tasks.spawn(controller.run(args.workers_per_signer, shutdown.clone()));
    }
    let cache = pcrs.informer().spawn();
    while tasks.join_next().await.is_some() {}
    cache.abort();
    Ok(())
}

#[cfg(test)]
mod rate_limit_tests {
    use super::client_rate_limit;

    #[test]
    fn rate_limit_from_flags() {
        assert_eq!(client_rate_limit(0.0, 0), None);
        assert_eq!(client_rate_limit(0.0, 10), None);
        assert_eq!(client_rate_limit(5.0, 10), Some((5.0, 10)));
        assert_eq!(client_rate_limit(2.5, 0), Some((2.5, 3)));
        assert_eq!(client_rate_limit(0.1, 0), Some((0.1, 1)));
    }
}
