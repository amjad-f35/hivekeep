//! Lease-backed replica membership for rendezvous hashing.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use hivekeep_k8s::api_errors::is_not_found;
use hivekeep_k8s::informer::Informer;
use hivekeep_node::clock::SharedClock;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta, OwnerReference};
use kube::api::{Api, PostParams};
use kube::runtime::watcher;
use kube::Client;
use tokio_util::sync::CancellationToken;

use super::{assign, Hasher};

/// Label naming the application a replica Lease belongs to.
pub const LABEL_KEY: &str = "rendezvous.hivekeep.io/application";

/// How long a Lease stays valid without renewal, and so how long a dead
/// replica keeps its assignments.
pub const LEASE_DURATION: Duration = Duration::from_secs(15);

/// How often a replica renews its Lease.
pub const LEASE_RENEWAL_PERIOD: Duration = Duration::from_secs(10);

/// This replica's view of the live replicas, from their Leases.
pub struct LeaseHasher {
    leases: Api<Lease>,
    namespace: String,
    application: String,
    replica_name: String,
    replica_uid: String,
    informer: Arc<Informer<Lease>>,
    clock: SharedClock,
}

impl LeaseHasher {
    /// A hasher for replica `replica_name` (the Pod name, with Pod UID
    /// `replica_uid`) of `application`, keeping Leases in `namespace`.
    pub fn new(
        client: Client,
        namespace: &str,
        application: &str,
        replica_name: &str,
        replica_uid: &str,
        clock: SharedClock,
    ) -> Self {
        let watched: Api<Lease> = if namespace.is_empty() {
            Api::all(client.clone())
        } else {
            Api::namespaced(client.clone(), namespace)
        };
        let informer = Informer::new(
            watched,
            watcher::Config::default().labels(&format!("{LABEL_KEY}={application}")),
        );
        LeaseHasher {
            leases: Api::namespaced(client, namespace),
            namespace: namespace.into(),
            application: application.into(),
            replica_name: replica_name.into(),
            replica_uid: replica_uid.into(),
            informer,
            clock,
        }
    }

    /// The Lease cache.
    pub fn informer(&self) -> &Arc<Informer<Lease>> {
        &self.informer
    }

    /// Watches Leases and renews this replica's Lease every
    /// [`LEASE_RENEWAL_PERIOD`] until `shutdown`.
    pub async fn run(&self, shutdown: CancellationToken) {
        let watch = self.informer.spawn();
        tokio::select! {
            _ = shutdown.cancelled() => {
                watch.abort();
                return;
            }
            _ = self.informer.wait_synced() => {}
        }
        let mut ticks = tokio::time::interval(LEASE_RENEWAL_PERIOD);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = ticks.tick() => {
                    if let Err(err) = self.ensure_lease().await {
                        tracing::error!(%err, "Error while ensuring lease");
                    }
                }
            }
        }
        watch.abort();
    }

    fn desired_lease(&self, now: DateTime<Utc>) -> Lease {
        let now = micro_time(now);
        Lease {
            metadata: ObjectMeta {
                namespace: Some(self.namespace.clone()),
                name: Some(self.replica_name.clone()),
                labels: Some([(LABEL_KEY.to_string(), self.application.clone())].into()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".into(),
                    kind: "Pod".into(),
                    name: self.replica_name.clone(),
                    uid: self.replica_uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: None,
                }]),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(self.replica_name.clone()),
                lease_duration_seconds: Some(LEASE_DURATION.as_secs() as i32),
                acquire_time: Some(now.clone()),
                renew_time: Some(now),
                lease_transitions: Some(1),
                ..Default::default()
            }),
        }
    }

    /// Creates this replica's Lease, or renews it (restoring its labels,
    /// owner and holder).
    pub async fn ensure_lease(&self) -> Result<(), kube::Error> {
        let desired = self.desired_lease(self.clock.now());
        let mut lease = match self.leases.get(&self.replica_name).await {
            Ok(lease) => lease,
            Err(err) if is_not_found(&err) => {
                self.leases.create(&PostParams::default(), &desired).await?;
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        let want = desired.spec.unwrap_or_default();
        lease.metadata.labels = desired.metadata.labels;
        lease.metadata.owner_references = desired.metadata.owner_references;
        let spec = lease.spec.get_or_insert_default();
        spec.holder_identity = want.holder_identity;
        spec.lease_duration_seconds = want.lease_duration_seconds;
        spec.renew_time = want.renew_time;
        self.leases
            .replace(&self.replica_name, &PostParams::default(), &lease)
            .await?;
        Ok(())
    }

    /// Holders of the cached Leases that were renewed within their
    /// duration.
    pub fn live_replicas(&self) -> Vec<String> {
        let now = micro_time(self.clock.now()).0;
        self.informer
            .list_filtered(|l| {
                self.namespace.is_empty()
                    || l.metadata.namespace.as_deref() == Some(&self.namespace)
            })
            .iter()
            .filter_map(|lease| {
                let spec = lease.spec.as_ref()?;
                let renewed = spec.renew_time.as_ref()?.0;
                let duration = i64::from(spec.lease_duration_seconds.unwrap_or(0));
                let deadline = renewed
                    .checked_add(k8s_openapi::jiff::SignedDuration::from_secs(duration))
                    .ok()?;
                if now > deadline {
                    return None;
                }
                spec.holder_identity.clone()
            })
            .collect()
    }
}

impl Hasher for LeaseHasher {
    fn assigned_to_this_replica(&self, item: &str) -> bool {
        let live = self.live_replicas();
        assign(item, live.iter().map(String::as_str)) == self.replica_name
    }
}

fn micro_time(t: DateTime<Utc>) -> MicroTime {
    MicroTime(
        k8s_openapi::jiff::Timestamp::from_microsecond(t.timestamp_micros())
            .unwrap_or(k8s_openapi::jiff::Timestamp::UNIX_EPOCH),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use hivekeep_k8s::fake::FakeApiServer;
    use hivekeep_node::clock::MockClock;

    use super::*;
    use crate::test_support::{start_time, wait_until};

    fn hasher(fake: &FakeApiServer, name: &str, clock: &Arc<MockClock>) -> LeaseHasher {
        LeaseHasher::new(
            fake.client(),
            "hk",
            "podcert",
            name,
            &format!("{name}-uid"),
            clock.clone(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn ensure_lease_creates_then_renews() {
        let fake = FakeApiServer::new();
        let clock = Arc::new(MockClock::new(start_time()));
        let h = hasher(&fake, "replica-a", &clock);
        h.ensure_lease().await.unwrap();
        let lease: Lease = fake.get("hk", "replica-a").unwrap();
        assert_eq!(
            lease.metadata.labels.as_ref().unwrap()[LABEL_KEY],
            "podcert"
        );
        let owner = &lease.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (owner.kind.as_str(), owner.uid.as_str(), owner.controller),
            ("Pod", "replica-a-uid", Some(true))
        );
        let spec = lease.spec.unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("replica-a"));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(spec.renew_time, spec.acquire_time);

        // Someone tampers with the lease; renewal restores it but keeps
        // the acquisition time.
        let mut tampered: Lease = fake.get("hk", "replica-a").unwrap();
        tampered.metadata.labels = None;
        tampered.spec.as_mut().unwrap().holder_identity = Some("other".into());
        Api::<Lease>::namespaced(fake.client(), "hk")
            .replace("replica-a", &PostParams::default(), &tampered)
            .await
            .unwrap();
        let clock2 = Arc::new(MockClock::new(start_time() + chrono::Duration::seconds(10)));
        let h = hasher(&fake, "replica-a", &clock2);
        h.ensure_lease().await.unwrap();
        let renewed = fake.get::<Lease>("hk", "replica-a").unwrap();
        assert_eq!(
            renewed.metadata.labels.as_ref().unwrap()[LABEL_KEY],
            "podcert"
        );
        let renewed = renewed.spec.unwrap();
        assert_eq!(renewed.holder_identity.as_deref(), Some("replica-a"));
        assert_eq!(renewed.acquire_time, spec.acquire_time);
        assert_eq!(
            renewed.renew_time.unwrap().0,
            spec.renew_time
                .unwrap()
                .0
                .checked_add(k8s_openapi::jiff::SignedDuration::from_secs(10))
                .unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn assignments_follow_live_leases() {
        let fake = FakeApiServer::new();
        let clock = Arc::new(MockClock::new(start_time()));
        let a = hasher(&fake, "replica-a", &clock);
        let b = hasher(&fake, "replica-b", &clock);
        a.ensure_lease().await.unwrap();
        b.ensure_lease().await.unwrap();
        // A lease of another application is ignored.
        let other = LeaseHasher::new(
            fake.client(),
            "hk",
            "other-app",
            "replica-c",
            "c",
            clock.clone(),
        );
        other.ensure_lease().await.unwrap();

        let task = a.informer().spawn();
        a.informer().wait_synced().await;
        wait_until(|| a.live_replicas().len() == 2).await;
        let mut live = a.live_replicas();
        live.sort();
        assert_eq!(live, ["replica-a", "replica-b"]);

        for i in 0..50 {
            let item = format!("ns/req-{i}");
            let owner = assign(&item, ["replica-a", "replica-b"]);
            assert_eq!(
                a.assigned_to_this_replica(&item),
                owner == "replica-a",
                "{item}"
            );
        }

        // Once replica-b stops renewing, everything is replica-a's.
        let later = Arc::new(MockClock::new(start_time() + chrono::Duration::seconds(16)));
        let mut late_a = hasher(&fake, "replica-a", &later);
        late_a.informer = a.informer().clone();
        // replica-a's own lease is just as stale, so renew it first.
        late_a.ensure_lease().await.unwrap();
        wait_until(|| late_a.live_replicas() == ["replica-a"]).await;
        for i in 0..20 {
            assert!(late_a.assigned_to_this_replica(&format!("ns/req-{i}")));
        }
        task.abort();
    }
}
