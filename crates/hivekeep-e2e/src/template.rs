//! Per-test ActorTemplates copied from an installed fixture, and waiting
//! for golden snapshots.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use hivekeep_proto::api::v1 as pb;
use kube::api::{Api, DynamicObject, PostParams};
use tonic::{Code, Status};

use crate::clients::Clients;
use crate::crd;
use crate::harness::T;
use crate::sandbox::{counter_fixture, template_ready_timeout, HiveFixture};

/// The template reference an Actor carries.
pub fn template_ref(at: &pb::ActorTemplate) -> pb::ObjectRef {
    let m = at.metadata.clone().unwrap_or_default();
    pb::ObjectRef {
        hive: m.hive,
        name: m.name,
    }
}

/// An edit applied to a template before it is created.
pub type TemplateEdit = Box<dyn FnOnce(&mut pb::ActorTemplate) + Send>;

/// Shapes [`create_template_from`].
#[derive(Default)]
pub struct HiveTemplateOptions {
    /// The new template's hive; created if missing. Hives are shared across
    /// a suite's tests, so `name` must be unique within it.
    pub hive: String,
    /// The new template's name.
    pub name: String,
    /// The WorkerPool created in the test's namespace.
    pub pool_name: String,
    /// Its replica count.
    pub pool_replicas: i32,
    /// Labels tying the template's worker selector to the pool, keeping
    /// this pool's workers invisible to other namespaces' Actors.
    pub labels: BTreeMap<String, String>,
    /// Snapshot config for the new template; `None` copies the source's.
    pub snapshot_config: Option<pb::SnapshotConfig>,
    /// Edits the template before it is created.
    pub modify: Option<TemplateEdit>,
}

/// Creates a per-test WorkerPool plus an ActorTemplate copying the resolved
/// runtime from the counter demo for the sandbox class under test.
pub async fn create_counter_template(
    t: &T,
    clients: &Clients,
    namespace: &str,
    opts: HiveTemplateOptions,
) -> pb::ActorTemplate {
    create_template_from(t, clients, namespace, &counter_fixture(), opts).await
}

/// Creates a per-test WorkerPool in `namespace` plus an ActorTemplate
/// copying the resolved runtime (worker image and sandbox class from the
/// pool; containers, limits and volumes from the template) out of the
/// installed fixture `src`. The template is deleted when the test ends (it
/// does not ride namespace deletion), and the call blocks until its golden
/// snapshot exists.
pub async fn create_template_from(
    t: &T,
    clients: &Clients,
    namespace: &str,
    src: &HiveFixture,
    opts: HiveTemplateOptions,
) -> pb::ActorTemplate {
    let pools: Api<DynamicObject> = Api::namespaced_with(
        clients.kube(),
        &src.pool_namespace,
        &crd::worker_pool_resource(),
    );
    let existing = pools.get(&src.pool_name).await.unwrap_or_else(|e| {
        t.fatal(format!(
            "failed to get WorkerPool {}/{} (deploy with: {}): {e}",
            src.pool_namespace, src.pool_name, src.deploy_with
        ))
    });
    let existing = crd::worker_pool_spec(&existing).unwrap_or_else(|e| {
        t.fatal(format!(
            "decoding WorkerPool {}/{}: {e}",
            src.pool_namespace, src.pool_name
        ))
    });
    let src_tmpl = clients
        .api()
        .get_actor_template(pb::GetActorTemplateRequest {
            actor_template: Some(pb::ObjectRef {
                hive: src.hive.clone(),
                name: src.name.clone(),
            }),
        })
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "failed to get ActorTemplate {}/{} (deploy with: {}): {e}",
                src.hive, src.name, src.deploy_with
            ))
        })
        .into_inner();

    let spec = crd::WorkerPoolSpec {
        replicas: opts.pool_replicas,
        worker_image: existing.worker_image,
        template: None,
        sandbox_classes: existing.sandbox_classes,
    };
    let pool = crd::new_worker_pool(namespace, &opts.pool_name, opts.labels.clone(), &spec);
    Api::<DynamicObject>::namespaced_with(clients.kube(), namespace, &crd::worker_pool_resource())
        .create(&PostParams::default(), &pool)
        .await
        .unwrap_or_else(|e| t.fatal(format!("failed to create WorkerPool: {e}")));

    // CreateActorTemplate needs the hive first.
    ensure_hive(t, clients, &opts.hive).await;

    let mut tmpl = pb::ActorTemplate {
        metadata: Some(pb::ResourceMetadata {
            hive: opts.hive.clone(),
            name: opts.name.clone(),
            ..Default::default()
        }),
        worker_selector: Some(pb::Selector {
            match_labels: opts.labels.clone().into_iter().collect(),
        }),
        containers: src_tmpl.containers.clone(),
        // The source's limits size the sandbox, and the API requires them.
        resources: src_tmpl.resources,
        snapshot_config: opts
            .snapshot_config
            .clone()
            .or(src_tmpl.snapshot_config.clone()),
        volumes: src_tmpl.volumes.clone(),
        ..Default::default()
    };
    if let Some(modify) = opts.modify {
        modify(&mut tmpl);
    }
    let created = clients
        .api()
        .create_actor_template(pb::CreateActorTemplateRequest {
            actor_template: Some(tmpl),
        })
        .await
        .unwrap_or_else(|e| {
            t.fatal(format!(
                "failed to create ActorTemplate {}/{}: {e}",
                opts.hive, opts.name
            ))
        })
        .into_inner();
    // Registered before the golden wait, so a template whose golden never
    // builds is still removed.
    register_template_cleanup(t, clients, &opts.hive, &opts.name);
    t.log(format!(
        "Waiting for ActorTemplate {}/{} golden snapshot...",
        opts.hive, opts.name
    ));
    wait_for_template_ready(t, clients, &opts.hive, &opts.name).await;
    created
}

/// Creates `hive`, tolerating one that exists.
pub async fn ensure_hive(t: &T, clients: &Clients, hive: &str) {
    let req = pb::CreateHiveRequest {
        hive: Some(pb::Hive {
            metadata: Some(pb::ResourceMetadata {
                name: hive.to_string(),
                ..Default::default()
            }),
        }),
    };
    if let Err(e) = clients.api().create_hive(req).await {
        if e.code() != Code::AlreadyExists {
            t.fatal(format!("failed to create hive {hive:?}: {e}"));
        }
    }
}

/// Deletes `hive/name` when the test ends, tolerating absence.
pub fn register_template_cleanup(t: &T, clients: &Clients, hive: &str, name: &str) {
    let mut api = clients.api();
    let r = pb::ObjectRef {
        hive: hive.to_string(),
        name: name.to_string(),
    };
    t.cleanup(move |t| async move {
        let req = pb::DeleteActorTemplateRequest {
            actor_template: Some(r.clone()),
            ..Default::default()
        };
        match tokio::time::timeout(Duration::from_secs(60), api.delete_actor_template(req)).await {
            Ok(Err(e)) if e.code() != Code::NotFound => t.log(format!(
                "failed to delete ActorTemplate {}/{}: {e}",
                r.hive, r.name
            )),
            Err(_) => t.log(format!(
                "failed to delete ActorTemplate {}/{}: timed out",
                r.hive, r.name
            )),
            _ => {}
        }
    });
}

/// Reads an ActorTemplate; implemented by the API client and by test
/// fakes.
pub trait TemplateSource {
    /// Gets `hive/name`.
    fn get_template(
        &self,
        hive: &str,
        name: &str,
    ) -> impl std::future::Future<Output = Result<pb::ActorTemplate, Status>> + Send;
}

impl TemplateSource for Clients {
    async fn get_template(&self, hive: &str, name: &str) -> Result<pb::ActorTemplate, Status> {
        self.api()
            .get_actor_template(pb::GetActorTemplateRequest {
                actor_template: Some(pb::ObjectRef {
                    hive: hive.to_string(),
                    name: name.to_string(),
                }),
            })
            .await
            .map(|r| r.into_inner())
    }
}

/// Blocks until the ActorTemplate's golden snapshot exists, returning at
/// once for a template that opted out of it. The budget follows the
/// sandbox class under test (see [`template_ready_timeout`]).
pub async fn wait_for_template_ready(t: &T, source: &impl TemplateSource, hive: &str, name: &str) {
    let timeout = template_ready_timeout(t);
    let deadline = Instant::now() + timeout;
    let mut last_status = None;
    let mut last_err = None;
    loop {
        match source.get_template(hive, name).await {
            Ok(at) => {
                let mode = at
                    .snapshot_config
                    .as_ref()
                    .and_then(|s| s.golden_snapshot_config.as_ref())
                    .map(|g| g.mode);
                if mode == Some(pb::GoldenSnapshotMode::Disabled as i32) {
                    // No golden snapshot will ever appear, and none is needed.
                    return;
                }
                let status = at
                    .status
                    .and_then(|s| s.golden_snapshot_status)
                    .unwrap_or_default();
                if status
                    .golden_tag
                    .as_ref()
                    .is_some_and(|g| !g.name.is_empty())
                {
                    return;
                }
                if !status.error_message.is_empty() {
                    t.fatal(format!(
                        "ActorTemplate {hive}/{name} failed to build its golden snapshot: {}",
                        status.error_message
                    ));
                }
                last_status = Some(status);
            }
            Err(e) => last_err = Some(e),
        }
        if Instant::now() > deadline {
            t.fatal(format!("timed out after {timeout:?} waiting for ActorTemplate {hive}/{name} golden snapshot (last status {last_status:?}, err {last_err:?})"));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::{run_outcome, Outcome};

    /// Serves one ActorTemplate that opted out of the golden snapshot and
    /// has no golden status.
    struct DisabledGolden;

    impl TemplateSource for DisabledGolden {
        async fn get_template(&self, hive: &str, name: &str) -> Result<pb::ActorTemplate, Status> {
            Ok(pb::ActorTemplate {
                metadata: Some(pb::ResourceMetadata {
                    hive: hive.into(),
                    name: name.into(),
                    ..Default::default()
                }),
                snapshot_config: Some(pb::SnapshotConfig {
                    golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
                        mode: pb::GoldenSnapshotMode::Disabled as i32,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            })
        }
    }

    /// A template that opted out never gets a golden tag or an error, so the
    /// wait must return instead of riding its timeout.
    #[tokio::test]
    async fn wait_returns_for_a_disabled_golden_template() {
        let started = Instant::now();
        let outcome = run_outcome("Disabled", |t| async move {
            wait_for_template_ready(&t, &DisabledGolden, "hivekeep-demo", "counter").await
        })
        .await;
        assert_eq!(outcome, Outcome::Passed);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn template_refs() {
        let at = pb::ActorTemplate {
            metadata: Some(pb::ResourceMetadata {
                hive: "h".into(),
                name: "n".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            template_ref(&at),
            pb::ObjectRef {
                hive: "h".into(),
                name: "n".into()
            }
        );
    }
}
