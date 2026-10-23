//! Deploying a fixture: the k8s pool half applied with kubectl, the
//! ActorTemplate half created through the API, golden snapshots awaited.

use std::time::Duration;

use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::clients::Clients;
use crate::harness::T;
use crate::manifest::{kubectl_apply, kubectl_delete, render_manifest, Substitutions};
use crate::protojson::decode_actor_templates;
use crate::sandbox::{fixture_substitutions, hive_template_substitutions, SandboxClass};
use crate::template::{ensure_hive, register_template_cleanup, wait_for_template_ready};

/// The two manifest templates a fixture is built from, relative to this
/// crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FixtureManifests {
    /// The k8s side: a Namespace plus the WorkerPool.
    pub pool: &'static str,
    /// One or more `---`-separated protojson ActorTemplate documents.
    pub template: &'static str,
}

/// Adjusts what [`deploy_hive_fixture`] installs.
#[derive(Debug, Clone, Default)]
pub struct FixtureOptions {
    /// Extra inline placeholders of the template manifest, keyed by bare
    /// name (`"BOOT_FETCH_URL"` fills `${BOOT_FETCH_URL}`).
    pub template_values: Vec<(String, String)>,
}

impl FixtureOptions {
    /// Replaces the inline placeholder `${name}` with `value`.
    pub fn with_template_value(mut self, name: &str, value: &str) -> Self {
        self.template_values
            .push((name.to_string(), value.to_string()));
        self
    }
}

/// The template substitutions of a fixture, with the caller's values.
pub fn template_substitutions(
    class: SandboxClass,
    bucket: &str,
    name: &str,
    trust_bundles: &[String],
    opts: &FixtureOptions,
) -> Substitutions {
    let mut subs = hive_template_substitutions(class, bucket, name, trust_bundles);
    for (k, v) in &opts.template_values {
        subs.inline(format!("${{{k}}}"), v.clone());
    }
    subs
}

/// Installs a fixture for the sandbox class under test and blocks until
/// every template's golden snapshot exists. `name` distinguishes the caller
/// (by convention its suite name): each suite gets its own copy, so no
/// suite's cleanup deletes one another is using.
///
/// Everything is removed when the test ends. The API resources need
/// explicit cleanup (they do not ride namespace deletion), and a template
/// leaked by an interrupted earlier run is cleared before its replacement
/// is created, since templates are immutable.
///
/// Returns the fixture's hive (also the name of the k8s namespace holding
/// the pool) and the created templates.
pub async fn deploy_hive_fixture(
    t: &T,
    clients: &Clients,
    manifests: FixtureManifests,
    bucket: &str,
    name: &str,
    trust_bundles: &[String],
    opts: FixtureOptions,
) -> (String, Vec<pb::ActorTemplate>) {
    let class = SandboxClass::current();
    // The pool manifest also carries the namespace. Its cleanup is
    // registered first so it runs last, after the templates selecting the
    // pool's workers are gone.
    let pool_manifest = render_manifest(
        t,
        manifests.pool,
        &fixture_substitutions(class, bucket, name),
    );
    kubectl_apply(t, &pool_manifest);
    t.cleanup(move |t| async move { kubectl_delete(&t, &pool_manifest) });

    let rendered = render_manifest(
        t,
        manifests.template,
        &template_substitutions(class, bucket, name, trust_bundles, &opts),
    );
    let text = std::fs::read_to_string(&rendered)
        .unwrap_or_else(|e| t.fatal(format!("reading {}: {e}", rendered.display())));
    let templates = decode_actor_templates(&text).unwrap_or_else(|e| t.fatal(e));
    let hive = templates[0].metadata.clone().unwrap_or_default().hive;
    for tmpl in &templates {
        let got = tmpl.metadata.clone().unwrap_or_default().hive;
        if got != hive {
            t.fatal(format!(
                "fixture {} declares templates in different hives ({hive:?} and {got:?})",
                manifests.template
            ));
        }
    }

    ensure_hive(t, clients, &hive).await;
    let mut api = clients.api();
    let h = hive.clone();
    t.cleanup(move |t| async move {
        // Best effort: a leaked Actor keeps the hive non-empty, and the next
        // run tolerates AlreadyExists anyway.
        let req = pb::DeleteHiveRequest {
            hive: Some(pb::ObjectRef {
                hive: String::new(),
                name: h.clone(),
            }),
            ..Default::default()
        };
        if let Ok(Err(e)) =
            tokio::time::timeout(Duration::from_secs(60), api.delete_hive(req)).await
        {
            if e.code() != Code::NotFound {
                t.log(format!("failed to delete hive {h:?}: {e}"));
            }
        }
    });

    let mut created = Vec::with_capacity(templates.len());
    for tmpl in templates {
        let tname = tmpl.metadata.clone().unwrap_or_default().name;
        let r = pb::ObjectRef {
            hive: hive.clone(),
            name: tname.clone(),
        };
        if let Err(e) = clients
            .api()
            .delete_actor_template(pb::DeleteActorTemplateRequest {
                actor_template: Some(r),
                ..Default::default()
            })
            .await
        {
            if e.code() != Code::NotFound {
                t.fatal(format!(
                    "failed to clear leaked ActorTemplate {hive}/{tname}: {e}"
                ));
            }
        }
        let c = clients
            .api()
            .create_actor_template(pb::CreateActorTemplateRequest {
                actor_template: Some(tmpl),
            })
            .await
            .unwrap_or_else(|e| {
                t.fatal(format!(
                    "failed to create ActorTemplate {hive}/{tname}: {e}"
                ))
            })
            .into_inner();
        // Registered before the golden wait so a template whose golden never
        // builds is still removed.
        register_template_cleanup(t, clients, &hive, &tname);
        created.push(c);
    }
    for tmpl in &created {
        let tname = tmpl.metadata.clone().unwrap_or_default().name;
        t.log(format!(
            "Waiting for ActorTemplate {hive}/{tname} golden snapshot..."
        ));
        wait_for_template_ready(t, clients, &hive, &tname).await;
    }
    (hive, created)
}

/// Renders the pool manifest at `rel_path` for the sandbox class under
/// test into a temp file and returns its path.
pub fn render_fixture_manifest(
    t: &T,
    rel_path: &str,
    bucket: &str,
    name: &str,
) -> std::path::PathBuf {
    render_manifest(
        t,
        rel_path,
        &fixture_substitutions(SandboxClass::current(), bucket, name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::WorkerPoolDoc;
    use crate::harness::run_test;
    use crate::manifest::{fixture_path, render_str, split_documents};

    /// Every fixture the harness deploys: pool half, template half, how many
    /// templates the latter yields, and the values its suite fills in.
    fn fixtures() -> Vec<(FixtureManifests, usize, FixtureOptions)> {
        let plain = FixtureOptions::default;
        vec![
            (crate::probe::PROBE_MANIFESTS, 1, plain()),
            (
                FixtureManifests {
                    pool: "fixtures/probe/probe.yaml.tmpl",
                    template: "fixtures/probe/probe-bootfetch-template.yaml.tmpl",
                },
                1,
                plain()
                    .with_template_value("BOOT_FETCH_HOST", "origin.example")
                    .with_template_value("BOOT_FETCH_URL", "https://origin.example/healthz"),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/probe/probe-sized.yaml.tmpl",
                    template: "fixtures/probe/probe-sized-template.yaml.tmpl",
                },
                1,
                plain(),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/probe/probe-multiactor.yaml.tmpl",
                    template: "fixtures/probe/probe-multiactor-template.yaml.tmpl",
                },
                1,
                plain(),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/probe/probe-parking.yaml.tmpl",
                    template: "fixtures/probe/probe-parking-template.yaml.tmpl",
                },
                1,
                plain(),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/security/hive-a-pool.yaml.tmpl",
                    template: "fixtures/security/hive-template.yaml.tmpl",
                },
                1,
                plain(),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/capabilities/capabilities.yaml.tmpl",
                    template: "fixtures/capabilities/capabilities-templates.yaml.tmpl",
                },
                3,
                plain(),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/testserver/websocket.yaml.tmpl",
                    template: "fixtures/testserver/websocket-template.yaml.tmpl",
                },
                1,
                plain(),
            ),
            (
                FixtureManifests {
                    pool: "fixtures/testserver/grpcecho.yaml.tmpl",
                    template: "fixtures/testserver/grpcecho-template.yaml.tmpl",
                },
                1,
                plain(),
            ),
        ]
    }

    /// Renders a pool manifest and strict-decodes its WorkerPool: the
    /// runtime block is injected as pre-indented text, so a placeholder at
    /// the wrong depth hangs the field off the wrong parent, which strict
    /// decoding reports instead of silently applying a pool that never gets
    /// a micro-VM worker.
    fn render_pool(class: SandboxClass, rel: &str) -> WorkerPoolDoc {
        let raw = std::fs::read_to_string(fixture_path(rel)).unwrap();
        let rendered = render_str(&raw, &fixture_substitutions(class, "test-bucket", "render"));
        let mut pool = None;
        for doc in split_documents(&rendered) {
            let v: serde_json::Value = serde_yaml_ng::from_str(doc)
                .unwrap_or_else(|e| panic!("rendered {rel} is not valid YAML: {e}\n{doc}"));
            if v.get("kind").and_then(|k| k.as_str()) != Some("WorkerPool") {
                continue;
            }
            pool = Some(
                serde_yaml_ng::from_str::<WorkerPoolDoc>(doc).unwrap_or_else(|e| {
                    panic!("rendered {rel} WorkerPool does not match the API type: {e}\n{doc}")
                }),
            );
        }
        pool.unwrap_or_else(|| panic!("rendered {rel} is missing a WorkerPool"))
    }

    /// Renders a template manifest and strict-decodes its documents; an
    /// unfilled placeholder fails, since it would reach the cluster as text.
    fn render_templates(
        class: SandboxClass,
        rel: &str,
        opts: &FixtureOptions,
    ) -> Vec<pb::ActorTemplate> {
        let raw = std::fs::read_to_string(fixture_path(rel)).unwrap();
        let rendered = render_str(
            &raw,
            &template_substitutions(class, "test-bucket", "render", &[], opts),
        );
        if let Some(i) = rendered.find("${") {
            panic!(
                "rendered {rel} keeps a placeholder: {:?}",
                &rendered[i..(i + 40).min(rendered.len())]
            );
        }
        decode_actor_templates(&rendered).unwrap_or_else(|e| panic!("{rel}: {e}"))
    }

    /// The default rendering: no micro-VM block survives, no placeholder is
    /// left, and the templates select their fixture's pool.
    #[test]
    fn render_fixtures_gvisor() {
        for (m, count, opts) in fixtures() {
            let pool = render_pool(SandboxClass::Gvisor, m.pool);
            assert!(
                pool.spec.worker_image.contains("hivekeep-sandbox-gvisor"),
                "{}: workerImage {}",
                m.pool,
                pool.spec.worker_image
            );
            assert_eq!(
                pool.spec.default_sandbox_class(),
                Some("gvisor"),
                "{}",
                m.pool
            );
            let templates = render_templates(SandboxClass::Gvisor, m.template, &opts);
            assert_eq!(templates.len(), count, "{}", m.template);
            let labels = pool.metadata.labels.clone().unwrap_or_default();
            for tmpl in templates {
                let name = tmpl.metadata.clone().unwrap_or_default().name;
                // An empty inline value must substitute, not delete its line:
                // the location is what the golden snapshot needs.
                let location = tmpl
                    .snapshot_config
                    .clone()
                    .unwrap_or_default()
                    .storage_location;
                assert!(
                    location.starts_with("gs://test-bucket/"),
                    "template {name} snapshot location {location:?}"
                );
                assert!(
                    !location.contains("-microvm"),
                    "template {name} snapshot location {location:?}"
                );
                // The selector is what ties the template to its fixture's pool.
                for (k, v) in tmpl
                    .worker_selector
                    .clone()
                    .unwrap_or_default()
                    .match_labels
                {
                    assert_eq!(labels.get(&k), Some(&v), "template {name} selects {k}={v}, which the pool's labels {labels:?} do not carry");
                }
            }
        }
    }

    /// The micro-VM rendering: templates carry limits and the snapshots
    /// land under their own prefix.
    #[test]
    fn render_fixtures_microvm() {
        for (m, count, opts) in fixtures() {
            let pool = render_pool(SandboxClass::MicroVm, m.pool);
            assert!(
                pool.spec.worker_image.contains("hivekeep-sandbox-microvm"),
                "{}: workerImage {}",
                m.pool,
                pool.spec.worker_image
            );
            assert_eq!(
                pool.spec.default_sandbox_class(),
                Some("microvm"),
                "{}",
                m.pool
            );
            let templates = render_templates(SandboxClass::MicroVm, m.template, &opts);
            assert_eq!(templates.len(), count, "{}", m.template);
            for tmpl in templates {
                let name = tmpl.metadata.clone().unwrap_or_default().name;
                let mem = tmpl
                    .resources
                    .unwrap_or_default()
                    .limits
                    .unwrap_or_default()
                    .memory_bytes;
                assert!(mem != 0, "template {name} declares no memory limit");
                let location = tmpl
                    .snapshot_config
                    .clone()
                    .unwrap_or_default()
                    .storage_location;
                assert!(
                    location.ends_with("-microvm-render/"),
                    "template {name} snapshot location {location:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn rendered_manifests_land_in_a_temp_file() {
        run_test("Render", |t| async move {
            let path = render_fixture_manifest(&t, "fixtures/probe/probe.yaml.tmpl", "b", "suite");
            assert_eq!(path.file_name().unwrap(), "probe.yaml");
            assert!(std::fs::read_to_string(&path)
                .unwrap()
                .contains("hivekeep-e2e-probe-suite"));
        })
        .await;
    }
}
