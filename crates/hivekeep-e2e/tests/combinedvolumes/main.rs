//! The three volume sources (image volumes, durable dirs and external
//! volumes) mounted together on one Actor, with the mount semantics they
//! share, against a live cluster.

mod registry;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use hivekeep_core::preview::Gate;
use hivekeep_core::resource::ActorRef;
use hivekeep_e2e::clients::Clients;
use hivekeep_e2e::env::check_env;
use hivekeep_e2e::harness::T;
use hivekeep_e2e::images::REGISTRY_ENV;
use hivekeep_e2e::namespace::{create_namespace, Namespace};
use hivekeep_e2e::preview::{is_enabled, require_preview};
use hivekeep_e2e::probe::{deploy_probe, ProbeOptions, PROBE_NAME};
use hivekeep_e2e::retry::resume_actor_await_capacity;
use hivekeep_e2e::router_client::RouterClient;
use hivekeep_e2e::sandbox::HiveFixture;
use hivekeep_e2e::settings::storage_class;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::{create_template_from, template_ref, HiveTemplateOptions};
use hivekeep_proto::api::v1 as pb;
use k8s_openapi::api::storage::v1::StorageClass;
use kube::api::Api;

const HIVE: &str = "combinedvolumes";

// Must not collide with anything the probe image ships. One image volume
// may be mounted at several paths.
const MOUNT_PATH: &str = "/mnt/hk-image-volume";
const MOUNT_PATH_ALIAS: &str = "/mnt/hk-image-volume-alias";

// The scratch durable dir, mounted at both; a write through one must be
// readable through the other.
const SCRATCH_PATH_A: &str = "/mnt/hk-scratch-a";
const SCRATCH_PATH_B: &str = "/mnt/hk-scratch-b";

// The external volume, provisioned per Actor and detached on suspend: it
// must come back attached once and visible at both paths.
const EXT_VOLUME: &str = "external";
const EXT_PATH_A: &str = "/mnt/hk-external-a";
const EXT_PATH_B: &str = "/mnt/hk-external-b";
const EXT_CAPACITY: i64 = 1 << 30;

/// What the probe's `/writefile` writes.
const PROBE_WRITTEN_CONTENT: &str = "written by probe";

const PAYLOAD_NAME: &str = "payload.txt";
const PAYLOAD_CONTENT: &str = "delivered by an image volume";
/// Written by two layers; the upper must win.
const SHADOWED_NAME: &str = "shadowed.txt";
const SHADOWED_CONTENT: &str = "from the middle layer";
/// Shipped by the bottom layer and whited out by the top.
const DELETED_NAME: &str = "deleted.txt";
/// Written after the last snapshot, so a revert's effect on a volume shows
/// as whether it still exists.
const POST_SNAPSHOT_NAME: &str = "post-snapshot.txt";

/// Pushes the three-layer fixture image and returns its digest-pinned
/// reference. A unique tag per run, since some registries refuse to
/// overwrite one; the returned reference is digest-pinned anyway.
async fn build_fixture_image(t: &T, repo: &str) -> String {
    let layers = [
        registry::tar_layer(&[
            (PAYLOAD_NAME, "from-the-bottom-layer"),
            (SHADOWED_NAME, "from-the-bottom-layer"),
            (DELETED_NAME, "should-not-survive"),
        ]),
        registry::tar_layer(&[(SHADOWED_NAME, SHADOWED_CONTENT)]),
        registry::tar_layer(&[
            (PAYLOAD_NAME, PAYLOAD_CONTENT),
            (&format!(".wh.{DELETED_NAME}"), ""),
        ]),
    ]
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap_or_else(|e| t.fatal(format!("building layers: {e}")));
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let reference = format!(
        "{}/hivekeep-e2e-combinedvolumes-fixture:{nanos}",
        repo.trim_end_matches('/')
    );
    registry::push(&reference, layers)
        .await
        .unwrap_or_else(|e| t.fatal(format!("pushing {reference:?}: {e}")))
}

/// The StorageClass to provision the external volume from, or "" when
/// external volumes are off or the class is missing: both are optional, so
/// the external cases skip instead of failing the suite.
async fn storage_class_or_empty(t: &T, clients: &Clients) -> String {
    if !is_enabled(Gate::ExternalVolumes) {
        t.log(format!(
            "preview gate {:?} is not enabled; the external-volume cases will be skipped",
            Gate::ExternalVolumes.name()
        ));
        return String::new();
    }
    let class = storage_class();
    if let Err(e) = Api::<StorageClass>::all(clients.kube()).get(&class).await {
        t.log(format!(
            "StorageClass {class:?} not found ({e}); the external-volume case will be skipped"
        ));
        return String::new();
    }
    class
}

fn mount(name: &str, path: &str) -> pb::VolumeMount {
    pb::VolumeMount {
        name: name.into(),
        mount_path: path.into(),
    }
}

/// A probe template with the fixture as an image volume, copying the
/// resolved runtime from the shared probe template. Its name is suffixed per
/// run since the hive outlives the per-test namespace. A non-empty
/// `storage_class` adds the external volume.
async fn create_template(
    t: &T,
    clients: &Arc<Clients>,
    ns: &Namespace,
    fixture_image: &str,
    storage_class: &str,
) -> pb::ActorTemplate {
    let env =
        check_env(&["HK_E2E_BUCKET_NAME"]).unwrap_or_else(|e| t.fatal(format!("CheckEnv: {e}")));
    let bucket = &env["HK_E2E_BUCKET_NAME"];
    let (probe_hive, _) = deploy_probe(
        t,
        clients,
        bucket,
        "combinedvolumes",
        ProbeOptions::default(),
    )
    .await;
    let src = HiveFixture {
        hive: probe_hive.clone(),
        name: PROBE_NAME.into(),
        pool_namespace: probe_hive,
        pool_name: PROBE_NAME.into(),
        deploy_with: "the combinedvolumes suite's own probe deployment".into(),
    };
    let (image, class) = (fixture_image.to_string(), storage_class.to_string());
    let opts = HiveTemplateOptions {
        hive: HIVE.into(),
        name: format!("probe-{}", ns.name),
        pool_name: PROBE_NAME.into(),
        pool_replicas: 2,
        // Unique to this namespace, so no other suite's Actors get its workers.
        labels: BTreeMap::from([("combinedvolumes".to_string(), ns.name.clone())]),
        snapshot_config: Some(pb::SnapshotConfig {
            storage_location: format!("gs://{bucket}/{}/", ns.name),
            ..Default::default()
        }),
        modify: Some(Box::new(move |tmpl: &mut pb::ActorTemplate| {
            // Every volume at two paths: read-only (image), writable
            // (durable dir) and per-Actor provisioned (external).
            let c = &mut tmpl.containers[0];
            c.volume_mounts.extend([
                mount("fixture", MOUNT_PATH),
                mount("fixture", MOUNT_PATH_ALIAS),
                mount("scratch", SCRATCH_PATH_A),
                mount("scratch", SCRATCH_PATH_B),
            ]);
            tmpl.volumes.push(pb::Volume {
                name: "fixture".into(),
                image: Some(pb::ImageVolumeSource { reference: image }),
                ..Default::default()
            });
            tmpl.volumes.push(pb::Volume {
                name: "scratch".into(),
                durable_dir: Some(pb::DurableDirVolumeSource {}),
                ..Default::default()
            });
            if class.is_empty() {
                return;
            }
            tmpl.containers[0]
                .volume_mounts
                .extend([mount(EXT_VOLUME, EXT_PATH_A), mount(EXT_VOLUME, EXT_PATH_B)]);
            tmpl.volumes.push(pb::Volume {
                name: EXT_VOLUME.into(),
                external: Some(pb::ExternalVolumeSource {
                    template: Some(pb::ExternalVolumeTemplate {
                        capacity_bytes: EXT_CAPACITY,
                        storage_class_name: class,
                    }),
                }),
                ..Default::default()
            });
        })),
    };
    create_template_from(t, clients, &ns.name, &src, opts).await
}

/// Calls a probe endpoint through the router and decodes its reply.
async fn probe_json(
    t: &T,
    router: &RouterClient,
    actor: &ActorRef,
    path: &str,
) -> HashMap<String, String> {
    let resp = router
        .get(actor, path)
        .await
        .unwrap_or_else(|e| t.fatal(format!("GET {path}: {e}")));
    if resp.status != 200 {
        t.fatal(format!(
            "GET {path}: status {}: {}",
            resp.status,
            resp.text()
        ));
    }
    serde_json::from_slice(&resp.body).unwrap_or_else(|e| t.fatal(format!("decoding {path}: {e}")))
}

fn field<'a>(m: &'a HashMap<String, String>, k: &str) -> &'a str {
    m.get(k).map(String::as_str).unwrap_or("")
}

/// Fails unless `path` holds `want`.
async fn require_content(t: &T, router: &RouterClient, actor: &ActorRef, path: &str, want: &str) {
    let got = probe_json(t, router, actor, &format!("/readfile?path={path}")).await;
    if !field(&got, "error").is_empty() {
        t.fatal(format!("reading {path}: {}", field(&got, "error")));
    }
    if field(&got, "content") != want {
        t.error(format!(
            "content at {path} = {:?}, want {want:?}",
            field(&got, "content")
        ));
    }
}

/// Fails unless both paths hold `want`: both mounts live and backed by one
/// volume.
async fn require_content_at_both(
    t: &T,
    router: &RouterClient,
    actor: &ActorRef,
    a: &str,
    b: &str,
    want: &str,
) {
    require_content(t, router, actor, a, want).await;
    require_content(t, router, actor, b, want).await;
}

/// Fails if `path` can be read.
async fn require_unreadable(t: &T, router: &RouterClient, actor: &ActorRef, path: &str) {
    let got = probe_json(t, router, actor, &format!("/readfile?path={path}")).await;
    if field(&got, "error").is_empty() {
        t.error(format!(
            "{path} is readable ({:?}), want it hidden",
            field(&got, "content")
        ));
    }
}

/// Fails if `path` can be written.
async fn require_write_rejected(t: &T, router: &RouterClient, actor: &ActorRef, path: &str) {
    let got = probe_json(t, router, actor, &format!("/writefile?path={path}")).await;
    if field(&got, "error").is_empty() {
        t.error(format!(
            "write to {path} succeeded, want it rejected as read-only"
        ));
    }
}

/// Writes through `path`, failing if rejected.
async fn require_write(t: &T, router: &RouterClient, actor: &ActorRef, path: &str) {
    let got = probe_json(t, router, actor, &format!("/writefile?path={path}")).await;
    if !field(&got, "error").is_empty() {
        t.fatal(format!("writing {path}: {}", field(&got, "error")));
    }
}

/// Writes through `write_path` and requires the content at both paths.
async fn require_shared_write(
    t: &T,
    router: &RouterClient,
    actor: &ActorRef,
    write_path: &str,
    alias_path: &str,
) {
    require_write(t, router, actor, write_path).await;
    require_content_at_both(
        t,
        router,
        actor,
        write_path,
        alias_path,
        PROBE_WRITTEN_CONTENT,
    )
    .await;
}

/// Fails unless the Actor records `want` as its assigned node ("" for none).
fn require_assigned_node(t: &T, actor: &pb::Actor, want: &str) {
    let got = actor.status.clone().unwrap_or_default().assigned_node;
    if got != want {
        t.error(format!("actor assigned_node = {got:?}, want {want:?}"));
    }
}

/// Fails unless the running Actor's assigned node is its worker's node;
/// returns that node.
async fn require_assigned_to_worker_node(t: &T, clients: &Clients, actor: &ActorRef) -> String {
    let got = clients
        .api()
        .get_actor(pb::GetActorRequest {
            actor: Some(actor.to_object_ref()),
        })
        .await
        .unwrap_or_else(|e| t.fatal(format!("GetActor: {e}")))
        .into_inner();
    let assignment = got.status.clone().unwrap_or_default().worker_assignment;
    let node = assignment.clone().unwrap_or_default().node_name;
    if node.is_empty() {
        t.fatal(format!(
            "running actor's worker assignment names no node: {assignment:?}"
        ));
    }
    require_assigned_node(t, &got, &node);
    node
}

async fn resume_within(t: &T, clients: &Clients, actor: &ActorRef, what: &str) {
    match tokio::time::timeout(
        Duration::from_secs(120),
        resume_actor_await_capacity(
            t,
            clients,
            pb::ResumeActorRequest {
                actor: Some(actor.to_object_ref()),
            },
        ),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => t.fatal(format!("ResumeActor {what}: {e}")),
        Err(_) => t.fatal(format!("ResumeActor {what}: timed out")),
    }
}

/// Skips unless the external volume is configured.
fn require_external_volume(t: &T, storage_class: &str) {
    require_preview(t, Gate::ExternalVolumes);
    if storage_class.is_empty() {
        t.skip(format!(
            "StorageClass {:?} is not installed",
            hivekeep_e2e::settings::storage_class()
        ));
    }
}

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn combined_volumes() {
    e2e_test("TestCombinedVolumes", |t| async move {
        let repo = std::env::var(REGISTRY_ENV).unwrap_or_default();
        if repo.is_empty() {
            t.skip(format!("{REGISTRY_ENV} is unset; it names the registry both this host and the cluster can reach"));
        }
        let clients = clients();
        let ns = create_namespace(&t).await;

        let fixture_image = build_fixture_image(&t, &repo).await;
        t.log(format!("fixture image: {fixture_image}"));
        let sc = storage_class_or_empty(&t, &clients).await;
        let tmpl = create_template(&t, &clients, &ns, &fixture_image, &sc).await;

        let actor = ActorRef::new(HIVE, format!("cv-{}", ns.name));
        let create = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: actor.hive.clone(),
                name: actor.name.clone(),
                ..Default::default()
            }),
            actor_template: Some(template_ref(&tmpl)),
            ..Default::default()
        };
        if let Err(e) = clients
            .api()
            .create_actor(pb::CreateActorRequest {
                actor: Some(create),
            })
            .await
        {
            t.fatal(format!("CreateActor: {e}"));
        }
        let (c, r) = (clients.clone(), actor.to_object_ref());
        t.cleanup(move |_| async move {
            let _ = c
                .api()
                .suspend_actor(pb::SuspendActorRequest {
                    actor: Some(r.clone()),
                    ..Default::default()
                })
                .await;
            let _ = c
                .api()
                .delete_actor(pb::DeleteActorRequest {
                    actor: Some(r),
                    ..Default::default()
                })
                .await;
        });
        if let Err(e) = resume_actor_await_capacity(
            &t,
            &clients,
            pb::ResumeActorRequest {
                actor: Some(actor.to_object_ref()),
            },
        )
        .await
        {
            t.fatal(format!("ResumeActor: {e}"));
        }
        require_assigned_to_worker_node(&t, &clients, &actor).await;

        let router = RouterClient::new(&t, &clients)
            .await
            .unwrap_or_else(|e| t.fatal(format!("NewRouterClient: {e}")));
        let (router, actor, sc, clients) = (&router, &actor, sc.as_str(), &clients);
        let payload = format!("{MOUNT_PATH}/{PAYLOAD_NAME}");
        let payload_alias = format!("{MOUNT_PATH_ALIAS}/{PAYLOAD_NAME}");
        let (payload, payload_alias) = (payload.as_str(), payload_alias.as_str());
        let scratch_a = format!("{SCRATCH_PATH_A}/multi.txt");
        let scratch_b = format!("{SCRATCH_PATH_B}/multi.txt");
        let ext_a = format!("{EXT_PATH_A}/multi.txt");
        let ext_b = format!("{EXT_PATH_B}/multi.txt");
        let (scratch_a, scratch_b, ext_a, ext_b) = (
            scratch_a.as_str(),
            scratch_b.as_str(),
            ext_a.as_str(),
            ext_b.as_str(),
        );

        t.run("DeliversImageContents", |t| async move {
            require_content(&t, router, actor, payload, PAYLOAD_CONTENT).await
        })
        .await;
        t.run("UpperLayerWins", |t| async move {
            require_content(
                &t,
                router,
                actor,
                &format!("{MOUNT_PATH}/{SHADOWED_NAME}"),
                SHADOWED_CONTENT,
            )
            .await
        })
        .await;
        t.run("WhiteoutHidesLowerLayerFile", |t| async move {
            require_unreadable(&t, router, actor, &format!("{MOUNT_PATH}/{DELETED_NAME}")).await
        })
        .await;
        t.run("MountIsReadOnly", |t| async move {
            require_write_rejected(&t, router, actor, &format!("{MOUNT_PATH}/should-not-exist"))
                .await
        })
        .await;
        t.run("SameImageVolumeAtTwoPaths", |t| async move {
            require_content_at_both(&t, router, actor, payload, payload_alias, PAYLOAD_CONTENT)
                .await
        })
        .await;
        t.run("SameDurableVolumeAtTwoPathsSharesWrites", |t| async move {
            require_shared_write(&t, router, actor, scratch_a, scratch_b).await
        })
        .await;
        t.run("SameExternalVolumeAtTwoPathsSharesWrites", |t| async move {
            require_external_volume(&t, sc);
            require_shared_write(&t, router, actor, ext_a, ext_b).await;
        })
        .await;

        t.run("SurvivesSuspendResume", |t| async move {
            let running_node = require_assigned_to_worker_node(&t, clients, actor).await;
            let req = pb::SuspendActorRequest {
                actor: Some(actor.to_object_ref()),
                options: Some(pb::SuspendOptions {
                    mode: pb::DurabilityMode::Synchronous as i32,
                }),
            };
            let suspended = clients
                .api()
                .suspend_actor(req)
                .await
                .unwrap_or_else(|e| t.fatal(format!("SuspendActor: {e}")))
                .into_inner();
            // A synchronous suspend keeps the node binding while the local
            // snapshot copy is retained.
            require_assigned_node(&t, &suspended.actor.unwrap_or_default(), &running_node);

            // Volume restore can outlast the router's request-parking budget
            // on a busy node, so resume through the control plane first.
            resume_within(&t, clients, actor, "after suspend").await;
            require_content_at_both(&t, router, actor, payload, payload_alias, PAYLOAD_CONTENT)
                .await;
            require_content_at_both(
                &t,
                router,
                actor,
                scratch_a,
                scratch_b,
                PROBE_WRITTEN_CONTENT,
            )
            .await;

            // Suspend detached the external volume; the resume must
            // reattach it once and restore both mounts.
            t.run("ExternalVolumeReattached", |t| async move {
                require_external_volume(&t, sc);
                require_content_at_both(&t, router, actor, ext_a, ext_b, PROBE_WRITTEN_CONTENT)
                    .await;
                require_assigned_to_worker_node(&t, clients, actor).await;
            })
            .await;
        })
        .await;

        // A revert rewinds to the last external snapshot.
        t.run("RevertRewindsOnlySnapshottedVolumes", |t| async move {
            // Written after the suspend above, so in no snapshot a revert
            // can return to.
            require_write(
                &t,
                router,
                actor,
                &format!("{SCRATCH_PATH_A}/{POST_SNAPSHOT_NAME}"),
            )
            .await;
            if !sc.is_empty() {
                require_write(
                    &t,
                    router,
                    actor,
                    &format!("{EXT_PATH_A}/{POST_SNAPSHOT_NAME}"),
                )
                .await;
            }
            let reverted = clients
                .api()
                .revert_actor(pb::RevertActorRequest {
                    actor: Some(actor.to_object_ref()),
                })
                .await
                .unwrap_or_else(|e| t.fatal(format!("RevertActor: {e}")))
                .into_inner()
                .actor
                .unwrap_or_default();
            let state = reverted
                .status
                .as_ref()
                .map(|s| s.state)
                .unwrap_or_default();
            if state != pb::ActorState::Suspended as i32 {
                t.fatal(format!(
                    "state after revert = {:?}, want SUSPENDED",
                    pb::ActorState::try_from(state)
                ));
            }
            require_assigned_node(&t, &reverted, "");
            resume_within(&t, clients, actor, "after revert").await;

            // The image volume is immutable and remounted from its digest.
            require_content_at_both(&t, router, actor, payload, payload_alias, PAYLOAD_CONTENT)
                .await;
            // The durable dir is in the snapshot: its write at snapshot time
            // returns and the later one is gone.
            require_content_at_both(
                &t,
                router,
                actor,
                scratch_a,
                scratch_b,
                PROBE_WRITTEN_CONTENT,
            )
            .await;
            require_unreadable(
                &t,
                router,
                actor,
                &format!("{SCRATCH_PATH_A}/{POST_SNAPSHOT_NAME}"),
            )
            .await;

            // An external volume's contents are never in a snapshot, so a
            // revert leaves it as the discarded execution left it.
            t.run("ExternalVolumeNotReverted", |t| async move {
                require_external_volume(&t, sc);
                require_content_at_both(
                    &t,
                    router,
                    actor,
                    &format!("{EXT_PATH_A}/{POST_SNAPSHOT_NAME}"),
                    &format!("{EXT_PATH_B}/{POST_SNAPSHOT_NAME}"),
                    PROBE_WRITTEN_CONTENT,
                )
                .await;
            })
            .await;
        })
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::registry;

    /// The registry helpers' pure parts: reference parsing, the image's
    /// content addressing, whiteout entries and the bearer challenge.
    #[test]
    fn fixture_image_shape() {
        assert_eq!(
            registry::parse_reference("localhost:5001/hk/x:1").unwrap(),
            ("localhost:5001".into(), "hk/x".into(), "1".into())
        );
        assert!(registry::parse_reference("noregistry").is_err());
        let layer = registry::tar_layer(&[("a.txt", "x"), (".wh.b.txt", "")]).unwrap();
        let mut archive = tar::Archive::new(layer.data.as_slice());
        let names: Vec<String> = archive
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().display().to_string())
            .collect();
        assert_eq!(names, vec!["a.txt", ".wh.b.txt"]);
        let (config, manifest) = registry::image(&[layer]);
        let m: serde_json::Value = serde_json::from_slice(&manifest.data).unwrap();
        assert_eq!(m["config"]["digest"], config.digest());
        assert_eq!(m["layers"].as_array().unwrap().len(), 1);
        assert!(!registry::basic("u", "p").is_empty());
    }
}
