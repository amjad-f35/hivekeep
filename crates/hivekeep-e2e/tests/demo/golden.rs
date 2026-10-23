//! The golden snapshot: built for a template in its own hive, owned by a Tag
//! named after the template, restored by new Actors and collected with the
//! template.

use std::collections::BTreeMap;
use std::time::Duration;

use hivekeep_core::resource::ActorRef;
use hivekeep_core::snapshot::{SnapshotOwner, SnapshotUri};
use hivekeep_e2e::namespace::create_namespace;
use hivekeep_e2e::suite::{clients, e2e_test};
use hivekeep_e2e::template::{create_counter_template, template_ref, HiveTemplateOptions};
use hivekeep_proto::api::v1 as pb;
use tonic::Code;

use crate::helpers::*;

#[test]
#[ignore = "needs a live cluster: set HK_E2E=1 and HK_E2E_KUBECONFIG"]
fn golden_snapshot_lifecycle() {
    e2e_test("TestGoldenSnapshotLifecycle", |t| async move {
        for fidelity in [pb::SnapshotFidelity::Memory, pb::SnapshotFidelity::Volumes] {
            t.run(fidelity.as_str_name(), move |t| async move {
                let c = clients();
                let ns = create_namespace(&t).await;
                let bucket = require_env(&t, "HK_E2E_BUCKET_NAME");
                // A hive of its own catches placement in a system hive and
                // keeps cleanup apart from the suite's shared demo hive.
                let hive = format!("golden-{}", ns.name);
                {
                    let (c, hive) = (c.clone(), hive.clone());
                    t.cleanup(move |_| async move {
                        let req = pb::DeleteHiveRequest {
                            hive: Some(pb::ObjectRef {
                                hive: String::new(),
                                name: hive,
                            }),
                            ..Default::default()
                        };
                        let _ =
                            tokio::time::timeout(Duration::from_secs(60), c.api().delete_hive(req))
                                .await;
                    });
                }
                let created = create_counter_template(
                    &t,
                    &c,
                    &ns.name,
                    HiveTemplateOptions {
                        hive: hive.clone(),
                        name: "counter".into(),
                        pool_name: "counter".into(),
                        pool_replicas: 1,
                        labels: BTreeMap::from([("golden-e2e".to_string(), ns.name.clone())]),
                        snapshot_config: Some(pb::SnapshotConfig {
                            storage_location: format!("gs://{bucket}/{hive}"),
                            preferred_fidelity: fidelity as i32,
                            ..Default::default()
                        }),
                        modify: None,
                    },
                )
                .await;
                let tref = template_ref(&created);
                let tmpl = c
                    .api()
                    .get_actor_template(pb::GetActorTemplateRequest {
                        actor_template: Some(tref.clone()),
                    })
                    .await
                    .unwrap_or_else(|e| t.fatal(e))
                    .into_inner();
                let uid = tmpl.metadata.clone().unwrap_or_default().uid;
                let tag_ref = tmpl
                    .status
                    .clone()
                    .and_then(|s| s.golden_snapshot_status)
                    .and_then(|g| g.golden_tag)
                    .unwrap_or_default();
                if tag_ref.hive != hive {
                    t.fatal(format!(
                        "golden tag hive = {:?}, want {hive:?}",
                        tag_ref.hive
                    ));
                }
                if tag_ref.name != uid {
                    t.fatal(format!(
                        "golden tag name = {:?}, want template UID {uid:?}",
                        tag_ref.name
                    ));
                }
                let tag = c
                    .api()
                    .get_tag(pb::GetTagRequest {
                        tag: Some(tag_ref.clone()),
                    })
                    .await
                    .unwrap_or_else(|e| t.fatal(e))
                    .into_inner();
                let source = tag.source_actor.clone().unwrap_or_default();
                if source.hive != hive || source.name != uid {
                    t.fatal(format!(
                        "golden source actor = {source:?}, want {hive}/{uid}"
                    ));
                }
                let got = tag
                    .status
                    .as_ref()
                    .and_then(|s| s.snapshot.as_ref())
                    .map(|s| s.fidelity)
                    .unwrap_or_default();
                if got != fidelity as i32 {
                    t.fatal(format!("golden fidelity = {got}, want {fidelity:?}"));
                }
                let uri =
                    SnapshotUri::parse(&tag_snapshot_uri(&tag)).unwrap_or_else(|e| t.fatal(e));
                let tag_uid = tag.metadata.clone().unwrap_or_default().uid;
                if !uri.owned_by(&SnapshotOwner::tag(hive.clone(), tag_uid)) {
                    t.fatal(format!(
                        "golden snapshot {uri} is not owned by its tenant tag"
                    ));
                }

                let actor_ref = pb::ObjectRef {
                    hive: hive.clone(),
                    name: "clone".into(),
                };
                {
                    let (c, r) = (c.clone(), actor_ref.clone());
                    t.cleanup(move |_| async move {
                        let _ = tokio::time::timeout(Duration::from_secs(60), delete_actor(&c, &r))
                            .await;
                    });
                }
                let actor = c
                    .api()
                    .create_actor(pb::CreateActorRequest {
                        actor: Some(pb::Actor {
                            metadata: Some(pb::ResourceMetadata {
                                hive: hive.clone(),
                                name: actor_ref.name.clone(),
                                ..Default::default()
                            }),
                            actor_template: Some(tref.clone()),
                            ..Default::default()
                        }),
                    })
                    .await
                    .unwrap_or_else(|e| t.fatal(e))
                    .into_inner();
                let got = durable_snapshot_uri(&status(&actor));
                if got != uri.as_str() {
                    t.fatal(format!(
                        "clone snapshot = {got:?}, want golden {:?}",
                        uri.as_str()
                    ));
                }
                if let Err(e) = resume(&t, &c, &actor_ref).await {
                    t.fatal(e);
                }
                let resp = call_actor(&t, &c, &ActorRef::from_object_ref(&actor_ref))
                    .await
                    .unwrap_or_else(|e| t.fatal(e));
                validate_counter_response(&t, &resp, "golden restore", 1, 1);
                // The borrower goes before the template's snapshot is collected.
                if let Err(e) = delete_actor(&c, &actor_ref).await {
                    t.fatal(e);
                }
                if let Err(e) = c
                    .api()
                    .delete_actor_template(pb::DeleteActorTemplateRequest {
                        actor_template: Some(tref.clone()),
                        ..Default::default()
                    })
                    .await
                {
                    t.fatal(e);
                }
                match c
                    .api()
                    .get_tag(pb::GetTagRequest {
                        tag: Some(tag_ref.clone()),
                    })
                    .await
                {
                    Err(e) if e.code() == Code::NotFound => {}
                    other => t.fatal(format!(
                        "golden tag after template deletion = {:?}, want NotFound",
                        other.err()
                    )),
                }
                match get_actor(&c, &tag_ref).await {
                    Err(e) if e.code() == Code::NotFound => {}
                    other => t.fatal(format!(
                        "golden actor after template deletion = {:?}, want NotFound",
                        other.err()
                    )),
                }
                t.log(format!(
                    "golden lifecycle passed: fidelity={fidelity:?}, snapshot={uri}"
                ));
            })
            .await;
        }
    });
}
