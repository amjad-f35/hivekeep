//! Tag request validation and the check of a merged update result.

mod common;

use common::*;
use hivekeep_core::field::{ErrorList, Path};
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

const ACTOR_UID: &str = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e";
const TAG_UID: &str = "2a5f8c1e-9b3d-4f7a-8e6c-1d0b4a7f2e93";

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn oref(hive: &str, name: &str) -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: hive.into(),
        name: name.into(),
    })
}

fn base_tag(scope: pb::TagScope, uid: &str, version: i64) -> pb::Tag {
    pb::Tag {
        metadata: Some(pb::ResourceMetadata {
            hive: "ns1".into(),
            name: "tag1".into(),
            uid: uid.into(),
            version,
            ..Default::default()
        }),
        scope: scope as i32,
        source_actor: oref("ns1", "id1"),
        status: None,
    }
}

fn md(t: &mut pb::Tag) -> &mut pb::ResourceMetadata {
    t.metadata.as_mut().unwrap()
}

fn src(t: &mut pb::Tag) -> &mut pb::ObjectRef {
    t.source_actor.as_mut().unwrap()
}

fn valid_snapshot() -> pb::Snapshot {
    pb::Snapshot {
        uuid: "snap-1".into(),
        generation: 1,
        owner: pb::SnapshotOwner::Actor as i32,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        locality: "node-1".into(),
        durable_snapshot: Some(pb::SnapshotStorage {
            state: pb::SnapshotStorageState::Completed as i32,
            object: Some(pb::ObjectSnapshot {
                snapshot_uri: format!("gs://private/hives/as/actors/{ACTOR_UID}/snapshots/snap-1"),
            }),
        }),
        ..Default::default()
    }
}

#[test]
fn create_tag_request() {
    let t = |f: fn(&mut pb::Tag)| with(base_tag(pb::TagScope::Hive, "", 0), f);
    let c = |tag: Option<pb::Tag>| {
        validate_create_tag_request(&Operation::create(), &pb::CreateTagRequest { tag })
    };
    run(vec![
        ("valid", c(Some(t(|_| {}))), vec![]),
        (
            // Status is server-owned, but a client echoing back a tag it read
            // is not tripped up by one.
            "valid with status",
            c(Some(t(|t| {
                t.status = Some(pb::TagStatus {
                    snapshot: Some(valid_snapshot()),
                    actor_template_uid: ACTOR_UID.into(),
                })
            }))),
            vec![],
        ),
        (
            // The source Actor's hive is checked against the tag's, never
            // used as a default.
            "empty tag.metadata.hive",
            c(Some(t(|t| md(t).hive.clear()))),
            vec![required("tag.metadata.hive")],
        ),
        (
            "invalid tag.metadata.hive",
            c(Some(t(|t| md(t).hive = "NS1".into()))),
            vec![invalid("tag.metadata.hive"), dns_label("tag.metadata.hive")],
        ),
        (
            "missing tag.source_actor",
            c(Some(t(|t| t.source_actor = None))),
            vec![required("tag.source_actor")],
        ),
        (
            // With no source hive there is nothing to compare the tag's to.
            "missing tag.source_actor.hive",
            c(Some(t(|t| src(t).hive.clear()))),
            vec![required("tag.source_actor.hive")],
        ),
        (
            "invalid tag.source_actor.hive",
            c(Some(t(|t| src(t).hive = "NS1".into()))),
            vec![
                invalid("tag.metadata.hive"),
                dns_label("tag.source_actor.hive"),
            ],
        ),
        (
            "missing tag.source_actor.name",
            c(Some(t(|t| src(t).name.clear()))),
            vec![required("tag.source_actor.name")],
        ),
        (
            "invalid tag.source_actor.name",
            c(Some(t(|t| src(t).name = "ID1".into()))),
            vec![dns_label("tag.source_actor.name")],
        ),
        ("missing tag", c(None), vec![required("tag")]),
        (
            "missing tag.metadata",
            c(Some(t(|t| t.metadata = None))),
            vec![required("tag.metadata")],
        ),
        (
            "missing tag.metadata.name",
            c(Some(t(|t| md(t).name.clear()))),
            vec![required("tag.metadata.name")],
        ),
        (
            "invalid tag.metadata.name",
            c(Some(t(|t| md(t).name = "TAG1".into()))),
            vec![dns_label("tag.metadata.name")],
        ),
        (
            "tag.metadata.hive is not the source actor's",
            c(Some(t(|t| md(t).hive = "ns2".into()))),
            vec![invalid("tag.metadata.hive")],
        ),
        (
            "unset tag.scope",
            c(Some(t(|t| t.scope = 0))),
            vec![required("tag.scope")],
        ),
        (
            "tag.scope above the enum",
            c(Some(t(|t| t.scope = 7))),
            vec![invalid_by("tag.scope", "maximum")],
        ),
        (
            "negative tag.scope",
            c(Some(t(|t| t.scope = -1))),
            vec![invalid_by("tag.scope", "minimum")],
        ),
    ]);
}

#[test]
fn update_tag_request() {
    let t = |f: fn(&mut pb::Tag)| with(base_tag(pb::TagScope::Published, TAG_UID, 7), f);
    let c = |tag: Option<pb::Tag>| {
        validate_update_tag_request(&Operation::create(), &pb::UpdateTagRequest { tag })
    };
    run(vec![
        ("valid", c(Some(t(|_| {}))), vec![]),
        ("missing tag", c(None), vec![required("tag")]),
        (
            "missing tag.metadata",
            c(Some(t(|t| t.metadata = None))),
            vec![required("tag.metadata")],
        ),
        (
            "missing tag.metadata.hive",
            c(Some(t(|t| md(t).hive.clear()))),
            vec![required("tag.metadata.hive")],
        ),
        (
            "invalid tag.metadata.hive",
            c(Some(t(|t| md(t).hive = "NS1".into()))),
            vec![dns_label("tag.metadata.hive")],
        ),
        (
            "missing tag.metadata.name",
            c(Some(t(|t| md(t).name.clear()))),
            vec![required("tag.metadata.name")],
        ),
        (
            "invalid tag.metadata.name",
            c(Some(t(|t| md(t).name = "TAG1".into()))),
            vec![dns_label("tag.metadata.name")],
        ),
        (
            // Preconditions are the store's to insist on: a blind write is
            // rejected once it reaches the store.
            "missing tag.metadata.uid precondition",
            c(Some(t(|t| md(t).uid.clear()))),
            vec![],
        ),
        (
            "invalid tag.metadata.uid precondition",
            c(Some(t(|t| md(t).uid = "not-a-uuid".into()))),
            vec![invalid_by("tag.metadata.uid", "format=uuid")],
        ),
        (
            "missing tag.metadata.version precondition",
            c(Some(t(|t| md(t).version = 0))),
            vec![],
        ),
        (
            "negative tag.metadata.version precondition",
            c(Some(t(|t| md(t).version = -1))),
            vec![invalid_by("tag.metadata.version", "minimum")],
        ),
        (
            // The body is checked against the stored tag instead.
            "tag body is not checked against the request",
            c(Some(t(|t| {
                t.source_actor = None;
                t.scope = 0;
            }))),
            vec![],
        ),
    ]);
}

#[test]
fn tag_update_result() {
    let stored = || base_tag(pb::TagScope::Hive, TAG_UID, 7);
    let t = |f: fn(&mut pb::Tag)| with(stored(), f);
    let u =
        |n: pb::Tag| validate_tag_update(&Operation::update(), &Path::from("tag"), &n, &stored());
    run(vec![
        ("unchanged", u(stored()), vec![]),
        (
            "publishing is allowed",
            u(t(|t| t.scope = pb::TagScope::Published as i32)),
            vec![],
        ),
        (
            "unset tag.scope",
            u(t(|t| t.scope = 0)),
            vec![required("tag.scope")],
        ),
        (
            "tag.scope above the enum",
            u(t(|t| t.scope = 7)),
            vec![invalid_by("tag.scope", "maximum")],
        ),
        (
            "negative tag.scope",
            u(t(|t| t.scope = -1)),
            vec![invalid_by("tag.scope", "minimum")],
        ),
        (
            // A tag never moves between snapshots, so never between sources.
            "repointed tag.source_actor",
            u(t(|t| t.source_actor = oref("ns1", "id2"))),
            vec![invalid_by("tag.source_actor", "immutable")],
        ),
        (
            // An update replaces the whole object, so dropping the source
            // would clear it.
            "missing tag.source_actor",
            u(t(|t| t.source_actor = None)),
            vec![
                invalid_by("tag.source_actor", "immutable"),
                required("tag.source_actor"),
            ],
        ),
        (
            "renamed tag",
            u(t(|t| md(t).name = "tag2".into())),
            vec![invalid_by("tag.metadata.name", "immutable")],
        ),
        (
            "tag moved to another hive",
            u(t(|t| md(t).hive = "ns2".into())),
            vec![invalid_by("tag.metadata.hive", "immutable")],
        ),
        (
            "cleared tag.metadata.hive",
            u(t(|t| md(t).hive.clear())),
            vec![
                required("tag.metadata.hive"),
                invalid_by("tag.metadata.hive", "immutable"),
            ],
        ),
        (
            "missing tag.metadata",
            u(t(|t| t.metadata = None)),
            vec![required("tag.metadata")],
        ),
    ]);
}

#[test]
fn list_tags_request() {
    let c = |hive: &str, options: Option<pb::ListOptions>| {
        validate_list_tags_request(
            &Operation::create(),
            &pb::ListTagsRequest {
                hive: hive.into(),
                options,
            },
        )
    };
    let size = |page_size| {
        Some(pb::ListOptions {
            page_size,
            ..Default::default()
        })
    };
    let token = |n: usize| {
        Some(pb::ListOptions {
            page_token: "x".repeat(n),
            ..Default::default()
        })
    };
    run(vec![
        ("valid, hive scoped", c("ns1", None), vec![]),
        // An empty hive lists every hive.
        ("valid, empty hive means all hives", c("", None), vec![]),
        ("invalid hive", c("NS1", None), vec![dns_label("hive")]),
        ("valid, positive page_size", c("ns1", size(10)), vec![]),
        (
            "negative page_size",
            c("ns1", size(-1)),
            vec![invalid_by("options.page_size", "minimum")],
        ),
        ("valid page_token", c("ns1", token(256)), vec![]),
        (
            "too-large page_token",
            c("ns1", token(257)),
            vec![too_long("options.page_token")],
        ),
    ]);
}

/// The ref cases shared by Get and Delete.
fn ref_cases(f: impl Fn(Option<pb::ObjectRef>) -> ErrorList) {
    run(vec![
        ("valid", f(oref("ns1", "tag1")), vec![]),
        ("missing tag", f(None), vec![required("tag")]),
        (
            "missing tag.hive",
            f(oref("", "tag1")),
            vec![required("tag.hive")],
        ),
        (
            "invalid tag.hive",
            f(oref("NS1", "tag1")),
            vec![dns_label("tag.hive")],
        ),
        (
            "missing tag.name",
            f(oref("ns1", "")),
            vec![required("tag.name")],
        ),
        (
            "invalid tag.name",
            f(oref("ns1", "TAG1")),
            vec![dns_label("tag.name")],
        ),
    ]);
}

#[test]
fn get_tag_request() {
    ref_cases(|tag| validate_get_tag_request(&Operation::create(), &pb::GetTagRequest { tag }));
}

#[test]
fn delete_tag_request() {
    ref_cases(|tag| {
        validate_delete_tag_request(
            &Operation::create(),
            &pb::DeleteTagRequest {
                tag,
                ..Default::default()
            },
        )
    });
}
