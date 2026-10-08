//! Shared public-API validators: metadata, object refs, system-info volumes,
//! external volumes, delete options, snapshots, tags and golden snapshot
//! status.

mod common;

use std::collections::HashMap;

use common::*;
use hivekeep_core::field::{ErrorKind, ErrorList, Path};
use hivekeep_core::validation::api::common::{
    delete_options, object_ref, resource_metadata, snapshot,
};
use hivekeep_core::validation::api::workload::{
    system_info_volume_source, trust_bundle_data_source,
};
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::{HiveRule, Operation};
use hivekeep_proto::api::v1 as pb;

const ACTOR_UID: &str = "6b1f9d0c-4a2e-4d38-9c77-5e0a1b2c3d4e";

fn ts(seconds: i64, nanos: i32) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos })
}

fn root() -> Path {
    Path::root()
}

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn too_short(path: &'static str) -> Want {
    invalid_by(path, "minLength")
}

pub fn valid_metadata() -> pb::ResourceMetadata {
    pb::ResourceMetadata {
        hive: "as".into(),
        name: "nm".into(),
        uid: "01234567-89ab-cdef-0123-456789abcdef".into(),
        version: 93,
        create_time: ts(867, 0),
        update_time: ts(5309, 0),
        delete_time: None,
    }
}

fn md(f: impl FnOnce(&mut pb::ResourceMetadata)) -> pb::ResourceMetadata {
    with(valid_metadata(), f)
}

#[test]
fn resource_metadata_create() {
    let c = |m: pb::ResourceMetadata| resource_metadata(&Operation::create(), &root(), &m, None);
    run(vec![
        ("valid", c(valid_metadata()), vec![]),
        ("valid hive: empty", c(md(|m| m.hive.clear())), vec![]),
        (
            "missing name",
            c(md(|m| m.name.clear())),
            vec![required("name")],
        ),
        ("unspecified uid", c(md(|m| m.uid.clear())), vec![]),
        (
            "invalid uid: close but not valid",
            c(md(|m| m.uid = "aaaaaaaa-bbbbcccc-dddd-eeeeeeeeeeee".into())),
            vec![invalid_by("uid", "format=uuid")],
        ),
        (
            "invalid uid: not even close",
            c(md(|m| m.uid = "not a uid".into())),
            vec![invalid_by("uid", "format=uuid")],
        ),
        ("unspecified version", c(md(|m| m.version = 0)), vec![]),
        (
            "valid version: large",
            c(md(|m| m.version = i64::MAX)),
            vec![],
        ),
        (
            "invalid version: negative",
            c(md(|m| m.version = -1)),
            vec![invalid_by("version", "minimum")],
        ),
        (
            "unspecified create_time",
            c(md(|m| m.create_time = None)),
            vec![],
        ),
        (
            "unspecified update_time",
            c(md(|m| m.update_time = None)),
            vec![],
        ),
        (
            "invalid create_time: seconds out of range",
            c(md(|m| m.create_time = ts(253402300800, 0))),
            vec![invalid("create_time")],
        ),
        (
            "invalid update_time: negative nanos",
            c(md(|m| m.update_time = ts(5309, -1))),
            vec![invalid("update_time")],
        ),
        (
            "invalid update_time: precedes create_time",
            c(md(|m| m.update_time = ts(866, 0))),
            vec![invalid("update_time")],
        ),
        (
            "valid update_time: equals create_time",
            c(md(|m| m.update_time = ts(867, 0))),
            vec![],
        ),
        (
            "valid update_time: set without create_time",
            c(md(|m| m.create_time = None)),
            vec![],
        ),
    ]);
}

#[test]
fn resource_metadata_update() {
    let u = |old: pb::ResourceMetadata, new: pb::ResourceMetadata| {
        resource_metadata(&Operation::update(), &root(), &new, Some(&old))
    };
    run(vec![
        ("valid", u(valid_metadata(), valid_metadata()), vec![]),
        (
            "hive: empty -> non-empty",
            u(md(|m| m.hive.clear()), md(|m| m.hive = "present".into())),
            vec![invalid_by("hive", "immutable")],
        ),
        (
            "hive: non-empty -> empty",
            u(md(|m| m.hive = "present".into()), md(|m| m.hive.clear())),
            vec![invalid_by("hive", "immutable")],
        ),
        (
            "hive: changed",
            u(
                md(|m| m.hive = "value-1".into()),
                md(|m| m.hive = "value-2".into()),
            ),
            vec![invalid_by("hive", "immutable")],
        ),
        (
            "name: unset",
            u(valid_metadata(), md(|m| m.name.clear())),
            vec![required("name"), invalid_by("name", "immutable")],
        ),
        (
            "name: changed",
            u(
                md(|m| m.name = "value-1".into()),
                md(|m| m.name = "value-2".into()),
            ),
            vec![invalid_by("name", "immutable")],
        ),
        (
            "uid: unset",
            u(valid_metadata(), md(|m| m.uid.clear())),
            vec![invalid_by("uid", "immutable")],
        ),
        (
            "uid: changed to valid",
            u(
                md(|m| m.uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into()),
                md(|m| m.uid = "11111111-2222-3333-4444-555555555555".into()),
            ),
            vec![invalid_by("uid", "immutable")],
        ),
        (
            "uid: changed to invalid",
            u(
                md(|m| m.uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".into()),
                md(|m| m.uid = "not a uid".into()),
            ),
            vec![invalid_by("uid", "immutable")],
        ),
        (
            "version: unset",
            u(valid_metadata(), md(|m| m.version = 0)),
            vec![invalid_by("version", "update")],
        ),
        (
            "version: changed to valid",
            u(md(|m| m.version = 123), md(|m| m.version = 456)),
            vec![],
        ),
        (
            "version: changed non-monotonically",
            u(md(|m| m.version = 456), md(|m| m.version = 123)),
            vec![invalid_by("version", "monotonic")],
        ),
        (
            "version: changed to invalid",
            u(md(|m| m.version = 456), md(|m| m.version = -1)),
            vec![
                invalid_by("version", "monotonic"),
                invalid_by("version", "minimum"),
            ],
        ),
        (
            "create_time: unset",
            u(valid_metadata(), md(|m| m.create_time = None)),
            vec![invalid_by("create_time", "immutable")],
        ),
        (
            "create_time: changed",
            u(
                md(|m| m.create_time = ts(123, 0)),
                md(|m| m.create_time = ts(456, 0)),
            ),
            vec![invalid_by("create_time", "immutable")],
        ),
        (
            "update_time: unset",
            u(valid_metadata(), md(|m| m.update_time = None)),
            vec![invalid_by("update_time", "update")],
        ),
        (
            "update_time: changed to valid",
            u(
                md(|m| m.update_time = ts(1000, 0)),
                md(|m| m.update_time = ts(6000, 0)),
            ),
            vec![],
        ),
        (
            "update_time: changed to precede create_time",
            u(valid_metadata(), md(|m| m.update_time = ts(866, 0))),
            vec![invalid("update_time")],
        ),
        (
            "delete_time: set",
            u(valid_metadata(), md(|m| m.delete_time = ts(6000, 0))),
            vec![],
        ),
        (
            "delete_time: moved",
            u(
                md(|m| m.delete_time = ts(6000, 0)),
                md(|m| m.delete_time = ts(6001, 0)),
            ),
            vec![invalid_by("delete_time", "update")],
        ),
        (
            "delete_time: cleared",
            u(md(|m| m.delete_time = ts(6000, 0)), valid_metadata()),
            vec![invalid_by("delete_time", "update")],
        ),
    ]);
}

/// The DNS-label cases shared by the name and hive format tests:
/// `(case, value, valid)`.
fn dns_label_cases() -> Vec<(&'static str, String, bool)> {
    let mut v: Vec<(&'static str, String, bool)> = vec![
        ("alphabetic", "myname".into(), true),
        ("dashes", "my-na-me".into(), true),
        ("repeat dashes", "my---na---me".into(), true),
        ("alphanumeric", "my-123-name".into(), true),
        ("leading numeric", "123-name".into(), true),
        ("trailing numeric", "my-123".into(), true),
        ("fully numeric", "123".into(), true),
        ("long", "x".repeat(63), true),
        ("uppercase", "MYNAME".into(), false),
        ("leading dash", "-name".into(), false),
        ("trailing dash", "my-".into(), false),
        ("dots", "my.name".into(), false),
        ("underscores", "my_name".into(), false),
        ("too long", "x".repeat(64), false),
    ];
    for (case, c) in [
        ("bang", '!'),
        ("at", '@'),
        ("pound", '#'),
        ("dollar", '$'),
        ("percent", '%'),
        ("caret", '^'),
        ("ampersand", '&'),
        ("star", '*'),
    ] {
        v.push((case, format!("my{c}name"), false));
    }
    v
}

#[test]
fn resource_metadata_name_and_hive_format() {
    let c = |m: pb::ResourceMetadata| resource_metadata(&Operation::create(), &root(), &m, None);
    assert_errs("valid", c(valid_metadata()), &[]);
    for (case, value, ok) in dns_label_cases() {
        let want: Vec<Want> = if ok { vec![] } else { vec![dns_label("hive")] };
        assert_errs(
            &format!("hive: {case}"),
            c(md(|m| m.hive = value.clone())),
            &want,
        );
        let want: Vec<Want> = if ok { vec![] } else { vec![dns_label("name")] };
        assert_errs(&format!("name: {case}"), c(md(|m| m.name = value)), &want);
    }
}

#[test]
fn object_ref_format() {
    let c = |hive: &str, name: &str| {
        object_ref(
            &Operation::create(),
            &root(),
            &pb::ObjectRef {
                hive: hive.into(),
                name: name.into(),
            },
            HiveRule::Optional,
        )
    };
    assert_errs("valid", c("as", "nm"), &[]);
    assert_errs("valid hive: empty", c("", "nm"), &[]);
    assert_errs("missing name", c("as", ""), &[required("name")]);
    for (case, value, ok) in dns_label_cases() {
        let want: Vec<Want> = if ok { vec![] } else { vec![dns_label("hive")] };
        assert_errs(&format!("hive: {case}"), c(&value, "nm"), &want);
        let want: Vec<Want> = if ok { vec![] } else { vec![dns_label("name")] };
        assert_errs(&format!("name: {case}"), c("as", &value), &want);
    }
}

fn item(field: pb::ActorMetadataField, path: &str) -> pb::ActorMetadataItem {
    pb::ActorMetadataItem {
        field: field as i32,
        path: path.into(),
    }
}

fn trust_bundle(path: &str) -> pb::TrustBundleDataSource {
    pb::TrustBundleDataSource {
        names: vec!["egress-mitm.hivekeep.io".into()],
        path: path.into(),
    }
}

fn valid_system_info() -> pb::SystemInfoVolumeSource {
    pb::SystemInfoVolumeSource {
        data_sources: vec![
            pb::SystemInfoDataSource {
                actor_metadata: Some(pb::ActorMetadataDataSource {
                    items: vec![
                        item(pb::ActorMetadataField::Name, "actor-name"),
                        item(pb::ActorMetadataField::Uid, "actor-uid"),
                    ],
                }),
                trust_bundle: None,
            },
            pb::SystemInfoDataSource {
                actor_metadata: None,
                trust_bundle: Some(trust_bundle("trust-bundle.pem")),
            },
        ],
    }
}

fn items(s: &mut pb::SystemInfoVolumeSource) -> &mut Vec<pb::ActorMetadataItem> {
    &mut s.data_sources[0].actor_metadata.as_mut().unwrap().items
}

#[test]
fn system_info_volume_source_rules() {
    let c = |f: &dyn Fn(&mut pb::SystemInfoVolumeSource)| {
        let mut s = valid_system_info();
        f(&mut s);
        system_info_volume_source(&Operation::create(), &root(), &s, None)
    };
    let ip = "data_sources[0].actor_metadata.items";
    run(vec![
        ("valid", c(&|_| {}), vec![]),
        (
            "valid: no data sources",
            c(&|s| s.data_sources.clear()),
            vec![],
        ),
        (
            "too many data sources",
            c(&|s| {
                while s.data_sources.len() <= 8 {
                    s.data_sources.push(pb::SystemInfoDataSource {
                        actor_metadata: None,
                        trust_bundle: Some(trust_bundle("tb.pem")),
                    });
                }
            }),
            vec![too_many("data_sources")],
        ),
        (
            "no union member set",
            c(&|s| s.data_sources[0].actor_metadata = None),
            vec![invalid_by("data_sources[0]", "union")],
        ),
        (
            "both union members set",
            c(&|s| s.data_sources[0].trust_bundle = Some(trust_bundle("tb2.pem"))),
            vec![invalid_by("data_sources[0]", "union")],
        ),
        ("empty items", c(&|s| items(s).clear()), vec![required(ip)]),
        (
            "duplicate projected field",
            c(&|s| items(s)[1].field = pb::ActorMetadataField::Name as i32),
            vec![duplicate("data_sources[0].actor_metadata.items[1]")],
        ),
        (
            "unspecified item field",
            c(&|s| items(s)[0].field = 0),
            vec![required("data_sources[0].actor_metadata.items[0].field")],
        ),
        (
            "item field outside the enum",
            c(&|s| items(s)[0].field = 99),
            vec![invalid_by(
                "data_sources[0].actor_metadata.items[0].field",
                "maximum",
            )],
        ),
        (
            "negative item field",
            c(&|s| items(s)[0].field = -1),
            vec![invalid_by(
                "data_sources[0].actor_metadata.items[0].field",
                "minimum",
            )],
        ),
        (
            "empty item path",
            c(&|s| items(s)[0].path.clear()),
            vec![required("data_sources[0].actor_metadata.items[0].path")],
        ),
        (
            "item path too long",
            c(&|s| items(s)[0].path = "p".repeat(256)),
            vec![too_long("data_sources[0].actor_metadata.items[0].path")],
        ),
        (
            "item path traversal",
            c(&|s| items(s)[0].path = "../../traversal-escape".into()),
            vec![invalid("data_sources[0].actor_metadata.items[0].path")],
        ),
        (
            "absolute item path",
            c(&|s| items(s)[0].path = "/actor-name".into()),
            vec![invalid("data_sources[0].actor_metadata.items[0].path")],
        ),
        (
            "trust bundle path traversal",
            c(&|s| s.data_sources[1].trust_bundle.as_mut().unwrap().path = "../escape".into()),
            vec![invalid("data_sources[1].trust_bundle.path")],
        ),
        (
            "duplicate path within items",
            c(&|s| items(s)[1].path = "actor-name".into()),
            vec![duplicate("data_sources[0].actor_metadata.items[1].path")],
        ),
        (
            "duplicate path across data sources",
            c(&|s| s.data_sources[1].trust_bundle.as_mut().unwrap().path = "actor-uid".into()),
            vec![duplicate("data_sources[1].trust_bundle.path")],
        ),
        (
            "second actor_metadata entry",
            c(&|s| {
                s.data_sources.push(pb::SystemInfoDataSource {
                    actor_metadata: Some(pb::ActorMetadataDataSource {
                        items: vec![item(pb::ActorMetadataField::Name, "actor-name-2")],
                    }),
                    trust_bundle: None,
                })
            }),
            vec![forbidden("data_sources[2].actor_metadata")],
        ),
    ]);
}

#[test]
fn trust_bundle_data_source_rules() {
    let c = |f: &dyn Fn(&mut pb::TrustBundleDataSource)| {
        let mut tb = trust_bundle("trust-bundle.pem");
        f(&mut tb);
        trust_bundle_data_source(&Operation::create(), &root(), &tb, None)
    };
    run(vec![
        ("valid", c(&|_| {}), vec![]),
        (
            "valid: multiple names",
            c(&|tb| {
                tb.names = vec![
                    "egress-mitm.hivekeep.io".into(),
                    "system-roots.hivekeep.io".into(),
                ]
            }),
            vec![],
        ),
        (
            "no names",
            c(&|tb| tb.names.clear()),
            vec![required("names")],
        ),
        (
            "too many names",
            c(&|tb| tb.names = "abcdefghi".chars().map(String::from).collect()),
            vec![too_many("names")],
        ),
        (
            "empty name",
            c(&|tb| tb.names = vec![String::new()]),
            vec![too_short("names[0]")],
        ),
        (
            "name too long",
            c(&|tb| tb.names = vec!["n".repeat(254)]),
            vec![too_long("names[0]")],
        ),
        (
            "empty path",
            c(&|tb| tb.path.clear()),
            vec![required("path")],
        ),
        (
            "path too long",
            c(&|tb| tb.path = "p".repeat(256)),
            vec![too_long("path")],
        ),
        (
            "path traversal",
            c(&|tb| tb.path = "../escape".into()),
            vec![invalid("path")],
        ),
    ]);
}

fn valid_external_volume() -> pb::ExternalVolumeStatus {
    pb::ExternalVolumeStatus {
        name: "my-vol".into(),
        storage_volume_id: "valid-storage-id".into(),
        volume_type: "mock".into(),
        volume_context: HashMap::new(),
    }
}

fn ev(f: impl FnOnce(&mut pb::ExternalVolumeStatus)) -> pb::ExternalVolumeStatus {
    with(valid_external_volume(), f)
}

#[test]
fn external_volume() {
    let c = |v: pb::ExternalVolumeStatus| {
        external_volume_status(&Operation::create(), &root(), &v, None)
    };
    run(vec![
        ("valid external volume", c(valid_external_volume()), vec![]),
        (
            "missing volume name",
            c(ev(|v| v.name.clear())),
            vec![required("name")],
        ),
        (
            "invalid volume name",
            c(ev(|v| v.name = "NOT A VOLUME".into())),
            vec![dns_label("name")],
        ),
        (
            "valid external volume with empty storage volume id",
            c(ev(|v| v.storage_volume_id.clear())),
            vec![],
        ),
        (
            "invalid storage volume id with NUL",
            c(ev(|v| v.storage_volume_id = "vol\0id".into())),
            vec![invalid("storage_volume_id")],
        ),
        (
            "storage volume id too long",
            c(ev(|v| v.storage_volume_id = "x".repeat(257))),
            vec![too_long("storage_volume_id")],
        ),
        (
            "missing volume type",
            c(ev(|v| v.volume_type.clear())),
            vec![required("volume_type")],
        ),
        (
            "invalid volume type with uppercase",
            c(ev(|v| v.volume_type = "MockPlugin".into())),
            vec![invalid("volume_type")],
        ),
        (
            "valid volume type with 253 characters",
            c(ev(|v| v.volume_type = "a".repeat(253))),
            vec![],
        ),
        (
            "invalid volume type exceeding 253 characters",
            c(ev(|v| v.volume_type = "a".repeat(254))),
            vec![invalid("volume_type"), too_long("volume_type")],
        ),
        (
            "valid volume with domain-prefixed volume type",
            c(ev(|v| v.volume_type = "hivekeep.io/mock".into())),
            vec![],
        ),
        (
            "storage volume id at the bound",
            c(ev(|v| v.storage_volume_id = "x".repeat(256))),
            vec![],
        ),
        (
            "volume_context with many entries",
            c(ev(|v| {
                v.volume_context = (0..100)
                    .map(|i| (format!("key-{i:03}"), "v".to_string()))
                    .collect()
            })),
            vec![],
        ),
        (
            "volume_context with one long value",
            c(ev(|v| {
                v.volume_context = [("attachment".to_string(), "v".repeat(4000))].into()
            })),
            vec![],
        ),
        (
            "volume_context over the size bound",
            c(ev(|v| {
                v.volume_context = [("attachment".to_string(), "v".repeat(40 * 1024))].into()
            })),
            vec![(ErrorKind::TooLong, "volume_context", None)],
        ),
    ]);
}

#[test]
fn external_volume_update() {
    let u = |old: pb::ExternalVolumeStatus, new: pb::ExternalVolumeStatus| {
        external_volume_status(&Operation::update(), &root(), &new, Some(&old))
    };
    run(vec![
        (
            "unchanged volume is valid",
            u(valid_external_volume(), valid_external_volume()),
            vec![],
        ),
        (
            "name changed is invalid",
            u(
                ev(|v| v.name = "vol1".into()),
                ev(|v| v.name = "vol2".into()),
            ),
            vec![invalid_by("name", "update")],
        ),
        (
            "storage_volume_id transition from empty to non-empty is valid",
            u(
                ev(|v| v.storage_volume_id.clear()),
                ev(|v| v.storage_volume_id = "vol-id-1".into()),
            ),
            vec![],
        ),
        (
            "storage_volume_id changed once set is invalid",
            u(
                ev(|v| v.storage_volume_id = "vol-id-1".into()),
                ev(|v| v.storage_volume_id = "vol-id-2".into()),
            ),
            vec![invalid_by("storage_volume_id", "update")],
        ),
        (
            "storage_volume_id unset once set is invalid",
            u(
                ev(|v| v.storage_volume_id = "vol-id-1".into()),
                ev(|v| v.storage_volume_id.clear()),
            ),
            vec![invalid_by("storage_volume_id", "update")],
        ),
        (
            "volume_type changed is invalid",
            u(
                ev(|v| v.volume_type = "mock".into()),
                ev(|v| v.volume_type = "pd.csi.storage.gke.io".into()),
            ),
            vec![invalid_by("volume_type", "update")],
        ),
        (
            "volume_context changed is valid",
            u(
                valid_external_volume(),
                ev(|v| v.volume_context = [("foo".to_string(), "bar".to_string())].into()),
            ),
            vec![],
        ),
    ]);
}

#[test]
fn delete_options_rules() {
    let c = |o: pb::DeleteOptions| delete_options(&Operation::create(), &root(), Some(&o));
    let d = pb::DeleteOptions::default;
    run(vec![
        ("valid", c(d()), vec![]),
        ("valid version", c(with(d(), |o| o.version = 1)), vec![]),
        (
            "invalid version",
            c(with(d(), |o| o.version = -1)),
            vec![invalid_by("version", "minimum")],
        ),
        (
            "valid uid",
            c(with(d(), |o| {
                o.uid = "11111111-2222-3333-4444-555555555555".into()
            })),
            vec![],
        ),
        (
            "invalid uid",
            c(with(d(), |o| o.uid = "not a uid".into())),
            vec![invalid_by("uid", "format=uuid")],
        ),
    ]);
}

pub fn valid_snapshot_uri() -> String {
    format!("gs://private/hives/as/actors/{ACTOR_UID}/snapshots/snap-1")
}

pub fn valid_snapshot() -> pb::Snapshot {
    pb::Snapshot {
        uuid: "snap-1".into(),
        generation: 1,
        owner: pb::SnapshotOwner::Actor as i32,
        fidelity: pb::SnapshotFidelity::Memory as i32,
        locality: "node-1".into(),
        durable_snapshot: Some(pb::SnapshotStorage {
            state: pb::SnapshotStorageState::Completed as i32,
            object: Some(pb::ObjectSnapshot {
                snapshot_uri: valid_snapshot_uri(),
            }),
        }),
        ..Default::default()
    }
}

fn uri_of(s: &mut pb::Snapshot) -> &mut String {
    &mut s
        .durable_snapshot
        .as_mut()
        .unwrap()
        .object
        .as_mut()
        .unwrap()
        .snapshot_uri
}

/// A snapshot breaking two rules at once, so a holder can be shown to reach
/// every field of it.
pub fn bad_snapshot() -> pb::Snapshot {
    with(valid_snapshot(), |s| {
        s.fidelity = 4;
        uri_of(s).clear();
    })
}

fn snap(f: impl FnOnce(&mut pb::Snapshot)) -> pb::Snapshot {
    with(valid_snapshot(), f)
}

#[test]
fn snapshot_rules() {
    let c = |s: pb::Snapshot| snapshot(&Operation::create(), &root(), &s, None);
    let state = |s: &mut pb::Snapshot, v: i32| s.durable_snapshot.as_mut().unwrap().state = v;
    run(vec![
        ("valid", c(valid_snapshot()), vec![]),
        (
            "valid optional fields unset",
            c(snap(|s| s.locality.clear())),
            vec![],
        ),
        (
            "missing uuid",
            c(snap(|s| s.uuid.clear())),
            vec![required("uuid")],
        ),
        (
            "invalid uuid",
            c(snap(|s| s.uuid = "SNAP 1".into())),
            vec![dns_label("uuid")],
        ),
        (
            "invalid locality",
            c(snap(|s| s.locality = "NODE 1".into())),
            vec![invalid_by("locality", "format=dns-subdomain")],
        ),
        (
            "missing generation",
            c(snap(|s| s.generation = 0)),
            vec![required("generation")],
        ),
        (
            "negative generation",
            c(snap(|s| s.generation = -1)),
            vec![invalid_by("generation", "minimum")],
        ),
        (
            "missing owner",
            c(snap(|s| s.owner = 0)),
            vec![required("owner")],
        ),
        (
            "owner above the enum",
            c(snap(|s| s.owner = 3)),
            vec![invalid_by("owner", "maximum")],
        ),
        (
            "valid fidelity: volumes",
            c(snap(|s| s.fidelity = pb::SnapshotFidelity::Volumes as i32)),
            vec![],
        ),
        (
            "missing fidelity",
            c(snap(|s| s.fidelity = 0)),
            vec![required("fidelity")],
        ),
        (
            "valid without durable_snapshot",
            c(snap(|s| s.durable_snapshot = None)),
            vec![],
        ),
        (
            "missing durable_snapshot.state",
            c(snap(|s| state(s, 0))),
            vec![required("durable_snapshot.state")],
        ),
        (
            "negative durable_snapshot.state",
            c(snap(|s| state(s, -1))),
            vec![invalid_by("durable_snapshot.state", "minimum")],
        ),
        (
            "durable_snapshot.state above the enum",
            c(snap(|s| state(s, 3))),
            vec![invalid_by("durable_snapshot.state", "maximum")],
        ),
        (
            "missing durable_snapshot.object",
            c(snap(|s| s.durable_snapshot.as_mut().unwrap().object = None)),
            vec![required("durable_snapshot.object")],
        ),
        (
            "missing snapshot_uri",
            c(snap(|s| uri_of(s).clear())),
            vec![required("durable_snapshot.object.snapshot_uri")],
        ),
        (
            "snapshot_uri too long",
            c(snap(|s| *uri_of(s) = format!("gs://{}", "x".repeat(2044)))),
            vec![too_long("durable_snapshot.object.snapshot_uri")],
        ),
        (
            "fidelity above the enum",
            c(snap(|s| s.fidelity = 4)),
            vec![invalid_by("fidelity", "maximum")],
        ),
        (
            "negative fidelity",
            c(snap(|s| s.fidelity = -1)),
            vec![invalid_by("fidelity", "minimum")],
        ),
        (
            "every field invalid",
            c(bad_snapshot()),
            vec![
                required("durable_snapshot.object.snapshot_uri"),
                invalid_by("fidelity", "maximum"),
            ],
        ),
    ]);
}

#[test]
fn snapshot_update() {
    let u = |old: pb::Snapshot, new: pb::Snapshot| {
        snapshot(&Operation::update(), &root(), &new, Some(&old))
    };
    run(vec![
        ("unchanged", u(valid_snapshot(), valid_snapshot()), vec![]),
        (
            // A field is only revalidated when it changes, so a row stored
            // before a rule existed does not block updates to the rest.
            "unchanged invalid fields are not revalidated",
            u(bad_snapshot(), bad_snapshot()),
            vec![],
        ),
        (
            "fidelity changed to a valid value",
            u(
                valid_snapshot(),
                snap(|s| s.fidelity = pb::SnapshotFidelity::Volumes as i32),
            ),
            vec![],
        ),
        (
            "fidelity cleared",
            u(valid_snapshot(), snap(|s| s.fidelity = 0)),
            vec![required("fidelity")],
        ),
        (
            "fidelity changed to a value outside the enum",
            u(valid_snapshot(), snap(|s| s.fidelity = 4)),
            vec![invalid_by("fidelity", "maximum")],
        ),
        (
            "snapshot_uri cleared",
            u(valid_snapshot(), snap(|s| uri_of(s).clear())),
            vec![required("durable_snapshot.object.snapshot_uri")],
        ),
        (
            "fidelity repaired",
            u(
                bad_snapshot(),
                with(bad_snapshot(), |s| {
                    s.fidelity = pb::SnapshotFidelity::Memory as i32
                }),
            ),
            vec![],
        ),
        (
            "snapshot_uri changed to a value that is too long",
            u(
                valid_snapshot(),
                snap(|s| *uri_of(s) = format!("gs://{}", "x".repeat(2044))),
            ),
            vec![too_long("durable_snapshot.object.snapshot_uri")],
        ),
        (
            "snapshot_uri repaired",
            u(
                bad_snapshot(),
                with(bad_snapshot(), |s| *uri_of(s) = valid_snapshot_uri()),
            ),
            vec![],
        ),
        (
            "every field repaired",
            u(bad_snapshot(), valid_snapshot()),
            vec![],
        ),
    ]);
}

fn valid_actor() -> pb::Actor {
    pb::Actor {
        metadata: Some(valid_metadata()),
        actor_template: Some(pb::ObjectRef {
            hive: "as".into(),
            name: "tmpl".into(),
        }),
        status: Some(pb::ActorStatus {
            state: pb::ActorState::Suspended as i32,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn nested_snapshot() {
    // Every holder of a Snapshot descends into it and reports under its own
    // path.
    let old = valid_actor();
    let new = with(valid_actor(), |a| {
        a.status.as_mut().unwrap().snapshots = vec![bad_snapshot()]
    });
    assert_errs(
        "actor.status.snapshots",
        validate_actor_update(&Operation::update(), &root(), &new, &old, false),
        &[
            required("status.snapshots[0].durable_snapshot.object.snapshot_uri"),
            invalid_by("status.snapshots[0].fidelity", "maximum"),
        ],
    );
    let t = with(valid_tag(), |t| {
        t.status.as_mut().unwrap().snapshot = Some(bad_snapshot())
    });
    assert_errs(
        "tag.status.snapshot",
        tag(&Operation::create(), &root(), &t, None),
        &[
            required("status.snapshot.durable_snapshot.object.snapshot_uri"),
            invalid_by("status.snapshot.fidelity", "maximum"),
        ],
    );
}

pub fn valid_tag() -> pb::Tag {
    pb::Tag {
        metadata: Some(valid_metadata()),
        status: Some(pb::TagStatus {
            snapshot: Some(snap(|s| s.owner = pb::SnapshotOwner::Tag as i32)),
            actor_template_uid: ACTOR_UID.into(),
        }),
        scope: pb::TagScope::Hive as i32,
        source_actor: Some(pb::ObjectRef {
            hive: "as".into(),
            name: "nm".into(),
        }),
    }
}

fn tg(f: impl FnOnce(&mut pb::Tag)) -> pb::Tag {
    with(valid_tag(), f)
}

fn tag_status(t: &mut pb::Tag) -> &mut pb::TagStatus {
    t.status.as_mut().unwrap()
}

#[test]
fn tag_rules() {
    let c = |t: pb::Tag| tag(&Operation::create(), &root(), &t, None);
    run(vec![
        ("valid", c(valid_tag()), vec![]),
        (
            "valid scope: published",
            c(tg(|t| t.scope = pb::TagScope::Published as i32)),
            vec![],
        ),
        ("missing status", c(tg(|t| t.status = None)), vec![]),
        (
            "missing metadata",
            c(tg(|t| t.metadata = None)),
            vec![required("metadata")],
        ),
        (
            // A tag is addressed through the hive that owns it.
            "missing metadata.hive",
            c(tg(|t| t.metadata.as_mut().unwrap().hive.clear())),
            vec![required("metadata.hive")],
        ),
        (
            "invalid nested metadata.name",
            c(tg(|t| t.metadata.as_mut().unwrap().name.clear())),
            vec![required("metadata.name")],
        ),
        (
            "unspecified scope",
            c(tg(|t| t.scope = 0)),
            vec![required("scope")],
        ),
        (
            "scope above the enum",
            c(tg(|t| t.scope = 3)),
            vec![invalid_by("scope", "maximum")],
        ),
        (
            "negative scope",
            c(tg(|t| t.scope = -1)),
            vec![invalid_by("scope", "minimum")],
        ),
        (
            "missing source_actor",
            c(tg(|t| t.source_actor = None)),
            vec![required("source_actor")],
        ),
        (
            "missing source_actor.hive",
            c(tg(|t| t.source_actor.as_mut().unwrap().hive.clear())),
            vec![required("source_actor.hive")],
        ),
        (
            "missing status.actor_template_uid",
            c(tg(|t| tag_status(t).actor_template_uid.clear())),
            vec![required("status.actor_template_uid")],
        ),
        (
            "malformed status.actor_template_uid",
            c(tg(|t| {
                tag_status(t).actor_template_uid = "not-a-uuid".into()
            })),
            vec![invalid_by("status.actor_template_uid", "format=uuid")],
        ),
        (
            // A reserved tag has no snapshot until it is recorded.
            "missing status.snapshot",
            c(tg(|t| tag_status(t).snapshot = None)),
            vec![],
        ),
        (
            "missing status.snapshot.durable_snapshot",
            c(tg(|t| {
                tag_status(t).snapshot.as_mut().unwrap().durable_snapshot = None
            })),
            vec![required("status.snapshot.durable_snapshot")],
        ),
    ]);
}

#[test]
fn tag_update() {
    let u = |old: pb::Tag, new: pb::Tag| tag(&Operation::update(), &root(), &new, Some(&old));
    run(vec![
        ("unchanged", u(valid_tag(), valid_tag()), vec![]),
        (
            "metadata.hive changed",
            u(
                valid_tag(),
                tg(|t| t.metadata.as_mut().unwrap().hive = "other".into()),
            ),
            vec![invalid_by("metadata.hive", "immutable")],
        ),
        (
            "source_actor changed",
            u(
                valid_tag(),
                tg(|t| t.source_actor.as_mut().unwrap().name = "other".into()),
            ),
            vec![invalid_by("source_actor", "immutable")],
        ),
        (
            "scope changed",
            u(
                valid_tag(),
                tg(|t| t.scope = pb::TagScope::Published as i32),
            ),
            vec![],
        ),
    ]);
}

#[test]
fn tag_request_payloads() {
    let op = Operation::create();
    let r = |hive: &str, name: &str| {
        Some(pb::ObjectRef {
            hive: hive.into(),
            name: name.into(),
        })
    };
    let bad = tg(|t| tag_status(t).snapshot = Some(bad_snapshot()));
    run(vec![
        (
            "create: valid",
            validate_create_tag_request(
                &op,
                &pb::CreateTagRequest {
                    tag: Some(valid_tag()),
                },
            ),
            vec![],
        ),
        (
            "create: missing tag",
            validate_create_tag_request(&op, &pb::CreateTagRequest { tag: None }),
            vec![required("tag")],
        ),
        (
            "create: invalid nested snapshot",
            validate_create_tag_request(
                &op,
                &pb::CreateTagRequest {
                    tag: Some(bad.clone()),
                },
            ),
            vec![
                required("tag.status.snapshot.durable_snapshot.object.snapshot_uri"),
                invalid_by("tag.status.snapshot.fidelity", "maximum"),
            ],
        ),
        (
            "update: valid",
            validate_update_tag_request(
                &op,
                &pb::UpdateTagRequest {
                    tag: Some(valid_tag()),
                },
            ),
            vec![],
        ),
        (
            "update: missing tag",
            validate_update_tag_request(&op, &pb::UpdateTagRequest::default()),
            vec![required("tag")],
        ),
        (
            // The body is validated again as a whole object against the tag
            // it replaces, so the request check does not descend into it.
            "update: nested snapshot is not descended into",
            validate_update_tag_request(&op, &pb::UpdateTagRequest { tag: Some(bad) }),
            vec![],
        ),
        (
            "get: valid",
            validate_get_tag_request(&op, &pb::GetTagRequest { tag: r("as", "nm") }),
            vec![],
        ),
        (
            "get: missing tag",
            validate_get_tag_request(&op, &pb::GetTagRequest { tag: None }),
            vec![required("tag")],
        ),
        (
            "get: missing tag.hive",
            validate_get_tag_request(&op, &pb::GetTagRequest { tag: r("", "nm") }),
            vec![required("tag.hive")],
        ),
        (
            "get: invalid tag.name",
            validate_get_tag_request(&op, &pb::GetTagRequest { tag: r("as", "NM") }),
            vec![dns_label("tag.name")],
        ),
        (
            "delete: valid",
            validate_delete_tag_request(
                &op,
                &pb::DeleteTagRequest {
                    tag: r("as", "nm"),
                    ..Default::default()
                },
            ),
            vec![],
        ),
        (
            "delete: missing tag",
            validate_delete_tag_request(&op, &pb::DeleteTagRequest::default()),
            vec![required("tag")],
        ),
        (
            "delete: missing tag.hive",
            validate_delete_tag_request(
                &op,
                &pb::DeleteTagRequest {
                    tag: r("", "nm"),
                    ..Default::default()
                },
            ),
            vec![required("tag.hive")],
        ),
        (
            // Listing may leave the hive out to ask for every hive at once.
            "list: empty request",
            validate_list_tags_request(&op, &pb::ListTagsRequest::default()),
            vec![],
        ),
        (
            "list: invalid hive",
            validate_list_tags_request(
                &op,
                &pb::ListTagsRequest {
                    hive: "AS".into(),
                    ..Default::default()
                },
            ),
            vec![dns_label("hive")],
        ),
        (
            "list: negative page_size",
            validate_list_tags_request(
                &op,
                &pb::ListTagsRequest {
                    options: Some(pb::ListOptions {
                        page_size: -1,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
            vec![invalid_by("options.page_size", "minimum")],
        ),
        (
            "list: over-long page_token",
            validate_list_tags_request(
                &op,
                &pb::ListTagsRequest {
                    options: Some(pb::ListOptions {
                        page_token: "t".repeat(257),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ),
            vec![too_long("options.page_token")],
        ),
    ]);
}

fn valid_golden_status() -> pb::GoldenSnapshotStatus {
    pb::GoldenSnapshotStatus {
        golden_tag: Some(pb::ObjectRef {
            hive: "team-a".into(),
            name: "01234567-89ab-cdef-0123-456789abcdef".into(),
        }),
        snapshot_time: ts(867, 0),
        error_message: String::new(),
    }
}

/// A valid template carrying `status`; golden snapshot status is only
/// reachable through its template.
fn template_with_status(s: pb::GoldenSnapshotStatus) -> pb::ActorTemplate {
    pb::ActorTemplate {
        metadata: Some(valid_metadata()),
        containers: vec![pb::Container {
            name: "main".into(),
            image: PINNED.into(),
            ..Default::default()
        }],
        resources: Some(pb::ResourceRequirements {
            limits: Some(pb::Resources {
                cpu_millicores: 1000,
                memory_bytes: 1 << 30,
            }),
        }),
        snapshot_config: Some(pb::SnapshotConfig {
            preferred_fidelity: pb::SnapshotFidelity::Memory as i32,
            ..Default::default()
        }),
        status: Some(pb::ActorTemplateStatus {
            golden_snapshot_status: Some(s),
        }),
        ..Default::default()
    }
}

fn golden(old: Option<pb::GoldenSnapshotStatus>, new: pb::GoldenSnapshotStatus) -> ErrorList {
    let n = template_with_status(new);
    match old {
        None => actor_template(&Operation::create(), &root(), &n, None),
        Some(o) => {
            let o = template_with_status(o);
            actor_template(&Operation::update(), &root(), &n, Some(&o))
        }
    }
}

#[test]
fn golden_snapshot_status() {
    let g = |f: fn(&mut pb::GoldenSnapshotStatus)| with(valid_golden_status(), f);
    let msg = "status.golden_snapshot_status.error_message";
    run(vec![
        ("valid", golden(None, valid_golden_status()), vec![]),
        (
            "valid empty",
            golden(None, pb::GoldenSnapshotStatus::default()),
            vec![],
        ),
        (
            "valid failed",
            golden(
                None,
                pb::GoldenSnapshotStatus {
                    snapshot_time: ts(867, 0),
                    error_message: "GoldenActorCrashed: golden actor crashed".into(),
                    ..Default::default()
                },
            ),
            vec![],
        ),
        (
            "error_message at the bound",
            golden(None, g(|s| s.error_message = "x".repeat(4096))),
            vec![],
        ),
        (
            "error_message too long",
            golden(None, g(|s| s.error_message = "x".repeat(4097))),
            vec![too_long(msg)],
        ),
        (
            "golden_tag missing hive",
            golden(None, g(|s| s.golden_tag.as_mut().unwrap().hive.clear())),
            vec![required("status.golden_snapshot_status.golden_tag.hive")],
        ),
    ]);
}

#[test]
fn golden_snapshot_status_update() {
    let g = |f: fn(&mut pb::GoldenSnapshotStatus)| with(valid_golden_status(), f);
    run(vec![
        (
            "unchanged",
            golden(Some(valid_golden_status()), valid_golden_status()),
            vec![],
        ),
        (
            // The warmup clock restarts on every resume, so it stays mutable.
            "snapshot_time changed",
            golden(
                Some(valid_golden_status()),
                g(|s| s.snapshot_time = ts(5309, 0)),
            ),
            vec![],
        ),
        (
            "snapshot_time set",
            golden(
                Some(pb::GoldenSnapshotStatus::default()),
                valid_golden_status(),
            ),
            vec![],
        ),
        (
            "error_message set",
            golden(
                Some(valid_golden_status()),
                g(|s| s.error_message = "GoldenActorCrashed: crashed".into()),
            ),
            vec![],
        ),
        (
            "error_message set too long",
            golden(
                Some(valid_golden_status()),
                g(|s| s.error_message = "x".repeat(4097)),
            ),
            vec![too_long("status.golden_snapshot_status.error_message")],
        ),
    ]);
}
