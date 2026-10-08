//! Object-storage layout of snapshots.

use std::collections::HashSet;

use hivekeep_core::names::is_valid_resource_name;
use hivekeep_core::snapshot::{
    new_snapshot_name, validate_snapshot_file_names, SnapshotOwner, SnapshotUri, StoragePrefix,
};

#[test]
fn new_actor_snapshot_uri() {
    struct Case {
        name: &'static str,
        location: &'static str,
        hive: &'static str,
        uid: &'static str,
        snap: &'static str,
        want: Option<(&'static str, &'static str)>,
    }
    let ok = |name, location, want, owner| Case {
        name,
        location,
        hive: "team-a",
        uid: "actor-uid",
        snap: "snap-1",
        want: Some((want, owner)),
    };
    let bad = |name, location, hive, uid, snap| Case {
        name,
        location,
        hive,
        uid,
        snap,
        want: None,
    };
    for c in [
        ok(
            "no trailing slash",
            "gs://bucket/root",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1",
            "gs://bucket/root/hives/team-a/actors/actor-uid",
        ),
        ok(
            "trailing slash",
            "gs://bucket/root/",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1",
            "gs://bucket/root/hives/team-a/actors/actor-uid",
        ),
        ok(
            "bucket only",
            "gs://bucket",
            "gs://bucket/hives/team-a/actors/actor-uid/snapshots/snap-1",
            "gs://bucket/hives/team-a/actors/actor-uid",
        ),
        ok(
            "location containing a hives segment",
            "gs://my-bucket/hives/secret-agent",
            "gs://my-bucket/hives/secret-agent/hives/team-a/actors/actor-uid/snapshots/snap-1",
            "gs://my-bucket/hives/secret-agent/hives/team-a/actors/actor-uid",
        ),
        bad("empty location", "", "team-a", "actor-uid", "snap-1"),
        bad(
            "location without a bucket",
            "/root",
            "team-a",
            "actor-uid",
            "snap-1",
        ),
        bad(
            "location with a query",
            "gs://bucket/root?generation=1",
            "team-a",
            "actor-uid",
            "snap-1",
        ),
        bad(
            "invalid hive",
            "gs://bucket/root",
            "Team_A",
            "actor-uid",
            "snap-1",
        ),
        bad("empty hive", "gs://bucket/root", "", "actor-uid", "snap-1"),
        bad(
            "invalid actor UID",
            "gs://bucket/root",
            "team-a",
            "Actor_UID",
            "snap-1",
        ),
        bad(
            "empty actor UID",
            "gs://bucket/root",
            "team-a",
            "",
            "snap-1",
        ),
        bad(
            "invalid snapshot name",
            "gs://bucket/root",
            "team-a",
            "actor-uid",
            "2026-08-05T10:04:05Z",
        ),
        bad(
            "empty snapshot name",
            "gs://bucket/root",
            "team-a",
            "actor-uid",
            "",
        ),
    ] {
        let got = SnapshotUri::for_actor(c.location, c.hive, c.uid, c.snap);
        match (got, c.want) {
            (Ok(u), Some((want, owner))) => {
                assert_eq!(u.to_string(), want, "{}", c.name);
                assert_eq!(u.owner_prefix().to_string(), owner, "{}", c.name);
            }
            (Err(_), None) => {}
            (got, _) => panic!("{}: got {got:?}", c.name),
        }
    }
}

/// A Tag holds exactly one snapshot, so its owner prefix and its snapshot's
/// are the same.
#[test]
fn new_tag_snapshot_uri() {
    for (name, location, hive, uid, want) in [
        (
            "no trailing slash",
            "gs://bucket/root",
            "team-a",
            "tag-uid-1",
            Some("gs://bucket/root/hives/team-a/tags/tag-uid-1"),
        ),
        (
            "trailing slash",
            "gs://bucket/root/",
            "team-a",
            "tag-uid-1",
            Some("gs://bucket/root/hives/team-a/tags/tag-uid-1"),
        ),
        (
            "bucket only",
            "gs://bucket",
            "team-a",
            "tag-uid-1",
            Some("gs://bucket/hives/team-a/tags/tag-uid-1"),
        ),
        ("empty location", "", "team-a", "tag-uid-1", None),
        (
            "invalid hive",
            "gs://bucket/root",
            "Team_A",
            "tag-uid-1",
            None,
        ),
        (
            "invalid tag UID",
            "gs://bucket/root",
            "team-a",
            "2026-08-05T10:04:05Z",
            None,
        ),
        ("empty tag UID", "gs://bucket/root", "team-a", "", None),
    ] {
        match (SnapshotUri::for_tag(location, hive, uid), want) {
            (Ok(u), Some(want)) => {
                assert_eq!(u.to_string(), want, "{name}");
                assert_eq!(u.owner_prefix().to_string(), u.to_string(), "{name}");
            }
            (Err(_), None) => {}
            (got, _) => panic!("{name}: got {got:?}"),
        }
    }
}

/// An owner may only reach its own objects, so an Actor borrowing a Tag's
/// snapshot cannot collect it from under the Tag's other clones.
#[test]
fn snapshot_owned_by() {
    let actor =
        SnapshotUri::for_actor("gs://bucket/root", "team-a", "actor-uid", "snap-1").unwrap();
    let tag = SnapshotUri::for_tag("gs://bucket/root", "team-a", "snap-2").unwrap();
    for (name, uri, owner, want) in [
        (
            "an actor owns the snapshot it took",
            &actor,
            SnapshotOwner::actor("team-a", "actor-uid"),
            true,
        ),
        (
            "a borrowed tag snapshot is not the actor's",
            &tag,
            SnapshotOwner::actor("team-a", "actor-uid"),
            false,
        ),
        (
            "a tag owns its own snapshot",
            &tag,
            SnapshotOwner::tag("team-a", "snap-2"),
            true,
        ),
        (
            "another actor's snapshot is not this actor's",
            &actor,
            SnapshotOwner::actor("team-a", "other-actor-uid"),
            false,
        ),
        (
            "the same UID in another hive is another owner",
            &actor,
            SnapshotOwner::actor("team-b", "actor-uid"),
            false,
        ),
        (
            "the zero URI is owned by nobody",
            &SnapshotUri::default(),
            SnapshotOwner::actor("team-a", "actor-uid"),
            false,
        ),
    ] {
        assert_eq!(uri.owned_by(&owner), want, "{name}");
    }
}

#[test]
fn snapshot_owner_prefix() {
    for (name, owner, location, want) in [
        (
            "actor",
            SnapshotOwner::actor("team-a", "actor-uid"),
            "gs://bucket/root",
            Some("gs://bucket/root/hives/team-a/actors/actor-uid"),
        ),
        (
            "tag",
            SnapshotOwner::tag("team-a", "snap-1"),
            "gs://bucket/root",
            Some("gs://bucket/root/hives/team-a/tags/snap-1"),
        ),
        (
            "the empty owner has no prefix",
            SnapshotOwner::actor("", ""),
            "gs://bucket/root",
            None,
        ),
        (
            "invalid location",
            SnapshotOwner::actor("team-a", "actor-uid"),
            "/root",
            None,
        ),
    ] {
        match (owner.prefix(location), want) {
            (Ok(p), Some(want)) => assert_eq!(p.to_string(), want, "{name}"),
            (Err(_), None) => {}
            (got, _) => panic!("{name}: got {got:?}"),
        }
    }
}

/// Every call yields a fresh, valid name: a recreated Tag cannot compute its
/// way onto the objects its predecessor left.
#[test]
fn new_snapshot_name_is_fresh() {
    let mut seen = HashSet::new();
    for _ in 0..100 {
        let n = new_snapshot_name();
        assert!(is_valid_resource_name(&n), "{n}");
        assert!(seen.insert(n.clone()), "{n} returned twice");
    }
}

#[test]
fn parse_snapshot_uri() {
    struct Want {
        location: &'static str,
        hive: &'static str,
        name: &'static str,
        owner: SnapshotOwner,
    }
    let actor = |location, name| Want {
        location,
        hive: "team-a",
        name,
        owner: SnapshotOwner::actor("team-a", "actor-uid"),
    };
    let cases: Vec<(&str, &str, Option<Want>)> = vec![
        (
            "reads an actor snapshot",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/f47ac10b-58cc-4372-a567-0e02b2c3d479",
            Some(actor("gs://bucket/root", "f47ac10b-58cc-4372-a567-0e02b2c3d479")),
        ),
        (
            "reads a tag snapshot",
            "gs://bucket/root/hives/team-a/tags/snap-1",
            Some(Want {
                location: "gs://bucket/root",
                hive: "team-a",
                name: "snap-1",
                owner: SnapshotOwner::tag("team-a", "snap-1"),
            }),
        ),
        (
            "tolerates a trailing slash",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/",
            Some(actor("gs://bucket/root", "snap-1")),
        ),
        (
            "bucket-only location",
            "gs://bucket/hives/team-a/actors/actor-uid/snapshots/snap-1",
            Some(actor("gs://bucket", "snap-1")),
        ),
        (
            "location containing a hives segment",
            "gs://my-bucket/hives/secret-agent/hives/team-a/actors/actor-uid/snapshots/snap-1",
            Some(actor("gs://my-bucket/hives/secret-agent", "snap-1")),
        ),
        ("rejects an actor prefix with no snapshot", "gs://bucket/root/hives/team-a/actors/actor-uid", None),
        (
            "rejects an object within an actor snapshot",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/manifest.json",
            None,
        ),
        ("rejects an object within a tag snapshot", "gs://bucket/root/hives/team-a/tags/snap-1/manifest.json", None),
        ("rejects the flat layout", "gs://bucket/root/snapshots/team-a/snap-1", None),
        ("rejects an unknown owner kind", "gs://bucket/root/hives/team-a/workers/worker-uid", None),
        ("rejects an actor snapshot with no hives segment", "gs://bucket/root/team-a/actors/actor-uid/snapshots/snap-1", None),
        ("rejects a bare name", "snap-1", None),
        ("rejects an empty URI", "", None),
        ("rejects a missing snapshot name", "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/", None),
        ("rejects a missing bucket", "/root/hives/team-a/actors/actor-uid/snapshots/snap-1", None),
        (
            "rejects a name that is not a resource name",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/2026-08-05T10:04:05Z-ABCDEFG",
            None,
        ),
        ("rejects a hive that is not a resource name", "gs://bucket/root/hives/Team_A/actors/actor-uid/snapshots/snap-1", None),
        (
            "rejects an actor UID that is not a resource name",
            "gs://bucket/root/hives/team-a/actors/Actor_UID/snapshots/snap-1",
            None,
        ),
        (
            "rejects a URI with a trailing query",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1?generation=1",
            None,
        ),
        (
            "rejects a URI with a trailing fragment",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1#frag",
            None,
        ),
        (
            "rejects malformed URI syntax",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1%zz",
            None,
        ),
    ];
    for (name, uri, want) in cases {
        match (SnapshotUri::parse(uri), want) {
            (Ok(got), Some(w)) => {
                assert_eq!(got.location(), w.location, "{name}");
                assert_eq!(got.hive(), w.hive, "{name}");
                assert_eq!(got.name(), w.name, "{name}");
                assert_eq!(got.owner(), Some(&w.owner), "{name}");
                // A URI is recorded and read back, so parsing reproduces it.
                let s = got.to_string();
                assert!(s == uri || format!("{s}/") == uri, "{name}: {s}");
            }
            (Err(_), None) => {}
            (got, _) => panic!("{name}: got {got:?}"),
        }
    }
}

#[test]
fn snapshot_uri_object() {
    let actor =
        SnapshotUri::for_actor("gs://bucket/root", "team-a", "actor-uid", "snap-1").unwrap();
    let tag = SnapshotUri::for_tag("gs://bucket/root", "team-a", "snap-2").unwrap();
    for (name, uri, object, want) in [
        (
            "manifest",
            &actor,
            "manifest.json",
            Some("gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/manifest.json"),
        ),
        (
            "image file",
            &actor,
            "memory.img.zstd",
            Some("gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/memory.img.zstd"),
        ),
        (
            "tag snapshot object",
            &tag,
            "manifest.json",
            Some("gs://bucket/root/hives/team-a/tags/snap-2/manifest.json"),
        ),
        ("malformed escape", &actor, "100%done.img", None),
    ] {
        match (uri.object_uri(object), want) {
            (Ok(got), Some(want)) => assert_eq!(got, want, "{name}"),
            (Err(_), None) => {}
            (got, _) => panic!("{name}: got {got:?}"),
        }
    }
}

#[test]
fn snapshot_uri_zero_value() {
    let zero = SnapshotUri::default();
    assert!(zero.is_zero());
    assert_eq!(zero.to_string(), "");
    assert!(zero.prefix().is_zero());
    assert!(zero.owner_prefix().is_zero());
    let uri = SnapshotUri::for_actor("gs://bucket/root", "team-a", "actor-uid", "snap-1").unwrap();
    assert!(!uri.is_zero());
}

#[test]
fn parse_storage_prefix() {
    for (uri, want_err) in [
        ("gs://bucket/root/hives/team-a/actors/uid1", false),
        (
            "s3://bucket/root/hives/team-a/actors/uid1/snapshots/snap1",
            false,
        ),
        ("gs://bucket", true),
        ("gs://bucket/", true),
        ("gs:///root", true),
        ("gs://bucket/root?x=1", true),
        ("gs://user@bucket/root", true),
    ] {
        match StoragePrefix::parse(uri) {
            Ok(p) => {
                assert!(!want_err, "{uri} accepted");
                assert_eq!(p.to_string(), uri);
            }
            Err(e) => assert!(want_err, "{uri}: {e}"),
        }
    }
}

#[test]
fn snapshot_file_names() {
    for (name, files, want_err) in [
        ("empty list", vec![], false),
        (
            "plain names",
            vec!["manifest.json", "memory.img", "durable-dir.tar"],
            false,
        ),
        (
            "fs subdir names",
            vec![
                "checkpoint.img",
                "fs/fscheckpoint.pb",
                "fs/multitar.img",
                "fs/pages.img",
                "fs/pages_meta.img",
            ],
            false,
        ),
        (
            "rootfs subdir names",
            vec![
                "checkpoint.img",
                "rootfs/fscheckpoint.pb",
                "rootfs/multitar.img",
                "rootfs/pages.img",
                "rootfs/pages_meta.img",
            ],
            false,
        ),
        ("empty name", vec![""], true),
        ("dot", vec!["."], true),
        ("dot dot", vec![".."], true),
        ("absolute", vec!["/etc/passwd"], true),
        ("nested arbitrary subdir", vec!["a/b"], true),
        ("deeply nested under fs", vec!["fs/a/b"], true),
        ("deeply nested under rootfs", vec!["rootfs/a/b"], true),
        ("escaping", vec!["../x"], true),
        ("duplicate", vec!["a", "b", "a"], true),
    ] {
        assert_eq!(
            validate_snapshot_file_names(&files).is_err(),
            want_err,
            "{name}: {files:?}"
        );
    }
}
