//! Resource references, SPIFFE identities, lifecycle classification and the
//! shared field checks.

use std::any::TypeId;
use std::collections::HashMap;

use hivekeep_core::actor_state::ActorState;
use hivekeep_core::field::{ErrorKind, ErrorList, FieldError, Path};
use hivekeep_core::names::{check_uuid, is_valid_resource_name};
use hivekeep_core::resource::{ActorRef, ActorTemplateRef, TagRef};
use hivekeep_core::spiffe::{
    actor_ref_from_spiffe_id, actor_ref_from_worker_for_actor_spiffe_id, actor_spiffe_id,
    worker_for_actor_spiffe_id,
};
use hivekeep_core::validation::checks;
use hivekeep_core::validation::rules::list;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

// ---------------------------------------------------------------- lifecycle

/// Every wire state must be classified, since the mint's host check and the
/// credential exchange refuse states outside the transfer set.
#[test]
fn transfer_states_classify_every_wire_state() {
    let want: HashMap<i32, bool> = [
        (pb::ActorState::Unspecified, false),
        (pb::ActorState::Resuming, true),
        (pb::ActorState::Running, true),
        (pb::ActorState::Suspending, true),
        (pb::ActorState::Suspended, true),
        (pb::ActorState::Crashed, false),
        (pb::ActorState::Reverting, false),
        (pb::ActorState::Deleting, false),
    ]
    .into_iter()
    .map(|(s, t)| (s as i32, t))
    .collect();
    let mut seen = 0;
    for value in 0..64 {
        let Ok(state) = pb::ActorState::try_from(value) else {
            continue;
        };
        seen += 1;
        let Some(&transfers) = want.get(&value) else {
            panic!("{state:?} is not classified: decide whether a node transfers snapshots in it");
        };
        let got =
            ActorState::from_proto(value).is_some_and(|s| ActorState::TRANSFER_STATES.contains(&s));
        assert_eq!(got, transfers, "{state:?}");
    }
    assert_eq!(seen, want.len());
}

// ---------------------------------------------------------------- refs

#[test]
fn ref_aliases_are_distinct_types() {
    let ids = [
        TypeId::of::<ActorRef>(),
        TypeId::of::<ActorTemplateRef>(),
        TypeId::of::<TagRef>(),
    ];
    for i in 0..ids.len() {
        for j in i + 1..ids.len() {
            assert_ne!(ids[i], ids[j], "the phantom kind marker was lost");
        }
    }
}

/// The kind comes from the type alone, so an empty ref still names it.
#[test]
fn resource_ref_log_value() {
    assert_eq!(
        format!("{:?}", ActorRef::new("", "")),
        r#"Actor { hive: "", name: "" }"#
    );
    assert_eq!(
        format!("{:?}", ActorTemplateRef::new("", "")),
        r#"ActorTemplate { hive: "", name: "" }"#
    );
    assert_eq!(
        format!("{:?}", ActorRef::new("team-a", "act-1")),
        r#"Actor { hive: "team-a", name: "act-1" }"#
    );
}

fn meta(hive: &str, name: &str) -> Option<pb::ResourceMetadata> {
    Some(pb::ResourceMetadata {
        hive: hive.into(),
        name: name.into(),
        ..Default::default()
    })
}

#[test]
fn actor_ref_string_round_trip_and_of() {
    let r = ActorRef::new("team-a", "act-1");
    assert_eq!(r.to_string(), "team-a/act-1");
    let obj = r.to_object_ref();
    assert_eq!((obj.hive.as_str(), obj.name.as_str()), ("team-a", "act-1"));
    assert_eq!(ActorRef::from_object_ref(&obj), r);
    let populated = pb::Actor {
        metadata: meta("team-a", "act-1"),
        ..Default::default()
    };
    assert_eq!(ActorRef::of(&populated), r);
    assert_eq!(ActorRef::of(&pb::Actor::default()), ActorRef::new("", ""));
}

#[test]
fn actor_template_ref_string_round_trip_and_of() {
    let r = ActorTemplateRef::new("team-a", "tmpl-1");
    assert_eq!(r.to_string(), "team-a/tmpl-1");
    let obj = r.to_object_ref();
    assert_eq!((obj.hive.as_str(), obj.name.as_str()), ("team-a", "tmpl-1"));
    assert_eq!(ActorTemplateRef::from_object_ref(&obj), r);
    let populated = pb::ActorTemplate {
        metadata: meta("team-a", "tmpl-1"),
        ..Default::default()
    };
    assert_eq!(ActorTemplateRef::of(&populated), r);
    assert_eq!(
        ActorTemplateRef::of(&pb::ActorTemplate::default()),
        ActorTemplateRef::new("", "")
    );
}

#[test]
fn tag_ref_string_round_trip_and_of() {
    let r = TagRef::new("team-a", "tag-1");
    assert_eq!(r.to_string(), "team-a/tag-1");
    let obj = r.to_object_ref();
    assert_eq!((obj.hive.as_str(), obj.name.as_str()), ("team-a", "tag-1"));
    assert_eq!(TagRef::from_object_ref(&obj), r);
    let populated = pb::Tag {
        metadata: meta("team-a", "tag-1"),
        ..Default::default()
    };
    assert_eq!(TagRef::of(&populated), r);
    assert_eq!(TagRef::of(&pb::Tag::default()), TagRef::new("", ""));
}

// ---------------------------------------------------------------- spiffe

#[test]
fn worker_for_actor_spiffe_id_rejects() {
    for id in [
        "",
        "spiffe://hivekeep-actor.local",
        "spiffe://hivekeep-actor.local/hive/team/actor",
        "spiffe://hivekeep-actor.local/hive/team/actor/agent/extra",
        "spiffe://hivekeep-actor.local/namespace/team/actor/agent",
        "spiffe://hivekeep-actor.local/hive/team/pod/agent",
        "spiffe://hivekeep-actor.local/hive//actor/agent",
        "spiffe://hivekeep-actor.local/hive/Team/actor/agent",
        "spiffe://hivekeep-actor.local/hive/team/actor/agent?x=1",
        "spiffe://hivekeep-actor.local/hive/team/actor/agent#f",
        "spiffe://cluster.local/hive/team/actor/agent",
        "spiffe://user@hivekeep-actor.local/hive/team/actor/agent",
        "https://hivekeep-actor.local/hive/team/actor/agent",
        "hivekeep-actor.local/hive/team/actor/agent",
        "spiffe://hivekeep-actor.local/hive/team/actor/agent/",
        "spiffe://hivekeep-actor.local/hive/te%2Fam/actor/agent",
    ] {
        assert!(
            actor_ref_from_worker_for_actor_spiffe_id(id).is_err(),
            "{id:?} was accepted"
        );
    }
}

const UUID_A: &str = "fd6cab8c-17c8-4c9e-8893-28e28aff724b";
const UUID_B: &str = "045841a7-5dcb-47eb-a76f-6d8460bfe009";

#[test]
fn actor_spiffe_id_round_trip() {
    for (uri, want) in [
        (
            "spiffe://hivekeep-actor.local/actor/foo/bar".to_string(),
            ActorRef::new("foo", "bar"),
        ),
        (
            format!("spiffe://hivekeep-actor.local/actor/{UUID_A}/{UUID_B}"),
            ActorRef::new(UUID_A, UUID_B),
        ),
    ] {
        let got = actor_ref_from_spiffe_id(&uri).unwrap();
        assert_eq!(got, want, "{uri}");
        assert_eq!(actor_spiffe_id(&got), uri);
    }
}

#[test]
fn worker_for_actor_spiffe_id_round_trip() {
    for (uri, want) in [
        (
            "spiffe://hivekeep-actor.local/worker-for-actor/foo/bar".to_string(),
            ActorRef::new("foo", "bar"),
        ),
        (
            format!("spiffe://hivekeep-actor.local/worker-for-actor/{UUID_A}/{UUID_B}"),
            ActorRef::new(UUID_A, UUID_B),
        ),
    ] {
        let got = actor_ref_from_worker_for_actor_spiffe_id(&uri).unwrap();
        assert_eq!(got, want, "{uri}");
        assert_eq!(worker_for_actor_spiffe_id(&got), uri);
    }
}

// ---------------------------------------------------------------- errors

fn required_actor_name() -> ErrorList {
    FieldError::required(&Path::from("actor_name"), "").into()
}

#[test]
fn to_grpc_status_error() {
    let status = required_actor_name()
        .into_result()
        .unwrap_err()
        .to_invalid_argument();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains("actor_name"),
        "{}",
        status.message()
    );
}

#[test]
fn to_api_error() {
    let err = required_actor_name().into_result().unwrap_err();
    let status: tonic::Status = err.clone().into();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(err.to_string().contains("actor_name"), "{err}");
}

#[test]
fn to_api_error_empty() {
    assert!(ErrorList::new().into_result().is_ok());
    assert!(ErrorList::from(Vec::new()).into_result().is_ok());
}

/// Messages compare by field value.
#[test]
fn messages_compare_by_value() {
    let a = pb::ObjectRef {
        hive: "a".into(),
        name: "x".into(),
    };
    let b = a.clone();
    let _ = format!("{a:?}");
    assert_eq!(a, b);
    assert_ne!(
        a,
        pb::ObjectRef {
            hive: "a".into(),
            name: "y".into(),
        }
    );
    assert_eq!(None::<pb::ObjectRef>, None);
    assert_eq!(vec!["a"], vec!["a"]);
    assert_ne!(1, 2);
}

/// A duplicate-entry error must not echo a secret carried by the entry, in
/// any rendering of the error.
#[test]
fn validation_errors_do_not_echo_entry_values() {
    const SECRET: &str = "sk-do-not-log";
    let env = vec![
        pb::EnvVar {
            name: "API_KEY".into(),
            value: SECRET.into(),
        },
        pb::EnvVar {
            name: "API_KEY".into(),
            value: SECRET.into(),
        },
    ];
    let op = Operation::create();
    let errs = list(&op, &Path::from("env"), &env, None)
        .unique_by(|e| e.name.clone())
        .finish();
    assert!(!errs.is_empty(), "no duplicate found");
    let err = errs.clone().into_result().unwrap_err();
    for (name, msg) in [
        ("display", err.to_string()),
        (
            "invalid argument",
            err.to_invalid_argument().message().to_string(),
        ),
        ("internal", err.to_internal().message().to_string()),
    ] {
        assert!(
            !msg.contains(SECRET),
            "{name}: {msg:?} contains the env value"
        );
        assert!(
            msg.contains("API_KEY"),
            "{name}: {msg:?} does not name the duplicate"
        );
    }
    assert_eq!(ErrorList::new().message(), "validation failed");
    assert_eq!(env[0].value, SECRET);
}

// ---------------------------------------------------------------- names

#[test]
fn is_valid_resource_name_cases() {
    for (name, value, valid) in [
        ("valid lowercase", "my-actor-1", true),
        ("valid single char", "a", true),
        ("missing name", "", false),
        ("invalid uppercase", "My-Actor", false),
        ("invalid start hyphen", "-actor", false),
        ("valid start number", "1actor", true),
        ("invalid end hyphen", "actor-", false),
        ("invalid special chars", "actor@1", false),
        ("invalid length", &"a".repeat(64), false),
    ] {
        assert_eq!(is_valid_resource_name(value), valid, "{name}");
    }
}

#[test]
fn worker_pod_uid_cases() {
    let p = Path::from("worker_pod_uid");
    for (name, uid, want_err) in [
        ("uuid valid", "422938ba-8860-4983-a25d-d6bcb0a69d4e", false),
        ("separator", "a/b", true),
        ("traversal", "..", true),
        ("empty", "", true),
        ("uppercase", "Pod-UID", true),
        ("too long", &"a".repeat(64), true),
    ] {
        assert_eq!(
            !checks::worker_pod_uid(&p, uid).is_empty(),
            want_err,
            "{name}"
        );
    }
}

#[test]
fn snapshot_location_cases() {
    let p = Path::from("location");
    for (name, loc, want_err) in [
        (
            "valid with trailing slash",
            "gs://bucket/actors/1234/snapshots/5678/",
            false,
        ),
        ("valid without path", "gs://bucket", false),
        ("valid alternate scheme", "s3://bucket/path", false),
        ("empty", "", true),
        ("missing bucket", "gs://", true),
        ("no scheme or bucket", "bucket/path", true),
        ("unparseable", "://bucket", true),
        ("query", "gs://bucket/path?x=1", true),
        ("fragment", "gs://bucket/path#frag", true),
        ("userinfo", "gs://user@bucket/path", true),
        ("opaque", "gs:bucket/path", true),
        ("dot dot", "gs://bucket/team-a/../team-b", true),
        ("trailing dot dot", "gs://bucket/path/..", true),
        ("escaped dot dot", "gs://bucket/%2e%2e/path", true),
        ("dot", "gs://bucket/./path", true),
        ("triple dot", "gs://bucket/.../path", true),
        ("space padded dot dot", "gs://bucket/a/%20..%20/b", true),
        ("trailing space dot dot", "gs://bucket/a/..%20/b", true),
        ("backslash", r"gs://bucket/team-a\..\team-b", true),
        ("escaped backslash", "gs://bucket/a%5Cb", true),
        ("dots in a name", "gs://bucket/a..b/.c/d.", false),
    ] {
        assert_eq!(
            !checks::snapshot_location(&p, loc).is_empty(),
            want_err,
            "{name}: {loc:?}"
        );
    }
}

#[test]
fn ip_cases() {
    let p = Path::from("ip");
    for (name, ip, want_msg) in [
        ("valid ipv4", "192.168.1.1", ""),
        ("valid ipv6", "2001:db8::1", ""),
        ("invalid format", "not-an-ip", "must be a valid IP address"),
        (
            "ipv4-mapped ipv6",
            "::ffff:192.168.1.1",
            "must not be an IPv4-mapped IPv6 address",
        ),
        (
            "non-canonical ipv6",
            "2001:db8:0:0:0:0:0:1",
            "must be in canonical form",
        ),
    ] {
        let errs = checks::ip(&p, ip);
        if want_msg.is_empty() {
            assert!(errs.is_empty(), "{name}: {}", errs.message());
        } else {
            let first = errs
                .iter()
                .next()
                .unwrap_or_else(|| panic!("{name}: no error"));
            assert!(first.to_string().contains(want_msg), "{name}: {first}");
        }
    }
}

#[test]
fn uuid_cases() {
    for (name, value, valid) in [
        ("valid", "123e4567-e89b-12d3-a456-426614174000", true),
        ("too short", "123e4567", false),
        ("too long", "123e4567-e89b-12d3-a456-4266141740001", false),
        ("missing dashes", "123e4567e89b12d3a456426614174000", false),
        (
            "uppercase hex",
            "123E4567-E89B-12D3-A456-426614174000",
            false,
        ),
        (
            "invalid characters",
            "123e4567-e89b-12d3-a456-42661417400g",
            false,
        ),
    ] {
        let msgs = check_uuid(value);
        if valid {
            assert!(msgs.is_empty(), "{name}: {msgs:?}");
        } else {
            assert!(
                msgs.first()
                    .is_some_and(|m| m.contains("must be a lowercase UUID")),
                "{name}: {msgs:?}"
            );
        }
    }
}

#[test]
fn sandbox_class_cases() {
    let p = Path::from("sandbox_class");
    for (name, class, want_err) in [
        ("gvisor", "gvisor", false),
        ("microvm", "microvm", false),
        ("empty left to tags", "", false),
        ("unknown class", "kata", true),
        ("case-sensitive", "GVISOR", true),
    ] {
        let errs = checks::sandbox_class(&p, class);
        if want_err {
            let kinds: Vec<_> = errs.iter().map(|e| (e.kind, e.path.as_str())).collect();
            assert_eq!(
                kinds,
                vec![(ErrorKind::NotSupported, "sandbox_class")],
                "{name}"
            );
        } else {
            assert!(errs.is_empty(), "{name}");
        }
    }
}

// ---------------------------------------------------------------- string rules

fn run_string_rule(rule: fn(&Path, &str) -> ErrorList, cases: &[(&str, &str, bool)]) {
    for (name, value, want_err) in cases {
        let errs = rule(&Path::from("f"), value);
        assert_eq!(
            !errs.is_empty(),
            *want_err,
            "{name}: value {value:?}: {}",
            errs.message()
        );
    }
}

const PINNED: &str =
    "example.com/app@sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[test]
fn pinned_image_cases() {
    run_string_rule(
        checks::pinned_image,
        &[
            ("empty is left to tags", "", false),
            ("pinned", PINNED, false),
            ("tag only", "example.com/app:v1", true),
            ("malformed digest", "example.com/app@sha256:abc", true),
            ("malformed reference", "Example.com/App", true),
        ],
    );
}

#[test]
fn mount_path_cases() {
    run_string_rule(
        checks::mount_path,
        &[
            ("empty is left to tags", "", false),
            ("clean absolute", "/data/cache", false),
            ("relative", "data", true),
            ("root", "/", true),
            ("trailing slash", "/data/", true),
            ("double slash", "/data//x", true),
            ("colon", "/data:x", true),
            ("dot segment", "/data/./x", true),
            ("dot-dot segment", "/data/../x", true),
            ("control character", "/data\x01", true),
        ],
    );
}

#[test]
fn http_get_path_cases() {
    run_string_rule(
        checks::http_get_path,
        &[
            ("empty is left to tags", "", false),
            ("simple", "/healthz", false),
            ("percent escape", "/a%20b", false),
            ("no leading slash", "healthz", true),
            ("query", "/healthz?x=1", true),
            ("fragment", "/healthz#x", true),
            ("bad percent escape", "/a%2", true),
        ],
    );
}

#[test]
fn env_var_name_cases() {
    run_string_rule(
        checks::env_var_name,
        &[
            ("empty is left to tags", "", false),
            ("conventional", "HOME_DIR", false),
            ("printable punctuation", "a.b-c", false),
            ("equals sign", "A=B", true),
            ("control character", "A\tB", true),
            ("non-ASCII", "é", true),
        ],
    );
}

#[test]
fn projected_path_cases() {
    run_string_rule(
        checks::projected_path,
        &[
            ("empty is left to tags", "", false),
            ("relative file", "identity/name", false),
            ("absolute", "/etc/name", true),
            ("escape", "../name", true),
        ],
    );
}

#[test]
fn storage_volume_id_cases() {
    run_string_rule(
        checks::storage_volume_id,
        &[
            ("empty", "", false),
            ("plain", "projects/p/disks/d-1", false),
            ("newline is allowed", "a\nb", false),
            ("NUL", "a\x00b", true),
            ("DEL", "a\x7fb", true),
            ("C1 control", "a\u{0085}b", true),
        ],
    );
}

#[test]
fn volume_type_cases() {
    run_string_rule(
        checks::volume_type,
        &[
            ("empty", "", false),
            ("dns subdomain", "pd.csi.storage.gke.io", false),
            ("hivekeep prefix", "hivekeep.io/gcs", false),
            ("uppercase", "PD", true),
            ("other prefix", "example.io/gcs", true),
        ],
    );
}

#[test]
fn host_port_cases() {
    run_string_rule(
        checks::host_port,
        &[
            ("empty is left to presence tags", "", false),
            ("dns name", "gate-egress.hk-system.svc:443", false),
            (
                "fully qualified dns name",
                "gate-egress.hk-system.svc.cluster.local.:443",
                false,
            ),
            ("ipv4", "10.0.0.1:8080", false),
            ("ipv6", "[fd00::1]:443", false),
            ("missing port", "gate-egress.hk-system.svc", true),
            ("empty host", ":443", true),
            ("bare dot host", ".:443", true),
            ("uppercase host", "Egress.Example:443", true),
            ("host with a path", "egress/../x:443", true),
            ("port zero", "egress.example:0", true),
            ("port too large", "egress.example:65536", true),
            ("named port", "egress.example:https", true),
            ("unbracketed ipv6", "fd00::1:443", true),
        ],
    );
}

#[test]
fn capabilities_cases() {
    let s = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    for (name, caps, allow_all, want) in [
        (
            "valid names",
            s(&["NET_BIND_SERVICE", "SYS_PTRACE"]),
            false,
            0,
        ),
        ("ALL rejected for add", s(&["ALL"]), false, 1),
        ("ALL accepted for drop", s(&["ALL"]), true, 0),
        ("CAP_ prefix", s(&["CAP_NET_ADMIN"]), false, 1),
        ("lowercase", s(&["net_admin"]), false, 1),
        ("too long", vec!["A".repeat(64)], false, 1),
        (
            "each bad entry reported",
            s(&["CAP_X", "ok", "NET_ADMIN"]),
            false,
            2,
        ),
    ] {
        let errs = checks::capabilities(&Path::from("add"), &caps, allow_all);
        assert_eq!(errs.len(), want, "{name}: {}", errs.message());
    }
}

fn kinds(errs: &ErrorList) -> Vec<(ErrorKind, String)> {
    errs.iter()
        .map(|e| (e.kind, e.path.as_str().to_string()))
        .collect()
}

#[test]
fn nested_mount_paths_cases() {
    let p = Path::from("volume_mounts");
    let nested = vec![(
        ErrorKind::Invalid,
        "volume_mounts[1].mount_path".to_string(),
    )];
    for (name, paths, want) in [
        ("siblings", vec!["/a", "/b", "/ab"], vec![]),
        (
            "identical paths are left to the list key",
            vec!["/a", "/a"],
            vec![],
        ),
        ("empty paths are skipped", vec!["", "/a"], vec![]),
        (
            "nested under an earlier mount",
            vec!["/a", "/a/b"],
            nested.clone(),
        ),
        (
            "nested over an earlier mount",
            vec!["/a/b", "/a"],
            nested.clone(),
        ),
    ] {
        assert_eq!(
            kinds(&checks::nested_mount_paths(&p, &paths)),
            want,
            "{name}"
        );
    }
}

/// Each finding lands on the field the caller maps its index to.
#[test]
fn nested_mount_paths_at_maps_indices() {
    let fields = [
        Path::from("external_volume_mounts")
            .index(0)
            .child("mount_path"),
        Path::from("image_volume_mounts")
            .index(0)
            .child("mount_path"),
        Path::from("image_volume_mounts")
            .index(1)
            .child("mount_path"),
    ];
    let got = checks::nested_mount_paths_at(&["/a", "/b", "/a/c"], |i| fields[i].clone());
    assert_eq!(
        kinds(&got),
        vec![(ErrorKind::Invalid, fields[2].as_str().to_string())]
    );
}

#[test]
fn csi_map_cases() {
    let p = Path::from("publish_context");
    let m = |pairs: Vec<(&str, String)>| -> HashMap<String, String> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    };
    let many: HashMap<String, String> = (0..100)
        .map(|i| (format!("key-{i:03}"), "v".into()))
        .collect();
    let over = vec![(ErrorKind::TooLong, "publish_context".to_string())];
    for (name, map, want) in [
        ("nil", HashMap::new(), vec![]),
        (
            "one long value",
            m(vec![("token", "x".repeat(4000))]),
            vec![],
        ),
        ("many small entries", many, vec![]),
        (
            "over the CSI spec limit, within the headroom",
            m(vec![("k", "x".repeat(8 * 1024))]),
            vec![],
        ),
        (
            "exactly the bound",
            m(vec![("k", "x".repeat(40 * 1024 - 1))]),
            vec![],
        ),
        (
            "one byte over",
            m(vec![("k", "x".repeat(40 * 1024))]),
            over.clone(),
        ),
        (
            "over across entries",
            m(vec![
                ("a", "x".repeat(20 * 1024)),
                ("b", "y".repeat(20 * 1024)),
            ]),
            over.clone(),
        ),
    ] {
        assert_eq!(kinds(&checks::csi_map(&p, &map)), want, "{name}");
    }
}
