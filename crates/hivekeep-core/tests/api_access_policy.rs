//! AccessPolicy request and update validation, for the global policy and
//! per-Hive policies.

mod common;

use common::*;
use hivekeep_core::access::{MAX_MEMBERS_PER_POLICY, ROLE_EDITOR, ROLE_OWNER, ROLE_VIEWER};
use hivekeep_core::field::{ErrorKind, ErrorList, Path};
use hivekeep_core::resource::SINGLETON_NAME;
use hivekeep_core::validation::api::*;
use hivekeep_core::validation::Operation;
use hivekeep_proto::api::v1 as pb;

const B: &str = "access_policy.bindings";

fn run(cases: Vec<(&str, ErrorList, Vec<Want>)>) {
    for (name, got, want) in cases {
        assert_errs(name, got, &want);
    }
}

fn binding(role: &str, members: Vec<String>) -> pb::Binding {
    pb::Binding {
        role: role.into(),
        members,
    }
}

fn valid_policy() -> pb::AccessPolicy {
    pb::AccessPolicy {
        metadata: Some(pb::ResourceMetadata {
            name: "default".into(),
            ..Default::default()
        }),
        bindings: vec![
            binding(ROLE_OWNER, vec!["user:alice@example.com".into()]),
            binding(ROLE_VIEWER, vec!["user:bob@example.com".into()]),
        ],
    }
}

fn pol(f: impl FnOnce(&mut pb::AccessPolicy)) -> Option<pb::AccessPolicy> {
    Some(with(valid_policy(), f))
}

/// `n` distinct valid members.
fn members(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("user:u{i}")).collect()
}

fn role(i: usize) -> String {
    format!("{B}[{i}].role")
}

fn member(i: usize) -> String {
    format!("{B}[{i}].members[0]")
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn unsupported_role(i: usize) -> Want {
    not_supported(leak(role(i)))
}

fn duplicate_role(i: usize) -> Want {
    duplicate(leak(role(i)))
}

fn bad_member(i: usize) -> Want {
    invalid(leak(member(i)))
}

fn hive_ref() -> Option<pb::ObjectRef> {
    Some(pb::ObjectRef {
        hive: String::new(),
        name: "team-a".into(),
    })
}

#[test]
fn create_global_access_policy_request() {
    let c = |access_policy: Option<pb::AccessPolicy>| {
        validate_create_global_access_policy_request(
            &Operation::create(),
            &pb::CreateGlobalAccessPolicyRequest { access_policy },
        )
    };
    run(vec![
        ("valid", c(Some(valid_policy())), vec![]),
        (
            "valid without bindings",
            c(pol(|p| p.bindings.clear())),
            vec![],
        ),
        (
            "missing access_policy",
            c(None),
            vec![required("access_policy")],
        ),
        (
            "missing metadata",
            c(pol(|p| p.metadata = None)),
            vec![required("access_policy.metadata")],
        ),
        (
            "hive set",
            c(pol(|p| p.metadata.as_mut().unwrap().hive = "team-a".into())),
            vec![forbidden("access_policy.metadata.hive")],
        ),
        (
            "missing name",
            c(pol(|p| p.metadata.as_mut().unwrap().name.clear())),
            vec![required("access_policy.metadata.name")],
        ),
        (
            "name not default",
            c(pol(|p| p.metadata.as_mut().unwrap().name = "other".into())),
            vec![invalid_by("access_policy.metadata.name", "custom=default")],
        ),
        (
            // editor is a Hive-only role.
            "editor role",
            c(pol(|p| p.bindings[0].role = ROLE_EDITOR.into())),
            vec![unsupported_role(0)],
        ),
        (
            "unknown role",
            c(pol(|p| p.bindings[1].role = "admin".into())),
            vec![unsupported_role(1)],
        ),
        (
            // System component roles come only from server configuration.
            "system component role",
            c(pol(|p| p.bindings[1].role = "ingress-router".into())),
            vec![unsupported_role(1)],
        ),
        (
            "duplicate role",
            c(pol(|p| p.bindings[1].role = ROLE_OWNER.into())),
            vec![duplicate_role(1)],
        ),
        (
            "member without user prefix",
            c(pol(|p| {
                p.bindings[0].members = vec!["alice@example.com".into()]
            })),
            vec![bad_member(0)],
        ),
        (
            "member with a control character",
            c(pol(|p| {
                p.bindings[0].members = vec!["user:alice\tbob".into()]
            })),
            vec![bad_member(0)],
        ),
        (
            "wildcard member",
            c(pol(|p| p.bindings[1].members = vec!["user:*".into()])),
            vec![bad_member(1)],
        ),
        (
            "duplicate member",
            c(pol(|p| {
                p.bindings[0].members = vec!["user:a".into(), "user:a".into()]
            })),
            vec![duplicate("access_policy.bindings[0].members[1]")],
        ),
        (
            "missing role",
            c(pol(|p| p.bindings[0].role.clear())),
            vec![required("access_policy.bindings[0].role")],
        ),
        (
            "missing members",
            c(pol(|p| p.bindings[0].members.clear())),
            vec![required("access_policy.bindings[0].members")],
        ),
        (
            "members at the policy limit",
            c(pol(|p| {
                p.bindings = vec![binding(ROLE_VIEWER, members(MAX_MEMBERS_PER_POLICY))]
            })),
            vec![],
        ),
        (
            // Each binding is within its own limit; together they exceed the
            // policy's.
            "members over the policy limit across bindings",
            c(pol(|p| {
                p.bindings = vec![
                    binding(ROLE_OWNER, members(1)),
                    binding(ROLE_VIEWER, members(MAX_MEMBERS_PER_POLICY)),
                ]
            })),
            vec![(ErrorKind::TooMany, B, None)],
        ),
    ]);
}

/// The singleton-name cases shared by every request naming a policy.
fn name_cases(f: impl Fn(&str) -> ErrorList) {
    run(vec![
        ("valid", f(SINGLETON_NAME), vec![]),
        ("missing name", f(""), vec![required("name")]),
        (
            "wrong name",
            f("other"),
            vec![invalid_by("name", "custom=default")],
        ),
    ]);
}

#[test]
fn get_global_access_policy_request() {
    name_cases(|name| {
        validate_get_global_access_policy_request(
            &Operation::create(),
            &pb::GetGlobalAccessPolicyRequest { name: name.into() },
        )
    });
}

#[test]
fn update_global_access_policy_request() {
    let c = |access_policy: Option<pb::AccessPolicy>| {
        validate_update_global_access_policy_request(
            &Operation::create(),
            &pb::UpdateGlobalAccessPolicyRequest { access_policy },
        )
    };
    run(vec![
        ("valid", c(Some(valid_policy())), vec![]),
        (
            "missing access_policy",
            c(None),
            vec![required("access_policy")],
        ),
        (
            "editor role",
            c(pol(|p| p.bindings[0].role = ROLE_EDITOR.into())),
            vec![unsupported_role(0)],
        ),
    ]);
}

#[test]
fn global_access_policy_update() {
    let u = |n: Option<pb::AccessPolicy>| {
        validate_global_access_policy_update(
            &Operation::update(),
            &Path::from("access_policy"),
            &n.unwrap(),
            &valid_policy(),
        )
    };
    run(vec![
        ("valid", u(Some(valid_policy())), vec![]),
        (
            "editor role",
            u(pol(|p| p.bindings[0].role = ROLE_EDITOR.into())),
            vec![unsupported_role(0)],
        ),
        (
            "invalid member",
            u(pol(|p| p.bindings[1].members = vec!["user:".into()])),
            vec![bad_member(1)],
        ),
    ]);
}

#[test]
fn create_hive_access_policy_request() {
    let c = |hive: Option<pb::ObjectRef>, access_policy: Option<pb::AccessPolicy>| {
        validate_create_hive_access_policy_request(
            &Operation::create(),
            &pb::CreateHiveAccessPolicyRequest {
                hive,
                access_policy,
            },
        )
    };
    run(vec![
        ("valid", c(hive_ref(), Some(valid_policy())), vec![]),
        (
            "valid with editor role",
            c(hive_ref(), pol(|p| p.bindings[1].role = ROLE_EDITOR.into())),
            vec![],
        ),
        (
            "missing hive",
            c(None, Some(valid_policy())),
            vec![required("hive")],
        ),
        (
            "missing access_policy",
            c(hive_ref(), None),
            vec![required("access_policy")],
        ),
        (
            "missing name",
            c(
                hive_ref(),
                pol(|p| p.metadata.as_mut().unwrap().name.clear()),
            ),
            vec![required("access_policy.metadata.name")],
        ),
        (
            "name not default",
            c(
                hive_ref(),
                pol(|p| p.metadata.as_mut().unwrap().name = "other".into()),
            ),
            vec![invalid_by("access_policy.metadata.name", "custom=default")],
        ),
        (
            "unknown role",
            c(hive_ref(), pol(|p| p.bindings[0].role = "admin".into())),
            vec![unsupported_role(0)],
        ),
        (
            "system component role",
            c(
                hive_ref(),
                pol(|p| p.bindings[0].role = "egress-gateway".into()),
            ),
            vec![unsupported_role(0)],
        ),
        (
            "invalid member",
            c(
                hive_ref(),
                pol(|p| p.bindings[0].members = vec!["group:eng".into()]),
            ),
            vec![bad_member(0)],
        ),
    ]);
}

#[test]
fn get_hive_access_policy_request() {
    let c = |hive: Option<pb::ObjectRef>, name: &str| {
        validate_get_hive_access_policy_request(
            &Operation::create(),
            &pb::GetHiveAccessPolicyRequest {
                hive,
                name: name.into(),
            },
        )
    };
    assert_errs("missing hive", c(None, SINGLETON_NAME), &[required("hive")]);
    name_cases(|name| c(hive_ref(), name));
}

#[test]
fn update_hive_access_policy_request() {
    let c = |access_policy: Option<pb::AccessPolicy>| {
        validate_update_hive_access_policy_request(
            &Operation::create(),
            &pb::UpdateHiveAccessPolicyRequest {
                hive: hive_ref(),
                access_policy,
            },
        )
    };
    run(vec![
        ("valid", c(Some(valid_policy())), vec![]),
        (
            "missing access_policy",
            c(None),
            vec![required("access_policy")],
        ),
        (
            "duplicate role",
            c(pol(|p| p.bindings[1].role = ROLE_OWNER.into())),
            vec![duplicate_role(1)],
        ),
    ]);
}

#[test]
fn hive_access_policy_update() {
    let u = |n: Option<pb::AccessPolicy>| {
        validate_hive_access_policy_update(
            &Operation::update(),
            &Path::from("access_policy"),
            &n.unwrap(),
            &valid_policy(),
        )
    };
    run(vec![
        (
            "valid with editor role",
            u(pol(|p| p.bindings[1].role = ROLE_EDITOR.into())),
            vec![],
        ),
        (
            "unknown role",
            u(pol(|p| p.bindings[0].role = "admin".into())),
            vec![unsupported_role(0)],
        ),
    ]);
}

#[test]
fn delete_hive_access_policy_request() {
    let c = |hive: Option<pb::ObjectRef>, name: &str| {
        validate_delete_hive_access_policy_request(
            &Operation::create(),
            &pb::DeleteHiveAccessPolicyRequest {
                hive,
                name: name.into(),
                options: None,
            },
        )
    };
    assert_errs("missing hive", c(None, SINGLETON_NAME), &[required("hive")]);
    name_cases(|name| c(hive_ref(), name));
}
