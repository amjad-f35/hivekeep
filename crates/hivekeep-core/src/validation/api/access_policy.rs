//! AccessPolicy validation.

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use crate::access::{parse_member, GLOBAL_ROLES, HIVE_ROLES, MAX_MEMBERS_PER_POLICY};
use crate::field::{ErrorList, FieldError, Path};
use crate::validation::checks;
use crate::validation::rules::{self, list, message, string, Format, HiveRule, Operation};

fn binding(op: &Operation, p: &Path, n: &pb::Binding, o: Option<&pb::Binding>) -> ErrorList {
    rules::all([
        string(op, &p.child("role"), &n.role, o.map(|o| o.role.as_str()))
            .required()
            .max_len(64)
            .finish(),
        list(
            op,
            &p.child("members"),
            &n.members,
            o.map(|o| o.members.as_slice()),
        )
        .max_items(MAX_MEMBERS_PER_POLICY)
        .required()
        .each(|p, s, _| string(op, p, s, None).max_len(512).finish())
        .unique_values()
        .finish(),
    ])
}

/// An AccessPolicy resource: the unscoped singleton named "default".
pub fn access_policy(
    op: &Operation,
    p: &Path,
    n: &pb::AccessPolicy,
    o: Option<&pb::AccessPolicy>,
) -> ErrorList {
    rules::all([
        message(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
        )
        .required()
        .then(|p, m, om| {
            let mut errs = ErrorList::new();
            if !m.hive.is_empty() {
                errs.push(FieldError::forbidden(&p.child("hive"), "must not be set"));
            }
            errs.extend(checks::singleton_name(&p.child("name"), &m.name));
            errs.extend(resource_metadata(op, p, m, om));
            errs
        })
        .finish(),
        list(
            op,
            &p.child("bindings"),
            &n.bindings,
            o.map(|o| o.bindings.as_slice()),
        )
        .max_items(100)
        .optional()
        .each(|p, n, o| binding(op, p, n, o))
        .finish(),
    ])
}

/// Each binding names an allowed role at most once, every member is a valid
/// `user:<id>`, and the policy stays within [`MAX_MEMBERS_PER_POLICY`].
pub fn validate_bindings(
    p: &Path,
    policy: Option<&pb::AccessPolicy>,
    allowed_roles: &[&str],
) -> ErrorList {
    let Some(policy) = policy else {
        return ErrorList::new();
    };
    let mut errs = ErrorList::new();
    let mut seen = std::collections::HashSet::new();
    let mut total = 0;
    for (i, b) in policy.bindings.iter().enumerate() {
        let bp = p.index(i);
        if !b.role.is_empty() {
            if !allowed_roles.contains(&b.role.as_str()) {
                errs.push(FieldError::not_supported(
                    &bp.child("role"),
                    &b.role,
                    allowed_roles,
                ));
            } else if !seen.insert(b.role.as_str()) {
                errs.push(FieldError::duplicate(&bp.child("role"), &b.role));
            }
        }
        total += b.members.len();
        for (j, m) in b.members.iter().enumerate() {
            if let Err(e) = parse_member(m) {
                errs.push(FieldError::invalid(
                    &bp.child("members").index(j),
                    m,
                    e.to_string(),
                ));
            }
        }
    }
    if total > MAX_MEMBERS_PER_POLICY {
        errs.push(FieldError::too_many(p, total, MAX_MEMBERS_PER_POLICY));
    }
    errs
}

fn bindings_path() -> Path {
    Path::new(["access_policy", "bindings"])
}

fn policy_field(op: &Operation, n: Option<&pb::AccessPolicy>, roles: &[&str]) -> ErrorList {
    rules::all([
        message(op, &Path::from("access_policy"), n, None)
            .required()
            .then(|p, n, _| access_policy(op, p, n, None))
            .finish(),
        validate_bindings(&bindings_path(), n, roles),
    ])
}

fn policy_name(op: &Operation, name: &str) -> ErrorList {
    string(op, &Path::from("name"), name, None)
        .required()
        .format(Format::DnsLabel)
        .custom(checks::singleton_name)
        .finish()
}

fn hive_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("hive"), r, None, HiveRule::Forbidden)
}

/// `GetGlobalAccessPolicy`.
pub fn validate_get_global_access_policy_request(
    op: &Operation,
    req: &pb::GetGlobalAccessPolicyRequest,
) -> ErrorList {
    policy_name(&op.as_create(), &req.name)
}

/// `CreateGlobalAccessPolicy`.
pub fn validate_create_global_access_policy_request(
    op: &Operation,
    req: &pb::CreateGlobalAccessPolicyRequest,
) -> ErrorList {
    policy_field(&op.as_create(), req.access_policy.as_ref(), &GLOBAL_ROLES)
}

/// `UpdateGlobalAccessPolicy`. The result is checked against the stored
/// policy with [`validate_global_access_policy_update`].
pub fn validate_update_global_access_policy_request(
    op: &Operation,
    req: &pb::UpdateGlobalAccessPolicyRequest,
) -> ErrorList {
    policy_field(&op.as_create(), req.access_policy.as_ref(), &GLOBAL_ROLES)
}

/// Validates an updated global policy against the stored one.
pub fn validate_global_access_policy_update(
    op: &Operation,
    p: &Path,
    new: &pb::AccessPolicy,
    old: &pb::AccessPolicy,
) -> ErrorList {
    rules::all([
        access_policy(&op.as_update(), p, new, Some(old)),
        validate_bindings(&p.child("bindings"), Some(new), &GLOBAL_ROLES),
    ])
}

/// `GetHiveAccessPolicy`.
pub fn validate_get_hive_access_policy_request(
    op: &Operation,
    req: &pb::GetHiveAccessPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_ref(&op, req.hive.as_ref()),
        policy_name(&op, &req.name),
    ])
}

/// `CreateHiveAccessPolicy`.
pub fn validate_create_hive_access_policy_request(
    op: &Operation,
    req: &pb::CreateHiveAccessPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_ref(&op, req.hive.as_ref()),
        policy_field(&op, req.access_policy.as_ref(), &HIVE_ROLES),
    ])
}

/// `UpdateHiveAccessPolicy`. The result is checked against the stored
/// policy with [`validate_hive_access_policy_update`].
pub fn validate_update_hive_access_policy_request(
    op: &Operation,
    req: &pb::UpdateHiveAccessPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_ref(&op, req.hive.as_ref()),
        policy_field(&op, req.access_policy.as_ref(), &HIVE_ROLES),
    ])
}

/// Validates an updated Hive policy against the stored one.
pub fn validate_hive_access_policy_update(
    op: &Operation,
    p: &Path,
    new: &pb::AccessPolicy,
    old: &pb::AccessPolicy,
) -> ErrorList {
    rules::all([
        access_policy(&op.as_update(), p, new, Some(old)),
        validate_bindings(&p.child("bindings"), Some(new), &HIVE_ROLES),
    ])
}

/// `DeleteHiveAccessPolicy`.
pub fn validate_delete_hive_access_policy_request(
    op: &Operation,
    req: &pb::DeleteHiveAccessPolicyRequest,
) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_ref(&op, req.hive.as_ref()),
        policy_name(&op, &req.name),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}
