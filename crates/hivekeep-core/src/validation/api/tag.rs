//! Tag validation.

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use crate::field::{ErrorList, FieldError, Path};
use crate::validation::rules::{self, int, message, string, Format, HiveRule, Operation};

fn tag_status(op: &Operation, p: &Path, n: &pb::TagStatus, o: Option<&pb::TagStatus>) -> ErrorList {
    rules::all([
        message(
            op,
            &p.child("snapshot"),
            n.snapshot.as_ref(),
            o.map(|o| o.snapshot.as_ref()),
        )
        .optional()
        .then(|p, n, o| {
            rules::all([
                message(
                    op,
                    &p.child("durable_snapshot"),
                    n.durable_snapshot.as_ref(),
                    o.map(|o| o.durable_snapshot.as_ref()),
                )
                .required()
                .finish(),
                snapshot(op, p, n, o),
            ])
        })
        .finish(),
        string(
            op,
            &p.child("actor_template_uid"),
            &n.actor_template_uid,
            o.map(|o| o.actor_template_uid.as_str()),
        )
        .required()
        .format(Format::Uuid)
        .finish(),
    ])
}

/// A Tag resource.
pub fn tag(op: &Operation, p: &Path, n: &pb::Tag, o: Option<&pb::Tag>) -> ErrorList {
    rules::all([
        metadata_field(
            op,
            &p.child("metadata"),
            n.metadata.as_ref(),
            o.map(|o| o.metadata.as_ref()),
            HiveRule::Required,
        ),
        message(
            op,
            &p.child("status"),
            n.status.as_ref(),
            o.map(|o| o.status.as_ref()),
        )
        .optional()
        .then(|p, n, o| tag_status(op, p, n, o))
        .finish(),
        int(op, &p.child("scope"), n.scope, o.map(|o| o.scope))
            .required()
            .min(1)
            .max(2)
            .finish(),
        message(
            op,
            &p.child("source_actor"),
            n.source_actor.as_ref(),
            o.map(|o| o.source_actor.as_ref()),
        )
        .immutable()
        .required()
        .then(|p, n, _| object_ref(op, p, n, HiveRule::Required))
        .finish(),
    ])
}

fn tag_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("tag"), r, None, HiveRule::Required)
}

/// `CreateTag`. The tag must live in its source Actor's Hive.
pub fn validate_create_tag_request(op: &Operation, req: &pb::CreateTagRequest) -> ErrorList {
    let op = op.as_create();
    let mut errs = ErrorList::new();
    if let Some(t) = &req.tag {
        let src = t
            .source_actor
            .as_ref()
            .map(|r| r.hive.as_str())
            .unwrap_or("");
        let own = t.metadata.as_ref().map(|m| m.hive.as_str()).unwrap_or("");
        if !src.is_empty() && !own.is_empty() && src != own {
            errs.push(FieldError::invalid(
                &Path::new(["tag", "metadata", "hive"]),
                own,
                "must match source_actor.hive",
            ));
        }
    }
    errs.extend(
        message(&op, &Path::from("tag"), req.tag.as_ref(), None)
            .required()
            .then(|p, n, _| tag(&op, p, n, None))
            .finish(),
    );
    errs
}

/// `GetTag`.
pub fn validate_get_tag_request(op: &Operation, req: &pb::GetTagRequest) -> ErrorList {
    tag_ref(&op.as_create(), req.tag.as_ref())
}

/// `ListTags`.
pub fn validate_list_tags_request(op: &Operation, req: &pb::ListTagsRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_filter(&op, &Path::from("hive"), &req.hive),
        list_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// `UpdateTag`. Only the request shape and metadata are checked; the merged
/// result is checked with [`validate_tag_update`].
pub fn validate_update_tag_request(op: &Operation, req: &pb::UpdateTagRequest) -> ErrorList {
    let op = op.as_create();
    message(&op, &Path::from("tag"), req.tag.as_ref(), None)
        .required()
        .then(|p, n, _| {
            metadata_field(
                &op,
                &p.child("metadata"),
                n.metadata.as_ref(),
                None,
                HiveRule::Required,
            )
        })
        .finish()
}

/// `DeleteTag`.
pub fn validate_delete_tag_request(op: &Operation, req: &pb::DeleteTagRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        tag_ref(&op, req.tag.as_ref()),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}

/// Validates a Tag against the stored one.
pub fn validate_tag_update(op: &Operation, p: &Path, new: &pb::Tag, old: &pb::Tag) -> ErrorList {
    tag(&op.as_update(), p, new, Some(old))
}
