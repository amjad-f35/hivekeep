//! Hive validation.

use hivekeep_proto::api::v1 as pb;

use super::common::*;
use crate::field::{ErrorList, Path};
use crate::validation::rules::{self, message, HiveRule, Operation};

/// A Hive resource. Hives are global: `metadata.hive` must be empty.
pub fn hive(op: &Operation, p: &Path, n: &pb::Hive, o: Option<&pb::Hive>) -> ErrorList {
    metadata_field(
        op,
        &p.child("metadata"),
        n.metadata.as_ref(),
        o.map(|o| o.metadata.as_ref()),
        HiveRule::Forbidden,
    )
}

fn hive_ref(op: &Operation, r: Option<&pb::ObjectRef>) -> ErrorList {
    object_ref_field(op, &Path::from("hive"), r, None, HiveRule::Forbidden)
}

/// `CreateHive`.
pub fn validate_create_hive_request(op: &Operation, req: &pb::CreateHiveRequest) -> ErrorList {
    let op = op.as_create();
    message(&op, &Path::from("hive"), req.hive.as_ref(), None)
        .required()
        .then(|p, n, _| hive(&op, p, n, None))
        .finish()
}

/// `GetHive`.
pub fn validate_get_hive_request(op: &Operation, req: &pb::GetHiveRequest) -> ErrorList {
    hive_ref(&op.as_create(), req.hive.as_ref())
}

/// `ListHives`.
pub fn validate_list_hives_request(op: &Operation, req: &pb::ListHivesRequest) -> ErrorList {
    list_options(
        &op.as_create(),
        &Path::from("options"),
        req.options.as_ref(),
    )
}

/// `DeleteHive`.
pub fn validate_delete_hive_request(op: &Operation, req: &pb::DeleteHiveRequest) -> ErrorList {
    let op = op.as_create();
    rules::all([
        hive_ref(&op, req.hive.as_ref()),
        delete_options(&op, &Path::from("options"), req.options.as_ref()),
    ])
}
