//! API-shape rules: predicates over a [`model::Api`](crate::apitool::model::Api)
//! that each report one [`Finding`] per violation.

use crate::apitool::model::{Api, ModelError};

mod documented;
mod fields;
mod method;
mod naming;
mod no_oneofs;
mod resources;
mod standard_methods;

#[cfg(test)]
mod fixtures;

pub use documented::DOCUMENTED;
pub use naming::{
    CUSTOM_RESPONSE_NAME_MATCHES_METHOD, ENUM_VALUES_PREFIXED, ENUM_ZERO_VALUE_UNSPECIFIED,
    REQUEST_NAME_MATCHES_METHOD, SUBRESOURCE_NAMING,
};
pub use no_oneofs::NO_ONEOFS;
pub use resources::{RESOURCE_METADATA, RESOURCE_STATUS_FIELD_SHAPE};
pub use standard_methods::{
    CREATE_REQUEST_SHAPE, DELETE_OPTIONS_SHAPE, DELETE_REQUEST_SHAPE, GET_REQUEST_SHAPE,
    LIST_METHOD_SHAPE, LIST_RESPONSE_NAME_MATCHES_METHOD, SINGLETON_NO_LIST,
    STANDARD_METHOD_RETURNS_RESOURCE, UPDATE_REQUEST_SHAPE,
};

/// One rule violation: which API entity it is about, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub subject: String,
    pub message: String,
}

impl Finding {
    pub fn new(subject: impl Into<String>, message: impl Into<String>) -> Self {
        Finding {
            subject: subject.into(),
            message: message.into(),
        }
    }
}

/// A rule could not be evaluated at all: the API is malformed in a way no rule
/// can check past, such as a standard method whose name does not parse.
#[derive(Debug, thiserror::Error)]
pub enum LintError {
    #[error(transparent)]
    Model(#[from] ModelError),
    #[error("{0}")]
    Method(String),
}

/// The outcome of running one rule.
pub type CheckResult = Result<Vec<Finding>, LintError>;

/// One predicate over the API model.
#[derive(Clone, Copy)]
pub struct Rule {
    pub name: &'static str,
    pub description: &'static str,
    pub check: fn(&Api) -> CheckResult,
}

impl std::fmt::Debug for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rule").field("name", &self.name).finish()
    }
}

/// Every rule `validate` runs, in the order their findings are reported.
pub const ALL: &[Rule] = &[
    RESOURCE_METADATA,
    LIST_METHOD_SHAPE,
    REQUEST_NAME_MATCHES_METHOD,
    STANDARD_METHOD_RETURNS_RESOURCE,
    GET_REQUEST_SHAPE,
    DELETE_REQUEST_SHAPE,
    DELETE_OPTIONS_SHAPE,
    CREATE_REQUEST_SHAPE,
    UPDATE_REQUEST_SHAPE,
    LIST_RESPONSE_NAME_MATCHES_METHOD,
    CUSTOM_RESPONSE_NAME_MATCHES_METHOD,
    SINGLETON_NO_LIST,
    SUBRESOURCE_NAMING,
    ENUM_ZERO_VALUE_UNSPECIFIED,
    ENUM_VALUES_PREFIXED,
    RESOURCE_STATUS_FIELD_SHAPE,
    NO_ONEOFS,
    DOCUMENTED,
];

/// Full names of the shared API types the rules check against.
pub(crate) const RESOURCE_METADATA_TYPE: &str = "hivekeep.api.v1.ResourceMetadata";
pub(crate) const OBJECT_REF_TYPE: &str = "hivekeep.api.v1.ObjectRef";
pub(crate) const CREATE_OPTIONS_TYPE: &str = "hivekeep.api.v1.CreateOptions";
pub(crate) const DELETE_OPTIONS_TYPE: &str = "hivekeep.api.v1.DeleteOptions";
pub(crate) const LIST_OPTIONS_TYPE: &str = "hivekeep.api.v1.ListOptions";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rule_names_are_unique() {
        let mut names: Vec<_> = ALL.iter().map(|r| r.name).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), ALL.len());
    }
}
