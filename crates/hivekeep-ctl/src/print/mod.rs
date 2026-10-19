//! Per-resource renderers.
//!
//! Each resource prints as a table of its most useful fields, or as the full
//! message in JSON or YAML. A list prints as the matching `List...Response`,
//! sorted so output is stable.

pub mod access_policy;
pub mod actor;
pub mod actor_template;
pub mod assignment;
pub mod egress_policy;
pub mod hive;
pub mod tag;
pub mod worker;
pub mod worker_top;

/// The value name of a generated protobuf enum, or the number for a value
/// the client does not know.
pub(crate) fn enum_name<E>(value: i32) -> String
where
    E: TryFrom<i32> + EnumName,
{
    E::try_from(value)
        .map(|e| e.name().to_string())
        .unwrap_or_else(|_| value.to_string())
}

/// Access to a generated enum's value name.
pub(crate) trait EnumName {
    fn name(&self) -> &'static str;
}

macro_rules! enum_names {
    ($($ty:ident),* $(,)?) => {
        $(impl EnumName for hivekeep_proto::api::v1::$ty {
            fn name(&self) -> &'static str { self.as_str_name() }
        })*
    };
}

enum_names!(ActorState, WorkerState, TagScope, SnapshotFidelity);
