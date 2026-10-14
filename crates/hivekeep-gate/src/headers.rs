//! The wire contract shared by everything that routes to Actors: the header
//! naming the target Actor of an ingress request.

use hivekeep_core::names::is_valid_resource_name;
use hivekeep_core::resource::ActorRef;

/// Names the Actor selected for ingress routing as `<hive>/<actor>`.
///
/// Field names are case-insensitive in HTTP; this is the lower-case HTTP/2
/// wire form so dataplane configuration and metadata can use it verbatim.
pub const TARGET_ACTOR_HEADER: &str = "hk-target-actor";

/// A [`TARGET_ACTOR_HEADER`] value that is not a valid `<hive>/<actor>`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid actor reference {0:?}")]
pub struct InvalidTargetActor(pub String);

/// Parses and validates a [`TARGET_ACTOR_HEADER`] value.
pub fn parse_target_actor(value: &str) -> Result<ActorRef, InvalidTargetActor> {
    let invalid = || InvalidTargetActor(value.to_string());
    let (hive, name) = value.split_once('/').ok_or_else(invalid)?;
    if name.contains('/') || !is_valid_resource_name(hive) || !is_valid_resource_name(name) {
        return Err(invalid());
    }
    Ok(ActorRef::new(hive, name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_actor_cases() {
        let cases: &[(&str, &str, Option<(&str, &str)>)] = &[
            ("valid", "team-a/actor-1", Some(("team-a", "actor-1"))),
            ("missing separator", "team-a", None),
            ("extra separator", "team-a/actor-1/extra", None),
            ("empty hive", "/actor-1", None),
            ("empty actor", "team-a/", None),
            ("invalid hive", "TEAM-A/actor-1", None),
            ("invalid actor", "team-a/ACTOR-1", None),
            ("comma-joined actors", "demo/a,secret/x", None),
            ("comma-separated with space", "demo/a, demo/b", None),
        ];
        for (name, value, want) in cases {
            let got = parse_target_actor(value);
            match want {
                Some((hive, actor)) => {
                    let got = got.unwrap_or_else(|e| panic!("{name}: {e}"));
                    assert_eq!(
                        (got.hive.as_str(), got.name.as_str()),
                        (*hive, *actor),
                        "{name}"
                    );
                }
                None => assert!(got.is_err(), "{name}: expected error, got {got:?}"),
            }
        }
    }
}
