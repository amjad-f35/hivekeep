//! Optimistic-concurrency guards for updates and deletes.

use hivekeep_proto::api::v1 as pb;

use crate::error::{Result, StoreError};
use crate::resource::Resource;

/// Guards an update with the uid and version the caller observed: the write
/// lands only if the stored object still matches both. Both are required;
/// blind writes are refused with [`StoreError::PreconditionRequired`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Precondition {
    /// The incarnation the write is for.
    pub uid: String,
    /// The revision the write is against.
    pub version: i64,
}

impl Precondition {
    /// A guard on `uid` at `version`.
    pub fn new(uid: impl Into<String>, version: i64) -> Self {
        Precondition {
            uid: uid.into(),
            version,
        }
    }

    /// The guards describing `md`.
    pub fn from_metadata(md: Option<&pb::ResourceMetadata>) -> Self {
        md.map(|m| Precondition::new(m.uid.clone(), m.version))
            .unwrap_or_default()
    }

    /// The guards describing the object the caller observed.
    pub fn of<T: Resource>(observed: &T) -> Self {
        Self::from_metadata(observed.metadata())
    }

    /// Whether both guards are present.
    pub fn validate(&self) -> Result<()> {
        if self.uid.is_empty() {
            return Err(StoreError::PreconditionRequired("uid"));
        }
        if self.version == 0 {
            return Err(StoreError::PreconditionRequired("version"));
        }
        Ok(())
    }

    /// Whether `md` still matches both guards. The uid is reported first: a
    /// new incarnation makes the version meaningless.
    pub fn check(&self, md: Option<&pb::ResourceMetadata>) -> Result<()> {
        DeletePreconditions::from(self.clone()).check_strict(md)
    }
}

/// Pins the incarnation a delete (or mark-for-deletion) may act on. Unlike
/// [`Precondition`], each guard is independently waivable: an empty uid or a
/// zero version accepts whatever is stored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DeletePreconditions {
    /// Accept only the object carrying this uid; empty accepts any.
    pub uid: String,
    /// Accept only this revision; zero accepts any.
    pub version: i64,
}

impl DeletePreconditions {
    /// No guards: acts on whatever holds the name.
    pub fn none() -> Self {
        Self::default()
    }

    /// Guards on `uid` only.
    pub fn uid(uid: impl Into<String>) -> Self {
        DeletePreconditions {
            uid: uid.into(),
            version: 0,
        }
    }

    /// Guards on `version` only.
    pub fn version(version: i64) -> Self {
        DeletePreconditions {
            uid: String::new(),
            version,
        }
    }

    /// Guards on both.
    pub fn new(uid: impl Into<String>, version: i64) -> Self {
        DeletePreconditions {
            uid: uid.into(),
            version,
        }
    }

    /// Whether `md` still describes the object the caller observed. Waived
    /// guards are not checked; the uid is reported before the version.
    pub fn check(&self, md: Option<&pb::ResourceMetadata>) -> Result<()> {
        let (uid, version) = md.map(|m| (m.uid.as_str(), m.version)).unwrap_or(("", 0));
        if !self.uid.is_empty() && self.uid != uid {
            return Err(StoreError::UidConflict);
        }
        if self.version != 0 && self.version != version {
            return Err(StoreError::VersionConflict);
        }
        Ok(())
    }

    fn check_strict(&self, md: Option<&pb::ResourceMetadata>) -> Result<()> {
        let (uid, version) = md.map(|m| (m.uid.as_str(), m.version)).unwrap_or(("", 0));
        if self.uid != uid {
            return Err(StoreError::UidConflict);
        }
        if self.version != version {
            return Err(StoreError::VersionConflict);
        }
        Ok(())
    }
}

impl From<Precondition> for DeletePreconditions {
    fn from(p: Precondition) -> Self {
        DeletePreconditions {
            uid: p.uid,
            version: p.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STORED_UID: &str = "stored-uid";
    const STALE_UID: &str = "stale-uid";
    const STORED_VER: i64 = 7;
    const STALE_VER: i64 = 6;

    fn stored() -> pb::ResourceMetadata {
        pb::ResourceMetadata {
            hive: "test-hive".into(),
            name: "actor-1".into(),
            uid: STORED_UID.into(),
            version: STORED_VER,
            ..Default::default()
        }
    }

    #[test]
    fn check() {
        let md = stored();
        let cases = [
            (Precondition::new(STORED_UID, STORED_VER), None),
            (Precondition::new(STALE_UID, STORED_VER), Some("uid")),
            (Precondition::new(STORED_UID, STALE_VER), Some("version")),
            // A new incarnation makes the version meaningless, so the uid wins.
            (Precondition::new(STALE_UID, STALE_VER), Some("uid")),
        ];
        for (p, want) in cases {
            let got = p.check(Some(&md));
            match want {
                None => assert!(got.is_ok(), "{p:?}: {got:?}"),
                Some("uid") => assert!(matches!(got, Err(StoreError::UidConflict)), "{p:?}"),
                Some(_) => assert!(matches!(got, Err(StoreError::VersionConflict)), "{p:?}"),
            }
        }
    }

    #[test]
    fn validate() {
        assert!(Precondition::new(STORED_UID, STORED_VER).validate().is_ok());
        for p in [
            Precondition::default(),
            Precondition::new(STORED_UID, 0),
            Precondition::new("", STORED_VER),
        ] {
            assert!(
                matches!(p.validate(), Err(StoreError::PreconditionRequired(_))),
                "{p:?}"
            );
        }
    }

    #[test]
    fn from_observed() {
        let actor = pb::Actor {
            metadata: Some(stored()),
            ..Default::default()
        };
        let p = Precondition::of(&actor);
        assert_eq!(p, Precondition::new(STORED_UID, STORED_VER));
        assert!(p.validate().is_ok());

        let unguarded = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "test-hive".into(),
                name: "actor-1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(Precondition::of(&unguarded), Precondition::default());
        assert!(Precondition::of(&unguarded).validate().is_err());

        let uid_only = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                uid: STORED_UID.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            Precondition::of(&uid_only),
            Precondition::new(STORED_UID, 0)
        );
        assert!(Precondition::of(&uid_only).validate().is_err());

        let version_only = pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                version: STORED_VER,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(
            Precondition::of(&version_only),
            Precondition::new("", STORED_VER)
        );
        assert!(Precondition::of(&version_only).validate().is_err());
        assert!(Precondition::of(&pb::Actor::default()).validate().is_err());
    }

    #[test]
    fn delete_guards_are_waivable() {
        let md = stored();
        assert!(DeletePreconditions::none().check(Some(&md)).is_ok());
        assert!(DeletePreconditions::uid(STORED_UID)
            .check(Some(&md))
            .is_ok());
        assert!(DeletePreconditions::version(STORED_VER)
            .check(Some(&md))
            .is_ok());
        assert!(matches!(
            DeletePreconditions::uid(STALE_UID).check(Some(&md)),
            Err(StoreError::UidConflict)
        ));
        assert!(matches!(
            DeletePreconditions::version(STALE_VER).check(Some(&md)),
            Err(StoreError::VersionConflict)
        ));
        assert!(matches!(
            DeletePreconditions::new(STALE_UID, STALE_VER).check(Some(&md)),
            Err(StoreError::UidConflict)
        ));
    }
}
