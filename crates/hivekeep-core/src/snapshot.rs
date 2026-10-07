//! Object-storage layout of snapshots.
//!
//! Every snapshot lives under the prefix of the resource that owns it, below
//! an ActorTemplate's storage location:
//!
//! ```text
//! gs://bucket/root                                              location
//! gs://bucket/root/hives/team-a/actors/<uid>                    an Actor's prefix
//! gs://bucket/root/hives/team-a/actors/<uid>/snapshots/<name>   one of its snapshots
//! gs://bucket/root/hives/team-a/tags/<uid>                      a Tag's prefix and its only snapshot
//! ```
//!
//! Ownership is therefore readable from a URI alone: a collector deletes
//! everything under its own prefix and can never name another owner's
//! objects.

use std::fmt;

use crate::names::is_valid_resource_name;
use crate::uri::Uri;

const HIVES_SEGMENT: &str = "hives";
const SNAPSHOTS_SEGMENT: &str = "snapshots";
const ACTORS_OWNER: &str = "actors";
const TAGS_OWNER: &str = "tags";

/// Subdirectory of a checkpoint directory holding durable-dir volume files.
pub const FS_CHECKPOINT_SUBDIR: &str = "fs";
/// Subdirectory of a checkpoint directory holding container rootfs files.
pub const ROOTFS_CHECKPOINT_SUBDIR: &str = "rootfs";

/// Error raised while building or parsing snapshot addresses.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    /// The storage location is unusable.
    #[error("invalid snapshot location {location:?}: {reason}")]
    InvalidLocation {
        /// The rejected location.
        location: String,
        /// Why.
        reason: String,
    },
    /// A storage prefix is unusable.
    #[error("invalid storage prefix {uri:?}: {reason}")]
    InvalidPrefix {
        /// The rejected prefix.
        uri: String,
        /// Why.
        reason: String,
    },
    /// The owner has an invalid hive, kind or id.
    #[error("invalid snapshot owner: {0}")]
    InvalidOwner(String),
    /// A snapshot URI does not follow the layout.
    #[error("invalid snapshot URI {uri:?}{}", reason.as_ref().map(|r| format!(": {r}")).unwrap_or_default())]
    InvalidUri {
        /// The rejected URI.
        uri: String,
        /// Why, when known.
        reason: Option<String>,
    },
    /// An object name could not be appended.
    #[error("invalid object name {name:?}: {reason}")]
    InvalidObject {
        /// The rejected name.
        name: String,
        /// Why.
        reason: String,
    },
    /// A snapshot file name escapes the checkpoint directory or repeats.
    #[error("snapshot_files[{index}] {name:?} {reason}")]
    InvalidFileName {
        /// Position in the list.
        index: usize,
        /// The rejected name.
        name: String,
        /// Why.
        reason: &'static str,
    },
}

/// A fresh, unique snapshot name (a lowercase UUID).
pub fn new_snapshot_name() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Checks that `location` is a URI with a scheme-agnostic bucket and an
/// optional path, and nothing that would swallow or redirect an appended
/// object name: no opaque part, userinfo, query or fragment, no '\', and no
/// segments made only of '.' and ' '.
pub fn validate_snapshot_location(location: &str) -> Result<(), SnapshotError> {
    let err = |reason: &str| SnapshotError::InvalidLocation {
        location: location.to_string(),
        reason: reason.to_string(),
    };
    let u = Uri::parse(location).map_err(|e| err(&e.to_string()))?;
    if u.host.is_empty() {
        return Err(err("missing bucket"));
    }
    if !u.opaque.is_empty() || u.user.is_some() || u.query.is_some() || u.fragment.is_some() {
        return Err(err("must contain only a scheme, bucket, and path"));
    }
    let path = u.path();
    if path.contains('\\') {
        return Err(err("must not contain '\\'"));
    }
    if path
        .split('/')
        .any(|seg| !seg.is_empty() && seg.trim_matches(|c| c == '.' || c == ' ').is_empty())
    {
        return Err(err("must not contain path segments of only '.' and ' '"));
    }
    Ok(())
}

fn join(base: &str, elems: &[&str]) -> Result<String, String> {
    let u = Uri::parse(base).map_err(|e| e.to_string())?;
    Ok(u.join_path(elems).map_err(|e| e.to_string())?.to_string())
}

/// A validated object-storage prefix: scheme, bucket and a non-empty path.
/// Deleting the prefix deletes every object below it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct StoragePrefix(String);

impl StoragePrefix {
    /// Parses a prefix received as a string. A bare bucket is rejected so a
    /// prefix can never name every object in a bucket.
    pub fn parse(uri: &str) -> Result<Self, SnapshotError> {
        validate_snapshot_location(uri)?;
        let u = Uri::parse(uri).map_err(|e| SnapshotError::InvalidPrefix {
            uri: uri.into(),
            reason: e.to_string(),
        })?;
        if u.path().trim_matches('/').is_empty() {
            return Err(SnapshotError::InvalidPrefix {
                uri: uri.into(),
                reason: "missing path".into(),
            });
        }
        Ok(StoragePrefix(uri.to_string()))
    }

    /// Whether this is the empty prefix.
    pub fn is_zero(&self) -> bool {
        self.0.is_empty()
    }

    /// The prefix as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StoragePrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Which kind of resource owns a snapshot prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OwnerKind {
    /// An Actor, for snapshots it took.
    Actor,
    /// A Tag, for the one snapshot it holds.
    Tag,
}

impl OwnerKind {
    fn segment(self) -> &'static str {
        match self {
            OwnerKind::Actor => ACTORS_OWNER,
            OwnerKind::Tag => TAGS_OWNER,
        }
    }
}

/// The resource whose prefix holds a snapshot's objects. Keyed by uid, so a
/// resource recreated under the same name never inherits its predecessor's
/// objects.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotOwner {
    kind: OwnerKind,
    hive: String,
    id: String,
}

impl SnapshotOwner {
    /// Owner of the snapshots an Actor takes.
    pub fn actor(hive: impl Into<String>, actor_uid: impl Into<String>) -> Self {
        SnapshotOwner {
            kind: OwnerKind::Actor,
            hive: hive.into(),
            id: actor_uid.into(),
        }
    }

    /// Owner of the snapshot a Tag holds.
    pub fn tag(hive: impl Into<String>, tag_uid: impl Into<String>) -> Self {
        SnapshotOwner {
            kind: OwnerKind::Tag,
            hive: hive.into(),
            id: tag_uid.into(),
        }
    }

    /// The owner's kind.
    pub fn kind(&self) -> OwnerKind {
        self.kind
    }

    /// The Hive the owner belongs to.
    pub fn hive(&self) -> &str {
        &self.hive
    }

    /// The owner's uid.
    pub fn id(&self) -> &str {
        &self.id
    }

    fn validate(&self) -> Result<(), SnapshotError> {
        if !is_valid_resource_name(&self.hive) {
            return Err(SnapshotError::InvalidOwner(format!(
                "hive {:?} is not a valid resource name",
                self.hive
            )));
        }
        if !is_valid_resource_name(&self.id) {
            return Err(SnapshotError::InvalidOwner(format!(
                "{:?} is not a valid resource name",
                self.id
            )));
        }
        Ok(())
    }

    /// The prefix holding all of this owner's snapshot objects under
    /// `location`.
    pub fn prefix(&self, location: &str) -> Result<StoragePrefix, SnapshotError> {
        self.validate()?;
        validate_snapshot_location(location)?;
        let uri = join(
            location,
            &[HIVES_SEGMENT, &self.hive, self.kind.segment(), &self.id],
        )
        .map_err(|reason| SnapshotError::InvalidPrefix {
            uri: location.into(),
            reason,
        })?;
        Ok(StoragePrefix(uri))
    }
}

impl fmt::Display for SnapshotOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.hive, self.kind.segment(), self.id)
    }
}

/// Where one snapshot's objects live: a location plus its owner's prefix.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct SnapshotUri {
    uri: String,
    location: String,
    owner: Option<SnapshotOwner>,
    owner_prefix: String,
    name: String,
}

impl SnapshotUri {
    /// The URI of a snapshot an Actor took.
    pub fn for_actor(
        location: &str,
        hive: &str,
        actor_uid: &str,
        name: &str,
    ) -> Result<Self, SnapshotError> {
        let owner = SnapshotOwner::actor(hive, actor_uid);
        let prefix = owner.prefix(location)?;
        if !is_valid_resource_name(name) {
            return Err(SnapshotError::InvalidUri {
                uri: prefix.to_string(),
                reason: Some(format!(
                    "snapshot name {name:?} is not a valid resource name"
                )),
            });
        }
        let uri = join(prefix.as_str(), &[SNAPSHOTS_SEGMENT, name]).map_err(|reason| {
            SnapshotError::InvalidUri {
                uri: prefix.to_string(),
                reason: Some(reason),
            }
        })?;
        Ok(SnapshotUri {
            uri,
            location: location.into(),
            owner: Some(owner),
            owner_prefix: prefix.0,
            name: name.into(),
        })
    }

    /// The URI of the snapshot a Tag owns. A Tag holds exactly one snapshot,
    /// so its prefix and its snapshot's are the same.
    pub fn for_tag(location: &str, hive: &str, tag_uid: &str) -> Result<Self, SnapshotError> {
        let owner = SnapshotOwner::tag(hive, tag_uid);
        let prefix = owner.prefix(location)?;
        Ok(SnapshotUri {
            uri: prefix.0.clone(),
            location: location.into(),
            owner: Some(owner),
            owner_prefix: prefix.0,
            name: tag_uid.into(),
        })
    }

    /// Parses a snapshot URI built by [`for_actor`](Self::for_actor) or
    /// [`for_tag`](Self::for_tag). A trailing '/' is tolerated.
    pub fn parse(uri: &str) -> Result<Self, SnapshotError> {
        let bad = |reason: Option<String>| SnapshotError::InvalidUri {
            uri: uri.into(),
            reason,
        };
        let mut u = Uri::parse(uri).map_err(|e| bad(Some(e.to_string())))?;
        let path = u.raw_path.trim_end_matches('/').to_string();
        let segs: Vec<&str> = path.split('/').collect();
        let n = segs.len();
        if n >= 6
            && segs[n - 6] == HIVES_SEGMENT
            && segs[n - 4] == ACTORS_OWNER
            && segs[n - 2] == SNAPSHOTS_SEGMENT
        {
            u.raw_path = segs[..n - 6].join("/");
            return Self::for_actor(&u.to_string(), segs[n - 5], segs[n - 3], segs[n - 1]);
        }
        if n >= 4 && segs[n - 4] == HIVES_SEGMENT && segs[n - 2] == TAGS_OWNER {
            u.raw_path = segs[..n - 4].join("/");
            return Self::for_tag(&u.to_string(), segs[n - 3], segs[n - 1]);
        }
        Err(bad(None))
    }

    /// The template storage location the snapshot is stored under.
    pub fn location(&self) -> &str {
        &self.location
    }

    /// Hive of the owning resource.
    pub fn hive(&self) -> &str {
        self.owner.as_ref().map(|o| o.hive.as_str()).unwrap_or("")
    }

    /// The snapshot's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The owning resource, unless this is the zero URI.
    pub fn owner(&self) -> Option<&SnapshotOwner> {
        self.owner.as_ref()
    }

    /// Whether `owner` owns this snapshot. A collector asks this before
    /// deleting: an Actor restored from a Tag does not own that snapshot.
    pub fn owned_by(&self, owner: &SnapshotOwner) -> bool {
        self.owner.as_ref() == Some(owner)
    }

    /// The prefix holding every snapshot of the owner.
    pub fn owner_prefix(&self) -> StoragePrefix {
        StoragePrefix(self.owner_prefix.clone())
    }

    /// The prefix holding this snapshot's objects.
    pub fn prefix(&self) -> StoragePrefix {
        StoragePrefix(self.uri.clone())
    }

    /// Whether this is the zero URI.
    pub fn is_zero(&self) -> bool {
        self.uri.is_empty()
    }

    /// The URI as a string.
    pub fn as_str(&self) -> &str {
        &self.uri
    }

    /// The address of one object of the snapshot. `name` is a path relative
    /// to the snapshot and may already be percent-encoded.
    pub fn object_uri(&self, name: &str) -> Result<String, SnapshotError> {
        join(&self.uri, &[name]).map_err(|reason| SnapshotError::InvalidObject {
            name: name.into(),
            reason,
        })
    }
}

impl fmt::Display for SnapshotUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.uri)
    }
}

/// Checks that each name is a distinct plain file name in the checkpoint
/// directory or one of its `fs`/`rootfs` subdirectories. File access must
/// still be confined to that directory, since this does not resolve
/// symlinks.
pub fn validate_snapshot_file_names<S: AsRef<str>>(files: &[S]) -> Result<(), SnapshotError> {
    let mut seen = std::collections::HashSet::new();
    for (index, name) in files.iter().enumerate() {
        let name = name.as_ref();
        let bad = |reason| SnapshotError::InvalidFileName {
            index,
            name: name.to_string(),
            reason,
        };
        let parts: Vec<&str> = name.split('/').collect();
        let local = !name.is_empty()
            && !name.starts_with('/')
            && parts
                .iter()
                .all(|p| !p.is_empty() && *p != "." && *p != "..");
        let ok_dir = match parts.as_slice() {
            [_] => true,
            [dir, _] => *dir == FS_CHECKPOINT_SUBDIR || *dir == ROOTFS_CHECKPOINT_SUBDIR,
            _ => false,
        };
        if !local || !ok_dir {
            return Err(bad("is not a file name in the checkpoint directory"));
        }
        if !seen.insert(name) {
            return Err(bad("is duplicated"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_locations() {
        for ok in [
            "gs://bucket/actors/1234/snapshots/5678/",
            "gs://bucket",
            "s3://bucket/path",
            "gs://bucket/a..b/.c/d.",
        ] {
            assert!(validate_snapshot_location(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "gs://",
            "bucket/path",
            "://bucket",
            "gs://bucket/path?x=1",
            "gs://bucket/path#frag",
            "gs://user@bucket/path",
            "gs:bucket/path",
            "gs://bucket/team-a/../team-b",
            "gs://bucket/path/..",
            "gs://bucket/%2e%2e/path",
            "gs://bucket/./path",
            "gs://bucket/.../path",
            "gs://bucket/a/%20..%20/b",
            "gs://bucket/a/..%20/b",
            r"gs://bucket/team-a\..\team-b",
            "gs://bucket/a%5Cb",
        ] {
            assert!(validate_snapshot_location(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn actor_snapshot_uris() {
        let cases = [
            (
                "gs://bucket/root",
                "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1",
            ),
            (
                "gs://bucket/root/",
                "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1",
            ),
            (
                "gs://bucket",
                "gs://bucket/hives/team-a/actors/actor-uid/snapshots/snap-1",
            ),
            (
                "gs://my-bucket/hives/secret",
                "gs://my-bucket/hives/secret/hives/team-a/actors/actor-uid/snapshots/snap-1",
            ),
        ];
        for (loc, want) in cases {
            let u = SnapshotUri::for_actor(loc, "team-a", "actor-uid", "snap-1").unwrap();
            assert_eq!(u.as_str(), want);
            assert_eq!(u.location(), loc);
            assert_eq!(u.hive(), "team-a");
            assert_eq!(u.name(), "snap-1");
        }
        for (loc, hive, uid, name) in [
            ("", "team-a", "actor-uid", "snap-1"),
            ("/root", "team-a", "actor-uid", "snap-1"),
            (
                "gs://bucket/root?generation=1",
                "team-a",
                "actor-uid",
                "snap-1",
            ),
            ("gs://bucket/root", "Team_A", "actor-uid", "snap-1"),
            ("gs://bucket/root", "", "actor-uid", "snap-1"),
            ("gs://bucket/root", "team-a", "Actor_UID", "snap-1"),
            ("gs://bucket/root", "team-a", "", "snap-1"),
            ("gs://bucket/root", "team-a", "actor-uid", "Snap/1"),
            ("gs://bucket/root", "team-a", "actor-uid", ""),
        ] {
            assert!(
                SnapshotUri::for_actor(loc, hive, uid, name).is_err(),
                "{loc} {hive} {uid} {name}"
            );
        }
    }

    #[test]
    fn tag_snapshot_uris() {
        let u = SnapshotUri::for_tag("gs://bucket/root/", "team-a", "tag-uid-1").unwrap();
        assert_eq!(u.as_str(), "gs://bucket/root/hives/team-a/tags/tag-uid-1");
        assert_eq!(u.prefix(), u.owner_prefix());
        assert_eq!(
            SnapshotUri::for_tag("gs://bucket", "team-a", "t")
                .unwrap()
                .as_str(),
            "gs://bucket/hives/team-a/tags/t"
        );
        assert!(
            SnapshotUri::for_tag("gs://bucket/root", "team-a", "2026-08-05T10:04:05Z").is_err()
        );
        assert!(SnapshotUri::for_tag("gs://bucket/root", "team-a", "").is_err());
        assert!(SnapshotUri::for_tag("", "team-a", "t").is_err());
    }

    #[test]
    fn ownership() {
        let actor_uri =
            SnapshotUri::for_actor("gs://bucket/root", "team-a", "actor-uid", "snap-1").unwrap();
        let tag_uri = SnapshotUri::for_tag("gs://bucket/root", "team-a", "tag-uid").unwrap();
        assert!(actor_uri.owned_by(&SnapshotOwner::actor("team-a", "actor-uid")));
        assert!(!tag_uri.owned_by(&SnapshotOwner::actor("team-a", "actor-uid")));
        assert!(tag_uri.owned_by(&SnapshotOwner::tag("team-a", "tag-uid")));
        assert!(!actor_uri.owned_by(&SnapshotOwner::actor("team-a", "other")));
        assert!(!actor_uri.owned_by(&SnapshotOwner::actor("team-b", "actor-uid")));
        assert!(!SnapshotUri::default().owned_by(&SnapshotOwner::actor("team-a", "actor-uid")));
        assert_eq!(
            actor_uri.owner_prefix().as_str(),
            "gs://bucket/root/hives/team-a/actors/actor-uid"
        );
    }

    #[test]
    fn owner_prefixes() {
        assert_eq!(
            SnapshotOwner::actor("team-a", "actor-uid")
                .prefix("gs://bucket/root")
                .unwrap()
                .as_str(),
            "gs://bucket/root/hives/team-a/actors/actor-uid"
        );
        assert_eq!(
            SnapshotOwner::tag("team-a", "snap-1")
                .prefix("gs://bucket/root")
                .unwrap()
                .as_str(),
            "gs://bucket/root/hives/team-a/tags/snap-1"
        );
        assert!(SnapshotOwner::actor("", "")
            .prefix("gs://bucket/root")
            .is_err());
        assert!(SnapshotOwner::actor("team-a", "x").prefix("/root").is_err());
        assert_eq!(SnapshotOwner::tag("h", "t").to_string(), "h/tags/t");
    }

    #[test]
    fn new_names_are_uuids() {
        let a = new_snapshot_name();
        assert!(crate::names::check_uuid(&a).is_empty());
        assert!(is_valid_resource_name(&a));
        assert_ne!(a, new_snapshot_name());
    }

    #[test]
    fn parse_uris() {
        let ok = [
            ("gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/f47ac10b-58cc-4372-a567-0e02b2c3d479", "gs://bucket/root"),
            ("gs://bucket/root/hives/team-a/tags/snap-1", "gs://bucket/root"),
            ("gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/", "gs://bucket/root"),
            ("gs://bucket/hives/team-a/actors/actor-uid/snapshots/snap-1", "gs://bucket"),
            ("gs://my-bucket/hives/secret/hives/team-a/actors/actor-uid/snapshots/snap-1", "gs://my-bucket/hives/secret"),
        ];
        for (uri, loc) in ok {
            let u = SnapshotUri::parse(uri).unwrap_or_else(|e| panic!("{uri}: {e}"));
            assert_eq!(u.location(), loc, "{uri}");
            assert_eq!(u.hive(), "team-a");
            assert_eq!(u.as_str(), uri.trim_end_matches('/'));
        }
        for bad in [
            "gs://bucket/root/hives/team-a/actors/actor-uid",
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/manifest.json",
            "gs://bucket/root/hives/team-a/tags/snap-1/manifest.json",
            "gs://bucket/root/hives/Team/tags/snap-1",
            "gs://bucket/root?x=1/hives/team-a/tags/snap-1",
            "",
        ] {
            assert!(SnapshotUri::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn object_uris() {
        let a =
            SnapshotUri::for_actor("gs://bucket/root", "team-a", "actor-uid", "snap-1").unwrap();
        let t = SnapshotUri::for_tag("gs://bucket/root", "team-a", "snap-2").unwrap();
        assert_eq!(
            a.object_uri("manifest.json").unwrap(),
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/manifest.json"
        );
        assert_eq!(
            a.object_uri("memory.img.zstd").unwrap(),
            "gs://bucket/root/hives/team-a/actors/actor-uid/snapshots/snap-1/memory.img.zstd"
        );
        assert_eq!(
            t.object_uri("manifest.json").unwrap(),
            "gs://bucket/root/hives/team-a/tags/snap-2/manifest.json"
        );
        assert!(a.object_uri("100%done.img").is_err());
    }

    #[test]
    fn zero_uri() {
        let z = SnapshotUri::default();
        assert!(
            z.is_zero()
                && z.as_str().is_empty()
                && z.prefix().is_zero()
                && z.owner_prefix().is_zero()
        );
        assert!(!SnapshotUri::for_tag("gs://b/r", "h", "t")
            .unwrap()
            .is_zero());
    }

    #[test]
    fn storage_prefixes() {
        assert!(StoragePrefix::parse("gs://bucket/root").is_ok());
        assert!(StoragePrefix::parse("gs://bucket/root/hives/a/actors/b").is_ok());
        for bad in [
            "gs://bucket",
            "gs://bucket/",
            "gs://bucket//",
            "",
            "gs://bucket/a?b",
        ] {
            assert!(StoragePrefix::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn snapshot_file_names() {
        assert!(
            validate_snapshot_file_names(&["checkpoint.img", "fs/vol.tar", "rootfs/c.tar"]).is_ok()
        );
        let empty: [&str; 0] = [];
        assert!(validate_snapshot_file_names(&empty).is_ok());
        for bad in [
            vec!["../x"],
            vec!["/abs"],
            vec!["other/x"],
            vec!["fs/a/b"],
            vec!["a//b"],
            vec!["./a"],
            vec!["fs"],
            vec![""],
            vec!["a", "a"],
        ] {
            // "fs" alone is a plain file name, which is fine.
            if bad == vec!["fs"] {
                assert!(validate_snapshot_file_names(&bad).is_ok());
                continue;
            }
            assert!(validate_snapshot_file_names(&bad).is_err(), "{bad:?}");
        }
    }
}
