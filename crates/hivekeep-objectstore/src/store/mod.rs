//! Managing the existence of the objects an external snapshot is made of:
//! listing them, copying them, and deleting them once nothing owns them.
//!
//! This is separate from [`crate::storage`], which reads and writes snapshot
//! content. The control plane never handles those bytes: it copies
//! server-side and deletes by name, so a multi-gigabyte memory image never
//! passes through hivekeep-api.

pub mod env;
pub mod fake;
pub mod gcs;
pub mod s3;

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use hivekeep_core::snapshot::StoragePrefix;

use crate::error::{Error, ErrorKind, Result};

/// How many per-object operations of one prefix run at once. A snapshot is a
/// handful of objects, so this is about not serializing their round trips.
pub const PREFIX_CONCURRENCY: usize = 8;

/// The object-storage surface the control plane needs; every method
/// addresses objects by name.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// The full names of the objects under `prefix` in `bucket`.
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>>;
    /// Removes one object. Deleting one already gone succeeds, so a retry
    /// over a partly collected snapshot finishes cleanly.
    async fn delete(&self, bucket: &str, object: &str) -> Result<()>;
    /// Copies one object server-side, overwriting the destination.
    async fn copy(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
    ) -> Result<()>;
}

#[async_trait]
impl<T: Store + ?Sized> Store for Arc<T> {
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        (**self).list(bucket, prefix).await
    }
    async fn delete(&self, bucket: &str, object: &str) -> Result<()> {
        (**self).delete(bucket, object).await
    }
    async fn copy(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
    ) -> Result<()> {
        (**self)
            .copy(src_bucket, src_object, dst_bucket, dst_object)
            .await
    }
}

/// Only a provider's word that the bucket is missing counts; any other
/// failure must keep a cleanup from being marked complete.
pub fn is_missing_bucket(err: &Error) -> bool {
    matches!(err.kind(), ErrorKind::BucketNotFound) || err.s3_code() == Some("NoSuchBucket")
}

/// Removes every object under `uri`: one external snapshot, or every
/// snapshot an owner holds. A prefix with nothing left is already collected
/// and succeeds, including when its bucket no longer exists.
pub async fn delete_prefix(s: &dyn Store, uri: &StoragePrefix) -> Result<()> {
    let (bucket, prefix) = bucket_prefix(uri)?;
    let objects = match s.list(&bucket, &prefix).await {
        Ok(o) => o,
        Err(e) if is_missing_bucket(&e) => {
            tracing::warn!(bucket = %bucket, prefix = %prefix, error = %e,
                "snapshot bucket is missing; treating prefix as already deleted");
            return Ok(());
        }
        Err(e) => return Err(e.context(format!("while listing {uri}"))),
    };
    stream::iter(objects.into_iter().map(Ok))
        .try_for_each_concurrent(PREFIX_CONCURRENCY, |object| {
            let bucket = bucket.clone();
            async move {
                match s.delete(&bucket, &object).await {
                    Ok(()) => Ok(()),
                    Err(e) if is_missing_bucket(&e) => {
                        tracing::warn!(bucket = %bucket, object = %object, error = %e,
                            "snapshot bucket is missing; treating object as already deleted");
                        Ok(())
                    }
                    Err(e) => {
                        Err(e.context(format!("while deleting {object} from bucket {bucket}")))
                    }
                }
            }
        })
        .await
}

/// Copies every object of the external snapshot at `src` to `dst`, keeping
/// each object's name relative to the prefix. The destination is
/// deterministic, so a retry overwrites what an earlier attempt wrote.
pub async fn copy_prefix(s: &dyn Store, src: &StoragePrefix, dst: &StoragePrefix) -> Result<()> {
    let (src_bucket, src_prefix) = bucket_prefix(src)?;
    let (dst_bucket, dst_prefix) = bucket_prefix(dst)?;
    let objects = s
        .list(&src_bucket, &src_prefix)
        .await
        .map_err(|e| e.context(format!("while listing {src}")))?;
    // Copying nothing would leave the destination naming a snapshot nothing
    // can restore from.
    if objects.is_empty() {
        return Err(Error::new(
            ErrorKind::EmptySource,
            format!("external snapshot {src}: no objects to copy"),
        ));
    }
    let results: Vec<Result<()>> = stream::iter(objects)
        .map(|object| {
            let dst_object = format!(
                "{dst_prefix}{}",
                object.strip_prefix(&src_prefix).unwrap_or(&object)
            );
            let (sb, db) = (src_bucket.clone(), dst_bucket.clone());
            async move {
                s.copy(&sb, &object, &db, &dst_object)
                    .await
                    .map_err(|e| e.context(format!("while copying {object} to {dst_object}")))
            }
        })
        .buffer_unordered(PREFIX_CONCURRENCY)
        .collect()
        .await;
    results.into_iter().collect()
}

/// Splits a storage prefix into its bucket and the object-name prefix its
/// objects share. The prefix keeps its trailing separator, so it cannot
/// match a sibling whose name it happens to start.
pub fn bucket_prefix(uri: &StoragePrefix) -> Result<(String, String)> {
    if uri.is_zero() {
        return Err(Error::other("empty storage prefix"));
    }
    let parsed = url::Url::parse(uri.as_str())
        .map_err(|e| Error::other(format!("while parsing storage prefix {uri}: {e}")))?;
    let host = parsed.host_str().unwrap_or_default();
    if host.is_empty() {
        return Err(Error::other(format!("storage prefix {uri} has no bucket")));
    }
    let path = percent_encoding::percent_decode_str(parsed.path())
        .decode_utf8_lossy()
        .into_owned();
    Ok((
        host.to_string(),
        format!("{}/", path.trim_start_matches('/')),
    ))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::error::{ServiceDetail, ServiceResponse};
    use crate::store::fake::FakeStore;
    use hivekeep_core::snapshot::{SnapshotOwner, SnapshotUri};
    use std::sync::Arc;

    pub const TEST_LOCATION: &str = "gs://bucket/root";

    pub fn actor_uri(location: &str, hive: &str, uid: &str, name: &str) -> SnapshotUri {
        SnapshotUri::for_actor(location, hive, uid, name).unwrap()
    }

    pub fn tag_uri(location: &str, hive: &str, name: &str) -> SnapshotUri {
        SnapshotUri::for_tag(location, hive, name).unwrap()
    }

    pub fn owner_prefix(hive: &str, uid: &str, location: &str) -> StoragePrefix {
        SnapshotOwner::actor(hive, uid).prefix(location).unwrap()
    }

    #[tokio::test]
    async fn delete_prefix_collects_only_its_subtree() {
        let a1s1 = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
        type Case<'a> = (
            &'a str,
            Vec<(SnapshotUri, Vec<&'a str>)>,
            StoragePrefix,
            Vec<&'a str>,
        );
        let cases: Vec<Case> = vec![
            (
                "releases every object of one snapshot",
                vec![(
                    a1s1.clone(),
                    vec!["manifest.json", "memory.zst", "disks/root.zst"],
                )],
                a1s1.prefix(),
                vec![],
            ),
            (
                "releases every snapshot an actor owns",
                vec![
                    (a1s1.clone(), vec!["manifest.json"]),
                    (
                        actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-2"),
                        vec!["manifest.json"],
                    ),
                ],
                owner_prefix("team-a", "actor-1", TEST_LOCATION),
                vec![],
            ),
            (
                "leaves a snapshot whose name it is a prefix of",
                vec![
                    (a1s1.clone(), vec!["manifest.json"]),
                    (
                        actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-10"),
                        vec!["manifest.json"],
                    ),
                ],
                a1s1.prefix(),
                vec!["bucket/root/hives/team-a/actors/actor-1/snapshots/snap-10/manifest.json"],
            ),
            (
                "leaves another actor's snapshots",
                vec![
                    (a1s1.clone(), vec!["manifest.json"]),
                    (
                        actor_uri(TEST_LOCATION, "team-a", "actor-2", "snap-1"),
                        vec!["manifest.json"],
                    ),
                ],
                owner_prefix("team-a", "actor-1", TEST_LOCATION),
                vec!["bucket/root/hives/team-a/actors/actor-2/snapshots/snap-1/manifest.json"],
            ),
            (
                "leaves the same actor UID in another hive",
                vec![
                    (a1s1.clone(), vec!["manifest.json"]),
                    (
                        actor_uri(TEST_LOCATION, "team-b", "actor-1", "snap-1"),
                        vec!["manifest.json"],
                    ),
                ],
                owner_prefix("team-a", "actor-1", TEST_LOCATION),
                vec!["bucket/root/hives/team-b/actors/actor-1/snapshots/snap-1/manifest.json"],
            ),
            (
                "an actor's prefix leaves a tag's snapshot alone",
                vec![
                    (a1s1.clone(), vec!["manifest.json"]),
                    (
                        tag_uri(TEST_LOCATION, "team-a", "snap-2"),
                        vec!["manifest.json"],
                    ),
                ],
                owner_prefix("team-a", "actor-1", TEST_LOCATION),
                vec!["bucket/root/hives/team-a/tags/snap-2/manifest.json"],
            ),
            (
                "an already collected snapshot succeeds",
                vec![],
                a1s1.prefix(),
                vec![],
            ),
        ];
        for (name, seed, target, remaining) in cases {
            let fake = FakeStore::new();
            for (uri, names) in &seed {
                fake.put_snapshot(uri, names);
            }
            delete_prefix(&fake, &target)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(fake.objects(), remaining, "{name}");
        }
    }

    fn missing_bucket(provider: &str) -> Error {
        match provider {
            "S3" => Error::service(
                ServiceResponse {
                    status: 404,
                    detail: ServiceDetail::S3 {
                        code: "NoSuchBucket".into(),
                    },
                },
                "NoSuchBucket",
            ),
            _ => Error::new(ErrorKind::BucketNotFound, "storage: bucket doesn't exist"),
        }
    }

    #[tokio::test]
    async fn delete_prefix_missing_bucket_warns() {
        for provider in ["S3", "GCS"] {
            for phase in ["list", "delete"] {
                let logs = crate::testing::LogCapture::default();
                let _guard = logs.install();
                let fake = FakeStore::new();
                let uri = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
                fake.put_snapshot(&uri, &["manifest.json"]);
                let err = missing_bucket(provider);
                let fail = Arc::new(move |_: &str, _: &str| Some(err.clone()));
                if phase == "list" {
                    fake.set_on_list(fail);
                } else {
                    fake.set_on_delete(fail);
                }
                delete_prefix(&fake, &uri.prefix()).await.unwrap();
                let out = logs.contents();
                assert!(
                    out.contains("\"level\":\"WARN\""),
                    "{provider}/{phase}: {out}"
                );
                assert!(
                    out.contains("\"bucket\":\"bucket\""),
                    "{provider}/{phase}: {out}"
                );
                assert!(
                    out.contains(&missing_bucket(provider).to_string()),
                    "{provider}/{phase}: {out}"
                );
            }
        }
    }

    #[tokio::test]
    async fn delete_prefix_failures() {
        for phase in ["list", "delete"] {
            let fake = FakeStore::new();
            let fail = Arc::new(|_: &str, _: &str| Some(Error::other("boom")));
            if phase == "list" {
                fake.set_on_list(fail);
            } else {
                fake.set_on_delete(fail);
            }
            let uri = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
            fake.put_snapshot(&uri, &["manifest.json", "memory.zst"]);
            let e = delete_prefix(&fake, &uri.prefix()).await.unwrap_err();
            assert!(e.to_string().contains("boom"), "{phase}: {e}");
        }
    }

    #[tokio::test]
    async fn delete_prefix_resumes() {
        let fake = FakeStore::new();
        let uri = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
        fake.put_snapshot(&uri, &["manifest.json", "memory.zst", "disks/root.zst"]);
        fake.set_on_delete(Arc::new(|_: &str, object: &str| {
            object.ends_with("memory.zst").then(|| Error::other("boom"))
        }));
        assert!(delete_prefix(&fake, &uri.prefix())
            .await
            .unwrap_err()
            .to_string()
            .contains("boom"));
        fake.clear_hooks();
        delete_prefix(&fake, &uri.prefix()).await.unwrap();
        assert!(fake.objects().is_empty());
    }

    #[tokio::test]
    async fn copy_prefix_copies_every_object() {
        let src = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
        let dst = tag_uri(TEST_LOCATION, "team-a", "snap-2");
        let fake = FakeStore::new();
        fake.put_snapshot(&src, &["manifest.json", "memory.zst", "disks/root.zst"]);
        copy_prefix(&fake, &src.prefix(), &dst.prefix())
            .await
            .unwrap();
        let want = vec!["disks/root.zst", "manifest.json", "memory.zst"];
        assert_eq!(fake.snapshot(&dst), want);
        assert_eq!(fake.snapshot(&src), want);
    }

    #[tokio::test]
    async fn copy_prefix_across_buckets() {
        let src = actor_uri("gs://old-bucket/root", "team-a", "actor-1", "snap-1");
        let dst = tag_uri("gs://new-bucket", "team-a", "snap-2");
        let fake = FakeStore::new();
        fake.put_snapshot(&src, &["manifest.json"]);
        copy_prefix(&fake, &src.prefix(), &dst.prefix())
            .await
            .unwrap();
        assert_eq!(
            fake.objects(),
            vec![
                "new-bucket/hives/team-a/tags/snap-2/manifest.json",
                "old-bucket/root/hives/team-a/actors/actor-1/snapshots/snap-1/manifest.json",
            ]
        );
    }

    #[tokio::test]
    async fn copy_prefix_retry_overwrites() {
        let src = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
        let dst = tag_uri(TEST_LOCATION, "team-a", "snap-2");
        let fake = FakeStore::new();
        fake.put_snapshot(&src, &["manifest.json", "memory.zst"]);
        fake.set_on_copy(Arc::new(|src_object: &str, _: &str| {
            src_object
                .ends_with("memory.zst")
                .then(|| Error::other("boom"))
        }));
        assert!(copy_prefix(&fake, &src.prefix(), &dst.prefix())
            .await
            .is_err());
        fake.clear_hooks();
        copy_prefix(&fake, &src.prefix(), &dst.prefix())
            .await
            .unwrap();
        assert_eq!(fake.snapshot(&dst), vec!["manifest.json", "memory.zst"]);
    }

    #[tokio::test]
    async fn copy_prefix_empty_source() {
        let src = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
        let dst = tag_uri(TEST_LOCATION, "team-a", "snap-2");
        let fake = FakeStore::new();
        let e = copy_prefix(&fake, &src.prefix(), &dst.prefix())
            .await
            .unwrap_err();
        assert!(matches!(e.kind(), ErrorKind::EmptySource));
        assert!(fake.objects().is_empty());
    }

    #[test]
    fn bucket_prefix_splits() {
        let cases = [
            (
                "gcs location with a path",
                actor_uri("gs://bucket/root", "team-a", "actor-1", "snap-1").prefix(),
                "bucket",
                "root/hives/team-a/actors/actor-1/snapshots/snap-1/",
            ),
            (
                "s3 location at the bucket root",
                actor_uri("s3://bucket", "team-a", "actor-1", "snap-1").prefix(),
                "bucket",
                "hives/team-a/actors/actor-1/snapshots/snap-1/",
            ),
            (
                "an actor's owner prefix",
                owner_prefix("team-a", "actor-1", "gs://bucket/root"),
                "bucket",
                "root/hives/team-a/actors/actor-1/",
            ),
            (
                "a tag's prefix",
                tag_uri("gs://bucket/root", "team-a", "snap-1").prefix(),
                "bucket",
                "root/hives/team-a/tags/snap-1/",
            ),
        ];
        for (name, prefix, bucket, path) in cases {
            assert_eq!(
                bucket_prefix(&prefix).unwrap(),
                (bucket.to_string(), path.to_string()),
                "{name}"
            );
        }
        assert!(
            bucket_prefix(&StoragePrefix::default()).is_err(),
            "the zero prefix"
        );
    }
}
