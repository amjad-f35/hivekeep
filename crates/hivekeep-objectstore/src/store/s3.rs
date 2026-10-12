//! The control plane's [`Store`] on S3.

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use http::Method;
use serde::Deserialize;

use super::Store;
use crate::error::{Error, Result};
use crate::http::encode_path;
use crate::s3::S3Client;

/// The largest object CopyObject accepts. A memory image routinely exceeds
/// it, so anything this size or above is copied in parts.
pub const COPY_OBJECT_MAX_SIZE: i64 = 5 << 30;
/// What each part copy moves. S3 allows 10,000 parts, so 1 GiB parts cover
/// 10 TiB while keeping round trips few.
pub const COPY_PART_SIZE: i64 = 1 << 30;
/// Parts of one object copied at once; it stacks with the prefix
/// concurrency, so it is small.
pub const COPY_PART_CONCURRENCY: usize = 4;

/// A [`Store`] on S3 or an S3-compatible store.
#[derive(Clone)]
pub struct S3Store {
    client: S3Client,
}

impl S3Store {
    /// A store over `client`.
    pub fn new(client: S3Client) -> Self {
        S3Store { client }
    }
}

/// Whether `err` says the object or bucket is not there. S3-compatible
/// stores do not all use the same code.
pub fn is_s3_not_found(err: &Error) -> bool {
    matches!(err.s3_code(), Some("NoSuchKey" | "NotFound" | "404"))
}

/// The `x-amz-copy-source` value: bucket and key escaped as one path.
pub fn copy_source(bucket: &str, object: &str) -> String {
    encode_path(&format!("{bucket}/{object}"))
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct ListBucketResult {
    #[serde(default)]
    is_truncated: bool,
    #[serde(default)]
    next_continuation_token: String,
    #[serde(default)]
    contents: Vec<Contents>,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Contents {
    key: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CopyPartResult {
    #[serde(rename = "ETag", default)]
    etag: String,
}

#[async_trait]
impl Store for S3Store {
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut token = String::new();
        loop {
            let mut q = vec![("list-type", "2"), ("prefix", prefix)];
            if !token.is_empty() {
                q.push(("continuation-token", token.as_str()));
            }
            let req = self.client.request(Method::GET, bucket, "", &q)?;
            let body = self.client.send_read(req).await.map_err(|e| {
                e.context(format!(
                    "while listing objects under s3://{bucket}/{prefix}"
                ))
            })?;
            let page: ListBucketResult =
                quick_xml::de::from_str(&String::from_utf8_lossy(&body))
                    .map_err(|e| Error::other(format!("listing response: {e}")))?;
            objects.extend(page.contents.into_iter().map(|c| c.key));
            if !page.is_truncated || page.next_continuation_token.is_empty() {
                return Ok(objects);
            }
            token = page.next_continuation_token;
        }
    }

    /// Objects are deleted one at a time rather than batched: a snapshot is a
    /// handful, and callers already run these in parallel.
    async fn delete(&self, bucket: &str, object: &str) -> Result<()> {
        let req = self.client.request(Method::DELETE, bucket, object, &[])?;
        match self.client.send_read(req).await {
            Ok(_) => Ok(()),
            // S3 reports deleting a missing object as success, but a
            // compatible store may not; either way it is the state asked for.
            Err(e) if is_s3_not_found(&e) => Ok(()),
            Err(e) => Err(e.context(format!("while deleting s3://{bucket}/{object}"))),
        }
    }

    async fn copy(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
    ) -> Result<()> {
        let head = self
            .client
            .send(
                self.client
                    .request(Method::HEAD, src_bucket, src_object, &[])?,
            )
            .await
            .map_err(|e| {
                e.context(format!(
                    "while reading metadata of s3://{src_bucket}/{src_object}"
                ))
            })?;
        let size: i64 = head
            .header_str("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        if size >= COPY_OBJECT_MAX_SIZE {
            return self
                .copy_multipart(src_bucket, src_object, dst_bucket, dst_object, size)
                .await;
        }
        let req = self
            .client
            .request(
                Method::PUT,
                dst_bucket,
                dst_object,
                &[("x-id", "CopyObject")],
            )?
            .header("x-amz-copy-source", copy_source(src_bucket, src_object));
        self.client.send_read(req).await.map(|_| ()).map_err(|e| {
            e.context(format!(
                "while copying s3://{src_bucket}/{src_object} to s3://{dst_bucket}/{dst_object}"
            ))
        })
    }
}

impl S3Store {
    /// Copies an object too large for CopyObject as server-side part copies.
    /// A failure aborts the upload, so a retry starts from a clean
    /// destination.
    async fn copy_multipart(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        size: i64,
    ) -> Result<()> {
        let created = self
            .client
            .send_read(self.client.request(
                Method::POST,
                dst_bucket,
                dst_object,
                &[("uploads", "")],
            )?)
            .await
            .map_err(|e| {
                e.context(format!(
                    "while starting multipart copy of s3://{src_bucket}/{src_object}"
                ))
            })?;
        let upload_id = quick_xml::de::from_str::<InitiateMultipartUploadResult>(
            &String::from_utf8_lossy(&created),
        )
        .map_err(|e| Error::other(format!("multipart upload response: {e}")))?
        .upload_id;
        let parts = match self
            .copy_parts(
                src_bucket, src_object, dst_bucket, dst_object, &upload_id, size,
            )
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return Err(self
                    .client
                    .abort_multipart(dst_bucket, dst_object, &upload_id, e)
                    .await)
            }
        };
        if let Err(e) = self
            .client
            .complete_multipart(dst_bucket, dst_object, &upload_id, &parts)
            .await
        {
            // A failed completion leaves the parts uploaded, which needs the
            // same cleanup.
            let e = e.context(format!(
                "while completing multipart copy to s3://{dst_bucket}/{dst_object}"
            ));
            return Err(self
                .client
                .abort_multipart(dst_bucket, dst_object, &upload_id, e)
                .await);
        }
        Ok(())
    }

    async fn copy_parts(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        upload_id: &str,
        size: i64,
    ) -> Result<Vec<(u32, String)>> {
        let source = copy_source(src_bucket, src_object);
        let count = (size + COPY_PART_SIZE - 1) / COPY_PART_SIZE;
        let results: Vec<Result<(u32, String)>> = stream::iter(0..count)
            .map(|i| {
                let number = (i + 1) as u32;
                let start = i * COPY_PART_SIZE;
                let end = (start + COPY_PART_SIZE).min(size) - 1;
                let source = source.clone();
                async move {
                    let req = self
                        .client
                        .request(
                            Method::PUT,
                            dst_bucket,
                            dst_object,
                            &[("partNumber", &number.to_string()), ("uploadId", upload_id)],
                        )?
                        .header("x-amz-copy-source", source)
                        .header("x-amz-copy-source-range", format!("bytes={start}-{end}"));
                    let body = self.client.send_read(req).await.map_err(|e| {
                        e.context(format!(
                            "while copying part {number} of s3://{src_bucket}/{src_object}"
                        ))
                    })?;
                    let etag =
                        quick_xml::de::from_str::<CopyPartResult>(&String::from_utf8_lossy(&body))
                            .map(|r| r.etag)
                            .unwrap_or_default();
                    Ok((number, etag))
                }
            })
            .buffer_unordered(COPY_PART_CONCURRENCY)
            .collect()
            .await;
        let mut parts = results.into_iter().collect::<Result<Vec<_>>>()?;
        // CompleteMultipartUpload wants part-number order.
        parts.sort();
        Ok(parts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::Response;
    use crate::s3::fake::{xml, FakeS3, S3Request};
    use crate::store::tests::{actor_uri, tag_uri, TEST_LOCATION};
    use crate::store::{bucket_prefix, copy_prefix, delete_prefix};

    fn store(fake: &FakeS3) -> S3Store {
        S3Store::new(fake.client())
    }

    #[tokio::test]
    async fn s3_list_spans_pages() {
        let fake = FakeS3::new();
        fake.set_handle(|req| {
            req.query("list-type")?;
            Some(Response::new(
                200,
                if req.query("continuation-token").is_none() {
                    xml("<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken><Contents><Key>root/snap/manifest.json</Key></Contents></ListBucketResult>")
                } else {
                    xml("<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>root/snap/memory.zst</Key></Contents></ListBucketResult>")
                },
            ))
        });
        assert_eq!(
            store(&fake).list("bucket", "root/snap/").await.unwrap(),
            vec!["root/snap/manifest.json", "root/snap/memory.zst"]
        );
    }

    #[tokio::test]
    async fn s3_delete_prefix_bucket_errors() {
        for (name, code, status, after_list, want_err) in [
            ("missing bucket", "NoSuchBucket", 404, false, false),
            (
                "bucket removed after listing",
                "NoSuchBucket",
                404,
                true,
                false,
            ),
            ("list access denied", "AccessDenied", 403, false, true),
            ("delete access denied", "AccessDenied", 403, true, true),
            ("unspecified not found", "NotFound", 404, false, true),
            ("service failure", "InternalError", 500, false, true),
        ] {
            let fake = FakeS3::new();
            let uri = actor_uri(TEST_LOCATION, "space", "actor-1", "current").prefix();
            let (bucket, prefix) = bucket_prefix(&uri).unwrap();
            let p = prefix.clone();
            fake.set_handle(move |req| {
                if after_list && req.method == Method::GET {
                    return Some(Response::new(
                        200,
                        xml(&format!("<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>{p}durable-dir.tar</Key></Contents></ListBucketResult>")),
                    ));
                }
                Some(Response::new(status, xml(&format!("<Error><Code>{code}</Code><Message>storage failure</Message></Error>"))))
            });
            let s = store(&fake);
            let r = delete_prefix(&s, &uri).await;
            assert_eq!(r.is_err(), want_err, "{name}: {r:?}");
            if let Err(e) = r {
                assert_eq!(e.s3_code(), Some(code), "{name}");
            }
            if !after_list {
                assert!(s.list(&bucket, &prefix).await.is_err(), "{name}: list");
                let dst = tag_uri(TEST_LOCATION, "space", "tag-1").prefix();
                assert!(copy_prefix(&s, &uri, &dst).await.is_err(), "{name}: copy");
            }
        }
    }

    #[tokio::test]
    async fn s3_delete_of_a_missing_object_succeeds() {
        for (name, status, body, want_err) in [
            ("deleted", 204, "", false),
            ("already gone", 404, "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>", false),
            ("access denied", 403, "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>", true),
        ] {
            let fake = FakeS3::new();
            fake.set_handle(move |_| Some(Response::new(status, xml(body))));
            let r = store(&fake).delete("bucket", "root/snap/memory.zst").await;
            assert_eq!(r.is_err(), want_err, "{name}: {r:?}");
        }
    }

    #[tokio::test]
    async fn s3_copy_small_object() {
        let fake = FakeS3::new();
        store(&fake)
            .copy(
                "src-bucket",
                "root/snap/memory.zst",
                "dst-bucket",
                "root/tag-v1/memory.zst",
            )
            .await
            .unwrap();
        assert_eq!(
            fake.requests(),
            vec![
                S3Request {
                    method: Method::HEAD,
                    path: "/src-bucket/root/snap/memory.zst".into(),
                    query: String::new(),
                    copy_source: String::new(),
                    copy_range: String::new(),
                },
                S3Request {
                    method: Method::PUT,
                    path: "/dst-bucket/root/tag-v1/memory.zst".into(),
                    query: "x-id=CopyObject".into(),
                    copy_source: "src-bucket/root/snap/memory.zst".into(),
                    copy_range: String::new(),
                },
            ]
        );
    }

    fn big_head(
        fake: &FakeS3,
        also: impl Fn(&crate::http::Request) -> Option<Response> + Send + Sync + 'static,
    ) {
        fake.set_handle(move |req| {
            if req.method == Method::HEAD {
                return Some(
                    Response::new(200, "")
                        .with_header("content-length", ((5i64 << 30) + 1).to_string()),
                );
            }
            also(req)
        });
    }

    #[tokio::test]
    async fn s3_copy_multipart() {
        let fake = FakeS3::new();
        big_head(&fake, |_| None);
        store(&fake)
            .copy(
                "bucket",
                "root/snap/memory.zst",
                "bucket",
                "root/tag-v1/memory.zst",
            )
            .await
            .unwrap();
        let mut ranges = Vec::new();
        let (mut started, mut completed, mut aborted) = (0, 0, 0);
        for r in fake.requests() {
            if r.query.contains("partNumber") {
                ranges.push(r.copy_range);
            } else if r.query.contains("uploads") {
                started += 1;
            } else if r.method == Method::POST {
                completed += 1;
            } else if r.method == Method::DELETE {
                aborted += 1;
            }
        }
        let mut want = vec![
            "bytes=0-1073741823",
            "bytes=1073741824-2147483647",
            "bytes=2147483648-3221225471",
            "bytes=3221225472-4294967295",
            "bytes=4294967296-5368709119",
            "bytes=5368709120-5368709120",
        ];
        ranges.sort();
        want.sort();
        assert_eq!(ranges, want);
        assert_eq!((started, completed, aborted), (1, 1, 0));
    }

    fn aborted(fake: &FakeS3) -> bool {
        fake.requests()
            .iter()
            .any(|r| r.method == Method::DELETE && r.query.contains("uploadId=upload-1"))
    }

    #[tokio::test]
    async fn s3_copy_multipart_aborts_on_complete_failure() {
        let fake = FakeS3::new();
        big_head(&fake, |req| {
            (req.method == Method::POST && req.query("uploadId").is_some()).then(|| {
                Response::new(500, xml("<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>"))
            })
        });
        assert!(store(&fake)
            .copy(
                "bucket",
                "root/snap/memory.zst",
                "bucket",
                "root/tag-v1/memory.zst"
            )
            .await
            .is_err());
        assert!(aborted(&fake), "requests = {:?}", fake.requests());
    }

    #[tokio::test]
    async fn s3_copy_multipart_aborts() {
        let fake = FakeS3::new();
        big_head(&fake, |req| {
            (req.query("partNumber").as_deref() == Some("3")).then(|| {
                Response::new(500, xml("<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>"))
            })
        });
        assert!(store(&fake)
            .copy(
                "bucket",
                "root/snap/memory.zst",
                "bucket",
                "root/tag-v1/memory.zst"
            )
            .await
            .is_err());
        assert!(aborted(&fake), "requests = {:?}", fake.requests());
    }
}
