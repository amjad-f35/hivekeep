//! The control plane's [`Store`] on Google Cloud Storage.

use std::time::Duration;

use async_trait::async_trait;
use http::Method;
use serde::Deserialize;

use super::Store;
use crate::error::{Error, ErrorKind, Result};
use crate::gcs::GcsConn;
use crate::retry::{retry, Backoff, RetryPolicy};

/// How transient errors (408, 429, 5xx) are retried, such as the 429s GCS
/// returns while it scales a cold bucket. Every caller waits on these, and
/// suspend holds the actor's lease meanwhile, so a call gives up after
/// [`GCS_MAX_ATTEMPTS`] (at most 12.75 s of sleep).
pub const GCS_BACKOFF: Backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(5));
/// The most attempts per call.
pub const GCS_MAX_ATTEMPTS: u32 = 7;

/// A [`Store`] on GCS. Retrying always is safe here: a repeated delete gets a
/// 404, which counts as success, and a repeated copy rewrites the same
/// object.
pub struct GcsStore {
    conn: GcsConn,
    policy: RetryPolicy,
}

impl GcsStore {
    /// A store over `conn`.
    pub fn new(conn: GcsConn) -> Self {
        GcsStore {
            conn,
            policy: RetryPolicy {
                backoff: GCS_BACKOFF,
                max_attempts: Some(GCS_MAX_ATTEMPTS),
            },
        }
    }

    /// The same store backing off by `backoff`, for tests.
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.policy.backoff = backoff;
        self
    }
}

#[derive(Deserialize)]
struct ListPage {
    #[serde(default)]
    items: Vec<ListItem>,
    #[serde(default, rename = "nextPageToken")]
    next_page_token: String,
}

#[derive(Deserialize)]
struct ListItem {
    name: String,
}

#[async_trait]
impl Store for GcsStore {
    async fn list(&self, bucket: &str, prefix: &str) -> Result<Vec<String>> {
        let mut objects = Vec::new();
        let mut token = String::new();
        loop {
            let mut url = self.conn.list_url(bucket)?;
            {
                let mut q = url.query_pairs_mut();
                q.append_pair("prefix", prefix);
                // Only names are used; asking for less makes GCS send less.
                q.append_pair("fields", "items(name),nextPageToken");
                if !token.is_empty() {
                    q.append_pair("pageToken", &token);
                }
            }
            let body = retry(self.policy, || {
                let req = Request::new(Method::GET, url.clone());
                async move { self.conn.send_read(req).await }
            })
            .await
            .map_err(|e| {
                let e = if e.http_status() == Some(404) {
                    Error::new(ErrorKind::BucketNotFound, "storage: bucket doesn't exist")
                } else {
                    e
                };
                e.context(format!(
                    "while listing objects under gs://{bucket}/{prefix}"
                ))
            })?;
            let page: ListPage = serde_json::from_slice(&body)
                .map_err(|e| Error::other(format!("listing response: {e}")))?;
            objects.extend(page.items.into_iter().map(|i| i.name));
            if page.next_page_token.is_empty() {
                return Ok(objects);
            }
            token = page.next_page_token;
        }
    }

    async fn delete(&self, bucket: &str, object: &str) -> Result<()> {
        match self.conn.delete(self.policy, bucket, object).await {
            Ok(()) => Ok(()),
            // An object already gone is the state this asks for.
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(e.context(format!("while deleting gs://{bucket}/{object}"))),
        }
    }

    async fn copy(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
    ) -> Result<()> {
        self.conn
            .rewrite(self.policy, src_bucket, src_object, dst_bucket, dst_object)
            .await
            .map_err(|e| e.context(format!("while copying gs://{src_bucket}/{src_object} to gs://{dst_bucket}/{dst_object}")))
    }
}

use crate::http::Request;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gcp::Anonymous;
    use crate::gcs::fake::FakeGcs;
    use crate::http::{transport_fn, Response};
    use crate::store::tests::{actor_uri, tag_uri, TEST_LOCATION};
    use crate::store::{bucket_prefix, copy_prefix, delete_prefix};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn fast(conn: GcsConn) -> GcsStore {
        GcsStore::new(conn).with_backoff(Backoff::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
        ))
    }

    /// A store over a transport answering scripted statuses per key before
    /// serving the fake.
    fn scripted(
        fake: &FakeGcs,
        script: HashMap<String, Vec<u16>>,
    ) -> (GcsStore, Arc<parking_lot::Mutex<HashMap<String, usize>>>) {
        let script = Arc::new(parking_lot::Mutex::new(script));
        let attempts: Arc<parking_lot::Mutex<HashMap<String, usize>>> = Arc::default();
        let inner = fake.transport();
        let a = attempts.clone();
        let t = transport_fn(move |req: Request| {
            let path = percent_encoding::percent_decode_str(req.url.path())
                .decode_utf8_lossy()
                .into_owned();
            let key = if req.method == Method::GET {
                format!("LIST {}", req.query("prefix").unwrap_or_default())
            } else if req.method == Method::DELETE {
                format!(
                    "DELETE {}",
                    path.rsplit_once("/o/").map(|(_, o)| o).unwrap_or("")
                )
            } else {
                format!(
                    "COPY {}",
                    path.rsplit_once("/o/").map(|(_, o)| o).unwrap_or("")
                )
            };
            *a.lock().entry(key.clone()).or_default() += 1;
            let next = script
                .lock()
                .get_mut(&key)
                .and_then(|v| (!v.is_empty()).then(|| v.remove(0)));
            let inner = inner.clone();
            Box::pin(async move {
                match next {
                    Some(code) => Ok(Response::new(
                        code,
                        format!(r#"{{"error":{{"code":{code},"message":"x"}}}}"#),
                    )),
                    None => inner.send(req).await,
                }
            })
        });
        (
            fast(GcsConn::new(
                t,
                Arc::new(Anonymous),
                url::Url::parse(crate::gcs::fake::BASE).unwrap(),
            )),
            attempts,
        )
    }

    #[tokio::test]
    async fn gcs_delete_retries() {
        for (name, statuses, want_code, want_attempts) in [
            ("429s, then success", vec![429, 429], None, 3),
            ("503, then 404", vec![503, 404], None, 2),
            ("gives up after 7 attempts", vec![429; 10], Some(429), 7),
            ("403 is not retried", vec![403], Some(403), 1),
        ] {
            let fake = FakeGcs::new();
            fake.insert("bucket", "snap/file", b"x".to_vec());
            let (store, attempts) = scripted(
                &fake,
                HashMap::from([("DELETE snap/file".to_string(), statuses)]),
            );
            let r =
                tokio::time::timeout(Duration::from_secs(20), store.delete("bucket", "snap/file"))
                    .await
                    .expect("delete ran until its deadline");
            match want_code {
                None => r.unwrap_or_else(|e| panic!("{name}: {e}")),
                Some(c) => assert_eq!(r.unwrap_err().http_status(), Some(c), "{name}"),
            }
            assert_eq!(attempts.lock()["DELETE snap/file"], want_attempts, "{name}");
        }
    }

    #[tokio::test]
    async fn gcs_delete_prefix_retries_429() {
        let uri = actor_uri(TEST_LOCATION, "team-a", "actor-1", "snap-1");
        let (_, prefix) = bucket_prefix(&uri.prefix()).unwrap();
        let fake = FakeGcs::new();
        let mut script = HashMap::new();
        let objects: Vec<String> = (0..50).map(|i| format!("{prefix}file-{i:02}")).collect();
        for o in &objects {
            fake.insert("bucket", o, b"x".to_vec());
            script.insert(format!("DELETE {o}"), vec![429, 429]);
        }
        let (store, attempts) = scripted(&fake, script);
        delete_prefix(&store, &uri.prefix()).await.unwrap();
        for o in &objects {
            assert_eq!(attempts.lock()[&format!("DELETE {o}")], 3, "{o}");
        }
        assert!(fake.keys().is_empty());
    }

    #[tokio::test]
    async fn gcs_copy_retries_429() {
        let fake = FakeGcs::new();
        fake.insert("bucket", "snap/file", b"x".to_vec());
        let (store, attempts) = scripted(
            &fake,
            HashMap::from([("COPY tag/file".to_string(), vec![429, 429])]),
        );
        store
            .copy("bucket", "snap/file", "bucket", "tag/file")
            .await
            .unwrap();
        assert_eq!(attempts.lock()["COPY tag/file"], 3);
        assert_eq!(fake.keys(), vec!["bucket/snap/file", "bucket/tag/file"]);
    }

    #[tokio::test]
    async fn gcs_list_gives_up() {
        let fake = FakeGcs::new();
        fake.insert("bucket", "snap/file", b"x".to_vec());
        let (store, attempts) = scripted(
            &fake,
            HashMap::from([("LIST snap/".to_string(), vec![429; 10])]),
        );
        let e = tokio::time::timeout(Duration::from_secs(20), store.list("bucket", "snap/"))
            .await
            .expect("list ran until its deadline")
            .unwrap_err();
        assert_eq!(e.http_status(), Some(429));
        assert_eq!(attempts.lock()["LIST snap/"], 7);
    }

    #[tokio::test]
    async fn gcs_delete_prefix_bucket_errors() {
        for status in [404u16, 403] {
            let t = transport_fn(move |_req: Request| {
                Box::pin(async move {
                    Ok(Response::new(
                        status,
                        r#"{"error":{"message":"storage failure"}}"#,
                    ))
                })
            });
            let store = fast(GcsConn::new(
                t,
                Arc::new(Anonymous),
                url::Url::parse("http://gcs.test/").unwrap(),
            ));
            let src = actor_uri(TEST_LOCATION, "space", "actor-1", "current").prefix();
            let dst = tag_uri(TEST_LOCATION, "space", "tag-1").prefix();
            let r = delete_prefix(&store, &src).await;
            assert_eq!(r.is_err(), status != 404, "status {status}: {r:?}");
            assert!(
                copy_prefix(&store, &src, &dst).await.is_err(),
                "status {status}"
            );
        }
    }

    #[tokio::test]
    async fn gcs_store_round_trip() {
        let fake = FakeGcs::new();
        let store = fast(fake.conn());
        let prefix = "gcs_store/";
        fake.insert("b", &format!("{prefix}manifest.json"), b"manifest".to_vec());
        fake.insert("b", &format!("{prefix}memory.zst"), b"memory".to_vec());
        assert_eq!(
            store.list("b", prefix).await.unwrap(),
            vec![
                format!("{prefix}manifest.json"),
                format!("{prefix}memory.zst")
            ]
        );
        store
            .copy(
                "b",
                &format!("{prefix}memory.zst"),
                "b",
                &format!("{prefix}copy/memory.zst"),
            )
            .await
            .unwrap();
        assert_eq!(
            &fake.get("b", &format!("{prefix}copy/memory.zst")).unwrap()[..],
            b"memory"
        );
        store
            .delete("b", &format!("{prefix}memory.zst"))
            .await
            .unwrap();
        // A second delete finds nothing to do.
        store
            .delete("b", &format!("{prefix}memory.zst"))
            .await
            .unwrap();
        assert_eq!(
            store.list("b", prefix).await.unwrap(),
            vec![
                format!("{prefix}copy/memory.zst"),
                format!("{prefix}manifest.json")
            ]
        );
    }
}
