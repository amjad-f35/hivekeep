//! Staging sandbox assets: through the object store, the anonymous
//! fallback, verification, credential hygiene, gVisor release trees and
//! the asset cache's startup checks.

use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

use tonic::{Code, Status};

use super::*;
use crate::assets::{extract_tar_archive, AssetEntry, SandboxAssetsRecord, GVISOR_ASSET_NAME};
use crate::objectstore::ObjectStore;
use crate::sandbox_test_support::*;

const BUCKET: &str = "cluster-bucket";

fn entry(url: &str, sha: &str) -> AssetEntry {
    AssetEntry {
        url: url.into(),
        sha256: sha.into(),
    }
}

/// A node whose store serves `scheme` assets from `bucket`, with an
/// anonymous reader of another bucket, `anon`.
struct Fixture {
    _d: tempfile::TempDir,
    root: std::path::PathBuf,
    bucket: Arc<Bucket>,
    anon: Arc<Bucket>,
    plugin: Arc<AssetStore>,
}

impl Fixture {
    fn new(scheme: &str) -> Fixture {
        let d = tempfile::tempdir().unwrap();
        let bucket = Arc::new(Bucket::default());
        let plugin = AssetStore::serving(scheme, bucket.clone());
        Fixture {
            root: d.path().to_path_buf(),
            _d: d,
            bucket,
            anon: Arc::new(Bucket::default()),
            plugin,
        }
    }

    fn with_store(&self, store: Option<Arc<dyn ObjectStore>>) -> AssetStager {
        stager_at(&self.root, store, Some(self.anon.clone()))
    }

    fn stager(&self) -> AssetStager {
        self.with_store(Some(self.plugin.clone()))
    }

    /// Puts `content` in the store's bucket and returns its sha256.
    fn put(&self, object: &str, content: &[u8]) -> String {
        self.bucket.put(&format!("{BUCKET}/{object}"), content);
        digest(content)
    }

    async fn fetch(&self, s: &AssetStager, e: AssetEntry) -> Result<std::path::PathBuf, Status> {
        stage_test_asset(s, &Ctx::background(), &self.root, e).await
    }

    fn assert_not_cached(&self) {
        assert_eq!(
            cached_assets(&self.root),
            0,
            "failed fetch left assets in the cache"
        );
    }
}

const CONTENT: &[u8] = b"micro-vm kernel bytes";

#[tokio::test]
async fn through_store_non_gs_scheme_goes_straight_to_the_store() {
    let f = Fixture::new("s3");
    let sum = f.put("kata-assets/vmlinux", CONTENT);
    let p = f
        .fetch(
            &f.stager(),
            entry(&format!("s3://{BUCKET}/kata-assets/vmlinux"), &sum),
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), CONTENT);
    assert_eq!(f.anon.gets(), 0, "an s3:// asset was tried anonymously");
}

#[tokio::test]
async fn through_store_gs_served_by_the_store_is_not_read_anonymously() {
    let f = Fixture::new("gs");
    let sum = f.put("kata-assets/vmlinux", CONTENT);
    f.anon
        .put(&format!("{BUCKET}/kata-assets/vmlinux"), CONTENT);
    let p = f
        .fetch(
            &f.stager(),
            entry(&format!("gs://{BUCKET}/kata-assets/vmlinux"), &sum),
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), CONTENT);
    assert_eq!((f.anon.gets(), f.plugin.calls()), (0, 1));
}

#[tokio::test]
async fn through_store_gs_asset_on_an_s3_store_is_read_anonymously_without_touching_s3() {
    let f = Fixture::new("s3");
    // An S3 bucket of the same name, which could be anyone's.
    f.bucket.put("gvisor/release.tar", b"someone else's bytes");
    f.anon.put("gvisor/release.tar", CONTENT);
    let p = f
        .fetch(
            &f.stager(),
            entry("gs://gvisor/release.tar", &digest(CONTENT)),
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), CONTENT);
    assert_eq!((f.bucket.gets(), f.anon.gets()), (0, 1));
}

#[tokio::test]
async fn through_store_public_gs_asset_the_store_lacks_is_read_anonymously() {
    let f = Fixture::new("s3");
    f.anon.put("gvisor/release.tar", CONTENT);
    let p = f
        .fetch(
            &f.stager(),
            entry("gs://gvisor/release.tar", &digest(CONTENT)),
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), CONTENT);
    assert_eq!((f.anon.gets(), f.plugin.calls()), (1, 1));
}

#[tokio::test]
async fn through_store_cache_hit() {
    let f = Fixture::new("s3");
    let sum = f.put("kata-assets/vmlinux", CONTENT);
    let s = f.stager();
    let e = entry(&format!("s3://{BUCKET}/kata-assets/vmlinux"), &sum);
    let first = f.fetch(&s, e.clone()).await.unwrap();
    let second = f.fetch(&s, e).await.unwrap();
    assert_eq!(
        f.plugin.calls(),
        1,
        "two fetches of one asset called the store twice"
    );
    let (a, b) = (
        std::fs::metadata(first).unwrap(),
        std::fs::metadata(second).unwrap(),
    );
    assert_eq!(
        (a.dev(), a.ino()),
        (b.dev(), b.ino()),
        "the staged paths are not one inode"
    );
}

#[tokio::test]
async fn through_store_sha256_mismatch() {
    let f = Fixture::new("s3");
    f.put("kata-assets/vmlinux", CONTENT);
    let err = f
        .fetch(
            &f.stager(),
            entry(
                &format!("s3://{BUCKET}/kata-assets/vmlinux"),
                &"a".repeat(64),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    f.assert_not_cached();
}

#[tokio::test]
async fn through_store_size_cap() {
    let f = Fixture::new("s3");
    let sum = f.put("kata-assets/vmlinux", CONTENT);
    let s = f.stager().with_max_bytes(CONTENT.len() as i64 - 1);
    let err = f
        .fetch(
            &s,
            entry(&format!("s3://{BUCKET}/kata-assets/vmlinux"), &sum),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    f.assert_not_cached();
}

/// Not NotFound: the control plane reads NotFound from Terminate as
/// "already terminated".
#[tokio::test]
async fn through_store_not_found() {
    let f = Fixture::new("s3");
    let url = format!("s3://{BUCKET}/kata-assets/missing");
    let err = f
        .fetch(&f.stager(), entry(&url, &"a".repeat(64)))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains(&url), "{err:?}");
    f.assert_not_cached();
}

#[tokio::test]
async fn through_store_snapshot_in_the_same_bucket_is_refused() {
    let f = Fixture::new("gs");
    let object = "root/hives/team-a/actors/uid1/snapshots/snap1/checkpoint.img.zstd";
    let sum = f.put(object, b"another actor's memory");
    f.anon
        .put(&format!("{BUCKET}/{object}"), b"another actor's memory");
    let err = f
        .fetch(&f.stager(), entry(&format!("gs://{BUCKET}/{object}"), &sum))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("snapshot location"), "{err:?}");
    assert_eq!(
        f.bucket.gets(),
        0,
        "the store read objects for a refused request"
    );
    assert_eq!(
        f.anon.gets(),
        0,
        "a refused snapshot file was tried anonymously"
    );
    f.assert_not_cached();
}

#[tokio::test]
async fn through_store_connection_error_is_unavailable() {
    let f = Fixture::new("s3");
    let s = f.with_store(Some(AssetStore::failing(Status::unavailable(
        "connection refused",
    ))));
    let err = f
        .fetch(
            &s,
            entry(
                &format!("s3://{BUCKET}/kata-assets/vmlinux"),
                &"a".repeat(64),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err:?}");
    f.assert_not_cached();
}

#[tokio::test]
async fn through_store_that_serves_no_assets() {
    let f = Fixture::new("s3");
    let s = f.with_store(Some(AssetStore::failing(Status::unimplemented(
        "unknown service",
    ))));
    let url = format!("s3://{BUCKET}/kata-assets/vmlinux");
    let err = f.fetch(&s, entry(&url, &"a".repeat(64))).await.unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    for want in [url.as_str(), "test-store", "does not serve assets"] {
        assert!(
            err.message().contains(want),
            "{err:?} does not name {want:?}"
        );
    }
}

const GVISOR: &[u8] = b"gvisor release bytes";
const OBJECT: &str = "gvisor/releases/runsc";
const GS_URL: &str = "gs://gvisor/releases/runsc";
const PLUGIN_MSG: &str = "plugin text SECRET-SIG";

/// Pinned here, not read from the production list, so dropping a code
/// from it fails.
const FALLBACK_CODES: [Code; 5] = [
    Code::NotFound,
    Code::InvalidArgument,
    Code::Unimplemented,
    Code::Unavailable,
    Code::Internal,
];

#[tokio::test]
async fn anonymous_fallback_codes_fall_back() {
    for code in FALLBACK_CODES {
        let logs = Logs::capture();
        let f = Fixture::new("s3");
        f.anon.put(OBJECT, GVISOR);
        // A partial write the store leaves behind is replaced.
        let s = f.with_store(Some(AssetStore::new(AssetMode::Fixed {
            data: Some(b"partial plugin write, longer than the asset".to_vec()),
            err: Some(Status::new(code, PLUGIN_MSG)),
        })));
        let p = f.fetch(&s, entry(GS_URL, &digest(GVISOR))).await.unwrap();
        assert_eq!(std::fs::read(p).unwrap(), GVISOR, "{code:?}");
        assert_eq!(f.anon.gets(), 1, "{code:?}");
        let text = logs.text();
        assert!(
            !text.contains("SECRET"),
            "{code:?}: logs carry the store's text:\n{text}"
        );
        let line = logs
            .last("downloading it anonymously")
            .unwrap_or_else(|| panic!("{code:?}: no fallback line:\n{text}"));
        assert_eq!(
            line.field("code"),
            Some(format!("{code:?}").as_str()),
            "{code:?}"
        );
        // A store that failed, rather than declined the URL, stays visible.
        let want = if matches!(code, Code::Internal | Code::Unavailable) {
            tracing::Level::WARN
        } else {
            tracing::Level::INFO
        };
        assert_eq!(line.level, want, "{code:?}");
    }
}

#[tokio::test]
async fn anonymous_fallback_failure_reports_both() {
    for code in FALLBACK_CODES {
        let logs = Logs::capture();
        let f = Fixture::new("s3");
        let s = f.with_store(Some(AssetStore::failing(Status::new(code, PLUGIN_MSG))));
        let err = f
            .fetch(&s, entry(GS_URL, &digest(GVISOR)))
            .await
            .unwrap_err();
        assert_eq!(f.anon.gets(), 1, "{code:?}");
        let want = match code {
            Code::Unavailable => Code::Unavailable,
            Code::Internal => Code::Internal,
            _ => Code::FailedPrecondition,
        };
        assert_eq!(err.code(), want, "{code:?}: {err:?}");
        for want in [
            GS_URL,
            "object store",
            "anonymous download failed",
            "object not found",
        ] {
            assert!(
                err.message().contains(want),
                "{code:?}: {err:?} lacks {want:?}"
            );
        }
        assert!(!err.message().contains("SECRET"), "{err:?}");
        assert!(!logs.text().contains("SECRET"), "{}", logs.text());
        f.assert_not_cached();
    }
}

#[tokio::test]
async fn anonymous_fallback_only_for_gs_urls() {
    for code in FALLBACK_CODES {
        for u in [
            format!("s3://{OBJECT}"),
            format!("https://storage.example/{OBJECT}"),
        ] {
            let f = Fixture::new("s3");
            f.anon.put(OBJECT, GVISOR);
            let s = f.with_store(Some(AssetStore::failing(Status::new(code, PLUGIN_MSG))));
            assert!(
                f.fetch(&s, entry(&u, &digest(GVISOR))).await.is_err(),
                "{code:?} {u}"
            );
            assert_eq!(f.anon.gets(), 0, "{code:?} {u}");
            f.assert_not_cached();
        }
    }
}

#[tokio::test]
async fn anonymous_fallback_not_after_a_refusal() {
    for code in [
        Code::PermissionDenied,
        Code::FailedPrecondition,
        Code::Unknown,
        Code::ResourceExhausted,
        Code::Cancelled,
        Code::DeadlineExceeded,
    ] {
        let f = Fixture::new("s3");
        f.anon.put(OBJECT, GVISOR);
        let s = f.with_store(Some(AssetStore::failing(Status::new(code, PLUGIN_MSG))));
        let err = f
            .fetch(&s, entry(GS_URL, &digest(GVISOR)))
            .await
            .unwrap_err();
        assert_eq!(f.anon.gets(), 0, "{code:?}");
        assert!(
            !err.message().contains("anonymous") && !err.message().contains("SECRET"),
            "{code:?}: {err:?}"
        );
        f.assert_not_cached();
    }
}

/// The caller's context ends while the store is asked. The download runs
/// detached in the cache, so the store's failure still falls back to the
/// anonymous read, and the caller sees its cancellation or the result.
#[tokio::test]
async fn anonymous_fallback_survives_a_canceled_caller() {
    let f = Fixture::new("s3");
    f.anon.put(OBJECT, GVISOR);
    let caller = Ctx::background().child();
    let s = f.with_store(Some(AssetStore::new(AssetMode::Canceling {
        cancel: caller.clone(),
        err: Status::unavailable(PLUGIN_MSG),
    })));
    match stage_test_asset(&s, &caller, &f.root, entry(GS_URL, &digest(GVISOR))).await {
        Ok(_) => {}
        Err(e) => assert_eq!(e.code(), Code::Cancelled, "{e:?}"),
    }
    // A second caller joins or hits what the first started.
    f.fetch(&s, entry(GS_URL, &digest(GVISOR))).await.unwrap();
    assert_eq!(f.anon.gets(), 1);
}

#[tokio::test]
async fn anonymous_fallback_without_a_store() {
    // gs:// is read anonymously.
    let f = Fixture::new("s3");
    f.anon.put(OBJECT, GVISOR);
    let p = f
        .fetch(&f.with_store(None), entry(GS_URL, &digest(GVISOR)))
        .await
        .unwrap();
    assert_eq!(std::fs::read(p).unwrap(), GVISOR);
    assert_eq!(f.anon.gets(), 1);

    // Its failure is reported alone.
    let f = Fixture::new("s3");
    let err = f
        .fetch(&f.with_store(None), entry(GS_URL, &digest(GVISOR)))
        .await
        .unwrap_err();
    assert!(
        err.message().contains("object not found") && !err.message().contains("object store"),
        "{err:?}"
    );
    f.assert_not_cached();

    // Anything else is refused.
    let f = Fixture::new("s3");
    f.anon.put(OBJECT, GVISOR);
    let err = f
        .fetch(
            &f.with_store(None),
            entry(&format!("s3://{OBJECT}"), &digest(GVISOR)),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition, "{err:?}");
    assert!(err.message().contains("no object store"), "{err:?}");
    assert_eq!(f.anon.gets(), 0);
    f.assert_not_cached();
}

/// The store's success is not trusted: what is cached is checked again.
#[tokio::test]
async fn verifies_what_the_store_wrote() {
    for (name, data, cap) in [
        ("wrong bytes", &b"a snapshot's bytes"[..], 1 << 20),
        ("over the cap", CONTENT, CONTENT.len() as i64 - 1),
    ] {
        let f = Fixture::new("s3");
        let s = f
            .with_store(Some(AssetStore::new(AssetMode::Fixed {
                data: Some(data.to_vec()),
                err: None,
            })))
            .with_max_bytes(cap);
        let res = f
            .fetch(
                &s,
                entry(
                    &format!("s3://{BUCKET}/kata-assets/vmlinux"),
                    &digest(CONTENT),
                ),
            )
            .await;
        assert!(res.is_err(), "{name}: accepted unchecked bytes");
        f.assert_not_cached();
    }
}

/// Neither a signed URL nor the store's status text reaches the logs or
/// the returned error, which becomes the actor's crash message. Some
/// texts carry credentials that are no URL.
#[tokio::test]
async fn keeps_credentials_out_of_errors() {
    const SECRET: &str = "SECRET-SIG";
    let signed = format!("https://usr0:pw-{SECRET}@acct.blob.example/assets/vmlinux?sig={SECRET}");
    let cases: Vec<(&str, Option<Status>, Code)> = vec![
        (
            "in-tree store rejects the URL",
            None,
            Code::FailedPrecondition,
        ),
        (
            "store quotes the URL",
            Some(Status::permission_denied(format!("denied {signed}"))),
            Code::FailedPrecondition,
        ),
        (
            "store quotes the URL in the query only",
            Some(Status::not_found(format!(
                "get https://acct.blob.example/assets/vmlinux?sig={SECRET}: 404"
            ))),
            Code::FailedPrecondition,
        ),
        (
            "store fails with credentials in its text",
            Some(Status::internal(format!("AccountKey={SECRET}"))),
            Code::Internal,
        ),
        (
            "store text is a bearer token",
            Some(Status::failed_precondition(format!(
                "Authorization: Bearer {SECRET}"
            ))),
            Code::FailedPrecondition,
        ),
        (
            "store unavailable",
            Some(Status::unavailable(format!("dial {signed}"))),
            Code::Unavailable,
        ),
    ];
    for (name, store_err, want) in cases {
        let logs = Logs::capture();
        let f = Fixture::new("s3");
        let s = match store_err {
            Some(e) => f.with_store(Some(AssetStore::failing(e))),
            None => f.stager(),
        };
        let err = f
            .fetch(&s, entry(&signed, &"a".repeat(64)))
            .await
            .unwrap_err();
        // As the RPC layer logs a failure.
        tracing::info!(code = ?err.code(), err = %err.message(), "RPC failed");
        assert_eq!(err.code(), want, "{name}: {err:?}");
        for (what, text) in [("error", err.message().to_string()), ("logs", logs.text())] {
            for leak in [SECRET, "usr0", "sig=", "Bearer", "AccountKey"] {
                assert!(
                    !text.contains(leak),
                    "{name}: {what} carries {leak:?}:\n{text}"
                );
            }
        }
        assert!(
            err.message()
                .contains("https://acct.blob.example/assets/vmlinux"),
            "{name}: {err:?} does not name the asset as scheme://host/path"
        );
    }
}

/// What the log does carry when the store fails: its code and the asset
/// as scheme://host/path.
#[tokio::test]
async fn logs_store_code_and_redacted_url() {
    let logs = Logs::capture();
    let f = Fixture::new("s3");
    let s = f.with_store(Some(AssetStore::failing(Status::permission_denied(
        "denied SECRET",
    ))));
    f.fetch(
        &s,
        entry(
            "https://acct.blob.example/assets/vmlinux?sig=SECRET",
            &"a".repeat(64),
        ),
    )
    .await
    .unwrap_err();
    let line = logs
        .last("Object store failed to fetch sandbox asset")
        .unwrap_or_else(|| panic!("no store failure line:\n{}", logs.text()));
    assert_eq!(
        line.field("url"),
        Some("https://acct.blob.example/assets/vmlinux")
    );
    assert_eq!(line.field("code"), Some("PermissionDenied"));
    assert!(
        !line.text().contains("SECRET") && !line.text().contains("denied"),
        "{}",
        line.text()
    );
}

#[test]
fn rejects_the_storage_backend_env() {
    assert!(crate::config::reject_storage_env(None).is_ok());
    for v in ["s3", "gcs", ""] {
        let err = crate::config::reject_storage_env(Some(v.into())).unwrap_err();
        assert!(err.contains("configure the object store"), "{v:?}: {err}");
    }
}

fn tarball_with(compress: fn(Vec<u8>) -> Vec<u8>) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut b = tar::Builder::new(&mut buf);
        let mut h = tar::Header::new_gnu();
        h.set_size(11);
        h.set_mode(0o644);
        b.append_data(&mut h, "test.txt", &b"hello world"[..])
            .unwrap();
        b.finish().unwrap();
    }
    compress(buf)
}

fn gzip(b: Vec<u8>) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(&b).unwrap();
    e.finish().unwrap()
}

fn zst(b: Vec<u8>) -> Vec<u8> {
    zstd::encode_all(&b[..], 0).unwrap()
}

fn plain(b: Vec<u8>) -> Vec<u8> {
    b
}

fn bz2(b: Vec<u8>) -> Vec<u8> {
    use std::io::Write;
    let mut e = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
    e.write_all(&b).unwrap();
    e.finish().unwrap()
}

#[test]
fn extract_tar_archive_formats() {
    for (url, compress) in [
        (
            "https://example.com/gvisor.tar.gz",
            gzip as fn(Vec<u8>) -> Vec<u8>,
        ),
        ("https://example.com/gvisor.tgz", gzip),
        ("https://example.com/gvisor.tar.zst", zst),
        ("https://example.com/gvisor.tar.zstd", zst),
        ("https://example.com/gvisor.tar", plain),
        ("https://example.com/gvisor.tar.bz2", bz2),
        ("https://example.com/gvisor.tbz2", bz2),
    ] {
        let d = tempfile::tempdir().unwrap();
        let archive = d.path().join("archive");
        std::fs::write(&archive, tarball_with(compress)).unwrap();
        let dest = d.path().join("dest");
        extract_tar_archive(&Ctx::background(), &archive, url, &dest)
            .unwrap_or_else(|e| panic!("{url}: {e:?}"));
        assert_eq!(
            std::fs::read(dest.join("test.txt")).unwrap(),
            b"hello world",
            "{url}"
        );
    }
}

#[test]
fn extract_tar_archive_rejects_an_unsupported_format() {
    let d = tempfile::tempdir().unwrap();
    let archive = d.path().join("archive");
    std::fs::write(&archive, b"zip bytes").unwrap();
    let err = extract_tar_archive(
        &Ctx::background(),
        &archive,
        "https://example.com/gvisor.zip",
        &d.path().join("dest"),
    )
    .unwrap_err();
    assert!(
        err.message().contains("unsupported archive format"),
        "{err:?}"
    );
}

/// A .tar.bz2 release is a supported format: bad bzip2 data fails as a
/// read error, not as an unsupported format.
#[test]
fn extract_tar_archive_reads_bzip2() {
    let d = tempfile::tempdir().unwrap();
    let archive = d.path().join("archive");
    std::fs::write(&archive, b"invalid bzip2 data").unwrap();
    let err = extract_tar_archive(
        &Ctx::background(),
        &archive,
        "https://example.com/gvisor.tar.bz2",
        &d.path().join("dest"),
    )
    .unwrap_err();
    assert!(
        !err.message().contains("unsupported archive format"),
        "{err:?}"
    );
}

fn gvisor_rec(url: &str, tarball: &[u8]) -> SandboxAssetsRecord {
    gvisor_record(url, tarball)
}

#[tokio::test]
async fn stage_gvisor_release() {
    let tarball = gvisor_release_tarball("runsc bytes");
    let d = tempfile::tempdir().unwrap();
    let root = d.path();
    let anon = Bucket::with(&[("gvisor/release.tar", &tarball)]);
    let s = stager_at(root, None, Some(anon.clone()));
    let rec = gvisor_rec("gs://gvisor/release.tar", &tarball);

    let a1 = root.join("actor-1");
    std::fs::create_dir_all(&a1).unwrap();
    let runsc1 = s.stage(&Ctx::background(), &rec, &a1).await.unwrap()[GVISOR_ASSET_NAME].clone();
    let dst1 = a1.join(GVISOR_ASSET_NAME);
    assert_eq!(Path::new(&runsc1), dst1.join("runsc"));
    for (rel, want) in [
        ("runsc", "runsc bytes"),
        ("gvisor-bin/containerd-shim", "shim bytes"),
    ] {
        assert_eq!(
            std::fs::read_to_string(dst1.join(rel)).unwrap(),
            want,
            "{rel}"
        );
        let mode = std::fs::metadata(dst1.join(rel))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o555, "{rel}");
    }
    assert!(
        std::fs::symlink_metadata(dst1.join(".gvisor-release-download")).is_err(),
        "the downloaded tarball was staged with the tree"
    );

    let a2 = root.join("actor-2");
    std::fs::create_dir_all(&a2).unwrap();
    let runsc2 = s.stage(&Ctx::background(), &rec, &a2).await.unwrap()[GVISOR_ASSET_NAME].clone();
    let (m1, m2) = (
        std::fs::metadata(&runsc1).unwrap(),
        std::fs::metadata(&runsc2).unwrap(),
    );
    assert_eq!(
        (m1.dev(), m1.ino()),
        (m2.dev(), m2.ino()),
        "the actors' runsc are not one inode"
    );
    assert_eq!(anon.gets(), 1, "release downloaded more than once");
    assert_eq!(cached_assets(root), 1);

    // Onto a staged tree.
    let again = s.stage(&Ctx::background(), &rec, &a1).await.unwrap()[GVISOR_ASSET_NAME].clone();
    assert_eq!(again, runsc1);

    // A tarball without runsc.
    let mut no_runsc = Vec::new();
    {
        let mut b = tar::Builder::new(&mut no_runsc);
        let mut h = tar::Header::new_gnu();
        h.set_size(2);
        h.set_mode(0o644);
        b.append_data(&mut h, "README", &b"hi"[..]).unwrap();
        b.finish().unwrap();
    }
    let s = stager_at(
        root,
        None,
        Some(Bucket::with(&[("gvisor/bad.tar", &no_runsc)])),
    );
    let a3 = root.join("actor-3");
    std::fs::create_dir_all(&a3).unwrap();
    let err = s
        .stage(
            &Ctx::background(),
            &gvisor_rec("gs://gvisor/bad.tar", &no_runsc),
            &a3,
        )
        .await
        .unwrap_err();
    assert!(err.message().contains("no runsc binary"), "{err:?}");
    assert_eq!(cached_assets(root), 1, "the rejected tarball was cached");
}

#[tokio::test]
async fn ensure_concurrent_same_actor() {
    let tarball = gvisor_release_tarball("runsc bytes");
    let kernel = b"kernel bytes";
    let d = tempfile::tempdir().unwrap();
    let root = d.path();
    let anon = Bucket::with(&[("gvisor/release.tar", &tarball), ("assets/vmlinux", kernel)]);
    let s = stager_at(root, None, Some(anon));
    let mut rec = gvisor_rec("gs://gvisor/release.tar", &tarball);
    rec.assets.insert(
        "kata-kernel".into(),
        entry("gs://assets/vmlinux", &digest(kernel)),
    );
    let dst = root.join("actor/sandbox-assets");
    let ctx = Ctx::background();
    let results = futures::future::join_all((0..8).map(|_| s.ensure(&ctx, &rec, &dst))).await;
    for (i, r) in results.into_iter().enumerate() {
        let paths = r.unwrap_or_else(|e| panic!("call {i}: {e:?}"));
        assert_eq!(
            Path::new(&paths[GVISOR_ASSET_NAME]),
            dst.join("gvisor/runsc"),
            "call {i}"
        );
        assert_eq!(
            Path::new(&paths["kata-kernel"]),
            dst.join("kata-kernel"),
            "call {i}"
        );
    }
    assert!(
        dst.join("gvisor/gvisor-bin/containerd-shim").exists(),
        "staged tree incomplete"
    );
    assert_eq!(
        std::fs::read_dir(&dst).unwrap().count(),
        2,
        "want the 2 assets only"
    );
}

/// A fetch error that reads like "file exists" is not a concurrent stage.
#[tokio::test]
async fn fetch_error_mentioning_exists_is_not_staged() {
    let d = tempfile::tempdir().unwrap();
    let root = d.path();
    let anon = Arc::new(Bucket::default());
    *anon.err.lock() = Some("object already there: File exists (os error 17)".into());
    let s = stager_at(root, None, Some(anon));
    let dir = root.join("actor");
    std::fs::create_dir_all(&dir).unwrap();
    let mut rec = SandboxAssetsRecord::default();
    rec.assets
        .insert("runsc".into(), entry("gs://bucket/runsc", &"d".repeat(64)));
    assert!(s.stage(&Ctx::background(), &rec, &dir).await.is_err());
    assert!(
        std::fs::symlink_metadata(dir.join("runsc")).is_err(),
        "destination exists"
    );
}

#[tokio::test]
async fn ensure_rejects_bad_asset_names() {
    let content = b"kernel bytes";
    for name in [
        ".",
        "..",
        "",
        "kata/kernel",
        "/kata-kernel",
        "./kata-kernel",
    ] {
        let d = tempfile::tempdir().unwrap();
        let anon = Bucket::with(&[("assets/vmlinux", content)]);
        let s = stager_at(d.path(), None, Some(anon.clone()));
        let dst = d.path().join("actor/sandbox-assets");
        let mut rec = SandboxAssetsRecord {
            sandbox_class: "microvm".into(),
            ..Default::default()
        };
        rec.assets
            .insert(name.into(), entry("gs://assets/vmlinux", &digest(content)));
        let err = s.ensure(&Ctx::background(), &rec, &dst).await.unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{name:?}: {err:?}");
        assert_eq!(anon.gets(), 0, "{name:?}: fetched for a refused name");
        let left = std::fs::read_dir(&dst).map(|r| r.count()).unwrap_or(0);
        assert_eq!(left, 0, "{name:?} left entries in the assets dir");
    }
    // A plain name is staged where it says.
    let d = tempfile::tempdir().unwrap();
    let s = stager_at(
        d.path(),
        None,
        Some(Bucket::with(&[("assets/vmlinux", content)])),
    );
    let dst = d.path().join("actor/sandbox-assets");
    let mut rec = SandboxAssetsRecord {
        sandbox_class: "microvm".into(),
        ..Default::default()
    };
    rec.assets.insert(
        "kata-kernel".into(),
        entry("gs://assets/vmlinux", &digest(content)),
    );
    let paths = s.ensure(&Ctx::background(), &rec, &dst).await.unwrap();
    assert_eq!(Path::new(&paths["kata-kernel"]), dst.join("kata-kernel"));
    assert!(std::fs::metadata(&paths["kata-kernel"]).unwrap().is_file());
}

fn args(extra: &[&str]) -> Result<crate::config::Args, String> {
    use clap::Parser;
    let mut v = vec!["hivekeep-keeper"];
    v.extend_from_slice(extra);
    let a = crate::config::Args::try_parse_from(v).map_err(|e| e.to_string())?;
    a.validate()?;
    Ok(a)
}

/// A zero min age is fine; a zero fetch timeout is not. Negative
/// durations do not parse, so they cannot invert the eviction veto.
#[test]
fn validates_asset_cache_flags() {
    assert!(args(&[]).is_ok(), "defaults");
    assert!(args(&[
        "--sandbox-asset-cache-min-age",
        "0s",
        "--sandbox-asset-fetch-timeout",
        "1m"
    ])
    .is_ok());
    assert!(args(&["--sandbox-asset-cache-min-age=-1s"]).is_err());
    assert!(args(&[
        "--sandbox-asset-cache-min-age",
        "1m",
        "--sandbox-asset-fetch-timeout",
        "0s"
    ])
    .is_err());
    assert!(args(&["--sandbox-asset-fetch-timeout=-1s"]).is_err());
}

#[tokio::test]
async fn open_asset_cache_resets_scratch() {
    let d = tempfile::tempdir().unwrap();
    let scratch = d.path().join("scratch");
    let leftover = scratch.join("cpu-features-123/runsc");
    std::fs::create_dir_all(leftover.parent().unwrap()).unwrap();
    std::fs::write(&leftover, b"stale link").unwrap();
    let dir = d.path().join("cache");
    open_sandbox_asset_cache(
        &dir,
        filecache::Options::default(),
        &scratch,
        &[&d.path().join("actors"), &d.path().join("workers")],
    )
    .await
    .unwrap();
    assert!(dir.join("entries").is_dir(), "cache layout not created");
    assert_eq!(
        std::fs::read_dir(&scratch).unwrap().count(),
        0,
        "scratch not emptied"
    );
}

#[test]
fn can_hard_link_on_one_filesystem() {
    let d = tempfile::tempdir().unwrap();
    let (cache, actors) = (d.path().join("cache"), d.path().join("actors"));
    std::fs::create_dir_all(&cache).unwrap();
    can_hard_link(&cache, &actors).unwrap();
    for dir in [&cache, &actors] {
        assert_eq!(
            std::fs::read_dir(dir).unwrap().count(),
            0,
            "{} not left empty",
            dir.display()
        );
    }
}

/// A directory on another mount than the test's temp dir.
#[cfg(target_os = "linux")]
fn other_mount() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("hk-link-probe-")
        .tempdir_in("/dev/shm")
        .expect("a directory on /dev/shm")
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "needs /dev/shm on another mount than the temp dir"]
fn can_hard_link_rejects_another_mount() {
    let d = tempfile::tempdir().unwrap();
    let cache = d.path().join("cache");
    std::fs::create_dir_all(&cache).unwrap();
    let other = other_mount();
    let actors = other.path().join("actors");
    let err = can_hard_link(&cache, &actors).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::CrossesDevices, "{err}");
    for dir in [&cache, &actors] {
        assert_eq!(
            std::fs::read_dir(dir).unwrap().count(),
            0,
            "{} not left empty",
            dir.display()
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "needs /dev/shm on another mount than the temp dir"]
async fn open_asset_cache_rejects_another_mount() {
    for which in ["actors", "workers"] {
        let d = tempfile::tempdir().unwrap();
        let other = other_mount();
        let (actors, workers) = if which == "actors" {
            (other.path().join("actors"), d.path().join("workers"))
        } else {
            (d.path().join("actors"), other.path().join("workers"))
        };
        let err = open_sandbox_asset_cache(
            &d.path().join("cache"),
            filecache::Options::default(),
            &d.path().join("scratch"),
            &[&actors, &workers],
        )
        .await
        .err()
        .expect("opening across mounts must fail");
        assert!(err.contains("different mounts"), "{which}: {err}");
    }
}
