//! Whole-object helpers: fetching small objects, opening streams, and
//! uploading or restoring a snapshot file with compression.

use std::fs::File;
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::AsyncReadExt;
use tokio_util::io::SyncIoBridge;

use super::codec::{decode_content, DecodeTarget};
use super::uploader::{Source, Uploader, DEFAULT_UPLOAD_MEMORY};
use super::{parse_object_url, ObjectReader, ObjectStorage};
use crate::error::{Error, Result};

/// Reads the whole object at `url` into memory.
pub async fn fetch_object(client: &dyn ObjectStorage, url: &str) -> Result<Vec<u8>> {
    let (bucket, object) = parse_object_url(url).map_err(|e| e.context("while parsing url"))?;
    let mut rc = client.get_object(&bucket, &object).await.map_err(|e| {
        e.context(format!(
            "while getting object bucket={bucket:?} object={object:?}"
        ))
    })?;
    let mut content = Vec::new();
    let read = rc.read_to_end(&mut content).await;
    let closed = rc.close();
    read.map_err(|e| Error::from(e).context("while reading all content"))?;
    closed?;
    Ok(content)
}

/// Opens the object at `url` as a stream, without buffering it.
pub async fn open_object(client: &dyn ObjectStorage, url: &str) -> Result<ObjectReader> {
    let (bucket, object) = parse_object_url(url).map_err(|e| e.context("while parsing url"))?;
    client.get_object(&bucket, &object).await.map_err(|e| {
        e.context(format!(
            "while getting object bucket={bucket:?} object={object:?}"
        ))
    })
}

/// Compresses the file at `path` and uploads it to `url`, on a budget of its
/// own.
pub async fn send_path_with_zstd(
    client: Arc<dyn ObjectStorage>,
    url: &str,
    path: &Path,
) -> Result<()> {
    let f = File::open(path)
        .map_err(|e| Error::from(e).context(format!("while opening {}", path.display())))?;
    let budget = DEFAULT_UPLOAD_MEMORY.max(
        super::uploader::upload_buffer_memory(client.backend())
            + super::uploader::PARALLEL_WORKER_MEMORY,
    );
    let u = Uploader::new(client, budget)?;
    u.send_zstd(url, Source::File(f))
        .await
        .map_err(|e| e.context("while sending"))
}

/// Downloads and decompresses `url` into a new file at `path`, mode 0600.
pub async fn fetch_to_path_with_zstd(
    client: &dyn ObjectStorage,
    url: &str,
    path: &Path,
) -> Result<()> {
    let f = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|e| Error::from(e).context(format!("while opening {}", path.display())))?;
    fetch_file_with_zstd(client, url, &f).await
}

/// Downloads and decompresses `url` into the open `file`, which the caller
/// keeps. The file's mode becomes 0600.
pub async fn fetch_file_with_zstd(
    client: &dyn ObjectStorage,
    url: &str,
    file: &File,
) -> Result<()> {
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|e| Error::from(e).context("while restricting the file to 0600"))?;
    let f = file.try_clone()?;
    fetch_with_zstd(client, url, Output::File(f))
        .await
        .map_err(|e| e.context(format!("while fetching {url:?}")))
}

/// Where [`fetch_with_zstd`] writes.
pub enum Output {
    /// A file, written sparse.
    File(File),
    /// Any writer.
    Writer(Box<dyn Write + Send>),
}

/// Downloads `url` and decompresses it into `out`. A failure to close the
/// object is reported once the content decoded.
pub async fn fetch_with_zstd(client: &dyn ObjectStorage, url: &str, out: Output) -> Result<()> {
    let (bucket, object) = parse_object_url(url).map_err(|e| e.context("while parsing URL"))?;
    let rc = client
        .get_object(&bucket, &object)
        .await
        .map_err(|e| e.context("while getting object"))?;
    let started = Instant::now();
    let (decoded, mut rc) = tokio::task::spawn_blocking(move || {
        let mut bridge = SyncIoBridge::new(rc);
        let res = match out {
            Output::File(mut f) => decode_content(DecodeTarget::File(&mut f), &mut bridge),
            Output::Writer(mut w) => decode_content(DecodeTarget::Writer(&mut *w), &mut bridge),
        };
        (res, bridge.into_inner())
    })
    .await
    .map_err(|e| Error::other(format!("decompression task failed: {e}")))?;
    let closed = rc.close();
    let res = decoded.map_err(Error::from)?;
    closed.map_err(Error::from)?;
    tracing::info!(
        sparse = res.sparse,
        logical_bytes = res.logical_bytes,
        written_bytes = res.written_bytes,
        took_ms = started.elapsed().as_millis() as u64,
        "Decompressed zstd download"
    );
    Ok(())
}

impl From<Box<dyn io::Read + Send>> for Source {
    fn from(r: Box<dyn io::Read + Send>) -> Self {
        Source::Reader(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::codec::{write_content, Content};
    use crate::storage::sparsezstd::SPARSE_MAGIC;
    use crate::storage::{ObjectRead, UploadBody};
    use crate::testing::MemObjects;
    use async_trait::async_trait;
    use std::os::unix::fs::FileExt;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio_util::sync::CancellationToken;

    struct CloseError(crate::storage::BytesReader);

    impl AsyncRead for CloseError {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl ObjectRead for CloseError {
        fn close(&mut self) -> io::Result<()> {
            Err(io::Error::other("object reader close failed"))
        }
    }

    struct CloseErrorStore(MemObjects);

    #[async_trait]
    impl ObjectStorage for CloseErrorStore {
        async fn get_object(&self, bucket: &str, object: &str) -> Result<ObjectReader> {
            let data = self
                .0
                .get(bucket, object)
                .ok_or_else(|| Error::not_found("absent"))?;
            Ok(Box::new(CloseError(crate::storage::BytesReader::new(data))))
        }
        async fn put_object(&self, bucket: &str, object: &str, body: UploadBody) -> Result<()> {
            self.0.put_object(bucket, object, body).await
        }
    }

    #[tokio::test]
    async fn fetch_with_zstd_close_error() {
        let mut compressed = Vec::new();
        let mut content = Content::Stream(Box::new(io::Cursor::new(b"snapshot payload".to_vec())));
        write_content(&CancellationToken::new(), &mut compressed, &mut content, 2).unwrap();
        let truncated = compressed[..compressed.len() - 1].to_vec();
        for (name, data, want) in [
            (
                "successful decode",
                compressed.clone(),
                "object reader close failed",
            ),
            ("truncated object", truncated, ""),
        ] {
            let store = CloseErrorStore(MemObjects::new());
            store.0.insert("bucket", "snapshot", data);
            let e = fetch_with_zstd(
                &store,
                "gs://bucket/snapshot",
                Output::Writer(Box::new(io::sink())),
            )
            .await
            .unwrap_err();
            if want.is_empty() {
                // A truncated object fails the decode, not the close.
                assert!(!e.to_string().contains("close failed"), "{name}: {e}");
            } else {
                assert!(e.to_string().contains(want), "{name}: {e}");
            }
        }
    }

    fn sparse_fixture(dir: &Path, size: usize) -> (std::path::PathBuf, Vec<u8>) {
        let mut want = vec![0u8; size];
        let regions = [(0usize, 4096usize), (2 << 20, 70000), (size - 9000, 5000)];
        for (start, n) in regions {
            for (i, b) in want.iter_mut().enumerate().skip(start).take(n) {
                *b = ((i * 7) % 251 + 1) as u8;
            }
        }
        let path = dir.join("memory-ranges");
        let f = File::create(&path).unwrap();
        f.set_len(size as u64).unwrap();
        for (start, n) in regions {
            f.write_all_at(&want[start..start + n], start as u64)
                .unwrap();
        }
        (path, want)
    }

    async fn sparse_round_trip(streaming: bool) {
        let size = 8usize << 20;
        let dir = tempfile::tempdir().unwrap();
        let (src, want) = sparse_fixture(dir.path(), size);
        let store = Arc::new(MemObjects::new().streaming(streaming));
        const URL: &str = "gs://bucket/snap/memory-ranges.zstd";
        send_path_with_zstd(store.clone(), URL, &src).await.unwrap();
        let stored = store.get("bucket", "snap/memory-ranges.zstd").unwrap();
        assert!(
            stored.starts_with(SPARSE_MAGIC),
            "stored object is not sparse-extent format"
        );
        assert!(
            stored.len() < size / 2,
            "stored {} bytes; holes not skipped",
            stored.len()
        );
        let dst = dir.path().join("restored");
        fetch_to_path_with_zstd(store.as_ref(), URL, &dst)
            .await
            .unwrap();
        assert!(std::fs::read(&dst).unwrap() == want, "round-trip mismatch");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sparse_upload_download_round_trip() {
        sparse_round_trip(false).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn sparse_upload_streaming_round_trip() {
        sparse_round_trip(true).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plain_zstd_backward_compat_round_trip() {
        let want = b"hivekeep snapshot payload\n".repeat(4096);
        let store = Arc::new(MemObjects::new());
        const URL: &str = "gs://bucket/snap/config.json.zstd";
        let u = Uploader::new(store.clone(), DEFAULT_UPLOAD_MEMORY).unwrap();
        u.send_zstd(URL, Source::Reader(Box::new(io::Cursor::new(want.clone()))))
            .await
            .unwrap();
        let stored = store.get("bucket", "snap/config.json.zstd").unwrap();
        assert!(
            !stored.starts_with(SPARSE_MAGIC),
            "non-file upload used the sparse-extent format"
        );
        let dir = tempfile::tempdir().unwrap();
        let dst = dir.path().join("config.json");
        fetch_to_path_with_zstd(store.as_ref(), URL, &dst)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&dst).unwrap(), want);
    }
}
