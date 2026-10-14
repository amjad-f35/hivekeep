//! Snapshot files and manifests between the node and object storage.
//!
//! A snapshot's manifest is uploaded last, in a call of its own: its
//! presence is the commit marker, and readers assume every file it lists
//! is present. A crash mid-upload leaves only orphaned objects, never a
//! manifest naming files that never landed; retries overwrite the same
//! object names.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::snapshot::SnapshotUri;
use tonic::{Code, Status};

use crate::assets::{read_snapshot_manifest, redact_url_str, SandboxAssetsRecord, MANIFEST_NAME};
use crate::atomicfile::{create_dir_all_mode, tempdir_in};
use crate::context::Ctx;
use crate::labels::fidelity_label;
use crate::objectstore::{FetchSnapshot, ObjectStore, UploadSnapshot};
use crate::paths::NodePaths;
use crate::snapshot_tracker::{self, SnapshotRef};
use crate::worker_jwt::{TransferActor, WorkerActorJwts};

/// Replaces an object store failure with one keeping only its code and the
/// redacted URI: the message may quote a signed URL or credential, and the
/// keeper's errors reach the control plane and actor crash messages.
pub fn store_call_error(op: &str, snapshot_uri: &str, err: Status) -> Status {
    let code = err.code();
    Status::new(
        code,
        format!(
            "object store {op} of {} failed ({code:?})",
            redact_url_str(snapshot_uri)
        ),
    )
}

/// Moves snapshots through an [`ObjectStore`] with the actor's JWT.
pub struct SnapshotTransfer {
    store: Arc<dyn ObjectStore>,
    jwts: Arc<WorkerActorJwts>,
    paths: Arc<NodePaths>,
    scratch_dir: PathBuf,
    boot_id: String,
}

impl SnapshotTransfer {
    /// A transfer through `store`. `boot_id` tells local copies written on
    /// this boot from older ones.
    pub fn new(
        store: Arc<dyn ObjectStore>,
        jwts: Arc<WorkerActorJwts>,
        paths: Arc<NodePaths>,
        boot_id: String,
    ) -> Self {
        let scratch_dir = paths.snapshot_scratch_dir();
        SnapshotTransfer {
            store,
            jwts,
            paths,
            scratch_dir,
            boot_id,
        }
    }

    /// The node's boot ID.
    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    /// Downloads `files` of the snapshot at `uri` into `dst_dir`.
    pub async fn fetch_snapshot_files(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &str,
        dst_dir: &Path,
        files: &[String],
    ) -> Result<(), Status> {
        if files.is_empty() {
            return Ok(());
        }
        let jwt = self.jwts.get(ctx, actor).await?;
        self.store
            .fetch_snapshot(
                ctx,
                FetchSnapshot {
                    snapshot_uri: uri.to_string(),
                    write_path: dst_dir.to_path_buf(),
                    files: files.to_vec(),
                    actor_jwt: jwt,
                },
            )
            .await
            .map_err(|e| store_call_error("fetch", uri, e))
    }

    /// Uploads `files` in `src_dir` to the snapshot at `uri`.
    pub async fn upload_snapshot_files(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &str,
        src_dir: &Path,
        files: &[String],
    ) -> Result<(), Status> {
        if files.is_empty() {
            return Ok(());
        }
        let jwt = self.jwts.get(ctx, actor).await?;
        self.store
            .upload_snapshot(
                ctx,
                UploadSnapshot {
                    snapshot_uri: uri.to_string(),
                    local_path: src_dir.to_path_buf(),
                    files: files.to_vec(),
                    actor_jwt: jwt,
                },
            )
            .await
            .map_err(|e| store_call_error("upload", uri, e))
    }

    /// A fresh scratch directory for one manifest transfer; the caller
    /// removes it.
    pub fn snapshot_scratch(&self) -> Result<PathBuf, Status> {
        create_dir_all_mode(&self.scratch_dir, 0o700)
            .map_err(|e| Status::internal(format!("while creating snapshot scratch dir: {e}")))?;
        tempdir_in(&self.scratch_dir, "manifest-")
            .map_err(|e| Status::internal(format!("while creating snapshot scratch dir: {e}")))
    }

    /// The snapshot's manifest. A missing one is NotFound.
    pub async fn fetch_manifest(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &str,
    ) -> Result<Vec<u8>, Status> {
        let dir = self.snapshot_scratch()?;
        let res = async {
            self.fetch_snapshot_files(ctx, actor, uri, &dir, &[MANIFEST_NAME.to_string()])
                .await?;
            std::fs::read(dir.join(MANIFEST_NAME))
                .map_err(|e| Status::internal(format!("while reading fetched manifest: {e}")))
        }
        .await;
        let _ = crate::actordirs::remove_all(&dir);
        res
    }

    /// Uploads `manifest` as the snapshot's manifest.
    pub async fn upload_manifest(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &str,
        manifest: &[u8],
    ) -> Result<(), Status> {
        let dir = self.snapshot_scratch()?;
        let res = async {
            std::fs::write(dir.join(MANIFEST_NAME), manifest)
                .map_err(|e| Status::internal(format!("while staging snapshot manifest: {e}")))?;
            self.upload_snapshot_files(ctx, actor, uri, &dir, &[MANIFEST_NAME.to_string()])
                .await
        }
        .await;
        let _ = crate::actordirs::remove_all(&dir);
        res
    }

    /// Uploads `rec`'s snapshot files from `src_dir`, then its manifest,
    /// which commits the snapshot. The boot ID describes a local copy, not
    /// the snapshot, so it is not uploaded.
    pub async fn upload_snapshot(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        uri: &SnapshotUri,
        src_dir: &Path,
        rec: &SandboxAssetsRecord,
    ) -> Result<(), Status> {
        self.upload_snapshot_files(ctx, actor, uri.as_str(), src_dir, &rec.snapshot_files)
            .await
            .map_err(|e| crate::status::wrap("while uploading snapshot files", e))?;
        let mut remote = rec.clone();
        remote.boot_id.clear();
        self.upload_manifest(ctx, actor, uri.as_str(), &remote.to_json())
            .await
            .map_err(|e| crate::status::wrap("while uploading snapshot manifest", e))
    }

    /// Uploads the local snapshot in `local_dir` to `uri`. Returns the
    /// sandbox class its manifest names (empty when the copy is gone) and
    /// whether it uploaded: it does not when the copy is gone but already
    /// uploaded. A copy from before the node rebooted counts as gone.
    pub async fn upload_local_checkpoint_dir(
        &self,
        ctx: &Ctx,
        actor: &TransferActor,
        snapshot: &hivekeep_proto::keeper::v1::Snapshot,
        local_dir: &Path,
        uri: &SnapshotUri,
    ) -> Result<(String, bool), Status> {
        let name = &snapshot.snapshot_uuid;
        let read = match read_snapshot_manifest(local_dir) {
            Ok(m) => {
                let rec = SandboxAssetsRecord::parse(&m)?;
                if rec.boot_id != self.boot_id {
                    None
                } else {
                    Some(rec)
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => {
                return Err(Status::internal(format!(
                    "while reading local snapshot manifest: {e}"
                )))
            }
        };
        let Some(rec) = read else {
            // Gone, or possibly torn by a reboot. A detach, or an upload
            // before the reboot, may have committed it: the remote manifest
            // is uploaded last, so its presence means the whole snapshot is.
            return match self.fetch_manifest(ctx, actor, uri.as_str()).await {
                Ok(_) => {
                    tracing::info!(snapshot_uri = %uri, "Local snapshot already uploaded and pruned; nothing to do");
                    Ok((String::new(), false))
                }
                Err(e) if e.code() == Code::NotFound => Err(Status::not_found(format!(
                    "local snapshot {name:?} is gone and no uploaded copy exists: {}",
                    e.message()
                ))),
                Err(e) => Err(crate::status::wrap(
                    "while probing for an already-uploaded snapshot manifest",
                    e,
                )),
            };
        };
        if rec.fidelity.is_empty() {
            return Err(Status::failed_precondition(format!(
                "local snapshot {name:?} has no fidelity recorded in its manifest; resume and suspend the actor again"
            )));
        }
        // Every suspend records what it captured, so a mismatch means the
        // store or the control plane drifted; refuse rather than guess.
        let desired = fidelity_label(snapshot.fidelity);
        if rec.fidelity != desired {
            return Err(Status::failed_precondition(format!(
                "local snapshot captured {}; upload asks for {desired}",
                rec.fidelity
            )));
        }
        // The local copy stays until the upload succeeds, so a retry can
        // upload it again.
        self.upload_snapshot(ctx, actor, uri, local_dir, &rec)
            .await?;
        Ok((rec.sandbox_class, true))
    }
}

#[async_trait]
impl snapshot_tracker::Store for SnapshotTransfer {
    async fn upload_local_snapshot(
        &self,
        ctx: &Ctx,
        actor_uid: &str,
        r: &SnapshotRef,
    ) -> Result<bool, Status> {
        let uri =
            SnapshotUri::parse(r.uri()).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let local_dir = self.paths.local_snapshot_dir(actor_uid, r.uuid());
        let actor = TransferActor {
            hive: r.actor.hive.clone(),
            name: r.actor.name.clone(),
            uid: actor_uid.to_string(),
        };
        let (_, uploaded) = self
            .upload_local_checkpoint_dir(ctx, &actor, &r.snapshot, &local_dir, &uri)
            .await?;
        Ok(uploaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::AssetEntry;
    use crate::objectstore::LocalObjectStore;
    use crate::worker_jwt::tests::FakeWorkers;
    use hivekeep_proto::keeper::v1 as pb;

    const URI: &str = "gs://bucket/hives/h/actors/u1/snapshots/s1";

    fn setup() -> (tempfile::TempDir, SnapshotTransfer, Arc<LocalObjectStore>) {
        let d = tempfile::tempdir().unwrap();
        let paths = Arc::new(NodePaths::new(d.path().join("node")));
        let store = Arc::new(LocalObjectStore::new(d.path().join("objects")));
        let jwts = Arc::new(WorkerActorJwts::new(
            Arc::new(FakeWorkers::default()),
            Arc::new(crate::clock::SystemClock),
        ));
        let t = SnapshotTransfer::new(store.clone(), jwts, paths, "boot-1".into());
        (d, t, store)
    }

    fn actor() -> TransferActor {
        TransferActor {
            hive: "h".into(),
            name: "a".into(),
            uid: "u1".into(),
        }
    }

    fn rec() -> SandboxAssetsRecord {
        let mut r = SandboxAssetsRecord {
            sandbox_class: "gvisor".into(),
            snapshot_files: vec!["mem".into()],
            fidelity: "memory".into(),
            boot_id: "boot-1".into(),
            ..Default::default()
        };
        r.assets.insert("gvisor".into(), AssetEntry::default());
        r
    }

    fn local_copy(dir: &Path, r: &SandboxAssetsRecord) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("mem"), b"memory").unwrap();
        std::fs::write(dir.join(MANIFEST_NAME), r.to_json()).unwrap();
    }

    fn memory_snapshot() -> pb::Snapshot {
        pb::Snapshot {
            snapshot_uuid: "s1".into(),
            generation: 1,
            fidelity: pb::SnapshotFidelity::Memory as i32,
            object: Some(pb::ObjectStorage {
                snapshot_uri: URI.into(),
                sharing: 0,
            }),
        }
    }

    #[tokio::test]
    async fn uploads_files_then_a_manifest_without_the_boot_id() {
        let (d, t, store) = setup();
        let local = d.path().join("local");
        local_copy(&local, &rec());
        let uri = SnapshotUri::parse(URI).unwrap();
        let (class, uploaded) = t
            .upload_local_checkpoint_dir(
                &Ctx::background(),
                &actor(),
                &memory_snapshot(),
                &local,
                &uri,
            )
            .await
            .unwrap();
        assert_eq!((class.as_str(), uploaded), ("gvisor", true));
        let remote = store.dir_for(URI).unwrap();
        assert_eq!(std::fs::read(remote.join("mem")).unwrap(), b"memory");
        let manifest =
            SandboxAssetsRecord::parse(&std::fs::read(remote.join(MANIFEST_NAME)).unwrap())
                .unwrap();
        assert!(manifest.boot_id.is_empty());
        let fetched = t
            .fetch_manifest(&Ctx::background(), &actor(), URI)
            .await
            .unwrap();
        assert_eq!(SandboxAssetsRecord::parse(&fetched).unwrap(), manifest);
    }

    #[tokio::test]
    async fn a_gone_copy_succeeds_only_if_already_uploaded() {
        let (d, t, _store) = setup();
        let uri = SnapshotUri::parse(URI).unwrap();
        let gone = d.path().join("gone");
        let err = t
            .upload_local_checkpoint_dir(
                &Ctx::background(),
                &actor(),
                &memory_snapshot(),
                &gone,
                &uri,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
        t.upload_manifest(&Ctx::background(), &actor(), URI, &rec().to_json())
            .await
            .unwrap();
        let (_, uploaded) = t
            .upload_local_checkpoint_dir(
                &Ctx::background(),
                &actor(),
                &memory_snapshot(),
                &gone,
                &uri,
            )
            .await
            .unwrap();
        assert!(!uploaded);
    }

    #[tokio::test]
    async fn a_copy_from_an_earlier_boot_counts_as_gone() {
        let (d, t, _store) = setup();
        let mut old = rec();
        old.boot_id = "boot-0".into();
        let local = d.path().join("local");
        local_copy(&local, &old);
        let uri = SnapshotUri::parse(URI).unwrap();
        let err = t
            .upload_local_checkpoint_dir(
                &Ctx::background(),
                &actor(),
                &memory_snapshot(),
                &local,
                &uri,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn refuses_a_fidelity_mismatch() {
        let (d, t, _store) = setup();
        let local = d.path().join("local");
        let mut r = rec();
        r.fidelity = "volumes".into();
        local_copy(&local, &r);
        let uri = SnapshotUri::parse(URI).unwrap();
        let err = t
            .upload_local_checkpoint_dir(
                &Ctx::background(),
                &actor(),
                &memory_snapshot(),
                &local,
                &uri,
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[test]
    fn store_errors_drop_their_message() {
        let e = store_call_error(
            "fetch",
            "https://h/p?sig=secret",
            Status::not_found("https://h/p?sig=secret missing"),
        );
        assert_eq!(e.code(), Code::NotFound);
        assert!(!e.message().contains("secret"));
    }
}

#[cfg(test)]
mod behavior_tests;
