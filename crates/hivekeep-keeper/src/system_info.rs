//! System-info volumes: read-only files the keeper generates into an
//! actor's volume roots on every Run and Restore (its metadata, projected
//! trust bundles), and refreshes while the actor runs when a bundle
//! changes.

use std::collections::{BTreeMap, HashMap};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1 as pb;
use parking_lot::Mutex;
use tonic::Status;

use crate::context::Ctx;
use crate::paths::NodePaths;
use crate::trustbundle::Source;
use crate::workqueue::{RateLimiter, WorkQueue};

/// First retry delay of a bundle whose refresh failed to write.
const REFRESH_RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(5);
/// Longest retry delay of a bundle whose refresh keeps failing.
const REFRESH_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(300);

/// One system-info volume of an actor.
#[derive(Debug, Clone)]
pub struct SystemInfoVolume {
    /// The volume's name.
    pub name: String,
    /// Its root directory on the node.
    pub root: PathBuf,
    /// What it projects.
    pub spec: pb::SystemInfoVolumeSource,
}

/// The system-info volumes of a workload spec.
pub fn system_info_volumes_for(
    paths: &NodePaths,
    actor_uid: &str,
    spec: Option<&pb::WorkloadSpec>,
) -> Vec<SystemInfoVolume> {
    spec.map(|s| s.volumes.as_slice())
        .unwrap_or(&[])
        .iter()
        .filter_map(|v| {
            v.system_info.as_ref().map(|si| SystemInfoVolume {
                name: v.name.clone(),
                root: paths.system_info_volume_root(actor_uid, &v.name),
                spec: si.clone(),
            })
        })
        .collect()
}

/// An actor whose volumes the refresher keeps current. Stale once
/// superseded or deregistered.
pub struct RegisteredActor {
    uid: String,
    actor: ActorRef,
    volumes: Vec<SystemInfoVolume>,
    /// Set once superseded or deregistered; held while writing the volumes.
    stale: Mutex<bool>,
}

impl RegisteredActor {
    /// Whether it was superseded or deregistered, for tests.
    #[cfg(test)]
    pub(crate) fn is_stale(&self) -> bool {
        *self.stale.lock()
    }
}

/// Populates and refreshes system-info volumes.
pub struct SystemInfoRefresher {
    bundles: Source,
    actors: Mutex<HashMap<String, Arc<RegisteredActor>>>,
    events: tokio::sync::mpsc::UnboundedSender<String>,
    pending: Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<String>>>,
}

impl SystemInfoRefresher {
    /// A refresher resolving trust bundles through `bundles`.
    pub fn new(bundles: Source) -> Self {
        let (events, pending) = tokio::sync::mpsc::unbounded_channel();
        SystemInfoRefresher {
            bundles,
            actors: Mutex::new(HashMap::new()),
            events,
            pending: Mutex::new(Some(pending)),
        }
    }

    /// Notes that the cluster trust bundle object `object_name` was added,
    /// changed or deleted: the bundle it backs, if any, is refreshed by
    /// [`SystemInfoRefresher::run`].
    pub fn on_trust_bundle_object(&self, object_name: &str) {
        if let Some(bundle) = crate::trustbundle::bundle_backed_by(object_name) {
            let _ = self.events.send(bundle.to_string());
        }
    }

    /// Refreshes bundles as their objects change until `ctx` ends. A
    /// refresh that fails to write is retried with growing delays until it
    /// lands; one whose bundle cannot be resolved waits for the next
    /// change. Runs at most once.
    pub async fn run(self: Arc<Self>, ctx: Ctx) {
        let Some(mut events) = self.pending.lock().take() else {
            return;
        };
        let queue = WorkQueue::new();
        let retry = RateLimiter::new(REFRESH_RETRY_BASE, REFRESH_RETRY_MAX);
        let feeder = {
            let queue = queue.clone();
            tokio::spawn(async move {
                while let Some(b) = events.recv().await {
                    queue.add(&b);
                }
            })
        };
        loop {
            let key = tokio::select! {
                k = queue.get() => k,
                _ = ctx.done() => None,
            };
            let Some(key) = key else { break };
            let (me, k) = (self.clone(), key.clone());
            let res = tokio::task::spawn_blocking(move || me.refresh_bundle(&k)).await;
            queue.done(&key);
            match res {
                Ok(Ok(_)) => retry.forget(&key),
                _ => queue.add_after(&key, retry.when(&key)),
            }
        }
        queue.shut_down();
        feeder.abort();
    }

    /// Bundle names waiting for the run loop, for tests.
    #[cfg(test)]
    pub(crate) fn drain_events(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(rx) = self.pending.lock().as_mut() {
            while let Ok(b) = rx.try_recv() {
                out.push(b);
            }
        }
        out
    }

    /// Writes the actor's volumes and keeps them current from now on,
    /// superseding an earlier registration. On failure nothing stays
    /// registered.
    pub fn register(
        &self,
        actor_uid: &str,
        actor: &ActorRef,
        volumes: Vec<SystemInfoVolume>,
    ) -> Result<Arc<RegisteredActor>, Status> {
        let reg = Arc::new(RegisteredActor {
            uid: actor_uid.to_string(),
            actor: actor.clone(),
            volumes: volumes.clone(),
            stale: Mutex::new(false),
        });
        // Hold the new registration's lock while writing, so a refresh
        // cannot interleave with the first population.
        let st = reg.stale.lock();
        let prev = self
            .actors
            .lock()
            .insert(actor_uid.to_string(), reg.clone());
        if let Some(prev) = prev {
            *prev.stale.lock() = true;
            tracing::info!(actor_uid, actor = %actor, "Superseded a stale system-info volume registration");
        }
        for v in &volumes {
            if let Err(e) = self.write(actor, actor_uid, v) {
                drop(st);
                let mut actors = self.actors.lock();
                if actors.get(actor_uid).is_some_and(|a| Arc::ptr_eq(a, &reg)) {
                    actors.remove(actor_uid);
                }
                *reg.stale.lock() = true;
                return Err(Status::internal(format!(
                    "while populating system-info volume {:?}: {e}",
                    v.name
                )));
            }
        }
        drop(st);
        Ok(reg)
    }

    /// Stops refreshing the actor's volumes.
    pub fn deregister(&self, actor_uid: &str) {
        let removed = self.actors.lock().remove(actor_uid);
        if let Some(a) = removed {
            *a.stale.lock() = true;
        }
    }

    /// Stops refreshing `owner`'s volumes, unless a newer registration of
    /// the actor superseded it.
    pub fn deregister_owned(&self, owner: &Arc<RegisteredActor>) {
        let mut actors = self.actors.lock();
        if !actors
            .get(&owner.uid)
            .is_some_and(|a| Arc::ptr_eq(a, owner))
        {
            return;
        }
        actors.remove(&owner.uid);
        drop(actors);
        *owner.stale.lock() = true;
    }

    /// Whether an actor is registered, for tests.
    pub fn is_registered(&self, actor_uid: &str) -> bool {
        self.actors.lock().contains_key(actor_uid)
    }

    /// The actor's live registration, for tests.
    #[cfg(test)]
    pub(crate) fn registration(&self, actor_uid: &str) -> Option<Arc<RegisteredActor>> {
        self.actors.lock().get(actor_uid).cloned()
    }

    fn collect(
        &self,
        actor: &ActorRef,
        actor_uid: &str,
        si: &pb::SystemInfoVolumeSource,
    ) -> Result<BTreeMap<String, Vec<u8>>, String> {
        let mut payload = BTreeMap::new();
        for ds in &si.data_sources {
            if let Some(tb) = &ds.trust_bundle {
                let pem = self
                    .bundles
                    .combined(&tb.names)
                    .map_err(|e| format!("system-info projection {:?}: {e}", tb.path))?;
                payload.insert(tb.path.clone(), pem);
            } else if let Some(md) = &ds.actor_metadata {
                for item in &md.items {
                    let value = match pb::ActorMetadataField::try_from(item.field) {
                        Ok(pb::ActorMetadataField::Name) => actor.name.clone(),
                        Ok(pb::ActorMetadataField::Hive) => actor.hive.clone(),
                        Ok(pb::ActorMetadataField::Uid) => actor_uid.to_string(),
                        _ => continue,
                    };
                    payload.insert(item.path.clone(), value.into_bytes());
                }
            }
        }
        Ok(payload)
    }

    fn write(
        &self,
        actor: &ActorRef,
        actor_uid: &str,
        v: &SystemInfoVolume,
    ) -> Result<bool, String> {
        let payload = self
            .collect(actor, actor_uid, &v.spec)
            .map_err(|e| format!("while collecting volume contents: {e}"))?;
        apply(&v.root, &payload)
    }

    /// Rewrites every registered volume projecting `bundle`. A bundle that
    /// cannot be resolved leaves the files with their last contents.
    pub fn refresh_bundle(&self, bundle: &str) -> Result<usize, String> {
        let targets: Vec<Arc<RegisteredActor>> = self
            .actors
            .lock()
            .values()
            .filter(|a| a.volumes.iter().any(|v| projects_bundle(&v.spec, bundle)))
            .cloned()
            .collect();
        let mut refreshed = 0;
        let mut last_err = None;
        for a in targets {
            let stale = a.stale.lock();
            if *stale {
                continue;
            }
            for v in a
                .volumes
                .iter()
                .filter(|v| projects_bundle(&v.spec, bundle))
            {
                let payload = match self.collect(&a.actor, &a.uid, &v.spec) {
                    Ok(p) => p,
                    Err(err) => {
                        tracing::warn!(actor_uid = %a.uid, volume = %v.name, bundle, %err, "Trust bundle unresolvable; projected files keep their last contents");
                        continue;
                    }
                };
                match apply(&v.root, &payload) {
                    Ok(true) => refreshed += 1,
                    Ok(false) => {}
                    Err(err) => {
                        tracing::error!(actor_uid = %a.uid, volume = %v.name, bundle, %err, "Failed to refresh system-info volume");
                        last_err = Some(err);
                    }
                }
            }
        }
        if refreshed > 0 {
            tracing::info!(
                bundle,
                volumes = refreshed,
                "Refreshed projected trust bundle under running actors"
            );
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(refreshed),
        }
    }
}

/// Whether a volume projects `bundle`.
pub fn projects_bundle(si: &pb::SystemInfoVolumeSource, bundle: &str) -> bool {
    si.data_sources.iter().any(|ds| {
        ds.trust_bundle
            .as_ref()
            .is_some_and(|tb| tb.names.iter().any(|n| n == bundle))
    })
}

/// Writes `payload` under `root`, each file atomically, skipping files that
/// already hold their content. No write follows a symlink out of `root`.
/// Reports whether any file changed.
fn apply(root: &Path, payload: &BTreeMap<String, Vec<u8>>) -> Result<bool, String> {
    crate::atomicfile::create_dir_all_mode(root, 0o755)
        .map_err(|e| format!("while creating {}: {e}", root.display()))?;
    let mut changed = false;
    for (rel, data) in payload {
        hivekeep_core::volume_path::validate_projected(rel)
            .map_err(|e| format!("invalid system-info path {rel:?}: {e}"))?;
        let rel_path = Path::new(rel);
        let dst = root.join(rel_path);
        if std::fs::symlink_metadata(&dst).is_ok_and(|m| m.is_file())
            && std::fs::read(&dst).is_ok_and(|cur| &cur == data)
        {
            continue;
        }
        // Create parents one component at a time, refusing symlinks.
        let mut dir = root.to_path_buf();
        if let Some(parent) = rel_path.parent() {
            for c in parent.components() {
                let Component::Normal(c) = c else {
                    return Err(format!("invalid system-info path {rel:?}"));
                };
                dir.push(c);
                match std::fs::symlink_metadata(&dir) {
                    Ok(m) if m.is_dir() => {}
                    Ok(_) => return Err(format!("{} is not a directory", dir.display())),
                    Err(_) => crate::atomicfile::create_dir_mode(&dir, 0o755).map_err(|e| {
                        format!(
                            "while creating parent of {rel:?} under {}: {e}",
                            root.display()
                        )
                    })?,
                }
            }
        }
        if std::fs::symlink_metadata(&dst).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(format!("{} is a symlink", dst.display()));
        }
        crate::atomicfile::write(&dst, data, 0o644).map_err(|e| {
            format!(
                "while writing system-info file {rel:?} under {}: {e}",
                root.display()
            )
        })?;
        changed = true;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trustbundle::tests::pem;
    use crate::trustbundle::{EGRESS_NAME, SYSTEM_ROOTS_NAME};

    fn spec() -> pb::SystemInfoVolumeSource {
        pb::SystemInfoVolumeSource {
            data_sources: vec![
                pb::SystemInfoDataSource {
                    actor_metadata: Some(pb::ActorMetadataDataSource {
                        items: vec![
                            pb::ActorMetadataItem {
                                field: pb::ActorMetadataField::Name as i32,
                                path: "meta/name".into(),
                            },
                            pb::ActorMetadataItem {
                                field: pb::ActorMetadataField::Uid as i32,
                                path: "uid".into(),
                            },
                        ],
                    }),
                    trust_bundle: None,
                },
                pb::SystemInfoDataSource {
                    actor_metadata: None,
                    trust_bundle: Some(pb::TrustBundleDataSource {
                        path: "ca.pem".into(),
                        names: vec![EGRESS_NAME.into(), SYSTEM_ROOTS_NAME.into()],
                    }),
                },
            ],
        }
    }

    fn refresher(egress: Arc<Mutex<String>>) -> SystemInfoRefresher {
        SystemInfoRefresher::new(Source::new(
            Arc::new(move |_: &str| Ok(Some(egress.lock().clone()))),
            pem(b"roots").into_bytes(),
        ))
    }

    #[test]
    fn registers_writes_and_refreshes() {
        let d = tempfile::tempdir().unwrap();
        let paths = NodePaths::new(d.path());
        let egress = Arc::new(Mutex::new(pem(b"ca-1")));
        let r = refresher(egress.clone());
        let ws = pb::WorkloadSpec {
            containers: vec![],
            volumes: vec![pb::Volume {
                name: "info".into(),
                system_info: Some(spec()),
                ..Default::default()
            }],
        };
        let vols = system_info_volumes_for(&paths, "u1", Some(&ws));
        let root = vols[0].root.clone();
        let reg = r
            .register("u1", &ActorRef::new("h", "counter"), vols)
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join("meta/name")).unwrap(),
            "counter"
        );
        assert_eq!(std::fs::read_to_string(root.join("uid")).unwrap(), "u1");
        let before = std::fs::read(root.join("ca.pem")).unwrap();
        assert_eq!(
            r.refresh_bundle(EGRESS_NAME).unwrap(),
            0,
            "unchanged bundle rewrites nothing"
        );
        *egress.lock() = pem(b"ca-2");
        assert_eq!(r.refresh_bundle(EGRESS_NAME).unwrap(), 1);
        assert_ne!(std::fs::read(root.join("ca.pem")).unwrap(), before);
        r.deregister_owned(&reg);
        assert!(!r.is_registered("u1"));
    }

    #[test]
    fn a_failed_population_registers_nothing() {
        let d = tempfile::tempdir().unwrap();
        let paths = NodePaths::new(d.path());
        let r = SystemInfoRefresher::new(Source::new(Arc::new(|_: &str| Ok(None)), Vec::new()));
        let ws = pb::WorkloadSpec {
            containers: vec![],
            volumes: vec![pb::Volume {
                name: "info".into(),
                system_info: Some(spec()),
                ..Default::default()
            }],
        };
        let vols = system_info_volumes_for(&paths, "u1", Some(&ws));
        assert!(r.register("u1", &ActorRef::new("h", "a"), vols).is_err());
        assert!(!r.is_registered("u1"));
    }

    #[test]
    fn apply_refuses_escapes_and_symlinks() {
        let d = tempfile::tempdir().unwrap();
        let mut p = BTreeMap::new();
        p.insert("../escape".to_string(), b"x".to_vec());
        assert!(apply(&d.path().join("root"), &p).is_err());
        #[cfg(unix)]
        {
            let root = d.path().join("root2");
            std::fs::create_dir_all(&root).unwrap();
            std::os::unix::fs::symlink(d.path(), root.join("link")).unwrap();
            let mut p = BTreeMap::new();
            p.insert("link/f".to_string(), b"x".to_vec());
            assert!(apply(&root, &p).is_err());
        }
    }
}

#[cfg(test)]
mod behavior_tests;
