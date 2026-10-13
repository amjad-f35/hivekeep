//! The sandbox runtimes this node's workers advertise.
//!
//! Run and Restore resolve the runtime an actor is to use here, by the hash
//! the worker registered with, so an actor runs exactly the binaries the
//! control plane placed it for. Two maps answer two questions: `runtimes`,
//! by hash, serves the hot path and never forgets an entry (a request may
//! carry a hash advertised earlier); `workers`, by pod UID, holds what each
//! worker advertises now, which is where the runtime to prewarm comes from.
//!
//! `runtimes` is also kept on disk, rewritten durably before a new entry is
//! added. A worker registers a hash only after its file is written, which
//! happens after the index was, so every hash registered before a keeper
//! restart resolves as soon as the keeper serves again.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use hivekeep_proto::keeper::v1 as pb;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::assets::SandboxAssetsRecord;

/// The attribute naming the runtime a worker runs an actor under.
pub const ATTR_SANDBOX_BUNDLE_HASH: &str = "sandbox_bundle_hash";

/// One advertised runtime: the record to stage, and where it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexedRuntime {
    /// The assets to stage.
    pub rec: SandboxAssetsRecord,
    /// The sandbox config it was computed from.
    pub config: String,
    /// The config version it was computed from.
    pub version: String,
}

/// What one worker advertises for one sandbox class.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClassRuntime {
    /// The sandbox config the worker's pool runs for the class.
    pub config: String,
    /// The default runtime's hash.
    pub default_hash: String,
    /// Every advertised runtime, the default first, each once.
    pub hashes: Vec<String>,
}

/// One version of a sandbox config, hashed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HashedVersion {
    /// The version's name.
    pub name: String,
    /// Whether it is the config's default.
    pub is_default: bool,
    /// The assets for this node's architecture.
    pub rec: SandboxAssetsRecord,
    /// The record's bundle hash.
    pub hash: String,
}

/// A sandbox class a worker advertises, with its config's versions.
#[derive(Debug, Clone)]
pub struct AdvertisedClass {
    /// The sandbox class.
    pub class: String,
    /// The config the pool runs for it.
    pub config: String,
    /// The config's versions.
    pub hashed: Vec<HashedVersion>,
}

#[derive(Serialize, Deserialize)]
struct IndexFile {
    runtimes: BTreeMap<String, IndexEntry>,
}

#[derive(Serialize, Deserialize)]
struct IndexEntry {
    config: String,
    version: String,
    record: Option<SandboxAssetsRecord>,
}

#[derive(Default)]
struct State {
    runtimes: HashMap<String, IndexedRuntime>,
    workers: HashMap<String, HashMap<String, ClassRuntime>>,
}

/// The node's runtime index.
pub struct RuntimeIndex {
    state: RwLock<State>,
    path: Option<PathBuf>,
}

impl RuntimeIndex {
    /// An index kept in memory only.
    pub fn in_memory() -> Self {
        RuntimeIndex {
            state: RwLock::new(State::default()),
            path: None,
        }
    }

    /// The index persisted at `path`, loaded if the file exists. An
    /// undecodable file starts empty, and an entry whose record does not
    /// hash to its key is dropped: the syncer writes the current runtimes
    /// again either way.
    pub fn open(path: PathBuf) -> Result<Self, String> {
        let x = RuntimeIndex {
            state: RwLock::new(State::default()),
            path: Some(path.clone()),
        };
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(path = %path.display(), "Sandbox runtime index starts empty");
                return Ok(x);
            }
            Err(e) => return Err(format!("while reading the sandbox runtime index: {e}")),
        };
        let f: IndexFile = match serde_json::from_slice(&data) {
            Ok(f) => f,
            Err(err) => {
                tracing::warn!(path = %path.display(), %err, "Sandbox runtime index is unreadable; starting empty");
                return Ok(x);
            }
        };
        {
            let mut st = x.state.write();
            for (hash, e) in f.runtimes {
                let Some(rec) = e.record else {
                    tracing::warn!(runtime_hash = %hash, "Dropping a sandbox runtime index entry with no record");
                    continue;
                };
                let got = rec.bundle_hash();
                if got != hash {
                    tracing::warn!(runtime_hash = %hash, got = %got, "Dropping a sandbox runtime index entry that hashes elsewhere");
                    continue;
                }
                st.runtimes.insert(
                    hash,
                    IndexedRuntime {
                        rec,
                        config: e.config,
                        version: e.version,
                    },
                );
            }
            tracing::info!(path = %path.display(), runtimes = st.runtimes.len(), "Sandbox runtime index loaded");
        }
        Ok(x)
    }

    fn write_index(
        &self,
        st: &State,
        extra_hash: &str,
        extra: &IndexedRuntime,
    ) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let mut runtimes: BTreeMap<String, IndexEntry> = st
            .runtimes
            .iter()
            .map(|(h, rt)| {
                (
                    h.clone(),
                    IndexEntry {
                        config: rt.config.clone(),
                        version: rt.version.clone(),
                        record: Some(rt.rec.clone()),
                    },
                )
            })
            .collect();
        runtimes.insert(
            extra_hash.to_string(),
            IndexEntry {
                config: extra.config.clone(),
                version: extra.version.clone(),
                record: Some(extra.rec.clone()),
            },
        );
        let data = serde_json::to_vec(&IndexFile { runtimes })
            .map_err(|e| format!("while encoding the sandbox runtime index: {e}"))?;
        crate::atomicfile::write_sync(path, &data, 0o600)
            .map_err(|e| format!("while writing the sandbox runtime index: {e}"))
    }

    fn add_runtime(&self, st: &mut State, config: &str, v: &HashedVersion) -> Result<(), String> {
        if st.runtimes.contains_key(&v.hash) {
            return Ok(());
        }
        let rt = IndexedRuntime {
            rec: v.rec.clone(),
            config: config.to_string(),
            version: v.name.clone(),
        };
        self.write_index(st, &v.hash, &rt)?;
        st.runtimes.insert(v.hash.clone(), rt);
        Ok(())
    }

    /// Records what the worker pod advertises, replacing what it advertised
    /// before, and makes each runtime resolvable. Fails only if a runtime
    /// not yet indexed cannot be persisted.
    pub fn register(&self, pod_uid: &str, advertised: &[AdvertisedClass]) -> Result<(), String> {
        let mut st = self.state.write();
        let mut classes = HashMap::new();
        for a in advertised {
            let mut cr = ClassRuntime {
                config: a.config.clone(),
                ..Default::default()
            };
            for v in &a.hashed {
                self.add_runtime(&mut st, &a.config, v)?;
                if v.is_default {
                    cr.default_hash = v.hash.clone();
                }
                if !cr.hashes.contains(&v.hash) {
                    cr.hashes.push(v.hash.clone());
                }
            }
            classes.insert(a.class.clone(), cr);
        }
        st.workers.insert(pod_uid.to_string(), classes);
        Ok(())
    }

    /// Makes each version of `config` resolvable without changing what any
    /// worker advertises, so a request for a new default resolves as soon
    /// as the config changes.
    pub fn register_runtimes(&self, config: &str, hashed: &[HashedVersion]) -> Result<(), String> {
        let mut st = self.state.write();
        for v in hashed {
            self.add_runtime(&mut st, config, v)?;
        }
        Ok(())
    }

    /// Drops what the worker pod advertised; its runtimes stay resolvable.
    pub fn forget(&self, pod_uid: &str) {
        self.state.write().workers.remove(pod_uid);
    }

    /// The runtime advertised under `hash`.
    pub fn lookup(&self, hash: &str) -> Option<IndexedRuntime> {
        self.state.read().runtimes.get(hash).cloned()
    }

    /// What the worker pod advertises now, by sandbox class.
    pub fn worker(&self, pod_uid: &str) -> Option<HashMap<String, ClassRuntime>> {
        self.state.read().workers.get(pod_uid).cloned()
    }

    /// The assets to stage for an actor placed on runtime `rt`: the record
    /// a worker here advertises under its bundle hash. A miss means the
    /// request reached a node whose workers never advertised the runtime.
    pub fn advertised_runtime(
        &self,
        rt: Option<&pb::ActorSandboxRuntime>,
    ) -> Result<SandboxAssetsRecord, Status> {
        let hash = bundle_hash_of(rt)
            .map_err(|e| Status::invalid_argument(format!("sandbox_runtime: {e}")))?;
        let class = rt.map(|r| r.sandbox_class.as_str()).unwrap_or("");
        let Some(indexed) = self.lookup(&hash) else {
            tracing::warn!(bundle_hash = %hash, sandbox_class = class, "Sandbox runtime is not advertised by any worker on this node");
            return Err(Status::failed_precondition(format!(
                "sandbox runtime {hash} (class {class}) is not advertised by any worker on this node"
            )));
        };
        if class != indexed.rec.sandbox_class {
            return Err(Status::failed_precondition(format!(
                "sandbox runtime {hash} is advertised for class {}, not {class}",
                indexed.rec.sandbox_class
            )));
        }
        Ok(indexed.rec)
    }
}

/// The bundle hash attribute of `rt`.
pub fn bundle_hash_of(rt: Option<&pb::ActorSandboxRuntime>) -> Result<String, String> {
    let rt = rt.ok_or("missing")?;
    for a in rt
        .version
        .as_ref()
        .map(|v| v.attributes.as_slice())
        .unwrap_or(&[])
    {
        if a.key != ATTR_SANDBOX_BUNDLE_HASH {
            continue;
        }
        if a.value.is_empty() {
            return Err(format!("empty {ATTR_SANDBOX_BUNDLE_HASH} attribute"));
        }
        return Ok(a.value.clone());
    }
    Err(format!("no {ATTR_SANDBOX_BUNDLE_HASH} attribute"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assets::AssetEntry;

    fn version(name: &str, digest: &str, is_default: bool) -> HashedVersion {
        let mut rec = SandboxAssetsRecord {
            sandbox_class: "gvisor".into(),
            ..Default::default()
        };
        rec.assets.insert(
            "gvisor".into(),
            AssetEntry {
                url: format!("gs://gvisor/{name}.tar.zst"),
                sha256: digest.repeat(64),
            },
        );
        let hash = rec.bundle_hash();
        HashedVersion {
            name: name.into(),
            is_default,
            rec,
            hash,
        }
    }

    fn runtime(class: &str, hash: &str) -> pb::ActorSandboxRuntime {
        pb::ActorSandboxRuntime {
            sandbox_class: class.into(),
            version: Some(pb::VersionedSandboxCompat {
                schema_version: "1".into(),
                attributes: vec![pb::AttributeEntry {
                    key: ATTR_SANDBOX_BUNDLE_HASH.into(),
                    value: hash.into(),
                }],
            }),
        }
    }

    #[test]
    fn registers_resolves_and_persists() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("index.json");
        let x = RuntimeIndex::open(path.clone()).unwrap();
        let v1 = version("v1", "a", true);
        let v2 = version("v2", "b", false);
        x.register(
            "pod-1",
            &[AdvertisedClass {
                class: "gvisor".into(),
                config: "cfg".into(),
                hashed: vec![v1.clone(), v2.clone(), v1.clone()],
            }],
        )
        .unwrap();
        let w = x.worker("pod-1").unwrap();
        assert_eq!(w["gvisor"].default_hash, v1.hash);
        assert_eq!(w["gvisor"].hashes, vec![v1.hash.clone(), v2.hash.clone()]);
        assert_eq!(
            x.advertised_runtime(Some(&runtime("gvisor", &v2.hash)))
                .unwrap(),
            v2.rec
        );
        assert_eq!(
            x.advertised_runtime(Some(&runtime("microvm", &v2.hash)))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            x.advertised_runtime(Some(&runtime("gvisor", "sha256:nope")))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            x.advertised_runtime(None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        x.forget("pod-1");
        assert!(x.worker("pod-1").is_none());
        // Reloaded after a restart, every runtime still resolves.
        let y = RuntimeIndex::open(path).unwrap();
        assert_eq!(y.lookup(&v1.hash).unwrap().version, "v1");
        assert!(y.lookup(&v2.hash).is_some());
    }

    #[test]
    fn drops_entries_that_hash_elsewhere_and_survives_garbage() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("index.json");
        let v = version("v1", "a", true);
        let bad = format!(
            r#"{{"runtimes":{{"sha256:wrong":{{"config":"c","version":"v","record":{}}}}}}}"#,
            String::from_utf8(v.rec.to_json()).unwrap()
        );
        std::fs::write(&path, bad).unwrap();
        assert!(RuntimeIndex::open(path.clone())
            .unwrap()
            .lookup("sha256:wrong")
            .is_none());
        std::fs::write(&path, b"garbage").unwrap();
        assert!(RuntimeIndex::open(path).is_ok());
    }

    #[test]
    fn bundle_hash_attribute() {
        assert!(bundle_hash_of(Some(&runtime("gvisor", ""))).is_err());
        assert_eq!(
            bundle_hash_of(Some(&runtime("gvisor", "sha256:x"))).unwrap(),
            "sha256:x"
        );
    }
}

#[cfg(test)]
mod behavior_tests;
