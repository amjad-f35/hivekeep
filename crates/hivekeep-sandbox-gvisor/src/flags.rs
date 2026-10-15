//! The runsc flag configuration: a JSON file of global and per-command flags,
//! held in memory and reloaded when the file changes, so building a command
//! line never touches the disk.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::{Mutex, RwLock};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

/// How often the backing file is checked for changes.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Flags for one runsc command.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CommandFlags {
    /// Passed before the command name.
    #[serde(default)]
    pub global_flags: Vec<String>,
    /// Passed after the command name.
    #[serde(default)]
    pub subcommand_flags: Vec<String>,
}

/// Global and per-command runsc flags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct FlagConfig {
    /// Passed to every command, before the command name.
    #[serde(default)]
    pub global_flags: Vec<String>,
    /// Per-command flags keyed by command name (`create`, `restore`, ...).
    #[serde(default)]
    pub commands: HashMap<String, CommandFlags>,
}

impl FlagConfig {
    /// The flags before `command`: the top-level ones, then the command's own.
    pub fn global_flags(&self, command: &str) -> Vec<String> {
        let mut out = self.global_flags.clone();
        if let Some(c) = self.commands.get(command) {
            out.extend(c.global_flags.iter().cloned());
        }
        out
    }

    /// The flags after `command`.
    pub fn subcommand_flags(&self, command: &str) -> &[String] {
        self.commands
            .get(command)
            .map(|c| c.subcommand_flags.as_slice())
            .unwrap_or_default()
    }
}

/// Why a flag configuration could not be loaded.
#[derive(Debug, thiserror::Error)]
pub enum FlagsError {
    /// The file could not be read.
    #[error("while reading flags config {path:?}: {source}")]
    Read {
        /// The file.
        path: PathBuf,
        /// The cause.
        source: std::io::Error,
    },
    /// The file is not a valid configuration.
    #[error("while parsing flags config {path:?}: invalid flags JSON: {source}")]
    Parse {
        /// The file.
        path: PathBuf,
        /// The cause.
        source: serde_json::Error,
    },
}

/// Parses a JSON flag configuration. Unknown fields and trailing data are
/// errors.
pub fn parse_flag_config(data: &[u8]) -> Result<FlagConfig, serde_json::Error> {
    serde_json::from_slice(data)
}

fn load_flags_file(path: &Path) -> Result<FlagConfig, FlagsError> {
    let data = std::fs::read(path).map_err(|source| FlagsError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    parse_flag_config(&data).map_err(|source| FlagsError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

type ReloadHook = Box<dyn Fn() + Send + Sync>;

/// The active flag configuration, swapped atomically when its file changes.
pub struct FlagStore {
    path: Option<PathBuf>,
    cfg: RwLock<Arc<FlagConfig>>,
    on_reload: Mutex<Option<ReloadHook>>,
}

impl std::fmt::Debug for FlagStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlagStore")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl FlagStore {
    /// A store with no flags and no backing file.
    pub fn empty() -> Arc<Self> {
        Self::with_config(FlagConfig::default())
    }

    /// A store holding `cfg`, with no backing file.
    pub fn with_config(cfg: FlagConfig) -> Arc<Self> {
        Arc::new(FlagStore {
            path: None,
            cfg: RwLock::new(Arc::new(cfg)),
            on_reload: Mutex::new(None),
        })
    }

    /// Loads `path` and watches it for changes until `cancel` fires. An
    /// empty path yields an empty store. Must be called inside a Tokio runtime.
    pub fn open(
        path: &Path,
        poll: Duration,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>, FlagsError> {
        if path.as_os_str().is_empty() {
            return Ok(Self::empty());
        }
        // Fingerprinted before the read, so a change racing it is seen.
        let seen = fingerprint(path);
        let cfg = load_flags_file(path)?;
        let store = Arc::new(FlagStore {
            path: Some(path.to_path_buf()),
            cfg: RwLock::new(Arc::new(cfg)),
            on_reload: Mutex::new(None),
        });
        tokio::spawn(watch_loop(
            Arc::downgrade(&store),
            path.to_path_buf(),
            seen,
            poll,
            cancel,
        ));
        Ok(store)
    }

    /// The current configuration.
    pub fn get(&self) -> Arc<FlagConfig> {
        self.cfg.read().clone()
    }

    /// Sets a hook run after every reload attempt.
    pub fn set_on_reload(&self, hook: impl Fn() + Send + Sync + 'static) {
        *self.on_reload.lock() = Some(Box::new(hook));
    }

    /// Re-reads the backing file. A bad file is logged and ignored, keeping
    /// the last good configuration; an unchanged one keeps the same `Arc`.
    pub fn reload(&self) {
        if let Some(path) = &self.path {
            match load_flags_file(path) {
                Err(err) => tracing::error!(
                    path = %path.display(),
                    err = %err,
                    "Failed to reload flags configuration; ignoring file changes"
                ),
                Ok(cfg) => {
                    let changed = {
                        let mut cur = self.cfg.write();
                        let changed = **cur != cfg;
                        if changed {
                            *cur = Arc::new(cfg);
                        }
                        changed
                    };
                    if changed {
                        tracing::info!(path = %path.display(), "Reloaded flags configuration");
                    }
                }
            }
        }
        if let Some(hook) = &*self.on_reload.lock() {
            hook();
        }
    }
}

/// What identifies one version of a file on disk.
#[derive(PartialEq, Eq)]
struct Fingerprint {
    modified: Option<SystemTime>,
    len: u64,
    ino: u64,
}

fn fingerprint(path: &Path) -> Option<Fingerprint> {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(path).ok()?;
    Some(Fingerprint {
        modified: m.modified().ok(),
        len: m.len(),
        ino: m.ino(),
    })
}

async fn watch_loop(
    store: std::sync::Weak<FlagStore>,
    path: PathBuf,
    mut last: Option<Fingerprint>,
    poll: Duration,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let now = fingerprint(&path);
        if now == last {
            continue;
        }
        last = now;
        let Some(store) = store.upgrade() else { return };
        store.reload();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_fields_and_trailing_data() {
        assert!(parse_flag_config(br#"{"globalFlags":["-a"]}"#).is_ok());
        assert!(parse_flag_config(br#"{"globalFlag":["-a"]}"#).is_err());
        assert!(parse_flag_config(br#"{"globalFlags":[]} {}"#).is_err());
        assert!(parse_flag_config(br#"{"commands":{"x":{"bogus":[]}}}"#).is_err());
    }

    #[test]
    fn combines_global_flags() {
        let cfg = parse_flag_config(
            br#"{"globalFlags":["-g"],"commands":{"restore":{"globalFlags":["-rg"],"subcommandFlags":["-rs"]}}}"#,
        )
        .unwrap();
        assert_eq!(cfg.global_flags("restore"), vec!["-g", "-rg"]);
        assert_eq!(cfg.global_flags("start"), vec!["-g"]);
        assert_eq!(cfg.subcommand_flags("restore"), ["-rs"]);
        assert!(cfg.subcommand_flags("start").is_empty());
    }

    #[test]
    fn shipped_defaults_parse() {
        let cfg = parse_flag_config(include_bytes!("../data/internal_flags.json")).unwrap();
        assert_eq!(cfg.subcommand_flags("restore"), ["-background", "-detach"]);
    }

    async fn wait_reload(rx: &mut tokio::sync::mpsc::UnboundedReceiver<()>) {
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for a reload");
    }

    #[tokio::test]
    async fn watches_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flags.json");
        std::fs::write(
            &path,
            r#"{"globalFlags":["-global1"],"commands":{"create":{"globalFlags":["-c1"]}}}"#,
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let store = FlagStore::open(&path, Duration::from_millis(10), cancel.clone()).unwrap();
        assert_eq!(store.get().global_flags("create"), vec!["-global1", "-c1"]);

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        store.set_on_reload(move || {
            let _ = tx.send(());
        });
        // A valid update, swapped in by rename.
        let updated = r#"{"globalFlags":["-global1","-global2"],"commands":{"create":{"globalFlags":["-c1","-c2"]},"delete":{"subcommandFlags":["-d"]}}}"#;
        let tmp = dir.path().join(".flags.json.tmp");
        std::fs::write(&tmp, updated).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        wait_reload(&mut rx).await;
        let want = vec!["-global1", "-global2", "-c1", "-c2"];
        assert_eq!(store.get().global_flags("create"), want);

        // A broken update keeps the last good configuration.
        std::fs::write(&tmp, r#"{"globalFlags": ["broken""#).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        wait_reload(&mut rx).await;
        assert_eq!(store.get().global_flags("create"), want);

        // Unchanged content keeps the same configuration object.
        std::fs::write(&tmp, updated).unwrap();
        std::fs::rename(&tmp, &path).unwrap();
        wait_reload(&mut rx).await;
        let before = store.get();
        store.reload();
        wait_reload(&mut rx).await;
        assert!(Arc::ptr_eq(&before, &store.get()));
        cancel.cancel();
    }

    #[test]
    fn empty_path_is_an_empty_store() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let _g = rt.enter();
        let store = FlagStore::open(
            Path::new(""),
            DEFAULT_POLL_INTERVAL,
            CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(*store.get(), FlagConfig::default());
    }
}
