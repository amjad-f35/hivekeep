//! Command-line flags and their validation.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::filecachegc::GcConfig;
use crate::image_gc::ImageGcConfig;
use crate::paths::{NodePaths, DEFAULT_BASE_DIR};
use crate::peer_identity::DEFAULT_POD_IDENTITY_OID;
use crate::snapshot_tracker as st;

/// The default port the Keeper service listens on.
pub const DEFAULT_PORT: u16 = 7443;
/// The default credential bundle the gRPC servers present.
pub const DEFAULT_CRED_BUNDLE: &str = "/run/podidentity.podcert.hivekeep.io/credential-bundle.pem";
/// The default CA bundle client certificates must chain to.
pub const DEFAULT_CLIENT_CA: &str = "/run/podidentity.podcert.hivekeep.io/trust-bundle.pem";
/// The only caller the Keeper service admits by default.
pub const DEFAULT_API_SPIFFE_ID: &str = "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-api";
/// The control plane's default address.
pub const DEFAULT_API_ADDRESS: &str = "https://hivekeep-api.hivekeep-system.svc:443";
/// The CA bundle the control plane's certificate chains to by default.
pub const DEFAULT_API_CA: &str = "/run/servicedns.podcert.hivekeep.io/trust-bundle.pem";
/// The DNS name expected on the control plane's certificate by default.
pub const DEFAULT_API_SERVER_NAME: &str = "hivekeep-api.hivekeep-system.svc";
/// The system WebPKI roots, as the distroless base image ships them.
pub const DEFAULT_SYSTEM_ROOTS: &str = "/etc/ssl/certs/ca-certificates.crt";
/// The node object-store plugin's socket in the deployed keeper pod.
pub const DEFAULT_OBJECTSTORE_PLUGIN_SOCKET: &str = "/run/objectstore-plugin/node.sock";
/// The default in-process upload reservation budget, 1 GiB.
pub const DEFAULT_DIRECT_UPLOAD_MEMORY: i64 = 1 << 30;
/// The default in-process asset staging capacity, 4 GiB.
pub const DEFAULT_DIRECT_ASSET_STAGING_BYTES: i64 = 4 << 30;
/// The largest throughput flag value, 1 TiB/s, so bytes cannot overflow.
const MAX_UPLOAD_EST_MIBPS: i64 = 1 << 20;

/// Where snapshots and sandbox assets move through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ObjectStoreKind {
    /// The node object-store plugin over its Unix socket (the deployed
    /// shape).
    Plugin,
    /// The object-store plugins in the keeper's own process, on the backend
    /// the environment selects (single-binary setups).
    Direct,
    /// A local directory standing in for object storage (development).
    Local,
}

/// The keeper's flags.
#[derive(Debug, Clone, Parser)]
#[command(name = "hivekeep-keeper", version, about = "Hivekeep node agent")]
pub struct Args {
    /// Port the Keeper gRPC service listens on.
    #[arg(long, default_value_t = DEFAULT_PORT)]
    pub port: u16,
    /// Address the Prometheus metrics endpoint listens on.
    #[arg(long, default_value = "0.0.0.0:9090")]
    pub metrics_listen_addr: String,
    /// Address the /readyz and /healthz endpoints listen on.
    #[arg(long, default_value = "0.0.0.0:9091")]
    pub health_listen_addr: String,
    /// Root of the keeper's on-node state, shared with the worker pods.
    #[arg(long, default_value = DEFAULT_BASE_DIR)]
    pub base_dir: PathBuf,
    /// Where actors' snapshots are staged; defaults to <base-dir>/snapshots.
    #[arg(long)]
    pub snapshots_dir: Option<PathBuf>,
    /// Credential bundle the gRPC servers present.
    #[arg(long, default_value = DEFAULT_CRED_BUNDLE)]
    pub grpc_server_cred_bundle: PathBuf,
    /// CA bundle client certificates are verified against.
    #[arg(long, default_value = DEFAULT_CLIENT_CA)]
    pub client_ca_certs: PathBuf,
    /// The only SPIFFE ID allowed to call the Keeper service.
    #[arg(long, default_value = DEFAULT_API_SPIFFE_ID)]
    pub api_allowed_spiffe_id: String,
    /// Control plane address.
    #[arg(long, default_value = DEFAULT_API_ADDRESS)]
    pub api_address: String,
    /// CA bundle the control plane is verified against.
    #[arg(long, default_value = DEFAULT_API_CA)]
    pub api_ca_file: PathBuf,
    /// DNS name expected on the control plane's certificate.
    #[arg(long, default_value = DEFAULT_API_SERVER_NAME)]
    pub api_server_name: String,
    /// Serve and dial without TLS. Development only.
    #[arg(long)]
    pub insecure_dev: bool,
    /// The certificate extension carrying a pod's identity.
    #[arg(long, default_value = DEFAULT_POD_IDENTITY_OID)]
    pub pod_identity_oid: String,
    /// A kubelet CredentialProviderConfig supplying image pull credentials.
    #[arg(long)]
    pub image_credential_provider_config: Option<PathBuf>,
    /// Directory of the credential provider executables.
    #[arg(long)]
    pub image_credential_provider_bin_dir: Option<PathBuf>,
    /// Minimum log level.
    #[arg(long, default_value = "info")]
    pub log_level: String,
    /// Preview gates to enable.
    #[arg(long, value_delimiter = ',')]
    pub preview: Vec<String>,
    /// Actor resource sampling frequency; 0 disables it.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1m")]
    pub actor_stats_poll_interval: Duration,
    /// How long to keep accepting RPCs after SIGTERM before draining.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "0s")]
    pub drain_delay: Duration,
    /// Deadline for the graceful drain on shutdown.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5m")]
    pub drain_timeout: Duration,
    /// Which object store snapshots and assets move through. Defaults to
    /// `local` when --object-store-dir is set, else `plugin`.
    #[arg(long, value_enum)]
    pub object_store: Option<ObjectStoreKind>,
    /// Unix socket of the node object-store plugin (`--object-store=plugin`).
    #[arg(long, default_value = DEFAULT_OBJECTSTORE_PLUGIN_SOCKET)]
    pub objectstore_plugin_socket: PathBuf,
    /// How long startup waits for the object-store plugin to serve.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1m")]
    pub objectstore_plugin_ready_timeout: Duration,
    /// In-process upload reservation budget in bytes (`--object-store=direct`).
    #[arg(long, default_value_t = DEFAULT_DIRECT_UPLOAD_MEMORY)]
    pub objectstore_upload_memory_bytes: i64,
    /// In-process asset staging directory (`--object-store=direct`); defaults
    /// to a directory beside <base-dir>, which it must not be under.
    #[arg(long)]
    pub objectstore_asset_staging_dir: Option<PathBuf>,
    /// The most the in-process asset staging directory holds at once.
    #[arg(long, default_value_t = DEFAULT_DIRECT_ASSET_STAGING_BYTES)]
    pub objectstore_asset_staging_bytes: i64,
    /// Local directory serving as object storage (`--object-store=local`).
    #[arg(long)]
    pub object_store_dir: Option<PathBuf>,
    /// Read public gs:// assets anonymously when the object store cannot.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub anonymous_gcs_assets: bool,
    /// A JSON cluster view (worker pods, pools, sandbox configs) read
    /// instead of watching the Kubernetes API. Development only.
    #[arg(long)]
    pub cluster_view_file: Option<PathBuf>,
    /// The node this keeper runs on, for the cluster view's pod watch.
    /// Defaults to the node named by the keeper's pod identity certificate.
    #[arg(long, env = "HK_NODE_NAME")]
    pub node_name: Option<String>,
    /// Directory of the node-local OCI image layer cache; defaults to
    /// <base-dir>/image-cache. Must be on the volume shared with the worker
    /// pods: the cached layers are their overlay lower dirs.
    #[arg(long)]
    pub image_cache_dir: Option<PathBuf>,
    /// Layers and image records younger than this are never evicted.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "2m")]
    pub image_cache_min_age: Duration,
    /// Registry endpoint replacing localhost and loopback registries, for
    /// local development (for example kind-registry:5000).
    #[arg(long, default_value = "")]
    pub localhost_registry_replacement: String,
    /// How often the worker sandbox runtimes are resynced.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "30s")]
    pub sandbox_sync_period: Duration,
    /// A file holding the egress gateway CA bundle to project.
    #[arg(long)]
    pub egress_ca_bundle_file: Option<PathBuf>,
    /// System WebPKI roots to project.
    #[arg(long, default_value = DEFAULT_SYSTEM_ROOTS)]
    pub system_roots_file: PathBuf,

    /// Upload delay after the oldest undurable snapshot.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5m")]
    pub snapshot_upload_delay: Duration,
    /// How long a suspended actor stays before it is detached.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "30m")]
    pub snapshot_detach_delay: Duration,
    /// Per-attempt upload timeout on top of the size allowance; 0 is off.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "2m")]
    pub snapshot_upload_timeout: Duration,
    /// Expected upload throughput in MiB/s of allocated size; 0 adds none.
    #[arg(long, default_value_t = 100)]
    pub snapshot_upload_est_throughput_mib: i64,
    /// Background uploads at once.
    #[arg(long, default_value_t = st::DEFAULT_UPLOAD_WORKERS)]
    pub snapshot_upload_workers: usize,
    /// Background detaches at once.
    #[arg(long, default_value_t = st::DEFAULT_DETACH_WORKERS)]
    pub snapshot_detach_workers: usize,
    /// Snapshot state reports at once.
    #[arg(long, default_value_t = st::DEFAULT_REPORT_WORKERS)]
    pub snapshot_report_workers: usize,
    /// How long a failed report is retried.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "10m")]
    pub snapshot_report_give_up_after: Duration,

    /// Shared snapshot file cache; empty disables caching.
    #[arg(long)]
    pub snapshot_cache_dir: Option<PathBuf>,
    /// Disable the shared snapshot file cache.
    #[arg(long)]
    pub no_snapshot_cache: bool,
    /// Cached snapshot files younger than this are never evicted.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "10m")]
    pub snapshot_cache_min_age: Duration,
    /// Bound on one cached snapshot file download.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "10m")]
    pub snapshot_cache_fetch_timeout: Duration,
    /// Snapshot cache eviction period; 0 disables it.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5m")]
    pub snapshot_cache_gc_period: Duration,
    /// Snapshot cache budget in bytes; 0 means none.
    #[arg(long, default_value_t = 0)]
    pub snapshot_cache_max_bytes: i64,
    /// Log snapshot cache eviction decisions without deleting.
    #[arg(long)]
    pub snapshot_cache_gc_dry_run: bool,

    /// Sandbox asset cache; on the actors dir's mount.
    #[arg(long)]
    pub sandbox_asset_cache_dir: Option<PathBuf>,
    /// Cached assets younger than this are never evicted.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "1h")]
    pub sandbox_asset_cache_min_age: Duration,
    /// Bound on one asset download.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5m")]
    pub sandbox_asset_fetch_timeout: Duration,
    /// Asset cache eviction period; 0 disables it.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5m")]
    pub sandbox_asset_cache_gc_period: Duration,
    /// Asset cache budget in bytes; 0 means none.
    #[arg(long, default_value_t = 0)]
    pub sandbox_asset_cache_max_bytes: i64,
    /// Log asset cache eviction decisions without deleting.
    #[arg(long)]
    pub sandbox_asset_cache_gc_dry_run: bool,

    /// Image cache eviction period; 0 disables it.
    #[arg(long, value_parser = humantime::parse_duration, default_value = "5m")]
    pub image_cache_gc_period: Duration,
    /// Volume usage percentage above which image eviction starts.
    #[arg(long, default_value_t = 85)]
    pub image_cache_high_percent: u32,
    /// Volume usage percentage image eviction frees down to.
    #[arg(long, default_value_t = 80)]
    pub image_cache_low_percent: u32,
    /// Absolute cap on cached layers in bytes; 0 means none.
    #[arg(long, default_value_t = 0)]
    pub image_cache_max_bytes: i64,
    /// Log image eviction decisions without deleting.
    #[arg(long)]
    pub image_cache_gc_dry_run: bool,
}

/// Refuses a set `HK_STORAGE_BACKEND`, whatever its value: only the
/// in-process object store (`--object-store=direct`) reads it, so a node
/// configured through it otherwise would not get the backend it names.
pub fn reject_storage_env(value: Option<String>) -> Result<(), String> {
    match value {
        None => Ok(()),
        Some(v) => Err(format!(
            "HK_STORAGE_BACKEND={v:?} is set on the keeper, which reads it only with --object-store=direct; configure the object store (its plugin) instead"
        )),
    }
}

impl Args {
    /// The node layout the flags describe.
    pub fn paths(&self) -> NodePaths {
        let p = NodePaths::new(&self.base_dir);
        match &self.snapshots_dir {
            Some(d) => p.with_snapshots_dir(d),
            None => p,
        }
    }

    /// The snapshot cache dir, or `None` when disabled.
    pub fn snapshot_cache_dir(&self) -> Option<PathBuf> {
        if self.no_snapshot_cache {
            return None;
        }
        Some(
            self.snapshot_cache_dir
                .clone()
                .unwrap_or_else(|| self.paths().snapshot_cache_dir()),
        )
    }

    /// The tracker's options.
    pub fn tracker_options(&self) -> st::Options {
        st::Options {
            upload_delay: self.snapshot_upload_delay,
            detach_delay: self.snapshot_detach_delay,
            upload_timeout: self.snapshot_upload_timeout,
            upload_est_throughput: self.snapshot_upload_est_throughput_mib << 20,
            upload_workers: self.snapshot_upload_workers,
            detach_workers: self.snapshot_detach_workers,
            report_workers: self.snapshot_report_workers,
            report_give_up_after: self.snapshot_report_give_up_after,
            ..st::Options::default()
        }
    }

    /// The snapshot cache's eviction settings.
    pub fn snapshot_cache_gc(&self) -> GcConfig {
        GcConfig {
            period: self.snapshot_cache_gc_period,
            max_bytes: self.snapshot_cache_max_bytes,
            dry_run: self.snapshot_cache_gc_dry_run,
        }
    }

    /// The asset cache's eviction settings.
    pub fn sandbox_asset_cache_gc(&self) -> GcConfig {
        GcConfig {
            period: self.sandbox_asset_cache_gc_period,
            max_bytes: self.sandbox_asset_cache_max_bytes,
            dry_run: self.sandbox_asset_cache_gc_dry_run,
        }
    }

    /// The object store the flags select.
    pub fn object_store_kind(&self) -> ObjectStoreKind {
        self.object_store
            .unwrap_or(if self.object_store_dir.is_some() {
                ObjectStoreKind::Local
            } else {
                ObjectStoreKind::Plugin
            })
    }

    /// The image layer cache's directory.
    pub fn image_cache_dir(&self) -> PathBuf {
        self.image_cache_dir
            .clone()
            .unwrap_or_else(|| self.paths().image_cache_dir())
    }

    /// The in-process asset staging directory.
    pub fn objectstore_asset_staging_dir(&self) -> PathBuf {
        self.objectstore_asset_staging_dir
            .clone()
            .unwrap_or_else(|| {
                let mut name = self.base_dir.file_name().unwrap_or_default().to_os_string();
                name.push("-asset-staging");
                self.base_dir.with_file_name(name)
            })
    }

    /// The image cache's eviction settings.
    pub fn image_cache_gc(&self) -> ImageGcConfig {
        ImageGcConfig {
            period: self.image_cache_gc_period,
            high_pct: self.image_cache_high_percent,
            low_pct: self.image_cache_low_percent,
            max_bytes: self.image_cache_max_bytes,
            dry_run: self.image_cache_gc_dry_run,
        }
    }

    /// Checks every flag, so a mistake fails at startup.
    pub fn validate(&self) -> Result<(), String> {
        if self.object_store_kind() != ObjectStoreKind::Direct {
            reject_storage_env(std::env::var("HK_STORAGE_BACKEND").ok())?;
        }
        self.validate_snapshot_delays()?;
        self.validate_snapshot_workers()?;
        if self.snapshot_cache_fetch_timeout.is_zero() {
            return Err("--snapshot-cache-fetch-timeout must be > 0".into());
        }
        if let Some(d) = self.snapshot_cache_dir() {
            if !self.paths().under_base(&d) {
                return Err(format!(
                    "--snapshot-cache-dir {} must be under {}, the only tree the object store writes to",
                    d.display(),
                    self.base_dir.display()
                ));
            }
        }
        if self.sandbox_asset_fetch_timeout.is_zero() {
            return Err("--sandbox-asset-fetch-timeout must be > 0".into());
        }
        self.snapshot_cache_gc().validate("snapshot-cache")?;
        self.sandbox_asset_cache_gc()
            .validate("sandbox-asset-cache")?;
        self.image_cache_gc().validate()?;
        if self.image_credential_provider_config.is_some()
            && self.image_credential_provider_bin_dir.is_none()
        {
            return Err("--image-credential-provider-bin-dir is required when --image-credential-provider-config is set".into());
        }
        self.validate_object_store()
    }

    fn validate_object_store(&self) -> Result<(), String> {
        match self.object_store_kind() {
            ObjectStoreKind::Local if self.object_store_dir.is_none() => {
                Err("--object-store=local requires --object-store-dir".into())
            }
            ObjectStoreKind::Plugin if self.objectstore_plugin_socket.as_os_str().is_empty() => {
                Err("--objectstore-plugin-socket must be set with --object-store=plugin".into())
            }
            ObjectStoreKind::Direct => {
                if self.objectstore_upload_memory_bytes <= 0 {
                    return Err("--objectstore-upload-memory-bytes must be > 0".into());
                }
                if self.objectstore_asset_staging_bytes <= 0 {
                    return Err("--objectstore-asset-staging-bytes must be > 0".into());
                }
                if self
                    .paths()
                    .under_base(&self.objectstore_asset_staging_dir())
                {
                    return Err(format!(
                        "--objectstore-asset-staging-dir must not be under {}",
                        self.base_dir.display()
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn validate_snapshot_delays(&self) -> Result<(), String> {
        if self.snapshot_upload_delay.is_zero() {
            return Err("--snapshot-upload-delay must be > 0".into());
        }
        if self.snapshot_detach_delay.is_zero() {
            return Err("--snapshot-detach-delay must be > 0".into());
        }
        if !(0..=MAX_UPLOAD_EST_MIBPS).contains(&self.snapshot_upload_est_throughput_mib) {
            return Err(format!(
                "--snapshot-upload-est-throughput-mib {} must be between 0 and {MAX_UPLOAD_EST_MIBPS}",
                self.snapshot_upload_est_throughput_mib
            ));
        }
        if self.snapshot_upload_delay > self.snapshot_detach_delay {
            return Err("--snapshot-upload-delay must not exceed --snapshot-detach-delay: a detach uploads anyway".into());
        }
        if self.snapshot_report_give_up_after.is_zero() {
            return Err("--snapshot-report-give-up-after must be > 0".into());
        }
        Ok(())
    }

    fn validate_snapshot_workers(&self) -> Result<(), String> {
        for (n, flag) in [
            (self.snapshot_upload_workers, "--snapshot-upload-workers"),
            (self.snapshot_detach_workers, "--snapshot-detach-workers"),
            (self.snapshot_report_workers, "--snapshot-report-workers"),
        ] {
            if n < 1 {
                return Err(format!("{flag} {n} must be >= 1"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(extra: &[&str]) -> Args {
        let mut v = vec!["hivekeep-keeper"];
        v.extend_from_slice(extra);
        Args::try_parse_from(v).unwrap()
    }

    #[test]
    fn defaults_validate() {
        let a = args(&[]);
        a.validate().unwrap();
        assert_eq!(
            a.snapshot_cache_dir().unwrap(),
            PathBuf::from("/var/lib/hivekeep/snapshot-cache")
        );
        assert_eq!(a.tracker_options().upload_est_throughput, 100 << 20);
    }

    #[test]
    fn rejects_bad_combinations() {
        assert!(args(&[
            "--snapshot-upload-delay",
            "2h",
            "--snapshot-detach-delay",
            "1h"
        ])
        .validate()
        .is_err());
        assert!(args(&["--snapshot-upload-workers", "0"])
            .validate()
            .is_err());
        assert!(args(&["--snapshot-upload-est-throughput-mib=-1"])
            .validate()
            .is_err());
        assert!(args(&["--snapshot-cache-dir", "/tmp/elsewhere"])
            .validate()
            .is_err());
        assert!(
            args(&["--no-snapshot-cache", "--snapshot-cache-dir", "/tmp/x"])
                .validate()
                .is_ok()
        );
        assert!(args(&["--image-cache-low-percent", "90"])
            .validate()
            .is_err());
        assert!(args(&["--image-credential-provider-config", "/c"])
            .validate()
            .is_err());
        assert!(args(&["--object-store", "local"]).validate().is_err());
        assert!(args(&[
            "--object-store",
            "direct",
            "--objectstore-asset-staging-dir",
            "/var/lib/hivekeep/staging"
        ])
        .validate()
        .is_err());
    }

    #[test]
    fn object_store_selection() {
        assert_eq!(args(&[]).object_store_kind(), ObjectStoreKind::Plugin);
        assert_eq!(
            args(&["--object-store-dir", "/tmp/o"]).object_store_kind(),
            ObjectStoreKind::Local
        );
        assert_eq!(
            args(&["--object-store", "direct"]).object_store_kind(),
            ObjectStoreKind::Direct
        );
        let a = args(&["--object-store", "direct"]);
        a.validate().unwrap();
        assert_eq!(
            a.objectstore_asset_staging_dir(),
            PathBuf::from("/var/lib/hivekeep-asset-staging")
        );
        assert_eq!(
            args(&[]).image_cache_dir(),
            PathBuf::from("/var/lib/hivekeep/image-cache")
        );
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod behavior_tests;
