//! Batch-validates that OCI images can be pulled, parsed and unpacked by
//! the image cache's keeper half: registry pull, parallel per-layer
//! streaming unpack, whiteout capture, record write, for every reference in
//! a file, with per-image results as CSV. It does not mount overlays or run
//! workloads; it answers "can this image be loaded into the cache".
//!
//! Disk is bounded by the cache's own eviction engine: below
//! `--min-free-gb` free space it asks for the shortfall back, and
//! `--evict-all` empties everything evictable and exits. Bundle specs in
//! the node's actors dir root placed actors' images. The engine's locks are
//! per process, so beside a live keeper only min-age protects images it is
//! about to mount, hence the floor on `--evict-idle` when an actors dir
//! exists. Run as the user owning the cache and actors dirs: an unreadable
//! actors dir gates every pass.

use std::fs;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _};
use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches, Parser};
use hivekeep_imagecache::error::ErrorKind;
use hivekeep_imagecache::keychain::Keychain;
use hivekeep_imagecache::manifest::Platform;
use hivekeep_imagecache::{Store, StoreOptions};
use hivekeep_node::context::Ctx;
use hivekeep_node::paths::{NodePaths, DEFAULT_BASE_DIR};
use rand::seq::SliceRandom;
use rand::SeedableRng;

/// Min-age is the only protection across processes, so on a live node it
/// cannot be tuned below this.
const LIVE_NODE_IDLE_FLOOR: Duration = Duration::from_secs(60);
/// How long a pass that freed nothing suppresses the next one.
const FRUITLESS_COOLDOWN: Duration = Duration::from_secs(30);
/// Flags valid with `--evict-all`; anything else fails closed.
const FLUSH_FLAGS: [&str; 4] = ["cache-dir", "evict-idle", "evict-all", "force"];

#[derive(Parser, Debug, Clone)]
#[command(
    name = "hivekeep-validate-image-cache",
    about = "Validate that images load into the node image cache"
)]
struct Args {
    #[command(flatten)]
    telemetry: hivekeep_telemetry::TelemetryArgs,
    /// File with one image reference per line (required unless --evict-all).
    #[arg(long = "refs-file", default_value = "")]
    refs_file: String,
    /// Validate a random sample of N references (0 = all).
    #[arg(long, default_value_t = 0)]
    sample: usize,
    /// Seed for reproducible sampling.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Cache root (required); reused across runs.
    #[arg(long = "cache-dir", default_value = "")]
    cache_dir: String,
    /// Results CSV path.
    #[arg(long = "out", default_value = "validate-results.csv")]
    out: PathBuf,
    /// Images validated concurrently (each pulls up to 4 layers at once).
    #[arg(long, default_value_t = 3)]
    parallel: usize,
    /// Per-image timeout.
    #[arg(long, default_value = "20m", value_parser = humantime::parse_duration)]
    timeout: Duration,
    /// Reclaim disk when the cache volume has less free space than this.
    #[arg(long = "min-free-gb", default_value_t = 150)]
    min_free_gb: u64,
    /// Eviction min-age: nothing younger is evicted. At least 1m on a node
    /// with an actors dir. Keep it well below the time the corpus takes to
    /// fill the disk, or nothing is evictable while it fills.
    #[arg(long = "evict-idle", default_value = "10m", value_parser = parse_signed_duration, allow_hyphen_values = true)]
    evict_idle: SignedDuration,
    /// Evict everything evictable and exit (no refs file). Rooted images and
    /// anything younger than --evict-idle survive. Needs --force on a node.
    #[arg(long = "evict-all")]
    evict_all: bool,
    /// Allow eviction on a node with an actors dir.
    #[arg(long)]
    force: bool,
    /// Image platform to pull.
    #[arg(long, default_value = "linux/amd64")]
    platform: String,
    /// kubelet credential provider config to authenticate pulls with.
    #[arg(long = "credential-provider-config")]
    credential_provider_config: Option<PathBuf>,
    /// Directory holding the credential provider binaries.
    #[arg(long = "credential-provider-bin-dir")]
    credential_provider_bin_dir: Option<PathBuf>,
    /// The node's base directory, whose actors dir roots images.
    #[arg(long = "base-dir", default_value = DEFAULT_BASE_DIR)]
    base_dir: PathBuf,
}

/// A duration that may be negative, so a negative min-age is rejected
/// with a clear message rather than a parse error.
#[derive(Debug, Clone, Copy, PartialEq)]
struct SignedDuration {
    negative: bool,
    abs: Duration,
}

fn parse_signed_duration(s: &str) -> Result<SignedDuration, String> {
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let abs = humantime::parse_duration(rest).map_err(|e| e.to_string())?;
    Ok(SignedDuration { negative, abs })
}

/// A configuration error that should print usage.
#[derive(Debug, PartialEq)]
enum ConfigError {
    Usage(String),
    Invalid(String),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Usage(m) => write!(f, "usage: {m}"),
            ConfigError::Invalid(m) => f.write_str(m),
        }
    }
}

/// The flag state validation checks, as a plain struct so the combination
/// matrix is table-testable.
#[derive(Debug, Clone, Default)]
struct RunConfig {
    cache_dir: String,
    refs_file: String,
    evict_all: bool,
    force: bool,
    evict_idle: Option<SignedDuration>,
    min_free_gb: u64,
    live: bool,
    actors_dir: String,
    set_flags: Vec<String>,
}

impl RunConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.cache_dir.is_empty()
            || (self.refs_file.is_empty() && !self.evict_all)
            || (!self.refs_file.is_empty() && self.evict_all)
        {
            return Err(ConfigError::Usage(
                "need --cache-dir plus exactly one of --refs-file or --evict-all".into(),
            ));
        }
        let idle = self.evict_idle.unwrap_or(SignedDuration {
            negative: false,
            abs: Duration::from_secs(600),
        });
        if idle.negative && !idle.abs.is_zero() {
            // A negative min-age moves the cutoff into the future, making
            // even in-flight pulls' layers evictable.
            return Err(ConfigError::Invalid(format!(
                "--evict-idle -{:?} must be >= 0",
                idle.abs
            )));
        }
        if self.min_free_gb > u64::MAX / 1_000_000_000 {
            return Err(ConfigError::Invalid(format!(
                "--min-free-gb {} overflows a byte count",
                self.min_free_gb
            )));
        }
        if self.evict_all {
            let stray: Vec<String> = self
                .set_flags
                .iter()
                .filter(|f| !FLUSH_FLAGS.contains(&f.as_str()))
                .map(|f| format!("--{f}"))
                .collect();
            if !stray.is_empty() {
                return Err(ConfigError::Usage(format!(
                    "{}: only valid with --refs-file",
                    stray.join(", ")
                )));
            }
        }
        if self.live {
            if idle.abs < LIVE_NODE_IDLE_FLOOR {
                return Err(ConfigError::Invalid(format!(
                    "--evict-idle={:?} is below {:?} with {} present: min-age is the only protection that applies across processes using the cache",
                    idle.abs, LIVE_NODE_IDLE_FLOOR, self.actors_dir
                )));
            }
            if !self.force {
                return Err(ConfigError::Invalid(format!(
                    "{} exists — this looks like a live node, and evictions are not synchronized with other processes using the cache; re-run with --force to proceed",
                    self.actors_dir
                )));
            }
        }
        Ok(())
    }
}

/// Whether `dir` (the node's actors dir) exists. Anything but "not found"
/// counts: an unreadable actors dir is still a node.
fn looks_like_live_node(dir: &Path) -> bool {
    match fs::metadata(dir) {
        Ok(_) => true,
        Err(e) => e.kind() != std::io::ErrorKind::NotFound,
    }
}

/// Free bytes on the volume holding `path`; `None` when unknown.
fn free_bytes(path: &Path) -> Option<u64> {
    hivekeep_node::image_gc::statfs(path)
        .ok()
        .map(|(_, avail)| avail)
}

/// Free space for logs, naming the unknown case instead of printing a
/// sentinel.
fn free_gb(path: &Path) -> String {
    match free_bytes(path) {
        None => "unknown".to_string(),
        Some(b) => format!("{:.0} GB", b as f64 / 1e9),
    }
}

fn load_refs(path: &Path) -> anyhow::Result<Vec<String>> {
    let f = fs::File::open(path).with_context(|| format!("opening {path:?}"))?;
    let mut refs = Vec::new();
    for line in std::io::BufReader::new(f).lines() {
        let line = line?;
        let t = line.trim();
        if !t.is_empty() {
            refs.push(t.to_string());
        }
    }
    Ok(refs)
}

fn short_ref(r: &str) -> &str {
    r.rsplit_once('/').map_or(r, |(_, s)| s)
}

fn csv_field(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// One eviction attempt per low-water episode; a pass that freed nothing
/// backs off so queued workers do not each run a full pass.
struct LowWater {
    store: Store,
    cache_dir: PathBuf,
    min_free: u64,
    last_fruitless: Option<Instant>,
}

impl LowWater {
    fn evict_if_low(&mut self) {
        let Some(free) = free_bytes(&self.cache_dir) else {
            return;
        };
        if free >= self.min_free {
            return;
        }
        if self
            .last_fruitless
            .is_some_and(|t| t.elapsed() < FRUITLESS_COOLDOWN)
        {
            return;
        }
        // Not capped at the pool size: on a dedicated disk an oversized
        // target just runs out of candidates.
        let (stats, err) =
            self.store
                .evict_unused(&Ctx::background(), (self.min_free - free) as i64, false);
        match err {
            Some(e) if e.is(ErrorKind::IncompleteEnumeration) => {
                tracing::warn!(err = %e, "eviction pass skipped, nothing attempted")
            }
            Some(e) => tracing::warn!(err = %e, "eviction pass finished with errors"),
            None => {}
        }
        if stats.evicted_images > 0 || stats.evicted_layers > 0 {
            tracing::info!(
                "evicted {} images / {} layers, {:.1} GB credited (free now {})",
                stats.evicted_images,
                stats.evicted_layers,
                stats.freed_bytes as f64 / 1e9,
                free_gb(&self.cache_dir)
            );
        } else {
            self.last_fruitless = Some(Instant::now());
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The dependency graph carries two rustls providers; pick one.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let matches = Args::command().get_matches();
    let args = Args::from_arg_matches(&matches)?;
    let _telemetry = hivekeep_telemetry::init(
        hivekeep_telemetry::InitOptions::from_args(
            "hivekeep-validate-image-cache",
            &args.telemetry,
        )
        .metrics(hivekeep_telemetry::MetricsMode::PushOnly)
        .stderr(),
    )?;
    let set_flags: Vec<String> = matches
        .ids()
        .filter(|id| matches.value_source(id.as_str()) == Some(ValueSource::CommandLine))
        // The shared logging flags apply in every mode.
        .filter(|id| !matches!(id.as_str(), "log_level" | "log_format" | "otlp_endpoint"))
        .map(|id| match id.as_str() {
            "refs_file" => "refs-file".to_string(),
            "cache_dir" => "cache-dir".to_string(),
            "min_free_gb" => "min-free-gb".to_string(),
            "evict_idle" => "evict-idle".to_string(),
            "evict_all" => "evict-all".to_string(),
            "credential_provider_config" => "credential-provider-config".to_string(),
            "credential_provider_bin_dir" => "credential-provider-bin-dir".to_string(),
            "base_dir" => "base-dir".to_string(),
            other => other.replace('_', "-"),
        })
        .collect();
    let actors_dir = NodePaths::new(&args.base_dir).actors_dir();
    let cfg = RunConfig {
        cache_dir: args.cache_dir.clone(),
        refs_file: args.refs_file.clone(),
        evict_all: args.evict_all,
        force: args.force,
        evict_idle: Some(args.evict_idle),
        min_free_gb: args.min_free_gb,
        live: looks_like_live_node(&actors_dir),
        actors_dir: actors_dir.display().to_string(),
        set_flags,
    };
    if let Err(e) = cfg.validate() {
        eprintln!("{e}");
        if matches!(e, ConfigError::Usage(_)) {
            Args::command().print_help()?;
            std::process::exit(2);
        }
        std::process::exit(1);
    }

    let cache_dir = PathBuf::from(&args.cache_dir);
    let mut opts = StoreOptions::default()
        .with_min_age(args.evict_idle.abs)
        .with_actors_dir(&actors_dir);

    if args.evict_all {
        // Flush mode: no refs, no registry auth. Opening also reclaims any
        // crash-debris orphans.
        let store = Store::open(&cache_dir, opts).context("opening cache")?;
        let (stats, err) = tokio::task::spawn_blocking(move || {
            store.evict_unused(&Ctx::background(), i64::MAX, false)
        })
        .await?;
        if let Some(e) = &err {
            if e.is(ErrorKind::IncompleteEnumeration) {
                bail!("evict-all did nothing: {e}");
            }
            tracing::warn!(err = %e, "evict-all finished with errors");
        }
        tracing::info!(
            "evict-all: {} images / {} layers evicted, {:.1} GB credited (free now {})",
            stats.evicted_images,
            stats.evicted_layers,
            stats.freed_bytes as f64 / 1e9,
            free_gb(&cache_dir)
        );
        if err.is_some() {
            std::process::exit(1);
        }
        return Ok(());
    }

    let mut refs = load_refs(Path::new(&args.refs_file)).context("loading refs")?;
    if args.sample > 0 && args.sample < refs.len() {
        refs.shuffle(&mut rand::rngs::StdRng::seed_from_u64(args.seed));
        refs.truncate(args.sample);
    }
    let (os, arch) = args
        .platform
        .split_once('/')
        .with_context(|| format!("invalid --platform {:?}", args.platform))?;
    opts = opts.with_platform(Platform {
        os: os.to_string(),
        architecture: arch.to_string(),
        variant: String::new(),
    });
    if let Some(config) = &args.credential_provider_config {
        let bin_dir = args
            .credential_provider_bin_dir
            .clone()
            .context("--credential-provider-config needs --credential-provider-bin-dir")?;
        let kc = hivekeep_node::credential_provider::Keychain::new(config, &bin_dir)
            .map_err(anyhow::Error::msg)?;
        opts = opts.with_keychain(Arc::new(kc) as Arc<dyn Keychain>);
    }
    let store = Store::open(&cache_dir, opts).context("opening cache")?;
    tracing::info!(
        "validating {} images (parallel={}, cache={})",
        refs.len(),
        args.parallel,
        cache_dir.display()
    );

    let out = Arc::new(parking_lot::Mutex::new(
        fs::File::create(&args.out).with_context(|| format!("creating {:?}", args.out))?,
    ));
    writeln!(out.lock(), "ref,digest,layers,seconds,error")?;
    let low = Arc::new(parking_lot::Mutex::new(LowWater {
        store: store.clone(),
        cache_dir: cache_dir.clone(),
        min_free: args.min_free_gb * 1_000_000_000,
        last_fruitless: None,
    }));
    let sem = Arc::new(tokio::sync::Semaphore::new(args.parallel.max(1)));
    let counts = Arc::new(parking_lot::Mutex::new((0usize, 0usize)));
    let total = refs.len();
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for r in refs {
        let permit = sem.clone().acquire_owned().await?;
        let (store, out, low, counts, timeout) = (
            store.clone(),
            out.clone(),
            low.clone(),
            counts.clone(),
            args.timeout,
        );
        tasks.spawn(async move {
            let _permit = permit;
            let l = low.clone();
            let _ = tokio::task::spawn_blocking(move || l.lock().evict_if_low()).await;
            let t = Instant::now();
            let ctx = Ctx::background().with_timeout(timeout);
            let res = store.ensure_image(&ctx, &r).await;
            let took = t.elapsed();
            let (digest, layers, err) = match &res {
                Ok(img) => (img.digest.to_string(), img.layer_dirs.len(), String::new()),
                Err(e) => (String::new(), 0, e.to_string()),
            };
            let _ = writeln!(
                out.lock(),
                "{},{},{},{:.1},{}",
                csv_field(&r),
                csv_field(&digest),
                layers,
                took.as_secs_f64(),
                csv_field(&err)
            );
            let mut c = counts.lock();
            c.0 += 1;
            if !err.is_empty() {
                c.1 += 1;
                tracing::warn!("FAIL [{}/{}] {}: {}", c.0, total, r, err);
            } else if c.0 % 10 == 0 || c.0 == total {
                let elapsed = started.elapsed();
                let eta = elapsed.mul_f64((total - c.0) as f64 / c.0 as f64);
                tracing::info!(
                    "ok [{}/{}] fails={} elapsed={}s eta={}m (last: {} in {:.0}s, {} layers)",
                    c.0,
                    total,
                    c.1,
                    elapsed.as_secs(),
                    eta.as_secs() / 60,
                    short_ref(&r),
                    took.as_secs_f64(),
                    layers
                );
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    let (done, fails) = *counts.lock();
    tracing::info!(
        "done: {done} images, {fails} failures, {}s total; results in {}",
        started.elapsed().as_secs(),
        args.out.display()
    );
    if fails > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idle(s: &str) -> Option<SignedDuration> {
        Some(parse_signed_duration(s).unwrap())
    }

    #[test]
    fn run_config_validate() {
        let valid = RunConfig {
            cache_dir: "/c".into(),
            refs_file: "r.txt".into(),
            evict_idle: idle("10m"),
            min_free_gb: 150,
            actors_dir: "/var/lib/hivekeep/actors".into(),
            ..Default::default()
        };
        type Mutate = fn(&mut RunConfig);
        let cases: Vec<(&str, Mutate, &str, bool)> = vec![
            ("refs mode defaults", |_| {}, "", false),
            (
                "missing cache-dir",
                |c| c.cache_dir.clear(),
                "exactly one",
                true,
            ),
            ("neither mode", |c| c.refs_file.clear(), "exactly one", true),
            ("both modes", |c| c.evict_all = true, "exactly one", true),
            (
                "negative idle rejected everywhere",
                |c| c.evict_idle = idle("-1h"),
                "must be >= 0",
                false,
            ),
            (
                "low idle fine on a validation host",
                |c| c.evict_idle = idle("10s"),
                "",
                false,
            ),
            (
                "low idle floored on a live node",
                |c| {
                    c.evict_idle = idle("10s");
                    c.live = true;
                    c.force = true;
                },
                "is below",
                false,
            ),
            (
                "live node refuses without force",
                |c| c.live = true,
                "--force",
                false,
            ),
            (
                "live node with force",
                |c| {
                    c.live = true;
                    c.force = true;
                },
                "",
                false,
            ),
            (
                "live flush refuses without force",
                |c| {
                    c.refs_file.clear();
                    c.evict_all = true;
                    c.live = true;
                },
                "--force",
                false,
            ),
            (
                "flush with a refs-mode flag",
                |c| {
                    c.refs_file.clear();
                    c.evict_all = true;
                    c.set_flags = vec!["cache-dir".into(), "evict-all".into(), "parallel".into()];
                },
                "only valid with --refs-file",
                true,
            ),
            (
                "flush with its own flags",
                |c| {
                    c.refs_file.clear();
                    c.evict_all = true;
                    c.set_flags = FLUSH_FLAGS.iter().map(|s| s.to_string()).collect();
                },
                "",
                false,
            ),
            (
                "future flags fail closed",
                |c| {
                    c.refs_file.clear();
                    c.evict_all = true;
                    c.set_flags = vec!["some-new-flag".into()];
                },
                "only valid with --refs-file",
                true,
            ),
            (
                "min-free-gb overflow",
                |c| c.min_free_gb = u64::MAX,
                "overflows",
                false,
            ),
        ];
        for (name, mutate, want, usage) in cases {
            let mut c = valid.clone();
            mutate(&mut c);
            match c.validate() {
                Ok(()) => assert!(want.is_empty(), "{name}: want an error containing {want:?}"),
                Err(e) => {
                    assert!(!want.is_empty(), "{name}: unexpected error {e}");
                    assert!(e.to_string().contains(want), "{name}: {e}");
                    assert_eq!(matches!(e, ConfigError::Usage(_)), usage, "{name}: usage");
                }
            }
        }
    }

    #[test]
    fn looks_like_live_node_cases() {
        let dir = tempfile::tempdir().unwrap();
        assert!(looks_like_live_node(dir.path()));
        assert!(!looks_like_live_node(&dir.path().join("missing")));
    }

    #[test]
    fn free_gb_names_the_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(free_gb(&dir.path().join("missing")), "unknown");
        assert_ne!(free_gb(dir.path()), "unknown");
    }

    #[test]
    fn csv_quoting() {
        assert_eq!(csv_field("plain"), "plain");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(short_ref("gcr.io/p/img@sha256:x"), "img@sha256:x");
    }
}
