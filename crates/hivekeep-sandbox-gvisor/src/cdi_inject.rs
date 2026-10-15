//! Applying CDI container edits to an Actor's OCI bundle, the gVisor way.
//!
//! This is specific to a sandbox on the host kernel: it rewrites the bundle's
//! `config.json` on disk and resolves device numbers by stat'ing the host. A
//! micro-VM would read the same [`crate::cdi`] spec and apply it its own way
//! (passing the device through first).
//!
//! Everything that comes from outside the worker is confined: device nodes
//! must sit under the device root, mount sources under an allowed host
//! directory, and writes into the rootfs go through [`ConfinedDir`] so an
//! image cannot redirect them with symlinks.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use serde_json::{json, Map, Value};

use crate::cdi::{self, Spec};
use crate::confined_dir::{self, ConfinedDir};
use crate::elf_soname;
use crate::error::{Error, Result};

/// The host directory device nodes must sit under.
pub const DEV_ROOT: &str = "/dev";

/// What an injection is allowed to do. Everything vendor specific lives
/// here rather than in the merge.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The CDI devices to apply.
    pub devices: Vec<String>,
    /// The `createContainer` hooks that may run, by their first argument. An
    /// allowlist because the generator is usually the cluster's own toolkit,
    /// whose newer versions may emit hooks nobody reviewed against the
    /// sandbox's posture.
    pub allowed_hooks: HashSet<String>,
    /// Replaces the hook binary the spec names, so hooks run from a binary
    /// the caller trusts.
    pub hook_binary: String,
    /// The host directories CDI mounts may come from. A mount's host path
    /// must resolve inside one of them without escaping through symlinks.
    pub allowed_host_dirs: Vec<String>,
    /// Prepended to the container's `LD_LIBRARY_PATH`.
    pub library_dirs: Vec<String>,
    /// Environment variables removed from the container.
    pub drop_env: Vec<String>,
}

/// Reads a library's SONAME: the allowed directory it was found under and
/// its path relative to it.
pub type SonameReader = dyn Fn(&Path, &Path) -> Result<Option<String>> + Send + Sync;

/// The host the injection reads: where device nodes live and how SONAMEs
/// are read. Replaceable for tests.
pub struct Host<'a> {
    /// The device root.
    pub dev_root: PathBuf,
    /// The SONAME reader.
    pub read_soname: &'a SonameReader,
}

impl Default for Host<'_> {
    fn default() -> Self {
        Host {
            dev_root: PathBuf::from(DEV_ROOT),
            read_soname: &elf_soname_in,
        }
    }
}

/// A mount added to the bundle, as the SONAME staging sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BundleMount {
    /// The host path.
    pub source: String,
    /// The path in the container.
    pub destination: String,
}

/// Merges the CDI spec's edits for `opts.devices` into `bundle_dir`'s
/// `config.json`: device nodes (numbers resolved from the host), mounts,
/// env and the allowlisted hooks. A bundle that already carries the
/// injected device nodes is left alone, since injecting twice would double
/// every entry.
pub fn into_bundle(bundle_dir: &Path, spec: &Spec, opts: &Options) -> Result<()> {
    into_bundle_on(bundle_dir, spec, opts, &Host::default())
}

/// [`into_bundle`] against `host`.
pub fn into_bundle_on(
    bundle_dir: &Path,
    spec: &Spec,
    opts: &Options,
    host: &Host<'_>,
) -> Result<()> {
    let edits = spec.edits_for(&opts.devices)?;
    if edits.device_nodes.is_empty() {
        return Err(Error::msg("CDI spec resolved no devices"));
    }
    let cfg_path = bundle_dir.join("config.json");
    let data = std::fs::read(&cfg_path)
        .map_err(|e| Error::io(format!("reading {}", cfg_path.display()), e))?;
    let mut oci: Value =
        serde_json::from_slice(&data).map_err(|e| Error::msg(format!("parsing OCI spec: {e}")))?;
    let root = oci
        .as_object_mut()
        .ok_or_else(|| Error::msg("parsing OCI spec: not an object"))?;
    if has_injected_device(root, &edits.device_nodes) {
        tracing::info!(bundle = %bundle_dir.display(), "Bundle already has CDI devices; skipping injection");
        return Ok(());
    }

    let mut devices = Vec::new();
    let mut allows = Vec::new();
    for dn in &edits.device_nodes {
        let (major, minor) = resolve_dev_numbers(&host.dev_root, &dn.path, dn.major, dn.minor)?;
        // Generators leave the type out for char devices; runsc needs it.
        let kind = if dn.kind.is_empty() { "c" } else { &dn.kind };
        devices.push(json!({"path": dn.path, "type": kind, "major": major, "minor": minor}));
        allows.push(
            json!({"allow": true, "type": kind, "major": major, "minor": minor, "access": "rwm"}),
        );
    }
    let linux = object(root, "linux");
    array(linux, "devices").extend(devices);
    array(object(linux, "resources"), "devices").extend(allows);

    let mut staged = Vec::with_capacity(edits.mounts.len());
    for m in &edits.mounts {
        validate_mount(m, &opts.allowed_host_dirs)?;
        // CDI leaves the type out of its bind mounts; runsc's gofer needs it.
        let kind = if m.kind.is_empty() { "bind" } else { &m.kind };
        let mut mount =
            json!({"destination": m.container_path, "type": kind, "source": m.host_path});
        if !m.options.is_empty() {
            mount["options"] = json!(m.options);
        }
        array(root, "mounts").push(mount);
        staged.push(BundleMount {
            source: m.host_path.clone(),
            destination: m.container_path.clone(),
        });
    }

    if let Some(process) = root.get_mut("process").and_then(Value::as_object_mut) {
        let mut env: Vec<String> = process
            .get("env")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        env.extend(edits.env.iter().cloned());
        for key in &opts.drop_env {
            env = drop_env_var(env, key);
        }
        env = prepend_library_path(env, &opts.library_dirs);
        process.insert("env".into(), json!(env));
    }

    let hooks = object(root, "hooks");
    for h in &edits.hooks {
        if h.hook_name != "createContainer" || h.args.len() < 2 {
            continue;
        }
        if !opts.allowed_hooks.contains(&h.args[1]) {
            tracing::info!(hook = %h.args[1], "Skipping CDI hook outside the allowlist");
            continue;
        }
        let binary = if opts.hook_binary.is_empty() {
            &h.path
        } else {
            &opts.hook_binary
        };
        let mut args = vec![binary.clone()];
        args.extend(h.args[1..].iter().cloned());
        let mut hook = json!({"path": binary, "args": args});
        if !h.env.is_empty() {
            hook["env"] = json!(h.env);
        }
        array(hooks, "createContainer").push(hook);
    }

    let rootfs = root
        .get("root")
        .and_then(|r| r.get("path"))
        .and_then(Value::as_str)
        .filter(|p| !p.is_empty())
        .unwrap_or("rootfs");
    let rootfs = bundle_dir.join(rootfs);
    stage_soname_symlinks_with(&rootfs, &staged, &opts.allowed_host_dirs, host.read_soname)
        .map_err(|e| Error::msg(format!("staging SONAME symlinks: {e}")))?;

    let out =
        serde_json::to_vec(&oci).map_err(|e| Error::msg(format!("serializing OCI spec: {e}")))?;
    std::fs::write(&cfg_path, out)
        .map_err(|e| Error::io(format!("writing {}", cfg_path.display()), e))
}

/// The object at `key`, made if missing or not an object.
fn object<'a>(m: &'a mut Map<String, Value>, key: &str) -> &'a mut Map<String, Value> {
    let v = m.entry(key).or_insert_with(|| json!({}));
    if !v.is_object() {
        *v = json!({});
    }
    v.as_object_mut().expect("just made an object")
}

/// The array at `key`, made if missing or not an array.
fn array<'a>(m: &'a mut Map<String, Value>, key: &str) -> &'a mut Vec<Value> {
    let v = m.entry(key).or_insert_with(|| json!([]));
    if !v.is_array() {
        *v = json!([]);
    }
    v.as_array_mut().expect("just made an array")
}

/// Whether the spec already carries one of the device nodes about to be
/// injected: how an injected bundle is recognised.
fn has_injected_device(root: &Map<String, Value>, nodes: &[cdi::DeviceNode]) -> bool {
    root.get("linux")
        .and_then(|l| l.get("devices"))
        .and_then(Value::as_array)
        .is_some_and(|devs| {
            devs.iter().any(|d| {
                let p = d.get("path").and_then(Value::as_str);
                nodes.iter().any(|n| p == Some(n.path.as_str()))
            })
        })
}

/// Lexically cleans a slash-separated path: no empty, `.` or resolvable
/// `..` components and no trailing slash.
pub fn clean(p: &str) -> String {
    let abs = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for c in p.split('/') {
        match c {
            "" | "." => {}
            ".." => {
                if out.last().is_some_and(|l| *l != "..") {
                    out.pop();
                } else if !abs {
                    out.push("..");
                }
            }
            c => out.push(c),
        }
    }
    let joined = out.join("/");
    match (abs, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".into(),
        (false, false) => joined,
    }
}

fn is_clean_abs(p: &str) -> bool {
    p.starts_with('/') && clean(p) == p
}

/// `path` below `dir`, both clean and absolute; `None` when it is not
/// strictly below.
fn below(dir: &str, path: &str) -> Option<String> {
    let rest = if dir == "/" {
        path.strip_prefix('/')?
    } else {
        path.strip_prefix(dir)?.strip_prefix('/')?
    };
    (!rest.is_empty()).then(|| rest.to_string())
}

/// The major and minor numbers of a device's `st_rdev`.
fn dev_numbers(rdev: u64) -> (i64, i64) {
    #[cfg(target_os = "linux")]
    {
        let major = ((rdev >> 8) & 0xfff) | ((rdev >> 32) & !0xfff);
        let minor = (rdev & 0xff) | ((rdev >> 12) & !0xff);
        (major as i64, minor as i64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        (((rdev >> 24) & 0xff) as i64, (rdev & 0xff_ffff) as i64)
    }
}

/// Checks that `path` is a clean device path confined to `dev_root` and
/// fills in its numbers from the host when the spec left them out. CDI
/// leaves that stat to the OCI runtime; the merge happens here, so the stat
/// does too, or the container would get 0,0 devices that reach no driver.
pub fn resolve_dev_numbers(
    dev_root: &Path,
    path: &str,
    major: i64,
    minor: i64,
) -> Result<(i64, i64)> {
    let root = clean(&dev_root.to_string_lossy());
    if !is_clean_abs(path) {
        return Err(Error::msg(format!(
            "CDI device path {path:?} must be a clean absolute path"
        )));
    }
    let rel = below(&root, path)
        .ok_or_else(|| Error::msg(format!("CDI device path {path:?} is outside {root}")))?;
    let dir = ConfinedDir::open(Path::new(&root))
        .map_err(|e| Error::io(format!("opening device root {root}"), e))?;
    let st = dir.stat(Path::new(&rel));
    let not_a_device = || Error::msg(format!("CDI device path {path} is not a device node"));
    if major != 0 {
        return match st {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                Err(Error::io(format!("stat device {path}"), e))
            }
            Ok(st) if !confined_dir::is_device(&st) => Err(not_a_device()),
            _ => Ok((major, minor)),
        };
    }
    let st = st.map_err(|e| Error::io(format!("stat device {path}"), e))?;
    if !confined_dir::is_device(&st) {
        return Err(not_a_device());
    }
    #[allow(clippy::unnecessary_cast)]
    Ok(dev_numbers(st.st_rdev as u64))
}

/// Checks that a CDI mount has a clean, rootfs-local container path and a
/// host path confined to an allowed directory.
fn validate_mount(m: &cdi::Mount, allowed: &[String]) -> Result<()> {
    if !is_clean_abs(&m.container_path) {
        return Err(Error::msg(format!(
            "CDI mount containerPath {:?} must be a clean absolute path",
            m.container_path
        )));
    }
    if m.container_path == "/" {
        return Err(Error::msg(format!(
            "CDI mount containerPath {:?} escapes the rootfs",
            m.container_path
        )));
    }
    resolve_allowed_host_path(&m.host_path, allowed).map(drop)
}

/// Checks that `host_path` is a clean absolute path inside one of
/// `allowed` that resolves there without leaving it through symlinks.
/// Returns the matching directory and the path relative to it.
pub fn resolve_allowed_host_path(
    host_path: &str,
    allowed: &[String],
) -> Result<(PathBuf, PathBuf)> {
    if !is_clean_abs(host_path) {
        return Err(Error::msg(format!(
            "CDI mount hostPath {host_path:?} must be a clean absolute path"
        )));
    }
    for dir in allowed.iter().filter(|d| !d.is_empty()) {
        let dir = clean(dir);
        if !dir.starts_with('/') {
            continue;
        }
        let Some(rel) = below(&dir, host_path) else {
            continue;
        };
        let root = ConfinedDir::open(Path::new(&dir))
            .map_err(|e| Error::io(format!("opening allowed host dir {dir:?}"), e))?;
        root.stat(Path::new(&rel)).map_err(|e| {
            Error::io(
                format!("CDI mount hostPath {host_path:?} invalid in allowed dir {dir:?}"),
                e,
            )
        })?;
        return Ok((PathBuf::from(dir), PathBuf::from(rel)));
    }
    Err(Error::msg(format!(
        "CDI mount hostPath {host_path:?} is not under any allowed host directory"
    )))
}

/// `env` without any `key=...` entry.
pub fn drop_env_var(env: Vec<String>, key: &str) -> Vec<String> {
    let prefix = format!("{key}=");
    env.into_iter()
        .filter(|e| !e.starts_with(&prefix))
        .collect()
}

/// Puts `dirs` at the front of `LD_LIBRARY_PATH`, keeping what the image
/// set, so an image without one can still find the injected driver
/// libraries (the stand-in for not running the loader-cache hook).
/// Directories already on the path are left where they are. It is weaker
/// than a loader cache (inherited by children, outranks DT_RUNPATH), so
/// callers pass only the directory holding the libraries.
pub fn prepend_library_path(env: Vec<String>, dirs: &[String]) -> Vec<String> {
    if dirs.is_empty() {
        return env;
    }
    // OCI semantics: a later entry wins.
    let existing = env
        .iter()
        .rev()
        .find_map(|e| e.strip_prefix("LD_LIBRARY_PATH="))
        .unwrap_or("")
        .to_string();
    let have: HashSet<&str> = existing.split(':').collect();
    let add: Vec<&str> = dirs
        .iter()
        .map(String::as_str)
        .filter(|d| !have.contains(d))
        .collect();
    if add.is_empty() {
        return env;
    }
    let mut val = add.join(":");
    if !existing.is_empty() {
        val.push(':');
        val.push_str(&existing);
    }
    let mut env = drop_env_var(env, "LD_LIBRARY_PATH");
    env.push(format!("LD_LIBRARY_PATH={val}"));
    env
}

/// Writes each driver library's SONAME symlink (`libcuda.so.1 ->
/// libcuda.so.580.65.06`) into `rootfs` next to its mount destination, so
/// programs linked against the SONAME resolve it once the library is bind
/// mounted there. A library whose SONAME cannot be read is skipped with a
/// warning rather than failing the Actor; the missing link would otherwise
/// surface much later as a loader error with nothing pointing here.
pub fn stage_soname_symlinks(
    rootfs: &Path,
    mounts: &[BundleMount],
    allowed: &[String],
) -> Result<()> {
    stage_soname_symlinks_with(rootfs, mounts, allowed, &elf_soname_in)
}

/// [`stage_soname_symlinks`] with a given SONAME reader.
pub fn stage_soname_symlinks_with(
    rootfs: &Path,
    mounts: &[BundleMount],
    allowed: &[String],
    read_soname: &SonameReader,
) -> Result<()> {
    // Every component under the rootfs comes from the Actor's image.
    let root = ConfinedDir::open(rootfs)
        .map_err(|e| Error::io(format!("while opening rootfs {:?}", rootfs.display()), e))?;
    for m in mounts {
        let dest = Path::new(&m.destination);
        let Some(base) = dest.file_name().and_then(|b| b.to_str()) else {
            continue;
        };
        // Only shared libraries (.../lib.../foo.so.<version>).
        if !base.contains(".so.") || m.source.is_empty() {
            continue;
        }
        let (host_root, rel) = resolve_allowed_host_path(&m.source, allowed)?;
        let soname = match read_soname(&host_root, &rel) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(library = %m.source, err = %e, "Skipping SONAME symlink for driver library");
                continue;
            }
        };
        let Some(soname) = soname.filter(|s| !s.is_empty() && s != base) else {
            continue;
        };
        // Read out of the library and joined into a path: a bare file name
        // or nothing.
        if soname.contains('/') || soname == "." || soname == ".." {
            tracing::warn!(library = %m.source, soname = %soname, "Skipping driver library whose SONAME is not a filename");
            continue;
        }
        let dir = m
            .destination
            .rsplit_once('/')
            .map(|(d, _)| d.trim_start_matches('/'))
            .unwrap_or("");
        if dir.split('/').any(|c| c == "..") {
            return Err(Error::msg(format!(
                "driver mount {:?} escapes the rootfs",
                m.destination
            )));
        }
        root.create_dir_all(Path::new(dir), 0o755)
            .map_err(|e| Error::io(format!("mkdir {dir}"), e))?;
        let link = Path::new(dir).join(&soname);
        let _ = root.remove_file(&link);
        root.symlink(Path::new(base), &link)
            .map_err(|e| Error::io(format!("symlink {} -> {base}", link.display()), e))?;
    }
    Ok(())
}

/// Reads the SONAME of `rel` inside `root_dir`, opened so that symlinks
/// cannot leave the directory. `None` for a library without one.
pub fn elf_soname_in(root_dir: &Path, rel: &Path) -> Result<Option<String>> {
    let shown = || format!("{}/{}", root_dir.display(), rel.display());
    let root = ConfinedDir::open(root_dir).map_err(|e| {
        Error::io(
            format!("opening allowed host dir {}", root_dir.display()),
            e,
        )
    })?;
    let f = root
        .open_file(rel)
        .map_err(|e| Error::io(format!("opening ELF {}", shown()), e))?;
    let meta = f
        .metadata()
        .map_err(|e| Error::io(format!("stat ELF {}", shown()), e))?;
    if !meta.is_file() {
        return Err(Error::msg(format!("ELF {} is not a regular file", shown())));
    }
    elf_soname::soname_of_file(f).map_err(|e| Error::msg(format!("parsing ELF {}: {e}", shown())))
}

#[cfg(test)]
mod tests;
