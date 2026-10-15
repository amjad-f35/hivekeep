//! Building and running runsc command lines.
//!
//! [`RunscCommand`] turns an operation into argv; [`ContainerRuntime`] is the
//! set of runsc operations the worker uses, implemented by [`Runsc`] on top of
//! a [`CommandRunner`] so either layer can be replaced in tests.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use async_trait::async_trait;
use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1 as pb;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio_util::sync::CancellationToken;

use crate::actor_path::{oci_bundle_path, pid_file_path, runsc_state_dir};
use crate::error::{Context, Error, Result};
use crate::flags::FlagStore;
use crate::oci::{self, SANDBOX_ID};
use crate::sizing::SandboxSize;

/// Where a runsc command's output goes.
#[derive(Debug, Clone)]
pub enum Output {
    /// The worker's own stdout and stderr.
    Inherit,
    /// Logged line by line, attributed to an Actor's container.
    Actor {
        /// The Actor.
        attribution: ActorAttribution,
        /// The container (or the sandbox).
        container: String,
    },
}

/// Runs one command to completion.
#[async_trait]
pub trait CommandRunner: Send + Sync {
    /// Runs `program args`, sending its output to `output`, and returns its
    /// stdout when `capture` is set. Cancelling `cancel` kills the process.
    async fn run(
        &self,
        program: &str,
        args: &[String],
        output: &Output,
        capture: bool,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>>;
}

/// Runs commands as child processes.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessRunner;

#[async_trait]
impl CommandRunner for ProcessRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        output: &Output,
        capture: bool,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>> {
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let mut cmd = tokio::process::Command::new(program);
        cmd.args(args).stdin(Stdio::null()).kill_on_drop(true);
        let piped = capture || matches!(output, Output::Actor { .. });
        if piped {
            cmd.stdout(Stdio::piped());
        }
        if matches!(output, Output::Actor { .. }) {
            cmd.stderr(Stdio::piped());
        }
        let mut child = cmd
            .spawn()
            .map_err(|e| Error::io(format!("starting {program}"), e))?;
        let mut forwarders = Vec::new();
        let mut captured = None;
        if let Some(out) = child.stdout.take() {
            if capture {
                captured = Some(tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut out = out;
                    let _ = tokio::io::AsyncReadExt::read_to_end(&mut out, &mut buf).await;
                    buf
                }));
            } else {
                forwarders.push(tokio::spawn(forward_lines(out, output.clone())));
            }
        }
        if let Some(err) = child.stderr.take() {
            forwarders.push(tokio::spawn(forward_lines(err, output.clone())));
        }
        let status = tokio::select! {
            s = child.wait() => s.map_err(|e| Error::io(format!("waiting for {program}"), e))?,
            _ = cancel.cancelled() => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                return Err(Error::Cancelled);
            }
        };
        for f in forwarders {
            let _ = f.await;
        }
        let stdout = match captured {
            Some(c) => c.await.unwrap_or_default(),
            None => Vec::new(),
        };
        if status.success() {
            Ok(stdout)
        } else {
            Err(Error::msg(status.to_string()))
        }
    }
}

/// One line of runsc output, as it is logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunscLine {
    /// The level runsc logged at; info for plain lines.
    pub level: tracing::Level,
    /// The message: a JSON line's `msg`, or a plain line verbatim.
    pub message: String,
    /// When runsc says the line was written, if it says.
    pub time: Option<String>,
}

/// Parses a line of runsc output. runsc writes JSON lines with `msg`,
/// `level` and `time`; anything else is a plain line. Attribution never
/// comes from the line: whatever keys it carries, the Actor and container
/// are the worker's to state.
pub fn parse_runsc_line(line: &str) -> RunscLine {
    let json = serde_json::from_str::<serde_json::Value>(line).ok();
    let msg = json
        .as_ref()
        .and_then(|v| v.get("msg"))
        .and_then(|m| m.as_str());
    let Some(msg) = msg else {
        return RunscLine {
            level: tracing::Level::INFO,
            message: line.to_string(),
            time: None,
        };
    };
    let field = |k: &str| {
        json.as_ref()
            .and_then(|v| v.get(k))
            .and_then(|m| m.as_str())
            .map(str::to_string)
    };
    let level = match field("level")
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("debug") => tracing::Level::DEBUG,
        Some("warning" | "warn") => tracing::Level::WARN,
        Some("error" | "fatal" | "panic") => tracing::Level::ERROR,
        _ => tracing::Level::INFO,
    };
    RunscLine {
        level,
        message: msg.to_string(),
        time: field("time"),
    }
}

/// Logs one runsc line as the Actor's container's.
fn log_runsc_line(attribution: &ActorAttribution, container: &str, line: &str) {
    let l = parse_runsc_line(line);
    let time = l.time.as_deref().unwrap_or("");
    macro_rules! emit {
        ($lvl:expr) => {
            tracing::event!(
                target: "hivekeep::actor",
                $lvl,
                hive = %attribution.actor.hive,
                actor = %attribution.actor.name,
                actor_uid = %attribution.uid,
                template_hive = %attribution.template_hive,
                template_name = %attribution.template_name,
                container = %container,
                runsc_time = %time,
                "{}",
                l.message
            )
        };
    }
    match l.level {
        tracing::Level::DEBUG => emit!(tracing::Level::DEBUG),
        tracing::Level::WARN => emit!(tracing::Level::WARN),
        tracing::Level::ERROR => emit!(tracing::Level::ERROR),
        _ => emit!(tracing::Level::INFO),
    }
}

/// Logs each line of `r` as the Actor's container's.
async fn forward_lines(r: impl AsyncRead + Unpin, output: Output) {
    let Output::Actor {
        attribution,
        container,
    } = output
    else {
        return;
    };
    let mut lines = BufReader::new(r).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        log_runsc_line(&attribution, &container, &line);
    }
}

/// The runsc invocations for one Actor.
#[derive(Debug, Clone)]
pub struct RunscCommand {
    /// The runsc binary.
    pub path: String,
    /// The Actor's uid.
    pub actor_uid: String,
    /// The Actor's directories.
    pub actor_dirs: pb::ActorDirs,
    /// The Actor's declared size; zero when only state is being saved.
    pub size: SandboxSize,
    /// Durable-dir volume names declared to the sandbox.
    pub durable_volumes: Vec<String>,
    /// The workload's containers.
    pub containers: Vec<pb::Container>,
    /// The configured flags.
    pub flags: Arc<FlagStore>,
}

impl RunscCommand {
    /// A command set that only saves or tears down state: no size, volumes
    /// or containers.
    pub fn bare(
        path: &str,
        actor_uid: &str,
        actor_dirs: &pb::ActorDirs,
        flags: Arc<FlagStore>,
    ) -> Self {
        RunscCommand {
            path: path.into(),
            actor_uid: actor_uid.into(),
            actor_dirs: actor_dirs.clone(),
            size: SandboxSize::default(),
            durable_volumes: Vec::new(),
            containers: Vec::new(),
            flags,
        }
    }

    /// `<global flags> -root <state dir> <command> <subcommand flags> <extra> [<container>]`.
    pub fn build_args(&self, command: &str, container: &str, extra: &[String]) -> Vec<String> {
        let cfg = self.flags.get();
        let mut args = cfg.global_flags(command);
        args.push("-root".into());
        args.push(runsc_state_dir(&self.actor_dirs).display().to_string());
        args.push(command.into());
        args.extend(cfg.subcommand_flags(command).iter().cloned());
        args.extend(extra.iter().cloned());
        if !container.is_empty() {
            args.push(container.into());
        }
        args
    }

    fn bundle(&self, container: &str) -> String {
        oci_bundle_path(&self.actor_dirs, container)
            .display()
            .to_string()
    }

    fn pid_file(&self, container: &str) -> String {
        pid_file_path(&self.actor_dirs, container)
            .display()
            .to_string()
    }

    /// argv of `runsc create`.
    pub fn create_args(&self, container: &str, additional: &[String]) -> Vec<String> {
        let mut extra = no_root_container_args(container);
        extra.extend([
            "-bundle".into(),
            self.bundle(container),
            "-pid-file".into(),
            self.pid_file(container),
        ]);
        extra.extend(additional.iter().cloned());
        self.build_args("create", container, &extra)
    }

    /// argv of `runsc checkpoint`.
    pub fn checkpoint_args(
        &self,
        container: &str,
        image_path: &str,
        fs_paths: &[String],
    ) -> Vec<String> {
        let mut extra = vec!["-image-path".to_string(), image_path.to_string()];
        for p in fs_paths {
            extra.push("-fs-checkpoint-paths".into());
            extra.push(p.clone());
        }
        self.build_args("checkpoint", container, &extra)
    }

    /// argv of `runsc fscheckpoint`.
    pub fn fs_checkpoint_args(
        &self,
        container: &str,
        image_path: &str,
        fs_paths: &[String],
    ) -> Vec<String> {
        let mut extra = vec!["-image-path".to_string(), image_path.to_string()];
        for p in fs_paths {
            extra.push("-path".into());
            extra.push(p.clone());
        }
        self.build_args("fscheckpoint", container, &extra)
    }

    /// argv of `runsc restore`.
    pub fn restore_args(&self, container: &str, image_path: &str) -> Vec<String> {
        let mut extra = no_root_container_args(container);
        extra.extend([
            "-bundle".into(),
            self.bundle(container),
            "-image-path".into(),
            image_path.into(),
            "-pid-file".into(),
            self.pid_file(container),
        ]);
        self.build_args("restore", container, &extra)
    }

    /// Builds the container's OCI spec, shapes it for gVisor and saves it into
    /// its bundle. The keeper makes the application bundles; the sandbox's is
    /// made here.
    pub fn write_spec(&self, container: &str) -> Result<()> {
        let bundle = oci_bundle_path(&self.actor_dirs, container);
        let mut spec = if container == SANDBOX_ID {
            crate::actor_path::create_private_dir(&bundle)
                .with_context(|| format!("while creating bundle {}", bundle.display()))?;
            oci::build_sandbox(&self.actor_uid)
        } else {
            let c = self
                .containers
                .iter()
                .find(|c| c.name == container)
                .ok_or_else(|| {
                    Error::msg(format!(
                        "container {container:?} is not in the workload spec"
                    ))
                })?;
            oci::build_container(&self.actor_uid, &self.actor_dirs, c)
        };
        oci::shape_gvisor(
            &mut spec,
            oci::GvisorOptions {
                actor_uid: &self.actor_uid,
                actor_dirs: &self.actor_dirs,
                container,
                durable_volumes: &self.durable_volumes,
                size: self.size,
            },
        );
        oci::save(&bundle, &spec).context("while saving the OCI spec")
    }
}

/// `-no-root-container` for the sandbox; runsc refuses it for anything else.
fn no_root_container_args(container: &str) -> Vec<String> {
    if container == SANDBOX_ID {
        vec!["-no-root-container".into()]
    } else {
        Vec::new()
    }
}

/// The sorted, deduplicated durable-dir volume names the containers mount.
pub fn durable_volume_names(spec: Option<&pb::WorkloadSpec>) -> Vec<String> {
    let mut names: Vec<String> = spec
        .map(|s| s.containers.as_slice())
        .unwrap_or_default()
        .iter()
        .flat_map(|c| c.durable_dir_volume_mounts.iter().map(|m| m.name.clone()))
        .collect();
    names.sort();
    names.dedup();
    names
}

/// The runsc operations the worker performs on one Actor's sandbox.
#[async_trait]
pub trait ContainerRuntime: Send + Sync {
    /// `runsc create`, after writing the container's spec.
    async fn create(
        &self,
        cancel: &CancellationToken,
        out: &Output,
        container: &str,
        extra: &[String],
    ) -> Result<()>;
    /// `runsc start`.
    async fn start(&self, cancel: &CancellationToken, out: &Output, container: &str) -> Result<()>;
    /// `runsc checkpoint` of the whole sandbox.
    async fn checkpoint(
        &self,
        cancel: &CancellationToken,
        container: &str,
        image_path: &str,
        fs_paths: &[String],
    ) -> Result<()>;
    /// `runsc fscheckpoint` of the given filesystems.
    async fn fs_checkpoint(
        &self,
        cancel: &CancellationToken,
        container: &str,
        image_path: &str,
        fs_paths: &[String],
    ) -> Result<()>;
    /// `runsc restore`, after writing the container's spec.
    async fn restore(
        &self,
        cancel: &CancellationToken,
        out: &Output,
        container: &str,
        image_path: &str,
    ) -> Result<()>;
    /// `runsc delete`.
    async fn delete(&self, cancel: &CancellationToken, container: &str) -> Result<()>;
    /// `runsc state`.
    async fn state(&self, cancel: &CancellationToken, container: &str) -> Result<()>;
    /// `runsc list`: the container ids runsc has a record of.
    async fn list(&self, cancel: &CancellationToken) -> Result<Vec<String>>;
    /// `runsc kill <container> <signal>`.
    async fn kill(&self, cancel: &CancellationToken, container: &str, signal: &str) -> Result<()>;
    /// `runsc wait`: until the container's process exits.
    async fn wait(&self, cancel: &CancellationToken, container: &str) -> Result<()>;
    /// `runsc wait -restore`: until background page loading finishes.
    async fn wait_restore(&self, cancel: &CancellationToken, container: &str) -> Result<()>;
    /// `runsc wait -fsrestore`: until background filesystem loading finishes.
    async fn wait_fs_restore(&self, cancel: &CancellationToken, container: &str) -> Result<()>;
}

/// [`ContainerRuntime`] over a real (or fake) runsc binary.
pub struct Runsc {
    /// The command lines.
    pub cmd: RunscCommand,
    runner: Arc<dyn CommandRunner>,
}

impl Runsc {
    /// runsc invocations of `cmd`, run by `runner`.
    pub fn new(cmd: RunscCommand, runner: Arc<dyn CommandRunner>) -> Self {
        Runsc { cmd, runner }
    }

    async fn exec(
        &self,
        name: &str,
        args: Vec<String>,
        out: &Output,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>> {
        self.runner
            .run(&self.cmd.path, &args, out, false, cancel)
            .await
            .with_context(|| format!("while running `runsc {name}`"))
    }

    async fn run_wait(
        &self,
        cancel: &CancellationToken,
        container: &str,
        extra: &[String],
    ) -> Result<()> {
        let args = self.cmd.build_args("wait", container, extra);
        self.exec("wait", args, &Output::Inherit, cancel)
            .await
            .map(drop)
    }
}

#[async_trait]
impl ContainerRuntime for Runsc {
    async fn create(
        &self,
        cancel: &CancellationToken,
        out: &Output,
        container: &str,
        extra: &[String],
    ) -> Result<()> {
        tracing::info!(container, "About to run runsc create");
        self.cmd
            .write_spec(container)
            .with_context(|| format!("while writing the OCI spec for {container:?}"))?;
        self.exec(
            "create",
            self.cmd.create_args(container, extra),
            out,
            cancel,
        )
        .await
        .map(drop)
    }

    async fn start(&self, cancel: &CancellationToken, out: &Output, container: &str) -> Result<()> {
        tracing::info!(container, "About to run runsc start");
        self.exec(
            "start",
            self.cmd.build_args("start", container, &[]),
            out,
            cancel,
        )
        .await
        .map(drop)
    }

    async fn checkpoint(
        &self,
        cancel: &CancellationToken,
        container: &str,
        image_path: &str,
        fs_paths: &[String],
    ) -> Result<()> {
        tracing::info!(container, "About to run runsc checkpoint");
        let args = self.cmd.checkpoint_args(container, image_path, fs_paths);
        self.exec("checkpoint", args, &Output::Inherit, cancel)
            .await
            .map(drop)
    }

    async fn fs_checkpoint(
        &self,
        cancel: &CancellationToken,
        container: &str,
        image_path: &str,
        fs_paths: &[String],
    ) -> Result<()> {
        tracing::info!(container, "About to run runsc fscheckpoint");
        let args = self.cmd.fs_checkpoint_args(container, image_path, fs_paths);
        self.exec("fscheckpoint", args, &Output::Inherit, cancel)
            .await
            .map(drop)
    }

    async fn restore(
        &self,
        cancel: &CancellationToken,
        out: &Output,
        container: &str,
        image_path: &str,
    ) -> Result<()> {
        tracing::info!(container, "About to run runsc restore");
        self.cmd
            .write_spec(container)
            .with_context(|| format!("while writing the OCI spec for {container:?}"))?;
        self.exec(
            "restore",
            self.cmd.restore_args(container, image_path),
            out,
            cancel,
        )
        .await
        .map(drop)
    }

    async fn delete(&self, cancel: &CancellationToken, container: &str) -> Result<()> {
        let args = self.cmd.build_args("delete", container, &[]);
        self.exec("delete", args, &Output::Inherit, cancel)
            .await
            .map(drop)
    }

    async fn state(&self, cancel: &CancellationToken, container: &str) -> Result<()> {
        let args = self.cmd.build_args("state", container, &[]);
        self.exec("state", args, &Output::Inherit, cancel)
            .await
            .map(drop)
    }

    async fn list(&self, cancel: &CancellationToken) -> Result<Vec<String>> {
        let args = self.cmd.build_args("list", "", &[]);
        let out = self
            .runner
            .run(&self.cmd.path, &args, &Output::Inherit, true, cancel)
            .await
            .context("while running `runsc list`")?;
        Ok(String::from_utf8_lossy(&out)
            .split_whitespace()
            .map(str::to_string)
            .collect())
    }

    async fn kill(&self, cancel: &CancellationToken, container: &str, signal: &str) -> Result<()> {
        tracing::info!(container, signal, "About to run runsc kill");
        let args = self
            .cmd
            .build_args("kill", "", &[container.to_string(), signal.to_string()]);
        self.exec("kill", args, &Output::Inherit, cancel)
            .await
            .map(drop)
    }

    async fn wait(&self, cancel: &CancellationToken, container: &str) -> Result<()> {
        tracing::info!(container, "About to run runsc wait");
        self.run_wait(cancel, container, &[]).await
    }

    async fn wait_restore(&self, cancel: &CancellationToken, container: &str) -> Result<()> {
        self.run_wait(cancel, container, &["-restore".into()]).await
    }

    async fn wait_fs_restore(&self, cancel: &CancellationToken, container: &str) -> Result<()> {
        self.run_wait(cancel, container, &["-fsrestore".into()])
            .await
    }
}

/// Builds the runtime for one Actor's command set. The service holds one, so
/// tests can substitute runsc entirely.
pub trait RuntimeFactory: Send + Sync {
    /// The runtime for `cmd`.
    fn runtime(&self, cmd: RunscCommand) -> Arc<dyn ContainerRuntime>;
}

/// Runs the real runsc through [`ProcessRunner`] (or another runner).
pub struct RunscFactory(pub Arc<dyn CommandRunner>);

impl Default for RunscFactory {
    fn default() -> Self {
        RunscFactory(Arc::new(ProcessRunner))
    }
}

impl RuntimeFactory for RunscFactory {
    fn runtime(&self, cmd: RunscCommand) -> Arc<dyn ContainerRuntime> {
        Arc::new(Runsc::new(cmd, self.0.clone()))
    }
}

/// The path of the Actor's sandbox pid file.
pub fn sandbox_pid_file(actor_dirs: &pb::ActorDirs) -> PathBuf {
    pid_file_path(actor_dirs, SANDBOX_ID)
}

#[cfg(test)]
mod tests;
