//! The command tree: every command path, its help, and the flags bound to
//! it from the setting registry.

use crate::config::{
    bind_command_flags, bind_flags, register_command, Setting, ValueKind, SANDBOX_CLASS_GVISOR,
};
use crate::demos;
use crate::gcp;
use clap::{Arg, ArgAction, Command};

pub const DEPLOY_SYSTEM_PATH: &str = "deploy hivekeep-system";
pub const DEPLOY_BENCHMARKS_PATH: &str = "deploy benchmarks";
pub const DELETE_BENCHMARKS_PATH: &str = "delete benchmarks";
pub const PUBLISH_RELEASE_IMAGES_PATH: &str = "publish release-images";
pub const PUBLISH_RELEASE_BINARIES_PATH: &str = "publish release-binaries";

/// Shortens the configuration report without the flag, for a caller that
/// runs the installer repeatedly.
pub const NO_REPORT_DEFAULTS_ENV: &str = "HK_NO_REPORT_DEFAULTS";

/// The `create` subcommands: name and summary.
pub const CREATE_COMMANDS: [(&str, &str); 8] = [
    (
        "jwt-authority-pool",
        "Create the actor-identity JWT authority pool secret",
    ),
    (
        "actor-id-ca-pool",
        "Create the actor-identity CA pool secret",
    ),
    (
        "actor-id-ca-certs",
        "Create the actor-identity CA trust bundle secret used by the egress gateway",
    ),
    (
        "egress-mitm-ca-pool",
        "Create the egress MITM CA pool secret used by the egress gateway with SDS mint",
    ),
    (
        "podcertificate-controller-cas",
        "Create the podcertificate controller's servicedns and podidentity CA pools",
    ),
    (
        "api-server-env-vars",
        "Create the hivekeep-api environment ConfigMap and Secret",
    ),
    (
        "api-authentication-config",
        "Create the default hivekeep-api authentication config",
    ),
    (
        "api-config",
        "Create the hivekeep-api config file, including its log level",
    ),
];

/// Registers the settings commands own. Idempotent, so building the tree
/// twice is harmless.
pub fn register_settings() {
    register_command([
        Setting::new("csi.setup", "SETUP_CSI", "setup-csi")
            .default_value("none")
            .commands(&[DEPLOY_SYSTEM_PATH])
            .usage("Also install CSI driver (nfs, hostpath, both, none; default: none)"),
        Setting::new("publish.binariesDir", "HK_PUBLISH_BINARIES_DIR", "out")
            .default_value("bin/release")
            .commands(&[PUBLISH_RELEASE_BINARIES_PATH])
            .usage("Directory the release binaries and their checksums are written to"),
        Setting::new("publish.overwrite", "HK_PUBLISH_OVERWRITE", "overwrite")
            .kind(ValueKind::Bool)
            .default_value("false")
            .commands(&[PUBLISH_RELEASE_IMAGES_PATH])
            .usage("Rebuild and replace images the registry already holds for the tag"),
        Setting::new(
            "benchmark.workerCount",
            "HK_BENCHMARK_WORKER_COUNT",
            "worker-count",
        )
        .kind(ValueKind::Int)
        .default_value("1")
        .commands(&[DEPLOY_BENCHMARKS_PATH, DELETE_BENCHMARKS_PATH])
        .usage("Number of WorkerPool replicas"),
        Setting::new(
            "benchmark.sandboxClass",
            "HK_BENCHMARK_SANDBOX_CLASS",
            "sandbox-class",
        )
        .default_value(SANDBOX_CLASS_GVISOR)
        .commands(&[DEPLOY_BENCHMARKS_PATH, DELETE_BENCHMARKS_PATH])
        .usage("Sandbox runtime for the benchmark WorkerPool: gvisor or microvm"),
    ]);
    demos::register_settings();
}

fn leaf(name: &'static str, about: &str) -> Command {
    Command::new(name).about(about.to_string())
}

fn group(name: &'static str, about: &str) -> Command {
    Command::new(name)
        .about(about.to_string())
        .subcommand_required(true)
        .arg_required_else_help(true)
}

fn setup() -> Command {
    group("setup", "Set up optional cluster add-ons").subcommand(
        leaf("csi", "Set up the hostpath and/or NFS CSI drivers")
            .long_about(
                "Install the CSI drivers that back the external volume demos.\n\n\
                 Driver options: nfs (default), hostpath, both, none. Note that hostpath is single-node Kind \
                 only, while NFS is not restricted to Kind.\n\n\
                 Both drivers expose their controllers over TCP so keeper and hivekeep-api can reach them. \
                 Re-running this removes the previous deployment first, which clears stale mounts left behind \
                 by an earlier install.",
            )
            .arg(Arg::new("driver").num_args(1).required(false).help("nfs, hostpath, both or none")),
    )
}

fn create() -> Command {
    let mut c = group("create", "Create an individual secret or config used by hivekeep-system").long_about(
        "Create one of the resources hivekeep-system depends on.\n\n\
         These are created automatically by \"hivekeep-setup deploy hivekeep-system\"; the subcommands here \
         exist to (re)create a single resource on an existing cluster.",
    );
    for (name, about) in CREATE_COMMANDS {
        c = c.subcommand(leaf(name, about));
    }
    c
}

fn deploy() -> Command {
    let system = leaf("hivekeep-system", "Deploy the core system: CRDs, RBAC, store, API server, controller, gate, and keeper")
        .long_about(
            "Deploy the whole Hivekeep control plane.\n\n\
             This installs the CRDs and RBAC, the podcertificate controller and the secrets it signs, \
             PostgreSQL, hivekeep-api, hivekeep-idp-server, hivekeep-controller, the gate dataplane, and the \
             keeper DaemonSet, then waits for each to roll out.\n\n\
             The bundled PostgreSQL StatefulSet is skipped when \
             HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING or the HK_API_POSTGRES_CLOUDSQL_* variables select \
             an external database.\n\n\
             Shape the install with the global --gate-dataplane, --cluster-size, \
             --cordon-system-infra-components, and --fake-data-plane flags.",
        );
    let system = bind_command_flags(DEPLOY_SYSTEM_PATH, system).mut_arg("setup-csi", |a| {
        a.num_args(0..=1)
            .require_equals(true)
            .default_missing_value("none")
    });

    let mut demo = group("demo", "Deploy a bundled demo");
    for d in demos::all() {
        let path = demos::deploy_command_path(d.as_ref());
        let cmd = Command::new(demos::demo_arg(d.as_ref()).to_string())
            .about(d.description().to_string());
        demo = demo.subcommand(bind_command_flags(&path, cmd));
    }

    let benchmarks = leaf(
        "benchmarks",
        "Deploy the benchmark workloads and the locust load test stack",
    );

    group("deploy", "Deploy Hivekeep components")
        .subcommand(system)
        .subcommand(leaf("keeper", "Deploy the keeper DaemonSet only"))
        .subcommand(
            leaf("api-server", "Deploy hivekeep-api and hivekeep-idp-server").visible_alias("apiserver"),
        )
        .subcommand(
            leaf("hivekeep-controller", "Deploy hivekeep-controller only, with the CRDs it serves")
                .visible_alias("controller"),
        )
        .subcommand(leaf("gate", "Deploy the gate dataplane only: router and egress"))
        .subcommand(leaf("podcertificate-controller", "Deploy podcertificate-controller only").visible_alias("podcert"))
        .subcommand(leaf(
            "sandboxconfig",
            "Deploy the SandboxConfig admission policy and the default gVisor SandboxConfig only",
        ))
        .subcommand(leaf("postgres", "Deploy the single-replica PostgreSQL StatefulSet").long_about(
            "Deploy the experimental single-replica PostgreSQL StatefulSet on its own.\n\n\
             \"deploy hivekeep-system\" already brings PostgreSQL up, unless \
             HK_API_POSTGRES_READ_WRITE_CONNECTION_STRING or the HK_API_POSTGRES_CLOUDSQL_* variables select \
             an external database; this subcommand is for bringing the StatefulSet up by itself.",
        ))
        .subcommand(bind_command_flags(DEPLOY_BENCHMARKS_PATH, benchmarks))
        .subcommand(demo)
}

fn delete() -> Command {
    let mut demo = group("demo", "Delete a bundled demo");
    for d in demos::all() {
        demo = demo.subcommand(
            Command::new(demos::demo_arg(d.as_ref()).to_string())
                .about(format!("Delete {}", d.name())),
        );
    }
    let benchmarks = leaf("benchmarks", "Delete the locust load test stack and the benchmark workloads").long_about(
        "Delete the locust load test stack and the benchmark workloads.\n\n\
         Pass --sandbox-class=microvm to also remove the micro-VM SandboxConfig; it is cluster-wide, so it is \
         left in place by default.",
    );
    group("delete", "Delete Hivekeep components")
        .subcommand(leaf("hivekeep-system", "Delete the core system"))
        .subcommand(leaf("gate", "Delete the gate dataplane"))
        .subcommand(leaf("all", "Delete every demo and then the core system"))
        .subcommand(bind_command_flags(DELETE_BENCHMARKS_PATH, benchmarks))
        .subcommand(demo)
}

fn publish() -> Command {
    group("publish", "Build release images and binaries, and images that no manifest references")
        .subcommand(leaf("worker-images", "Build and push the worker images for this build and print their refs"))
        .subcommand(bind_command_flags(
            PUBLISH_RELEASE_IMAGES_PATH,
            leaf("release-images", "Build and push the release images, tagged with the release version").long_about(
                "Build and push the images a release publishes to HK_BUILD_REPO, all tagged with the build \
                 version, and print their pushed references. The version comes from HK_VERSION, else git \
                 describe, and has to be a release tag (vMAJOR.MINOR.PATCH[-PRERELEASE]).\n\n\
                 If the repository already holds every image for the tag on every platform, nothing is built \
                 unless --overwrite is set. After pushing, every image is checked for every platform, and a \
                 missing one fails the command.",
            ),
        ))
        .subcommand(bind_command_flags(
            PUBLISH_RELEASE_BINARIES_PATH,
            leaf("release-binaries", "Build the hivectl release binaries and their checksums into a directory"),
        ))
}

/// The whole command tree. Registers the command-scoped settings first, so
/// their flags exist.
pub fn root() -> Command {
    register_settings();
    let root = Command::new("hivekeep-setup")
        .about("Install and tear down Hivekeep on a Kubernetes cluster")
        .long_about(
            "hivekeep-setup deploys the Hivekeep control plane, its supporting secrets and config, and the \
             bundled demos.\n\n\
             Cluster selection follows kubeconfig: pass --context to target a specific cluster, or set \
             KUBECTL_CONTEXT. Use --kind for a local Kind cluster, which selects the kind manifest overlays, \
             the local image registry, and host-only image builds.\n\n\
             Developer settings are read from .hivekeep-dev-env.sh at the repository root when it is present \
             (skipped for --kind, and by NO_DEV_ENV=1).",
        )
        .version(env!("CARGO_PKG_VERSION"))
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .long("config")
                .global(true)
                .num_args(1)
                .help("Path to the installation configuration file (or HK_CONFIG). Its settings outrank the environment and are outranked by flags"),
        )
        .arg(
            Arg::new("no-dev-env")
                .long("no-dev-env")
                .global(true)
                .action(ArgAction::SetTrue)
                .help("Do not source .hivekeep-dev-env.sh"),
        )
        .arg(
            Arg::new("no-report-defaults")
                .long("no-report-defaults")
                .global(true)
                .action(ArgAction::SetTrue)
                .help("List only the settings something set in the configuration report (or HK_NO_REPORT_DEFAULTS)"),
        );
    bind_flags(root)
        .subcommand(setup())
        .subcommand(create())
        .subcommand(deploy())
        .subcommand(delete())
        .subcommand(publish())
        .subcommand(gcp::command())
}

/// Resolves typed words (aliases included) to the canonical command path,
/// for callers that inspect the tree rather than run it.
pub fn find(root: &Command, words: &[&str]) -> Option<Vec<String>> {
    let mut cur = root;
    let mut path = Vec::new();
    for w in words {
        cur = cur.find_subcommand(w)?;
        path.push(cur.get_name().to_string());
    }
    Some(path)
}

/// Every command path in the tree, without the program name, depth first.
pub fn all_paths(root: &Command) -> Vec<String> {
    fn walk(c: &Command, prefix: &str, out: &mut Vec<String>) {
        for sub in c.get_subcommands() {
            let p = if prefix.is_empty() {
                sub.get_name().to_string()
            } else {
                format!("{prefix} {}", sub.get_name())
            };
            out.push(p.clone());
            walk(sub, &p, out);
        }
    }
    let mut out = Vec::new();
    walk(root, "", &mut out);
    out
}
