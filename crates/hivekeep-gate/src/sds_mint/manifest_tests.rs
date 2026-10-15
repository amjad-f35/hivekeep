//! The egress pod keeps the MITM signing CA on the sdsmint container only.

use clap::Parser;
use serde_json::Value;

use super::Args;
use crate::router::manifests::support::*;

const SDSMINT_CONTAINER: &str = "sdsmint";

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    args: Args,
    #[arg(long = "log-level")]
    _log_level: Option<String>,
}

/// The --ca-pool-path the manifest passes, parsed as the binary would.
fn ca_pool_path(pod: &Value) -> String {
    let a = args(container(pod, SDSMINT_CONTAINER));
    assert!(
        a.first().map(String::as_str) == Some("sdsmint"),
        "the {SDSMINT_CONTAINER} container's args are {a:?}; they should invoke the sdsmint subcommand"
    );
    let cli = Cli::try_parse_from(a.iter().cloned()).unwrap_or_else(|e| {
        panic!("the {SDSMINT_CONTAINER} flags are not ones the binary accepts: {e}")
    });
    let path = cli
        .args
        .ca_pool_path
        .expect("the manifest passes no --ca-pool-path; sdsmint requires it");
    path.to_string_lossy().into_owned()
}

/// The mount a container reads file through.
fn mount_supplying<'a>(pod: &'a Value, container_name: &str, file: &str) -> &'a Value {
    list(container(pod, container_name), "volumeMounts")
        .into_iter()
        .find(|m| file.starts_with(&format!("{}/", s(m, "mountPath").trim_end_matches('/'))))
        .unwrap_or_else(|| {
            panic!("container {container_name:?} mounts nothing that would supply {file}")
        })
}

/// Reached through sdsmint's own flag rather than the volume name, so a
/// renamed volume cannot quietly make this check nothing.
#[test]
fn manifest_keeps_the_ca_off_the_data_plane() {
    let pod = pod_spec(EGRESS_MANIFEST, EGRESS_DEPLOYMENT);
    let path = ca_pool_path(&pod);
    let ca_volume = s(mount_supplying(&pod, SDSMINT_CONTAINER, &path), "name").to_string();
    for c in all_containers(&pod) {
        if s(c, "name") == SDSMINT_CONTAINER {
            continue;
        }
        for m in list(c, "volumeMounts") {
            assert_ne!(
                s(m, "name"),
                ca_volume,
                "container {:?} mounts the MITM CA volume at {}; only {SDSMINT_CONTAINER} may see the signing key",
                s(c, "name"),
                s(m, "mountPath")
            );
        }
    }
}
