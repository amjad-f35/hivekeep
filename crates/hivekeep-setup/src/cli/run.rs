//! Running the leaf command a parse selected, on a prepared [`Env`].

use super::options::{benchmark_options, deploy_options, publish_release_options};
use super::CliError;
use crate::demos::{self, DemoDeleter};
use crate::steps::{Deleter, Env};
use clap::ArgMatches;
use std::path::PathBuf;

/// The CSI driver `setup csi` was given, defaulting to nfs.
pub fn csi_driver(matches: &ArgMatches) -> String {
    matches
        .get_one::<String>("driver")
        .cloned()
        .unwrap_or_else(|| "nfs".to_string())
}

/// Runs the command at `path` (canonical names, without the program name).
pub async fn run(path: &str, matches: &ArgMatches, env: &Env) -> Result<(), CliError> {
    let words: Vec<&str> = path.split_whitespace().collect();
    match words.as_slice() {
        ["setup", "csi"] => env.setup_csi(&csi_driver(matches)).await?,

        ["create", "jwt-authority-pool"] => env.create_jwt_authority_pool_secret().await?,
        ["create", "actor-id-ca-pool"] => env.create_actor_id_ca_pool_secret().await?,
        ["create", "actor-id-ca-certs"] => env.create_actor_id_ca_certs_secret().await?,
        ["create", "egress-mitm-ca-pool"] => env.create_egress_mitm_ca_pool_secret().await?,
        ["create", "podcertificate-controller-cas"] => {
            env.create_pod_certificate_controller_cas().await?
        }
        ["create", "api-server-env-vars"] => {
            env.ensure_env_vars_safe_standalone().await?;
            env.update_api_server_env_vars().await?
        }
        ["create", "api-authentication-config"] => env.create_api_authentication_config().await?,
        ["create", "api-config"] => env.create_api_config().await?,

        ["deploy", "hivekeep-system"] => {
            let opts = deploy_options(&env.cfg)?;
            env.deploy_hivekeep_system(&opts).await?
        }
        ["deploy", "keeper"] => env.deploy_keeper().await?,
        ["deploy", "api-server"] => env.deploy_api_server().await?,
        ["deploy", "hivekeep-controller"] => env.deploy_controller().await?,
        ["deploy", "gate"] => env.deploy_gate().await?,
        ["deploy", "podcertificate-controller"] => env.deploy_pod_certificate_controller().await?,
        ["deploy", "sandboxconfig"] => env.deploy_sandbox_config().await?,
        ["deploy", "postgres"] => env.deploy_postgres().await?,
        ["deploy", "benchmarks"] => {
            let opts = benchmark_options(&env.cfg)?;
            env.deploy_benchmarks(&opts).await?
        }
        ["deploy", "demo", name] => {
            let demo = find_demo(name)?;
            env.require_control_plane().await?;
            demo.deploy(env).await?
        }

        ["delete", "hivekeep-system"] => env.delete_hivekeep_system().await?,
        ["delete", "gate"] => env.delete_gate().await?,
        ["delete", "all"] => {
            let all = demos::all();
            let wrapped: Vec<DemoDeleter<'_>> =
                all.iter().map(|d| DemoDeleter(d.as_ref())).collect();
            let deleters: Vec<&dyn Deleter> = wrapped.iter().map(|d| d as &dyn Deleter).collect();
            env.delete_all(&deleters).await?
        }
        ["delete", "benchmarks"] => {
            let opts = benchmark_options(&env.cfg)?;
            env.delete_benchmarks(&opts).await?
        }
        ["delete", "demo", name] => find_demo(name)?.delete(env).await?,

        ["publish", "worker-images"] => env.publish_worker_images(&mut std::io::stdout()).await?,
        ["publish", "release-images"] => {
            env.publish_release_images(&mut std::io::stdout(), publish_release_options(&env.cfg))
                .await?
        }
        ["publish", "release-binaries"] => {
            let out = PathBuf::from(env.cfg.resolved().string("publish.binariesDir"));
            env.publish_release_binaries(&mut std::io::stdout(), &out)
                .await?
        }

        _ => return Err(CliError::Usage(format!("no command {path:?}"))),
    }
    Ok(())
}

fn find_demo(arg: &str) -> Result<Box<dyn demos::Demo>, CliError> {
    demos::all()
        .into_iter()
        .find(|d| demos::demo_arg(d.as_ref()) == arg)
        .ok_or_else(|| CliError::Usage(format!("unknown demo {arg:?}")))
}
