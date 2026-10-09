//! Opening a driver by name: look up its config, connect to the right
//! service, check it is the driver asked for and learn its capabilities.

use std::path::PathBuf;

use tonic::Status;

use super::client::CsiClient;
use super::driver_config::CsiDriverConfigGetter;
use super::plugin::CsiPlugin;
use super::tls::{resolve_tls_config, TlsPaths};

/// Which side of the driver to talk to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// The controller service, from the control plane (with TLS when
    /// configured).
    Controller,
    /// The node service, over the node-local socket.
    Node,
}

/// Options for [`open_csi_plugin_with`].
#[derive(Debug, Clone, Default)]
pub struct OpenOptions {
    /// Controller TLS credentials; the pod identity defaults when unset.
    pub tls_paths: Option<TlsPaths>,
    /// The node staging directory; the default node layout when unset.
    pub staging_dir: Option<PathBuf>,
}

/// Opens `driver_name` for `role` with the default credentials and layout.
pub async fn open_csi_plugin(
    getter: &dyn CsiDriverConfigGetter,
    driver_name: &str,
    role: Role,
) -> Result<CsiPlugin, Status> {
    open_csi_plugin_with(getter, driver_name, role, OpenOptions::default()).await
}

/// Opens `driver_name` for `role`. Fails when the driver has no config,
/// cannot be reached, or reports another name; a failed capability query
/// is not fatal (every optional RPC is then assumed supported).
pub async fn open_csi_plugin_with(
    getter: &dyn CsiDriverConfigGetter,
    driver_name: &str,
    role: Role,
    opts: OpenOptions,
) -> Result<CsiPlugin, Status> {
    let cfg = getter.get(driver_name).await.map_err(|e| {
        Status::failed_precondition(format!(
            "failed to retrieve CSIDriverConfig for {driver_name:?}: {e}"
        ))
    })?;
    let endpoint = match role {
        Role::Controller => cfg.controller_endpoint.clone(),
        Role::Node => {
            if !cfg.node_socket_override.is_empty() {
                tracing::info!(driver = driver_name, endpoint = %cfg.node_socket_override, "Found CSIDriverConfig with NodeSocketOverride");
            }
            cfg.node_endpoint()
        }
    };
    let tls = match role {
        Role::Controller => resolve_tls_config(Some(&cfg), &opts.tls_paths.unwrap_or_default())
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "failed to resolve TLS config for {driver_name:?}: {e}"
                ))
            })?,
        Role::Node => None,
    };
    let client = CsiClient::new(&endpoint, tls.as_ref()).map_err(|e| {
        Status::failed_precondition(format!(
            "failed to initialize CSI client from endpoint {endpoint:?}: {e}"
        ))
    })?;
    let mut plugin = CsiPlugin::new(client);
    if let Some(dir) = opts.staging_dir {
        plugin = plugin.with_staging_dir(dir);
    }
    let reported = plugin.driver_name().await.map_err(|e| {
        Status::unavailable(format!(
            "failed to get driver name from plugin {driver_name:?}: {}",
            e.message()
        ))
    })?;
    if reported != driver_name {
        return Err(Status::failed_precondition(format!(
            "reported driver name {reported:?} does not match requested name {driver_name:?}"
        )));
    }
    match role {
        Role::Controller => plugin.init_controller_capabilities().await,
        Role::Node => plugin.init_node_capabilities().await,
    }
    Ok(plugin)
}
