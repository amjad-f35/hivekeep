//! A Container Storage Interface (CSI) client for external volumes.
//!
//! External volumes are provisioned and attached by the control plane
//! through a driver's controller service, then staged and published on the
//! node through its node service. This module holds both sides:
//!
//! - [`pb`]: the generated CSI v1 protocol (clients, and servers for the
//!   fake driver).
//! - [`endpoint`]: `unix://`, `tcp://` and `dns://` driver endpoints.
//! - [`driver_config`]: per-driver connection settings and their lookup.
//! - [`tls`]: controller mutual TLS with the rotating pod identity.
//! - [`client`]: a lazily connected channel and its service clients.
//! - [`plugin`]: [`CsiPlugin`], the volume operations over one driver,
//!   following its advertised capabilities.
//! - [`open`]: opening a driver by name for the controller or node role.
//! - `fake` (tests, or the `testing` feature): an in-process driver.

/// The generated CSI v1 protocol.
#[allow(missing_docs, clippy::all, clippy::pedantic)]
pub mod pb {
    tonic::include_proto!("csi.v1");
}

pub mod client;
pub mod driver_config;
pub mod endpoint;
#[cfg(any(test, feature = "testing"))]
pub mod fake;
pub mod open;
pub mod plugin;
pub mod tls;

#[cfg(test)]
mod client_tests;
#[cfg(test)]
mod plugin_tests;
#[cfg(test)]
mod test_certs;
#[cfg(test)]
mod tls_tests;

pub use client::CsiClient;
pub use driver_config::{
    kubelet_plugin_socket_path, CsiDriverConfig, CsiDriverConfigGetter, CsiDriverTls,
    StaticCsiDriverConfigs,
};
pub use endpoint::{parse_endpoint, Endpoint};
pub use open::{open_csi_plugin, open_csi_plugin_with, OpenOptions, Role};
pub use plugin::{
    AttachVolumeRequest, AttachVolumeResponse, ControllerRpc, CreateVolumeRequest,
    CreateVolumeResponse, CsiPlugin, MountVolumeRequest, NodeRpc,
};
pub use tls::{resolve_tls_config, ControllerTls, TlsPaths};
