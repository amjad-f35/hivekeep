//! Building and removing an Actor's sandbox namespaces.
//!
//! gVisor claims every interface of the namespace it runs in, so it gets a
//! namespace of its own joined by a veth pair to a gateway namespace that
//! holds the kernel-owned peer, the DNS relay and the tunnel's sockets.
//! A micro-VM's tap lives in a single namespace shared with the tunnel.

use std::io;
#[cfg(target_os = "linux")]
use std::net::Ipv4Addr;

use hivekeep_gate::netns::{self, NetNs};

use crate::node_path::actor_netns_name;

/// The namespace holding an Actor's gateway when it has one of its own.
pub fn gateway_netns_name(actor_uid: &str) -> String {
    format!("{}-gw", actor_netns_name(actor_uid))
}

/// What to build for one Actor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxNetworkConfig {
    /// The Actor's UID; it names the namespaces so teardown can find them.
    pub actor_uid: String,
    /// Separate the runtime from the gateway with a veth pair (gVisor).
    /// Without it the runtime brings its own link (a micro-VM tap).
    pub veth: bool,
    /// Where the tunnel serves this Actor's egress. Every TCP connection
    /// the Actor makes is redirected to it, whatever port it aimed at.
    pub egress_port: u16,
}

/// An Actor's runtime and gateway namespaces.
#[derive(Debug)]
pub struct SandboxNetwork {
    /// The Actor's UID.
    pub actor_uid: String,
    runtime: NetNs,
    /// `None` when the gateway shares the runtime namespace.
    gateway: Option<NetNs>,
}

impl SandboxNetwork {
    /// Where the sandbox runs.
    pub fn runtime_netns(&self) -> &NetNs {
        &self.runtime
    }

    /// Where the gateway address, DNS relay and tunnel sockets live.
    pub fn gateway_netns(&self) -> &NetNs {
        self.gateway.as_ref().unwrap_or(&self.runtime)
    }

    /// Whether runtime and gateway are one namespace.
    pub fn shares_namespace(&self) -> bool {
        self.gateway.is_none()
    }
}

/// Removes a namespace name, ignoring its absence.
fn remove(name: &str) -> io::Result<()> {
    netns::remove_named(name)
        .map_err(|e| io::Error::new(e.kind(), format!("while deleting netns {name}: {e}")))
}

/// Closes the namespace handles and removes their names. Deleting the
/// namespaces takes the veth pair with them. Every failure is reported.
pub fn cleanup_sandbox_network(network: SandboxNetwork) -> io::Result<()> {
    let uid = network.actor_uid.clone();
    drop(network);
    let errs: Vec<String> = [actor_netns_name(&uid), gateway_netns_name(&uid)]
        .iter()
        .filter_map(|n| remove(n).err().map(|e| e.to_string()))
        .collect();
    if errs.is_empty() {
        Ok(())
    } else {
        Err(io::Error::other(errs.join("; ")))
    }
}

/// Builds the Actor's isolated networking with the fixed sandbox
/// addresses. Blocking: it runs work on threads inside the namespaces.
///
/// The firewall at the gateway redirects outbound TCP to the tunnel,
/// keeping its original destination. Traffic to the gateway address is
/// not redirected, so DNS over UDP and TCP reaches the relay's sockets.
pub fn setup_sandbox_network(cfg: &SandboxNetworkConfig) -> io::Result<SandboxNetwork> {
    if cfg.actor_uid.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "actor UID is required",
        ));
    }
    if cfg.egress_port == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the tunnel egress port is required",
        ));
    }
    let actor_name = actor_netns_name(&cfg.actor_uid);
    let runtime = netns::create_named(&actor_name).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while creating the actor netns {actor_name}: {e}"),
        )
    })?;
    let mut network = SandboxNetwork {
        actor_uid: cfg.actor_uid.clone(),
        runtime,
        gateway: None,
    };
    match build(cfg, &mut network) {
        Ok(()) => Ok(network),
        Err(e) => {
            let _ = cleanup_sandbox_network(network);
            Err(e)
        }
    }
}

#[cfg(target_os = "linux")]
fn build(cfg: &SandboxNetworkConfig, network: &mut SandboxNetwork) -> io::Result<()> {
    network.runtime.run(prepare_namespace)?;
    if cfg.veth {
        network.gateway = Some(setup_veth_pair(cfg, &network.runtime)?);
    }
    install_egress_rules(network.gateway_netns(), cfg.egress_port)
}

#[cfg(not(target_os = "linux"))]
fn build(_: &SandboxNetworkConfig, _: &mut SandboxNetwork) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "sandbox networks are only supported on Linux",
    ))
}

/// Pins the sandbox sysctls and brings up `lo`, which carries the
/// namespace's own traffic (gVisor builds the Actor's loopback from it).
#[cfg(target_os = "linux")]
fn prepare_namespace() -> io::Result<()> {
    netns::set_sysctls(&super::sysctls::sandbox_sysctls())?;
    super::rtnl::Route::open()?.link_up("lo")
}

#[cfg(any(target_os = "linux", test))]
fn ipv4(s: &str) -> io::Result<std::net::Ipv4Addr> {
    s.parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{s}: {e}")))
}

/// Creates the gateway namespace and the veth pair joining it to the
/// runtime namespace, and addresses both ends.
#[cfg(target_os = "linux")]
fn setup_veth_pair(cfg: &SandboxNetworkConfig, runtime: &NetNs) -> io::Result<NetNs> {
    use super::{ACTOR_PREFIX_LEN, ACTOR_VETH_NAME, GATEWAY_VETH_NAME};
    use crate::wakeup_probe::{ACTOR_VETH_GATEWAY, ACTOR_VETH_IP};

    let name = gateway_netns_name(&cfg.actor_uid);
    let gateway = netns::create_named(&name).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while creating the gateway netns {name}: {e}"),
        )
    })?;
    let gw_ip: Ipv4Addr = ipv4(ACTOR_VETH_GATEWAY)?;
    let actor_ip: Ipv4Addr = ipv4(ACTOR_VETH_IP)?;
    let runtime_fd = runtime.as_raw_fd();
    // The peer is created straight in the runtime namespace: moving a link
    // afterwards costs far more, all under the global RTNL lock, and this
    // runs on the resume path.
    gateway.run(move || {
        prepare_namespace()?;
        let mut r = super::rtnl::Route::open()?;
        r.add_veth(GATEWAY_VETH_NAME, ACTOR_VETH_NAME, runtime_fd)?;
        r.replace_addr(GATEWAY_VETH_NAME, gw_ip, ACTOR_PREFIX_LEN)?;
        r.link_up(GATEWAY_VETH_NAME)
    })?;
    // gVisor imports these addresses and routes into its own stack.
    runtime.run(move || {
        let mut r = super::rtnl::Route::open()?;
        r.replace_addr(ACTOR_VETH_NAME, actor_ip, ACTOR_PREFIX_LEN)?;
        r.link_up(ACTOR_VETH_NAME)?;
        r.replace_default_route(ACTOR_VETH_NAME, gw_ip)
    })?;
    Ok(gateway)
}

/// Installs the sandbox firewall in `ns`.
#[cfg(target_os = "linux")]
fn install_egress_rules(ns: &NetNs, egress_port: u16) -> io::Result<()> {
    let batch = super::nft::egress_rules(super::actor_subnet(), egress_port)?;
    ns.run(move || super::nft::apply(batch, "while installing the actor egress redirect"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_the_gateway_after_the_actor() {
        assert_eq!(gateway_netns_name("uid-a"), "worker-actor:uid-a-gw");
        assert!(ipv4("169.254.17.1").is_ok());
        assert!(ipv4("nope").is_err());
    }

    #[test]
    fn requires_a_uid_and_a_port() {
        let cfg = |uid: &str, port| SandboxNetworkConfig {
            actor_uid: uid.into(),
            veth: true,
            egress_port: port,
        };
        let e = setup_sandbox_network(&cfg("", 15001)).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
        let e = setup_sandbox_network(&cfg("u", 0)).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
    }
}
