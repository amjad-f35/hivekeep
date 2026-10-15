//! Per-Actor sandbox networking: each Actor runs in network namespaces of
//! its own, with a fixed point-to-point /30, a firewall that sends all of
//! its TCP to the tunnel and rejects what the tunnel must never see, and
//! the tunnel's DNS relay and egress listeners on its gateway.
//!
//! Layers, bottom up: [`netlink`] (message codec and socket), [`rtnl`]
//! (links, addresses, routes), [`nft`] (the firewall), [`sysctls`],
//! [`setup`] (namespaces) and [`session`] (serving them).

pub mod netlink;
pub mod nft;
pub mod rtnl;
pub mod session;
pub mod setup;
pub mod sysctls;

#[cfg(all(test, target_os = "linux"))]
mod root_tests;

use ipnet::IpNet;

pub use session::{EgressServer, SandboxSession, ServeFn, SessionServices};
pub use setup::{
    cleanup_sandbox_network, gateway_netns_name, setup_sandbox_network, SandboxNetwork,
    SandboxNetworkConfig,
};

/// The Actor-side interface.
pub const ACTOR_VETH_NAME: &str = "eth0";
/// The gateway-side veth peer.
pub const GATEWAY_VETH_NAME: &str = "gwside";
/// The prefix length of the Actor's point-to-point network.
pub const ACTOR_PREFIX_LEN: u8 = 30;
/// The Actor's point-to-point network.
pub const ACTOR_SUBNET: &str = "169.254.17.0/30";
/// The in-sandbox HTTP endpoint the tunnel forwards Actor ingress to.
pub const ACTOR_HTTP_UPSTREAM: &str = "http://169.254.17.2:80";

/// [`ACTOR_SUBNET`], parsed.
pub fn actor_subnet() -> IpNet {
    ACTOR_SUBNET.parse().expect("constant subnet")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wakeup_probe::{ACTOR_VETH_GATEWAY, ACTOR_VETH_IP};

    #[test]
    fn addresses_share_the_subnet() {
        let net = actor_subnet();
        assert_eq!(net.prefix_len(), ACTOR_PREFIX_LEN);
        assert!(net.contains(&ACTOR_VETH_IP.parse::<std::net::IpAddr>().unwrap()));
        assert!(net.contains(&ACTOR_VETH_GATEWAY.parse::<std::net::IpAddr>().unwrap()));
        assert_eq!(ACTOR_HTTP_UPSTREAM, format!("http://{ACTOR_VETH_IP}:80"));
        assert!(GATEWAY_VETH_NAME.len() <= rtnl::MAX_IFNAME_LEN);
    }
}
