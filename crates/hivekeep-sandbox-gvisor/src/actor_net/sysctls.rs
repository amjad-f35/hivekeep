//! The kernel settings pinned in every sandbox namespace.
//!
//! A new namespace copies its IPv4 `conf/all` and `conf/default` from the
//! node, where kube-proxy and CNI plugins set their own values, and takes
//! the kernel's IPv6 defaults. Neither suits a link with an untrusted Actor
//! on the other end. They are written before the namespace has any link but
//! `lo`, because a link copies `conf/default` when it is created and a later
//! write does not reach it.

use hivekeep_gate::netns::Sysctl;

/// The settings, in the order they are written.
pub const SANDBOX_SYSCTLS: &[(&str, &str)] = &[
    // The DNS relay answers on 53 and the worker holds no
    // CAP_NET_BIND_SERVICE. Per-namespace, so it grants nothing outside.
    ("net/ipv4/ip_unprivileged_port_start", "0"),
    // Nothing in a sandbox namespace routes; the forward chain backs this up.
    ("net/ipv4/conf/all/forwarding", "0"),
    ("net/ipv4/conf/default/forwarding", "0"),
    ("net/ipv6/conf/all/forwarding", "0"),
    ("net/ipv6/conf/default/forwarding", "0"),
    // conf/all/accept_redirects is reset to 1 whenever conf/all/forwarding
    // changes to 0, so it comes after forwarding.
    ("net/ipv4/conf/all/accept_redirects", "0"),
    ("net/ipv4/conf/default/accept_redirects", "0"),
    // kube-proxy enables this on nodes; it would deliver Actor packets for
    // 127.0.0.0/8 to sockets in the gateway namespace.
    ("net/ipv4/conf/all/route_localnet", "0"),
    ("net/ipv4/conf/default/route_localnet", "0"),
    // Drop packets claiming a gateway source, carrying a source route, or
    // coming from outside the /30.
    ("net/ipv4/conf/all/accept_local", "0"),
    ("net/ipv4/conf/default/accept_local", "0"),
    ("net/ipv4/conf/all/accept_source_route", "0"),
    ("net/ipv4/conf/default/accept_source_route", "0"),
    ("net/ipv4/conf/all/rp_filter", "1"),
    ("net/ipv4/conf/default/rp_filter", "1"),
    // No router advertisements or redirects from the Actor; IPv6 reads both
    // only from the link's own copy, taken from conf/default.
    ("net/ipv6/conf/default/accept_ra", "0"),
    ("net/ipv6/conf/default/accept_redirects", "0"),
];

/// [`SANDBOX_SYSCTLS`] as gate sysctl settings.
pub fn sandbox_sysctls() -> Vec<Sysctl> {
    SANDBOX_SYSCTLS
        .iter()
        .map(|(k, v)| Sysctl::new(*k, *v))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirects_are_pinned_after_forwarding() {
        let pos = |k: &str| {
            SANDBOX_SYSCTLS
                .iter()
                .position(|(key, _)| *key == k)
                .unwrap()
        };
        assert!(pos("net/ipv4/conf/all/accept_redirects") > pos("net/ipv4/conf/all/forwarding"));
        assert_eq!(sandbox_sysctls().len(), SANDBOX_SYSCTLS.len());
        assert!(SANDBOX_SYSCTLS.iter().all(|(k, _)| k.starts_with("net/")));
    }
}
