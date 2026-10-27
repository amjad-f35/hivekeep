//! An Actor's sandbox network as the worker builds it, without the sandbox:
//! two network namespaces joined by a veth pair, nftables in the gateway
//! namespace redirecting the Actor's TCP egress to the tunnel's egress
//! listener, and the tunnel's egress listener and DNS relay serving there.
//!
//! The runtime namespace is where a sandbox would run; here the benchmark's
//! ingress and egress actors run in it instead. Setting up namespaces,
//! links and nftables needs Linux and root; the `ip` and `nft` tools do the
//! netlink work. Elsewhere [`serve_sandbox`] fails.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;

use futures::future::BoxFuture;
use hivekeep_gate::netns::{self, NetNs, NsConn, Sysctl};
use hivekeep_gate::tunnel::dns::{self, PolicySource, Relay};
use hivekeep_gate::tunnel::egress::{BoxIo, Proxy};
use hivekeep_gate::tunnel::ingress::Dial;
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{Error, Result};

/// The Actor-side veth, inside the runtime namespace.
pub const ACTOR_VETH: &str = "eth0";
/// The gateway-side veth.
pub const GATEWAY_VETH: &str = "hkside";
/// The Actor's address.
pub const ACTOR_IP: &str = "169.254.17.2";
/// The gateway's address, the Actor's default route and DNS server.
pub const GATEWAY_IP: &str = "169.254.17.1";
/// The point-to-point /30 the veth pair lives on.
pub const ACTOR_SUBNET: &str = "169.254.17.0/30";
/// Where the Actor serves HTTP, which the tunnel's ingress proxies to.
pub const ACTOR_PORT: u16 = 80;
/// The in-sandbox HTTP endpoint the tunnel proxies Actor ingress to.
pub const ACTOR_HTTP_UPSTREAM: &str = "http://169.254.17.2:80";

/// The name of an Actor's runtime namespace, as the worker names it.
pub fn actor_netns_name(actor_uid: &str) -> String {
    format!("worker-actor:{actor_uid}")
}

/// The name of the namespace holding the gateway veth and the tunnel's
/// sockets for one Actor.
pub fn gateway_netns_name(actor_uid: &str) -> String {
    format!("{}-gw", actor_netns_name(actor_uid))
}

/// The bind mount naming an Actor's runtime namespace.
pub fn actor_netns_path(actor_uid: &str) -> PathBuf {
    PathBuf::from(netns::NAMED_NETNS_DIR).join(actor_netns_name(actor_uid))
}

/// Set in both namespaces before any link but `lo` exists.
fn sandbox_sysctls() -> Vec<Sysctl> {
    vec![
        // The tunnel answers DNS on 53 without CAP_NET_BIND_SERVICE; this
        // is per namespace, so it grants nothing outside.
        Sysctl::new("net/ipv4/ip_unprivileged_port_start", "0"),
        // Nothing in a sandbox namespace routes.
        Sysctl::new("net/ipv4/conf/all/forwarding", "0"),
        Sysctl::new("net/ipv4/conf/default/forwarding", "0"),
        Sysctl::new("net/ipv6/conf/all/forwarding", "0"),
        Sysctl::new("net/ipv6/conf/default/forwarding", "0"),
        // Turning forwarding off re-enables redirects, so these come after.
        Sysctl::new("net/ipv4/conf/all/accept_redirects", "0"),
        Sysctl::new("net/ipv4/conf/default/accept_redirects", "0"),
        Sysctl::new("net/ipv6/conf/default/accept_ra", "0"),
        Sysctl::new("net/ipv6/conf/default/accept_redirects", "0"),
    ]
}

/// The nftables rules for the gateway namespace: nothing is forwarded,
/// link-local destinations other than the Actor's own /30 are refused,
/// non-TCP egress fails fast, and every other TCP connection is redirected
/// to the tunnel's egress port, keeping its original destination for the
/// tunnel to read.
pub fn egress_rules(egress_port: u16) -> String {
    format!(
        r#"table ip hk-actor {{
  chain forward {{
    type filter hook forward priority filter; policy drop;
  }}
  chain disallowed-egress {{
    type filter hook prerouting priority raw;
    ip daddr {ACTOR_SUBNET} accept
    ip daddr 169.254.0.0/16 reject with icmp type admin-prohibited
    meta l4proto != tcp reject with icmp type net-unreachable
  }}
  chain prerouting {{
    type nat hook prerouting priority dstnat;
    ip daddr != {ACTOR_SUBNET} meta l4proto tcp redirect to :{egress_port}
  }}
}}
table ip6 hk-actor-v6 {{
  chain forward {{
    type filter hook forward priority filter; policy drop;
  }}
  chain disallowed-egress {{
    type filter hook prerouting priority raw;
    ip6 daddr fe80::/10 reject with icmpv6 type admin-prohibited
  }}
}}
"#
    )
}

/// What one Actor's sandbox network needs.
#[derive(Clone)]
pub struct SandboxConfig {
    pub actor_uid: String,
    /// Where the tunnel serves this Actor's egress; every TCP connection
    /// the Actor makes is redirected to it.
    pub egress_port: u16,
    /// Supplies the policy the Actor's DNS queries are checked against.
    pub dns_policy: Option<Arc<dyn PolicySource>>,
}

/// A sandbox's network and the sockets serving it.
pub struct SandboxSession {
    actor_uid: String,
    gateway: Arc<Mutex<Option<Arc<NetNs>>>>,
    dns: Option<dns::Server>,
    cancel: CancellationToken,
    serving: Mutex<Vec<JoinHandle<()>>>,
}

async fn run(program: &str, args: &[&str], stdin: Option<&str>) -> Result<()> {
    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args).stdout(Stdio::null()).stderr(Stdio::piped());
    if stdin.is_some() {
        cmd.stdin(Stdio::piped());
    }
    let mut child = cmd
        .spawn()
        .map_err(|e| Error::msg(format!("{program}: {e}")))?;
    if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
        pipe.write_all(input.as_bytes()).await?;
    }
    let out = child.wait_with_output().await?;
    if !out.status.success() {
        return Err(Error::msg(format!(
            "{program} {}: {}: {}",
            args.join(" "),
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    Ok(())
}

fn prepare(ns: &NetNs) -> Result<()> {
    ns.run(|| netns::set_sysctls(&sandbox_sysctls()))?;
    Ok(())
}

/// Builds the sandbox network and serves egress and DNS from its gateway
/// namespace.
pub async fn serve_sandbox(
    cfg: SandboxConfig,
    egress: &Proxy,
    relay: &Relay,
) -> Result<SandboxSession> {
    if cfg.actor_uid.is_empty() {
        return Err(Error::msg("sandbox network: Actor UID is required"));
    }
    if cfg.egress_port == 0 {
        return Err(Error::msg(
            "sandbox network: the tunnel's egress port is required",
        ));
    }
    let actor_name = actor_netns_name(&cfg.actor_uid);
    let gateway_name = gateway_netns_name(&cfg.actor_uid);
    let actor_ns = netns::create_named(&actor_name)?;
    let gateway_ns = Arc::new(netns::create_named(&gateway_name)?);
    let mut session = SandboxSession {
        actor_uid: cfg.actor_uid.clone(),
        gateway: Arc::new(Mutex::new(Some(gateway_ns.clone()))),
        dns: None,
        cancel: CancellationToken::new(),
        serving: Mutex::new(Vec::new()),
    };
    if let Err(e) = setup(
        &cfg,
        &actor_name,
        &gateway_name,
        &actor_ns,
        &gateway_ns,
        egress,
        relay,
        &mut session,
    )
    .await
    {
        let _ = session.close().await;
        return Err(e);
    }
    Ok(session)
}

#[allow(clippy::too_many_arguments)]
async fn setup(
    cfg: &SandboxConfig,
    actor_name: &str,
    gateway_name: &str,
    actor_ns: &NetNs,
    gateway_ns: &Arc<NetNs>,
    egress: &Proxy,
    relay: &Relay,
    session: &mut SandboxSession,
) -> Result<()> {
    prepare(actor_ns)?;
    prepare(gateway_ns)?;
    for ns in [actor_name, gateway_name] {
        run("ip", &["-n", ns, "link", "set", "lo", "up"], None).await?;
    }
    run(
        "ip",
        &[
            "-n",
            gateway_name,
            "link",
            "add",
            GATEWAY_VETH,
            "type",
            "veth",
            "peer",
            "name",
            ACTOR_VETH,
            "netns",
            actor_name,
        ],
        None,
    )
    .await?;
    run(
        "ip",
        &[
            "-n",
            gateway_name,
            "addr",
            "replace",
            &format!("{GATEWAY_IP}/30"),
            "dev",
            GATEWAY_VETH,
        ],
        None,
    )
    .await?;
    run(
        "ip",
        &["-n", gateway_name, "link", "set", GATEWAY_VETH, "up"],
        None,
    )
    .await?;
    run(
        "ip",
        &[
            "-n",
            actor_name,
            "addr",
            "replace",
            &format!("{ACTOR_IP}/30"),
            "dev",
            ACTOR_VETH,
        ],
        None,
    )
    .await?;
    run(
        "ip",
        &["-n", actor_name, "link", "set", ACTOR_VETH, "up"],
        None,
    )
    .await?;
    run(
        "ip",
        &[
            "-n", actor_name, "route", "replace", "default", "via", GATEWAY_IP,
        ],
        None,
    )
    .await?;
    run(
        "ip",
        &["netns", "exec", gateway_name, "nft", "-f", "-"],
        Some(&egress_rules(cfg.egress_port)),
    )
    .await?;

    session.dns = Some(relay.serve(gateway_ns, cfg.dns_policy.clone()).await?);

    // Egress last: its binding is released by the serving task, so nothing
    // may fail between binding and starting it.
    let listeners = netns::listen(gateway_ns, &[cfg.egress_port])?;
    let mut serving = session.serving.lock();
    for l in listeners {
        l.set_nonblocking(true)?;
        let l = tokio::net::TcpListener::from_std(l)?;
        let cancel = session.cancel.clone();
        let binding = egress
            .bind(&cfg.actor_uid)
            .map_err(|e| Error::msg(e.to_string()))?;
        serving.push(tokio::spawn(async move {
            if let Err(err) = binding.serve(l, cancel).await {
                tracing::warn!(%err, "sandbox egress listener stopped");
            }
        }));
    }
    Ok(())
}

impl SandboxSession {
    /// Reaches the sandbox from the gateway namespace, the only place its
    /// address is routable.
    pub fn dialer(&self) -> Dial {
        let gateway = self.gateway.clone();
        Arc::new(
            move |addr: String| -> BoxFuture<'static, std::io::Result<BoxIo>> {
                let ns = gateway.lock().clone();
                Box::pin(async move {
                    let ns = ns.ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::NotConnected,
                            "sandbox network closed",
                        )
                    })?;
                    match netns::dial(&ns, "tcp", &addr).await? {
                        NsConn::Tcp(s) => Ok(Box::new(s) as BoxIo),
                        NsConn::Udp(_) => Err(std::io::Error::other("unexpected UDP connection")),
                    }
                })
            },
        )
    }

    /// The bind mount of the runtime namespace.
    pub fn runtime_netns_path(&self) -> PathBuf {
        actor_netns_path(&self.actor_uid)
    }

    /// Stops serving, waits for the serving tasks, then removes both
    /// namespaces (taking the veth pair with them). Every step runs even
    /// when an earlier one fails.
    pub async fn close(&self) -> Result<()> {
        self.cancel.cancel();
        if let Some(dns) = &self.dns {
            dns.stop().await;
        }
        let tasks: Vec<_> = std::mem::take(&mut *self.serving.lock());
        for t in tasks {
            let _ = t.await;
        }
        self.gateway.lock().take();
        let mut errs = Vec::new();
        for name in [
            actor_netns_name(&self.actor_uid),
            gateway_netns_name(&self.actor_uid),
        ] {
            if let Err(e) = netns::remove_named(&name) {
                errs.push(format!("deleting netns {name}: {e}"));
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(Error::msg(errs.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_paths() {
        assert_eq!(actor_netns_name("u1"), "worker-actor:u1");
        assert_eq!(gateway_netns_name("u1"), "worker-actor:u1-gw");
        assert_eq!(
            actor_netns_path("u1"),
            PathBuf::from("/run/netns/worker-actor:u1")
        );
        assert_eq!(
            ACTOR_HTTP_UPSTREAM,
            format!("http://{ACTOR_IP}:{ACTOR_PORT}")
        );
    }

    #[test]
    fn egress_rules_redirect_tcp_but_not_the_actor_subnet() {
        let r = egress_rules(15001);
        assert!(
            r.contains("ip daddr != 169.254.17.0/30 meta l4proto tcp redirect to :15001"),
            "{r}"
        );
        assert!(r.contains("ip daddr 169.254.17.0/30 accept"));
        assert!(r.contains("policy drop"));
    }
}
