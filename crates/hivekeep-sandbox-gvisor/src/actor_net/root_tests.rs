//! Sandbox network tests against the real kernel. They create network
//! namespaces, links and nftables rules, so they need root on Linux:
//! `sudo -E cargo test -p hivekeep-sandbox-gvisor -- --ignored actor_net`.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream, UdpSocket};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hivekeep_gate::netns::{self, NetNs};
use tokio_util::sync::CancellationToken;

use super::nft::{self, Family};
use super::rtnl::{index_of, interface_names, Route};
use super::*;
use crate::node_path::actor_netns_name;
use crate::wakeup_probe::ACTOR_VETH_IP;

const EGRESS_PORT: u16 = 15001;

fn cfg(uid: &str, veth: bool) -> SandboxNetworkConfig {
    SandboxNetworkConfig {
        actor_uid: uid.into(),
        veth,
        egress_port: EGRESS_PORT,
    }
}

/// Removes the network when the test ends, however it ends.
struct Cleanup(Option<SandboxNetwork>);

impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Some(n) = self.0.take() {
            let _ = cleanup_sandbox_network(n);
        }
    }
}

impl std::ops::Deref for Cleanup {
    type Target = SandboxNetwork;
    fn deref(&self) -> &SandboxNetwork {
        self.0.as_ref().unwrap()
    }
}

fn setup(uid: &str, veth: bool) -> Cleanup {
    Cleanup(Some(
        setup_sandbox_network(&cfg(uid, veth)).expect("setup_sandbox_network"),
    ))
}

fn connect_in(ns: &NetNs, addr: &str, timeout: Duration) -> io::Result<TcpStream> {
    let addr: SocketAddr = addr.parse().unwrap();
    ns.run(move || TcpStream::connect_timeout(&addr, timeout))
}

/// Serves `body` to every connection on `addr` inside `ns`.
fn serve_in(ns: &NetNs, addr: &str, body: &'static str) -> std::net::TcpListener {
    let addr: SocketAddr = addr.parse().unwrap();
    let l = ns
        .run(move || std::net::TcpListener::bind(addr))
        .expect("listen in the actor");
    let accept = l.try_clone().unwrap();
    std::thread::spawn(move || {
        for mut c in accept.incoming().flatten() {
            let _ = c.write_all(body.as_bytes());
        }
    });
    l
}

fn read_all(mut c: TcpStream) -> String {
    let mut s = String::new();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let _ = c.read_to_string(&mut s);
    s
}

fn read_sysctl(ns: &NetNs, key: &str) -> io::Result<String> {
    let path = format!("/proc/sys/{key}");
    ns.run(move || std::fs::read_to_string(&path).map(|s| s.trim().to_string()))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root: creates network namespaces"]
async fn dialing_while_closing_is_safe() {
    let session = SandboxSession::serve(cfg("concurrent-close", false), SessionServices::default())
        .await
        .unwrap();
    let mut workers = Vec::new();
    for _ in 0..32 {
        let s = session.clone();
        workers.push(tokio::spawn(async move {
            for _ in 0..32 {
                match s.dial("udp", "127.0.0.1:9").await {
                    Ok(_) => {}
                    Err(e) => {
                        assert_eq!(
                            e.kind(),
                            io::ErrorKind::NotConnected,
                            "dial during close: {e}"
                        );
                        return;
                    }
                }
            }
        }));
    }
    session.close(None).await.unwrap();
    for w in workers {
        w.await.unwrap();
    }
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn actors_are_reached_only_from_their_own_namespace() {
    let mut actors = Vec::new();
    for (uid, body) in [
        ("11111111-1111-1111-1111-111111111111", "i-am-11111111"),
        ("22222222-2222-2222-2222-222222222222", "i-am-22222222"),
    ] {
        let n = setup(uid, true);
        let l = serve_in(n.runtime_netns(), &format!("{ACTOR_VETH_IP}:80"), body);
        actors.push((n, l, body));
    }
    for (n, _, body) in &actors {
        let c = connect_in(
            n.runtime_netns(),
            &format!("{ACTOR_VETH_IP}:80"),
            Duration::from_secs(5),
        )
        .expect("reaching the actor");
        assert_eq!(read_all(c), *body);
    }
    // Sandbox addresses must not be reachable from the worker namespace.
    let addr: SocketAddr = format!("{ACTOR_VETH_IP}:80").parse().unwrap();
    assert!(
        TcpStream::connect_timeout(&addr, Duration::from_secs(2)).is_err(),
        "the worker namespace reached an actor directly"
    );
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn egress_fails_closed_without_the_tunnel() {
    let n = setup("33333333-3333-3333-3333-333333333333", true);
    for dst in ["93.184.216.34:443", "93.184.216.34:8080"] {
        assert!(
            connect_in(n.runtime_netns(), dst, Duration::from_secs(3)).is_err(),
            "the actor reached {dst} with no tunnel listening"
        );
    }
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn ingress_crosses_the_pair_while_egress_is_captured() {
    let n = setup("44444444-4444-4444-4444-444444444444", true);
    let _app = serve_in(
        n.runtime_netns(),
        &format!("{ACTOR_VETH_IP}:80"),
        "the-actor",
    );
    let c = connect_in(
        n.gateway_netns(),
        &format!("{ACTOR_VETH_IP}:80"),
        Duration::from_secs(5),
    )
    .expect("ingress dial");
    assert_eq!(read_all(c), "the-actor");
    // An unopened port is refused, not redirected to the tunnel.
    assert!(connect_in(
        n.gateway_netns(),
        &format!("{ACTOR_VETH_IP}:81"),
        Duration::from_secs(3)
    )
    .is_err());
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn a_tap_shaped_network_has_one_namespace_and_only_lo() {
    let n = setup("77777777-7777-7777-7777-777777777777", false);
    assert!(n.shares_namespace());
    let names = n.runtime_netns().run(interface_names).unwrap();
    assert_eq!(names, vec!["lo".to_string()]);
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn setup_succeeds_over_a_leftover_namespace() {
    const UID: &str = "aaaaaaaa-0000-0000-0000-00000000000a";
    // An interrupted teardown: the handles are gone, the names remain.
    drop(setup_sandbox_network(&cfg(UID, true)).unwrap());
    for name in [actor_netns_name(UID), gateway_netns_name(UID)] {
        assert!(
            Path::new(netns::NAMED_NETNS_DIR).join(&name).exists(),
            "leftover {name}"
        );
    }
    let n = setup(UID, true);
    n.runtime_netns()
        .run(|| index_of(ACTOR_VETH_NAME))
        .expect("actor interface missing after reuse");
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn actor_udp_has_nowhere_to_go_beyond_the_pair() {
    let n = setup("bbbbbbbb-0000-0000-0000-00000000000b", true);
    for dst in ["93.184.216.34:443", "93.184.216.34:53"] {
        let sent = n.gateway_netns().run(move || {
            let s = UdpSocket::bind("0.0.0.0:0")?;
            s.connect(dst)?;
            s.send(b"probe").map(drop)
        });
        assert!(
            sent.is_err(),
            "the gateway namespace reaches {dst} over UDP"
        );
    }
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn cleanup_releases_a_single_namespace_once() {
    let n = setup_sandbox_network(&cfg("close-once", false)).unwrap();
    assert!(n.shares_namespace());
    cleanup_sandbox_network(n).unwrap();
}

/// Holds its serving task open until released, ignoring cancellation.
struct SlowEgress(Arc<tokio::sync::Notify>);

impl EgressServer for SlowEgress {
    fn bind(&self, _: &str) -> io::Result<ServeFn> {
        let release = self.0.clone();
        Ok(Box::new(move |l, _cancel: CancellationToken| {
            Box::pin(async move {
                release.notified().await;
                drop(l);
                Ok(())
            })
        }))
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root: creates network namespaces"]
async fn close_waits_for_serving_to_stop() {
    let release = Arc::new(tokio::sync::Notify::new());
    let services = SessionServices {
        egress: Some(Arc::new(SlowEgress(release.clone()))),
        ..Default::default()
    };
    let session = SandboxSession::serve(cfg("close-waits", false), services)
        .await
        .unwrap();
    let s = session.clone();
    let mut closing = tokio::spawn(async move { s.close(None).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(250), &mut closing)
            .await
            .is_err(),
        "close returned while the session was still serving"
    );
    release.notify_waiters();
    tokio::time::timeout(Duration::from_secs(30), closing)
        .await
        .expect("close did not return after serving stopped")
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root: creates network namespaces"]
async fn close_reports_a_wait_it_could_not_finish_and_still_removes_namespaces() {
    let release = Arc::new(tokio::sync::Notify::new());
    let services = SessionServices {
        egress: Some(Arc::new(SlowEgress(release.clone()))),
        ..Default::default()
    };
    let session = SandboxSession::serve(cfg("close-deadline", false), services)
        .await
        .unwrap();
    let err = session
        .close(Some(Duration::from_millis(100)))
        .await
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::TimedOut, "{err}");
    assert!(netns::get_from_name(&actor_netns_name("close-deadline")).is_err());
    release.notify_waiters();
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn sandbox_namespaces_do_not_forward() {
    for (uid, veth) in [
        ("cccccccc-0000-0000-0000-00000000000c", true),
        ("dddddddd-0000-0000-0000-00000000000d", false),
    ] {
        let n = setup(uid, veth);
        for ns in [n.runtime_netns(), n.gateway_netns()] {
            for key in [
                "net/ipv4/conf/all/forwarding",
                "net/ipv4/conf/default/forwarding",
            ] {
                assert_eq!(read_sysctl(ns, key).unwrap(), "0", "{key} ({uid})");
            }
        }
        let chains = n.gateway_netns().run(nft::list_chains).unwrap();
        for family in [Family::Ipv4, Family::Ipv6] {
            assert!(
                chains.iter().any(|c| c.family == Some(family)
                    && c.hooknum == Some(nft::hook::FORWARD)
                    && c.policy == Some(nft::verdict::DROP)),
                "no dropping forward chain for {family:?}"
            );
        }
        assert!(chains
            .iter()
            .any(|c| c.family == Some(Family::Ipv4) && c.name == nft::DISALLOWED_EGRESS_CHAIN));
        let rules = n
            .gateway_netns()
            .run(|| nft::rule_count(Family::Ipv4, nft::TABLE_V4, nft::DISALLOWED_EGRESS_CHAIN))
            .unwrap();
        assert_eq!(rules, 3);
    }
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn sandbox_namespaces_do_not_inherit_node_settings() {
    let per_link = [
        ("forwarding", "0"),
        ("accept_redirects", "0"),
        ("route_localnet", "0"),
        ("accept_local", "0"),
        ("rp_filter", "1"),
    ];
    let per_link_v6 = [
        ("forwarding", "0"),
        ("accept_ra", "0"),
        ("accept_redirects", "0"),
    ];
    for (uid, veth) in [
        ("a0a0a0a0-0000-0000-0000-00000000000a", true),
        ("b0b0b0b0-0000-0000-0000-00000000000b", false),
    ] {
        let n = setup(uid, veth);
        // A link added after setup, as a micro-VM tap is.
        let gw_fd = n.gateway_netns().as_raw_fd();
        n.gateway_netns()
            .run(move || {
                let mut r = Route::open()?;
                r.add_veth("late0", "late1", gw_fd)?;
                r.link_up("late0")?;
                r.link_up("late1")
            })
            .expect("adding a late link");
        for ns in [n.runtime_netns(), n.gateway_netns()] {
            let mut want: Vec<(String, String)> = sysctls::SANDBOX_SYSCTLS
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            for name in ns.run(interface_names).unwrap() {
                if name == "lo" {
                    continue;
                }
                want.extend(
                    per_link
                        .iter()
                        .map(|(k, v)| (format!("net/ipv4/conf/{name}/{k}"), v.to_string())),
                );
                want.extend(
                    per_link_v6
                        .iter()
                        .map(|(k, v)| (format!("net/ipv6/conf/{name}/{k}"), v.to_string())),
                );
            }
            for (key, value) in want {
                match read_sysctl(ns, &key) {
                    Err(e)
                        if e.kind() == io::ErrorKind::NotFound && key.starts_with("net/ipv6/") => {}
                    Err(e) => panic!("{key}: {e}"),
                    Ok(got) => assert_eq!(got, value, "{key} ({uid})"),
                }
            }
        }
    }
}

#[test]
#[ignore = "needs root: creates network namespaces"]
fn the_gateway_does_not_route_actor_traffic_over_a_stray_link() {
    let n = setup("eeeeeeee-0000-0000-0000-00000000000e", true);
    // Without disallowed-egress, only forwarding and the forward chain stand
    // between the Actor and the stray link.
    let mut b = nft::Batch::new();
    b.flush_chain(Family::Ipv4, nft::TABLE_V4, nft::DISALLOWED_EGRESS_CHAIN)
        .del_chain(Family::Ipv4, nft::TABLE_V4, nft::DISALLOWED_EGRESS_CHAIN);
    n.gateway_netns()
        .run(move || nft::apply(b, "removing disallowed-egress"))
        .unwrap();

    let probe_name = format!("actornet-forward-probe-{}", std::process::id());
    let probe = netns::create_named(&probe_name).unwrap();
    struct RemoveName(String);
    impl Drop for RemoveName {
        fn drop(&mut self) {
            let _ = netns::remove_named(&self.0);
        }
    }
    let _probe_name = RemoveName(probe_name);

    let gw: std::net::Ipv4Addr = "10.99.0.1".parse().unwrap();
    let probe_ip: std::net::Ipv4Addr = "10.99.0.2".parse().unwrap();
    let probe_fd = probe.as_raw_fd();
    n.gateway_netns()
        .run(move || {
            let mut r = Route::open()?;
            r.add_veth("stray0", "stray1", probe_fd)?;
            r.replace_addr("stray0", gw, 30)?;
            r.link_up("stray0")
        })
        .expect("adding the stray link");
    let listener = probe
        .run(move || {
            let mut r = Route::open()?;
            r.replace_addr("stray1", probe_ip, 30)?;
            r.link_up("stray1")?;
            // A return route, so reverse-path filtering cannot hide a forward.
            r.replace_route("stray1", "169.254.17.0".parse().unwrap(), 30, gw)?;
            UdpSocket::bind("10.99.0.2:9999")
        })
        .expect("setting up the probe");

    let arrives = |wait: Duration| {
        n.runtime_netns()
            .run(|| {
                let s = UdpSocket::bind("0.0.0.0:0")?;
                s.connect("10.99.0.2:9999")?;
                s.send(b"routed").map(drop)
            })
            .expect("sending from the actor");
        listener.set_read_timeout(Some(wait)).unwrap();
        let mut buf = [0u8; 16];
        listener.recv_from(&mut buf).is_ok()
    };

    assert!(
        !arrives(Duration::from_millis(200)),
        "the gateway routed actor traffic as set up"
    );
    n.gateway_netns()
        .run(|| std::fs::write("/proc/sys/net/ipv4/conf/all/forwarding", b"1\n"))
        .expect("re-enabling forwarding");
    assert!(
        !arrives(Duration::from_millis(200)),
        "the gateway routed actor traffic once forwarding was re-enabled; the forward chain is not dropping"
    );
    let mut b = nft::Batch::new();
    b.del_chain(Family::Ipv4, nft::TABLE_V4, nft::FORWARD_CHAIN);
    n.gateway_netns()
        .run(move || nft::apply(b, "removing the forward chain"))
        .unwrap();
    assert!(
        arrives(Duration::from_secs(5)),
        "the probe saw nothing with forwarding on and no forward chain; this test cannot detect routing"
    );
}

async fn accept_original_destination(l: &tokio::net::TcpListener) -> (String, String) {
    let (mut c, _) = l.accept().await.unwrap();
    let dst = hivekeep_gate::tunnel::original_dst::tcp_original_destination(&c).unwrap();
    let mut buf = [0u8; 5];
    use tokio::io::AsyncReadExt;
    let _ = c.read_exact(&mut buf).await;
    (dst, String::from_utf8_lossy(&buf).into_owned())
}

fn tunnel_listener(n: &SandboxNetwork) -> tokio::net::TcpListener {
    let mut ls = netns::listen(n.gateway_netns(), &[EGRESS_PORT]).unwrap();
    let l = ls.remove(0);
    l.set_nonblocking(true).unwrap();
    tokio::net::TcpListener::from_std(l).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root: creates network namespaces"]
async fn egress_reaches_the_tunnel_on_any_port() {
    let n = setup("66666666-6666-6666-6666-666666666666", true);
    let l = tunnel_listener(&n);
    for want in [
        "93.184.216.34:443",
        "93.184.216.34:8080",
        "93.184.216.34:9999",
    ] {
        let ns = n.runtime_netns().try_clone().unwrap();
        let send = tokio::task::spawn_blocking(move || {
            let mut c = connect_in(&ns, want, Duration::from_secs(5))?;
            c.write_all(b"hello")
        });
        let (dst, payload) =
            tokio::time::timeout(Duration::from_secs(10), accept_original_destination(&l))
                .await
                .unwrap_or_else(|_| panic!("the connection to {want} never reached the tunnel"));
        send.await.unwrap().unwrap();
        assert_eq!(dst, want);
        assert_eq!(payload, "hello");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs root: creates network namespaces"]
async fn link_local_destinations_do_not_reach_the_tunnel() {
    let n = setup("99999999-9999-9999-9999-999999999999", true);
    let l = tunnel_listener(&n);
    let err = connect_in(
        n.runtime_netns(),
        "169.254.169.254:80",
        Duration::from_millis(250),
    )
    .expect_err("dial to a link-local address succeeded");
    assert_ne!(
        err.kind(),
        io::ErrorKind::TimedOut,
        "link-local dial timed out instead of being rejected"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), l.accept())
            .await
            .is_err(),
        "a link-local connection reached the tunnel"
    );

    const ALLOWED: &str = "192.0.2.1:443";
    let ns = n.runtime_netns().try_clone().unwrap();
    let send = tokio::task::spawn_blocking(move || {
        let mut c = connect_in(&ns, ALLOWED, Duration::from_secs(5))?;
        c.write_all(b"hello")
    });
    let (dst, _) = tokio::time::timeout(Duration::from_secs(10), accept_original_destination(&l))
        .await
        .expect("accepting the allowed connection");
    send.await.unwrap().unwrap();
    assert_eq!(dst, ALLOWED);
}
