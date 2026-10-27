//! The tunnel hosting Actors with the worker's own code, through real
//! network namespaces, veths and nftables. Needs Linux, root, and the `ip`
//! and `nft` tools: run with `sudo -E cargo test -p hivekeep-bench-tunnel
//! --test workerhost_linux -- --ignored`.

#![cfg(target_os = "linux")]

use std::time::Duration;

use hivekeep_bench_tunnel::fake_keeper::FakeKeeper;
use hivekeep_bench_tunnel::httpio::{self, BodyKind};
use hivekeep_bench_tunnel::pki;
use hivekeep_bench_tunnel::sink::{Gateway, GatewayConfig};
use hivekeep_bench_tunnel::workerhost::{Host, ACTOR_PORT};
use hivekeep_gate::egress_policy::DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL;
use hivekeep_gate::headers::TARGET_ACTOR_HEADER;
use hivekeep_gate::install;
use hivekeep_gate::netns::{self, NsConn};
use hivekeep_gate::tunnel::TunnelConfig;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn free_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}

/// Starts the tunnel with two Actors, then sends ingress to a server in a
/// sandbox and egress out of it, through the sandbox's real network and
/// redirect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs root on Linux: creates network namespaces and nftables rules"]
async fn hosts_actors_with_the_workers_code() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cancel = CancellationToken::new();
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    pki::generate(
        dir,
        &pki::Options {
            worker_hosts: vec!["127.0.0.1".into()],
            gateway_hosts: vec!["127.0.0.1".into()],
            ..Default::default()
        },
    )
    .unwrap();

    let socket = dir.join("worker-support.sock");
    let keeper = FakeKeeper::new(dir).unwrap();
    let (d, s, c) = (dir.to_path_buf(), socket.clone(), cancel.clone());
    tokio::spawn(async move { keeper.serve(&d, &s, c).await });

    let gateway = Gateway::new(&GatewayConfig {
        credential_bundle: dir.join(pki::GATEWAY_BUNDLE),
        client_ca: dir.join(pki::CA_FILE),
        body_bytes: 16,
    })
    .unwrap();
    let gateway_lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_addr = gateway_lis.local_addr().unwrap().to_string();
    let (g, c) = (gateway.clone(), cancel.clone());
    tokio::spawn(async move { g.serve(gateway_lis, c).await });

    let resolv = dir.join("resolv.conf");
    std::fs::write(&resolv, "nameserver 127.0.0.1\n").unwrap();
    let ingress_addr = free_address().await;
    let ns = install::SYSTEM_NAMESPACE;
    let host = Host::start_with(
        TunnelConfig {
            listen_address: ingress_addr.clone(),
            connect_listen_address: free_address().await,
            credential_bundle: dir.join(pki::WORKER_BUNDLE),
            trust_bundle: dir.join(pki::CA_FILE),
            client_identity: install::router_spiffe_id(ns),
            keeper_identity: install::keeper_spiffe_id(ns),
            egress_listen_address: "0.0.0.0:15001".into(),
            egress_trust_bundle: dir.join(pki::CA_FILE),
            dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
        },
        2,
        &gateway_addr,
        &resolv,
        socket,
        cancel.clone(),
    )
    .await
    .unwrap();
    let info = host.info();
    assert_eq!(
        (info.sandboxes.len(), info.actor_port),
        (2, ACTOR_PORT),
        "{info:?}"
    );

    // The ingress actor: a server inside the first Actor's sandbox.
    let sandbox = netns::NetNs::open(&info.sandboxes[0].netns).unwrap();
    let lis = netns::listen(&sandbox, &[ACTOR_PORT]).unwrap().remove(0);
    lis.set_nonblocking(true).unwrap();
    let lis = TcpListener::from_std(lis).unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut c, _)) = lis.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut r = BufReader::new(&mut c);
                while let Ok(Some(_)) = httpio::read_head(&mut r).await {
                    let _ = r
                        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 13\r\n\r\ningress-actor")
                        .await;
                }
            });
        }
    });

    // Ingress, as the router sends it.
    let tls = pki::client_config(
        &dir.join(pki::ROUTER_BUNDLE),
        &dir.join(pki::CA_FILE),
        &[b"http/1.1"],
    )
    .unwrap();
    let tcp = tokio::net::TcpStream::connect(&ingress_addr).await.unwrap();
    let conn = tokio_rustls::TlsConnector::from(tls)
        .connect(ServerName::try_from("127.0.0.1").unwrap(), tcp)
        .await
        .unwrap();
    let mut r = BufReader::new(conn);
    r.write_all(format!("GET / HTTP/1.1\r\nHost: actor\r\n{TARGET_ACTOR_HEADER}: {}\r\nConnection: close\r\n\r\n", info.sandboxes[0].actor).as_bytes()).await.unwrap();
    let head = httpio::read_head(&mut r).await.unwrap().unwrap();
    assert_eq!(head.status(), Some(200), "ingress: {head:?}");
    assert_eq!(
        httpio::discard_body(&mut r, &head, BodyKind::Response)
            .await
            .unwrap(),
        13,
        "ingress reached something else"
    );

    // Egress from inside the sandbox, redirected to the tunnel and carried
    // to the gateway, which answers on port 80.
    let NsConn::Tcp(conn) = netns::dial(&sandbox, "tcp", "192.0.2.10:80").await.unwrap() else {
        panic!("not TCP")
    };
    let mut r = BufReader::new(conn);
    r.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let head = tokio::time::timeout(Duration::from_secs(10), httpio::read_head(&mut r))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        httpio::discard_body(&mut r, &head, BodyKind::Response)
            .await
            .unwrap(),
        16,
        "egress did not reach the gateway"
    );
    assert!(
        gateway.stats().tunnels > 0,
        "gateway saw no tunnels: {:?}",
        gateway.stats()
    );

    tokio::time::timeout(Duration::from_secs(30), host.close())
        .await
        .unwrap()
        .unwrap();
    cancel.cancel();
}
