//! Runs the load generator's ladder end to end on loopback: ingress through
//! the real tunnel to an ingress actor, egress through egress actors on
//! Unix sockets. The sandbox redirect needs Linux and root, so here the
//! egress actors send straight to stand-ins for the far end.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use hivekeep_bench_tunnel::admin::{self, Extra};
use hivekeep_bench_tunnel::load::{self, egress_actor, Runner, Targets};
use hivekeep_bench_tunnel::report::Verdict;
use hivekeep_bench_tunnel::sink::{Gateway, GatewayConfig, IngressActor};
use hivekeep_bench_tunnel::{pki, workerhost};
use hivekeep_core::resource::{ActorAttribution, ActorRef};
use hivekeep_gate::egress_policy::DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL;
use hivekeep_gate::install;
use hivekeep_gate::tunnel::egress::BoxIo;
use hivekeep_gate::tunnel::ingress::Dial;
use hivekeep_gate::tunnel::{Tunnel, TunnelConfig};
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn ingress_actor(stats_socket: &Path, cancel: &CancellationToken) -> (IngressActor, String) {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let actor = IngressActor::new(16);
    let (a, c) = (actor.clone(), cancel.clone());
    tokio::spawn(async move { a.serve(lis, c).await });
    let a = actor.clone();
    let extra: Extra = Arc::new(move || {
        let mut m = serde_json::Map::new();
        m.insert(
            "ingress_actor".into(),
            serde_json::to_value(a.stats()).unwrap(),
        );
        m
    });
    tokio::spawn(admin::serve_unix(
        admin::listen_unix(stats_socket).unwrap(),
        admin::stats_handler(Some(extra)),
        cancel.clone(),
    ));
    (actor, addr)
}

async fn discard_server(cancel: &CancellationToken) -> String {
    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            let (mut c, _) =
                tokio::select! { _ = cancel.cancelled() => return, a = lis.accept() => a.unwrap() };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 << 10];
                while matches!(c.read(&mut buf).await, Ok(n) if n > 0) {}
            });
        }
    });
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ladder_runs_scenarios_through_the_tunnel() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cancel = CancellationToken::new();
    let tmp = tempfile::tempdir().unwrap();
    let pki_dir = tmp.path().join("pki");
    let shared = tmp.path().join("shared");
    std::fs::create_dir_all(&shared).unwrap();
    pki::generate(
        &pki_dir,
        &pki::Options {
            worker_hosts: vec!["127.0.0.1".into()],
            gateway_hosts: vec!["127.0.0.1".into()],
            ..Default::default()
        },
    )
    .unwrap();

    // The far ends: an ingress actor, and stand-ins for the egress paths.
    let (actor, actor_addr) = ingress_actor(&shared.join("ingress-actor-0.sock"), &cancel).await;
    let (_actor1, actor1_addr) = ingress_actor(&shared.join("ingress-actor-1.sock"), &cancel).await;
    let discard = discard_server(&cancel).await;
    for i in 0..2 {
        let lis = admin::listen_unix(&shared.join(format!("egress-actor-{i}.sock"))).unwrap();
        let targets = Targets {
            egress: actor_addr.clone(),
            egress_discard: discard.clone(),
            ..Default::default()
        };
        tokio::spawn(egress_actor::serve_with(lis, targets, cancel.clone()));
    }

    // The tunnel, with the worker's flags, routing both Actors to the
    // ingress actor.
    let resolv = tmp.path().join("resolv.conf");
    std::fs::write(&resolv, "nameserver 127.0.0.1\n").unwrap();
    let (ingress_port, connect_port) = (free_port().await, free_port().await);
    let ns = install::SYSTEM_NAMESPACE;
    let cfg = TunnelConfig {
        listen_address: format!("127.0.0.1:{ingress_port}"),
        connect_listen_address: format!("127.0.0.1:{connect_port}"),
        credential_bundle: pki_dir.join(pki::WORKER_BUNDLE),
        trust_bundle: pki_dir.join(pki::CA_FILE),
        client_identity: install::router_spiffe_id(ns),
        keeper_identity: install::keeper_spiffe_id(ns),
        egress_listen_address: "0.0.0.0:15001".into(),
        egress_trust_bundle: pki_dir.join(pki::CA_FILE),
        dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
    };
    let upstream = format!("http://{actor_addr}");
    let tunnel = Tunnel::start_with(
        cfg,
        &upstream,
        &resolv,
        tmp.path().join("keeper.sock"),
        cancel.clone(),
    )
    .await
    .unwrap();
    let mut actors = Vec::new();
    for i in 0..2 {
        let attribution = ActorAttribution {
            actor: ActorRef::new(workerhost::HIVE, format!("actor-{i}")),
            uid: workerhost::actor_uid(i),
            template_hive: String::new(),
            template_name: String::new(),
        };
        let addr = if i == 0 {
            actor_addr.clone()
        } else {
            actor1_addr.clone()
        };
        let dial: Dial = Arc::new(move |_| -> BoxFuture<'static, std::io::Result<BoxIo>> {
            let addr = addr.clone();
            Box::pin(
                async move { Ok(Box::new(tokio::net::TcpStream::connect(addr).await?) as BoxIo) },
            )
        });
        tunnel.activate_ingress(&attribution, dial).unwrap();
        actors.push(
            serde_json::json!({ "actor": format!("bench/actor-{i}"), "uid": attribution.uid }),
        );
    }
    let actor_port: u16 = actor_addr.rsplit_once(':').unwrap().1.parse().unwrap();
    let tunnel_info = serde_json::json!({ "sandboxes": actors, "actor_port": actor_port });
    let extra: Extra = Arc::new(move || {
        let mut m = serde_json::Map::new();
        m.insert("tunnel".into(), tunnel_info.clone());
        m
    });
    let tunnel_admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tunnel_admin_url = format!("http://{}", tunnel_admin.local_addr().unwrap());
    tokio::spawn(admin::serve_tcp(
        tunnel_admin,
        admin::stats_handler(Some(extra)),
        cancel.clone(),
    ));

    // The sink's admin endpoint, as the ladder reads it.
    let gateway = Gateway::new(&GatewayConfig {
        credential_bundle: pki_dir.join(pki::GATEWAY_BUNDLE),
        client_ca: pki_dir.join(pki::CA_FILE),
        body_bytes: 16,
    })
    .unwrap();
    let sink_admin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sink_admin_url = format!("http://{}", sink_admin.local_addr().unwrap());
    tokio::spawn(admin::serve_tcp(
        sink_admin,
        gateway.admin_handler(),
        cancel.clone(),
    ));

    let targets = Targets {
        ingress: format!("127.0.0.1:{ingress_port}"),
        connect: format!("127.0.0.1:{connect_port}"),
        server_name: "127.0.0.1".into(),
        router_bundle: pki_dir.join(pki::ROUTER_BUNDLE),
        ca: pki_dir.join(pki::CA_FILE),
        ..Default::default()
    };
    let mut runner = Runner::new(
        targets,
        tunnel_admin_url,
        sink_admin_url,
        Duration::from_millis(600),
        Duration::from_millis(100),
        16,
        Some(shared.clone()),
    );
    runner.prepare(&cancel).await.expect("prepare");

    for (name, rung) in [
        ("IngressH1", 50.0),
        ("IngressH2", 50.0),
        ("Connect", 20.0),
        ("EgressHTTP", 50.0),
        ("EgressIdleConns", 10.0),
        ("EgressTCP", 1.0),
    ] {
        let s = load::select(name).unwrap().remove(0);
        let (rep, result) = runner.run(&s, &[rung], &cancel).await;
        result.unwrap_or_else(|e| panic!("{name}: {e}"));
        let rep = rep.unwrap();
        println!("{rep}");
        let r = &rep.rungs[0];
        assert!(r.load.completed > 0 && r.load.errors == 0, "{name}: {rep}");
        assert_ne!(r.verdict, Verdict::Down, "{name}: {rep}");
        assert!(r.tunnel.cpus > 0, "{name}: tunnel readings missing: {rep}");
        if name == "EgressIdleConns" {
            assert_eq!(r.load.completed, 10, "{rep}");
        }
    }
    assert!(actor.stats().requests > 0);
    cancel.cancel();
}
