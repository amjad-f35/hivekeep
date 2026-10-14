use clap::Parser;
use hivekeep_core::resource::ActorRef;
use hivekeep_proto::keeper::v1::worker_support_server::WorkerSupport;
use parking_lot::Mutex;
use tonic::{Request, Response, Status};

use super::*;
use crate::egress_policy::DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL;
use crate::test_pki::TestCa;
use crate::tunnel::egress::credential::tests::{serve_keeper, FakeKeeper};
use crate::tunnel::keeper_dial::DEFAULT_KEEPER_SPIFFE_ID;

#[derive(Parser, Debug)]
struct Cli {
    #[command(flatten)]
    tunnel: TunnelConfig,
}

#[test]
fn register_flags() {
    let defaults = Cli::try_parse_from(["t"]).unwrap().tunnel;
    assert_eq!(
        (
            defaults.listen_address.as_str(),
            defaults.connect_listen_address.as_str(),
            defaults.egress_listen_address.as_str()
        ),
        (":443", ":8443", "0.0.0.0:15001")
    );
    assert!(!defaults.client_identity.is_empty() && !defaults.keeper_identity.is_empty());
    assert!(
        !defaults.credential_bundle.as_os_str().is_empty()
            && !defaults.egress_trust_bundle.as_os_str().is_empty()
    );
    assert_eq!(defaults.dns_egress_policy, "");
    let parsed = Cli::try_parse_from([
        "t",
        "--tunnel-listen-address=:1",
        "--tunnel-connect-listen-address=:2",
        "--tunnel-credential-bundle=cred",
        "--tunnel-trust-bundle=trust",
        "--tunnel-client-identity=client",
        "--tunnel-keeper-identity=broker",
        "--tunnel-egress-listen-address=0.0.0.0:3",
        "--tunnel-egress-trust-bundle=egress-trust",
        "--dns-egress-policy=enforce-in-tunnel",
    ])
    .unwrap()
    .tunnel;
    assert_eq!(
        parsed,
        TunnelConfig {
            listen_address: ":1".into(),
            connect_listen_address: ":2".into(),
            credential_bundle: "cred".into(),
            trust_bundle: "trust".into(),
            client_identity: "client".into(),
            keeper_identity: "broker".into(),
            egress_listen_address: "0.0.0.0:3".into(),
            egress_trust_bundle: "egress-trust".into(),
            dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
        }
    );
}

#[derive(Default)]
struct PolicyKeeper {
    resp: Mutex<Option<pb_keeper::GetActorEgressPolicyResponse>>,
    got: Mutex<Vec<pb_keeper::GetActorEgressPolicyRequest>>,
}

use hivekeep_proto::keeper::v1 as pb_keeper;

struct KeeperSvc(Arc<PolicyKeeper>);

#[tonic::async_trait]
impl WorkerSupport for KeeperSvc {
    async fn mint_actor_certificate(
        &self,
        _: Request<pb_keeper::MintActorCertificateRequest>,
    ) -> Result<Response<pb_keeper::MintActorCertificateResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn acquire_worker_epoch(
        &self,
        _: Request<pb_keeper::AcquireWorkerEpochRequest>,
    ) -> Result<Response<pb_keeper::AcquireWorkerEpochResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn register_worker(
        &self,
        _: Request<pb_keeper::RegisterWorkerRequest>,
    ) -> Result<Response<pb_keeper::RegisterWorkerResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn set_worker_sandbox_runtimes(
        &self,
        _: Request<pb_keeper::SetWorkerSandboxRuntimesRequest>,
    ) -> Result<Response<pb_keeper::SetWorkerSandboxRuntimesResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn request_actor_suspend(
        &self,
        _: Request<pb_keeper::RequestActorSuspendRequest>,
    ) -> Result<Response<pb_keeper::RequestActorSuspendResponse>, Status> {
        Err(Status::unimplemented(""))
    }
    async fn get_actor_egress_policy(
        &self,
        req: Request<pb_keeper::GetActorEgressPolicyRequest>,
    ) -> Result<Response<pb_keeper::GetActorEgressPolicyResponse>, Status> {
        self.0.got.lock().push(req.into_inner());
        match self.0.resp.lock().clone() {
            Some(r) => Ok(Response::new(r)),
            None => Err(Status::unavailable("the control plane is unavailable")),
        }
    }
    async fn report_actor_crashed(
        &self,
        _: Request<pb_keeper::ReportActorCrashedRequest>,
    ) -> Result<Response<pb_keeper::ReportActorCrashedResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

fn free_address() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}

struct Env {
    cfg: TunnelConfig,
    resolv: tempfile::TempDir,
    keeper: FakeKeeper,
    svc: Arc<PolicyKeeper>,
}

impl Env {
    fn resolv_conf(&self) -> std::path::PathBuf {
        self.resolv.path().join("resolv.conf")
    }

    async fn start(&self, cfg: TunnelConfig, upstream: &str) -> Result<Tunnel, String> {
        Tunnel::start_with(
            cfg,
            upstream,
            &self.resolv_conf(),
            self.keeper.socket.clone(),
            CancellationToken::new(),
        )
        .await
    }
}

async fn env() -> Env {
    let ca = TestCa::new();
    let svc = Arc::new(PolicyKeeper::default());
    let keeper = serve_keeper(&ca, "node-a", KeeperSvc(svc.clone())).await;
    let resolv = tempfile::tempdir().unwrap();
    std::fs::write(resolv.path().join("resolv.conf"), "nameserver 127.0.0.1\n").unwrap();
    let cfg = TunnelConfig {
        listen_address: free_address(),
        connect_listen_address: free_address(),
        credential_bundle: keeper.cred.clone(),
        trust_bundle: keeper.trust.clone(),
        client_identity: "spiffe://cluster.local/ns/hivekeep-system/sa/hivekeep-gate-router".into(),
        keeper_identity: DEFAULT_KEEPER_SPIFFE_ID.into(),
        egress_listen_address: "0.0.0.0:15001".into(),
        egress_trust_bundle: keeper.trust.clone(),
        dns_egress_policy: DNS_EGRESS_POLICY_ENFORCE_IN_TUNNEL.into(),
    };
    Env {
        cfg,
        resolv,
        keeper,
        svc,
    }
}

const UPSTREAM: &str = "http://169.254.17.2:80";

#[tokio::test]
async fn start() {
    let e = env().await;
    let t = e.start(e.cfg.clone(), UPSTREAM).await.unwrap();
    assert_eq!(t.egress_port, 15001);
    for addr in [&e.cfg.listen_address, &e.cfg.connect_listen_address] {
        tokio::net::TcpStream::connect(addr.as_str())
            .await
            .unwrap_or_else(|err| panic!("nothing serves {addr}: {err}"));
    }
}

#[tokio::test]
async fn start_errors() {
    let in_use = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy = in_use.local_addr().unwrap().to_string();
    let e = env().await;
    type Edit = Box<dyn Fn(&mut TunnelConfig, &Env)>;
    let cases: Vec<(&str, Edit)> = vec![
        (
            "missing dns egress policy",
            Box::new(|c, _| c.dns_egress_policy = String::new()),
        ),
        (
            "invalid dns egress policy",
            Box::new(|c, _| c.dns_egress_policy = "invalid".into()),
        ),
        (
            "no resolvers",
            Box::new(|_, e| std::fs::write(e.resolv_conf(), "").unwrap()),
        ),
        (
            "missing credentials",
            Box::new(|c, e| c.credential_bundle = e.resolv.path().join("absent.pem")),
        ),
        (
            "no keeper identity",
            Box::new(|c, _| c.keeper_identity = String::new()),
        ),
        ("ingress address in use", {
            let b = busy.clone();
            Box::new(move |c, _| c.listen_address = b.clone())
        }),
        ("CONNECT address in use", {
            let b = busy.clone();
            Box::new(move |c, _| c.connect_listen_address = b.clone())
        }),
        (
            "egress address without a port",
            Box::new(|c, _| c.egress_listen_address = "0.0.0.0".into()),
        ),
    ];
    for (name, edit) in cases {
        std::fs::write(e.resolv_conf(), "nameserver 127.0.0.1\n").unwrap();
        let mut cfg = e.cfg.clone();
        cfg.listen_address = free_address();
        cfg.connect_listen_address = free_address();
        edit(&mut cfg, &e);
        assert!(e.start(cfg, UPSTREAM).await.is_err(), "{name}");
    }
    std::fs::write(e.resolv_conf(), "nameserver 127.0.0.1\n").unwrap();
    let mut cfg = e.cfg.clone();
    cfg.listen_address = free_address();
    cfg.connect_listen_address = free_address();
    assert!(e.start(cfg, "http://[::1").await.is_err(), "bad upstream");
}

fn actor() -> ActorAttribution {
    ActorAttribution {
        actor: ActorRef::new("space", "actor"),
        uid: "uid".into(),
        template_hive: String::new(),
        template_name: String::new(),
    }
}

#[tokio::test]
async fn prepare_egress() {
    let e = env().await;
    let t = e.start(e.cfg.clone(), UPSTREAM).await.unwrap();
    assert!(t.prepare_egress(&actor(), None).await.unwrap().is_none());
    for (name, address) in [
        ("empty address", ""),
        ("missing a port", "gateway.example"),
        ("unbalanced port", "[::1"),
    ] {
        let gw = worker::EgressGateway {
            address: address.into(),
        };
        assert!(
            t.prepare_egress(&actor(), Some(&gw)).await.is_err(),
            "{name}"
        );
    }
}

#[tokio::test]
async fn activate_and_deactivate() {
    let e = env().await;
    let t = e.start(e.cfg.clone(), UPSTREAM).await.unwrap();
    let dial: Dial = Arc::new(|_| {
        Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::NotConnected)) })
    });
    t.activate_egress(&actor(), None).unwrap();
    t.activate_ingress(&actor(), dial.clone()).unwrap();
    t.deactivate(&actor()).await;
    t.deactivate(&actor()).await;
    let nameless = ActorAttribution {
        actor: ActorRef::new("", ""),
        ..actor()
    };
    let err = t.activate_ingress(&nameless, dial).unwrap_err();
    assert!(err.contains("while activating Actor ingress"), "{err}");
}

#[tokio::test]
async fn get_egress_policy_cases() {
    let e = env().await;
    let policy = pb_keeper::EgressPolicy {
        rules: vec![pb_keeper::EgressRule {
            https: Some(pb_keeper::EgressDestinations {
                hostnames: vec!["example.com".into()],
                ports: None,
            }),
            ..Default::default()
        }],
    };
    let tls =
        keeper_dial::tls_config(&e.keeper.cred, &e.keeper.trust, DEFAULT_KEEPER_SPIFFE_ID).unwrap();
    let channel = keeper_dial::connect(&e.keeper.socket, tls).await.unwrap();
    let mut client = keeper::worker_support_client::WorkerSupportClient::new(channel);
    for (name, resp, want, want_err) in [
        (
            "policy",
            Some(pb_keeper::GetActorEgressPolicyResponse {
                egress_policy: Some(policy.clone()),
            }),
            Some(policy.clone()),
            false,
        ),
        (
            "no policy",
            Some(pb_keeper::GetActorEgressPolicyResponse::default()),
            None,
            false,
        ),
        ("keeper error", None, None, true),
    ] {
        *e.svc.resp.lock() = resp;
        let got = get_egress_policy(&mut client, &ActorRef::new("space", "actor")).await;
        assert_eq!(got.is_err(), want_err, "{name}");
        if let Ok(got) = got {
            assert_eq!(got, want, "{name}");
        } else {
            assert_eq!(got.unwrap_err().code(), tonic::Code::Unavailable);
        }
        let req = e.svc.got.lock().pop().unwrap();
        assert_eq!(
            (req.actor_hive.as_str(), req.actor_name.as_str()),
            ("space", "actor"),
            "{name}"
        );
    }
}

#[tokio::test]
async fn tunnel_fetches_actor_egress_policy_from_keeper() {
    let e = env().await;
    let t = e.start(e.cfg.clone(), UPSTREAM).await.unwrap();
    let policy = pb_keeper::EgressPolicy {
        rules: vec![pb_keeper::EgressRule {
            https: Some(pb_keeper::EgressDestinations {
                hostnames: vec!["example.com".into()],
                ports: None,
            }),
            ..Default::default()
        }],
    };
    for (name, resp, want) in [
        (
            "policy",
            pb_keeper::GetActorEgressPolicyResponse {
                egress_policy: Some(policy.clone()),
            },
            Some(policy.clone()),
        ),
        (
            "empty policy",
            pb_keeper::GetActorEgressPolicyResponse {
                egress_policy: Some(Default::default()),
            },
            Some(Default::default()),
        ),
        (
            "no policy",
            pb_keeper::GetActorEgressPolicyResponse::default(),
            None,
        ),
    ] {
        *e.svc.resp.lock() = Some(resp);
        let got = t
            .actor_egress_policy(&actor())
            .egress_policy()
            .await
            .unwrap();
        assert_eq!(got.as_deref(), want.as_ref(), "{name}");
    }
    let got = e.svc.got.lock();
    assert_eq!(got.len(), 3);
    assert!(got
        .iter()
        .all(|r| r.actor_hive == "space" && r.actor_name == "actor"));
}
