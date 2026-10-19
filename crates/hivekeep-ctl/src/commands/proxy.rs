//! `proxy actor`: forwards local TCP ports to an Actor through the gate.

use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use hivekeep_core::resource::ActorRef;
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use crate::cli::GateArgs;
use crate::context::Ctx;
use crate::error::{CliError, Result};
use crate::gate::{ActorDialer, GateDialer};
use crate::install;
use crate::io::Sink;

/// Local port used when no mapping is given.
pub const DEFAULT_LOCAL_PORT: u16 = 8000;
/// Actor port used when no mapping is given.
pub const DEFAULT_ACTOR_PORT: u16 = 80;

/// One forwarded port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PortMapping {
    /// Local port; 0 picks a free one.
    pub local: u16,
    /// Port on the Actor.
    pub remote: u16,
}

fn port(s: &str, min: u16) -> Option<u16> {
    s.parse::<u16>().ok().filter(|p| *p >= min)
}

/// Parses `[local-port:]remote-port` specs. With none, local 8000 forwards
/// to Actor port 80.
pub fn parse_port_mappings(specs: &[String]) -> Result<Vec<PortMapping>> {
    if specs.is_empty() {
        return Ok(vec![PortMapping {
            local: DEFAULT_LOCAL_PORT,
            remote: DEFAULT_ACTOR_PORT,
        }]);
    }
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(specs.len());
    for spec in specs {
        let mapping = match spec.split_once(':') {
            None => {
                let p = port(spec, 1).ok_or_else(|| {
                    CliError::usage(format!(
                        "invalid port {spec:?}: must be 1-65535 or [local-port:]remote-port"
                    ))
                })?;
                PortMapping {
                    local: p,
                    remote: p,
                }
            }
            Some((local, remote)) => {
                let remote = port(remote, 1).ok_or_else(|| {
                    CliError::usage(format!("invalid remote port in {spec:?}: must be 1-65535"))
                })?;
                let local = if local.is_empty() {
                    0
                } else {
                    port(local, 0).ok_or_else(|| {
                        CliError::usage(format!("invalid local port in {spec:?}: must be 0-65535"))
                    })?
                };
                PortMapping { local, remote }
            }
        };
        if mapping.local != 0 && !seen.insert(mapping.local) {
            return Err(CliError::usage(format!(
                "duplicate local port {}",
                mapping.local
            )));
        }
        out.push(mapping);
    }
    Ok(out)
}

/// Accepts on every listener and forwards each connection to the paired
/// Actor port, until a listener fails.
pub async fn serve<D: ActorDialer>(
    listeners: Vec<(TcpListener, u16)>,
    actor: ActorRef,
    dialer: Arc<D>,
    err: Sink,
) -> io::Result<()> {
    let mut accept_loops = JoinSet::new();
    for (listener, remote) in listeners {
        let (actor, dialer, err) = (actor.clone(), dialer.clone(), err.clone());
        accept_loops.spawn(async move {
            let mut conns = JoinSet::new();
            loop {
                let (mut client, _) = match listener.accept().await {
                    Ok(accepted) => accepted,
                    Err(e) => return Err::<(), io::Error>(e),
                };
                let (actor, dialer, err) = (actor.clone(), dialer.clone(), err.clone());
                conns.spawn(async move {
                    match dialer.dial(&actor, remote).await {
                        Ok(mut upstream) => {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
                        }
                        Err(e) => err.line(&format!("Failed to connect to {actor}:{remote}: {e}")),
                    }
                });
                // Reap finished connections so the set does not grow.
                while conns.try_join_next().is_some() {}
            }
        });
    }
    match accept_loops.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(io::Error::other(e)),
        None => Ok(()),
    }
}

/// `proxy actor <name> [ports...]`.
pub async fn run(
    ctx: &mut Ctx,
    name: &str,
    hive: &str,
    ports: &[String],
    address: &str,
    gate: &GateArgs,
) -> Result<()> {
    let actor = ActorRef::new(hive, name);
    let mappings = parse_port_mappings(ports)?;
    let server_name = gate
        .gate_server_name
        .clone()
        .unwrap_or_else(install::gate_server_name);
    let dialer = GateDialer::new(
        &gate.gate,
        &gate.gate_ca_file,
        &server_name,
        &gate.client_cert,
        &gate.client_key,
    )
    .map_err(|e| CliError::failed(format!("failed to set up the gate connection: {e}")))?;

    let mut listeners = Vec::with_capacity(mappings.len());
    for m in &mappings {
        let listener = TcpListener::bind((address, m.local))
            .await
            .map_err(|e| CliError::failed(format!("listen on {address}:{}: {e}", m.local)))?;
        let local = listener.local_addr()?;
        ctx.err()
            .line(&format!("Proxying {local} -> {actor}:{}", m.remote));
        listeners.push((listener, m.remote));
    }
    serve(listeners, actor, Arc::new(dialer), ctx.err())
        .await
        .map_err(|e| CliError::failed(format!("proxy stopped: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::BoxedConn;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn specs(s: &[&str]) -> Vec<String> {
        s.iter().map(|s| s.to_string()).collect()
    }

    fn m(local: u16, remote: u16) -> PortMapping {
        PortMapping { local, remote }
    }

    #[test]
    fn port_mappings() {
        for (input, want) in [
            (&[][..], vec![m(8000, 80)]),
            (&["9090"], vec![m(9090, 9090)]),
            (&["8080:80"], vec![m(8080, 80)]),
            (&[":80", "0:9090"], vec![m(0, 80), m(0, 9090)]),
            (&["8000:80", "9090"], vec![m(8000, 80), m(9090, 9090)]),
        ] {
            assert_eq!(
                parse_port_mappings(&specs(input)).unwrap(),
                want,
                "{input:?}"
            );
        }
        for input in [
            &["0"][..],
            &["http"],
            &["8000:"],
            &["8000:0"],
            &["8000:65536"],
            &["-1:80"],
            &["65536:80"],
            &["8000:80:80"],
            &["8000:80", "8000:9090"],
        ] {
            let err = parse_port_mappings(&specs(input)).unwrap_err();
            assert!(err.is_usage(), "{input:?}");
        }
    }

    /// Dials a local echo server, recording what it was asked for.
    struct EchoDialer {
        addr: std::net::SocketAddr,
        asked: std::sync::Mutex<Vec<(String, u16)>>,
    }

    impl ActorDialer for EchoDialer {
        async fn dial(&self, actor: &ActorRef, port: u16) -> io::Result<BoxedConn> {
            self.asked.lock().unwrap().push((actor.to_string(), port));
            Ok(Box::new(TcpStream::connect(self.addr).await?))
        }
    }

    struct FailingDialer;

    impl ActorDialer for FailingDialer {
        async fn dial(&self, _: &ActorRef, _: u16) -> io::Result<BoxedConn> {
            Err(io::Error::other("router unavailable"))
        }
    }

    async fn echo_server() -> std::net::SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                tokio::spawn(async move {
                    let (mut r, mut w) = s.split();
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn forwards_each_port() {
        let dialer = Arc::new(EchoDialer {
            addr: echo_server().await,
            asked: Default::default(),
        });
        let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (a_addr, b_addr) = (a.local_addr().unwrap(), b.local_addr().unwrap());
        let server = tokio::spawn(serve(
            vec![(a, 80), (b, 9090)],
            ActorRef::new("team", "a1"),
            dialer.clone(),
            Sink::null(),
        ));
        for addr in [a_addr, b_addr] {
            let mut c = TcpStream::connect(addr).await.unwrap();
            c.write_all(b"hello").await.unwrap();
            let mut got = [0u8; 5];
            c.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"hello");
        }
        let mut asked = dialer.asked.lock().unwrap().clone();
        asked.sort();
        assert_eq!(
            asked,
            [("team/a1".to_string(), 80), ("team/a1".to_string(), 9090)]
        );
        server.abort();
    }

    /// Dials a fresh backend per connection that reads the whole request,
    /// then answers with the Actor, the port and what it read.
    struct ReplyingDialer;

    impl ActorDialer for ReplyingDialer {
        async fn dial(&self, actor: &ActorRef, port: u16) -> io::Result<BoxedConn> {
            let backend = TcpListener::bind("127.0.0.1:0").await?;
            let addr = backend.local_addr()?;
            let tag = format!("{actor}:{port}:");
            tokio::spawn(async move {
                let (mut s, _) = backend.accept().await.unwrap();
                let mut payload = Vec::new();
                s.read_to_end(&mut payload).await.unwrap();
                let mut reply = tag.into_bytes();
                reply.extend(payload);
                s.write_all(&reply).await.unwrap();
            });
            Ok(Box::new(TcpStream::connect(addr).await?))
        }
    }

    #[tokio::test]
    async fn half_closed_requests_get_their_reply() {
        let a = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let b = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (a_addr, b_addr) = (a.local_addr().unwrap(), b.local_addr().unwrap());
        let server = tokio::spawn(serve(
            vec![(a, 80), (b, 9090)],
            ActorRef::new("team", "my-counter"),
            Arc::new(ReplyingDialer),
            Sink::null(),
        ));
        for (addr, send, want) in [
            (a_addr, "hello", "team/my-counter:80:hello"),
            (b_addr, "metrics", "team/my-counter:9090:metrics"),
        ] {
            let mut c = TcpStream::connect(addr).await.unwrap();
            c.write_all(send.as_bytes()).await.unwrap();
            c.shutdown().await.unwrap();
            let mut got = String::new();
            c.read_to_string(&mut got).await.unwrap();
            assert_eq!(got, want);
        }
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn dial_failures_are_reported_and_closed() {
        let (err, buf) = Sink::buffer();
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let server = tokio::spawn(serve(
            vec![(l, 80)],
            ActorRef::new("team", "a1"),
            Arc::new(FailingDialer),
            err,
        ));
        let mut c = TcpStream::connect(addr).await.unwrap();
        let mut rest = Vec::new();
        c.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        assert!(buf
            .contents()
            .contains("Failed to connect to team/a1:80: router unavailable"));
        server.abort();
    }
}
