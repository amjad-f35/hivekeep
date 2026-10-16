//! Waiting for an Actor's containers to answer their wakeup probes: an HTTP
//! GET polled through the sandbox dialer until it returns 200.

use std::time::{Duration, Instant};

use hivekeep_proto::worker::v1 as pb;
use hivekeep_sandbox_gvisor::error::{Error, Result};
use hivekeep_sandbox_gvisor::ports::{direct_dialer, Dialer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

/// How long one probe request may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_millis(250);
/// The pause between attempts. It bounds how late readiness is noticed.
pub const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// The probe's endpoint. A path without a leading slash gets one.
pub fn url(probe: &pb::WakeupProbe, actor_ip: &str) -> Result<String> {
    let hg = probe
        .http_get
        .as_ref()
        .ok_or_else(|| Error::msg("httpGet is required"))?;
    if !(1..=65535).contains(&hg.port) {
        return Err(Error::msg(format!("invalid port {}", hg.port)));
    }
    if hg.path.is_empty() {
        return Err(Error::msg("path is required"));
    }
    let path = if hg.path.starts_with('/') {
        hg.path.clone()
    } else {
        format!("/{}", hg.path)
    };
    Ok(format!("http://{actor_ip}:{}{path}", hg.port))
}

/// How long the probe may take to pass.
pub fn poll_timeout(probe: &pb::WakeupProbe) -> Result<Duration> {
    match probe.timeout_seconds {
        s if s > 0 => Ok(Duration::from_secs(s as u64)),
        s => Err(Error::msg(format!(
            "timeout_seconds must be positive, got {s}"
        ))),
    }
}

/// Splits a probe URL into the dial address and the request path.
fn split_url(url: &str) -> (&str, &str) {
    let rest = url.strip_prefix("http://").unwrap_or(url);
    match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    }
}

/// Polls one container's probe through `dial` (direct when `None`) until it
/// returns 200, `cancel` fires, or the probe's own timeout passes.
pub async fn wait(
    container: &str,
    probe: &pb::WakeupProbe,
    actor_ip: &str,
    dial: Option<Dialer>,
    cancel: &CancellationToken,
) -> Result<()> {
    let invalid = |e: Error| {
        Error::msg(format!(
            "invalid wakeup probe config for {container:?}: {e}"
        ))
    };
    let url = url(probe, actor_ip).map_err(invalid)?;
    let timeout = poll_timeout(probe).map_err(invalid)?;
    let (addr, path) = split_url(&url);
    let dial = dial.unwrap_or_else(direct_dialer);
    let start = Instant::now();
    let mut attempts = 0u32;
    let mut last_err = String::from("<nil>");
    loop {
        if cancel.is_cancelled() {
            return Err(Error::Context {
                context: format!(
                    "wakeup probe cancelled for {container:?} after {:?} ({attempts} attempts, last error: {last_err})",
                    start.elapsed()
                ),
                source: Box::new(Error::Cancelled),
            });
        }
        if start.elapsed() > timeout {
            return Err(Error::msg(format!(
                "wakeup probe for {container:?} never returned 200 within {timeout:?} ({attempts} attempts, last error: {last_err})"
            )));
        }
        attempts += 1;
        let attempt = tokio::time::timeout(REQUEST_TIMEOUT, get(&dial, addr, path));
        let res = tokio::select! {
            r = attempt => r,
            _ = cancel.cancelled() => continue,
        };
        match res {
            Ok(Ok(200)) => {
                tracing::info!(container, url, elapsed = ?start.elapsed(), attempts, "Wakeup probe reached 200");
                return Ok(());
            }
            Ok(Ok(code)) => last_err = format!("HTTP {code}"),
            Ok(Err(e)) => last_err = e.to_string(),
            Err(_) => last_err = "request timed out".into(),
        }
        tokio::select! {
            _ = cancel.cancelled() => {}
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }
    }
}

/// Waits for every container with a probe, failing with the first error
/// (which stops the others). Containers without a probe have no wakeup gate.
pub async fn wait_all(
    containers: &[pb::Container],
    actor_ip: &str,
    dial: Option<Dialer>,
    cancel: &CancellationToken,
) -> Result<()> {
    let group = cancel.child_token();
    let waits = containers.iter().filter_map(|c| {
        let probe = c.wakeup_probe.as_ref()?;
        let (dial, group) = (dial.clone(), group.clone());
        Some(async move {
            let r = wait(&c.name, probe, actor_ip, dial, &group).await;
            if r.is_err() {
                group.cancel();
            }
            r
        })
    });
    let results = futures::future::join_all(waits).await;
    // The first error by time is the one that cancelled the rest; report it
    // rather than a cancellation it caused.
    let mut errs: Vec<Error> = results.into_iter().filter_map(|r| r.err()).collect();
    if let Some(i) = errs.iter().position(|e| !e.is_context_error()) {
        return Err(errs.swap_remove(i));
    }
    errs.into_iter().next().map_or(Ok(()), Err)
}

/// One GET; the response's status code.
async fn get(dial: &Dialer, addr: &str, path: &str) -> std::io::Result<u16> {
    let mut conn = dial(addr.to_string()).await?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nUser-Agent: hivekeep-wakeup-probe\r\n\r\n"
    );
    conn.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(64);
    let mut chunk = [0u8; 256];
    while !buf.windows(2).any(|w| w == b"\r\n") {
        let n = conn.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    String::from_utf8_lossy(&buf)
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "malformed HTTP status line",
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn probe(path: &str, port: i32, timeout: i32) -> pb::WakeupProbe {
        pb::WakeupProbe {
            http_get: Some(pb::HttpGetAction {
                path: path.into(),
                port,
            }),
            timeout_seconds: timeout,
        }
    }

    /// Serves `/readyz` with 200 once `ready` is set, 503 before; any other
    /// path is 404.
    async fn server(ready: Arc<AtomicBool>) -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let ready = ready.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 1024];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req.split_whitespace().nth(1).unwrap_or("");
                    let status = match (path, ready.load(Ordering::SeqCst)) {
                        ("/readyz", true) => "200 OK",
                        ("/readyz", false) => "503 Service Unavailable",
                        _ => "404 Not Found",
                    };
                    let _ = s
                        .write_all(
                            format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                        )
                        .await;
                });
            }
        });
        port
    }

    fn free_port() -> i32 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port() as i32
    }

    #[test]
    fn builds_urls() {
        let empty = pb::WakeupProbe::default();
        for (name, p, ip, want) in [
            ("missing path", probe("", 8080, 0), "169.254.17.2", None),
            (
                "explicit path",
                probe("/health", 9000, 0),
                "169.254.17.2",
                Some("http://169.254.17.2:9000/health"),
            ),
            (
                "path without leading slash is normalized",
                probe("ready", 80, 0),
                "10.0.0.1",
                Some("http://10.0.0.1:80/ready"),
            ),
            ("missing httpGet", empty, "1.2.3.4", None),
            ("port zero", probe("/x", 0, 0), "1.2.3.4", None),
            ("port too large", probe("/x", 70000, 0), "1.2.3.4", None),
        ] {
            assert_eq!(url(&p, ip).ok().as_deref(), want, "{name}");
        }
    }

    #[test]
    fn poll_timeouts() {
        let t = |s| pb::WakeupProbe {
            timeout_seconds: s,
            ..Default::default()
        };
        assert!(poll_timeout(&t(0)).is_err(), "unset is rejected");
        assert_eq!(poll_timeout(&t(300)).unwrap(), Duration::from_secs(300));
        assert!(poll_timeout(&t(-1)).is_err(), "negative is rejected");
    }

    #[tokio::test]
    async fn returns_on_first_200() {
        let port = server(Arc::new(AtomicBool::new(true))).await;
        wait(
            "main",
            &probe("/readyz", port.into(), 1),
            "127.0.0.1",
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn waits_for_the_server_to_become_ready() {
        let ready = Arc::new(AtomicBool::new(false));
        let port = server(ready.clone()).await;
        let flip = ready.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flip.store(true, Ordering::SeqCst);
        });
        let start = Instant::now();
        wait(
            "main",
            &probe("/readyz", port.into(), 1),
            "127.0.0.1",
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(40),
            "returned early: {elapsed:?}"
        );
        assert!(
            elapsed <= Duration::from_millis(500),
            "returned late: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn honours_cancellation() {
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            c.cancel();
        });
        let err = wait(
            "main",
            &probe("/readyz", free_port(), 1),
            "127.0.0.1",
            None,
            &cancel,
        )
        .await
        .unwrap_err();
        assert!(err.is_context_error(), "{err}");
    }

    #[tokio::test]
    async fn gives_up_at_the_probe_timeout() {
        let start = Instant::now();
        let err = wait(
            "main",
            &probe("/readyz", free_port(), 1),
            "127.0.0.1",
            None,
            &CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("never returned 200"), "{err}");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_secs(1) && elapsed <= Duration::from_secs(5),
            "{elapsed:?}"
        );
    }

    #[tokio::test]
    async fn wait_all_skips_containers_without_a_probe() {
        let cs = [
            pb::Container {
                name: "a".into(),
                ..Default::default()
            },
            pb::Container {
                name: "b".into(),
                ..Default::default()
            },
        ];
        tokio::time::timeout(
            Duration::from_millis(100),
            wait_all(&cs, "127.0.0.1", None, &CancellationToken::new()),
        )
        .await
        .unwrap()
        .unwrap();
    }
}
