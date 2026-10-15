//! Waiting for an Actor's containers to answer their wakeup probes.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use hivekeep_proto::worker::v1 as pb;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::ports::Dialer;

/// The address of the Actor inside its sandbox network.
pub const ACTOR_VETH_IP: &str = "169.254.17.2";
/// The sandbox network's gateway, as seen from the Actor.
pub const ACTOR_VETH_GATEWAY: &str = "169.254.17.1";

/// How long one probe request may take.
pub const REQUEST_TIMEOUT: Duration = Duration::from_millis(250);
/// The pause between probe attempts.
pub const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Blocks until every container with a wakeup probe answers it.
#[async_trait]
pub trait WakeupProber: Send + Sync {
    /// Waits for all probes, failing on the first that times out.
    async fn wait_all(
        &self,
        containers: &[pb::Container],
        dialer: Dialer,
        cancel: &CancellationToken,
    ) -> Result<()>;
}

/// Probes over HTTP/1.1 through the sandbox dialer.
#[derive(Debug, Clone)]
pub struct HttpProber {
    /// The Actor's address.
    pub actor_ip: String,
}

impl Default for HttpProber {
    fn default() -> Self {
        HttpProber {
            actor_ip: ACTOR_VETH_IP.into(),
        }
    }
}

#[async_trait]
impl WakeupProber for HttpProber {
    async fn wait_all(
        &self,
        containers: &[pb::Container],
        dialer: Dialer,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let waits = containers.iter().filter_map(|c| {
            c.wakeup_probe
                .as_ref()
                .map(|p| wait(&c.name, p, &self.actor_ip, dialer.clone(), cancel))
        });
        futures::future::try_join_all(waits).await.map(drop)
    }
}

/// Checks a probe's shape: an HTTP GET to a port and absolute path, with a
/// positive timeout.
pub fn target(probe: &pb::WakeupProbe, actor_ip: &str) -> Result<(String, String, Duration)> {
    let hg = probe
        .http_get
        .as_ref()
        .ok_or_else(|| Error::msg("httpGet is required"))?;
    if !(1..=65535).contains(&hg.port) {
        return Err(Error::msg(format!("invalid port {}", hg.port)));
    }
    if !hg.path.starts_with('/') {
        return Err(Error::msg(format!(
            "path must start with '/', got {:?}",
            hg.path
        )));
    }
    if probe.timeout_seconds <= 0 {
        return Err(Error::msg(format!(
            "timeout_seconds must be positive, got {}",
            probe.timeout_seconds
        )));
    }
    Ok((
        format!("{actor_ip}:{}", hg.port),
        hg.path.clone(),
        Duration::from_secs(probe.timeout_seconds as u64),
    ))
}

async fn wait(
    name: &str,
    probe: &pb::WakeupProbe,
    actor_ip: &str,
    dialer: Dialer,
    cancel: &CancellationToken,
) -> Result<()> {
    let (addr, path, timeout) = target(probe, actor_ip)
        .map_err(|e| Error::msg(format!("invalid wakeup probe config for {name:?}: {e}")))?;
    let start = Instant::now();
    let mut attempts = 0u32;
    let mut last_err = String::new();
    loop {
        if cancel.is_cancelled() {
            return Err(Error::Context {
                context: format!(
                    "wakeup probe cancelled for {name:?} after {:?} ({attempts} attempts, last error: {last_err})",
                    start.elapsed()
                ),
                source: Box::new(Error::Cancelled),
            });
        }
        if start.elapsed() > timeout {
            return Err(Error::msg(format!(
                "wakeup probe for {name:?} never returned 200 within {timeout:?} ({attempts} attempts, last error: {last_err})"
            )));
        }
        attempts += 1;
        match tokio::time::timeout(REQUEST_TIMEOUT, try_once(&dialer, &addr, &path)).await {
            Ok(Ok(200)) => {
                tracing::info!(container = name, addr, path, elapsed = ?start.elapsed(), attempts, "Wakeup probe reached 200");
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

/// One GET; the response's status code.
async fn try_once(dialer: &Dialer, addr: &str, path: &str) -> std::io::Result<u16> {
    let mut conn = dialer(addr.to_string()).await?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nUser-Agent: hivekeep-wakeup-probe\r\n\r\n");
    conn.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(64);
    let mut chunk = [0u8; 64];
    while !buf.windows(2).any(|w| w == b"\r\n") {
        let n = conn.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let line = String::from_utf8_lossy(&buf);
    let code = line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "malformed HTTP status line",
            )
        })?;
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::direct_dialer;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn container(port: u16, timeout: i32) -> pb::Container {
        pb::Container {
            name: "app".into(),
            wakeup_probe: Some(pb::WakeupProbe {
                http_get: Some(pb::HttpGetAction {
                    path: "/healthz".into(),
                    port: port.into(),
                }),
                timeout_seconds: timeout,
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn waits_for_200() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                let n = h.fetch_add(1, Ordering::SeqCst);
                let mut buf = [0u8; 512];
                let _ = s.read(&mut buf).await;
                let status = if n < 2 {
                    "503 Service Unavailable"
                } else {
                    "200 OK"
                };
                let _ = s
                    .write_all(format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\n\r\n").as_bytes())
                    .await;
            }
        });
        let p = HttpProber {
            actor_ip: "127.0.0.1".into(),
        };
        p.wait_all(
            &[container(port, 5), pb::Container::default()],
            direct_dialer(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(hits.load(Ordering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn gives_up_at_the_timeout_or_cancellation() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        let p = HttpProber {
            actor_ip: "127.0.0.1".into(),
        };
        let err = p
            .wait_all(
                &[container(port, 1)],
                direct_dialer(),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("never returned 200"), "{err}");
        let cancel = CancellationToken::new();
        cancel.cancel();
        let err = p
            .wait_all(&[container(port, 30)], direct_dialer(), &cancel)
            .await
            .unwrap_err();
        assert!(err.is_context_error(), "{err}");
    }

    #[test]
    fn rejects_bad_probes() {
        let mut c = container(0, 1);
        assert!(target(c.wakeup_probe.as_ref().unwrap(), "x").is_err());
        c = container(80, 0);
        assert!(target(c.wakeup_probe.as_ref().unwrap(), "x").is_err());
        assert_eq!(
            target(container(80, 2).wakeup_probe.as_ref().unwrap(), "1.2.3.4")
                .unwrap()
                .0,
            "1.2.3.4:80"
        );
    }
}
