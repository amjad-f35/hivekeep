//! Relaying a CONNECT tunnel between the router and the Actor.

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use crate::tunnel::egress::stream_copy::stream_copy;

/// How a relay ended.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelayOutcome {
    /// Bytes the router sent to the Actor.
    pub to_actor: u64,
    /// Bytes the Actor sent back.
    pub from_actor: u64,
    /// Whether the relay was cut short by cancellation.
    pub canceled: bool,
}

/// Relays until the Actor stops sending (or `cancel` fires). The router's
/// end of stream is passed to the Actor as a half-close; when the Actor
/// finishes, the tunnel is closed even if the router is still sending.
pub async fn relay_with_half_close<U, D>(
    cancel: &CancellationToken,
    upstream: U,
    downstream: D,
) -> RelayOutcome
where
    U: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    let (mut ur, mut uw) = tokio::io::split(upstream);
    let (mut dr, mut dw) = tokio::io::split(downstream);
    let mut out = RelayOutcome::default();
    let to_actor = async {
        let n = match stream_copy(&mut dr, &mut uw).await {
            Ok(s) => s.written,
            Err(e) => e.stats().written,
        };
        tracing::debug!(bytes = n, "tunnel CONNECT router stopped sending");
        let _ = uw.shutdown().await;
        n
    };
    let from_actor = async {
        let n = match stream_copy(&mut ur, &mut dw).await {
            Ok(s) => s.written,
            Err(e) => e.stats().written,
        };
        tracing::debug!(bytes = n, "tunnel CONNECT Actor stopped sending");
        let _ = dw.shutdown().await;
        n
    };
    tokio::pin!(to_actor, from_actor);
    let mut to_done = None;
    loop {
        tokio::select! {
            n = &mut to_actor, if to_done.is_none() => to_done = Some(n),
            n = &mut from_actor => {
                out.from_actor = n;
                break;
            }
            _ = cancel.cancelled() => {
                tracing::debug!("tunnel CONNECT canceled: Actor deactivated");
                out.canceled = true;
                break;
            }
        }
    }
    out.to_actor = to_done.unwrap_or_default();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{duplex, AsyncReadExt};

    #[tokio::test]
    async fn relay_with_half_close_passes_both_directions() {
        let (upstream, mut actor) = duplex(1024);
        let (downstream, mut router) = duplex(1024);
        let cancel = CancellationToken::new();
        let relay =
            tokio::spawn(async move { relay_with_half_close(&cancel, upstream, downstream).await });
        router.write_all(b"request").await.unwrap();
        router.shutdown().await.unwrap();
        let mut req = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), actor.read_to_end(&mut req))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(req, b"request");
        actor.write_all(b"response").await.unwrap();
        drop(actor);
        let out = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .expect("relay did not finish")
            .unwrap();
        let mut resp = Vec::new();
        router.read_to_end(&mut resp).await.unwrap();
        assert_eq!(resp, b"response");
        assert_eq!(
            out,
            RelayOutcome {
                to_actor: 7,
                from_actor: 8,
                canceled: false
            }
        );
    }

    #[tokio::test]
    async fn closes_downstream_when_actor_closes() {
        let (upstream, mut actor) = duplex(1024);
        let (downstream, mut router) = duplex(1024);
        let cancel = CancellationToken::new();
        let relay =
            tokio::spawn(async move { relay_with_half_close(&cancel, upstream, downstream).await });
        actor.write_all(b"response").await.unwrap();
        drop(actor);
        let out = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .expect("relay kept the tunnel open after the Actor closed")
            .unwrap();
        assert_eq!(out.from_actor, 8);
        let mut resp = Vec::new();
        router.read_to_end(&mut resp).await.unwrap();
        assert_eq!(resp, b"response");
        // The relay is gone, so the router's side is closed.
        let mut sink = Vec::new();
        let _ = router.read_to_end(&mut sink).await;
        assert!(
            router.write_all(b"next request").await.is_err(),
            "downstream remained open"
        );
    }

    #[tokio::test]
    async fn cancellation_closes_both_sides() {
        let (upstream, mut actor) = duplex(1024);
        let (downstream, mut router) = duplex(1024);
        let cancel = CancellationToken::new();
        let c2 = cancel.clone();
        let relay =
            tokio::spawn(async move { relay_with_half_close(&c2, upstream, downstream).await });
        cancel.cancel();
        let out = tokio::time::timeout(Duration::from_secs(1), relay)
            .await
            .expect("relay did not return")
            .unwrap();
        assert!(out.canceled);
        let mut b = [0u8; 1];
        assert_eq!(
            actor.read(&mut b).await.unwrap_or(0),
            0,
            "Actor side stayed open"
        );
        assert!(
            router.write_all(b"request").await.is_err(),
            "downstream remained open"
        );
    }
}
