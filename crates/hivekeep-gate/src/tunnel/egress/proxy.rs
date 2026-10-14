//! Accepts an Actor's redirected connections and tunnels each to its
//! original destination, while the Actor's egress is active and its
//! certificate valid.
//!
//! A *binding* attaches a listener to an Actor; an *activation* supplies
//! the tunnel dialer and certificate. Deactivating ends the activation:
//! its listener closes, its tunnels are torn down, and a later activation
//! for the same Actor never inherits anything from it.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::stream_copy::stream_copy;
use super::{ActorCertificateSource, BoxIo, Io, TunnelDialer};

/// Resolves where an Actor connection was originally headed.
pub type OriginalDestination = Arc<dyn Fn(&dyn Io) -> io::Result<String> + Send + Sync>;

/// How long a tunnel may take to open.
pub const TUNNEL_OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// A source of Actor connections. `Ok(None)` means the listener closed.
pub trait Accept: Send + 'static {
    /// The next connection.
    fn accept(&mut self) -> BoxFuture<'_, io::Result<Option<BoxIo>>>;
}

impl Accept for TcpListener {
    fn accept(&mut self) -> BoxFuture<'_, io::Result<Option<BoxIo>>> {
        Box::pin(async move {
            let (conn, _) = TcpListener::accept(self).await?;
            Ok(Some(Box::new(conn) as BoxIo))
        })
    }
}

#[derive(Default)]
struct ActState {
    dialer: Option<Arc<dyn TunnelDialer>>,
    source: Option<Arc<dyn ActorCertificateSource>>,
    expires_at: Option<SystemTime>,
}

struct Activation {
    actor_uid: String,
    cancel: CancellationToken,
    tracker: TaskTracker,
    state: Mutex<ActState>,
}

struct Inner {
    original_destination: OriginalDestination,
    tunnel_open_timeout: Mutex<Duration>,
    active: Mutex<HashMap<String, Arc<Activation>>>,
}

/// The per-worker egress proxy, serving every Actor of the worker.
#[derive(Clone)]
pub struct Proxy {
    inner: Arc<Inner>,
}

/// A listener attachment for one Actor, served with [`Binding::serve`].
pub struct Binding {
    proxy: Proxy,
    active: Arc<Activation>,
}

/// Errors activating egress.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ActivateError {
    /// No Actor UID was given.
    #[error("egress: actor UID is required")]
    MissingUid,
    /// The certificate is already expired.
    #[error("egress: valid actor certificate is required")]
    Expired,
    /// The Actor already has active egress.
    #[error("egress: actor {0} already has active egress")]
    AlreadyActive(String),
}

fn renew_after(expires_at: SystemTime) -> Duration {
    let remaining = expires_at
        .duration_since(SystemTime::now())
        .unwrap_or_default();
    remaining - remaining / 10
}

/// How long to wait before retrying a failed renewal.
pub fn retry_after(expires_at: SystemTime) -> Duration {
    match expires_at.duration_since(SystemTime::now()) {
        Ok(remaining) if !remaining.is_zero() => (remaining / 10)
            .max(Duration::from_secs(1))
            .min(Duration::from_secs(30))
            .min(remaining),
        _ => Duration::from_secs(25) + Duration::from_millis(rand::rng().random_range(0..10_000)),
    }
}

impl Proxy {
    /// A proxy resolving original destinations with `original_destination`.
    pub fn new(original_destination: OriginalDestination) -> Proxy {
        Proxy {
            inner: Arc::new(Inner {
                original_destination,
                tunnel_open_timeout: Mutex::new(TUNNEL_OPEN_TIMEOUT),
                active: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Overrides how long a tunnel may take to open.
    pub fn set_tunnel_open_timeout(&self, d: Duration) {
        *self.inner.tunnel_open_timeout.lock() = d;
    }

    fn activation_locked(map: &mut HashMap<String, Arc<Activation>>, uid: &str) -> Arc<Activation> {
        map.entry(uid.to_string())
            .or_insert_with(|| {
                Arc::new(Activation {
                    actor_uid: uid.to_string(),
                    cancel: CancellationToken::new(),
                    tracker: TaskTracker::new(),
                    state: Mutex::new(ActState::default()),
                })
            })
            .clone()
    }

    /// Attaches a listener to `actor_uid`'s current (or next) activation.
    pub fn bind(&self, actor_uid: &str) -> Result<Binding, ActivateError> {
        if actor_uid.is_empty() {
            return Err(ActivateError::MissingUid);
        }
        let active = Self::activation_locked(&mut self.inner.active.lock(), actor_uid);
        Ok(Binding {
            proxy: self.clone(),
            active,
        })
    }

    /// Activates egress for `actor_uid` with a certificate valid until
    /// `expires_at`, renewing it from `source` until deactivated.
    pub fn activate(
        &self,
        actor_uid: &str,
        dialer: Arc<dyn TunnelDialer>,
        source: Arc<dyn ActorCertificateSource>,
        expires_at: SystemTime,
    ) -> Result<(), ActivateError> {
        if actor_uid.is_empty() {
            return Err(ActivateError::MissingUid);
        }
        if expires_at <= SystemTime::now() {
            return Err(ActivateError::Expired);
        }
        let active = {
            let mut map = self.inner.active.lock();
            let active = Self::activation_locked(&mut map, actor_uid);
            let mut st = active.state.lock();
            if st.dialer.is_some() {
                return Err(ActivateError::AlreadyActive(actor_uid.to_string()));
            }
            st.dialer = Some(dialer);
            st.source = Some(source.clone());
            st.expires_at = Some(expires_at);
            drop(st);
            active
        };
        let proxy = self.clone();
        let a = active.clone();
        active
            .tracker
            .spawn(async move { proxy.renew(a, source, expires_at).await });
        Ok(())
    }

    async fn renew(
        &self,
        active: Arc<Activation>,
        source: Arc<dyn ActorCertificateSource>,
        mut expires_at: SystemTime,
    ) {
        let mut delay = renew_after(expires_at);
        let mut expired = false;
        loop {
            if delay.is_zero() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = active.cancel.cancelled() => return,
            }
            if expires_at <= SystemTime::now() && !expired {
                tracing::warn!(actor_uid = %active.actor_uid, "tunnel Actor certificate expired; blocking new egress connections");
                expired = true;
            }
            let minted = tokio::select! {
                r = source.mint() => r,
                _ = active.cancel.cancelled() => return,
            };
            let next = match minted {
                Err(err) => {
                    if matches!(
                        err.code,
                        Some(
                            tonic::Code::Aborted
                                | tonic::Code::FailedPrecondition
                                | tonic::Code::PermissionDenied
                        )
                    ) {
                        active.state.lock().expires_at = None;
                        tracing::warn!(actor_uid = %active.actor_uid, %err, "tunnel Actor certificate renewal was denied; blocking new egress connections");
                        return;
                    }
                    delay = retry_after(expires_at);
                    continue;
                }
                Ok(next) if next <= SystemTime::now() => {
                    delay = retry_after(expires_at);
                    continue;
                }
                Ok(next) => next,
            };
            {
                let _map = self.inner.active.lock();
                if active.cancel.is_cancelled() {
                    return;
                }
                active.state.lock().expires_at = Some(next);
            }
            if expired {
                tracing::info!(actor_uid = %active.actor_uid, "tunnel Actor certificate renewed; allowing new egress connections");
                expired = false;
            }
            expires_at = next;
            delay = renew_after(expires_at);
        }
    }

    /// Ends `actor_uid`'s egress: blocks new tunnels, tears down open ones,
    /// waits for them, and closes the dialer. Wrap in a timeout to bound
    /// the wait.
    pub async fn deactivate(&self, actor_uid: &str) {
        let active = {
            let mut map = self.inner.active.lock();
            let active = map.remove(actor_uid);
            if let Some(a) = &active {
                a.state.lock().expires_at = None;
                a.cancel.cancel();
            }
            active
        };
        let Some(active) = active else {
            return;
        };
        active.tracker.close();
        active.tracker.wait().await;
        let dialer = active.state.lock().dialer.clone();
        if let Some(d) = dialer {
            d.close();
        }
    }

    /// Whether `actor_uid` has a binding or activation.
    pub fn is_armed(&self, actor_uid: &str) -> bool {
        self.inner.active.lock().contains_key(actor_uid)
    }

    /// When `actor_uid`'s current certificate expires, if it has one.
    pub fn expires_at(&self, actor_uid: &str) -> Option<SystemTime> {
        self.inner
            .active
            .lock()
            .get(actor_uid)
            .and_then(|a| a.state.lock().expires_at)
    }

    fn lookup(&self, actor_uid: &str) -> Option<Arc<Activation>> {
        self.inner.active.lock().get(actor_uid).cloned()
    }

    /// Tunnels one Actor connection for `actor_uid`, or closes it when the
    /// Actor's egress is not active or its certificate is not valid.
    pub fn handle_for(&self, downstream: BoxIo, actor_uid: &str) {
        let active = self.lookup(actor_uid);
        self.handle(downstream, active.as_ref());
    }

    fn handle(&self, downstream: BoxIo, active: Option<&Arc<Activation>>) {
        let Some(active) = active.filter(|a| !a.cancel.is_cancelled()) else {
            return;
        };
        let dialer = {
            let _map = self.inner.active.lock();
            let st = active.state.lock();
            let Some(dialer) = st.dialer.clone() else {
                tracing::debug!(actor_uid = %active.actor_uid, "tunnel dropped an egress connection: Actor egress is not active");
                return;
            };
            if st.expires_at.is_none_or(|e| SystemTime::now() >= e) {
                tracing::debug!(actor_uid = %active.actor_uid, "tunnel dropped an egress connection: Actor has no valid certificate");
                return;
            }
            dialer
        };
        let inner = self.inner.clone();
        let cancel = active.cancel.clone();
        let timeout = *inner.tunnel_open_timeout.lock();
        active.tracker.spawn(async move {
            let destination = match (inner.original_destination)(&*downstream) {
                Ok(d) => d,
                Err(err) => {
                    tracing::warn!(%err, "tunnel failed to resolve original egress destination");
                    return;
                }
            };
            let upstream = tokio::select! {
                r = tokio::time::timeout(timeout, dialer.dial(&destination)) => match r {
                    Ok(Ok(u)) => u,
                    Ok(Err(err)) => {
                        tracing::warn!(%destination, %err, "tunnel failed to open egress tunnel");
                        return;
                    }
                    Err(_) => {
                        tracing::warn!(%destination, "tunnel failed to open egress tunnel: timed out");
                        return;
                    }
                },
                _ = cancel.cancelled() => return,
            };
            tokio::select! {
                _ = copy_both_ways(downstream, upstream) => {}
                _ = cancel.cancelled() => {}
            }
        });
    }
}

/// Copies both directions until both ends finish, passing each end of
/// stream on as a half-close.
pub async fn copy_both_ways(a: BoxIo, b: BoxIo) {
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    tokio::join!(
        async {
            let _ = stream_copy(&mut br, &mut aw).await;
            let _ = aw.shutdown().await;
        },
        async {
            let _ = stream_copy(&mut ar, &mut bw).await;
            let _ = bw.shutdown().await;
        },
    );
}

impl Binding {
    /// Accepts Actor connections until `cancel` fires, the activation ends
    /// or the listener closes. A binding never activated is dropped when
    /// its listener closes.
    pub async fn serve(
        self,
        mut listener: impl Accept,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        let result = loop {
            let accepted = tokio::select! {
                r = listener.accept() => r,
                _ = cancel.cancelled() => break Ok(()),
                _ = self.active.cancel.cancelled() => break Ok(()),
            };
            match accepted {
                Ok(Some(conn)) => self.proxy.handle(conn, Some(&self.active)),
                Ok(None) => break Ok(()),
                Err(e) => {
                    break Err(io::Error::new(
                        e.kind(),
                        format!("egress: accepting Actor egress connection: {e}"),
                    ))
                }
            }
        };
        drop(listener);
        let mut map = self.proxy.inner.active.lock();
        if map
            .get(&self.active.actor_uid)
            .is_some_and(|a| Arc::ptr_eq(a, &self.active))
            && self.active.state.lock().dialer.is_none()
        {
            map.remove(&self.active.actor_uid);
            self.active.cancel.cancel();
        }
        result
    }
}

#[cfg(test)]
#[path = "proxy_tests.rs"]
mod tests;
