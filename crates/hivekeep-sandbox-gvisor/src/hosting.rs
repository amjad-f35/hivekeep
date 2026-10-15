//! Admitting, hosting and unhosting Actors.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use hivekeep_core::resource::ActorAttribution;
use hivekeep_proto::worker::v1::ActorDirs;

use crate::error::{status, Context, Result};
use crate::hosted::{HostedActor, WorkloadSession};
use crate::ports::{Dialer, SandboxNetwork};
use crate::sandbox_process::SandboxProcess;
use crate::service::WorkerService;

/// An admitted activation, and the network of the one it replaced.
pub(crate) type Admitted = (Arc<HostedActor>, Option<Arc<dyn SandboxNetwork>>);

impl WorkerService {
    /// Reserves capacity for an Actor before its network is set up. An Actor
    /// already hosted keeps its slot (a retry cannot lose its place); its old
    /// network is returned for the caller to close.
    pub(crate) fn admit_actor(
        &self,
        attribution: ActorAttribution,
    ) -> Result<Admitted, tonic::Status> {
        let mut t = self.table.write();
        let mut stale = None;
        if let Some(old) = t.actors.get(&attribution.uid) {
            stale = old.network();
            old.token.cancel();
            // The old activation's sandbox exiting from here on is no crash.
            old.stopping.store(true, Ordering::SeqCst);
            old.set_sandbox(None);
        } else if t.actors.len() + t.draining >= self.cfg.max_actors {
            return Err(status::resource_exhausted(format!(
                "worker is full: {} actors",
                self.cfg.max_actors
            )));
        }
        let hosted = Arc::new(HostedActor::new(attribution));
        t.actors
            .insert(hosted.attribution.uid.clone(), hosted.clone());
        Ok((hosted, stale))
    }

    /// Admits the Actor and sets up its network, replacing a stale one.
    pub(crate) async fn host_actor(
        &self,
        attribution: ActorAttribution,
    ) -> Result<Arc<HostedActor>> {
        let (hosted, stale) = self.admit_actor(attribution)?;
        let uid = hosted.attribution.uid.clone();
        let evict = || {
            hosted.token.cancel();
            let mut t = self.table.write();
            if t.actors.get(&uid).is_some_and(|h| Arc::ptr_eq(h, &hosted)) {
                t.actors.remove(&uid);
            }
        };
        if let Some(stale) = stale {
            if let Err(e) = stale.close().await {
                evict();
                return Err(e)
                    .with_context(|| format!("while clearing stale state for actor {uid}"));
            }
        }
        match self.cfg.tunnel.serve_sandbox(&hosted.attribution).await {
            Ok(n) => {
                hosted.set_network(Some(n));
                Ok(hosted)
            }
            Err(e) => {
                evict();
                Err(e).context("while setting up the sandbox network")
            }
        }
    }

    /// Removes the Actor and its network. Repeated calls are fine. The Actor
    /// keeps its capacity until its network is gone.
    pub(crate) async fn unhost_actor(&self, uid: &str) -> Result<()> {
        let hosted = {
            let mut t = self.table.write();
            let h = t.actors.remove(uid);
            if h.is_some() {
                t.draining += 1;
            }
            h
        };
        let Some(hosted) = hosted else { return Ok(()) };
        hosted.token.cancel();
        hosted.set_sandbox(None);
        let result = match hosted.network() {
            Some(n) => n.close().await.inspect_err(|e| {
                tracing::warn!(err = %e, "Failed to remove the sandbox network");
            }),
            None => Ok(()),
        };
        self.table.write().draining -= 1;
        result
    }

    /// Pins the Actor's sandbox process, which runsc has just created.
    pub(crate) fn pin_sandbox(&self, hosted: &HostedActor, actor_dirs: &ActorDirs) {
        match SandboxProcess::open(actor_dirs) {
            Ok(p) => hosted.set_sandbox(Some(Arc::new(p))),
            Err(e) => {
                tracing::warn!(actor_uid = %hosted.attribution.uid, err = %e, "Cannot pin the sandbox process")
            }
        }
    }

    /// The hosted Actor, without taking its lifecycle lock.
    pub fn lookup_actor(&self, uid: &str) -> Option<Arc<HostedActor>> {
        self.table.read().actors.get(uid).cloned()
    }

    /// Whether `h` is still the hosted activation of its Actor.
    pub(crate) fn is_current(&self, h: &Arc<HostedActor>) -> bool {
        self.lookup_actor(&h.attribution.uid)
            .is_some_and(|c| Arc::ptr_eq(&c, h))
    }

    /// A snapshot of the hosted Actors.
    pub fn hosted_actors(&self) -> Vec<Arc<HostedActor>> {
        self.table.read().actors.values().cloned().collect()
    }

    /// The runsc sessions of every hosted Actor whose containers exist.
    pub(crate) fn hosted_sessions(&self) -> Vec<WorkloadSession> {
        self.table
            .read()
            .actors
            .values()
            .filter_map(|h| h.session())
            .collect()
    }

    /// Records the runsc session of a hosted Actor.
    pub(crate) fn set_session(&self, uid: &str, session: WorkloadSession) {
        if let Some(h) = self.table.read().actors.get(uid) {
            h.set_session(session);
        }
    }

    /// Dials into the Actor's sandbox, looking the Actor up on every dial so
    /// a replaced activation is never reached through a stale network.
    pub(crate) fn sandbox_dialer(self: &Arc<Self>, uid: &str) -> Dialer {
        let me = Arc::downgrade(self);
        let uid = uid.to_string();
        Arc::new(move |addr| {
            let network = me
                .upgrade()
                .and_then(|s| s.lookup_actor(&uid))
                .and_then(|h| h.network());
            let uid = uid.clone();
            Box::pin(async move {
                match network {
                    Some(n) => n.dialer()(addr).await,
                    None => Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("actor {uid} is not hosted here"),
                    )),
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attribution::uid_only;
    use crate::service::ServiceConfig;

    fn service(max_actors: usize) -> Arc<WorkerService> {
        WorkerService::new(ServiceConfig {
            max_actors,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn admission_enforces_the_ceiling_concurrently() {
        let s = service(8);
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let s = s.clone();
                tokio::spawn(async move {
                    let _ = s.admit_actor(uid_only(&format!("actor-{i}")));
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(s.hosted_actors().len(), 8);
    }

    #[test]
    fn readmission_keeps_the_slot() {
        let s = service(1);
        let (first, _) = s.admit_actor(uid_only("actor-a")).unwrap();
        let (again, _) = s.admit_actor(uid_only("actor-a")).unwrap();
        assert!(
            !Arc::ptr_eq(&first, &again),
            "readers could not tell the activations apart"
        );
        assert!(first.token.is_cancelled() && first.stopping.load(Ordering::SeqCst));
        assert_eq!(
            s.admit_actor(uid_only("actor-b")).err().unwrap().code(),
            tonic::Code::ResourceExhausted
        );
        assert_eq!(s.hosted_actors().len(), 1);
    }

    #[tokio::test]
    async fn draining_actors_hold_capacity() {
        let s = service(1);
        s.table.write().draining = 1;
        assert!(s.host_actor(uid_only("actor-a")).await.is_err());
    }

    #[tokio::test]
    async fn unhosting_is_idempotent() {
        let s = service(2);
        let h = s.host_actor(uid_only("actor-a")).await.unwrap();
        assert!(h.network().is_some());
        s.unhost_actor("actor-a").await.unwrap();
        assert!(h.token.is_cancelled());
        assert!(s.lookup_actor("actor-a").is_none());
        s.unhost_actor("actor-a").await.unwrap();
        assert_eq!(s.table.read().draining, 0);
        let dial = s.sandbox_dialer("actor-a");
        assert_eq!(
            dial("127.0.0.1:1".into()).await.unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }
}
