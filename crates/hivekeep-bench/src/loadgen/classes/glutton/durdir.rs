//! DurdirUser: checks that a file written into an Actor's durable directory
//! survives suspend and resume byte for byte. Each cycle hibernates the
//! Actor, resumes it, reads the file back twice (after resume, then warm)
//! verifying its digest, and overwrites it with fresh bytes.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use hivekeep_proto::api::v1 as api;
use parking_lot::Mutex;
use prost::Message;
use sha2::{Digest, Sha256};

use super::calls::{self, post, traced_call, traced_request, ClassRecorder, Recorder};
use super::knobs::{DurDirKnobs, READ_MODE_DIGEST};
use super::{DURDIR_USER_CLASS, TEMPLATE_HIVE};
use crate::glutton::{READ_DISK_ROUTE, WRITE_DISK_ROUTE};
use crate::loadgen::dynconfig::{LIFECYCLE_MODE_SYNC, RESUME_MODE_IMPLICIT};
use crate::loadgen::metrics;
use crate::loadgen::telemetry::{
    self, elapsed_from_headers, log_sampled_trace, SOURCE_CLIENT, SOURCE_SERVER,
};
use crate::loadgen::userclass::{Config, UserTask, VuId};
use crate::pb::glutton::v1 as pb;
use crate::wire::SERVER_ELAPSED_HEADER;

/// The ActorTemplate used when `durdir_template` is unset.
pub const DEFAULT_DUR_TEMPLATE: &str = "glutton-durdir-data";
/// The file each user writes and re-reads.
pub const DURDIR_TEST_FILE: &str = "bench-data";
/// The file size used when `durdir_file_size_bytes` is unset: 8 MiB.
pub const DEFAULT_FILE_SIZE: i64 = 8 << 20;

/// One DurdirUser and its Actor.
pub struct DurDirUser {
    pub(crate) cfg: Arc<Config>,
    pub(crate) actor_name: String,
    pub(crate) template_name: String,
    pub(crate) user_class: String,
    pub(crate) expected_digest: String,
    pub(crate) expected_size: i64,
}

impl DurDirUser {
    fn rec(&self) -> ClassRecorder {
        ClassRecorder(self.user_class.clone())
    }

    async fn ensure_hive(&self) -> Result<(), tonic::Status> {
        calls::ensure_hive(&self.cfg, &self.rec()).await
    }

    async fn create(&self) -> Result<(), tonic::Status> {
        let cfg = &self.cfg;
        let req = calls::create_request(cfg, &self.actor_name, TEMPLATE_HIVE, &self.template_name);
        traced_call(cfg, &self.rec(), "CreateActor", |span| {
            cfg.api.create_actor(traced_request(req, &span))
        })
        .await
        .map(|_| ())
    }

    /// Resumes the Actor unless the mode is implicit, where router traffic
    /// wakes it. Whether the Actor is (or will be) up.
    async fn resume(&self, mode: &str) -> bool {
        if mode == RESUME_MODE_IMPLICIT {
            return true;
        }
        let cfg = &self.cfg;
        let req = api::ResumeActorRequest {
            actor: Some(calls::actor_ref(cfg, &self.actor_name)),
        };
        traced_call(cfg, &self.rec(), "ResumeActor", |span| {
            cfg.api.resume_actor(traced_request(req, &span))
        })
        .await
        .is_ok()
    }

    async fn suspend_booked(&self, name: &str, mode: api::DurabilityMode) {
        let cfg = &self.cfg;
        let req = calls::suspend_request(cfg, &self.actor_name, mode);
        let _ = traced_call(cfg, &self.rec(), name, |span| {
            cfg.api.suspend_actor(traced_request(req, &span))
        })
        .await;
    }

    async fn hibernate(&self, k: &DurDirKnobs) {
        if k.lifecycle.lifecycle_mode == LIFECYCLE_MODE_SYNC {
            self.suspend_booked("SuspendActor", api::DurabilityMode::Synchronous)
                .await
        } else {
            self.suspend_booked("SuspendActorAsync", api::DurabilityMode::Asynchronous)
                .await
        }
    }

    /// Hibernates, then deletes. The hibernate is a teardown precondition
    /// and goes unbooked; the delete is booked so real leaks still show.
    pub async fn hibernate_and_delete(&self, k: &DurDirKnobs) {
        let mode = if k.lifecycle.lifecycle_mode == LIFECYCLE_MODE_SYNC {
            api::DurabilityMode::Synchronous
        } else {
            api::DurabilityMode::Asynchronous
        };
        let req = calls::suspend_request(&self.cfg, &self.actor_name, mode);
        let _ = self.cfg.api.suspend_actor(tonic::Request::new(req)).await;
        self.delete().await;
    }

    async fn delete(&self) {
        let cfg = &self.cfg;
        let req = api::DeleteActorRequest {
            actor: Some(calls::actor_ref(cfg, &self.actor_name)),
            ..Default::default()
        };
        let _ = traced_call(cfg, &self.rec(), "DeleteActor", |span| {
            cfg.api.delete_actor(traced_request(req, &span))
        })
        .await;
    }

    fn params(k: &DurDirKnobs) -> (i64, pb::ReadMode) {
        let size = if k.file_size <= 0 {
            DEFAULT_FILE_SIZE
        } else {
            k.file_size
        };
        let mode = if k.read_mode == READ_MODE_DIGEST {
            pb::ReadMode::DigestOnly
        } else {
            pb::ReadMode::Data
        };
        (size, mode)
    }

    /// One cycle: hibernate, resume, read back twice, overwrite.
    pub async fn step(&mut self, k: &DurDirKnobs) {
        let (size, mode) = Self::params(k);
        self.hibernate(k).await;
        if !self.resume(&k.lifecycle.resume_mode).await {
            return;
        }
        // Durability: the restored bytes must be the ones written.
        if self
            .read_disk("DurDirServeAfterResume", mode)
            .await
            .is_err()
        {
            return;
        }
        // An immediate second read: the page-cache warming delta.
        if self.read_disk("DurDirServeWarm", mode).await.is_err() {
            return;
        }
        let _ = self
            .write_disk("DurDirOverwrite", size, pb::WriteMode::Truncate)
            .await;
    }

    /// First resume, write and verifying read.
    pub async fn bootstrap(&mut self, k: &DurDirKnobs) -> Result<(), String> {
        let (size, mode) = Self::params(k);
        if !self.resume(&k.lifecycle.resume_mode).await {
            return Err("initial resume failed".into());
        }
        self.write_disk("DurDirWrite", size, pb::WriteMode::Truncate)
            .await
            .map_err(|e| format!("initial WriteDisk failed: {e}"))?;
        self.read_disk("DurDirServeInitial", mode)
            .await
            .map_err(|e| format!("initial ReadDisk failed: {e}"))
    }

    /// Writes `size` fresh bytes and remembers the digest the Actor reports.
    pub async fn write_disk(
        &mut self,
        metric: &str,
        size: i64,
        mode: pb::WriteMode,
    ) -> Result<(), String> {
        let req = pb::WriteDiskRequest {
            key: DURDIR_TEST_FILE.into(),
            size: size as i32,
            write_mode: mode as i32,
        };
        let mut seen = (String::new(), 0i64);
        self.http_proto_call(metric, WRITE_DISK_ROUTE, req.encode_to_vec(), |body| {
            let resp = pb::WriteDiskResponse::decode(body)
                .map_err(|e| format!("unmarshal WriteDiskResponse: {e}"))?;
            if resp.size != size {
                return Err(format!(
                    "WriteDisk size mismatch: got {}, want {size}",
                    resp.size
                ));
            }
            if resp.sha256.is_empty() {
                return Err("WriteDisk sha256 is empty".into());
            }
            seen = (hex::encode(&resp.sha256), resp.size);
            Ok(())
        })
        .await?;
        self.expected_digest = seen.0;
        self.expected_size = seen.1;
        Ok(())
    }

    /// Reads the file back and checks size and digest; in data mode the
    /// payload's own digest too.
    pub async fn read_disk(&self, metric: &str, mode: pb::ReadMode) -> Result<(), String> {
        let req = pb::ReadDiskRequest {
            key: DURDIR_TEST_FILE.into(),
            read_mode: mode as i32,
        };
        let (want_digest, want_size) = (self.expected_digest.clone(), self.expected_size);
        self.http_proto_call(metric, READ_DISK_ROUTE, req.encode_to_vec(), |body| {
            let resp = pb::ReadDiskResponse::decode(body)
                .map_err(|e| format!("unmarshal ReadDiskResponse: {e}"))?;
            if resp.size != want_size {
                return Err(format!(
                    "ReadDisk size mismatch: got {}, want {want_size}",
                    resp.size
                ));
            }
            let got = hex::encode(&resp.sha256);
            if got != want_digest {
                return Err(format!(
                    "ReadDisk response sha256 mismatch: got {got:?}, want {want_digest:?}"
                ));
            }
            if mode == pb::ReadMode::DigestOnly {
                return Ok(());
            }
            let computed = hex::encode(Sha256::digest(&resp.data));
            if computed != want_digest {
                return Err(format!(
                    "ReadDisk payload sha256 mismatch: computed {computed:?}, want {want_digest:?}"
                ));
            }
            Ok(())
        })
        .await
    }

    /// POSTs `body` to `route`, validates the answer and books it under
    /// `metric` with the client's latency (the system under test is the
    /// platform, not the glutton).
    async fn http_proto_call(
        &self,
        metric: &str,
        route: &str,
        body: Vec<u8>,
        validate: impl FnOnce(&[u8]) -> Result<(), String>,
    ) -> Result<(), String> {
        let rec = self.rec();
        let mut span = self.cfg.tracer.start(metric);
        let ctx = *span.context();
        let start = Instant::now();
        let reply = post(&self.cfg, &self.actor_name, route, body, &ctx).await;
        let latency = start.elapsed();
        let res = (|| {
            let reply = reply?;
            let (server, source) =
                elapsed_from_headers(&reply.headers, SERVER_ELAPSED_HEADER, latency);
            if source == SOURCE_SERVER {
                span.set_attribute("server.elapsed_ms", telemetry::ms_float(server));
            }
            if reply.status.as_u16() >= 400 {
                return Err(format!(
                    "HTTP {}: {}",
                    reply.status.as_u16(),
                    String::from_utf8_lossy(&reply.body).trim()
                ));
            }
            validate(&reply.body)?;
            Ok(reply.body.len() as i64)
        })();
        log_sampled_trace(
            &ctx,
            metric,
            latency,
            SOURCE_CLIENT,
            res.as_ref().err().map(String::as_str),
        );
        span.end();
        match res {
            Ok(n) => {
                rec.success("http", metric, latency, n);
                Ok(())
            }
            Err(e) => {
                rec.failure("http", metric, latency, &e);
                Err(e)
            }
        }
    }
}

/// The DurdirUser task.
pub struct DurDirRuntime {
    pub(crate) cfg: Arc<Config>,
    pub(crate) users: Mutex<HashMap<VuId, Arc<tokio::sync::Mutex<DurDirUser>>>>,
}

impl DurDirRuntime {
    /// A runtime over `cfg`.
    pub fn new(cfg: Arc<Config>) -> Self {
        DurDirRuntime {
            cfg,
            users: Mutex::new(HashMap::new()),
        }
    }

    fn knobs(&self) -> DurDirKnobs {
        self.cfg.dyn_config.get::<DurDirKnobs>()
    }

    fn dynamic_wait(&self) -> Duration {
        self.knobs().wait.draw()
    }

    /// Creates the user's Actor and writes its first file. A failed
    /// bootstrap hibernates and deletes the Actor.
    pub async fn start_user(&self, k: &DurDirKnobs) -> Result<DurDirUser, String> {
        let template = if k.template.is_empty() {
            DEFAULT_DUR_TEMPLATE.to_string()
        } else {
            k.template.clone()
        };
        let mut u = DurDirUser {
            cfg: self.cfg.clone(),
            actor_name: format!("sb-{}", uuid::Uuid::new_v4()),
            template_name: template,
            user_class: DURDIR_USER_CLASS.into(),
            expected_digest: String::new(),
            expected_size: 0,
        };
        metrics::update_users(DURDIR_USER_CLASS, 1.0);
        if let Err(e) = u.ensure_hive().await {
            metrics::update_users(DURDIR_USER_CLASS, -1.0);
            return Err(e.message().to_string());
        }
        if let Err(e) = u.create().await {
            metrics::update_users(DURDIR_USER_CLASS, -1.0);
            return Err(e.message().to_string());
        }
        if let Err(e) = u.bootstrap(k).await {
            u.hibernate_and_delete(k).await;
            metrics::update_users(DURDIR_USER_CLASS, -1.0);
            return Err(e);
        }
        Ok(u)
    }

    /// One cycle for user `vu`, starting the user on its first call.
    pub async fn iterate(&self, vu: VuId) {
        let existing = self.users.lock().get(&vu).cloned();
        let user = match existing {
            Some(u) => u,
            None => match self.start_user(&self.knobs()).await {
                Ok(u) => self
                    .users
                    .lock()
                    .entry(vu)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(u)))
                    .clone(),
                Err(e) => {
                    tracing::warn!(err = %e, "durdir on_start failed; the user will retry next iteration");
                    tokio::time::sleep(self.dynamic_wait()).await;
                    return;
                }
            },
        };
        let k = self.knobs();
        user.lock().await.step(&k).await;
        tokio::time::sleep(self.dynamic_wait()).await;
    }

    /// Hibernates and deletes every user's Actor.
    pub async fn shutdown(&self) {
        let k = self.knobs();
        let users: Vec<_> = self.users.lock().values().cloned().collect();
        for u in users {
            u.lock().await.hibernate_and_delete(&k).await;
            metrics::update_users(DURDIR_USER_CLASS, -1.0);
        }
    }
}

#[async_trait]
impl UserTask for DurDirRuntime {
    async fn iterate(&self, vu: VuId) {
        DurDirRuntime::iterate(self, vu).await
    }
    async fn shutdown(&self) {
        DurDirRuntime::shutdown(self).await
    }
}

#[cfg(test)]
#[path = "durdir_tests.rs"]
mod tests;
