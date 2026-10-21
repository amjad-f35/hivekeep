//! Talking to the installed control plane: creating the hives and actor
//! templates the demos need, waiting for golden snapshots, and cleaning a
//! demo's actors up again.

use super::{Env, Result, StepError};
use crate::log;
use async_trait::async_trait;
use hivekeep_core::resource::{ActorRef, ActorTemplateRef};
use hivekeep_proto::api::v1 as pb;
use prost_reflect::{DescriptorPool, DeserializeOptions, DynamicMessage};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use tonic::{Code, Status};

/// Environment variable naming the hivekeep-api gRPC endpoint the installer
/// dials (for example a `kubectl port-forward` to the `api` Service).
pub const API_ENDPOINT_ENV: &str = "HK_API_ENDPOINT";
/// Environment variable holding a bearer token for hivekeep-api.
pub const API_TOKEN_ENV: &str = "HK_API_TOKEN";

/// Interval between golden-snapshot polls.
const GOLDEN_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The control plane calls the installer makes.
#[async_trait]
pub trait ControlPlane: Send + Sync {
    async fn create_hive(&self, hive: &str) -> std::result::Result<(), Status>;
    async fn delete_hive(&self, hive: &str) -> std::result::Result<(), Status>;
    async fn create_actor_template(
        &self,
        template: pb::ActorTemplate,
    ) -> std::result::Result<(), Status>;
    async fn get_actor_template(
        &self,
        r: pb::ObjectRef,
    ) -> std::result::Result<pb::ActorTemplate, Status>;
    async fn delete_actor_template(&self, r: pb::ObjectRef) -> std::result::Result<(), Status>;
    async fn list_actors(
        &self,
        page_token: &str,
    ) -> std::result::Result<pb::ListActorsResponse, Status>;
    async fn delete_actor(&self, r: pb::ObjectRef) -> std::result::Result<(), Status>;

    /// Pages through every actor in every hive.
    async fn list_all_actors(&self) -> std::result::Result<Vec<pb::Actor>, Status> {
        let mut all = Vec::new();
        let mut token = String::new();
        loop {
            let resp = self.list_actors(&token).await?;
            all.extend(resp.actors);
            if resp.next_page_token.is_empty() {
                return Ok(all);
            }
            token = resp.next_page_token;
        }
    }
}

type Channel = tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Headers>;

/// Adds the bearer token and the preview opt-in to every call.
#[derive(Clone)]
pub struct Headers {
    token: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    preview: bool,
}

impl tonic::service::Interceptor for Headers {
    fn call(
        &mut self,
        mut req: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, Status> {
        if let Some(t) = &self.token {
            req.metadata_mut().insert("authorization", t.clone());
        }
        if self.preview {
            req.metadata_mut().insert(
                hivekeep_core::preview::PREVIEW_HEADER,
                "true".parse().expect("ascii"),
            );
        }
        Ok(req)
    }
}

/// [`ControlPlane`] over gRPC.
pub struct GrpcControlPlane {
    client: pb::control_client::ControlClient<Channel>,
}

impl GrpcControlPlane {
    /// Dials `endpoint` (`http://host:port`).
    pub async fn connect(endpoint: &str, token: Option<&str>, preview: bool) -> Result<Self> {
        let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| StepError::msg(format!("invalid {API_ENDPOINT_ENV} {endpoint:?}: {e}")))?
            .connect()
            .await
            .map_err(|e| {
                StepError::msg(format!(
                    "while connecting to hivekeep-api at {endpoint}: {e}"
                ))
            })?;
        let token = match token {
            Some(t) => Some(format!("Bearer {t}").parse().map_err(|_| {
                StepError::msg(format!("{API_TOKEN_ENV} is not a valid header value"))
            })?),
            None => None,
        };
        let client = pb::control_client::ControlClient::with_interceptor(
            channel,
            Headers { token, preview },
        );
        Ok(Self { client })
    }
}

#[async_trait]
impl ControlPlane for GrpcControlPlane {
    async fn create_hive(&self, hive: &str) -> std::result::Result<(), Status> {
        let req = pb::CreateHiveRequest {
            hive: Some(pb::Hive {
                metadata: Some(pb::ResourceMetadata {
                    name: hive.to_string(),
                    ..Default::default()
                }),
            }),
        };
        self.client.clone().create_hive(req).await.map(|_| ())
    }
    async fn delete_hive(&self, hive: &str) -> std::result::Result<(), Status> {
        let req = pb::DeleteHiveRequest {
            hive: Some(pb::ObjectRef {
                hive: String::new(),
                name: hive.to_string(),
            }),
            options: None,
        };
        self.client.clone().delete_hive(req).await.map(|_| ())
    }
    async fn create_actor_template(
        &self,
        template: pb::ActorTemplate,
    ) -> std::result::Result<(), Status> {
        let req = pb::CreateActorTemplateRequest {
            actor_template: Some(template),
        };
        self.client
            .clone()
            .create_actor_template(req)
            .await
            .map(|_| ())
    }
    async fn get_actor_template(
        &self,
        r: pb::ObjectRef,
    ) -> std::result::Result<pb::ActorTemplate, Status> {
        let req = pb::GetActorTemplateRequest {
            actor_template: Some(r),
        };
        self.client
            .clone()
            .get_actor_template(req)
            .await
            .map(|r| r.into_inner())
    }
    async fn delete_actor_template(&self, r: pb::ObjectRef) -> std::result::Result<(), Status> {
        let req = pb::DeleteActorTemplateRequest {
            actor_template: Some(r),
            ..Default::default()
        };
        self.client
            .clone()
            .delete_actor_template(req)
            .await
            .map(|_| ())
    }
    async fn list_actors(
        &self,
        page_token: &str,
    ) -> std::result::Result<pb::ListActorsResponse, Status> {
        // An empty hive lists across all hives.
        let req = pb::ListActorsRequest {
            hive: String::new(),
            options: Some(pb::ListOptions {
                page_size: 1000,
                page_token: page_token.to_string(),
            }),
        };
        self.client
            .clone()
            .list_actors(req)
            .await
            .map(|r| r.into_inner())
    }
    async fn delete_actor(&self, r: pb::ObjectRef) -> std::result::Result<(), Status> {
        let req = pb::DeleteActorRequest {
            actor: Some(r),
            ..Default::default()
        };
        self.client.clone().delete_actor(req).await.map(|_| ())
    }
}

impl Env {
    /// Connects to hivekeep-api at `$HK_API_ENDPOINT`, opting in to preview
    /// features when the install enables any.
    pub async fn api_client(&self) -> Result<Box<dyn ControlPlane>> {
        let endpoint = std::env::var(API_ENDPOINT_ENV).unwrap_or_default();
        if endpoint.is_empty() {
            return Err(StepError::msg(format!(
                "set {API_ENDPOINT_ENV} to hivekeep-api's gRPC endpoint (for example http://localhost:8443 \
                 through `kubectl port-forward -n {} svc/hivekeep-api 8443:443`)",
                self.namespace()
            )));
        }
        let token = std::env::var(API_TOKEN_ENV).ok().filter(|t| !t.is_empty());
        let client = GrpcControlPlane::connect(
            &endpoint,
            token.as_deref(),
            !self.cfg.preview_gates.is_empty(),
        )
        .await?;
        Ok(Box::new(client))
    }

    /// Removes a demo's control-plane resources: every actor created from
    /// the templates, then the templates (which also removes their golden
    /// actors and snapshots), then the hives. A cluster without a reachable
    /// hivekeep-api has nothing to clean up.
    pub async fn delete_hivekeep_demo(
        &self,
        refs: &[ActorTemplateRef],
        hives: &[String],
    ) -> Result<()> {
        if !self
            .kube()?
            .deployment_exists(self.namespace(), super::deploy::API_SERVER)
            .await?
        {
            log::step("hivekeep-api not found; skipping actor cleanup");
            return Ok(());
        }
        let client = match self.api_client().await {
            Ok(c) => c,
            Err(e) => {
                log::warn(&format!(
                    "could not connect to hivekeep-api; skipping actor cleanup: {e}"
                ));
                return Ok(());
            }
        };
        delete_demo_resources(client.as_ref(), refs, hives).await;
        Ok(())
    }
}

/// The cleanup behind [`Env::delete_hivekeep_demo`]; failures are logged,
/// not returned, so one stuck object does not keep the rest.
pub async fn delete_demo_resources(
    client: &dyn ControlPlane,
    refs: &[ActorTemplateRef],
    hives: &[String],
) {
    let actors = match client.list_all_actors().await {
        Ok(a) => a,
        Err(e) => {
            log::warn(&format!(
                "could not list actors; skipping actor cleanup: {}",
                e.message()
            ));
            return;
        }
    };
    for r in refs {
        log::step(&format!("Deleting actors for {r}"));
        for actor in &actors {
            let template = actor
                .actor_template
                .as_ref()
                .map(ActorTemplateRef::from_object_ref);
            if template.as_ref() != Some(r) {
                continue;
            }
            let actor_ref = ActorRef::of(actor);
            log::step(&format!(
                "  deleting actor {}/{}",
                actor_ref.hive, actor_ref.name
            ));
            if let Err(e) = client.delete_actor(actor_ref.to_object_ref()).await {
                log::warn(&format!(
                    "while deleting actor {}/{}: {}",
                    actor_ref.hive,
                    actor_ref.name,
                    e.message()
                ));
            }
        }
    }
    for r in refs {
        if let Err(e) = client.delete_actor_template(r.to_object_ref()).await {
            if e.code() != Code::NotFound {
                log::warn(&format!(
                    "while deleting actor template {r}: {}",
                    e.message()
                ));
            }
        }
    }
    for hive in hives {
        if let Err(e) = client.delete_hive(hive).await {
            if e.code() != Code::NotFound {
                log::warn(&format!("while deleting hive {hive}: {}", e.message()));
            }
        }
    }
}

/// Creates the hive unless it exists. The store requires an ActorTemplate's
/// hive to exist when the template is created.
pub async fn ensure_hive(client: &dyn ControlPlane, hive: &str) -> Result<()> {
    match client.create_hive(hive).await {
        Ok(()) => Ok(()),
        Err(e) if e.code() == Code::AlreadyExists => Ok(()),
        Err(e) => Err(StepError::msg(format!(
            "creating hive {hive}: {}",
            e.message()
        ))),
    }
}

static POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(hivekeep_proto::FILE_DESCRIPTOR_SET)
        .expect("the embedded descriptor set is valid")
});

/// Parses one protobuf-JSON-shaped YAML or JSON document into an
/// ActorTemplate. Unknown fields are an error, so a typo cannot silently
/// drop configuration.
pub fn actor_template_from_manifest(manifest: &[u8]) -> Result<pb::ActorTemplate> {
    let text =
        std::str::from_utf8(manifest).map_err(|e| StepError::msg(format!("invalid YAML: {e}")))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(text).map_err(|e| StepError::msg(format!("invalid YAML: {e}")))?;
    let json: serde_json::Value =
        serde_json::to_value(&yaml).map_err(|e| StepError::msg(format!("invalid YAML: {e}")))?;
    if json.is_null() {
        return Err(StepError::msg("manifest is empty"));
    }
    let desc = POOL
        .get_message_by_name("hivekeep.api.v1.ActorTemplate")
        .ok_or_else(|| StepError::msg("the descriptor set has no ActorTemplate"))?;
    let dynamic = DynamicMessage::deserialize_with_options(desc, json, &DeserializeOptions::new())
        .map_err(|e| StepError::msg(e.to_string()))?;
    dynamic
        .transcode_to::<pb::ActorTemplate>()
        .map_err(|e| StepError::msg(e.to_string()))
}

/// Creates the template. Templates are immutable, so an existing one is
/// kept: delete the demo and redeploy to change it.
pub async fn create_actor_template(
    client: &dyn ControlPlane,
    template: pb::ActorTemplate,
) -> Result<()> {
    let r = ActorTemplateRef::of(&template);
    match client.create_actor_template(template).await {
        Ok(()) => Ok(()),
        Err(e) if e.code() == Code::AlreadyExists => {
            log::step(&format!(
                "actor template {r} already exists; keeping it (delete the demo to replace it)"
            ));
            Ok(())
        }
        Err(e) => Err(StepError::msg(format!(
            "creating actor template {r}: {}",
            e.message()
        ))),
    }
}

/// Blocks until the template's golden snapshot is built. Fails fast on a
/// reconciler error and returns at once for a template that opted out.
pub async fn wait_actor_template_golden(
    client: &dyn ControlPlane,
    r: &ActorTemplateRef,
    timeout: Duration,
) -> Result<()> {
    wait_actor_template_golden_every(client, r, timeout, GOLDEN_POLL_INTERVAL).await
}

async fn wait_actor_template_golden_every(
    client: &dyn ControlPlane,
    r: &ActorTemplateRef,
    timeout: Duration,
    interval: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        // The last error rides along in the timeout, so a wait spent on a
        // broken connection says so.
        let last_err = match client.get_actor_template(r.to_object_ref()).await {
            Ok(template) => {
                let mode = template
                    .snapshot_config
                    .as_ref()
                    .and_then(|s| s.golden_snapshot_config.as_ref())
                    .map(|g| g.mode)
                    .unwrap_or_default();
                if mode == pb::GoldenSnapshotMode::Disabled as i32 {
                    return Ok(());
                }
                let status = template
                    .status
                    .as_ref()
                    .and_then(|s| s.golden_snapshot_status.as_ref());
                if status
                    .and_then(|s| s.golden_tag.as_ref())
                    .is_some_and(|t| !t.name.is_empty())
                {
                    return Ok(());
                }
                if let Some(msg) = status
                    .map(|s| s.error_message.as_str())
                    .filter(|m| !m.is_empty())
                {
                    return Err(StepError::msg(format!("actor template {r} failed: {msg}")));
                }
                None
            }
            Err(e) => Some(e),
        };
        if Instant::now() >= deadline {
            return Err(StepError::msg(match last_err {
                Some(e) => format!(
                    "timed out waiting for actor template {r} golden snapshot: last error: {}",
                    e.message()
                ),
                None => format!("timed out waiting for actor template {r} golden snapshot"),
            }));
        }
        tokio::time::sleep(interval).await;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use parking_lot::Mutex;

    /// An in-memory control plane.
    #[derive(Default)]
    pub(crate) struct FakeControlPlane {
        pub templates: Mutex<Vec<pb::ActorTemplate>>,
        pub actors: Mutex<Vec<pb::Actor>>,
        pub hives: Mutex<Vec<String>>,
        pub get_error: Option<Status>,
        pub calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ControlPlane for FakeControlPlane {
        async fn create_hive(&self, hive: &str) -> std::result::Result<(), Status> {
            self.calls.lock().push(format!("create_hive {hive}"));
            let mut h = self.hives.lock();
            if h.iter().any(|x| x == hive) {
                return Err(Status::already_exists(hive));
            }
            h.push(hive.into());
            Ok(())
        }
        async fn delete_hive(&self, hive: &str) -> std::result::Result<(), Status> {
            self.calls.lock().push(format!("delete_hive {hive}"));
            let mut h = self.hives.lock();
            let before = h.len();
            h.retain(|x| x != hive);
            if h.len() == before {
                return Err(Status::not_found(hive));
            }
            Ok(())
        }
        async fn create_actor_template(
            &self,
            t: pb::ActorTemplate,
        ) -> std::result::Result<(), Status> {
            let r = ActorTemplateRef::of(&t);
            self.calls.lock().push(format!("create_actor_template {r}"));
            let mut ts = self.templates.lock();
            if ts.iter().any(|x| ActorTemplateRef::of(x) == r) {
                return Err(Status::already_exists(r.to_string()));
            }
            ts.push(t);
            Ok(())
        }
        async fn get_actor_template(
            &self,
            r: pb::ObjectRef,
        ) -> std::result::Result<pb::ActorTemplate, Status> {
            if let Some(e) = &self.get_error {
                return Err(e.clone());
            }
            let want = ActorTemplateRef::from_object_ref(&r);
            self.templates
                .lock()
                .iter()
                .find(|t| ActorTemplateRef::of(t) == want)
                .cloned()
                .ok_or_else(|| Status::not_found(want.to_string()))
        }
        async fn delete_actor_template(&self, r: pb::ObjectRef) -> std::result::Result<(), Status> {
            let want = ActorTemplateRef::from_object_ref(&r);
            self.calls
                .lock()
                .push(format!("delete_actor_template {want}"));
            self.templates
                .lock()
                .retain(|t| ActorTemplateRef::of(t) != want);
            Ok(())
        }
        async fn list_actors(
            &self,
            page_token: &str,
        ) -> std::result::Result<pb::ListActorsResponse, Status> {
            // One actor per page, to exercise paging.
            let actors = self.actors.lock();
            let i: usize = if page_token.is_empty() {
                0
            } else {
                page_token.parse().unwrap()
            };
            let page = actors.get(i).cloned().into_iter().collect();
            let next = if i + 1 < actors.len() {
                (i + 1).to_string()
            } else {
                String::new()
            };
            Ok(pb::ListActorsResponse {
                actors: page,
                next_page_token: next,
            })
        }
        async fn delete_actor(&self, r: pb::ObjectRef) -> std::result::Result<(), Status> {
            let want = ActorRef::from_object_ref(&r);
            self.calls.lock().push(format!("delete_actor {want}"));
            self.actors.lock().retain(|a| ActorRef::of(a) != want);
            Ok(())
        }
    }

    fn template(hive: &str, name: &str) -> pb::ActorTemplate {
        pb::ActorTemplate {
            metadata: Some(pb::ResourceMetadata {
                hive: hive.into(),
                name: name.into(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn actor_template_from_manifest_parses_protojson() {
        let manifest = "metadata:\n  hive: hivekeep-demo-counter\n  name: counter\n";
        let t = actor_template_from_manifest(manifest.as_bytes()).unwrap();
        assert_eq!(
            ActorTemplateRef::of(&t),
            ActorTemplateRef::new("hivekeep-demo-counter", "counter")
        );
    }

    #[test]
    fn actor_template_from_manifest_errors() {
        for (name, manifest) in [
            ("empty", ""),
            ("not yaml", ":\t:"),
            // Strict: a typo must fail rather than drop the field.
            (
                "unknown field",
                "metadata:\n  hive: a\n  name: t\nsnapshotsConfig: {}\n",
            ),
        ] {
            assert!(
                actor_template_from_manifest(manifest.as_bytes()).is_err(),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn wait_actor_template_golden_timeout_includes_last_error() {
        let client = FakeControlPlane {
            get_error: Some(Status::unavailable("connection refused")),
            ..Default::default()
        };
        let r = ActorTemplateRef::new("hivekeep-demo", "counter");
        let err = wait_actor_template_golden(&client, &r, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("connection refused"), "{err}");
    }

    #[tokio::test]
    async fn wait_actor_template_golden_disabled_template_ready() {
        let mut t = template("hivekeep-demo", "counter");
        t.snapshot_config = Some(pb::SnapshotConfig {
            golden_snapshot_config: Some(pb::GoldenSnapshotConfig {
                mode: pb::GoldenSnapshotMode::Disabled as i32,
            }),
            ..Default::default()
        });
        let client = FakeControlPlane::default();
        client.templates.lock().push(t);
        let r = ActorTemplateRef::new("hivekeep-demo", "counter");
        wait_actor_template_golden(&client, &r, Duration::ZERO)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_template_and_hive_tolerate_existing() {
        let client = FakeControlPlane::default();
        ensure_hive(&client, "h").await.unwrap();
        ensure_hive(&client, "h").await.unwrap();
        create_actor_template(&client, template("h", "t"))
            .await
            .unwrap();
        create_actor_template(&client, template("h", "t"))
            .await
            .unwrap();
        assert_eq!(client.templates.lock().len(), 1);
    }

    #[tokio::test]
    async fn delete_demo_resources_removes_matching_actors_then_templates_then_hives() {
        let client = FakeControlPlane::default();
        client.hives.lock().push("h".into());
        client.templates.lock().push(template("h", "t"));
        let actor = |name: &str, tmpl: &str| pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                hive: "h".into(),
                name: name.into(),
                ..Default::default()
            }),
            actor_template: Some(pb::ObjectRef {
                hive: "h".into(),
                name: tmpl.into(),
            }),
            ..Default::default()
        };
        client
            .actors
            .lock()
            .extend([actor("a1", "t"), actor("other", "u"), actor("a2", "t")]);
        delete_demo_resources(&client, &[ActorTemplateRef::new("h", "t")], &["h".into()]).await;
        let left: Vec<String> = client
            .actors
            .lock()
            .iter()
            .map(|a| ActorRef::of(a).name)
            .collect();
        assert_eq!(left, vec!["other"]);
        assert!(client.templates.lock().is_empty());
        assert!(client.hives.lock().is_empty());
        let calls = client.calls.lock().clone();
        let tpl = calls
            .iter()
            .position(|c| c.starts_with("delete_actor_template"))
            .unwrap();
        let last_actor = calls
            .iter()
            .rposition(|c| c.starts_with("delete_actor "))
            .unwrap();
        assert!(last_actor < tpl, "{calls:?}");
    }
}
