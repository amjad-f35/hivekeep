//! The raw cluster operations the installer needs, as a trait so the
//! higher-level [`super::Client`] runs unchanged against a live API server
//! ([`super::live::LiveCluster`]) or an in-memory one
//! ([`super::fake::FakeCluster`]).

use super::{Gvk, KubeError, Object};
use async_trait::async_trait;
use serde_json::Value;

/// How a resource is addressed, as discovery reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceInfo {
    pub gvk: Gvk,
    /// The plural resource name: `deployments`.
    pub plural: String,
    pub namespaced: bool,
}

impl ResourceInfo {
    /// The namespace a request for an object should use: empty for
    /// cluster-scoped resources, and `default` for namespaced ones that name
    /// none.
    pub fn effective_namespace<'a>(&self, requested: &'a str) -> &'a str {
        if !self.namespaced {
            ""
        } else if requested.is_empty() {
            "default"
        } else {
            requested
        }
    }
}

/// Delete propagation policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Propagation {
    Foreground,
    Background,
    Orphan,
}

/// A patch body and its type.
#[derive(Clone, Debug, PartialEq)]
pub enum Patch {
    /// RFC 7386 JSON merge patch.
    Merge(Value),
    /// Kubernetes strategic merge patch.
    Strategic(Value),
}

/// What an exec in a pod produced.
#[derive(Clone, Debug, Default)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[async_trait]
pub trait Cluster: Send + Sync {
    /// Looks the gvk up in discovery. [`KubeError::NoMatch`] when nothing
    /// serves it.
    async fn resolve(&self, gvk: &Gvk) -> Result<ResourceInfo, KubeError>;
    /// The group versions discovery lists for an API group.
    async fn group_versions(&self, group: &str) -> Result<Vec<String>, KubeError>;
    /// `gitVersion` of the API server.
    async fn server_version(&self) -> Result<String, KubeError>;
    /// Server-side apply with this installer's field manager, forcing
    /// ownership of conflicting fields.
    async fn apply(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        obj: &Object,
    ) -> Result<Object, KubeError>;
    async fn get(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
    ) -> Result<Object, KubeError>;
    async fn list(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        label_selector: Option<&str>,
    ) -> Result<Vec<Object>, KubeError>;
    async fn create(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        obj: &Object,
    ) -> Result<Object, KubeError>;
    async fn delete(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
        propagation: Option<Propagation>,
    ) -> Result<(), KubeError>;
    async fn patch(
        &self,
        res: &ResourceInfo,
        namespace: &str,
        name: &str,
        patch: &Patch,
    ) -> Result<Object, KubeError>;
    /// A GET of an absolute API path, returning the body.
    async fn raw_get(&self, path: &str) -> Result<Vec<u8>, KubeError>;
    /// Runs a command in a pod's container and collects its output.
    async fn exec(
        &self,
        namespace: &str,
        pod: &str,
        container: &str,
        command: &[String],
        stdin: Option<Vec<u8>>,
    ) -> Result<ExecOutput, KubeError>;
}
