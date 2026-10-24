//! The client side: talking to hivekeep-api and to the Actor through the gate.

use crate::server::PROCESS_PATH;
use crate::wire::{ProcessRequest, ProcessResponse};
use hivekeep_proto::api::v1 as pb;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

/// Header naming the Actor a request through the gate is for.
pub const TARGET_ACTOR_HEADER: &str = "hk-target-actor";

/// Client failures.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("sending request: {0}")]
    Send(#[from] reqwest::Error),
    #[error("unexpected status code {status}: {body}")]
    Status { status: u16, body: String },
    #[error("decoding response: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("invalid hivekeep-api endpoint {0:?}: {1}")]
    Endpoint(String, String),
    #[error("connecting to hivekeep-api: {0}")]
    Connect(#[from] tonic::transport::Error),
    #[error("{0}")]
    Rpc(#[from] tonic::Status),
}

/// An Actor, addressed as `hive/name`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorRef {
    pub hive: String,
    pub name: String,
}

impl ActorRef {
    pub fn to_object_ref(&self) -> pb::ObjectRef {
        pb::ObjectRef {
            hive: self.hive.clone(),
            name: self.name.clone(),
        }
    }
}

impl std::fmt::Display for ActorRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.hive, self.name)
    }
}

/// What one REPL line asks for.
#[derive(Debug, PartialEq, Eq)]
pub enum Line {
    Skip,
    Exit,
    Run(String),
}

/// Classifies an input line.
pub fn classify(line: &str) -> Line {
    match line.trim() {
        "" => Line::Skip,
        "exit" => Line::Exit,
        cmd => Line::Run(cmd.to_string()),
    }
}

/// Runs `command` through `sh -c` in the Actor, via the gate listening at
/// `gate_addr` (`host:port`).
pub async fn run_command(
    http: &reqwest::Client,
    gate_addr: &str,
    actor: &ActorRef,
    command: &str,
) -> Result<ProcessResponse, ClientError> {
    let body = ProcessRequest {
        command: vec!["sh".into(), "-c".into(), command.into()],
        ..Default::default()
    };
    let resp = http
        .post(format!("http://{gate_addr}{PROCESS_PATH}"))
        .header(TARGET_ACTOR_HEADER, actor.to_string())
        .json(&body)
        .send()
        .await?;
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if status != reqwest::StatusCode::OK {
        return Err(ClientError::Status {
            status: status.as_u16(),
            body: String::from_utf8_lossy(&bytes).into_owned(),
        });
    }
    Ok(serde_json::from_slice(&bytes)?)
}

/// Adds the bearer token, when there is one, to every call.
#[derive(Clone)]
pub struct Bearer(Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>);

impl tonic::service::Interceptor for Bearer {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(t) = &self.0 {
            req.metadata_mut().insert("authorization", t.clone());
        }
        Ok(req)
    }
}

pub type Control = pb::control_client::ControlClient<InterceptedService<Channel, Bearer>>;

/// Dials hivekeep-api at `endpoint` (`http://host:port`, plaintext).
pub async fn connect(endpoint: &str, token: Option<&str>) -> Result<Control, ClientError> {
    let channel = tonic::transport::Endpoint::from_shared(endpoint.to_string())
        .map_err(|e| ClientError::Endpoint(endpoint.into(), e.to_string()))?
        .connect()
        .await?;
    let token = match token.filter(|t| !t.is_empty()) {
        Some(t) => Some(format!("Bearer {t}").parse().map_err(|_| {
            ClientError::Endpoint(endpoint.into(), "token is not a valid header value".into())
        })?),
        None => None,
    };
    Ok(pb::control_client::ControlClient::with_interceptor(
        channel,
        Bearer(token),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_lines() {
        assert_eq!(classify("   "), Line::Skip);
        assert_eq!(classify(" exit "), Line::Exit);
        assert_eq!(classify(" ls -la "), Line::Run("ls -la".into()));
    }

    #[test]
    fn actor_ref_formats() {
        let a = ActorRef {
            hive: "hivekeep-demo-sandbox".into(),
            name: "s1".into(),
        };
        assert_eq!(a.to_string(), "hivekeep-demo-sandbox/s1");
        assert_eq!(a.to_object_ref().name, "s1");
    }
}
