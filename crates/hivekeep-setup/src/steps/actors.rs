//! Paging through the control plane's actors, which demo teardown matches
//! against the templates the demo manifests own.

use super::{Result, StepError};
use async_trait::async_trait;
use hivekeep_proto::api::v1 as pb;

/// A demo's ActorTemplate. Each demo's hive is named after the Kubernetes
/// namespace holding its worker pool, so the hive also addresses the
/// namespaced objects a demo waits on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TemplateRef {
    pub hive: String,
    pub name: String,
}

/// One page of `ListActors`.
#[async_trait]
pub trait ActorPager: Send {
    async fn list_actors(&mut self, req: pb::ListActorsRequest) -> Result<pb::ListActorsResponse>;
}

#[async_trait]
impl ActorPager for pb::control_client::ControlClient<tonic::transport::Channel> {
    async fn list_actors(&mut self, req: pb::ListActorsRequest) -> Result<pb::ListActorsResponse> {
        pb::control_client::ControlClient::list_actors(self, req)
            .await
            .map(|r| r.into_inner())
            .map_err(|s| StepError::msg(format!("while listing actors: {}", s.message())))
    }
}

/// Every actor in every hive (an empty hive lists across all).
pub async fn list_all_actors(client: &mut dyn ActorPager) -> Result<Vec<pb::Actor>> {
    let mut all = Vec::new();
    let mut page_token = String::new();
    loop {
        let resp = client
            .list_actors(pb::ListActorsRequest {
                hive: String::new(),
                options: Some(pb::ListOptions {
                    page_size: 1000,
                    page_token: page_token.clone(),
                }),
            })
            .await?;
        all.extend(resp.actors);
        page_token = resp.next_page_token;
        if page_token.is_empty() {
            return Ok(all);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Pages(Vec<pb::ListActorsResponse>, Vec<String>);

    #[async_trait]
    impl ActorPager for Pages {
        async fn list_actors(
            &mut self,
            req: pb::ListActorsRequest,
        ) -> Result<pb::ListActorsResponse> {
            self.1.push(req.options.unwrap().page_token);
            Ok(self.0.remove(0))
        }
    }

    #[tokio::test]
    async fn pages_until_no_token() {
        let actor = pb::Actor::default();
        let mut p = Pages(
            vec![
                pb::ListActorsResponse {
                    actors: vec![actor.clone()],
                    next_page_token: "t1".into(),
                },
                pb::ListActorsResponse {
                    actors: vec![actor.clone(), actor],
                    next_page_token: String::new(),
                },
            ],
            vec![],
        );
        assert_eq!(list_all_actors(&mut p).await.unwrap().len(), 3);
        assert_eq!(p.1, ["", "t1"]);
    }
}
