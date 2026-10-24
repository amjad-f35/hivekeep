//! The row shapes the dashboard page renders, derived from control-plane
//! Workers and Actors.
//!
//! Field names are part of the page's contract (`index.html` reads them
//! directly), so they stay pod-flavoured even though they describe
//! Hivekeep workers: `node` carries the worker pool and `started_at` is
//! always empty because the API does not report a start time.

use hivekeep_proto::api::v1 as pb;
use serde::Serialize;

/// One worker, rendered like a pod.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PodSummary {
    pub name: String,
    pub node: String,
    pub phase: String,
    pub ready: bool,
    pub started_at: String,
}

/// One Actor row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ActorSummary {
    pub kind: String,
    pub name: String,
    pub phase: String,
    pub message: String,
}

/// The phase label for an Actor state. The page colours anything containing
/// "running" green and everything else as pending.
pub fn actor_state_label(state: i32) -> &'static str {
    match pb::ActorState::try_from(state) {
        Ok(pb::ActorState::Resuming) => "Resuming",
        Ok(pb::ActorState::Running) => "Running",
        Ok(pb::ActorState::Suspending) => "Suspending",
        Ok(pb::ActorState::Suspended) => "Suspended",
        Ok(pb::ActorState::Crashed) => "Crashed",
        Ok(pb::ActorState::Deleting) => "Deleting",
        Ok(pb::ActorState::Reverting) => "Reverting",
        _ => "?",
    }
}

/// Number of Actors a worker currently hosts.
fn hosted_actors(w: &pb::Worker) -> i32 {
    w.status
        .as_ref()
        .and_then(|s| s.allocated.as_ref())
        .map(|a| a.actors)
        .unwrap_or(0)
}

/// "Running" for a worker hosting at least one Actor, else "Idle".
pub fn worker_phase(w: &pb::Worker) -> &'static str {
    if hosted_actors(w) > 0 {
        "Running"
    } else {
        "Idle"
    }
}

/// Worker rows sorted by pod name. Workers are not filtered by hive: a
/// Worker does not say which Actors it holds, so there is nothing to match.
pub fn pods_from_workers(workers: &[pb::Worker]) -> Vec<PodSummary> {
    let mut pods: Vec<PodSummary> = workers
        .iter()
        .map(|w| PodSummary {
            name: w.worker_pod.clone(),
            node: w.worker_pool.clone(),
            phase: worker_phase(w).to_string(),
            ready: hosted_actors(w) > 0,
            started_at: String::new(),
        })
        .collect();
    pods.sort_by(|a, b| a.name.cmp(&b.name));
    pods
}

/// Whether an Actor belongs to the demo: its template lives in `hive`. An
/// empty filter, or an Actor whose template names no hive, passes.
pub fn in_hive(actor: &pb::Actor, hive: &str) -> bool {
    let template_hive = actor
        .actor_template
        .as_ref()
        .map(|t| t.hive.as_str())
        .unwrap_or("");
    hive.is_empty() || template_hive.is_empty() || template_hive == hive
}

fn actor_name(a: &pb::Actor) -> &str {
    a.metadata.as_ref().map(|m| m.name.as_str()).unwrap_or("")
}

/// Actor rows in the demo hive, sorted by name. The template name goes in
/// `message` as a provenance hint under each row.
pub fn actors_in_hive(actors: &[pb::Actor], hive: &str) -> Vec<ActorSummary> {
    let mut out: Vec<ActorSummary> = actors
        .iter()
        .filter(|a| in_hive(a, hive))
        .map(|a| {
            let template = a
                .actor_template
                .as_ref()
                .map(|t| t.name.as_str())
                .unwrap_or("");
            ActorSummary {
                kind: "Actor".to_string(),
                name: actor_name(a).to_string(),
                phase: actor_state_label(a.status.as_ref().map(|s| s.state).unwrap_or(0))
                    .to_string(),
                message: if template.is_empty() {
                    String::new()
                } else {
                    format!("template: {template}")
                },
            }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Sorted, non-empty names of the demo hive's Actors: the agents a task can
/// be given to.
pub fn agent_names(actors: &[pb::Actor], hive: &str) -> Vec<String> {
    let mut names: Vec<String> = actors
        .iter()
        .filter(|a| in_hive(a, hive))
        .map(actor_name)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
        .collect();
    names.sort();
    names
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::pb;

    pub fn worker(pod: &str, pool: &str, actors: i32) -> pb::Worker {
        pb::Worker {
            worker_pod: pod.into(),
            worker_pool: pool.into(),
            status: Some(pb::WorkerStatus {
                allocated: Some(pb::WorkerResources {
                    actors,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub fn actor(
        name: &str,
        template_hive: &str,
        template: &str,
        state: pb::ActorState,
    ) -> pb::Actor {
        pb::Actor {
            metadata: Some(pb::ResourceMetadata {
                name: name.into(),
                ..Default::default()
            }),
            actor_template: Some(pb::ObjectRef {
                hive: template_hive.into(),
                name: template.into(),
            }),
            status: Some(pb::ActorStatus {
                state: state as i32,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn actor_state_labels() {
        for (state, want) in [
            (pb::ActorState::Unspecified, "?"),
            (pb::ActorState::Resuming, "Resuming"),
            (pb::ActorState::Running, "Running"),
            (pb::ActorState::Suspending, "Suspending"),
            (pb::ActorState::Suspended, "Suspended"),
            (pb::ActorState::Crashed, "Crashed"),
            (pb::ActorState::Deleting, "Deleting"),
            (pb::ActorState::Reverting, "Reverting"),
        ] {
            assert_eq!(actor_state_label(state as i32), want);
        }
        assert_eq!(actor_state_label(999), "?");
    }

    #[test]
    fn worker_phases() {
        assert_eq!(worker_phase(&worker("p", "pool", 1)), "Running");
        assert_eq!(worker_phase(&worker("p", "pool", 0)), "Idle");
        assert_eq!(worker_phase(&pb::Worker::default()), "Idle");
    }

    #[test]
    fn pods_sorted_with_pool_as_node() {
        let pods = pods_from_workers(&[worker("w-b", "pool", 0), worker("w-a", "pool", 2)]);
        assert_eq!(
            pods,
            vec![
                PodSummary {
                    name: "w-a".into(),
                    node: "pool".into(),
                    phase: "Running".into(),
                    ready: true,
                    started_at: String::new()
                },
                PodSummary {
                    name: "w-b".into(),
                    node: "pool".into(),
                    phase: "Idle".into(),
                    ready: false,
                    started_at: String::new()
                },
            ]
        );
    }

    #[test]
    fn actors_filtered_by_template_hive() {
        let actors = [
            actor(
                "agent-mars",
                "demo",
                "agent-mars",
                pb::ActorState::Suspended,
            ),
            actor("other", "elsewhere", "x", pb::ActorState::Running),
            actor("agent-luna", "demo", "agent-luna", pb::ActorState::Running),
            actor("loose", "", "", pb::ActorState::Unspecified),
        ];
        let rows = actors_in_hive(&actors, "demo");
        let names: Vec<_> = rows.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["agent-luna", "agent-mars", "loose"]);
        assert_eq!(rows[0].message, "template: agent-luna");
        assert_eq!(rows[0].phase, "Running");
        assert_eq!(rows[2].message, "");
        assert_eq!(rows[2].phase, "?");
        assert_eq!(actors_in_hive(&actors, "").len(), 4);
    }

    #[test]
    fn agent_names_skip_unnamed() {
        let mut unnamed = actor("", "demo", "t", pb::ActorState::Running);
        unnamed.metadata = None;
        let actors = [
            actor("b", "demo", "t", pb::ActorState::Running),
            unnamed,
            actor("a", "demo", "t", pb::ActorState::Running),
            actor("z", "other", "t", pb::ActorState::Running),
        ];
        assert_eq!(agent_names(&actors, "demo"), ["a", "b"]);
    }

    #[test]
    fn json_shapes() {
        let pod = serde_json::to_value(&pods_from_workers(&[worker("w", "pool", 1)])[0]).unwrap();
        assert_eq!(
            pod,
            serde_json::json!({"name": "w", "node": "pool", "phase": "Running", "ready": true, "started_at": ""})
        );
        let a = actors_in_hive(&[actor("n", "h", "t", pb::ActorState::Suspended)], "h");
        assert_eq!(
            serde_json::to_value(&a[0]).unwrap(),
            serde_json::json!({"kind": "Actor", "name": "n", "phase": "Suspended", "message": "template: t"})
        );
    }
}
