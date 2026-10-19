//! Worker commands: get (with filters), drain, mark available/unavailable,
//! and listing a Worker's Actor assignments.

use std::collections::HashSet;

use hivekeep_proto::api::v1 as pb;

use crate::cli::WorkerFilter;
use crate::client::Api;
use crate::context::{rpc_error, Ctx};
use crate::error::{CliError, Result};
use crate::label_selector::LabelSelector;
use crate::print;
use crate::print::worker::sandbox_class;
use crate::refs::global_ref;

fn some(v: &Option<String>) -> &str {
    v.as_deref().unwrap_or_default()
}

/// Checks the filter flags before connecting: none of them apply when
/// getting Workers by name, and the selector must parse.
pub fn validate_filter(names: &[String], f: &WorkerFilter) -> Result<Option<LabelSelector>> {
    if !names.is_empty() {
        for (set, flag) in [
            (!some(&f.namespace).is_empty(), "-n/--namespace"),
            (!some(&f.hive).is_empty(), "-H/--hive"),
            (!some(&f.selector).is_empty(), "-l/--selector"),
            (!some(&f.sandbox_class).is_empty(), "--sandbox-class"),
        ] {
            if set {
                return Err(CliError::usage(format!(
                    "{flag} cannot be used when getting workers by name"
                )));
            }
        }
    }
    let sel = some(&f.selector);
    if sel.is_empty() {
        return Ok(None);
    }
    LabelSelector::parse(sel)
        .map(Some)
        .map_err(|e| CliError::usage(format!("invalid label selector {sel:?}: {e}")))
}

/// The names of the Workers hosting an Actor in `hive`. Asked of the
/// Actors, since a Worker listing says how full a Worker is, not what it
/// holds.
pub async fn workers_hosting_hive(api: &Api, hive: &str) -> Result<HashSet<String>> {
    let actors = api
        .list_all_actors(hive)
        .await
        .map_err(|s| rpc_error(format!("failed to list actors in hive {hive:?}"), &s))?;
    Ok(actors
        .into_iter()
        .filter_map(|a| a.status?.worker_assignment?.worker.map(|w| w.name))
        .filter(|n| !n.is_empty())
        .collect())
}

/// Lists every Worker and applies the filter. An idle Worker hosts nothing,
/// so a hive filter never matches it.
pub async fn list_filtered(
    api: &Api,
    f: &WorkerFilter,
    selector: Option<&LabelSelector>,
) -> Result<Vec<pb::Worker>> {
    let workers = api
        .list_all_workers()
        .await
        .map_err(|s| rpc_error("failed to list workers", &s))?;
    let hosting = match some(&f.hive) {
        "" => None,
        hive => Some(workers_hosting_hive(api, hive).await?),
    };
    Ok(workers
        .into_iter()
        .filter(|w| some(&f.namespace).is_empty() || w.worker_namespace == some(&f.namespace))
        .filter(|w| {
            hosting
                .as_ref()
                .is_none_or(|h| w.metadata.as_ref().is_some_and(|m| h.contains(&m.name)))
        })
        .filter(|w| selector.is_none_or(|s| s.matches(&w.labels)))
        .filter(|w| some(&f.sandbox_class).is_empty() || sandbox_class(w) == some(&f.sandbox_class))
        .collect())
}

/// `get worker [names...]`.
pub async fn get(ctx: &mut Ctx, names: &[String], filter: &WorkerFilter) -> Result<()> {
    let selector = validate_filter(names, filter)?;
    let api = ctx.api().await?;
    let mut out = ctx.out();
    if names.is_empty() {
        let workers = list_filtered(&api, filter, selector.as_ref()).await?;
        return print::worker::print_list(&ctx.printer, &mut out, workers);
    }
    let mut workers = Vec::with_capacity(names.len());
    for name in names {
        let w = api
            .get_worker(pb::GetWorkerRequest {
                worker: Some(global_ref(name)),
            })
            .await
            .map_err(|s| rpc_error(format!("failed to get worker {name:?}"), &s))?;
        workers.push(w);
    }
    if let [one] = workers.as_slice() {
        return print::worker::print_one(&ctx.printer, &mut out, one);
    }
    print::worker::print_list(&ctx.printer, &mut out, workers)
}

/// `get worker-assignments <worker>`.
pub async fn assignments(ctx: &mut Ctx, worker: &str) -> Result<()> {
    let api = ctx.api().await?;
    let list = api.list_all_worker_assignments(worker).await.map_err(|s| {
        rpc_error(
            format!("failed to list assignments of worker {worker:?}"),
            &s,
        )
    })?;
    print::assignment::print_list(&ctx.printer, &mut ctx.out(), list)
}

/// A change to a Worker's placement state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Stop placing Actors on it and move its Actors off.
    Drain,
    /// Return it to service.
    MarkAvailable,
    /// Take it out of service.
    MarkUnavailable,
}

/// `drain worker`, `mark-available worker`, `mark-unavailable worker`.
pub async fn transition(ctx: &mut Ctx, name: &str, t: Transition) -> Result<()> {
    let api = ctx.api().await?;
    let worker = Some(global_ref(name));
    let result = match t {
        Transition::Drain => api
            .drain_worker(pb::DrainWorkerRequest { worker })
            .await
            .map(|r| r.worker),
        Transition::MarkAvailable => api
            .mark_worker_available(pb::MarkWorkerAvailableRequest { worker })
            .await
            .map(|r| r.worker),
        Transition::MarkUnavailable => api
            .mark_worker_unavailable(pb::MarkWorkerUnavailableRequest { worker })
            .await
            .map(|r| r.worker),
    };
    let verb = match t {
        Transition::Drain => "drain",
        Transition::MarkAvailable => "mark available",
        Transition::MarkUnavailable => "mark unavailable",
    };
    let worker = result.map_err(|s| rpc_error(format!("failed to {verb} worker {name:?}"), &s))?;
    print::worker::print_one(&ctx.printer, &mut ctx.out(), &worker.unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_are_rejected_by_name() {
        let names = vec!["w1".to_string()];
        for (f, want) in [
            (
                WorkerFilter {
                    namespace: Some("ns".into()),
                    ..Default::default()
                },
                "-n/--namespace",
            ),
            (
                WorkerFilter {
                    hive: Some("h".into()),
                    ..Default::default()
                },
                "-H/--hive",
            ),
            (
                WorkerFilter {
                    selector: Some("a=b".into()),
                    ..Default::default()
                },
                "-l/--selector",
            ),
            (
                WorkerFilter {
                    sandbox_class: Some("gvisor".into()),
                    ..Default::default()
                },
                "--sandbox-class",
            ),
        ] {
            let err = validate_filter(&names, &f).unwrap_err();
            assert!(err.is_usage() && err.message().starts_with(want), "{err}");
            assert!(validate_filter(&[], &f).is_ok());
        }
        assert!(validate_filter(&names, &WorkerFilter::default())
            .unwrap()
            .is_none());
    }

    #[test]
    fn bad_selector_is_a_usage_error() {
        let f = WorkerFilter {
            selector: Some("a in (b".into()),
            ..Default::default()
        };
        let err = validate_filter(&[], &f).unwrap_err();
        assert!(err.is_usage() && err.message().contains("invalid label selector"));
    }
}
