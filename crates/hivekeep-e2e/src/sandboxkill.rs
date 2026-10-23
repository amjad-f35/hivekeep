//! Killing an Actor's sandbox the way a crash would.

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{AppArmorProfile, EphemeralContainer, Pod, SecurityContext};
use kube::api::{Api, PostParams};

use crate::clients::Clients;
use crate::harness::T;
use crate::images::Image;
use crate::kube_util::is_conflict;

/// Kills the Actor's sandbox processes on its worker from an ephemeral
/// container sharing the worker's PID namespace. The test server's `kill`
/// subcommand matches the sandbox and its helpers (runsc and its gofer, or
/// the VMM and virtiofsd) by the Actor UID on their command lines.
pub async fn kill_actor_sandbox(
    t: &T,
    clients: &Clients,
    namespace: &str,
    pod_name: &str,
    actor_uid: &str,
) {
    let pods: Api<Pod> = Api::namespaced(clients.kube(), namespace);
    let image = Image::Testserver.reference();
    let mut attempts = 0;
    let name = loop {
        attempts += 1;
        let mut pod = pods
            .get(pod_name)
            .await
            .unwrap_or_else(|e| t.fatal(format!("getting worker pod {namespace}/{pod_name}: {e}")));
        let spec = pod.spec.get_or_insert_with(Default::default);
        let ephemeral = spec.ephemeral_containers.get_or_insert_with(Vec::new);
        let name = format!("kill-sandbox-{}", ephemeral.len());
        ephemeral.push(EphemeralContainer {
            name: name.clone(),
            image: Some(image.clone()),
            target_container_name: Some("worker".into()),
            args: Some(vec![
                "kill".into(),
                "--cmdline-contains".into(),
                actor_uid.into(),
            ]),
            // Root, like the sandbox processes, and unconfined: a default
            // AppArmor profile may forbid signalling them.
            security_context: Some(SecurityContext {
                run_as_user: Some(0),
                app_armor_profile: Some(AppArmorProfile {
                    type_: "Unconfined".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
        match pods
            .replace_subresource(
                "ephemeralcontainers",
                pod_name,
                &PostParams::default(),
                &pod,
            )
            .await
        {
            Ok(_) => break name,
            Err(e) if is_conflict(&e) && attempts < 5 => continue,
            Err(e) => t.fatal(format!(
                "adding ephemeral container to {namespace}/{pod_name}: {e}"
            )),
        }
    };

    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        let pod = pods
            .get(pod_name)
            .await
            .unwrap_or_else(|e| t.fatal(format!("getting worker pod {namespace}/{pod_name}: {e}")));
        let statuses = pod
            .status
            .and_then(|s| s.ephemeral_container_statuses)
            .unwrap_or_default();
        if let Some(st) = statuses.into_iter().find(|s| s.name == name) {
            let state = st.state.unwrap_or_default();
            if let Some(w) = state.waiting {
                let reason = w.reason.unwrap_or_default();
                if !reason.is_empty()
                    && reason != "ContainerCreating"
                    && reason != "PodInitializing"
                {
                    t.fatal(format!(
                        "kill container for actor {actor_uid} cannot start: {reason}: {}",
                        w.message.unwrap_or_default()
                    ));
                }
            }
            if let Some(term) = state.terminated {
                if term.exit_code != 0 {
                    t.fatal(format!(
                        "killing the sandbox of actor {actor_uid} exited {}",
                        term.exit_code
                    ));
                }
                t.log(format!(
                    "Killed the sandbox of actor {actor_uid} on {namespace}/{pod_name}"
                ));
                return;
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    t.fatal(format!(
        "timed out killing the sandbox of actor {actor_uid}"
    ))
}
