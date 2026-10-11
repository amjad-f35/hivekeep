//! Shaping a spec for gVisor: runsc CRI annotations, durable-dir mount
//! hints on the sandbox, and a per-container cgroup leaf sized from the
//! actor's limits (one sentry backs every container, so the sandbox cgroup
//! is the one that binds).

use std::path::Path;

use hivekeep_proto::worker::v1 as pb;

use super::sizing::SandboxSize;
use super::{Linux, Spec};

/// The runsc ID of the sandbox, created with no root container. The
/// underscore keeps it out of the DNS-label names containers get.
pub const SANDBOX_ID: &str = "_sandbox";

/// The gVisor context of one actor container.
#[derive(Debug, Clone, Default)]
pub struct GvisorOptions<'a> {
    /// The actor.
    pub actor_uid: &'a str,
    /// The actor's directories.
    pub actor_dirs: Option<&'a pb::ActorDirs>,
    /// The container, or [`SANDBOX_ID`].
    pub container_name: &'a str,
    /// Durable-dir volumes, declared on the sandbox only.
    pub durable_volumes: &'a [String],
    /// Sizes the container's cgroup leaf.
    pub size: SandboxSize,
}

/// An actor container's cgroup, relative to the pod scope.
pub fn gvisor_cgroup_leaf(actor_uid: &str, container: &str) -> String {
    format!("{actor_uid}-{container}")
}

/// Adds the runsc annotations, mount hints and cgroup. Idempotent.
pub fn shape_gvisor(spec: &mut Spec, o: GvisorOptions<'_>) {
    let a = &mut spec.annotations;
    a.insert(
        "io.kubernetes.cri.container-name".into(),
        o.container_name.to_string(),
    );
    if o.container_name == SANDBOX_ID {
        a.insert("io.kubernetes.cri.container-type".into(), "sandbox".into());
        let durable_dir = o
            .actor_dirs
            .map(|d| d.durable_dir_volume_mounts_dir.as_str())
            .unwrap_or_default();
        for v in o.durable_volumes {
            a.insert(format!("dev.gvisor.spec.mount.{v}.type"), "tmpfs".into());
            a.insert(
                format!("dev.gvisor.spec.mount.{v}.share"),
                "container".into(),
            );
            a.insert(
                format!("dev.gvisor.spec.mount.{v}.source"),
                Path::new(durable_dir)
                    .join(v)
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    } else {
        a.insert(
            "io.kubernetes.cri.container-type".into(),
            "container".into(),
        );
        a.insert("io.kubernetes.cri.sandbox-id".into(), SANDBOX_ID.into());
    }
    let linux = spec.linux.get_or_insert_with(Linux::default);
    // Colon-free: runsc reads colons as systemd cgroup syntax.
    if linux.cgroups_path.is_empty() {
        linux.cgroups_path = format!("/{}", gvisor_cgroup_leaf(o.actor_uid, o.container_name));
    }
    o.size.apply_to_oci_spec(spec);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(actor: &str, container: &str) -> String {
        let mut spec = Spec::default();
        shape_gvisor(
            &mut spec,
            GvisorOptions {
                actor_uid: actor,
                container_name: container,
                ..Default::default()
            },
        );
        spec.linux.unwrap().cgroups_path
    }

    #[test]
    fn shape_gvisor_gives_each_actor_its_own_cgroup_leaf() {
        let (first, second) = (shape("uid-a", SANDBOX_ID), shape("uid-b", SANDBOX_ID));
        assert_ne!(first, second);
        assert!(first.contains("uid-a"));
        assert!(!first.contains(':'), "runsc reads colons as systemd syntax");
        assert_ne!(shape("uid-a", SANDBOX_ID), shape("uid-a", "app"));
    }

    #[test]
    fn gvisor_cgroup_leaf_matches_the_shaped_path() {
        assert_eq!(
            shape("uid-a", SANDBOX_ID),
            format!("/{}", gvisor_cgroup_leaf("uid-a", SANDBOX_ID))
        );
    }
}
