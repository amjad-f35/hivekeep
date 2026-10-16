//! The guest kernel's command line parameters.

/// What every guest boots with.
pub const BASE_KERNEL_PARAMS: &str = "cgroup_no_v1=all systemd.unified_cgroup_hierarchy=1";

/// Enables the agent's debug log and the debug console on vsock port 1026.
/// Repeated calls are no-ops.
pub fn with_agent_debug(params: &str) -> String {
    append_kernel_params(
        params,
        "agent.log=",
        "agent.log=debug agent.debug_console agent.debug_console_vport=1026",
    )
}

/// Appends `add` unless `marker` is already present.
fn append_kernel_params(params: &str, marker: &str, add: &str) -> String {
    if params.contains(marker) {
        params.to_string()
    } else if params.is_empty() {
        add.to_string()
    } else {
        format!("{params} {add}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_params_have_no_debug_console() {
        for forbidden in ["agent.debug_console", "agent.debug_console_vport", "1026"] {
            assert!(!BASE_KERNEL_PARAMS.contains(forbidden), "{forbidden}");
        }
    }

    #[test]
    fn agent_debug_is_added_once() {
        let got = with_agent_debug(BASE_KERNEL_PARAMS);
        for want in [
            "agent.log=debug",
            "agent.debug_console",
            "agent.debug_console_vport=1026",
        ] {
            assert!(got.contains(want), "{got}");
        }
        assert_eq!(with_agent_debug(&got), got);
    }
}
