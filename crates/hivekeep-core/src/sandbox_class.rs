//! Sandbox runtime families a worker pool can select.

/// The gVisor (`runsc`) runtime. The default.
pub const GVISOR: &str = "gvisor";
/// The micro-VM runtime. Needs `/dev/kvm` and vhost devices.
pub const MICROVM: &str = "microvm";

/// Every sandbox class.
pub const ALL: [&str; 2] = [GVISOR, MICROVM];

/// Whether `class` names a known sandbox class (case-sensitive).
pub fn is_valid(class: &str) -> bool {
    ALL.contains(&class)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes() {
        assert!(is_valid("gvisor") && is_valid("microvm"));
        assert!(!is_valid("GVisor") && !is_valid("") && !is_valid("kata"));
        assert!(!is_valid("GVISOR"));
    }

    /// The class list is a constant: callers cannot change what is valid.
    #[test]
    fn all_is_immutable() {
        let mut copy = ALL;
        copy[0] = "mutated";
        assert_eq!(copy[0], "mutated");
        assert!(is_valid(GVISOR));
        assert_eq!(ALL, [GVISOR, MICROVM]);
    }
}
