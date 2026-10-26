//! The user classes a worker can run.

use super::userclass::Registry;

pub mod glutton;
pub mod hkenv;

pub mod resumepaused;
pub mod sweperf;

pub mod agentsession;

/// Every user class this build knows.
pub fn registry() -> Registry {
    let mut r = Registry::new();
    for e in glutton::entries()
        .into_iter()
        .chain(hkenv::entries())
        .chain(resumepaused::entries())
        .chain(sweperf::entries())
        .chain(agentsession::entries())
    {
        r.add(e);
    }
    r
}

#[cfg(test)]
mod tests {
    #[test]
    fn every_class_is_registered_once() {
        let names = super::registry().names();
        for n in [
            "agentsession",
            "durdir",
            "glutton",
            "hkenv",
            "resumepaused",
            "spawn",
            "sweperf",
        ] {
            assert!(names.contains(&n), "{n} missing from {names:?}");
        }
    }
}
