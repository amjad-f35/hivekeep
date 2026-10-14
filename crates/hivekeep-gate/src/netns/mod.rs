//! Linux network namespaces: named namespaces under `/run/netns`, running
//! code and opening sockets inside one, and `/proc/sys` writes.
//!
//! Work that must happen inside a namespace runs on a short-lived OS thread
//! that joins the namespace and then exits, so no thread of the async
//! runtime ever changes namespace. Sockets keep the namespace they were
//! created in, so they can be used from any thread afterwards.
//!
//! Everything except [`sysctl`] and the target validation is Linux-only; on
//! other systems the operations fail with [`io::ErrorKind::Unsupported`].

mod dial;
mod handle;
pub mod sysctl;

use std::io;

pub use dial::{dial, validate_dial_target, Network, NsConn};
pub use handle::{create_named, get_from_name, listen, remove_named, NetNs};
pub use sysctl::{set_sysctls, Sysctl};

/// Directory holding the bind mounts that name namespaces.
pub const NAMED_NETNS_DIR: &str = "/run/netns";

/// Rejects names that would escape [`NAMED_NETNS_DIR`].
pub(crate) fn validate_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\0']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid network namespace name {name:?}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn named_netns_rejects_invalid_names() {
        for name in [
            "",
            ".",
            "..",
            "/absolute",
            "../outside",
            "nested/name",
            "worker-actor:uid/../../outside",
            "nul\0name",
        ] {
            let kind = |r: io::Result<()>| r.unwrap_err().kind();
            assert_eq!(
                kind(remove_named(name)),
                io::ErrorKind::InvalidInput,
                "{name:?}"
            );
            assert_eq!(
                kind(create_named(name).map(drop)),
                io::ErrorKind::InvalidInput,
                "{name:?}"
            );
            assert_eq!(
                kind(get_from_name(name).map(drop)),
                io::ErrorKind::InvalidInput,
                "{name:?}"
            );
        }
    }
}
