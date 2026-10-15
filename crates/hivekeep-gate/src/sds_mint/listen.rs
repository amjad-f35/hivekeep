//! The server's socket: owner-only, replacing a stale one.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use tokio::net::UnixListener;

/// Listens on `uds`, removing a stale socket and restricting the new one
/// to the owner.
pub fn listen(uds: &Path) -> anyhow::Result<UnixListener> {
    match std::fs::remove_file(uds) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            anyhow::bail!("removing stale socket {}: {e}", uds.display())
        }
        _ => {}
    }
    if let Some(dir) = uds.parent() {
        std::fs::create_dir_all(dir)
            .map_err(|e| anyhow::anyhow!("creating socket directory: {e}"))?;
    }
    let lis = UnixListener::bind(uds)
        .map_err(|e| anyhow::anyhow!("listening on {}: {e}", uds.display()))?;
    std::fs::set_permissions(uds, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| anyhow::anyhow!("restricting socket permissions: {e}"))?;
    Ok(lis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn replaces_stale_socket_and_restricts_it() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("sub/sds.sock");
        drop(listen(&p).unwrap());
        let _l = listen(&p).expect("a stale socket is replaced");
        assert_eq!(
            std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}
