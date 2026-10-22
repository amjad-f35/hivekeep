//! Identity, file and name-resolution endpoints.

use std::collections::BTreeMap;
use std::fs::File;
use std::sync::OnceLock;

use serde_json::{json, Value};

use super::{HIVE_FILE, IDENTITY_FILE, TRUST_FILE, UID_FILE};

/// The identity file, opened at startup and held for the probe's life. A
/// snapshot taken after startup then carries live guest file state for a
/// system-info file, which a restore has to re-bind; `/whoami` reads through
/// it on every request, and after a restore from a shared golden snapshot
/// the read must yield the restored Actor's own id.
static HELD_IDENTITY: OnceLock<File> = OnceLock::new();

/// Opens and holds the identity file (see [`HELD_IDENTITY`]).
pub fn hold_identity() {
    match File::open(IDENTITY_FILE) {
        Ok(f) => {
            let _ = HELD_IDENTITY.set(f);
        }
        Err(e) => eprintln!("probe: opening {IDENTITY_FILE} at startup: {e}"),
    }
}

/// Reads a file's full contents from offset 0 without moving its offset, so
/// concurrent requests do not race on the shared handle.
fn read_all_at(f: &File) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; 4096];
    #[cfg(unix)]
    let n = {
        use std::os::unix::fs::FileExt;
        f.read_at(&mut buf, 0)?
    };
    #[cfg(not(unix))]
    let n = {
        let _ = f;
        0
    };
    buf.truncate(n);
    Ok(buf)
}

/// The `/whoami` reply: the identity as read at request time. Every missing
/// file is named in `error`, so a failed assertion explains itself.
pub fn whoami() -> BTreeMap<String, String> {
    let mut resp = BTreeMap::new();
    let host = std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_default();
    resp.insert("hostname".to_string(), host);
    let mut error = String::new();
    for (key, path) in [
        ("file", IDENTITY_FILE),
        ("hive", HIVE_FILE),
        ("uid", UID_FILE),
        ("trust", TRUST_FILE),
    ] {
        match std::fs::read(path) {
            Ok(b) => {
                resp.insert(key.to_string(), String::from_utf8_lossy(&b).into_owned());
            }
            Err(e) => {
                resp.insert(key.to_string(), String::new());
                error.push_str(&format!("{path}: {e}; "));
            }
        }
    }
    resp.insert("held".to_string(), String::new());
    match HELD_IDENTITY.get() {
        None => error.push_str("identity file was not open at startup; "),
        Some(f) => match read_all_at(f) {
            Ok(b) => {
                resp.insert("held".to_string(), String::from_utf8_lossy(&b).into_owned());
            }
            Err(e) => error.push_str(&format!("reading held identity fd: {e}; ")),
        },
    }
    if !error.is_empty() {
        resp.insert("error".to_string(), error);
    }
    resp
}

/// The `/readfile` reply: what a volume actually delivered at `path`.
pub fn readfile(path: &str) -> BTreeMap<String, String> {
    let mut resp = BTreeMap::from([("path".to_string(), path.to_string())]);
    match std::fs::read(path) {
        Ok(b) => resp.insert(
            "content".to_string(),
            String::from_utf8_lossy(&b).into_owned(),
        ),
        Err(e) => resp.insert("error".to_string(), e.to_string()),
    };
    resp
}

/// The `/writefile` reply: whether `path` accepted a write.
pub fn writefile(path: &str) -> BTreeMap<String, String> {
    let mut resp = BTreeMap::from([("path".to_string(), path.to_string())]);
    match std::fs::write(path, b"written by probe") {
        Ok(()) => resp.insert("ok".to_string(), "true".to_string()),
        Err(e) => resp.insert("error".to_string(), e.to_string()),
    };
    resp
}

/// The `/lookup` reply: the addresses `host` resolves to. The system
/// resolver consults `/etc/hosts` before DNS, so a suite can assert on the
/// names the sandbox resolves locally.
pub async fn lookup(host: &str) -> Value {
    match tokio::net::lookup_host((host, 0)).await {
        Ok(addrs) => {
            let mut out: Vec<String> = Vec::new();
            for a in addrs {
                let ip = a.ip().to_string();
                if !out.contains(&ip) {
                    out.push(ip);
                }
            }
            json!({ "host": host, "addrs": out })
        }
        Err(e) => json!({ "host": host, "error": e.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readfile_and_writefile_report_results() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f");
        let path = path.to_str().unwrap();
        assert!(readfile(path).contains_key("error"));
        assert_eq!(writefile(path).get("ok").map(String::as_str), Some("true"));
        assert_eq!(
            readfile(path).get("content").map(String::as_str),
            Some("written by probe")
        );
    }

    #[test]
    fn read_all_at_does_not_move_the_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("id");
        std::fs::write(&path, b"actor-a").unwrap();
        let f = File::open(&path).unwrap();
        assert_eq!(read_all_at(&f).unwrap(), b"actor-a");
        std::fs::write(&path, b"actor-b").unwrap();
        assert_eq!(read_all_at(&f).unwrap(), b"actor-b");
    }

    #[tokio::test]
    async fn lookup_resolves_localhost() {
        let v = lookup("localhost").await;
        assert!(v["addrs"].as_array().is_some_and(|a| !a.is_empty()), "{v}");
    }
}
