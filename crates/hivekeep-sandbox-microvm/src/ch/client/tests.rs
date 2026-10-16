use super::*;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

#[derive(Debug, Clone, PartialEq)]
struct Recorded {
    method: String,
    path: String,
    body: String,
}

/// A stand-in VMM API server recording what it is asked. A ping answers like
/// a real VMM; everything else gets 204, and `vm.info` an empty body.
async fn fake_ch(dir: &Path) -> (PathBuf, Arc<Mutex<Vec<Recorded>>>) {
    let sock = dir.join("ch.sock");
    let l = UnixListener::bind(&sock).unwrap();
    let seen: Arc<Mutex<Vec<Recorded>>> = Arc::default();
    let s = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut c, _)) = l.accept().await {
            let s = s.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                let (head, body) = loop {
                    let n = c.read(&mut chunk).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).to_string();
                        let len = head
                            .lines()
                            .find_map(|l| l.strip_prefix("Content-Length: "))
                            .map_or(0, |v| v.trim().parse().unwrap());
                        while buf.len() < i + 4 + len {
                            let n = c.read(&mut chunk).await.unwrap();
                            buf.extend_from_slice(&chunk[..n]);
                        }
                        break (
                            head,
                            String::from_utf8_lossy(&buf[i + 4..i + 4 + len]).to_string(),
                        );
                    }
                };
                let mut first = head.lines().next().unwrap().split_whitespace();
                let (method, path) = (
                    first.next().unwrap().to_string(),
                    first.next().unwrap().to_string(),
                );
                s.lock().push(Recorded {
                    method,
                    path: path.clone(),
                    body,
                });
                let resp = if path == "/api/v1/vmm.ping" {
                    let b = r#"{"build_version":"v52.0","version":"52.0.0","pid":1,"features":["kvm"]}"#;
                    format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{b}", b.len())
                } else {
                    "HTTP/1.1 204 No Content\r\n\r\n".to_string()
                };
                let _ = c.write_all(resp.as_bytes()).await;
            });
        }
    });
    (sock, seen)
}

#[test]
fn snapshot_urls() {
    assert_eq!(
        snapshot_url(Path::new("/var/lib/snap")),
        "file:///var/lib/snap"
    );
}

#[tokio::test]
async fn lifecycle_calls() {
    let d = tempfile::tempdir().unwrap();
    let (sock, seen) = fake_ch(d.path()).await;
    let c = Client::new(&sock);
    let info = c
        .wait_ready(Duration::from_secs(1), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(info.version, "52.0.0");
    assert_eq!(c.info().build_version, "v52.0");
    c.pause().await.unwrap();
    let snap = d.path().join("snap");
    c.snapshot(&snap).await.unwrap();
    c.resume().await.unwrap();
    let got: Vec<Recorded> = seen
        .lock()
        .iter()
        .filter(|r| r.path != "/api/v1/vmm.ping")
        .cloned()
        .collect();
    let want = [
        // Pause checks the state first; the empty reply is no state, so it
        // goes on to pause.
        ("GET", "/api/v1/vm.info"),
        ("PUT", "/api/v1/vm.pause"),
        ("PUT", "/api/v1/vm.snapshot"),
        ("PUT", "/api/v1/vm.resume"),
    ];
    assert_eq!(got.len(), want.len(), "{got:?}");
    for (g, (m, p)) in got.iter().zip(want) {
        assert_eq!((g.method.as_str(), g.path.as_str()), (m, p));
    }
    let body: serde_json::Value = serde_json::from_str(&got[2].body).unwrap();
    assert_eq!(body["destination_url"], snapshot_url(&snap));
}

#[tokio::test]
async fn wait_ready_times_out() {
    let d = tempfile::tempdir().unwrap();
    let c = Client::new(d.path().join("nonexistent.sock"));
    assert!(c
        .wait_ready(Duration::from_millis(50), &CancellationToken::new())
        .await
        .is_err());
}

#[tokio::test]
async fn restore_passes_tap_fds() {
    use std::os::fd::AsRawFd;
    let d = tempfile::tempdir().unwrap();
    let (sock, seen) = fake_ch(d.path()).await;
    let c = Client::new(&sock);
    let f = std::fs::File::open("/dev/null").unwrap();
    let nets = [RestoredNet {
        id: "_net1".into(),
        fds: vec![f.as_raw_fd()],
    }];
    c.restore_with_net_fds(Path::new("/snap"), &nets, crate::ch::MEM_RESTORE_ON_DEMAND)
        .await
        .unwrap();
    let r = seen.lock().last().cloned().unwrap();
    assert_eq!(r.path, "/api/v1/vm.restore");
    let body: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(body["source_url"], "file:///snap");
    assert_eq!(body["memory_restore_mode"], "OnDemand");
    assert_eq!(body["net_fds"][0]["id"], "_net1");
    assert_eq!(body["net_fds"][0]["num_fds"], 1);
}

#[test]
fn reads_snapshot_net_devices() {
    let d = tempfile::tempdir().unwrap();
    std::fs::write(
        d.path().join("config.json"),
        r#"{"net":[{"id":"_net1","num_queues":2,"mac":"02:a8:1e:00:00:02"},{"id":"_net2","num_queues":0,"mac":""}]}"#,
    )
    .unwrap();
    let got = snapshot_net_devices(d.path()).unwrap();
    assert_eq!(
        got[0],
        SnapshotNetDevice {
            id: "_net1".into(),
            queue_pairs: 1,
            mac: "02:a8:1e:00:00:02".into()
        }
    );
    assert_eq!(got[1].queue_pairs, 1);
}
