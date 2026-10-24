use std::io::{Read, Write};

#[tokio::test]
async fn request_returns_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("h.log");
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let app = hivekeep_demo_fspersist::app::router(path.clone());
    tokio::spawn(async move { axum::serve(l, app).await });
    let body = tokio::task::spawn_blocking(move || {
        let mut c = std::net::TcpStream::connect(addr).unwrap();
        c.write_all(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut s = String::new();
        c.read_to_string(&mut s).unwrap();
        s
    })
    .await
    .unwrap();
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.contains("\n--- history ---\npod="), "{body}");
    assert!(std::fs::read_to_string(path).unwrap().contains("count=0"));
}
