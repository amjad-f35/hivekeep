//! A small HTTP/1.1 client for the VMM's API socket: one fresh connection per
//! request (a kept-alive one can hang once the VMM closes idle connections),
//! optionally passing file descriptors with the request (SCM_RIGHTS), which
//! is how the VMM takes tap devices.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, Interest};
use tokio::net::UnixStream;

/// A response: status code and body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// The status code.
    pub status: u16,
    /// The body.
    pub body: Vec<u8>,
}

impl Response {
    /// Whether the status is 2xx.
    pub fn ok(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The body as trimmed text, for error messages.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).trim().to_string()
    }
}

/// Sends one request over a fresh connection to `socket`, passing `fds` in
/// the ancillary data of its first bytes, and reads the response. The whole
/// exchange is bounded by `timeout`.
pub async fn request(
    socket: &Path,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    fds: &[RawFd],
    timeout: Duration,
) -> io::Result<Response> {
    let work = async {
        let mut s = UnixStream::connect(socket).await?;
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nAccept: */*\r\nConnection: close\r\n"
        );
        if let Some(b) = body {
            req.push_str(&format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                b.len()
            ));
        }
        req.push_str("\r\n");
        let mut bytes = req.into_bytes();
        bytes.extend_from_slice(body.unwrap_or_default());
        let mut sent = 0;
        if !fds.is_empty() {
            sent = send_with_fds(&s, &bytes, fds).await?;
        }
        s.write_all(&bytes[sent..]).await?;
        read_response(&mut s).await
    };
    tokio::time::timeout(timeout, work).await.map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{method} {path}: timed out"),
        )
    })?
}

async fn send_with_fds(s: &UnixStream, bytes: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags, UnixAddr};
    loop {
        s.writable().await?;
        let r = s.try_io(Interest::WRITABLE, || {
            let iov = [io::IoSlice::new(bytes)];
            let cmsg = [ControlMessage::ScmRights(fds)];
            sendmsg::<UnixAddr>(s.as_raw_fd(), &iov, &cmsg, MsgFlags::empty(), None)
                .map_err(io::Error::from)
        });
        match r {
            Ok(n) => return Ok(n),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
}

async fn read_response(s: &mut UnixStream) -> io::Result<Response> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let n = s.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before the response headers",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "malformed HTTP status line"))?;
    let len = lines.find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case("content-length")
            .then(|| v.trim().parse::<usize>().ok())?
    });
    let mut body = buf[head_end..].to_vec();
    match len {
        Some(n) => {
            while body.len() < n {
                let r = s.read(&mut chunk).await?;
                if r == 0 {
                    break;
                }
                body.extend_from_slice(&chunk[..r]);
            }
            body.truncate(n);
        }
        None if status == 204 || status == 304 => body.clear(),
        None => {
            s.read_to_end(&mut body).await?;
        }
    }
    Ok(Response { status, body })
}
