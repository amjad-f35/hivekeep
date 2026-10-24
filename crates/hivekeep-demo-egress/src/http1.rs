//! A small HTTP/1.x response reader for the endpoints that write their
//! requests byte by byte (delayed bodies, split header blocks, CONNECT) and
//! therefore cannot use a regular client.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt};

/// Largest response head accepted.
const MAX_HEAD: usize = 64 << 10;
const MAX_HEADERS: usize = 100;

/// A parsed response with its body read up to the caller's limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub status: u16,
    /// `HTTP/1.0` or `HTTP/1.1`.
    pub protocol: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    /// First value of header `name`, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

fn bad(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Reads bytes from `stream` while keeping what was read ahead.
struct Reader<'a, R> {
    stream: &'a mut R,
    buf: Vec<u8>,
}

impl<R: AsyncRead + Unpin> Reader<'_, R> {
    /// Reads more data; returns false at end of stream.
    async fn fill(&mut self) -> io::Result<bool> {
        let mut chunk = [0u8; 8192];
        let n = self.stream.read(&mut chunk).await?;
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(n > 0)
    }

    /// Takes up to `n` bytes, reading as needed; fewer at end of stream.
    async fn take(&mut self, n: usize) -> io::Result<Vec<u8>> {
        while self.buf.len() < n {
            if !self.fill().await? {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ));
            }
        }
        Ok(self.buf.drain(..n).collect())
    }

    /// Takes one CRLF-terminated line, without the terminator.
    async fn line(&mut self) -> io::Result<Vec<u8>> {
        loop {
            if let Some(i) = self.buf.windows(2).position(|w| w == b"\r\n") {
                let line: Vec<u8> = self.buf.drain(..i + 2).take(i).collect();
                return Ok(line);
            }
            if self.buf.len() > MAX_HEAD {
                return Err(bad("line too long"));
            }
            if !self.fill().await? {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ));
            }
        }
    }
}

/// A parsed status line and header block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub status: u16,
    /// `HTTP/1.0` or `HTTP/1.1`.
    pub protocol: String,
    pub headers: Vec<(String, String)>,
}

impl Head {
    /// First value of header `name`, case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// Reads a response head. `buf` carries bytes already read from `stream`
/// on entry and holds whatever followed the head on return.
pub async fn read_head<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: &mut Vec<u8>,
) -> io::Result<Head> {
    let mut reader = Reader {
        stream,
        buf: std::mem::take(buf),
    };
    let result = loop {
        let mut slots = [httparse::EMPTY_HEADER; MAX_HEADERS];
        let mut parsed = httparse::Response::new(&mut slots);
        match parsed.parse(&reader.buf) {
            Err(e) => break Err(bad(format!("malformed HTTP response: {e}"))),
            Ok(httparse::Status::Complete(len)) => {
                let headers = parsed
                    .headers
                    .iter()
                    .map(|h| {
                        (
                            h.name.to_string(),
                            String::from_utf8_lossy(h.value).into_owned(),
                        )
                    })
                    .collect::<Vec<_>>();
                let protocol = format!("HTTP/1.{}", parsed.version.unwrap_or(1));
                let head = Head {
                    status: parsed.code.unwrap_or_default(),
                    protocol,
                    headers,
                };
                reader.buf.drain(..len);
                break Ok(head);
            }
            Ok(httparse::Status::Partial) => {
                if reader.buf.len() > MAX_HEAD {
                    break Err(bad("response head too large"));
                }
                match reader.fill().await {
                    Ok(true) => {}
                    Ok(false) => {
                        break Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected EOF",
                        ))
                    }
                    Err(e) => break Err(e),
                }
            }
        }
    };
    *buf = reader.buf;
    result
}

/// Reads the body that follows `head` for a request made with `method`,
/// keeping at most `max_body` bytes. A 2xx answer to CONNECT has no body:
/// the connection has become the tunnel.
pub async fn read_body<R: AsyncRead + Unpin>(
    stream: &mut R,
    buf: Vec<u8>,
    head: &Head,
    method: &str,
    max_body: usize,
) -> io::Result<Vec<u8>> {
    let status = head.status;
    let no_body = (method.eq_ignore_ascii_case("CONNECT") && (200..300).contains(&status))
        || method.eq_ignore_ascii_case("HEAD")
        || (100..200).contains(&status)
        || status == 204
        || status == 304;
    if no_body {
        return Ok(Vec::new());
    }
    let mut reader = Reader { stream, buf };

    let chunked = head
        .header("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    if chunked {
        let mut body = Vec::new();
        while body.len() < max_body {
            let line = reader.line().await?;
            let size_text = String::from_utf8_lossy(&line);
            let size_text = size_text.split(';').next().unwrap_or_default().trim();
            let size =
                usize::from_str_radix(size_text, 16).map_err(|_| bad("malformed chunk size"))?;
            if size == 0 {
                break;
            }
            let chunk = reader.take(size).await?;
            reader.line().await?;
            body.extend_from_slice(&chunk);
        }
        body.truncate(max_body);
        return Ok(body);
    }

    if let Some(length) = head.header("content-length") {
        let length: usize = length
            .trim()
            .parse()
            .map_err(|_| bad("malformed Content-Length"))?;
        return reader.take(length.min(max_body)).await;
    }

    // Delimited by the end of the connection.
    while reader.buf.len() < max_body {
        if !reader.fill().await? {
            break;
        }
    }
    reader.buf.truncate(max_body);
    Ok(reader.buf)
}

/// Reads one complete response; see [`read_body`] for the body rules.
pub async fn read_response<R: AsyncRead + Unpin>(
    stream: &mut R,
    method: &str,
    max_body: usize,
) -> io::Result<Response> {
    let mut buf = Vec::new();
    let head = read_head(stream, &mut buf).await?;
    let body = read_body(stream, buf, &head, method, max_body).await?;
    Ok(Response {
        status: head.status,
        protocol: head.protocol,
        headers: head.headers,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read(raw: &str, method: &str, max: usize) -> io::Result<Response> {
        let mut input = raw.as_bytes();
        read_response(&mut input, method, max).await
    }

    #[tokio::test]
    async fn content_length_body() {
        let r = read(
            "HTTP/1.1 403 Forbidden\r\nContent-Length: 6\r\n\r\ndenied",
            "GET",
            1024,
        )
        .await
        .unwrap();
        assert_eq!(
            (r.status, r.protocol.as_str(), r.body.as_slice()),
            (403, "HTTP/1.1", b"denied".as_slice())
        );
    }

    #[tokio::test]
    async fn chunked_body_and_limit() {
        let raw =
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n";
        assert_eq!(read(raw, "GET", 1024).await.unwrap().body, b"abcde");
        assert_eq!(read(raw, "GET", 4).await.unwrap().body, b"abcd");
    }

    #[tokio::test]
    async fn close_delimited_and_connect() {
        let r = read("HTTP/1.0 200 OK\r\n\r\nrest of stream", "GET", 4)
            .await
            .unwrap();
        assert_eq!(
            (r.protocol.as_str(), r.body.as_slice()),
            ("HTTP/1.0", b"rest".as_slice())
        );
        let r = read(
            "HTTP/1.1 200 Connection established\r\n\r\ntunnel bytes",
            "CONNECT",
            1024,
        )
        .await
        .unwrap();
        assert!(r.body.is_empty());
    }

    #[tokio::test]
    async fn truncated_head_fails() {
        assert!(read("HTTP/1.1 200 OK\r\nContent-", "GET", 10)
            .await
            .is_err());
        assert!(read("garbage\r\n\r\n", "GET", 10).await.is_err());
    }
}
