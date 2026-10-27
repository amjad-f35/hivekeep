//! Just enough HTTP/1.1 to speak it over raw streams: inside CONNECT
//! tunnels, where the benchmark writes requests and reads responses by hand.

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt};

/// Largest request or response head accepted.
const MAX_HEAD: usize = 64 << 10;

/// A request or response head: the start line and the header fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Head {
    /// `GET / HTTP/1.1` or `HTTP/1.1 200 OK`.
    pub start: String,
    /// Header fields in order, names lowercased.
    pub headers: Vec<(String, String)>,
}

impl Head {
    /// The first value of header `name` (lowercase).
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    /// The response status code.
    pub fn status(&self) -> Option<u16> {
        let mut parts = self.start.split(' ');
        parts.next().filter(|v| v.starts_with("HTTP/"))?;
        parts.next()?.parse().ok()
    }

    /// The request method and target.
    pub fn request_line(&self) -> Option<(&str, &str)> {
        let mut parts = self.start.split(' ');
        let method = parts.next()?;
        let target = parts.next()?;
        parts.next().filter(|v| v.starts_with("HTTP/"))?;
        Some((method, target))
    }

    fn content_length(&self) -> io::Result<Option<u64>> {
        self.header("content-length")
            .map(|v| {
                v.trim()
                    .parse()
                    .map_err(|_| invalid("invalid Content-Length"))
            })
            .transpose()
    }

    fn chunked(&self) -> bool {
        self.header("transfer-encoding")
            .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"))
    }
}

fn invalid(m: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, m.to_string())
}

async fn read_line<R: AsyncBufRead + Unpin>(
    r: &mut R,
    budget: &mut usize,
) -> io::Result<Option<String>> {
    let mut line = Vec::new();
    let n = (&mut *r)
        .take(*budget as u64 + 1)
        .read_until(b'\n', &mut line)
        .await?;
    if n == 0 {
        return Ok(None);
    }
    if n > *budget {
        return Err(invalid("HTTP head too large"));
    }
    *budget -= n;
    if !line.ends_with(b"\n") {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated HTTP head",
        ));
    }
    while line.last().is_some_and(|b| *b == b'\n' || *b == b'\r') {
        line.pop();
    }
    String::from_utf8(line)
        .map(Some)
        .map_err(|_| invalid("HTTP head is not UTF-8"))
}

/// Reads one head. `Ok(None)` when the stream ends before its first byte.
pub async fn read_head<R: AsyncBufRead + Unpin>(r: &mut R) -> io::Result<Option<Head>> {
    let mut budget = MAX_HEAD;
    let Some(start) = read_line(r, &mut budget).await? else {
        return Ok(None);
    };
    let mut head = Head {
        start,
        headers: Vec::new(),
    };
    loop {
        let line = read_line(r, &mut budget)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated HTTP head"))?;
        if line.is_empty() {
            return Ok(Some(head));
        }
        let (k, v) = line
            .split_once(':')
            .ok_or_else(|| invalid("malformed header line"))?;
        head.headers
            .push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
    }
}

/// How a message's body is delimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyKind {
    /// A request: no body unless the head says so.
    Request,
    /// A response: without a length it runs to the end of the stream.
    Response,
}

/// Reads and discards the body that follows `head`, returning its length.
pub async fn discard_body<R: AsyncBufRead + Unpin>(
    r: &mut R,
    head: &Head,
    kind: BodyKind,
) -> io::Result<u64> {
    if head.chunked() {
        return discard_chunked(r).await;
    }
    match head.content_length()? {
        Some(n) => {
            let copied = tokio::io::copy(&mut (&mut *r).take(n), &mut tokio::io::sink()).await?;
            if copied != n {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated HTTP body",
                ));
            }
            Ok(n)
        }
        None if kind == BodyKind::Response => tokio::io::copy(r, &mut tokio::io::sink()).await,
        None => Ok(0),
    }
}

async fn discard_chunked<R: AsyncBufRead + Unpin>(r: &mut R) -> io::Result<u64> {
    let mut total = 0;
    loop {
        let mut budget = MAX_HEAD;
        let line = read_line(r, &mut budget)
            .await?
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated chunk"))?;
        let size = line.split(';').next().unwrap_or("").trim();
        let size = u64::from_str_radix(size, 16).map_err(|_| invalid("invalid chunk size"))?;
        if size == 0 {
            // Trailers, up to the empty line.
            while !read_line(r, &mut budget)
                .await?
                .unwrap_or_default()
                .is_empty()
            {}
            return Ok(total);
        }
        let copied = tokio::io::copy(&mut (&mut *r).take(size), &mut tokio::io::sink()).await?;
        if copied != size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated chunk",
            ));
        }
        total += size;
        read_line(r, &mut budget).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn reads_heads_and_bodies() {
        let input: &[u8] = b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nX-A: b\r\n\r\nabcHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nab\r\n1\r\nc\r\n0\r\n\r\nGET / HTTP/1.1\r\nHost: x\r\n\r\n";
        let mut r = BufReader::new(input);
        let h = read_head(&mut r).await.unwrap().unwrap();
        assert_eq!(h.status(), Some(200));
        assert_eq!(h.header("x-a"), Some("b"));
        assert_eq!(
            discard_body(&mut r, &h, BodyKind::Response).await.unwrap(),
            3
        );
        let h = read_head(&mut r).await.unwrap().unwrap();
        assert_eq!(
            discard_body(&mut r, &h, BodyKind::Response).await.unwrap(),
            3
        );
        let h = read_head(&mut r).await.unwrap().unwrap();
        assert_eq!(h.request_line(), Some(("GET", "/")));
        assert_eq!(
            discard_body(&mut r, &h, BodyKind::Request).await.unwrap(),
            0
        );
        assert_eq!(read_head(&mut r).await.unwrap(), None);
    }
}
