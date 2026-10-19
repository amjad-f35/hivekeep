//! Shared output streams.
//!
//! Commands write to a [`Sink`] rather than to the process streams directly,
//! so tests can capture what a command prints and background tasks (a log
//! follower's monitor, a proxy's connections) can share one stream.

use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};

/// A cloneable, thread-safe writer.
#[derive(Clone)]
pub struct Sink(Arc<Mutex<Box<dyn Write + Send>>>);

impl Sink {
    /// Wraps `w`.
    pub fn new(w: impl Write + Send + 'static) -> Self {
        Sink(Arc::new(Mutex::new(Box::new(w))))
    }

    /// The process's standard output.
    pub fn stdout() -> Self {
        Sink::new(io::stdout())
    }

    /// The process's standard error.
    pub fn stderr() -> Self {
        Sink::new(io::stderr())
    }

    /// A sink that discards everything.
    pub fn null() -> Self {
        Sink::new(io::sink())
    }

    /// A sink writing to an in-memory buffer, and the buffer.
    pub fn buffer() -> (Self, SharedBuf) {
        let buf = SharedBuf::default();
        (Sink::new(buf.clone()), buf)
    }

    /// Writes `line` followed by a newline in one locked write, so lines from
    /// concurrent writers never interleave.
    pub fn line(&self, line: &str) {
        let mut w = self.0.lock().unwrap_or_else(|p| p.into_inner());
        let _ = w.write_all(line.as_bytes());
        let _ = w.write_all(b"\n");
        let _ = w.flush();
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).flush()
    }
}

/// An in-memory buffer shared between clones.
#[derive(Clone, Default)]
pub struct SharedBuf(Arc<Mutex<Vec<u8>>>);

impl SharedBuf {
    /// The bytes written so far, as text.
    pub fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|p| p.into_inner())).into_owned()
    }
}

impl Write for SharedBuf {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// The streams a command runs against.
pub struct Io {
    /// Command output.
    pub out: Sink,
    /// Diagnostics.
    pub err: Sink,
    /// Input, read when a manifest or token is given as `-`.
    pub stdin: Box<dyn Read + Send>,
}

impl Io {
    /// The process's own streams.
    pub fn process() -> Self {
        Io {
            out: Sink::stdout(),
            err: Sink::stderr(),
            stdin: Box::new(io::stdin()),
        }
    }

    /// In-memory streams for tests: empty stdin, captured stdout and stderr.
    pub fn captured() -> (Self, SharedBuf, SharedBuf) {
        Self::captured_with_stdin(Vec::new())
    }

    /// Like [`Io::captured`], with `stdin` as input.
    pub fn captured_with_stdin(stdin: Vec<u8>) -> (Self, SharedBuf, SharedBuf) {
        let (out, out_buf) = Sink::buffer();
        let (err, err_buf) = Sink::buffer();
        (
            Io {
                out,
                err,
                stdin: Box::new(io::Cursor::new(stdin)),
            },
            out_buf,
            err_buf,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_share_the_buffer() {
        let (mut sink, buf) = Sink::buffer();
        let other = sink.clone();
        write!(sink, "a").unwrap();
        other.line("b");
        assert_eq!(buf.contents(), "ab\n");
    }
}
