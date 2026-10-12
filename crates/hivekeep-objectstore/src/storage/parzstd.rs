//! Parallel zstd compression as independent, size-declaring frames.
//!
//! A streaming zstd encoder compresses one block at a time however many cores
//! it is given, which capped a snapshot upload at one core of compression
//! (about 190 MB/s of output on a cloud worker node, less than the parallel
//! part upload below it can take). [`ParZstd`] instead cuts the stream into
//! chunks and compresses each as its own frame on its own core.
//! Concatenated frames decode as one stream, so the output reads back
//! through a plain zstd reader and the snapshot format is unchanged.
//! Independent chunks cost a little ratio (no matches across a boundary),
//! which the chunk size keeps negligible.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// The largest per-frame chunk.
pub const PAR_ZSTD_CHUNK: i64 = 8 << 20;
/// The smallest per-frame chunk, covering a fast encoder's fixed tables,
/// block buffers and minimum history.
pub const PAR_ZSTD_MIN_CHUNK: i64 = 1 << 20;
/// Chunks outstanding per worker, so the producer can run ahead.
pub const PAR_ZSTD_QUEUE: i64 = 2;
/// The compression level: the fastest, since a guest memory image is mostly
/// zeros and higher levels scan it far slower for little gain.
pub const ZSTD_LEVEL: i32 = 1;

const CLOSED: &str = "parzstd: write after close";

/// The per-frame chunk for a sparse file with `populated` data bytes: a
/// power of two between [`PAR_ZSTD_MIN_CHUNK`] and [`PAR_ZSTD_CHUNK`].
pub fn par_zstd_chunk_size(populated: i64) -> i64 {
    let mut chunk = PAR_ZSTD_MIN_CHUNK;
    while chunk < PAR_ZSTD_CHUNK && chunk < populated {
        chunk <<= 1;
    }
    chunk
}

/// The memory a worker reserves for frames of `chunk` bytes: its queued
/// input chunks and their frames, the encoder's state, and the frame being
/// written.
pub fn par_zstd_worker_memory(chunk: i64) -> i64 {
    (PAR_ZSTD_QUEUE * 3 + 2) * chunk
}

/// The usable parallelism of this process.
pub fn max_procs() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// An encoder for one chunk at a time. Every frame declares its content
/// size, even a short tail, so a reader can size each frame without bounding
/// it; a chunk below 4 MiB also bounds the window to the chunk.
pub fn new_chunk_encoder(chunk: i64) -> zstd::bulk::Compressor<'static> {
    let mut c = zstd::bulk::Compressor::new(ZSTD_LEVEL).expect("zstd level is valid");
    // Parameters are fixed powers of two the library accepts.
    let _ = c.set_parameter(zstd::zstd_safe::CParameter::ContentSizeFlag(true));
    let _ = c.set_parameter(zstd::zstd_safe::CParameter::ChecksumFlag(false));
    if chunk > 0 && chunk < 4 << 20 {
        let window = chunk.max(PAR_ZSTD_MIN_CHUNK) as u64;
        let log = 63 - window.leading_zeros();
        let _ = c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(log));
    }
    c
}

/// Compresses `data` as one frame.
pub fn encode_frame(enc: &mut zstd::bulk::Compressor<'static>, data: &[u8]) -> io::Result<Vec<u8>> {
    enc.compress(data)
}

type JobResult = io::Result<(Vec<u8>, Vec<u8>)>;

struct Job {
    chunk: Vec<u8>,
    out: SyncSender<JobResult>,
}

struct Pool {
    jobs: Option<SyncSender<Job>>,
    pending: VecDeque<Receiver<JobResult>>,
    max_pending: usize,
    free: Vec<Vec<u8>>,
    threads: Vec<JoinHandle<()>>,
}

enum Mode {
    Inline(Box<zstd::bulk::Compressor<'static>>),
    Pool(Pool),
}

/// A writer compressing what it is given as independent zstd frames of at
/// most `chunk` input bytes, written to `dst` in order.
///
/// With one worker (inline mode) each full chunk is compressed on the
/// caller's thread; it suits callers that already run one encoder per
/// parallel unit of work, such as an upload part. With more, chunks go to a
/// pool of threads and frames are written as they complete, in order. A
/// failure writing `dst` is returned from the next write or from
/// [`close`](Self::close), so a failed upload stops reading its source.
pub struct ParZstd<W: Write> {
    dst: Option<W>,
    chunk: usize,
    buf: Vec<u8>,
    mode: Option<Mode>,
    err: Option<(io::ErrorKind, String)>,
}

impl<W: Write> ParZstd<W> {
    /// An encoder with up to `workers` threads (capped at the process's
    /// parallelism; zero or less asks for all of it) and full-size chunks.
    pub fn new(dst: W, workers: i64) -> Self {
        Self::with_chunk(dst, workers, PAR_ZSTD_CHUNK)
    }

    /// An encoder with chunks of `chunk` bytes (at most
    /// [`PAR_ZSTD_CHUNK`]).
    pub fn with_chunk(dst: W, workers: i64, chunk: i64) -> Self {
        let procs = max_procs() as i64;
        let workers = if workers <= 0 || workers > procs {
            procs
        } else {
            workers
        };
        let chunk = if chunk <= 0 || chunk > PAR_ZSTD_CHUNK {
            PAR_ZSTD_CHUNK
        } else {
            chunk
        } as usize;
        let mode = if workers == 1 {
            Mode::Inline(Box::new(new_chunk_encoder(chunk as i64)))
        } else {
            let (tx, rx) = mpsc::sync_channel::<Job>(workers as usize);
            let rx = Arc::new(Mutex::new(rx));
            let threads = (0..workers)
                .map(|_| {
                    let rx = rx.clone();
                    std::thread::spawn(move || {
                        let mut enc = new_chunk_encoder(chunk as i64);
                        loop {
                            let job = {
                                let guard = rx.lock().expect("job queue lock");
                                guard.recv()
                            };
                            let Ok(job) = job else { return };
                            let res = encode_frame(&mut enc, &job.chunk).map(|f| (f, job.chunk));
                            let _ = job.out.send(res);
                        }
                    })
                })
                .collect();
            Mode::Pool(Pool {
                jobs: Some(tx),
                pending: VecDeque::new(),
                max_pending: (workers * PAR_ZSTD_QUEUE) as usize,
                free: Vec::new(),
                threads,
            })
        };
        ParZstd {
            dst: Some(dst),
            chunk,
            buf: Vec::with_capacity(chunk),
            mode: Some(mode),
            err: None,
        }
    }

    /// Whether chunks are compressed on the caller's thread.
    pub fn is_inline(&self) -> bool {
        matches!(self.mode, Some(Mode::Inline(_)))
    }

    fn error(&self) -> Option<io::Error> {
        self.err
            .as_ref()
            .map(|(k, m)| io::Error::new(*k, m.clone()))
    }

    fn fail(&mut self, e: io::Error) -> io::Error {
        if self.err.is_none() {
            self.err = Some((e.kind(), e.to_string()));
        }
        e
    }

    fn write_frame(&mut self, frame: &[u8]) -> io::Result<()> {
        if let Some(e) = self.error() {
            return Err(e);
        }
        let dst = self.dst.as_mut().expect("destination is held until finish");
        if let Err(e) = dst.write_all(frame) {
            return Err(self.fail(e));
        }
        Ok(())
    }

    fn drain_one(&mut self) -> io::Result<()> {
        let rx = match &mut self.mode {
            Some(Mode::Pool(p)) => p.pending.pop_front(),
            _ => None,
        };
        let Some(rx) = rx else { return Ok(()) };
        match rx.recv() {
            Ok(Ok((frame, mut chunk))) => {
                chunk.clear();
                if let Some(Mode::Pool(p)) = &mut self.mode {
                    p.free.push(chunk);
                }
                self.write_frame(&frame)
            }
            Ok(Err(e)) => Err(self.fail(e)),
            Err(_) => Err(self.fail(io::Error::other("parzstd: worker exited"))),
        }
    }

    fn flush_chunk(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if let Some(e) = self.error() {
            return Err(e);
        }
        match &mut self.mode {
            Some(Mode::Inline(enc)) => {
                let frame = encode_frame(enc, &self.buf);
                self.buf.clear();
                match frame {
                    Ok(f) => self.write_frame(&f),
                    Err(e) => Err(self.fail(e)),
                }
            }
            Some(Mode::Pool(p)) => {
                let fresh = p
                    .free
                    .pop()
                    .unwrap_or_else(|| Vec::with_capacity(self.chunk));
                let chunk = std::mem::replace(&mut self.buf, fresh);
                let (tx, rx) = mpsc::sync_channel(1);
                p.pending.push_back(rx);
                let full = p.pending.len() >= p.max_pending;
                let sent = p
                    .jobs
                    .as_ref()
                    .map(|j| j.send(Job { chunk, out: tx }).is_ok())
                    .unwrap_or(false);
                if !sent {
                    return Err(self.fail(io::Error::other("parzstd: workers stopped")));
                }
                // Write whatever is done, and wait for the oldest when the
                // queue is full, which bounds the memory in flight.
                if full {
                    self.drain_one()?;
                }
                self.drain_ready()
            }
            None => Err(io::Error::other(CLOSED)),
        }
    }

    fn drain_ready(&mut self) -> io::Result<()> {
        loop {
            let ready = match &mut self.mode {
                Some(Mode::Pool(p)) => match p.pending.front() {
                    Some(rx) => match rx.try_recv() {
                        Ok(r) => {
                            p.pending.pop_front();
                            Some(r)
                        }
                        Err(_) => None,
                    },
                    None => None,
                },
                _ => None,
            };
            let Some(res) = ready else { return Ok(()) };
            match res {
                Ok((frame, mut chunk)) => {
                    chunk.clear();
                    if let Some(Mode::Pool(p)) = &mut self.mode {
                        p.free.push(chunk);
                    }
                    self.write_frame(&frame)?;
                }
                Err(e) => return Err(self.fail(e)),
            }
        }
    }

    /// Flushes the tail, stops the workers and reports the first error.
    /// Later calls to write or close fail.
    pub fn close(&mut self) -> io::Result<()> {
        let Some(_) = self.mode else {
            return Err(self.error().unwrap_or_else(|| io::Error::other(CLOSED)));
        };
        let mut res = match self.error() {
            Some(e) => Err(e),
            None => self.flush_chunk(),
        };
        let failed_before = self.err.is_some();
        if let Some(Mode::Pool(_)) = &self.mode {
            loop {
                let more = matches!(&self.mode, Some(Mode::Pool(p)) if !p.pending.is_empty());
                if !more {
                    break;
                }
                if let Err(e) = self.drain_one() {
                    if res.is_ok() {
                        res = Err(e);
                    }
                }
            }
        }
        if let Some(Mode::Pool(mut p)) = self.mode.take() {
            p.jobs.take();
            for t in p.threads.drain(..) {
                let _ = t.join();
            }
        }
        self.mode = None;
        if res.is_ok() {
            let flushed = self.dst.as_mut().map(|d| d.flush()).unwrap_or(Ok(()));
            if let Err(e) = flushed {
                res = Err(self.fail(e));
            }
        }
        if let Err(e) = &res {
            if self.err.is_none() {
                self.err = Some((e.kind(), e.to_string()));
            }
        }
        if res.is_ok() && !failed_before {
            self.err = Some((io::ErrorKind::Other, CLOSED.into()));
        }
        res
    }

    /// Closes the encoder and returns the destination.
    pub fn finish(mut self) -> io::Result<W> {
        self.close()?;
        Ok(self.dst.take().expect("destination is held until finish"))
    }

    /// The destination.
    pub fn get_mut(&mut self) -> &mut W {
        self.dst.as_mut().expect("destination is held until finish")
    }
}

impl<W: Write> Write for ParZstd<W> {
    fn write(&mut self, mut b: &[u8]) -> io::Result<usize> {
        if self.mode.is_none() {
            return Err(self.error().unwrap_or_else(|| io::Error::other(CLOSED)));
        }
        let total = b.len();
        while !b.is_empty() {
            if let Some(e) = self.error() {
                return Err(e);
            }
            let n = (self.chunk - self.buf.len()).min(b.len());
            self.buf.extend_from_slice(&b[..n]);
            b = &b[n..];
            if self.buf.len() == self.chunk {
                self.flush_chunk()?;
            }
        }
        Ok(total)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<W: Write> Drop for ParZstd<W> {
    fn drop(&mut self) {
        if let Some(Mode::Pool(mut p)) = self.mode.take() {
            p.jobs.take();
            p.pending.clear();
            for t in p.threads.drain(..) {
                let _ = t.join();
            }
        }
    }
}

/// Whether `e` is the error a closed encoder returns.
pub fn is_closed(e: &io::Error) -> bool {
    e.to_string().contains(CLOSED)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::storage::testdata::{
        random_bytes, write_in_pieces, zstd_decode_all, zstd_frame_stats,
    };

    const CHUNK: usize = PAR_ZSTD_CHUNK as usize;

    #[test]
    fn inline_writes_sized_frames() {
        for n in [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 3 * CHUNK + 4321] {
            let data = random_bytes(n as u64, n);
            let mut zw = ParZstd::new(Vec::new(), 1);
            assert!(zw.is_inline(), "ParZstd::new(w, 1) is not inline");
            write_in_pieces(&mut zw, &data);
            let out = zw.finish().unwrap();
            assert_eq!(zstd_decode_all(&out), data, "n={n}: round trip differs");
            let (frames, sized, max_size) = zstd_frame_stats(&out);
            assert_eq!(frames, n.div_ceil(CHUNK), "n={n}: frames");
            assert_eq!(
                sized, frames,
                "n={n}: {sized} of {frames} frames declare their size"
            );
            assert!(max_size <= CHUNK as u64, "n={n}: frame of {max_size} bytes");
        }
    }

    #[test]
    fn matches_chunked_encode_all() {
        let data = random_bytes(7, 2 * CHUNK + CHUNK / 2 + 123);
        let mut enc = new_chunk_encoder(PAR_ZSTD_CHUNK);
        let mut want = Vec::new();
        for c in data.chunks(CHUNK) {
            want.extend(encode_frame(&mut enc, c).unwrap());
        }
        for workers in [1, 2] {
            let mut zw = ParZstd::new(Vec::new(), workers);
            write_in_pieces(&mut zw, &data);
            let out = zw.finish().unwrap();
            assert!(
                out == want,
                "workers={workers}: output differs from chunked encode ({} vs {} bytes)",
                out.len(),
                want.len()
            );
        }
    }

    #[test]
    fn reused_buffers_do_not_leak_data() {
        for workers in [1i64, 2] {
            let upload = move |seed: u64, n: usize| {
                let data = random_bytes(seed, n);
                let mut zw = ParZstd::new(Vec::new(), workers);
                write_in_pieces(&mut zw, &data);
                let out = zw.finish().unwrap();
                assert_eq!(zstd_decode_all(&out), data, "workers={workers} seed {seed}");
            };
            upload(1, 3 * CHUNK + CHUNK / 2);
            upload(2, CHUNK + CHUNK / 4);
            upload(3, 100);
            let handles: Vec<_> = (0..6)
                .map(|i| std::thread::spawn(move || upload(10 + i as u64, (i + 1) * CHUNK / 2)))
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        }
    }

    pub(crate) struct FailAfter {
        pub n: usize,
        pub msg: &'static str,
    }

    impl Write for FailAfter {
        fn write(&mut self, p: &[u8]) -> io::Result<usize> {
            if self.n == 0 {
                return Err(io::Error::other(self.msg));
            }
            self.n -= 1;
            Ok(p.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn inline_reports_write_errors() {
        let mut zw = ParZstd::new(FailAfter { n: 1, msg: "boom" }, 1);
        let data = random_bytes(5, 3 * CHUNK);
        let mut werr = None;
        for c in data.chunks(1 << 20) {
            if let Err(e) = zw.write(c) {
                werr = Some(e);
                break;
            }
        }
        assert!(werr.unwrap().to_string().contains("boom"));
        assert!(zw.close().unwrap_err().to_string().contains("boom"));
        assert!(zw.close().unwrap_err().to_string().contains("boom"));
    }

    #[test]
    fn write_after_close() {
        for workers in [1i64, 2] {
            let mut zw = ParZstd::new(io::sink(), workers);
            assert_eq!(zw.is_inline(), workers == 1);
            zw.write_all(b"x").unwrap();
            zw.close().unwrap();
            assert!(is_closed(&zw.write(b"x").unwrap_err()), "workers={workers}");
            assert!(is_closed(&zw.close().unwrap_err()), "workers={workers}");
        }
    }

    #[test]
    fn chunk_sizes() {
        assert_eq!(par_zstd_chunk_size(0), PAR_ZSTD_MIN_CHUNK);
        assert_eq!(par_zstd_chunk_size(3 << 20), 4 << 20);
        assert_eq!(par_zstd_chunk_size(1 << 30), PAR_ZSTD_CHUNK);
    }
}
