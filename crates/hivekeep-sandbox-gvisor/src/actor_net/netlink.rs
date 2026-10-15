//! A minimal netlink codec: building request messages with nested
//! attributes, splitting replies, and (on Linux) a socket that sends a
//! request batch and collects its acknowledgements.
//!
//! The encoding is plain bytes so it is tested on every platform; only the
//! socket is Linux-specific. Headers and attribute headers are in host byte
//! order, as the kernel expects.

use std::io;

/// `NLM_F_REQUEST`.
pub const NLM_F_REQUEST: u16 = 0x1;
/// `NLM_F_MULTI`.
pub const NLM_F_MULTI: u16 = 0x2;
/// `NLM_F_ACK`.
pub const NLM_F_ACK: u16 = 0x4;
/// `NLM_F_REPLACE` (for new requests).
pub const NLM_F_REPLACE: u16 = 0x100;
/// `NLM_F_EXCL` (for new requests).
pub const NLM_F_EXCL: u16 = 0x200;
/// `NLM_F_CREATE` (for new requests).
pub const NLM_F_CREATE: u16 = 0x400;
/// `NLM_F_APPEND` (for new requests).
pub const NLM_F_APPEND: u16 = 0x800;
/// `NLM_F_DUMP` (for get requests).
pub const NLM_F_DUMP: u16 = 0x300;

/// `NLMSG_ERROR`: an error or, with code 0, an acknowledgement.
pub const NLMSG_ERROR: u16 = 2;
/// `NLMSG_DONE`: the end of a dump.
pub const NLMSG_DONE: u16 = 3;

/// `NLA_F_NESTED`: marks an attribute holding attributes.
pub const NLA_F_NESTED: u16 = 0x8000;
/// The attribute type bits, without the nested and byte-order flags.
pub const NLA_TYPE_MASK: u16 = 0x3fff;

/// `NETLINK_ROUTE`.
pub const NETLINK_ROUTE: i32 = 0;
/// `NETLINK_NETFILTER`.
pub const NETLINK_NETFILTER: i32 = 12;

const HEADER_LEN: usize = 16;

/// Rounds `n` up to the 4-byte netlink alignment.
pub fn align(n: usize) -> usize {
    (n + 3) & !3
}

/// One netlink message under construction.
#[derive(Debug, Clone)]
pub struct Message {
    buf: Vec<u8>,
    nests: Vec<usize>,
}

impl Message {
    /// A message of type `ty` with `flags`; length and sequence number are
    /// filled in by [`Message::finish`].
    pub fn new(ty: u16, flags: u16) -> Message {
        let mut buf = vec![0u8; HEADER_LEN];
        buf[4..6].copy_from_slice(&ty.to_ne_bytes());
        buf[6..8].copy_from_slice(&flags.to_ne_bytes());
        Message {
            buf,
            nests: Vec::new(),
        }
    }

    /// The message type.
    pub fn ty(&self) -> u16 {
        u16::from_ne_bytes([self.buf[4], self.buf[5]])
    }

    /// The message flags.
    pub fn flags(&self) -> u16 {
        u16::from_ne_bytes([self.buf[6], self.buf[7]])
    }

    /// Appends a family header (`ifinfomsg`, `nfgenmsg`, ...), padded.
    pub fn header(mut self, bytes: &[u8]) -> Message {
        self.buf.extend_from_slice(bytes);
        self.pad();
        self
    }

    fn pad(&mut self) {
        let n = align(self.buf.len());
        self.buf.resize(n, 0);
    }

    /// Appends attribute `ty` holding `data`.
    pub fn put(&mut self, ty: u16, data: &[u8]) -> &mut Message {
        let len = (4 + data.len()) as u16;
        self.buf.extend_from_slice(&len.to_ne_bytes());
        self.buf.extend_from_slice(&ty.to_ne_bytes());
        self.buf.extend_from_slice(data);
        self.pad();
        self
    }

    /// Appends a NUL-terminated string attribute.
    pub fn put_str(&mut self, ty: u16, s: &str) -> &mut Message {
        let mut data = Vec::with_capacity(s.len() + 1);
        data.extend_from_slice(s.as_bytes());
        data.push(0);
        self.put(ty, &data)
    }

    /// Appends a host-order `u32` attribute.
    pub fn put_u32(&mut self, ty: u16, v: u32) -> &mut Message {
        self.put(ty, &v.to_ne_bytes())
    }

    /// Appends a network-order `u32` attribute (netfilter's convention).
    pub fn put_be32(&mut self, ty: u16, v: u32) -> &mut Message {
        self.put(ty, &v.to_be_bytes())
    }

    /// Appends a one-byte attribute.
    pub fn put_u8(&mut self, ty: u16, v: u8) -> &mut Message {
        self.put(ty, &[v])
    }

    /// Opens a nested attribute; close it with [`Message::end`].
    pub fn begin(&mut self, ty: u16) -> &mut Message {
        self.nests.push(self.buf.len());
        self.buf.extend_from_slice(&0u16.to_ne_bytes());
        self.buf
            .extend_from_slice(&(ty | NLA_F_NESTED).to_ne_bytes());
        self
    }

    /// Appends raw bytes inside the open nest (a header nested in an
    /// attribute, as a veth peer's `ifinfomsg`), padded.
    pub fn raw(&mut self, bytes: &[u8]) -> &mut Message {
        self.buf.extend_from_slice(bytes);
        self.pad();
        self
    }

    /// Closes the innermost open nest.
    pub fn end(&mut self) -> &mut Message {
        let start = self.nests.pop().expect("netlink: end without begin");
        let len = (self.buf.len() - start) as u16;
        self.buf[start..start + 2].copy_from_slice(&len.to_ne_bytes());
        self
    }

    /// The encoded message with its length and sequence number `seq`.
    pub fn finish(mut self, seq: u32) -> Vec<u8> {
        assert!(self.nests.is_empty(), "netlink: unclosed nest");
        let len = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&len.to_ne_bytes());
        self.buf[8..12].copy_from_slice(&seq.to_ne_bytes());
        self.buf
    }
}

/// One message of a reply.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    /// Message type.
    pub ty: u16,
    /// Message flags.
    pub flags: u16,
    /// Sequence number of the request it answers.
    pub seq: u32,
    /// Everything after the header.
    pub payload: Vec<u8>,
}

impl Reply {
    /// For an `NLMSG_ERROR`, the kernel's code: 0 for an acknowledgement,
    /// otherwise a negated errno.
    pub fn error_code(&self) -> Option<i32> {
        if self.ty != NLMSG_ERROR || self.payload.len() < 4 {
            return None;
        }
        Some(i32::from_ne_bytes(self.payload[0..4].try_into().unwrap()))
    }
}

/// Splits a received buffer into messages.
pub fn parse_replies(mut buf: &[u8]) -> io::Result<Vec<Reply>> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "netlink: truncated message");
    let mut out = Vec::new();
    while buf.len() >= HEADER_LEN {
        let len = u32::from_ne_bytes(buf[0..4].try_into().unwrap()) as usize;
        if len < HEADER_LEN || len > buf.len() {
            return Err(bad());
        }
        out.push(Reply {
            ty: u16::from_ne_bytes([buf[4], buf[5]]),
            flags: u16::from_ne_bytes([buf[6], buf[7]]),
            seq: u32::from_ne_bytes(buf[8..12].try_into().unwrap()),
            payload: buf[HEADER_LEN..len].to_vec(),
        });
        buf = &buf[align(len).min(buf.len())..];
    }
    Ok(out)
}

/// Iterates the attributes in `buf` as `(type without flags, data)`.
pub fn attrs(mut buf: &[u8]) -> impl Iterator<Item = (u16, &[u8])> {
    std::iter::from_fn(move || {
        if buf.len() < 4 {
            return None;
        }
        let len = u16::from_ne_bytes([buf[0], buf[1]]) as usize;
        let ty = u16::from_ne_bytes([buf[2], buf[3]]) & NLA_TYPE_MASK;
        if len < 4 || len > buf.len() {
            return None;
        }
        let data = &buf[4..len];
        buf = &buf[align(len).min(buf.len())..];
        Some((ty, data))
    })
}

/// The first attribute of type `ty` in `buf`.
pub fn find_attr(buf: &[u8], ty: u16) -> Option<&[u8]> {
    attrs(buf).find(|(t, _)| *t == ty).map(|(_, d)| d)
}

/// A NUL-terminated string attribute's text.
pub fn attr_str(data: &[u8]) -> String {
    let end = data.iter().position(|b| *b == 0).unwrap_or(data.len());
    String::from_utf8_lossy(&data[..end]).into_owned()
}

/// The error a failed acknowledgement stands for.
pub fn ack_error(code: i32, what: &str) -> io::Error {
    let e = io::Error::from_raw_os_error(-code);
    io::Error::new(e.kind(), format!("{what}: {e}"))
}

#[cfg(target_os = "linux")]
pub use socket::Socket;

#[cfg(target_os = "linux")]
mod socket {
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    use super::{ack_error, parse_replies, Reply, NLMSG_DONE, NLMSG_ERROR};

    /// A netlink socket in the calling thread's network namespace.
    #[derive(Debug)]
    pub struct Socket {
        fd: OwnedFd,
    }

    impl Socket {
        /// Opens and binds a socket for `protocol`.
        pub fn open(protocol: i32) -> io::Result<Socket> {
            // SAFETY: plain socket(2); the descriptor is owned right away.
            let fd = unsafe {
                libc::socket(
                    libc::AF_NETLINK,
                    libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                    protocol,
                )
            };
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: `fd` was just returned by socket(2) and is not owned elsewhere.
            let fd = unsafe { OwnedFd::from_raw_fd(fd) };
            // SAFETY: a zeroed sockaddr_nl is valid; the kernel assigns the port.
            let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
            addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
            // SAFETY: `addr` is a valid sockaddr_nl of the given length.
            let rc = unsafe {
                libc::bind(
                    fd.as_raw_fd(),
                    &addr as *const libc::sockaddr_nl as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
                )
            };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Socket { fd })
        }

        fn send(&self, buf: &[u8]) -> io::Result<()> {
            // SAFETY: `buf` is a valid readable buffer of its length.
            let n = unsafe {
                libc::send(
                    self.fd.as_raw_fd(),
                    buf.as_ptr() as *const libc::c_void,
                    buf.len(),
                    0,
                )
            };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            if n as usize != buf.len() {
                return Err(io::Error::other("netlink: short send"));
            }
            Ok(())
        }

        fn recv(&self) -> io::Result<Vec<Reply>> {
            let mut buf = vec![0u8; 64 * 1024];
            loop {
                // SAFETY: `buf` is a valid writable buffer of its length.
                let n = unsafe {
                    libc::recv(
                        self.fd.as_raw_fd(),
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                        0,
                    )
                };
                if n < 0 {
                    let e = io::Error::last_os_error();
                    if e.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(e);
                }
                return parse_replies(&buf[..n as usize]);
            }
        }

        /// Sends `batch` (one or more encoded messages) and waits for an
        /// acknowledgement of each sequence number in `acks`. The first
        /// error the kernel reports is returned, described by `what`.
        pub fn transact(&self, batch: &[u8], acks: &[u32], what: &str) -> io::Result<()> {
            self.send(batch)?;
            let mut pending: Vec<u32> = acks.to_vec();
            while !pending.is_empty() {
                for r in self.recv()? {
                    if let Some(code) = r.error_code() {
                        if code != 0 {
                            return Err(ack_error(code, what));
                        }
                        pending.retain(|s| *s != r.seq);
                    }
                }
            }
            Ok(())
        }

        /// Sends a dump request and collects its messages until the end.
        pub fn dump(&self, request: &[u8], what: &str) -> io::Result<Vec<Reply>> {
            self.send(request)?;
            let mut out = Vec::new();
            loop {
                for r in self.recv()? {
                    match r.ty {
                        NLMSG_DONE => return Ok(out),
                        NLMSG_ERROR => match r.error_code() {
                            Some(0) | None => {}
                            Some(code) => return Err(ack_error(code, what)),
                        },
                        _ => out.push(r),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_headers_attributes_and_nests() {
        let mut m = Message::new(16, NLM_F_REQUEST | NLM_F_ACK).header(&[1, 2, 3]);
        m.put_str(3, "eth0");
        m.begin(18);
        m.put_u32(1, 7);
        m.end();
        let b = m.finish(9);
        assert_eq!(b.len() % 4, 0);
        assert_eq!(
            u32::from_ne_bytes(b[0..4].try_into().unwrap()) as usize,
            b.len()
        );
        assert_eq!(u16::from_ne_bytes([b[4], b[5]]), 16);
        assert_eq!(u16::from_ne_bytes([b[6], b[7]]), NLM_F_REQUEST | NLM_F_ACK);
        assert_eq!(u32::from_ne_bytes(b[8..12].try_into().unwrap()), 9);
        // Family header padded to 4.
        assert_eq!(&b[16..20], &[1, 2, 3, 0]);
        let got: Vec<_> = attrs(&b[20..]).collect();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], (3, &b"eth0\0"[..]));
        assert_eq!(got[1].0, 18);
        // The nested flag is set on the wire and masked when reading.
        assert_eq!(u16::from_ne_bytes([b[34], b[35]]), 18 | NLA_F_NESTED);
        assert_eq!(find_attr(got[1].1, 1), Some(&7u32.to_ne_bytes()[..]));
    }

    #[test]
    fn parses_replies_and_acks() {
        let mut ack = Message::new(NLMSG_ERROR, 0)
            .header(&0i32.to_ne_bytes())
            .finish(4);
        let err = Message::new(NLMSG_ERROR, 0)
            .header(&(-libc::EEXIST).to_ne_bytes())
            .finish(5);
        ack.extend_from_slice(&err);
        let r = parse_replies(&ack).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!((r[0].seq, r[0].error_code()), (4, Some(0)));
        assert_eq!((r[1].seq, r[1].error_code()), (5, Some(-libc::EEXIST)));
        assert_eq!(
            ack_error(-libc::EEXIST, "x").kind(),
            io::ErrorKind::AlreadyExists
        );
        assert!(parse_replies(&ack[..10]).unwrap().is_empty());
        let mut bad = ack.clone();
        bad[0] = 200;
        assert!(parse_replies(&bad).is_err());
    }

    #[test]
    fn reads_strings() {
        assert_eq!(attr_str(b"filter\0"), "filter");
        assert_eq!(attr_str(b"nat"), "nat");
    }
}
