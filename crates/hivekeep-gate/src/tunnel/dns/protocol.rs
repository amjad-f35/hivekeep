//! The slice of the DNS wire format the relay needs: header fields, the
//! question section, synthesized error and NXDOMAIN replies, and TCP
//! length-prefixed framing.
//!
//! The relay forwards the bytes it receives; it only ever parses the header
//! and the first question, so there is no full message model here.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Length of the fixed DNS header.
pub const HEADER_LEN: usize = 12;

/// Record types the relay looks at.
pub mod rtype {
    /// IPv4 address.
    pub const A: u16 = 1;
    /// Start of authority.
    pub const SOA: u16 = 6;
    /// IPv6 address.
    pub const AAAA: u16 = 28;
    /// Incremental zone transfer.
    pub const IXFR: u16 = 251;
    /// Full zone transfer.
    pub const AXFR: u16 = 252;
}

/// The Internet class.
pub const CLASS_INET: u16 = 1;

/// Response codes the relay produces or reacts to.
pub mod rcode {
    /// No error.
    pub const SUCCESS: u8 = 0;
    /// The query could not be parsed.
    pub const FORMAT_ERROR: u8 = 1;
    /// The server failed.
    pub const SERVER_FAILURE: u8 = 2;
    /// The name does not exist.
    pub const NAME_ERROR: u8 = 3;
    /// The kind of query is not supported.
    pub const NOT_IMPLEMENTED: u8 = 4;
    /// The server refuses to answer.
    pub const REFUSED: u8 = 5;
}

/// The decoded fixed header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Header {
    /// Transaction ID.
    pub id: u16,
    /// QR: set on responses.
    pub response: bool,
    /// Operation code (4 bits).
    pub opcode: u8,
    /// AA bit.
    pub authoritative: bool,
    /// TC bit.
    pub truncated: bool,
    /// RD bit.
    pub recursion_desired: bool,
    /// RA bit.
    pub recursion_available: bool,
    /// Response code (4 bits).
    pub rcode: u8,
}

impl Header {
    /// Decodes the header of `raw`, which must be at least [`HEADER_LEN`]
    /// bytes.
    pub fn parse(raw: &[u8]) -> Option<Header> {
        if raw.len() < HEADER_LEN {
            return None;
        }
        Some(Header {
            id: u16::from_be_bytes([raw[0], raw[1]]),
            response: raw[2] & 0x80 != 0,
            opcode: (raw[2] >> 3) & 0x0f,
            authoritative: raw[2] & 0x04 != 0,
            truncated: raw[2] & 0x02 != 0,
            recursion_desired: raw[2] & 0x01 != 0,
            recursion_available: raw[3] & 0x80 != 0,
            rcode: raw[3] & 0x0f,
        })
    }

    fn flags(&self) -> [u8; 2] {
        let mut b2 = (self.opcode & 0x0f) << 3;
        if self.response {
            b2 |= 0x80;
        }
        if self.authoritative {
            b2 |= 0x04;
        }
        if self.truncated {
            b2 |= 0x02;
        }
        if self.recursion_desired {
            b2 |= 0x01;
        }
        let mut b3 = self.rcode & 0x0f;
        if self.recursion_available {
            b3 |= 0x80;
        }
        [b2, b3]
    }
}

/// One question. The name is in presentation form with a trailing dot
/// (`example.com.`), as raw bytes since labels may hold any octet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Question {
    /// The queried name, dot-terminated.
    pub name: Vec<u8>,
    /// QTYPE.
    pub qtype: u16,
    /// QCLASS.
    pub qclass: u16,
}

impl Question {
    /// A question for `name` (presentation form, dot-terminated).
    pub fn new(name: &str, qtype: u16, qclass: u16) -> Self {
        Question {
            name: name.as_bytes().to_vec(),
            qtype,
            qclass,
        }
    }

    /// The name as text (non-UTF-8 bytes replaced).
    pub fn name_str(&self) -> String {
        String::from_utf8_lossy(&self.name).into_owned()
    }

    /// The question with ASCII letters of the name folded to lowercase, so
    /// equal names in different case compare equal.
    pub fn canonical(&self) -> Question {
        Question {
            name: self.name.to_ascii_lowercase(),
            ..self.clone()
        }
    }
}

/// Whether `raw` looks like a query: a full header with QR clear.
pub fn request_sanity_check(raw: &[u8]) -> bool {
    raw.len() >= HEADER_LEN && raw[2] & 0x80 == 0
}

/// Whether `raw` looks like a response to one question: a full header, QR
/// set and QDCOUNT 1.
pub fn response_sanity_check(raw: &[u8]) -> bool {
    raw.len() >= HEADER_LEN && raw[2] & 0x80 != 0 && qd_count(raw) == 1
}

/// Overwrites the transaction ID.
pub fn set_txn_id(raw: &mut [u8], id: u16) {
    raw[0..2].copy_from_slice(&id.to_be_bytes());
}

/// QDCOUNT of a message with a full header.
pub fn qd_count(raw: &[u8]) -> u16 {
    u16::from_be_bytes([raw[4], raw[5]])
}

const MAX_NAME_LEN: usize = 255;
const MAX_LABEL_LEN: usize = 63;
const MAX_POINTERS: usize = 10;

/// Reads a possibly compressed name at `off`, returning it in presentation
/// form and the offset just past it in the original position.
fn read_name(raw: &[u8], mut off: usize) -> Option<(Vec<u8>, usize)> {
    let mut name = Vec::new();
    let mut end = None;
    let mut wire_len = 0;
    let mut pointers = 0;
    loop {
        let len = *raw.get(off)? as usize;
        match len & 0xc0 {
            0x00 => {
                off += 1;
                if len == 0 {
                    if name.is_empty() {
                        name.push(b'.');
                    }
                    return Some((name, end.unwrap_or(off)));
                }
                let label = raw.get(off..off + len)?;
                wire_len += len + 1;
                if wire_len + 1 > MAX_NAME_LEN {
                    return None;
                }
                name.extend_from_slice(label);
                name.push(b'.');
                off += len;
            }
            0xc0 => {
                let lo = *raw.get(off + 1)? as usize;
                pointers += 1;
                if pointers > MAX_POINTERS {
                    return None;
                }
                end.get_or_insert(off + 2);
                off = ((len & 0x3f) << 8) | lo;
            }
            _ => return None,
        }
    }
}

/// Parses the first question, which starts right after the header.
pub fn first_question(raw: &[u8]) -> Option<Question> {
    if raw.len() < HEADER_LEN || qd_count(raw) == 0 {
        return None;
    }
    let (name, off) = read_name(raw, HEADER_LEN)?;
    let fixed = raw.get(off..off + 4)?;
    Some(Question {
        name,
        qtype: u16::from_be_bytes([fixed[0], fixed[1]]),
        qclass: u16::from_be_bytes([fixed[2], fixed[3]]),
    })
}

/// Encodes a dot-terminated presentation name, or `None` if it is not one.
fn encode_name(name: &[u8], out: &mut Vec<u8>) -> Option<()> {
    if name.last() != Some(&b'.') {
        return None;
    }
    if name == b"." {
        out.push(0);
        return Some(());
    }
    let body = &name[..name.len() - 1];
    let start = out.len();
    for label in body.split(|b| *b == b'.') {
        if label.is_empty() || label.len() > MAX_LABEL_LEN {
            out.truncate(start);
            return None;
        }
        out.push(label.len() as u8);
        out.extend_from_slice(label);
    }
    out.push(0);
    if out.len() - start > MAX_NAME_LEN {
        out.truncate(start);
        return None;
    }
    Some(())
}

fn encode_question(q: &Question, out: &mut Vec<u8>) -> Option<()> {
    encode_name(&q.name, out)?;
    out.extend_from_slice(&q.qtype.to_be_bytes());
    out.extend_from_slice(&q.qclass.to_be_bytes());
    Some(())
}

fn response_header(req: &Header, rcode: u8) -> Header {
    Header {
        id: req.id,
        response: true,
        opcode: req.opcode,
        recursion_desired: req.recursion_desired,
        recursion_available: true,
        rcode,
        ..Default::default()
    }
}

fn write_header(h: &Header, counts: [u16; 4], out: &mut Vec<u8>) {
    out.extend_from_slice(&h.id.to_be_bytes());
    out.extend_from_slice(&h.flags());
    for c in counts {
        out.extend_from_slice(&c.to_be_bytes());
    }
}

/// A reply to `req` with `rcode`, echoing the question when it is given
/// and encodable.
pub fn error_packet(req: &Header, rcode: u8, q: Option<&Question>) -> Vec<u8> {
    let mut question = Vec::new();
    let qd = q.and_then(|q| encode_question(q, &mut question)).is_some();
    let mut out = Vec::with_capacity(HEADER_LEN + question.len());
    write_header(
        &response_header(req, rcode),
        [u16::from(qd), 0, 0, 0],
        &mut out,
    );
    out.extend_from_slice(&question);
    out
}

/// An NXDOMAIN reply to `req` for `q`, carrying a root SOA with TTL and
/// negative-caching minimum 0 so resolvers do not cache the denial past a
/// policy change.
pub fn nxdomain_packet(req: &Header, q: &Question) -> Vec<u8> {
    let mut question = Vec::new();
    if encode_question(q, &mut question).is_none() {
        return error_packet(req, rcode::NAME_ERROR, Some(q));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + question.len() + 33);
    write_header(
        &response_header(req, rcode::NAME_ERROR),
        [1, 0, 1, 0],
        &mut out,
    );
    out.extend_from_slice(&question);
    // Owner ".", type SOA, class IN, TTL 0.
    out.push(0);
    out.extend_from_slice(&rtype::SOA.to_be_bytes());
    out.extend_from_slice(&CLASS_INET.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    // RDATA: MNAME ".", RNAME ".", serial/refresh/retry/expire/minimum 0.
    out.extend_from_slice(&22u16.to_be_bytes());
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&[0; 20]);
    out
}

/// Reads one length-prefixed TCP message. `Ok(None)` on a clean end of
/// stream before the prefix; a stream ending mid-message is
/// [`io::ErrorKind::UnexpectedEof`].
pub async fn read_tcp_frame<R: AsyncRead + Unpin + ?Sized>(
    r: &mut R,
) -> io::Result<Option<Vec<u8>>> {
    let mut len = [0u8; 2];
    if r.read(&mut len[..1]).await? == 0 {
        return Ok(None);
    }
    r.read_exact(&mut len[1..]).await?;
    let mut msg = vec![0; u16::from_be_bytes(len) as usize];
    r.read_exact(&mut msg).await?;
    Ok(Some(msg))
}

/// Writes one length-prefixed TCP message in a single write.
pub async fn write_tcp_frame<W: AsyncWrite + Unpin + ?Sized>(
    w: &mut W,
    msg: &[u8],
) -> io::Result<()> {
    let len = u16::try_from(msg.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "dns: TCP message length {} exceeds uint16 maximum",
                msg.len()
            ),
        )
    })?;
    let mut frame = Vec::with_capacity(2 + msg.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(msg);
    w.write_all(&frame).await
}

/// A parsed resource record, for tests and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Owner name.
    pub name: Vec<u8>,
    /// TYPE.
    pub rtype: u16,
    /// CLASS.
    pub class: u16,
    /// TTL.
    pub ttl: u32,
    /// Raw RDATA.
    pub rdata: Vec<u8>,
}

/// Every section of a message, for tests and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The header.
    pub header: Header,
    /// Questions.
    pub questions: Vec<Question>,
    /// Answers.
    pub answers: Vec<Record>,
    /// Authorities.
    pub authorities: Vec<Record>,
    /// Additional records.
    pub additionals: Vec<Record>,
}

impl Message {
    /// Parses a whole message.
    pub fn parse(raw: &[u8]) -> Option<Message> {
        let header = Header::parse(raw)?;
        let count = |i: usize| u16::from_be_bytes([raw[4 + 2 * i], raw[5 + 2 * i]]) as usize;
        let mut off = HEADER_LEN;
        let mut questions = Vec::new();
        for _ in 0..count(0) {
            let (name, next) = read_name(raw, off)?;
            let f = raw.get(next..next + 4)?;
            questions.push(Question {
                name,
                qtype: u16::from_be_bytes([f[0], f[1]]),
                qclass: u16::from_be_bytes([f[2], f[3]]),
            });
            off = next + 4;
        }
        let mut sections = [Vec::new(), Vec::new(), Vec::new()];
        for (i, section) in sections.iter_mut().enumerate() {
            for _ in 0..count(i + 1) {
                let (name, next) = read_name(raw, off)?;
                let f = raw.get(next..next + 10)?;
                let rdlen = u16::from_be_bytes([f[8], f[9]]) as usize;
                let rdata = raw.get(next + 10..next + 10 + rdlen)?.to_vec();
                section.push(Record {
                    name,
                    rtype: u16::from_be_bytes([f[0], f[1]]),
                    class: u16::from_be_bytes([f[2], f[3]]),
                    ttl: u32::from_be_bytes([f[4], f[5], f[6], f[7]]),
                    rdata,
                });
                off = next + 10 + rdlen;
            }
        }
        let [answers, authorities, additionals] = sections;
        Some(Message {
            header,
            questions,
            answers,
            authorities,
            additionals,
        })
    }
}

/// Builds a query for tests and probes.
pub fn query_packet(id: u16, q: &Question, recursion_desired: bool) -> Vec<u8> {
    let h = Header {
        id,
        recursion_desired,
        ..Default::default()
    };
    let mut out = Vec::new();
    write_header(&h, [1, 0, 0, 0], &mut out);
    encode_question(q, &mut out).expect("encodable question");
    out
}

/// Builds a response with an A record answer, for tests.
pub fn answer_packet(id: u16, q: &Question, rcode: u8, addr: Option<[u8; 4]>) -> Vec<u8> {
    let h = Header {
        id,
        response: true,
        recursion_desired: true,
        recursion_available: true,
        rcode,
        ..Default::default()
    };
    let mut out = Vec::new();
    write_header(&h, [1, u16::from(addr.is_some()), 0, 0], &mut out);
    encode_question(q, &mut out).expect("encodable question");
    if let Some(a) = addr {
        encode_name(&q.name, &mut out).expect("encodable name");
        out.extend_from_slice(&rtype::A.to_be_bytes());
        out.extend_from_slice(&CLASS_INET.to_be_bytes());
        out.extend_from_slice(&60u32.to_be_bytes());
        out.extend_from_slice(&4u16.to_be_bytes());
        out.extend_from_slice(&a);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(bytes: [u8; 4], qd: u16) -> Vec<u8> {
        let mut v = bytes.to_vec();
        v.extend_from_slice(&qd.to_be_bytes());
        v.extend_from_slice(&[0; 6]);
        v
    }

    #[test]
    fn request_sanity_check_cases() {
        let cases: Vec<(&str, Vec<u8>, bool)> = vec![
            ("nil slice", vec![], false),
            ("short packet (11 bytes)", vec![0; 11], false),
            ("valid 12-byte query (QR=0)", vec![0; 12], true),
            (
                "valid query with non-QR bits set in byte 2",
                hdr([0x12, 0x34, 0x7f, 0], 1),
                true,
            ),
            (
                "response packet (QR=1)",
                hdr([0x12, 0x34, 0x80, 0], 1),
                false,
            ),
        ];
        for (name, raw, want) in cases {
            assert_eq!(request_sanity_check(&raw), want, "{name}");
        }
        assert!(!request_sanity_check(&[]), "empty slice");
    }

    #[test]
    fn response_sanity_check_cases() {
        let mut valid = hdr([0x12, 0x34, 0x81, 0x80], 1);
        valid[7] = 1;
        let cases: Vec<(&str, Vec<u8>, bool)> = vec![
            ("nil slice", vec![], false),
            ("short packet (11 bytes)", vec![0; 11], false),
            (
                "query packet (QR=0) with QDCOUNT=1",
                hdr([0x12, 0x34, 0, 0], 1),
                false,
            ),
            (
                "response packet (QR=1) with QDCOUNT=0",
                hdr([0x12, 0x34, 0x80, 0], 0),
                false,
            ),
            (
                "response packet (QR=1) with QDCOUNT=2",
                hdr([0x12, 0x34, 0x80, 0], 2),
                false,
            ),
            ("valid response packet (QR=1, QDCOUNT=1)", valid, true),
        ];
        for (name, raw, want) in cases {
            assert_eq!(response_sanity_check(&raw), want, "{name}");
        }
    }

    #[test]
    fn qd_count_values() {
        for want in [0u16, 1, 2, 0x1234, 0xffff] {
            let mut raw = [0u8; 12];
            raw[4..6].copy_from_slice(&want.to_be_bytes());
            assert_eq!(qd_count(&raw), want);
        }
    }

    #[test]
    fn set_txn_id_writes_id() {
        let mut raw = [0u8; 12];
        set_txn_id(&mut raw, 0xcafe);
        assert_eq!(u16::from_be_bytes([raw[0], raw[1]]), 0xcafe);
    }

    #[test]
    fn error_packet_with_valid_question() {
        let q = Question::new("example.com.", rtype::A, CLASS_INET);
        let req = Header {
            id: 0xabcd,
            recursion_desired: true,
            ..Default::default()
        };
        let m = Message::parse(&error_packet(&req, rcode::REFUSED, Some(&q))).unwrap();
        assert_eq!(m.header.id, 0xabcd);
        assert!(m.header.response);
        assert_eq!(m.header.opcode, 0);
        assert!(m.header.recursion_desired);
        assert!(m.header.recursion_available);
        assert_eq!(m.header.rcode, rcode::REFUSED);
        assert_eq!(m.questions, vec![q]);
    }

    #[test]
    fn error_packet_with_no_question() {
        let req = Header {
            id: 0x1234,
            opcode: 1,
            ..Default::default()
        };
        let m = Message::parse(&error_packet(&req, rcode::NOT_IMPLEMENTED, None)).unwrap();
        assert_eq!(m.header.id, 0x1234);
        assert!(m.header.response);
        assert_eq!(m.header.opcode, 1);
        assert!(!m.header.recursion_desired);
        assert!(m.header.recursion_available);
        assert_eq!(m.header.rcode, rcode::NOT_IMPLEMENTED);
        assert!(m.questions.is_empty());
    }

    #[test]
    fn error_packet_with_unencodable_question_has_empty_question_section() {
        let bad = Question {
            name: vec![],
            qtype: rtype::A,
            qclass: CLASS_INET,
        };
        let req = Header {
            id: 0x4321,
            recursion_desired: true,
            ..Default::default()
        };
        let pkt = error_packet(&req, rcode::FORMAT_ERROR, Some(&bad));
        let h = Header::parse(&pkt).unwrap();
        assert_eq!(h.id, 0x4321);
        assert_eq!(h.rcode, rcode::FORMAT_ERROR);
        assert_eq!(qd_count(&pkt), 0);
    }

    #[test]
    fn nxdomain_packet_has_root_soa() {
        let q = Question::new("blocked.example.com.", rtype::A, CLASS_INET);
        let req = Header {
            id: 0xabcd,
            recursion_desired: true,
            ..Default::default()
        };
        let m = Message::parse(&nxdomain_packet(&req, &q)).unwrap();
        assert_eq!(m.header.id, 0xabcd);
        assert!(m.header.response && m.header.recursion_desired && m.header.recursion_available);
        assert_eq!(m.header.rcode, rcode::NAME_ERROR);
        assert_eq!(m.questions, vec![q]);
        assert!(m.answers.is_empty());
        assert_eq!(m.authorities.len(), 1);
        let soa = &m.authorities[0];
        assert_eq!(soa.name, b".");
        assert_eq!((soa.rtype, soa.class, soa.ttl), (rtype::SOA, CLASS_INET, 0));
        assert_eq!(
            &soa.rdata[soa.rdata.len() - 4..],
            &[0, 0, 0, 0],
            "SOA MINIMUM"
        );
    }

    #[test]
    fn nxdomain_packet_unencodable_question() {
        let bad = Question {
            name: vec![],
            qtype: rtype::A,
            qclass: CLASS_INET,
        };
        let pkt = nxdomain_packet(
            &Header {
                id: 0x4321,
                ..Default::default()
            },
            &bad,
        );
        let h = Header::parse(&pkt).unwrap();
        assert_eq!(h.id, 0x4321);
        assert_eq!(h.rcode, rcode::NAME_ERROR);
    }

    #[tokio::test]
    async fn tcp_frame_round_trip() {
        let payloads: Vec<Vec<u8>> = vec![
            vec![],
            vec![0x42],
            [0xab, 0xcd].repeat(256),
            vec![0xff; 0xffff],
        ];
        let mut buf = Vec::new();
        for p in &payloads {
            write_tcp_frame(&mut buf, p).await.unwrap();
        }
        let mut r = buf.as_slice();
        for want in &payloads {
            assert_eq!(read_tcp_frame(&mut r).await.unwrap().as_ref(), Some(want));
        }
        assert_eq!(read_tcp_frame(&mut r).await.unwrap(), None, "clean EOF");
    }

    #[tokio::test]
    async fn read_tcp_frame_errors() {
        let mut empty: &[u8] = &[];
        assert_eq!(read_tcp_frame(&mut empty).await.unwrap(), None);
        for (name, input) in [
            ("1-byte truncated length prefix", vec![0x00]),
            ("length prefix with empty body", vec![0x00, 0x05]),
            ("length prefix with partial body", vec![0x00, 0x05, 1, 2, 3]),
        ] {
            let mut r = input.as_slice();
            let err = read_tcp_frame(&mut r).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof, "{name}");
        }
    }

    struct FailingWriter;
    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            std::task::Poll::Ready(Err(io::Error::other("write failed")))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn write_tcp_frame_errors() {
        let mut buf = Vec::new();
        assert!(write_tcp_frame(&mut buf, &vec![0; 0x10000]).await.is_err());
        assert!(buf.is_empty());
        let err = write_tcp_frame(&mut FailingWriter, &[1, 2, 3])
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "write failed");
    }
}
