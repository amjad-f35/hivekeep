//! The destination a Linux `REDIRECT` rule rewrote, read back from the
//! accepted socket (`SO_ORIGINAL_DST`).

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// `IP6T_SO_ORIGINAL_DST` from `linux/netfilter_ipv6/ip6_tables.h`.
pub const IP6T_SO_ORIGINAL_DST: i32 = 80;
/// `SO_ORIGINAL_DST` from `linux/netfilter_ipv4.h`.
pub const SO_ORIGINAL_DST: i32 = 80;

/// Renders an address read from the kernel; `raw_port` is in network byte
/// order as the sockaddr holds it. A v4-mapped address renders as IPv4.
pub fn format_original_destination(ip: &[u8], raw_port: u16) -> io::Result<String> {
    let port = u16::from_be(raw_port);
    if port == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "original TCP destination has port zero",
        ));
    }
    let ip = match ip.len() {
        4 => IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])),
        16 => {
            let mut b = [0u8; 16];
            b.copy_from_slice(ip);
            let v6 = Ipv6Addr::from(b);
            v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4)
        }
        n => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("address of {n} bytes"),
            ))
        }
    };
    Ok(SocketAddr::new(ip, port).to_string())
}

/// The destination preserved by a `REDIRECT` rule for the accepted TCP
/// socket `fd` whose local address is `local`. The IPv6 option is only
/// meaningful on an IPv6 flow; a v4-mapped local address is an IPv4 flow.
#[cfg(target_os = "linux")]
pub fn tcp_original_destination(
    fd: std::os::fd::BorrowedFd<'_>,
    local: SocketAddr,
) -> io::Result<String> {
    use std::os::fd::AsRawFd;
    let ipv6 = match local.ip() {
        IpAddr::V4(_) => false,
        IpAddr::V6(v6) => v6.to_ipv4_mapped().is_none(),
    };
    let family = if ipv6 { "IPv6" } else { "IPv4" };
    let wrap = |e: io::Error| {
        io::Error::new(
            e.kind(),
            format!("reading original {family} TCP destination: {e}"),
        )
    };
    if ipv6 {
        // SAFETY: zeroed sockaddr_in6 of the exact size the kernel validates.
        let mut addr: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        // SAFETY: addr and len are valid for writes of len bytes.
        let rc = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_IPV6,
                IP6T_SO_ORIGINAL_DST,
                (&mut addr as *mut libc::sockaddr_in6).cast(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(wrap(io::Error::last_os_error()));
        }
        format_original_destination(&addr.sin6_addr.s6_addr, addr.sin6_port)
    } else {
        // SAFETY: zeroed sockaddr_in of the exact size the kernel validates.
        let mut addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
        let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
        // SAFETY: addr and len are valid for writes of len bytes.
        let rc = unsafe {
            libc::getsockopt(
                fd.as_raw_fd(),
                libc::SOL_IP,
                SO_ORIGINAL_DST,
                (&mut addr as *mut libc::sockaddr_in).cast(),
                &mut len,
            )
        };
        if rc != 0 {
            return Err(wrap(io::Error::last_os_error()));
        }
        format_original_destination(&addr.sin_addr.s_addr.to_ne_bytes(), addr.sin_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_original_destinations() {
        let v6 = |s: &str| s.parse::<Ipv6Addr>().unwrap().octets().to_vec();
        for (name, ip, port, want) in [
            ("IPv4", vec![198, 18, 0, 1], 443u16, Some("198.18.0.1:443")),
            (
                "IPv6 is bracketed",
                v6("fd00:198:18::1"),
                443,
                Some("[fd00:198:18::1]:443"),
            ),
            (
                "v4-mapped IPv6 renders as IPv4",
                v6("::ffff:198.18.0.1"),
                8080,
                Some("198.18.0.1:8080"),
            ),
            (
                "high port is not sign-extended",
                vec![198, 18, 0, 1],
                65535,
                Some("198.18.0.1:65535"),
            ),
            ("port zero is rejected", vec![198, 18, 0, 1], 0, None),
        ] {
            let got = format_original_destination(&ip, port.to_be()).ok();
            assert_eq!(got.as_deref(), want, "{name}");
        }
    }

    /// Without a REDIRECT the kernel has no original destination, and its
    /// ENOENT must come back as is.
    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "needs a network namespace without conntrack hooks (root)"]
    fn preserves_the_kernel_errno() {
        use std::os::fd::AsFd;
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let _c = std::net::TcpStream::connect(l.local_addr().unwrap()).unwrap();
        let (s, _) = l.accept().unwrap();
        let err = tcp_original_destination(s.as_fd(), s.local_addr().unwrap()).unwrap_err();
        assert_eq!(err.raw_os_error(), None);
        assert!(
            err.to_string().contains("original IPv4 TCP destination"),
            "{err}"
        );
        assert!(err.to_string().contains("No such file"), "{err}");
    }
}
