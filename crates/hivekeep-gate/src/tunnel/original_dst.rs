//! The destination an Actor connection was headed to before the sandbox
//! firewall redirected it to the tunnel.

use std::io;

use crate::tunnel::egress::Io;

/// The original `ip:port` of a redirected TCP connection.
pub fn tcp_original_destination(conn: &dyn Io) -> io::Result<String> {
    let any: &dyn std::any::Any = conn;
    let Some(tcp) = any.downcast_ref::<tokio::net::TcpStream>() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a TCP connection",
        ));
    };
    original_dst(tcp)
}

#[cfg(target_os = "linux")]
fn original_dst(tcp: &tokio::net::TcpStream) -> io::Result<String> {
    use std::os::fd::AsRawFd;
    const SO_ORIGINAL_DST: libc::c_int = 80;
    let mut addr: libc::sockaddr_in = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    // SAFETY: `addr` and `len` describe a writable sockaddr_in buffer.
    let rc = unsafe {
        libc::getsockopt(
            tcp.as_raw_fd(),
            libc::SOL_IP,
            SO_ORIGINAL_DST,
            &mut addr as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let ip = std::net::Ipv4Addr::from(u32::from_be(addr.sin_addr.s_addr));
    Ok(format!("{ip}:{}", u16::from_be(addr.sin_port)))
}

#[cfg(not(target_os = "linux"))]
fn original_dst(_: &tokio::net::TcpStream) -> io::Result<String> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "original destinations are only available on Linux",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_non_tcp_connections() {
        let (a, _b) = tokio::io::duplex(8);
        assert_eq!(
            tcp_original_destination(&a).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
