//! Route-netlink requests the sandbox network needs: a veth pair whose peer
//! is created straight in another namespace, bringing links up, IPv4
//! addresses and a default route.

use std::io;
use std::net::Ipv4Addr;

use super::netlink::{Message, NLM_F_ACK, NLM_F_CREATE, NLM_F_EXCL, NLM_F_REPLACE, NLM_F_REQUEST};

/// `RTM_NEWLINK`.
pub const RTM_NEWLINK: u16 = 16;
/// `RTM_NEWADDR`.
pub const RTM_NEWADDR: u16 = 20;
/// `RTM_NEWROUTE`.
pub const RTM_NEWROUTE: u16 = 24;

const IFLA_IFNAME: u16 = 3;
const IFLA_LINKINFO: u16 = 18;
const IFLA_NET_NS_FD: u16 = 28;
const IFLA_INFO_KIND: u16 = 1;
const IFLA_INFO_DATA: u16 = 2;
const VETH_INFO_PEER: u16 = 1;
const IFA_ADDRESS: u16 = 1;
const IFA_LOCAL: u16 = 2;
const RTA_DST: u16 = 1;
const RTA_OIF: u16 = 4;
const RTA_GATEWAY: u16 = 5;

const AF_INET: u8 = 2;
const IFF_UP: u32 = 0x1;
const RT_TABLE_MAIN: u8 = 254;
const RTPROT_BOOT: u8 = 3;
const RT_SCOPE_UNIVERSE: u8 = 0;
const RTN_UNICAST: u8 = 1;

/// The longest interface name the kernel takes.
pub const MAX_IFNAME_LEN: usize = 15;

/// An `ifinfomsg`.
pub fn ifinfomsg(index: i32, flags: u32, change: u32) -> [u8; 16] {
    let mut b = [0u8; 16];
    // family (AF_UNSPEC), pad, type stay zero.
    b[4..8].copy_from_slice(&index.to_ne_bytes());
    b[8..12].copy_from_slice(&flags.to_ne_bytes());
    b[12..16].copy_from_slice(&change.to_ne_bytes());
    b
}

fn check_name(name: &str) -> io::Result<()> {
    if name.is_empty() || name.len() > MAX_IFNAME_LEN || name.contains(['/', '\0', ' ']) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid interface name {name:?}"),
        ));
    }
    Ok(())
}

/// Creates veth `name` here and its peer `peer` in the namespace open on
/// descriptor `peer_netns_fd`.
pub fn new_veth(name: &str, peer: &str, peer_netns_fd: u32) -> io::Result<Message> {
    check_name(name)?;
    check_name(peer)?;
    let mut m = Message::new(
        RTM_NEWLINK,
        NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK,
    )
    .header(&ifinfomsg(0, 0, 0));
    m.put_str(IFLA_IFNAME, name);
    m.begin(IFLA_LINKINFO);
    m.put_str(IFLA_INFO_KIND, "veth");
    m.begin(IFLA_INFO_DATA);
    m.begin(VETH_INFO_PEER);
    m.raw(&ifinfomsg(0, 0, 0));
    m.put_str(IFLA_IFNAME, peer);
    m.put_u32(IFLA_NET_NS_FD, peer_netns_fd);
    m.end();
    m.end();
    m.end();
    Ok(m)
}

/// Brings link `index` up.
pub fn link_up(index: u32) -> Message {
    Message::new(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK).header(&ifinfomsg(
        index as i32,
        IFF_UP,
        IFF_UP,
    ))
}

/// Sets `addr/prefix` on link `index`, replacing an existing one.
pub fn replace_addr_v4(index: u32, addr: Ipv4Addr, prefix: u8) -> Message {
    let mut h = [0u8; 8];
    h[0] = AF_INET;
    h[1] = prefix;
    // flags, scope stay zero.
    h[4..8].copy_from_slice(&index.to_ne_bytes());
    let mut m = Message::new(
        RTM_NEWADDR,
        NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK,
    )
    .header(&h);
    m.put(IFA_LOCAL, &addr.octets());
    m.put(IFA_ADDRESS, &addr.octets());
    m
}

/// Routes everything via `gateway` out of link `index`, replacing an
/// existing default route.
pub fn replace_default_route_v4(index: u32, gateway: Ipv4Addr) -> Message {
    replace_route_v4(index, None, gateway)
}

/// Routes `dst` (address and prefix length; `None` for the default route)
/// via `gateway` out of link `index`, replacing an existing route.
pub fn replace_route_v4(index: u32, dst: Option<(Ipv4Addr, u8)>, gateway: Ipv4Addr) -> Message {
    // rtmsg: family, dst_len, src_len, tos, table, protocol, scope, type, flags.
    let mut h = [0u8; 12];
    h[0] = AF_INET;
    h[1] = dst.map_or(0, |(_, p)| p);
    h[4] = RT_TABLE_MAIN;
    h[5] = RTPROT_BOOT;
    h[6] = RT_SCOPE_UNIVERSE;
    h[7] = RTN_UNICAST;
    let mut m = Message::new(
        RTM_NEWROUTE,
        NLM_F_REQUEST | NLM_F_CREATE | NLM_F_REPLACE | NLM_F_ACK,
    )
    .header(&h);
    if let Some((d, _)) = dst {
        m.put(RTA_DST, &d.octets());
    }
    m.put(RTA_GATEWAY, &gateway.octets());
    m.put_u32(RTA_OIF, index);
    m
}

#[cfg(target_os = "linux")]
pub use imp::{index_of, interface_names, Route};

#[cfg(target_os = "linux")]
mod imp {
    use std::ffi::{CStr, CString};
    use std::io;
    use std::net::Ipv4Addr;

    use super::super::netlink::{Socket, NETLINK_ROUTE};

    /// The index of interface `name` in the calling thread's namespace.
    pub fn index_of(name: &str) -> io::Result<u32> {
        let c = CString::new(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: `c` is a valid NUL-terminated string.
        let i = unsafe { libc::if_nametoindex(c.as_ptr()) };
        if i == 0 {
            let e = io::Error::last_os_error();
            return Err(io::Error::new(e.kind(), format!("interface {name}: {e}")));
        }
        Ok(i)
    }

    /// The names of the interfaces in the calling thread's namespace.
    pub fn interface_names() -> io::Result<Vec<String>> {
        // SAFETY: if_nameindex returns an array terminated by a zero entry,
        // freed below.
        let list = unsafe { libc::if_nameindex() };
        if list.is_null() {
            return Err(io::Error::last_os_error());
        }
        let mut out = Vec::new();
        let mut p = list;
        // SAFETY: walks the array up to its terminator.
        unsafe {
            while (*p).if_index != 0 {
                out.push(CStr::from_ptr((*p).if_name).to_string_lossy().into_owned());
                p = p.add(1);
            }
            libc::if_freenameindex(list);
        }
        Ok(out)
    }

    /// Route-netlink requests made from the calling thread's namespace.
    #[derive(Debug)]
    pub struct Route {
        sock: Socket,
        seq: u32,
    }

    impl Route {
        /// Opens a route-netlink socket here.
        pub fn open() -> io::Result<Route> {
            Ok(Route {
                sock: Socket::open(NETLINK_ROUTE)?,
                seq: 0,
            })
        }

        fn request(&mut self, m: super::Message, what: &str) -> io::Result<()> {
            self.seq += 1;
            let seq = self.seq;
            self.sock.transact(&m.finish(seq), &[seq], what)
        }

        /// See [`super::new_veth`].
        pub fn add_veth(&mut self, name: &str, peer: &str, peer_netns_fd: i32) -> io::Result<()> {
            let m = super::new_veth(name, peer, peer_netns_fd as u32)?;
            self.request(m, "while creating the veth pair")
        }

        /// Brings `name` up.
        pub fn link_up(&mut self, name: &str) -> io::Result<()> {
            let i = index_of(name)?;
            self.request(super::link_up(i), &format!("while bringing up {name}"))
        }

        /// Sets an IPv4 address on `name`.
        pub fn replace_addr(&mut self, name: &str, addr: Ipv4Addr, prefix: u8) -> io::Result<()> {
            let i = index_of(name)?;
            self.request(
                super::replace_addr_v4(i, addr, prefix),
                &format!("while assigning {addr}/{prefix} to {name}"),
            )
        }

        /// Routes everything via `gateway` out of `name`.
        pub fn replace_default_route(&mut self, name: &str, gateway: Ipv4Addr) -> io::Result<()> {
            let i = index_of(name)?;
            self.request(
                super::replace_default_route_v4(i, gateway),
                &format!("while routing via {gateway} on {name}"),
            )
        }

        /// Routes `dst/prefix` via `gateway` out of `name`.
        pub fn replace_route(
            &mut self,
            name: &str,
            dst: Ipv4Addr,
            prefix: u8,
            gateway: Ipv4Addr,
        ) -> io::Result<()> {
            let i = index_of(name)?;
            self.request(
                super::replace_route_v4(i, Some((dst, prefix)), gateway),
                &format!("while routing {dst}/{prefix} on {name}"),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::netlink::{attr_str, attrs, find_attr};
    use super::*;

    #[test]
    fn veth_carries_the_peer_and_its_namespace() {
        let b = new_veth("gwside", "eth0", 42).unwrap().finish(1);
        assert_eq!(u16::from_ne_bytes([b[4], b[5]]), RTM_NEWLINK);
        let body = &b[16 + 16..];
        assert_eq!(attr_str(find_attr(body, IFLA_IFNAME).unwrap()), "gwside");
        let info = find_attr(body, IFLA_LINKINFO).unwrap();
        assert_eq!(attr_str(find_attr(info, IFLA_INFO_KIND).unwrap()), "veth");
        let data = find_attr(info, IFLA_INFO_DATA).unwrap();
        let peer = find_attr(data, VETH_INFO_PEER).unwrap();
        // The peer starts with its own ifinfomsg.
        assert_eq!(&peer[..16], &ifinfomsg(0, 0, 0));
        let peer_attrs: Vec<_> = attrs(&peer[16..]).collect();
        assert_eq!(peer_attrs[0], (IFLA_IFNAME, &b"eth0\0"[..]));
        assert_eq!(peer_attrs[1], (IFLA_NET_NS_FD, &42u32.to_ne_bytes()[..]));
    }

    #[test]
    fn rejects_bad_interface_names() {
        assert!(new_veth("", "eth0", 1).is_err());
        assert!(new_veth("a-very-long-ifname", "eth0", 1).is_err());
        assert!(new_veth("ok", "bad/name", 1).is_err());
    }

    #[test]
    fn link_up_sets_and_masks_the_up_flag() {
        let b = link_up(7).finish(1);
        assert_eq!(&b[16..32], &ifinfomsg(7, IFF_UP, IFF_UP));
        assert_eq!(b.len(), 32);
    }

    #[test]
    fn addresses_and_routes() {
        let b = replace_addr_v4(3, Ipv4Addr::new(169, 254, 17, 1), 30).finish(1);
        assert_eq!(b[16], AF_INET);
        assert_eq!(b[17], 30);
        assert_eq!(u32::from_ne_bytes(b[20..24].try_into().unwrap()), 3);
        assert_eq!(find_attr(&b[24..], IFA_LOCAL), Some(&[169, 254, 17, 1][..]));

        let r = replace_default_route_v4(4, Ipv4Addr::new(169, 254, 17, 1)).finish(1);
        assert_eq!(
            &r[16..24],
            &[AF_INET, 0, 0, 0, RT_TABLE_MAIN, RTPROT_BOOT, 0, RTN_UNICAST]
        );
        assert_eq!(
            find_attr(&r[28..], RTA_GATEWAY),
            Some(&[169, 254, 17, 1][..])
        );
        assert_eq!(find_attr(&r[28..], RTA_OIF), Some(&4u32.to_ne_bytes()[..]));
        assert_eq!(find_attr(&r[28..], RTA_DST), None);

        let r = replace_route_v4(
            4,
            Some((Ipv4Addr::new(169, 254, 17, 0), 30)),
            Ipv4Addr::new(10, 0, 0, 1),
        )
        .finish(1);
        assert_eq!(r[17], 30);
        assert_eq!(find_attr(&r[28..], RTA_DST), Some(&[169, 254, 17, 0][..]));
    }
}
