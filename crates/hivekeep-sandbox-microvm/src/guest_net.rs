//! The guest's network: the host tap the VMM backs its virtio-net with, and
//! the guest-side interface, routes and neighbor entry the agent installs.
//!
//! Both MACs are fixed rather than random: a snapshot freezes the guest's
//! interface and its ARP entry for the gateway, and a restore on another pod
//! must present the same ones or guest egress blackholes until the entry
//! expires.

use std::io;
use std::os::fd::OwnedFd;

use async_trait::async_trait;

use crate::agentpb::types as tpb;
use crate::kata::agent::{AgentClient, AgentError};
use hivekeep_sandbox_gvisor::wakeup_probe::{ACTOR_VETH_GATEWAY, ACTOR_VETH_IP};

/// The gateway's (the tap's) MAC.
pub const GATEWAY_MAC: &str = "02:a8:1e:00:00:01";
/// The guest eth0's MAC.
pub const ACTOR_GUEST_MAC: &str = "02:a8:1e:00:00:02";
/// The MTU the guest was snapshotted against: the kernel default.
pub const ACTOR_TAP_MTU: u32 = 1500;
/// The guest's interface.
pub const ACTOR_VETH_NAME: &str = "eth0";
/// The point-to-point subnet between the guest and the gateway.
pub const ACTOR_VETH_SUBNET: &str = "169.254.17.0/30";
/// The tap's name for the boot-time device.
pub const BOOT_TAP: &str = "tap0_kata";
/// `RT_SCOPE_LINK`.
const RT_SCOPE_LINK: u32 = 253;
/// `NUD_PERMANENT`.
const NUD_PERMANENT: i32 = 0x80;

/// Creates the taps the VMM adopts, in the Actor's network.
#[async_trait]
pub trait TapFactory: Send + Sync {
    /// Creates tap `name` with `queue_pairs` queues (one fd each), the
    /// gateway's fixed address and MAC, up.
    async fn create_tap(
        &self,
        actor_uid: &str,
        name: &str,
        queue_pairs: usize,
    ) -> io::Result<Vec<OwnedFd>>;
    /// The tap's MTU, falling back to [`ACTOR_TAP_MTU`].
    fn tap_mtu(&self, actor_uid: &str, name: &str) -> u32;
}

/// Taps in the worker's own network namespace. A deployment with per-Actor
/// namespaces plugs in a factory that creates them there.
#[derive(Debug, Default, Clone, Copy)]
pub struct HostTaps;

#[async_trait]
impl TapFactory for HostTaps {
    async fn create_tap(
        &self,
        _actor_uid: &str,
        name: &str,
        queue_pairs: usize,
    ) -> io::Result<Vec<OwnedFd>> {
        let name = name.to_string();
        tokio::task::spawn_blocking(move || imp::create_tap(&name, queue_pairs.max(1)))
            .await
            .map_err(io::Error::other)?
    }

    fn tap_mtu(&self, _actor_uid: &str, name: &str) -> u32 {
        imp::mtu(name).unwrap_or_else(|e| {
            tracing::warn!(link = name, default_mtu = ACTOR_TAP_MTU, err = %e, "Failed to read actor tap MTU; using default");
            ACTOR_TAP_MTU
        })
    }
}

/// Taps in each Actor's own network namespace, which the Actor's sandbox
/// network session created; the tunnel's sockets share that namespace.
#[derive(Debug, Default, Clone, Copy)]
pub struct NetnsTaps;

fn actor_netns(actor_uid: &str) -> io::Result<hivekeep_gate::netns::NetNs> {
    let name = hivekeep_sandbox_gvisor::node_path::actor_netns_name(actor_uid);
    hivekeep_gate::netns::get_from_name(&name).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("while opening the actor netns {name}: {e}"),
        )
    })
}

#[async_trait]
impl TapFactory for NetnsTaps {
    async fn create_tap(
        &self,
        actor_uid: &str,
        name: &str,
        queue_pairs: usize,
    ) -> io::Result<Vec<OwnedFd>> {
        let ns = actor_netns(actor_uid)?;
        let name = name.to_string();
        tokio::task::spawn_blocking(move || ns.run(|| imp::create_tap(&name, queue_pairs.max(1))))
            .await
            .map_err(io::Error::other)?
    }

    fn tap_mtu(&self, actor_uid: &str, name: &str) -> u32 {
        actor_netns(actor_uid)
            .and_then(|ns| ns.run(|| imp::mtu(name)))
            .unwrap_or_else(|e| {
                tracing::warn!(link = name, default_mtu = ACTOR_TAP_MTU, err = %e, "Failed to read actor tap MTU; using default");
                ACTOR_TAP_MTU
            })
    }
}

/// Parses `aa:bb:cc:dd:ee:ff`.
pub fn parse_mac(s: &str) -> Option<[u8; 6]> {
    let mut out = [0u8; 6];
    let mut parts = s.split(':');
    for b in &mut out {
        *b = u8::from_str_radix(parts.next()?, 16).ok()?;
    }
    parts.next().is_none().then_some(out)
}

#[cfg(target_os = "linux")]
mod imp {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd};

    const IFF_TAP: i16 = 0x0002;
    const IFF_NO_PI: i16 = 0x1000;
    const IFF_VNET_HDR: i16 = 0x4000;
    const IFF_MULTI_QUEUE: i16 = 0x0100;
    const TUNSETIFF: libc::c_ulong = 0x400454ca;

    #[repr(C)]
    struct IfReqShort {
        name: [u8; 16],
        value: i16,
        _pad: [u8; 22],
    }

    #[repr(C)]
    struct IfReqInt {
        name: [u8; 16],
        value: i32,
        _pad: [u8; 20],
    }

    #[repr(C)]
    struct IfReqAddr {
        name: [u8; 16],
        addr: libc::sockaddr,
        _pad: [u8; 8],
    }

    fn ifname(name: &str) -> io::Result<[u8; 16]> {
        let b = name.as_bytes();
        if b.is_empty() || b.len() >= 16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bad interface name {name:?}"),
            ));
        }
        let mut n = [0u8; 16];
        n[..b.len()].copy_from_slice(b);
        Ok(n)
    }

    fn ioctl<T>(fd: i32, req: libc::c_ulong, arg: &mut T) -> io::Result<()> {
        // SAFETY: arg is a properly sized ifreq for req.
        if unsafe { libc::ioctl(fd, req as _, arg as *mut T) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn ctl_socket() -> io::Result<OwnedFd> {
        // SAFETY: plain socket call.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the kernel just returned this descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }

    fn inet(a: [u8; 4]) -> libc::sockaddr {
        let sin = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 0,
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(a),
            },
            sin_zero: [0; 8],
        };
        // SAFETY: sockaddr_in and sockaddr have the same size.
        unsafe { std::mem::transmute::<libc::sockaddr_in, libc::sockaddr>(sin) }
    }

    pub fn create_tap(name: &str, queue_pairs: usize) -> io::Result<Vec<OwnedFd>> {
        let n = ifname(name)?;
        let mut flags = IFF_TAP | IFF_NO_PI | IFF_VNET_HDR;
        if queue_pairs > 1 {
            flags |= IFF_MULTI_QUEUE;
        }
        let mut fds = Vec::with_capacity(queue_pairs);
        for _ in 0..queue_pairs {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open("/dev/net/tun")?;
            let mut req = IfReqShort {
                name: n,
                value: flags,
                _pad: [0; 22],
            };
            ioctl(f.as_raw_fd(), TUNSETIFF, &mut req)
                .map_err(|e| io::Error::new(e.kind(), format!("creating tap {name:?}: {e}")))?;
            fds.push(OwnedFd::from(f));
        }
        let s = ctl_socket()?;
        let mut mtu = IfReqInt {
            name: n,
            value: ACTOR_TAP_MTU as i32,
            _pad: [0; 20],
        };
        ioctl(s.as_raw_fd(), libc::SIOCSIFMTU, &mut mtu)?;
        let mac = parse_mac(GATEWAY_MAC).expect("valid gateway MAC");
        let mut hw: libc::sockaddr = inet([0; 4]);
        hw.sa_family = libc::ARPHRD_ETHER;
        for (i, b) in mac.iter().enumerate() {
            hw.sa_data[i] = *b as libc::c_char;
        }
        let mut hwreq = IfReqAddr {
            name: n,
            addr: hw,
            _pad: [0; 8],
        };
        ioctl(s.as_raw_fd(), libc::SIOCSIFHWADDR, &mut hwreq).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("setting the gateway MAC on tap {name:?}: {e}"),
            )
        })?;
        let gw: std::net::Ipv4Addr = ACTOR_VETH_GATEWAY.parse().expect("valid gateway address");
        let mut addr = IfReqAddr {
            name: n,
            addr: inet(gw.octets()),
            _pad: [0; 8],
        };
        ioctl(s.as_raw_fd(), libc::SIOCSIFADDR, &mut addr).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("assigning the gateway address to tap {name:?}: {e}"),
            )
        })?;
        let mut mask = IfReqAddr {
            name: n,
            addr: inet([255, 255, 255, 252]),
            _pad: [0; 8],
        };
        ioctl(s.as_raw_fd(), libc::SIOCSIFNETMASK, &mut mask)?;
        let mut up = IfReqShort {
            name: n,
            value: 0,
            _pad: [0; 22],
        };
        ioctl(s.as_raw_fd(), libc::SIOCGIFFLAGS, &mut up)?;
        up.value |= libc::IFF_UP as i16;
        ioctl(s.as_raw_fd(), libc::SIOCSIFFLAGS, &mut up)
            .map_err(|e| io::Error::new(e.kind(), format!("bringing up tap {name:?}: {e}")))?;
        Ok(fds)
    }

    pub fn mtu(name: &str) -> io::Result<u32> {
        let s = ctl_socket()?;
        let mut req = IfReqInt {
            name: ifname(name)?,
            value: 0,
            _pad: [0; 20],
        };
        ioctl(s.as_raw_fd(), libc::SIOCGIFMTU, &mut req)?;
        Ok(req.value as u32)
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::*;

    pub fn create_tap(_name: &str, _queue_pairs: usize) -> io::Result<Vec<OwnedFd>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "tap devices are only supported on Linux",
        ))
    }

    pub fn mtu(_name: &str) -> io::Result<u32> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "tap devices are only supported on Linux",
        ))
    }
}

/// Configures the guest's network over the agent, as a kata shim would:
/// eth0's address, MAC and MTU, the connected and default routes, and the
/// gateway's neighbor entry pinned to its fixed MAC.
pub async fn configure_guest_network(ac: &AgentClient, mtu: u32) -> Result<(), AgentError> {
    ac.update_interface(tpb::Interface {
        device: ACTOR_VETH_NAME.into(),
        name: ACTOR_VETH_NAME.into(),
        hw_addr: ACTOR_GUEST_MAC.into(),
        mtu: mtu as u64,
        ip_addresses: vec![tpb::IpAddress {
            family: tpb::IpFamily::V4 as i32,
            address: ACTOR_VETH_IP.into(),
            mask: "30".into(),
        }],
        ..Default::default()
    })
    .await?;
    ac.update_routes(vec![
        tpb::Route {
            dest: ACTOR_VETH_SUBNET.into(),
            device: ACTOR_VETH_NAME.into(),
            scope: RT_SCOPE_LINK,
            family: tpb::IpFamily::V4 as i32,
            ..Default::default()
        },
        tpb::Route {
            gateway: ACTOR_VETH_GATEWAY.into(),
            device: ACTOR_VETH_NAME.into(),
            family: tpb::IpFamily::V4 as i32,
            ..Default::default()
        },
    ])
    .await?;
    ac.add_arp_neighbors(vec![tpb::ArpNeighbor {
        to_ip_address: Some(tpb::IpAddress {
            family: tpb::IpFamily::V4 as i32,
            address: ACTOR_VETH_GATEWAY.into(),
            mask: String::new(),
        }),
        device: ACTOR_VETH_NAME.into(),
        lladdr: GATEWAY_MAC.into(),
        state: NUD_PERMANENT,
        flags: 0,
    }])
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macs_parse() {
        assert_eq!(parse_mac(GATEWAY_MAC), Some([0x02, 0xa8, 0x1e, 0, 0, 1]));
        assert!(parse_mac(ACTOR_GUEST_MAC).is_some());
        assert_ne!(GATEWAY_MAC, ACTOR_GUEST_MAC);
        assert!(parse_mac("02:a8").is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    #[ignore = "needs root: creates tap devices"]
    async fn creates_a_tap_with_the_fixed_gateway() {
        let fds = HostTaps.create_tap("uid", "hktaptest0", 1).await.unwrap();
        assert_eq!(fds.len(), 1, "one descriptor per queue pair");
        assert_eq!(HostTaps.tap_mtu("uid", "hktaptest0"), ACTOR_TAP_MTU);
        let mac = std::fs::read_to_string("/sys/class/net/hktaptest0/address").unwrap();
        assert_eq!(mac.trim(), GATEWAY_MAC);
        let flags = std::fs::read_to_string("/sys/class/net/hktaptest0/flags").unwrap();
        assert!(
            u32::from_str_radix(flags.trim().trim_start_matches("0x"), 16).unwrap() & 1 != 0,
            "tap is down"
        );
    }
}
