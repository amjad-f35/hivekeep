//! nf_tables over netlink: the expressions, chains and batches the sandbox
//! firewall is built from, and the firewall itself.
//!
//! The sandbox gateway namespace gets, per address family, a `forward` chain
//! that drops everything (nothing in a sandbox namespace routes) and a
//! raw-priority `disallowed-egress` prerouting chain that rejects link-local
//! destinations; IPv4 additionally rejects non-TCP traffic leaving the
//! Actor's /30 and redirects every other TCP connection to the tunnel's
//! egress port, keeping its original destination for `SO_ORIGINAL_DST`.

use std::io;
use std::net::IpAddr;

use ipnet::IpNet;

use super::netlink::{Message, NLM_F_ACK, NLM_F_APPEND, NLM_F_CREATE, NLM_F_REQUEST};

/// `NFNL_SUBSYS_NFTABLES`.
const SUBSYS_NFTABLES: u16 = 10;
/// `NFNL_MSG_BATCH_BEGIN` / `_END`.
const MSG_BATCH_BEGIN: u16 = 0x10;
const MSG_BATCH_END: u16 = 0x11;

/// nf_tables message types.
pub const MSG_NEWTABLE: u16 = 0;
/// `NFT_MSG_NEWCHAIN`.
pub const MSG_NEWCHAIN: u16 = 3;
/// `NFT_MSG_GETCHAIN`.
pub const MSG_GETCHAIN: u16 = 4;
/// `NFT_MSG_DELCHAIN`.
pub const MSG_DELCHAIN: u16 = 5;
/// `NFT_MSG_NEWRULE`.
pub const MSG_NEWRULE: u16 = 6;
/// `NFT_MSG_GETRULE`.
pub const MSG_GETRULE: u16 = 7;
/// `NFT_MSG_DELRULE`.
pub const MSG_DELRULE: u16 = 8;

/// The full netlink type of nf_tables message `msg`.
pub fn msg_type(msg: u16) -> u16 {
    (SUBSYS_NFTABLES << 8) | msg
}

/// A table address family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    /// `NFPROTO_IPV4`.
    Ipv4,
    /// `NFPROTO_IPV6`.
    Ipv6,
}

impl Family {
    /// The `NFPROTO_*` value.
    pub fn proto(self) -> u8 {
        match self {
            Family::Ipv4 => 2,
            Family::Ipv6 => 10,
        }
    }

    /// The family of an `NFPROTO_*` value.
    pub fn from_proto(p: u8) -> Option<Family> {
        match p {
            2 => Some(Family::Ipv4),
            10 => Some(Family::Ipv6),
            _ => None,
        }
    }
}

/// Netfilter hooks.
pub mod hook {
    /// `NF_INET_PRE_ROUTING`.
    pub const PREROUTING: u32 = 0;
    /// `NF_INET_FORWARD`.
    pub const FORWARD: u32 = 2;
}

/// Chain priorities.
pub mod priority {
    /// `NF_IP_PRI_RAW`.
    pub const RAW: i32 = -300;
    /// `NF_IP_PRI_NAT_DST`.
    pub const NAT_DEST: i32 = -100;
    /// `NF_IP_PRI_FILTER`.
    pub const FILTER: i32 = 0;
}

/// Verdicts.
pub mod verdict {
    /// `NF_DROP`.
    pub const DROP: i32 = 0;
    /// `NF_ACCEPT`.
    pub const ACCEPT: i32 = 1;
}

const NFT_REG_VERDICT: u32 = 0;
const NFT_REG_1: u32 = 1;
const NFT_META_L4PROTO: u32 = 16;
const NFT_PAYLOAD_NETWORK_HEADER: u32 = 1;
const NFT_CMP_EQ: u32 = 0;
const NFT_CMP_NEQ: u32 = 1;
const NFT_REJECT_ICMP_UNREACH: u32 = 0;
const IPPROTO_TCP: u8 = 6;

/// ICMP "net unreachable".
pub const ICMP_NET_UNREACHABLE: u8 = 0;
/// ICMP "communication administratively prohibited".
pub const ICMP_ADMIN_PROHIBITED: u8 = 13;
/// ICMPv6 "communication with destination administratively prohibited".
pub const ICMPV6_ADMIN_PROHIBITED: u8 = 1;

/// One rule expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expr {
    /// Loads the layer-4 protocol into register 1.
    MetaL4Proto,
    /// Compares register 1 with `data`.
    Cmp {
        /// Equal (`true`) or not equal.
        eq: bool,
        /// The value.
        data: Vec<u8>,
    },
    /// Loads `len` bytes at `offset` of the network header into register 1.
    Payload {
        /// Byte offset.
        offset: u32,
        /// Length.
        len: u32,
    },
    /// Register 1 &= `mask` (xor zero).
    Mask(Vec<u8>),
    /// Loads `data` into register 1.
    Immediate(Vec<u8>),
    /// Ends evaluation with a verdict.
    Verdict(i32),
    /// Redirects to the port in register 1.
    Redirect,
    /// Rejects with an ICMP unreachable `code`.
    Reject(u8),
}

fn data_value(m: &mut Message, ty: u16, data: &[u8]) {
    m.begin(ty);
    m.put(1, data); // NFTA_DATA_VALUE
    m.end();
}

impl Expr {
    fn name(&self) -> &'static str {
        match self {
            Expr::MetaL4Proto => "meta",
            Expr::Cmp { .. } => "cmp",
            Expr::Payload { .. } => "payload",
            Expr::Mask(_) => "bitwise",
            Expr::Immediate(_) | Expr::Verdict(_) => "immediate",
            Expr::Redirect => "redir",
            Expr::Reject(_) => "reject",
        }
    }

    fn encode(&self, m: &mut Message) {
        m.begin(1); // NFTA_LIST_ELEM
        m.put_str(1, self.name()); // NFTA_EXPR_NAME
        m.begin(2); // NFTA_EXPR_DATA
        match self {
            Expr::MetaL4Proto => {
                m.put_be32(2, NFT_META_L4PROTO); // NFTA_META_KEY
                m.put_be32(1, NFT_REG_1); // NFTA_META_DREG
            }
            Expr::Cmp { eq, data } => {
                m.put_be32(1, NFT_REG_1); // NFTA_CMP_SREG
                m.put_be32(2, if *eq { NFT_CMP_EQ } else { NFT_CMP_NEQ }); // NFTA_CMP_OP
                data_value(m, 3, data); // NFTA_CMP_DATA
            }
            Expr::Payload { offset, len } => {
                m.put_be32(1, NFT_REG_1); // NFTA_PAYLOAD_DREG
                m.put_be32(2, NFT_PAYLOAD_NETWORK_HEADER); // NFTA_PAYLOAD_BASE
                m.put_be32(3, *offset); // NFTA_PAYLOAD_OFFSET
                m.put_be32(4, *len); // NFTA_PAYLOAD_LEN
            }
            Expr::Mask(mask) => {
                m.put_be32(1, NFT_REG_1); // NFTA_BITWISE_SREG
                m.put_be32(2, NFT_REG_1); // NFTA_BITWISE_DREG
                m.put_be32(3, mask.len() as u32); // NFTA_BITWISE_LEN
                data_value(m, 4, mask); // NFTA_BITWISE_MASK
                data_value(m, 5, &vec![0; mask.len()]); // NFTA_BITWISE_XOR
            }
            Expr::Immediate(data) => {
                m.put_be32(1, NFT_REG_1); // NFTA_IMMEDIATE_DREG
                data_value(m, 2, data); // NFTA_IMMEDIATE_DATA
            }
            Expr::Verdict(code) => {
                m.put_be32(1, NFT_REG_VERDICT);
                m.begin(2); // NFTA_IMMEDIATE_DATA
                m.begin(2); // NFTA_DATA_VERDICT
                m.put_be32(1, *code as u32); // NFTA_VERDICT_CODE
                m.end();
                m.end();
            }
            Expr::Redirect => {
                m.put_be32(1, NFT_REG_1); // NFTA_REDIR_REG_PROTO_MIN
            }
            Expr::Reject(code) => {
                m.put_be32(1, NFT_REJECT_ICMP_UNREACH); // NFTA_REJECT_TYPE
                m.put_u8(2, *code); // NFTA_REJECT_ICMP_CODE
            }
        }
        m.end();
        m.end();
    }
}

/// A base chain's hook.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hook {
    /// `"filter"` or `"nat"`.
    pub kind: &'static str,
    /// The hook number.
    pub hooknum: u32,
    /// The priority.
    pub priority: i32,
    /// The default verdict, when not accept.
    pub policy: Option<i32>,
}

/// A batch of nf_tables changes, applied atomically.
#[derive(Debug, Default, Clone)]
pub struct Batch {
    msgs: Vec<Message>,
}

fn nfgenmsg(family: u8, res_id: u16) -> [u8; 4] {
    let r = res_id.to_be_bytes();
    [family, 0, r[0], r[1]]
}

impl Batch {
    /// An empty batch.
    pub fn new() -> Batch {
        Batch::default()
    }

    fn msg(&mut self, family: Family, ty: u16, flags: u16) -> &mut Message {
        self.msgs.push(
            Message::new(msg_type(ty), NLM_F_REQUEST | NLM_F_ACK | flags)
                .header(&nfgenmsg(family.proto(), 0)),
        );
        self.msgs.last_mut().unwrap()
    }

    /// Adds a table.
    pub fn add_table(&mut self, family: Family, table: &str) -> &mut Batch {
        let m = self.msg(family, MSG_NEWTABLE, NLM_F_CREATE);
        m.put_str(1, table); // NFTA_TABLE_NAME
        m.put_be32(2, 0); // NFTA_TABLE_FLAGS
        self
    }

    /// Adds a base chain.
    pub fn add_chain(
        &mut self,
        family: Family,
        table: &str,
        chain: &str,
        hook: Hook,
    ) -> &mut Batch {
        let m = self.msg(family, MSG_NEWCHAIN, NLM_F_CREATE);
        m.put_str(1, table); // NFTA_CHAIN_TABLE
        m.put_str(3, chain); // NFTA_CHAIN_NAME
        m.begin(4); // NFTA_CHAIN_HOOK
        m.put_be32(1, hook.hooknum); // NFTA_HOOK_HOOKNUM
        m.put_be32(2, hook.priority as u32); // NFTA_HOOK_PRIORITY
        m.end();
        if let Some(p) = hook.policy {
            m.put_be32(5, p as u32); // NFTA_CHAIN_POLICY
        }
        m.put_str(7, hook.kind); // NFTA_CHAIN_TYPE
        self
    }

    /// Appends a rule to a chain.
    pub fn add_rule(
        &mut self,
        family: Family,
        table: &str,
        chain: &str,
        exprs: &[Expr],
    ) -> &mut Batch {
        let m = self.msg(family, MSG_NEWRULE, NLM_F_CREATE | NLM_F_APPEND);
        m.put_str(1, table); // NFTA_RULE_TABLE
        m.put_str(2, chain); // NFTA_RULE_CHAIN
        m.begin(4); // NFTA_RULE_EXPRESSIONS
        for e in exprs {
            e.encode(m);
        }
        m.end();
        self
    }

    /// Deletes every rule of a chain.
    pub fn flush_chain(&mut self, family: Family, table: &str, chain: &str) -> &mut Batch {
        let m = self.msg(family, MSG_DELRULE, 0);
        m.put_str(1, table);
        m.put_str(2, chain);
        self
    }

    /// Deletes an empty chain.
    pub fn del_chain(&mut self, family: Family, table: &str, chain: &str) -> &mut Batch {
        let m = self.msg(family, MSG_DELCHAIN, 0);
        m.put_str(1, table);
        m.put_str(3, chain);
        self
    }

    /// How many changes the batch holds.
    pub fn len(&self) -> usize {
        self.msgs.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.msgs.is_empty()
    }

    /// The batch on the wire, framed by begin and end markers, and the
    /// sequence numbers that must be acknowledged.
    pub fn encode(self, first_seq: u32) -> (Vec<u8>, Vec<u32>) {
        let frame = |ty, seq| {
            Message::new(ty, NLM_F_REQUEST)
                .header(&nfgenmsg(0, SUBSYS_NFTABLES))
                .finish(seq)
        };
        let mut out = frame(MSG_BATCH_BEGIN, first_seq);
        let mut acks = Vec::with_capacity(self.msgs.len());
        let mut seq = first_seq;
        for m in self.msgs {
            seq += 1;
            out.extend_from_slice(&m.finish(seq));
            acks.push(seq);
        }
        out.extend_from_slice(&frame(MSG_BATCH_END, seq + 1));
        (out, acks)
    }
}

/// The IPv4 table of the sandbox firewall.
pub const TABLE_V4: &str = "worker-actor";
/// The IPv6 table of the sandbox firewall.
pub const TABLE_V6: &str = "worker-actor-v6";
/// The chain dropping routed traffic.
pub const FORWARD_CHAIN: &str = "forward";
/// The chain rejecting traffic the tunnel never sees.
pub const DISALLOWED_EGRESS_CHAIN: &str = "disallowed-egress";
/// The chain redirecting TCP to the tunnel.
pub const PREROUTING_CHAIN: &str = "prerouting";

/// Destinations an Actor may never reach: all link-local space. The
/// Actor's own /30 is accepted ahead of these, since it carries ingress
/// and the DNS relay.
pub fn link_local_ranges() -> [IpNet; 2] {
    [
        "169.254.0.0/16".parse().unwrap(),
        "fe80::/10".parse().unwrap(),
    ]
}

fn prefix_mask(net: &IpNet) -> (Vec<u8>, Vec<u8>) {
    match (net.addr(), net.netmask()) {
        (IpAddr::V4(a), IpAddr::V4(m)) => (a.octets().to_vec(), m.octets().to_vec()),
        (IpAddr::V6(a), IpAddr::V6(m)) => (a.octets().to_vec(), m.octets().to_vec()),
        _ => unreachable!("an IpNet's address and mask share a family"),
    }
}

/// The sandbox firewall for an Actor whose /30 is `actor_subnet`, with
/// captured TCP redirected to `egress_port`.
pub fn egress_rules(actor_subnet: IpNet, egress_port: u16) -> io::Result<Batch> {
    if egress_port == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the tunnel egress port is required",
        ));
    }
    let IpNet::V4(_) = actor_subnet else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the Actor subnet must be IPv4",
        ));
    };
    let (subnet_base, subnet_mask) = prefix_mask(&actor_subnet);
    // Offsets of the destination address in the IPv4 / IPv6 headers.
    const V4_DST: u32 = 16;
    const V6_DST: u32 = 24;
    let in_subnet = |eq: bool| {
        vec![
            Expr::Payload {
                offset: V4_DST,
                len: 4,
            },
            Expr::Mask(subnet_mask.clone()),
            Expr::Cmp {
                eq,
                data: subnet_base.clone(),
            },
        ]
    };

    let mut b = Batch::new();
    for (family, table, dst, len, reject) in [
        (Family::Ipv4, TABLE_V4, V4_DST, 4u32, ICMP_ADMIN_PROHIBITED),
        (
            Family::Ipv6,
            TABLE_V6,
            V6_DST,
            16u32,
            ICMPV6_ADMIN_PROHIBITED,
        ),
    ] {
        b.add_table(family, table);
        b.add_chain(
            family,
            table,
            FORWARD_CHAIN,
            Hook {
                kind: "filter",
                hooknum: hook::FORWARD,
                priority: priority::FILTER,
                policy: Some(verdict::DROP),
            },
        );
        b.add_chain(
            family,
            table,
            DISALLOWED_EGRESS_CHAIN,
            Hook {
                kind: "filter",
                hooknum: hook::PREROUTING,
                priority: priority::RAW,
                policy: None,
            },
        );
        if family == Family::Ipv4 {
            let mut accept = in_subnet(true);
            accept.push(Expr::Verdict(verdict::ACCEPT));
            b.add_rule(family, table, DISALLOWED_EGRESS_CHAIN, &accept);
        }
        for net in link_local_ranges() {
            if (family == Family::Ipv4) != matches!(net, IpNet::V4(_)) {
                continue;
            }
            let (addr, mask) = prefix_mask(&net);
            b.add_rule(
                family,
                table,
                DISALLOWED_EGRESS_CHAIN,
                &[
                    Expr::Payload { offset: dst, len },
                    Expr::Mask(mask),
                    Expr::Cmp {
                        eq: true,
                        data: addr,
                    },
                    Expr::Reject(reject),
                ],
            );
        }
        if family == Family::Ipv4 {
            // With forwarding off, unroutable packets vanish silently; reject
            // non-TCP leaving the /30 so UDP and ping fail fast.
            b.add_rule(
                family,
                table,
                DISALLOWED_EGRESS_CHAIN,
                &[
                    Expr::MetaL4Proto,
                    Expr::Cmp {
                        eq: false,
                        data: vec![IPPROTO_TCP],
                    },
                    Expr::Reject(ICMP_NET_UNREACHABLE),
                ],
            );
        }
    }

    b.add_chain(
        Family::Ipv4,
        TABLE_V4,
        PREROUTING_CHAIN,
        Hook {
            kind: "nat",
            hooknum: hook::PREROUTING,
            priority: priority::NAT_DEST,
            policy: None,
        },
    );
    let mut redirect = in_subnet(false);
    redirect.extend([
        Expr::MetaL4Proto,
        Expr::Cmp {
            eq: true,
            data: vec![IPPROTO_TCP],
        },
        Expr::Immediate(egress_port.to_be_bytes().to_vec()),
        Expr::Redirect,
    ]);
    b.add_rule(Family::Ipv4, TABLE_V4, PREROUTING_CHAIN, &redirect);
    Ok(b)
}

/// A chain as listed by the kernel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainInfo {
    /// The table's family.
    pub family: Option<Family>,
    /// The table.
    pub table: String,
    /// The chain.
    pub name: String,
    /// The hook, for base chains.
    pub hooknum: Option<u32>,
    /// The default verdict, for base chains.
    pub policy: Option<i32>,
}

/// Decodes one `NFT_MSG_NEWCHAIN` dump message's payload.
pub fn parse_chain(payload: &[u8]) -> Option<ChainInfo> {
    use super::netlink::{attr_str, attrs, find_attr};
    let family = Family::from_proto(*payload.first()?);
    let body = payload.get(4..)?;
    let mut c = ChainInfo {
        family,
        table: String::new(),
        name: String::new(),
        hooknum: None,
        policy: None,
    };
    let be32 = |d: &[u8]| {
        d.get(..4)
            .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
    };
    for (ty, data) in attrs(body) {
        match ty {
            1 => c.table = attr_str(data),
            3 => c.name = attr_str(data),
            4 => c.hooknum = find_attr(data, 1).and_then(be32),
            5 => c.policy = be32(data).map(|p| p as i32),
            _ => {}
        }
    }
    Some(c)
}

#[cfg(target_os = "linux")]
pub use imp::{apply, list_chains, rule_count};

#[cfg(target_os = "linux")]
mod imp {
    use std::io;

    use super::super::netlink::{Message, Socket, NETLINK_NETFILTER, NLM_F_DUMP, NLM_F_REQUEST};
    use super::{
        msg_type, nfgenmsg, parse_chain, Batch, ChainInfo, Family, MSG_GETCHAIN, MSG_GETRULE,
        MSG_NEWCHAIN, MSG_NEWRULE,
    };

    /// Applies `batch` in the calling thread's namespace.
    pub fn apply(batch: Batch, what: &str) -> io::Result<()> {
        let sock = Socket::open(NETLINK_NETFILTER)?;
        let (bytes, acks) = batch.encode(1);
        sock.transact(&bytes, &acks, what)
    }

    /// Every chain in the calling thread's namespace.
    pub fn list_chains() -> io::Result<Vec<ChainInfo>> {
        let sock = Socket::open(NETLINK_NETFILTER)?;
        let req = Message::new(msg_type(MSG_GETCHAIN), NLM_F_REQUEST | NLM_F_DUMP)
            .header(&nfgenmsg(0, 0))
            .finish(1);
        Ok(sock
            .dump(&req, "while listing nftables chains")?
            .into_iter()
            .filter(|r| r.ty == msg_type(MSG_NEWCHAIN))
            .filter_map(|r| parse_chain(&r.payload))
            .collect())
    }

    /// How many rules `table`/`chain` holds.
    pub fn rule_count(family: Family, table: &str, chain: &str) -> io::Result<usize> {
        let sock = Socket::open(NETLINK_NETFILTER)?;
        let mut m = Message::new(msg_type(MSG_GETRULE), NLM_F_REQUEST | NLM_F_DUMP)
            .header(&nfgenmsg(family.proto(), 0));
        m.put_str(1, table);
        m.put_str(2, chain);
        Ok(sock
            .dump(&m.finish(1), "while listing nftables rules")?
            .into_iter()
            .filter(|r| r.ty == msg_type(MSG_NEWRULE))
            .count())
    }
}

#[cfg(test)]
mod tests {
    use super::super::netlink::{attr_str, attrs, find_attr, parse_replies, NLA_F_NESTED};
    use super::*;

    fn subnet() -> IpNet {
        "169.254.17.0/30".parse().unwrap()
    }

    #[test]
    fn requires_a_port_and_an_ipv4_subnet() {
        assert!(egress_rules(subnet(), 0).is_err());
        assert!(egress_rules("fd00::/126".parse().unwrap(), 15001).is_err());
    }

    #[test]
    fn batch_is_framed_and_every_change_acknowledged() {
        let b = egress_rules(subnet(), 15001).unwrap();
        // 2 tables, 2x2 filter chains, 3 v4 + 1 v6 filter rules, nat chain and rule.
        assert_eq!(b.len(), 2 + 4 + 4 + 2);
        let (bytes, acks) = b.encode(1);
        let msgs = parse_replies(&bytes).unwrap();
        assert_eq!(msgs.len(), 14);
        assert_eq!(msgs[0].ty, MSG_BATCH_BEGIN);
        assert_eq!(msgs[13].ty, MSG_BATCH_END);
        assert_eq!(&msgs[0].payload, &[0, 0, 0, 10]);
        assert_eq!(acks, (2..=13).collect::<Vec<_>>());
        for m in &msgs[1..13] {
            assert_eq!(m.ty >> 8, SUBSYS_NFTABLES);
            assert_ne!(m.flags & NLM_F_ACK, 0);
        }
        let seqs: Vec<_> = msgs.iter().map(|m| m.seq).collect();
        assert_eq!(seqs, (1..=14).collect::<Vec<_>>());
    }

    #[test]
    fn chains_carry_hooks_priorities_and_policies() {
        let (bytes, _) = egress_rules(subnet(), 15001).unwrap().encode(1);
        let chains: Vec<_> = parse_replies(&bytes)
            .unwrap()
            .into_iter()
            .filter(|m| m.ty == msg_type(MSG_NEWCHAIN))
            .collect();
        assert_eq!(chains.len(), 5);
        let info: Vec<_> = chains
            .iter()
            .map(|m| parse_chain(&m.payload).unwrap())
            .collect();
        let forward: Vec<_> = info.iter().filter(|c| c.name == FORWARD_CHAIN).collect();
        assert_eq!(forward.len(), 2);
        for c in forward {
            assert_eq!(c.hooknum, Some(hook::FORWARD));
            assert_eq!(c.policy, Some(verdict::DROP));
        }
        let v4_disallowed = info
            .iter()
            .find(|c| c.name == DISALLOWED_EGRESS_CHAIN && c.family == Some(Family::Ipv4))
            .unwrap();
        assert_eq!(v4_disallowed.table, TABLE_V4);
        assert_eq!(v4_disallowed.policy, None);
        // Priorities are signed, sent in network order.
        let raw = chains
            .iter()
            .find(|m| parse_chain(&m.payload).unwrap().name == DISALLOWED_EGRESS_CHAIN)
            .unwrap();
        let hook_attr = find_attr(&raw.payload[4..], 4).unwrap();
        assert_eq!(find_attr(hook_attr, 2), Some(&(-300i32).to_be_bytes()[..]));
        let nat = chains
            .iter()
            .find(|m| parse_chain(&m.payload).unwrap().name == PREROUTING_CHAIN)
            .unwrap();
        assert_eq!(attr_str(find_attr(&nat.payload[4..], 7).unwrap()), "nat");
    }

    fn rule_exprs(payload: &[u8]) -> Vec<(String, Vec<u8>)> {
        let list = find_attr(&payload[4..], 4).unwrap();
        attrs(list)
            .map(|(_, elem)| {
                (
                    attr_str(find_attr(elem, 1).unwrap()),
                    find_attr(elem, 2).unwrap().to_vec(),
                )
            })
            .collect()
    }

    #[test]
    fn disallowed_egress_holds_three_ipv4_rules_and_the_redirect_keeps_the_port() {
        let (bytes, _) = egress_rules(subnet(), 15001).unwrap().encode(1);
        let rules: Vec<_> = parse_replies(&bytes)
            .unwrap()
            .into_iter()
            .filter(|m| m.ty == msg_type(MSG_NEWRULE))
            .collect();
        let chain_of = |m: &super::super::netlink::Reply| {
            (
                m.payload[0],
                attr_str(find_attr(&m.payload[4..], 2).unwrap()),
            )
        };
        let v4_disallowed = rules
            .iter()
            .filter(|m| chain_of(m) == (2, DISALLOWED_EGRESS_CHAIN.to_string()))
            .count();
        assert_eq!(v4_disallowed, 3);
        let v6_disallowed = rules
            .iter()
            .filter(|m| chain_of(m) == (10, DISALLOWED_EGRESS_CHAIN.to_string()))
            .count();
        assert_eq!(v6_disallowed, 1);

        // The first IPv4 rule accepts the Actor's own /30.
        let first = rule_exprs(&rules[0].payload);
        let names: Vec<_> = first.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["payload", "bitwise", "cmp", "immediate"]);
        let cmp_data = find_attr(&first[2].1, 3).unwrap();
        assert_eq!(find_attr(cmp_data, 1), Some(&[169, 254, 17, 0][..]));

        let redirect = rules.last().unwrap();
        assert_eq!(chain_of(redirect), (2, PREROUTING_CHAIN.to_string()));
        let exprs = rule_exprs(&redirect.payload);
        let names: Vec<_> = exprs.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(
            names,
            [
                "payload",
                "bitwise",
                "cmp",
                "meta",
                "cmp",
                "immediate",
                "redir"
            ]
        );
        // Outside the subnet, TCP, to port 15001.
        assert_eq!(find_attr(&exprs[2].1, 2), Some(&1u32.to_be_bytes()[..]));
        let imm = find_attr(&exprs[5].1, 2).unwrap();
        assert_eq!(find_attr(imm, 1), Some(&15001u16.to_be_bytes()[..]));
    }

    #[test]
    fn nests_are_flagged() {
        let mut b = Batch::new();
        b.add_rule(Family::Ipv4, "t", "c", &[Expr::Verdict(verdict::ACCEPT)]);
        let (bytes, _) = b.encode(1);
        let m = &parse_replies(&bytes).unwrap()[1];
        let body = &m.payload[4..];
        // table, chain, then the expressions nest.
        let mut off = 0;
        for _ in 0..2 {
            let len = u16::from_ne_bytes([body[off], body[off + 1]]) as usize;
            off += super::super::netlink::align(len);
        }
        let ty = u16::from_ne_bytes([body[off + 2], body[off + 3]]);
        assert_eq!(ty, 4 | NLA_F_NESTED);
    }

    #[test]
    fn deletes_address_tables_and_chains_by_name() {
        let mut b = Batch::new();
        b.flush_chain(Family::Ipv4, TABLE_V4, DISALLOWED_EGRESS_CHAIN)
            .del_chain(Family::Ipv4, TABLE_V4, DISALLOWED_EGRESS_CHAIN);
        let (bytes, acks) = b.encode(1);
        assert_eq!(acks.len(), 2);
        let msgs = parse_replies(&bytes).unwrap();
        assert_eq!(msgs[1].ty, msg_type(MSG_DELRULE));
        assert_eq!(msgs[2].ty, msg_type(MSG_DELCHAIN));
        assert_eq!(
            attr_str(find_attr(&msgs[2].payload[4..], 3).unwrap()),
            DISALLOWED_EGRESS_CHAIN
        );
    }
}
