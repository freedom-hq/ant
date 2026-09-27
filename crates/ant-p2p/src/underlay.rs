//! Serialize / deserialize underlay multiaddrs (Bee `bzz.SerializeUnderlays`).
//!
//! Bee 2.8 caps the number and serialized size of advertised underlays
//! per peer (`maxUnderlaysPerPeer = 20`, `maxUnderlayBytes = 2048`) and
//! rejects out-of-bound payloads up-front — see
//! [pkg/bzz/transport.go](../bee/pkg/bzz/transport.go) and
//! [pkg/bzz/underlay.go](../bee/pkg/bzz/underlay.go). We mirror both
//! limits on send and receive so a misbehaving peer can't blow our
//! buffer and we don't get silently rejected once our own listen-set
//! grows beyond the cap.

use libp2p::core::multiaddr::Protocol;
use libp2p::multiaddr::{Error as MultiaddrError, Multiaddr};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use unsigned_varint::decode::Error as VarintError;

const UNDERLAY_LIST_PREFIX: u8 = 0x99;

/// Maximum number of underlay addresses we will pack into a single
/// serialized record. Matches bee 2.8 `maxUnderlaysPerPeer`.
pub const MAX_UNDERLAYS_PER_PEER: usize = 20;

/// Maximum size of the serialized underlay payload. Matches bee 2.8
/// `maxUnderlayBytes` — chosen there as ~10× the typical multiaddr
/// budget so the cap rarely bites in practice.
pub const MAX_UNDERLAY_BYTES: usize = 2048;

#[derive(Debug, thiserror::Error)]
pub enum UnderlayError {
    #[error("empty underlay bytes")]
    Empty,
    #[error("invalid multiaddr: {0}")]
    Multiaddr(#[from] MultiaddrError),
    #[error("varint: {0}")]
    Varint(#[from] VarintError),
    #[error("underlay count {0} exceeds cap of {MAX_UNDERLAYS_PER_PEER}")]
    CountExceeded(usize),
    #[error("underlay bytes {0} exceed cap of {MAX_UNDERLAY_BYTES}")]
    ByteSizeExceeded(usize),
}

/// Sort + truncate `addrs` to fit inside bee 2.8's count and byte
/// caps, prioritising IPv4 public TCP > IPv4 public WS/WSS > private >
/// loopback / non-IPv4. Mirrors `bzz.SortUnderlaysByPriority` +
/// `bzz.TruncateUnderlays`. Returns the truncated list in priority
/// order; callers should pass this into [`serialize_underlays`].
#[must_use]
pub fn truncate_underlays(addrs: &[Multiaddr]) -> Vec<Multiaddr> {
    let mut sorted: Vec<Multiaddr> = addrs.to_vec();
    sorted.sort_by_key(underlay_score);

    let mut out: Vec<Multiaddr> = Vec::with_capacity(sorted.len().min(MAX_UNDERLAYS_PER_PEER));
    // Account for the 0x99 list-prefix byte that `serialize_underlays`
    // emits when there are 2+ entries. Single-entry records use the
    // backward-compat raw multiaddr encoding (no prefix); we
    // pre-charge the prefix here as an upper bound — slightly
    // conservative for the single-entry case, exactly right for
    // multi-entry records.
    let mut total = 1_usize;
    for addr in sorted {
        if out.len() >= MAX_UNDERLAYS_PER_PEER {
            break;
        }
        let b = addr.to_vec();
        let mut enc = unsigned_varint::encode::u64_buffer();
        let v = unsigned_varint::encode::u64(b.len() as u64, &mut enc);
        let size = v.len() + b.len();
        if total + size > MAX_UNDERLAY_BYTES {
            break;
        }
        total += size;
        out.push(addr);
    }
    out
}

/// Lower score = higher priority. Matches bee 2.8 `underlayScore`:
/// - non-IPv4 +100
/// - loopback +20, else private +10
/// - transport priority: TCP 0, WS 1, WSS 2, else 3
fn underlay_score(addr: &Multiaddr) -> i32 {
    let mut score = 0;
    let mut has_ip4 = false;
    let mut is_loopback = false;
    let mut is_private = false;
    for p in addr {
        match p {
            Protocol::Ip4(ip) => {
                has_ip4 = true;
                if ip.is_loopback() {
                    is_loopback = true;
                } else if is_private_ipv4(ip) {
                    is_private = true;
                }
            }
            Protocol::Ip6(ip) => {
                if ip.is_loopback() {
                    is_loopback = true;
                } else if is_private_ipv6(ip) {
                    is_private = true;
                }
            }
            _ => {}
        }
    }
    if !has_ip4 {
        score += 100;
    }
    if is_loopback {
        score += 20;
    } else if is_private {
        score += 10;
    }
    score += transport_priority(addr);
    score
}

fn transport_priority(addr: &Multiaddr) -> i32 {
    let mut tcp = false;
    let mut ws = false;
    let mut wss = false;
    for p in addr {
        match p {
            Protocol::Tcp(_) => tcp = true,
            Protocol::Ws(_) => ws = true,
            Protocol::Wss(_) => wss = true,
            _ => {}
        }
    }
    if wss {
        2
    } else if ws {
        1
    } else if tcp {
        0
    } else {
        3
    }
}

fn is_private_ipv4(ip: Ipv4Addr) -> bool {
    ip.is_private() || ip.is_link_local()
}

fn is_private_ipv6(ip: Ipv6Addr) -> bool {
    // RFC 4193 ULA (fc00::/7) — matches `manet.IsPrivateAddr`.
    let octets = ip.octets();
    (octets[0] & 0xfe) == 0xfc
}

/// Reachability class of an IP address, from the point of view of a node
/// somewhere else on the internet. Shared by the "is our observed address
/// worth advertising" check and the "may we dial this peer underlay"
/// filter, so both agree on what "unroutable" means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IpScope {
    /// Publicly routable.
    Global,
    /// Belongs to *some* private network: RFC1918, IPv4 link-local
    /// (`169.254/16`), CGNAT (`100.64/10`), IPv6 ULA (`fc00::/7`).
    /// Only reachable from a host attached to that same network.
    Private,
    /// Never meaningful as a remote peer's address: loopback, unspecified,
    /// `0/8`, broadcast, multicast, documentation ranges, and IPv6
    /// link-local (`fe80::/10` — undialable without the zone id, which
    /// multiaddrs don't carry).
    Local,
}

/// Classify `ip`. IPv4-mapped IPv6 (`::ffff:a.b.c.d`) is classified as the
/// embedded IPv4 address.
pub(crate) fn ip_scope(ip: IpAddr) -> IpScope {
    match ip {
        IpAddr::V4(v4) => ipv4_scope(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return ipv4_scope(v4);
            }
            let seg0 = v6.segments()[0];
            if v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                // fe80::/10 link-local
                || (seg0 & 0xffc0) == 0xfe80
                // 2001:db8::/32 documentation
                || (seg0 == 0x2001 && v6.segments()[1] == 0x0db8)
            {
                IpScope::Local
            } else if is_private_ipv6(v6) {
                IpScope::Private
            } else {
                IpScope::Global
            }
        }
    }
}

fn ipv4_scope(ip: Ipv4Addr) -> IpScope {
    if ip.is_loopback()
        || ip.is_unspecified()
        // 0.0.0.0/8 "this network"
        || ip.octets()[0] == 0
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
    {
        IpScope::Local
    } else if is_private_ipv4(ip) || is_cgnat_ipv4(ip) {
        IpScope::Private
    } else {
        IpScope::Global
    }
}

/// `100.64.0.0/10` shared address space (RFC 6598, carrier-grade NAT).
fn is_cgnat_ipv4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    o[0] == 100 && (o[1] & 0xc0) == 64
}

/// True when `ip` is reachable from anywhere on the internet.
pub(crate) fn is_globally_routable_ip(ip: IpAddr) -> bool {
    ip_scope(ip) == IpScope::Global
}

/// The IP subnets this host is directly attached to (non-loopback
/// interfaces). A peer underlay in [`IpScope::Private`] space is only worth
/// dialing when it falls inside one of these — e.g. phone on
/// `192.168.1.0/24` and a bee node on the same Wi-Fi advertising
/// `192.168.1.20`. Traffic to a directly-attached subnet never leaves the
/// local network, so it can't look like a scan to an upstream provider.
#[derive(Debug, Clone, Default)]
pub(crate) struct LocalSubnets {
    nets: Vec<(IpAddr, u8)>,
}

impl LocalSubnets {
    /// Snapshot the host's interfaces. An enumeration failure yields an
    /// empty set, i.e. private underlays are dropped — the safe direction.
    pub(crate) fn from_interfaces() -> Self {
        let nets = match if_addrs::get_if_addrs() {
            Ok(ifs) => ifs
                .into_iter()
                .filter(|i| !i.is_loopback())
                .map(|i| match i.addr {
                    if_addrs::IfAddr::V4(a) => (IpAddr::V4(a.ip), a.prefixlen),
                    if_addrs::IfAddr::V6(a) => (IpAddr::V6(a.ip), a.prefixlen),
                })
                .collect(),
            Err(e) => {
                tracing::debug!(target: "ant_p2p", "enumerate local interfaces: {e}");
                Vec::new()
            }
        };
        Self { nets }
    }

    #[cfg(test)]
    pub(crate) fn from_nets(nets: Vec<(IpAddr, u8)>) -> Self {
        Self { nets }
    }

    pub(crate) fn contains(&self, ip: IpAddr) -> bool {
        self.nets.iter().any(|&(net, prefix)| match (net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => prefix_match(&n.octets(), &a.octets(), prefix),
            (IpAddr::V6(n), IpAddr::V6(a)) => prefix_match(&n.octets(), &a.octets(), prefix),
            _ => false,
        })
    }
}

/// Compare the first `prefix` bits of two equal-length octet strings. A
/// zero prefix (a default-route-sized "subnet") never matches: it would
/// admit every private address.
fn prefix_match(a: &[u8], b: &[u8], prefix: u8) -> bool {
    let prefix = usize::from(prefix);
    if prefix == 0 || prefix > a.len() * 8 {
        return false;
    }
    let full = prefix / 8;
    if a[..full] != b[..full] {
        return false;
    }
    let rem = prefix % 8;
    if rem == 0 {
        return true;
    }
    let mask = 0xffu8 << (8 - rem);
    (a[full] & mask) == (b[full] & mask)
}

/// Keep only the peer underlays that are worth dialing from this host.
///
/// A peer's private / loopback / link-local underlays describe *its own*
/// network: from anywhere else they are unreachable or, worse, hit an
/// unrelated host on ours. Dialing them en masse (bee nodes in
/// Docker/Kubernetes advertise their pod addresses) looks like a network
/// scan to a hosting provider (issue #88). So an address survives when every
/// IP component is globally routable, or is [`IpScope::Private`] and inside
/// one of `local`'s subnets (same-LAN peer). Addresses without an IP
/// component (`/dns4/…`) pass through. `allow_private` disables the filter
/// entirely, for dev/test networks that live on one private network or
/// loopback.
pub(crate) fn filter_dialable_underlays(
    addrs: &[Multiaddr],
    local: &LocalSubnets,
    allow_private: bool,
) -> Vec<Multiaddr> {
    if allow_private {
        return addrs.to_vec();
    }
    addrs
        .iter()
        .filter(|addr| {
            addr.iter().all(|p| {
                let ip = match p {
                    Protocol::Ip4(ip) => IpAddr::V4(ip),
                    Protocol::Ip6(ip) => IpAddr::V6(ip),
                    _ => return true,
                };
                match ip_scope(ip) {
                    IpScope::Global => true,
                    IpScope::Private => local.contains(ip),
                    IpScope::Local => false,
                }
            })
        })
        .cloned()
        .collect()
}

/// Single-address backward-compatible encoding uses raw multiaddr bytes.
///
/// Returns the serialized form unconditionally; the cap checks live in
/// [`serialize_underlays_checked`]. Existing callers that rely on the
/// infallible signature don't change behaviour because we already feed
/// them small, hand-curated address sets.
pub fn serialize_underlays(addrs: &[Multiaddr]) -> Vec<u8> {
    if addrs.len() == 1 {
        return addrs[0].to_vec();
    }
    let mut buf = Vec::new();
    buf.push(UNDERLAY_LIST_PREFIX);
    for addr in addrs {
        let b = addr.to_vec();
        let mut enc = unsigned_varint::encode::u64_buffer();
        let v = unsigned_varint::encode::u64(b.len() as u64, &mut enc);
        buf.extend_from_slice(v);
        buf.extend_from_slice(&b);
    }
    buf
}

/// Like [`serialize_underlays`] but returns an explicit error when the
/// count / byte caps would be exceeded. Prefer this on the handshake
/// hot path so we surface the cap locally instead of letting bee 2.8
/// reject the payload mid-handshake with `ErrUnderlayCountExceeded` /
/// `ErrUnderlayByteSizeExceeded`.
pub fn serialize_underlays_checked(addrs: &[Multiaddr]) -> Result<Vec<u8>, UnderlayError> {
    if addrs.len() > MAX_UNDERLAYS_PER_PEER {
        return Err(UnderlayError::CountExceeded(addrs.len()));
    }
    let out = serialize_underlays(addrs);
    if out.len() > MAX_UNDERLAY_BYTES {
        return Err(UnderlayError::ByteSizeExceeded(out.len()));
    }
    Ok(out)
}

pub fn deserialize_underlays(data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    if data.is_empty() {
        return Err(UnderlayError::Empty);
    }
    if data.len() > MAX_UNDERLAY_BYTES {
        return Err(UnderlayError::ByteSizeExceeded(data.len()));
    }
    if data[0] == UNDERLAY_LIST_PREFIX {
        return deserialize_list(&data[1..]);
    }
    Ok(vec![Multiaddr::try_from(data.to_vec())?])
}

fn deserialize_list(data: &[u8]) -> Result<Vec<Multiaddr>, UnderlayError> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < data.len() {
        if out.len() >= MAX_UNDERLAYS_PER_PEER {
            return Err(UnderlayError::CountExceeded(out.len() + 1));
        }
        let slice = &data[i..];
        let (len, rest) = unsigned_varint::decode::u64(slice)?;
        let used = slice.len() - rest.len();
        i += used;
        let len = len as usize;
        if data.len() < i + len {
            return Err(UnderlayError::Empty);
        }
        out.push(Multiaddr::try_from(data[i..i + len].to_vec())?);
        i += len;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    #[test]
    fn priority_orders_ipv4_public_tcp_first() {
        let public = ma("/ip4/1.2.3.4/tcp/1634");
        let private = ma("/ip4/10.0.0.1/tcp/1634");
        let loopback = ma("/ip4/127.0.0.1/tcp/1634");
        let ipv6 = ma("/ip6/2001:db8::1/tcp/1634");
        let wss = ma("/ip4/1.2.3.4/tcp/1634/wss");
        assert!(underlay_score(&public) < underlay_score(&private));
        assert!(underlay_score(&private) < underlay_score(&loopback));
        assert!(underlay_score(&public) < underlay_score(&ipv6));
        assert!(underlay_score(&public) < underlay_score(&wss));
    }

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    #[test]
    fn ip_scope_classifies_unroutable_ranges() {
        for g in ["1.2.3.4", "8.8.8.8", "2a01:4f8::1", "::ffff:1.2.3.4"] {
            assert_eq!(ip_scope(ip(g)), IpScope::Global, "{g}");
        }
        for p in [
            "10.233.1.2",
            "172.17.0.3",
            "192.168.1.20",
            "169.254.1.1",
            "100.64.0.1",
            "100.127.255.254",
            "fd00::1",
            "fc00::1",
            "::ffff:10.0.0.1",
        ] {
            assert_eq!(ip_scope(ip(p)), IpScope::Private, "{p}");
        }
        for l in [
            "127.0.0.1",
            "0.0.0.0",
            "0.1.2.3",
            "255.255.255.255",
            "224.0.0.1",
            "192.0.2.1",
            "::1",
            "::",
            "fe80::1",
            "febf::1",
            "ff02::1",
            "2001:db8::1",
        ] {
            assert_eq!(ip_scope(ip(l)), IpScope::Local, "{l}");
        }
        // Just outside CGNAT / link-local on either side.
        assert_eq!(ip_scope(ip("100.63.255.255")), IpScope::Global);
        assert_eq!(ip_scope(ip("100.128.0.0")), IpScope::Global);
    }

    #[test]
    fn local_subnets_prefix_match() {
        let local = LocalSubnets::from_nets(vec![
            (ip("192.168.1.7"), 24),
            (ip("10.0.2.15"), 20),
            (ip("fd12:3456::7"), 64),
            (ip("100.101.102.103"), 32),
            (ip("172.16.0.1"), 0),
        ]);
        assert!(local.contains(ip("192.168.1.20")));
        assert!(!local.contains(ip("192.168.2.20")));
        assert!(local.contains(ip("10.0.15.255")));
        assert!(!local.contains(ip("10.0.16.0")));
        assert!(local.contains(ip("fd12:3456::99")));
        assert!(!local.contains(ip("fd12:3457::99")));
        assert!(local.contains(ip("100.101.102.103")));
        assert!(!local.contains(ip("100.101.102.104")));
        // A /0 "subnet" must not admit everything.
        assert!(!local.contains(ip("172.17.0.3")));
        // Families never cross-match.
        assert!(!local.contains(ip("::ffff:192.168.1.20")));
    }

    #[test]
    fn filter_drops_foreign_private_underlays_keeps_same_lan() {
        let local = LocalSubnets::from_nets(vec![(ip("192.168.1.7"), 24)]);
        let addrs = vec![
            ma("/ip4/10.233.64.12/tcp/1634/p2p/QmcgpsyWgH8Y8ajJz1Cu72KnS5uo2Aa2LpzU7kinSupNKC"),
            ma("/ip4/172.17.0.3/tcp/1634"),
            ma("/ip4/127.0.0.1/tcp/1634"),
            ma("/ip4/100.64.3.4/tcp/1634"),
            ma("/ip6/fe80::1/tcp/1634"),
            ma("/ip6/fd00::5/tcp/1634"),
            ma("/ip6/::1/tcp/1634"),
            ma("/ip4/192.168.1.20/tcp/1634"),
            ma("/ip4/1.2.3.4/tcp/1634"),
            ma("/ip6/2a01:4f8::1/tcp/1634"),
            ma("/dns4/node.example.org/tcp/1634"),
        ];
        let kept = filter_dialable_underlays(&addrs, &local, false);
        assert_eq!(
            kept,
            vec![
                ma("/ip4/192.168.1.20/tcp/1634"),
                ma("/ip4/1.2.3.4/tcp/1634"),
                ma("/ip6/2a01:4f8::1/tcp/1634"),
                ma("/dns4/node.example.org/tcp/1634"),
            ],
        );
        // Off the LAN, the same-subnet address goes too.
        let kept = filter_dialable_underlays(&addrs, &LocalSubnets::default(), false);
        assert!(!kept.contains(&ma("/ip4/192.168.1.20/tcp/1634")));
        // Escape hatch keeps everything.
        assert_eq!(filter_dialable_underlays(&addrs, &local, true), addrs);
    }

    #[test]
    fn truncate_drops_low_priority_when_over_cap() {
        let mut addrs: Vec<Multiaddr> = (0..MAX_UNDERLAYS_PER_PEER + 5)
            .map(|i| ma(&format!("/ip6/::1/tcp/{i}")))
            .collect();
        // One IPv4 public TCP entry should always survive truncation.
        let must_keep = ma("/ip4/1.2.3.4/tcp/1634");
        addrs.push(must_keep.clone());
        let truncated = truncate_underlays(&addrs);
        assert!(truncated.len() <= MAX_UNDERLAYS_PER_PEER);
        assert_eq!(truncated[0], must_keep);
    }

    #[test]
    fn serialize_checked_rejects_oversize_count() {
        let addrs: Vec<Multiaddr> = (0..=MAX_UNDERLAYS_PER_PEER)
            .map(|i| ma(&format!("/ip4/127.0.0.1/tcp/{i}")))
            .collect();
        let err = serialize_underlays_checked(&addrs).unwrap_err();
        matches!(err, UnderlayError::CountExceeded(_));
    }

    #[test]
    fn deserialize_rejects_oversize_payload() {
        let oversize = vec![0u8; MAX_UNDERLAY_BYTES + 1];
        let err = deserialize_underlays(&oversize).unwrap_err();
        matches!(err, UnderlayError::ByteSizeExceeded(_));
    }

    #[test]
    fn deserialize_rejects_too_many_entries() {
        let mut buf = vec![UNDERLAY_LIST_PREFIX];
        let addr = ma("/ip4/127.0.0.1/tcp/1");
        let b = addr.to_vec();
        let mut enc = unsigned_varint::encode::u64_buffer();
        let v = unsigned_varint::encode::u64(b.len() as u64, &mut enc).to_vec();
        for _ in 0..=MAX_UNDERLAYS_PER_PEER {
            buf.extend_from_slice(&v);
            buf.extend_from_slice(&b);
        }
        if buf.len() > MAX_UNDERLAY_BYTES {
            // Test would conflate two limits; keep the byte budget below
            // the size cap so the count cap is the one that fires.
            buf.truncate(MAX_UNDERLAY_BYTES);
        }
        let err = deserialize_underlays(&buf).unwrap_err();
        matches!(err, UnderlayError::CountExceeded(_));
    }
}
