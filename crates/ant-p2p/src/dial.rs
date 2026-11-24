//! Multiaddr helpers shared between the bootnode dial path and the UI.

use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};

/// For UI: first IP literal or DNS label in a multiaddr.
pub fn endpoint_host(ma: &Multiaddr) -> String {
    for p in ma {
        match p {
            Protocol::Ip4(ip) => return ip.to_string(),
            Protocol::Ip6(ip) => return ip.to_string(),
            Protocol::Dns4(d) | Protocol::Dns6(d) | Protocol::Dnsaddr(d) => {
                return d.to_string();
            }
            _ => {}
        }
    }
    String::new()
}

/// Extract the destination `/p2p/<peer_id>` from a multiaddr, if present:
/// the *last* `/p2p/` component, past any relay hop of a circuit address
/// (`/../p2p/<relay>/p2p-circuit/p2p/<target>`), which is the peer a dial
/// reaches and the one a returning `ConnectionEstablished` names. The
/// handshake binds a `BzzAddress` to its connection by this same
/// component, so a hive hint keyed by it is attributed to the peer its
/// record was signed for, not to the relay in front of it. Addresses
/// without one are skipped by the bootnode dial path: we need the id to
/// match back to the in-flight dial.
pub fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter()
        .filter_map(|p| match p {
            Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .last()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_host_returns_ipv4_literal() {
        let a: Multiaddr = "/ip4/1.2.3.4/tcp/1634".parse().unwrap();
        assert_eq!(endpoint_host(&a), "1.2.3.4");
    }

    #[test]
    fn endpoint_host_returns_dnsaddr_label() {
        let a: Multiaddr = "/dnsaddr/mainnet.ethswarm.org".parse().unwrap();
        assert_eq!(endpoint_host(&a), "mainnet.ethswarm.org");
    }

    #[test]
    fn extract_peer_id_finds_p2p() {
        let a: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        assert!(extract_peer_id(&a).is_some());
    }

    /// A circuit address names the relay first and the destination last;
    /// the destination is the peer.
    #[test]
    fn extract_peer_id_takes_circuit_destination() {
        let (relay, target) = (PeerId::random(), PeerId::random());
        let a: Multiaddr = format!("/ip4/1.2.3.4/tcp/1634/p2p/{relay}/p2p-circuit/p2p/{target}")
            .parse()
            .unwrap();
        assert_eq!(extract_peer_id(&a), Some(target));
    }

    #[test]
    fn extract_peer_id_none_without_p2p() {
        let a: Multiaddr = "/ip4/1.2.3.4/tcp/1634".parse().unwrap();
        assert!(extract_peer_id(&a).is_none());
    }
}
