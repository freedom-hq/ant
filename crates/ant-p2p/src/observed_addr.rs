//! Which of the addresses remote peers *observe* us at we advertise as
//! external (#300).
//!
//! libp2p-identify reports, per connection, the address the remote sees
//! us at; the swarm loop used to advertise any single such report once it
//! was globally routable, so one peer could make us advertise an address
//! of its choosing, without bound and forever. This keeps one vote per
//! connection and advertises an address only once
//! [`OBSERVED_ADDR_QUORUM`] observers agree on it, counted both by peer id
//! and by IP group, so several connections (or several freshly minted
//! peer ids) from one host still count as one. At most
//! [`MAX_OBSERVED_EXTERNAL`] addresses are advertised, the ones with the
//! most observers; a vote that hasn't been re-confirmed for
//! [`OBSERVATION_TTL`] stops counting.
//!
//! Identify only emits a fresh candidate per connection when the observed
//! address first appears or changes, so a vote is kept alive by the
//! periodic identify `Received` on its connection carrying the same
//! observed address ([`ObservedAddrs::on_identified`]), not by candidate
//! events: a stable node keeps its correct address.

use libp2p::swarm::ConnectionId;
use libp2p::{Multiaddr, PeerId};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::time::{Duration, Instant};

/// Distinct observers (by peer id *and* by IP group) that must report an
/// address before we advertise it. go-libp2p's observed-address manager
/// uses 4; bee nodes behind the same NAT are rare enough that 3 suffices.
pub const OBSERVED_ADDR_QUORUM: usize = 3;
/// A vote not re-confirmed by identify for this long stops counting.
/// Identify re-runs every 5 min per connection, so a live connection
/// refreshes its vote several times within one TTL.
pub const OBSERVATION_TTL: Duration = Duration::from_mins(20);
/// Most observed addresses advertised at once.
pub const MAX_OBSERVED_EXTERNAL: usize = 4;
/// Most distinct candidate addresses one connection's vote may carry.
/// Identify translates one observation into one candidate per listener,
/// so this only bites a misbehaving peer.
const MAX_ADDRS_PER_VOTE: usize = 4;

/// IPv4 by full address, IPv6 by /56 (a typical end-site allocation), the
/// grouping go-libp2p uses for its observer count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum IpGroup {
    V4([u8; 4]),
    V6([u8; 7]),
}

fn ip_group(ip: IpAddr) -> IpGroup {
    match ip {
        IpAddr::V4(v4) => IpGroup::V4(v4.octets()),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                IpGroup::V4(v4.octets())
            } else {
                let mut g = [0u8; 7];
                g.copy_from_slice(&v6.octets()[..7]);
                IpGroup::V6(g)
            }
        }
    }
}

/// One connection's current report.
#[derive(Debug)]
struct Vote {
    peer: PeerId,
    /// `None` for a connection whose remote isn't an IP address; such a
    /// vote never counts.
    group: Option<IpGroup>,
    /// The raw observed address identify last reported on this
    /// connection; a different one replaces the vote.
    raw: Option<Multiaddr>,
    /// The candidates identify derived from `raw`.
    addrs: Vec<Multiaddr>,
    /// When `raw` was last reported.
    seen: Instant,
}

#[derive(Debug, Default)]
pub struct ObservedAddrs {
    votes: HashMap<ConnectionId, Vote>,
    /// The connection whose identify `Received` came last. Identify queues
    /// a connection's candidate events right behind its `Received`, so this
    /// is who a following candidate belongs to.
    last_identified: Option<ConnectionId>,
}

impl ObservedAddrs {
    pub fn on_connection(
        &mut self,
        conn: ConnectionId,
        peer: PeerId,
        remote: &Multiaddr,
        now: Instant,
    ) {
        let group = remote_ip(remote).map(ip_group);
        self.votes.insert(
            conn,
            Vote {
                peer,
                group,
                raw: None,
                addrs: Vec::new(),
                seen: now,
            },
        );
    }

    pub fn on_closed(&mut self, conn: ConnectionId) {
        self.votes.remove(&conn);
        if self.last_identified == Some(conn) {
            self.last_identified = None;
        }
    }

    /// Identify `Received` on `conn`, reporting `observed`. The same
    /// address as before refreshes the vote; a different one starts a new
    /// vote, which the candidate events that follow fill in.
    pub fn on_identified(&mut self, conn: ConnectionId, observed: &Multiaddr, now: Instant) {
        self.last_identified = Some(conn);
        let Some(vote) = self.votes.get_mut(&conn) else {
            return;
        };
        if vote.raw.as_ref() != Some(observed) {
            vote.raw = Some(observed.clone());
            vote.addrs.clear();
        }
        vote.seen = now;
    }

    /// A `NewExternalAddrCandidate`. Returns `false` when it can't be tied
    /// to a connection's identify report, in which case it is ignored.
    pub fn on_candidate(&mut self, addr: Multiaddr) -> bool {
        let Some(vote) = self
            .last_identified
            .and_then(|conn| self.votes.get_mut(&conn))
        else {
            return false;
        };
        if !vote.addrs.contains(&addr) && vote.addrs.len() < MAX_ADDRS_PER_VOTE {
            vote.addrs.push(addr);
        }
        true
    }

    /// The addresses to advertise now: those with at least
    /// [`OBSERVED_ADDR_QUORUM`] live observers, most observed first, at
    /// most [`MAX_OBSERVED_EXTERNAL`]. On a tie, an address in
    /// `advertised` stays ahead of a newcomer so equal counts don't churn.
    pub fn wanted(&self, now: Instant, advertised: impl Fn(&Multiaddr) -> bool) -> Vec<Multiaddr> {
        let mut tally: HashMap<&Multiaddr, (HashSet<PeerId>, HashSet<IpGroup>)> = HashMap::new();
        for vote in self.votes.values() {
            let Some(group) = vote.group else { continue };
            if now.saturating_duration_since(vote.seen) >= OBSERVATION_TTL {
                continue;
            }
            for addr in &vote.addrs {
                let (peers, groups) = tally.entry(addr).or_default();
                peers.insert(vote.peer);
                groups.insert(group);
            }
        }
        let mut ranked: Vec<(usize, bool, &Multiaddr)> = tally
            .into_iter()
            .map(|(addr, (peers, groups))| (peers.len().min(groups.len()), advertised(addr), addr))
            .filter(|(n, _, _)| *n >= OBSERVED_ADDR_QUORUM)
            .collect();
        ranked.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then(b.1.cmp(&a.1))
                .then_with(|| a.2.to_vec().cmp(&b.2.to_vec()))
        });
        ranked
            .into_iter()
            .take(MAX_OBSERVED_EXTERNAL)
            .map(|(_, _, a)| a.clone())
            .collect()
    }
}

fn remote_ip(addr: &Multiaddr) -> Option<IpAddr> {
    use libp2p::multiaddr::Protocol;
    addr.iter().find_map(|p| match p {
        Protocol::Ip4(ip) => Some(IpAddr::V4(ip)),
        Protocol::Ip6(ip) => Some(IpAddr::V6(ip)),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ma(s: &str) -> Multiaddr {
        s.parse().unwrap()
    }

    /// Connection `n` from host `10.0.<n>.1`, reporting `observed`.
    fn vote(
        o: &mut ObservedAddrs,
        n: usize,
        peer: PeerId,
        host: &str,
        observed: &str,
        now: Instant,
    ) {
        let conn = ConnectionId::new_unchecked(n);
        o.on_connection(conn, peer, &ma(&format!("/ip4/{host}/tcp/1634")), now);
        o.on_identified(conn, &ma(observed), now);
        assert!(o.on_candidate(ma(observed)));
    }

    const OURS: &str = "/ip4/203.0.113.7/tcp/1634";
    const NONE: Vec<Multiaddr> = Vec::new();

    #[test]
    fn one_observer_is_not_enough() {
        let now = Instant::now();
        let mut o = ObservedAddrs::default();
        vote(&mut o, 1, PeerId::random(), "198.51.100.1", OURS, now);
        assert_eq!(o.wanted(now, |_| false), NONE);
    }

    #[test]
    fn quorum_of_distinct_observers_is_advertised() {
        let now = Instant::now();
        let mut o = ObservedAddrs::default();
        for n in 1..=OBSERVED_ADDR_QUORUM {
            vote(
                &mut o,
                n,
                PeerId::random(),
                &format!("198.51.100.{n}"),
                OURS,
                now,
            );
        }
        assert_eq!(o.wanted(now, |_| false), vec![ma(OURS)]);
    }

    /// One peer on several connections, or several peer ids from one
    /// host, is one observer.
    #[test]
    fn one_peer_or_one_host_is_one_observer() {
        let now = Instant::now();
        let mut o = ObservedAddrs::default();
        let peer = PeerId::random();
        for n in 1..=5 {
            vote(&mut o, n, peer, &format!("198.51.100.{n}"), OURS, now);
        }
        assert_eq!(o.wanted(now, |_| false), NONE, "one peer id");
        let mut o = ObservedAddrs::default();
        for n in 1..=5 {
            vote(&mut o, n, PeerId::random(), "198.51.100.1", OURS, now);
        }
        assert_eq!(o.wanted(now, |_| false), NONE, "one host");
    }

    /// A candidate that no identify report precedes is ignored.
    #[test]
    fn unattributed_candidate_is_dropped() {
        let mut o = ObservedAddrs::default();
        assert!(!o.on_candidate(ma(OURS)));
    }

    /// A connection that reports a new address moves its one vote; it
    /// can't vote for several addresses by cycling through them.
    #[test]
    fn a_connection_has_one_current_vote() {
        let now = Instant::now();
        let mut o = ObservedAddrs::default();
        for n in 1..=OBSERVED_ADDR_QUORUM {
            vote(
                &mut o,
                n,
                PeerId::random(),
                &format!("198.51.100.{n}"),
                OURS,
                now,
            );
        }
        let other = "/ip4/203.0.113.99/tcp/1634";
        let conn = ConnectionId::new_unchecked(1);
        o.on_identified(conn, &ma(other), now);
        assert!(o.on_candidate(ma(other)));
        assert_eq!(o.wanted(now, |_| false), NONE);
        o.on_closed(ConnectionId::new_unchecked(2));
        assert_eq!(o.wanted(now, |_| false), NONE);
    }

    /// Long-lived connections whose periodic identify keeps reporting the
    /// same address keep it advertised past the TTL; once they stop, it
    /// expires.
    #[test]
    fn refreshed_votes_outlive_the_ttl_and_stale_ones_expire() {
        let start = Instant::now();
        let mut o = ObservedAddrs::default();
        for n in 1..=OBSERVED_ADDR_QUORUM {
            vote(
                &mut o,
                n,
                PeerId::random(),
                &format!("198.51.100.{n}"),
                OURS,
                start,
            );
        }
        let mut now = start;
        for _ in 0..12 {
            now += Duration::from_mins(5);
            for n in 1..=OBSERVED_ADDR_QUORUM {
                o.on_identified(ConnectionId::new_unchecked(n), &ma(OURS), now);
            }
            assert_eq!(
                o.wanted(now, |_| false),
                vec![ma(OURS)],
                "at {:?}",
                now - start
            );
        }
        assert!(now - start > OBSERVATION_TTL);
        let later = now + OBSERVATION_TTL;
        assert_eq!(o.wanted(later, |_| false), NONE);
    }

    /// However many addresses reach quorum, at most
    /// `MAX_OBSERVED_EXTERNAL` are wanted, the most observed first.
    #[test]
    fn wanted_set_is_capped_and_keeps_the_most_observed() {
        let now = Instant::now();
        let mut o = ObservedAddrs::default();
        let mut n = 0;
        // 10 colluder addresses with exactly a quorum each.
        for a in 0..10 {
            for _ in 0..OBSERVED_ADDR_QUORUM {
                n += 1;
                let host = format!("198.51.{}.{}", n / 200, n % 200 + 1);
                vote(
                    &mut o,
                    n,
                    PeerId::random(),
                    &host,
                    &format!("/ip4/192.0.2.{a}/tcp/1"),
                    now,
                );
            }
        }
        // The real address, seen by more observers.
        for _ in 0..=OBSERVED_ADDR_QUORUM {
            n += 1;
            let host = format!("198.51.{}.{}", n / 200, n % 200 + 1);
            vote(&mut o, n, PeerId::random(), &host, OURS, now);
        }
        let wanted = o.wanted(now, |_| false);
        assert_eq!(wanted.len(), MAX_OBSERVED_EXTERNAL);
        assert_eq!(wanted[0], ma(OURS));
    }

    #[test]
    fn ipv6_observers_group_by_56() {
        let a: IpAddr = "2001:db8:aa:bb01::1".parse().unwrap();
        let b: IpAddr = "2001:db8:aa:bbff::2".parse().unwrap();
        let c: IpAddr = "2001:db8:aa:cc00::1".parse().unwrap();
        assert_eq!(ip_group(a), ip_group(b));
        assert_ne!(ip_group(a), ip_group(c));
    }
}
