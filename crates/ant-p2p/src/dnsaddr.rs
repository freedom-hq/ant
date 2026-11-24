//! Recursive `/dnsaddr/` multiaddr resolution.
//!
//! Unlike `libp2p-dns`'s transport — which resolves one TXT chain per dial and
//! picks a single winner — this walks the entire `_dnsaddr.<domain>` TXT tree
//! up front and returns every leaf multiaddr. That matches how Swarm publishes
//! bootnodes: `/dnsaddr/mainnet.ethswarm.org` fans out through a per-region
//! chain into multiple concrete peer addresses, and we want to dial them in
//! parallel.
//!
//! Design adopted from nxm-rs/vertex (`crates/net/dnsaddr`).
//!
//! The resolver uses the system nameservers but strips any search domains so
//! captive-portal / intranet suffixes never end up concatenated onto public
//! Swarm FQDNs. Falls back to Cloudflare if `/etc/resolv.conf` is unreadable.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::name_server::TokioConnectionProvider;
use hickory_resolver::TokioResolver;
use libp2p::multiaddr::{Multiaddr, Protocol};
use thiserror::Error;
use tracing::{debug, warn};

/// Recursion cap — the Swarm tree is ~3 levels deep; anything beyond 10 is a loop.
const MAX_RECURSION_DEPTH: usize = 10;

/// Total TXT queries one top-level `/dnsaddr/` entry may cost, across its
/// whole tree (same figure as `libp2p-dns`'s `MAX_DNS_LOOKUPS`). The depth
/// cap alone doesn't bound a *wide* tree: every record can fan out to more
/// distinct names, so a hostile or broken zone could make one bootnode
/// entry cost thousands of lookups. `mainnet.ethswarm.org` needs 5.
const MAX_DNS_QUERIES: usize = 32;

/// True if this multiaddr contains a `/dnsaddr/<domain>` component.
#[must_use]
pub fn is_dnsaddr(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| matches!(p, Protocol::Dnsaddr(_)))
}

/// Expand every `/dnsaddr/` in `addrs` into concrete leaf multiaddrs.
///
/// - Non-dnsaddr entries pass through untouched.
/// - A shared `seen` set deduplicates TXT targets across the whole batch,
///   so identical sub-trees aren't re-walked and CNAME-style cycles terminate.
/// - Each top-level entry may cost at most [`MAX_DNS_QUERIES`] TXT lookups;
///   past that its tree is cut short and the leaves found so far are kept.
/// - On lookup failure the original `/dnsaddr/…` is kept as a fallback, so the
///   libp2p-dns transport still has a chance to resolve it later.
pub async fn resolve_all<I>(addrs: I) -> Vec<Multiaddr>
where
    I: IntoIterator<Item = Multiaddr>,
{
    resolve_all_with(&build_resolver(), addrs).await
}

/// One `_dnsaddr.<domain>` TXT lookup, returning each TXT string. A seam so
/// tests can serve a synthetic tree without a nameserver.
trait TxtLookup: Sync {
    fn txt<'a>(
        &'a self,
        name: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send + 'a>>;
}

impl TxtLookup for TokioResolver {
    fn txt<'a>(
        &'a self,
        name: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send + 'a>> {
        Box::pin(async move {
            let records = self.txt_lookup(name).await.map_err(|e| e.to_string())?;
            Ok(records
                .iter()
                .flat_map(|r| r.txt_data().iter())
                .map(|txt| String::from_utf8_lossy(txt).into_owned())
                .collect())
        })
    }
}

async fn resolve_all_with<L, I>(resolver: &L, addrs: I) -> Vec<Multiaddr>
where
    L: TxtLookup,
    I: IntoIterator<Item = Multiaddr>,
{
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for addr in addrs {
        if !is_dnsaddr(&addr) {
            out.push(addr);
            continue;
        }
        let mut budget = MAX_DNS_QUERIES;
        match resolve_recursive(resolver, &addr, &mut seen, 0, &mut budget).await {
            Ok(v) => {
                debug!(target: "ant_p2p::dnsaddr", %addr, count = v.len(), "resolved");
                out.extend(v);
            }
            Err(e) => {
                warn!(target: "ant_p2p::dnsaddr", %addr, "resolve failed: {e}; keeping original");
                out.push(addr);
            }
        }
    }
    out
}

#[derive(Debug, Error)]
enum ResolveError {
    #[error("dns lookup: {0}")]
    DnsLookup(String),
    #[error("max recursion depth exceeded")]
    MaxRecursionDepth,
}

fn extract_domain(addr: &Multiaddr) -> Option<String> {
    addr.iter().find_map(|p| match p {
        Protocol::Dnsaddr(d) => Some(d.to_string()),
        _ => None,
    })
}

/// Walk one `/dnsaddr/` tree. `budget` is the number of TXT queries this
/// walk may still send; once it hits zero the remaining sub-trees are
/// skipped (with a warning) and the leaves found so far are returned.
fn resolve_recursive<'a, L: TxtLookup>(
    resolver: &'a L,
    addr: &'a Multiaddr,
    seen: &'a mut HashSet<String>,
    depth: usize,
    budget: &'a mut usize,
) -> Pin<Box<dyn Future<Output = Result<Vec<Multiaddr>, ResolveError>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_RECURSION_DEPTH {
            return Err(ResolveError::MaxRecursionDepth);
        }

        let Some(domain) = extract_domain(addr) else {
            return Ok(vec![addr.clone()]);
        };

        let key = format!("_dnsaddr.{domain}");
        if !seen.insert(key.clone()) {
            debug!(target: "ant_p2p::dnsaddr", %domain, "already seen");
            return Ok(Vec::new());
        }

        if *budget == 0 {
            warn!(
                target: "ant_p2p::dnsaddr",
                name = %key,
                max = MAX_DNS_QUERIES,
                "dnsaddr query budget exhausted; skipping",
            );
            return Ok(Vec::new());
        }
        *budget -= 1;

        debug!(target: "ant_p2p::dnsaddr", name = %key, "TXT query");
        let records = resolver
            .txt(&key)
            .await
            .map_err(|e| ResolveError::DnsLookup(format!("{key}: {e}")))?;

        let mut out = Vec::new();
        for s in &records {
            let Some(value) = s.strip_prefix("dnsaddr=") else {
                continue;
            };
            match value.parse::<Multiaddr>() {
                Ok(next) => {
                    let nested =
                        resolve_recursive(resolver, &next, seen, depth + 1, budget).await?;
                    out.extend(nested);
                }
                Err(e) => {
                    warn!(target: "ant_p2p::dnsaddr", %value, "bad multiaddr in TXT: {e}");
                }
            }
        }
        Ok(out)
    })
}

fn build_resolver() -> TokioResolver {
    let (system_cfg, opts) = match hickory_resolver::system_conf::read_system_conf() {
        Ok(c) => c,
        Err(_) => (ResolverConfig::cloudflare(), ResolverOpts::default()),
    };
    // System nameservers first (so corporate / split-horizon DNS for
    // private bootnodes still works), then Cloudflare's anycast as a
    // public fallback. The latter is load-bearing on the iOS
    // simulator: even after a sim reboot, `system_conf` returns
    // resolvers that the simulator's network sandbox can't reach
    // (every UDP send returns `EHOSTUNREACH` / "No route to host"
    // os error 65), so without a backup `/dnsaddr/mainnet.ethswarm.org`
    // resolution fails forever and the player has no dialable
    // bootnodes on a fresh install. Verified empirically: clean
    // `data_dir` + clean sim, system DNS only → zero peers in 40 s;
    // with Cloudflare appended → 100 peers in 3 s. hickory's name
    // server pool walks the list on per-server connection failures,
    // so a healthy host's system DNS still wins on the happy path —
    // we only fall through when those entries time out / refuse.
    let mut servers = system_cfg.name_servers().to_vec();
    servers.extend(ResolverConfig::cloudflare().name_servers().iter().cloned());
    let sanitized = ResolverConfig::from_parts(None, Vec::new(), servers);
    let builder = TokioResolver::builder_with_config(sanitized, TokioConnectionProvider::default())
        .with_options(opts);
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_dnsaddr_true_for_dnsaddr() {
        let addr: Multiaddr = "/dnsaddr/mainnet.ethswarm.org".parse().unwrap();
        assert!(is_dnsaddr(&addr));
    }

    #[test]
    fn is_dnsaddr_false_for_ip() {
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/1634".parse().unwrap();
        assert!(!is_dnsaddr(&addr));
    }

    #[test]
    fn is_dnsaddr_false_for_dns4() {
        let addr: Multiaddr = "/dns4/example.com/tcp/1634".parse().unwrap();
        assert!(!is_dnsaddr(&addr));
    }

    #[test]
    fn extract_domain_returns_dnsaddr_domain() {
        let addr: Multiaddr = "/dnsaddr/mainnet.ethswarm.org".parse().unwrap();
        assert_eq!(
            extract_domain(&addr),
            Some("mainnet.ethswarm.org".to_string())
        );
    }

    #[test]
    fn extract_domain_returns_none_for_ip() {
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/1634".parse().unwrap();
        assert_eq!(extract_domain(&addr), None);
    }

    /// Synthetic zone: `_dnsaddr.<name>` → TXT strings. Counts queries.
    struct FakeZone {
        records: std::collections::HashMap<String, Vec<String>>,
        queries: std::sync::atomic::AtomicUsize,
    }

    impl TxtLookup for FakeZone {
        fn txt<'a>(
            &'a self,
            name: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send + 'a>> {
            self.queries
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let r = self.records.get(name).cloned().unwrap_or_default();
            Box::pin(async move { Ok(r) })
        }
    }

    /// A root fanning out to `width` distinct sub-domains, each holding one
    /// leaf peer address.
    fn wide_zone(root: &str, width: usize) -> FakeZone {
        let mut records = std::collections::HashMap::new();
        records.insert(
            format!("_dnsaddr.{root}"),
            (0..width)
                .map(|i| format!("dnsaddr=/dnsaddr/n{i}.{root}"))
                .collect(),
        );
        for i in 0..width {
            records.insert(
                format!("_dnsaddr.n{i}.{root}"),
                vec![format!("dnsaddr=/ip4/1.2.3.{}/tcp/1634", i % 256)],
            );
        }
        FakeZone {
            records,
            queries: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Issue #301: a tree wider than the budget stops at `MAX_DNS_QUERIES`
    /// lookups, keeping the leaves it found before the cut.
    #[tokio::test]
    async fn wide_dnsaddr_tree_stops_at_query_budget() {
        let zone = wide_zone("evil.example", 1000);
        let root: Multiaddr = "/dnsaddr/evil.example".parse().unwrap();
        let out = resolve_all_with(&zone, [root]).await;
        assert_eq!(
            zone.queries.load(std::sync::atomic::Ordering::SeqCst),
            MAX_DNS_QUERIES
        );
        // Root query + one per resolved sub-domain.
        assert_eq!(out.len(), MAX_DNS_QUERIES - 1);
        assert!(out.iter().all(|a| !is_dnsaddr(a)));
    }

    /// A tree inside the budget resolves completely, and each top-level
    /// entry gets its own budget (one wide entry can't starve the next).
    #[tokio::test]
    async fn dnsaddr_budget_is_per_top_level_entry() {
        let mut zone = wide_zone("big.example", 1000);
        zone.records.insert(
            "_dnsaddr.small.example".into(),
            vec![
                "dnsaddr=/ip4/5.6.7.8/tcp/1634".into(),
                "dnsaddr=/ip4/5.6.7.9/tcp/1634".into(),
            ],
        );
        let out = resolve_all_with(
            &zone,
            [
                "/dnsaddr/big.example".parse().unwrap(),
                "/dnsaddr/small.example".parse().unwrap(),
            ],
        )
        .await;
        assert_eq!(
            zone.queries.load(std::sync::atomic::Ordering::SeqCst),
            MAX_DNS_QUERIES + 1
        );
        assert!(out.contains(&"/ip4/5.6.7.8/tcp/1634".parse().unwrap()));
        assert!(out.contains(&"/ip4/5.6.7.9/tcp/1634".parse().unwrap()));

        let zone = wide_zone("ok.example", 5);
        let out = resolve_all_with(&zone, ["/dnsaddr/ok.example".parse().unwrap()]).await;
        assert_eq!(out.len(), 5);
        assert_eq!(zone.queries.load(std::sync::atomic::Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn resolve_all_passes_non_dnsaddr_through() {
        let addr: Multiaddr = "/ip4/127.0.0.1/tcp/1634".parse().unwrap();
        let resolved = resolve_all(std::iter::once(addr.clone())).await;
        assert_eq!(resolved, vec![addr]);
    }
}
