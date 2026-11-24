//! `Host` allowlist: the outermost layer of the router (issue #312).
//!
//! Without it, DNS rebinding turns any website into a same-origin
//! client of the API: a page at `http://evil.example:1633` whose name is
//! re-pointed at `127.0.0.1` talks to the gateway with
//! `Host: evil.example:1633`, and the browser treats every response as
//! its own. Checking `Host` closes that, since the attacker controls the
//! name but not what the browser sends as `Host`.
//!
//! The policy:
//!
//! - Always accepted: `localhost`, `127.0.0.1` and `[::1]`, any
//!   subdomain of `localhost` (`abc.bzz.localhost`, as Bee's subdomain
//!   router uses), and the literal IP the listener is bound to. Each
//!   with or without the bound port; a different port is refused. A
//!   wildcard bind (`0.0.0.0`, `[::]`) has no single IP of its own, so
//!   there any IP literal clients use (`192.168.1.5:1633`) must be
//!   listed in `allowed-hosts` like a name.
//! - Extra names come from `allowed-hosts` (antd) /
//!   `ant_set_gateway_allowed_hosts` (ant-ffi), e.g. the public name a
//!   reverse proxy forwards. An entry without a port matches that name
//!   on any port; `name:port` only that port. Browsers and proxies leave
//!   a default port out of `Host`, so `name:443` and `name:80` also
//!   match a bare `name`. IP literals match in any spelling
//!   (`[fd00:0:0:0:0:0:0:1]` is `[fd00::1]`, `[::ffff:10.0.0.2]` is
//!   `10.0.0.2`). `*` turns the check off.
//! - A listener bound to a non-loopback address with no `allowed-hosts`
//!   is not checked (clients reach it under names the node can't know);
//!   [`crate::Gateway::serve_listener`] (behind both
//!   [`crate::Gateway::serve`] and ant-ffi's `ant_start_gateway`) warns at
//!   startup.
//! - A request without any `Host` (HTTP/1.0, in-process callers) passes:
//!   browsers always send one. Several `Host` values are refused.
//!
//! A refused request gets `421 Misdirected Request` with a JSON body.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::handle::GatewayHandle;

/// Parsed `allowed-hosts` entries plus the address the listener is bound
/// to. Built with [`HostPolicy::new`] from config and resolved against
/// the real listener address by [`crate::Gateway::serve_listener`]
/// ([`HostPolicy::bound_to`]). An unbound policy (a router built without
/// a listener, as tests do) enforces the loopback names on any port.
#[derive(Debug, Clone, Default)]
pub struct HostPolicy {
    /// `*` was configured: no check at all.
    allow_all: bool,
    /// Extra names (lowercased, no trailing dot; IP literals in canonical
    /// form: IPv6 compressed and without brackets, IPv4-mapped as IPv4)
    /// and an optional port.
    hosts: Vec<(String, Option<u16>)>,
    /// The listener's local address, once known.
    bound: Option<SocketAddr>,
}

impl HostPolicy {
    /// Build from the raw `allowed-hosts` values. Blank entries are
    /// ignored; a malformed entry is dropped (it never matches
    /// anything). Hosts that want a hard error validate each entry with
    /// [`HostPolicy::check_entry`] first.
    pub fn new<I, S>(entries: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut policy = Self::default();
        for raw in entries {
            let e = raw.as_ref().trim();
            if e.is_empty() {
                continue;
            }
            if e == "*" {
                policy.allow_all = true;
            } else if let Ok(entry) = parse_entry(e) {
                policy.hosts.push(entry);
            }
        }
        policy
    }

    /// Validate one `allowed-hosts` entry: `*`, or a host name / IP
    /// literal with an optional port (`vibing.at`, `node.lan:1633`,
    /// `[fd00::1]:1633`). No scheme, path or wildcard. A blank entry is
    /// fine: [`HostPolicy::new`] ignores it.
    ///
    /// # Errors
    ///
    /// Why the entry is not a `host[:port]`.
    pub fn check_entry(entry: &str) -> Result<(), String> {
        let e = entry.trim();
        if e.is_empty() || e == "*" {
            return Ok(());
        }
        parse_entry(e).map(|_| ())
    }

    /// This policy resolved against the listener's local address.
    #[must_use]
    pub fn bound_to(&self, addr: SocketAddr) -> Self {
        Self {
            bound: Some(addr),
            ..self.clone()
        }
    }

    /// Whether any `allowed-hosts` entry (`*` included) was configured.
    #[must_use]
    pub fn configured(&self) -> bool {
        self.allow_all || !self.hosts.is_empty()
    }

    /// Whether requests are checked at all: not with `*`, and not on a
    /// non-loopback listener without `allowed-hosts`.
    #[must_use]
    pub fn enforced(&self) -> bool {
        if self.allow_all {
            return false;
        }
        match self.bound {
            Some(addr) => bind_is_loopback(addr) || !self.hosts.is_empty(),
            None => true,
        }
    }

    /// Whether one `Host` value (`name[:port]`) is accepted. Assumes
    /// [`HostPolicy::enforced`].
    fn allows(&self, value: &str) -> bool {
        let Ok((host, port)) = split_host_port(value) else {
            return false;
        };
        let port_ok = |p: Option<u16>| match (p, self.bound) {
            (None, _) | (_, None) => true,
            (Some(p), Some(addr)) => p == addr.port(),
        };
        if is_local_name(&host) && port_ok(port) {
            return true;
        }
        // A wildcard bind has no IP of its own to accept: an IP literal
        // then has to be listed like any other name.
        if let (Ok(ip), Some(addr)) = (host.parse::<IpAddr>(), self.bound) {
            let bound_ip = addr.ip().to_canonical();
            if !bound_ip.is_unspecified() && ip.to_canonical() == bound_ip && port_ok(port) {
                return true;
            }
        }
        self.hosts
            .iter()
            .any(|(h, p)| *h == host && entry_port_matches(*p, port))
    }
}

/// Whether an entry's port accepts a `Host`'s port. A bare `Host` is
/// what browsers and proxies send for the scheme's default port, so an
/// entry naming `443` or `80` takes it too (the scheme isn't known here).
fn entry_port_matches(entry: Option<u16>, host: Option<u16>) -> bool {
    match (entry, host) {
        (None, _) => true,
        (Some(e), Some(h)) => e == h,
        (Some(e), None) => e == 443 || e == 80,
    }
}

/// `localhost`, a subdomain of it, `127.0.0.1` or `::1`.
fn is_local_name(host: &str) -> bool {
    if host == "localhost" {
        return true;
    }
    if let Some(sub) = host.strip_suffix(".localhost") {
        return is_dns_name(sub);
    }
    matches!(
        host.parse::<IpAddr>(),
        Ok(IpAddr::V4(Ipv4Addr::LOCALHOST) | IpAddr::V6(Ipv6Addr::LOCALHOST))
    )
}

fn bind_is_loopback(addr: SocketAddr) -> bool {
    addr.ip().to_canonical().is_loopback()
}

/// Dot-separated, non-empty labels of ASCII letters, digits, `-`, `_`.
fn is_dns_name(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|label| {
            !label.is_empty()
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        })
}

/// Split a `Host` value into a lowercased host (one trailing dot
/// dropped; an IP literal in canonical form, IPv6 without brackets) and
/// its port.
fn split_host_port(value: &str) -> Result<(String, Option<u16>), &'static str> {
    let v = value.trim().to_ascii_lowercase();
    let (host, port) = if let Some(rest) = v.strip_prefix('[') {
        let (h, after) = rest.split_once(']').ok_or("unclosed `[`")?;
        h.parse::<Ipv6Addr>().map_err(|_| "bad IPv6 literal")?;
        let port = match after {
            "" => None,
            p => Some(p.strip_prefix(':').ok_or("junk after `]`")?),
        };
        (h.to_string(), port)
    } else {
        match v.split_once(':') {
            Some((_, p)) if p.contains(':') => return Err("IPv6 literal needs brackets"),
            Some((h, p)) => (h.to_string(), Some(p)),
            None => (v.clone(), None),
        }
    };
    let port = match port {
        None => None,
        Some(p) if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) => {
            Some(p.parse::<u16>().map_err(|_| "port out of range")?)
        }
        Some(_) => return Err("bad port"),
    };
    let host = host.strip_suffix('.').unwrap_or(&host);
    // IP literals are compared in one spelling: `[fd00:0:0:0:0:0:0:1]`
    // and `[fd00::1]` (what browsers send) are the same host, and so are
    // `[::ffff:10.0.0.2]` and `10.0.0.2`.
    let host = match host.parse::<IpAddr>() {
        Ok(ip) => ip.to_canonical().to_string(),
        Err(_) if is_dns_name(host) => host.to_string(),
        Err(_) => return Err("not a host name or IP literal"),
    };
    Ok((host, port))
}

fn parse_entry(entry: &str) -> Result<(String, Option<u16>), String> {
    let bad = |why: &str| Err(format!("invalid allowed-hosts entry {entry:?}: {why}"));
    if entry.contains("://") || entry.contains('/') {
        return bad("expected host or host:port, without scheme or path");
    }
    if entry.contains('*') {
        return bad("wildcards are not supported; `*` alone turns the check off");
    }
    match split_host_port(entry) {
        Ok(parsed) => Ok(parsed),
        Err(why) => bad(why),
    }
}

/// axum middleware applying [`GatewayHandle::hosts`]. Mounted outermost
/// in [`crate::router::build`], so even a CORS preflight with a foreign
/// `Host` is refused.
pub async fn host_guard(State(handle): State<GatewayHandle>, req: Request, next: Next) -> Response {
    let policy = &handle.hosts;
    if !policy.enforced() {
        return next.run(req).await;
    }
    let mut values = req.headers().get_all(header::HOST).iter();
    let host = values.next();
    let ok = if values.next().is_some() {
        false
    } else {
        // An absolute-form or HTTP/2 request carries its authority in
        // the URI too; both have to pass.
        let header_ok = host.is_none_or(|v| v.to_str().is_ok_and(|s| policy.allows(s)));
        let authority_ok = req
            .uri()
            .authority()
            .is_none_or(|a| policy.allows(a.as_str()));
        header_ok && authority_ok
    };
    if ok {
        return next.run(req).await;
    }
    tracing::debug!(
        target: "ant_gateway",
        host = ?host,
        path = %req.uri().path(),
        "request refused: Host not allowed"
    );
    crate::error::json_error(
        StatusCode::MISDIRECTED_REQUEST,
        "host not allowed; add it to allowed-hosts to serve it",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loopback(extra: &[&str]) -> HostPolicy {
        HostPolicy::new(extra.iter().copied()).bound_to("127.0.0.1:1633".parse().unwrap())
    }

    #[test]
    fn loopback_names_pass_with_or_without_the_bound_port() {
        let p = loopback(&[]);
        assert!(p.enforced());
        for ok in [
            "localhost",
            "localhost:1633",
            "LOCALHOST:1633",
            "localhost.:1633",
            "127.0.0.1",
            "127.0.0.1:1633",
            "[::1]",
            "[::1]:1633",
            "abc.bzz.localhost:1633",
            "a.b.swarm.localhost",
        ] {
            assert!(p.allows(ok), "{ok}");
        }
    }

    #[test]
    fn foreign_names_and_ports_are_refused() {
        let p = loopback(&[]);
        for bad in [
            "evil.example:1633",
            "evil.example",
            "localhost:1634",
            "127.0.0.1:80",
            "[::1]:1",
            "localhost.evil.example",
            "notlocalhost",
            "evil.localhost.example",
            ".localhost",
            "a..localhost",
            "127.0.0.2:1633",
            "::1",
            "[::1",
            "localhost:",
            "localhost:99999",
            "localhost:16x3",
            "",
        ] {
            assert!(!p.allows(bad), "{bad}");
        }
    }

    #[test]
    fn allowed_hosts_extend_the_list() {
        let p = loopback(&["vibing.at", "node.lan:8080", "[fd00::1]"]);
        assert!(p.allows("vibing.at"));
        assert!(p.allows("VIBING.AT:443"));
        assert!(p.allows("node.lan:8080"));
        assert!(!p.allows("node.lan:8081"));
        assert!(!p.allows("node.lan"));
        assert!(p.allows("[fd00::1]:1633"));
        assert!(!p.allows("sub.vibing.at"));
    }

    #[test]
    fn ip_literal_entries_match_any_spelling() {
        let p = loopback(&[
            "[fd00:0:0:0:0:0:0:1]",
            "[FD00::2]:8080",
            "[::ffff:10.0.0.2]",
            "10.0.0.3",
        ]);
        assert!(p.allows("[fd00::1]:1633"));
        assert!(p.allows("[fd00:0000::0001]"));
        assert!(p.allows("[fd00::2]:8080"));
        assert!(p.allows("[fd00:0:0:0:0:0:0:2]:8080"));
        assert!(!p.allows("[fd00::2]:8081"));
        assert!(p.allows("10.0.0.2:1633"));
        assert!(p.allows("[::ffff:10.0.0.2]:1633"));
        assert!(p.allows("[::ffff:10.0.0.3]"));
        assert!(!p.allows("[fd00::3]"));
        assert!(!p.allows("10.0.0.4"));
        // A mapped loopback is loopback.
        assert!(p.allows("[::ffff:127.0.0.1]:1633"));
    }

    #[test]
    fn default_port_entries_match_a_bare_host() {
        let p = loopback(&["vibing.at:443", "plain.example:80", "node.lan:8080"]);
        assert!(p.allows("vibing.at"));
        assert!(p.allows("vibing.at:443"));
        assert!(!p.allows("vibing.at:8443"));
        assert!(p.allows("plain.example"));
        assert!(!p.allows("plain.example:443"));
        assert!(!p.allows("node.lan"));
    }

    #[test]
    fn star_disables_the_check() {
        assert!(!loopback(&["*"]).enforced());
    }

    #[test]
    fn non_loopback_bind_enforces_only_with_allowed_hosts() {
        let any: SocketAddr = "0.0.0.0:1633".parse().unwrap();
        assert!(!HostPolicy::new(Vec::<String>::new())
            .bound_to(any)
            .enforced());
        let p = HostPolicy::new(["node.lan"]).bound_to(any);
        assert!(p.enforced());
        assert!(p.allows("node.lan:1633"));
        assert!(p.allows("localhost:1633"));
        assert!(!p.allows("evil.example:1633"));
        // A wildcard bind has no own IP: a literal must be listed.
        assert!(!p.allows("192.168.1.5:1633"));
        assert!(!p.allows("0.0.0.0:1633"));
        let listed = HostPolicy::new(["node.lan", "192.168.1.5"]).bound_to(any);
        assert!(listed.allows("192.168.1.5:1633"));
        assert!(!listed.allows("192.168.1.6:1633"));
        // A specific non-loopback bind also accepts its own IP literal.
        let lan = HostPolicy::new(["node.lan"]).bound_to("192.168.1.5:1633".parse().unwrap());
        assert!(lan.allows("192.168.1.5:1633"));
        assert!(!lan.allows("192.168.1.6:1633"));
    }

    #[test]
    fn unbound_policy_enforces_loopback_names_on_any_port() {
        let p = HostPolicy::new(Vec::<String>::new());
        assert!(p.enforced());
        assert!(p.allows("127.0.0.1:41234"));
        assert!(!p.allows("evil.example:41234"));
    }

    #[test]
    fn check_entry_rejects_urls_and_wildcards() {
        for ok in [
            "*",
            "vibing.at",
            "node.lan:1633",
            "[fd00::1]:1633",
            "10.0.0.2",
            "",
            " ",
        ] {
            assert_eq!(HostPolicy::check_entry(ok), Ok(()), "{ok}");
        }
        for bad in [
            "https://vibing.at",
            "vibing.at/ant",
            "*.vibing.at",
            "vibing.at:http",
            "fd00::1",
            "bad host",
        ] {
            assert!(HostPolicy::check_entry(bad).is_err(), "{bad}");
        }
    }
}
