//! Bee-compatible CORS (PLAN.md J.4.8).
//!
//! Freedom's dweb pages issue `fetch` / SOC uploads from the page's own
//! origin, and Freedom configures the node with `cors-allowed-origins:
//! "null"` (a page loaded from a `bzz://` / `file://` / sandboxed
//! context reports its `Origin` as the literal string `null`). Without
//! the matching CORS response headers the browser blocks every one of
//! those requests before `antd` ever sees a useful status, so the whole
//! in-page Swarm API is dead.
//!
//! This mirrors what bee does with gorilla/handlers `CORS`:
//!
//! - Allowed origins come from config. `*` allows any origin; the
//!   literal `null` allows opaque-origin pages (what Freedom needs);
//!   otherwise an exact, case-insensitive origin match is required.
//! - When the request's `Origin` is allowed we **echo it back** in
//!   `Access-Control-Allow-Origin` (not `*`) and set `Vary: Origin`, so
//!   the response stays correct for credentialed requests and shared
//!   caches.
//! - A CORS **preflight** (`OPTIONS` carrying
//!   `Access-Control-Request-Method`) is answered directly with `204`
//!   and the allow-method / allow-header / max-age headers — it never
//!   reaches the route handlers.
//! - Actual responses also advertise `Access-Control-Expose-Headers`
//!   for the `Swarm-*` response headers bee-js reads (tag uid, feed
//!   index, …) which are otherwise invisible to cross-origin JS.
//!
//! When no origins are configured the middleware is a no-op (no CORS
//! headers), matching a bee node started without `cors-allowed-origins`.

use axum::extract::{Request, State};
use axum::http::{header, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;

use crate::handle::GatewayHandle;

/// Methods bee allows cross-origin. Covers the full Tier-A/Tier-B verb
/// set (`PATCH` for stamp topup/dilute, `DELETE` for pin removal) so a
/// preflight for any routed endpoint passes.
const ALLOW_METHODS: &str = "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS";

/// Default `Access-Control-Allow-Headers` returned on a preflight that
/// doesn't enumerate its own `Access-Control-Request-Headers`. Includes
/// the bee upload-control headers (`Swarm-*`) plus the usual content /
/// auth set so bee-js never trips the browser's preflight check.
const DEFAULT_ALLOW_HEADERS: &str = "Origin, Accept, Authorization, Content-Type, \
X-Requested-With, Range, \
Swarm-Tag, Swarm-Pin, Swarm-Encrypt, Swarm-Collection, Swarm-Index-Document, \
Swarm-Error-Document, Swarm-Postage-Batch-Id, Swarm-Deferred-Upload, \
Swarm-Redundancy-Level, Swarm-Redundancy-Strategy, Swarm-Redundancy-Fallback-Mode, \
Swarm-Chunk-Retrieval-Timeout, Swarm-Feed-Index, Swarm-Feed-Index-Next, \
Swarm-Soc-Signature, Swarm-Act, Swarm-Act-Publisher, Swarm-Act-History-Address, \
Gas-Price, Gas-Limit";

/// `Access-Control-Expose-Headers`: the response headers cross-origin
/// JS would otherwise be unable to read. Modern bee-js reads
/// `Swarm-Tag` (bee's upload-session echo); older consumers read ant's
/// legacy `Swarm-Tag-Uid`; the feed-index pair drives feed polling; the
/// content headers let a page-side `fetch` introspect length / type /
/// disposition / range.
const EXPOSE_HEADERS: &str = "Swarm-Tag, Swarm-Tag-Uid, Swarm-Feed-Index, Swarm-Feed-Index-Next, \
Swarm-Soc-Signature, Swarm-Act-History-Address, ETag, \
Content-Type, Content-Length, Content-Disposition, Content-Range, Accept-Ranges";

/// How long (seconds) a browser may cache a preflight result. One day,
/// matching bee's default, so a busy page doesn't re-preflight every
/// upload.
const MAX_AGE_SECS: &str = "86400";

/// Parsed `cors-allowed-origins` policy. Construction lowercases and
/// classifies each entry once so the per-request hot path is a couple
/// of cheap comparisons.
#[derive(Debug, Default, Clone)]
pub struct CorsConfig {
    /// Any origin allowed (`*` was configured).
    allow_all: bool,
    /// The opaque `null` origin allowed (Freedom's case).
    allow_null: bool,
    /// Exact origins (already lowercased, e.g. `https://app.example`).
    origins: Vec<String>,
}

impl CorsConfig {
    /// Build from the raw `cors-allowed-origins` values (bee accepts a
    /// list; a single `"*"` or `"null"` are the common shapes). Empty
    /// input → CORS disabled.
    pub fn new<I, S>(origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut cfg = Self::default();
        for raw in origins {
            let o = raw.as_ref().trim();
            if o.is_empty() {
                continue;
            }
            match o {
                "*" => cfg.allow_all = true,
                _ if o.eq_ignore_ascii_case("null") => cfg.allow_null = true,
                // Origins are compared case-insensitively on scheme+host
                // (RFC 6454); lowercasing the whole token is sufficient
                // because origins never contain a path.
                _ => cfg.origins.push(o.to_ascii_lowercase()),
            }
        }
        cfg
    }

    /// True when no origin is configured — the middleware then adds no
    /// CORS headers at all.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        !self.allow_all && !self.allow_null && self.origins.is_empty()
    }

    /// Resolve the `Access-Control-Allow-Origin` value to echo for a
    /// request whose `Origin` header is `origin`, or `None` when the
    /// origin isn't allowed. We always echo the concrete origin (even
    /// for `*`) so credentialed requests stay valid and the `Vary:
    /// Origin` we set is meaningful.
    fn allow_origin_value(&self, origin: &str) -> Option<String> {
        if origin.eq_ignore_ascii_case("null") {
            return (self.allow_null || self.allow_all).then(|| "null".to_string());
        }
        if self.allow_all {
            return Some(origin.to_string());
        }
        let lower = origin.to_ascii_lowercase();
        self.origins
            .iter()
            .any(|o| o == &lower)
            .then(|| origin.to_string())
    }
}

const VARY: HeaderName = HeaderName::from_static("vary");
const ACA_ORIGIN: HeaderName = HeaderName::from_static("access-control-allow-origin");
const ACA_METHODS: HeaderName = HeaderName::from_static("access-control-allow-methods");
const ACA_HEADERS: HeaderName = HeaderName::from_static("access-control-allow-headers");
const ACA_EXPOSE: HeaderName = HeaderName::from_static("access-control-expose-headers");
const ACA_MAX_AGE: HeaderName = HeaderName::from_static("access-control-max-age");
const ACR_METHOD: HeaderName = HeaderName::from_static("access-control-request-method");
const ACR_HEADERS: HeaderName = HeaderName::from_static("access-control-request-headers");

/// axum middleware applying [`CorsConfig`]. Mounted via
/// `from_fn_with_state` so it sees the same [`GatewayHandle`] state the
/// handlers do. A preflight is short-circuited; every other request is
/// passed through and decorated with the allow-origin / expose headers.
pub async fn cors_middleware(
    State(handle): State<GatewayHandle>,
    req: Request,
    next: Next,
) -> Response {
    let cfg = handle.cors.clone();
    if cfg.is_disabled() {
        return next.run(req).await;
    }

    // The `Origin` header is what the policy keys on. A same-origin /
    // non-browser request without it just flows through untouched.
    let origin = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);

    let allow_origin = origin.as_deref().and_then(|o| cfg.allow_origin_value(o));

    let is_preflight = req.method() == Method::OPTIONS && req.headers().contains_key(&ACR_METHOD);

    if is_preflight {
        return preflight_response(&req, allow_origin.as_deref());
    }

    let mut resp = next.run(req).await;
    decorate_actual(&mut resp, allow_origin.as_deref());
    resp
}

/// Guard for the routes that spend the node wallet's **xDAI**
/// (`POST /v0/storage/buy`, `POST /v0/storage/extend`,
/// `POST /v0/settlement/deposit`): refuse any request a web page made,
/// unless the page's origin is listed exactly in `cors-allowed-origins`.
///
/// These routes take only query parameters, so a page can send them as
/// CORS-*simple* requests (`fetch(url, {method: 'POST', mode:
/// 'no-cors'})`): the browser sends them without a preflight and only
/// hides the response, which is too late — the swap and the buy have
/// already happened. So the policy can't live in the CORS headers; the
/// request itself has to be refused.
///
/// A browser always sends `Origin` on a `POST` (and `Sec-Fetch-Site` on
/// any request); the intended callers — Freedom desktop's main process,
/// the ant-ffi host app, `curl` — send neither. So:
///
/// - no `Origin` and no `Sec-Fetch-Site` (or `Sec-Fetch-Site: none`, a
///   user-typed request): allowed;
/// - an `Origin` listed exactly in `cors-allowed-origins`: allowed (the
///   operator named that site);
/// - anything else, including `null` (every `bzz://` / sandboxed dweb
///   page) and origins only matched by `*`, and a page served by the
///   gateway itself: `403`. `null` and `*` are for reading and uploading,
///   not for spending the wallet.
pub async fn wallet_spend_guard(
    State(handle): State<GatewayHandle>,
    req: Request,
    next: Next,
) -> Response {
    if browser_request_allowed(&handle.cors, req.headers()) {
        next.run(req).await
    } else {
        crate::error::json_error(
            StatusCode::FORBIDDEN,
            "this route spends the node wallet's xDAI and does not accept requests from web pages; \
             list the page's origin explicitly in cors-allowed-origins to allow it",
        )
    }
}

/// The decision behind [`wallet_spend_guard`].
fn browser_request_allowed(cfg: &CorsConfig, h: &axum::http::HeaderMap) -> bool {
    let origin = h.get(header::ORIGIN).map(|v| v.to_str().unwrap_or(""));
    match origin {
        Some(o) => {
            // Only an exact listed origin; `*` and `null` don't count.
            let o = o.trim().to_ascii_lowercase();
            !o.is_empty() && o != "null" && cfg.origins.iter().any(|a| a == &o)
        }
        None => h
            .get(SEC_FETCH_SITE)
            .is_none_or(|v| v.as_bytes().eq_ignore_ascii_case(b"none")),
    }
}

const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// Build the `204 No Content` answer to a CORS preflight. When the
/// origin is disallowed we still return `204` (matching bee/gorilla) but
/// without the allow-origin header, so the browser cleanly blocks the
/// real request rather than erroring on a malformed preflight.
fn preflight_response(req: &Request, allow_origin: Option<&str>) -> Response {
    let mut resp = Response::new(axum::body::Body::empty());
    *resp.status_mut() = StatusCode::NO_CONTENT;
    let h = resp.headers_mut();
    if let Some(origin) = allow_origin {
        if let Ok(v) = HeaderValue::from_str(origin) {
            h.insert(ACA_ORIGIN, v);
        }
        h.insert(ACA_METHODS, HeaderValue::from_static(ALLOW_METHODS));
        // Echo the requested headers verbatim when present (so a custom
        // bee-js header always passes); otherwise advertise our default
        // bee header set.
        let allow_headers = req
            .headers()
            .get(&ACR_HEADERS)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| HeaderValue::from_str(s).ok())
            .unwrap_or_else(|| HeaderValue::from_static(DEFAULT_ALLOW_HEADERS));
        h.insert(ACA_HEADERS, allow_headers);
        h.insert(ACA_MAX_AGE, HeaderValue::from_static(MAX_AGE_SECS));
        append_vary_origin(h);
    }
    resp
}

/// Decorate a real (non-preflight) response with the allow-origin and
/// expose-headers entries. No-op when the origin wasn't allowed.
fn decorate_actual(resp: &mut Response, allow_origin: Option<&str>) {
    let Some(origin) = allow_origin else { return };
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(origin) {
        h.insert(ACA_ORIGIN, v);
    }
    h.insert(ACA_EXPOSE, HeaderValue::from_static(EXPOSE_HEADERS));
    append_vary_origin(h);
}

/// Add `Origin` to the `Vary` header without clobbering an existing
/// value (a content handler may already vary on `Accept`).
fn append_vary_origin(h: &mut axum::http::HeaderMap) {
    match h.get(&VARY).and_then(|v| v.to_str().ok()) {
        Some(existing) if existing.to_ascii_lowercase().contains("origin") => {}
        Some(existing) => {
            if let Ok(v) = HeaderValue::from_str(&format!("{existing}, Origin")) {
                h.insert(VARY, v);
            }
        }
        None => {
            h.insert(VARY, HeaderValue::from_static("Origin"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_when_empty() {
        assert!(CorsConfig::new(Vec::<String>::new()).is_disabled());
        assert!(CorsConfig::new([" ", ""]).is_disabled());
    }

    #[test]
    fn null_origin_allowed_only_when_configured() {
        let cfg = CorsConfig::new(["null"]);
        assert_eq!(cfg.allow_origin_value("null").as_deref(), Some("null"));
        assert_eq!(cfg.allow_origin_value("https://x.example"), None);
    }

    #[test]
    fn wildcard_echoes_origin() {
        let cfg = CorsConfig::new(["*"]);
        assert_eq!(
            cfg.allow_origin_value("https://app.example").as_deref(),
            Some("https://app.example"),
        );
        // `*` also covers the opaque origin.
        assert_eq!(cfg.allow_origin_value("null").as_deref(), Some("null"));
    }

    #[test]
    fn exact_match_is_case_insensitive_on_host() {
        let cfg = CorsConfig::new(["https://App.Example"]);
        assert_eq!(
            cfg.allow_origin_value("https://app.example").as_deref(),
            Some("https://app.example"),
        );
        assert_eq!(cfg.allow_origin_value("https://evil.example"), None);
    }
}
