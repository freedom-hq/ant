//! CORS behaviour (PLAN.md J.4.8). Freedom configures
//! `cors-allowed-origins: "null"` and drives the HTTP API from opaque
//! (`null`) origin dweb pages; these tests lock the bee-compatible
//! preflight + actual-response header shape the browser requires.

mod common;

use ant_gateway::CorsConfig;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{send, snapshot_with_one_peer, status_only_router, status_router_with_cors};

fn null_cors_router() -> axum::Router {
    status_router_with_cors(snapshot_with_one_peer(), CorsConfig::new(["null"]))
}

/// A preflight from the `null` origin is answered 204 with the
/// allow-origin / methods / headers / max-age set, and never reaches a
/// route handler.
#[tokio::test]
async fn preflight_from_null_origin_is_allowed() {
    let resp = send(
        null_cors_router(),
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/bzz")
            .header("origin", "null")
            .header("access-control-request-method", "POST")
            .header(
                "access-control-request-headers",
                "swarm-postage-batch-id, content-type",
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let h = resp.headers();
    assert_eq!(h.get("access-control-allow-origin").unwrap(), "null");
    assert!(h.contains_key("access-control-allow-methods"));
    // Requested headers are echoed verbatim so any bee-js header passes.
    assert_eq!(
        h.get("access-control-allow-headers").unwrap(),
        "swarm-postage-batch-id, content-type",
    );
    assert!(h.contains_key("access-control-max-age"));
    assert!(h
        .get("vary")
        .unwrap()
        .to_str()
        .unwrap()
        .to_ascii_lowercase()
        .contains("origin"));
}

/// A preflight that lists no `Access-Control-Request-Headers` gets the
/// default allow-header set, which must carry `Swarm-Cache` and
/// `Swarm-Postage-Stamp` (bee parity,
/// <https://github.com/freedom-hq/ant/issues/273>,
/// <https://github.com/ethersphere/bee/pull/5698>).
#[tokio::test]
async fn preflight_default_allow_headers_include_swarm_cache_and_postage_stamp() {
    let resp = send(
        null_cors_router(),
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/chunks")
            .header("origin", "null")
            .header("access-control-request-method", "POST")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let h = resp.headers();
    assert_eq!(h.get("access-control-allow-origin").unwrap(), "null");
    let allow: Vec<String> = h
        .get("access-control-allow-headers")
        .unwrap()
        .to_str()
        .unwrap()
        .split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .collect();
    for name in ["swarm-cache", "swarm-postage-stamp"] {
        assert!(allow.iter().any(|a| a == name), "{name} missing: {allow:?}");
    }
}

/// An actual (non-preflight) request from an allowed origin gets the
/// allow-origin echo plus the expose-headers list bee-js needs to read
/// `Swarm-Tag-Uid` etc.
#[tokio::test]
async fn actual_request_gets_allow_origin_and_expose_headers() {
    let resp = send(
        null_cors_router(),
        Request::builder()
            .method(Method::GET)
            .uri("/node")
            .header("origin", "null")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    let h = resp.headers();
    assert_eq!(h.get("access-control-allow-origin").unwrap(), "null");
    let expose = h
        .get("access-control-expose-headers")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(expose.contains("Swarm-Tag-Uid"), "expose: {expose}");
}

/// A disallowed origin gets no allow-origin header (browser blocks it),
/// but the request itself still succeeds for same-origin/non-browser
/// callers.
#[tokio::test]
async fn disallowed_origin_gets_no_cors_headers() {
    let resp = send(
        null_cors_router(),
        Request::builder()
            .method(Method::GET)
            .uri("/node")
            .header("origin", "https://evil.example")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

/// With CORS disabled (no origins configured) the middleware is a no-op
/// even when an Origin is present — matches a bee node started without
/// `cors-allowed-origins`.
#[tokio::test]
async fn cors_disabled_adds_no_headers() {
    let resp = send(
        status_only_router(snapshot_with_one_peer()),
        Request::builder()
            .method(Method::GET)
            .uri("/node")
            .header("origin", "null")
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("access-control-allow-origin").is_none());
}

/// `*` echoes any concrete origin (so credentialed requests stay valid)
/// and a preflight against it succeeds.
#[tokio::test]
async fn wildcard_echoes_request_origin() {
    let router = status_router_with_cors(snapshot_with_one_peer(), CorsConfig::new(["*"]));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/node")
            .header("origin", "https://app.example")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://app.example",
    );
}
