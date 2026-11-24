//! The gateway's defences against web pages (issue #312): the `Host`
//! allowlist (DNS rebinding), the global cross-origin and same-origin
//! write block (a port of bee's `isForbiddenCrossOriginRequest`,
//! ethersphere/bee PR 5651) and the WebSocket `Origin` check. The
//! spending routes' extra guard is covered in `chain.rs`.

mod common;

use ant_gateway::{CorsConfig, HostPolicy};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{handle_with_fixture_node_and_policy, send};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;

fn router(cors: CorsConfig, hosts: HostPolicy) -> axum::Router {
    handle_with_fixture_node_and_policy(cors, hosts)
}

/// A policy resolved against a loopback listener on `:1633`, as
/// `Gateway::serve` does.
fn loopback(extra: &[&str]) -> HostPolicy {
    HostPolicy::new(extra.iter().copied()).bound_to("127.0.0.1:1633".parse().unwrap())
}

async fn status(
    router: axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
) -> StatusCode {
    let mut b = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    send(router, b.body(Body::empty()).unwrap()).await.status()
}

#[tokio::test]
async fn host_allowlist_on_a_loopback_bind() {
    let r = router(CorsConfig::default(), loopback(&[]));
    for host in [
        "evil.example:1633",
        "evil.example",
        "localhost:1634",
        "localhost.evil.example:1633",
    ] {
        assert_eq!(
            status(r.clone(), Method::GET, "/health", &[("host", host)]).await,
            StatusCode::MISDIRECTED_REQUEST,
            "{host}",
        );
    }
    for host in [
        "localhost:1633",
        "localhost",
        "127.0.0.1:1633",
        "[::1]:1633",
        "abc.bzz.localhost:1633",
    ] {
        assert_eq!(
            status(r.clone(), Method::GET, "/health", &[("host", host)]).await,
            StatusCode::OK,
            "{host}",
        );
    }
    // No Host at all (an in-process caller, HTTP/1.0) passes.
    assert_eq!(
        status(r.clone(), Method::GET, "/health", &[]).await,
        StatusCode::OK
    );
    // Two Host headers are refused.
    let resp = send(
        r.clone(),
        Request::builder()
            .uri("/health")
            .header("host", "localhost:1633")
            .header("host", "evil.example:1633")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::MISDIRECTED_REQUEST);
    // The refusal is a JSON body, and a preflight is refused too: the
    // check runs before CORS.
    let resp = send(
        router(CorsConfig::new(["*"]), loopback(&[])),
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/bzz")
            .header("host", "evil.example:1633")
            .header("origin", "http://evil.example:1633")
            .header("access-control-request-method", "POST")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::MISDIRECTED_REQUEST);
    assert!(resp.headers().get("access-control-allow-origin").is_none());
    let body: serde_json::Value = serde_json::from_slice(&common::body_bytes(resp).await).unwrap();
    assert_eq!(body["code"], 421);
}

#[tokio::test]
async fn allowed_hosts_extend_the_list_and_star_disables_it() {
    let r = router(CorsConfig::default(), loopback(&["vibing.at"]));
    assert_eq!(
        status(r.clone(), Method::GET, "/health", &[("host", "vibing.at")]).await,
        StatusCode::OK
    );
    assert_eq!(
        status(r, Method::GET, "/health", &[("host", "evil.example")]).await,
        StatusCode::MISDIRECTED_REQUEST
    );
    let r = router(CorsConfig::default(), loopback(&["*"]));
    assert_eq!(
        status(r, Method::GET, "/health", &[("host", "evil.example:1633")]).await,
        StatusCode::OK
    );
    // A non-loopback bind without allowed-hosts isn't checked.
    let r = router(
        CorsConfig::default(),
        HostPolicy::new(Vec::<String>::new()).bound_to("0.0.0.0:1633".parse().unwrap()),
    );
    assert_eq!(
        status(r, Method::GET, "/health", &[("host", "evil.example:1633")]).await,
        StatusCode::OK
    );
}

/// A CORS-simple write from a foreign page is refused before any
/// handler runs, whatever the CORS list says about other origins.
#[tokio::test]
async fn cross_origin_writes_are_refused() {
    let foreign: [&[(&str, &str)]; 4] = [
        &[("origin", "https://evil")],
        &[("sec-fetch-site", "cross-site")],
        &[("origin", "https://evil"), ("sec-fetch-site", "cross-site")],
        &[
            ("origin", "https://evil.app.example"),
            ("sec-fetch-site", "same-site"),
        ],
    ];
    for cors in [
        CorsConfig::default(),
        CorsConfig::new(["https://app.example"]),
        CorsConfig::new(["null"]),
    ] {
        let r = router(cors.clone(), loopback(&[]));
        for (method, uri) in [
            (Method::POST, "/tags"),
            (Method::POST, "/pins/x"),
            (Method::DELETE, "/pins/x"),
            (Method::POST, "/bytes"),
        ] {
            for h in foreign {
                assert_eq!(
                    status(r.clone(), method.clone(), uri, h).await,
                    StatusCode::FORBIDDEN,
                    "{method} {uri} {h:?} {cors:?}",
                );
            }
        }
        // Reads are not writes.
        assert_eq!(
            status(
                r.clone(),
                Method::GET,
                "/health",
                &[("origin", "https://evil"), ("sec-fetch-site", "cross-site")],
            )
            .await,
            StatusCode::OK,
        );
    }
}

/// `/bzz` pages are served on the API's own origin, so `same-origin`
/// isn't proof of a trusted caller: it needs an exactly listed origin.
#[tokio::test]
async fn same_origin_writes_need_an_exactly_listed_origin() {
    let page = [
        ("origin", "http://127.0.0.1:1633"),
        ("sec-fetch-site", "same-origin"),
        ("host", "127.0.0.1:1633"),
    ];
    let old_browser = [
        ("origin", "http://127.0.0.1:1633"),
        ("host", "127.0.0.1:1633"),
    ];
    for cors in [
        CorsConfig::default(),
        CorsConfig::new(["*"]),
        CorsConfig::new(["null"]),
    ] {
        let r = router(cors.clone(), loopback(&[]));
        for h in [
            &page[..],
            &old_browser[..],
            &[("sec-fetch-site", "same-origin")],
        ] {
            assert_eq!(
                status(r.clone(), Method::POST, "/tags", h).await,
                StatusCode::FORBIDDEN,
                "{h:?} {cors:?}",
            );
        }
    }
    let r = router(CorsConfig::new(["http://127.0.0.1:1633"]), loopback(&[]));
    for h in [&page[..], &old_browser[..]] {
        assert_eq!(
            status(r.clone(), Method::POST, "/tags", h).await,
            StatusCode::CREATED,
            "{h:?}",
        );
    }
}

/// What still gets through: non-browser callers (neither header), a
/// user-typed request, and a cross-site page whose origin is in the CORS
/// list, `null` and `*` included (bee's rule; Freedom's dweb pages
/// upload from `Origin: null`).
#[tokio::test]
async fn allowed_writes_still_pass() {
    let r = router(CorsConfig::default(), loopback(&[]));
    for h in [
        &[][..],
        &[("host", "localhost:1633")],
        &[("sec-fetch-site", "none")],
    ] {
        assert_eq!(
            status(r.clone(), Method::POST, "/tags", h).await,
            StatusCode::CREATED,
            "{h:?}"
        );
    }
    for (cors, origin) in [
        (CorsConfig::new(["null"]), "null"),
        (CorsConfig::new(["*"]), "https://anything.example"),
        (
            CorsConfig::new(["https://app.example"]),
            "https://app.example",
        ),
        (
            CorsConfig::new(["https://*.bzz.freedom.baby"]),
            "https://abc.bzz.freedom.baby",
        ),
    ] {
        let r = router(cors, loopback(&[]));
        for h in [
            &[("origin", origin), ("sec-fetch-site", "cross-site")][..],
            &[("origin", origin)],
        ] {
            assert_eq!(
                status(r.clone(), Method::POST, "/tags", h).await,
                StatusCode::CREATED,
                "{h:?}"
            );
        }
    }
}

/// Serve `router` on an ephemeral loopback port; return the ws base.
async fn listen(router: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("ws://{addr}")
}

/// The handshake status for `path` with an optional `Origin`: `101` when
/// it upgraded.
async fn ws_status(base: &str, path: &str, origin: Option<&str>) -> u16 {
    let mut req = format!("{base}{path}").into_client_request().unwrap();
    if let Some(o) = origin {
        req.headers_mut()
            .insert("origin", HeaderValue::from_str(o).unwrap());
    }
    match tokio_tungstenite::connect_async(req).await {
        Ok((_ws, resp)) => resp.status().as_u16(),
        Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => resp.status().as_u16(),
        Err(e) => panic!("{path}: handshake failed oddly: {e}"),
    }
}

#[tokio::test]
async fn websocket_upgrades_check_the_origin() {
    let paths = [
        "/chunks/stream".to_string(),
        format!("/gsoc/subscribe/{}", "ab".repeat(32)),
        "/pss/subscribe/test".to_string(),
    ];
    for (cors, refused) in [
        (CorsConfig::default(), &["https://evil", "null"][..]),
        // `null` and `*` grant reads, not sockets.
        (CorsConfig::new(["null"]), &["null"]),
        (CorsConfig::new(["*"]), &["https://evil", "null"]),
        (
            CorsConfig::new(["https://*.bzz.freedom.baby"]),
            &["https://abc.bzz.freedom.baby"],
        ),
    ] {
        let base = listen(router(cors.clone(), HostPolicy::default())).await;
        for path in &paths {
            for origin in refused {
                assert_eq!(
                    ws_status(&base, path, Some(origin)).await,
                    403,
                    "{path} {origin} {cors:?}"
                );
            }
            assert_eq!(ws_status(&base, path, None).await, 101, "{path} {cors:?}");
        }
    }
    // An exactly listed origin may open a socket.
    let base = listen(router(
        CorsConfig::new(["https://app.example"]),
        HostPolicy::default(),
    ))
    .await;
    for path in &paths {
        assert_eq!(
            ws_status(&base, path, Some("https://app.example")).await,
            101,
            "{path}"
        );
    }
}
