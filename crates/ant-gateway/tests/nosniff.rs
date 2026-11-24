//! Content-type hardening (issue #297, audit GW-02): every response
//! carries `X-Content-Type-Options: nosniff` except a `/bzz` entry stored
//! with an empty content type (bee's rule since ethersphere/bee#5651), and
//! `/bytes?filename=` never turns uploaded bytes into a type a browser
//! would run on the API's origin (HTML, SVG, XML, JavaScript).

mod common;

use ant_control::{ControlAck, ControlCommand, StreamRange};
use ant_gateway::{CorsConfig, HostPolicy};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use common::{
    body_bytes, handle_with_fixture_node, handle_with_fixture_node_and_policy,
    router_with_dispatcher, send,
};

const DATA_REF: &str = "285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296";
const MANIFEST_ROOT: &str = "ab77201f6541a9ceafb98a46c643273cfa397a87798273dd17feb2aa366ce2e6";
/// A 64-byte (128-hex) reference: the gateway's encrypted `/bytes` path.
const ENC_REF: &str = concat!(
    "285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296",
    "0101010101010101010101010101010101010101010101010101010101010101",
);
const PAYLOAD: &[u8] = b"<html><body><script>alert(document.domain)</script></body></html>";

fn slice(range: Option<StreamRange>) -> Vec<u8> {
    match range {
        Some(r) => PAYLOAD[r.start as usize..=r.end_inclusive as usize].to_vec(),
        None => PAYLOAD.to_vec(),
    }
}

/// A node that serves [`PAYLOAD`] for every `/bytes` reference, plain or
/// encrypted, and for every `/bzz` path. The `/bzz` entry's stored
/// content type comes from the path: `empty` → `Some("")` (a collection
/// file with an unknown extension), `untyped` → no metadata at all,
/// anything else → `text/html; charset=utf-8`.
fn html_node() -> axum::Router {
    router_with_dispatcher(|cmd| async move {
        match cmd {
            ControlCommand::StreamBytes {
                range,
                head_only,
                ack,
                ..
            } => {
                let _ = ack
                    .send(ControlAck::BytesStreamStart {
                        total_bytes: PAYLOAD.len() as u64,
                    })
                    .await;
                if !head_only {
                    let _ = ack
                        .send(ControlAck::BytesChunk { data: slice(range) })
                        .await;
                }
                let _ = ack.send(ControlAck::StreamDone).await;
            }
            ControlCommand::GetBytesEncrypted { ack, .. } => {
                let _ = ack
                    .send(ControlAck::Bytes {
                        data: PAYLOAD.to_vec(),
                    })
                    .await;
            }
            ControlCommand::StreamBzz {
                path,
                range,
                head_only,
                ack,
                ..
            } => {
                let content_type = match path.as_str() {
                    "empty" => Some(String::new()),
                    "untyped" => None,
                    _ => Some("text/html; charset=utf-8".to_string()),
                };
                let _ = ack
                    .send(ControlAck::BzzStreamStart {
                        total_bytes: PAYLOAD.len() as u64,
                        reference: vec![0x11; 32],
                        content_type,
                        filename: Some(path),
                        feed: None,
                    })
                    .await;
                if !head_only {
                    let _ = ack
                        .send(ControlAck::BytesChunk { data: slice(range) })
                        .await;
                }
                let _ = ack.send(ControlAck::StreamDone).await;
            }
            _ => {}
        }
    })
}

async fn request(
    router: axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
) -> axum::http::Response<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    send(router, b.body(Body::empty()).unwrap()).await
}

fn nosniff(resp: &axum::http::Response<Body>) -> bool {
    resp.headers()
        .get(header::X_CONTENT_TYPE_OPTIONS)
        .is_some_and(|v| v == "nosniff")
}

fn content_type(resp: &axum::http::Response<Body>) -> String {
    resp.headers()
        .get(header::CONTENT_TYPE)
        .map_or_else(String::new, |v| v.to_str().unwrap().to_string())
}

/// `GET /bytes/<ref>?filename=a.html` (and every other active
/// extension) must not come back as a renderable type, on any of the
/// `/bytes` response shapes: full GET, HEAD, ranges from 0 and from the
/// middle, an empty range, multipart ranges, and the encrypted path.
#[tokio::test]
async fn bytes_filename_never_yields_an_active_content_type() {
    let router = html_node();
    let names = [
        "a.html", "a.HTM", "a.xhtml", "a.svg", "a.xml", "a.xsl", "a.rdf", "a.js", "a.mjs",
    ];
    let shapes: &[(Method, Option<&str>)] = &[
        (Method::GET, None),
        (Method::HEAD, None),
        (Method::GET, Some("bytes=0-9")),
        (Method::GET, Some("bytes=5-20")),
        (Method::GET, Some("bytes=-0")),
        (Method::GET, Some("bytes=0-1,5-6")),
    ];
    for reference in [DATA_REF, ENC_REF] {
        for name in names {
            for (method, range) in shapes {
                let uri = format!("/bytes/{reference}?filename={name}");
                let headers: Vec<(&str, &str)> = range.iter().map(|r| ("range", *r)).collect();
                let resp = request(router.clone(), method.clone(), &uri, &headers).await;
                let what = format!("{method} {uri} range={range:?}");
                assert!(resp.status().is_success(), "{what}: {}", resp.status());
                assert!(nosniff(&resp), "{what}: no nosniff");
                let ct = content_type(&resp);
                if ct.starts_with("multipart/byteranges") {
                    let body = String::from_utf8_lossy(&body_bytes(resp).await).into_owned();
                    assert!(
                        body.contains("Content-Type: application/octet-stream\r\n"),
                        "{what}: multipart part type: {body}"
                    );
                    assert!(!body.contains("html"), "{what}: {body}");
                } else {
                    assert_eq!(ct, "application/octet-stream", "{what}");
                }
            }
        }
    }
}

/// A passive filename hint still picks the type (`?filename=` is how a
/// browser gets a playable `/bytes` media file), and still has
/// `nosniff`.
#[tokio::test]
async fn bytes_filename_still_types_passive_content() {
    let router = handle_with_fixture_node();
    for (name, want) in [
        ("tile.wav", "audio/wav"),
        ("tile.png", "image/png"),
        ("notes.txt", "text/plain; charset=utf-8"),
        ("paper.pdf", "application/pdf"),
        ("data.json", "application/json"),
        (
            "report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
    ] {
        let uri = format!("/bytes/{DATA_REF}?filename={name}");
        let resp = request(router.clone(), Method::GET, &uri, &[]).await;
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
        assert_eq!(content_type(&resp), want, "{name}");
        assert!(nosniff(&resp), "{name}");
    }
}

/// Every kind of answer carries `nosniff`: content, JSON, errors from a
/// handler, the 501 fallback, a 416, a preflight, and the refusals the
/// outer `Host`/CORS guards return before any route runs.
#[tokio::test]
async fn every_response_carries_nosniff() {
    let router = handle_with_fixture_node();
    type Case = (
        Method,
        String,
        Vec<(&'static str, &'static str)>,
        StatusCode,
    );
    let cases: Vec<Case> = vec![
        (Method::GET, "/health".into(), vec![], StatusCode::OK),
        (Method::GET, "/node".into(), vec![], StatusCode::OK),
        (
            Method::GET,
            format!("/bytes/{DATA_REF}"),
            vec![],
            StatusCode::OK,
        ),
        (
            Method::HEAD,
            format!("/bytes/{DATA_REF}"),
            vec![],
            StatusCode::OK,
        ),
        (
            Method::GET,
            format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png"),
            vec![],
            StatusCode::OK,
        ),
        (
            Method::GET,
            format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png"),
            vec![("range", "bytes=0-9")],
            StatusCode::PARTIAL_CONTENT,
        ),
        (
            Method::GET,
            format!("/bytes/{DATA_REF}"),
            vec![("range", "bytes=999999-")],
            StatusCode::RANGE_NOT_SATISFIABLE,
        ),
        (
            Method::GET,
            "/bytes/abcd".into(),
            vec![],
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::GET,
            "/no-such-endpoint".into(),
            vec![],
            StatusCode::NOT_IMPLEMENTED,
        ),
    ];
    for (method, uri, headers, want) in cases {
        let resp = request(router.clone(), method.clone(), &uri, &headers).await;
        assert_eq!(resp.status(), want, "{method} {uri}");
        assert!(nosniff(&resp), "{method} {uri} ({want}): no nosniff");
    }

    // A preflight (answered by the CORS layer before any route) and the
    // refusals from the outer guards.
    let guarded = handle_with_fixture_node_and_policy(
        CorsConfig::new(["http://localhost:3000"]),
        HostPolicy::new(std::iter::empty::<&str>()).bound_to("127.0.0.1:1633".parse().unwrap()),
    );
    let resp = request(
        guarded.clone(),
        Method::GET,
        "/health",
        &[("host", "evil.example")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::MISDIRECTED_REQUEST);
    assert!(nosniff(&resp), "Host refusal: no nosniff");
    let resp = request(
        guarded.clone(),
        Method::OPTIONS,
        "/bytes",
        &[
            ("origin", "http://localhost:3000"),
            ("access-control-request-method", "POST"),
        ],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    assert!(nosniff(&resp), "preflight: no nosniff");
    let resp = request(
        guarded,
        Method::POST,
        "/bytes",
        &[
            ("origin", "http://evil.example"),
            ("sec-fetch-site", "cross-site"),
        ],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert!(nosniff(&resp), "cross-origin write refusal: no nosniff");
}

/// A `/bzz` entry stored with an EMPTY content type (a collection file
/// with an unknown extension) is served without one, so it must stay
/// sniffable — no `nosniff`, on every response shape. An entry with a
/// declared type, or with no type metadata at all (served as
/// `application/octet-stream`), keeps `nosniff`.
#[tokio::test]
async fn bzz_entry_with_empty_content_type_has_no_nosniff() {
    let router = html_node();
    let shapes: &[(Method, Option<&str>)] = &[
        (Method::GET, None),
        (Method::HEAD, None),
        (Method::GET, Some("bytes=0-9")),
        (Method::GET, Some("bytes=5-20")),
        (Method::GET, Some("bytes=-0")),
        (Method::GET, Some("bytes=0-1,5-6")),
    ];
    for (path, want_nosniff) in [("empty", false), ("untyped", true), ("page.html", true)] {
        for (method, range) in shapes {
            let uri = format!("/bzz/{MANIFEST_ROOT}/{path}");
            let headers: Vec<(&str, &str)> = range.iter().map(|r| ("range", *r)).collect();
            let resp = request(router.clone(), method.clone(), &uri, &headers).await;
            let what = format!("{method} {uri} range={range:?}");
            assert!(resp.status().is_success(), "{what}: {}", resp.status());
            assert_eq!(nosniff(&resp), want_nosniff, "{what}");
        }
    }
    // The empty-type entry really goes out without a type to sniff
    // past; the untyped one is pinned to octet-stream.
    let resp = request(
        router.clone(),
        Method::GET,
        &format!("/bzz/{MANIFEST_ROOT}/empty"),
        &[],
    )
    .await;
    assert_eq!(content_type(&resp), "");
    let resp = request(
        router,
        Method::GET,
        &format!("/bzz/{MANIFEST_ROOT}/untyped"),
        &[],
    )
    .await;
    assert_eq!(content_type(&resp), "application/octet-stream");
}
