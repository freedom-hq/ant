//! D.2.3 retrieval endpoints, replayed end-to-end against the same
//! `bzz_fixture.rs` chunks `ant-retrieval` already locks against. The
//! fake "node loop" in `common::handle_with_fixture_node` answers the
//! same `ControlCommand` shape production hits, so these tests exercise
//! the gateway's HTTP layer + the joiner / mantaray / cache machinery
//! without needing libp2p.
//!
//! Fixture facts (see `ant-retrieval/tests/bzz_fixture.rs`):
//! - Manifest root:  `ab77201f6541a9ceafb98a46c643273cfa397a87798273dd17feb2aa366ce2e6`
//! - Path:           `13/4358/2645.png`
//! - Data ref:       `285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296`
//! - Body sha-256:   `2fedb435506d7f61f6c1014d94a7422f53e4a1a6bdd1ff8231c1215033e5ea3d`
//! - Body length:    18 604 bytes (a 256x256 PNG tile)

mod common;

use ant_control::{ControlAck, ControlCommand};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use common::{body_bytes, fixture_dir, handle_with_fixture_node, router_with_dispatcher, send};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::Mutex;

const MANIFEST_ROOT: &str = "ab77201f6541a9ceafb98a46c643273cfa397a87798273dd17feb2aa366ce2e6";
const DATA_REF: &str = "285e4c1564cce481fcb21039208795b86ef42042cc0bb45b9d7f16d638d3c296";
const BODY_LEN: usize = 18_604;
const BODY_SHA256: &str = "2fedb435506d7f61f6c1014d94a7422f53e4a1a6bdd1ff8231c1215033e5ea3d";

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// `GET /bzz/{root}/{path}` walks the manifest, joins the data tree,
/// and returns the file body. Verifies the same byte stream as
/// `ant-retrieval/tests/bzz_fixture.rs` plus the bee-shaped headers
/// (`Content-Type: image/png`, `Accept-Ranges: bytes`).
#[tokio::test]
async fn bzz_with_path_returns_fixture_png() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png",
    );
    assert_eq!(resp.headers().get(header::ACCEPT_RANGES).unwrap(), "bytes",);
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
    assert!(bytes.starts_with(&[0x89, b'P', b'N', b'G']));
}

/// `HEAD /bzz/...` must mirror the `GET` headers but return an empty body.
#[tokio::test]
async fn bzz_head_omits_body() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png",
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        BODY_LEN.to_string().as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert!(bytes.is_empty(), "HEAD must not carry a body");
}

/// `Range: bytes=0-1023` returns 206 + the requested slice + the right
/// `Content-Range` header.
#[tokio::test]
async fn bzz_range_request_returns_partial_content() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=0-1023")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes 0-1023/{BODY_LEN}").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 1024);
}

/// `/bzz/<root>` and `/bytes/<root>` must dispatch the joiner command
/// with a `max_bytes` ceiling well above the joiner's CLI-grade default
/// (32 MiB). Real bzz sites routinely carry single files >32 MiB; the
/// gateway has to opt in to a larger ceiling or every video / archive
/// 502s at the joiner step. Regression for the
/// "join data ...: file too large: span 36879400 bytes, cap 33554432
/// bytes" report from freedom-browser.
#[tokio::test]
async fn gateway_raises_joiner_max_bytes_above_cli_default() {
    use ant_retrieval::DEFAULT_MAX_FILE_BYTES;

    let captured: Arc<Mutex<Vec<Option<u64>>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_for_dispatch = captured.clone();
    let router = router_with_dispatcher(move |cmd| {
        let captured = captured_for_dispatch.clone();
        async move {
            match cmd {
                ControlCommand::StreamBytes { max_bytes, ack, .. } => {
                    captured.lock().await.push(max_bytes);
                    let _ = ack
                        .send(ControlAck::BytesStreamStart { total_bytes: 4 })
                        .await;
                    let _ = ack
                        .send(ControlAck::BytesChunk {
                            data: b"stub".to_vec(),
                        })
                        .await;
                    let _ = ack.send(ControlAck::StreamDone).await;
                }
                ControlCommand::StreamBzz { max_bytes, ack, .. } => {
                    captured.lock().await.push(max_bytes);
                    let _ = ack
                        .send(ControlAck::BzzStreamStart {
                            total_bytes: 4,
                            reference: vec![0u8; 32],
                            content_type: Some("text/plain".to_string()),
                            filename: None,
                            mutable: false,
                        })
                        .await;
                    let _ = ack
                        .send(ControlAck::BytesChunk {
                            data: b"stub".to_vec(),
                        })
                        .await;
                    let _ = ack.send(ControlAck::StreamDone).await;
                }
                _ => {}
            }
        }
    });

    let uri_bytes = format!("/bytes/{DATA_REF}");
    let _ = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(&uri_bytes)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let uri_bzz = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let _ = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri_bzz)
            .body(Body::empty())
            .unwrap(),
    )
    .await;

    let seen = captured.lock().await.clone();
    assert_eq!(seen.len(), 2, "both commands should reach the dispatcher");
    for (i, max_bytes) in seen.iter().enumerate() {
        let n = max_bytes.unwrap_or_else(|| {
            panic!("dispatch {i}: gateway must set max_bytes, got None (would inherit CLI cap)")
        });
        assert!(
            n > DEFAULT_MAX_FILE_BYTES as u64,
            "dispatch {i}: max_bytes={n} must exceed CLI default {DEFAULT_MAX_FILE_BYTES}",
        );
    }
}

/// `/bzz/<root>/` (trailing slash, no path) must route to the manifest
/// root just like `/bzz/<root>`. freedom-browser's `bzz://` protocol
/// handler canonicalises every URL via `new URL(...)` which always
/// produces a trailing `/`, so a regression here would break it.
#[tokio::test]
async fn bzz_trailing_slash_resolves_to_manifest_root() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // The fixture manifest has no `website-index-document`, so a
    // properly-routed request resolves to a 404 from `lookup_path`,
    // not the 501 catch-all.
    assert_ne!(
        resp.status(),
        StatusCode::NOT_IMPLEMENTED,
        "trailing-slash bzz must NOT fall through to the 501 fallback",
    );
}

/// Multi-range requests are rejected with 416 (PLAN.md D.2.3).
#[tokio::test]
async fn bzz_multi_range_serves_multipart_byteranges() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=0-99,200-299")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // Go's http.ServeContent (what bee serves through) answers a
    // multi-range request with 206 multipart/byteranges.
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    assert!(
        ct.starts_with("multipart/byteranges; boundary="),
        "content type: {ct}"
    );
    let boundary = ct
        .strip_prefix("multipart/byteranges; boundary=")
        .unwrap()
        .to_string();
    assert_eq!(boundary.len(), 60, "Go-shaped 60-hex boundary");
    let body = body_bytes(resp).await;
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("Content-Range: bytes 0-99/18604"), "{text}");
    assert!(
        text.contains("Content-Range: bytes 200-299/18604"),
        "{text}"
    );
    assert!(text.ends_with(&format!("\r\n--{boundary}--\r\n")), "{text}");
}

/// Go `ServeContent` 416 parity: a range past the end is a text/plain
/// "invalid range: failed to overlap" with `Content-Range: bytes */N`;
/// a malformed header is "invalid range" without one.
#[tokio::test]
async fn range_416_matches_go_servecontent() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=99999999-")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        &format!("bytes */{BODY_LEN}")
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
    assert_eq!(
        body_bytes(resp).await,
        b"invalid range: failed to overlap\n"
    );

    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=oops")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert!(resp.headers().get(header::CONTENT_RANGE).is_none());
    assert_eq!(body_bytes(resp).await, b"invalid range\n");
}

/// `GET /bytes/{addr}` joins the multi-chunk tree directly without a
/// manifest hop. Pointing at the fixture's data ref must reproduce the
/// exact same body the manifest path resolves to.
#[tokio::test]
async fn bytes_returns_fixture_payload() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png"
    );
    // Bee never invents a Content-Disposition for `/bytes` …
    assert!(resp.headers().get(header::CONTENT_DISPOSITION).is_none());
    // … but does stamp the quoted reference as ETag on GET.
    assert_eq!(
        resp.headers().get(header::ETAG).unwrap(),
        format!("\"{DATA_REF}\"").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
}

#[tokio::test]
async fn bytes_filename_query_sets_content_headers() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}?filename=tile.wav");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "audio/wav"
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_DISPOSITION).unwrap(),
        "inline; filename=\"tile.wav\"",
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
}

/// `GET /chunks/{addr}` returns the **wire bytes** (`span (8 LE) || payload`),
/// matching bee's `chunkstore.Get`. Compare against the on-disk fixture
/// chunk to lock that wiring.
#[tokio::test]
async fn chunks_returns_wire_bytes() {
    let router = handle_with_fixture_node();
    let uri = format!("/chunks/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Bee's chunkGetHandler writes the quirky `binary/octet-stream`.
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "binary/octet-stream",
    );
    let bytes = body_bytes(resp).await;
    let expected =
        std::fs::read(fixture_dir().join(format!("{DATA_REF}.bin"))).expect("fixture chunk");
    assert_eq!(bytes, expected);
}

/// Bad reference (wrong length / non-hex) maps to 400 with the bee-shaped
/// error body, not 500 / 404.
#[tokio::test]
async fn bytes_rejects_short_reference_with_400() {
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/bytes/abc123")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_bytes(resp).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], 400);
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "address");
}

/// `HEAD /bytes/{addr}` mirrors `HEAD /bzz/...`: it must return
/// `Content-Length` and `Accept-Ranges` headers identical to the GET
/// response, with an empty body.
#[tokio::test]
async fn bytes_head_omits_body() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        BODY_LEN.to_string().as_str(),
    );
    // Bee's `bytesHeadHandler` never reaches ServeContent, so no
    // `Accept-Ranges` on HEAD (GET carries it).
    assert!(resp.headers().get(header::ACCEPT_RANGES).is_none());
    let bytes = body_bytes(resp).await;
    assert!(bytes.is_empty(), "HEAD must not carry a body");
}

/// `HEAD /bzz/...` and `HEAD /bytes/...` must signal `head_only=true`
/// to the daemon so it returns size + content-type without joining the
/// chunk tree. Asserts directly on the dispatched `ControlCommand` —
/// catches regressions where the gateway accidentally falls back to the
/// full-body path and just hides the body inside axum.
#[tokio::test]
async fn head_dispatches_head_only_flag() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let saw_head_only = Arc::new(AtomicBool::new(false));
    let saw_body_join = Arc::new(AtomicBool::new(false));
    let head_clone = saw_head_only.clone();
    let body_clone = saw_body_join.clone();
    let router = router_with_dispatcher(move |cmd| {
        let head = head_clone.clone();
        let body = body_clone.clone();
        async move {
            if let ControlCommand::StreamBzz { head_only, ack, .. } = cmd {
                if head_only {
                    head.store(true, Ordering::SeqCst);
                }
                let _ = ack
                    .send(ControlAck::BzzStreamStart {
                        total_bytes: 1024,
                        reference: vec![0u8; 32],
                        content_type: Some("video/mp4".to_string()),
                        filename: Some("clip.mp4".to_string()),
                        mutable: false,
                    })
                    .await;
                if !head_only {
                    // If the gateway ever drove a HEAD request through
                    // the body path, we'd see this branch fire and
                    // surface as an extra `BytesChunk` in the response.
                    body.store(true, Ordering::SeqCst);
                    let _ = ack
                        .send(ControlAck::BytesChunk {
                            data: vec![0u8; 1024],
                        })
                        .await;
                }
                let _ = ack.send(ControlAck::StreamDone).await;
            }
        }
    });

    let uri = format!("/bzz/{MANIFEST_ROOT}/clip.mp4");
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get(header::CONTENT_LENGTH).unwrap(), "1024",);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "video/mp4",
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes, [] as [u8; 0]);
    assert!(
        saw_head_only.load(Ordering::SeqCst),
        "HEAD must dispatch with head_only=true",
    );
    assert!(
        !saw_body_join.load(Ordering::SeqCst),
        "HEAD must not trigger any body chunk fetch",
    );
}

/// `Range: bytes=0-` (browser range-probe) returns `206 Partial Content`
/// covering the whole file. Browsers issue this exact request to decide
/// whether the server supports ranges; without `206` they fall back to
/// progressive download and lose seeking.
#[tokio::test]
async fn bzz_range_zero_dash_returns_206() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=0-")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes 0-{}/{BODY_LEN}", BODY_LEN - 1).as_str(),
    );
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        BODY_LEN.to_string().as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN);
    assert_eq!(sha256_hex(&bytes), BODY_SHA256);
}

/// Suffix range (`bytes=-N`) returns the last N bytes. This is what
/// browsers issue when a media container's metadata sits at the tail of
/// the file (MP4 with `moov` at the end).
#[tokio::test]
async fn bzz_suffix_range_returns_tail() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=-256")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let expected_start = BODY_LEN - 256;
    let expected_end = BODY_LEN - 1;
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes {expected_start}-{expected_end}/{BODY_LEN}").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 256);
}

/// `Range: bytes=START-` (open-ended) returns `[START, END]` of the file.
/// This is the canonical "resume my interrupted download" shape.
#[tokio::test]
async fn bzz_open_ended_range_returns_remainder() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let start: usize = 10_000;
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, format!("bytes={start}-"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes {start}-{}/{BODY_LEN}", BODY_LEN - 1).as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), BODY_LEN - start);
}

/// Range request with a single-range header that goes past EOF returns
/// `416 Range Not Satisfiable` with a `Content-Range: bytes */<total>`
/// header so the client can correct its request.
#[tokio::test]
async fn bzz_range_past_eof_returns_416() {
    let router = handle_with_fixture_node();
    let uri = format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=1000000-2000000")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes */{BODY_LEN}").as_str(),
    );
}

/// Range request on `/bytes/{addr}` mirrors the `/bzz` semantics.
#[tokio::test]
async fn bytes_range_request_returns_partial_content() {
    let router = handle_with_fixture_node();
    let uri = format!("/bytes/{DATA_REF}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, "bytes=100-199")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes 100-199/{BODY_LEN}").as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes.len(), 100);
}

/// Unknown chunk → bee-shaped 404 (the joiner's "fetch failed" lands on
/// `ControlAck::Error`, which the gateway maps to 404 for the retrieval
/// surface).
#[tokio::test]
async fn chunks_404_on_missing_reference() {
    let router = handle_with_fixture_node();
    let unknown = "00".repeat(32);
    let uri = format!("/chunks/{unknown}");
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 404);
}

/// #113: the terminal `StreamBzz` errors `run_stream_bzz` emits for a
/// raw `/bytes` reference behind `/bzz` are 404s, never a 502 that
/// Freedom's `bzz://` handler retries for most of a minute. The
/// messages are the node's exact shapes (`path_below_raw_bytes_message`
/// and an exhausted bare-root lookup in `ant-p2p`'s `behaviour.rs`). A
/// lookup whose typed fetcher error confirms the miss now arrives as
/// `ControlAck::NotFound` instead (#123, next test); these pin the
/// message mapping for an untyped `ControlAck::Error`.
#[tokio::test]
async fn bzz_raw_bytes_misses_are_404_not_502() {
    let raw = "6c63c82445b9d80000d22e3a6acad2af956920beb475f1b20abadc4ae8dc16b1";
    let cases = [
        (
            format!("/bzz/{raw}/foo"),
            format!("path 'foo' not found: {raw} is raw bytes, not a mantaray manifest"),
            "path address not found",
        ),
        (
            format!("/bzz/{raw}/"),
            format!(
                "manifest lookup '': fetch manifest node: fetch chunk {raw}: all peers failed \
                 for chunk {raw} after 33 attempts (last: remote: retrieve chunk: storage: not found)"
            ),
            "Not Found",
        ),
        (
            format!("/bzz/{raw}"),
            "manifest lookup '': fetch manifest node: erasure recovery failed: erasure recovery \
             impossible: 95 of 128 shards retrievable, need 119 (119 data + 9 parity refs); \
             trigger: no BZZ peers available"
                .to_string(),
            "Not Found",
        ),
    ];
    for (uri, message, want) in cases {
        let router = router_with_dispatcher(move |cmd| {
            let message = message.clone();
            async move {
                if let ControlCommand::StreamBzz { ack, .. } = cmd {
                    let _ = ack.send(ControlAck::Error { message }).await;
                }
            }
        });
        let resp = send(
            router,
            Request::builder()
                .method(Method::GET)
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{uri}");
        let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(json["message"], want, "{uri}");
    }
}

/// #123: a root the network confirmed missing reaches the gateway typed
/// as `ControlAck::NotFound` (`pre_stream_failure_ack` in `ant-p2p`'s
/// `behaviour.rs`) and answers bee's `404 "Not Found"` on `/bytes` and
/// `/bzz`, whichever miss tail the fetch ended in. The `no peer found`
/// tail used to come through as a plain error and answer 502, which
/// Freedom retries for ~50 s. A miss from a starved peer pool comes as
/// `NotReady` and stays retryable (503), never a 404 (#114).
#[tokio::test]
async fn confirmed_missing_root_is_404_and_starved_miss_is_503() {
    let missing = "8bb914d8c06c185734f5853acc4b10746d1ccec7462dac2d94b57612f2acbe74";
    let exhausted = |tail: &str| {
        format!(
            "all peers failed for chunk {missing} after 34 attempts (last: remote: retrieve chunk: {tail})"
        )
    };
    let uris = [
        format!("/bytes/{missing}"),
        format!("/bzz/{missing}/"),
        format!("/bzz/{missing}/index.html"),
    ];
    for uri in &uris {
        for tail in ["no peer found", "storage: not found"] {
            for method in [Method::GET, Method::HEAD] {
                let message = if uri.starts_with("/bytes") {
                    format!("fetch root chunk {missing}: {}", exhausted(tail))
                } else {
                    format!(
                        "manifest lookup '': fetch chunk {missing}: {}",
                        exhausted(tail)
                    )
                };
                let router = router_with_dispatcher(move |cmd| {
                    let message = message.clone();
                    async move {
                        if let ControlCommand::StreamBytes { ack, .. }
                        | ControlCommand::StreamBzz { ack, .. } = cmd
                        {
                            let _ = ack.send(ControlAck::NotFound { message }).await;
                        }
                    }
                });
                let resp = send(
                    router,
                    Request::builder()
                        .method(method.clone())
                        .uri(uri)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await;
                assert_eq!(
                    resp.status(),
                    StatusCode::NOT_FOUND,
                    "{method} {uri} ({tail})"
                );
                if method == Method::GET {
                    let json: serde_json::Value =
                        serde_json::from_slice(&body_bytes(resp).await).unwrap();
                    assert_eq!(json["code"], 404, "{uri} ({tail})");
                    assert_eq!(json["message"], "Not Found", "{uri} ({tail})");
                }
            }
        }

        let message = exhausted("storage: not found");
        let router = router_with_dispatcher(move |cmd| {
            let message = message.clone();
            async move {
                if let ControlCommand::StreamBytes { ack, .. }
                | ControlCommand::StreamBzz { ack, .. } = cmd
                {
                    let _ = ack.send(ControlAck::NotReady { message }).await;
                }
            }
        });
        let resp = send(
            router,
            Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{uri} (starved)"
        );
    }
}

/// #146: a single chunk / SOC the network confirmed missing (a feed
/// slot not written yet) reaches the gateway typed as
/// `ControlAck::NotFound` and answers 404 on `/chunks` and `/soc`,
/// whichever miss tail the lookup ended in. The `no peer found` tail
/// used to answer 502 on `/chunks` (the message has no "not found"),
/// which Freedom's feed reader reports as an error instead of "entry not
/// found" — about one live-edge poll in five on mainnet.
#[tokio::test]
async fn confirmed_missing_chunk_and_soc_are_404() {
    let missing = "8bb914d8c06c185734f5853acc4b10746d1ccec7462dac2d94b57612f2acbe74";
    let owner = "b818ff019bc15bc3dfbdad4ce0ab66a6f74e8f1e";
    let uris = [
        (format!("/chunks/{missing}"), "chunk not found"),
        (
            format!("/soc/{owner}/{missing}"),
            "requested chunk cannot be retrieved",
        ),
    ];
    for (uri, want) in &uris {
        for tail in ["no peer found", "storage: not found"] {
            let message = format!(
                "retrieval: all peers failed for chunk {missing} after 17 attempts \
                 (last: remote: retrieve chunk: {tail})"
            );
            let router = router_with_dispatcher(move |cmd| {
                let message = message.clone();
                async move {
                    if let ControlCommand::GetChunkRaw { ack, .. } = cmd {
                        let _ = ack.send(ControlAck::NotFound { message });
                    }
                }
            });
            let resp = send(
                router,
                Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::NOT_FOUND, "{uri} ({tail})");
            let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
            assert_eq!(json["message"], *want, "{uri} ({tail})");
        }
    }
}

/// Every content-addressed endpoint must emit the `Cache-Control:
/// public, max-age=..., immutable` header so the browser can serve
/// reloads from its own HTTP cache without re-walking the manifest.
/// Bee has set this header on `/bzz` since at least its 1.x line; not
/// matching it makes a music-player-style site that reloads the same
/// 13 audio tracks + artwork on every navigation feel dramatically
/// slower than against bee, even when our disk cache hit rate is
/// 100% (cf. discussion 2026-05 / `c4f8a45301...`). The test pins
/// every endpoint at once because the response builders share a
/// helper — a regression that disables one would likely disable all
/// — and a single failing endpoint is enough to defeat the
/// browser's reload-from-cache behaviour for a typical bzz site.
#[tokio::test]
async fn content_addressed_endpoints_emit_immutable_cache_control() {
    let router = handle_with_fixture_node();

    // The chunk endpoint serves the raw manifest root chunk by hash.
    let chunk_addr = "ed5b81dac5d34d22acd6db28ee864bc6f4d0d31db17f9f4ec6e62a89c1f31cab";
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/chunks/{chunk_addr}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // Don't assert status — the fixture pool may or may not contain
    // this exact chunk; we only care that whatever response shape we
    // *do* return for content-addressed paths carries the header.
    if resp.status() == StatusCode::OK {
        let cc = resp
            .headers()
            .get(header::CACHE_CONTROL)
            .expect("/chunks must emit Cache-Control");
        assert!(
            cc.to_str().unwrap().contains("immutable"),
            "/chunks Cache-Control missing `immutable`: {cc:?}",
        );
    }

    // The bytes endpoint streams the joiner output for a content
    // hash. Always immutable.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/bytes/{DATA_REF}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/bytes must emit Cache-Control");
    let cc_str = cc.to_str().unwrap();
    assert!(
        cc_str.contains("immutable") && cc_str.contains("max-age="),
        "/bytes Cache-Control missing `immutable` / `max-age`: {cc_str}",
    );
    let _ = body_bytes(resp).await;

    // The bzz endpoint resolves a manifest path. Always immutable.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/bzz must emit Cache-Control");
    assert!(
        cc.to_str().unwrap().contains("immutable"),
        "/bzz Cache-Control missing `immutable`: {cc:?}",
    );
    let _ = body_bytes(resp).await;

    // Range responses (206) MUST also emit Cache-Control or the
    // browser will re-fetch the partial bytes on every replay.
    let resp = send(
        router.clone(),
        Request::builder()
            .method(Method::GET)
            .uri(format!("/bzz/{MANIFEST_ROOT}/13/4358/2645.png"))
            .header(header::RANGE, "bytes=0-1023")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/bzz 206 must emit Cache-Control");
    assert!(
        cc.to_str().unwrap().contains("immutable"),
        "/bzz 206 Cache-Control missing `immutable`: {cc:?}",
    );
    let _ = body_bytes(resp).await;

    // The Ant-specific manifest listing is content-addressed too.
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(format!("/v0/manifest/{MANIFEST_ROOT}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/v0/manifest must emit Cache-Control");
    assert!(
        cc.to_str().unwrap().contains("immutable"),
        "/v0/manifest Cache-Control missing `immutable`: {cc:?}",
    );
}

/// `POST /chunks` round-trips a stamped CAC chunk via the
/// `PushChunk` control command and returns bee-shaped
/// `{"reference": "<lowercase 64-hex>"}` with `201 Created`.
/// The fixture node responds with the BMT hash of the wire bytes
/// (mirrors what `ant-p2p::handle_control_command` does locally
/// before pushsync), so we can lock the wire shape without
/// standing up libp2p.
#[tokio::test]
async fn post_chunks_returns_bee_shaped_reference() {
    let payload: &[u8] = b"hello swarm";
    let (addr, wire) = ant_crypto::bmt::cac_new(payload).expect("cac_new");

    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/chunks")
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(
        json["reference"].as_str().unwrap(),
        hex::encode(addr),
        "POST /chunks reference must be bare lowercase 64-hex (no 0x), bee-compatible",
    );
}

/// Empty / over-sized bodies must be rejected with a 400 bee-shaped
/// error before the upload path is even contacted, so a malicious
/// caller can't burn a postage slot on a bogus chunk.
#[tokio::test]
async fn post_chunks_rejects_oversized_body() {
    let router = handle_with_fixture_node();
    let too_big = vec![0u8; 8 + 4097];
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/chunks")
            .body(Body::from(too_big))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 400);
    assert_eq!(json["message"], "chunk data exceeds required length");
}

/// `POST /bzz?name=hello.txt` chunks the request body, builds a
/// single-file mantaray, pushes every chunk via `PushChunk`, then
/// returns a Bee-shaped 201 with the manifest root. Reusing the fake
/// node loop's BMT-only `PushChunk` handler means we only assert
/// reference shape + chunk count here; the production smoke test
/// covers the live pushsync handshake.
#[tokio::test]
async fn post_bzz_single_file_returns_manifest_reference() {
    let payload = b"uploaded via ant\n".to_vec();
    let pushed_chunks = Arc::new(Mutex::new(Vec::<[u8; 32]>::new()));
    let pushed = pushed_chunks.clone();
    let router = router_with_dispatcher(move |cmd| {
        let pushed = pushed.clone();
        async move {
            match cmd {
                ControlCommand::PushChunk { wire, ack, .. } => {
                    let mut span = [0u8; 8];
                    span.copy_from_slice(&wire[..8]);
                    let payload = &wire[8..];
                    let addr = ant_crypto::bmt::bmt_hash_with_span(&span, payload).unwrap();
                    pushed.lock().await.push(addr);
                    let _ = ack.send(ControlAck::ChunkUploaded {
                        reference: format!("0x{}", hex::encode(addr)),
                    });
                }
                // Default redundancy level 1 also mints dispersed
                // replica SOCs; ack them so the upload can finish (they
                // aren't counted among the pushed CAC chunks).
                ControlCommand::PushSoc { address, ack, .. } => {
                    let _ = ack.send(ControlAck::ChunkUploaded {
                        reference: format!("0x{}", hex::encode(address)),
                    });
                }
                _ => {}
            }
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bzz?name=hello.txt")
            .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(payload.clone()))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let reference = json["reference"].as_str().unwrap().to_string();
    assert_eq!(reference.len(), 64, "reference must be bare 64-hex");

    // Single-file manifest = data leaf (1) + slash stub + file leaf + root = 4 chunks.
    let pushed = pushed_chunks.lock().await;
    assert_eq!(
        pushed.len(),
        4,
        "expected 4 chunks (data + slash stub + file leaf + manifest root), got {}: {:?}",
        pushed.len(),
        pushed.iter().map(hex::encode).collect::<Vec<_>>(),
    );
    // The last chunk pushed isn't strictly the root (stream::buffer_unordered
    // doesn't guarantee order), but the manifest root *must* be in the set.
    let mut want = [0u8; 32];
    hex::decode_to_slice(&reference, &mut want).unwrap();
    assert!(
        pushed.iter().any(|a| a == &want),
        "manifest root {reference} not among pushed chunks",
    );
}

/// `POST /bzz` with `swarm-collection: true` and a tar archive should
/// produce a multi-file manifest. The returned reference must be
/// retrievable as a manifest with each archive entry's path resolving
/// to its own data ref. We don't have round-trip retrieval for the
/// just-uploaded chunks here (the dispatcher only handles `PushChunk`),
/// but we do verify the chunk count makes sense and the response
/// shape is bee-compatible.
#[tokio::test]
async fn post_bzz_collection_uploads_tar_archive() {
    // Build a tiny tar archive with two files.
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        for (path, body) in [
            ("index.html", &b"<h1>hi</h1>"[..]),
            ("style.css", &b"body{color:red}"[..]),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(body.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append(&header, body).unwrap();
        }
        builder.finish().unwrap();
    }

    let pushed_chunks = Arc::new(Mutex::new(Vec::<[u8; 32]>::new()));
    let pushed = pushed_chunks.clone();
    let router = router_with_dispatcher(move |cmd| {
        let pushed = pushed.clone();
        async move {
            match cmd {
                ControlCommand::PushChunk { wire, ack, .. } => {
                    let mut span = [0u8; 8];
                    span.copy_from_slice(&wire[..8]);
                    let payload = &wire[8..];
                    let addr = ant_crypto::bmt::bmt_hash_with_span(&span, payload).unwrap();
                    pushed.lock().await.push(addr);
                    let _ = ack.send(ControlAck::ChunkUploaded {
                        reference: format!("0x{}", hex::encode(addr)),
                    });
                }
                // Default redundancy level 1 also mints dispersed
                // replica SOCs; ack them so the upload can finish (they
                // aren't counted among the pushed CAC chunks).
                ControlCommand::PushSoc { address, ack, .. } => {
                    let _ = ack.send(ControlAck::ChunkUploaded {
                        reference: format!("0x{}", hex::encode(address)),
                    });
                }
                _ => {}
            }
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bzz")
            .header("swarm-collection", "true")
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(tar_bytes))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let reference = json["reference"].as_str().unwrap().to_string();
    assert_eq!(reference.len(), 64);

    // 2 file data chunks (each ≤ CHUNK_SIZE = 1 leaf each) + 2 manifest
    // file leaves + slash stub for index-doc + root = 6 chunks.
    let pushed = pushed_chunks.lock().await;
    assert!(
        pushed.len() >= 5,
        "expected ≥5 chunks for 2-file collection, got {}",
        pushed.len(),
    );
    let mut want = [0u8; 32];
    hex::decode_to_slice(&reference, &mut want).unwrap();
    assert!(
        pushed.iter().any(|a| a == &want),
        "collection manifest root {reference} not among pushed chunks",
    );
}

// =====================================================================
// SOC reads: GET / HEAD `/soc/{owner}/{id}`.
//
// Freedom Browser navigates `bzz://` URLs; once the gateway resolves a
// feed-mounted manifest it sometimes wants the underlying SOC bytes
// directly (e.g. to surface the publisher key without re-walking the
// feed). These tests use `router_with_dispatcher` so we can stage a
// single `(addr → soc_wire)` mapping rather than dropping fixture
// chunks on disk; the gateway only ever sends `GetChunkRaw` for SOCs,
// so the dispatcher stays small.
// =====================================================================

/// Owner secret + id + payload bundle. Every test builds one with a
/// fresh `secret` byte so the owner address is deterministic but
/// unique per test, avoiding any cross-test cache interference.
struct SocFixture {
    owner: [u8; 20],
    id: [u8; 32],
    soc_address: [u8; 32],
    signature: [u8; 65],
    wire: Vec<u8>,
    inner_cac_wire: Vec<u8>,
}

/// Build a self-consistent SOC: signs `keccak256(id || inner_cac_addr)`
/// with `secret`, packs `id || sig || inner_cac_wire`, and computes the
/// `keccak256(id || owner)` address that `GET /soc/{owner}/{id}` should
/// resolve to.
fn signed_soc(secret: [u8; 32], id: [u8; 32], payload: &[u8]) -> SocFixture {
    use k256::ecdsa::{SigningKey, VerifyingKey};

    let sk = SigningKey::from_bytes(&secret.into()).expect("valid secret");
    let vk = VerifyingKey::from(&sk);
    let owner = ant_crypto::ethereum_address_from_public_key(&vk);

    let (inner_cac_addr, inner_cac_wire) = ant_crypto::cac_new(payload).expect("cac_new");

    let mut digest_input = [0u8; 64];
    digest_input[..32].copy_from_slice(&id);
    digest_input[32..].copy_from_slice(&inner_cac_addr);
    let sig = ant_crypto::sign_handshake_data(&secret, &ant_crypto::keccak256(&digest_input))
        .expect("sign");

    let mut soc_addr_input = [0u8; 52];
    soc_addr_input[..32].copy_from_slice(&id);
    soc_addr_input[32..].copy_from_slice(&owner);
    let soc_address = ant_crypto::keccak256(&soc_addr_input);

    let mut wire = Vec::with_capacity(32 + 65 + inner_cac_wire.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&inner_cac_wire);

    SocFixture {
        owner,
        id,
        soc_address,
        signature: sig,
        wire,
        inner_cac_wire,
    }
}

/// Build a router that serves the staged SOC at its derived address and
/// errors on any other reference. Mirrors how the production node loop
/// resolves `GetChunkRaw` against the chunk store.
fn router_serving_soc(fixture: &SocFixture) -> axum::Router {
    let addr = fixture.soc_address;
    let wire = fixture.wire.clone();
    router_with_dispatcher(move |cmd| {
        let wire = wire.clone();
        async move {
            match cmd {
                ControlCommand::GetChunkRaw { reference, ack } => {
                    let reply = if reference == addr {
                        ControlAck::Bytes { data: wire }
                    } else {
                        ControlAck::Error {
                            message: format!("not found: {}", hex::encode(reference)),
                        }
                    };
                    let _ = ack.send(reply);
                }
                _ => panic!("unexpected command in SOC test: {cmd:?}"),
            }
        }
    })
}

#[tokio::test]
async fn get_soc_returns_unwrapped_payload_with_signature_header() {
    let fixture = signed_soc([0x11; 32], [0xa1; 32], b"hello soc");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );
    // Bee's socGetHandler serves the wrapped chunk's *content* and
    // echoes the SOC signature + a quoted ETag of the wrapped address.
    assert_eq!(
        resp.headers().get("swarm-soc-signature").unwrap(),
        hex::encode(fixture.signature).as_str(),
    );
    assert!(resp
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with('"'));
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes, b"hello soc");
}

#[tokio::test]
async fn get_soc_accepts_0x_prefixed_hex() {
    let fixture = signed_soc([0x12; 32], [0xa2; 32], b"0x-prefixed");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/0x{}/0X{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_bytes(resp).await, b"0x-prefixed");
}

#[tokio::test]
async fn get_soc_rejects_bad_owner_hex() {
    let fixture = signed_soc([0x13; 32], [0xa3; 32], b"x");
    let router = router_serving_soc(&fixture);
    // Owner is too short (19 bytes worth of hex).
    let uri = format!("/soc/{}/{}", "ab".repeat(19), hex::encode(fixture.id));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 400);
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "owner");
}

#[tokio::test]
async fn get_soc_rejects_bad_id_hex() {
    let fixture = signed_soc([0x14; 32], [0xa4; 32], b"x");
    let router = router_serving_soc(&fixture);
    // Id contains a non-hex character.
    let uri = format!("/soc/{}/{}", hex::encode(fixture.owner), "zz".repeat(32));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "id");
    assert!(json["reasons"][0]["error"]
        .as_str()
        .unwrap()
        .starts_with("invalid hex byte"));
}

#[tokio::test]
async fn get_soc_returns_404_when_chunk_missing() {
    let fixture = signed_soc([0x15; 32], [0xa5; 32], b"x");
    // Drop the wire so the dispatcher can't find this address.
    let other_id = [0xff; 32];
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(other_id)
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn head_soc_returns_200_no_body() {
    let fixture = signed_soc([0x16; 32], [0xa6; 32], b"head probe");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Content-Length reflects the unwrapped payload bee serves.
    let expected_len = (fixture.inner_cac_wire.len() - 8).to_string();
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        expected_len.as_str(),
    );
    let bytes = body_bytes(resp).await;
    assert!(
        bytes.is_empty(),
        "HEAD body must be empty, got {} bytes",
        bytes.len()
    );
}

#[tokio::test]
async fn get_soc_does_not_set_immutable_cache_control() {
    // SOCs are mutable at the address level — same `(owner, id)` after
    // a re-upload returns a different inner CAC. The cache header must
    // not let the browser pin the first response forever.
    let fixture = signed_soc([0x17; 32], [0xa7; 32], b"mutable");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/soc must emit Cache-Control");
    let cc_str = cc.to_str().unwrap();
    assert!(
        !cc_str.contains("immutable"),
        "/soc Cache-Control must not be immutable: {cc_str}",
    );
    assert!(
        cc_str.contains("no-cache"),
        "/soc Cache-Control should be `no-cache`, got {cc_str}",
    );
}

#[tokio::test]
async fn get_soc_root_chunk_returns_wrapped_wire() {
    // With `Swarm-Only-Root-Chunk: true` bee writes the wrapped
    // chunk's wire data (`span(8 LE) || payload`) verbatim — not the
    // SOC header, and not the bare payload — with no ETag.
    let fixture = signed_soc([0x18; 32], [0xa8; 32], b"layout pin");
    let router = router_serving_soc(&fixture);
    let uri = format!(
        "/soc/{}/{}",
        hex::encode(fixture.owner),
        hex::encode(fixture.id),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header("swarm-only-root-chunk", "true")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("swarm-soc-signature").unwrap(),
        hex::encode(fixture.signature).as_str(),
    );
    assert!(resp.headers().get(header::ETAG).is_none());
    let bytes = body_bytes(resp).await;
    assert_eq!(bytes, fixture.inner_cac_wire);
}

/// Oversized body must be rejected with 413 before chunking begins.
#[tokio::test]
async fn post_bzz_rejects_oversized_body() {
    let router = handle_with_fixture_node();
    // GATEWAY_MAX_UPLOAD_BYTES is 64 MiB; build something just over.
    let too_big = vec![0u8; 64 * 1024 * 1024 + 1];
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/bzz")
            .body(Body::from(too_big))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

// =====================================================================
// Sequence-feed reads: GET `/feeds/{owner}/{topic}`.
//
// These tests stage their own SOC chunks at every sequence-update
// address the resolver will probe. The dispatcher answers
// `GetChunkRaw` from a HashMap so the gateway → control → resolver
// wiring is exercised end-to-end without standing up a swarm.
// =====================================================================

use std::collections::HashMap;

/// Build a v1 sequence-feed update SOC chunk (`id || sig || inner_cac`)
/// for `(owner, topic)` at `index`, embedding `wrapped_ref` and `ts`.
/// Returns `(soc_addr, soc_wire)`. Mirrors the helper in
/// `ant-retrieval::feed::tests::make_sequence_update_v1`.
fn make_feed_update(
    secret: &[u8; 32],
    owner: &[u8; 20],
    topic: &[u8; 32],
    index: u64,
    ts: u64,
    wrapped_ref: &[u8; 32],
) -> ([u8; 32], Vec<u8>) {
    let mut id_input = Vec::with_capacity(40);
    id_input.extend_from_slice(topic);
    id_input.extend_from_slice(&index.to_be_bytes());
    let id = ant_crypto::keccak256(&id_input);

    let mut payload = Vec::with_capacity(40);
    payload.extend_from_slice(&ts.to_be_bytes());
    payload.extend_from_slice(wrapped_ref);
    let (inner_cac_addr, inner_cac_wire) = ant_crypto::cac_new(&payload).expect("cac");

    let mut sig_input = Vec::with_capacity(64);
    sig_input.extend_from_slice(&id);
    sig_input.extend_from_slice(&inner_cac_addr);
    let prehash = ant_crypto::keccak256(&sig_input);
    let sig = ant_crypto::sign_handshake_data(secret, &prehash).expect("sign");

    let mut soc_addr_input = Vec::with_capacity(52);
    soc_addr_input.extend_from_slice(&id);
    soc_addr_input.extend_from_slice(owner);
    let soc_addr = ant_crypto::keccak256(&soc_addr_input);

    let mut wire = Vec::with_capacity(32 + 65 + inner_cac_wire.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&inner_cac_wire);
    (soc_addr, wire)
}

/// Build a **v2** sequence-feed update SOC: the SOC wraps `content`'s
/// CAC directly (no `ts || ref` indirection), so the feed resolves to
/// the content CAC's own address. Returns `(soc_addr, soc_wire,
/// content_addr, content_wire)`. Mirrors bee's v2 feed updates.
fn make_feed_update_v2(
    secret: &[u8; 32],
    owner: &[u8; 20],
    topic: &[u8; 32],
    index: u64,
    content: &[u8],
) -> ([u8; 32], Vec<u8>, [u8; 32], Vec<u8>) {
    let mut id_input = Vec::with_capacity(40);
    id_input.extend_from_slice(topic);
    id_input.extend_from_slice(&index.to_be_bytes());
    let id = ant_crypto::keccak256(&id_input);

    let (content_addr, content_wire) = ant_crypto::cac_new(content).expect("content cac");

    let mut sig_input = Vec::with_capacity(64);
    sig_input.extend_from_slice(&id);
    sig_input.extend_from_slice(&content_addr);
    let prehash = ant_crypto::keccak256(&sig_input);
    let sig = ant_crypto::sign_handshake_data(secret, &prehash).expect("sign");

    let mut soc_addr_input = Vec::with_capacity(52);
    soc_addr_input.extend_from_slice(&id);
    soc_addr_input.extend_from_slice(owner);
    let soc_addr = ant_crypto::keccak256(&soc_addr_input);

    let mut wire = Vec::with_capacity(32 + 65 + content_wire.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&content_wire);
    (soc_addr, wire, content_addr, content_wire)
}

fn deterministic_owner(secret: &[u8; 32]) -> [u8; 20] {
    use k256::ecdsa::{SigningKey, VerifyingKey};
    let sk = SigningKey::from_bytes(secret.into()).expect("valid secret");
    let vk = VerifyingKey::from(&sk);
    ant_crypto::ethereum_address_from_public_key(&vk)
}

/// Stage `updates` (index → `wrapped_ref`) and return a router that
/// resolves them via the real `resolve_sequence_feed_full`. The
/// dispatcher answers `GetChunkRaw` from a `HashMap`; the gateway's
/// `download_feed` uses `ControlCommand::GetFeed`, but the test
/// fixture's `GetFeed` arm hands the work to a `RoutingFetcher`-style
/// `ChunkFetcher` over the same chunk map. Here we plug in directly.
struct FeedScenario {
    secret: [u8; 32],
    owner: [u8; 20],
    topic: [u8; 32],
}

impl FeedScenario {
    fn new(secret_seed: u8, topic_seed: u8) -> Self {
        let secret = [secret_seed; 32];
        let owner = deterministic_owner(&secret);
        let topic = [topic_seed; 32];
        Self {
            secret,
            owner,
            topic,
        }
    }

    /// Stage `updates` (index → content bytes → ts). The wrapped
    /// reference of each update is the CAC address of its content, so
    /// `GetFeed` resolves to that address and the gateway's dereference
    /// (`StreamBytes`) serves the staged content back. Content blobs in
    /// these tests are single-chunk (≤ 4096 bytes).
    fn router(&self, updates: &[(u64, &[u8], u64)]) -> axum::Router {
        let mut chunks: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
        for (index, content, ts) in updates {
            let (content_addr, content_wire) = ant_crypto::cac_new(content).expect("content cac");
            let (addr, wire) = make_feed_update(
                &self.secret,
                &self.owner,
                &self.topic,
                *index,
                *ts,
                &content_addr,
            );
            chunks.insert(addr, wire);
            chunks.insert(content_addr, content_wire);
        }
        let owner = self.owner;
        let topic = self.topic;
        router_with_dispatcher(move |cmd| {
            let chunks = chunks.clone();
            let owner = owner;
            let topic = topic;
            async move {
                handle_feed_test_command(&chunks, owner, topic, cmd).await;
            }
        })
    }

    /// Stage a single **v2** update at `index`: the SOC wraps the
    /// content CAC directly, so the feed resolves to the content's own
    /// address. The content CAC is also staged standalone (bee uploads
    /// it via POST /bytes before wrapping it).
    fn router_v2(&self, index: u64, content: &[u8]) -> axum::Router {
        let mut chunks: HashMap<[u8; 32], Vec<u8>> = HashMap::new();
        let (soc_addr, soc_wire, content_addr, content_wire) =
            make_feed_update_v2(&self.secret, &self.owner, &self.topic, index, content);
        chunks.insert(soc_addr, soc_wire);
        chunks.insert(content_addr, content_wire);
        let owner = self.owner;
        let topic = self.topic;
        router_with_dispatcher(move |cmd| {
            let chunks = chunks.clone();
            let owner = owner;
            let topic = topic;
            async move {
                handle_feed_test_command(&chunks, owner, topic, cmd).await;
            }
        })
    }
}

async fn handle_feed_test_command(
    chunks: &HashMap<[u8; 32], Vec<u8>>,
    expected_owner: [u8; 20],
    expected_topic: [u8; 32],
    cmd: ControlCommand,
) {
    match cmd {
        ControlCommand::GetChunkRaw { reference, ack } => {
            let reply = match chunks.get(&reference) {
                Some(wire) => ControlAck::Bytes { data: wire.clone() },
                None => ControlAck::Error {
                    message: format!("not found: {}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::GetFeed {
            owner,
            topic,
            after,
            ack,
        } => {
            // Resolve via the real `resolve_sequence_feed_after` against
            // the staged HashMap. Mirrors how the production node-side
            // handler runs the resolver, so an end-to-end gateway test
            // exercises the wiring without a swarm.
            assert_eq!(owner, expected_owner);
            assert_eq!(topic, expected_topic);
            let feed = ant_retrieval::Feed {
                owner,
                topic,
                kind: ant_retrieval::FeedType::Sequence,
            };
            let fetcher = MapFetcher {
                chunks: chunks.clone(),
            };
            let reply =
                match ant_retrieval::resolve_sequence_feed_after(&fetcher, &feed, after).await {
                    Ok(r) => ControlAck::FeedResolved {
                        reference: r.reference,
                        index: r.index,
                        signature: r.signature,
                        v2: r.v2,
                        decrypt_key: r.decrypt_key,
                    },
                    Err(ant_retrieval::FeedError::NoUpdates { .. }) => ControlAck::FeedNotFound,
                    Err(e) => ControlAck::Error {
                        message: format!("feed lookup: {e}"),
                    },
                };
            let _ = ack.send(reply);
        }
        ControlCommand::StreamBytes {
            reference,
            range,
            head_only,
            ack,
            ..
        } => {
            serve_single_chunk_stream(chunks, reference, range, head_only, ack).await;
        }
        other => panic!("unexpected command in feed test: {other:?}"),
    }
}

/// Serve the staged single-chunk content at `reference` over the
/// streaming-ack protocol the gateway's `download_feed` dereference
/// path consumes: a `BytesStreamStart` prologue, then (unless this is a
/// `HEAD`) one `BytesChunk` carrying the requested byte slice, then
/// `StreamDone`. The CAC wire is `span(8 LE) || payload`; tests stage
/// payloads that fit in a single chunk so we never have to join a tree.
async fn serve_single_chunk_stream(
    chunks: &HashMap<[u8; 32], Vec<u8>>,
    reference: [u8; 32],
    range: Option<ant_control::StreamRange>,
    head_only: bool,
    ack: tokio::sync::mpsc::Sender<ControlAck>,
) {
    let Some(wire) = chunks.get(&reference) else {
        let _ = ack
            .send(ControlAck::Error {
                message: format!("not found: {}", hex::encode(reference)),
            })
            .await;
        return;
    };
    let payload = &wire[8..];
    let total = payload.len() as u64;
    if ack
        .send(ControlAck::BytesStreamStart { total_bytes: total })
        .await
        .is_err()
    {
        return;
    }
    if head_only {
        let _ = ack.send(ControlAck::StreamDone).await;
        return;
    }
    let slice: Vec<u8> = match range {
        Some(r) if total > 0 => {
            let start = r.start.min(total - 1) as usize;
            let end = r.end_inclusive.min(total - 1) as usize;
            payload[start..=end].to_vec()
        }
        _ => payload.to_vec(),
    };
    let _ = ack.send(ControlAck::BytesChunk { data: slice }).await;
    let _ = ack.send(ControlAck::StreamDone).await;
}

/// Tiny `ChunkFetcher` over an owned `HashMap<addr, wire>` so the feed
/// test dispatcher can drive `resolve_sequence_feed_full` without
/// touching the live network.
struct MapFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
}

#[async_trait::async_trait]
impl ant_retrieval::ChunkFetcher for MapFetcher {
    async fn fetch(
        &self,
        addr: [u8; 32],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.chunks.get(&addr).cloned().ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                format!("not found: {}", hex::encode(addr)).into()
            },
        )
    }
}

fn feed_uri(owner: &[u8; 20], topic: &[u8; 32]) -> String {
    format!("/feeds/{}/{}", hex::encode(owner), hex::encode(topic))
}

/// 8-byte big-endian hex encoding of a sequence index, the exact shape
/// bee's `swarm-feed-index` / `swarm-feed-index-next` headers carry
/// (`index::MarshalBinary`).
fn index_hex(index: u64) -> String {
    hex::encode(index.to_be_bytes())
}

#[tokio::test]
async fn get_feed_dereferences_to_latest_content_with_bee_headers() {
    // Bee's `GET /feeds` resolves the latest update and streams the
    // *content* the update points at, with the index / next-index /
    // signature / resolved-version headers. Three updates: the body
    // must be the content of update 2, and the headers must reflect
    // index 2 (and next index 3) in bee's 8-byte big-endian shape.
    let scenario = FeedScenario::new(0x21, 0xb1);
    let latest = b"the-latest-feed-content";
    let router = scenario.router(&[
        (0, b"first".as_slice(), 100),
        (1, b"second".as_slice(), 200),
        (2, latest.as_slice(), 300),
    ]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_TYPE).unwrap(),
        "application/octet-stream",
    );
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        index_hex(2).as_str(),
        "swarm-feed-index must be the 8-byte big-endian sequence index, like bee",
    );
    assert_eq!(
        resp.headers().get("swarm-feed-index-next").unwrap(),
        index_hex(3).as_str(),
    );
    assert_eq!(
        resp.headers().get("swarm-feed-resolved-version").unwrap(),
        "v1",
    );
    let sig = resp
        .headers()
        .get("swarm-soc-signature")
        .expect("swarm-soc-signature header present")
        .to_str()
        .unwrap();
    assert_eq!(sig.len(), 130, "65-byte signature is 130 hex chars");

    let body = body_bytes(resp).await;
    assert_eq!(body.as_slice(), latest, "body is the dereferenced content");
}

#[tokio::test]
async fn get_feed_v2_dereferences_wrapped_cac_with_v2_version_header() {
    // A v2 update wraps the content root chunk directly. The feed
    // resolves to the wrapped CAC's own address, the body is that
    // content, and `swarm-feed-resolved-version` is `v2`. Content is
    // > 40 bytes so the wrapped CAC is unambiguously non-v1-length.
    let scenario = FeedScenario::new(0x2a, 0xba);
    let content = b"v2 feed content that is comfortably longer than forty bytes long";
    let router = scenario.router_v2(0, content.as_slice());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("swarm-feed-resolved-version").unwrap(),
        "v2",
    );
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        index_hex(0).as_str(),
    );
    let body = body_bytes(resp).await;
    assert_eq!(
        body.as_slice(),
        content,
        "v2 body is the wrapped CAC's content",
    );
}

#[tokio::test]
async fn get_feed_only_root_chunk_returns_raw_chunk() {
    // `Swarm-Only-Root-Chunk: true` returns the resolved reference's
    // root chunk verbatim (`span(8 LE) || payload`) instead of the
    // joined content, matching bee's only-root-chunk branch.
    let scenario = FeedScenario::new(0x22, 0xb2);
    let content = b"root-chunk-bytes";
    let router = scenario.router(&[(0, content.as_slice(), 7)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header("swarm-only-root-chunk", "true")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        index_hex(0).as_str(),
    );
    let (_addr, expected_wire) = ant_crypto::cac_new(content).unwrap();
    let body = body_bytes(resp).await;
    assert_eq!(
        body.as_slice(),
        expected_wire.as_slice(),
        "only-root-chunk body is span(8 LE) || payload of the content chunk",
    );
}

#[tokio::test]
async fn get_feed_range_returns_partial_content() {
    let scenario = FeedScenario::new(0x23, 0xb3);
    let content = b"0123456789abcdef";
    let router = scenario.router(&[(0, content.as_slice(), 1)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .header(header::RANGE, "bytes=2-5")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        "bytes 2-5/16",
    );
    // Range responses still carry the feed headers.
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        index_hex(0).as_str(),
    );
    let body = body_bytes(resp).await;
    assert_eq!(body.as_slice(), b"2345");
}

#[tokio::test]
async fn get_feed_defaults_to_sequence_type() {
    // No `?type=` parameter; should default to Sequence (the only
    // implemented variant) and resolve.
    let scenario = FeedScenario::new(0x25, 0xb5);
    let router = scenario.router(&[(0, b"x".as_slice(), 1)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_feed_explicit_type_sequence_accepted() {
    let scenario = FeedScenario::new(0x26, 0xb6);
    let router = scenario.router(&[(0, b"x".as_slice(), 1)]);
    let uri = format!(
        "/feeds/{}/{}?type=sequence",
        hex::encode(scenario.owner),
        hex::encode(scenario.topic),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_feed_type_param_is_ignored_like_bee() {
    // Bee's feedGetHandler never reads `?type=` — `epoch`, `bogus`,
    // anything runs the normal sequence lookup. On an empty feed that
    // is bee's `404 "no update found"`.
    for kind in ["epoch", "bogus"] {
        let scenario = FeedScenario::new(0x27, 0xb7);
        let router = scenario.router(&[]);
        let uri = format!(
            "/feeds/{}/{}?type={kind}",
            hex::encode(scenario.owner),
            hex::encode(scenario.topic),
        );
        let resp = send(
            router,
            Request::builder()
                .method(Method::GET)
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(json["message"], "no update found");
    }
}

#[tokio::test]
async fn get_feed_accepts_0x_prefixed_hex() {
    let scenario = FeedScenario::new(0x29, 0xb9);
    let router = scenario.router(&[(0, b"x".as_slice(), 2)]);
    let uri = format!(
        "/feeds/0x{}/0X{}",
        hex::encode(scenario.owner),
        hex::encode(scenario.topic),
    );
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn get_feed_rejects_bad_owner_hex() {
    let scenario = FeedScenario::new(0x2a, 0xba);
    let router = scenario.router(&[]);
    let uri = format!("/feeds/{}/{}", "ab".repeat(19), hex::encode(scenario.topic));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "owner");
}

#[tokio::test]
async fn get_feed_rejects_bad_topic_hex() {
    let scenario = FeedScenario::new(0x2b, 0xbb);
    let router = scenario.router(&[]);
    let uri = format!("/feeds/{}/{}", hex::encode(scenario.owner), "zz".repeat(32));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "topic");
}

#[tokio::test]
async fn get_feed_returns_404_for_empty_feed() {
    let scenario = FeedScenario::new(0x2c, 0xbc);
    let router = scenario.router(&[]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_feed_does_not_set_immutable_cache_control() {
    let scenario = FeedScenario::new(0x2d, 0xbd);
    let router = scenario.router(&[(0, b"x".as_slice(), 1)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .expect("/feeds must emit Cache-Control");
    let cc_str = cc.to_str().unwrap();
    assert!(
        !cc_str.contains("immutable"),
        "/feeds Cache-Control must not be immutable: {cc_str}",
    );
    assert!(
        cc_str.contains("no-cache"),
        "/feeds Cache-Control should be `no-cache`, got {cc_str}",
    );
}

#[tokio::test]
async fn head_feed_returns_headers_no_body() {
    // `HEAD /feeds/...` runs the full lookup so existence-probe clients
    // can read the feed headers and `Content-Length` without paying for
    // the body transfer.
    let scenario = FeedScenario::new(0x30, 0xc0);
    let content = b"head-content-blob";
    let router = scenario.router(&[(0, content.as_slice(), 5)]);
    let resp = send(
        router,
        Request::builder()
            .method(Method::HEAD)
            .uri(feed_uri(&scenario.owner, &scenario.topic))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get(header::CONTENT_LENGTH).unwrap(),
        content.len().to_string().as_str(),
        "HEAD must echo the GET body length (the dereferenced content)",
    );
    assert_eq!(
        resp.headers().get("swarm-feed-index").unwrap(),
        index_hex(0).as_str(),
    );
    let body = body_bytes(resp).await;
    assert!(body.is_empty(), "HEAD body must be empty");
}

// `POST /soc/{owner}/{id}` SOC upload coverage.
//
// The SOC handler validates the path, header, and body before
// dispatching `ControlCommand::PushSoc` to the node loop. Tests below
// pin every branch:
//
// 1. Happy path: a properly signed SOC dispatches and returns the
//    `keccak256(id || owner)` reference at status 201.
// 2. Path, header, and body validation each rejects with 400 and a
//    bee-shaped JSON error before the channel is touched.
// 3. The dispatched `ControlCommand::PushSoc` carries the correct
//    address and wire bytes (`id || sig || inner_cac`).
// 4. Node-side errors map onto the right HTTP status (503 for
//    unconfigured uploads, 502 for everything else).

const SOC_SIGNATURE_HEADER: &str = "swarm-soc-signature";

fn soc_uri(owner: &[u8; 20], id: &[u8; 32]) -> String {
    format!("/soc/{}/{}", hex::encode(owner), hex::encode(id))
}

/// `POST /soc/{owner}/{id}` with a valid signature returns
/// `{"reference": "<keccak256(id || owner) hex>"}` at 201. The fixture
/// node's `PushSoc` arm trusts the gateway's `soc_valid` pre-check and
/// echoes the SOC address, the same shape as the production `ant-p2p`
/// handler, so a regression in either side trips the assertion.
#[tokio::test]
async fn post_soc_returns_bee_shaped_reference() {
    let secret = [0xa1u8; 32];
    let id = [0x11u8; 32];
    let fx = signed_soc(secret, id, b"soc happy path");

    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&fx.owner, &fx.id))
            .header(SOC_SIGNATURE_HEADER, hex::encode(fx.signature))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(fx.inner_cac_wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(
        json["reference"].as_str().unwrap(),
        hex::encode(fx.soc_address),
        "reference must be bare lowercase 64-hex matching keccak256(id || owner)",
    );
}

/// `0x` / `0X` prefixes on path components and the signature header
/// are accepted (mirrors bee, which is lenient about prefixes).
#[tokio::test]
async fn post_soc_accepts_0x_prefixed_hex() {
    let secret = [0x77u8; 32];
    let id = [0x42u8; 32];
    let fx = signed_soc(secret, id, b"prefixed hex");

    let router = handle_with_fixture_node();
    let uri = format!("/soc/0x{}/0X{}", hex::encode(fx.owner), hex::encode(fx.id));
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(uri)
            .header(
                SOC_SIGNATURE_HEADER,
                format!("0x{}", hex::encode(fx.signature)),
            )
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(fx.inner_cac_wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
}

/// `keccak256(id || owner)` must drive the dispatched `PushSoc.address`,
/// and the wire must be the literal `id || sig || inner_cac` concatenation,
/// not, e.g., a re-stamped CAC. Drives a custom dispatcher so we can
/// inspect the exact `ControlCommand` the gateway emits.
#[tokio::test]
async fn post_soc_dispatches_expected_address_and_wire() {
    let secret = [0x33u8; 32];
    let id = [0x55u8; 32];
    let fx = signed_soc(secret, id, b"dispatch shape");
    let expected_wire = {
        let mut w = Vec::with_capacity(32 + 65 + fx.inner_cac_wire.len());
        w.extend_from_slice(&fx.id);
        w.extend_from_slice(&fx.signature);
        w.extend_from_slice(&fx.inner_cac_wire);
        w
    };

    struct Captured {
        wire: Vec<u8>,
        address: [u8; 32],
    }
    let captured: Arc<Mutex<Option<Captured>>> = Arc::new(Mutex::new(None));
    let captured_for_dispatch = captured.clone();
    let router = router_with_dispatcher(move |cmd| {
        let captured = captured_for_dispatch.clone();
        async move {
            if let ControlCommand::PushSoc {
                address, wire, ack, ..
            } = cmd
            {
                *captured.lock().await = Some(Captured { wire, address });
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(address)),
                });
            }
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&fx.owner, &fx.id))
            .header(SOC_SIGNATURE_HEADER, hex::encode(fx.signature))
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(fx.inner_cac_wire.clone()))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let guard = captured.lock().await;
    let cap = guard.as_ref().expect("PushSoc must be dispatched");
    assert_eq!(cap.address, fx.soc_address);
    assert_eq!(cap.wire, expected_wire);
}

/// SOC writes are *mutable* at the address level (the owner can resign
/// at the same id), so the handler must not emit `Cache-Control:
/// immutable` the way `upload_chunk` does for CAC chunks.
#[tokio::test]
async fn post_soc_omits_immutable_cache_control() {
    let secret = [0x12u8; 32];
    let id = [0x34u8; 32];
    let fx = signed_soc(secret, id, b"mutable address");

    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&fx.owner, &fx.id))
            .header(SOC_SIGNATURE_HEADER, hex::encode(fx.signature))
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(fx.inner_cac_wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);
    let cc = resp.headers().get(header::CACHE_CONTROL);
    assert!(
        cc.is_none_or(|v| !v.to_str().unwrap().contains("immutable")),
        "POST /soc must not advertise `immutable` (SOC addresses can be re-signed): {cc:?}",
    );
}

/// Owner path component must decode to exactly 20 bytes; anything else
/// is bee's `400 invalid path params` and never reaches the channel.
#[tokio::test]
async fn post_soc_rejects_bad_owner_hex() {
    let id = [0u8; 32];
    let body = vec![0u8; 16];
    let captured: Arc<Mutex<bool>> = Arc::new(Mutex::new(false));
    let captured_for_dispatch = captured.clone();
    let router = router_with_dispatcher(move |_cmd| {
        let captured = captured_for_dispatch.clone();
        async move {
            *captured.lock().await = true;
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(format!("/soc/deadbeef/{}", hex::encode(id)))
            .header(SOC_SIGNATURE_HEADER, hex::encode([0u8; 65]))
            .body(Body::from(body))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 400);
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "owner");
    assert!(
        !*captured.lock().await,
        "validation must fail before the command channel is touched",
    );
}

/// Id path component must decode to exactly 32 bytes; anything else is
/// bee's `400 invalid path params`.
#[tokio::test]
async fn post_soc_rejects_bad_id_hex() {
    let owner = [0u8; 20];
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(format!("/soc/{}/abc", hex::encode(owner)))
            .header(SOC_SIGNATURE_HEADER, hex::encode([0u8; 65]))
            .body(Body::from(vec![0u8; 16]))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "invalid path params");
    assert_eq!(json["reasons"][0]["field"], "id");
}

/// A request with neither the `swarm-soc-signature` header nor the
/// `?sig=` query param is a 400. The signature is mandatory: there's no
/// fall-back recovery path in bee.
#[tokio::test]
async fn post_soc_rejects_missing_signature_header() {
    let owner = [0u8; 20];
    let id = [0u8; 32];
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&owner, &id))
            .body(Body::from(vec![0u8; 16]))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    // Bee: the required `?sig=` query parameter is missing.
    assert_eq!(json["message"], "invalid query params");
    assert_eq!(json["reasons"][0]["field"], "sig");
    assert_eq!(json["reasons"][0]["error"], "want required:");
}

/// Non-ASCII bytes in the signature header are caught by `to_str()`
/// before the hex parser sees them.
#[tokio::test]
async fn post_soc_rejects_non_ascii_signature_header() {
    use axum::http::HeaderValue;

    let owner = [0u8; 20];
    let id = [0u8; 32];
    let mut req = Request::builder()
        .method(Method::POST)
        .uri(soc_uri(&owner, &id))
        .body(Body::from(vec![0u8; 16]))
        .unwrap();
    req.headers_mut().insert(
        SOC_SIGNATURE_HEADER,
        HeaderValue::from_bytes(&[0xC3, 0xA9]).expect("non-ascii header value"),
    );

    let router = handle_with_fixture_node();
    let resp = send(router, req).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "invalid header params");
    assert_eq!(json["reasons"][0]["field"], "swarm-soc-signature");
}

/// A signature header that's the wrong length, or contains non-hex
/// characters, surfaces as a bee-shaped header-params error.
#[tokio::test]
async fn post_soc_rejects_malformed_signature_hex() {
    let owner = [0u8; 20];
    let id = [0u8; 32];
    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&owner, &id))
            .header(SOC_SIGNATURE_HEADER, "deadbeef")
            .body(Body::from(vec![0u8; 16]))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "invalid header params");
    assert_eq!(json["reasons"][0]["field"], "swarm-soc-signature");
}

/// Bodies smaller than the inner-CAC span (8 bytes) and larger than
/// `8 + 4096` are rejected up-front so the gateway can't be coerced
/// into stamping a malformed chunk.
#[tokio::test]
async fn post_soc_rejects_body_size_out_of_range() {
    let secret = [0x66u8; 32];
    let id = [0u8; 32];
    let fx = signed_soc(secret, id, b"size check");

    for (label, body) in [
        ("under-span", vec![0u8; 7]),
        ("over-cap", vec![0u8; 8 + 4097]),
    ] {
        let router = handle_with_fixture_node();
        let resp = send(
            router,
            Request::builder()
                .method(Method::POST)
                .uri(soc_uri(&fx.owner, &fx.id))
                .header(SOC_SIGNATURE_HEADER, hex::encode(fx.signature))
                .body(Body::from(body))
                .unwrap(),
        )
        .await;
        // Bee: `400 "short chunk data"` below the span size,
        // `413 "payload too large"` above the CAC wire cap.
        let json_status = resp.status();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        if label == "under-span" {
            assert_eq!(json_status, StatusCode::BAD_REQUEST, "{label}");
            assert_eq!(json["message"], "short chunk data", "{label}");
        } else {
            assert_eq!(json_status, StatusCode::PAYLOAD_TOO_LARGE, "{label}");
            assert_eq!(json["message"], "payload too large", "{label}");
        }
    }
}

/// `soc_valid` rejection (signature recovers a different owner than
/// the path claims) maps to bee's `401 "invalid chunk"`. Constructed
/// by signing with `secret` but submitting under another secret's
/// owner address.
#[tokio::test]
async fn post_soc_rejects_self_binding_mismatch() {
    let real_secret = [0x01u8; 32];
    let id = [0u8; 32];
    let fx_real = signed_soc(real_secret, id, b"valid sig");

    // Compute a *different* owner from a second secret so the path
    // mismatches the recovered signer.
    let other_owner = signed_soc([0x02u8; 32], id, b"unused").owner;
    assert_ne!(other_owner, fx_real.owner);

    let router = handle_with_fixture_node();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&other_owner, &fx_real.id))
            .header(SOC_SIGNATURE_HEADER, hex::encode(fx_real.signature))
            .body(Body::from(fx_real.inner_cac_wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["code"], 401);
    assert_eq!(json["message"], "invalid chunk");
}

/// A node-side readiness signal (`uploads not configured`, "no peers
/// yet") arrives as `ControlAck::NotReady` and must surface as 503 so
/// publishers can distinguish "the daemon needs more startup" from
/// "the daemon refused this request" — the same shape the SOC read
/// path uses for missing-peer states.
#[tokio::test]
async fn post_soc_maps_not_ready_to_503() {
    let secret = [0x88u8; 32];
    let id = [0u8; 32];
    let fx = signed_soc(secret, id, b"unconfigured");

    let router = router_with_dispatcher(|cmd| async move {
        if let ControlCommand::PushSoc { ack, .. } = cmd {
            let _ = ack.send(ControlAck::NotReady {
                message: "uploads not configured: pass --postage-batch and --postage-owner-key"
                    .into(),
            });
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&fx.owner, &fx.id))
            .header(SOC_SIGNATURE_HEADER, hex::encode(fx.signature))
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(fx.inner_cac_wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// Generic node-side errors fall through to `502 Bad Gateway`. The
/// `pushsync: …` shape is what `ant-p2p`'s real handler emits when
/// the neighbourhood refuses the chunk.
#[tokio::test]
async fn post_soc_maps_pushsync_failure_to_502() {
    let secret = [0x99u8; 32];
    let id = [0u8; 32];
    let fx = signed_soc(secret, id, b"pushsync fail");

    let router = router_with_dispatcher(|cmd| async move {
        if let ControlCommand::PushSoc { ack, .. } = cmd {
            let _ = ack.send(ControlAck::Error {
                message: "pushsync: no peers in neighbourhood".into(),
            });
        }
    });

    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri(soc_uri(&fx.owner, &fx.id))
            .header(SOC_SIGNATURE_HEADER, hex::encode(fx.signature))
            .header(
                "swarm-postage-batch-id",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .body(Body::from(fx.inner_cac_wire))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}

/// `/pss/send` must reject an unusable batch *before* nonce mining: the
/// fixture node has no registered batches, so the request fails fast
/// with the standard "not usable" message instead of spending ~2^24 BMT
/// hashes on the 3-byte target and only then learning the batch is
/// bogus.
#[tokio::test]
async fn pss_send_rejects_unusable_batch_before_mining() {
    let router = handle_with_fixture_node();
    let started = std::time::Instant::now();
    let resp = send(
        router,
        Request::builder()
            .method(Method::POST)
            .uri("/pss/send/test-topic/aabbcc")
            .header("swarm-postage-batch-id", "11".repeat(32))
            .body(Body::from("hello"))
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let text = String::from_utf8_lossy(&body_bytes(resp).await).into_owned();
    assert!(text.contains("not usable"), "got: {text}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "must fail fast, not mine"
    );
}

/// `/pss/send`'s pre-flight applies `PushChunk`'s bar: a just-bought
/// batch still propagating to the storers (`usable: false,
/// propagating: true`) is accepted — the node push waits the
/// propagation out — while a peer-rejected batch (`usable: false`
/// alone) is refused before mining.
#[tokio::test]
async fn pss_send_accepts_a_propagating_batch_but_not_a_rejected_one() {
    use ant_control::PostageStatusView;
    let batch_hex = "22".repeat(32);
    for (propagating, want) in [
        (true, StatusCode::CREATED),
        (false, StatusCode::BAD_REQUEST),
    ] {
        let batch_id = format!("0x{batch_hex}");
        let router = router_with_dispatcher(move |cmd| {
            let batch_id = batch_id.clone();
            async move {
                match cmd {
                    ControlCommand::PostageList { ack } => {
                        let _ = ack.send(ControlAck::PostageList(vec![PostageStatusView {
                            enabled: true,
                            usable: false,
                            propagating,
                            batch_id,
                            batch_depth: 20,
                            bucket_depth: 16,
                            ..PostageStatusView::default()
                        }]));
                    }
                    ControlCommand::PushChunk { ack, .. } => {
                        let _ = ack.send(ControlAck::ChunkUploaded {
                            reference: format!("0x{}", "00".repeat(32)),
                        });
                    }
                    _ => {}
                }
            }
        });
        let resp = send(
            router,
            Request::builder()
                .method(Method::POST)
                // A 1-byte target: mining is a few hundred hashes.
                .uri("/pss/send/test-topic/aa")
                .header("swarm-postage-batch-id", batch_hex.as_str())
                .body(Body::from("hello"))
                .unwrap(),
        )
        .await;
        let status = resp.status();
        let text = String::from_utf8_lossy(&body_bytes(resp).await).into_owned();
        assert_eq!(status, want, "propagating={propagating}: {text}");
    }
}
