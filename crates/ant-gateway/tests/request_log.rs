//! Per-request debug logging (issue #82): every request gets an id in
//! its `http` span, is logged on arrival with its `Range`, and the
//! response body logs when it finishes — bytes sent vs `Content-Length`,
//! duration, and whether it ended early — so a truncated body can be
//! tied back to the request it belongs to. Below `debug` none of it runs.

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use ant_control::{ControlAck, ControlCommand};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use common::{router_with_dispatcher, send};
use http_body_util::BodyExt;
use tracing_subscriber::fmt::MakeWriter;

const REF: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// Install a thread-local subscriber at `level` (the tests run on a
/// current-thread runtime, so handler, body polling and drop all log
/// through it).
fn capture(level: &str) -> (Captured, tracing::subscriber::DefaultGuard) {
    let out = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(level))
        .with_writer(out.clone())
        .with_ansi(false)
        .finish();
    let guard = tracing::subscriber::set_default(subscriber);
    (out, guard)
}

/// `/bytes` stub that announces `total` bytes, sends `data`, then ends
/// with `tail` (`StreamDone`, an `Error`, or nothing at all).
fn router(total: u64, data: &'static [u8], tail: Option<ControlAck>) -> axum::Router {
    router_with_dispatcher(move |cmd| {
        let tail = tail.clone();
        async move {
            if let ControlCommand::StreamBytes { ack, .. } = cmd {
                let _ = ack
                    .send(ControlAck::BytesStreamStart { total_bytes: total })
                    .await;
                let _ = ack
                    .send(ControlAck::BytesChunk {
                        data: data.to_vec(),
                    })
                    .await;
                if let Some(tail) = tail {
                    let _ = ack.send(tail).await;
                }
            }
        }
    })
}

fn get(range: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method(Method::GET)
        .uri(format!("/bytes/{REF}"));
    if let Some(r) = range {
        b = b.header(header::RANGE, r);
    }
    b.body(Body::empty()).unwrap()
}

/// The one log line carrying `needle`, asserting there is exactly one.
fn line<'a>(log: &'a str, needle: &str) -> &'a str {
    let hits: Vec<&str> = log.lines().filter(|l| l.contains(needle)).collect();
    assert_eq!(hits.len(), 1, "want one {needle:?} line in:\n{log}");
    hits[0]
}

/// The `req=N` id in the line's span prefix.
fn req_id(line: &str) -> &str {
    let at = line.find("req=").expect("req id in span") + 4;
    let rest = &line[at..];
    &rest[..rest.find(|c: char| !c.is_ascii_digit()).unwrap()]
}

#[tokio::test]
async fn complete_body_logs_bytes_against_content_length() {
    let (out, _g) = capture("ant_gateway=debug");
    let resp = send(router(4, b"stub", Some(ControlAck::StreamDone)), get(None)).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"stub");

    let log = out.text();
    let received = line(&log, "request received");
    let finished = line(&log, "response body finished");
    assert!(
        received.contains(&format!("path=/bytes/{REF}")),
        "{received}"
    );
    assert!(received.contains("range=\"-\""), "{received}");
    assert_eq!(req_id(received), req_id(finished));
    for want in [
        "status=200",
        "bytes_sent=4",
        "content_length=4",
        "end=\"complete\"",
        "ended_early=false",
        "elapsed_ms=",
    ] {
        assert!(finished.contains(want), "{want} missing: {finished}");
    }
}

/// The #79 shape: `200` with the full `Content-Length`, then the node's
/// stream fails after part of the body. The finish line must name the
/// shortfall and the node's error.
#[tokio::test]
async fn body_error_after_headers_logs_ended_early_with_reason() {
    let (out, _g) = capture("ant_gateway=debug");
    let resp = send(
        router(
            8,
            b"half",
            Some(ControlAck::Error {
                message: "retrieve chunk: no peer found".into(),
            }),
        ),
        get(None),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.into_body().collect().await.is_err());

    let log = out.text();
    let finished = line(&log, "response body finished");
    for want in [
        "bytes_sent=4",
        "content_length=8",
        "end=\"error\"",
        "ended_early=true",
        "no peer found",
    ] {
        assert!(finished.contains(want), "{want} missing: {finished}");
    }
    assert_eq!(req_id(line(&log, "request received")), req_id(finished));
}

/// A node stream that just stops (channel closed) short of the announced
/// length ends the body "cleanly" — the byte count is what shows it.
#[tokio::test]
async fn body_ending_short_of_content_length_is_flagged() {
    let (out, _g) = capture("ant_gateway=debug");
    let resp = send(router(8, b"half", None), get(None)).await;
    let _ = resp.into_body().collect().await;

    let log = out.text();
    let finished = line(&log, "response body finished");
    for want in ["bytes_sent=4", "content_length=8", "ended_early=true"] {
        assert!(finished.contains(want), "{want} missing: {finished}");
    }
}

/// Client goes away mid-body: the body is dropped before its end.
#[tokio::test]
async fn abandoned_body_logs_aborted() {
    let (out, _g) = capture("ant_gateway=debug");
    let resp = send(router(8, b"half", None), get(None)).await;
    let mut body = resp.into_body();
    let first = body.frame().await.unwrap().unwrap();
    assert_eq!(first.data_ref().map(axum::body::Bytes::len), Some(4));
    drop(body);

    let log = out.text();
    let finished = line(&log, "response body finished");
    for want in ["bytes_sent=4", "end=\"aborted\"", "ended_early=true"] {
        assert!(finished.contains(want), "{want} missing: {finished}");
    }
}

#[tokio::test]
async fn range_header_is_logged() {
    let (out, _g) = capture("ant_gateway=debug");
    let resp = send(
        router(4, b"stub", Some(ControlAck::StreamDone)),
        get(Some("bytes=0-1")),
    )
    .await;
    let _ = resp.into_body().collect().await;
    let log = out.text();
    let received = line(&log, "request received");
    assert!(received.contains("range=\"bytes=0-1\""), "{received}");
}

/// `HEAD` carries the would-be length over an empty body; not a cut.
#[tokio::test]
async fn head_is_not_wrapped() {
    let (out, _g) = capture("ant_gateway=debug");
    let resp = send(
        router(4, b"stub", Some(ControlAck::StreamDone)),
        Request::builder()
            .method(Method::HEAD)
            .uri(format!("/bytes/{REF}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let _ = resp.into_body().collect().await;
    let log = out.text();
    assert!(log.contains("request received"), "{log}");
    assert!(!log.contains("response body finished"), "{log}");
}

/// At `info` none of the per-request debug lines appear.
#[tokio::test]
async fn info_level_logs_nothing_per_request() {
    let (out, _g) = capture("info");
    let resp = send(
        router(
            8,
            b"half",
            Some(ControlAck::Error {
                message: "boom".into(),
            }),
        ),
        get(None),
    )
    .await;
    let _ = resp.into_body().collect().await;
    let log = out.text();
    assert!(!log.contains("request received"), "{log}");
    assert!(!log.contains("response body finished"), "{log}");
}

/// Through a real hyper connection: hyper stops polling once it has
/// written `Content-Length` bytes and drops the body without its end of
/// stream, which must still read as complete (an in-process `oneshot`
/// drains to the end and can't show this).
#[tokio::test]
async fn complete_body_over_real_connection_is_not_aborted() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (out, _g) = capture("ant_gateway=debug");
    let app = router(4, b"stub", Some(ControlAck::StreamDone));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    conn.write_all(
        format!("GET /bytes/{REF} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut resp = Vec::new();
    conn.read_to_end(&mut resp).await.unwrap();
    assert!(resp.ends_with(b"\r\n\r\nstub"));

    // The body is dropped by the server task; give it a moment.
    let mut log = String::new();
    for _ in 0..100 {
        log = out.text();
        if log.contains("response body finished") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let finished = line(&log, "response body finished");
    for want in [
        "bytes_sent=4",
        "content_length=4",
        "end=\"complete\"",
        "ended_early=false",
    ] {
        assert!(finished.contains(want), "{want} missing: {finished}");
    }
}
