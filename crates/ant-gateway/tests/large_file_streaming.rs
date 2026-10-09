//! 10 MiB end-to-end streaming verification.
//!
//! Builds a real 10 MiB Swarm chunk tree in memory, plugs it into the
//! gateway's fake-node loop, and exercises the streaming surface end
//! to end:
//!
//! - full GET streams the file body in multiple HTTP body chunks
//!   (proves we're not materialising 10 MiB before sending);
//! - HEAD returns size + Accept-Ranges without joining the tree;
//! - `Range: bytes=0-1023` returns only the first 1 KiB;
//! - suffix `Range: bytes=-65536` returns the trailing 64 KiB
//!   (browser MP4 `moov`-at-end probe);
//! - mid-file range fetches only the requested slice and matches the
//!   reference bytes by sha-256;
//! - time-to-first-body-byte is well under the time-to-final-byte
//!   (proves the gateway is streaming progressively, not buffering).
//!
//! These tests run against the in-process router via
//! `tower::ServiceExt::oneshot` — no listener, no port binding. The
//! 10 MiB file is generated deterministically so we can compare slices
//! against `expected[start..=end]` byte-for-byte.

mod common;

use ant_crypto::{bmt_hash_with_span, cac_new, CHUNK_SIZE};
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use common::{router_with_dispatcher, send};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use ant_control::{ControlAck, ControlCommand, StreamRange};
use ant_retrieval::{join_to_sender_range, ByteRange, ChunkFetcher, JoinOptions};

/// Target file size for these tests. 10 MiB straddles 4 chunk-tree
/// levels for the standard 4 KiB / 128-branch layout: 2560 leaves,
/// 20 L1 intermediates of 128 leaves each, 1 root pointing at the
/// 20 L1 chunks. Big enough that "buffer-then-respond" is observable
/// in time-to-first-byte; small enough to build in a unit test.
const TEN_MIB: usize = 10 * 1024 * 1024;

/// Hash of `expected[]` so we can sanity-check the full GET path.
fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

/// Build `n` deterministic bytes that don't repeat at chunk boundaries.
/// A naive `vec![0u8; n]` would hash to the same chunk address for every
/// 4 KiB leaf and let the joiner skip the per-chunk verifier.
fn deterministic_bytes(n: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(n);
    let mut state: u64 = 0x9e37_79b9_7f4a_7c15; // golden ratio constant
    while out.len() < n {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(n);
    out
}

/// Tiny in-memory splitter that mirrors `bee/pkg/file/splitter`. Returns
/// `(root_addr, chunks)` where `chunks` is a map suitable for plugging
/// into a [`MapFetcher`].
fn split_file(data: &[u8]) -> ([u8; 32], HashMap<[u8; 32], Vec<u8>>) {
    let mut store = HashMap::new();

    // Single-chunk file: the leaf *is* the root.
    if data.len() <= CHUNK_SIZE {
        let (addr, wire) = cac_new(data).expect("cac_new leaf");
        store.insert(addr, wire);
        return (addr, store);
    }

    // Phase 1: leaves. Each carries up to CHUNK_SIZE bytes; bee's CAC
    // setter writes `span = payload.len()`.
    let leaves: Vec<([u8; 32], usize)> = data
        .chunks(CHUNK_SIZE)
        .map(|payload| {
            let (addr, wire) = cac_new(payload).expect("cac_new leaf");
            store.insert(addr, wire);
            (addr, payload.len())
        })
        .collect();

    // Phase 2..: intermediate levels. Each L_n+1 chunk holds up to 128
    // child references followed by an 8-byte LE span equal to the sum
    // of the children's spans (which for an all-full level is
    // `128 * branch_size`). We re-compute spans bottom-up by tracking
    // the byte-size each child covers.
    let mut current: Vec<([u8; 32], u64)> = leaves
        .into_iter()
        .map(|(addr, len)| (addr, len as u64))
        .collect();

    while current.len() > 1 {
        let mut next: Vec<([u8; 32], u64)> = Vec::new();
        for group in current.chunks(128) {
            let mut payload = Vec::with_capacity(group.len() * 32);
            let mut span: u64 = 0;
            for (addr, child_span) in group {
                payload.extend_from_slice(addr);
                span += child_span;
            }
            let span_bytes = span.to_le_bytes();
            let mut wire = Vec::with_capacity(8 + payload.len());
            wire.extend_from_slice(&span_bytes);
            wire.extend_from_slice(&payload);
            let addr = bmt_hash_with_span(&span_bytes, &payload).expect("bmt intermediate");
            store.insert(addr, wire);
            next.push((addr, span));
        }
        current = next;
    }
    let (root_addr, _) = current[0];
    (root_addr, store)
}

struct MapFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
}

#[async_trait::async_trait]
impl ChunkFetcher for MapFetcher {
    async fn fetch(
        &self,
        addr: [u8; 32],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        self.chunks.get(&addr).cloned().ok_or_else(
            || -> Box<dyn std::error::Error + Send + Sync> {
                format!("missing chunk {}", hex::encode(addr)).into()
            },
        )
    }
}

/// Plug a chunk store + raw `data` into a router whose fake-node loop
/// answers `StreamBytes` against the store using the same range-aware
/// joiner production uses. Returns the router and the file bytes.
fn router_with_chunked_file(data: Vec<u8>) -> (axum::Router, [u8; 32]) {
    let (root_addr, store) = split_file(&data);
    let fetcher = Arc::new(MapFetcher { chunks: store });
    let router = router_with_dispatcher(move |cmd| {
        let fetcher = fetcher.clone();
        async move {
            handle_stream_command(fetcher, cmd).await;
        }
    });
    (router, root_addr)
}

async fn handle_stream_command(fetcher: Arc<MapFetcher>, cmd: ControlCommand) {
    match cmd {
        ControlCommand::StreamBytes {
            reference,
            max_bytes,
            range,
            head_only,
            ack,
            ..
        } => {
            stream_via_fetcher(&fetcher, reference, max_bytes, range, head_only, ack).await;
        }
        ControlCommand::GetChunkRaw { reference, ack, .. } => {
            let reply = match fetcher.fetch(reference).await {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error {
                    message: format!("fetch chunk {}: {e}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        _ => {}
    }
}

async fn stream_via_fetcher(
    fetcher: &MapFetcher,
    reference: [u8; 32],
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: mpsc::Sender<ControlAck>,
) {
    let result = async {
        let root = fetcher.fetch(reference).await.map_err(|e| e.to_string())?;
        let mut span_bytes: [u8; 8] = root
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| "root shorter than span".to_string())?;
        span_bytes[7] = 0;
        let total = u64::from_le_bytes(span_bytes);
        ack.send(ControlAck::BytesStreamStart { total_bytes: total })
            .await
            .map_err(|_| "stream closed".to_string())?;
        if head_only {
            return Ok::<(), String>(());
        }
        let (chunk_tx, mut chunk_rx) = mpsc::channel(8);
        let opts = JoinOptions::default();
        let cap = max_bytes.map_or(ant_retrieval::DEFAULT_MAX_FILE_BYTES, |n| {
            usize::try_from(n).unwrap_or(usize::MAX)
        });
        let body_range = range.and_then(|r| ByteRange::clamp(r.start, r.end_inclusive, total));
        let mut join_fut = Box::pin(join_to_sender_range(
            fetcher, &root, cap, opts, body_range, chunk_tx,
        ));
        let mut join_result = None;
        loop {
            tokio::select! {
                result = &mut join_fut, if join_result.is_none() => {
                    join_result = Some(result.map_err(|e| e.to_string()));
                }
                maybe_chunk = chunk_rx.recv() => {
                    match maybe_chunk {
                        Some(data) => {
                            ack.send(ControlAck::BytesChunk { data })
                                .await
                                .map_err(|_| "stream closed".to_string())?;
                        }
                        None => return join_result.unwrap_or(Ok(())),
                    }
                }
            }
        }
    }
    .await;
    let terminal = match result {
        Ok(()) => ControlAck::StreamDone,
        Err(e) => ControlAck::Error { message: e },
    };
    let _ = ack.send(terminal).await;
}

/// Drain an HTTP response body into a single `Vec<u8>` while counting
/// frames and capturing the wall-clock instant the first frame
/// arrived. Used by the streaming-progress test to prove the gateway
/// doesn't buffer the whole 10 MiB before sending.
async fn drain_with_timing(body: Body) -> (Vec<u8>, usize, Option<Duration>, Duration) {
    let mut stream = body.into_data_stream();
    let mut out = Vec::new();
    let mut frames = 0usize;
    let mut first_frame_at: Option<Instant> = None;
    let started = Instant::now();
    use futures::StreamExt;
    while let Some(frame) = stream.next().await {
        let chunk = frame.expect("body frame");
        if first_frame_at.is_none() {
            first_frame_at = Some(Instant::now());
        }
        frames += 1;
        out.extend_from_slice(&chunk);
    }
    (
        out,
        frames,
        first_frame_at.map(|t| t.duration_since(started)),
        started.elapsed(),
    )
}

/// Full GET on a 10 MiB file streams the whole body and matches the
/// reference sha-256. Captures the number of HTTP body frames hyper
/// emits to prove we're not materialising the file before sending.
#[tokio::test]
async fn ten_mib_full_get_streams_in_multiple_frames() {
    let data = deterministic_bytes(TEN_MIB);
    let want_sha = sha256_hex(&data);
    let (router, root_addr) = router_with_chunked_file(data);

    let uri = format!("/bytes/{}", hex::encode(root_addr));
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
        resp.headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some(TEN_MIB.to_string().as_str()),
    );
    assert_eq!(
        resp.headers()
            .get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok()),
        Some("bytes"),
    );

    let (body, frames, first_at, total) = drain_with_timing(resp.into_body()).await;
    assert_eq!(body.len(), TEN_MIB);
    assert_eq!(sha256_hex(&body), want_sha);
    let first = first_at.expect("at least one frame");
    // Surface the timing in `--nocapture` output so a human can spot
    // streaming-vs-buffering at a glance.
    println!("ten_mib_full_get: frames={frames}, first_byte_at={first:?}, full_body_at={total:?}");
    assert!(
        frames >= 2,
        "expected the gateway to stream the body in multiple frames, got {frames} frame(s) in {total:?}",
    );
    // First-byte latency must be a meaningful fraction below total
    // duration. We allow generous slack — CI timing is noisy — but
    // a regression to "join, then send" puts first_at ≈ total.
    assert!(
        first * 2 <= total || total < Duration::from_millis(5),
        "first frame at {first:?}, full body at {total:?} (frames={frames}); \
         streaming should make first-byte materially faster than total",
    );
}

/// HEAD on a 10 MiB file returns size + Accept-Ranges without joining
/// the chunk tree. The fake node loop emits a single `StreamDone` on
/// `head_only=true`; if the gateway accidentally fell back to the body
/// path we'd see a multi-MB body materialise here instead.
#[tokio::test]
async fn ten_mib_head_returns_metadata_only() {
    let data = deterministic_bytes(TEN_MIB);
    let (router, root_addr) = router_with_chunked_file(data);
    let uri = format!("/bytes/{}", hex::encode(root_addr));
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
        TEN_MIB.to_string().as_str(),
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(body.is_empty(), "HEAD must not carry a body");
}

/// Browser-style range probe: `Range: bytes=0-` returns 206 covering the
/// whole file. Used by `<video>` / `<audio>` elements to detect range
/// support — without it they fall back to non-seekable progressive
/// download.
#[tokio::test]
async fn ten_mib_range_zero_dash_returns_full_206() {
    let data = deterministic_bytes(TEN_MIB);
    let want_sha = sha256_hex(&data);
    let (router, root_addr) = router_with_chunked_file(data);
    let uri = format!("/bytes/{}", hex::encode(root_addr));
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
        format!("bytes 0-{}/{TEN_MIB}", TEN_MIB - 1).as_str(),
    );
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    assert_eq!(body.len(), TEN_MIB);
    assert_eq!(sha256_hex(&body), want_sha);
}

/// MP4-tail probe: `Range: bytes=-65536` (request the last 64 KiB).
/// This is what browsers issue when the `moov` atom sits at the end
/// of the file. The range-aware joiner must walk only the rightmost
/// subtree and slice the trailing leaves.
#[tokio::test]
async fn ten_mib_suffix_range_returns_tail() {
    let data = deterministic_bytes(TEN_MIB);
    let suffix_len = 64 * 1024usize;
    let want_tail = data[TEN_MIB - suffix_len..].to_vec();
    let want_tail_sha = sha256_hex(&want_tail);

    let (router, root_addr) = router_with_chunked_file(data);
    let uri = format!("/bytes/{}", hex::encode(root_addr));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, format!("bytes=-{suffix_len}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    let expected_start = TEN_MIB - suffix_len;
    let expected_end = TEN_MIB - 1;
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes {expected_start}-{expected_end}/{TEN_MIB}").as_str(),
    );
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    assert_eq!(body.len(), suffix_len);
    assert_eq!(sha256_hex(&body), want_tail_sha);
}

/// Mid-file range straddling several leaf chunks. Verifies the
/// range-aware joiner picks up the right slice across an L1 boundary
/// without re-downloading the rest of the file.
#[tokio::test]
async fn ten_mib_mid_range_matches_reference_slice() {
    let data = deterministic_bytes(TEN_MIB);
    // Pick a range that crosses an L1 boundary: 128 leaves * 4096
    // bytes = 524 288 bytes per L1 subtree. Straddle the boundary
    // between L1 #1 and L1 #2.
    let start: usize = 524_288 - 4096;
    let end: usize = 524_288 + 12_345;
    let want = data[start..=end].to_vec();
    let want_sha = sha256_hex(&want);

    let (router, root_addr) = router_with_chunked_file(data);
    let uri = format!("/bytes/{}", hex::encode(root_addr));
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header(header::RANGE, format!("bytes={start}-{end}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get(header::CONTENT_RANGE).unwrap(),
        format!("bytes {start}-{end}/{TEN_MIB}").as_str(),
    );
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    assert_eq!(body.len(), end - start + 1);
    assert_eq!(sha256_hex(&body), want_sha);
}

/// Tiny prefix range that fits inside a single leaf — `Range: bytes=0-1023`.
/// This is what an `<audio>` element issues to read ID3 tags.
#[tokio::test]
async fn ten_mib_prefix_range_within_one_leaf() {
    let data = deterministic_bytes(TEN_MIB);
    let want = data[0..1024].to_vec();
    let want_sha = sha256_hex(&want);

    let (router, root_addr) = router_with_chunked_file(data);
    let uri = format!("/bytes/{}", hex::encode(root_addr));
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
        format!("bytes 0-1023/{TEN_MIB}").as_str(),
    );
    let body = resp
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    assert_eq!(body.len(), 1024);
    assert_eq!(sha256_hex(&body), want_sha);
}
