//! D.2.3 retrieval endpoints, streaming edition.
//!
//! Wraps the existing `ant-retrieval` machinery exposed by the node loop
//! over `ant-control`'s command channel. The gateway never talks to
//! libp2p directly — every chunk fetch goes through
//! [`ant_control::ControlCommand`], reusing the routing-aware
//! [`ant_retrieval::RoutingFetcher`], its hedging, and the long-lived
//! chunk cache. Tests swap a fake "node loop" in that replies to the
//! same commands from a fixture-backed map.
//!
//! `/bytes` and `/bzz` go through the streaming dispatch path in all
//! cases:
//!
//! - **GET, no Range**: full streaming join. Headers go out as soon as
//!   the daemon emits `BytesStreamStart` / `BzzStreamStart`; body bytes
//!   stream as the joiner emits ordered subtrees.
//! - **HEAD**: streaming dispatch with `head_only = true`. The daemon
//!   resolves manifest + root span and returns metadata without joining
//!   the chunk tree.
//! - **GET, single Range**: streaming dispatch with a clamped
//!   `StreamRange`. The daemon's range-aware joiner walks only the
//!   chunks that overlap the requested byte interval; the gateway emits
//!   `206 Partial Content` with `Content-Range` and the slice length.
//!
//! Multi-range requests are still rejected with `416` — the joiner
//! produces one ordered byte stream, multipart `byteranges` would
//! demand framing logic the plan explicitly defers (PLAN.md E.20).
//! Body payloads do not materialise in memory: `Body::from_stream`
//! pumps `BytesChunk` acks into hyper as they arrive.
//!
//! `GATEWAY_MAX_FILE_BYTES` keeps the joiner's per-request size cap
//! generous enough for real bzz sites with media payloads (videos,
//! archives, large images); the CLI continues to use
//! `ant_retrieval::DEFAULT_MAX_FILE_BYTES` (32 MiB) where `antctl get`
//! is the only caller.

use std::time::{Duration, Instant};

use ant_control::{
    ActiveRequestGuard, ControlAck, ControlCommand, GatewayRequestKind, StreamRange,
};
use ant_retrieval::{
    build_collection_manifest, build_single_file_manifest, replica_chunks,
    split_bytes_with_redundancy, IndexAnchor, ManifestFile, SplitChunk,
};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::stream;
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::error::{
    json_error, params_error, parse_hex_param, status_text_error, ParamKind, Reason,
    JSON_CONTENT_TYPE,
};
use crate::handle::GatewayHandle;

/// Bee-shaped per-request override for the per-chunk retrieval timeout.
/// We accept the header and use it to cap the *whole* request in this
/// first cut — finer-grained per-chunk plumbing through the control
/// channel is deferred (PLAN.md D.2.3 explicitly allows this scope).
const SWARM_CHUNK_RETRIEVAL_TIMEOUT: HeaderName =
    HeaderName::from_static("swarm-chunk-retrieval-timeout");
/// Bee download headers selecting the redundancy prefetch strategy
/// (`NONE`/`DATA`/`PROX`/`RACE`) and whether weaker strategies may be
/// tried as fallback. **Accepted and ignored** (never an error): ant's
/// fetch model has one fixed behavior that is the practical equivalent
/// of bee's `DATA` strategy with recovery fallback — data chunks are
/// fetched directly and a miss triggers a Reed-Solomon recovery sweep
/// over the node's remaining shards + parities (`ant_retrieval::rs`).
/// There is no PROX/RACE machinery to select, so the headers carry no
/// information for us; we still parse them so bee clients that always
/// send them (bee-js does) get identical responses.
const SWARM_REDUNDANCY_STRATEGY: HeaderName = HeaderName::from_static("swarm-redundancy-strategy");
/// See [`SWARM_REDUNDANCY_STRATEGY`].
const SWARM_REDUNDANCY_FALLBACK_MODE: HeaderName =
    HeaderName::from_static("swarm-redundancy-fallback-mode");
/// Bee **upload** header (`POST /bytes`, `POST /bzz`, `POST /feeds`):
/// erasure-code the chunk tree at redundancy level 0–4. Level > 0 adds
/// Reed-Solomon parity chunks to every intermediate node
/// (`ant_retrieval::rs_encode`, bee's `pkg/file/redundancy` pipeline)
/// and uploads 2^level dispersed replica SOCs of every pipeline root
/// (the data root and each manifest node), so the content stays
/// retrievable when chunks go missing. Absent → `MEDIUM` (1), exactly
/// like bee (`redundancy.DefaultUploadLevel` in every bee upload
/// handler) — a multi-chunk upload must hash to the same reference on
/// ant and bee even when the client sends no header. Send `0`
/// explicitly to opt out.
const SWARM_REDUNDANCY_LEVEL: HeaderName = HeaderName::from_static("swarm-redundancy-level");

/// Bee's `redundancy.DefaultUploadLevel` (MEDIUM): applied when the
/// upload header is absent or empty.
const DEFAULT_UPLOAD_REDUNDANCY_LEVEL: u8 = 1;
/// Bee request header: when `true`, treat the body as a tar archive and
/// build a multi-file manifest. Default `false` (single-file upload).
const SWARM_COLLECTION: HeaderName = HeaderName::from_static("swarm-collection");
/// Bee request header on `POST /bytes` and `POST /bzz` (file and
/// collection): when true, the upload is content-encrypted — every
/// chunk under a fresh random key, 64-byte references, and a 128-hex
/// `reference` in the response (bee `SwarmEncryptHeader`).
const SWARM_ENCRYPT: HeaderName = HeaderName::from_static("swarm-encrypt");

/// Bee's `Swarm-Pin` upload header: also pin the uploaded content
/// locally alongside the upload.
const SWARM_PIN: HeaderName = HeaderName::from_static("swarm-pin");
/// Bee request header: name of the manifest entry that should resolve
/// when the user requests `bzz://<root>/` (no path). Mirrored on the
/// root manifest's `/` fork as `website-index-document` metadata.
const SWARM_INDEX_DOCUMENT: HeaderName = HeaderName::from_static("swarm-index-document");
/// Bee request header on `POST /soc/{owner}/{id}`: 65-byte secp256k1
/// signature over `keccak256(id || inner_cac_addr)` (EIP-191 wrapped),
/// hex-encoded with or without an `0x` prefix.
const SWARM_SOC_SIGNATURE: HeaderName = HeaderName::from_static("swarm-soc-signature");
/// Bee request header on `GET /feeds/{owner}/{topic}` (and `/soc`,
/// `/bzz`): when `true`, return only the root chunk's data
/// (`span(8 LE) || payload`) of the resolved content instead of joining
/// and streaming the whole chunk tree. Matches the `Swarm-Only-Root-Chunk`
/// parameter in `bee/pkg/api/feed.go::feedGetHandler`.
const SWARM_ONLY_ROOT_CHUNK: HeaderName = HeaderName::from_static("swarm-only-root-chunk");
/// Bee request header on uploads: a pre-created tag uid to attach this
/// upload to (bee-js sends it when the caller created a tag via
/// `POST /tags` first). Absent on most uploads — bee then auto-creates
/// a tag server-side.
const SWARM_TAG: HeaderName = HeaderName::from_static("swarm-tag");
/// Bee response header on uploads: the tag uid bee-js polls via
/// `GET /tags/{uid}` for progress (PLAN.md J.2.4 / J.4.5).
const SWARM_TAG_UID: HeaderName = HeaderName::from_static("swarm-tag-uid");
/// Bee request header on every upload: the 32-byte postage batch id
/// (64 hex, optional `0x`) that stamps the uploaded chunks. bee-js sends
/// it as `swarm-postage-batch-id` on `POST /bytes|/bzz|/chunks|/soc`.
/// Required: the node selects the matching registered issuer, so a
/// missing / malformed / unknown batch is a `400` (bee's shape).
const SWARM_POSTAGE_BATCH_ID: HeaderName = HeaderName::from_static("swarm-postage-batch-id");
/// Bee request header on `POST /chunks` and `POST /soc/...`: a complete
/// **pre-signed** 113-byte postage stamp (hex), layout `batch_id(32) ‖
/// index(8) ‖ timestamp(8) ‖ sig(65)` — bee's `postage.Stamp`
/// `MarshalBinary` form, typically obtained from `POST /envelope`.
/// When present, `swarm-postage-batch-id` becomes optional and the
/// chunk is pushed with *this* stamp instead of one issued locally
/// (bee's `postage.NewPresignedStamper` path).
const SWARM_POSTAGE_STAMP: HeaderName = HeaderName::from_static("swarm-postage-stamp");

/// Bee's error body when `POST /chunks` / `POST /soc` carries neither a
/// batch id nor a pre-signed stamp (`batchIdOrStampSig` in
/// `bee/pkg/api/api.go`) — verbatim, casing included.
const BATCH_ID_OR_STAMP_MSG: &str =
    "Either 'Swarm-Postage-Stamp' or 'Swarm-Postage-Batch-Id' header must be set in the request";

/// Concurrency cap on outbound `PushChunk` commands during a `POST /bzz`
/// upload. Matched to the daemon-side
/// `ant_node::uploads::MAX_PUSH_CONCURRENCY`. See the doc comment
/// there for sizing rationale: bee's per-peer stream acceptance is
/// the bottleneck, not our concurrency cap.
const MAX_PUSH_CONCURRENCY: usize = 32;

/// Hard cap on a single `POST /bzz` request body. Sized to comfortably
/// hold a small website or media payload while keeping the in-memory
/// buffering of the body and split chunks bounded — the gateway holds
/// the entire upload + every assembled chunk in memory until pushsync
/// finishes. 64 MiB covers a typical photo album or short audio file
/// without letting one bad client tip the daemon over.
pub const GATEWAY_MAX_UPLOAD_BYTES: u64 = 64 * 1024 * 1024;

/// `Cache-Control` value emitted on every content-addressed response
/// (`/chunks`, `/bytes`, `/bzz`, `/v0/manifest`). All four endpoints
/// take a 32-byte content hash in the URL — the asset *is* its hash,
/// so revisiting the same URL is provably safe to serve from the
/// browser's HTTP cache forever.
///
/// This matches what bee's gateway has historically returned (cf.
/// `ethersphere/swarm` issue #2213, where bee shipped `max-age=
/// 2147483648, immutable` for bzz responses) and is the reason bee
/// "loads super-fast on reload": after the first fetch, the browser
/// services subsequent requests entirely out of its own HTTP cache
/// without re-walking the manifest or re-joining the BMT. Without
/// this header, a music-player UI loading 13 audio tracks plus
/// artwork on every page navigation re-pays the full retrieval cost,
/// which is a noticeable user-facing regression vs bee.
///
/// Year (`31_536_000` s) instead of bee's 68-year value because some
/// HTTP intermediaries treat unrealistically large `max-age` as
/// invalid; `immutable` already encodes "don't ever revalidate" for
/// browsers that support it (Firefox, Safari, modern Chrome).
const IMMUTABLE_CACHE_CONTROL: HeaderValue =
    HeaderValue::from_static("public, max-age=31536000, immutable");

/// Apply the immutable `Cache-Control` header to a response targeted
/// at a content-addressed endpoint. Idempotent — calling twice just
/// overwrites the existing entry. Public to the module so each
/// response builder can opt in without re-inserting the header
/// inline.
fn set_immutable_cache_headers(resp: &mut Response) {
    let _ = resp
        .headers_mut()
        .insert(header::CACHE_CONTROL, IMMUTABLE_CACHE_CONTROL);
}
/// Stamp bee's quoted `ETag` header: `ETag: "<64-hex reference>"`.
/// Bee sets it via `fmt.Sprintf("%q", reference)` on `POST /bzz`
/// responses and on every `downloadHandler`-served body (`/bytes`,
/// `/bzz`, `/soc`, `/feeds`).
fn set_etag(resp: &mut Response, reference: impl AsRef<[u8]>) {
    if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", hex::encode(reference.as_ref()))) {
        resp.headers_mut().insert(header::ETAG, v);
    }
}

/// Override a response's `Cache-Control` with `no-cache`, for content
/// that is *mutable* at its URL — feed-backed bzz references resolve to
/// rolling content at a stable reference, so the immutable cache the
/// content-addressed builders apply by default would pin a stale
/// resolution. Used to downgrade those responses after the fact.
fn set_mutable_cache_headers(resp: &mut Response) {
    let _ = resp
        .headers_mut()
        .insert(header::CACHE_CONTROL, MUTABLE_CACHE_CONTROL);
}
/// Hard cap on a `/bytes` or `/bzz` request even with no header.
///
/// Sized for the realistic worst case observed in production: a 44 MiB
/// WAV (~11 K leaf chunks, 88 L1 intermediates) over the live mainnet
/// completes a *single* fetch in ~165 s at our `FETCH_FANOUT=8` joiner
/// width and ~150 ms per-chunk RTT. Concurrent `bzz://` fetches
/// (browsers happily issue 4–6 at once for media galleries) share the
/// process-wide retrieval semaphore (`RETRIEVAL_INFLIGHT_CAP=32` in
/// ant-p2p). 600 s is still a large envelope, but it keeps a wedged
/// handler from pinning a hyper task indefinitely while allowing the
/// concurrent media path to complete under normal mainnet latency.
pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_mins(10);
/// Single-chunk fetches are bounded by `ant-retrieval`'s internal
/// timeout (20 s) plus the outer retry loop. 60 s leaves room for
/// retries on a single chunk without inheriting the much larger
/// multi-chunk envelope.
pub(crate) const CHUNK_REQUEST_TIMEOUT: Duration = Duration::from_mins(1);
/// Default cap on a single-chunk *push* (`POST /chunks`, `/soc`,
/// `/feeds`, `/pss/send`'s push, each `/chunks/stream` chunk). Longer
/// than [`CHUNK_REQUEST_TIMEOUT`]: right after a buy, the node waits
/// for the storers to sync the new batch instead of failing the push
/// (`BATCH_PROPAGATION` + `PROPAGATION_REJECTION_GRACE` in `ant-p2p`,
/// up to 100 s after registration, then one 12 s patience budget).
/// Matches the upload manager's 2-minute push timeout, so a push the
/// node is still legitimately *waiting* out isn't reported as a 504
/// while it lands in the background. That covers the waits, not the
/// walks: deadlines are checked between walks, and a walk in flight
/// can take up to one 45 s pushsync timeout per candidate peer, so a
/// slow last walk can still overshoot into a 504 while the node keeps
/// pushing in the background (see `PROPAGATION_REJECTION_GRACE`).
pub(crate) const CHUNK_PUSH_TIMEOUT: Duration = Duration::from_mins(2);

/// Abort a streaming body once it goes this long without delivering any
/// further bytes — even while the daemon keeps heart-beating `Progress`.
///
/// The node emits a `Progress` ack every ~150 ms regardless of whether
/// data is actually flowing, so the body stream can't detect a stall by
/// ack silence; it watches `bytes_done` (and delivered `BytesChunk`s)
/// for forward movement instead. When the joiner gets wedged on a chunk
/// it can't fetch (a missing/shallow tail leaf), bytes plateau while it
/// burns its full multi-round retry budget — leaving a browser spinning
/// on a half-loaded response for minutes. This cap fails the request
/// fast instead. Sized comfortably above a single chunk's worst-case
/// fetch (`RETRIEVE_TIMEOUT` 30 s plus a couple of hedged retries) so a
/// slow-but-progressing transfer is never aborted.
const BODY_STALL_TIMEOUT: Duration = Duration::from_secs(90);

/// Per-request body-size ceiling for `/bytes` and `/bzz`. Bee streams,
/// so it imposes no cap at all; the joiner used to materialise the
/// whole body before responding (hence the original 1 GiB sizing),
/// but `dispatch_bzz`/`dispatch_bytes` now go through
/// `join_to_sender_range`, which streams via a bounded
/// (`FETCH_FANOUT * STREAM_PIPE_DEPTH * CHUNK_SIZE` ≈ 4 MiB) pipeline
/// regardless of declared span. The cap is therefore a "sanity bound
/// on the declared span" rather than a memory cap: it stops a
/// pathological manifest claiming a petabyte from making us walk
/// trillions of intermediate chunks before failing.
///
/// 16 GiB sized to cover the largest AI model weights we expect to
/// host (e.g. 70B-parameter LLMs at 4-bit quantisation are ~35-50 GB
/// — those still need a higher ceiling, but at that point the
/// operator should opt in explicitly), and to comfortably swallow
/// modern AV1 long-form video and disk images. The CLI keeps the
/// much tighter `ant_retrieval::DEFAULT_MAX_FILE_BYTES` (32 MiB)
/// since its audience is interactive `antctl get`, not a browser.
///
/// Pre-2026-05-09 this was 1 GiB; raised after the gemma4:e2b
/// 6.7 GiB GGUF upload surfaced "file too large: span 7162394016
/// bytes, cap 1073741824 bytes" on the very first retrieval (see
/// PLAN.md Phase 7g).
pub const GATEWAY_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// `Cache-Control` value for endpoints returning chunks at addresses
/// that may be re-stamped with a new payload. SOCs are addressed by
/// `keccak256(id || owner)`, which is stable across payload updates,
/// so the same URL can return different bytes after an owner update;
/// browsers must not freeze the first response forever the way they do
/// for content-addressed `/chunks/{addr}`.
const MUTABLE_CACHE_CONTROL: HeaderValue = HeaderValue::from_static("no-cache");

/// `GET /chunks/{addr}` and `HEAD /chunks/{addr}`. Returns the chunk's
/// **wire bytes** (`span(8 LE) || payload`) — bee's `chunkstore.Get`
/// returns `Chunk.Data()` which is the wire form, so we must too.
/// Otherwise consumers reuploading via `/chunks` POST would get a
/// malformed chunk.
pub async fn chunk(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    // ACT resolution (bee wraps `chunkGetHandler` in its
    // `actDecryptionHandler`): a 32-byte ACT-encrypted address
    // decrypts to the stored chunk's address.
    let reference: [u8; 32] =
        match crate::act::act_maybe_resolve(&handle, &headers, reference.to_vec()).await {
            Ok(r) => r.try_into().expect("32-byte input resolves to 32 bytes"),
            Err(resp) => return resp,
        };

    // Register the request with the live gateway-activity registry so
    // it shows up in the `antop` Retrieval tab. The guard's
    // `Drop` impl removes the entry when this handler returns, so
    // panics and early `return`s clean up automatically.
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, short_reference(&addr));

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    // A miss is an answer here (a feed slot not written yet): fast-miss
    // lookup (issue #146).
    let cmd = ControlCommand::GetChunkRaw {
        reference,
        fast_miss: true,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    // Single-chunk fetches go through `GetChunkRaw`, which doesn't
    // emit `Progress` acks — we just record the in-flight count
    // until the bytes come back. The body materialises in one
    // shot so there's no streaming-body lifetime to worry about
    // beyond this handler frame.
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("retrieval timed out after {}s", timeout.as_secs()),
            );
        }
    };

    let bytes = match ack {
        ControlAck::Bytes { data } => data,
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        // Peers confirmed the chunk missing, typed by the node (issue
        // #146): bee's 404 even when the last answer was `no peer found`,
        // which the substring check below would have turned into a 502.
        ControlAck::NotFound { message } => {
            debug!(target: "ant_gateway", %message, "chunk not found");
            return json_error(StatusCode::NOT_FOUND, "chunk not found");
        }
        ControlAck::Error { message } => {
            // Bee: a miss is `404 "chunk not found"`; anything else is
            // its generic `"read chunk failed"`. Detail goes to the log.
            debug!(target: "ant_gateway", %message, "chunk fetch failed");
            if message.contains("not found") {
                return json_error(StatusCode::NOT_FOUND, "chunk not found");
            }
            return json_error(StatusCode::BAD_GATEWAY, "read chunk failed");
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };
    let len = bytes.len() as u64;
    guard.update(1, 1, 0, len);
    chunk_response(method, bytes)
}

/// `GET /soc/{owner}/{id}` and `HEAD /soc/{owner}/{id}` — fetch a
/// single-owner-chunk by `(owner, id)`. The path carries the 20-byte
/// owner Ethereum address and the 32-byte id, both hex-encoded with
/// optional `0x` / `0X` prefix. The gateway derives
/// `address = keccak256(id || owner)` and fetches the SOC wire via the
/// same retrieval path `/chunks/{addr}` uses.
///
/// Response shape mirrors bee's `socGetHandler` (`bee/pkg/api/soc.go`):
/// the body is the **unwrapped content** the SOC carries (the joined
/// chunk tree rooted at the wrapped CAC), served as
/// `application/octet-stream` with the SOC's 65-byte signature in the
/// `swarm-soc-signature` header and a quoted `ETag` of the wrapped
/// chunk's address. With `Swarm-Only-Root-Chunk: true` the body is the
/// wrapped chunk's wire data (`span(8 LE) ‖ payload`) instead, with no
/// `ETag` (bee's only-root-chunk branch bypasses `downloadHandler`).
/// Cache-Control is `no-cache` because the same address is mutable: a
/// later `POST /soc/{owner}/{id}` by the same owner replaces the
/// payload at the same URL.
pub async fn download_soc(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, id_hex)): Path<(String, String)>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let (owner, id) = match parse_owner_pair(&owner_hex, "id", &id_hex) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    // Bee validates the header struct before fetching anything, so a
    // malformed `swarm-only-root-chunk` 400s even for a chunk that
    // doesn't exist.
    let only_root_chunk = match only_root_chunk_header(&headers) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let mut addr_input = [0u8; 32 + 20];
    addr_input[..32].copy_from_slice(&id);
    addr_input[32..].copy_from_slice(&owner);
    let reference = ant_crypto::keccak256(&addr_input);

    let guard = handle.activity.begin(
        GatewayRequestKind::Soc,
        short_reference(&hex::encode(reference.as_ref())),
    );

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    // A miss is an answer here (a feed slot not written yet): fast-miss
    // lookup (issue #146).
    let cmd = ControlCommand::GetChunkRaw {
        reference,
        fast_miss: true,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("retrieval timed out after {}s", timeout.as_secs()),
            );
        }
    };

    let wire = match ack {
        ControlAck::Bytes { data } => data,
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        ControlAck::NotFound { message } | ControlAck::Error { message } => {
            // Bee: `"requested chunk cannot be retrieved"` for any SOC
            // fetch failure. Detail goes to the log.
            debug!(target: "ant_gateway", %message, "soc fetch failed");
            return json_error(StatusCode::NOT_FOUND, "requested chunk cannot be retrieved");
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw (soc)");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };
    guard.update(1, 1, 0, wire.len() as u64);

    // SOC wire: id(32) ‖ sig(65) ‖ wrapped_cac(span(8 LE) ‖ payload).
    if wire.len() < 32 + 65 + 8 {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "chunk is not a single owner chunk",
        );
    }
    let mut signature = [0u8; 65];
    signature.copy_from_slice(&wire[32..32 + 65]);
    let wrapped = &wire[32 + 65..];
    let mut span_bytes = [0u8; 8];
    span_bytes.copy_from_slice(&wrapped[..8]);
    let Some(wrapped_addr) = ant_crypto::bmt_hash_with_span(&span_bytes, &wrapped[8..]) else {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "chunk is not a single owner chunk",
        );
    };

    let head_only = method == Method::HEAD;
    if only_root_chunk {
        // Bee writes the wrapped chunk's wire data verbatim, without
        // the ETag `downloadHandler` would add.
        let mut resp = soc_body_response(head_only, wrapped.to_vec());
        apply_soc_headers(&mut resp, &signature);
        return resp;
    }

    // Default: serve the wrapped chunk's *content* like bee's
    // `downloadHandler(..., wc.Address(), ..., rootCh: wc)`. A
    // single-chunk span is already in hand — serve the payload
    // directly; a larger span means the wrapped CAC is an intermediate
    // root, so join the tree it references via the byte streamer.
    let mut span_masked = span_bytes;
    span_masked[7] = 0;
    let span = u64::from_le_bytes(span_masked);
    if span as usize == wrapped.len() - 8 {
        let mut resp = soc_body_response(head_only, wrapped[8..].to_vec());
        // Bee's full-deref path serves through http.ServeContent, which
        // stamps `Accept-Ranges: bytes` (the root-chunk path doesn't).
        resp.headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        apply_soc_headers(&mut resp, &signature);
        set_etag(&mut resp, wrapped_addr);
        return resp;
    }

    let stream_timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let started =
        match dispatch_stream_bytes(&handle, wrapped_addr, stream_timeout, None, head_only, None)
            .await
        {
            Ok(s) => s,
            Err(e) => return e,
        };
    let total = started.total_bytes;
    let mut resp = streaming_response(
        head_only,
        total,
        started.into_body(head_only),
        "application/octet-stream",
        None,
    );
    apply_soc_headers(&mut resp, &signature);
    set_etag(&mut resp, wrapped_addr);
    resp
}

/// Build the `200 OK` (or empty `200 OK` for `HEAD`) response carrying
/// a SOC-derived body.
fn soc_body_response(head_only: bool, body: Vec<u8>) -> Response {
    let len = body.len();
    let body = if head_only {
        Body::empty()
    } else {
        Body::from(body)
    };
    let mut resp = Response::new(body);
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    resp
}

/// Overlay bee's SOC response headers: `swarm-soc-signature` plus the
/// CORS expose entry, and the mutable cache policy (SOCs can be
/// re-signed at the same address).
fn apply_soc_headers(resp: &mut Response, signature: &[u8; 65]) {
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&hex::encode(signature)) {
        h.insert(SWARM_SOC_SIGNATURE, v);
    }
    if let Ok(v) = HeaderValue::from_str(SWARM_SOC_SIGNATURE.as_str()) {
        h.append(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
    }
    h.insert(header::CACHE_CONTROL, MUTABLE_CACHE_CONTROL);
}

/// Extract the postage batch id from the `swarm-postage-batch-id`
/// request header. Returns bee's `400 invalid header params` (with a
/// `want required:` reason) when the header is missing, bee's hex-parse
/// reasons when it isn't hex, and bee's `"invalid batch id"` when the
/// id isn't 32 bytes — the node then selects the registered issuer for
/// this id (an unknown batch fails the push with `"batch … not
/// usable"`, which the upload handlers map to `400`).
#[allow(clippy::result_large_err)]
pub(crate) fn parse_postage_batch_header(headers: &HeaderMap) -> Result<[u8; 32], Response> {
    let raw = headers
        .get(&SWARM_POSTAGE_BATCH_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            params_error(
                ParamKind::Header,
                vec![Reason::required(SWARM_POSTAGE_BATCH_ID.as_str())],
            )
        })?;
    let mut reasons = Vec::new();
    match parse_hex_param::<32>(SWARM_POSTAGE_BATCH_ID.as_str(), raw, &mut reasons) {
        Some(batch) => Ok(batch),
        None if reasons
            .iter()
            .any(|r| r.error.starts_with("invalid length")) =>
        {
            // Valid hex of the wrong size: bee's mapStructure accepts it
            // and the stamper putter then rejects it as an invalid batch.
            Err(json_error(StatusCode::BAD_REQUEST, "invalid batch id"))
        }
        None => Err(params_error(ParamKind::Header, reasons)),
    }
}

/// Parse the `swarm-redundancy-level` upload header (0–4; absent or
/// empty → [`DEFAULT_UPLOAD_REDUNDANCY_LEVEL`], matching bee's
/// `DefaultUploadLevel = MEDIUM`). Error bodies replicate bee's `mapStructure`
/// exactly, including the field casing split: Go's `strconv` parse
/// failures report the map-tag-cased field (`Swarm-Redundancy-Level`,
/// errors `invalid syntax` / `value out of range`), while a
/// well-formed uint8 outside 0–4 fails bee's `rLevel` validator with
/// the lowercased field and `want redundancy level to be between 0
/// and 4` (`pkg/api/validation.go`).
#[allow(clippy::result_large_err)]
pub(crate) fn parse_redundancy_level_header(headers: &HeaderMap) -> Result<u8, Response> {
    let Some(raw) = headers
        .get(&SWARM_REDUNDANCY_LEVEL)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(DEFAULT_UPLOAD_REDUNDANCY_LEVEL);
    };
    match raw.parse::<u8>() {
        Ok(level) if level <= ant_retrieval::rs::MAX_LEVEL => Ok(level),
        Ok(_) => Err(params_error(
            ParamKind::Header,
            vec![Reason::new(
                SWARM_REDUNDANCY_LEVEL.as_str(),
                "want redundancy level to be between 0 and 4",
            )],
        )),
        Err(e) => {
            let msg = if *e.kind() == std::num::IntErrorKind::PosOverflow {
                "value out of range"
            } else {
                "invalid syntax"
            };
            Err(params_error(
                ParamKind::Header,
                vec![Reason::new("Swarm-Redundancy-Level", msg)],
            ))
        }
    }
}

/// A pre-signed 113-byte postage stamp from the `swarm-postage-stamp`
/// request header, if the header is present and non-empty.
///
/// Bee's shapes, in order: non-hex → `400 invalid header params` with a
/// Go-style hex reason; valid hex of the wrong length →
/// `400 "Stamp deserialization failure"` (`postage.Stamp.
/// UnmarshalBinary` rejecting a non-113-byte blob, message verbatim
/// from `chunkUploadHandler`/`socUploadHandler`).
#[allow(clippy::result_large_err)]
fn parse_postage_stamp_header(headers: &HeaderMap) -> Result<Option<[u8; 113]>, Response> {
    let Some(raw) = headers
        .get(&SWARM_POSTAGE_STAMP)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let mut reasons = Vec::new();
    match parse_hex_param::<113>(SWARM_POSTAGE_STAMP.as_str(), raw, &mut reasons) {
        Some(stamp) => Ok(Some(stamp)),
        None if reasons
            .iter()
            .any(|r| r.error.starts_with("invalid length")) =>
        {
            // Valid hex, wrong size: bee's mapStructure accepts the
            // bytes and `Stamp.UnmarshalBinary` then rejects them.
            Err(json_error(
                StatusCode::BAD_REQUEST,
                "Stamp deserialization failure",
            ))
        }
        None => Err(params_error(ParamKind::Header, reasons)),
    }
}

/// Resolve the postage source for `POST /chunks` / `POST /soc`:
/// the pre-signed stamp wins when present; otherwise the batch id
/// header; neither → bee's `batchIdOrStampSig` 400 (those two
/// endpoints don't mark the batch header `required`, unlike
/// `/bytes`/`/bzz`).
#[allow(clippy::result_large_err)]
fn parse_stamp_or_batch(headers: &HeaderMap) -> Result<(Option<[u8; 113]>, [u8; 32]), Response> {
    if let Some(stamp) = parse_postage_stamp_header(headers)? {
        let batch_id: [u8; 32] = stamp[0..32].try_into().expect("stamp slice");
        return Ok((Some(stamp), batch_id));
    }
    let has_batch = headers
        .get(&SWARM_POSTAGE_BATCH_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .is_some_and(|s| !s.is_empty());
    if !has_batch {
        return Err(json_error(StatusCode::BAD_REQUEST, BATCH_ID_OR_STAMP_MSG));
    }
    Ok((None, parse_postage_batch_header(headers)?))
}

/// Verify a pre-signed stamp plausibly covers `addr`: recover the
/// signer's public key from the stamp signature over bee's stamp digest
/// (`keccak256(addr ‖ batch ‖ index ‖ ts)`). Recovery failing means the
/// stamp cannot have been signed over this chunk by anyone. Bee runs
/// the equivalent check (plus an on-chain owner comparison a light
/// node can't make) inside `postage.NewPresignedStamper` at Put time.
fn presigned_stamp_covers(stamp: &[u8; 113], addr: &[u8; 32]) -> bool {
    let batch: &[u8; 32] = stamp[0..32].try_into().expect("stamp slice");
    let index: &[u8; 8] = stamp[32..40].try_into().expect("stamp slice");
    let ts: &[u8; 8] = stamp[40..48].try_into().expect("stamp slice");
    let sig: &[u8; 65] = stamp[48..113].try_into().expect("stamp slice");
    let digest = ant_postage::postage_sign_digest(addr, batch, index, ts);
    ant_crypto::recover_public_key(sig, digest.as_ref()).is_ok()
}

/// `POST /chunks` — accept a single content-addressed chunk's wire
/// bytes (`span(8 LE) || payload`), stamp it locally, and pushsync it
/// to the closest neighbourhood peer. Body cap matches bee:
/// `8 + 4096`. Returns bee-shaped `{"reference": "<lowercase 64-hex>"}`
/// with status 201 on success.
pub async fn upload_chunk(
    State(handle): State<GatewayHandle>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Bee: a body shorter than the 8-byte span prefix is
    // `400 "insufficient data length"` (`chunkUploadHandler`).
    if body.len() < 8 {
        return json_error(StatusCode::BAD_REQUEST, "insufficient data length");
    }
    if body.len() > 8 + 4096 {
        return json_error(
            StatusCode::BAD_REQUEST,
            "chunk data exceeds required length",
        );
    }

    let (stamp, batch_id) = match parse_stamp_or_batch(&headers) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    let pin = match parse_pin_header(&headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let act = match crate::act::parse_act_upload(&headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    // Pre-signed stamp: check the signature actually covers this chunk
    // before pushing. Bee surfaces the presigned stamper's
    // `ErrInvalidBatchSignature` as `400 "stamp signature is invalid"`.
    if let Some(stamp) = &stamp {
        let mut span = [0u8; 8];
        span.copy_from_slice(&body[..8]);
        let addr = ant_crypto::bmt_hash_with_span(&span, &body[8..]);
        if !addr.is_some_and(|addr| presigned_stamp_covers(stamp, &addr)) {
            return json_error(StatusCode::BAD_REQUEST, "stamp signature is invalid");
        }
    }

    let timeout = request_timeout(&headers, CHUNK_PUSH_TIMEOUT);

    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, "upload".to_string());

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::PushChunk {
        wire: body.to_vec(),
        batch_id,
        stamp: stamp.map(|s| s.to_vec()),
        // Bee-parity: accept a shallow receipt after the deeper-storer
        // hunt, exactly like bee's own pusher (strict receipts are the
        // upload-job manager's policy, not the HTTP API's).
        require_deep: false,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("upload timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::ChunkUploaded { reference } => {
            guard.update(1, 1, 0, body.len() as u64);
            let stripped = reference
                .strip_prefix("0x")
                .or_else(|| reference.strip_prefix("0X"))
                .unwrap_or(reference.as_str())
                .to_string();
            if pin {
                let mut address = [0u8; 32];
                if hex::decode_to_slice(&stripped, &mut address).is_ok() {
                    if let Err(resp) =
                        pin_uploaded_reference(&handle, address.to_vec(), timeout).await
                    {
                        return resp;
                    }
                }
            }
            // ACT on `POST /chunks` (bee `chunkUploadHandler`): wrap
            // the chunk address like any other reference.
            let mut response_ref = stripped.clone();
            let mut act_history: Option<String> = None;
            if let Some(act_params) = &act {
                let raw = hex::decode(&stripped).unwrap_or_default();
                match crate::act::act_encrypt_upload(&handle, act_params, &raw, batch_id, timeout)
                    .await
                {
                    Ok(outcome) => {
                        response_ref = hex::encode(outcome.reference);
                        act_history = Some(outcome.history_hex);
                    }
                    Err(resp) => return resp,
                }
            }
            let mut resp = json_response(
                StatusCode::CREATED,
                &serde_json::json!({ "reference": response_ref }),
            );
            set_immutable_cache_headers(&mut resp);
            // Bee only attaches (and echoes) a tag on `POST /chunks`
            // when the request carried one in `Swarm-Tag`.
            if let Some(uid) = requested_tag(&headers) {
                let mut address = [0u8; 32];
                if hex::decode_to_slice(&stripped, &mut address).is_ok() {
                    handle.tags.complete(uid, 1, address);
                }
                echo_tag_headers(uid, &mut resp);
            }
            if let Some(history_hex) = &act_history {
                crate::act::set_act_history_header(&mut resp, history_hex);
            }
            resp
        }
        ControlAck::Error { message } => {
            // Bee mirrors not-stamped / out-of-batch failures with 4xx;
            // map "uploads not configured" to 503 because the operator
            // can fix it without a code change.
            let status = if message.starts_with("uploads not configured") {
                StatusCode::SERVICE_UNAVAILABLE
            } else if message.starts_with("chunk wire size") || message.contains("not usable") {
                StatusCode::BAD_REQUEST
            } else if message.contains("rejected by") && message.contains("not found on-chain") {
                // Peer-attested phantom batch — deterministic, not
                // retryable (see the PushSoc arm below).
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::BAD_GATEWAY
            };
            json_error(status, message)
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PushChunk");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// Query params for `POST /pss/send/{topic}/{targets}`.
#[derive(serde::Deserialize)]
pub struct PssSendQuery {
    /// Optional recipient PSS public key (hex, SEC1 compressed or
    /// uncompressed). Absent ⇒ bee's topic-derived broadcast key.
    recipient: Option<String>,
}

/// `POST /pss/send/{topic}/{targets}` — wrap `body` as a PSS trojan
/// chunk addressed to one of `targets` (comma-separated overlay-address
/// hex prefixes, ≤3 bytes each), stamp it, and pushsync it. `topic` is a
/// string hashed with keccak256 (bee's `NewTopic`). With `?recipient=`
/// the payload is ECIES-encrypted to that key; without it, to the
/// topic-derived key so any node registered on the topic can read it.
///
/// Bee exposes the same route (`bee/pkg/api/pss.go`); ant can send from a
/// light node (only the ephemeral wrap + a usable postage batch are
/// needed). Returns 201 on success. Mining is CPU-bound and runs on a
/// blocking thread.
pub async fn upload_pss(
    State(handle): State<GatewayHandle>,
    Path((topic_str, targets_str)): Path<(String, String)>,
    Query(query): Query<PssSendQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    use ant_crypto::pss::{
        topic_from_string, wrap_cancellable, Recipient, MAX_PAYLOAD_SIZE, MAX_TARGET_LEN,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    if body.len() > MAX_PAYLOAD_SIZE {
        return json_error(
            StatusCode::BAD_REQUEST,
            format!("pss message exceeds {MAX_PAYLOAD_SIZE} bytes"),
        );
    }

    // Parse targets: comma-separated hex prefixes, all the same length,
    // 1..=3 bytes each (bee's API cap). 1-byte targets are accepted for
    // bee compatibility but deliver unreliably on mainnet: an 8-bit prefix
    // is shallower than storage depth `d`, so the trojan lands in the
    // target's neighbourhood only with probability 2^-(d-8) (~1/4 at
    // d = 10), and ant's lurker receives even fewer: only a trojan that
    // agrees with the target past bit `b_p` of a covering peer sits in
    // bin `b_p`, the bin the lurker pulls anyway (~2^-(b_p-7), ~1/64 at
    // b_p = 13). One with d <= PO(c, target) <= b_p is stored in the
    // neighbourhood but filed under a bin the lurker does not pull. The
    // lurker does not pull the extra `8 + Geom` bins a full 1-byte
    // cover would need — see `PSS_MINED_PREFIX_BITS` in ant-p2p's
    // lurker.
    let mut targets: Vec<Vec<u8>> = Vec::new();
    for t in targets_str.split(',') {
        let t = t.trim();
        match hex::decode(t) {
            Ok(bytes) if !bytes.is_empty() && bytes.len() <= MAX_TARGET_LEN => targets.push(bytes),
            _ => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "each target must be 1..=3 bytes of hex",
                );
            }
        }
    }
    if targets.is_empty() || targets.iter().any(|t| t.len() != targets[0].len()) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "targets must be non-empty and all the same length",
        );
    }
    // Duplicate targets change nothing about which addresses can match
    // but make every mining iteration slower; drop them, and cap the
    // list so a pathological request can't inflate the per-hash
    // comparison cost.
    targets.sort();
    targets.dedup();
    const MAX_PSS_TARGETS: usize = 64;
    if targets.len() > MAX_PSS_TARGETS {
        return json_error(
            StatusCode::BAD_REQUEST,
            format!("at most {MAX_PSS_TARGETS} distinct targets"),
        );
    }

    // Recipient: explicit key, or the topic-derived broadcast key.
    let recipient = match &query.recipient {
        Some(hex_pk) => match hex::decode(hex_pk.trim())
            .ok()
            .and_then(|b| Recipient::from_sec1(&b).ok())
        {
            Some(r) => r,
            None => {
                return json_error(StatusCode::BAD_REQUEST, "invalid recipient public key");
            }
        },
        None => Recipient::TopicDerived,
    };

    let (stamp, batch_id) = match parse_stamp_or_batch(&headers) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };
    // A pre-signed stamp binds a chunk *address*, but the trojan address
    // is mined server-side and unknowable to the client in advance — the
    // stamp could never cover it. Reject explicitly rather than parse it
    // and silently stamp with the local issuer instead.
    if stamp.is_some() {
        return json_error(
            StatusCode::BAD_REQUEST,
            "pre-signed swarm-postage-stamp is not supported on /pss/send (the trojan \
             address is mined server-side); pass swarm-postage-batch-id",
        );
    }

    // Reject an unusable batch *before* burning CPU on mining: this
    // route is unauthenticated, and a 3-byte target costs ~2^24 BMT
    // hashes across all cores. The push stamps locally against a
    // registered issuer, so "registered + usable" is exactly the bar the
    // push itself would apply — just checked while it's still cheap.
    if let Err(resp) = require_usable_batch(&handle, &batch_id).await {
        return resp;
    }

    let topic = topic_from_string(&topic_str);
    let msg = body.to_vec();
    // ONE budget for the whole request: mining and the push-ack wait
    // draw down the same deadline, so the worst case is `timeout`, not
    // 2×`timeout` (mining alone can eat the full budget on deep targets).
    // Without a timeout header, mining keeps the single-chunk cap and
    // only the push may run on to the push cap (a just-bought batch's
    // propagation wait, see `CHUNK_PUSH_TIMEOUT`): this route is
    // unauthenticated, so the CPU budget doesn't grow with it.
    let start = tokio::time::Instant::now();
    let mine_timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);
    let mine_deadline = start + mine_timeout;
    let timeout = request_timeout(&headers, CHUNK_PUSH_TIMEOUT);
    let deadline = start + timeout;

    // At most a couple of concurrent mining jobs per gateway; everyone
    // else waits briefly, then sheds with 503 instead of stacking
    // unbounded CPU work.
    static PSS_MINING: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);
    let Ok(Ok(permit)) = tokio::time::timeout(Duration::from_secs(10), PSS_MINING.acquire()).await
    else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "pss mining is busy; retry later",
        );
    };

    // Mining is CPU-heavy; keep it off the async runtime — and make it
    // stop when this request goes away (a client disconnect drops the
    // handler future, and the guard flips the flag the miner threads
    // poll) or when it exceeds the request timeout.
    struct CancelOnDrop(Arc<AtomicBool>);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let cancel = Arc::new(AtomicBool::new(false));
    let _cancel_guard = CancelOnDrop(Arc::clone(&cancel));
    let mine_cancel = Arc::clone(&cancel);
    let mining = tokio::task::spawn_blocking(move || {
        wrap_cancellable(&topic, &msg, &recipient, &targets, &mine_cancel)
    });
    let Ok(wrapped) = tokio::time::timeout_at(mine_deadline, mining).await else {
        cancel.store(true, Ordering::Relaxed);
        return json_error(
            StatusCode::GATEWAY_TIMEOUT,
            format!("pss mining timed out after {}s", mine_timeout.as_secs()),
        );
    };
    drop(permit); // mining finished — release before the network round-trip
    let (addr, wire) = match wrapped {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return json_error(StatusCode::BAD_REQUEST, e.to_string()),
        Err(_) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, "pss wrap task failed"),
    };
    tracing::debug!(target: "ant_gateway", pss_chunk = %hex::encode(addr), "pss send: mined trojan chunk");

    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, "pss-send".to_string());

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::PushChunk {
        wire,
        batch_id,
        stamp: None,
        require_deep: false,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout_at(deadline, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("pss send timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::ChunkUploaded { .. } => {
            guard.update(1, 1, 0, body.len() as u64);
            StatusCode::CREATED.into_response()
        }
        ControlAck::Error { message } => {
            let status = if message.starts_with("uploads not configured") {
                StatusCode::SERVICE_UNAVAILABLE
            } else if message.contains("not usable") {
                StatusCode::BAD_REQUEST
            } else if message.contains("rejected by") && message.contains("not found on-chain") {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::BAD_GATEWAY
            };
            json_error(status, message)
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from pss PushChunk");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// Pre-flight for `/pss/send`: the batch must be a **registered, usable
/// issuer** on this node — the same bar (and the same "not usable"
/// wording, so status mapping stays uniform) that `PushChunk` applies,
/// just checked before mining instead of after. A just-bought batch
/// still propagating to the storers (`propagating`) passes, as it does
/// for `PushChunk`, which waits the propagation out.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
async fn require_usable_batch(handle: &GatewayHandle, batch_id: &[u8; 32]) -> Result<(), Response> {
    let (ack_tx, ack_rx) = oneshot::channel();
    if handle
        .commands
        .send(ControlCommand::PostageList { ack: ack_tx })
        .await
        .is_err()
    {
        return Err(node_unavailable());
    }
    let views = match tokio::time::timeout(Duration::from_secs(5), ack_rx).await {
        Ok(Ok(ControlAck::PostageList(views))) => views,
        Ok(Ok(other)) => {
            warn!(target: "ant_gateway", ?other, "unexpected ack for postage list");
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected node ack",
            ));
        }
        _ => return Err(node_unavailable()),
    };
    let id_hex = hex::encode(batch_id);
    let usable = views.iter().any(|v| {
        v.enabled
            && (v.usable || v.propagating)
            && v.batch_id
                .trim_start_matches("0x")
                .eq_ignore_ascii_case(&id_hex)
    });
    if usable {
        Ok(())
    } else {
        Err(json_error(
            StatusCode::BAD_REQUEST,
            format!("batch 0x{id_hex} not usable"),
        ))
    }
}

/// `POST /soc/{owner}/{id}` — accept a single-owner-chunk's inner CAC
/// payload, validate the signature, then stamp + pushsync the SOC. The
/// path carries the 32-byte `id` and the 20-byte `owner` Ethereum
/// address; the signature is supplied via the `swarm-soc-signature`
/// header (bee convention). The body is the **inner CAC wire**:
/// `span(8 LE) || payload(≤4096)`. The full SOC wire that hits the
/// network is `id || sig || body`, addressed at
/// `keccak256(id || owner)`.
///
/// Bee's HTTP API exposes the same shape on `POST /soc/{owner}/{id}`,
/// see `bee/pkg/api/soc.go`. Returns bee-shaped
/// `{"reference": "<lowercase 64-hex>"}` with status 201 on success.
pub async fn upload_soc(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, id_hex)): Path<(String, String)>,
    Query(query): Query<UploadSocQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (owner, id) = match parse_owner_pair(&owner_hex, "id", &id_hex) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    // Bee only reads the SOC signature from the `?sig=…` query
    // parameter (required); ant additionally accepts the legacy
    // `swarm-soc-signature` header older bee-js generations sent.
    // Validation failures are shaped like bee's `mapStructure`
    // responses for the parameter group the value came from.
    let (sig_str, sig_kind, sig_field): (&str, ParamKind, &str) =
        if let Some(h) = headers.get(&SWARM_SOC_SIGNATURE) {
            match h.to_str() {
                Ok(s) => (s, ParamKind::Header, "swarm-soc-signature"),
                Err(_) => {
                    return params_error(
                        ParamKind::Header,
                        vec![Reason::new(
                            SWARM_SOC_SIGNATURE.as_str(),
                            "value is not ascii",
                        )],
                    );
                }
            }
        } else if let Some(s) = query.sig.as_deref() {
            (s, ParamKind::Query, "sig")
        } else {
            // Bee: `queries.Sig []byte validate:"required"` →
            // `400 invalid query params` with a `want required:` reason.
            return params_error(ParamKind::Query, vec![Reason::required("sig")]);
        };
    let sig = {
        let mut reasons = Vec::new();
        match parse_hex_param::<65>(sig_field, sig_str, &mut reasons) {
            Some(s) => s,
            None => return params_error(sig_kind, reasons),
        }
    };

    // Bee: `"short chunk data"` below the span size, 413 above the
    // maximum CAC wire (`socUploadHandler`).
    if body.len() < 8 {
        return json_error(StatusCode::BAD_REQUEST, "short chunk data");
    }
    if body.len() > 8 + 4096 {
        return json_error(StatusCode::PAYLOAD_TOO_LARGE, "payload too large");
    }

    // SOC wire: id(32) || sig(65) || inner_cac(span(8) || payload).
    let mut wire = Vec::with_capacity(32 + 65 + body.len());
    wire.extend_from_slice(&id);
    wire.extend_from_slice(&sig);
    wire.extend_from_slice(&body);

    // Address binds id and owner: address = keccak256(id || owner).
    let mut addr_input = [0u8; 32 + 20];
    addr_input[..32].copy_from_slice(&id);
    addr_input[32..].copy_from_slice(&owner);
    let address = ant_crypto::keccak256(&addr_input);

    if !ant_crypto::soc_valid(&address, &wire) {
        // Bee: `soc.Valid` failure is `401 "invalid chunk"`
        // (`socUploadHandler`). The recovery detail stays in the log.
        debug!(
            target: "ant_gateway",
            owner = %hex::encode(owner),
            "soc signature does not recover the supplied owner",
        );
        return json_error(StatusCode::UNAUTHORIZED, "invalid chunk");
    }

    let (stamp, batch_id) = match parse_stamp_or_batch(&headers) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    let pin = match parse_pin_header(&headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let act = match crate::act::parse_act_upload(&headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    // Pre-signed stamp: check the signature covers the SOC's
    // owner-bound address. Bee's soc handler surfaces the presigned
    // stamper's rejection at Put time as `400 "chunk write error"`.
    if let Some(stamp) = &stamp {
        if !presigned_stamp_covers(stamp, &address) {
            return json_error(StatusCode::BAD_REQUEST, "chunk write error");
        }
    }

    let timeout = request_timeout(&headers, CHUNK_PUSH_TIMEOUT);

    let guard = handle.activity.begin(
        GatewayRequestKind::Soc,
        short_reference(&hex::encode(address)),
    );

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::PushSoc {
        address,
        wire,
        batch_id,
        stamp: stamp.map(|s| s.to_vec()),
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("upload timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::ChunkUploaded { reference } => {
            guard.update(1, 1, 0, body.len() as u64);
            let stripped = reference
                .strip_prefix("0x")
                .or_else(|| reference.strip_prefix("0X"))
                .unwrap_or(reference.as_str())
                .to_string();
            if pin {
                if let Err(resp) = pin_uploaded_reference(&handle, address.to_vec(), timeout).await
                {
                    return resp;
                }
            }
            // ACT on `POST /soc` (bee `socUploadHandler`): wrap the
            // SOC address like any other reference.
            let mut response_ref = stripped.clone();
            let mut act_history: Option<String> = None;
            if let Some(act_params) = &act {
                let raw = hex::decode(&stripped).unwrap_or_default();
                match crate::act::act_encrypt_upload(&handle, act_params, &raw, batch_id, timeout)
                    .await
                {
                    Ok(outcome) => {
                        response_ref = hex::encode(outcome.reference);
                        act_history = Some(outcome.history_hex);
                    }
                    Err(resp) => return resp,
                }
            }
            // SOC writes are mutable at the address level (the owner can
            // sign a new payload at the same id), so don't apply the
            // immutable cache headers `upload_chunk` uses.
            let mut resp = json_response(
                StatusCode::CREATED,
                &serde_json::json!({ "reference": response_ref }),
            );
            // Like `/chunks`, bee only echoes `Swarm-Tag` on `/soc`
            // uploads that carried one in the request.
            if let Some(uid) = requested_tag(&headers) {
                handle.tags.complete(uid, 1, address);
                echo_tag_headers(uid, &mut resp);
            }
            if let Some(history_hex) = &act_history {
                crate::act::set_act_history_header(&mut resp, history_hex);
            }
            resp
        }
        ControlAck::NotReady { message } => json_error(StatusCode::SERVICE_UNAVAILABLE, message),
        ControlAck::Error { message } => json_error(upload_error_status(&message), message),
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PushSoc");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `POST /bzz` — single-file or collection upload. Bee-shaped: the
/// gateway splits the body into a chunk tree (4 KiB leaves +
/// intermediate join chunks), wraps it in a Mantaray manifest with
/// the right per-file metadata, then pushes every chunk to the
/// network's pushsync neighbourhood. Returns
/// `{"reference":"<root manifest hash>"}` with status 201.
///
/// Body interpretation depends on the `swarm-collection` request
/// header (Bee compat) — defaults to `false`:
///
/// - `swarm-collection: false`: the body is one file. The filename
///   comes from the `?name=` query parameter (bee uses the same
///   knob); if absent, the manifest entry is `index.html`. The MIME
///   type comes from the `Content-Type` request header (default
///   `application/octet-stream`).
/// - `swarm-collection: true`: the body is an uncompressed tar
///   archive. Each regular file becomes a manifest entry under its
///   archive path. `swarm-index-document` selects the file that
///   `bzz://<root>/` resolves to (default: `index.html` if present).
///
/// Cap: [`GATEWAY_MAX_UPLOAD_BYTES`] on the overall body. The
/// manifest itself today has to fit in one CAC chunk (≤ ~30 entries
/// with average filename length); collections that exceed that
/// return 413 with a message pointing at the future multi-chunk
/// manifest work.
pub async fn upload_bzz(
    State(handle): State<GatewayHandle>,
    Query(query): Query<UploadBzzQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() as u64 > GATEWAY_MAX_UPLOAD_BYTES {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "request body {} bytes exceeds upload cap {} bytes",
                body.len(),
                GATEWAY_MAX_UPLOAD_BYTES
            ),
        );
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let level = match parse_redundancy_level_header(&headers) {
        Ok(l) => l,
        Err(resp) => return resp,
    };

    let collection = headers
        .get(SWARM_COLLECTION)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"));
    let encrypt = match parse_encrypt_header(&headers) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let pin = match parse_pin_header(&headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let act = match crate::act::parse_act_upload(&headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);

    let activity_label = match (collection, query.name()) {
        (true, _) => "upload-bzz-collection".to_string(),
        (false, Some(name)) => format!("upload-bzz {name}"),
        (false, None) => "upload-bzz".to_string(),
    };
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Bzz, activity_label);

    let assembled = match assemble_bzz(&query, &headers, &body, collection, level, encrypt) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let total_chunks = assembled.chunks.len() + assembled.replicas.len();
    debug!(
        target: "ant_gateway",
        collection,
        body_len = body.len(),
        chunks = assembled.chunks.len(),
        replicas = assembled.replicas.len(),
        redundancy_level = level,
        manifest_root = %hex::encode(&assembled.root),
        "bzz upload assembled"
    );
    guard.update(0, total_chunks as u64, total_chunks as u32, 0);

    if let Err(err_resp) = push_chunks(&handle, &assembled.chunks, batch_id, timeout, &guard).await
    {
        return err_resp;
    }
    if let Err(err_resp) = push_replica_socs(&handle, &assembled.replicas, batch_id, timeout).await
    {
        return err_resp;
    }
    if pin {
        if let Err(resp) = pin_uploaded_reference(&handle, assembled.root.clone(), timeout).await {
            return resp;
        }
    }

    // ACT wraps the manifest root reference; the manifest and content
    // pipelines above are untouched (bee's `actEncryptionHandler` runs
    // on the finished manifest reference).
    let mut response_ref = assembled.root.clone();
    let mut act_history: Option<String> = None;
    if let Some(act_params) = &act {
        match crate::act::act_encrypt_upload(&handle, act_params, &response_ref, batch_id, timeout)
            .await
        {
            Ok(outcome) => {
                response_ref = outcome.reference;
                act_history = Some(outcome.history_hex);
            }
            Err(resp) => return resp,
        }
    }

    let reference_hex = hex::encode(&response_ref);
    let mut resp = json_response(
        StatusCode::CREATED,
        &serde_json::json!({ "reference": reference_hex }),
    );
    set_immutable_cache_headers(&mut resp);
    // Bee stamps the returned reference on the *single-file* upload
    // response as a quoted `ETag` (`bzzUploadHandler` uses the
    // ACT-encrypted reference when ACT is on); its collection path
    // (`dirUploadHandler`) sets none.
    if !collection {
        set_etag(&mut resp, &response_ref);
    }
    // Tags key on the root chunk's 32-byte address (the leading half of
    // an encrypted reference).
    let root_address: [u8; 32] = assembled.root[..32]
        .try_into()
        .expect("assembled root is at least 32 bytes");
    finalize_upload_tag(
        &handle,
        &headers,
        total_chunks as u64,
        root_address,
        &mut resp,
    );
    if let Some(history_hex) = &act_history {
        crate::act::set_act_history_header(&mut resp, history_hex);
    }
    resp
}

/// The uid a client supplied in the `Swarm-Tag` *request* header, if
/// any (bee-js sends it when the caller pre-created a tag via
/// `POST /tags`).
fn requested_tag(headers: &HeaderMap) -> Option<u32> {
    headers
        .get(&SWARM_TAG)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u32>().ok())
        .filter(|&uid| uid != 0)
}

/// Stamp the upload-tag response headers: bee's `Swarm-Tag` (what
/// modern bee-js reads for `UploadResult.tagUid`) plus ant's legacy
/// `Swarm-Tag-Uid` echo, which existing consumers (Freedom's
/// `getUploadStatus`) already key off. Bee only sends `Swarm-Tag`; the
/// extra header is additive and harmless.
fn echo_tag_headers(uid: u32, resp: &mut Response) {
    if let Ok(v) = HeaderValue::from_str(&uid.to_string()) {
        resp.headers_mut().insert(SWARM_TAG, v.clone());
        resp.headers_mut().insert(SWARM_TAG_UID, v);
    }
}

/// Resolve (or create) the upload tag for a just-finished synchronous
/// `POST /bytes` / `POST /bzz` and stamp the tag response headers. Bee
/// creates a session implicitly for every deferred upload (the default
/// mode) and echoes its id in `Swarm-Tag`; if the client pre-created a
/// tag and sent it in `Swarm-Tag`, that one is marked complete and
/// echoed instead. The whole upload is already pushsynced by the time
/// we get here, so the tag is reported fully synced immediately.
fn finalize_upload_tag(
    handle: &GatewayHandle,
    headers: &HeaderMap,
    total_chunks: u64,
    root: [u8; 32],
    resp: &mut Response,
) {
    let uid = match requested_tag(headers) {
        Some(existing) => {
            handle.tags.complete(existing, total_chunks, root);
            existing
        }
        None => handle.tags.create_completed(total_chunks, root),
    };
    echo_tag_headers(uid, resp);
}

/// `POST /bytes` — raw byte upload. Splits the body into a content-
/// addressed chunk tree (no manifest) and pushsyncs every chunk,
/// returning `{"reference":"<data-root hash>"}` with status 201. This
/// is what bee-js uses for feed payloads larger than one chunk and for
/// direct `bee.uploadData` calls (PLAN.md J.2.5 / C1). The returned
/// reference is a `/bytes/<ref>` reference — it has no mantaray
/// manifest, unlike `POST /bzz`.
pub async fn upload_bytes(
    State(handle): State<GatewayHandle>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if body.len() as u64 > GATEWAY_MAX_UPLOAD_BYTES {
        return json_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "request body {} bytes exceeds upload cap {} bytes",
                body.len(),
                GATEWAY_MAX_UPLOAD_BYTES
            ),
        );
    }

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let level = match parse_redundancy_level_header(&headers) {
        Ok(l) => l,
        Err(resp) => return resp,
    };
    let encrypt = match parse_encrypt_header(&headers) {
        Ok(e) => e,
        Err(resp) => return resp,
    };
    let pin = match parse_pin_header(&headers) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let act = match crate::act::parse_act_upload(&headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Bytes, "upload-bytes".to_string());

    if encrypt {
        // `swarm-encrypt: true` — bee's encryption pipeline: fresh
        // random key per chunk, 64-byte references, halved branching,
        // parities (at level > 0) computed over the ciphertext, and a
        // 128-hex reference in the response. Dispersed replicas key on
        // the root's 32-byte *address*; the decryption key travels only
        // in the returned reference.
        let split = ant_retrieval::split_bytes_encrypted(&body, level);
        let root_address = split.root_address();
        let replicas = replica_chunks(&root_address, &split.root_wire, level);
        let total_chunks = split.chunks.len() + replicas.len();
        debug!(
            target: "ant_gateway",
            body_len = body.len(),
            chunks = split.chunks.len(),
            replicas = replicas.len(),
            redundancy_level = level,
            data_root = %hex::encode(root_address),
            "encrypted bytes upload assembled"
        );
        guard.update(0, total_chunks as u64, total_chunks as u32, 0);

        if let Err(err_resp) = push_chunks(&handle, &split.chunks, batch_id, timeout, &guard).await
        {
            return err_resp;
        }
        if let Err(err_resp) = push_replica_socs(&handle, &replicas, batch_id, timeout).await {
            return err_resp;
        }
        if pin {
            // Bee records the pin under the full (64-byte) encrypted
            // reference the response returns.
            if let Err(resp) =
                pin_uploaded_reference(&handle, split.root_ref.to_vec(), timeout).await
            {
                return resp;
            }
        }

        // ACT composes with encryption: the content pipeline above is
        // unchanged, ACT wraps the resulting 64-byte reference
        // (bee's `actEncryptionHandler` runs after the split).
        let mut response_ref = split.root_ref.to_vec();
        let mut act_history: Option<String> = None;
        if let Some(act_params) = &act {
            match crate::act::act_encrypt_upload(
                &handle,
                act_params,
                &response_ref,
                batch_id,
                timeout,
            )
            .await
            {
                Ok(outcome) => {
                    response_ref = outcome.reference;
                    act_history = Some(outcome.history_hex);
                }
                Err(resp) => return resp,
            }
        }

        let mut resp = json_response(
            StatusCode::CREATED,
            &serde_json::json!({ "reference": hex::encode(&response_ref) }),
        );
        set_immutable_cache_headers(&mut resp);
        finalize_upload_tag(
            &handle,
            &headers,
            total_chunks as u64,
            root_address,
            &mut resp,
        );
        if let Some(history_hex) = &act_history {
            crate::act::set_act_history_header(&mut resp, history_hex);
        }
        return resp;
    }

    let split = split_bytes_with_redundancy(&body, level);
    // Redundant uploads also carry 2^level dispersed replica SOCs of
    // the data root, like bee's `replicas.NewPutter` inside the
    // pipeline sum.
    let replicas = replica_chunks(&split.root, &split.root_wire, level);
    let total_chunks = split.chunks.len() + replicas.len();
    debug!(
        target: "ant_gateway",
        body_len = body.len(),
        chunks = split.chunks.len(),
        replicas = replicas.len(),
        redundancy_level = level,
        data_root = %hex::encode(split.root),
        "bytes upload assembled"
    );
    guard.update(0, total_chunks as u64, total_chunks as u32, 0);

    if let Err(err_resp) = push_chunks(&handle, &split.chunks, batch_id, timeout, &guard).await {
        return err_resp;
    }
    if let Err(err_resp) = push_replica_socs(&handle, &replicas, batch_id, timeout).await {
        return err_resp;
    }
    if pin {
        if let Err(resp) = pin_uploaded_reference(&handle, split.root.to_vec(), timeout).await {
            return resp;
        }
    }

    let mut response_ref = split.root.to_vec();
    let mut act_history: Option<String> = None;
    if let Some(act_params) = &act {
        match crate::act::act_encrypt_upload(&handle, act_params, &response_ref, batch_id, timeout)
            .await
        {
            Ok(outcome) => {
                response_ref = outcome.reference;
                act_history = Some(outcome.history_hex);
            }
            Err(resp) => return resp,
        }
    }

    let reference_hex = hex::encode(&response_ref);
    let mut resp = json_response(
        StatusCode::CREATED,
        &serde_json::json!({ "reference": reference_hex }),
    );
    set_immutable_cache_headers(&mut resp);
    finalize_upload_tag(
        &handle,
        &headers,
        total_chunks as u64,
        split.root,
        &mut resp,
    );
    if let Some(history_hex) = &act_history {
        crate::act::set_act_history_header(&mut resp, history_hex);
    }
    resp
}

/// `POST /feeds/{owner}/{topic}` — create a feed manifest (bee-js
/// `createFeedManifest`). Builds the mantaray feed manifest binding
/// `(owner, topic)` for a `Sequence` feed, pushsyncs its single chunk,
/// and returns `{"reference":"<manifest root>"}` (PLAN.md J.2.5 / C2).
/// The reference resolves transparently to the feed's current update
/// via any bee gateway. Like bee's `feedPostHandler`, the `?type=`
/// query parameter is ignored — bee only writes `Sequence` manifests
/// regardless of what the caller asks for.
pub async fn create_feed(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, topic_hex)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let (owner, topic) = match parse_owner_pair(&owner_hex, "topic", &topic_hex) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let level = match parse_redundancy_level_header(&headers) {
        Ok(l) => l,
        Err(resp) => return resp,
    };
    let act = match crate::act::parse_act_upload(&headers) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, CHUNK_PUSH_TIMEOUT);
    let guard = handle.activity.begin(
        GatewayRequestKind::Feed,
        short_reference(&hex::encode(topic)),
    );

    let manifest = match ant_retrieval::build_feed_manifest(&owner, &topic) {
        Ok(m) => m,
        Err(e) => return map_manifest_error(e),
    };
    // A feed manifest is a single mantaray node, so at redundancy
    // level > 0 there is nothing to erasure-code — bee runs it through
    // the redundant pipeline all the same, which only mints dispersed
    // replicas of each node (pipeline root).
    let mut replicas = Vec::new();
    for node in &manifest.chunks {
        replicas.extend(replica_chunks(&node.address, &node.wire, level));
    }
    let total_chunks = manifest.chunks.len() + replicas.len();
    guard.update(0, total_chunks as u64, total_chunks as u32, 0);

    if let Err(err_resp) = push_chunks(&handle, &manifest.chunks, batch_id, timeout, &guard).await {
        return err_resp;
    }
    if let Err(err_resp) = push_replica_socs(&handle, &replicas, batch_id, timeout).await {
        return err_resp;
    }

    // ACT on `POST /feeds` (bee `feedPostHandler`): wrap the feed
    // manifest reference.
    let mut response_ref = manifest.root.to_vec();
    let mut act_history: Option<String> = None;
    if let Some(act_params) = &act {
        match crate::act::act_encrypt_upload(&handle, act_params, &response_ref, batch_id, timeout)
            .await
        {
            Ok(outcome) => {
                response_ref = outcome.reference;
                act_history = Some(outcome.history_hex);
            }
            Err(resp) => return resp,
        }
    }

    let reference_hex = hex::encode(&response_ref);
    let mut resp = json_response(
        StatusCode::CREATED,
        &serde_json::json!({ "reference": reference_hex }),
    );
    // Feed manifests are content-addressed and immutable (the manifest
    // binds owner+topic forever; only the feed *updates* it points at
    // change), so the manifest reference itself caches like any CAC.
    set_immutable_cache_headers(&mut resp);
    // Bee's `feedPostHandler` creates an upload session (visible in
    // `GET /tags`) but does *not* echo `Swarm-Tag` on the response.
    let _ = handle
        .tags
        .create_completed(total_chunks as u64, manifest.root);
    if let Some(history_hex) = &act_history {
        crate::act::set_act_history_header(&mut resp, history_hex);
    }
    resp
}

/// Plan: address of the manifest root + every chunk that has to be
/// pushed for the upload to be retrievable (data leaves +
/// intermediate join chunks + parity chunks + manifest chunks), plus
/// the dispersed replica SOCs a redundant upload carries.
struct AssembledUpload {
    /// Manifest root reference: 32 bytes plain, 64 (`address ‖ key`)
    /// when the upload is encrypted.
    root: Vec<u8>,
    chunks: Vec<SplitChunk>,
    /// Replica SOCs to push via `PushSoc` when the redundancy level
    /// is nonzero. Bee mints 2^level replicas of **every pipeline
    /// root**: each file's data root and every mantaray manifest node
    /// (each node is saved through its own mini-pipeline in bee's
    /// loadsave, so each counts as a root).
    replicas: Vec<SplitChunk>,
}

#[allow(clippy::result_large_err)]
fn assemble_bzz(
    query: &UploadBzzQuery,
    headers: &HeaderMap,
    body: &Bytes,
    collection: bool,
    level: u8,
    encrypt: bool,
) -> Result<AssembledUpload, Response> {
    // Header values are read as raw UTF-8, not ASCII: Go's net/http
    // hands bee the raw header bytes, so a unicode
    // `swarm-index-document` (e.g. a Japanese filename) is honored by
    // bee — `HeaderValue::to_str()` would silently drop it and fork
    // the manifest reference.
    let index_doc_header = || {
        headers
            .get(SWARM_INDEX_DOCUMENT)
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
    };
    if collection {
        let index_doc = index_doc_header()
            .map(|s| s.trim().trim_start_matches('/').to_string())
            .filter(|s| !s.is_empty());
        assemble_collection(body, index_doc.as_deref(), level, encrypt)
    } else {
        let filename = query
            .name()
            .map(|s| s.trim().trim_start_matches('/').to_string())
            .or_else(|| index_doc_header().map(|s| s.trim().trim_start_matches('/').to_string()))
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "index.html".to_string());

        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && s != "application/octet-stream");

        assemble_single_file(body, &filename, content_type.as_deref(), level, encrypt)
    }
}

#[allow(clippy::result_large_err)]
fn assemble_single_file(
    body: &Bytes,
    filename: &str,
    content_type: Option<&str>,
    level: u8,
    encrypt: bool,
) -> Result<AssembledUpload, Response> {
    if encrypt {
        // Encrypted upload: encrypt-split the file, then wrap its
        // 64-byte reference in an encrypted mantaray manifest (nodes
        // themselves stored encrypted, random obfuscation keys) — bee's
        // `fileUploadHandler` with an encrypting pipeline + loadsave.
        let split = ant_retrieval::split_bytes_encrypted(body, level);
        let manifest = match ant_retrieval::manifest_writer::build_single_file_manifest_encrypted(
            filename,
            content_type,
            &split.root_ref,
            level,
        ) {
            Ok(m) => m,
            Err(e) => return Err(map_manifest_error(e)),
        };
        let mut replicas = replica_chunks(&split.root_address(), &split.root_wire, level);
        for (addr, wire) in &manifest.node_roots {
            replicas.extend(replica_chunks(addr, wire, level));
        }
        let mut chunks = split.chunks;
        chunks.extend(manifest.chunks);
        return Ok(AssembledUpload {
            root: manifest.root_ref.to_vec(),
            chunks,
            replicas,
        });
    }
    let split = split_bytes_with_redundancy(body, level);
    let manifest = match build_single_file_manifest(filename, content_type, split.root) {
        Ok(m) => m,
        Err(e) => return Err(map_manifest_error(e)),
    };
    let mut replicas = replica_chunks(&split.root, &split.root_wire, level);
    for node in &manifest.chunks {
        replicas.extend(replica_chunks(&node.address, &node.wire, level));
    }
    let mut chunks = split.chunks;
    chunks.extend(manifest.chunks);
    Ok(AssembledUpload {
        root: manifest.root.to_vec(),
        chunks,
        replicas,
    })
}

#[allow(clippy::result_large_err)]
fn assemble_collection(
    body: &Bytes,
    index_doc: Option<&str>,
    level: u8,
    encrypt: bool,
) -> Result<AssembledUpload, Response> {
    let mut archive = tar::Archive::new(std::io::Cursor::new(body.as_ref()));
    let mut files: Vec<ManifestFile> = Vec::new();
    let mut data_chunks: Vec<SplitChunk> = Vec::new();
    let mut replicas: Vec<SplitChunk> = Vec::new();

    let entries = match archive.entries() {
        Ok(e) => e,
        Err(e) => {
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                format!("invalid tar archive: {e}"),
            ));
        }
    };
    for entry in entries {
        let mut entry = match entry {
            Ok(e) => e,
            Err(e) => {
                return Err(json_error(
                    StatusCode::BAD_REQUEST,
                    format!("malformed tar entry: {e}"),
                ));
            }
        };
        // Only regular files become manifest entries. Directories,
        // symlinks, hardlinks, longlink/longname pax extensions etc. are
        // silently skipped — bee's collection upload does the same.
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        let path_owned = match entry.path() {
            Ok(p) => p.to_path_buf(),
            Err(e) => {
                return Err(json_error(
                    StatusCode::BAD_REQUEST,
                    format!("tar entry has bad path: {e}"),
                ));
            }
        };
        let path_str = match path_owned.to_str() {
            Some(s) => s
                .trim_start_matches("./")
                .trim_start_matches('/')
                .to_string(),
            None => {
                return Err(json_error(
                    StatusCode::BAD_REQUEST,
                    "tar entry path is not valid UTF-8",
                ));
            }
        };
        if path_str.is_empty() {
            continue;
        }

        let mut buf = Vec::with_capacity(entry.header().size().unwrap_or(0) as usize);
        if let Err(e) = std::io::Read::read_to_end(&mut entry, &mut buf) {
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                format!("tar entry {path_str}: read failed: {e}"),
            ));
        }
        // Bee's tar reader stores `mime.TypeByExtension` verbatim —
        // including the EMPTY string for unknown/absent extensions
        // (dirs.go always writes the Content-Type metadata key). An
        // octet-stream substitute here would change the manifest bytes
        // and fork the /bzz reference.
        let ct = Some(
            content_type_from_extension(&path_str)
                .unwrap_or_default()
                .to_string(),
        );
        let data_ref = if encrypt {
            let split = ant_retrieval::split_bytes_encrypted(&buf, level);
            replicas.extend(replica_chunks(
                &split.root_address(),
                &split.root_wire,
                level,
            ));
            data_chunks.extend(split.chunks);
            split.root_ref.to_vec()
        } else {
            let split = split_bytes_with_redundancy(&buf, level);
            replicas.extend(replica_chunks(&split.root, &split.root_wire, level));
            data_chunks.extend(split.chunks);
            split.root.to_vec()
        };
        files.push(ManifestFile {
            path: path_str,
            content_type: ct,
            data_ref,
        });
    }
    if files.is_empty() {
        return Err(json_error(
            StatusCode::BAD_REQUEST,
            "tar archive contains no regular files",
        ));
    }

    // Index document comes ONLY from the `swarm-index-document` header.
    // Bee's HTTP API (`storeDir` in bee/pkg/api/dirs.go) adds no root
    // metadata when the header is absent — a tar that happens to
    // contain `index.html` must NOT grow an implicit anchor, or the
    // manifest reference forks from bee's. (bee-js's `uploadCollection`
    // convenience default lives client-side, not in the API.)
    let resolved_index = index_doc.map(str::to_string);
    if encrypt {
        let manifest = match ant_retrieval::manifest_writer::build_collection_manifest_encrypted(
            &files,
            resolved_index.as_deref(),
            IndexAnchor::ZeroEntry,
            level,
        ) {
            Ok(m) => m,
            Err(e) => return Err(map_manifest_error(e)),
        };
        for (addr, wire) in &manifest.node_roots {
            replicas.extend(replica_chunks(addr, wire, level));
        }
        let mut chunks = data_chunks;
        chunks.extend(manifest.chunks);
        return Ok(AssembledUpload {
            root: manifest.root_ref.to_vec(),
            chunks,
            replicas,
        });
    }
    let manifest = match build_collection_manifest(
        &files,
        resolved_index.as_deref(),
        IndexAnchor::ZeroEntry,
    ) {
        Ok(m) => m,
        Err(e) => return Err(map_manifest_error(e)),
    };

    for node in &manifest.chunks {
        replicas.extend(replica_chunks(&node.address, &node.wire, level));
    }
    let mut chunks = data_chunks;
    chunks.extend(manifest.chunks);
    Ok(AssembledUpload {
        root: manifest.root.to_vec(),
        chunks,
        replicas,
    })
}

#[allow(clippy::result_large_err)]
fn map_manifest_error(e: ant_retrieval::manifest_writer::ManifestWriteError) -> Response {
    use ant_retrieval::manifest_writer::ManifestWriteError;
    match e {
        ManifestWriteError::SegmentTooLong { .. } | ManifestWriteError::EmptyPath => {
            json_error(StatusCode::BAD_REQUEST, e.to_string())
        }
        ManifestWriteError::ManifestTooBig { .. } => {
            json_error(StatusCode::PAYLOAD_TOO_LARGE, e.to_string())
        }
        // Internal invariant: the gateway always hands the builder
        // references of the width it asked for.
        ManifestWriteError::RefWidth { .. } => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

/// Map a node-side upload failure onto bee's HTTP status.
///
/// One function for every write endpoint that dispatches a `PushChunk`
/// / `PushSoc` (`/bzz`, `/bytes`, `/chunks`, `/soc`, `/feeds`): the
/// status class is what a client's retry logic keys off, so `/soc`
/// answering a saturated batch with a different code than `/bzz` would
/// make the same condition look transient on one endpoint and fatal on
/// the other.
///
/// * `503` — the node cannot stamp at all (no upload runtime).
/// * `402 "batch is overissued"` (bee's `postage.ErrBucketFull`
///   mapping) — the batch's collision bucket is full. **Not** retryable:
///   the caller has to dilute or buy. This is the one a long-running
///   writer hits, e.g. an `AntStream` broadcast walking a batch's
///   buckets for an hour (#67 stage 2), and reporting it as a 502 sent
///   publishers into an unbounded retry against a permanently full
///   bucket.
/// * `400` — the batch is not registered/usable here.
/// * `422` — storer peers attested the batch does not exist on-chain
///   (phantom / expired / unsynced). Deterministic, not retryable. The
///   node doesn't report this for a batch it just bought: it waits out
///   the storers' sync instead (`BATCH_PROPAGATION` in `ant-p2p`).
/// * `502` — everything else: transient pushsync failure.
pub(crate) fn upload_error_status(message: &str) -> StatusCode {
    if message.starts_with("uploads not configured") {
        StatusCode::SERVICE_UNAVAILABLE
    } else if message.contains("overissued")
        || message.contains("bucket full")
        || message.contains("saturated")
    {
        StatusCode::PAYMENT_REQUIRED
    } else if message.contains("not usable") {
        StatusCode::BAD_REQUEST
    } else if message.contains("rejected by") && message.contains("not found on-chain") {
        StatusCode::UNPROCESSABLE_ENTITY
    } else {
        StatusCode::BAD_GATEWAY
    }
}

/// Push `chunks` to the network in parallel via `PushChunk` control
/// commands. Bounded by `MAX_PUSH_CONCURRENCY` so a 1 K-chunk file
/// doesn't open 1 K simultaneous outbound libp2p streams. Returns an
/// HTTP error response on the first chunk that fails to push.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
pub(crate) async fn push_chunks(
    handle: &GatewayHandle,
    chunks: &[SplitChunk],
    batch_id: [u8; 32],
    timeout: std::time::Duration,
    guard: &ActiveRequestGuard,
) -> Result<(), Response> {
    let total = chunks.len() as u64;
    let pushed = std::sync::atomic::AtomicU64::new(0);
    let bytes_done = std::sync::atomic::AtomicU64::new(0);
    let push_one = |chunk: SplitChunk| {
        let handle = handle.clone();
        let pushed = &pushed;
        let bytes_done = &bytes_done;
        async move {
            let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
            let cmd = ControlCommand::PushChunk {
                wire: chunk.wire.clone(),
                batch_id,
                stamp: None,
                // Bee-parity accept-shallow (see POST /chunks above).
                require_deep: false,
                ack: ack_tx,
            };
            if handle.commands.send(cmd).await.is_err() {
                return Err(node_unavailable());
            }
            match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(ControlAck::ChunkUploaded { reference })) => {
                    let stripped = reference
                        .strip_prefix("0x")
                        .or_else(|| reference.strip_prefix("0X"))
                        .unwrap_or(reference.as_str())
                        .to_string();
                    let want = hex::encode(chunk.address);
                    if stripped != want {
                        return Err(json_error(
                            StatusCode::BAD_GATEWAY,
                            format!("node reference {stripped} != local {want} (BMT mismatch)"),
                        ));
                    }
                    pushed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    bytes_done.fetch_add(
                        chunk.wire.len() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    Ok(())
                }
                Ok(Ok(ControlAck::Error { message })) => Err(json_error(
                    upload_error_status(&message),
                    format!("push chunk failed: {message}"),
                )),
                Ok(Ok(other)) => {
                    warn!(target: "ant_gateway", ?other, "unexpected ack from PushChunk");
                    Err(json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unexpected node ack",
                    ))
                }
                Ok(Err(_)) => Err(node_unavailable()),
                Err(_) => Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("upload timed out after {}s", timeout.as_secs()),
                )),
            }
        }
    };

    let mut stream = stream::iter(chunks.iter().cloned())
        .map(push_one)
        .buffer_unordered(MAX_PUSH_CONCURRENCY);

    while let Some(result) = stream.next().await {
        result?;
        let p = pushed.load(std::sync::atomic::Ordering::Relaxed);
        let b = bytes_done.load(std::sync::atomic::Ordering::Relaxed);
        let in_flight = (total - p).min(u64::from(u32::MAX)) as u32;
        guard.update(p, total, in_flight, b);
    }
    Ok(())
}

/// Push dispersed-replica SOC chunks (minted by
/// `ant_retrieval::replica_chunks`) via `PushSoc` control commands —
/// the upload-side counterpart of bee's `replicas.NewPutter`, which
/// stores the root's replica SOCs inside every redundant pipeline sum.
/// Stamped against the same batch as the upload's CAC chunks. Empty
/// input (redundancy level 0) is a no-op.
#[allow(clippy::result_large_err)]
async fn push_replica_socs(
    handle: &GatewayHandle,
    replicas: &[SplitChunk],
    batch_id: [u8; 32],
    timeout: std::time::Duration,
) -> Result<(), Response> {
    let push_one = |chunk: SplitChunk| {
        let handle = handle.clone();
        async move {
            let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
            let cmd = ControlCommand::PushSoc {
                address: chunk.address,
                wire: chunk.wire,
                batch_id,
                stamp: None,
                ack: ack_tx,
            };
            if handle.commands.send(cmd).await.is_err() {
                return Err(node_unavailable());
            }
            match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(ControlAck::ChunkUploaded { .. })) => Ok(()),
                Ok(Ok(ControlAck::Error { message })) => {
                    let status = if message.starts_with("uploads not configured") {
                        StatusCode::SERVICE_UNAVAILABLE
                    } else if message.contains("not usable") {
                        StatusCode::BAD_REQUEST
                    } else {
                        StatusCode::BAD_GATEWAY
                    };
                    Err(json_error(
                        status,
                        format!("push replica failed: {message}"),
                    ))
                }
                Ok(Ok(other)) => {
                    warn!(target: "ant_gateway", ?other, "unexpected ack from PushSoc");
                    Err(json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "unexpected node ack",
                    ))
                }
                Ok(Err(_)) => Err(node_unavailable()),
                Err(_) => Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("upload timed out after {}s", timeout.as_secs()),
                )),
            }
        }
    };

    let mut stream = stream::iter(replicas.iter().cloned())
        .map(push_one)
        .buffer_unordered(MAX_PUSH_CONCURRENCY);
    while let Some(result) = stream.next().await {
        result?;
    }
    Ok(())
}

/// Trim a hex reference to its leading 8 chars (16 if the caller
/// passed `0x` prefix) so the `antop` Retrieval tab column stays
/// readable. Empty / invalid references just round-trip unchanged.
fn short_reference(addr: &str) -> String {
    let stripped = addr
        .strip_prefix("0x")
        .or_else(|| addr.strip_prefix("0X"))
        .unwrap_or(addr);
    let head: String = stripped.chars().take(8).collect();
    if head.is_empty() {
        addr.to_string()
    } else {
        head
    }
}

fn chunk_response(method: Method, body: Vec<u8>) -> Response {
    let len = body.len();
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(body)
    };
    let mut resp = Response::new(body);
    // Bee's `chunkGetHandler` really does write `binary/octet-stream`
    // (a non-standard media type); match it byte for byte.
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("binary/octet-stream"),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    set_immutable_cache_headers(&mut resp);
    resp
}

/// `GET /bytes/{addr}` and `HEAD /bytes/{addr}`.
///
/// Drives the streaming dispatcher for every shape: full GET, HEAD, and
/// single-range GET. The daemon's range-aware joiner does the heavy
/// lifting — the gateway only translates HTTP semantics into a
/// [`StreamRange`] and emits the right status / headers.
pub async fn bytes(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    Query(query): Query<BytesQuery>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let any_reference = match parse_any_reference(&addr) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    // ACT resolution (bee's `actDecryptionHandler` middleware): with
    // the publisher + history headers present, the path address is an
    // ACT-encrypted reference — decrypt it and serve what it points at.
    let any_reference = match crate::act::act_maybe_resolve(&handle, &headers, any_reference).await
    {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    let raw_range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    note_redundancy_headers(&headers);
    let head_only = method == Method::HEAD;

    let activity_guard = Some(
        handle
            .activity
            .begin(GatewayRequestKind::Bytes, short_reference(&addr)),
    );

    // 64-byte (128-hex) encrypted reference: the node decrypt-joins the
    // tree into a buffer (bee's decrypting store under the joiner); the
    // gateway serves it in one shot, slicing for any Range.
    if let Ok(enc_ref) =
        <[u8; ant_retrieval::ENCRYPTED_REF_SIZE]>::try_from(any_reference.as_slice())
    {
        let _guard = activity_guard;
        return serve_encrypted_bytes(
            &handle,
            enc_ref,
            query.filename(),
            head_only,
            raw_range.as_deref(),
            timeout,
        )
        .await;
    }
    let reference: [u8; 32] = any_reference
        .as_slice()
        .try_into()
        .expect("parse_any_reference returns 32 or 64 bytes");

    // Phase 1: dispatch with head_only=true if the caller is HEAD;
    // for GET we go straight to the streaming dispatcher.
    let mut started =
        match dispatch_stream_bytes(&handle, reference, timeout, None, head_only, activity_guard)
            .await
        {
            Ok(s) => s,
            Err(e) => return e,
        };

    let total = started.total_bytes;
    // Range semantics are Go's `http.ServeContent` (bee serves through
    // it). HEAD ignores Range entirely: bee routes `HEAD /bytes` to
    // `bytesHeadHandler`, which never looks at the header and answers
    // `200` with the full length.
    let parsed = if head_only {
        GoRanges::Full
    } else {
        parse_go_ranges(raw_range.as_deref(), total)
    };
    let parsed_range = match parsed {
        GoRanges::Invalid => {
            started.cancel();
            return go_range_not_satisfiable(None, None);
        }
        GoRanges::NoOverlap => {
            started.cancel();
            return go_range_not_satisfiable(Some(total), None);
        }
        GoRanges::Full => None,
        GoRanges::Single { start, length } => {
            if length == 0 {
                started.cancel();
                let content_type = query
                    .filename()
                    .and_then(content_type_from_extension)
                    .unwrap_or("application/octet-stream");
                let mut resp = zero_length_partial_response(start, total, content_type);
                set_etag(&mut resp, reference);
                return resp;
            }
            Some((start, start + length - 1))
        }
        GoRanges::Multi(ranges) => {
            // Multipart: collect the whole body from the already-live
            // full stream once, then slice each part like Go seeks
            // the underlying reader.
            let content_type = sniff_content_type(&started.first_bytes, query.filename());
            let full = match started.collect_body().await {
                Ok(b) => b,
                Err(message) => return map_retrieval_error(message),
            };
            let mut resp = multipart_byteranges_response(&ranges, total, content_type, &full, None);
            set_etag(&mut resp, reference);
            return resp;
        }
    };

    // No Range, or Range covers the whole object: serve the in-flight
    // full-stream response we already started. Bee never invents a
    // `Content-Disposition` for `/bytes` — only an explicit `?name=`
    // (an ant extension) earns one. GET carries bee's quoted `ETag`;
    // bee's `bytesHeadHandler` sets none on HEAD.
    if parsed_range.is_none() {
        let content_type = sniff_content_type(&started.first_bytes, query.filename());
        let mut resp = streaming_response(
            head_only,
            total,
            started.into_body(head_only),
            content_type,
            query.filename(),
        );
        if !head_only {
            set_etag(&mut resp, reference);
        }
        return resp;
    }

    // Range request: cancel the full-body stream we just started and
    // dispatch a fresh range-scoped one. Cancellation is a drop on the
    // ack receiver; the daemon notices the closed channel and stops
    // joining further subtrees. Move the activity guard from the
    // cancelled `Started` into the new dispatch so the registry slot
    // stays alive across the swap (otherwise `antop` would see
    // the row blink out and back in for every range request).
    let carry_guard = started.take_activity_guard();
    started.cancel();
    let (start, end) = parsed_range.expect("parsed_range was just checked");
    let stream_range = StreamRange {
        start,
        end_inclusive: end,
    };
    let ranged = match dispatch_stream_bytes(
        &handle,
        reference,
        timeout,
        Some(stream_range),
        head_only,
        carry_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };
    let sniffable_total = ranged.total_bytes;
    // Sniffing on a tail range would mis-detect an MP4 as octet-stream.
    // Use the filename hint when it's a partial response and the leading
    // bytes don't start at offset 0.
    let content_type = if start == 0 {
        sniff_content_type(&ranged.first_bytes, query.filename())
    } else {
        query
            .filename()
            .and_then(content_type_from_extension)
            .unwrap_or("application/octet-stream")
    };
    let body_len = end - start + 1;
    let mut resp = partial_content_response(
        head_only,
        sniffable_total,
        start,
        end,
        body_len,
        ranged.into_body(head_only),
        content_type,
        query.filename(),
    );
    if !head_only {
        set_etag(&mut resp, reference);
    }
    resp
}

/// Serve `GET|HEAD /bytes/{128-hex}`: ask the node to decrypt-join the
/// tree behind `address ‖ key` into a buffer, then answer with the
/// usual bee-shaped headers, applying any `Range` by slicing. Encrypted
/// content is never streamed chunkwise (each chunk's key arrives with
/// its parent), matching the encrypted-feed serving path.
async fn serve_encrypted_bytes(
    handle: &GatewayHandle,
    enc_ref: [u8; ant_retrieval::ENCRYPTED_REF_SIZE],
    filename: Option<&str>,
    head_only: bool,
    raw_range: Option<&str>,
    timeout: Duration,
) -> Response {
    let reference: [u8; 32] = enc_ref[..32].try_into().expect("64-byte ref");
    let key: [u8; 32] = enc_ref[32..].try_into().expect("64-byte ref");

    let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(1);
    let cmd = ControlCommand::GetBytesEncrypted {
        reference,
        key,
        bypass_cache: false,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    let ack = match tokio::time::timeout(timeout, ack_rx.recv()).await {
        Ok(Some(ack)) => ack,
        Ok(None) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("retrieval timed out after {}s", timeout.as_secs()),
            );
        }
    };
    let data = match ack {
        ControlAck::Bytes { data } => data,
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        ControlAck::Error { message } => {
            return map_retrieval_error(message);
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetBytesEncrypted");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };
    let total = data.len() as u64;

    // Go `ServeContent` range semantics; HEAD ignores Range (bee's
    // headers-only handlers never reach `ServeContent`).
    let parsed = if head_only {
        GoRanges::Full
    } else {
        parse_go_ranges(raw_range, total)
    };
    let parsed_range = match parsed {
        GoRanges::Invalid => return go_range_not_satisfiable(None, None),
        GoRanges::NoOverlap => return go_range_not_satisfiable(Some(total), None),
        GoRanges::Full => None,
        GoRanges::Single { start, length } => {
            if length == 0 {
                let content_type = filename
                    .and_then(content_type_from_extension)
                    .unwrap_or("application/octet-stream");
                let mut resp = zero_length_partial_response(start, total, content_type);
                set_etag(&mut resp, enc_ref);
                return resp;
            }
            Some((start, start + length - 1))
        }
        GoRanges::Multi(ranges) => {
            let content_type = sniff_content_type(&data, filename);
            let mut resp = multipart_byteranges_response(&ranges, total, content_type, &data, None);
            set_etag(&mut resp, enc_ref);
            return resp;
        }
    };

    match parsed_range {
        None => {
            let content_type = sniff_content_type(&data, filename);
            let body = Body::from(data);
            let mut resp = streaming_response(head_only, total, body, content_type, filename);
            if !head_only {
                set_etag(&mut resp, enc_ref);
            }
            resp
        }
        Some((start, end)) => {
            let content_type = if start == 0 {
                sniff_content_type(&data, filename)
            } else {
                filename
                    .and_then(content_type_from_extension)
                    .unwrap_or("application/octet-stream")
            };
            let body_len = end - start + 1;
            let slice = data[start as usize..=end as usize].to_vec();
            let mut resp = partial_content_response(
                head_only,
                total,
                start,
                end,
                body_len,
                Body::from(slice),
                content_type,
                filename,
            );
            if !head_only {
                set_etag(&mut resp, enc_ref);
            }
            resp
        }
    }
}

/// `GET /bzz/{addr}` and `HEAD /bzz/{addr}`. Empty path resolves to
/// the manifest's `website-index-document` metadata; our `lookup_path`
/// already implements that fallback.
pub async fn bzz_root(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    bzz_inner(handle, addr, String::new(), None, method, headers).await
}

/// `GET /bzz/{addr}/{*path}` and the matching HEAD. Walks the
/// manifest, joins the data tree, returns bytes. Content-Type comes
/// from the manifest's per-file metadata when present; otherwise
/// defaults to `application/octet-stream` (bee does the same).
pub async fn bzz_with_path(
    State(handle): State<GatewayHandle>,
    Path((addr, path)): Path<(String, String)>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    method: Method,
    headers: HeaderMap,
) -> Response {
    bzz_inner(handle, addr, path, query, method, headers).await
}

/// `GET /v0/manifest/{addr}`. Lists the paths and metadata currently
/// discoverable in a mantaray manifest. This is an Ant-specific
/// extension — bee does not expose manifest enumeration on its public
/// HTTP API — and is namespaced under `/v0/` to keep the bee Tier-A
/// surface byte-for-byte compatible.
pub async fn manifest(State(handle): State<GatewayHandle>, Path(addr): Path<String>) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    let _guard = handle
        .activity
        .begin(GatewayRequestKind::Manifest, short_reference(&addr));

    match dispatch_list_bzz(&handle, reference, DEFAULT_REQUEST_TIMEOUT).await {
        Ok(entries) => {
            // Manifest listing keys off the same content-addressed
            // root the user passed in, so the listing itself is
            // immutable. Cache it like a `/bzz` payload to avoid
            // re-walking the manifest on every page navigation.
            let mut resp = json_response(StatusCode::OK, &ManifestListing { entries });
            set_immutable_cache_headers(&mut resp);
            resp
        }
        Err(e) => e,
    }
}

async fn bzz_inner(
    handle: GatewayHandle,
    addr: String,
    path: String,
    query: Option<String>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    // 32-byte plain or 64-byte encrypted manifest root; the node loop
    // walks encrypted manifests transparently (`StreamBzz` takes both).
    let reference = match parse_any_reference(&addr) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    // ACT resolution (bee wraps `bzzDownloadHandler`/`bzzHeadHandler`
    // in its `actDecryptionHandler`).
    let reference = match crate::act::act_maybe_resolve(&handle, &headers, reference).await {
        Ok(r) => r,
        Err(resp) => return resp,
    };

    let raw_range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    note_redundancy_headers(&headers);
    let head_only = method == Method::HEAD;

    let activity_label = if path.is_empty() {
        short_reference(&addr)
    } else {
        format!("{}/{path}", short_reference(&addr))
    };
    let activity_guard = Some(
        handle
            .activity
            .begin(GatewayRequestKind::Bzz, activity_label),
    );

    let mut started = match dispatch_stream_bzz(
        &handle,
        reference.clone(),
        path.clone(),
        timeout,
        None,
        head_only,
        activity_guard,
    )
    .await
    {
        Ok(s) => s,
        // `map_retrieval_error` signals a manifest directory miss with
        // a bare 308 (it can't know the request URL); complete it here
        // into bee's exact redirect shape.
        Err(e) if e.status() == StatusCode::PERMANENT_REDIRECT => {
            return directory_redirect(&addr, &path, query.as_deref(), &method);
        }
        Err(e) => return e,
    };

    let total = started.total_bytes;
    // Go `ServeContent` range semantics; HEAD ignores Range (bee's
    // `bzzHeadHandler` runs headers-only and never reaches
    // `ServeContent`).
    let parsed = if head_only {
        GoRanges::Full
    } else {
        parse_go_ranges(raw_range.as_deref(), total)
    };

    let content_type = started
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let filename = started.filename.clone();
    // Resolved data reference for the quoted `ETag` (bee's
    // `downloadHandler` sets it on GET and HEAD alike).
    let data_ref = started.reference.clone();
    // Feed-backed bzz references resolve to mutable content at a stable
    // URL; serving them immutably (as `streaming_response` does by
    // default) makes browsers/proxies pin a stale resolution and never
    // re-fetch — the exact "feed never updates" failure. Downgrade to
    // `no-cache` for these.
    let mutable = started.mutable;

    let parsed_range = match parsed {
        GoRanges::Invalid => {
            started.cancel();
            return go_range_not_satisfiable(None, filename.as_deref());
        }
        GoRanges::NoOverlap => {
            started.cancel();
            return go_range_not_satisfiable(Some(total), filename.as_deref());
        }
        GoRanges::Full => None,
        GoRanges::Single { start, length } => {
            if length == 0 {
                started.cancel();
                let mut resp = zero_length_partial_response(start, total, &content_type);
                if let Some(r) = data_ref {
                    set_etag(&mut resp, r);
                }
                if mutable {
                    set_mutable_cache_headers(&mut resp);
                }
                return resp;
            }
            Some((start, start + length - 1))
        }
        GoRanges::Multi(ranges) => {
            let full = match started.collect_body().await {
                Ok(b) => b,
                Err(message) => return map_retrieval_error(message),
            };
            let mut resp = multipart_byteranges_response(
                &ranges,
                total,
                &content_type,
                &full,
                filename.as_deref(),
            );
            if let Some(r) = data_ref {
                set_etag(&mut resp, r);
            }
            if mutable {
                set_mutable_cache_headers(&mut resp);
            }
            return resp;
        }
    };

    if parsed_range.is_none() {
        let mut resp = streaming_response(
            head_only,
            total,
            started.into_body(head_only),
            &content_type,
            filename.as_deref(),
        );
        if let Some(r) = data_ref {
            set_etag(&mut resp, r);
        }
        if mutable {
            set_mutable_cache_headers(&mut resp);
        }
        return resp;
    }

    let carry_guard = started.take_activity_guard();
    started.cancel();
    let (start, end) = parsed_range.expect("parsed_range was just checked");
    let stream_range = StreamRange {
        start,
        end_inclusive: end,
    };
    let ranged = match dispatch_stream_bzz(
        &handle,
        reference,
        path,
        timeout,
        Some(stream_range),
        head_only,
        carry_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };
    let body_len = end - start + 1;
    let mut resp = partial_content_response(
        head_only,
        total,
        start,
        end,
        body_len,
        ranged.into_body(head_only),
        &content_type,
        filename.as_deref(),
    );
    if let Some(r) = data_ref {
        set_etag(&mut resp, r);
    }
    if mutable {
        set_mutable_cache_headers(&mut resp);
    }
    resp
}

#[derive(Debug, Serialize)]
struct ManifestListing {
    entries: Vec<ant_control::ManifestEntryInfo>,
}

#[derive(Debug, Default, Deserialize)]
pub struct BytesQuery {
    #[serde(default, alias = "name")]
    filename: Option<String>,
}

impl BytesQuery {
    fn filename(&self) -> Option<&str> {
        self.filename
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

/// `?name=<filename>` for `POST /bzz` single-file mode. Bee accepts the
/// same query parameter on its `/bzz` upload endpoint to override the
/// manifest entry name without requiring a multipart body.
#[derive(Debug, Default, Deserialize)]
pub struct UploadBzzQuery {
    #[serde(default)]
    name: Option<String>,
}

impl UploadBzzQuery {
    fn name(&self) -> Option<&str> {
        self.name
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

/// `?sig=<65-byte hex>` for `POST /soc/{owner}/{id}`. Bee-js ≥ 10
/// (matching modern bee) sends the SOC signature here instead of in
/// the `swarm-soc-signature` header. `upload_soc` accepts either form.
#[derive(Debug, Default, Deserialize)]
pub struct UploadSocQuery {
    #[serde(default)]
    sig: Option<String>,
}

/// Result of dispatching a streaming retrieval against the node loop:
/// the prologue ack (size + optional manifest metadata) plus the live
/// receiver still draining body chunks. The gateway sends headers as
/// soon as it has this struct in hand and, for non-HEAD GETs, drains
/// the receiver into the HTTP response body via `Body::from_stream`.
///
/// `cancel()` drops the receiver so the daemon notices the closed
/// channel and stops fetching further subtrees. We use that when a
/// `Range` request comes in: we issue an unranged dispatch first to
/// learn `total_bytes`, then re-dispatch with a `StreamRange` once
/// the range parses. Cheap because the daemon's joiner has barely
/// started by the time the gateway has parsed the range.
struct Started {
    total_bytes: u64,
    first_bytes: Vec<u8>,
    /// Manifest metadata: only populated for `StreamBzz`. `None` for
    /// `StreamBytes` since the raw chunk tree carries no content type
    /// or filename.
    content_type: Option<String>,
    filename: Option<String>,
    /// Resolved data reference of the served manifest entry — only
    /// populated for `StreamBzz`. Bee quotes it in the `ETag` header.
    reference: Option<Vec<u8>>,
    /// `true` when the bzz reference resolved through a feed manifest, so
    /// the response is mutable and must be served `no-cache`. Always
    /// `false` for `StreamBytes`.
    mutable: bool,
    first_chunk: Option<Vec<u8>>,
    rx: mpsc::Receiver<ControlAck>,
    /// RAII guard for the gateway-activity registry slot. Lives for
    /// the duration of the HTTP exchange: handed off into the
    /// streaming-body unfold state on `into_body` so the slot stays
    /// alive while the response body drains, and dropped explicitly
    /// in `cancel()` when we redispatch a fresh `Started` for a
    /// range request (the new `Started` brings its own guard).
    activity_guard: Option<ActiveRequestGuard>,
}

impl Started {
    fn cancel(&mut self) {
        self.rx.close();
    }

    /// Extract the activity guard so the caller can carry the
    /// registry slot across a cancel-and-redispatch (Range parsing
    /// path). The redispatched `Started` brings its own `cancel`
    /// semantics; reusing the existing guard keeps the entry stable
    /// across the brief window where two streams are active.
    const fn take_activity_guard(&mut self) -> Option<ActiveRequestGuard> {
        self.activity_guard.take()
    }

    /// Drain the remaining stream into one buffer (used by the
    /// `multipart/byteranges` path, which needs random access to the
    /// full body like Go's seekable reader). Applies the same stall
    /// timeout the streaming body uses; errors come back as the node's
    /// message for `map_retrieval_error`.
    async fn collect_body(mut self) -> Result<Vec<u8>, String> {
        let mut out = self.first_chunk.take().unwrap_or_default();
        loop {
            match tokio::time::timeout(BODY_STALL_TIMEOUT, self.rx.recv()).await {
                Err(_) => {
                    return Err(
                        "stream stalled: a chunk could not be retrieved from the network".into(),
                    )
                }
                Ok(Some(ControlAck::BytesChunk { data })) => out.extend_from_slice(&data),
                Ok(Some(ControlAck::StreamDone) | None) => return Ok(out),
                Ok(Some(ControlAck::Progress(_))) => {}
                Ok(Some(ControlAck::Error { message })) => return Err(message),
                Ok(Some(other)) => {
                    warn!(target: "ant_gateway", ?other, "unexpected ack while buffering body");
                    return Err("unexpected node ack".into());
                }
            }
        }
    }

    /// Hand back the response body. For `HEAD` we want an empty body
    /// regardless of what the daemon sent; for `GET` we wrap the live
    /// receiver in a stream that pulls each `BytesChunk` out as a
    /// `Bytes` slice. Consumes `self` because the caller can either
    /// drain the body or cancel — never both.
    fn into_body(self, head_only: bool) -> Body {
        if head_only {
            self.rx.close_now();
            return Body::empty();
        }
        let Self {
            first_chunk,
            rx,
            activity_guard,
            ..
        } = self;
        let activity = activity_guard
            .as_ref()
            .map(ant_control::ActiveRequestGuard::handle);
        // Stall detection: the joiner reports `bytes_done` on every
        // `Progress` tick (~150 ms) whether or not data is moving, so we
        // can't key off ack silence. Track the high-water byte mark and a
        // sliding deadline that only advances when bytes actually do. If
        // the deadline lapses the joiner is wedged on an unfetchable
        // chunk — surface an error and tear the response down rather than
        // wait out its multi-minute retry budget.
        let deadline = Instant::now() + BODY_STALL_TIMEOUT;
        let body_stream = stream::unfold(
            (first_chunk, rx, activity, activity_guard, 0u64, deadline),
            |(mut first, mut rx, activity, guard, mut max_bytes, mut deadline)| async move {
                if let Some(data) = first.take() {
                    max_bytes = max_bytes.max(data.len() as u64);
                    deadline = Instant::now() + BODY_STALL_TIMEOUT;
                    return Some((
                        Ok::<Bytes, std::io::Error>(Bytes::from(data)),
                        (None, rx, activity, guard, max_bytes, deadline),
                    ));
                }
                loop {
                    let now = Instant::now();
                    let wait = deadline.saturating_duration_since(now);
                    if wait.is_zero() {
                        return Some((
                            Err(std::io::Error::other(
                                "stream stalled: a chunk could not be retrieved from the network",
                            )),
                            (None, rx, activity, guard, max_bytes, deadline),
                        ));
                    }
                    match tokio::time::timeout(wait, rx.recv()).await {
                        Err(_) => {
                            return Some((
                                Err(std::io::Error::other(
                                    "stream stalled: a chunk could not be retrieved from the network",
                                )),
                                (None, rx, activity, guard, max_bytes, deadline),
                            ));
                        }
                        Ok(Some(ControlAck::BytesChunk { data })) => {
                            deadline = Instant::now() + BODY_STALL_TIMEOUT;
                            return Some((
                                Ok::<Bytes, std::io::Error>(Bytes::from(data)),
                                (None, rx, activity, guard, max_bytes, deadline),
                            ));
                        }
                        Ok(Some(ControlAck::StreamDone) | None) => return None,
                        Ok(Some(ControlAck::Progress(p))) => {
                            if let Some(h) = activity.as_ref() {
                                h.update(
                                    p.chunks_done,
                                    p.total_chunks_estimate,
                                    p.in_flight,
                                    p.bytes_done,
                                );
                            }
                            // Only forward byte movement resets the stall
                            // clock; a Progress tick on its own does not.
                            if p.bytes_done > max_bytes {
                                max_bytes = p.bytes_done;
                                deadline = Instant::now() + BODY_STALL_TIMEOUT;
                            }
                        }
                        Ok(Some(ControlAck::Error { message })) => {
                            return Some((
                                Err(std::io::Error::other(message)),
                                (None, rx, activity, guard, max_bytes, deadline),
                            ));
                        }
                        Ok(Some(other)) => {
                            warn!(
                                target: "ant_gateway",
                                ?other,
                                "unexpected ack during streaming body"
                            );
                            return Some((
                                Err(std::io::Error::other("unexpected node ack")),
                                (None, rx, activity, guard, max_bytes, deadline),
                            ));
                        }
                    }
                }
            },
        );
        Body::from_stream(body_stream)
    }
}

/// Helper trait so we can `close_now()` a receiver synchronously after
/// we've consumed `self`. `Receiver::close()` is async-cancellable but
/// `close()` itself is sync — we just want it shut so the daemon's
/// `send().await` returns Err and the spawned task tears down.
trait ReceiverClose {
    fn close_now(self);
}

impl ReceiverClose for mpsc::Receiver<ControlAck> {
    fn close_now(mut self) {
        self.close();
    }
}

// `Err(Response)` is the routine "early-return an error response" path —
// boxing it would add a heap alloc to the failure case for no benefit.
#[allow(clippy::result_large_err)]
async fn dispatch_stream_bytes(
    handle: &GatewayHandle,
    reference: [u8; 32],
    timeout: Duration,
    range: Option<StreamRange>,
    head_only: bool,
    activity_guard: Option<ActiveRequestGuard>,
) -> Result<Started, Response> {
    let (ack_tx, ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::StreamBytes {
        reference,
        bypass_cache: false,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        range,
        head_only,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    consume_stream_prologue(ack_rx, timeout, false, head_only, activity_guard).await
}

#[allow(clippy::result_large_err)]
async fn dispatch_stream_bzz(
    handle: &GatewayHandle,
    reference: Vec<u8>,
    path: String,
    timeout: Duration,
    range: Option<StreamRange>,
    head_only: bool,
    activity_guard: Option<ActiveRequestGuard>,
) -> Result<Started, Response> {
    let (ack_tx, ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::StreamBzz {
        reference,
        path,
        // The plan calls out that mainnet bzz roots commonly carry a
        // redundancy level byte we don't yet RS-decode; bee's reader
        // strips it and serves anyway. We follow that — the joiner
        // still fails fast if a data chunk is actually missing.
        allow_degraded_redundancy: true,
        bypass_cache: false,
        max_bytes: Some(GATEWAY_MAX_FILE_BYTES),
        range,
        head_only,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    consume_stream_prologue(ack_rx, timeout, true, head_only, activity_guard).await
}

/// Walk the prologue of a `StreamBytes` / `StreamBzz` response: forward
/// `Progress` samples to the activity registry, accept the appropriate
/// `*StreamStart` ack, then peek the first body chunk so the caller can
/// sniff content type and hand the rest off to hyper as a stream. For
/// `head_only = true` we also accept a terminal `StreamDone`
/// immediately after the prologue — no body to peek.
#[allow(clippy::result_large_err)]
async fn consume_stream_prologue(
    mut rx: mpsc::Receiver<ControlAck>,
    timeout: Duration,
    expect_bzz: bool,
    head_only: bool,
    activity_guard: Option<ActiveRequestGuard>,
) -> Result<Started, Response> {
    let activity_handle = activity_guard
        .as_ref()
        .map(ant_control::ActiveRequestGuard::handle);
    let total_bytes;
    let mut content_type = None;
    let mut filename = None;
    let mut reference = None;
    let mut mutable = false;
    loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ControlAck::BytesStreamStart { total_bytes: t })) if !expect_bzz => {
                total_bytes = t;
                break;
            }
            Ok(Some(ControlAck::BzzStreamStart {
                total_bytes: t,
                reference: r,
                content_type: ct,
                filename: fn_,
                mutable: m,
            })) if expect_bzz => {
                total_bytes = t;
                content_type = ct;
                filename = fn_;
                reference = Some(r);
                mutable = m;
                break;
            }
            Ok(Some(ControlAck::Progress(p))) => {
                if let Some(h) = activity_handle.as_ref() {
                    h.update(
                        p.chunks_done,
                        p.total_chunks_estimate,
                        p.in_flight,
                        p.bytes_done,
                    );
                }
            }
            // Peers confirmed the chunk missing (issue #123): bee's
            // `jsonhttp.NotFound(w, nil)`, the same 404 a `storage: not
            // found` tail already got through `map_retrieval_error`.
            Ok(Some(ControlAck::NotFound { message })) => {
                debug!(target: "ant_gateway", %message, "retrieval: chunk not found");
                return Err(status_text_error(StatusCode::NOT_FOUND));
            }
            // A miss from a starved peer pool: not the network's answer
            // (#114), so a retryable 503 rather than a 404.
            Ok(Some(ControlAck::NotReady { message })) => {
                return Err(json_error(StatusCode::SERVICE_UNAVAILABLE, message));
            }
            Ok(Some(ControlAck::Error { message })) => {
                return Err(map_retrieval_error(message));
            }
            Ok(Some(other)) => {
                warn!(target: "ant_gateway", ?other, expect_bzz, "unexpected ack before stream start");
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ));
            }
            Ok(None) => return Err(node_unavailable()),
            Err(_) => {
                return Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                ));
            }
        }
    }

    if head_only {
        return Ok(Started {
            total_bytes,
            first_bytes: Vec::new(),
            content_type,
            filename,
            reference,
            mutable,
            first_chunk: None,
            rx,
            activity_guard,
        });
    }

    // Same stall guard as the body stream: the node heart-beats `Progress`
    // every ~150 ms, so a plain per-recv `timeout` would never fire while
    // the joiner spins on a missing *first* leaf. Bound the wait on byte
    // movement instead, capped by the request envelope.
    let mut max_bytes = 0u64;
    let mut stall_deadline = Instant::now() + BODY_STALL_TIMEOUT.min(timeout);
    let first_chunk = loop {
        let wait = stall_deadline.saturating_duration_since(Instant::now());
        if wait.is_zero() {
            return Err(json_error(
                StatusCode::GATEWAY_TIMEOUT,
                "retrieval stalled: a chunk could not be retrieved from the network",
            ));
        }
        match tokio::time::timeout(wait, rx.recv()).await {
            Ok(Some(ControlAck::BytesChunk { data })) => break Some(data),
            Ok(Some(ControlAck::StreamDone)) => break None,
            Ok(Some(ControlAck::Progress(p))) => {
                if let Some(h) = activity_handle.as_ref() {
                    h.update(
                        p.chunks_done,
                        p.total_chunks_estimate,
                        p.in_flight,
                        p.bytes_done,
                    );
                }
                if p.bytes_done > max_bytes {
                    max_bytes = p.bytes_done;
                    stall_deadline = Instant::now() + BODY_STALL_TIMEOUT.min(timeout);
                }
            }
            Ok(Some(ControlAck::Error { message })) => {
                return Err(map_retrieval_error(message));
            }
            Ok(Some(other)) => {
                warn!(target: "ant_gateway", ?other, "unexpected ack before first body chunk");
                return Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ));
            }
            Ok(None) => return Err(node_unavailable()),
            Err(_) => {
                return Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    "retrieval stalled: a chunk could not be retrieved from the network",
                ));
            }
        }
    };
    let first_bytes = first_chunk
        .as_ref()
        .map(|b| b[..b.len().min(512)].to_vec())
        .unwrap_or_default();

    Ok(Started {
        total_bytes,
        first_bytes,
        content_type,
        filename,
        reference,
        mutable,
        first_chunk,
        rx,
        activity_guard,
    })
}

/// Map a daemon-reported retrieval error string into the bee-shaped
/// HTTP response for the condition, keeping the detailed internal
/// message in the log rather than the body:
///
/// - a feed manifest whose legacy update points at an unretrievable
///   chunk → bee's `404 "bzz download: feed pointing to the wrapped
///   chunk not found"` (`serveReference` in `bee/pkg/api/bzz.go`);
/// - a manifest path miss (`"path '…' not found"` and friends) → bee's
///   `404 "path address not found"`;
/// - any other miss (`"not found"` from the joiner / fetcher) → bee's
///   `jsonhttp.NotFound(w, nil)` = `404 "Not Found"`. A `/bytes` or
///   `/bzz` root the network confirmed missing doesn't come through
///   here: the node sends it typed as `ControlAck::NotFound`, whichever
///   miss tail (`storage: not found` / `no peer found`) it ended in
///   (issue #123);
/// - the rest are bad-gateway conditions where the message stays —
///   bee has no equivalent failure (it *is* the network) and operators
///   debug through this body today.
fn map_retrieval_error(message: String) -> Response {
    debug!(target: "ant_gateway", %message, "retrieval error");
    if message.contains("feed pointing to the wrapped chunk not found") {
        return json_error(
            StatusCode::NOT_FOUND,
            "bzz download: feed pointing to the wrapped chunk not found",
        );
    }
    // Manifest directory miss (`ManifestError::Directory`): bee 308s to
    // the slash-terminated URL. Returned as a bare status here;
    // `bzz_inner` fills in the Location header + Go-shaped HTML body
    // (it knows the request path, this function doesn't).
    if message.contains("is a directory (redirect to trailing slash)") {
        return StatusCode::PERMANENT_REDIRECT.into_response();
    }
    // Bare-root miss (empty path, no resolvable index document):
    // bee's empty-path branch has its own message, distinct from the
    // subpath miss below (`bzzDownloadHandler` in bee/pkg/api/bzz.go).
    if message.contains("path '(root)' not found") {
        return json_error(StatusCode::NOT_FOUND, "address not found or incorrect");
    }
    if message.contains("path '") {
        return json_error(StatusCode::NOT_FOUND, "path address not found");
    }
    if message.contains("not found") || message.contains('\'') {
        return status_text_error(StatusCode::NOT_FOUND);
    }
    json_error(StatusCode::BAD_GATEWAY, message)
}

#[allow(clippy::result_large_err)]
async fn dispatch_list_bzz(
    handle: &GatewayHandle,
    reference: [u8; 32],
    timeout: Duration,
) -> Result<Vec<ant_control::ManifestEntryInfo>, Response> {
    let (ack_tx, mut ack_rx) = ant_control::streaming_ack_channel();
    let cmd = ControlCommand::ListBzz {
        reference,
        bypass_cache: false,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    drain_terminal(&mut ack_rx, timeout)
        .await
        .and_then(|ack| match ack {
            ControlAck::Manifest { entries } => Ok(entries),
            ControlAck::Error { message } => Err(json_error(StatusCode::NOT_FOUND, message)),
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from ListBzz");
                Err(json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "unexpected node ack",
                ))
            }
        })
}

/// Walk the streaming ack channel, dropping `Progress` samples until a
/// terminal variant lands. Mirrors `ant-control::server::dispatch_streaming`
/// but without rewriting each ack to a JSON wire response — we only
/// care about the terminal payload.
#[allow(clippy::result_large_err)]
async fn drain_terminal(
    rx: &mut mpsc::Receiver<ControlAck>,
    timeout: Duration,
) -> Result<ControlAck, Response> {
    loop {
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(ControlAck::Progress(_))) => {}
            Ok(Some(ack)) => return Ok(ack),
            Ok(None) => return Err(node_unavailable()),
            Err(_) => {
                return Err(json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                ));
            }
        }
    }
}

/// Parse the `{address}` path segment of `/bytes`, `/chunks`, `/bzz`,
/// and `/v0/manifest` bee-shaped: a malformed value is bee's
/// `400 {"code":400,"message":"invalid path params","reasons":[{"field":
/// "address","error":"invalid hex byte: …"}]}`.
#[allow(clippy::result_large_err)]
pub(crate) fn parse_reference(s: &str) -> Result<[u8; 32], Response> {
    let mut reasons = Vec::new();
    parse_hex_param::<32>("address", s, &mut reasons)
        .ok_or_else(|| params_error(ParamKind::Path, reasons))
}

/// [`parse_reference`] that also accepts a 64-byte (128-hex)
/// **encrypted** reference — `address(32) ‖ decryption key(32)` — the
/// way bee's `swarm.ParseHexAddress` does on `/bytes` and `/bzz`
/// downloads. Returns the raw 32- or 64-byte reference.
#[allow(clippy::result_large_err)]
fn parse_any_reference(s: &str) -> Result<Vec<u8>, Response> {
    let mut reasons = Vec::new();
    if s.len() == 2 * ant_retrieval::ENCRYPTED_REF_SIZE {
        return parse_hex_param::<{ ant_retrieval::ENCRYPTED_REF_SIZE }>(
            "address",
            s,
            &mut reasons,
        )
        .map(|r| r.to_vec())
        .ok_or_else(|| params_error(ParamKind::Path, reasons));
    }
    parse_reference(s).map(|r| r.to_vec())
}

/// Parse bee's `Swarm-Encrypt` request header (absent → `false`).
/// Bee's `mapStructure` uses `strconv.ParseBool`, which accepts
/// `1/t/T/true/TRUE/True` and `0/f/F/false/FALSE/False` and rejects
/// everything else with `400 invalid header params`.
#[allow(clippy::result_large_err)]
fn parse_encrypt_header(headers: &HeaderMap) -> Result<bool, Response> {
    let Some(raw) = headers.get(&SWARM_ENCRYPT) else {
        return Ok(false);
    };
    match raw.to_str().map(str::trim) {
        Ok("1" | "t" | "T" | "true" | "TRUE" | "True") => Ok(true),
        Ok("0" | "f" | "F" | "false" | "FALSE" | "False") => Ok(false),
        _ => Err(params_error(
            ParamKind::Header,
            vec![Reason {
                field: "Swarm-Encrypt".to_string(),
                error: "invalid syntax".to_string(),
            }],
        )),
    }
}

/// Parse bee's `Swarm-Pin` request header (absent → `false`), with the
/// same Go `strconv.ParseBool` semantics as `Swarm-Encrypt`.
#[allow(clippy::result_large_err)]
fn parse_pin_header(headers: &HeaderMap) -> Result<bool, Response> {
    let Some(raw) = headers.get(&SWARM_PIN) else {
        return Ok(false);
    };
    match raw.to_str().map(str::trim) {
        Ok("1" | "t" | "T" | "true" | "TRUE" | "True") => Ok(true),
        Ok("0" | "f" | "F" | "false" | "FALSE" | "False") => Ok(false),
        _ => Err(params_error(
            ParamKind::Header,
            vec![Reason {
                field: "Swarm-Pin".to_string(),
                error: "invalid syntax".to_string(),
            }],
        )),
    }
}

/// Record a local pin for a just-uploaded root reference (bee's
/// `Swarm-Pin: true` upload behaviour: the upload session's chunks are
/// pinned when `putter.Done` runs). The chunks were pushed a moment
/// ago and sit in the node's local caches, so the pin traversal is a
/// cheap local walk. Failure maps to bee's 500 for a failed `Done`.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
async fn pin_uploaded_reference(
    handle: &GatewayHandle,
    reference: Vec<u8>,
    timeout: Duration,
) -> Result<(), Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::PinAdd {
        reference,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(node_unavailable());
    }
    match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ControlAck::PinAdded { .. })) => Ok(()),
        Ok(Ok(ControlAck::Error { message })) => {
            warn!(target: "ant_gateway", %message, "pin-on-upload failed");
            Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "done split failed",
            ))
        }
        Ok(Ok(other)) => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PinAdd (upload)");
            Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected node ack",
            ))
        }
        Ok(Err(_)) => Err(node_unavailable()),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "pin-on-upload timed out",
        )),
    }
}

/// Parse the `{owner}` + `{id|topic}` path pair of `/soc` and `/feeds`
/// bee-shaped, collecting every malformed segment into one
/// `invalid path params` response like bee's `mapStructure` does.
#[allow(clippy::result_large_err)]
fn parse_owner_pair(
    owner_hex: &str,
    second_field: &'static str,
    second_hex: &str,
) -> Result<([u8; 20], [u8; 32]), Response> {
    let mut reasons = Vec::new();
    let owner = parse_hex_param::<20>("owner", owner_hex, &mut reasons);
    let second = parse_hex_param::<32>(second_field, second_hex, &mut reasons);
    match (owner, second) {
        (Some(owner), Some(second)) => Ok((owner, second)),
        _ => Err(params_error(ParamKind::Path, reasons)),
    }
}

pub(crate) fn request_timeout(headers: &HeaderMap, default: Duration) -> Duration {
    headers
        .get(&SWARM_CHUNK_RETRIEVAL_TIMEOUT)
        .and_then(|v| v.to_str().ok())
        .and_then(humantime::parse_duration_lite)
        .unwrap_or(default)
}

/// Parse-and-ignore bee's download-side redundancy headers (see the
/// constants' doc comment for why they are no-ops here). Any value —
/// including ones bee would reject — is accepted: the read path always
/// runs the same DATA-equivalent strategy with RS recovery, so
/// rejecting a strategy we won't use would only break clients.
fn note_redundancy_headers(headers: &HeaderMap) {
    for name in [&SWARM_REDUNDANCY_STRATEGY, &SWARM_REDUNDANCY_FALLBACK_MODE] {
        if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok()) {
            tracing::trace!(
                target: "ant_gateway",
                header = %name,
                value,
                "redundancy header accepted and ignored (ant always fetches data shards and RS-recovers on miss)",
            );
        }
    }
}

/// Response-extension marker: this response's `Content-Type` is served
/// VERBATIM (it came from manifest metadata / sniffing, the way bee's
/// `serveManifestEntry` copies the stored value byte-for-byte) and must
/// not be normalized by the router's bee-`jsonhttp` charset middleware.
/// Without it, a manifest entry stored as `application/json` would grow
/// a `; charset=utf-8` suffix bee doesn't send.
#[derive(Clone, Copy)]
pub(crate) struct VerbatimContentType;

/// Build a `200 OK` (or empty `200 OK` for `HEAD`) response for a full
/// streaming GET. `body` should already be `Body::empty()` when
/// `head_only`; we set headers identically either way so HEAD and GET
/// agree on `Content-Length` / `Accept-Ranges` / `Content-Disposition`.
fn streaming_response(
    head_only: bool,
    total_bytes: u64,
    body: Body,
    content_type: &str,
    filename: Option<&str>,
) -> Response {
    let body = if head_only { Body::empty() } else { body };
    let mut resp = Response::new(body);
    resp.extensions_mut().insert(VerbatimContentType);
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(total_bytes));
    // `Accept-Ranges: bytes` comes from Go's `ServeContent`, which only
    // the GET paths reach — bee's HEAD handlers (`bytesHeadHandler`,
    // `downloadHandler` headers-only) never set it.
    if !head_only {
        let _ = resp
            .headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    }
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
        }
    }
    set_immutable_cache_headers(&mut resp);
    resp
}

/// Build a `206 Partial Content` (or empty `206 Partial Content` for
/// `HEAD`) response with the canonical bee-shape headers: `Content-Range`,
/// `Content-Length` (the slice length, not the total), `Accept-Ranges`,
/// and `Content-Disposition` when a filename is known. `body` should
/// already only contain the requested byte interval — the streaming
/// joiner emits exactly those bytes on a range dispatch.
#[allow(clippy::too_many_arguments)]
fn partial_content_response(
    head_only: bool,
    total_bytes: u64,
    start: u64,
    end_inclusive: u64,
    body_len: u64,
    body: Body,
    content_type: &str,
    filename: Option<&str>,
) -> Response {
    let body = if head_only { Body::empty() } else { body };
    let mut resp = Response::new(body);
    resp.extensions_mut().insert(VerbatimContentType);
    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    let _ = resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(body_len));
    let _ = resp
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    let cr = format!("bytes {start}-{end_inclusive}/{total_bytes}");
    if let Ok(v) = HeaderValue::from_str(&cr) {
        resp.headers_mut().insert(header::CONTENT_RANGE, v);
    }
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            resp.headers_mut().insert(header::CONTENT_DISPOSITION, v);
        }
    }
    set_immutable_cache_headers(&mut resp);
    resp
}

fn content_disposition(filename: &str) -> Option<HeaderValue> {
    // Bee keeps only the base name of the manifest entry's `Filename`
    // metadata (`filepath.Base` in `serveManifestEntry`), then escapes
    // ONLY backslash and double-quote (`escapeQuotes` in
    // bee/pkg/api/util.go) — characters like `<`, `>` or `'` pass
    // through verbatim, and so must ant's (the header value is part of
    // the compared API surface). `HeaderValue::from_str` still rejects
    // control bytes; such names simply get no disposition header.
    let base = filename.rsplit('/').next().unwrap_or(filename);
    let escaped = base.replace('\\', "\\\\").replace('"', "\\\"");
    HeaderValue::from_str(&format!("inline; filename=\"{escaped}\"")).ok()
}

/// Go `strconv.ParseInt`/`ParseUint` error text, as bee's mapStructure
/// surfaces it in a validation `reasons` entry: a malformed number is
/// `"invalid syntax"` (`strconv.ErrSyntax` — including `-1` for a
/// uint64), an out-of-range one is `"value out of range"`
/// (`strconv.ErrRange`).
pub(crate) fn strconv_error_text(e: &std::num::ParseIntError) -> &'static str {
    use std::num::IntErrorKind;
    match e.kind() {
        IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => "value out of range",
        _ => "invalid syntax",
    }
}

/// Bee's directory redirect (`bzzDownloadHandler`): `GET
/// /bzz/<ref>/<dir>` where the manifest holds entries under `<dir>/`
/// answers `308` with `Location: /bzz/<ref>/<dir>/` (query preserved)
/// and — Go's `http.Redirect` on GET — a small HTML anchor body.
fn directory_redirect(addr: &str, path: &str, query: Option<&str>, method: &Method) -> Response {
    let mut url = format!("/bzz/{addr}/{}/", go_encode_path(path));
    if let Some(q) = query {
        url.push('?');
        url.push_str(q);
    }
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::PERMANENT_REDIRECT;
    if let Ok(v) = HeaderValue::from_str(&url) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    // Go writes the anchor body (and its content-type) only for GET —
    // HEAD gets the bare status + Location.
    if method == Method::GET {
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        let body = format!(
            "<a href=\"{}\">Permanent Redirect</a>.\n",
            html_escape_go(&url)
        );
        *resp.body_mut() = Body::from(body);
    }
    resp
}

/// Go `net/url` path escaping (`shouldEscape` with `encodePath`):
/// alphanumerics, `-_.~` and the path sub-delims `$&+,/:;=@` stay
/// literal; everything else (including `?`, spaces, quotes and all
/// non-ASCII bytes) is percent-encoded uppercase. Bee's redirect
/// Location is produced by exactly this encoding (`u.String()` after
/// mutating `u.Path` drops the raw path and re-encodes).
fn go_encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for &b in path.as_bytes() {
        let literal = b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'-' | b'_'
                    | b'.'
                    | b'~'
                    | b'$'
                    | b'&'
                    | b'+'
                    | b','
                    | b'/'
                    | b':'
                    | b';'
                    | b'='
                    | b'@'
            );
        if literal {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            write!(out, "%{b:02X}").expect("writing to String");
        }
    }
    out
}

/// Go's `htmlEscape` (net/http): the five entities `http.Redirect`
/// substitutes into the anchor body.
fn html_escape_go(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

fn sniff_content_type(bytes: &[u8], filename: Option<&str>) -> &'static str {
    if let Some(name) = filename {
        if let Some(content_type) = content_type_from_extension(name) {
            return content_type;
        }
    }
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        return "image/png";
    }
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        return "image/jpeg";
    }
    if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        return "image/gif";
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WAVE" {
        return "audio/wav";
    }
    if bytes.len() >= 12 && &bytes[..4] == b"RIFF" && &bytes[8..12] == b"WEBP" {
        return "image/webp";
    }
    if bytes.starts_with(b"%PDF-") {
        return "application/pdf";
    }
    if bytes.starts_with(b"ID3")
        || bytes.starts_with(&[0xff, 0xfb])
        || bytes.starts_with(&[0xff, 0xf3])
    {
        return "audio/mpeg";
    }
    if bytes.starts_with(b"OggS") {
        return "application/ogg";
    }
    if bytes.len() >= 12 && &bytes[4..8] == b"ftyp" {
        return "video/mp4";
    }
    "application/octet-stream"
}

/// MIME type by file extension, mirroring what bee sees from Go's
/// `mime.TypeByExtension`: the complete Go ≥ 1.25 built-in table (the
/// Chromium-derived list, including the surprising `.webm → audio/webm`
/// and `.ico → image/vnd.microsoft.icon`), which takes precedence over
/// `/etc/mime.types`, plus the handful of web-relevant extensions
/// (fonts, `.m4v`) that stock Linux `mime.types` files resolve
/// identically everywhere. A file with *no* extension (or an unknown
/// one) gets `None`, and bee stores an **empty** Content-Type for it —
/// do not substitute `application/octet-stream` here (that forks the
/// manifest reference from bee's; caught by the manifest fuzz).
fn content_type_from_extension(filename: &str) -> Option<&'static str> {
    // Go's `filepath.Ext`: the suffix after the last '.' in the last
    // path element; a dotless name has no extension (`rsplit('.')`
    // would wrongly treat the whole name as one).
    let basename = filename.rsplit('/').next().unwrap_or(filename);
    let dot = basename.rfind('.')?;
    let ext = basename[dot + 1..].to_ascii_lowercase();
    match ext.as_str() {
        // --- Go's builtinTypesLower, verbatim ---
        "ai" | "eps" | "ps" => Some("application/postscript"),
        "apk" => Some("application/vnd.android.package-archive"),
        "apng" => Some("image/apng"),
        "avif" => Some("image/avif"),
        "bin" | "com" | "exe" => Some("application/octet-stream"),
        "bmp" => Some("image/bmp"),
        "css" => Some("text/css; charset=utf-8"),
        "csv" => Some("text/csv; charset=utf-8"),
        "doc" => Some("application/msword"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "ehtml" | "htm" | "html" | "shtml" => Some("text/html; charset=utf-8"),
        "eml" => Some("message/rfc822"),
        "flac" => Some("audio/flac"),
        "gif" => Some("image/gif"),
        "gz" => Some("application/gzip"),
        "ico" => Some("image/vnd.microsoft.icon"),
        "ics" => Some("text/calendar; charset=utf-8"),
        "jfif" | "jpeg" | "jpg" | "pjp" | "pjpeg" => Some("image/jpeg"),
        "js" | "mjs" => Some("text/javascript; charset=utf-8"),
        "json" => Some("application/json"),
        "m4a" => Some("audio/mp4"),
        "mp3" => Some("audio/mpeg"),
        // `.m4v` is a /etc/mime.types extra (identical across
        // distros); Go's builtin table only lists `.mp4`.
        "mp4" | "m4v" => Some("video/mp4"),
        "oga" | "ogg" | "opus" => Some("audio/ogg"),
        "ogv" => Some("video/ogg"),
        "pdf" => Some("application/pdf"),
        "png" => Some("image/png"),
        "ppt" => Some("application/vnd.ms-powerpoint"),
        "pptx" => Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        "rdf" => Some("application/rdf+xml"),
        "rtf" => Some("application/rtf"),
        "svg" => Some("image/svg+xml"),
        "text" | "txt" => Some("text/plain; charset=utf-8"),
        "tif" | "tiff" => Some("image/tiff"),
        "vtt" => Some("text/vtt; charset=utf-8"),
        "wasm" => Some("application/wasm"),
        "wav" => Some("audio/wav"),
        "webm" => Some("audio/webm"),
        "webp" => Some("image/webp"),
        "xbl" | "xml" | "xsl" => Some("text/xml; charset=utf-8"),
        "xbm" => Some("image/x-xbitmap"),
        "xht" | "xhtml" => Some("application/xhtml+xml"),
        "xls" => Some("application/vnd.ms-excel"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "zip" => Some("application/zip"),
        // --- stable /etc/mime.types extras (identical across distros) ---
        "woff" => Some("font/woff"),
        "woff2" => Some("font/woff2"),
        _ => None,
    }
}

/// Outcome of parsing a `Range` header with **Go `net/http`
/// semantics** — bee serves every download through
/// `http.ServeContent`, so its Range behaviour (including the odd
/// corners) is exactly Go's `parseRange` + `serveContent`:
///
/// * no header, an all-empty range set (`bytes=`), a range set whose
///   summed length exceeds the resource, or any range against an
///   empty resource → serve the **full body, 200**;
/// * a syntactically bad header (wrong unit, missing `-`, non-numeric
///   bounds, `start > end`, negative suffix) → **416** with the
///   text/plain body `invalid range` and *no* `Content-Range`;
/// * only past-the-end ranges → **416** with `Content-Range:
///   bytes */{total}` and the body `invalid range: failed to overlap`;
/// * exactly one satisfiable range → **206** with `Content-Range`;
/// * several satisfiable ranges → **206** `multipart/byteranges`.
#[derive(Debug, PartialEq, Eq)]
enum GoRanges {
    Full,
    /// `(start, length)`. `length` can be `0` (Go emits a zero-length
    /// 206 for `bytes=-0`).
    Single {
        start: u64,
        length: u64,
    },
    Multi(Vec<(u64, u64)>),
    NoOverlap,
    Invalid,
}

/// Go `textproto.TrimString`: strip leading/trailing spaces and tabs.
fn go_trim(s: &str) -> &str {
    s.trim_matches([' ', '\t'])
}

/// Mirror of Go's `net/http` `parseRange` plus the range-set decisions
/// `serveContent` makes on its result (empty-file quirk, sum-overflow
/// fallback). See [`GoRanges`].
fn parse_go_ranges(raw: Option<&str>, total: u64) -> GoRanges {
    let Some(s) = raw else {
        return GoRanges::Full;
    };
    let Some(rest) = s.strip_prefix("bytes=") else {
        return GoRanges::Invalid;
    };
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    let mut no_overlap = false;
    for ra in rest.split(',') {
        let ra = go_trim(ra);
        if ra.is_empty() {
            continue;
        }
        let Some((start_s, end_s)) = ra.split_once('-') else {
            return GoRanges::Invalid;
        };
        let (start_s, end_s) = (go_trim(start_s), go_trim(end_s));
        if start_s.is_empty() {
            // Suffix range `-N`: the last N bytes. Go rejects an empty
            // or negative suffix ("invalid range").
            if end_s.is_empty() || end_s.starts_with('-') {
                return GoRanges::Invalid;
            }
            let Ok(n) = end_s.parse::<i64>() else {
                return GoRanges::Invalid;
            };
            if n < 0 {
                return GoRanges::Invalid;
            }
            let n = (n as u64).min(total);
            ranges.push((total - n, n));
        } else {
            let Ok(start) = start_s.parse::<i64>() else {
                return GoRanges::Invalid;
            };
            if start < 0 {
                return GoRanges::Invalid;
            }
            let start = start as u64;
            if start >= total {
                // Starts after the end: doesn't overlap; Go keeps
                // scanning the rest of the set.
                no_overlap = true;
                continue;
            }
            let length = if end_s.is_empty() {
                total - start
            } else {
                let Ok(end) = end_s.parse::<i64>() else {
                    return GoRanges::Invalid;
                };
                if end < 0 || start as i64 > end {
                    return GoRanges::Invalid;
                }
                let end = (end as u64).min(total.saturating_sub(1));
                end - start + 1
            };
            ranges.push((start, length));
        }
    }
    if no_overlap && ranges.is_empty() {
        // `serveContent`: an empty resource ignores the Range header
        // entirely rather than 416ing.
        if total == 0 {
            return GoRanges::Full;
        }
        return GoRanges::NoOverlap;
    }
    let sum: u64 = ranges.iter().map(|r| r.1).sum();
    if sum > total {
        // Go treats an over-committing range set as an attack / dumb
        // client and serves the whole body.
        return GoRanges::Full;
    }
    match ranges.len() {
        0 => GoRanges::Full,
        1 => GoRanges::Single {
            start: ranges[0].0,
            length: ranges[0].1,
        },
        _ => GoRanges::Multi(ranges),
    }
}

/// Go `http.Error` shape for the two 416s `ServeContent` produces:
/// `text/plain; charset=utf-8` + `X-Content-Type-Options: nosniff`,
/// body = message + `\n`. `total = Some(_)` is the no-overlap flavour,
/// which additionally carries `Content-Range: bytes */{total}`.
fn go_range_not_satisfiable(total: Option<u64>, filename: Option<&str>) -> Response {
    let msg = match total {
        Some(_) => "invalid range: failed to overlap",
        None => "invalid range",
    };
    let mut resp = Response::new(Body::from(format!("{msg}\n")));
    *resp.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    h.insert(
        HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    if let Some(t) = total {
        if let Ok(v) = HeaderValue::from_str(&format!("bytes */{t}")) {
            h.insert(header::CONTENT_RANGE, v);
        }
    }
    // Bee sets Content-Disposition (from the manifest filename) before
    // ServeContent runs; Go's serveError only strips Cache-Control /
    // Content-Encoding / Etag / Last-Modified, so the disposition
    // survives on bee's /bzz 416s. (/bytes never has one.)
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            h.insert(header::CONTENT_DISPOSITION, v);
        }
    }
    // Keep the router's charset middleware away from the text body.
    resp.extensions_mut().insert(VerbatimContentType);
    resp
}

/// `Content-Range: bytes {start}-{end}/{total}` for a `(start, length)`
/// range. Formats through i128 so Go's zero-length quirk
/// (`bytes N-(N-1)/N` for `bytes=-0`) renders instead of underflowing.
fn go_content_range(start: u64, length: u64, total: u64) -> String {
    format!(
        "bytes {start}-{}/{total}",
        i128::from(start) + i128::from(length) - 1
    )
}

/// Random multipart boundary, shaped like Go's
/// `multipart.Writer.Boundary()`: 60 hex characters. Sourced from a
/// keccak over process-unique state — boundaries need uniqueness, not
/// cryptographic unpredictability.
fn random_boundary() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut seed = Vec::with_capacity(32);
    seed.extend_from_slice(&std::process::id().to_le_bytes());
    seed.extend_from_slice(&COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    seed.extend_from_slice(&now.to_le_bytes());
    hex::encode(&ant_crypto::keccak256(&seed)[..30])
}

/// Assemble a `multipart/byteranges` 206 exactly the way Go's
/// `serveContent` + `mime/multipart.Writer` do: each part opens with
/// `--boundary`, carries `Content-Range` then `Content-Type` (Go sorts
/// the MIME header keys), and the body closes with `--boundary--`.
/// `full_body` is the complete resource; parts are sliced from it
/// (Go seeks the underlying reader per range).
fn multipart_byteranges_response(
    ranges: &[(u64, u64)],
    total: u64,
    content_type: &str,
    full_body: &[u8],
    filename: Option<&str>,
) -> Response {
    let boundary = random_boundary();
    let mut body: Vec<u8> = Vec::new();
    for (i, &(start, length)) in ranges.iter().enumerate() {
        if i == 0 {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        } else {
            body.extend_from_slice(format!("\r\n--{boundary}\r\n").as_bytes());
        }
        body.extend_from_slice(
            format!(
                "Content-Range: {}\r\nContent-Type: {content_type}\r\n\r\n",
                go_content_range(start, length, total)
            )
            .as_bytes(),
        );
        let s = usize::try_from(start)
            .unwrap_or(usize::MAX)
            .min(full_body.len());
        let e = usize::try_from(start.saturating_add(length))
            .unwrap_or(usize::MAX)
            .min(full_body.len());
        body.extend_from_slice(&full_body[s..e]);
    }
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let len = body.len();
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    resp.extensions_mut().insert(VerbatimContentType);
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&format!("multipart/byteranges; boundary={boundary}")) {
        h.insert(header::CONTENT_TYPE, v);
    }
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Some(name) = filename {
        if let Some(v) = content_disposition(name) {
            h.insert(header::CONTENT_DISPOSITION, v);
        }
    }
    set_immutable_cache_headers(&mut resp);
    resp
}

/// Zero-length single range (Go's `bytes=-0`): a 206 with an empty
/// body and the quirky `Content-Range: bytes N-(N-1)/N`.
fn zero_length_partial_response(start: u64, total: u64, content_type: &str) -> Response {
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
    resp.extensions_mut().insert(VerbatimContentType);
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(content_type) {
        h.insert(header::CONTENT_TYPE, v);
    }
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(0u64));
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(v) = HeaderValue::from_str(&go_content_range(start, 0, total)) {
        h.insert(header::CONTENT_RANGE, v);
    }
    set_immutable_cache_headers(&mut resp);
    resp
}

pub(crate) fn node_unavailable() -> Response {
    json_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "node loop is no longer accepting commands",
    )
}

pub(crate) fn json_response<T: Serialize>(status: StatusCode, value: &T) -> Response {
    let body = serde_json::to_vec(value).expect("serialize json response");
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = status;
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_TYPE, JSON_CONTENT_TYPE);
    resp
}

/// `swarm-feed-index` HTTP response header carrying the resolved
/// update's sequence index as bee marshals it: the 8-byte big-endian
/// `u64`, hex-encoded to 16 lowercase chars (e.g. `0000000000000002`).
/// Matches `cur.MarshalBinary()` in `bee/pkg/api/feed.go::feedGetHandler`
/// and `bee/pkg/feeds/sequence/sequence.go::index::MarshalBinary`.
const SWARM_FEED_INDEX: HeaderName = HeaderName::from_static("swarm-feed-index");

/// `swarm-feed-index-next` — the next possible update index
/// (`index + 1`), same 8-byte big-endian hex shape. bee-js uses it to
/// address the next write without rescanning the feed.
const SWARM_FEED_INDEX_NEXT: HeaderName = HeaderName::from_static("swarm-feed-index-next");

/// `swarm-feed-resolved-version` — `"v1"` or `"v2"`. We only resolve
/// the legacy v1 payload layout, so this is always `v1`.
const SWARM_FEED_RESOLVED_VERSION: HeaderName =
    HeaderName::from_static("swarm-feed-resolved-version");

/// `GET /feeds/{owner}/{topic}` — resolve a sequence feed to its latest
/// update and serve the content it points at, byte-for-byte like bee's
/// `feedGetHandler`:
///
/// - The body is the **dereferenced content** at the update's wrapped
///   reference (the joined chunk tree), served as
///   `application/octet-stream`. With `Swarm-Only-Root-Chunk: true` we
///   instead return that reference's root chunk verbatim
///   (`span(8 LE) || payload`), matching bee's only-root-chunk branch.
/// - `swarm-feed-index` carries the resolved index as 8-byte big-endian
///   hex (`cur.MarshalBinary()`); `swarm-feed-index-next` is `index + 1`;
///   `swarm-soc-signature` is the update chunk's 65-byte signature (hex);
///   `swarm-feed-resolved-version` is `v1`.
/// - `Cache-Control: no-cache` because feed contents are mutable, and an
///   `ETag` of the resolved content reference is set so conditional
///   re-fetches short-circuit.
/// - Range requests are honored (`206 Partial Content`); `HEAD` returns
///   the headers with an empty body.
pub async fn download_feed(
    State(handle): State<GatewayHandle>,
    Path((owner_hex, topic_hex)): Path<(String, String)>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    method: Method,
    headers: HeaderMap,
) -> Response {
    let (owner, topic) = match parse_owner_pair(&owner_hex, "topic", &topic_hex) {
        Ok(pair) => pair,
        Err(resp) => return resp,
    };

    // Query params, validated bee-shaped (`feedGetHandler`'s
    // mapStructure over `{At int64; After uint64}`):
    //
    // - `at` must parse as an int64 but is otherwise IGNORED — bee
    //   defaults it to time.Now() and passes it to the sequence
    //   finder, whose `get` never reads it (it's an epoch-feed hint,
    //   and the handler hardcodes the Sequence lookup anyway).
    // - `after` anchors the finder at that index (absent ⇒ 404 "no
    //   update found", even when earlier updates exist).
    // - `type` is ignored entirely — bee never parses it here, so even
    //   `?type=epoch` runs the sequence path.
    //
    // Parse failures collect into bee's `reasons` array with Go's
    // strconv error text ("invalid syntax" / "value out of range").
    let mut reasons: Vec<Reason> = Vec::new();
    if let Some(raw) = query.get("at") {
        if let Err(e) = raw.parse::<i64>() {
            reasons.push(Reason::new("at", strconv_error_text(&e)));
        }
    }
    let mut after = 0u64;
    if let Some(raw) = query.get("after") {
        match raw.parse::<u64>() {
            Ok(v) => after = v,
            Err(e) => reasons.push(Reason::new("after", strconv_error_text(&e))),
        }
    }
    if !reasons.is_empty() {
        return params_error(ParamKind::Query, reasons);
    }
    // Header struct is validated after the queries (bee's handler
    // order), and before any lookup work.
    let only_root_chunk = match only_root_chunk_header(&headers) {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    let guard = handle.activity.begin(
        GatewayRequestKind::Feed,
        short_reference(&hex::encode(topic)),
    );

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::GetFeed {
        owner,
        topic,
        after,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return node_unavailable();
    }
    guard.update(0, 1, 1, 0);

    let ack = match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return node_unavailable(),
        Err(_) => {
            return json_error(
                StatusCode::GATEWAY_TIMEOUT,
                format!("feed lookup timed out after {}s", timeout.as_secs()),
            );
        }
    };

    let (reference, index, signature, v2, decrypt_key) = match ack {
        ControlAck::FeedResolved {
            reference,
            index,
            signature,
            v2,
            decrypt_key,
        } => (reference, index, signature, v2, decrypt_key),
        ControlAck::FeedNotFound => {
            // Bee's KLUDGE branch for a never-updated feed:
            // `404 "no update found"`.
            return json_error(StatusCode::NOT_FOUND, "no update found");
        }
        ControlAck::NotReady { message } => {
            return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
        }
        ControlAck::Error { message } => {
            // Detailed retrieval errors (peer addresses, internal
            // failure modes) belong in the operator log, not in the
            // public response body.
            warn!(target: "ant_gateway", %message, "feed lookup failed");
            // A legacy update pointing at an unretrievable chunk is
            // bee's `404 "wrapped chunk cannot be retrieved"`
            // (`feedGetHandler` on a failed `resolveFeed`).
            if message.contains("feed pointing to the wrapped chunk not found") {
                return json_error(StatusCode::NOT_FOUND, "wrapped chunk cannot be retrieved");
            }
            return json_error(StatusCode::BAD_GATEWAY, "feed lookup failed");
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from GetFeed");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
        }
    };

    let head_only = method == Method::HEAD;

    // Encrypted feed content (bee's 80-byte v1 payload): the node joins
    // and decrypts `reference ‖ key` into a buffer (the payload is small).
    // We serve it in one shot — applying any Range by slicing — rather
    // than through the streaming joiner, which only handles plaintext.
    if let Some(key) = decrypt_key {
        let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(1);
        let cmd = ControlCommand::GetBytesEncrypted {
            reference,
            key,
            bypass_cache: false,
            max_bytes: None,
            ack: ack_tx,
        };
        if handle.commands.send(cmd).await.is_err() {
            return node_unavailable();
        }
        let ack = match tokio::time::timeout(timeout, ack_rx.recv()).await {
            Ok(Some(ack)) => ack,
            Ok(None) => return node_unavailable(),
            Err(_) => {
                return json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                );
            }
        };
        let data = match ack {
            ControlAck::Bytes { data } => data,
            ControlAck::NotReady { message } => {
                return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
            }
            ControlAck::Error { message } => {
                warn!(target: "ant_gateway", %message, "encrypted feed content fetch failed");
                return json_error(StatusCode::BAD_GATEWAY, "feed content cannot be retrieved");
            }
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from GetBytesEncrypted");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
            }
        };
        let total = data.len() as u64;
        guard.update(1, 1, 0, total);

        let raw_range = headers
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let parsed = if head_only {
            GoRanges::Full
        } else {
            parse_go_ranges(raw_range.as_deref(), total)
        };
        let parsed_range = match parsed {
            GoRanges::Invalid => return go_range_not_satisfiable(None, None),
            GoRanges::NoOverlap => return go_range_not_satisfiable(Some(total), None),
            GoRanges::Full => None,
            GoRanges::Single { start, length } => {
                if length == 0 {
                    let mut resp =
                        zero_length_partial_response(start, total, "application/octet-stream");
                    apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
                    return resp;
                }
                Some((start, start + length - 1))
            }
            GoRanges::Multi(ranges) => {
                let mut resp = multipart_byteranges_response(
                    &ranges,
                    total,
                    "application/octet-stream",
                    &data,
                    None,
                );
                apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
                return resp;
            }
        };

        let mut resp = match parsed_range {
            None => {
                let len = data.len();
                let body = if head_only {
                    Body::empty()
                } else {
                    Body::from(data)
                };
                let mut resp = Response::new(body);
                let _ = resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                let _ = resp
                    .headers_mut()
                    .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
                // Bee's feed content goes through http.ServeContent →
                // `Accept-Ranges: bytes`.
                let _ = resp
                    .headers_mut()
                    .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                resp
            }
            Some((start, end)) => {
                let slice = if head_only {
                    Vec::new()
                } else {
                    data[start as usize..=end as usize].to_vec()
                };
                let body_len = end - start + 1;
                let body = if head_only {
                    Body::empty()
                } else {
                    Body::from(slice)
                };
                let mut resp = Response::new(body);
                *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
                let _ = resp.headers_mut().insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/octet-stream"),
                );
                let _ = resp
                    .headers_mut()
                    .insert(header::CONTENT_LENGTH, HeaderValue::from(body_len));
                let _ = resp
                    .headers_mut()
                    .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                if let Ok(v) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")) {
                    resp.headers_mut().insert(header::CONTENT_RANGE, v);
                }
                resp
            }
        };
        apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
        return resp;
    }

    // `Swarm-Only-Root-Chunk: true` — return the resolved reference's
    // root chunk verbatim (`span(8 LE) || payload`) without joining the
    // whole tree. Bee writes `wc.Data()` here; we fetch the raw chunk
    // at `reference` via the same path `/chunks/{addr}` uses.
    if only_root_chunk {
        let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
        // The resolved root chunk is expected to exist: no fast-miss
        // fan-out (issue #146).
        let cmd = ControlCommand::GetChunkRaw {
            reference,
            fast_miss: false,
            ack: ack_tx,
        };
        if handle.commands.send(cmd).await.is_err() {
            return node_unavailable();
        }
        let ack = match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(ack)) => ack,
            Ok(Err(_)) => return node_unavailable(),
            Err(_) => {
                return json_error(
                    StatusCode::GATEWAY_TIMEOUT,
                    format!("retrieval timed out after {}s", timeout.as_secs()),
                );
            }
        };
        let data = match ack {
            ControlAck::Bytes { data } => data,
            ControlAck::NotReady { message } => {
                return json_error(StatusCode::SERVICE_UNAVAILABLE, message);
            }
            ControlAck::NotFound { message } | ControlAck::Error { message } => {
                warn!(target: "ant_gateway", %message, "feed root chunk fetch failed");
                return json_error(StatusCode::NOT_FOUND, "wrapped chunk cannot be retrieved");
            }
            other => {
                warn!(target: "ant_gateway", ?other, "unexpected ack from GetChunkRaw (feed)");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack");
            }
        };
        guard.update(1, 1, 0, data.len() as u64);
        let len = data.len();
        let body = if head_only {
            Body::empty()
        } else {
            Body::from(data)
        };
        let mut resp = Response::new(body);
        let _ = resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let _ = resp
            .headers_mut()
            .insert(header::CONTENT_LENGTH, HeaderValue::from(len));
        apply_feed_headers(&mut resp, reference, index, &signature, v2, false);
        return resp;
    }

    // Default path: dereference `reference` and stream its content, the
    // same join `/bytes/{addr}` performs. Reuse the streaming dispatch
    // (with Range support) and then overlay the bee feed headers and a
    // forced `application/octet-stream` content type.
    let raw_range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let stream_timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);

    let mut started = match dispatch_stream_bytes(
        &handle,
        reference,
        stream_timeout,
        None,
        head_only,
        Some(guard),
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };

    let total = started.total_bytes;
    let parsed = if head_only {
        GoRanges::Full
    } else {
        parse_go_ranges(raw_range.as_deref(), total)
    };
    let parsed_range = match parsed {
        GoRanges::Invalid => {
            started.cancel();
            return go_range_not_satisfiable(None, None);
        }
        GoRanges::NoOverlap => {
            started.cancel();
            return go_range_not_satisfiable(Some(total), None);
        }
        GoRanges::Full => None,
        GoRanges::Single { start, length } => {
            if length == 0 {
                started.cancel();
                let mut resp =
                    zero_length_partial_response(start, total, "application/octet-stream");
                apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
                return resp;
            }
            Some((start, start + length - 1))
        }
        GoRanges::Multi(ranges) => {
            let full = match started.collect_body().await {
                Ok(b) => b,
                Err(message) => return map_retrieval_error(message),
            };
            let mut resp = multipart_byteranges_response(
                &ranges,
                total,
                "application/octet-stream",
                &full,
                None,
            );
            apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
            return resp;
        }
    };

    if parsed_range.is_none() {
        let mut resp = streaming_response(
            head_only,
            total,
            started.into_body(head_only),
            "application/octet-stream",
            None,
        );
        apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
        return resp;
    }

    let carry_guard = started.take_activity_guard();
    started.cancel();
    let (start, end) = parsed_range.expect("parsed_range was just checked");
    let stream_range = StreamRange {
        start,
        end_inclusive: end,
    };
    let ranged = match dispatch_stream_bytes(
        &handle,
        reference,
        stream_timeout,
        Some(stream_range),
        head_only,
        carry_guard,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => return e,
    };
    let sniffable_total = ranged.total_bytes;
    let body_len = end - start + 1;
    let mut resp = partial_content_response(
        head_only,
        sniffable_total,
        start,
        end,
        body_len,
        ranged.into_body(head_only),
        "application/octet-stream",
        None,
    );
    apply_feed_headers(&mut resp, reference, index, &signature, v2, true);
    resp
}

/// Overlay the bee-shaped feed response headers onto a response built by
/// the shared byte-streaming helpers. Sets `swarm-feed-index` /
/// `swarm-feed-index-next` (8-byte big-endian hex of `index` and
/// `index + 1`), `swarm-soc-signature` (hex of the 65-byte signature),
/// `swarm-feed-resolved-version` (`v1` or `v2` per `v2`), an `ETag` of
/// the resolved content reference, and exposes the feed headers via
/// `Access-Control-Expose-Headers`. Forces `Cache-Control: no-cache`
/// because feed contents are mutable (overriding the immutable cache the
/// byte-stream helpers stamp).
fn apply_feed_headers(
    resp: &mut Response,
    reference: [u8; 32],
    index: u64,
    signature: &[u8; 65],
    v2: bool,
    etag: bool,
) {
    let h = resp.headers_mut();
    if let Ok(v) = HeaderValue::from_str(&hex::encode(index.to_be_bytes())) {
        h.insert(SWARM_FEED_INDEX, v);
    }
    if let Ok(v) = HeaderValue::from_str(&hex::encode(index.saturating_add(1).to_be_bytes())) {
        h.insert(SWARM_FEED_INDEX_NEXT, v);
    }
    if let Ok(v) = HeaderValue::from_str(&hex::encode(signature)) {
        h.insert(SWARM_SOC_SIGNATURE, v);
    }
    h.insert(
        SWARM_FEED_RESOLVED_VERSION,
        HeaderValue::from_static(if v2 { "v2" } else { "v1" }),
    );
    if etag {
        if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", hex::encode(reference.as_ref()))) {
            h.insert(header::ETAG, v);
        }
    }
    h.insert(header::CACHE_CONTROL, MUTABLE_CACHE_CONTROL);
    for name in [SWARM_FEED_INDEX, SWARM_FEED_INDEX_NEXT, SWARM_SOC_SIGNATURE] {
        if let Ok(v) = HeaderValue::from_str(name.as_str()) {
            h.append(header::ACCESS_CONTROL_EXPOSE_HEADERS, v);
        }
    }
}

/// Parse a bee-style boolean request header (`Swarm-Only-Root-Chunk`,
/// etc.). Accepts the values Go's `strconv.ParseBool` treats as true
/// that clients actually send: `1`, `t`, `true` (any case).
/// Go's `strconv.ParseBool` — the exact forms bee's mapStructure
/// accepts for a `bool` header. Anything else (including mixed case
/// like `tRuE`) is a syntax error.
pub(crate) fn go_parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// The `swarm-only-root-chunk` header, validated the way bee's
/// mapStructure does (`OnlyRootChunk bool` in the soc/feed GET
/// handlers): absent ⇒ `false`; present but not a Go bool ⇒
/// `400 invalid header params` with the map-tag-cased field name.
#[allow(clippy::result_large_err)]
fn only_root_chunk_header(headers: &HeaderMap) -> Result<bool, Response> {
    let Some(v) = headers.get(&SWARM_ONLY_ROOT_CHUNK) else {
        return Ok(false);
    };
    v.to_str().ok().and_then(go_parse_bool).ok_or_else(|| {
        params_error(
            ParamKind::Header,
            vec![Reason::new("Swarm-Only-Root-Chunk", "invalid syntax")],
        )
    })
}

/// Tiny `humantime`-shaped duration parser so we don't pull in a whole
/// crate for one optional header. Accepts plain integers (interpreted
/// as seconds, matching bee) and `<n>ms`, `<n>s`, `<n>m` suffixes.
mod humantime {
    use std::time::Duration;

    pub fn parse_duration_lite(s: &str) -> Option<Duration> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if let Ok(secs) = s.parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }
        for (suffix, factor_ms) in [("ms", 1u64), ("s", 1_000), ("m", 60_000), ("h", 3_600_000)] {
            if let Some(num) = s.strip_suffix(suffix) {
                if let Ok(n) = num.trim().parse::<u64>() {
                    return Some(Duration::from_millis(n * factor_ms));
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every write endpoint shares one status mapping, and a saturated
    /// batch is bee's `402 "batch is overissued"` — not a 502 a client
    /// will retry forever. The saturation strings are the ones the node
    /// actually emits: `PushChunk`/`PushSoc`'s immutable-batch refusal
    /// and `ant_postage::PostageError::BucketFull`'s `Display`.
    #[test]
    fn a_saturated_batch_is_payment_required_not_a_bad_gateway() {
        for message in [
            "batch 0xab… saturated: collision bucket full at depth 20 on an immutable batch — stamping would evict an existing chunk; buy or dilute to a larger batch",
            "stamp issue failed: bucket full",
            "batch is overissued",
        ] {
            assert_eq!(
                upload_error_status(message),
                StatusCode::PAYMENT_REQUIRED,
                "{message}",
            );
        }
    }

    #[test]
    fn the_other_upload_error_classes_keep_their_status() {
        assert_eq!(
            upload_error_status("uploads not configured: node cannot stamp"),
            StatusCode::SERVICE_UNAVAILABLE,
        );
        assert_eq!(
            upload_error_status("batch 0xab not usable"),
            StatusCode::BAD_REQUEST,
        );
        assert_eq!(
            upload_error_status("rejected by 0xpeer: batch 0xab not found on-chain",),
            StatusCode::UNPROCESSABLE_ENTITY,
        );
        // Transient pushsync trouble stays retryable.
        assert_eq!(
            upload_error_status("pushsync: exhausted pushsync peers"),
            StatusCode::BAD_GATEWAY,
        );
    }

    /// Go `parseRange` parity, case by case (matches Go 1.26's
    /// `net/http` — bee serves ranges via `http.ServeContent`).
    #[test]
    fn go_range_parity() {
        use GoRanges::{Full, Invalid, Multi, NoOverlap, Single};
        // Plain single ranges.
        assert_eq!(
            parse_go_ranges(Some("bytes=0-9"), 1024),
            Single {
                start: 0,
                length: 10
            }
        );
        assert_eq!(
            parse_go_ranges(Some("bytes=10-"), 1024),
            Single {
                start: 10,
                length: 1014
            }
        );
        assert_eq!(
            parse_go_ranges(Some("bytes=-100"), 1024),
            Single {
                start: 924,
                length: 100
            }
        );
        // End clamped to size-1.
        assert_eq!(
            parse_go_ranges(Some("bytes=0-999999"), 1024),
            Single {
                start: 0,
                length: 1024
            }
        );
        // Exactly-at-end / past-end starts → noOverlap 416.
        assert_eq!(parse_go_ranges(Some("bytes=1024-"), 1024), NoOverlap);
        assert_eq!(parse_go_ranges(Some("bytes=2000-3000"), 1024), NoOverlap);
        // Malformed → Go's "invalid range" 416.
        assert_eq!(parse_go_ranges(Some("bytes=abc"), 1024), Invalid);
        assert_eq!(parse_go_ranges(Some("items=0-5"), 1024), Invalid);
        assert_eq!(parse_go_ranges(Some("bytes=5-2"), 1024), Invalid);
        assert_eq!(parse_go_ranges(Some("bytes=--5"), 1024), Invalid);
        // Multi-range → multipart; a trailing comma is skipped.
        assert_eq!(
            parse_go_ranges(Some("bytes=0-1,5-9"), 1024),
            Multi(vec![(0, 2), (5, 5)])
        );
        assert_eq!(
            parse_go_ranges(Some("bytes=0-1,"), 1024),
            Single {
                start: 0,
                length: 2
            }
        );
        // Over-committing set is ignored (Go serves the full body).
        assert_eq!(parse_go_ranges(Some("bytes=0-,0-"), 10), Full);
        // Empty resource ignores any range.
        assert_eq!(parse_go_ranges(Some("bytes=5-9"), 0), Full);
        // Suffix -0 is Go's zero-length 206.
        assert_eq!(
            parse_go_ranges(Some("bytes=-0"), 100),
            Single {
                start: 100,
                length: 0
            }
        );
        assert_eq!(go_content_range(100, 0, 100), "bytes 100-99/100");
        // No header at all.
        assert_eq!(parse_go_ranges(None, 100), Full);
    }

    #[test]
    fn parses_decimal_seconds_for_timeout() {
        assert_eq!(
            humantime::parse_duration_lite("5"),
            Some(Duration::from_secs(5))
        );
        assert_eq!(
            humantime::parse_duration_lite("250ms"),
            Some(Duration::from_millis(250))
        );
    }
}
