//! `GET /chunks/stream`: bee's WebSocket chunk stream
//! (`bee/pkg/api/chunk_stream.go`), in two flavours on the same path.
//!
//! # `bee-chunk-stream-v1` (bee API `9.0.0`)
//!
//! Spoken when the client offers the `bee-chunk-stream-v1` subprotocol
//! in `Sec-WebSocket-Protocol`; the node then selects it, as bee does
//! (<https://github.com/ethersphere/bee/pull/5618>). One protobuf
//! message (bee `pkg/api/pb/chunkstream.proto`, mirrored in [`pb`]) per
//! binary WebSocket message:
//!
//! * A `Request` is a `GetRequest` (address, optional `CacheOption`) or
//!   a `PutRequest` (address, data, optional pre-signed `Stamp`,
//!   optional `BatchId`). Every request gets exactly one `GetResponse`
//!   / `PutResponse`, in completion order, echoing the request's
//!   address. A failing request is answered with its own status and
//!   leaves the socket open.
//! * Get goes through the `GET /chunks/{addr}` lookup
//!   ([`fetch_raw_chunk`]): `Swarm-Cache` / `?cache=` set the
//!   connection default and `CacheOption` overrides it per request,
//!   with ant's `swarm-cache: false` meaning (no cache read *and* no
//!   write-back, <https://github.com/freedom-hq/ant/issues/80>). A
//!   miss is `NOT_FOUND`, a timeout or other failure `ERROR`. A
//!   `NotReady` ack (`GET /chunks`' 503) would be `UNAVAILABLE`, but
//!   `GetChunkRaw`'s fetcher has no accounting attached, so its pool is
//!   never reported starved and the node doesn't send one for a Get
//!   today (see `pre_stream_failure_ack` in ant-p2p): a thin miss comes
//!   back `NOT_FOUND` (or `ERROR` with no peer to ask). The arm is kept so the mapping matches
//!   `GET /chunks` if that changes.
//! * Put rebuilds the chunk from `Data` and takes it only if it
//!   produces the requested address as a CAC or as a validly signed
//!   SOC; the address settles which. It is then pushed through the
//!   `POST /chunks` / `POST /soc` push ([`bee_push_command`]), and `OK`
//!   is only sent once the push is done. Stamping precedence: the
//!   request's pre-signed `Stamp`, then its `BatchId`, then the
//!   connection's `Swarm-Postage-Batch-Id`, which pins the connection
//!   to that batch (a different `BatchId` or stamp batch is
//!   `BAD_REQUEST`). A node-stamped chunk's batch is looked up in the
//!   node's postage list on every chunk.
//! * Handshake: `Swarm-Tag` / `?swarm-tag=` is `400` (uploads are
//!   always direct); a bad connection batch is `404` (unknown), `422`
//!   (not usable) or `500` (lookup failed).
//! * Closing: `1003` for a non-binary message, an undecodable request
//!   or one without a body, `1009` for a message over 8 KiB (one over
//!   64 KiB is refused by the WebSocket layer itself, with a close
//!   frame that carries no code). 15 minutes
//!   without any client frame drops the connection; the node pings
//!   every 60 s so a client only waiting for replies stays connected.
//!   A ping is written ahead of queued replies, so this holds for a
//!   client slowly draining a full reply queue too.
//! * Backpressure: Gets and Puts each have a 1024-deep queue served by
//!   8 workers; a full queue is answered `BUSY`. Replies go through a
//!   bounded channel to one writer, so a client that stops reading
//!   stalls the workers, then the queues, then the reader.
//!
//! Differences from bee, by design or for lack of chain state: a
//! pre-signed stamp is checked the way `POST /chunks` checks one (its
//! signature must recover over the chunk address, and its batch must
//! match a pinned connection batch), not against the batch owner,
//! depth and bucket the way bee's `Stamp.Valid` does, because a light
//! node has no batch store to read those from per chunk. A node-stamped
//! batch that storers rejected answers `batch not usable yet or does
//! not exist`, one the node does not hold `batch with id not found`.
//! A node shutting down drops the socket instead of sending bee's
//! `1001 "node shutting down"`.
//!
//! # Legacy upload-only framing
//!
//! A client that does **not** offer `bee-chunk-stream-v1` gets the
//! upload-only stream bee served before that PR, unchanged. Bee dropped it
//! (a raw chunk there is now closed with `1003`); ant keeps it, because
//! ant's own clients and older tooling still send raw chunks. Protocol,
//! mirrored from bee's old stream exactly:
//!
//! * The upgrade request carries `swarm-postage-batch-id` (optional)
//!   and `swarm-tag` (optional; also accepted as a `?swarm-tag=` query
//!   parameter because browser WebSocket clients can't set headers).
//!   An unknown tag is a pre-upgrade `404 "tag not found"`.
//! * Every **binary** message is one chunk. With a batch header the
//!   message is the bare chunk wire (`span(8 LE) ‖ payload`); without
//!   one the caller prepends a pre-signed 113-byte postage stamp
//!   (`decodeChunkWithStamp`).
//! * Each successfully stored chunk is acked with an **empty binary
//!   message** (bee's `successWsMsg = []byte{}`).
//! * Errors close the socket with a Close frame: `1003` ("invalid
//!   message") for a non-binary message, `1011` with bee's message for
//!   decode/validation/store failures ("message too small for stamp +
//!   chunk", "invalid chunk data", "batch is overissued",
//!   "chunk write error"). A message shorter than the 8-byte span is
//!   dropped by closing the connection without a Close frame, exactly
//!   like bee's bare `return`.
//!
//! Each chunk is pushed through the same [`ControlCommand::PushChunk`]
//! path `POST /chunks` uses, so stamping/pushsync semantics are
//! identical; a `swarm-tag` session has its counters advanced once per
//! acked chunk.

use std::sync::Arc;
use std::time::Duration;

use ant_control::{ControlAck, ControlCommand};
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures::{SinkExt, StreamExt};
use prost::Message as _;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinSet;
use tracing::debug;

use crate::error::{json_error, params_error, parse_hex_param, ParamKind, Reason};
use crate::handle::GatewayHandle;
use crate::retrieval::{
    batch_use, bee_push_command, fetch_raw_chunk, parse_cache_header, presigned_stamp_covers,
    BatchUse, RawChunk, CHUNK_PUSH_TIMEOUT, CHUNK_REQUEST_TIMEOUT,
};

/// Bee's `websocket.CloseUnsupportedData`.
const CLOSE_UNSUPPORTED: u16 = 1003;
/// Bee's `websocket.CloseMessageTooBig`.
const CLOSE_TOO_BIG: u16 = 1009;
/// Bee's `websocket.CloseInternalServerErr`.
const CLOSE_INTERNAL: u16 = 1011;

/// 113-byte postage stamp prefix used in header-less mode.
const STAMP_SIZE: usize = 113;
/// `span(8) ‖ payload(≤4096)`.
const MAX_CHUNK_WIRE: usize = 8 + 4096;

/// Bee's `chunkStreamSubprotocol`: offered by the client, selected by
/// the node, and the switch between the two protocols on this path.
pub const CHUNK_STREAM_SUBPROTOCOL: &str = "bee-chunk-stream-v1";

/// Bee's `maxStreamFrameSize`: the largest v1 client message. The
/// largest valid request (a full SOC put with a stamp and a batch id)
/// is under 4.5 KiB.
const MAX_STREAM_FRAME: usize = 8 * 1024;
/// The WebSocket library's own message cap on a v1 connection: above
/// [`MAX_STREAM_FRAME`], so a message between the two is read and
/// answered with bee's `1009` instead of failing the socket bare.
const MAX_WS_MESSAGE: usize = 64 * 1024;
/// Bee's `defaultStreamSubWorkers`, per queue.
const STREAM_WORKERS: usize = 8;
/// Bee's `maxStreamQueueSize`, per queue.
const STREAM_QUEUE: usize = 1024;
/// Encoded replies waiting for the writer. Small: when the client stops
/// reading, this fills first and stalls the workers.
const REPLY_QUEUE: usize = 32;
/// Bee's `streamReadTimeout`: no client frame for this long ends the
/// stream (without a close frame, bee's `1006`).
const STREAM_READ_TIMEOUT: Duration = Duration::from_mins(15);
/// Bee's `chunkDeliveryWriteDeadline`: one reply (or ping) write. A
/// client that pauses reading while its buffer drains keeps its stream.
const STREAM_WRITE_TIMEOUT: Duration = Duration::from_mins(5);
/// Bee's `WsPingPeriod`.
const STREAM_PING_PERIOD: Duration = Duration::from_mins(1);
/// Bee's `chunkStreamCloseDeadline`: queueing a close frame.
const STREAM_CLOSE_TIMEOUT: Duration = Duration::from_millis(250);

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    /// Browser fallback for the `swarm-tag` header.
    #[serde(rename = "swarm-tag")]
    swarm_tag: Option<String>,
    /// v1 only: the connection's default for caching retrieved chunks
    /// (Go `ParseBool`); `Swarm-Cache` overrides it.
    cache: Option<String>,
}

/// `GET /chunks/stream` upgrade handler.
pub async fn chunk_stream(
    State(handle): State<GatewayHandle>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    if offers_subprotocol(&headers) {
        return v1_handshake(handle, query, &headers, ws).await;
    }
    let ws = match ws {
        Ok(ws) => ws,
        Err(rejection) => return rejection.into_response(),
    };
    legacy_handshake(handle, query, &headers, ws)
}

/// Whether `Sec-WebSocket-Protocol` offers [`CHUNK_STREAM_SUBPROTOCOL`]
/// (the same comma-split, trimmed, exact match axum's `protocols`
/// selection does, so the two agree).
fn offers_subprotocol(headers: &HeaderMap) -> bool {
    headers
        .get_all(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|p| p.trim() == CHUNK_STREAM_SUBPROTOCOL)
}

/// Legacy upload-only stream's pre-upgrade checks.
fn legacy_handshake(
    handle: GatewayHandle,
    query: StreamQuery,
    headers: &HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Optional batch header: present-but-malformed is bee's
    // mapStructure 400; absent means "pre-signed stamps per message".
    let batch_id = match optional_batch_header(headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    // Tag: header first (mapStructure uint64), then the query-param
    // fallback with bee's dedicated error message.
    let tag = match tag_from_request(headers, query.swarm_tag.as_deref()) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if let Some(uid) = tag {
        if handle.tags.get(uid).is_none() {
            return json_error(StatusCode::NOT_FOUND, "tag not found");
        }
    }

    ws.on_upgrade(move |socket| handle_upload_stream(handle, socket, batch_id, tag))
}

/// v1 pre-upgrade checks, in bee's order: upgrade request, header
/// syntax, tag rejection, connection batch, `?cache=`.
async fn v1_handshake(
    handle: GatewayHandle,
    query: StreamQuery,
    headers: &HeaderMap,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let Ok(ws) = ws else {
        return json_error(StatusCode::BAD_REQUEST, "not a websocket upgrade request");
    };

    // Header syntax (bee's mapStructure over batch, cache and tag).
    let batch = match v1_batch_header(headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let cache_header = match headers.get("swarm-cache") {
        None => None,
        Some(_) => match parse_cache_header(headers) {
            // `parse_cache_header` answers "bypass the cache".
            Ok(bypass) => Some(!bypass),
            Err(resp) => return resp,
        },
    };
    let tag_header = headers.get("swarm-tag");
    if let Some(raw) = tag_header {
        if raw
            .to_str()
            .ok()
            .map(str::trim)
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return params_error(
                ParamKind::Header,
                vec![Reason::new("Swarm-Tag", "invalid syntax")],
            );
        }
    }

    // Uploads on the stream are always direct: a tag is rejected, not
    // ignored, so a client expecting deferred uploads finds out here.
    if tag_header.is_some() || query.swarm_tag.as_deref().is_some_and(|t| !t.is_empty()) {
        return json_error(
            StatusCode::BAD_REQUEST,
            "swarm-tag is not supported on the chunk stream",
        );
    }

    // The connection batch is checked up front, so a bad one fails the
    // handshake instead of every upload relying on it.
    let default_batch = match batch {
        None => None,
        Some(V1Batch::WrongLength) => {
            return json_error(StatusCode::NOT_FOUND, "batch with id not found");
        }
        Some(V1Batch::Id(id)) => match batch_use(&handle, &id).await {
            Ok(BatchUse::Usable) => Some(id),
            Ok(BatchUse::Unusable) => {
                return json_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "batch not usable yet or does not exist",
                );
            }
            Ok(BatchUse::Unknown) => {
                return json_error(StatusCode::NOT_FOUND, "batch with id not found");
            }
            Err(_) => {
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "cannot get batch");
            }
        },
    };

    let mut default_cache = true;
    if let Some(raw) = query.cache.as_deref().filter(|s| !s.is_empty()) {
        match parse_go_bool(raw) {
            Some(c) => default_cache = c,
            None => {
                return json_error(StatusCode::BAD_REQUEST, "invalid cache query parameter");
            }
        }
    }
    if let Some(c) = cache_header {
        default_cache = c;
    }

    let conn = Arc::new(V1Conn {
        handle,
        default_batch,
        default_cache,
        timing: V1Timing::BEE,
    });
    ws.protocols([CHUNK_STREAM_SUBPROTOCOL])
        .max_message_size(MAX_WS_MESSAGE)
        .max_frame_size(MAX_WS_MESSAGE)
        .on_upgrade(move |socket| run_v1(conn, socket))
}

/// A parsed v1 `Swarm-Postage-Batch-Id`.
enum V1Batch {
    Id([u8; 32]),
    /// Valid hex of the wrong size: bee's `[]byte` header hook accepts
    /// it and the batch lookup then finds no such batch.
    WrongLength,
}

#[allow(clippy::result_large_err)]
fn v1_batch_header(headers: &HeaderMap) -> Result<Option<V1Batch>, Response> {
    let Some(raw) = headers
        .get("swarm-postage-batch-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let mut reasons = Vec::new();
    match parse_hex_param::<32>("swarm-postage-batch-id", raw, &mut reasons) {
        Some(batch) => Ok(Some(V1Batch::Id(batch))),
        None if reasons
            .iter()
            .any(|r| r.error.starts_with("invalid length")) =>
        {
            Ok(Some(V1Batch::WrongLength))
        }
        None => Err(params_error(ParamKind::Header, reasons)),
    }
}

/// Go's `strconv.ParseBool`.
fn parse_go_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "true" | "TRUE" | "True" => Some(true),
        "0" | "f" | "F" | "false" | "FALSE" | "False" => Some(false),
        _ => None,
    }
}

/// Per-connection state of a v1 stream.
struct V1Conn {
    handle: GatewayHandle,
    /// `Swarm-Postage-Batch-Id`, already checked usable: the default
    /// batch, and the one every upload is pinned to.
    default_batch: Option<[u8; 32]>,
    /// Cache retrieved chunks unless a request says otherwise.
    default_cache: bool,
    timing: V1Timing,
}

/// A v1 connection's keepalive and write bounds (shortened by tests).
#[derive(Clone, Copy)]
struct V1Timing {
    /// No client frame (request, ping or pong) for this long ends the
    /// stream.
    read_timeout: Duration,
    /// One reply or ping write.
    write_timeout: Duration,
    ping_period: Duration,
}

impl V1Timing {
    const BEE: Self = Self {
        read_timeout: STREAM_READ_TIMEOUT,
        write_timeout: STREAM_WRITE_TIMEOUT,
        ping_period: STREAM_PING_PERIOD,
    };
}

/// What the writer sends besides its own pings.
enum Outgoing {
    Reply(Vec<u8>),
    Close(u16, &'static str),
}

/// Serve one v1 connection: a reader queueing requests, `STREAM_WORKERS`
/// Get and Put workers each, and one writer behind a bounded reply
/// channel. The first of a client close, a protocol violation, a read
/// timeout or a failed write ends it; requests still queued or in
/// flight then get no reply (a client resends them on a new socket).
async fn run_v1(conn: Arc<V1Conn>, socket: WebSocket) {
    let (sink, mut stream) = socket.split();
    let (out_tx, out_rx) = mpsc::channel::<Outgoing>(REPLY_QUEUE);
    let mut writer = tokio::spawn(write_replies(sink, out_rx, conn.timing));

    let (get_tx, get_rx) = mpsc::channel::<pb::GetRequest>(STREAM_QUEUE);
    let (put_tx, put_rx) = mpsc::channel::<pb::PutRequest>(STREAM_QUEUE);
    let get_rx = Arc::new(Mutex::new(get_rx));
    let put_rx = Arc::new(Mutex::new(put_rx));
    let mut workers = JoinSet::new();
    for _ in 0..STREAM_WORKERS {
        let (c, rx, out) = (conn.clone(), get_rx.clone(), out_tx.clone());
        workers.spawn(async move {
            loop {
                let Some(req) = rx.lock().await.recv().await else {
                    return;
                };
                let reply = handle_get(&c, req).await;
                if out
                    .send(Outgoing::Reply(reply.encode_to_vec()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        let (c, rx, out) = (conn.clone(), put_rx.clone(), out_tx.clone());
        workers.spawn(async move {
            loop {
                let Some(req) = rx.lock().await.recv().await else {
                    return;
                };
                let reply = handle_put(&c, req).await;
                if out
                    .send(Outgoing::Reply(reply.encode_to_vec()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
    }

    let timing = conn.timing;
    // One deadline for the whole loop, moved only by a client frame:
    // the node's own pings must not keep a silent client alive.
    let idle = tokio::time::sleep(timing.read_timeout);
    tokio::pin!(idle);
    let mut writer_done = false;
    loop {
        let msg = tokio::select! {
            // Biased so that a client frame already waiting wins over an
            // idle deadline that ran out while this reader was blocked on
            // a busy reply (below): that frame, often a pong, is the
            // client's proof it is alive.
            biased;
            // The writer only ends on a failed or timed-out write.
            _ = &mut writer => {
                writer_done = true;
                break;
            }
            msg = stream.next() => msg,
            () = &mut idle => {
                debug!(target: "ant_gateway", "chunk stream: read timeout");
                break;
            }
        };
        // Any client frame, pongs and pings included, extends it.
        idle.as_mut()
            .reset(tokio::time::Instant::now() + timing.read_timeout);
        let data = match msg {
            None | Some(Err(_) | Ok(Message::Close(_))) => break,
            // axum answers pings itself.
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Text(_))) => {
                send_close(&out_tx, CLOSE_UNSUPPORTED, "invalid message type").await;
                break;
            }
            Some(Ok(Message::Binary(data))) => data,
        };
        if data.len() > MAX_STREAM_FRAME {
            // gorilla's read limit: close 1009 with no reason text.
            send_close(&out_tx, CLOSE_TOO_BIG, "").await;
            break;
        }
        let Ok(req) = pb::Request::decode(&data[..]) else {
            send_close(&out_tx, CLOSE_UNSUPPORTED, "invalid protobuf request").await;
            break;
        };
        // A request is answered by echoing its address, so one without
        // a body has nothing to answer with.
        let busy = match req.body {
            Some(pb::request::Body::Get(get)) => match get_tx.try_send(get) {
                Ok(()) => None,
                Err(mpsc::error::TrySendError::Full(get)) => Some(pb::get_response(
                    get.address,
                    pb::Status::Busy,
                    Vec::new(),
                    "request queue full",
                )),
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            },
            Some(pb::request::Body::Put(put)) => match put_tx.try_send(put) {
                Ok(()) => None,
                Err(mpsc::error::TrySendError::Full(put)) => Some(pb::put_response(
                    put.address,
                    pb::Status::Busy,
                    "request queue full",
                )),
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            },
            None => {
                send_close(&out_tx, CLOSE_UNSUPPORTED, "missing request body").await;
                break;
            }
        };
        // The busy reply waits for the socket like any other, so a
        // client that stops reading eventually stops this reader too.
        // The idle deadline is not raced here: while this reader is
        // blocked, the client's pongs sit unread, so it can't count as
        // silent. A client that stopped reading altogether is ended by
        // the writer's write timeout instead, which drops the receiver
        // and fails this send; one that reads but sends nothing meets
        // the (by then elapsed) deadline right after the send.
        if let Some(busy) = busy {
            let sent = tokio::select! {
                sent = out_tx.send(Outgoing::Reply(busy.encode_to_vec())) => sent.is_ok(),
                _ = &mut writer => {
                    writer_done = true;
                    break;
                }
            };
            if !sent {
                break;
            }
        }
    }

    // Requests in flight get no reply; the node's own push or fetch
    // finishes or times out on its own.
    workers.abort_all();
    drop(out_tx);
    // Let the writer flush a close frame (or finish a reply), but never
    // wait on a client that stopped reading. A writer that already
    // ended must not be polled again.
    if !writer_done
        && tokio::time::timeout(Duration::from_secs(1), &mut writer)
            .await
            .is_err()
    {
        writer.abort();
    }
}

/// Queue a close frame behind the replies already queued, giving up
/// quickly when the client has stopped reading.
async fn send_close(out: &mpsc::Sender<Outgoing>, code: u16, reason: &'static str) {
    let _ = tokio::time::timeout(
        STREAM_CLOSE_TIMEOUT,
        out.send(Outgoing::Close(code, reason)),
    )
    .await;
}

/// The single writer of a v1 connection, and its pinger. Returns on a
/// failed or timed-out write, after a close frame, or once every sender
/// is gone.
///
/// Pings don't go through the reply queue: a due ping is written before
/// the next queued reply, the way bee's `WriteControl` ping goes out
/// whatever is queued. A client draining a long backlog slowly keeps the
/// queue full, and its pongs are what keep it inside the read timeout.
async fn write_replies(
    mut sink: futures::stream::SplitSink<WebSocket, Message>,
    mut out: mpsc::Receiver<Outgoing>,
    timing: V1Timing,
) {
    let mut ping = tokio::time::interval_at(
        tokio::time::Instant::now() + timing.ping_period,
        timing.ping_period,
    );
    // Ticks missed during a slow write collapse into one ping.
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let item = tokio::select! {
            biased;
            _ = ping.tick() => None,
            item = out.recv() => match item {
                Some(item) => Some(item),
                None => break,
            },
        };
        let (msg, last) = match item {
            None => (Message::Ping(Vec::new().into()), false),
            Some(Outgoing::Reply(bytes)) => (Message::Binary(bytes.into()), false),
            Some(Outgoing::Close(code, reason)) => (
                Message::Close(Some(CloseFrame {
                    code,
                    reason: reason.into(),
                })),
                true,
            ),
        };
        match tokio::time::timeout(timing.write_timeout, sink.send(msg)).await {
            Ok(Ok(())) if !last => {}
            _ => return,
        }
    }
    let _ = sink.close().await;
}

/// Answer a Get. Mirrors `GET /chunks/{addr}`'s outcome mapping,
/// including its `NotReady` → `UNAVAILABLE` arm, which the node doesn't
/// reach for `GetChunkRaw` today (no accounting on its fetcher, so no
/// starved pool).
async fn handle_get(conn: &V1Conn, req: pb::GetRequest) -> pb::Response {
    let Ok(reference) = <[u8; 32]>::try_from(&req.address[..]) else {
        return pb::get_response(
            req.address,
            pb::Status::BadRequest,
            Vec::new(),
            "invalid chunk address length",
        );
    };
    let cache = match pb::CacheOption::try_from(req.cache) {
        Ok(pb::CacheOption::Enable) => true,
        Ok(pb::CacheOption::Disable) => false,
        _ => conn.default_cache,
    };
    let (status, data, error) =
        match fetch_raw_chunk(&conn.handle, reference, !cache, CHUNK_REQUEST_TIMEOUT).await {
            RawChunk::Data(data) => (pb::Status::Ok, data, ""),
            RawChunk::NotFound => (pb::Status::NotFound, Vec::new(), "chunk not found"),
            RawChunk::NotReady(message) => {
                debug!(target: "ant_gateway", %message, "chunk stream: no usable peer");
                (
                    pb::Status::Unavailable,
                    Vec::new(),
                    "chunk could not be retrieved because no usable peer was available",
                )
            }
            RawChunk::Timeout => (pb::Status::Error, Vec::new(), "chunk retrieval timed out"),
            RawChunk::NodeGone | RawChunk::Failed | RawChunk::Unexpected => {
                (pb::Status::Error, Vec::new(), "read chunk failed")
            }
        };
    pb::get_response(req.address, status, data, error)
}

/// A chunk an upload's data rebuilds to at its requested address.
enum Rebuilt {
    Cac,
    Soc,
}

/// Bee's `chunkForAddress`: the data must produce `address` as a CAC
/// or as a validly signed SOC. Small SOC data also parses as a valid
/// CAC; only one reading produces the address, so CAC is tried first
/// and the address settles it.
fn chunk_for_address(address: &[u8; 32], data: &[u8]) -> Option<Rebuilt> {
    if (8..=MAX_CHUNK_WIRE).contains(&data.len()) {
        let span: [u8; 8] = data[..8].try_into().expect("span slice");
        if ant_crypto::bmt_hash_with_span(&span, &data[8..]).as_ref() == Some(address) {
            return Some(Rebuilt::Cac);
        }
    }
    ant_crypto::soc_valid(address, data).then_some(Rebuilt::Soc)
}

/// Answer a Put: validate, pick the stamp or batch, push, and answer
/// once the push is done.
async fn handle_put(conn: &V1Conn, req: pb::PutRequest) -> pb::Response {
    let reply = |status, error: &str| pb::put_response(req.address.clone(), status, error);
    let Ok(address) = <[u8; 32]>::try_from(&req.address[..]) else {
        return reply(pb::Status::BadRequest, "invalid chunk address length");
    };
    if req.data.len() < 8 {
        return reply(pb::Status::BadRequest, "insufficient data for chunk");
    }
    let Some(kind) = chunk_for_address(&address, &req.data) else {
        return reply(pb::Status::BadRequest, "address does not match chunk data");
    };

    let (batch_id, stamp) = match stamp_for(conn, &req, &address).await {
        Ok(pair) => pair,
        Err((status, error)) => return reply(status, error),
    };

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let soc_address = matches!(kind, Rebuilt::Soc).then_some(address);
    let cmd = bee_push_command(soc_address, req.data.clone(), batch_id, stamp, ack_tx);
    if conn.handle.commands.send(cmd).await.is_err() {
        return reply(pb::Status::Error, "chunk write error");
    }
    // Like bee, the reply waits for the node's push with no time limit
    // of its own; it comes when the push is done or has given up (bee's
    // reason: a timeout here lets a client retry a chunk whose first
    // push is still running). The wait ends with the stream, which
    // aborts this worker.
    let Ok(ack) = ack_rx.await else {
        return reply(pb::Status::Error, "chunk push failed");
    };
    match ack {
        ControlAck::ChunkUploaded { .. } => reply(pb::Status::Ok, ""),
        ControlAck::Error { message } => {
            debug!(target: "ant_gateway", %message, "chunk stream: push failed");
            let (status, error) = push_error_reply(&message);
            reply(status, error)
        }
        other => {
            debug!(target: "ant_gateway", ?other, "chunk stream: unexpected push ack");
            reply(pb::Status::Error, "chunk push failed")
        }
    }
}

/// Bee's `stampErrorReply` for the node's push errors: stamping
/// failures get bee's stamping messages, anything else is a failed push.
fn push_error_reply(message: &str) -> (pb::Status, &'static str) {
    if message.contains("not usable") {
        (
            pb::Status::BadRequest,
            "batch not usable yet or does not exist",
        )
    } else if message.contains("overissued") || message.contains("saturated") {
        (pb::Status::Error, "batch is overissued")
    } else if message.starts_with("presigned stamp") {
        (pb::Status::BadRequest, "invalid postage stamp")
    } else if message.starts_with("uploads not configured")
        || message.starts_with("stamp issue failed")
    {
        (pb::Status::Error, "failed to stamp chunk")
    } else {
        (pb::Status::Error, "chunk push failed")
    }
}

/// Bee's stamp precedence: a pre-signed stamp (its batch then ignores
/// `BatchId`), the request's `BatchId`, the connection batch. A
/// connection batch pins both paths.
async fn stamp_for(
    conn: &V1Conn,
    req: &pb::PutRequest,
    address: &[u8; 32],
) -> Result<([u8; 32], Option<Vec<u8>>), (pb::Status, &'static str)> {
    let bad = |e| (pb::Status::BadRequest, e);
    if !req.stamp.is_empty() {
        let Ok(stamp) = <[u8; STAMP_SIZE]>::try_from(&req.stamp[..]) else {
            return Err(bad("invalid postage stamp"));
        };
        let batch: [u8; 32] = stamp[..32].try_into().expect("stamp slice");
        if conn.default_batch.is_some_and(|b| b != batch) {
            return Err(bad("stamp batch differs from the connection batch"));
        }
        if !presigned_stamp_covers(&stamp, address) {
            return Err(bad("invalid postage stamp"));
        }
        return Ok((batch, Some(stamp.to_vec())));
    }

    let batch = if req.batch_id.is_empty() {
        conn.default_batch
            .ok_or(bad("missing postage stamp or batch id"))?
    } else {
        <[u8; 32]>::try_from(&req.batch_id[..]).map_err(|_| bad("invalid batch id"))?
    };
    if conn.default_batch.is_some_and(|b| b != batch) {
        return Err(bad("batch id differs from the connection batch"));
    }
    // Checked for every chunk, so a batch storers start rejecting
    // while the stream is open is no longer used.
    match batch_use(&conn.handle, &batch).await {
        Ok(BatchUse::Usable) => Ok((batch, None)),
        Ok(BatchUse::Unusable) => Err(bad("batch not usable yet or does not exist")),
        Ok(BatchUse::Unknown) => Err(bad("batch with id not found")),
        Err(_) => Err((pb::Status::Error, "failed to stamp chunk")),
    }
}

/// Parse an *optional* `swarm-postage-batch-id`: `Ok(None)` when
/// absent/empty, bee's header-param error shapes when malformed.
#[allow(clippy::result_large_err)]
fn optional_batch_header(headers: &HeaderMap) -> Result<Option<[u8; 32]>, Response> {
    let Some(raw) = headers
        .get("swarm-postage-batch-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return Ok(None);
    };
    let mut reasons = Vec::new();
    match parse_hex_param::<32>("swarm-postage-batch-id", raw, &mut reasons) {
        Some(batch) => Ok(Some(batch)),
        None if reasons
            .iter()
            .any(|r| r.error.starts_with("invalid length")) =>
        {
            // Valid hex, wrong size: bee's []byte map hook accepts it
            // and the putter rejects it as an invalid batch id.
            Err(json_error(StatusCode::BAD_REQUEST, "invalid batch id"))
        }
        None => Err(params_error(ParamKind::Header, reasons)),
    }
}

/// Resolve the requested tag uid (header, then query fallback).
#[allow(clippy::result_large_err)]
fn tag_from_request(headers: &HeaderMap, query_tag: Option<&str>) -> Result<Option<u32>, Response> {
    if let Some(raw) = headers
        .get("swarm-tag")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return match raw.parse::<u64>() {
            Ok(0) => Ok(None),
            Ok(uid) => Ok(Some(u32::try_from(uid).unwrap_or(u32::MAX))),
            Err(_) => Err(params_error(
                ParamKind::Header,
                vec![Reason::new("Swarm-Tag", "invalid syntax")],
            )),
        };
    }
    if let Some(raw) = query_tag.map(str::trim).filter(|s| !s.is_empty()) {
        return match raw.parse::<u64>() {
            Ok(0) => Ok(None),
            Ok(uid) => Ok(Some(u32::try_from(uid).unwrap_or(u32::MAX))),
            Err(_) => Err(json_error(
                StatusCode::BAD_REQUEST,
                "invalid swarm-tag query parameter",
            )),
        };
    }
    Ok(None)
}

/// Post-upgrade message loop.
async fn handle_upload_stream(
    handle: GatewayHandle,
    mut socket: WebSocket,
    batch_id: Option<[u8; 32]>,
    tag: Option<u32>,
) {
    // Close the socket with bee's `sendErrorClose` shape.
    async fn error_close(socket: &mut WebSocket, code: u16, reason: &str) {
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code,
                reason: reason.to_string().into(),
            })))
            .await;
    }

    loop {
        // Client gone / transport error: nothing more to do.
        let Some(Ok(msg)) = socket.recv().await else {
            return;
        };
        let data = match msg {
            Message::Binary(data) => data,
            Message::Close(_) => return,
            // axum answers pings automatically; ignore strays.
            Message::Ping(_) | Message::Pong(_) => continue,
            Message::Text(_) => {
                error_close(&mut socket, CLOSE_UNSUPPORTED, "invalid message").await;
                return;
            }
        };

        // Bee: a message shorter than the span drops the connection
        // without a close frame.
        if data.len() < 8 {
            debug!(target: "ant_gateway", "chunk upload stream: insufficient data");
            return;
        }

        // Decode: bare chunk wire with a connection batch, else a
        // 113-byte pre-signed stamp prefix.
        let (wire, stamp, chunk_batch): (&[u8], Option<Vec<u8>>, [u8; 32]) =
            if let Some(batch) = batch_id {
                (&data[..], None, batch)
            } else {
                if data.len() < STAMP_SIZE + 8 {
                    error_close(
                        &mut socket,
                        CLOSE_INTERNAL,
                        "message too small for stamp + chunk",
                    )
                    .await;
                    return;
                }
                let stamp = data[..STAMP_SIZE].to_vec();
                let batch: [u8; 32] = stamp[..32].try_into().expect("stamp slice");
                (&data[STAMP_SIZE..], Some(stamp), batch)
            };

        // Bee's `cac.NewWithDataSpan` rejects wires outside
        // `[8, 8+4096]` ("invalid chunk data").
        if wire.len() > MAX_CHUNK_WIRE {
            error_close(&mut socket, CLOSE_INTERNAL, "invalid chunk data").await;
            return;
        }

        let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
        let cmd = ControlCommand::PushChunk {
            wire: wire.to_vec(),
            batch_id: chunk_batch,
            stamp,
            // Bee-parity accept-shallow (see POST /chunks).
            require_deep: false,
            ack_when_stored: false,
            ack: ack_tx,
        };
        if handle.commands.send(cmd).await.is_err() {
            error_close(&mut socket, CLOSE_INTERNAL, "chunk write error").await;
            return;
        }
        let Ok(Ok(ack)) = tokio::time::timeout(CHUNK_PUSH_TIMEOUT, ack_rx).await else {
            error_close(&mut socket, CLOSE_INTERNAL, "chunk write error").await;
            return;
        };
        match ack {
            ControlAck::ChunkUploaded { .. } => {
                if let Some(uid) = tag {
                    handle.tags.add_synced(uid, 1);
                }
                // Bee's per-chunk success ack: an empty binary message.
                if socket
                    .send(Message::Binary(Vec::new().into()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            ControlAck::Error { message } => {
                let reason = if message.contains("overissued") {
                    "batch is overissued"
                } else if message.contains("wire size out of range") || message.contains("BMT-hash")
                {
                    "invalid chunk data"
                } else {
                    "chunk write error"
                };
                debug!(target: "ant_gateway", %message, "chunk upload stream: write failed");
                error_close(&mut socket, CLOSE_INTERNAL, reason).await;
                return;
            }
            other => {
                debug!(target: "ant_gateway", ?other, "chunk upload stream: unexpected ack");
                error_close(&mut socket, CLOSE_INTERNAL, "chunk write error").await;
                return;
            }
        }
    }
}

/// The `bee-chunk-stream-v1` messages, field for field bee's
/// `pkg/api/pb/chunkstream.proto` (package `chunkstream`), derived by
/// hand like ant-p2p's wire messages instead of generated from the
/// `.proto`.
pub mod pb {
    /// A client's download (`Get`) or upload (`Put`).
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Request {
        #[prost(oneof = "request::Body", tags = "1, 2")]
        pub body: Option<request::Body>,
    }

    pub mod request {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Body {
            #[prost(message, tag = "1")]
            Get(super::GetRequest),
            #[prost(message, tag = "2")]
            Put(super::PutRequest),
        }
    }

    /// Whether a retrieved chunk is cached (`CACHE_DEFAULT`: the
    /// connection's `Swarm-Cache` / `?cache=`).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum CacheOption {
        Default = 0,
        Disable = 1,
        Enable = 2,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct GetRequest {
        /// 32-byte chunk address (CAC or SOC).
        #[prost(bytes = "vec", tag = "1")]
        pub address: Vec<u8>,
        #[prost(enumeration = "CacheOption", tag = "2")]
        pub cache: i32,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct PutRequest {
        /// 32-byte address the data must rebuild to.
        #[prost(bytes = "vec", tag = "1")]
        pub address: Vec<u8>,
        /// CAC: `span ‖ payload`; SOC: `id ‖ sig ‖ span ‖ payload`.
        #[prost(bytes = "vec", tag = "2")]
        pub data: Vec<u8>,
        /// Pre-signed 113-byte postage stamp; wins over `batch_id`.
        #[prost(bytes = "vec", tag = "3")]
        pub stamp: Vec<u8>,
        /// 32-byte batch for the node to stamp with.
        #[prost(bytes = "vec", tag = "4")]
        pub batch_id: Vec<u8>,
    }

    /// The node's reply, with the request's body type.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct Response {
        #[prost(oneof = "response::Body", tags = "1, 2")]
        pub body: Option<response::Body>,
    }

    pub mod response {
        #[derive(Clone, PartialEq, prost::Oneof)]
        pub enum Body {
            #[prost(message, tag = "1")]
            Get(super::GetResponse),
            #[prost(message, tag = "2")]
            Put(super::PutResponse),
        }
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct GetResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub address: Vec<u8>,
        #[prost(enumeration = "Status", tag = "2")]
        pub status: i32,
        #[prost(bytes = "vec", tag = "3")]
        pub data: Vec<u8>,
        #[prost(string, tag = "4")]
        pub error: String,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    pub struct PutResponse {
        #[prost(bytes = "vec", tag = "1")]
        pub address: Vec<u8>,
        #[prost(enumeration = "Status", tag = "2")]
        pub status: i32,
        #[prost(string, tag = "3")]
        pub error: String,
    }

    /// Outcome of a request (bee's `STATUS_*`; `Unspecified` is never
    /// sent).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum Status {
        Unspecified = 0,
        Ok = 1,
        NotFound = 2,
        Error = 3,
        BadRequest = 4,
        Busy = 5,
        Unavailable = 6,
    }

    pub(crate) fn get_response(
        address: Vec<u8>,
        status: Status,
        data: Vec<u8>,
        error: &str,
    ) -> Response {
        Response {
            body: Some(response::Body::Get(GetResponse {
                address,
                status: status as i32,
                data,
                error: error.to_string(),
            })),
        }
    }

    pub(crate) fn put_response(address: Vec<u8>, status: Status, error: &str) -> Response {
        Response {
            body: Some(response::Body::Put(PutResponse {
                address,
                status: status as i32,
                error: error.to_string(),
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::pb::{self, request, response, Status};
    use super::*;
    use ant_control::{GatewayActivity, PostageStatusView};
    use axum::http::HeaderValue;
    use futures::stream::{SplitSink, SplitStream};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex as StdMutex;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
    use tokio_tungstenite::tungstenite::Message as WsMsg;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    type Ws = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

    const BATCH_A: [u8; 32] = [0xaa; 32];
    const BATCH_B: [u8; 32] = [0xbb; 32];
    /// Registered, but storers rejected its stamps.
    const BATCH_REJECTED: [u8; 32] = [0xcc; 32];
    /// Never registered.
    const BATCH_UNKNOWN: [u8; 32] = [0xdd; 32];
    /// `GetChunkRaw` answers `NotReady`. The real node doesn't send this
    /// for `GetChunkRaw`; the fake does, to pin the mapping.
    const NOT_READY: [u8; 32] = [0xee; 32];
    /// `GetChunkRaw` answers late.
    const SLOW: [u8; 32] = [0x51; 32];
    /// `GetChunkRaw` never answers a reference starting with this byte.
    const HANG: u8 = 0x99;
    const SECRET: [u8; 32] = [0x11; 32];

    /// What the fake node was asked to do.
    #[derive(Debug, Clone, PartialEq)]
    enum Seen {
        Get {
            reference: [u8; 32],
            bypass_cache: bool,
        },
        PushChunk {
            batch_id: [u8; 32],
            stamp: Option<Vec<u8>>,
        },
        PushSoc {
            address: [u8; 32],
            batch_id: [u8; 32],
            stamp: Option<Vec<u8>>,
        },
    }

    struct Node {
        chunks: HashMap<[u8; 32], Vec<u8>>,
        seen: StdMutex<Vec<Seen>>,
        /// Answer `PostageList` with a failure instead.
        postage_down: AtomicBool,
    }

    impl Node {
        fn seen(&self) -> Vec<Seen> {
            self.seen.lock().unwrap().clone()
        }
        fn gets(&self) -> usize {
            self.seen()
                .iter()
                .filter(|s| matches!(s, Seen::Get { .. }))
                .count()
        }
    }

    fn view(batch: [u8; 32], usable: bool) -> PostageStatusView {
        PostageStatusView {
            enabled: true,
            usable,
            batch_id: format!("0x{}", hex::encode(batch)),
            ..Default::default()
        }
    }

    async fn answer(node: Arc<Node>, cmd: ControlCommand) {
        match cmd {
            ControlCommand::GetChunkRaw {
                reference,
                bypass_cache,
                ack,
                ..
            } => {
                node.seen.lock().unwrap().push(Seen::Get {
                    reference,
                    bypass_cache,
                });
                if reference == SLOW {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                }
                if reference[0] == HANG {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
                let reply = if reference == NOT_READY {
                    ControlAck::NotReady {
                        message: "no peer left to ask".into(),
                    }
                } else if let Some(data) = node.chunks.get(&reference) {
                    ControlAck::Bytes { data: data.clone() }
                } else {
                    ControlAck::NotFound {
                        message: "chunk not found".into(),
                    }
                };
                let _ = ack.send(reply);
            }
            ControlCommand::PushChunk {
                batch_id,
                stamp,
                ack,
                wire,
                ..
            } => {
                node.seen
                    .lock()
                    .unwrap()
                    .push(Seen::PushChunk { batch_id, stamp });
                let span: [u8; 8] = wire[..8].try_into().unwrap();
                let addr = ant_crypto::bmt_hash_with_span(&span, &wire[8..]).unwrap();
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(addr)),
                });
            }
            ControlCommand::PushSoc {
                address,
                batch_id,
                stamp,
                ack,
                ..
            } => {
                node.seen.lock().unwrap().push(Seen::PushSoc {
                    address,
                    batch_id,
                    stamp,
                });
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(address)),
                });
            }
            ControlCommand::PostageList { ack } => {
                if node.postage_down.load(Ordering::SeqCst) {
                    drop(ack);
                    return;
                }
                let _ = ack.send(ControlAck::PostageList(vec![
                    view(BATCH_A, true),
                    view(BATCH_B, true),
                    view(BATCH_REJECTED, false),
                ]));
            }
            other => panic!("unexpected command {other:?}"),
        }
    }

    /// A gateway handle over a fake node holding `chunks`.
    fn fake_node(chunks: HashMap<[u8; 32], Vec<u8>>) -> (GatewayHandle, Arc<Node>) {
        let node = Arc::new(Node {
            chunks,
            seen: StdMutex::new(Vec::new()),
            postage_down: AtomicBool::new(false),
        });
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(64);
        let n = node.clone();
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                tokio::spawn(answer(n.clone(), cmd));
            }
        });
        let (status_tx, status_rx) =
            tokio::sync::watch::channel(ant_control::StatusSnapshot::default());
        Box::leak(Box::new(status_tx));
        let handle = GatewayHandle {
            agent: Arc::new("antd/test".into()),
            api_version: Arc::new("7.2.0".into()),
            identity: Arc::new(crate::handle::GatewayIdentity {
                overlay_hex: String::new(),
                ethereum_hex: String::new(),
                public_key_hex: String::new(),
                peer_id: String::new(),
            }),
            status: status_rx,
            commands: cmd_tx,
            activity: GatewayActivity::new(),
            tags: Arc::new(crate::tags::TagRegistry::new()),
            cors: Arc::new(crate::cors::CorsConfig::default()),
            chain_state: crate::handle::GatewayChainState {
                light_mode: true,
                chain: None,
            }
            .preset(),
            act_secret: Arc::new([7u8; 32]),
            on_batch_bought: None,
            on_chequebook_refused: None,
            cache: None,
        };
        (handle, node)
    }

    async fn listen(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        format!("ws://{addr}")
    }

    /// Serve the real router over a fake node holding `chunks`.
    async fn serve(chunks: HashMap<[u8; 32], Vec<u8>>) -> (String, Arc<Node>, axum::Router) {
        let (handle, node) = fake_node(chunks);
        let router = crate::router::build(handle);
        let base = listen(router.clone()).await;
        (base, node, router)
    }

    /// Serve [`run_v1`] directly with shortened `timing`, reporting
    /// through the returned receiver whether the connection's task
    /// ended without a panic. `send_buffer` caps the accepted sockets'
    /// kernel send buffer, which loopback otherwise grows to megabytes.
    fn serve_timed(
        chunks: HashMap<[u8; 32], Vec<u8>>,
        timing: V1Timing,
        send_buffer: Option<u32>,
    ) -> (String, Arc<Node>, mpsc::Receiver<bool>) {
        let (handle, node) = fake_node(chunks);
        let (done_tx, done_rx) = mpsc::channel(4);
        let router = axum::Router::new().route(
            "/chunks/stream",
            axum::routing::get(move |ws: WebSocketUpgrade| {
                let conn = Arc::new(V1Conn {
                    handle: handle.clone(),
                    default_batch: None,
                    default_cache: true,
                    timing,
                });
                let done = done_tx.clone();
                async move {
                    ws.protocols([CHUNK_STREAM_SUBPROTOCOL])
                        .on_upgrade(move |socket| async move {
                            let ended = tokio::spawn(run_v1(conn, socket)).await;
                            let _ = done.send(ended.is_ok()).await;
                        })
                }
            }),
        );
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        if let Some(size) = send_buffer {
            // Accepted sockets inherit it from the listener.
            socket.set_send_buffer_size(size).unwrap();
        }
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(64).unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        (format!("ws://{addr}"), node, done_rx)
    }

    /// Open `/chunks/stream{query}`, offering v1 when `v1`.
    async fn connect(
        base: &str,
        query: &str,
        headers: &[(&'static str, &str)],
        v1: bool,
    ) -> Result<(Ws, Option<String>), tokio_tungstenite::tungstenite::Error> {
        let mut req = format!("{base}/chunks/stream{query}")
            .into_client_request()
            .unwrap();
        if v1 {
            req.headers_mut().insert(
                "sec-websocket-protocol",
                HeaderValue::from_static("other, bee-chunk-stream-v1"),
            );
        }
        for (k, v) in headers {
            req.headers_mut()
                .insert(*k, HeaderValue::from_str(v).unwrap());
        }
        let (ws, resp) = tokio_tungstenite::connect_async(req).await?;
        let selected = resp
            .headers()
            .get("sec-websocket-protocol")
            .map(|v| v.to_str().unwrap().to_string());
        Ok((ws, selected))
    }

    async fn handshake_status(base: &str, query: &str, headers: &[(&'static str, &str)]) -> u16 {
        match connect(base, query, headers, true).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => resp.status().as_u16(),
            Ok(_) => panic!("handshake {query} {headers:?} unexpectedly upgraded"),
            Err(e) => panic!("handshake failed oddly: {e}"),
        }
    }

    fn get(address: &[u8], cache: pb::CacheOption) -> pb::Request {
        pb::Request {
            body: Some(request::Body::Get(pb::GetRequest {
                address: address.to_vec(),
                cache: cache as i32,
            })),
        }
    }

    fn put(address: &[u8], data: &[u8], stamp: &[u8], batch: &[u8]) -> pb::Request {
        pb::Request {
            body: Some(request::Body::Put(pb::PutRequest {
                address: address.to_vec(),
                data: data.to_vec(),
                stamp: stamp.to_vec(),
                batch_id: batch.to_vec(),
            })),
        }
    }

    async fn send(ws: &mut Ws, req: &pb::Request) {
        ws.send(WsMsg::Binary(req.encode_to_vec().into()))
            .await
            .unwrap();
    }

    async fn next_msg<S>(ws: &mut S) -> WsMsg
    where
        S: futures::Stream<Item = Result<WsMsg, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        loop {
            let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
                .await
                .expect("reply in time")
                .expect("socket open")
                .expect("frame");
            if !matches!(msg, WsMsg::Ping(_) | WsMsg::Pong(_)) {
                return msg;
            }
        }
    }

    async fn reply<S>(ws: &mut S) -> response::Body
    where
        S: futures::Stream<Item = Result<WsMsg, tokio_tungstenite::tungstenite::Error>> + Unpin,
    {
        match next_msg(ws).await {
            WsMsg::Binary(b) => pb::Response::decode(&b[..]).unwrap().body.expect("body"),
            other => panic!("expected a reply, got {other:?}"),
        }
    }

    async fn get_reply(ws: &mut Ws) -> pb::GetResponse {
        match reply(ws).await {
            response::Body::Get(g) => g,
            other @ response::Body::Put(_) => panic!("expected a GetResponse, got {other:?}"),
        }
    }

    async fn put_reply(ws: &mut Ws) -> pb::PutResponse {
        match reply(ws).await {
            response::Body::Put(p) => p,
            other @ response::Body::Get(_) => panic!("expected a PutResponse, got {other:?}"),
        }
    }

    async fn expect_close(ws: &mut Ws, code: CloseCode, reason: &str) {
        match next_msg(ws).await {
            WsMsg::Close(Some(cf)) => {
                assert_eq!(cf.code, code);
                assert_eq!(cf.reason.as_str(), reason);
            }
            other => panic!("expected close {code:?}, got {other:?}"),
        }
    }

    fn cac(payload: &[u8]) -> ([u8; 32], Vec<u8>) {
        ant_crypto::cac_new(payload).unwrap()
    }

    /// `id ‖ sig ‖ inner CAC` signed by [`SECRET`], and its address.
    fn soc(id: [u8; 32], payload: &[u8]) -> ([u8; 32], Vec<u8>) {
        let sk = k256::ecdsa::SigningKey::from_bytes(&SECRET.into()).unwrap();
        let owner = ant_crypto::ethereum_address_from_public_key(sk.verifying_key());
        let (inner_addr, inner) = cac(payload);
        let mut to_sign = [0u8; 64];
        to_sign[..32].copy_from_slice(&id);
        to_sign[32..].copy_from_slice(&inner_addr);
        let sig =
            ant_crypto::sign_handshake_data(&SECRET, &ant_crypto::keccak256(&to_sign)).unwrap();
        let mut addr_input = [0u8; 52];
        addr_input[..32].copy_from_slice(&id);
        addr_input[32..].copy_from_slice(&owner);
        let mut wire = id.to_vec();
        wire.extend_from_slice(&sig);
        wire.extend_from_slice(&inner);
        (ant_crypto::keccak256(&addr_input), wire)
    }

    /// A pre-signed stamp of `batch` over `address`.
    fn stamp(batch: [u8; 32], address: &[u8; 32]) -> Vec<u8> {
        let index = [0u8; 8];
        let ts = 1u64.to_be_bytes();
        let digest = ant_postage::postage_sign_digest(address, &batch, &index, &ts);
        let sig = ant_crypto::sign_handshake_data(&SECRET, &digest).unwrap();
        let mut s = batch.to_vec();
        s.extend_from_slice(&index);
        s.extend_from_slice(&ts);
        s.extend_from_slice(&sig);
        s
    }

    /// A stamp whose all-zero signature recovers no key.
    fn unsigned_stamp(batch: [u8; 32]) -> Vec<u8> {
        let mut s = batch.to_vec();
        s.resize(STAMP_SIZE, 0);
        s
    }

    #[tokio::test]
    async fn put_then_get_round_trip_over_one_socket() {
        let (addr, wire) = cac(b"round trip");
        let (base, node, _) = serve(HashMap::from([(addr, wire.clone())])).await;
        let batch = hex::encode(BATCH_A);
        let (mut ws, selected) = connect(
            &base,
            "",
            &[("swarm-postage-batch-id", batch.as_str())],
            true,
        )
        .await
        .unwrap();
        assert_eq!(selected.as_deref(), Some(CHUNK_STREAM_SUBPROTOCOL));

        send(&mut ws, &put(&addr, &wire, &[], &[])).await;
        let p = put_reply(&mut ws).await;
        assert_eq!(
            (p.address.as_slice(), p.status),
            (&addr[..], Status::Ok as i32)
        );

        send(&mut ws, &get(&addr, pb::CacheOption::Default)).await;
        let g = get_reply(&mut ws).await;
        assert_eq!(g.status, Status::Ok as i32);
        assert_eq!(g.address, addr);
        assert_eq!(g.data, wire);

        assert_eq!(
            node.seen(),
            vec![
                Seen::PushChunk {
                    batch_id: BATCH_A,
                    stamp: None
                },
                Seen::Get {
                    reference: addr,
                    bypass_cache: false
                },
            ]
        );
    }

    #[tokio::test]
    async fn replies_come_in_completion_order() {
        let (fast, wire) = cac(b"fast");
        let (base, _, _) = serve(HashMap::from([(fast, wire), (SLOW, b"slow".to_vec())])).await;
        let (mut ws, _) = connect(&base, "", &[], true).await.unwrap();
        send(&mut ws, &get(&SLOW, pb::CacheOption::Default)).await;
        send(&mut ws, &get(&fast, pb::CacheOption::Default)).await;
        assert_eq!(get_reply(&mut ws).await.address, fast);
        assert_eq!(get_reply(&mut ws).await.address, SLOW);
    }

    #[tokio::test]
    async fn per_request_errors_leave_the_socket_open() {
        let (addr, wire) = cac(b"valid");
        let (other, _) = cac(b"other");
        let (base, node, _) = serve(HashMap::from([(addr, wire.clone())])).await;
        let batch = hex::encode(BATCH_A);
        let (mut ws, _) = connect(
            &base,
            "",
            &[("swarm-postage-batch-id", batch.as_str())],
            true,
        )
        .await
        .unwrap();

        for (req, status, error) in [
            (
                get(&other, pb::CacheOption::Default),
                Status::NotFound,
                "chunk not found",
            ),
            (
                get(&NOT_READY, pb::CacheOption::Default),
                Status::Unavailable,
                "chunk could not be retrieved because no usable peer was available",
            ),
            (
                get(b"too-short", pb::CacheOption::Default),
                Status::BadRequest,
                "invalid chunk address length",
            ),
            (
                put(b"too-short", &wire, &[], &[]),
                Status::BadRequest,
                "invalid chunk address length",
            ),
            (
                put(&addr, &[1, 2, 3], &[], &[]),
                Status::BadRequest,
                "insufficient data for chunk",
            ),
            (
                put(&other, &wire, &[], &[]),
                Status::BadRequest,
                "address does not match chunk data",
            ),
            (
                put(&addr, &wire, &[], b"short"),
                Status::BadRequest,
                "invalid batch id",
            ),
        ] {
            let expect_addr = match &req.body {
                Some(request::Body::Get(g)) => g.address.clone(),
                Some(request::Body::Put(p)) => p.address.clone(),
                None => unreachable!(),
            };
            send(&mut ws, &req).await;
            let (got_addr, got_status, got_error) = match reply(&mut ws).await {
                response::Body::Get(g) => (g.address, g.status, g.error),
                response::Body::Put(p) => (p.address, p.status, p.error),
            };
            assert_eq!(got_addr, expect_addr, "{error}");
            assert_eq!((got_status, got_error.as_str()), (status as i32, error));
        }

        // Still usable afterwards.
        send(&mut ws, &put(&addr, &wire, &[], &[])).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
        send(&mut ws, &get(&addr, pb::CacheOption::Default)).await;
        assert_eq!(get_reply(&mut ws).await.data, wire);
        // Rejected puts never reached the node.
        assert_eq!(
            node.seen()
                .iter()
                .filter(|s| matches!(s, Seen::PushChunk { .. }))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn the_address_settles_cac_versus_soc() {
        let (base, node, _) = serve(HashMap::new()).await;
        let batch = hex::encode(BATCH_A);
        let (mut ws, _) = connect(
            &base,
            "",
            &[("swarm-postage-batch-id", batch.as_str())],
            true,
        )
        .await
        .unwrap();

        // A small SOC: its bytes also read as a valid CAC.
        let (soc_addr, soc_wire) = soc([3u8; 32], b"feed update");
        let span: [u8; 8] = soc_wire[..8].try_into().unwrap();
        let as_cac = ant_crypto::bmt_hash_with_span(&span, &soc_wire[8..]).unwrap();

        send(&mut ws, &put(&soc_addr, &soc_wire, &[], &[])).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
        send(&mut ws, &put(&as_cac, &soc_wire, &[], &[])).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);

        // A SOC whose signature doesn't recover the owner behind the
        // address: neither reading matches.
        let mut forged = soc_wire.clone();
        forged[40] ^= 1;
        send(&mut ws, &put(&soc_addr, &forged, &[], &[])).await;
        let p = put_reply(&mut ws).await;
        assert_eq!(
            (p.status, p.error.as_str()),
            (
                Status::BadRequest as i32,
                "address does not match chunk data"
            )
        );

        assert_eq!(
            node.seen(),
            vec![
                Seen::PushSoc {
                    address: soc_addr,
                    batch_id: BATCH_A,
                    stamp: None
                },
                Seen::PushChunk {
                    batch_id: BATCH_A,
                    stamp: None
                },
            ]
        );
    }

    #[tokio::test]
    async fn stamp_precedence_without_a_connection_batch() {
        let (base, node, _) = serve(HashMap::new()).await;
        let (mut ws, _) = connect(&base, "", &[], true).await.unwrap();
        let (addr, wire) = cac(b"stamped");

        for (req, error) in [
            (
                put(&addr, &wire, &[], &[]),
                "missing postage stamp or batch id",
            ),
            (
                put(&addr, &wire, &[], &BATCH_UNKNOWN),
                "batch with id not found",
            ),
            (
                put(&addr, &wire, &[], &BATCH_REJECTED),
                "batch not usable yet or does not exist",
            ),
            (put(&addr, &wire, &[1, 2, 3], &[]), "invalid postage stamp"),
            // A signature that recovers no key. (One signed over another
            // chunk still recovers *a* key; without the batch owner that
            // is `POST /chunks`' bar too, see the module doc.)
            (
                put(&addr, &wire, &unsigned_stamp(BATCH_B), &[]),
                "invalid postage stamp",
            ),
        ] {
            send(&mut ws, &req).await;
            let p = put_reply(&mut ws).await;
            assert_eq!(
                (p.status, p.error.as_str()),
                (Status::BadRequest as i32, error)
            );
        }
        assert!(node.seen().is_empty(), "nothing pushed: {:?}", node.seen());

        // One connection, several batches: the request's batch, and a
        // pre-signed stamp, which wins over a `BatchId` next to it.
        let st = stamp(BATCH_B, &addr);
        send(&mut ws, &put(&addr, &wire, &[], &BATCH_A)).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
        send(&mut ws, &put(&addr, &wire, &st, &BATCH_A)).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
        assert_eq!(
            node.seen(),
            vec![
                Seen::PushChunk {
                    batch_id: BATCH_A,
                    stamp: None
                },
                Seen::PushChunk {
                    batch_id: BATCH_B,
                    stamp: Some(st)
                },
            ]
        );
    }

    #[tokio::test]
    async fn a_connection_batch_pins_every_upload() {
        let (base, node, _) = serve(HashMap::new()).await;
        let batch = hex::encode(BATCH_A);
        let (mut ws, _) = connect(
            &base,
            "",
            &[("swarm-postage-batch-id", batch.as_str())],
            true,
        )
        .await
        .unwrap();
        let (addr, wire) = cac(b"pinned");

        for (req, error) in [
            (
                put(&addr, &wire, &[], &BATCH_B),
                "batch id differs from the connection batch",
            ),
            (
                put(&addr, &wire, &stamp(BATCH_B, &addr), &[]),
                "stamp batch differs from the connection batch",
            ),
        ] {
            send(&mut ws, &req).await;
            let p = put_reply(&mut ws).await;
            assert_eq!(
                (p.status, p.error.as_str()),
                (Status::BadRequest as i32, error)
            );
        }
        let st = stamp(BATCH_A, &addr);
        send(&mut ws, &put(&addr, &wire, &[], &BATCH_A)).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
        send(&mut ws, &put(&addr, &wire, &st, &[])).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
        assert_eq!(
            node.seen(),
            vec![
                Seen::PushChunk {
                    batch_id: BATCH_A,
                    stamp: None
                },
                Seen::PushChunk {
                    batch_id: BATCH_A,
                    stamp: Some(st)
                },
            ]
        );
    }

    #[tokio::test]
    async fn cache_default_and_per_request_override() {
        let (addr, wire) = cac(b"cached");
        let (base, node, _) = serve(HashMap::from([(addr, wire)])).await;
        // (query, header, request option) -> bypass the cache?
        for (query, header, option, bypass) in [
            ("", None, pb::CacheOption::Default, false),
            ("?cache=false", None, pb::CacheOption::Default, true),
            (
                "?cache=false",
                Some("true"),
                pb::CacheOption::Default,
                false,
            ),
            ("", Some("false"), pb::CacheOption::Default, true),
            ("", Some("false"), pb::CacheOption::Enable, false),
            ("", None, pb::CacheOption::Disable, true),
        ] {
            let headers: Vec<(&'static str, &str)> =
                header.map(|h| ("swarm-cache", h)).into_iter().collect();
            let (mut ws, _) = connect(&base, query, &headers, true).await.unwrap();
            send(&mut ws, &get(&addr, option)).await;
            assert_eq!(get_reply(&mut ws).await.status, Status::Ok as i32);
            assert_eq!(
                node.seen().last(),
                Some(&Seen::Get {
                    reference: addr,
                    bypass_cache: bypass
                }),
                "{query} {header:?} {option:?}"
            );
        }
    }

    #[tokio::test]
    async fn handshake_rejections() {
        let (base, node, router) = serve(HashMap::new()).await;
        let unknown = hex::encode(BATCH_UNKNOWN);
        let rejected = hex::encode(BATCH_REJECTED);
        let a = hex::encode(BATCH_A);
        assert_eq!(
            handshake_status(&base, "", &[("swarm-tag", "1")]).await,
            400
        );
        assert_eq!(
            handshake_status(&base, "", &[("swarm-tag", "0")]).await,
            400
        );
        assert_eq!(handshake_status(&base, "?swarm-tag=1", &[]).await, 400);
        assert_eq!(
            handshake_status(&base, "", &[("swarm-postage-batch-id", unknown.as_str())]).await,
            404
        );
        assert_eq!(
            handshake_status(&base, "", &[("swarm-postage-batch-id", "abcd")]).await,
            404
        );
        assert_eq!(
            handshake_status(&base, "", &[("swarm-postage-batch-id", rejected.as_str())]).await,
            422
        );
        assert_eq!(
            handshake_status(&base, "", &[("swarm-postage-batch-id", "zz")]).await,
            400
        );
        assert_eq!(handshake_status(&base, "?cache=maybe", &[]).await, 400);
        assert_eq!(
            handshake_status(&base, "", &[("swarm-cache", "maybe")]).await,
            400
        );
        node.postage_down.store(true, Ordering::SeqCst);
        assert_eq!(
            handshake_status(&base, "", &[("swarm-postage-batch-id", a.as_str())]).await,
            500
        );

        // A plain GET offering the protocol is bee's 400.
        let resp = tower::ServiceExt::oneshot(
            router,
            axum::http::Request::get("/chunks/stream")
                .header("sec-websocket-protocol", CHUNK_STREAM_SUBPROTOCOL)
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn protocol_violations_close_the_socket() {
        let (base, _, _) = serve(HashMap::new()).await;
        let empty = pb::Request { body: None }.encode_to_vec();
        for (msg, code, reason) in [
            (
                WsMsg::Text("hello".into()),
                CloseCode::Unsupported,
                "invalid message type",
            ),
            (
                WsMsg::Binary(vec![0xff, 0xff, 0xff].into()),
                CloseCode::Unsupported,
                "invalid protobuf request",
            ),
            (
                WsMsg::Binary(empty.clone().into()),
                CloseCode::Unsupported,
                "missing request body",
            ),
            (
                WsMsg::Binary(vec![0u8; MAX_STREAM_FRAME + 1].into()),
                CloseCode::Size,
                "",
            ),
        ] {
            let (mut ws, _) = connect(&base, "", &[], true).await.unwrap();
            ws.send(msg).await.unwrap();
            expect_close(&mut ws, code, reason).await;
        }
    }

    /// A client that stops reading stalls the node's reads of its
    /// requests, then gets every reply once it reads again.
    #[tokio::test]
    async fn a_client_that_stops_reading_stops_the_reader() {
        const N: usize = 8000;
        let (addr, wire) = cac(&[7u8; 4096]);
        let (base, node, _) = serve(HashMap::from([(addr, wire)])).await;
        let (ws, _) = connect(&base, "", &[], true).await.unwrap();
        let (mut tx, mut rx): (SplitSink<Ws, WsMsg>, SplitStream<Ws>) = ws.split();
        let frame = get(&addr, pb::CacheOption::Default).encode_to_vec();
        let sender = tokio::spawn(async move {
            for _ in 0..N {
                tx.send(WsMsg::Binary(frame.clone().into())).await.unwrap();
            }
            tx
        });

        // Wait until the node's work plateaus with replies unread.
        let mut last = usize::MAX;
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let now = node.gets();
            if now == last {
                break;
            }
            last = now;
        }
        assert!(
            last < N,
            "every request was served without the client reading"
        );

        let mut answered = 0;
        while answered < N {
            match reply(&mut rx).await {
                response::Body::Get(g) => {
                    assert!(
                        g.status == Status::Ok as i32 || g.status == Status::Busy as i32,
                        "status {}",
                        g.status
                    );
                    answered += 1;
                }
                response::Body::Put(_) => panic!("no puts were sent"),
            }
        }
        drop(sender.await.unwrap());
    }

    /// Without the subprotocol the old upload-only framing still works,
    /// tags included.
    #[tokio::test]
    async fn legacy_framing_without_the_subprotocol() {
        let (base, node, _) = serve(HashMap::new()).await;
        let batch = hex::encode(BATCH_A);
        let (mut ws, selected) = connect(
            &base,
            "",
            &[("swarm-postage-batch-id", batch.as_str())],
            false,
        )
        .await
        .unwrap();
        assert_eq!(selected, None);
        let (_, wire) = cac(b"legacy");
        ws.send(WsMsg::Binary(wire.into())).await.unwrap();
        assert_eq!(next_msg(&mut ws).await, WsMsg::Binary(Vec::new().into()));
        assert_eq!(
            node.seen(),
            vec![Seen::PushChunk {
                batch_id: BATCH_A,
                stamp: None
            }]
        );
        ws.send(WsMsg::Text("x".into())).await.unwrap();
        expect_close(&mut ws, CloseCode::Unsupported, "invalid message").await;

        // A legacy tag is still taken (an unknown one is its 404), not
        // rejected the way v1 rejects every tag.
        match connect(&base, "?swarm-tag=4242", &[], false).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(resp)) => {
                assert_eq!(resp.status().as_u16(), 404);
            }
            other => panic!("expected the legacy tag 404, got {:?}", other.map(|_| ())),
        }
    }

    /// The messages' bytes, worked out by hand from bee's
    /// `chunkstream.proto` field numbers and wire types, so a wrong tag
    /// can't pass by being wrong on both ends.
    #[test]
    fn wire_format_matches_bees_proto() {
        // Request{Get=1{Address=1: 32x01, Cache=2: DISABLE(1)}}
        let mut want = vec![0x0a, 0x24, 0x0a, 0x20];
        want.extend([1u8; 32]);
        want.extend([0x10, 0x01]);
        assert_eq!(
            get(&[1u8; 32], pb::CacheOption::Disable).encode_to_vec(),
            want
        );

        // Request{Put=2{Address=1, Data=2: dead, BatchId=4}}, no Stamp.
        let mut want = vec![0x12, 0x48, 0x0a, 0x20];
        want.extend([2u8; 32]);
        want.extend([0x12, 0x02, 0xde, 0xad, 0x22, 0x20]);
        want.extend([3u8; 32]);
        assert_eq!(
            put(&[2u8; 32], &[0xde, 0xad], &[], &[3u8; 32]).encode_to_vec(),
            want
        );
        // Stamp is field 3.
        let with_stamp = put(&[2u8; 32], &[], &[0x55], &[]).encode_to_vec();
        assert_eq!(&with_stamp[36..], &[0x1a, 0x01, 0x55]);

        // Response{Get=1{Address=1, Status=2: OK(1), Data=3: ab}}
        let mut want = vec![0x0a, 0x27, 0x0a, 0x20];
        want.extend([1u8; 32]);
        want.extend([0x10, 0x01, 0x1a, 0x01, 0xab]);
        assert_eq!(
            pb::get_response(vec![1u8; 32], Status::Ok, vec![0xab], "").encode_to_vec(),
            want
        );
        // Response{Put=2{Address=1, Status=2: BAD_REQUEST(4), Error=3: "x"}}
        let mut want = vec![0x12, 0x27, 0x0a, 0x20];
        want.extend([2u8; 32]);
        want.extend([0x10, 0x04, 0x1a, 0x01, b'x']);
        assert_eq!(
            pb::put_response(vec![2u8; 32], Status::BadRequest, "x").encode_to_vec(),
            want
        );
        // The rest of the status numbering.
        assert_eq!(
            [
                Status::NotFound,
                Status::Error,
                Status::Busy,
                Status::Unavailable
            ]
            .map(|s| s as i32),
            [2, 3, 5, 6]
        );
        assert_eq!(pb::CacheOption::Enable as i32, 2);
    }

    /// With every Get worker busy and the queue full, the next Get is
    /// answered `BUSY`, echoing its address.
    #[tokio::test]
    async fn a_full_queue_answers_busy() {
        let (base, node, _) = serve(HashMap::new()).await;
        let (mut ws, _) = connect(&base, "", &[], true).await.unwrap();
        let hang = |i: u16| {
            let mut a = [HANG; 32];
            a[1..3].copy_from_slice(&i.to_be_bytes());
            a
        };
        for i in 0..STREAM_WORKERS as u16 {
            send(&mut ws, &get(&hang(i), pb::CacheOption::Default)).await;
        }
        while node.gets() < STREAM_WORKERS {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        for i in 0..STREAM_QUEUE as u16 {
            send(&mut ws, &get(&hang(1000 + i), pb::CacheOption::Default)).await;
        }
        let last = hang(9999);
        send(&mut ws, &get(&last, pb::CacheOption::Default)).await;
        let g = get_reply(&mut ws).await;
        assert_eq!(
            (g.address.as_slice(), g.status, g.error.as_str()),
            (&last[..], Status::Busy as i32, "request queue full")
        );
        // A full Get queue doesn't hold up Puts.
        let (addr, wire) = cac(b"not blocked");
        send(&mut ws, &put(&addr, &wire, &[], &BATCH_A)).await;
        assert_eq!(put_reply(&mut ws).await.status, Status::Ok as i32);
    }

    /// A client that sends nothing, not even a pong, is dropped once the
    /// read timeout passes, although the node keeps pinging it.
    #[tokio::test]
    async fn a_silent_client_is_dropped() {
        let (base, _, mut done) = serve_timed(
            HashMap::new(),
            V1Timing {
                read_timeout: Duration::from_millis(300),
                write_timeout: Duration::from_secs(5),
                ping_period: Duration::from_millis(50),
            },
            None,
        );
        // Not reading means not answering the node's pings either.
        let (_ws, _) = connect(&base, "", &[], true).await.unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(3), done.recv())
            .await
            .expect("the stream outlived its read timeout");
        assert_eq!(ended, Some(true));
    }

    /// A client that sent a burst of Gets and then only reads, slower
    /// than the node answers, keeps the reply queue full the whole time.
    /// It must still get pings (and so send pongs), or the idle deadline
    /// drops it with replies still pending, which bee never does.
    #[tokio::test]
    async fn a_slow_reader_with_a_full_reply_queue_is_kept_alive() {
        const GETS: usize = 1000;
        let (addr, wire) = cac(&[9u8; 4096]);
        let (base, node, mut done) = serve_timed(
            HashMap::from([(addr, wire)]),
            V1Timing {
                read_timeout: Duration::from_millis(600),
                write_timeout: Duration::from_secs(30),
                ping_period: Duration::from_millis(50),
            },
            Some(16 * 1024),
        );
        // Small socket buffers keep loopback from buffering the whole
        // backlog in the kernel, where a ping would queue behind it on
        // bee too: the backlog has to sit in the node's reply queue.
        let tcp = tokio::net::TcpSocket::new_v4().unwrap();
        tcp.set_recv_buffer_size(8 * 1024).unwrap();
        let tcp = tcp
            .connect(base.trim_start_matches("ws://").parse().unwrap())
            .await
            .unwrap();
        let mut req = format!("{base}/chunks/stream")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static("bee-chunk-stream-v1"),
        );
        let (mut ws, _) = tokio_tungstenite::client_async(req, MaybeTlsStream::Plain(tcp))
            .await
            .unwrap();
        for _ in 0..GETS {
            send(&mut ws, &get(&addr, pb::CacheOption::Default)).await;
        }
        // Let the workers fill the socket buffers and the reply queue.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            node.gets() < GETS / 2,
            "the kernel buffered the backlog: {}",
            node.gets()
        );
        // Drain several read timeouts' worth slower than replies arrive,
        // sending nothing but the pongs the client library answers with.
        let started = tokio::time::Instant::now();
        for i in 0..GETS {
            let reply = tokio::time::timeout(Duration::from_secs(5), get_reply(&mut ws))
                .await
                .unwrap_or_else(|_| panic!("reply {i} never came"));
            assert_eq!(reply.status, Status::Ok as i32, "reply {i}");
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        assert!(started.elapsed() > Duration::from_millis(1800));
        assert!(done.try_recv().is_err(), "the stream was dropped");
        // And the socket still serves requests.
        send(&mut ws, &get(&addr, pb::CacheOption::Default)).await;
        assert_eq!(get_reply(&mut ws).await.status, Status::Ok as i32);
    }

    /// A client over the Get queue limit gets `BUSY`, and the reader
    /// blocks on that reply behind a full reply queue for longer than
    /// the read timeout, while the client reads slowly and answers every
    /// ping. Its pongs are waiting when the send completes, so the
    /// elapsed deadline must not drop it.
    #[tokio::test]
    async fn a_reader_blocked_on_busy_keeps_a_ponging_client() {
        // Enough to fill the workers, the Get queue and the reply queue,
        // and then some that can only be answered `BUSY`.
        const GETS: usize = STREAM_WORKERS + STREAM_QUEUE + REPLY_QUEUE + 300;
        let (addr, wire) = cac(&[5u8; 4096]);
        let (base, _, mut done) = serve_timed(
            HashMap::from([(addr, wire)]),
            V1Timing {
                read_timeout: Duration::from_millis(250),
                write_timeout: Duration::from_secs(30),
                ping_period: Duration::from_millis(20),
            },
            Some(16 * 1024),
        );
        let tcp = tokio::net::TcpSocket::new_v4().unwrap();
        tcp.set_recv_buffer_size(8 * 1024).unwrap();
        let tcp = tcp
            .connect(base.trim_start_matches("ws://").parse().unwrap())
            .await
            .unwrap();
        let mut req = format!("{base}/chunks/stream")
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            "sec-websocket-protocol",
            HeaderValue::from_static("bee-chunk-stream-v1"),
        );
        let (ws, _) = tokio_tungstenite::client_async(req, MaybeTlsStream::Plain(tcp))
            .await
            .unwrap();
        let (mut tx, mut rx) = ws.split();
        let frame = get(&addr, pb::CacheOption::Default).encode_to_vec();
        let sender = tokio::spawn(async move {
            for _ in 0..GETS {
                if tx.send(WsMsg::Binary(frame.clone().into())).await.is_err() {
                    break;
                }
            }
            tx
        });
        // Let the backlog build until the reader is stuck on a busy reply.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Read slowly: each reply frees one reply-queue slot, and the
        // reader's busy send waits behind the workers' sends for it, so
        // it stays blocked for several reads, past the read timeout.
        // The client library answers each ping as it reads it.
        let (mut ok, mut busy) = (0, 0);
        let started = tokio::time::Instant::now();
        while ok + busy < GETS {
            let body = tokio::time::timeout(Duration::from_secs(10), reply(&mut rx))
                .await
                .unwrap_or_else(|_| panic!("reply {} never came", ok + busy));
            match body {
                response::Body::Get(g) if g.status == Status::Ok as i32 => ok += 1,
                response::Body::Get(g) if g.status == Status::Busy as i32 => busy += 1,
                other => panic!("unexpected reply {other:?}"),
            }
            if started.elapsed() < Duration::from_secs(3) {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }
        assert!(busy > 0, "no request was answered BUSY");
        assert!(done.try_recv().is_err(), "the stream was dropped");
        let mut tx = sender.await.unwrap();
        tx.send(WsMsg::Binary(
            get(&addr, pb::CacheOption::Default).encode_to_vec().into(),
        ))
        .await
        .unwrap();
        let body = tokio::time::timeout(Duration::from_secs(5), reply(&mut rx))
            .await
            .expect("the socket stopped serving");
        assert!(matches!(body, response::Body::Get(g) if g.status == Status::Ok as i32));
    }

    /// A reply write that times out (a client that stopped reading) ends
    /// the stream cleanly.
    #[tokio::test]
    async fn a_reply_write_timeout_ends_the_stream() {
        let (addr, wire) = cac(&[7u8; 4096]);
        let (base, node, mut done) = serve_timed(
            HashMap::from([(addr, wire)]),
            V1Timing {
                read_timeout: Duration::from_secs(60),
                write_timeout: Duration::from_millis(300),
                ping_period: Duration::from_secs(60),
            },
            None,
        );
        let (mut ws, _) = connect(&base, "", &[], true).await.unwrap();
        // More replies than the socket buffers hold, but few enough to
        // queue, so the reader is idle when the writer gives up.
        for _ in 0..1000 {
            send(&mut ws, &get(&addr, pb::CacheOption::Default)).await;
        }
        let ended = tokio::time::timeout(Duration::from_secs(10), done.recv())
            .await
            .expect("the stream outlived its write timeout");
        assert_eq!(ended, Some(true), "the connection task panicked");
        assert!(node.gets() < 1000, "every reply was written");
    }
}
