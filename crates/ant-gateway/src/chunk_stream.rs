//! `GET /chunks/stream` — bee's WebSocket chunk-upload stream
//! (`bee/pkg/api/chunk_stream.go`).
//!
//! Protocol, mirrored from bee exactly:
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

use ant_control::{ControlAck, ControlCommand};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use serde::Deserialize;
use tokio::sync::oneshot;
use tracing::debug;

use crate::error::{json_error, params_error, parse_hex_param, ParamKind, Reason};
use crate::handle::GatewayHandle;
use crate::retrieval::CHUNK_PUSH_TIMEOUT;

/// Bee's `websocket.CloseUnsupportedData`.
const CLOSE_UNSUPPORTED: u16 = 1003;
/// Bee's `websocket.CloseInternalServerErr`.
const CLOSE_INTERNAL: u16 = 1011;

/// 113-byte postage stamp prefix used in header-less mode.
const STAMP_SIZE: usize = 113;
/// `span(8) ‖ payload(≤4096)`.
const MAX_CHUNK_WIRE: usize = 8 + 4096;

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    /// Browser fallback for the `swarm-tag` header.
    #[serde(rename = "swarm-tag")]
    swarm_tag: Option<String>,
}

/// `GET /chunks/stream` upgrade handler.
pub async fn chunk_stream(
    State(handle): State<GatewayHandle>,
    Query(query): Query<StreamQuery>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Optional batch header: present-but-malformed is bee's
    // mapStructure 400; absent means "pre-signed stamps per message".
    let batch_id = match optional_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    // Tag: header first (mapStructure uint64), then the query-param
    // fallback with bee's dedicated error message.
    let tag = match tag_from_request(&headers, query.swarm_tag.as_deref()) {
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
