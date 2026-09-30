//! Bee-parity `/pins` surface ("pin-light").
//!
//! Ant's product call: mobile offline-first needs durable local copies,
//! so `/pins` is implemented against the persistent `SQLite` disk chunk
//! cache instead of bee's reserve-backed pinstore. Shapes mirror
//! `bee/pkg/api/pin.go` exactly:
//!
//! * `POST /pins/{reference}` — traverse the content tree (fetching
//!   cold chunks through the normal network fetcher, like bee's
//!   `storer.Download(true)` getter) and mark every chunk
//!   pin-protected. `201 {"code":201,"message":"Created"}`; an
//!   already-pinned root is idempotent `200 {"code":200,"message":"OK"}`;
//!   unreachable content is `404 "pin collection failed"`.
//! * `DELETE /pins/{reference}` — unpin. `200 OK`, or bee's
//!   `404 {"code":404,"message":"Not Found"}` when the root isn't
//!   pinned.
//! * `GET /pins/{reference}` — `200 {"reference":"<hex>"}` / `404`.
//! * `GET /pins` — `200 {"references":[...]}` in pin-creation order.
//! * `GET /pins/check[?ref=...]` — bee's pin-integrity stream: one
//!   NDJSON line `{"reference","total","missing","invalid"}` per
//!   inspected pin (chunked, `application/json; charset=utf-8`).
//!
//! Pinned references may be 64-byte encrypted references
//! (`address ‖ decryption key`); the node traverses those by
//! decrypting and pins the stored ciphertext chunks (the "addr half").

use ant_control::{ControlAck, ControlCommand};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use serde::Deserialize;
use tokio::sync::oneshot;
use tracing::warn;

use crate::error::{json_error, params_error, parse_hex_param, ParamKind, JSON_CONTENT_TYPE};
use crate::handle::GatewayHandle;
use crate::retrieval::{node_unavailable, request_timeout, DEFAULT_REQUEST_TIMEOUT};

/// Parse the `{reference}` pin path segment: 32-byte plain or 64-byte
/// encrypted reference, bee's `map:"reference"` field name in the
/// validation reasons.
#[allow(clippy::result_large_err)]
fn parse_pin_reference(s: &str) -> Result<Vec<u8>, Response> {
    let mut reasons = Vec::new();
    if s.len() == 128 {
        return parse_hex_param::<64>("reference", s, &mut reasons)
            .map(|r| r.to_vec())
            .ok_or_else(|| params_error(ParamKind::Path, reasons));
    }
    parse_hex_param::<32>("reference", s, &mut reasons)
        .map(|r| r.to_vec())
        .ok_or_else(|| params_error(ParamKind::Path, reasons))
}

/// Dispatch a pin command and await its single ack.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
async fn dispatch(
    handle: &GatewayHandle,
    headers: &HeaderMap,
    cmd_of: impl FnOnce(oneshot::Sender<ControlAck>) -> ControlCommand,
) -> Result<ControlAck, Response> {
    let timeout = request_timeout(headers, DEFAULT_REQUEST_TIMEOUT);
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    if handle.commands.send(cmd_of(ack_tx)).await.is_err() {
        return Err(node_unavailable());
    }
    match tokio::time::timeout(timeout, ack_rx).await {
        Ok(Ok(ack)) => Ok(ack),
        Ok(Err(_)) => Err(node_unavailable()),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            format!("pin operation timed out after {}s", timeout.as_secs()),
        )),
    }
}

/// `POST /pins/{reference}` — bee `pinRootHash`.
pub async fn pin_post(
    State(handle): State<GatewayHandle>,
    Path(reference): Path<String>,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_pin_reference(&reference) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    // Bee parses (and validates) `Swarm-Redundancy-Level` before
    // pinning; the value only tunes its traversal strategy, so ant
    // validates for the error shape and otherwise ignores it.
    if let Err(resp) = crate::retrieval::parse_redundancy_level_header(&headers) {
        return resp;
    }

    let ack = match dispatch(&handle, &headers, |ack| ControlCommand::PinAdd {
        reference,
        ack,
    })
    .await
    {
        Ok(ack) => ack,
        Err(resp) => return resp,
    };
    match ack {
        ControlAck::PinAdded { already_pinned } => {
            if already_pinned {
                // Bee: HasPin short-circuit → jsonhttp.OK(w, nil).
                json_error(StatusCode::OK, "OK")
            } else {
                json_error(StatusCode::CREATED, "Created")
            }
        }
        ControlAck::Error { message } => {
            // Bee maps a traversal storage.ErrNotFound to 404, the
            // rest to 500, both with "pin collection failed".
            if message.contains("not found") {
                json_error(StatusCode::NOT_FOUND, "pin collection failed")
            } else {
                warn!(target: "ant_gateway", %message, "pin collection failed");
                json_error(StatusCode::INTERNAL_SERVER_ERROR, "pin collection failed")
            }
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PinAdd");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `DELETE /pins/{reference}` — bee `unpinRootHash`.
pub async fn pin_delete(
    State(handle): State<GatewayHandle>,
    Path(reference): Path<String>,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_pin_reference(&reference) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let ack = match dispatch(&handle, &headers, |ack| ControlCommand::PinRemove {
        reference,
        ack,
    })
    .await
    {
        Ok(ack) => ack,
        Err(resp) => return resp,
    };
    match ack {
        ControlAck::PinRemoved { was_pinned: true } => json_error(StatusCode::OK, "OK"),
        // Bee: `jsonhttp.NotFound(w, nil)`.
        ControlAck::PinRemoved { was_pinned: false } => {
            json_error(StatusCode::NOT_FOUND, "Not Found")
        }
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "unpin failed");
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unpin root hash: deletion of pin failed",
            )
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PinRemove");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `GET /pins/{reference}` — bee `getPinnedRootHash`.
pub async fn pin_get(
    State(handle): State<GatewayHandle>,
    Path(reference): Path<String>,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_pin_reference(&reference) {
        Ok(r) => r,
        Err(resp) => return resp,
    };
    let hex_ref = hex::encode(&reference);
    let ack = match dispatch(&handle, &headers, |ack| ControlCommand::PinHas {
        reference,
        ack,
    })
    .await
    {
        Ok(ack) => ack,
        Err(resp) => return resp,
    };
    match ack {
        ControlAck::PinPresent { pinned: true } => crate::retrieval::json_response(
            StatusCode::OK,
            &serde_json::json!({ "reference": hex_ref }),
        ),
        ControlAck::PinPresent { pinned: false } => json_error(StatusCode::NOT_FOUND, "Not Found"),
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "pin lookup failed");
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "pinned root hash: check reference failed",
            )
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PinHas");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `GET /pins` — bee `listPinnedRootHashes`.
pub async fn pin_list(State(handle): State<GatewayHandle>, headers: HeaderMap) -> Response {
    let ack = match dispatch(&handle, &headers, |ack| ControlCommand::PinList { ack }).await {
        Ok(ack) => ack,
        Err(resp) => return resp,
    };
    match ack {
        ControlAck::PinList { references } => {
            let refs: Vec<String> = references.iter().map(hex::encode).collect();
            crate::retrieval::json_response(
                StatusCode::OK,
                &serde_json::json!({ "references": refs }),
            )
        }
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "pin listing failed");
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "list pinned root references failed",
            )
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PinList");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// Query of `GET /pins/check`: an optional `ref` (bee's zero-value
/// `swarm.Address` when absent ⇒ inspect every pin).
#[derive(Debug, Deserialize)]
pub struct PinCheckQuery {
    #[serde(rename = "ref")]
    reference: Option<String>,
}

/// `GET /pins/check` — bee `pinIntegrityHandler`. Streams one NDJSON
/// line per inspected pin. (Ant buffers the stats node-side — pin
/// counts are small — and writes them out newline-delimited so clients
/// written against bee's chunked stream parse it identically.)
pub async fn pin_check(
    State(handle): State<GatewayHandle>,
    Query(query): Query<PinCheckQuery>,
    headers: HeaderMap,
) -> Response {
    let reference = match query.reference.as_deref() {
        None | Some("") => None,
        Some(raw) => {
            let mut reasons = Vec::new();
            let parsed = if raw.len() == 128 {
                parse_hex_param::<64>("ref", raw, &mut reasons).map(|r| r.to_vec())
            } else {
                parse_hex_param::<32>("ref", raw, &mut reasons).map(|r| r.to_vec())
            };
            match parsed {
                Some(r) => Some(r),
                None => return params_error(ParamKind::Query, reasons),
            }
        }
    };
    let ack = match dispatch(&handle, &headers, |ack| ControlCommand::PinCheck {
        reference,
        ack,
    })
    .await
    {
        Ok(ack) => ack,
        Err(resp) => return resp,
    };
    match ack {
        ControlAck::PinCheck { stats } => {
            // Bee's `PinIntegrityResponse` field order (Go structs keep
            // declaration order): reference, total, missing, invalid.
            #[derive(serde::Serialize)]
            struct Line {
                reference: String,
                total: u64,
                missing: u64,
                invalid: u64,
            }
            let mut body = String::new();
            for stat in stats {
                let line = Line {
                    reference: hex::encode(&stat.reference),
                    total: stat.total,
                    missing: stat.missing,
                    invalid: stat.invalid,
                };
                body.push_str(&serde_json::to_string(&line).expect("serialize pin check line"));
                body.push('\n');
            }
            let mut resp = Response::new(Body::from(body));
            *resp.status_mut() = StatusCode::OK;
            let _ = resp
                .headers_mut()
                .insert(header::CONTENT_TYPE, JSON_CONTENT_TYPE);
            resp
        }
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "pin integrity check failed");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "pin check failed")
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from PinCheck");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}
