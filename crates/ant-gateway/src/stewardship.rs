//! Bee-parity `POST /envelope/{address}` and `GET|PUT
//! /stewardship/{address}`.
//!
//! * `POST /envelope/{address}` (`bee/pkg/api/envelope.go`): issue a
//!   pre-signed postage stamp for a chunk address the caller will
//!   upload later — `stamper.Stamp(address, address)` against the
//!   registered issuer for the `swarm-postage-batch-id` header. `201`
//!   with `{"issuer","index","timestamp","signature"}`, issuer as an
//!   EIP-55 checksummed `0x` address (Go's `common.Address.Hex()`).
//! * `GET /stewardship/{address}` (`bee/pkg/api/stewardship.go`):
//!   traverse the content tree and report
//!   `{"isRetrievable": true|false}`.
//! * `GET /stewardship/{address}/stamps` (proposed for bee in
//!   ethersphere/bee#5682): walk the tree like the `GET` above and
//!   report, from this node's own stamp-issuer records, how many of its
//!   chunks each of the node's batches stamps and how many none does:
//!   `{"chunks","batches":[{"batchID","chunks"}],"unstamped"}`.
//! * `PUT /stewardship/{address}`: re-upload the tree — fetch every
//!   chunk, stamp against the batch header, pushsync. `200` with bee's
//!   `jsonhttp.OK(w, nil)` body: `{"code":200,"message":"OK"}`.

use ant_control::{ControlAck, ControlCommand, GatewayRequestKind};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use tokio::sync::oneshot;
use tracing::warn;

use crate::error::json_error;
use crate::handle::GatewayHandle;
use crate::retrieval::{
    json_response, node_unavailable, parse_postage_batch_header, parse_reference, request_timeout,
    CHUNK_REQUEST_TIMEOUT, DEFAULT_REQUEST_TIMEOUT,
};

/// EIP-55 checksummed rendering of a 20-byte Ethereum address — the
/// exact format Go's `common.Address.Hex()` produces for bee's
/// envelope `issuer` field: `0x` + 40 hex nibbles where each alphabetic
/// nibble is uppercased iff the corresponding nibble of
/// `keccak256(lowercase_hex_without_prefix)` is ≥ 8.
pub(crate) fn eip55_address(addr: &[u8; 20]) -> String {
    let lower = hex::encode(addr);
    let hash = ant_crypto::keccak256(lower.as_bytes());
    let mut out = String::with_capacity(42);
    out.push_str("0x");
    for (i, c) in lower.chars().enumerate() {
        let nibble = (hash[i / 2] >> (if i % 2 == 0 { 4 } else { 0 })) & 0x0f;
        if c.is_ascii_alphabetic() && nibble >= 8 {
            out.push(c.to_ascii_uppercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// `POST /envelope/{address}` — bee checks headers before the path
/// (`envelopePostHandler` maps the header struct first), so a request
/// missing the batch header on a bogus address still gets the header
/// error.
pub async fn post_envelope(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    headers: HeaderMap,
) -> Response {
    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let address = match parse_reference(&addr) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, CHUNK_REQUEST_TIMEOUT);
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, "envelope".to_string());

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::Envelope {
        address,
        batch_id,
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
                format!("envelope timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::Envelope { issuer, stamp } => {
            guard.update(1, 1, 0, 0);
            json_response(
                StatusCode::CREATED,
                &serde_json::json!({
                    "issuer": eip55_address(&issuer),
                    "index": hex::encode(&stamp[32..40]),
                    "timestamp": hex::encode(&stamp[40..48]),
                    "signature": hex::encode(&stamp[48..113]),
                }),
            )
        }
        ControlAck::Error { message } => {
            // Bee's `envelopePostHandler` mappings: unusable / unknown
            // batch → 422; a full immutable bucket → 402; the rest of
            // the stamping path → 500.
            if message.contains("not usable") {
                json_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "batch not usable yet or does not exist",
                )
            } else if message.contains("overissued") {
                json_error(StatusCode::PAYMENT_REQUIRED, "batch is overissued")
            } else if message.starts_with("uploads not configured") {
                json_error(StatusCode::SERVICE_UNAVAILABLE, message)
            } else {
                json_error(StatusCode::INTERNAL_SERVER_ERROR, "stamping failed")
            }
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from Envelope");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `GET /stewardship/{address}` — 200 `{"isRetrievable": bool}`.
pub async fn stewardship_get(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, "stewardship-check".to_string());

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::StewardshipGet {
        reference,
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
                format!("stewardship check timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::Retrievable { retrievable } => {
            guard.update(1, 1, 0, 0);
            json_response(
                StatusCode::OK,
                &serde_json::json!({ "isRetrievable": retrievable }),
            )
        }
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "stewardship check failed");
            json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "is retrievable check failed",
            )
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from StewardshipGet");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// `GET /stewardship/{address}/stamps` — `200 {"chunks": N, "batches":
/// [{"batchID", "chunks"}], "unstamped": M}`. `batches` lists every batch
/// this node issues stamps from, sorted by id and including those that
/// stamp none of the tree; a chunk stamped by two of them counts in
/// both; `unstamped` counts the chunks none of them stamped. The walk
/// fetches only interior and manifest nodes (leaves are listed from
/// their parents), so the counts say nothing about whether the leaves
/// are retrievable — that is the plain `GET`'s question. A walk that
/// can't complete is an error, never partial counts; so is a batch whose
/// records are unavailable (`500`) or a node with no stamp issuers yet
/// (`503`). Read-only and open on the same terms as `/stamps`.
///
/// The counts are this data dir's records, with the limits
/// `stamps::stamp_chunk` lists: stamps issued by an older Ant,
/// by bee, or into a lost data dir count as unstamped, and on a mutable
/// batch whose bucket wrapped a counted chunk may have lost its slot.
pub async fn stewardship_stamps(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(a) => a,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Chunk, "stewardship-stamps".to_string());

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::StewardshipStamps {
        reference,
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
                format!("stamps check timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::StewardshipStamps(view) => {
            guard.update(1, 1, 0, 0);
            json_response(StatusCode::OK, &stewardship_stamps_body(&view))
        }
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "stewardship stamps check failed");
            if message.starts_with("uploads not configured") {
                json_error(StatusCode::SERVICE_UNAVAILABLE, message)
            } else {
                json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("stamps check failed: {message}"),
                )
            }
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from StewardshipStamps");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

/// JSON body of `GET /stewardship/{address}/stamps`.
fn stewardship_stamps_body(view: &ant_control::StewardshipStampsView) -> serde_json::Value {
    serde_json::json!({
        "chunks": view.chunks,
        "batches": view
            .batches
            .iter()
            .map(|b| serde_json::json!({
                "batchID": hex::encode(b.batch_id),
                "chunks": b.chunks,
            }))
            .collect::<Vec<_>>(),
        "unstamped": view.unstamped,
    })
}

/// `PUT /stewardship/{address}` — bee parses the path first, then the
/// (required) batch header, then re-uploads. Success is bee's
/// `jsonhttp.OK(w, nil)`: `{"code":200,"message":"OK"}`.
pub async fn stewardship_put(
    State(handle): State<GatewayHandle>,
    Path(addr): Path<String>,
    headers: HeaderMap,
) -> Response {
    let reference = match parse_reference(&addr) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let guard = handle.activity.begin(
        GatewayRequestKind::Chunk,
        "stewardship-reupload".to_string(),
    );

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::StewardshipPut {
        reference,
        batch_id,
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
                format!("re-upload timed out after {}s", timeout.as_secs()),
            );
        }
    };

    match ack {
        ControlAck::Ok { .. } => {
            guard.update(1, 1, 0, 0);
            // Bee's `jsonhttp.OK(w, nil)` renders Go's
            // `http.StatusText(200)`.
            json_error(StatusCode::OK, "OK")
        }
        ControlAck::Error { message } => {
            // Bee's `stewardshipPutHandler` getStamper mappings mirror
            // the envelope handler; a traversal / push failure is a
            // plain 500 "re-upload failed".
            if message.contains("not usable") {
                json_error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "batch not usable yet or does not exist",
                )
            } else if message.starts_with("uploads not configured") {
                json_error(StatusCode::SERVICE_UNAVAILABLE, message)
            } else {
                warn!(target: "ant_gateway", %message, "stewardship re-upload failed");
                json_error(StatusCode::INTERNAL_SERVER_ERROR, "re-upload failed")
            }
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from StewardshipPut");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected node ack")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::eip55_address;

    /// EIP-55 test vectors from the EIP itself plus the beemock node
    /// key's address observed in the differential report.
    #[test]
    fn eip55_checksums_match_reference_vectors() {
        let cases = [
            "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
            "0xfB6916095ca1df60bB79Ce92cE3Ea74c37c5d359",
            "0xdbF03B407c01E7cD3CBea99509d93f8DDDC8C6FB",
            "0xD1220A0cf47c7B9Be7A2E6BA89F429762e7b9aDb",
            "0x3B09f374c31347b5dE1F8b734873693cfB608e74",
        ];
        for want in cases {
            let mut raw = [0u8; 20];
            hex::decode_to_slice(want.trim_start_matches("0x").to_lowercase(), &mut raw).unwrap();
            assert_eq!(eip55_address(&raw), want);
        }
    }
}
