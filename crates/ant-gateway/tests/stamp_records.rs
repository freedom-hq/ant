//! `GET /stamps/{id}/chunks/{address}` and
//! `GET /stewardship/{reference}/stamps` (ethersphere/bee#5682): the
//! HTTP shape and status mapping over the node's `StampedChunk` /
//! `StewardshipStamps` acks. The counting itself is unit-tested in
//! `ant-p2p` against real `StampIssuer`s.

mod common;

use ant_control::{BatchChunkCount, ControlAck, ControlCommand, StewardshipStampsView};
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{body_bytes, handle_with_fixture_node, router_with_dispatcher, send};
use serde_json::Value;

const BATCH: [u8; 32] = [0xba; 32];
const CHUNK: [u8; 32] = [0x5a; 32];

fn get(uri: String) -> Request<Body> {
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

/// A dispatcher with one batch whose records hold `CHUNK` only.
fn one_batch_router() -> axum::Router {
    router_with_dispatcher(|cmd| async move {
        match cmd {
            ControlCommand::StampedChunk {
                batch_id,
                address,
                ack,
            } => {
                let reply = if batch_id == [0x00; 32] {
                    ControlAck::Error {
                        message: "uploads not configured: node has no stamp issuers".into(),
                    }
                } else if batch_id != BATCH {
                    ControlAck::Error {
                        message: format!("issuer does not exist: 0x{}", hex::encode(batch_id)),
                    }
                } else if address == CHUNK {
                    let mut stamp = [0u8; 113];
                    stamp[..32].copy_from_slice(&BATCH);
                    stamp[32..40].copy_from_slice(&[0, 0, 0, 7, 0, 0, 0, 3]);
                    stamp[40..48].copy_from_slice(&1_700_000_000_000_000_000u64.to_be_bytes());
                    ControlAck::StampedChunk { stamp: Some(stamp) }
                } else if address == [0xee; 32] {
                    ControlAck::Error {
                        message: "stamp records unavailable for batch 0xba..: unreadable sidecar"
                            .into(),
                    }
                } else {
                    ControlAck::StampedChunk { stamp: None }
                };
                let _ = ack.send(reply);
            }
            ControlCommand::StewardshipStamps { reference, ack } => {
                let reply = if reference == [0xee; 32] {
                    ControlAck::Error {
                        message: "stamps check traversal failed: fetch eeee: not found".into(),
                    }
                } else {
                    ControlAck::StewardshipStamps(StewardshipStampsView {
                        chunks: 5,
                        batches: vec![
                            BatchChunkCount {
                                batch_id: [0x01; 32],
                                chunks: 0,
                            },
                            BatchChunkCount {
                                batch_id: BATCH,
                                chunks: 4,
                            },
                        ],
                        unstamped: 1,
                    })
                };
                let _ = ack.send(reply);
            }
            other => panic!("unexpected command {other:?}"),
        }
    })
}

#[tokio::test]
async fn stamped_chunk_reports_index_and_timestamp() {
    let resp = send(
        one_batch_router(),
        get(format!(
            "/stamps/{}/chunks/{}",
            hex::encode(BATCH),
            hex::encode(CHUNK)
        )),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["batchID"], hex::encode(BATCH));
    assert_eq!(json["chunkAddress"], hex::encode(CHUNK));
    assert_eq!(json["index"], "0000000700000003");
    assert_eq!(
        json["timestamp"],
        hex::encode(1_700_000_000_000_000_000u64.to_be_bytes())
    );
}

#[tokio::test]
async fn unstamped_chunk_is_404() {
    let resp = send(
        one_batch_router(),
        get(format!(
            "/stamps/{}/chunks/{}",
            hex::encode(BATCH),
            hex::encode([0x77; 32])
        )),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "chunk not stamped by this batch");
}

#[tokio::test]
async fn unknown_batch_is_bees_issuer_404() {
    let resp = send(
        handle_with_fixture_node(),
        get(format!(
            "/stamps/{}/chunks/{}",
            hex::encode([0x99; 32]),
            hex::encode(CHUNK)
        )),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "issuer does not exist");
}

/// Records that couldn't be read back must not read as "not stamped".
#[tokio::test]
async fn unavailable_records_are_not_a_404() {
    let resp = send(
        one_batch_router(),
        get(format!(
            "/stamps/{}/chunks/{}",
            hex::encode(BATCH),
            hex::encode([0xee; 32])
        )),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("stamp records unavailable"));
}

#[tokio::test]
async fn bad_path_params_are_bee_400() {
    let resp = send(
        one_batch_router(),
        get(format!("/stamps/{}/chunks/xyz", hex::encode(BATCH))),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn stewardship_stamps_lists_batches_and_unstamped() {
    let resp = send(
        one_batch_router(),
        get(format!("/stewardship/{}/stamps", hex::encode([0x42; 32]))),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["chunks"], 5);
    assert_eq!(json["unstamped"], 1);
    let batches = json["batches"].as_array().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0]["batchID"], hex::encode([0x01; 32]));
    assert_eq!(batches[0]["chunks"], 0);
    assert_eq!(batches[1]["batchID"], hex::encode(BATCH));
    assert_eq!(batches[1]["chunks"], 4);
}

#[tokio::test]
async fn stewardship_stamps_walk_failure_is_an_error_with_cause() {
    let resp = send(
        one_batch_router(),
        get(format!("/stewardship/{}/stamps", hex::encode([0xee; 32]))),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert!(json["message"]
        .as_str()
        .unwrap()
        .contains("traversal failed"));
}

#[tokio::test]
async fn stewardship_stamps_without_issuers_is_503() {
    let resp = send(
        handle_with_fixture_node(),
        get(format!("/stewardship/{}/stamps", hex::encode([0x42; 32]))),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// The new routes sit beside `/stamps/{id}/buckets` and the plain
/// `/stewardship/{address}`; those still route to their own handlers.
#[tokio::test]
async fn neighbouring_routes_still_resolve() {
    let resp = send(
        handle_with_fixture_node(),
        get(format!("/stamps/{}/buckets", hex::encode([0x99; 32]))),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["message"], "issuer does not exist");
}

/// No upload runtime yet (antd before chain init) is not "not stamped".
#[tokio::test]
async fn stamped_chunk_without_issuers_is_503_not_404() {
    let resp = send(
        one_batch_router(),
        get(format!(
            "/stamps/{}/chunks/{}",
            hex::encode([0x00; 32]),
            hex::encode(CHUNK)
        )),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}
