//! D.2.2 status endpoints.
//!
//! Each test pins both the HTTP status and the JSON shape the bee-js
//! consumers index by. Where bee uses `camelCase`, the assertion uses
//! the verbatim wire field name (not the Rust struct field) — that's
//! the contract that makes `bee-js` reach `bzz` over us.

mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{
    body_bytes, empty_snapshot, send, snapshot_with_one_peer, status_only_router,
    swap_switch_router, test_identity,
};
use serde_json::Value;

#[tokio::test]
async fn health_returns_ok_status_with_versions() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["version"], "antd/test");
    assert_eq!(json["apiVersion"], "7.2.0");
}

/// `/readiness` answers 200 once a BZZ-handshaked peer that can serve is
/// in the routing table.
#[tokio::test]
async fn readiness_200_when_peer_handshaked() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/readiness")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    // Bee marshals a JSON status body; clients (bee-js, our Swift
    // `BeeReadiness`) index `status`, so a bodyless 200 reads as not-ready.
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["status"], "ready");
}

/// `/readiness` answers 503 when no peer has handshaked yet.
#[tokio::test]
async fn readiness_503_when_no_peers() {
    let router = status_only_router(empty_snapshot());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/readiness")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["status"], "unready");
}

/// Issue #83: a handshaked peer whose connection stopped answering
/// pings (`stale`) doesn't make the node ready — that was the probe gate
/// hosts were lied to by while every retrieval hung. The gateway can't
/// see staleness itself: it answers from the published
/// `RoutingInfo::serving`, which the node loop's `serving_peer_count`
/// computes without stale peers. That half is covered by
/// `ant_p2p`'s `serving_peer_count_skips_stale_peers`; this test pins the
/// gateway half: a stale-only snapshot, as the node loop publishes it,
/// answers 503.
#[tokio::test]
async fn readiness_503_for_a_stale_only_snapshot() {
    let mut snap = snapshot_with_one_peer();
    snap.peers.connected_peers[0].stale = true;
    snap.peers.connected = 0;
    // What `serving_peer_count` publishes for a stale-only table.
    snap.peers.routing.serving = 0;
    let router = status_only_router(snap);
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/readiness")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

/// `/readiness` stays 503 until a routing peer can serve (#78), however
/// many libp2p connections are open. A cold node's first connection, and
/// its first routing-table entry, is a bootnode that resets the connection
/// right after the handshake; while that's all it has, every `/bzz`
/// answers 502 "no peers available".
#[tokio::test]
async fn readiness_503_until_a_routing_peer_can_serve() {
    // Connections, a handshaked row, even a routing-table entry — but it's
    // a fresh bootnode, so nothing serves.
    let mut bootnode_only = snapshot_with_one_peer();
    bootnode_only.peers.connected = 3;
    bootnode_only.peers.routing.serving = 0;
    // Connections alone, with an empty routing table.
    let mut connections_only = bootnode_only.clone();
    connections_only.peers.routing.size = 0;
    connections_only.peers.routing.bins = vec![0; 32];
    for snap in [bootnode_only, connections_only] {
        let router = status_only_router(snap);
        let resp = send(
            router,
            Request::builder()
                .method(Method::GET)
                .uri("/readiness")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
        assert_eq!(json["status"], "unready");
    }
}

#[tokio::test]
async fn node_returns_ultra_light_mode() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/node")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    assert_eq!(json["beeMode"], "ultra-light");
    assert_eq!(json["gatewayMode"], false);
    assert_eq!(json["chequebookEnabled"], false);
    assert_eq!(json["swapEnabled"], false);
    assert_eq!(json["settlement"]["supported"], false);
    assert_eq!(json["settlement"]["paying"], false);
}

async fn call(
    router: axum::Router,
    method: Method,
    uri: &str,
    origin: Option<&str>,
    body: Option<&str>,
) -> (StatusCode, Value) {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(o) = origin {
        b = b.header("origin", o);
    }
    if body.is_some() {
        b = b.header("content-type", "application/json");
    }
    let resp = send(
        router,
        b.body(body.map_or_else(Body::empty, |s| Body::from(s.to_string())))
            .unwrap(),
    )
    .await;
    let status = resp.status();
    let bytes = body_bytes(resp).await;
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

fn paying_snapshot() -> ant_control::StatusSnapshot {
    let mut snap = snapshot_with_one_peer();
    snap.settlement = ant_control::SettlementInfo {
        swap_enabled: true,
        chequebook: Some("0x370e6965a8c169dbf3456edf852f5a7ffa2f1e81".into()),
        paying: true,
    };
    snap
}

/// `/node` says whether this node settles with SWAP and can switch it at
/// runtime, and the switch's live state (Freedom's capability signal),
/// with bee's `swapEnabled` following the switch.
#[tokio::test]
async fn node_reports_the_settlement_switch() {
    let router = swap_switch_router(paying_snapshot(), ant_gateway::CorsConfig::default());
    let (status, json) = call(router.clone(), Method::GET, "/node", None, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["beeMode"], "light");
    assert_eq!(json["swapEnabled"], true);
    let s = &json["settlement"];
    assert_eq!(s["supported"], true);
    assert_eq!(s["swapSwitch"], true);
    assert_eq!(s["swapEnabled"], true);
    assert_eq!(s["paying"], true);
    assert_eq!(
        s["chequebook"],
        "0x370e6965a8c169dbf3456edf852f5a7ffa2f1e81"
    );

    let (status, _) = call(
        router.clone(),
        Method::PUT,
        "/v0/settlement/swap",
        None,
        Some(r#"{"swapEnabled":false}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, json) = call(router, Method::GET, "/node", None, None).await;
    assert_eq!(json["swapEnabled"], false);
    assert_eq!(json["settlement"]["swapEnabled"], false);
    assert_eq!(json["settlement"]["paying"], false);
}

/// `GET`/`PUT /v0/settlement/swap` read and flip the running node's
/// `swap-enable` through the node loop, answer the state afterwards and
/// say it isn't persisted; a malformed body is refused, and so is a
/// switch from a web page (#105) unless its origin is listed exactly —
/// `null` (dweb pages) and `*` never count.
#[tokio::test]
async fn swap_switch_reads_and_sets_the_running_node() {
    let cors = ant_gateway::CorsConfig::new(["https://wallet.example"]);
    let router = swap_switch_router(paying_snapshot(), cors);
    let (status, json) = call(
        router.clone(),
        Method::GET,
        "/v0/settlement/swap",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["swapEnabled"], true);
    assert_eq!(json["paying"], true);
    assert_eq!(json["persisted"], false);

    let put = |origin, body| {
        let router = router.clone();
        async move {
            call(
                router,
                Method::PUT,
                "/v0/settlement/swap",
                origin,
                Some(body),
            )
            .await
        }
    };
    let (status, json) = put(None, r#"{"swapEnabled":false}"#).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["swapEnabled"], false);
    assert_eq!(json["paying"], false);
    for origin in ["null", "https://evil.example"] {
        let (status, json) = put(Some(origin), r#"{"swapEnabled":true}"#).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{origin}: {json}");
    }
    let (_, json) = call(
        router.clone(),
        Method::GET,
        "/v0/settlement/swap",
        None,
        None,
    )
    .await;
    assert_eq!(json["swapEnabled"], false, "a refused page changed nothing");
    let (status, json) = put(Some("https://wallet.example"), r#"{"swapEnabled":true}"#).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["swapEnabled"], true);
    let (status, _) = put(None, r#"{"swapEnabled":"yes"}"#).await;
    assert!(status.is_client_error(), "{status}");
}

/// `/addresses` echoes the static `GatewayIdentity`. Overlay/publicKey
/// drop the `0x` prefix, ethereum keeps it. `pssPublicKey` is required
/// to be present (bee-js panics on `undefined`); we mirror `publicKey`
/// because we don't run a separate PSS key.
#[tokio::test]
async fn addresses_renders_identity_and_listeners() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/addresses")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let identity = test_identity();
    assert_eq!(json["overlay"], identity.overlay_hex);
    assert_eq!(json["ethereum"], identity.ethereum_hex);
    assert_eq!(json["publicKey"], identity.public_key_hex);
    assert_eq!(json["pssPublicKey"], identity.public_key_hex);
    let underlay = json["underlay"].as_array().expect("underlay array");
    assert!(
        underlay.iter().any(|v| v == "/ip4/127.0.0.1/tcp/1634"),
        "underlay should include the listener: {underlay:?}"
    );
}

/// `/peers` reports BZZ-handshaked peers using their **swarm overlay**
/// (no `0x` prefix), not the libp2p peer id. Pre-handshake peers are
/// filtered out.
#[tokio::test]
async fn peers_lists_handshaked_overlays() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/peers")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let peers = json["peers"].as_array().expect("peers array");
    assert_eq!(peers.len(), 1);
    assert_eq!(
        peers[0]["address"],
        "deadbeef00000000000000000000000000000000000000000000000000000000",
    );
    assert_eq!(peers[0]["fullNode"], true);
}

/// `/topology` mirrors bee's kademlia snapshot layout: a `bins` map with
/// 32 named entries (`bin_0` … `bin_31`) plus the meta fields.
#[tokio::test]
async fn topology_emits_32_bins_and_summary_fields() {
    let router = status_only_router(snapshot_with_one_peer());
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri("/topology")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    let identity = test_identity();
    assert_eq!(json["baseAddr"], identity.overlay_hex);
    assert_eq!(json["population"], 1);
    assert_eq!(json["connected"], 1);
    let bins = json["bins"].as_object().expect("bins map");
    assert_eq!(bins.len(), 32, "bee snapshot is fixed at 32 bins");
    for i in 0..32 {
        let key = format!("bin_{i}");
        assert!(bins.contains_key(&key), "missing bin {key}");
    }
    assert_eq!(bins["bin_5"]["population"], 1);
    assert_eq!(bins["bin_5"]["connected"], 1);
    assert_eq!(bins["bin_0"]["population"], 0);
    assert!(json["timestamp"].is_string());
    assert!(json["nnLowWatermark"].is_number());
    assert!(json["depth"].is_number());
    assert!(json["reachability"].is_string());
    assert!(json["networkAvailability"].is_string());
}
