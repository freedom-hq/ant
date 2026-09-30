//! End-to-end WebSocket tests for `/gsoc/subscribe/{address}` and
//! `/pss/subscribe/{topic}` — a real listener, a real client, real
//! frames. The fixture node (see `common/mod.rs`) delivers one message
//! per subscription and then ends it; an all-`0xEE` GSOC address
//! simulates the registry's neighborhood-cap rejection.

mod common;

use common::handle_with_fixture_node;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;

/// Serve the fixture router on an ephemeral port; return its base URL.
async fn serve() -> String {
    let router = handle_with_fixture_node();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, router).await.expect("serve");
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn gsoc_subscribe_delivers_binary_frames_then_closes_with_reason() {
    let base = serve().await;
    let url = format!("{base}/gsoc/subscribe/{}", "ab".repeat(32));
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("ws");

    // The fixture's single delivery arrives as a binary frame — the
    // bee-compatible shape bee-js `gsocSubscribe` consumes.
    let frame = ws.next().await.expect("frame").expect("ok");
    assert_eq!(
        frame,
        Message::Binary(b"fixture-lurker-payload".to_vec().into())
    );

    // The fixture then ends the subscription: the gateway must close
    // WITH a reason, not silently drop the socket.
    let close = ws.next().await.expect("close frame").expect("ok");
    match close {
        Message::Close(Some(cf)) => {
            assert_eq!(cf.code, CloseCode::Away);
            assert!(
                cf.reason.contains("closed by node"),
                "unexpected close reason: {}",
                cf.reason
            );
        }
        other => panic!("expected a Close frame with a reason, got {other:?}"),
    }
}

#[tokio::test]
async fn pss_subscribe_delivers_the_topic_message() {
    let base = serve().await;
    let (mut ws, _resp) = tokio_tungstenite::connect_async(format!("{base}/pss/subscribe/test"))
        .await
        .expect("ws");
    let frame = ws.next().await.expect("frame").expect("ok");
    assert_eq!(
        frame,
        Message::Binary(b"fixture-lurker-payload".to_vec().into())
    );
}

/// Mailbox mode: `?history=true` reaches the node as
/// `LurkerSubscribe { history: true }` (the fixture echoes the flag into
/// its payload), and is off by default — guards the query wiring end to
/// end. The sweep itself is unit-tested in `ant_p2p` (`sweep_window`,
/// the registry's ticket routing).
#[tokio::test]
async fn pss_subscribe_forwards_history_query_to_the_node() {
    let base = serve().await;
    for (query, expected) in [
        ("?history=true", &b"fixture-lurker-payload+history"[..]),
        ("?history=false", &b"fixture-lurker-payload"[..]),
        ("", &b"fixture-lurker-payload"[..]),
    ] {
        let url = format!("{base}/pss/subscribe/test{query}");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("ws");
        let frame = ws.next().await.expect("frame").expect("ok");
        assert_eq!(
            frame,
            Message::Binary(expected.to_vec().into()),
            "query {query:?}"
        );
    }
}

/// GSOC has no mailbox: the gateway never forwards `history` for it,
/// even if a client appends `?history=true`.
#[tokio::test]
async fn gsoc_subscribe_never_requests_history() {
    let base = serve().await;
    let url = format!("{base}/gsoc/subscribe/{}?history=true", "ab".repeat(32));
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("ws");
    let frame = ws.next().await.expect("frame").expect("ok");
    assert_eq!(
        frame,
        Message::Binary(b"fixture-lurker-payload".to_vec().into())
    );
}

#[tokio::test]
async fn capped_subscription_is_rejected_with_a_close_reason() {
    let base = serve().await;
    let url = format!("{base}/gsoc/subscribe/{}", "ee".repeat(32));
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("ws");

    // Rejection must be visible to the client: Close code 1013 ("try
    // again later") and a reason naming the cap — a silent drop would
    // invite a blind retry loop against the same refusal.
    let close = ws.next().await.expect("close frame").expect("ok");
    match close {
        Message::Close(Some(cf)) => {
            assert_eq!(cf.code, CloseCode::Again);
            assert!(
                cf.reason.contains("neighborhood limit"),
                "unexpected close reason: {}",
                cf.reason
            );
        }
        other => panic!("expected a Close frame with a reason, got {other:?}"),
    }
}

#[tokio::test]
async fn invalid_gsoc_address_is_rejected_before_upgrade() {
    let base = serve().await;
    let url = format!("{base}/gsoc/subscribe/not-hex");
    let err = tokio_tungstenite::connect_async(&url)
        .await
        .expect_err("must not upgrade");
    match err {
        tokio_tungstenite::tungstenite::Error::Http(resp) => {
            assert_eq!(resp.status(), 400);
        }
        other => panic!("expected an HTTP 400 rejection, got {other:?}"),
    }
}

#[tokio::test]
async fn server_answers_client_pings() {
    let base = serve().await;
    let url = format!("{base}/gsoc/subscribe/{}", "cd".repeat(32));
    let (mut ws, _resp) = tokio_tungstenite::connect_async(&url).await.expect("ws");
    ws.send(Message::Ping(b"hb".to_vec().into()))
        .await
        .expect("ping");
    // Expect a Pong among the next frames (the fixture's delivery may
    // interleave).
    for _ in 0..3 {
        if let Message::Pong(p) = ws.next().await.expect("frame").expect("ok") {
            assert_eq!(p.as_ref(), b"hb");
            return;
        }
    }
    panic!("no pong received");
}
