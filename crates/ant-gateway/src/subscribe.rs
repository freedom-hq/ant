//! GSOC/PSS subscription `WebSockets` — bee's `GET /gsoc/subscribe/{address}`
//! and `GET /pss/subscribe/{topic}`.
//!
//! Each endpoint opens a lurker subscription on the node (see
//! `ant_p2p::lurker`): the node resides in the relevant neighborhood,
//! pulls its bin live, decodes matching GSOC/PSS messages, and streams
//! their payloads back. This handler forwards each payload to the client
//! as a **binary** WebSocket frame — bee-compatible, so bee-js
//! `gsocSubscribe` / `pssSubscribe` work unmodified.
//!
//! - `/gsoc/subscribe/{address}`: `{address}` is the 32-byte SOC address
//!   (`keccak256(id ‖ owner)`) the app mined; the node watches exactly it
//!   and resides in its neighborhood.
//! - `/pss/subscribe/{topic}`: `{topic}` is the topic string
//!   (keccak256-hashed, bee's `NewTopic`); the node watches that topic in
//!   its own neighborhood (where PSS messages to this node land) and
//!   receives topic-broadcast messages.

use ant_control::{ControlAck, ControlCommand};
use axum::extract::ws::{close_code, CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::debug;

use crate::error::{params_error, parse_hex_param, ParamKind};
use crate::handle::GatewayHandle;

/// Depth of the node→gateway message buffer for one subscription.
const SUB_CHANNEL_CAP: usize = 64;

/// Server → client Ping cadence. A subscription holds a lurker slot
/// (`MAX_LURKER_NEIGHBORHOODS` node-wide), so a dead client must not pin
/// one until the kernel notices: we ping like bee's API does and drop
/// the socket when the client stops answering.
const WS_PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// How long a client may go without any inbound frame (Pong, data,
/// Close) before the subscription is torn down. Three missed pings.
const WS_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// Optional query for `/pss/subscribe`.
#[derive(Debug, Deserialize)]
pub struct PssSubscribeQuery {
    /// Rendezvous-neighborhood override (64-hex overlay). When set, the
    /// node lurks *this* neighborhood instead of its own overlay — the
    /// basis for many-to-many rooms that no participant owns: every client
    /// lurks the same topic-derived neighborhood and senders address
    /// messages there. Absent ⇒ the node's own neighborhood (directed PSS
    /// to this node).
    neighborhood: Option<String>,
    /// **Mailbox mode** (`?history=true`): on subscribe, sweep a bounded
    /// recent window of the trojan-bin backlog so messages sent while
    /// this client was offline are delivered to it (alongside live
    /// traffic) — not just tail from now. Recent, not complete, on a busy
    /// bin; see `ant_p2p::lurker::HISTORY_BACKLOG`.
    #[serde(default)]
    history: bool,
}

/// `GET /gsoc/subscribe/{address}` upgrade handler.
pub async fn gsoc_subscribe(
    State(handle): State<GatewayHandle>,
    Path(address_hex): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    let mut reasons = Vec::new();
    let Some(address) = parse_hex_param::<32>("address", &address_hex, &mut reasons) else {
        return params_error(ParamKind::Path, reasons);
    };
    // Watch exactly this SOC address; reside in its neighborhood.
    // GSOC has no mailbox mode (a SOC has a latest value, not a message
    // backlog) — always live, even when it shares a lurker with a
    // history PSS subscriber: sweeps only pull PSS bins, only emit PSS
    // messages, and the registry routes backlog to the requesting
    // subscriber alone.
    let cmd_target = address;
    ws.on_upgrade(move |socket| {
        run_subscription(handle, socket, cmd_target, vec![address], Vec::new(), false)
    })
}

/// `GET /pss/subscribe/{topic}` upgrade handler.
pub async fn pss_subscribe(
    State(handle): State<GatewayHandle>,
    Path(topic_str): Path<String>,
    Query(query): Query<PssSubscribeQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let topic = ant_crypto::pss::topic_from_string(&topic_str);
    // With `?neighborhood=`, lurk that rendezvous neighborhood; otherwise
    // all-zeros → the node's own neighborhood (directed PSS to this node).
    let target = match &query.neighborhood {
        Some(hex) => {
            let mut reasons = Vec::new();
            match parse_hex_param::<32>("neighborhood", hex, &mut reasons) {
                Some(t) => t,
                None => return params_error(ParamKind::Query, reasons),
            }
        }
        None => [0u8; 32],
    };
    let history = query.history;
    ws.on_upgrade(move |socket| {
        run_subscription(handle, socket, target, Vec::new(), vec![topic], history)
    })
}

/// Drive one subscription: open the lurker on the node, forward each
/// decoded payload to the client as a binary frame, and tear down when
/// either side closes.
async fn run_subscription(
    handle: GatewayHandle,
    mut socket: WebSocket,
    target: [u8; 32],
    gsoc_addresses: Vec<[u8; 32]>,
    pss_topics: Vec<[u8; 32]>,
    history: bool,
) {
    let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(SUB_CHANNEL_CAP);
    let cmd = ControlCommand::LurkerSubscribe {
        target,
        gsoc_addresses,
        pss_topics,
        history,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        let _ = socket
            .send(close_frame(close_code::ERROR, "node unavailable"))
            .await;
        return;
    }

    // Keep-alive: ping on a fixed cadence and require *some* inbound
    // frame (Pong, data, Close) within WS_IDLE_TIMEOUT. Without this a
    // dead/abandoned client pins its lurker slot until the kernel gives
    // up on the connection — and the slots are a small node-wide pool.
    let mut ping = tokio::time::interval(WS_PING_INTERVAL);
    ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ping.tick().await; // immediate first tick — consume it
    let mut last_inbound = Instant::now();

    loop {
        tokio::select! {
            // A decoded message from the node → binary frame to the client.
            frame = ack_rx.recv() => match frame {
                Some(ControlAck::LurkerMessage { payload, .. }) => {
                    if socket.send(Message::Binary(payload.into())).await.is_err() {
                        break;
                    }
                }
                // Rejection (neighborhood cap, nothing to watch): tell
                // the client why so it doesn't blind-retry into the
                // same refusal.
                Some(ControlAck::Error { message }) => {
                    let _ = socket
                        .send(close_frame(close_code::AGAIN, &message))
                        .await;
                    break;
                }
                Some(_) => {} // no other ack kinds on this stream
                None => {
                    // Node closed the subscription.
                    let _ = socket
                        .send(close_frame(close_code::AWAY, "subscription closed by node"))
                        .await;
                    break;
                }
            },
            // Client activity: a Close (or a socket error) ends the sub.
            // Dropping `ack_rx` stops the node-side lurker. Any inbound
            // frame — Pong included (axum answers client Pings itself) —
            // counts as liveness.
            client = socket.recv() => match client {
                Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                Some(Ok(_)) => last_inbound = Instant::now(),
            },
            _ = ping.tick() => {
                if last_inbound.elapsed() > WS_IDLE_TIMEOUT {
                    debug!(target: "ant_gateway", "subscription client unresponsive; dropping");
                    break;
                }
                if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
        }
    }
    debug!(target: "ant_gateway", "subscription closed");
}

/// Build a Close frame carrying a machine-actionable code and a human
/// reason (bee likewise closes subscription sockets with a reason).
fn close_frame(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code,
        reason: truncate_close_reason(reason).into(),
    }))
}

/// RFC 6455 caps a Close payload at 125 bytes, 2 of which are the code —
/// so the reason must fit 123 **bytes**, not chars. Truncate on a char
/// boundary at or below 120 bytes; a char-count cap would let a
/// multibyte-UTF-8 reason overflow the frame and make the send fail
/// (dropping the reason entirely — the very thing the frame exists for).
fn truncate_close_reason(reason: &str) -> &str {
    if reason.len() <= 120 {
        return reason;
    }
    let mut end = 120;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

#[cfg(test)]
mod tests {
    use super::truncate_close_reason;

    #[test]
    fn close_reason_truncates_on_byte_boundaries() {
        // Short ASCII passes through untouched.
        assert_eq!(truncate_close_reason("cap reached"), "cap reached");
        // Long ASCII cuts at exactly 120 bytes.
        let long = "x".repeat(200);
        assert_eq!(truncate_close_reason(&long).len(), 120);
        // Multibyte: 3-byte chars, 120 not on a boundary (40×3 = 120 is,
        // so use 4-byte emoji: 120/4 = 30 exact; shift with a prefix).
        let tricky = format!("ab{}", "\u{1F980}".repeat(40)); // 2 + 160 bytes
        let cut = truncate_close_reason(&tricky);
        assert!(cut.len() <= 120, "must fit RFC 6455's byte budget");
        assert!(cut.is_char_boundary(cut.len()), "must stay valid UTF-8");
        assert_eq!(cut.len(), 118); // 2 + 29×4 — boundary below 120
    }
}
