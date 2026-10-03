//! Bee-parity read-only settlement/balance endpoints.
//!
//! * `GET /balances` + `/balances/{peer}` (`bee/pkg/api/balances.go`,
//!   `compensatedBalancesHandler`): per-peer accounting balances,
//!   bee's sign convention (positive = the peer owes us). Bee's
//!   handlers never populate `thresholdreceived`/`thresholdgiven`, so
//!   they render as JSON `null` — matched here verbatim.
//! * `GET /consumed` + `/consumed/{peer}`: bee's *uncompensated*
//!   balances (`balancesHandler`). Bee's compensated balance is
//!   `balance − surplus`; ant's light-client mirror has no surplus
//!   tracking (we never receive payments beyond our debt), so both
//!   families render the same number.
//! * `GET /settlements` + `/settlements/{peer}`
//!   (`bee/pkg/api/settlements.go`): SWAP (cheque) totals. `sent` =
//!   the outbound ledger's cumulative payout per peer, `received` =
//!   the inbound credit ledger's accepted cumulative.
//! * `GET /timesettlements`: pseudosettle totals, same JSON shape.
//!   Ant refreshes only as a client, and its inbound pseudosettle
//!   handler always acks zero, so `received` is always `"0"`.
//! * `GET /chequebook/cheque` + `/chequebook/cheque/{peer}`
//!   (`bee/pkg/api/chequebook.go`, `chequebookAllLastHandler` /
//!   `chequebookLastPeerHandler`): last cheques per peer. Bee's peer
//!   handler tolerates `chequebook.ErrNoCheque` on either side, so an
//!   unknown peer gets `200` with `lastreceived`/`lastsent` = `null`
//!   — not a 404.
//!
//! All amounts are bee's `bigint` rendering: decimal strings. All
//! data comes from one `ControlCommand::AccountingSnapshot`; a fresh
//! node (or `MemNode`) acks an empty snapshot, which renders as bee's
//! empty-collection shapes.

use ant_control::{AccountingSnapshotView, ControlAck, ControlCommand, PeerAccountingView};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::warn;

use crate::error::{go_invalid_hex_byte, json_error, params_error, ParamKind, Reason};
use crate::handle::GatewayHandle;
use crate::retrieval::{json_response, node_unavailable};
use crate::stewardship::eip55_address;

/// The node loop answers `AccountingSnapshot` synchronously from
/// in-memory state, so a short fixed timeout is plenty — no
/// `x-request-timeout` plumbing like the content endpoints need.
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);

/// Bee's `errNoBalance` (`balances.go`).
const ERR_NO_BALANCE: &str = "No balance for peer";
/// Bee's `settlement.ErrPeerNoSettlements` (`pkg/settlement`).
const ERR_NO_SETTLEMENTS: &str = "no settlements for peer";

/// Fetch the settlement/balance snapshot from the node loop.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
pub(crate) async fn fetch_snapshot(
    handle: &GatewayHandle,
) -> Result<AccountingSnapshotView, Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    if handle
        .commands
        .send(ControlCommand::AccountingSnapshot { ack: ack_tx })
        .await
        .is_err()
    {
        return Err(node_unavailable());
    }
    let ack = match tokio::time::timeout(SNAPSHOT_TIMEOUT, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => return Err(node_unavailable()),
        Err(_) => {
            return Err(json_error(
                StatusCode::GATEWAY_TIMEOUT,
                "accounting snapshot timed out",
            ));
        }
    };
    match ack {
        ControlAck::Accounting(snapshot) => Ok(snapshot),
        ControlAck::Error { message } => {
            warn!(target: "ant_gateway", %message, "accounting snapshot failed");
            Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Cannot get balances",
            ))
        }
        other => {
            warn!(target: "ant_gateway", ?other, "unexpected ack from AccountingSnapshot");
            Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unexpected node ack",
            ))
        }
    }
}

/// Parse the `{peer}` path segment the way bee's `mapStructure` hook
/// parses a `swarm.Address`: any even-length hex is a valid address
/// (bee doesn't enforce 32 bytes — a short address simply never
/// matches a stored record). Returns the lowercase hex key used to
/// match snapshot rows.
#[allow(clippy::result_large_err)] // Response-as-Err is the house pattern (see retrieval.rs)
fn parse_peer_param(value: &str) -> Result<String, Response> {
    let lower = value.to_ascii_lowercase();
    if let Some(bad) = lower.chars().find(|c| !c.is_ascii_hexdigit()) {
        return Err(params_error(
            ParamKind::Path,
            vec![Reason::new("peer", go_invalid_hex_byte(bad))],
        ));
    }
    if !lower.len().is_multiple_of(2) {
        return Err(params_error(
            ParamKind::Path,
            vec![Reason::new("peer", "odd length hex string")],
        ));
    }
    Ok(lower)
}

/// Bee's balance sign convention is "positive = the peer owes us".
/// Ant's mirror tracks the opposite single direction (our debt toward
/// the peer), so the rendered balance is the negation.
fn render_balance(debt_to_peer: &str) -> String {
    if debt_to_peer == "0" {
        "0".to_string()
    } else {
        format!("-{debt_to_peer}")
    }
}

/// One entry of bee's `balancesResponse`. The two threshold fields are
/// declared in bee's `balanceResponse` struct but never populated by
/// any `/balances` / `/consumed` handler, so they marshal as `null`.
fn balance_entry(peer: &str, debt_to_peer: &str) -> Value {
    json!({
        "peer": peer,
        "balance": render_balance(debt_to_peer),
        "thresholdreceived": Value::Null,
        "thresholdgiven": Value::Null,
    })
}

/// `GET /balances` and `GET /consumed` share one renderer: bee's
/// compensated balance subtracts the surplus, which is identically
/// zero in ant's one-directional mirror.
async fn balances_list(handle: GatewayHandle) -> Response {
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let balances: Vec<Value> = snapshot
        .peers
        .iter()
        .filter_map(|row| {
            row.debt_to_peer
                .as_deref()
                .map(|debt| balance_entry(&row.peer, debt))
        })
        .collect();
    json_response(StatusCode::OK, &json!({ "balances": balances }))
}

async fn balances_peer(handle: GatewayHandle, peer: String) -> Response {
    let peer = match parse_peer_param(&peer) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let debt = snapshot
        .peers
        .iter()
        .find(|row| row.peer == peer)
        .and_then(|row| row.debt_to_peer.clone());
    match debt {
        Some(debt) => json_response(StatusCode::OK, &balance_entry(&peer, &debt)),
        // Bee's `accounting.ErrPeerNoBalance` mapping.
        None => json_error(StatusCode::NOT_FOUND, ERR_NO_BALANCE),
    }
}

/// `GET /balances` — bee's *compensated* balances.
pub async fn balances(State(handle): State<GatewayHandle>) -> Response {
    balances_list(handle).await
}

/// `GET /balances/{peer}`.
pub async fn peer_balance(
    State(handle): State<GatewayHandle>,
    Path(peer): Path<String>,
) -> Response {
    balances_peer(handle, peer).await
}

/// `GET /consumed` — bee's uncompensated balances (same numbers in
/// ant; see the module docs).
pub async fn consumed(State(handle): State<GatewayHandle>) -> Response {
    balances_list(handle).await
}

/// `GET /consumed/{peer}`.
pub async fn peer_consumed(
    State(handle): State<GatewayHandle>,
    Path(peer): Path<String>,
) -> Response {
    balances_peer(handle, peer).await
}

/// School-book addition of two non-negative decimal strings, so
/// settlement totals never overflow a machine word (cheque cumulative
/// payouts are U256 on the wire).
fn add_dec(a: &str, b: &str) -> String {
    let (long, short) = if a.len() >= b.len() { (a, b) } else { (b, a) };
    let long = long.as_bytes();
    let short = short.as_bytes();
    let mut out = Vec::with_capacity(long.len() + 1);
    let mut carry = 0u8;
    for i in 0..long.len() {
        let da = long[long.len() - 1 - i] - b'0';
        let db = if i < short.len() {
            short[short.len() - 1 - i] - b'0'
        } else {
            0
        };
        let sum = da + db + carry;
        out.push(b'0' + (sum % 10));
        carry = sum / 10;
    }
    if carry > 0 {
        out.push(b'0' + carry);
    }
    out.reverse();
    let s = String::from_utf8(out).expect("digits are ascii");
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Bee's `settlementsResponse` for a row source: `sent`/`received`
/// accessors return `None` when the peer has no record of that kind.
fn settlements_body<S, R>(rows: &[PeerAccountingView], sent: S, received: R) -> Value
where
    S: Fn(&PeerAccountingView) -> Option<&str>,
    R: Fn(&PeerAccountingView) -> Option<&str>,
{
    let mut total_sent = "0".to_string();
    let mut total_received = "0".to_string();
    let mut settlements = Vec::new();
    for row in rows {
        let s = sent(row);
        let r = received(row);
        if s.is_none() && r.is_none() {
            continue;
        }
        let s = s.unwrap_or("0");
        let r = r.unwrap_or("0");
        total_sent = add_dec(&total_sent, s);
        total_received = add_dec(&total_received, r);
        settlements.push(json!({
            "peer": row.peer,
            "received": r,
            "sent": s,
        }));
    }
    json!({
        "totalReceived": total_received,
        "totalSent": total_sent,
        "settlements": settlements,
    })
}

/// `GET /settlements` — SWAP (cheque) settlements.
pub async fn settlements(State(handle): State<GatewayHandle>) -> Response {
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    json_response(
        StatusCode::OK,
        &settlements_body(
            &snapshot.peers,
            |row| row.swap_sent.as_deref(),
            |row| row.swap_received.as_deref(),
        ),
    )
}

/// `GET /settlements/{peer}` — 404 `no settlements for peer` when the
/// peer has neither a sent nor a received SWAP record (bee's
/// `settlement.ErrPeerNoSettlements` on both lookups).
pub async fn peer_settlements(
    State(handle): State<GatewayHandle>,
    Path(peer): Path<String>,
) -> Response {
    let peer = match parse_peer_param(&peer) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let row = snapshot.peers.iter().find(|row| row.peer == peer);
    let (sent, received) = match row {
        Some(row) => (row.swap_sent.as_deref(), row.swap_received.as_deref()),
        None => (None, None),
    };
    if sent.is_none() && received.is_none() {
        return json_error(StatusCode::NOT_FOUND, ERR_NO_SETTLEMENTS);
    }
    json_response(
        StatusCode::OK,
        &json!({
            "peer": peer,
            "received": received.unwrap_or("0"),
            "sent": sent.unwrap_or("0"),
        }),
    )
}

/// `GET /timesettlements` — pseudosettle settlements, same shape as
/// `/settlements`. `received` is structurally `"0"`: ant's inbound
/// pseudosettle handler always acks zero acceptance.
pub async fn timesettlements(State(handle): State<GatewayHandle>) -> Response {
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    json_response(
        StatusCode::OK,
        &settlements_body(&snapshot.peers, |row| row.time_sent.as_deref(), |_| None),
    )
}

/// Bee's `chequebookLastChequePeerResponse`: addresses are Go
/// `common.Address.String()` — EIP-55 checksummed with `0x`.
fn cheque_json(view: &ant_control::LastChequeView) -> Value {
    let checksum = |hex_str: &str| -> Value {
        let mut raw = [0u8; 20];
        match hex::decode_to_slice(hex_str, &mut raw) {
            Ok(()) => Value::String(eip55_address(&raw)),
            Err(_) => Value::String(format!("0x{hex_str}")),
        }
    };
    json!({
        "beneficiary": checksum(&view.beneficiary),
        "chequebook": checksum(&view.chequebook),
        "payout": view.payout,
    })
}

fn last_cheques_entry(row: &PeerAccountingView) -> Value {
    json!({
        "peer": row.peer,
        "lastreceived": row.cheque_received.as_ref().map(cheque_json),
        "lastsent": row.cheque_sent.as_ref().map(cheque_json),
    })
}

/// `GET /chequebook/cheque` — bee's `chequebookAllLastHandler`.
pub async fn chequebook_cheque(State(handle): State<GatewayHandle>) -> Response {
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let lastcheques: Vec<Value> = snapshot
        .peers
        .iter()
        .filter(|row| row.cheque_sent.is_some() || row.cheque_received.is_some())
        .map(last_cheques_entry)
        .collect();
    json_response(StatusCode::OK, &json!({ "lastcheques": lastcheques }))
}

/// `GET /chequebook/cheque/{peer}` — bee's `chequebookLastPeerHandler`
/// tolerates `ErrNoCheque` on both sides, so an unknown peer is `200`
/// with both cheques `null`, not a 404.
pub async fn chequebook_cheque_peer(
    State(handle): State<GatewayHandle>,
    Path(peer): Path<String>,
) -> Response {
    let peer = match parse_peer_param(&peer) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let snapshot = match fetch_snapshot(&handle).await {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let (lastreceived, lastsent) =
        snapshot
            .peers
            .iter()
            .find(|row| row.peer == peer)
            .map_or((None, None), |row| {
                (
                    row.cheque_received.as_ref().map(cheque_json),
                    row.cheque_sent.as_ref().map(cheque_json),
                )
            });
    json_response(
        StatusCode::OK,
        &json!({
            "peer": peer,
            "lastreceived": lastreceived,
            "lastsent": lastsent,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::{add_dec, render_balance};

    #[test]
    fn decimal_addition_handles_carries_and_zero() {
        assert_eq!(add_dec("0", "0"), "0");
        assert_eq!(add_dec("999", "1"), "1000");
        assert_eq!(add_dec("1", "999"), "1000");
        assert_eq!(add_dec("123456789", "987654321"), "1111111110");
        // Beyond u64: 2^64 + 2^64.
        assert_eq!(
            add_dec("18446744073709551616", "18446744073709551616"),
            "36893488147419103232"
        );
    }

    #[test]
    fn balance_negates_debt_per_bee_sign_convention() {
        assert_eq!(render_balance("0"), "0");
        assert_eq!(render_balance("1350000"), "-1350000");
    }
}
