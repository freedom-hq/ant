//! Chain-backed endpoints `/wallet`, `/chequebook/*`, `/status`,
//! `/chainstate` (PLAN.md J.5 A2/A3/D1/D2). Driven with a deterministic
//! fake [`ChainReader`] so the bee response shapes + the no-chain
//! fallbacks are locked without a live RPC.

mod common;

use std::sync::Arc;

use ant_gateway::{
    ChainContext, ChainReader, ChainWriter, ChequebookRefusal, ChequebookSlot, CorsConfig,
    DepositView, FundingFailure, FundingView, NewBatch, StorageQuoteView, WalletTxLock, WriteGate,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{
    body_bytes, send, snapshot_with_one_peer, status_only_router,
    status_router_recording_registrations, status_router_with_chain,
    status_router_with_chain_and_failing_snapshot, status_router_with_chain_and_hook,
    status_router_with_chain_and_issued, status_router_with_chain_and_issued_by,
    status_router_with_chain_and_lost_ledger, status_router_with_chain_hooks_and_cors,
    status_router_with_postage,
};
use serde_json::Value;

/// A fake reader returning fixed values; also records whether it was hit
/// so we can assert the no-chain fallbacks don't call it.
struct FakeChain;

#[async_trait]
impl ChainReader for FakeChain {
    async fn block_number(&self) -> Result<u64, String> {
        Ok(38_123_456)
    }
    async fn current_price(&self) -> Result<u128, String> {
        Ok(24_000)
    }
    async fn total_amount(&self) -> Result<u128, String> {
        Ok(987_654_321)
    }
    async fn bzz_balance(&self, _who: [u8; 20]) -> Result<u128, String> {
        Ok(FAKE_BZZ_BALANCE)
    }
    async fn native_balance(&self, _who: [u8; 20]) -> Result<u128, String> {
        Ok(250_000_000_000_000_000)
    }
    async fn chequebook_balance(&self, _cb: [u8; 20]) -> Result<u128, String> {
        Ok(42_000_000)
    }
    async fn chequebook_total_paid_out(&self, _cb: [u8; 20]) -> Result<u128, String> {
        Ok(3_000_000)
    }
    /// Every batch is depth [`FAKE_BATCH_DEPTH`] and the node wallet's
    /// (`0x11…`), except [`FOREIGN_BATCH`] (someone else's) and
    /// [`MISSING_BATCH`] (zero owner: not on chain).
    async fn batch_meta(&self, batch_id: [u8; 32]) -> Result<ant_gateway::BatchMetaView, String> {
        let owner = match batch_id {
            FOREIGN_BATCH => [0x22; 20],
            MISSING_BATCH => [0; 20],
            _ => [0x11; 20],
        };
        Ok(ant_gateway::BatchMetaView {
            owner,
            depth: FAKE_BATCH_DEPTH,
            bucket_depth: 16,
            immutable: true,
        })
    }
    /// Every batch has balance left, except [`EXPIRED_BATCH`] (`0`, as
    /// the contract clamps an expired batch) and [`MISSING_BATCH`]
    /// (`remainingBalance` reverts for a batch the contract lacks).
    async fn batch_remaining_balance(&self, batch_id: [u8; 32]) -> Result<u128, String> {
        match batch_id {
            EXPIRED_BATCH => Ok(0),
            MISSING_BATCH => Err("execution reverted".into()),
            NEAR_FLOOR_BATCH => Ok(601_000),
            _ => Ok(1_000_000),
        }
    }
    /// [`FAKE_MIN_BALANCE`]: the `1_000_000` left on a batch covers a
    /// dilute of up to two levels (`250_000` per chunk, `178_000` once
    /// the gateway takes 12 blocks of payout at `24_000` off), not three.
    async fn minimum_balance_per_chunk(&self) -> Result<u128, String> {
        Ok(FAKE_MIN_BALANCE)
    }
}

/// [`FakeChain`]'s `minimumInitialBalancePerChunk`.
const FAKE_MIN_BALANCE: u128 = 150_000;

/// [`FakeChain`]'s node-wallet xBZZ balance.
const FAKE_BZZ_BALANCE: u128 = 1_500_000_000_000_000_000;
/// [`FakeChain`]'s depth for every batch.
const FAKE_BATCH_DEPTH: u8 = 20;
const FOREIGN_BATCH: [u8; 32] = [0xF0; 32];
const MISSING_BATCH: [u8; 32] = [0x0E; 32];
const EXPIRED_BATCH: [u8; 32] = [0xE1; 32];
/// A batch whose balance (`601_000`) halved twice (`150_250`) clears
/// [`FAKE_MIN_BALANCE`] as of the read, but not once a few blocks of
/// payout have come off it by the time the dilute would execute.
const NEAR_FLOOR_BATCH: [u8; 32] = [0x5E; 32];

fn chain_ctx(chequebook: Option<[u8; 20]>) -> Arc<ChainContext> {
    Arc::new(ChainContext {
        reader: Arc::new(FakeChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(chequebook),
        chain_id: 100,
        writer: None,
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    })
}

/// Fake writer echoing its inputs so handler plumbing (path/query
/// parsing, response shape) is testable without a live chain.
struct FakeWriter;

#[async_trait]
impl ChainWriter for FakeWriter {
    async fn buy_batch(
        &self,
        _amount: u128,
        _depth: u8,
        _immutable: bool,
    ) -> Result<NewBatch, String> {
        Ok(NewBatch {
            id: [0x7E; 32],
            block: 48_500_001,
        })
    }
    async fn topup_batch(&self, _id: [u8; 32], _amount: u128) -> Result<(), String> {
        Ok(())
    }
    async fn dilute_batch(&self, _id: [u8; 32], _depth: u8) -> Result<(), String> {
        Ok(())
    }
    async fn deposit_chequebook(&self, _amount: u128) -> Result<[u8; 32], String> {
        Ok([0xD0; 32])
    }
}

fn chain_ctx_rw(chequebook: Option<[u8; 20]>) -> Arc<ChainContext> {
    Arc::new(ChainContext {
        reader: Arc::new(FakeChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(chequebook),
        chain_id: 100,
        writer: Some(Arc::new(FakeWriter)),
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    })
}

async fn req(router: axum::Router, method: Method, uri: &str) -> (StatusCode, Value) {
    let resp = send(
        router,
        Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let status = resp.status();
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    (status, json)
}

async fn get(router: axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = send(
        router,
        Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    let status = resp.status();
    let json: Value = serde_json::from_slice(&body_bytes(resp).await).unwrap();
    (status, json)
}

#[tokio::test]
async fn wallet_reports_real_balances_when_chain_configured() {
    let cb = [0xAB; 20];
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(Some(cb)));
    let (status, json) = get(router, "/wallet").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["bzzBalance"], "1500000000000000000");
    assert_eq!(json["nativeTokenBalance"], "250000000000000000");
    assert_eq!(json["chainID"], 100);
    // bee-js's WalletBalance parser requires these as strings (issue #5).
    assert_eq!(
        json["walletAddress"],
        format!("0x{}", hex::encode([0x11; 20]))
    );
    assert_eq!(
        json["chequebookContractAddress"],
        format!("0x{}", hex::encode(cb)),
    );
}

#[tokio::test]
async fn wallet_zero_stub_without_chain() {
    let (status, json) = get(status_only_router(snapshot_with_one_peer()), "/wallet").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["bzzBalance"], "0");
    assert_eq!(json["nativeTokenBalance"], "0");
    assert_eq!(json["chainID"], 100);
    // Even the no-chain stub emits the address fields as strings so bee-js
    // doesn't throw on `undefined` (issue #5).
    let zero = "0x0000000000000000000000000000000000000000";
    assert_eq!(json["walletAddress"], zero);
    assert_eq!(json["chequebookContractAddress"], zero);
}

#[tokio::test]
async fn wallet_chequebook_zero_when_not_deployed() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(None));
    let (status, json) = get(router, "/wallet").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["chequebookContractAddress"],
        "0x0000000000000000000000000000000000000000",
    );
}

#[tokio::test]
async fn chequebook_address_reports_configured_address() {
    let cb = [0xAB; 20];
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(Some(cb)));
    let (status, json) = get(router, "/chequebook/address").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["chequebookAddress"], format!("0x{}", hex::encode(cb)));
}

/// A chequebook resolved after startup (a deploy triggered by a stamp
/// buy) shows up without rebuilding the chain context or restarting.
#[tokio::test]
async fn chequebook_set_after_startup_is_reported_live() {
    let ctx = chain_ctx(None);
    let router = status_router_with_chain(snapshot_with_one_peer(), Arc::clone(&ctx));

    let (_, json) = get(router.clone(), "/chequebook/address").await;
    assert_eq!(json["chequebookAddress"], format!("0x{}", "0".repeat(40)));

    let cb = [0xCD; 20];
    ctx.chequebook.set(cb);
    let (status, json) = get(router.clone(), "/chequebook/address").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["chequebookAddress"], format!("0x{}", hex::encode(cb)));
    let (_, json) = get(router, "/wallet").await;
    assert_eq!(
        json["chequebookContractAddress"],
        format!("0x{}", hex::encode(cb))
    );
}

#[tokio::test]
async fn chequebook_address_zero_when_not_deployed() {
    // Chain present but no chequebook → zero sentinel.
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(None));
    let (status, json) = get(router, "/chequebook/address").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["chequebookAddress"],
        "0x0000000000000000000000000000000000000000",
    );
}

#[tokio::test]
async fn chequebook_balance_real_then_zero() {
    let cb = [0xCD; 20];
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(Some(cb)));
    let (status, json) = get(router, "/chequebook/balance").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["totalBalance"], "42000000");
    assert_eq!(json["availableBalance"], "42000000");

    // No chequebook → zeros.
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(None));
    let (_, json) = get(router, "/chequebook/balance").await;
    assert_eq!(json["totalBalance"], "0");
}

/// `availableBalance` is bee's `AvailableBalance`: balance + cashed out
/// − every cheque the node issued, so download and upload cheques show
/// up in it before any peer cashes them (issue #121). Without outbound
/// settlement nothing is issued and it is the balance.
#[tokio::test]
async fn chequebook_available_balance_deducts_issued_cheques() {
    let cb = [0xCD; 20];
    // 42 M balance + 3 M cashed out − 30 M issued.
    let router = status_router_with_chain_and_issued(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        Some(30_000_000),
    );
    let (status, json) = get(router, "/chequebook/balance").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["totalBalance"], "42000000");
    assert_eq!(json["availableBalance"], "15000000");

    // Issued past the funds: nothing available, not a wrap-around.
    let router = status_router_with_chain_and_issued(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        Some(50_000_000),
    );
    let (_, json) = get(router, "/chequebook/balance").await;
    assert_eq!(json["availableBalance"], "0");

    let router =
        status_router_with_chain_and_issued(snapshot_with_one_peer(), chain_ctx(Some(cb)), None);
    let (_, json) = get(router, "/chequebook/balance").await;
    assert_eq!(json["availableBalance"], "42000000");
}

/// The balance read alone answers `/chequebook/balance` (PR #126
/// R1-M3): when the node's snapshot fails, `totalBalance` still comes
/// back, with `availableBalance` at that upper bound and flagged.
#[tokio::test]
async fn chequebook_balance_survives_a_failed_node_snapshot() {
    let router = status_router_with_chain_and_failing_snapshot(
        snapshot_with_one_peer(),
        chain_ctx(Some([0xCD; 20])),
    );
    let (status, json) = get(router, "/chequebook/balance").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["totalBalance"], "42000000");
    assert_eq!(json["availableBalance"], "42000000");
    assert!(json["availableBalanceError"].is_string(), "{json}");

    let router = status_router_with_chain_and_issued(
        snapshot_with_one_peer(),
        chain_ctx(Some([0xCD; 20])),
        Some(30_000_000),
    );
    let (_, json) = get(router, "/chequebook/balance").await;
    assert!(json.get("availableBalanceError").is_none(), "{json}");
}

/// The node's issued total belongs to the chequebook its snapshot names
/// (PR #126 R2-M1): it comes off this chequebook's balance only when
/// that is this chequebook; another chequebook's total, or an
/// unreadable ledger, leaves `availableBalance` at the balance, flagged.
#[tokio::test]
async fn chequebook_available_balance_counts_only_its_own_cheques() {
    let cb = [0xCD; 20];
    let router = status_router_with_chain_and_issued_by(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        Some(30_000_000),
        cb,
    );
    let (_, json) = get(router, "/chequebook/balance").await;
    assert_eq!(json["availableBalance"], "15000000");
    assert!(json.get("availableBalanceError").is_none(), "{json}");

    let router = status_router_with_chain_and_issued_by(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        Some(30_000_000),
        [0xEF; 20],
    );
    let (status, json) = get(router, "/chequebook/balance").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["availableBalance"], "42000000");
    let err = json["availableBalanceError"].as_str().expect("flagged");
    assert!(err.contains("efefef"), "{err}");

    let router = status_router_with_chain_and_issued_by(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        None,
        cb,
    );
    let (_, json) = get(router, "/chequebook/balance").await;
    assert_eq!(json["availableBalance"], "42000000");
    assert!(json["availableBalanceError"].is_string(), "{json}");
}

/// A chequebook whose cheque figures the node lost (its outbound ledger
/// was moved aside, PR #126 R4-M1) says so: `chequeLedgerLost` carries
/// the node's reason, and `availableBalance` isn't computed from a total
/// that misses the earlier cheques.
#[tokio::test]
async fn chequebook_balance_flags_a_lost_cheque_ledger() {
    let cb = [0xCD; 20];
    let router = status_router_with_chain_and_lost_ledger(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        30_000_000,
        cb,
        "moved aside; confirm with --confirm-cheque-liability",
    );
    let (status, json) = get(router, "/chequebook/balance").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["totalBalance"], "42000000");
    assert_eq!(json["availableBalance"], "42000000");
    assert_eq!(
        json["chequeLedgerLost"],
        "moved aside; confirm with --confirm-cheque-liability"
    );
    assert!(json["availableBalanceError"].is_string(), "{json}");

    let router = status_router_with_chain_and_issued_by(
        snapshot_with_one_peer(),
        chain_ctx(Some(cb)),
        Some(30_000_000),
        cb,
    );
    let (_, json) = get(router, "/chequebook/balance").await;
    assert!(json.get("chequeLedgerLost").is_none(), "{json}");
}

#[tokio::test]
async fn status_reports_last_synced_block() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(None));
    let (status, json) = get(router, "/status").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["lastSyncedBlock"], 38_123_456u64);
    assert_eq!(json["beeMode"], "light");
    // bee Status fields bee-js indexes must all be present.
    for field in [
        "overlay",
        "connectedPeers",
        "reserveSize",
        "storageRadius",
        "isReachable",
    ] {
        assert!(json.get(field).is_some(), "missing /status field {field}");
    }
}

#[tokio::test]
async fn status_501_without_chain() {
    let resp = send(
        status_only_router(snapshot_with_one_peer()),
        Request::builder()
            .method(Method::GET)
            .uri("/status")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn chainstate_reports_price_block_total() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(None));
    let (status, json) = get(router, "/chainstate").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["currentPrice"], "24000");
    assert_eq!(json["totalAmount"], "987654321");
    assert_eq!(json["block"], 38_123_456u64);
    assert_eq!(json["chainTip"], 38_123_456u64);
}

#[tokio::test]
async fn chainstate_501_without_chain() {
    let resp = send(
        status_only_router(snapshot_with_one_peer()),
        Request::builder()
            .method(Method::GET)
            .uri("/chainstate")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
}

// --- on-chain write endpoints ---

#[tokio::test]
async fn buy_stamp_returns_batch_id() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(None));
    let (status, json) = req(router, Method::POST, "/stamps/1000000/20?immutable=true").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["batchID"], hex::encode([0x7E; 32]));
}

/// A recording after-buy hook plus the batch ids it was called with.
fn recording_hook() -> (
    ant_gateway::BatchBoughtHook,
    Arc<std::sync::Mutex<Vec<[u8; 32]>>>,
) {
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let hook: ant_gateway::BatchBoughtHook = Arc::new(move |id| sink.lock().unwrap().push(id));
    (hook, seen)
}

/// The embedder hook (ant-ffi: switch settlement on) runs once the
/// bought batch is registered, with the new batch id.
#[tokio::test]
async fn buy_stamp_calls_the_after_buy_hook() {
    let (hook, seen) = recording_hook();
    let router =
        status_router_with_chain_and_hook(snapshot_with_one_peer(), chain_ctx_rw(None), Some(hook));
    let (status, _) = req(router, Method::POST, "/stamps/1000000/20").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(*seen.lock().unwrap(), vec![[0x7E; 32]]);
}

/// A buy that is refused (nothing bought) must not trigger it.
#[tokio::test]
async fn refused_buy_does_not_call_the_after_buy_hook() {
    let (hook, seen) = recording_hook();
    let router =
        status_router_with_chain_and_hook(snapshot_with_one_peer(), chain_ctx_rw(None), Some(hook));
    let (status, _) = req(router, Method::POST, "/stamps/1000/16").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn buy_stamp_rejects_bad_amount() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(None));
    let (status, _) = req(router, Method::POST, "/stamps/notanumber/20").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn buy_stamp_insufficient_funds_is_bee_400_out_of_funds() {
    // The FakeChain wallet holds 1.5 xBZZ; this buy costs
    // 15355000000000000 × 2^17 ≈ 2013 xBZZ — far more than the balance,
    // so it must fail up front like bee (400 "out of funds") rather than
    // fall through to a reverted tx surfaced as 502 (issue #5).
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(None));
    let (status, json) = req(router, Method::POST, "/stamps/15355000000000000/17").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["code"], 400);
    assert_eq!(json["message"], "out of funds");
}

#[tokio::test]
async fn buy_stamp_rejects_depth_at_or_below_bucket_depth() {
    // bee requires depth > bucketDepth (16); a too-shallow batch would
    // revert on-chain, so reject it with bee's "invalid depth" (issue #5).
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(None));
    let (status, json) = req(router, Method::POST, "/stamps/1000/16").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(json["message"], "invalid depth");
}

#[tokio::test]
async fn topup_and_dilute_echo_batch_id() {
    let id = hex::encode([0xAB; 32]);
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(None));
    let (status, json) = req(router, Method::PATCH, &format!("/stamps/topup/{id}/500")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["batchID"], id);

    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(None));
    let (status, json) = req(router, Method::PATCH, &format!("/stamps/dilute/{id}/22")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["batchID"], id);
}

#[tokio::test]
async fn deposit_returns_tx_hash() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(Some([0xCD; 20])));
    let (status, json) = req(router, Method::POST, "/chequebook/deposit?amount=100000000").await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        json["transactionHash"],
        format!("0x{}", hex::encode([0xD0; 32])),
    );
}

/// An embedder sending from the node wallet outside the gateway (antd's
/// after-buy chequebook top-up) holds the context's `tx_lock`; every
/// write endpoint waits for it, so the two can't race for a nonce or
/// spend the same xBZZ.
#[tokio::test]
async fn write_endpoints_wait_for_the_wallet_tx_lock() {
    let id = hex::encode([0xAB; 32]);
    let dilute = format!("/stamps/dilute/{id}/22");
    let topup = format!("/stamps/topup/{id}/500");
    for (method, uri) in [
        (Method::POST, "/stamps/1000000/20"),
        (Method::PATCH, topup.as_str()),
        (Method::PATCH, dilute.as_str()),
        (Method::POST, "/chequebook/deposit?amount=1"),
    ] {
        let ctx = chain_ctx_rw(Some([0xCD; 20]));
        let held = ctx.tx_lock.clone().lock_owned().await;
        let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
        let (m, u) = (method.clone(), uri.to_string());
        let mut write = tokio::spawn(async move { req(router, m, &u).await });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut write)
                .await
                .is_err(),
            "{method} {uri} sent while another wallet tx held the lock",
        );
        drop(held);
        let (status, _) = tokio::time::timeout(std::time::Duration::from_secs(5), write)
            .await
            .expect("write finishes once the lock is released")
            .unwrap();
        assert!(status.is_success(), "{method} {uri}: {status}");
    }
}

/// `POST /chequebook/deposit` refuses, with a `400` and no transaction,
/// a deposit of nothing and one the wallet can't cover (it would
/// revert on chain and burn the gas). Exactly the balance goes through.
#[tokio::test(start_paused = true)]
async fn deposit_refuses_zero_and_more_than_the_wallet_holds() {
    let over = format!("/chequebook/deposit?amount={}", FAKE_BZZ_BALANCE + 1);
    for (uri, message) in [
        (
            "/chequebook/deposit?amount=0",
            "amount must be more than 0 PLUR",
        ),
        (over.as_str(), "out of funds"),
    ] {
        let writer = Arc::new(FundingWriter::default());
        let router = status_router_with_chain(
            snapshot_with_one_peer(),
            funding_ctx_with_chequebook(writer.clone(), Some([0xCD; 20])),
        );
        let (status, json) = req(router, Method::POST, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(json["message"], message, "{uri}");
        assert!(writer.calls().is_empty(), "{uri}: {:?}", writer.calls());
    }

    // No chequebook: that's the answer, even for an amount the wallet
    // can't cover either (not a masking "out of funds").
    for uri in [over.as_str(), "/chequebook/deposit?amount=1"] {
        let writer = Arc::new(FundingWriter::default());
        let router =
            status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
        let (status, json) = req(router, Method::POST, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            json["message"], "no chequebook configured to deposit into",
            "{uri}"
        );
        assert!(writer.calls().is_empty(), "{uri}: {:?}", writer.calls());
    }

    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(
        snapshot_with_one_peer(),
        funding_ctx_with_chequebook(writer.clone(), Some([0xCD; 20])),
    );
    let all = format!("/chequebook/deposit?amount={FAKE_BZZ_BALANCE}");
    let (status, _) = req(router, Method::POST, &all).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        writer.calls(),
        [format!("deposit_chequebook({FAKE_BZZ_BALANCE})")]
    );
}

/// A deposit whose balance read comes back short is read again for
/// about two blocks before the "out of funds": a lagging RPC backend
/// that catches up within them lets the deposit through, and one that
/// never shows the xBZZ still refuses it, with nothing sent either way.
#[tokio::test(start_paused = true)]
async fn deposit_rereads_a_short_balance_before_refusing() {
    /// [`FakeChain`], but the wallet's xBZZ balance reads `0` for the
    /// first `stale_reads` reads (a backend behind the crediting block).
    struct LaggingBalance {
        stale_reads: usize,
        reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait]
    impl ChainReader for LaggingBalance {
        async fn block_number(&self) -> Result<u64, String> {
            FakeChain.block_number().await
        }
        async fn current_price(&self) -> Result<u128, String> {
            FakeChain.current_price().await
        }
        async fn total_amount(&self) -> Result<u128, String> {
            FakeChain.total_amount().await
        }
        async fn bzz_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            let n = self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.stale_reads {
                Ok(0)
            } else {
                FakeChain.bzz_balance(who).await
            }
        }
        async fn native_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.native_balance(who).await
        }
        async fn chequebook_balance(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_balance(cb).await
        }
        async fn chequebook_total_paid_out(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_total_paid_out(cb).await
        }
    }
    for (stale_reads, want, reads) in [
        (1, StatusCode::CREATED, 2),
        (2, StatusCode::CREATED, 3),
        (usize::MAX, StatusCode::BAD_REQUEST, 3),
    ] {
        let reader = Arc::new(LaggingBalance {
            stale_reads,
            reads: std::sync::atomic::AtomicUsize::default(),
        });
        let writer = Arc::new(FundingWriter::default());
        let ctx = Arc::new(ChainContext {
            reader: reader.clone(),
            wallet_eth: [0x11; 20],
            chequebook: ChequebookSlot::new(Some([0xCD; 20])),
            chain_id: 100,
            writer: Some(writer.clone()),
            writes: WriteGate::default(),
            tx_lock: WalletTxLock::default(),
        });
        let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
        let started = tokio::time::Instant::now();
        let (status, json) = req(router, Method::POST, "/chequebook/deposit?amount=1000").await;
        assert_eq!(status, want, "{stale_reads}: {json}");
        assert_eq!(
            reader.reads.load(std::sync::atomic::Ordering::SeqCst),
            reads,
            "{stale_reads}"
        );
        // Bounded: never more than the two re-read waits.
        assert!(started.elapsed() <= std::time::Duration::from_secs(10));
        if want == StatusCode::CREATED {
            assert_eq!(writer.calls(), ["deposit_chequebook(1000)"]);
        } else {
            assert_eq!(json["message"], "out of funds");
            assert!(writer.calls().is_empty(), "{:?}", writer.calls());
        }
    }
}

/// `PATCH /stamps/dilute` reads the batch first and refuses, with a
/// `400` and no `increaseDepth`, a depth that doesn't raise it, a batch
/// the node doesn't own and an expired one (all would revert on chain). A batch the
/// contract doesn't have is a `404`. One depth up goes through.
#[tokio::test]
async fn dilute_refuses_a_shallower_depth_and_a_foreign_batch() {
    let ours = hex::encode([0xAB; 32]);
    let foreign = hex::encode(FOREIGN_BATCH);
    let missing = hex::encode(MISSING_BATCH);
    let expired = hex::encode(EXPIRED_BATCH);
    let near_floor = hex::encode(NEAR_FLOOR_BATCH);
    for (uri, want) in [
        (
            format!("/stamps/dilute/{ours}/{}", FAKE_BATCH_DEPTH - 1),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/stamps/dilute/{ours}/{FAKE_BATCH_DEPTH}"),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/stamps/dilute/{foreign}/{}", FAKE_BATCH_DEPTH + 1),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("/stamps/dilute/{missing}/{}", FAKE_BATCH_DEPTH + 1),
            StatusCode::NOT_FOUND,
        ),
        (
            format!("/stamps/dilute/{expired}/{}", FAKE_BATCH_DEPTH + 1),
            StatusCode::BAD_REQUEST,
        ),
        // `1_000_000 >> 3` is under the contract's floor:
        // `increaseDepth` would revert with `InsufficientBalance`.
        (
            format!("/stamps/dilute/{ours}/{}", FAKE_BATCH_DEPTH + 3),
            StatusCode::BAD_REQUEST,
        ),
        // Clears the floor at the read's block by 250 PLUR, which a
        // block or two of payout before the transaction lands eats.
        (
            format!("/stamps/dilute/{near_floor}/{}", FAKE_BATCH_DEPTH + 2),
            StatusCode::BAD_REQUEST,
        ),
        (format!("/stamps/dilute/{ours}/60"), StatusCode::BAD_REQUEST),
        // A shift past the balance's width leaves nothing, not a panic.
        (
            format!("/stamps/dilute/{ours}/255"),
            StatusCode::BAD_REQUEST,
        ),
    ] {
        let writer = Arc::new(FundingWriter::default());
        let (router, seen) = status_router_recording_registrations(
            snapshot_with_one_peer(),
            funding_ctx(writer.clone()),
        );
        let (status, json) = req(router, Method::PATCH, &uri).await;
        assert_eq!(status, want, "{uri}: {json}");
        assert!(writer.calls().is_empty(), "{uri}: {:?}", writer.calls());
        assert!(seen.lock().unwrap().is_empty(), "{uri} re-registered");
    }

    let uri = format!("/stamps/dilute/{ours}/{}", FAKE_BATCH_DEPTH + 3);
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let (_, json) = req(router, Method::PATCH, &uri).await;
    assert_eq!(
        json["message"],
        format!(
            "insufficient balance: diluting to depth {} leaves 125000 PLUR per chunk, too \
             close to the contract's minimum of {FAKE_MIN_BALANCE} to still clear it once the \
             transaction lands; top the batch up first",
            FAKE_BATCH_DEPTH + 3
        )
    );

    // One and two levels up (the most the balance covers) go through.
    for levels in [1, 2] {
        let writer = Arc::new(FundingWriter::default());
        let router =
            status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
        let uri = format!("/stamps/dilute/{ours}/{}", FAKE_BATCH_DEPTH + levels);
        let (status, _) = req(router, Method::PATCH, &uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(
            writer.calls(),
            [format!("dilute_batch(ab, {})", FAKE_BATCH_DEPTH + levels)]
        );
    }
}

/// A failed `minimumInitialBalancePerChunk` read fails the dilute
/// (`502`) instead of sending it unchecked.
#[tokio::test]
async fn dilute_fails_when_the_minimum_read_fails() {
    struct NoMinimumReads;
    #[async_trait]
    impl ChainReader for NoMinimumReads {
        async fn block_number(&self) -> Result<u64, String> {
            FakeChain.block_number().await
        }
        async fn current_price(&self) -> Result<u128, String> {
            FakeChain.current_price().await
        }
        async fn total_amount(&self) -> Result<u128, String> {
            FakeChain.total_amount().await
        }
        async fn bzz_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.bzz_balance(who).await
        }
        async fn native_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.native_balance(who).await
        }
        async fn chequebook_balance(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_balance(cb).await
        }
        async fn chequebook_total_paid_out(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_total_paid_out(cb).await
        }
        async fn batch_meta(&self, id: [u8; 32]) -> Result<ant_gateway::BatchMetaView, String> {
            FakeChain.batch_meta(id).await
        }
        async fn batch_remaining_balance(&self, id: [u8; 32]) -> Result<u128, String> {
            FakeChain.batch_remaining_balance(id).await
        }
    }
    let writer = Arc::new(FundingWriter::default());
    let ctx = Arc::new(ChainContext {
        reader: Arc::new(NoMinimumReads),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(None),
        chain_id: 100,
        writer: Some(writer.clone()),
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    });
    let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
    let uri = format!(
        "/stamps/dilute/{}/{}",
        hex::encode([0xAB; 32]),
        FAKE_BATCH_DEPTH + 1
    );
    let (status, _) = req(router, Method::PATCH, &uri).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(writer.calls().is_empty(), "{:?}", writer.calls());
}

/// A failed batch read fails the dilute (`502`) instead of sending it
/// unchecked.
#[tokio::test]
async fn dilute_fails_when_the_batch_read_fails() {
    struct NoBatchReads;
    #[async_trait]
    impl ChainReader for NoBatchReads {
        async fn block_number(&self) -> Result<u64, String> {
            FakeChain.block_number().await
        }
        async fn current_price(&self) -> Result<u128, String> {
            FakeChain.current_price().await
        }
        async fn total_amount(&self) -> Result<u128, String> {
            FakeChain.total_amount().await
        }
        async fn bzz_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.bzz_balance(who).await
        }
        async fn native_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.native_balance(who).await
        }
        async fn chequebook_balance(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_balance(cb).await
        }
        async fn chequebook_total_paid_out(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_total_paid_out(cb).await
        }
    }
    let writer = Arc::new(FundingWriter::default());
    let ctx = Arc::new(ChainContext {
        reader: Arc::new(NoBatchReads),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(None),
        chain_id: 100,
        writer: Some(writer.clone()),
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    });
    let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
    let uri = format!("/stamps/dilute/{}/22", hex::encode([0xAB; 32]));
    let (status, _) = req(router, Method::PATCH, &uri).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(writer.calls().is_empty(), "{:?}", writer.calls());
}

/// A failed `remainingBalance` read (the expiry check) fails the
/// dilute (`502`) instead of sending it unchecked.
#[tokio::test]
async fn dilute_fails_when_the_balance_read_fails() {
    struct NoBalanceReads;
    #[async_trait]
    impl ChainReader for NoBalanceReads {
        async fn block_number(&self) -> Result<u64, String> {
            FakeChain.block_number().await
        }
        async fn current_price(&self) -> Result<u128, String> {
            FakeChain.current_price().await
        }
        async fn total_amount(&self) -> Result<u128, String> {
            FakeChain.total_amount().await
        }
        async fn bzz_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.bzz_balance(who).await
        }
        async fn native_balance(&self, who: [u8; 20]) -> Result<u128, String> {
            FakeChain.native_balance(who).await
        }
        async fn chequebook_balance(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_balance(cb).await
        }
        async fn chequebook_total_paid_out(&self, cb: [u8; 20]) -> Result<u128, String> {
            FakeChain.chequebook_total_paid_out(cb).await
        }
        async fn batch_meta(&self, id: [u8; 32]) -> Result<ant_gateway::BatchMetaView, String> {
            FakeChain.batch_meta(id).await
        }
    }
    let writer = Arc::new(FundingWriter::default());
    let ctx = Arc::new(ChainContext {
        reader: Arc::new(NoBalanceReads),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(None),
        chain_id: 100,
        writer: Some(writer.clone()),
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    });
    let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
    let uri = format!(
        "/stamps/dilute/{}/{}",
        hex::encode([0xAB; 32]),
        FAKE_BATCH_DEPTH + 1
    );
    let (status, _) = req(router, Method::PATCH, &uri).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(writer.calls().is_empty(), "{:?}", writer.calls());
}

/// The node's view of [`MISSING_BATCH`] (which the fake chain reads as
/// zero owner, like an RPC backend that hasn't seen its block yet).
fn missing_batch_view(
    depth: u8,
    registered_secs_ago: Option<u64>,
    block_number: Option<u64>,
) -> ant_control::PostageStatusView {
    ant_control::PostageStatusView {
        enabled: true,
        batch_id: format!("0x{}", hex::encode(MISSING_BATCH)),
        batch_depth: depth,
        bucket_depth: 16,
        registered_secs_ago,
        block_number,
        ..Default::default()
    }
}

/// A batch this node bought moments ago that the chain read can't see
/// yet (zero owner from a lagging RPC backend) is diluted against the
/// node's own view of it, not refused as `404`; a depth that doesn't
/// raise that view's is still a `400`. Outside the grace window, or for
/// a batch the node didn't buy itself, the zero owner stands: `404`.
#[tokio::test]
async fn dilute_trusts_the_node_for_a_batch_it_just_bought() {
    let missing = hex::encode(MISSING_BATCH);
    let up = format!("/stamps/dilute/{missing}/{}", FAKE_BATCH_DEPTH + 1);

    let writer = Arc::new(FundingWriter::default());
    let (router, seen) = status_router_with_postage(
        snapshot_with_one_peer(),
        funding_ctx(writer.clone()),
        vec![missing_batch_view(
            FAKE_BATCH_DEPTH,
            Some(5),
            Some(48_500_001),
        )],
    );
    let (status, json) = req(router, Method::PATCH, &up).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(
        writer.calls(),
        [format!("dilute_batch(0e, {})", FAKE_BATCH_DEPTH + 1)]
    );
    assert_eq!(seen.lock().unwrap().len(), 1);

    let same = format!("/stamps/dilute/{missing}/{FAKE_BATCH_DEPTH}");
    for (uri, view, want) in [
        (
            same.as_str(),
            missing_batch_view(FAKE_BATCH_DEPTH, Some(5), Some(48_500_001)),
            StatusCode::BAD_REQUEST,
        ),
        (
            up.as_str(),
            missing_batch_view(FAKE_BATCH_DEPTH, Some(600), Some(48_500_001)),
            StatusCode::NOT_FOUND,
        ),
        (
            up.as_str(),
            missing_batch_view(FAKE_BATCH_DEPTH, Some(5), None),
            StatusCode::NOT_FOUND,
        ),
        (
            up.as_str(),
            missing_batch_view(FAKE_BATCH_DEPTH, None, Some(48_500_001)),
            StatusCode::NOT_FOUND,
        ),
    ] {
        let writer = Arc::new(FundingWriter::default());
        let (router, seen) = status_router_with_postage(
            snapshot_with_one_peer(),
            funding_ctx(writer.clone()),
            vec![view.clone()],
        );
        let (status, json) = req(router, Method::PATCH, uri).await;
        assert_eq!(status, want, "{uri} {view:?}: {json}");
        assert!(writer.calls().is_empty(), "{uri}: {:?}", writer.calls());
        assert!(seen.lock().unwrap().is_empty(), "{uri} re-registered");
    }
}

#[tokio::test]
async fn deposit_requires_amount() {
    let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx_rw(Some([0xCD; 20])));
    let (status, _) = req(router, Method::POST, "/chequebook/deposit").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn writes_501_without_writer() {
    // Chain present (reader only), but no writer → 501 on every write.
    for (method, uri) in [
        (Method::POST, "/stamps/1000/20"),
        (
            Method::PATCH,
            format!("/stamps/topup/{}/5", hex::encode([0; 32])).leak(),
        ),
        (Method::POST, "/chequebook/deposit?amount=1"),
    ] {
        let router = status_router_with_chain(snapshot_with_one_peer(), chain_ctx(None));
        let resp = send(
            router,
            Request::builder()
                .method(method)
                .uri(uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED, "uri {uri}");
    }
}

// --- xDAI storage funding (`/v0/storage/*`, `/v0/settlement/deposit`) ---

/// A writer implementing the xDAI funding routes. It records each call
/// and can hold a buy open until released, to test the write gate.
#[derive(Default)]
struct FundingWriter {
    calls: std::sync::Mutex<Vec<String>>,
    /// Hold `buy_with_xdai` until this is notified; `started` fires
    /// once it's waiting.
    hold_buy: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
    short_of_xdai: bool,
    /// Answer the deposit top-up with the chain refusing the chequebook.
    refuse_deposit: Option<ChequebookRefusal>,
    /// Answer a withdrawal with this instead of a transaction hash.
    refuse_withdraw: Option<FundingFailure>,
    /// Answer `GET /v0/settlement/deposit` with this instead of the
    /// default unfunded deposit.
    deposit: Option<DepositView>,
    /// Price a new plan with this instead of [`QUOTED_FUNDING`].
    quote_funding: Option<FundingView>,
}

impl FundingWriter {
    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

const QUOTED_FUNDING: FundingView = FundingView {
    wallet_bzz: Some(0),
    wallet_xdai: Some(5),
    bzz_to_acquire: 11,
    swap_input_wei: 400,
    gas_reserve_wei: 15,
    xdai_required_wei: 415,
    xdai_to_send_wei: 410,
    sufficient: false,
};

#[async_trait]
impl ChainWriter for FundingWriter {
    async fn buy_batch(
        &self,
        amount: u128,
        depth: u8,
        immutable: bool,
    ) -> Result<NewBatch, String> {
        self.record(format!(
            "buy_batch({amount}, {depth}, immutable={immutable})"
        ));
        Ok(NewBatch {
            id: [0x7E; 32],
            block: 48_500_001,
        })
    }
    async fn topup_batch(&self, _id: [u8; 32], amount: u128) -> Result<(), String> {
        self.record(format!("topup_batch({amount})"));
        Ok(())
    }
    async fn dilute_batch(&self, id: [u8; 32], depth: u8) -> Result<(), String> {
        self.record(format!("dilute_batch({:02x}, {depth})", id[0]));
        Ok(())
    }
    async fn deposit_chequebook(&self, amount: u128) -> Result<[u8; 32], String> {
        self.record(format!("deposit_chequebook({amount})"));
        Ok([0xD0; 32])
    }
    async fn quote_plan(&self, depth: u8, days: u64) -> Result<StorageQuoteView, FundingFailure> {
        self.record(format!("quote_plan({depth}, {days})"));
        Ok(StorageQuoteView {
            depth,
            days,
            amount_per_chunk: 1_000,
            plan_cost_plur: 1_000 << depth,
            deposit_due_plur: 10_000_000_000_000,
            funding: self.quote_funding.unwrap_or(QUOTED_FUNDING),
        })
    }
    async fn quote_extend(
        &self,
        batch_id: [u8; 32],
        new_depth: Option<u8>,
        days: u64,
    ) -> Result<StorageQuoteView, FundingFailure> {
        self.record(format!(
            "quote_extend({:02x}, {new_depth:?}, {days})",
            batch_id[0]
        ));
        Ok(StorageQuoteView {
            depth: new_depth.unwrap_or(20),
            days,
            amount_per_chunk: 7,
            plan_cost_plur: 7 << 20,
            deposit_due_plur: 0,
            funding: FundingView::default(),
        })
    }
    async fn buy_with_xdai(
        &self,
        depth: u8,
        amount: u128,
        immutable: bool,
        max_xdai: u128,
    ) -> Result<NewBatch, FundingFailure> {
        self.record(format!(
            "buy_with_xdai({depth}, {amount}, immutable={immutable}, max={max_xdai})"
        ));
        if self.short_of_xdai {
            return Err(FundingFailure::Rejected(
                "not enough xDAI: send 0.0410 more xDAI to your account, then try again".into(),
            ));
        }
        if let Some((release, started)) = &self.hold_buy {
            started.notify_one();
            release.notified().await;
        }
        Ok(NewBatch {
            id: [0xB0; 32],
            block: 48_500_002,
        })
    }
    async fn extend_with_xdai(
        &self,
        batch_id: [u8; 32],
        new_depth: Option<u8>,
        amount: u128,
        max_xdai: u128,
    ) -> Result<u8, FundingFailure> {
        self.record(format!(
            "extend_with_xdai({:02x}, {new_depth:?}, {amount}, max={max_xdai})",
            batch_id[0]
        ));
        Ok(new_depth.unwrap_or(20))
    }
    async fn deposit_status(&self, amount: Option<u128>) -> Result<DepositView, FundingFailure> {
        if let Some(a) = amount {
            self.record(format!("deposit_status amount={a}"));
        }
        if let Some(d) = self.deposit {
            return Ok(d);
        }
        Ok(DepositView {
            chequebook: Some([0xCB; 20]),
            managed: true,
            deposited_plur: 0,
            target_plur: 10_000_000_000_000,
            shortfall_plur: 10_000_000_000_000,
            funding: QUOTED_FUNDING,
        })
    }
    async fn fund_deposit_with_xdai(
        &self,
        amount: Option<u128>,
        max_xdai: u128,
    ) -> Result<DepositView, FundingFailure> {
        self.record(match amount {
            None => format!("fund_deposit_with_xdai max={max_xdai}"),
            Some(a) => format!("fund_deposit_with_xdai amount={a} max={max_xdai}"),
        });
        if let Some(refusal) = self.refuse_deposit {
            return Err(FundingFailure::ChequebookRefused {
                chequebook: [0xCB; 20],
                refusal,
                message: "chequebook 0xcbcb… is not registered; nothing was deposited".into(),
            });
        }
        Ok(DepositView {
            chequebook: Some([0xCB; 20]),
            managed: true,
            deposited_plur: 10_000_000_000_000,
            target_plur: 10_000_000_000_000,
            shortfall_plur: 0,
            funding: FundingView::default(),
        })
    }
    async fn withdraw_chequebook(
        &self,
        amount: u128,
        _node: tokio::sync::mpsc::Sender<ant_control::ControlCommand>,
    ) -> Result<[u8; 32], FundingFailure> {
        self.record(format!("withdraw_chequebook({amount})"));
        match &self.refuse_withdraw {
            Some(f) => Err(f.clone()),
            None => Ok([0xE0; 32]),
        }
    }
}

fn funding_ctx(writer: Arc<FundingWriter>) -> Arc<ChainContext> {
    funding_ctx_with_chequebook(writer, None)
}

fn funding_ctx_with_chequebook(
    writer: Arc<FundingWriter>,
    chequebook: Option<[u8; 20]>,
) -> Arc<ChainContext> {
    Arc::new(ChainContext {
        reader: Arc::new(FakeChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(chequebook),
        chain_id: 100,
        writer: Some(writer),
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    })
}

#[tokio::test]
async fn v0_quote_prices_a_new_plan_in_xdai() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let (status, json) = get(router, "/v0/storage/quote?depth=20&days=30").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(writer.calls(), ["quote_plan(20, 30)"]);
    assert_eq!(json["depth"], 20);
    assert_eq!(json["days"], 30);
    assert_eq!(json["amountPerChunk"], "1000");
    assert_eq!(json["planCostPlur"], (1_000u128 << 20).to_string());
    assert_eq!(json["settlementDepositPlur"], "10000000000000");
    assert_eq!(
        json["walletAddress"],
        format!("0x{}", hex::encode([0x11; 20]))
    );
    assert_eq!(json["walletBzzPlur"], "0");
    assert_eq!(json["walletXdaiWei"], "5");
    assert_eq!(json["bzzToAcquirePlur"], "11");
    assert_eq!(json["swapInputWei"], "400");
    assert_eq!(json["gasReserveWei"], "15");
    assert_eq!(json["xdaiRequiredWei"], "415");
    assert_eq!(json["xdaiToSendWei"], "410");
    assert_eq!(json["sufficientFunds"], false);
}

#[tokio::test]
async fn v0_quote_with_a_batch_prices_an_extension_or_a_resize() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let id = hex::encode([0xAB; 32]);
    let (status, json) = get(
        router.clone(),
        &format!("/v0/storage/quote?batchId={id}&days=30"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["settlementDepositPlur"], "0");
    // A resize may add no days.
    let (status, json) = get(
        router,
        &format!("/v0/storage/quote?batchId=0x{id}&days=0&depth=22"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["depth"], 22);
    assert_eq!(
        writer.calls(),
        [
            "quote_extend(ab, None, 30)",
            "quote_extend(ab, Some(22), 0)"
        ]
    );
}

#[tokio::test]
async fn v0_quote_rejects_bad_params() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    for uri in [
        "/v0/storage/quote?depth=20",
        "/v0/storage/quote?days=30",
        "/v0/storage/quote?depth=20&days=0",
        "/v0/storage/quote?depth=16&days=30",
        "/v0/storage/quote?depth=x&days=30",
        "/v0/storage/quote?batchId=nothex&days=30",
    ] {
        let (status, _) = get(router.clone(), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
    }
    assert!(writer.calls().is_empty(), "{:?}", writer.calls());
}

/// A buy defaults to an immutable batch, like bee, registers it and
/// runs the after-buy settlement hook, like `POST /stamps`.
#[tokio::test]
async fn v0_buy_defaults_to_immutable_and_runs_the_after_buy_hook() {
    let writer = Arc::new(FundingWriter::default());
    let (hook, seen) = recording_hook();
    let router = status_router_with_chain_and_hook(
        snapshot_with_one_peer(),
        funding_ctx(writer.clone()),
        Some(hook),
    );
    let (status, json) = req(
        router.clone(),
        Method::POST,
        "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["batchID"], hex::encode([0xB0; 32]));
    assert_eq!(*seen.lock().unwrap(), vec![[0xB0; 32]]);
    let (status, _) = req(
        router,
        Method::POST,
        "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500&immutable=false",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        writer.calls(),
        [
            "buy_with_xdai(20, 1000, immutable=true, max=500)",
            "buy_with_xdai(20, 1000, immutable=false, max=500)"
        ]
    );
}

/// Short of xDAI: `400` with the message the apps show, and no hook.
#[tokio::test]
async fn v0_buy_short_of_xdai_is_a_400_with_the_amount() {
    let writer = Arc::new(FundingWriter {
        short_of_xdai: true,
        ..FundingWriter::default()
    });
    let (hook, seen) = recording_hook();
    let router = status_router_with_chain_and_hook(
        snapshot_with_one_peer(),
        funding_ctx(writer),
        Some(hook),
    );
    let (status, json) = req(
        router,
        Method::POST,
        "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        json["message"],
        "not enough xDAI: send 0.0410 more xDAI to your account, then try again"
    );
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn v0_extend_with_a_depth_resizes() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let id = hex::encode([0xAB; 32]);
    let (status, json) = req(
        router.clone(),
        Method::POST,
        &format!("/v0/storage/extend?batchId={id}&amountPerChunk=7&maxXdaiWei=500"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["batchID"], id);
    let (status, _) = req(
        router,
        Method::POST,
        &format!("/v0/storage/extend?batchId={id}&amountPerChunk=7&maxXdaiWei=500&depth=22"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        writer.calls(),
        [
            "extend_with_xdai(ab, None, 7, max=500)",
            "extend_with_xdai(ab, Some(22), 7, max=500)"
        ]
    );
}

#[tokio::test]
async fn v0_settlement_deposit_reports_and_tops_up() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let (status, json) = get(router.clone(), "/v0/settlement/deposit").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["chequebook"], format!("0x{}", hex::encode([0xCB; 20])));
    assert_eq!(json["managed"], true);
    assert_eq!(json["depositPlur"], "0");
    assert_eq!(json["targetPlur"], "10000000000000");
    assert_eq!(json["shortfallPlur"], "10000000000000");
    assert_eq!(json["needsTopUp"], true);
    assert_eq!(json["xdaiToSendWei"], "410");
    let (status, json) = req(
        router,
        Method::POST,
        "/v0/settlement/deposit?maxXdaiWei=500",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["needsTopUp"], false);
    assert_eq!(json["xdaiToSendWei"], "0");
    assert_eq!(writer.calls(), ["fund_deposit_with_xdai max=500"]);
}

/// #241: a deposit at its target still reports what the node wallet
/// holds, with every top-up figure 0; a balance that couldn't be read is
/// `null` (unknown), not `"0"` (empty).
#[tokio::test]
async fn v0_settlement_deposit_reports_wallet_balances_without_a_shortfall() {
    let full = |wallet_bzz, wallet_xdai| DepositView {
        chequebook: Some([0xCB; 20]),
        managed: true,
        deposited_plur: 14_948_000_000_000_000,
        target_plur: 10_000_000_000_000_000,
        shortfall_plur: 0,
        funding: FundingView {
            wallet_bzz,
            wallet_xdai,
            ..FundingView::default()
        },
    };
    let deposit_json = |d| async move {
        let writer = Arc::new(FundingWriter {
            deposit: Some(d),
            ..FundingWriter::default()
        });
        let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer));
        let (status, json) = get(router, "/v0/settlement/deposit").await;
        assert_eq!(status, StatusCode::OK);
        json
    };

    let json = deposit_json(full(
        Some(165_277_000_000_000_000),
        Some(18_862_000_000_000_000),
    ))
    .await;
    assert_eq!(json["needsTopUp"], false);
    assert_eq!(json["walletBzzPlur"], "165277000000000000");
    assert_eq!(json["walletXdaiWei"], "18862000000000000");
    for field in [
        "bzzToAcquirePlur",
        "swapInputWei",
        "gasReserveWei",
        "xdaiRequiredWei",
        "xdaiToSendWei",
    ] {
        assert_eq!(json[field], "0", "{field}");
    }

    let json = deposit_json(full(None, None)).await;
    assert!(json["walletBzzPlur"].is_null(), "{json}");
    assert!(json["walletXdaiWei"].is_null(), "{json}");
}

/// `GET /v0/storage/quote` reports an unread balance as `null` too,
/// while a balance read as zero stays `"0"`, and the over-quote it priced
/// is passed through unchanged.
#[tokio::test]
async fn v0_quote_reports_an_unread_balance_as_null() {
    let writer = Arc::new(FundingWriter {
        quote_funding: Some(FundingView {
            wallet_xdai: None,
            ..QUOTED_FUNDING
        }),
        ..FundingWriter::default()
    });
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer));
    let (status, json) = get(router, "/v0/storage/quote?depth=20&days=30").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["walletBzzPlur"], "0");
    assert!(json["walletXdaiWei"].is_null(), "{json}");
    assert_eq!(json["xdaiToSendWei"], "410");
}

/// #305: every xDAI-swapping write takes the most xDAI its swap may
/// send (`maxXdaiWei`, the quote's `swapInputWei`). Without it, or with
/// a value that isn't a number, the route is a `400` and the writer is
/// never called; with it, the cap reaches the writer unchanged.
#[tokio::test]
async fn v0_xdai_writes_require_the_quoted_cap() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let id = hex::encode([0xAB; 32]);
    let extend = format!("/v0/storage/extend?batchId={id}&amountPerChunk=7");
    for uri in [
        "/v0/storage/buy?depth=20&amountPerChunk=1000",
        extend.as_str(),
        "/v0/settlement/deposit",
        "/v0/settlement/deposit?amount=5",
    ] {
        let sep = if uri.contains('?') { '&' } else { '?' };
        for bad in [String::new(), format!("{sep}maxXdaiWei=lots")] {
            let (status, json) = req(router.clone(), Method::POST, &format!("{uri}{bad}")).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}{bad}: {json}");
            assert!(
                json["message"].as_str().unwrap().contains("maxXdaiWei"),
                "{uri}{bad}: {json}"
            );
        }
        let (status, json) = req(
            router.clone(),
            Method::POST,
            &format!("{uri}{sep}maxXdaiWei=400"),
        )
        .await;
        assert!(status.is_success(), "{uri}: {status} {json}");
    }
    assert_eq!(
        writer.calls(),
        [
            "buy_with_xdai(20, 1000, immutable=true, max=400)",
            "extend_with_xdai(ab, None, 7, max=400)",
            "fund_deposit_with_xdai max=400",
            "fund_deposit_with_xdai amount=5 max=400",
        ]
    );
}

/// `GET /v0/settlement/deposit?amount=` asks the writer to price that
/// explicit deposit: the quote whose `swapInputWei` caps the matching
/// `POST`. A zero or non-numeric amount is refused.
#[tokio::test]
async fn v0_settlement_deposit_quotes_an_amount() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let (status, json) = get(router.clone(), "/v0/settlement/deposit?amount=5").await;
    assert_eq!(status, StatusCode::OK, "{json}");
    assert_eq!(json["swapInputWei"], "400");
    for bad in ["0", "lots"] {
        let (status, json) = get(
            router.clone(),
            &format!("/v0/settlement/deposit?amount={bad}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "amount={bad}: {json}");
    }
    assert_eq!(writer.calls(), ["deposit_status amount=5"]);
}

/// `?amount=` (Freedom's "add browsing credit") reaches the writer as
/// an explicit deposit; a zero or non-numeric amount is refused before
/// the writer is called, and the web-page guard still applies.
#[tokio::test]
async fn v0_settlement_deposit_takes_an_optional_amount() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let (status, _) = req(
        router.clone(),
        Method::POST,
        "/v0/settlement/deposit?amount=50000000000000&maxXdaiWei=500",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for bad in ["0", "lots", "-1"] {
        let (status, json) = req(
            router.clone(),
            Method::POST,
            &format!("/v0/settlement/deposit?amount={bad}&maxXdaiWei=500"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "amount={bad}: {json}");
    }
    let page = Request::builder()
        .method(Method::POST)
        .uri("/v0/settlement/deposit?amount=50000000000000&maxXdaiWei=500")
        .header("origin", "null")
        .body(Body::empty())
        .unwrap();
    assert_eq!(send(router, page).await.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        writer.calls(),
        ["fund_deposit_with_xdai amount=50000000000000 max=500"]
    );
}

/// One on-chain write at a time: while a buy runs, other writes (new
/// and bee routes alike) answer `409` instead of queueing a second
/// purchase, and quotes are still answered.
#[tokio::test]
async fn v0_writes_are_one_at_a_time() {
    let release = Arc::new(tokio::sync::Notify::new());
    let started = Arc::new(tokio::sync::Notify::new());
    let writer = Arc::new(FundingWriter {
        hold_buy: Some((release.clone(), started.clone())),
        ..FundingWriter::default()
    });
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let first = tokio::spawn(req(
        router.clone(),
        Method::POST,
        "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500",
    ));
    started.notified().await;

    for (method, uri) in [
        (
            Method::POST,
            "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500",
        ),
        (Method::POST, "/stamps/1000000/20"),
        (Method::POST, "/v0/settlement/deposit?maxXdaiWei=500"),
        (Method::POST, "/chequebook/deposit?amount=5"),
        (Method::POST, "/chequebook/withdraw?amount=5"),
    ] {
        let (status, json) = req(router.clone(), method, uri).await;
        assert_eq!(status, StatusCode::CONFLICT, "{uri}");
        assert_eq!(json["code"], 409);
    }
    let (status, _) = get(router.clone(), "/v0/storage/quote?depth=20&days=30").await;
    assert_eq!(status, StatusCode::OK, "a quote is answered during a buy");
    let (status, _) = get(router.clone(), "/v0/settlement/deposit").await;
    assert_eq!(status, StatusCode::OK, "so is the deposit status");

    release.notify_one();
    let (status, _) = first.await.unwrap();
    assert_eq!(status, StatusCode::CREATED);
    // The gate is free again.
    let (status, _) = req(router, Method::POST, "/stamps/1000000/20").await;
    assert_eq!(status, StatusCode::CREATED);
}

/// A writer without xDAI funding (another embedder's) answers `501`;
/// no writer at all answers the usual "configure a wallet" `501`.
#[tokio::test]
async fn v0_routes_501_without_funding_support() {
    for ctx in [chain_ctx_rw(None), chain_ctx(None)] {
        let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
        for (method, uri) in [
            (Method::GET, "/v0/storage/quote?depth=20&days=30"),
            (
                Method::POST,
                "/v0/storage/buy?depth=20&amountPerChunk=1&maxXdaiWei=500",
            ),
            (Method::GET, "/v0/settlement/deposit"),
        ] {
            let (status, _) = req(router.clone(), method, uri).await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{uri}");
        }
    }
}

/// `POST /stamps` buys an immutable batch unless told otherwise, like
/// bee, and reads bee-js's `immutable` header as well as the query.
#[tokio::test]
async fn buy_stamp_is_immutable_unless_the_header_or_query_says_not() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    let buy = |uri: &'static str, header: Option<&'static str>| {
        let router = router.clone();
        async move {
            let mut request = Request::builder().method(Method::POST).uri(uri);
            if let Some(value) = header {
                request = request.header("immutable", value);
            }
            send(router, request.body(Body::empty()).unwrap())
                .await
                .status()
        }
    };
    assert_eq!(buy("/stamps/1000000/20", None).await, StatusCode::CREATED);
    assert_eq!(
        buy("/stamps/1000000/20", Some("false")).await,
        StatusCode::CREATED
    );
    assert_eq!(
        buy("/stamps/1000000/20", Some("true")).await,
        StatusCode::CREATED
    );
    assert_eq!(
        buy("/stamps/1000000/20?immutable=false", None).await,
        StatusCode::CREATED
    );
    assert_eq!(
        buy("/stamps/1000000/20?immutable=true", Some("false")).await,
        StatusCode::CREATED,
        "the query wins over the header"
    );
    assert_eq!(
        buy("/stamps/1000000/20", Some("maybe")).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        writer.calls(),
        [
            "buy_batch(1000000, 20, immutable=true)",
            "buy_batch(1000000, 20, immutable=false)",
            "buy_batch(1000000, 20, immutable=true)",
            "buy_batch(1000000, 20, immutable=false)",
            "buy_batch(1000000, 20, immutable=true)",
        ]
    );
}

/// The xDAI write routes hold the context's wallet tx lock across their
/// transactions, like `POST /stamps`: while the embedder's background
/// settlement holds it they wait (they don't `409`: the write gate is
/// the routes' own), and run once it's released. Quotes and the deposit
/// status don't wait.
#[tokio::test]
async fn v0_writes_wait_for_the_wallet_tx_lock() {
    let batch = hex::encode([0xAB; 32]);
    let extend = format!("/v0/storage/extend?batchId={batch}&amountPerChunk=1&maxXdaiWei=500");
    for uri in [
        "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500",
        extend.as_str(),
        "/v0/settlement/deposit?maxXdaiWei=500",
    ] {
        let writer = Arc::new(FundingWriter::default());
        let ctx = funding_ctx(writer.clone());
        let held = ctx.tx_lock.clone().lock_owned().await;
        let router = status_router_with_chain(snapshot_with_one_peer(), ctx);

        let (status, _) = get(router.clone(), "/v0/storage/quote?depth=20&days=30").await;
        assert_eq!(status, StatusCode::OK, "a quote doesn't wait for the lock");
        let (status, _) = get(router.clone(), "/v0/settlement/deposit").await;
        assert_eq!(status, StatusCode::OK, "nor does the deposit status");

        let u = uri.to_string();
        let mut write = tokio::spawn({
            let router = router.clone();
            async move { req(router, Method::POST, &u).await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut write)
                .await
                .is_err(),
            "{uri} ran while another wallet tx held the lock",
        );
        let writes = || {
            writer
                .calls()
                .into_iter()
                .filter(|c| !c.starts_with("quote"))
                .count()
        };
        assert_eq!(writes(), 0, "{uri}: {:?}", writer.calls());
        drop(held);
        let (status, _) = tokio::time::timeout(std::time::Duration::from_secs(5), write)
            .await
            .expect("the write finishes once the lock is released")
            .unwrap();
        assert!(status.is_success(), "{uri}: {status}");
        assert_eq!(writes(), 1, "{uri}: {:?}", writer.calls());
    }
}

/// A deposit top-up the chain refused goes to the embedder's hook, after
/// the wallet tx lock is released (the hook may take the embedder's
/// settlement lock). A refusal that stands is a `400` with the reason;
/// one the embedder reads as a lagging RPC is a `503` to retry.
#[tokio::test]
async fn v0_deposit_refusal_goes_to_the_embedder_hook() {
    for (stands, want) in [
        (true, StatusCode::BAD_REQUEST),
        (false, StatusCode::SERVICE_UNAVAILABLE),
    ] {
        let writer = Arc::new(FundingWriter {
            refuse_deposit: Some(ChequebookRefusal::NotRegistered),
            ..FundingWriter::default()
        });
        let ctx = funding_ctx(writer.clone());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let hook: ant_gateway::ChequebookRefusedHook = {
            let (seen, lock) = (seen.clone(), ctx.tx_lock.clone());
            Arc::new(move |chequebook, refusal| {
                let (seen, lock) = (seen.clone(), lock.clone());
                Box::pin(async move {
                    let released = lock.try_lock().is_ok();
                    seen.lock().unwrap().push((chequebook, refusal, released));
                    stands
                })
            })
        };
        let router = common::status_router_with_chain_and_hooks(
            snapshot_with_one_peer(),
            ctx,
            None,
            Some(hook),
        );
        let (status, json) = req(
            router,
            Method::POST,
            "/v0/settlement/deposit?maxXdaiWei=500",
        )
        .await;
        assert_eq!(status, want, "{json}");
        if stands {
            assert!(json["message"]
                .as_str()
                .unwrap()
                .contains("nothing was deposited"));
        }
        assert_eq!(
            *seen.lock().unwrap(),
            [([0xCB; 20], ChequebookRefusal::NotRegistered, true)],
            "the hook ran once, with the wallet tx lock free",
        );
    }

    // No hook: the refusal is answered as it stands.
    let writer = Arc::new(FundingWriter {
        refuse_deposit: Some(ChequebookRefusal::IssuerMismatch([0x5E; 20])),
        ..FundingWriter::default()
    });
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer));
    let (status, _) = req(
        router,
        Method::POST,
        "/v0/settlement/deposit?maxXdaiWei=500",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// Send `method uri` with the given extra headers; return the status.
async fn status_with_headers(
    router: axum::Router,
    method: Method,
    uri: &str,
    headers: &[(&str, &str)],
) -> StatusCode {
    let mut b = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    send(router, b.body(Body::empty()).unwrap()).await.status()
}

/// The on-chain postage and chequebook writes refuse web pages the same
/// way (issue #312): a no-cors `POST`/`PATCH` from a foreign page, a
/// `null`-origin page or the API's own pages buys, tops up, dilutes or
/// deposits nothing. A request with neither `Origin` nor
/// `Sec-Fetch-Site` (curl, a host app) still goes through.
#[tokio::test]
async fn postage_and_chequebook_writes_refuse_web_pages() {
    let id = "ab".repeat(32);
    let writes: [(Method, String); 4] = [
        (Method::POST, "/stamps/1/17".to_string()),
        (Method::PATCH, format!("/stamps/topup/{id}/500")),
        (Method::PATCH, format!("/stamps/dilute/{id}/22")),
        (Method::POST, "/chequebook/deposit?amount=100".to_string()),
    ];
    let browser_headers: [&[(&str, &str)]; 5] = [
        &[("origin", "https://evil")],
        &[("sec-fetch-site", "cross-site")],
        &[("origin", "null"), ("sec-fetch-site", "cross-site")],
        &[("origin", "https://anything.example")],
        &[
            ("origin", "http://127.0.0.1:1633"),
            ("sec-fetch-site", "same-origin"),
        ],
    ];
    for cors in [
        CorsConfig::default(),
        CorsConfig::new(["*", "null"]),
        CorsConfig::new(["https://app.example"]),
    ] {
        let writer = Arc::new(FundingWriter::default());
        let router = status_router_with_chain_hooks_and_cors(
            snapshot_with_one_peer(),
            funding_ctx_with_chequebook(writer.clone(), Some([0xCD; 20])),
            None,
            None,
            cors.clone(),
        );
        for (method, uri) in &writes {
            for h in browser_headers {
                assert_eq!(
                    status_with_headers(router.clone(), method.clone(), uri, h).await,
                    StatusCode::FORBIDDEN,
                    "{method} {uri} {h:?} {cors:?}",
                );
            }
        }
        assert_eq!(writer.calls(), Vec::<String>::new(), "{cors:?}");
        // Without browser headers every one of them runs.
        for (method, uri) in &writes {
            let s = status_with_headers(router.clone(), method.clone(), uri, &[]).await;
            assert!(s.is_success(), "{method} {uri}: {s}");
        }
        assert_eq!(
            writer.calls(),
            vec![
                "buy_batch(1, 17, immutable=true)".to_string(),
                "topup_batch(500)".to_string(),
                "dilute_batch(ab, 22)".to_string(),
                "deposit_chequebook(100)".to_string(),
            ],
            "{cors:?}"
        );
    }
}

/// The xDAI-spending routes are query-only, so a page could send them
/// as CORS-simple requests with no preflight. They refuse anything a
/// browser sent unless the origin is listed exactly: nothing is swapped
/// or bought for a cross-site page, a `null`-origin dweb page, or an
/// origin only `*` covers. Reads and non-browser callers still work.
#[tokio::test]
async fn v0_wallet_spending_routes_refuse_web_pages() {
    const WRITES: [&str; 4] = [
        "/v0/storage/buy?depth=24&amountPerChunk=1000&maxXdaiWei=500",
        "/v0/storage/extend?batchId=abababababababababababababababababababababababababababababababab&amountPerChunk=1000&maxXdaiWei=500",
        "/v0/settlement/deposit?maxXdaiWei=500",
        "/chequebook/withdraw?amount=5",
    ];
    let browser_headers: [&[(&str, &str)]; 5] = [
        &[
            ("origin", "https://evil.example"),
            ("sec-fetch-site", "cross-site"),
        ],
        &[("origin", "null")],
        &[("origin", "https://app.example:8443")],
        &[
            ("origin", "http://127.0.0.1:1633"),
            ("sec-fetch-site", "same-origin"),
        ],
        &[("sec-fetch-site", "cross-site")],
    ];
    for cors in [
        CorsConfig::default(),
        CorsConfig::new(["*", "null"]),
        CorsConfig::new(["https://app.example"]),
    ] {
        let writer = Arc::new(FundingWriter::default());
        let router = status_router_with_chain_hooks_and_cors(
            snapshot_with_one_peer(),
            funding_ctx(writer.clone()),
            None,
            None,
            cors.clone(),
        );
        for uri in WRITES {
            for h in browser_headers {
                assert_eq!(
                    status_with_headers(router.clone(), Method::POST, uri, h).await,
                    StatusCode::FORBIDDEN,
                    "{uri} {h:?} {cors:?}",
                );
            }
        }
        assert_eq!(writer.calls(), Vec::<String>::new(), "{cors:?}");
        // Quotes and the deposit status are reads: not guarded.
        for uri in [
            "/v0/storage/quote?depth=20&days=30",
            "/v0/settlement/deposit",
        ] {
            assert_eq!(
                status_with_headers(
                    router.clone(),
                    Method::GET,
                    uri,
                    &[("origin", "https://evil.example")]
                )
                .await,
                StatusCode::OK,
                "{uri}",
            );
        }
    }

    // Non-browser callers (Freedom's main process, the ant-ffi host,
    // curl), a user-typed request, and an exactly listed origin go
    // through.
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain_hooks_and_cors(
        snapshot_with_one_peer(),
        funding_ctx(writer.clone()),
        None,
        None,
        CorsConfig::new(["https://App.Example", "null"]),
    );
    let buy = "/v0/storage/buy?depth=24&amountPerChunk=1000&maxXdaiWei=500";
    for h in [
        &[][..],
        &[("sec-fetch-site", "none")][..],
        &[
            ("origin", "https://app.example"),
            ("sec-fetch-site", "cross-site"),
        ][..],
    ] {
        assert_eq!(
            status_with_headers(router.clone(), Method::POST, buy, h).await,
            StatusCode::CREATED,
            "{h:?}",
        );
    }
    assert_eq!(writer.calls().len(), 3);
}

/// A buy hands the node its `createBatch` block, so the node holds the
/// new batch back as not usable until the storers have synced it
/// (bee's confirmation window). A dilute re-registers without one.
#[tokio::test]
async fn buys_register_the_batch_with_its_creation_block() {
    let writer = Arc::new(FundingWriter::default());
    let (router, seen) =
        status_router_recording_registrations(snapshot_with_one_peer(), funding_ctx(writer));
    let (status, _) = req(router.clone(), Method::POST, "/stamps/1000000/20").await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = req(
        router.clone(),
        Method::POST,
        "/v0/storage/buy?depth=20&amountPerChunk=1000&maxXdaiWei=500",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let dilute = format!("/stamps/dilute/{}/21", hex::encode([0xAB; 32]));
    let (status, _) = req(router, Method::PATCH, &dilute).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            ([0x7E; 32], 20, true, Some(48_500_001)),
            ([0xB0; 32], 20, true, Some(48_500_002)),
            ([0xAB; 32], 21, false, None),
        ]
    );
}

/// `POST /chequebook/withdraw?amount=` answers bee's `200
/// {transactionHash}`, after holding the wallet tx lock like every
/// other write.
#[tokio::test]
async fn withdraw_returns_tx_hash_under_the_wallet_lock() {
    let writer = Arc::new(FundingWriter::default());
    let ctx = funding_ctx(writer.clone());
    let held = ctx.tx_lock.clone().lock_owned().await;
    let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
    let mut call = tokio::spawn(req(
        router,
        Method::POST,
        "/chequebook/withdraw?amount=1500000000000000",
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), &mut call)
            .await
            .is_err(),
        "withdrew while another wallet tx held the lock",
    );
    drop(held);
    let (status, json) = call.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json["transactionHash"],
        format!("0x{}", hex::encode([0xE0; 32]))
    );
    assert_eq!(writer.calls(), ["withdraw_chequebook(1500000000000000)"]);
}

/// A missing or zero amount never reaches the writer; the writer's
/// refusals keep their status: over the available credit `400`, an
/// unknown cheque total `503`, a failed transaction `502`.
#[tokio::test]
async fn withdraw_validates_and_maps_refusals() {
    let writer = Arc::new(FundingWriter::default());
    let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer.clone()));
    for uri in [
        "/chequebook/withdraw",
        "/chequebook/withdraw?amount=0",
        "/chequebook/withdraw?amount=-1",
        "/chequebook/withdraw?amount=lots",
    ] {
        let (status, _) = req(router.clone(), Method::POST, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
    }
    assert_eq!(
        writer.calls(),
        Vec::<String>::new(),
        "nothing reached the writer"
    );

    for (refusal, want) in [
        (
            FundingFailure::Rejected("can withdraw at most 0.0007 xBZZ now".into()),
            StatusCode::BAD_REQUEST,
        ),
        (
            FundingFailure::Unavailable("not withdrawing: ledger unreadable".into()),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            FundingFailure::Chain("withdraw transaction: reverted".into()),
            StatusCode::BAD_GATEWAY,
        ),
    ] {
        let message = match &refusal {
            FundingFailure::Rejected(m)
            | FundingFailure::Unavailable(m)
            | FundingFailure::Chain(m) => m.clone(),
            _ => unreachable!(),
        };
        let writer = Arc::new(FundingWriter {
            refuse_withdraw: Some(refusal),
            ..FundingWriter::default()
        });
        let router = status_router_with_chain(snapshot_with_one_peer(), funding_ctx(writer));
        let (status, json) = req(router, Method::POST, "/chequebook/withdraw?amount=7").await;
        assert_eq!(status, want);
        assert_eq!(json["message"], message);
    }
}

/// No writer, or one that can't withdraw: `501`.
#[tokio::test]
async fn withdraw_501_without_support() {
    for ctx in [chain_ctx(Some([0xCD; 20])), chain_ctx_rw(Some([0xCD; 20]))] {
        let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
        let (status, _) = req(router, Method::POST, "/chequebook/withdraw?amount=1").await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    }
}

// --- chain error bodies carry no RPC URL (issue #296, GW-11) ---

/// Error text as an RPC client would quote it, API key in the path.
const URL_ERROR: &str =
    "error sending request for url (https://rpc.example/v2/SECRET-KEY): operation timed out";

/// A reader whose every read fails quoting the RPC URL.
struct UrlQuotingChain;

#[async_trait]
impl ChainReader for UrlQuotingChain {
    async fn block_number(&self) -> Result<u64, String> {
        Err(URL_ERROR.into())
    }
    async fn current_price(&self) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn total_amount(&self) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn bzz_balance(&self, _who: [u8; 20]) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn native_balance(&self, _who: [u8; 20]) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn chequebook_balance(&self, _cb: [u8; 20]) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn chequebook_total_paid_out(&self, _cb: [u8; 20]) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn batch_meta(&self, _id: [u8; 32]) -> Result<ant_gateway::BatchMetaView, String> {
        Err(URL_ERROR.into())
    }
    async fn batch_remaining_balance(&self, _id: [u8; 32]) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
    async fn minimum_balance_per_chunk(&self) -> Result<u128, String> {
        Err(URL_ERROR.into())
    }
}

/// A writer whose every write fails quoting the RPC URL.
struct UrlQuotingWriter;

#[async_trait]
impl ChainWriter for UrlQuotingWriter {
    async fn buy_batch(&self, _a: u128, _d: u8, _i: bool) -> Result<NewBatch, String> {
        Err(URL_ERROR.into())
    }
    async fn topup_batch(&self, _id: [u8; 32], _a: u128) -> Result<(), String> {
        Err(URL_ERROR.into())
    }
    async fn dilute_batch(&self, _id: [u8; 32], _d: u8) -> Result<(), String> {
        Err(URL_ERROR.into())
    }
    async fn deposit_chequebook(&self, _a: u128) -> Result<[u8; 32], String> {
        Err(URL_ERROR.into())
    }
}

fn assert_no_url(status: StatusCode, json: &Value) {
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    let body = json.to_string();
    assert!(!body.contains("SECRET-KEY"), "{body}");
    assert!(!body.contains("rpc.example"), "{body}");
    assert!(body.contains("<url>"), "{body}");
}

/// `guarded` (every chain read route) scrubs the URL from its 502 body.
#[tokio::test]
async fn a_chain_read_502_body_carries_no_url() {
    let ctx = Arc::new(ChainContext {
        reader: Arc::new(UrlQuotingChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(None),
        chain_id: 100,
        writer: None,
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    });
    let (status, json) = get(
        status_router_with_chain(snapshot_with_one_peer(), ctx),
        "/chainstate",
    )
    .await;
    assert_no_url(status, &json);
}

/// `guarded_tx` (the bee write routes) scrubs it too.
#[tokio::test]
async fn a_chain_write_502_body_carries_no_url() {
    let ctx = Arc::new(ChainContext {
        reader: Arc::new(FakeChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(None),
        chain_id: 100,
        writer: Some(Arc::new(UrlQuotingWriter)),
        writes: WriteGate::default(),
        tx_lock: WalletTxLock::default(),
    });
    let (status, json) = req(
        status_router_with_chain(snapshot_with_one_peer(), ctx),
        Method::POST,
        "/stamps/1000000/20?immutable=true",
    )
    .await;
    assert_no_url(status, &json);
}

/// End to end through the real `ant-chain` reader: an unreachable RPC
/// whose URL carries a key answers `/chainstate` with a 502 that doesn't
/// quote it.
#[cfg(feature = "chain")]
#[tokio::test]
async fn a_real_rpc_failure_502_body_carries_no_url() {
    let ctx = ant_gateway::chainreader::build(
        // Nothing listens on port 1: a refused connect.
        Some("http://127.0.0.1:1/v2/SECRET-KEY".into()),
        None,
        ant_chain::GNOSIS_POSTAGE_STAMP.into(),
        [0x11; 20],
        None,
        100,
        None,
        None,
    )
    .unwrap();
    let (status, json) = get(
        status_router_with_chain(snapshot_with_one_peer(), ctx),
        "/chainstate",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    let body = json.to_string();
    assert!(!body.contains("SECRET-KEY"), "{body}");
    assert!(!body.contains("/v2/"), "{body}");
}

/// A JSON-RPC stub for the real writer: `batchOwner` answers the owner
/// `owner_of` picks for the batch id, `batchDepth` answers 20, every
/// other call fails. Records each request as `method` or
/// `eth_call:<selector>`.
#[cfg(feature = "chain")]
async fn spawn_batch_rpc(
    owner_of: fn(&str) -> [u8; 20],
) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
    use axum::routing::post;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    let router = axum::Router::new().route(
        "/",
        post(move |axum::Json(req): axum::Json<Value>| {
            let log = log.clone();
            async move {
                let method = req["method"].as_str().unwrap_or_default().to_string();
                let data = req["params"][0]["data"]
                    .as_str()
                    .or_else(|| req["params"][0]["input"].as_str())
                    .unwrap_or_default()
                    .trim_start_matches("0x")
                    .to_string();
                let answer = if method == "eth_call" {
                    let selector = data.get(..8).unwrap_or_default().to_string();
                    log.lock().unwrap().push(format!("eth_call:{selector}"));
                    match selector.as_str() {
                        "2182ddb1" => Some(format!(
                            "0x{}{}",
                            "00".repeat(12),
                            hex::encode(owner_of(data.get(8..).unwrap_or_default()))
                        )),
                        "44beae8e" => Some(format!("0x{:064x}", 20)),
                        _ => None,
                    }
                } else {
                    log.lock().unwrap().push(method);
                    None
                };
                axum::Json(match answer {
                    Some(result) => {
                        serde_json::json!({"jsonrpc": "2.0", "id": req["id"], "result": result})
                    }
                    None => serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": req["id"],
                        "error": {"code": -32601, "message": "stub: not served"},
                    }),
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (url, seen)
}

/// Issue #296 R2-M1: a top-up and an extend read only what they use from
/// the batch (one `eth_call` for a top-up's depth; owner then depth for
/// an extend), never the four-call metadata (bucket depth, immutable
/// flag), since each read adds a request timeout to the write's bound.
/// The owner checks still hold: a batch not on chain is `404` and a
/// resize of someone else's batch `400`, both before any other read.
#[cfg(feature = "chain")]
#[tokio::test]
async fn batch_writes_read_only_the_owner_and_depth() {
    const BUCKET_DEPTH: &str = "eth_call:32ac57dd";
    const IMMUTABLE: &str = "eth_call:d968f44b";
    const OWNER: &str = "eth_call:2182ddb1";
    const DEPTH: &str = "eth_call:44beae8e";
    // Batch 00…: not on chain. Any other: owned by a stranger.
    let (url, seen) = spawn_batch_rpc(|id| {
        if id.bytes().all(|b| b == b'0') {
            [0; 20]
        } else {
            [0x22; 20]
        }
    })
    .await;
    let ctx = ant_gateway::chainreader::build(
        Some(url),
        None,
        ant_chain::GNOSIS_POSTAGE_STAMP.into(),
        [0x11; 20],
        None,
        100,
        Some([0x42; 32]),
        None,
    )
    .unwrap();
    let router = status_router_with_chain(snapshot_with_one_peer(), ctx);
    let run = |method, uri: String| {
        let router = router.clone();
        let seen = seen.clone();
        async move {
            seen.lock().unwrap().clear();
            let (status, json) = req(router, method, &uri).await;
            let calls = seen.lock().unwrap().clone();
            assert!(
                !calls.iter().any(|c| c == BUCKET_DEPTH || c == IMMUTABLE),
                "{uri}: {calls:?}"
            );
            (status, json, calls)
        }
    };
    let stranger = hex::encode([0xAB; 32]);
    let missing = hex::encode([0; 32]);

    // Top-up: the depth, then the approval's nonce read (which fails).
    let (status, json, calls) = run(Method::PATCH, format!("/stamps/topup/{stranger}/500")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(
        calls.iter().filter(|c| c.starts_with("eth_call")).count(),
        1,
        "{calls:?}"
    );
    assert_eq!(calls[0], DEPTH, "{calls:?}");

    let extend = |id: &str, depth: &str| {
        format!("/v0/storage/extend?batchId={id}&amountPerChunk=7&maxXdaiWei=500{depth}")
    };
    let (status, json, calls) = run(Method::POST, extend(&missing, "")).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{json}");
    assert_eq!(calls, [OWNER]);

    let (status, json, calls) = run(Method::POST, extend(&stranger, "&depth=22")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{json}");
    assert_eq!(calls, [OWNER]);

    // A plain extend is open to any payer: owner, depth, then the
    // funding reads (the first of which fails here).
    let (status, json, calls) = run(Method::POST, extend(&stranger, "")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{json}");
    assert_eq!(calls[..2], [OWNER, DEPTH], "{calls:?}");
}
