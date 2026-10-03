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
    status_router_with_chain_and_hook, status_router_with_chain_and_issued,
    status_router_with_chain_hooks_and_cors,
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
        Ok(1_500_000_000_000_000_000)
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
}

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
    wallet_bzz: 0,
    wallet_xdai: 5,
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
    async fn topup_batch(&self, _id: [u8; 32], _amount: u128) -> Result<(), String> {
        Ok(())
    }
    async fn dilute_batch(&self, _id: [u8; 32], _depth: u8) -> Result<(), String> {
        Ok(())
    }
    async fn deposit_chequebook(&self, _amount: u128) -> Result<[u8; 32], String> {
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
            funding: QUOTED_FUNDING,
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
    ) -> Result<NewBatch, FundingFailure> {
        self.record(format!(
            "buy_with_xdai({depth}, {amount}, immutable={immutable})"
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
    ) -> Result<u8, FundingFailure> {
        self.record(format!(
            "extend_with_xdai({:02x}, {new_depth:?}, {amount})",
            batch_id[0]
        ));
        Ok(new_depth.unwrap_or(20))
    }
    async fn deposit_status(&self) -> Result<DepositView, FundingFailure> {
        Ok(DepositView {
            chequebook: Some([0xCB; 20]),
            managed: true,
            deposited_plur: 0,
            target_plur: 10_000_000_000_000,
            shortfall_plur: 10_000_000_000_000,
            funding: QUOTED_FUNDING,
        })
    }
    async fn fund_deposit_with_xdai(&self) -> Result<DepositView, FundingFailure> {
        self.record("fund_deposit_with_xdai".into());
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
}

fn funding_ctx(writer: Arc<FundingWriter>) -> Arc<ChainContext> {
    Arc::new(ChainContext {
        reader: Arc::new(FakeChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(None),
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
        "/v0/storage/buy?depth=20&amountPerChunk=1000",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["batchID"], hex::encode([0xB0; 32]));
    assert_eq!(*seen.lock().unwrap(), vec![[0xB0; 32]]);
    let (status, _) = req(
        router,
        Method::POST,
        "/v0/storage/buy?depth=20&amountPerChunk=1000&immutable=false",
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(
        writer.calls(),
        [
            "buy_with_xdai(20, 1000, immutable=true)",
            "buy_with_xdai(20, 1000, immutable=false)"
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
        "/v0/storage/buy?depth=20&amountPerChunk=1000",
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
        &format!("/v0/storage/extend?batchId={id}&amountPerChunk=7"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["batchID"], id);
    let (status, _) = req(
        router,
        Method::POST,
        &format!("/v0/storage/extend?batchId={id}&amountPerChunk=7&depth=22"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        writer.calls(),
        [
            "extend_with_xdai(ab, None, 7)",
            "extend_with_xdai(ab, Some(22), 7)"
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
    let (status, json) = req(router, Method::POST, "/v0/settlement/deposit").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["needsTopUp"], false);
    assert_eq!(json["xdaiToSendWei"], "0");
    assert_eq!(writer.calls(), ["fund_deposit_with_xdai"]);
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
        "/v0/storage/buy?depth=20&amountPerChunk=1000",
    ));
    started.notified().await;

    for (method, uri) in [
        (Method::POST, "/v0/storage/buy?depth=20&amountPerChunk=1000"),
        (Method::POST, "/stamps/1000000/20"),
        (Method::POST, "/v0/settlement/deposit"),
        (Method::POST, "/chequebook/deposit?amount=5"),
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
            (Method::POST, "/v0/storage/buy?depth=20&amountPerChunk=1"),
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
    let extend = format!("/v0/storage/extend?batchId={batch}&amountPerChunk=1");
    for uri in [
        "/v0/storage/buy?depth=20&amountPerChunk=1000",
        extend.as_str(),
        "/v0/settlement/deposit",
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
        let (status, json) = req(router, Method::POST, "/v0/settlement/deposit").await;
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
    let (status, _) = req(router, Method::POST, "/v0/settlement/deposit").await;
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

/// The xDAI-spending routes are query-only, so a page could send them
/// as CORS-simple requests with no preflight. They refuse anything a
/// browser sent unless the origin is listed exactly: nothing is swapped
/// or bought for a cross-site page, a `null`-origin dweb page, or an
/// origin only `*` covers. Reads and non-browser callers still work.
#[tokio::test]
async fn v0_wallet_spending_routes_refuse_web_pages() {
    const WRITES: [&str; 3] = [
        "/v0/storage/buy?depth=24&amountPerChunk=1000",
        "/v0/storage/extend?batchId=abababababababababababababababababababababababababababababababab&amountPerChunk=1000",
        "/v0/settlement/deposit",
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
    let buy = "/v0/storage/buy?depth=24&amountPerChunk=1000";
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
        "/v0/storage/buy?depth=20&amountPerChunk=1000",
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
