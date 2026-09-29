//! Chain-backed endpoints `/wallet`, `/chequebook/*`, `/status`,
//! `/chainstate` (PLAN.md J.5 A2/A3/D1/D2). Driven with a deterministic
//! fake [`ChainReader`] so the bee response shapes + the no-chain
//! fallbacks are locked without a live RPC.

mod common;

use std::sync::Arc;

use ant_gateway::{
    ChainContext, ChainReader, ChainWriter, ChequebookSlot, DepositView, FundingFailure,
    FundingView, StorageQuoteView, WriteGate,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use common::{
    body_bytes, send, snapshot_with_one_peer, status_only_router, status_router_with_chain,
    status_router_with_chain_and_hook,
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
}

fn chain_ctx(chequebook: Option<[u8; 20]>) -> Arc<ChainContext> {
    Arc::new(ChainContext {
        reader: Arc::new(FakeChain),
        wallet_eth: [0x11; 20],
        chequebook: ChequebookSlot::new(chequebook),
        chain_id: 100,
        writer: None,
        writes: WriteGate::default(),
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
    ) -> Result<[u8; 32], String> {
        Ok([0x7E; 32])
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
    ) -> Result<[u8; 32], String> {
        self.record(format!(
            "buy_batch({amount}, {depth}, immutable={immutable})"
        ));
        Ok([0x7E; 32])
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
    ) -> Result<[u8; 32], FundingFailure> {
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
        Ok([0xB0; 32])
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
