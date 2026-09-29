//! Chain-backed read endpoints: `/wallet`, `/chequebook/*`, `/status`,
//! `/chainstate` (PLAN.md J.5 A2/A3/D1/D2).
//!
//! These need live Gnosis state (balances, block height, postage price)
//! that the node loop doesn't hold, so rather than route them through
//! the control channel they read directly from a [`ChainReader`] the
//! daemon installs on the [`GatewayHandle`]. The trait keeps
//! `ant-gateway` free of an `ant-chain` / `reqwest` dependency and lets
//! the integration tests drive every branch with a deterministic fake.
//!
//! When no chain context is configured (read-only / no RPC) the
//! balances degrade to the bee zero-stub shape and the chain-state
//! endpoints fall to `501` — exactly the behaviour bee shows on a node
//! without a configured backend, so bee-js UIs stay happy.

use std::time::Duration;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use tokio::sync::oneshot;

use ant_control::{ControlAck, ControlCommand};

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// Collision-bucket depth baked into every `createBatch` transaction
/// (bee's constant). The runtime issuer registered for a freshly-bought
/// batch must use the same value or its stamps won't validate.
const POSTAGE_BUCKET_DEPTH: u8 = 16;

/// Bound on every chain RPC a request makes, so a slow / wedged RPC
/// endpoint surfaces as `504` rather than hanging the HTTP handler.
const CHAIN_RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// Read-only Gnosis views the chain-backed endpoints need. All amounts
/// are PLUR/wei lower-128 bits (enough for any sane balance) returned as
/// `u128`; the handlers stringify them into bee's bigint-as-string JSON.
#[async_trait]
pub trait ChainReader: Send + Sync {
    async fn block_number(&self) -> Result<u64, String>;
    async fn current_price(&self) -> Result<u128, String>;
    async fn total_amount(&self) -> Result<u128, String>;
    async fn bzz_balance(&self, who: [u8; 20]) -> Result<u128, String>;
    async fn native_balance(&self, who: [u8; 20]) -> Result<u128, String>;
    async fn chequebook_balance(&self, chequebook: [u8; 20]) -> Result<u128, String>;
    /// `PostageStamp.remainingBalance(batchId)` — per-chunk balance left
    /// on a batch. Used to enrich `GET /stamps` with bee's `amount` /
    /// `batchTTL`. Defaulted to "unsupported" so non-chain readers (test
    /// fakes / embedders) needn't implement it; `/stamps` then keeps its
    /// placeholders.
    async fn batch_remaining_balance(&self, _batch_id: [u8; 32]) -> Result<u128, String> {
        Err("batch_remaining_balance unsupported".to_string())
    }
    /// `PostageStamp.batchOwner/batchDepth/batchBucketDepth/
    /// batchImmutableFlag` — the on-chain views backing the light
    /// `GET /batches/{id}` lookup. Defaulted to "unsupported" like
    /// [`Self::batch_remaining_balance`].
    async fn batch_meta(&self, _batch_id: [u8; 32]) -> Result<BatchMetaView, String> {
        Err("batch_meta unsupported".to_string())
    }
    /// `PostageStamp.batchOwner` alone — the single view `/stamps` needs
    /// to confirm a batch is gone (zero owner) after its balance read
    /// fails. Defaults to [`Self::batch_meta`]'s owner; real readers
    /// override it with one call instead of `batch_meta`'s four, so the
    /// check doesn't eat the shared enrichment timeout.
    async fn batch_owner(&self, batch_id: [u8; 32]) -> Result<[u8; 20], String> {
        self.batch_meta(batch_id).await.map(|meta| meta.owner)
    }
}

/// On-chain views of one postage batch, read directly from the
/// `PostageStamp` contract (no event sync). `start` (the creation
/// block) is not derivable from contract views alone, so it is absent
/// here; the `/batches/{id}` handler reports bee's `start` as `0`.
#[derive(Debug, Clone, Copy)]
pub struct BatchMetaView {
    pub owner: [u8; 20],
    pub depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
}

/// On-chain write surface backing the postage-buy / topup / dilute and
/// chequebook-deposit endpoints (PLAN.md J.5 B2/B3, D3). Each call signs
/// and submits a Gnosis transaction and waits for the receipt, so the
/// handlers run under a longer timeout than the read path. `None` on the
/// [`ChainContext`] when no funded wallet key is configured — the write
/// endpoints then fall to `501`.
#[async_trait]
pub trait ChainWriter: Send + Sync {
    /// `PostageStamp.createBatch` (after the BZZ `approve`). `amount` is
    /// the per-chunk balance (PLUR) bee's `POST /stamps/{amount}/{depth}`
    /// takes verbatim. Returns the new 32-byte batch id.
    async fn buy_batch(
        &self,
        amount_per_chunk: u128,
        depth: u8,
        immutable: bool,
    ) -> Result<[u8; 32], String>;
    /// `PostageStamp.topUp(batchId, amountPerChunk)`.
    async fn topup_batch(&self, batch_id: [u8; 32], amount_per_chunk: u128) -> Result<(), String>;
    /// `PostageStamp.increaseDepth(batchId, newDepth)` (a.k.a "dilute").
    async fn dilute_batch(&self, batch_id: [u8; 32], new_depth: u8) -> Result<(), String>;
    /// Fund the chequebook by transferring `amount` xBZZ into it.
    /// Returns the 32-byte transaction hash.
    async fn deposit_chequebook(&self, amount: u128) -> Result<[u8; 32], String>;
}

/// The node's chequebook address, as the gateway reports and funds it.
///
/// Shared between the [`ChainContext`] and the writer (cloning shares
/// the slot), and updatable after startup. The embedder resolves the
/// chequebook at startup, but it can also appear later: a deploy
/// triggered by a stamp buy, or one adopted by `ant-ffi`'s gateway-start
/// chain init. Setting it here makes `/wallet`, `/chequebook/*` and
/// `POST /chequebook/deposit` see it without a restart.
#[derive(Debug, Clone, Default)]
pub struct ChequebookSlot(std::sync::Arc<std::sync::RwLock<Option<[u8; 20]>>>);

impl ChequebookSlot {
    #[must_use]
    pub fn new(chequebook: Option<[u8; 20]>) -> Self {
        Self(std::sync::Arc::new(std::sync::RwLock::new(chequebook)))
    }

    /// The current chequebook, if any.
    #[must_use]
    pub fn get(&self) -> Option<[u8; 20]> {
        *self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record the chequebook the node now settles with.
    pub fn set(&self, chequebook: [u8; 20]) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(chequebook);
    }

    /// Forget the chequebook: the node no longer settles with one (e.g.
    /// the chain check disqualified it), so `/chequebook/*` reports none
    /// and `POST /chequebook/deposit` has nothing to fund.
    pub fn clear(&self) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

impl From<Option<[u8; 20]>> for ChequebookSlot {
    fn from(chequebook: Option<[u8; 20]>) -> Self {
        Self::new(chequebook)
    }
}

/// Serializes every transaction sent from the node wallet.
///
/// The wallet fetches its nonce as "pending count" per tx, so two
/// senders interleaving (a second `POST /stamps` while the first is
/// still confirming, or the embedder topping up the chequebook in the
/// background after a buy) race for the same nonce, and a transfer can
/// drain the xBZZ a buy's balance guard just counted. The write
/// endpoints hold it from their pre-checks through the last receipt;
/// an embedder that sends from the same wallet outside the gateway
/// clones it from [`ChainContext::tx_lock`] (antd) or passes its own to
/// [`crate::chainreader::build_with_transport`] (ant-ffi, which rebuilds
/// the context on every gateway start) and holds it the same way. It
/// only serializes senders that share the one lock.
pub type WalletTxLock = std::sync::Arc<tokio::sync::Mutex<()>>;

/// Everything the chain-backed endpoints need beyond the reader: the
/// wallet address whose balances `/wallet` reports, the chequebook
/// address (if one is deployed), the chain id bee-js branches on, and
/// the optional [`ChainWriter`] for the on-chain mutation endpoints.
pub struct ChainContext {
    pub reader: std::sync::Arc<dyn ChainReader>,
    pub wallet_eth: [u8; 20],
    pub chequebook: ChequebookSlot,
    pub chain_id: u64,
    /// Signer for the on-chain write endpoints. `None` → those endpoints
    /// return `501`.
    pub writer: Option<std::sync::Arc<dyn ChainWriter>>,
    /// Held across every wallet transaction; see [`WalletTxLock`].
    pub tx_lock: WalletTxLock,
}

/// Write txs (approve + createBatch, topUp, transfer) must clear a
/// Gnosis block and confirm; the `Wallet` waits up to ~60 s per tx and a
/// buy is two txs, so bound the handler at 3 minutes.
const CHAIN_TX_TIMEOUT: Duration = Duration::from_mins(3);

const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Run a chain future under [`CHAIN_RPC_TIMEOUT`], mapping the error /
/// timeout to a bee-shaped response so handlers stay terse.
async fn guarded<F, T>(fut: F) -> Result<T, Response>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    match tokio::time::timeout(CHAIN_RPC_TIMEOUT, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(json_error(
            StatusCode::BAD_GATEWAY,
            format!("chain rpc: {e}"),
        )),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "chain rpc request timed out",
        )),
    }
}

#[derive(Serialize)]
struct WalletBody {
    #[serde(rename = "bzzBalance")]
    bzz_balance: String,
    #[serde(rename = "nativeTokenBalance")]
    native_token_balance: String,
    #[serde(rename = "chainID")]
    chain_id: u64,
    /// Node wallet (EOA) address. bee-js's `WalletBalance` parser requires
    /// this as a string — omitting it makes `getWalletBalance()` throw
    /// (issue #5).
    #[serde(rename = "walletAddress")]
    wallet_address: String,
    /// Deployed chequebook contract address, or the all-zero sentinel when
    /// none is configured. Also required-as-string by bee-js.
    #[serde(rename = "chequebookContractAddress")]
    chequebook_contract_address: String,
}

/// `GET /wallet`. Real `bzzBalance` (xBZZ ERC-20) + `nativeTokenBalance`
/// (xDAI) for the node's wallet when a chain is configured; bee's
/// zero-stub with `chainID:100` otherwise (PLAN.md D1). Includes
/// `walletAddress` + `chequebookContractAddress` so bee-js's
/// `getWalletBalance()` parser is satisfied (issue #5).
pub async fn wallet(State(handle): State<GatewayHandle>) -> Response {
    let Some(chain) = handle.chain() else {
        if handle.chain_state().is_none() {
            return crate::error::chain_initializing();
        }
        return Json(WalletBody {
            bzz_balance: "0".into(),
            native_token_balance: "0".into(),
            chain_id: 100,
            wallet_address: ZERO_ADDRESS.into(),
            chequebook_contract_address: ZERO_ADDRESS.into(),
        })
        .into_response();
    };
    let bzz = match guarded(chain.reader.bzz_balance(chain.wallet_eth)).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let native = match guarded(chain.reader.native_balance(chain.wallet_eth)).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    Json(WalletBody {
        bzz_balance: bzz.to_string(),
        native_token_balance: native.to_string(),
        chain_id: chain.chain_id,
        wallet_address: format!("0x{}", hex::encode(chain.wallet_eth)),
        chequebook_contract_address: chain.chequebook.get().map_or_else(
            || ZERO_ADDRESS.to_string(),
            |a| format!("0x{}", hex::encode(a)),
        ),
    })
    .into_response()
}

#[derive(Serialize)]
struct ChequebookAddressBody {
    #[serde(rename = "chequebookAddress")]
    chequebook_address: String,
}

/// `GET /chequebook/address`. The deployed chequebook address, or the
/// all-zero sentinel when none is configured (bee's "no chequebook"
/// signal — Freedom keys "publish ready" off a non-zero value).
pub async fn chequebook_address(State(handle): State<GatewayHandle>) -> Response {
    // Freedom keys "publish ready" off a non-zero value, so during
    // chain init a premature zero would read as "no chequebook";
    // answer the retryable 503 instead.
    if handle.chain_state().is_none() {
        return crate::error::chain_initializing();
    }
    let addr = handle.chain().and_then(|c| c.chequebook.get()).map_or_else(
        || ZERO_ADDRESS.to_string(),
        |a| format!("0x{}", hex::encode(a)),
    );
    Json(ChequebookAddressBody {
        chequebook_address: addr,
    })
    .into_response()
}

#[derive(Serialize)]
struct ChequebookBalanceBody {
    #[serde(rename = "totalBalance")]
    total_balance: String,
    #[serde(rename = "availableBalance")]
    available_balance: String,
}

/// `GET /chequebook/balance`. Reports the chequebook contract's xBZZ
/// balance (bee's `Balance()` is just `BZZ.balanceOf(chequebook)`).
/// `availableBalance` mirrors it because `antd` doesn't draw down the
/// chequebook on-chain mid-session. Zeros when no chequebook is
/// configured (PLAN.md D2).
pub async fn chequebook_balance(State(handle): State<GatewayHandle>) -> Response {
    let zero = || {
        Json(ChequebookBalanceBody {
            total_balance: "0".into(),
            available_balance: "0".into(),
        })
        .into_response()
    };
    let Some(chain) = handle.chain() else {
        if handle.chain_state().is_none() {
            return crate::error::chain_initializing();
        }
        return zero();
    };
    let Some(cb) = chain.chequebook.get() else {
        return zero();
    };
    let bal = match guarded(chain.reader.chequebook_balance(cb)).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    Json(ChequebookBalanceBody {
        total_balance: bal.to_string(),
        available_balance: bal.to_string(),
    })
    .into_response()
}

#[derive(Serialize)]
struct StatusBody {
    overlay: String,
    #[serde(rename = "beeMode")]
    bee_mode: &'static str,
    #[serde(rename = "connectedPeers")]
    connected_peers: u32,
    #[serde(rename = "lastSyncedBlock")]
    last_synced_block: u64,
    proximity: u32,
    #[serde(rename = "reserveSize")]
    reserve_size: u64,
    #[serde(rename = "reserveSizeWithinRadius")]
    reserve_size_within_radius: u64,
    #[serde(rename = "pullsyncRate")]
    pullsync_rate: f64,
    #[serde(rename = "storageRadius")]
    storage_radius: u32,
    #[serde(rename = "neighborhoodSize")]
    neighborhood_size: u32,
    #[serde(rename = "requestFailed")]
    request_failed: bool,
    #[serde(rename = "batchCommitment")]
    batch_commitment: u64,
    #[serde(rename = "isReachable")]
    is_reachable: bool,
    #[serde(rename = "committedDepth")]
    committed_depth: u32,
}

/// `GET /status`. Freedom's sync-progress UI reads `lastSyncedBlock`;
/// the rest of bee's status body is emitted (mostly zeros for a light
/// node) so bee-js's `Status` parser sees every field it expects
/// (PLAN.md A2). Falls to `501` when no chain is configured.
pub async fn status(State(handle): State<GatewayHandle>) -> Response {
    let Some(chain) = handle.chain() else {
        if handle.chain_state().is_none() {
            return crate::error::chain_initializing();
        }
        return json_error(
            StatusCode::NOT_IMPLEMENTED,
            "status requires a configured chain RPC endpoint",
        );
    };
    let block = match guarded(chain.reader.block_number()).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let connected = handle.status.borrow().peers.connected;
    Json(StatusBody {
        overlay: format!("0x{}", handle.identity.overlay_hex),
        // The chain-initializing early-return above guarantees the
        // chain state is resolved by the time we read `light_mode`.
        bee_mode: if handle.chain_state().is_some_and(|s| s.light_mode) {
            "light"
        } else {
            "ultra-light"
        },
        connected_peers: connected,
        last_synced_block: block,
        proximity: 0,
        reserve_size: 0,
        reserve_size_within_radius: 0,
        pullsync_rate: 0.0,
        storage_radius: 0,
        neighborhood_size: 0,
        request_failed: false,
        batch_commitment: 0,
        is_reachable: true,
        committed_depth: 0,
    })
    .into_response()
}

#[derive(Serialize)]
struct ChainStateBody {
    #[serde(rename = "chainTip")]
    chain_tip: u64,
    block: u64,
    #[serde(rename = "totalAmount")]
    total_amount: String,
    #[serde(rename = "currentPrice")]
    current_price: String,
}

/// `GET /chainstate`. bee-js's stamp-cost math reads `currentPrice`;
/// `block` / `chainTip` / `totalAmount` round out the bee body
/// (PLAN.md A3, reclassified from the full-node 501). Falls to `501`
/// when no chain is configured.
pub async fn chainstate(State(handle): State<GatewayHandle>) -> Response {
    let Some(chain) = handle.chain() else {
        if handle.chain_state().is_none() {
            return crate::error::chain_initializing();
        }
        return json_error(
            StatusCode::NOT_IMPLEMENTED,
            "chainstate requires a configured chain RPC endpoint",
        );
    };
    let block = match guarded(chain.reader.block_number()).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let price = match guarded(chain.reader.current_price()).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    let total = match guarded(chain.reader.total_amount()).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    Json(ChainStateBody {
        chain_tip: block,
        block,
        total_amount: total.to_string(),
        current_price: price.to_string(),
    })
    .into_response()
}

// --- on-chain write endpoints (PLAN.md J.5 B2/B3, D3) ---

use axum::extract::{Path, Query};
use std::collections::HashMap;

/// Run a write (tx-submitting) chain future under [`CHAIN_TX_TIMEOUT`].
async fn guarded_tx<F, T>(fut: F) -> Result<T, Response>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    match tokio::time::timeout(CHAIN_TX_TIMEOUT, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(json_error(
            StatusCode::BAD_GATEWAY,
            format!("chain tx: {e}"),
        )),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "chain transaction timed out",
        )),
    }
}

/// Resolve the writer or produce the bee-shaped `501` used when no
/// funded wallet is configured.
/// Also returns the wallet's [`WalletTxLock`], which the caller holds
/// across its transaction.
#[allow(clippy::result_large_err)]
fn writer(
    handle: &GatewayHandle,
) -> Result<(std::sync::Arc<dyn ChainWriter>, WalletTxLock), Response> {
    if handle.chain_state().is_none() {
        return Err(crate::error::chain_initializing());
    }
    handle
        .chain()
        .and_then(|c| Some((c.writer.clone()?, c.tx_lock.clone())))
        .ok_or_else(|| {
            json_error(
                StatusCode::NOT_IMPLEMENTED,
                "on-chain writes require a configured wallet key + RPC endpoint",
            )
        })
}

#[allow(clippy::result_large_err)]
fn parse_batch_id(s: &str) -> Result<[u8; 32], Response> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut id = [0u8; 32];
    hex::decode_to_slice(s, &mut id)
        .map_err(|_| json_error(StatusCode::BAD_REQUEST, "batch id must be 32-byte hex"))?;
    Ok(id)
}

/// Register (or refresh) a freshly bought / diluted batch with the
/// running node so it can immediately stamp uploads against it — the
/// crux of bee/Freedom's "buy then publish" flow. Returns a bee-shaped
/// error response if the node loop can't be reached or rejects the
/// batch; the caller surfaces it instead of a misleading `201`.
#[allow(clippy::result_large_err)]
async fn register_batch(
    handle: &GatewayHandle,
    batch_id: [u8; 32],
    depth: u8,
    immutable: bool,
) -> Result<(), Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::RegisterBatch {
        batch_id,
        depth,
        bucket_depth: POSTAGE_BUCKET_DEPTH,
        immutable,
        ack: ack_tx,
    };
    if handle.commands.send(cmd).await.is_err() {
        return Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop is no longer accepting commands",
        ));
    }
    match tokio::time::timeout(CHAIN_RPC_TIMEOUT, ack_rx).await {
        Ok(Ok(ControlAck::Ok { .. })) => Ok(()),
        Ok(Ok(ControlAck::Error { message })) => Err(json_error(
            StatusCode::BAD_GATEWAY,
            format!("batch registration failed: {message}"),
        )),
        Ok(Ok(other)) => Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unexpected node ack for batch registration: {other:?}"),
        )),
        Ok(Err(_)) => Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop dropped the batch registration request",
        )),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "batch registration timed out",
        )),
    }
}

#[derive(Serialize)]
struct BatchIdBody {
    #[serde(rename = "batchID")]
    batch_id: String,
}

#[derive(Serialize)]
struct TxHashBody {
    #[serde(rename = "transactionHash")]
    transaction_hash: String,
}

/// `POST /stamps/{amount}/{depth}`. Buys a postage batch on-chain
/// (`approve` → `createBatch`) and returns its `batchID` (PLAN.md B2).
/// `amount` is the per-chunk balance (PLUR); `?immutable=true` makes the
/// batch immutable. Freedom then polls `GET /stamps/{id}` for `usable`.
pub async fn buy_stamp(
    State(handle): State<GatewayHandle>,
    Path((amount, depth)): Path<(String, u8)>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let Some(chain) = handle.chain() else {
        if handle.chain_state().is_none() {
            return crate::error::chain_initializing();
        }
        return json_error(
            StatusCode::NOT_IMPLEMENTED,
            "on-chain writes require a configured wallet key + RPC endpoint",
        );
    };
    let Some(w) = chain.writer.clone() else {
        return json_error(
            StatusCode::NOT_IMPLEMENTED,
            "on-chain writes require a configured wallet key + RPC endpoint",
        );
    };
    let amount: u128 = match amount.parse() {
        Ok(a) => a,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "amount must be a decimal integer"),
    };
    // Mirror bee's `CreateBatch` pre-submit guards so an obviously-bad buy
    // fails the bee way (a `400`) instead of falling through to a reverted
    // transaction surfaced as `502` (issue #5). bee requires `depth` to
    // exceed the bucket depth and the wallet's xBZZ balance to cover the
    // total cost `amount × 2^depth`.
    if depth <= POSTAGE_BUCKET_DEPTH {
        return json_error(StatusCode::BAD_REQUEST, "invalid depth");
    }
    let total_cost = 1u128
        .checked_shl(u32::from(depth))
        .and_then(|factor| amount.checked_mul(factor));
    // Held from the balance guard through the buy's last receipt, so a
    // background top-up can't spend the xBZZ counted here or take the
    // buy's nonce.
    let tx = chain.tx_lock.lock().await;
    let balance = match guarded(chain.reader.bzz_balance(chain.wallet_eth)).await {
        Ok(v) => v,
        Err(r) => return r,
    };
    // `total_cost == None` means it overflowed `u128`, i.e. it dwarfs any
    // realistic balance → treat as unaffordable.
    if total_cost.is_none_or(|cost| balance < cost) {
        return json_error(StatusCode::BAD_REQUEST, "out of funds");
    }
    let immutable = q
        .get("immutable")
        .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1");
    let batch_id = match guarded_tx(w.buy_batch(amount, depth, immutable)).await {
        Ok(id) => id,
        Err(r) => return r,
    };
    drop(tx);
    // Register the issuer with the running node *before* returning 201,
    // so Freedom's immediate `POST /bzz … Swarm-Postage-Batch-Id: <id>`
    // finds a usable batch without a restart. We construct the issuer
    // from the known buy params (depth, bucket_depth=16, immutable) and
    // never chain-read the batch back — Gnosis indexing lags the receipt.
    if let Err(r) = register_batch(&handle, batch_id, depth, immutable).await {
        return r;
    }
    if let Some(hook) = &handle.on_batch_bought {
        hook(batch_id);
    }
    (
        StatusCode::CREATED,
        Json(BatchIdBody {
            batch_id: hex::encode(batch_id),
        }),
    )
        .into_response()
}

/// `PATCH /stamps/topup/{id}/{amount}`. Tops up an existing batch's
/// per-chunk balance, extending its TTL (PLAN.md B3).
pub async fn topup_stamp(
    State(handle): State<GatewayHandle>,
    Path((id, amount)): Path<(String, String)>,
) -> Response {
    let (w, tx_lock) = match writer(&handle) {
        Ok(w) => w,
        Err(r) => return r,
    };
    let batch_id = match parse_batch_id(&id) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let amount: u128 = match amount.parse() {
        Ok(a) => a,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "amount must be a decimal integer"),
    };
    let _tx = tx_lock.lock().await;
    if let Err(r) = guarded_tx(w.topup_batch(batch_id, amount)).await {
        return r;
    }
    Json(BatchIdBody { batch_id: id }).into_response()
}

/// `PATCH /stamps/dilute/{id}/{depth}`. Increases a batch's depth
/// (doubling capacity per +1) — bee's "dilute" (PLAN.md B3).
pub async fn dilute_stamp(
    State(handle): State<GatewayHandle>,
    Path((id, depth)): Path<(String, u8)>,
) -> Response {
    let (w, tx_lock) = match writer(&handle) {
        Ok(w) => w,
        Err(r) => return r,
    };
    let batch_id = match parse_batch_id(&id) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let tx = tx_lock.lock().await;
    if let Err(r) = guarded_tx(w.dilute_batch(batch_id, depth)).await {
        return r;
    }
    drop(tx);
    // Bump the live issuer's depth so subsequent uploads use the larger
    // capacity. `immutable` is irrelevant for an existing issuer (the
    // register handler only updates depth when the batch is already
    // live), so pass `false`.
    if let Err(r) = register_batch(&handle, batch_id, depth, false).await {
        return r;
    }
    Json(BatchIdBody { batch_id: id }).into_response()
}

/// `POST /chequebook/deposit?amount=`. Funds the chequebook by
/// transferring xBZZ into it; returns the `transactionHash` (PLAN.md
/// D3). Freedom auto-deposits post-purchase.
pub async fn chequebook_deposit(
    State(handle): State<GatewayHandle>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let (w, tx_lock) = match writer(&handle) {
        Ok(w) => w,
        Err(r) => return r,
    };
    let amount: u128 = match q.get("amount").map(|a| a.parse()) {
        Some(Ok(a)) => a,
        Some(Err(_)) => {
            return json_error(StatusCode::BAD_REQUEST, "amount must be a decimal integer")
        }
        None => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "missing required ?amount= query param",
            )
        }
    };
    let _tx = tx_lock.lock().await;
    let tx = match guarded_tx(w.deposit_chequebook(amount)).await {
        Ok(h) => h,
        Err(r) => return r,
    };
    (
        StatusCode::CREATED,
        Json(TxHashBody {
            transaction_hash: format!("0x{}", hex::encode(tx)),
        }),
    )
        .into_response()
}
