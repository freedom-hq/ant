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
    ) -> Result<NewBatch, String>;
    /// `PostageStamp.topUp(batchId, amountPerChunk)`.
    async fn topup_batch(&self, batch_id: [u8; 32], amount_per_chunk: u128) -> Result<(), String>;
    /// `PostageStamp.increaseDepth(batchId, newDepth)` (a.k.a "dilute").
    async fn dilute_batch(&self, batch_id: [u8; 32], new_depth: u8) -> Result<(), String>;
    /// Fund the chequebook by transferring `amount` xBZZ into it.
    /// Returns the 32-byte transaction hash.
    async fn deposit_chequebook(&self, amount: u128) -> Result<[u8; 32], String>;

    // xDAI-only storage funding (`/v0/storage/*`, `/v0/settlement/deposit`):
    // the node swaps the xBZZ it lacks itself, so a user only ever sends
    // it plain xDAI. `ant_chain::funding` implements these for both
    // `antd` and `ant-ffi`. Defaulted to [`FundingFailure::Unsupported`]
    // (`501`) so test fakes and other embedders needn't implement them.

    /// Price a new plan of `depth` lasting `days`, including the
    /// chequebook deposit the purchase funds.
    async fn quote_plan(&self, _depth: u8, _days: u64) -> Result<StorageQuoteView, FundingFailure> {
        Err(FundingFailure::Unsupported)
    }
    /// Price extending `batch_id` by `days`, or with `new_depth`,
    /// resizing it while keeping its expiry plus `days`.
    async fn quote_extend(
        &self,
        _batch_id: [u8; 32],
        _new_depth: Option<u8>,
        _days: u64,
    ) -> Result<StorageQuoteView, FundingFailure> {
        Err(FundingFailure::Unsupported)
    }
    /// Buy a plan, swapping xDAI for the xBZZ it and the deposit need.
    /// Returns the new batch id; the route registers it.
    async fn buy_with_xdai(
        &self,
        _depth: u8,
        _amount_per_chunk: u128,
        _immutable: bool,
    ) -> Result<NewBatch, FundingFailure> {
        Err(FundingFailure::Unsupported)
    }
    /// Extend (and with `new_depth`, resize) `batch_id`, swapping xDAI
    /// for the xBZZ it needs. Returns the batch's depth afterwards.
    async fn extend_with_xdai(
        &self,
        _batch_id: [u8; 32],
        _new_depth: Option<u8>,
        _amount_per_chunk: u128,
    ) -> Result<u8, FundingFailure> {
        Err(FundingFailure::Unsupported)
    }
    /// The chequebook's settlement deposit and what topping it up takes.
    async fn deposit_status(&self) -> Result<DepositView, FundingFailure> {
        Err(FundingFailure::Unsupported)
    }
    /// Top the chequebook's deposit up to its target, swapping xDAI for
    /// the xBZZ it needs. Returns the status afterwards.
    async fn fund_deposit_with_xdai(&self) -> Result<DepositView, FundingFailure> {
        Err(FundingFailure::Unsupported)
    }
}

/// A batch a buy just created: its id and the block of its
/// `createBatch` receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewBatch {
    pub id: [u8; 32],
    pub block: u64,
}

/// Why an xDAI storage-funding call failed, so the route answers with
/// the right status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FundingFailure {
    /// The writer can't fund storage with xDAI. → `501`.
    Unsupported,
    /// Bad input, or the wallet is short of xDAI. The message is written
    /// for users. → `400`.
    Rejected(String),
    /// The batch isn't on chain. → `404`.
    NotFound(String),
    /// A chain read or transaction failed. → `502`.
    Chain(String),
    /// The chain says no to the node's chequebook (the deposit top-up's
    /// checks): nothing was swapped or sent. The route hands it to
    /// [`GatewayHandle::on_chequebook_refused`] before answering.
    ChequebookRefused {
        chequebook: [u8; 20],
        refusal: ChequebookRefusal,
        /// For the user.
        message: String,
    },
}

/// Which chequebook check said no (`ant_chain::chequebook_store::
/// ChequebookVerdict`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChequebookRefusal {
    /// The Swarm chequebook factory doesn't know it. For a chequebook
    /// the node deployed moments ago this can be an RPC that hasn't seen
    /// the deploy yet.
    NotRegistered,
    /// Its `issuer()` is this other address, not the node wallet.
    IssuerMismatch([u8; 20]),
}

/// What paying with xDAI takes (`ant_chain::funding::Funding`). PLUR
/// for xBZZ, wei for xDAI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FundingView {
    pub wallet_bzz: u128,
    pub wallet_xdai: u128,
    pub bzz_to_acquire: u128,
    pub swap_input_wei: u128,
    pub gas_reserve_wei: u128,
    pub xdai_required_wei: u128,
    pub xdai_to_send_wei: u128,
    pub sufficient: bool,
}

/// The price of a new plan, an extension or a resize.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageQuoteView {
    /// The plan's depth: the new depth for a resize.
    pub depth: u8,
    pub days: u64,
    /// Per-chunk balance the transaction pays. Clients pass it back
    /// unchanged to buy or extend.
    pub amount_per_chunk: u128,
    pub plan_cost_plur: u128,
    /// The chequebook deposit bought along with a new plan.
    pub deposit_due_plur: u128,
    pub funding: FundingView,
}

/// The chequebook's settlement deposit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepositView {
    /// `None` while the node has no chequebook yet.
    pub chequebook: Option<[u8; 20]>,
    /// Whether the node keeps the deposit topped up itself.
    pub managed: bool,
    pub deposited_plur: u128,
    pub target_plur: u128,
    pub shortfall_plur: u128,
    /// All zero when nothing is missing.
    pub funding: FundingView,
}

/// Lets one on-chain write *route* run at a time on a node. The write
/// routes answer `409` while another one runs instead of queueing, so a
/// client retrying a slow buy can't buy twice.
///
/// Only the HTTP write routes take it. The embedder's background chain
/// work (antd's after-buy settlement and startup top-up, ant-ffi's C
/// API and after-buy task) never does: it would otherwise `409` the
/// user's next buy for as long as a deposit transfer confirms. Those
/// tasks and the routes are kept apart by the [`WalletTxLock`] instead,
/// which a route takes *after* the gate and waits for.
///
/// # Lock order
///
/// Four locks guard the node wallet, each with its own job. Whoever
/// holds more than one took them in this order, and nothing takes an
/// earlier one while holding a later one:
///
/// 1. [`WriteGate`] (this): one write route at a time, `try` only.
/// 2. The embedder's settlement lock, if any (antd's after-buy state,
///    ant-ffi's chequebook-setup lock), which a refused-chequebook hook
///    takes; routes call that hook only after releasing 3.
/// 3. [`WalletTxLock`]: one read-then-write sequence at a time (balance
///    guard → `createBatch`, shortfall read → transfer), shared with the
///    embedder's own spend paths. Not reentrant: released before the
///    after-buy hook or any other path that takes it.
/// 4. `ant-chain`'s per-key sender lock inside every transaction send
///    (nonce read → receipt). A leaf: nothing is taken while it's held.
///    It also covers a send that shares no [`WalletTxLock`] (antd's
///    startup chequebook deploy, before the gateway's chain context
///    exists).
#[derive(Debug, Clone, Default)]
pub struct WriteGate(std::sync::Arc<tokio::sync::Mutex<()>>);

impl WriteGate {
    /// Hold the gate for one write; `None` while another write holds it.
    #[must_use]
    pub fn try_begin(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        self.0.clone().try_lock_owned().ok()
    }
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
pub struct ChequebookSlot(
    std::sync::Arc<std::sync::RwLock<Option<[u8; 20]>>>,
    std::sync::Arc<std::sync::RwLock<Option<[u8; 20]>>>,
);

impl ChequebookSlot {
    #[must_use]
    pub fn new(chequebook: Option<[u8; 20]>) -> Self {
        Self(
            std::sync::Arc::new(std::sync::RwLock::new(chequebook)),
            std::sync::Arc::default(),
        )
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
        self.set_refused(None);
    }

    /// Forget the chequebook: the node no longer settles with one (e.g.
    /// the chain check disqualified it), so `/chequebook/*` reports none
    /// and `POST /chequebook/deposit` has nothing to fund.
    pub fn clear(&self) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        self.set_refused(None);
    }

    /// Forget `chequebook` because the chain check disqualified it:
    /// like [`Self::clear`], and the node still *has* that chequebook,
    /// so a buy won't deploy another one. `/v0/storage/quote` then
    /// prices no deposit and `/v0/settlement/deposit` funds none.
    /// [`Self::set`] (a chequebook that checks out) undoes it.
    pub fn refuse(&self, chequebook: [u8; 20]) {
        self.clear();
        self.set_refused(Some(chequebook));
    }

    /// The chequebook [`Self::refuse`] recorded, while no usable one
    /// replaced it.
    #[must_use]
    pub fn refused(&self) -> Option<[u8; 20]> {
        *self
            .1
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn set_refused(&self, chequebook: Option<[u8; 20]>) {
        *self
            .1
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = chequebook;
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
/// only serializes senders that share the one lock. Taken after the
/// [`WriteGate`] and before `ant-chain`'s per-key sender lock; see
/// [`WriteGate`]'s lock order.
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
    /// One on-chain write *route* at a time: the write routes answer
    /// `409` while another runs; see [`WriteGate`].
    pub writes: WriteGate,
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
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
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
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
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

/// The chain context and its writer, or the chain-init `503`, or the
/// bee-shaped `501` used when no funded wallet is configured. A write
/// route takes the context's [`WriteGate`], then holds its
/// [`WalletTxLock`] across its transactions.
#[allow(clippy::result_large_err)]
fn writer(
    handle: &GatewayHandle,
) -> Result<
    (
        std::sync::Arc<ChainContext>,
        std::sync::Arc<dyn ChainWriter>,
    ),
    Response,
> {
    if handle.chain_state().is_none() {
        return Err(crate::error::chain_initializing());
    }
    handle
        .chain()
        .and_then(|c| c.writer.clone().map(|w| (c, w)))
        .ok_or_else(|| {
            json_error(
                StatusCode::NOT_IMPLEMENTED,
                "on-chain writes require a configured wallet key + RPC endpoint",
            )
        })
}

/// The `409` a write route answers while another write holds the
/// [`WriteGate`].
fn busy() -> Response {
    json_error(
        StatusCode::CONFLICT,
        "another on-chain operation of this node is in progress; retry when it finishes",
    )
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
    bought_at_block: Option<u64>,
) -> Result<(), Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    let cmd = ControlCommand::RegisterBatch {
        batch_id,
        depth,
        bucket_depth: POSTAGE_BUCKET_DEPTH,
        immutable,
        bought_at_block,
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
/// `amount` is the per-chunk balance (PLUR). The batch is immutable
/// unless the `immutable` header (bee-js) or `?immutable=` query says
/// `false`. Freedom then polls `GET /stamps/{id}` for `usable`.
pub async fn buy_stamp(
    State(handle): State<GatewayHandle>,
    Path((amount, depth)): Path<(String, u8)>,
    Query(q): Query<HashMap<String, String>>,
    headers: axum::http::HeaderMap,
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
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
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
    // bee-js sends the flag as an `immutable` header; antd used to read
    // only the query, so every bee-js buy came out mutable. Honour both
    // (the query wins when both are set) and default to immutable, like
    // bee: a full mutable batch silently overwrites its oldest chunks,
    // where a full immutable one fails the upload and can be resized.
    let immutable = match flag(
        q.get("immutable")
            .map(String::as_str)
            .or_else(|| headers.get("immutable").and_then(|v| v.to_str().ok())),
        "immutable",
        true,
    ) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let new = match guarded_tx(w.buy_batch(amount, depth, immutable)).await {
        Ok(new) => new,
        Err(r) => return r,
    };
    drop(tx);
    bought(&handle, new, depth, immutable).await
}

/// Finish a buy: register the batch, fire the after-buy hook, answer
/// `201 {batchID}`. Shared by `POST /stamps` and `POST /v0/storage/buy`.
/// The caller has already released the [`WalletTxLock`]: the hook's
/// settlement work takes it itself.
///
/// The issuer is registered with the running node *before* the `201`,
/// so Freedom's immediate `POST /bzz … Swarm-Postage-Batch-Id: <id>`
/// finds a usable batch without a restart. It's built from the known
/// buy params (depth, `bucket_depth` = 16, immutable); the batch is never
/// read back from chain, because Gnosis indexing lags the receipt.
async fn bought(handle: &GatewayHandle, new: NewBatch, depth: u8, immutable: bool) -> Response {
    // With its creation block, the node reports the batch `usable:
    // false` until the storers have synced it (bee's confirmation
    // window), instead of letting the first upload be rejected.
    if let Err(r) = register_batch(handle, new.id, depth, immutable, Some(new.block)).await {
        return r;
    }
    if let Some(hook) = &handle.on_batch_bought {
        hook(new.id);
    }
    (
        StatusCode::CREATED,
        Json(BatchIdBody {
            batch_id: hex::encode(new.id),
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
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
    };
    let batch_id = match parse_batch_id(&id) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let amount: u128 = match amount.parse() {
        Ok(a) => a,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "amount must be a decimal integer"),
    };
    let _tx = chain.tx_lock.lock().await;
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
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
    };
    let batch_id = match parse_batch_id(&id) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let tx = chain.tx_lock.lock().await;
    if let Err(r) = guarded_tx(w.dilute_batch(batch_id, depth)).await {
        return r;
    }
    drop(tx);
    // Bump the live issuer's depth so subsequent uploads use the larger
    // capacity. `immutable` is irrelevant for an existing issuer (the
    // register handler only updates depth when the batch is already
    // live), so pass `false`.
    if let Err(r) = register_batch(&handle, batch_id, depth, false, None).await {
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
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
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
    let _tx = chain.tx_lock.lock().await;
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

// --- xDAI storage funding (`/v0/storage/*`, `/v0/settlement/deposit`) ---
//
// Ant-specific routes, namespaced under `/v0/` like `/v0/manifest`. They
// price and pay for storage from the node wallet's plain xDAI: the node
// swaps whatever xBZZ it lacks itself (`ant_chain::funding`). Amounts
// are decimal strings, PLUR for xBZZ and wei for xDAI.

/// Bound on a quote or deposit read: several RPC reads (price, pool,
/// balances, chequebook), so longer than one [`CHAIN_RPC_TIMEOUT`].
const FUNDING_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Bound on an xDAI-funded write: up to four transactions (swap,
/// approve, `createBatch` or `topUp`, `increaseDepth`), each waiting up
/// to 60 s, possibly queued behind a transaction the node is already
/// sending.
const FUNDING_TX_TIMEOUT: Duration = Duration::from_mins(5);

#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
async fn funding_call<F, T>(limit: Duration, timed_out: &str, fut: F) -> Result<T, Response>
where
    F: std::future::Future<Output = Result<T, FundingFailure>>,
{
    match tokio::time::timeout(limit, fut).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(f)) => Err(failure_response(f)),
        Err(_) => Err(json_error(StatusCode::GATEWAY_TIMEOUT, timed_out)),
    }
}

fn failure_response(f: FundingFailure) -> Response {
    match f {
        FundingFailure::Unsupported => json_error(
            StatusCode::NOT_IMPLEMENTED,
            "this node can't fund storage with xDAI",
        ),
        FundingFailure::Rejected(m) | FundingFailure::ChequebookRefused { message: m, .. } => {
            json_error(StatusCode::BAD_REQUEST, m)
        }
        FundingFailure::NotFound(m) => json_error(StatusCode::NOT_FOUND, m),
        FundingFailure::Chain(m) => json_error(StatusCode::BAD_GATEWAY, m),
    }
}

/// `?name=` parsed as `T`, `None` when absent.
#[allow(clippy::result_large_err)]
fn param<T: std::str::FromStr>(
    q: &HashMap<String, String>,
    name: &str,
) -> Result<Option<T>, Response> {
    q.get(name)
        .map(|v| {
            v.trim()
                .parse()
                .map_err(|_| json_error(StatusCode::BAD_REQUEST, format!("invalid ?{name}=")))
        })
        .transpose()
}

#[allow(clippy::result_large_err)]
fn required<T: std::str::FromStr>(q: &HashMap<String, String>, name: &str) -> Result<T, Response> {
    param(q, name)?.ok_or_else(|| {
        json_error(
            StatusCode::BAD_REQUEST,
            format!("missing required ?{name}= query param"),
        )
    })
}

/// A boolean flag given as `true`/`false`/`1`/`0`.
#[allow(clippy::result_large_err)]
fn flag(value: Option<&str>, name: &str, default: bool) -> Result<bool, Response> {
    match value.map(str::trim) {
        None => Ok(default),
        Some(v) if v.eq_ignore_ascii_case("true") || v == "1" => Ok(true),
        Some(v) if v.eq_ignore_ascii_case("false") || v == "0" => Ok(false),
        Some(_) => Err(json_error(
            StatusCode::BAD_REQUEST,
            format!("{name} must be true or false"),
        )),
    }
}

#[allow(clippy::result_large_err)]
fn valid_depth(depth: u8) -> Result<u8, Response> {
    if depth <= POSTAGE_BUCKET_DEPTH {
        return Err(json_error(StatusCode::BAD_REQUEST, "invalid depth"));
    }
    Ok(depth)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct FundingBody {
    wallet_address: String,
    wallet_bzz_plur: String,
    wallet_xdai_wei: String,
    bzz_to_acquire_plur: String,
    swap_input_wei: String,
    gas_reserve_wei: String,
    xdai_required_wei: String,
    xdai_to_send_wei: String,
    sufficient_funds: bool,
}

impl FundingBody {
    fn of(wallet: [u8; 20], f: &FundingView) -> Self {
        Self {
            wallet_address: format!("0x{}", hex::encode(wallet)),
            wallet_bzz_plur: f.wallet_bzz.to_string(),
            wallet_xdai_wei: f.wallet_xdai.to_string(),
            bzz_to_acquire_plur: f.bzz_to_acquire.to_string(),
            swap_input_wei: f.swap_input_wei.to_string(),
            gas_reserve_wei: f.gas_reserve_wei.to_string(),
            xdai_required_wei: f.xdai_required_wei.to_string(),
            xdai_to_send_wei: f.xdai_to_send_wei.to_string(),
            sufficient_funds: f.sufficient,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct QuoteBody {
    depth: u8,
    days: u64,
    amount_per_chunk: String,
    plan_cost_plur: String,
    settlement_deposit_plur: String,
    #[serde(flatten)]
    funding: FundingBody,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DepositBody {
    chequebook: Option<String>,
    managed: bool,
    deposit_plur: String,
    target_plur: String,
    shortfall_plur: String,
    needs_top_up: bool,
    #[serde(flatten)]
    funding: FundingBody,
}

fn deposit_response(wallet: [u8; 20], d: &DepositView) -> Response {
    Json(DepositBody {
        chequebook: d.chequebook.map(|cb| format!("0x{}", hex::encode(cb))),
        managed: d.managed,
        deposit_plur: d.deposited_plur.to_string(),
        target_plur: d.target_plur.to_string(),
        shortfall_plur: d.shortfall_plur.to_string(),
        needs_top_up: d.shortfall_plur > 0,
        funding: FundingBody::of(wallet, &d.funding),
    })
    .into_response()
}

/// `GET /v0/storage/quote?depth=&days=` prices a new plan;
/// `?batchId=&days=` prices extending a batch by `days`, and
/// `?batchId=&days=&depth=` resizing it to `depth` while keeping its
/// expiry plus `days` (which may be 0). No transaction is sent, and the
/// write gate isn't taken: a quote is answered during a buy too.
pub async fn storage_quote(
    State(handle): State<GatewayHandle>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    let quote = async {
        let days: u64 = required(&q, "days")?;
        let depth = param::<u8>(&q, "depth")?.map(valid_depth).transpose()?;
        #[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
        let batch_id = q.get("batchId").map(|id| parse_batch_id(id)).transpose()?;
        let fut = async {
            match (batch_id, depth) {
                (Some(id), new_depth) => w.quote_extend(id, new_depth, days).await,
                (None, Some(depth)) => w.quote_plan(depth, days).await,
                (None, None) => Err(FundingFailure::Rejected(
                    "missing required ?depth= (new plan) or ?batchId= (extend)".into(),
                )),
            }
        };
        if days == 0 && (batch_id.is_none() || depth.is_none()) {
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                "days must be at least 1 (0 only when resizing)",
            ));
        }
        funding_call(FUNDING_READ_TIMEOUT, "chain rpc request timed out", fut).await
    };
    match quote.await {
        Ok(v) => Json(QuoteBody {
            depth: v.depth,
            days: v.days,
            amount_per_chunk: v.amount_per_chunk.to_string(),
            plan_cost_plur: v.plan_cost_plur.to_string(),
            settlement_deposit_plur: v.deposit_due_plur.to_string(),
            funding: FundingBody::of(chain.wallet_eth, &v.funding),
        })
        .into_response(),
        Err(r) => r,
    }
}

/// `POST /v0/storage/buy?depth=&amountPerChunk=&immutable=` buys a plan
/// paid from the node wallet's xDAI (swapping for the xBZZ it and the
/// chequebook deposit need), registers it, and runs the same after-buy
/// settlement as `POST /stamps`. `immutable` defaults to `true`, like
/// bee. `amountPerChunk` comes from the matching quote.
pub async fn storage_buy(
    State(handle): State<GatewayHandle>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    #[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
    let params = (|| {
        let depth = valid_depth(required(&q, "depth")?)?;
        let amount: u128 = required(&q, "amountPerChunk")?;
        let immutable = flag(q.get("immutable").map(String::as_str), "immutable", true)?;
        Ok::<_, Response>((depth, amount, immutable))
    })();
    let (depth, amount, immutable) = match params {
        Ok(p) => p,
        Err(r) => return r,
    };
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
    };
    // Held from the balance reads through the last receipt, so the
    // embedder's background settlement can't spend the xBZZ counted
    // here; released before `bought` fires the after-buy hook, whose
    // settlement work takes it itself.
    let tx = chain.tx_lock.lock().await;
    let new = match funding_call(
        FUNDING_TX_TIMEOUT,
        "chain transaction timed out",
        w.buy_with_xdai(depth, amount, immutable),
    )
    .await
    {
        Ok(new) => new,
        Err(r) => return r,
    };
    drop(tx);
    bought(&handle, new, depth, immutable).await
}

/// `POST /v0/storage/extend?batchId=&amountPerChunk=[&depth=]` tops a
/// batch up paid from the node wallet's xDAI; with `depth`, then resizes
/// it and re-registers it at the new depth. `amountPerChunk` comes from
/// the matching quote (same `depth`).
pub async fn storage_extend(
    State(handle): State<GatewayHandle>,
    Query(q): Query<HashMap<String, String>>,
) -> Response {
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    #[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
    let params = (|| {
        let batch_id = parse_batch_id(q.get("batchId").map_or("", String::as_str))?;
        let amount: u128 = required(&q, "amountPerChunk")?;
        let new_depth = param::<u8>(&q, "depth")?.map(valid_depth).transpose()?;
        Ok::<_, Response>((batch_id, amount, new_depth))
    })();
    let (batch_id, amount, new_depth) = match params {
        Ok(p) => p,
        Err(r) => return r,
    };
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
    };
    let tx = chain.tx_lock.lock().await;
    let depth = match funding_call(
        FUNDING_TX_TIMEOUT,
        "chain transaction timed out",
        w.extend_with_xdai(batch_id, new_depth, amount),
    )
    .await
    {
        Ok(d) => d,
        Err(r) => return r,
    };
    drop(tx);
    if new_depth.is_some() {
        // Bump the live issuer's depth so uploads use the new capacity
        // (`immutable` is ignored for an existing issuer, see
        // `dilute_stamp`).
        if let Err(r) = register_batch(&handle, batch_id, depth, false, None).await {
            return r;
        }
    }
    Json(BatchIdBody {
        batch_id: hex::encode(batch_id),
    })
    .into_response()
}

/// `GET /v0/settlement/deposit`: the chequebook's settlement deposit and
/// what topping it up to its target takes, priced in xDAI. No
/// transaction is sent.
pub async fn settlement_deposit(State(handle): State<GatewayHandle>) -> Response {
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    match funding_call(
        FUNDING_READ_TIMEOUT,
        "chain rpc request timed out",
        w.deposit_status(),
    )
    .await
    {
        Ok(d) => deposit_response(chain.wallet_eth, &d),
        Err(r) => r,
    }
}

/// `POST /v0/settlement/deposit` tops the chequebook's deposit up to
/// its target, paid from the node wallet's xDAI. A no-op when it's
/// already there. Returns the status afterwards.
///
/// The deposit and balances are read under the [`WalletTxLock`], so a
/// background top-up that just landed isn't paid twice. Nothing is
/// spent on a chequebook the chain rejects (`ant_chain::funding`); such
/// a refusal goes to [`GatewayHandle::on_chequebook_refused`] once the
/// lock is released, and the embedder decides whether it stands (and
/// switches settlement off) or is an RPC that hasn't seen its deploy.
pub async fn settlement_fund_deposit(State(handle): State<GatewayHandle>) -> Response {
    let (chain, w) = match writer(&handle) {
        Ok(cw) => cw,
        Err(r) => return r,
    };
    let Some(_one_write) = chain.writes.try_begin() else {
        return busy();
    };
    let tx = chain.tx_lock.lock().await;
    let funded = tokio::time::timeout(FUNDING_TX_TIMEOUT, w.fund_deposit_with_xdai()).await;
    drop(tx);
    match funded {
        Ok(Ok(d)) => deposit_response(chain.wallet_eth, &d),
        Ok(Err(FundingFailure::ChequebookRefused {
            chequebook,
            refusal,
            message,
        })) => {
            let stands = match &handle.on_chequebook_refused {
                Some(hook) => hook(chequebook, refusal).await,
                None => true,
            };
            if stands {
                json_error(StatusCode::BAD_REQUEST, message)
            } else {
                json_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "the chain RPC hasn't caught up with this node's just-deployed chequebook \
                     yet; nothing was sent, try again in a few minutes",
                )
            }
        }
        Ok(Err(f)) => failure_response(f),
        Err(_) => json_error(StatusCode::GATEWAY_TIMEOUT, "chain transaction timed out"),
    }
}
