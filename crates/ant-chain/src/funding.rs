//! Storage funding shared by `antd` and `ant-ffi`: price a postage plan
//! against the node wallet, and pay for it (or for extending or resizing
//! it, or for the chequebook's settlement deposit) with **xDAI only**.
//!
//! The node wallet is the only account involved. The user sends it plain
//! xDAI; the node swaps whatever xBZZ it lacks through the stateless
//! `BzzSwapHelper` ([`crate::tx::swap_helper_address`]), then approves
//! and buys. Every quote is computed from the wallet's balances at that
//! moment, so a retry after a partial failure (the swap landed,
//! `createBatch` didn't) only asks for, and only swaps, what is still
//! missing.
//!
//! Every paying call takes a `max_xdai_wei`: the most xDAI the caller
//! lets the swap send, normally the `swap_input_wei` of the quote the
//! user approved. The swap input is re-priced from the pool's `slot0()`
//! at execution time, and nothing else ties it to that quote, so a
//! forged or moved price (a sandwich, an untrusted fallback read) is
//! refused with [`FundingError::PriceMoved`] before anything is sent
//! (audit MY-03, #305). A small upward tick that the quote's swap buffer
//! still absorbs goes through, sending at most the cap
//! ([`capped_swap_value`]), once a dry run of it passes; a dry run that
//! reverts is refused the same way, after the one-time helper deploy
//! may have gone out (the refusal then says so).
//!
//! One orchestration, two sequencers (AGENTS.md): `ant-ffi`'s C API
//! (`ant_storage_quote`, `ant_storage_buy_xdai`, …) and the gateway's
//! `/v0/storage/*` routes (served by `antd` and by `ant-ffi`'s
//! `ant_start_gateway`) both call the `pub` helpers here. The
//! `parity_guard` test in `ant-ffi` checks that.

use primitive_types::U256;
use thiserror::Error;

use crate::chequebook_store::{ChequebookError, ChequebookVerdict, IssuerRead, TopUp};
use crate::tx::{TxError, Wallet};
use crate::ChainClient;

/// xBZZ has 16 decimals: one whole xBZZ is `10^16` PLUR.
pub const PLUR_PER_BZZ: u128 = 10_000_000_000_000_000;

/// xDAI, Gnosis's native gas token, has 18 decimals.
pub const WEI_PER_XDAI: u128 = 1_000_000_000_000_000_000;

/// Gnosis block time in seconds. The postage price is per chunk *per
/// block*, so a duration in days converts to a per-chunk balance through
/// the block count.
pub const GNOSIS_BLOCK_SECS: u128 = 5;

/// Postage collision-bucket depth (bee's constant). Every `createBatch`
/// uses it, the registered issuer must match it, and a batch's depth
/// must exceed it.
pub const POSTAGE_BUCKET_DEPTH: u8 = 16;

/// xDAI (wei) a funding operation keeps aside for the node's own gas, on
/// top of the swap input. A first xDAI buy submits up to six
/// transactions (helper deploy, swap, approve, `createBatch`, then the
/// one-time chequebook deploy and its deposit transfer), about
/// 0.007 xDAI at the 2 gwei default. 0.015 xDAI keeps that comfortable:
/// running the wallet dry right before the chequebook step would leave
/// settlement off, which is the failure #73 fixed.
pub const GAS_RESERVE_WEI: u128 = 15_000_000_000_000_000;

/// Slippage and fee buffer on the fair-value swap input: we send
/// `fair × 105 / 100` xDAI so the 0.3% pool fee and a few percent of
/// price impact still clear the `amountOutMin` floor. Any excess stays
/// in the wallet as a little extra xBZZ.
const SWAP_BUFFER_NUM: u128 = 105;
const SWAP_BUFFER_DEN: u128 = 100;

/// The least margin over fair value a capped swap still goes through
/// with: when the price ticked up since the quote and the re-priced
/// input (fresh price + [`SWAP_BUFFER_NUM`]) is over the caller's cap,
/// the swap sends the cap itself as long as the cap still covers the
/// fresh fair value by this much (the 0.3% pool fee plus a little price
/// impact). `amountOutMin` stays the xBZZ needed, so the chain refuses
/// a swap that would buy less, and nothing past the approved cap is
/// ever sent. A large swap's price impact can eat that margin, so a
/// capped swap is dry-run (`eth_call`) before it is sent and refused
/// with [`FundingError::PriceMoved`] if it would revert, instead of
/// spending the gas of a reverted transaction.
const SWAP_MIN_MARGIN_NUM: u128 = 101;
const SWAP_MIN_MARGIN_DEN: u128 = 100;

/// What a storage purchase funds besides the plan itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepositPolicy {
    /// The node doesn't manage a chequebook deposit (`antd`
    /// `--no-auto-chequebook`, or a manual `--chequebook`): price and
    /// buy the plan alone.
    Unmanaged,
    /// The node keeps its chequebook at `target` PLUR. `chequebook` is
    /// the one it settles with now, or `None` when the purchase is what
    /// will deploy one (funded).
    Managed {
        chequebook: Option<[u8; 20]>,
        target: u128,
    },
}

/// What paying with xDAI takes: the xBZZ still to acquire, the swap
/// input and gas reserve that buys it, and how that compares to the
/// wallet. Every field is a raw amount (PLUR for xBZZ, wei for xDAI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Funding {
    /// The node wallet's xBZZ, or `None` when it couldn't be read. The
    /// pricing below counts an unread balance as `0`, so a flaky RPC
    /// over-quotes rather than under-quotes.
    pub wallet_bzz: Option<u128>,
    /// The node wallet's xDAI, or `None` when it couldn't be read. As
    /// with [`Self::wallet_bzz`], the pricing counts it as `0`, so a
    /// flaky RPC asks for too much rather than too little.
    pub wallet_xdai: Option<u128>,
    /// xBZZ the operation still has to acquire by swapping.
    pub bzz_to_acquire: u128,
    /// xDAI the node will swap for [`Self::bzz_to_acquire`].
    pub swap_input_wei: u128,
    /// xDAI kept for the node's own transactions ([`GAS_RESERVE_WEI`]),
    /// or `0` when there is nothing to do.
    pub gas_reserve_wei: u128,
    /// `swap_input_wei + gas_reserve_wei`: what the wallet must hold.
    pub xdai_required_wei: u128,
    /// What the user still has to send the node wallet.
    pub xdai_to_send_wei: u128,
    /// Whether the wallet already holds [`Self::xdai_required_wei`].
    pub sufficient: bool,
}

/// The price of a new plan, or of extending or resizing an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanQuote {
    /// The plan's depth: the new depth for a resize.
    pub depth: u8,
    pub days: u64,
    /// Per-chunk balance (PLUR) the transaction pays: `createBatch`'s
    /// initial balance, or `topUp`'s amount. Pass it back unchanged so
    /// the charge matches the quote the user saw.
    pub amount_per_chunk: u128,
    /// What the postage transaction costs in xBZZ.
    pub plan_cost_plur: u128,
    /// The chequebook deposit bought along with a new plan (see
    /// [`DepositPolicy`]). Always `0` for an extension or resize.
    pub deposit_due_plur: u128,
    pub funding: Funding,
}

/// A chequebook's settlement deposit, and what topping it up to its
/// target would take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepositStatus {
    pub chequebook: [u8; 20],
    /// The chequebook's xBZZ, which is what backs its cheques.
    pub deposited_plur: u128,
    pub target_plur: u128,
    /// `target − deposited`, floored at 0.
    pub shortfall_plur: u128,
    /// The wallet's balances, and what a top-up takes: every top-up
    /// figure is 0 when the deposit is already at its target, but the
    /// balances are still read and reported.
    pub funding: Funding,
}

impl DepositStatus {
    /// Whether the deposit is below its target. The one predicate the
    /// status surfaces and the top-up action both read.
    #[must_use]
    pub const fn needs_top_up(&self) -> bool {
        self.shortfall_plur > 0
    }
}

/// A batch a buy just created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewBatch {
    pub id: [u8; 32],
    /// The block of its `createBatch` receipt: bee's `blockNumber`, and
    /// where the storers' propagation window starts.
    pub block: u64,
}

/// Why a funding call failed. `Display` is the message the apps show.
#[derive(Debug, Error)]
pub enum FundingError {
    /// Bad input: a zero price, a depth the contract would reject, a
    /// cost that overflows.
    #[error("{0}")]
    Invalid(String),
    /// The swap would send more xDAI than the caller's `max_xdai_wei`
    /// (the quote the user approved) allows: the pool price moved past
    /// what the cap still covers (see [`SWAP_MIN_MARGIN_NUM`]), the read
    /// was forged, or the call has more xBZZ to buy than when it was
    /// quoted (the wallet's xBZZ was spent, or the deposit due grew), or
    /// a dry run of the capped swap reverted (the cap doesn't cover the
    /// pool fee plus price impact). Nothing was sent, apart from the
    /// one-time swap helper deploy in that last case when
    /// `helper_deployed` (later swaps reuse it).
    #[error("{}", price_moved_message(*input_wei, *max_wei, *helper_deployed))]
    PriceMoved {
        input_wei: u128,
        max_wei: u128,
        helper_deployed: bool,
    },
    /// The wallet holds less xDAI than the swap plus the gas reserve.
    #[error(
        "not enough xDAI: send {} more xDAI to your account, then try again",
        format_xdai_up(*short_wei)
    )]
    InsufficientXdai { short_wei: u128 },
    /// A chain read failed.
    #[error("{what}: {message}")]
    Read { what: &'static str, message: String },
    /// A transaction failed to send, reverted or timed out.
    #[error("{what}: {source}")]
    Tx { what: &'static str, source: TxError },
    /// `createBatch` confirmed without a `BatchCreated` event.
    #[error("storage purchase receipt had no batch")]
    NoBatchInReceipt,
    /// The chain says no to the chequebook a deposit was meant for
    /// ([`crate::chequebook_store::check_chequebook`]): nothing was
    /// swapped or sent. A factory "not registered" for a chequebook the
    /// node just deployed can be a lagging RPC; the caller tells the two
    /// apart with [`crate::chequebook_store::not_registered_may_be_lag`]
    /// before it switches settlement off.
    #[error("{}", refusal_message(chequebook, *verdict))]
    ChequebookRefused {
        chequebook: [u8; 20],
        verdict: ChequebookVerdict,
    },
    /// The deposit transfer, or the checks right before it, failed
    /// ([`crate::chequebook_store::top_up_chequebook`]).
    #[error("deposit into chequebook: {0}")]
    Deposit(ChequebookError),
}

/// [`FundingError::PriceMoved`] for the user. The figures carry 8
/// decimals, so a small move doesn't read as "needs 0.0035, more than
/// the 0.0035 you approved".
fn price_moved_message(input_wei: u128, max_wei: u128, helper_deployed: bool) -> String {
    let xdai = |wei: u128| {
        let step = WEI_PER_XDAI / 100_000_000;
        let steps = wei.div_ceil(step);
        format!("{}.{:08}", steps / 100_000_000, steps % 100_000_000)
    };
    let approved = if max_wei == 0 {
        "no swap was approved".to_string()
    } else {
        format!("more than the {} xDAI you approved", xdai(max_wei))
    };
    let sent = if helper_deployed {
        "only the one-time swap helper setup was sent (later swaps reuse it)"
    } else {
        "nothing was sent"
    };
    // `acquire_bzz` only sees the cap, not the quote's xBZZ amount, so
    // it can't tell a price move from more xBZZ to buy: name both.
    format!(
        "the cost of the xBZZ swap changed since your quote (the xBZZ price moved, or there is \
         more xBZZ to buy): the swap now needs {} xDAI, {approved}; {sent}, get a new quote and \
         try again",
        xdai(input_wei)
    )
}

/// Why a chequebook refused by the chain can't take a deposit, for the
/// user.
fn refusal_message(chequebook: &[u8; 20], verdict: ChequebookVerdict) -> String {
    match verdict {
        ChequebookVerdict::IssuerMismatch(issuer) => format!(
            "chequebook 0x{} is issued by 0x{}, not this node; peers would drop every cheque \
             drawn on it, so nothing was deposited into it",
            hex::encode(chequebook),
            hex::encode(issuer),
        ),
        ChequebookVerdict::NotRegistered | ChequebookVerdict::Usable => format!(
            "chequebook 0x{} is not registered with the Swarm chequebook factory; peers drop \
             every cheque drawn on it, so nothing was deposited into it",
            hex::encode(chequebook),
        ),
    }
}

/// The node wallet a funding call prices or spends from, and the chain
/// and postage contract it runs against.
#[derive(Clone, Copy)]
pub struct Payer<'a> {
    pub client: &'a ChainClient,
    pub wallet: &'a Wallet,
    pub postage: [u8; 20],
}

impl<'a> Payer<'a> {
    /// A payer on the Gnosis mainnet `PostageStamp` contract.
    #[must_use]
    pub fn gnosis(client: &'a ChainClient, wallet: &'a Wallet) -> Self {
        let mut postage = [0u8; 20];
        hex::decode_to_slice(&crate::GNOSIS_POSTAGE_STAMP[2..], &mut postage)
            .expect("valid postage contract constant");
        Self {
            client,
            wallet,
            postage,
        }
    }

    fn owner(&self) -> &[u8; 20] {
        self.wallet.address()
    }

    fn postage_hex(&self) -> String {
        format!("0x{}", hex::encode(self.postage))
    }
}

// --- pricing math (pure, so the default test gate covers it) ---

/// Per-chunk balance that keeps a batch alive for `days` at
/// `price_per_block`. A zero price reads as 1 PLUR per chunk per block
/// so the plan stays valid, and a zero duration as one block.
fn amount_per_chunk_for(price_per_block: u128, days: u64) -> u128 {
    let blocks = (u128::from(days) * 86_400 / GNOSIS_BLOCK_SECS).max(1);
    price_per_block.max(1).saturating_mul(blocks)
}

/// `amount_per_chunk × 2^depth`, or `None` when it overflows.
fn plan_cost_plur(amount_per_chunk: u128, depth: u8) -> Option<u128> {
    1u128
        .checked_shl(u32::from(depth))
        .and_then(|chunks| amount_per_chunk.checked_mul(chunks))
}

/// xBZZ (PLUR) an operation has to acquire: the plan's cost plus the
/// deposit it funds, less what the wallet already holds.
///
/// A buy that sized its swap from the plan alone would spend every xBZZ
/// it acquired on the batch and deploy a chequebook backing nothing,
/// which stalls uploads once peers stop extending credit (#73).
fn bzz_to_acquire(plan_plur: u128, deposit_due_plur: u128, wallet_bzz: u128) -> u128 {
    plan_plur
        .saturating_add(deposit_due_plur)
        .saturating_sub(wallet_bzz)
}

/// Fair-value xDAI (wei) for `bzz_plur` of xBZZ at the pool's
/// `sqrtPriceX96`, plus the swap buffer. The raw price
/// `(sqrtPriceX96 / 2^96)^2` is WXDAI-wei per BZZ-PLUR (token1 has 18
/// decimals, token0 16), so `plur × price` is the WXDAI to send. `None`
/// when the result doesn't fit a `u128`.
#[cfg(test)]
fn swap_input_for(sqrt_price_x96: &[u8; 32], bzz_plur: u128) -> Option<u128> {
    Some(buffered(fair_swap_input_for(sqrt_price_x96, bzz_plur)?))
}

/// The swap buffer on a fair-value input.
fn buffered(fair: u128) -> u128 {
    fair.saturating_mul(SWAP_BUFFER_NUM) / SWAP_BUFFER_DEN
}

/// What a capped swap sends: the buffered input when the cap allows it,
/// else the cap itself when it still covers `fair` by
/// [`SWAP_MIN_MARGIN_NUM`], else `None` (refuse).
fn capped_swap_value(fair: u128, max_xdai_wei: u128) -> Option<u128> {
    let input = buffered(fair);
    if input <= max_xdai_wei {
        return Some(input);
    }
    let least = fair
        .saturating_mul(SWAP_MIN_MARGIN_NUM)
        .div_ceil(SWAP_MIN_MARGIN_DEN);
    (least <= max_xdai_wei).then_some(max_xdai_wei)
}

/// Fair-value xDAI (wei) for `bzz_plur` of xBZZ at the pool's
/// `sqrtPriceX96`, without the swap buffer.
fn fair_swap_input_for(sqrt_price_x96: &[u8; 32], bzz_plur: u128) -> Option<u128> {
    use primitive_types::U512;
    let sp = U512::from_big_endian(sqrt_price_x96);
    let fair = sp
        .checked_mul(sp)?
        .checked_mul(U512::from(bzz_plur))
        .map(|v| v >> 192)?;
    if fair > U512::from(u128::MAX) {
        return None;
    }
    Some(fair.low_u128())
}

/// The `topUp` per chunk that lets a batch holding `remaining` per chunk
/// be resized by `delta` depth and still hold `remaining + add`
/// afterwards. `increaseDepth` divides the per-chunk balance by
/// `2^delta`, so the batch needs `(remaining + add) × 2^delta` first.
fn resize_top_up_per_chunk(remaining: u128, add: u128, delta: u8) -> Option<u128> {
    let factor = 1u128.checked_shl(u32::from(delta))?;
    remaining
        .checked_add(add)?
        .checked_mul(factor)
        .map(|needed| needed - remaining)
}

/// xDAI (wei) as a decimal with 4 places, rounded **up**, for "send N
/// more" messages: sending the rounded-down figure would leave the
/// wallet short.
fn format_xdai_up(wei: u128) -> String {
    let step = WEI_PER_XDAI / 10_000;
    let steps = wei.div_ceil(step);
    format!("{}.{:04}", steps / 10_000, steps % 10_000)
}

fn validate_depth(depth: u8) -> Result<(), FundingError> {
    if depth <= POSTAGE_BUCKET_DEPTH {
        return Err(FundingError::Invalid(format!(
            "invalid depth {depth}: a batch must be deeper than {POSTAGE_BUCKET_DEPTH}"
        )));
    }
    Ok(())
}

// --- chain-backed helpers ---

/// The node wallet's xBZZ for a **quote**: `None` when it can't be
/// read. The quote prices that as `0`, so a flaky RPC over-quotes (asks
/// for the swap the wallet may not need) rather than under-quotes, while
/// the reported balance stays "unknown" instead of a made-up `0`.
/// Nothing is spent on this figure; the paying paths use
/// [`wallet_bzz_for_spend`].
async fn wallet_bzz(payer: &Payer<'_>) -> Option<u128> {
    wallet_bzz_for_spend(payer).await.ok()
}

/// The node wallet's xBZZ for sizing a **swap**. A failed read is an
/// error, not `0`: after a partial failure (the swap landed, the buy
/// didn't) the wallet already holds the xBZZ, and reading "none" on an
/// RPC hiccup would swap for it again, or refuse with a bogus "send N
/// more xDAI".
async fn wallet_bzz_for_spend(payer: &Payer<'_>) -> Result<u128, FundingError> {
    payer
        .client
        .erc20_balance_of_lower128(crate::GNOSIS_BZZ_TOKEN, payer.owner())
        .await
        .map_err(|e| FundingError::Read {
            what: "read xBZZ balance",
            message: e.to_string(),
        })
}

/// The node wallet's xDAI, or `None` when it can't be read (priced as
/// `0`, like [`wallet_bzz`]).
async fn wallet_xdai(payer: &Payer<'_>) -> Option<u128> {
    payer
        .client
        .eth_get_balance_lower128(payer.owner())
        .await
        .ok()
}

/// Bound on each balance read [`wallet_funding`] makes. Those balances
/// are only reported, never priced, so a read that stalls rather than
/// errors is cut off and reported as unknown (`None`) instead of holding
/// up the deposit status it rides along with: the gateway's whole
/// deposit read is bounded at 30 s, and before #241 a deposit at its
/// target never waited on the wallet at all.
const WALLET_REPORT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// The node wallet's balances with nothing to acquire: what a deposit
/// at or above its target reports, so a host still sees what the wallet
/// holds. Every pricing figure is `0`. The two reads run concurrently,
/// each bounded by [`WALLET_REPORT_TIMEOUT`]; one that fails or stalls
/// is `None`.
pub async fn wallet_funding(payer: &Payer<'_>) -> Funding {
    async fn bounded(read: impl std::future::Future<Output = Option<u128>>) -> Option<u128> {
        tokio::time::timeout(WALLET_REPORT_TIMEOUT, read)
            .await
            .ok()
            .flatten()
    }
    let (wallet_bzz, wallet_xdai) =
        tokio::join!(bounded(wallet_bzz(payer)), bounded(wallet_xdai(payer)));
    Funding {
        wallet_bzz,
        wallet_xdai,
        ..Funding::default()
    }
}

/// Whether a deposit status reads the node wallet's balances when the
/// deposit is already at its target. With a shortfall they are always
/// read, since the top-up is priced against them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletRead {
    /// Always read and report them (the gateway's
    /// `GET`/`POST /v0/settlement/deposit`, which return
    /// `walletBzzPlur`/`walletXdaiWei`).
    Always,
    /// Only when there is a shortfall to price. At the target the
    /// balances are `None` and no wallet read is made (the C API's
    /// settlement deposit JSON, which carries no wallet balances).
    WhenShort,
}

async fn storage_price(payer: &Payer<'_>) -> Result<u128, FundingError> {
    payer
        .client
        .postage_last_price(&payer.postage_hex())
        .await
        .map_err(|e| FundingError::Read {
            what: "read storage price",
            message: e.to_string(),
        })
}

async fn chequebook_deposit(
    payer: &Payer<'_>,
    chequebook: &[u8; 20],
) -> Result<u128, FundingError> {
    payer
        .client
        .erc20_balance_of_lower128(crate::GNOSIS_BZZ_TOKEN, chequebook)
        .await
        .map_err(|e| FundingError::Read {
            what: "read settlement deposit",
            message: e.to_string(),
        })
}

/// xDAI (wei, buffered) the swap helper needs for `bzz_plur` of xBZZ.
async fn swap_input(payer: &Payer<'_>, bzz_plur: u128) -> Result<u128, FundingError> {
    Ok(buffered(fair_swap_input(payer, bzz_plur).await?))
}

/// [`swap_input`] without the buffer.
async fn fair_swap_input(payer: &Payer<'_>, bzz_plur: u128) -> Result<u128, FundingError> {
    if bzz_plur == 0 {
        return Ok(0);
    }
    let word = payer
        .client
        .pool_sqrt_price_x96(crate::GNOSIS_BZZ_WXDAI_POOL)
        .await
        .map_err(|e| FundingError::Read {
            what: "read swap price",
            message: e.to_string(),
        })?;
    fair_swap_input_for(&word, bzz_plur)
        .ok_or_else(|| FundingError::Invalid("swap amount too large".into()))
}

/// Price acquiring `to_acquire` PLUR against the wallet's balances.
/// `reserve_gas` is false when the operation has nothing to do, so a
/// funded account isn't asked for a gas reserve it won't spend. An
/// unread xDAI balance counts as `0` here (over-quote), and is reported
/// as `None`.
async fn price_funding(
    payer: &Payer<'_>,
    to_acquire: u128,
    wallet_bzz: Option<u128>,
    wallet_xdai: Option<u128>,
    reserve_gas: bool,
) -> Result<Funding, FundingError> {
    let swap_input_wei = swap_input(payer, to_acquire).await?;
    let gas_reserve_wei = if reserve_gas { GAS_RESERVE_WEI } else { 0 };
    let xdai_required_wei = swap_input_wei.saturating_add(gas_reserve_wei);
    let have_xdai = wallet_xdai.unwrap_or(0);
    Ok(Funding {
        wallet_bzz,
        wallet_xdai,
        bzz_to_acquire: to_acquire,
        swap_input_wei,
        gas_reserve_wei,
        xdai_required_wei,
        xdai_to_send_wei: xdai_required_wei.saturating_sub(have_xdai),
        sufficient: have_xdai >= xdai_required_wei,
    })
}

/// The chequebook deposit a new plan should fund under `policy`: the
/// full target when the buy will deploy the chequebook, otherwise what
/// the current one is short.
///
/// When the deposit can't be read, the plan is priced alone rather than
/// charging for a deposit we couldn't size. An under-quote is
/// recoverable through the deposit top-up; an over-quote takes the
/// user's money for nothing.
async fn deposit_due(payer: &Payer<'_>, policy: DepositPolicy) -> u128 {
    match policy {
        DepositPolicy::Unmanaged => 0,
        DepositPolicy::Managed {
            chequebook: None,
            target,
        } => target,
        DepositPolicy::Managed {
            chequebook: Some(cb),
            target,
        } => match chequebook_deposit(payer, &cb).await {
            Ok(have) => target.saturating_sub(have),
            Err(e) => {
                tracing::warn!(
                    target: "ant_chain::funding",
                    "could not read the settlement deposit; pricing the plan alone: {e}",
                );
                0
            }
        },
    }
}

/// Swap enough xDAI for `bzz_plur` of xBZZ, delivered to the node
/// wallet. Sends at most `max_xdai_wei` ([`capped_swap_value`]), and
/// refuses before sending anything (the helper deploy included) when the
/// cap no longer covers the freshly priced swap
/// ([`FundingError::PriceMoved`]), or when the wallet can't also keep
/// [`GAS_RESERVE_WEI`] for the transactions that follow. A swap sending
/// the cap is dry-run first (`eth_call`) and refused the same way if it
/// would revert; by then the one-time helper deploy may have gone out,
/// which later swaps reuse, and the refusal says so
/// (`helper_deployed`). A dry run that passes without the swap's
/// `bzzOut` answer (a lagging RPC backend that doesn't see the helper
/// yet) fails the call as a read, sending nothing more.
async fn acquire_bzz(
    payer: &Payer<'_>,
    bzz_plur: u128,
    max_xdai_wei: u128,
) -> Result<(), FundingError> {
    if bzz_plur == 0 {
        return Ok(());
    }
    let xdai = payer
        .client
        .eth_get_balance_lower128(payer.owner())
        .await
        .map_err(|e| FundingError::Read {
            what: "read xDAI balance",
            message: e.to_string(),
        })?;
    let fair = fair_swap_input(payer, bzz_plur).await?;
    let Some(input) = capped_swap_value(fair, max_xdai_wei) else {
        return Err(FundingError::PriceMoved {
            input_wei: buffered(fair),
            max_wei: max_xdai_wei,
            helper_deployed: false,
        });
    };
    let required = input.saturating_add(GAS_RESERVE_WEI);
    if xdai < required {
        return Err(FundingError::InsufficientXdai {
            short_wei: required - xdai,
        });
    }
    let (helper, helper_deployed) = payer
        .wallet
        .ensure_swap_helper(payer.client)
        .await
        .map_err(|source| FundingError::Tx {
            what: "prepare swap",
            source,
        })?;
    if input < buffered(fair) {
        // Capped: the cap covers fair value by as little as
        // [`SWAP_MIN_MARGIN_NUM`], which the pool fee plus a large swap's
        // price impact can eat. Dry-run it so a swap that would miss
        // `amountOutMin` is refused here rather than reverting on chain
        // and burning its gas. A failed dry run that isn't a revert
        // fails the call too (an empty answer included, see
        // `simulate_swap_xdai_for_bzz`): the swap isn't sent on a check
        // we couldn't make.
        if let Err(e) = payer
            .wallet
            .simulate_swap_xdai_for_bzz(
                payer.client,
                &helper,
                payer.owner(),
                input,
                U256::from(bzz_plur),
            )
            .await
        {
            return Err(match e {
                crate::RpcError::Rpc(msg) if is_revert(&msg) => FundingError::PriceMoved {
                    input_wei: buffered(fair),
                    max_wei: max_xdai_wei,
                    helper_deployed,
                },
                e => FundingError::Read {
                    what: "check the capped swap",
                    message: e.to_string(),
                },
            });
        }
    }
    payer
        .wallet
        .swap_xdai_for_bzz(
            payer.client,
            &helper,
            payer.owner(),
            U256::from(input),
            U256::from(bzz_plur),
        )
        .await
        .map_err(|source| FundingError::Tx {
            what: "swap xDAI for xBZZ",
            source,
        })?;
    Ok(())
}

/// Whether an `eth_call` error (the RPC's error JSON) is the call
/// reverting, as opposed to the RPC failing to run it.
fn is_revert(error_json: &str) -> bool {
    error_json.to_ascii_lowercase().contains("revert")
}

// --- the shared orchestration steps ---

/// Price a new plan of `depth` lasting `days` for this node, including
/// the chequebook deposit `policy` says the purchase funds. No
/// transaction is sent.
pub async fn quote_plan(
    payer: &Payer<'_>,
    policy: DepositPolicy,
    depth: u8,
    days: u64,
) -> Result<PlanQuote, FundingError> {
    validate_depth(depth)?;
    let price = storage_price(payer).await?;
    let amount_per_chunk = amount_per_chunk_for(price, days);
    let plan_cost_plur = plan_cost_plur(amount_per_chunk, depth)
        .ok_or_else(|| FundingError::Invalid("plan cost overflows".into()))?;
    let deposit_due_plur = deposit_due(payer, policy).await;
    let (bzz, xdai) = (wallet_bzz(payer).await, wallet_xdai(payer).await);
    let to_acquire = bzz_to_acquire(plan_cost_plur, deposit_due_plur, bzz.unwrap_or(0));
    Ok(PlanQuote {
        depth,
        days,
        amount_per_chunk,
        plan_cost_plur,
        deposit_due_plur,
        funding: price_funding(payer, to_acquire, bzz, xdai, true).await?,
    })
}

/// Price extending batch `batch_id` (currently `depth`) by `days`. With
/// `new_depth`, price resizing it instead: it keeps its current expiry
/// plus `days` (which may be 0) at the larger depth. No transaction is
/// sent.
pub async fn quote_extend(
    payer: &Payer<'_>,
    batch_id: &[u8; 32],
    depth: u8,
    new_depth: Option<u8>,
    days: u64,
) -> Result<PlanQuote, FundingError> {
    let price = storage_price(payer).await?;
    let amount_per_chunk = match new_depth {
        None => amount_per_chunk_for(price, days),
        Some(new_depth) => {
            let delta = resize_delta(depth, new_depth)?;
            let remaining = payer
                .client
                .postage_remaining_balance(&payer.postage_hex(), batch_id)
                .await
                .map_err(|e| FundingError::Read {
                    what: "read storage balance",
                    message: e.to_string(),
                })?;
            let add = if days == 0 {
                0
            } else {
                amount_per_chunk_for(price, days)
            };
            resize_top_up_per_chunk(remaining, add, delta)
                .ok_or_else(|| FundingError::Invalid("resize cost overflows".into()))?
        }
    };
    let plan_cost_plur = plan_cost_plur(amount_per_chunk, depth)
        .ok_or_else(|| FundingError::Invalid("top-up cost overflows".into()))?;
    let (bzz, xdai) = (wallet_bzz(payer).await, wallet_xdai(payer).await);
    let to_acquire = bzz_to_acquire(plan_cost_plur, 0, bzz.unwrap_or(0));
    Ok(PlanQuote {
        depth: new_depth.unwrap_or(depth),
        days,
        amount_per_chunk,
        plan_cost_plur,
        deposit_due_plur: 0,
        funding: price_funding(payer, to_acquire, bzz, xdai, true).await?,
    })
}

fn resize_delta(depth: u8, new_depth: u8) -> Result<u8, FundingError> {
    if new_depth <= depth {
        return Err(FundingError::Invalid(format!(
            "new depth {new_depth} must be greater than the batch's depth {depth}"
        )));
    }
    Ok(new_depth - depth)
}

/// Buy a plan paying only with xDAI: swap the xBZZ the plan and the
/// deposit under `policy` still need, then [`buy_batch`]. Returns the
/// new batch id. The caller registers the batch with the node and then
/// runs its after-buy settlement, which funds the chequebook from the
/// xBZZ acquired here.
///
/// Recomputes the shortfall from current balances, so a retry after a
/// partial failure only swaps what is still missing. The swap sends at
/// most `max_xdai_wei` (the quote's `swap_input_wei`); a swap the cap no
/// longer covers is refused without swapping, at most after the one-time
/// helper deploy ([`FundingError::PriceMoved`]).
pub async fn buy_plan_with_xdai(
    payer: &Payer<'_>,
    policy: DepositPolicy,
    depth: u8,
    amount_per_chunk: u128,
    immutable: bool,
    max_xdai_wei: u128,
) -> Result<NewBatch, FundingError> {
    validate_depth(depth)?;
    let plan = checked_cost(amount_per_chunk, depth, "plan")?;
    let deposit = deposit_due(payer, policy).await;
    let to_acquire = bzz_to_acquire(plan, deposit, wallet_bzz_for_spend(payer).await?);
    acquire_bzz(payer, to_acquire, max_xdai_wei).await?;
    buy_batch(payer, amount_per_chunk, depth, immutable).await
}

/// Extend batch `batch_id` (currently `depth`) paying only with xDAI:
/// swap any xBZZ shortfall, `approve`, `topUp(amount_per_chunk)`. With
/// `new_depth`, then `increaseDepth` too; the caller re-registers the
/// batch at the new depth. The swap sends at most `max_xdai_wei` (the
/// quote's `swap_input_wei`), as in [`buy_plan_with_xdai`].
pub async fn extend_with_xdai(
    payer: &Payer<'_>,
    batch_id: &[u8; 32],
    depth: u8,
    new_depth: Option<u8>,
    amount_per_chunk: u128,
    max_xdai_wei: u128,
) -> Result<(), FundingError> {
    if let Some(new_depth) = new_depth {
        resize_delta(depth, new_depth)?;
    }
    let cost = checked_cost(amount_per_chunk, depth, "top-up")?;
    acquire_bzz(
        payer,
        cost.saturating_sub(wallet_bzz_for_spend(payer).await?),
        max_xdai_wei,
    )
    .await?;
    top_up_batch(payer, batch_id, depth, amount_per_chunk).await?;
    if let Some(new_depth) = new_depth {
        payer
            .wallet
            .increase_depth(payer.client, &payer.postage, batch_id, new_depth)
            .await
            .map_err(|source| FundingError::Tx {
                what: "resize storage",
                source,
            })?;
    }
    Ok(())
}

/// Buy a plan from the xBZZ the wallet already holds: `approve` the
/// postage contract for `amount_per_chunk × 2^depth`, then
/// `createBatch` owned by the node wallet. Returns the batch id from the
/// receipt.
pub async fn buy_batch(
    payer: &Payer<'_>,
    amount_per_chunk: u128,
    depth: u8,
    immutable: bool,
) -> Result<NewBatch, FundingError> {
    validate_depth(depth)?;
    let total = checked_cost(amount_per_chunk, depth, "plan")?;
    payer
        .wallet
        .approve_bzz(
            payer.client,
            &crate::chequebook::GNOSIS_BZZ_TOKEN_BYTES,
            &payer.postage,
            U256::from(total),
        )
        .await
        .map_err(|source| FundingError::Tx {
            what: "authorise payment",
            source,
        })?;
    let nonce = ant_crypto::random_overlay_nonce();
    let receipt = payer
        .wallet
        .create_batch(
            payer.client,
            &payer.postage,
            payer.owner(),
            U256::from(amount_per_chunk),
            depth,
            POSTAGE_BUCKET_DEPTH,
            &nonce,
            immutable,
        )
        .await
        .map_err(|source| FundingError::Tx {
            what: "buy storage",
            source,
        })?;
    let id = crate::tx::extract_created_batch_id(&receipt, &payer.postage)
        .ok_or(FundingError::NoBatchInReceipt)?;
    Ok(NewBatch {
        id,
        block: receipt.block_number,
    })
}

/// Extend batch `batch_id` (currently `depth`) from the xBZZ the wallet
/// already holds: `approve` `amount_per_chunk × 2^depth`, then
/// `PostageStamp.topUp`.
pub async fn top_up_batch(
    payer: &Payer<'_>,
    batch_id: &[u8; 32],
    depth: u8,
    amount_per_chunk: u128,
) -> Result<(), FundingError> {
    let total = checked_cost(amount_per_chunk, depth, "top-up")?;
    payer
        .wallet
        .approve_bzz(
            payer.client,
            &crate::chequebook::GNOSIS_BZZ_TOKEN_BYTES,
            &payer.postage,
            U256::from(total),
        )
        .await
        .map_err(|source| FundingError::Tx {
            what: "authorise payment",
            source,
        })?;
    payer
        .wallet
        .top_up(
            payer.client,
            &payer.postage,
            batch_id,
            U256::from(amount_per_chunk),
        )
        .await
        .map_err(|source| FundingError::Tx {
            what: "extend storage",
            source,
        })?;
    Ok(())
}

/// A non-zero per-chunk amount's total cost, or the error the apps show.
fn checked_cost(amount_per_chunk: u128, depth: u8, what: &str) -> Result<u128, FundingError> {
    if amount_per_chunk == 0 {
        return Err(FundingError::Invalid(format!(
            "{what} price must be greater than zero"
        )));
    }
    plan_cost_plur(amount_per_chunk, depth)
        .ok_or_else(|| FundingError::Invalid(format!("{what} cost overflows")))
}

/// The settlement deposit behind `chequebook`, read from chain, and what
/// topping it up to `target` would take. No transaction is sent.
/// `wallet` says whether the wallet's balances are read at the target
/// too (see [`WalletRead`]).
///
/// With `add`, [`DepositStatus::funding`] prices growing the deposit by
/// that many PLUR instead (what [`fund_deposit_with_xdai`] with the same
/// `add` would swap), so the quote for an explicit amount carries the
/// `swap_input_wei` that call takes as its cap. The other fields still
/// describe the deposit against `target`. `Some(0)` prices nothing.
pub async fn deposit_status(
    payer: &Payer<'_>,
    chequebook: &[u8; 20],
    target: u128,
    add: Option<u128>,
    wallet: WalletRead,
) -> Result<DepositStatus, FundingError> {
    let deposited = chequebook_deposit(payer, chequebook).await?;
    status_for(payer, chequebook, deposited, target, add, wallet).await
}

async fn status_for(
    payer: &Payer<'_>,
    chequebook: &[u8; 20],
    deposited: u128,
    target: u128,
    add: Option<u128>,
    wallet: WalletRead,
) -> Result<DepositStatus, FundingError> {
    let shortfall_plur = target.saturating_sub(deposited);
    // What the priced top-up deposits: the shortfall, or `add`.
    let deposit_plur = add.unwrap_or(shortfall_plur);
    // With `WalletRead::Always` the wallet's balances are reported
    // either way: a deposit at its target still has a wallet behind it
    // (#241). Only the top-up figures stay 0 when nothing is missing.
    let funding = if deposit_plur == 0 {
        match wallet {
            WalletRead::Always => wallet_funding(payer).await,
            WalletRead::WhenShort => Funding {
                wallet_bzz: None,
                wallet_xdai: None,
                ..Funding::default()
            },
        }
    } else {
        let (bzz, xdai) = tokio::join!(wallet_bzz(payer), wallet_xdai(payer));
        let to_acquire = deposit_plur.saturating_sub(bzz.unwrap_or(0));
        price_funding(payer, to_acquire, bzz, xdai, true).await?
    };
    Ok(DepositStatus {
        chequebook: *chequebook,
        deposited_plur: deposited,
        target_plur: target,
        shortfall_plur,
        funding,
    })
}

/// Top `chequebook` up to `target`, paying only with xDAI: swap the
/// xBZZ the wallet lacks, then transfer the shortfall into the
/// chequebook through the shared
/// [`top_up_chequebook`](crate::chequebook_store::top_up_chequebook).
/// A no-op when the deposit is already at its target. Returns the
/// deposit as the chain reports it afterwards, measured against
/// `target`.
///
/// With `add`, the deposit grows by that many PLUR instead, whatever
/// `target` is: the goal becomes the deposit read here (under the
/// caller's wallet lock) plus `add`. This is how a host buys more
/// browsing credit than the node's default deposit target (Freedom's
/// wallet UI). `Some(0)` is refused.
///
/// The swap sends at most `max_xdai_wei`: the `swap_input_wei` of the
/// [`deposit_status`] the user approved (priced with the same `add`),
/// or `0` when the wallet already holds the xBZZ and no swap may happen.
/// A swap the cap no longer covers is refused without swapping, at most
/// after the one-time helper deploy ([`FundingError::PriceMoved`]).
///
/// Nothing is spent on a chequebook the chain rejects: before a swap
/// the chequebook's checks (factory registration, `issuer()` is the
/// node wallet) must both read "yes", and `top_up_chequebook` runs them
/// again right before the transfer. A "no" is
/// [`FundingError::ChequebookRefused`]; a check that couldn't be read
/// is an error too, since a deposit can't be taken back.
///
/// The caller holds its wallet tx lock (`ant_gateway::WalletTxLock`)
/// around this call: the deposit and the wallet's balances are read
/// here, after the lock is taken, so a concurrent top-up that already
/// landed isn't sent a second time.
///
/// This is the explicit top-up: a chequebook deployed before its
/// deposit was funded (every install before #73) can be funded no other
/// way.
pub async fn fund_deposit_with_xdai(
    payer: &Payer<'_>,
    chequebook: &[u8; 20],
    target: u128,
    add: Option<u128>,
    wallet: WalletRead,
    max_xdai_wei: u128,
) -> Result<DepositStatus, FundingError> {
    let deposited = chequebook_deposit(payer, chequebook).await?;
    let goal = match add {
        None => target,
        Some(0) => {
            return Err(FundingError::Invalid(
                "a deposit amount must be more than 0 PLUR".into(),
            ))
        }
        Some(add) => deposited.checked_add(add).ok_or_else(|| {
            FundingError::Invalid(format!("a deposit of {add} PLUR is out of range"))
        })?,
    };
    let short = goal.saturating_sub(deposited);
    if short == 0 {
        return status_for(payer, chequebook, deposited, target, None, wallet).await;
    }
    let to_acquire = short.saturating_sub(wallet_bzz_for_spend(payer).await?);
    if to_acquire > 0 {
        verify_for_deposit(payer, chequebook).await?;
        acquire_bzz(payer, to_acquire, max_xdai_wei).await?;
    }
    match crate::chequebook_store::top_up_chequebook(
        payer.client,
        payer.wallet,
        payer.owner(),
        chequebook,
        goal,
    )
    .await
    {
        Ok(TopUp::NotNeeded | TopUp::Funded { .. }) => {}
        Ok(TopUp::WalletEmpty { .. }) => {
            return Err(FundingError::Deposit(ChequebookError::Chain(
                "the wallet holds no xBZZ to deposit".into(),
            )));
        }
        Ok(TopUp::Refused(verdict)) => {
            return Err(FundingError::ChequebookRefused {
                chequebook: *chequebook,
                verdict,
            });
        }
        Err(e) => return Err(FundingError::Deposit(e)),
    }
    // Re-read rather than assume: report what the chain says now.
    deposit_status(payer, chequebook, target, None, wallet).await
}

/// The chequebook checks [`top_up_chequebook`] runs before its transfer,
/// run before the swap that pays for it: the same strict rule (both
/// checks read, both "yes").
///
/// [`top_up_chequebook`]: crate::chequebook_store::top_up_chequebook
async fn verify_for_deposit(payer: &Payer<'_>, chequebook: &[u8; 20]) -> Result<(), FundingError> {
    let checks = crate::chequebook_store::check_chequebook(
        payer.client,
        chequebook,
        IssuerRead::UnlessUnregistered,
    )
    .await;
    match checks.verdict(payer.owner()) {
        ChequebookVerdict::Usable => {}
        verdict => {
            return Err(FundingError::ChequebookRefused {
                chequebook: *chequebook,
                verdict,
            })
        }
    }
    let unread = [checks.registered.err(), checks.issuer.and_then(Result::err)];
    match unread.into_iter().flatten().next() {
        Some(e) => Err(FundingError::Read {
            what: "verify the chequebook before depositing into it",
            message: e.to_string(),
        }),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR as TARGET;

    #[test]
    fn a_day_is_17280_blocks_of_the_price() {
        assert_eq!(amount_per_chunk_for(24_000, 1), 24_000 * 17_280);
        assert_eq!(amount_per_chunk_for(24_000, 30), 24_000 * 17_280 * 30);
        // A zero price or duration still yields a valid, non-zero plan.
        assert_eq!(amount_per_chunk_for(0, 1), 17_280);
        assert_eq!(amount_per_chunk_for(24_000, 0), 24_000);
    }

    #[test]
    fn plan_cost_is_checked() {
        assert_eq!(plan_cost_plur(10, 20), Some(10 << 20));
        assert_eq!(plan_cost_plur(u128::MAX, 1), None);
        assert_eq!(
            plan_cost_plur(1, 200),
            None,
            "a shift past 127 must not panic"
        );
    }

    #[test]
    fn buy_acquires_the_plan_plus_the_missing_deposit() {
        let plan = 5 * PLUR_PER_BZZ;
        // Fresh account: nothing in the wallet, nothing behind the
        // chequebook. The buy acquires both, or it spends everything on
        // postage and deploys a chequebook backing nothing.
        assert_eq!(bzz_to_acquire(plan, TARGET, 0), plan + TARGET);
        // Chequebook already funded: the plan alone, no double charge.
        assert_eq!(bzz_to_acquire(plan, 0, 0), plan);
        // Wallet already holds the deposit: only the plan is missing.
        assert_eq!(bzz_to_acquire(plan, TARGET, TARGET), plan);
        // Wallet covers everything: nothing to swap.
        assert_eq!(bzz_to_acquire(plan, TARGET, plan + TARGET), 0);
        assert_eq!(bzz_to_acquire(plan, TARGET, u128::MAX), 0);
    }

    #[test]
    fn deposit_status_asks_only_for_what_is_missing() {
        let status = |deposited: u128| DepositStatus {
            chequebook: [0; 20],
            deposited_plur: deposited,
            target_plur: TARGET,
            shortfall_plur: TARGET.saturating_sub(deposited),
            funding: Funding::default(),
        };
        assert!(status(0).needs_top_up());
        assert_eq!(status(TARGET / 4).shortfall_plur, TARGET - TARGET / 4);
        assert!(!status(TARGET).needs_top_up());
        assert!(!status(TARGET * 10).needs_top_up());
    }

    /// The pool price on Gnosis in September 2026 (1 xBZZ ≈ 0.06 xDAI).
    /// Pins the fixed-point math: `(sqrtP / 2^96)^2` is wei per PLUR.
    #[test]
    fn swap_input_is_the_pool_price_plus_five_percent() {
        let sqrt_p: u128 = 193_688_776_437_782_838_521_006_693_800;
        let mut word = [0u8; 32];
        word[16..].copy_from_slice(&sqrt_p.to_be_bytes());
        let one_bzz = swap_input_for(&word, PLUR_PER_BZZ).unwrap();
        // fair ≈ 0.05976 xDAI; +5% ≈ 0.06275 xDAI.
        assert!(
            (62_700_000_000_000_000..62_800_000_000_000_000).contains(&one_bzz),
            "{one_bzz}"
        );
        assert_eq!(swap_input_for(&word, 0), Some(0));
        assert_eq!(swap_input_for(&[0xff; 32], u128::MAX), None);
    }

    #[test]
    fn a_resize_keeps_the_expiry_and_adds_the_extra_days() {
        // One depth up halves the per-chunk balance, so the batch needs
        // twice what it should hold afterwards.
        assert_eq!(resize_top_up_per_chunk(1_000, 0, 1), Some(1_000));
        assert_eq!(resize_top_up_per_chunk(1_000, 500, 1), Some(2_000));
        assert_eq!(resize_top_up_per_chunk(1_000, 0, 2), Some(3_000));
        assert_eq!(resize_top_up_per_chunk(u128::MAX, 1, 1), None);
        assert!(resize_delta(20, 20).is_err());
        assert!(resize_delta(21, 20).is_err());
        assert_eq!(resize_delta(20, 22).unwrap(), 2);
    }

    #[test]
    fn insufficient_xdai_rounds_the_request_up() {
        let e = FundingError::InsufficientXdai {
            short_wei: 120_000_000_000_000_001,
        };
        assert_eq!(
            e.to_string(),
            "not enough xDAI: send 0.1201 more xDAI to your account, then try again"
        );
        assert_eq!(format_xdai_up(WEI_PER_XDAI), "1.0000");
        assert_eq!(format_xdai_up(1), "0.0001");
    }

    #[test]
    fn a_moved_price_names_both_figures_apart() {
        let m = FundingError::PriceMoved {
            input_wei: 3_500_000_000_000_001,
            max_wei: 3_500_000_000_000_000,
            helper_deployed: false,
        }
        .to_string();
        assert!(
            m.contains("needs 0.00350001 xDAI, more than the 0.00350000 xDAI"),
            "{m}"
        );
        // R1-M2: a larger xBZZ amount at an unchanged price is refused
        // the same way, so the message doesn't blame the price alone.
        assert!(m.contains("or there is more xBZZ to buy"), "{m}");
        let m = FundingError::PriceMoved {
            input_wei: WEI_PER_XDAI,
            max_wei: 0,
            helper_deployed: false,
        }
        .to_string();
        assert!(
            m.contains("needs 1.00000000 xDAI, no swap was approved"),
            "{m}"
        );
    }

    #[test]
    fn depth_must_exceed_the_bucket_depth() {
        assert!(validate_depth(POSTAGE_BUCKET_DEPTH).is_err());
        assert!(validate_depth(POSTAGE_BUCKET_DEPTH + 1).is_ok());
        assert!(checked_cost(0, 20, "plan").is_err());
    }

    /// A scripted Gnosis backend for the xDAI buy: fixed price, pool and
    /// balances, every transaction mined at once, and every receipt
    /// carrying a `BatchCreated` log. It records each transaction by the
    /// contract it went to.
    #[derive(Default)]
    struct ScriptedChain {
        wallet_bzz: u128,
        wallet_xdai: u128,
        /// [`CHEQUEBOOK`]'s xBZZ, and its two chain checks: factory
        /// registration and `issuer()`.
        chequebook_bzz: u128,
        registered: bool,
        issuer: [u8; 20],
        /// The node wallet's `balanceOf` read fails (an RPC hiccup).
        wallet_bzz_unreadable: bool,
        /// The node wallet's `eth_getBalance` read fails.
        wallet_xdai_unreadable: bool,
        /// The node wallet's `eth_getBalance` is left to the client's
        /// fallback RPC URL (a server that never answers): a read that
        /// stalls rather than errors.
        wallet_xdai_stalls: bool,
        /// `slot0()`'s `sqrtPriceX96` when set (a forged or moved pool
        /// price); [`HONEST_SQRT_PRICE`] otherwise.
        sqrt_price: Option<u128>,
        /// The swap helper isn't deployed yet (`eth_getCode` is empty), so
        /// a swap starts with its deploy transaction.
        no_helper: bool,
        /// Wallet balance reads served or refused: `balanceOf` the node
        /// wallet and `eth_getBalance`.
        wallet_reads: std::sync::atomic::AtomicUsize,
        sent: std::sync::Mutex<Vec<&'static str>>,
        /// The xDAI (wei) each swap transaction carried.
        swap_values: std::sync::Mutex<Vec<u128>>,
        /// A dry run (`eth_call`) of the swap reverts: the pool fee plus
        /// price impact leaves less than `amountOutMin`.
        swap_reverts: bool,
        /// A dry run of the swap fails without reverting (rate limit).
        swap_dry_run_fails: bool,
        /// The swap dry run succeeds with empty output (a lagging RPC
        /// backend that doesn't see the helper's code yet).
        swap_dry_run_empty: bool,
        /// The xDAI (wei) each swap dry run carried.
        dry_run_values: std::sync::Mutex<Vec<u128>>,
    }

    const CHEQUEBOOK: [u8; 20] = [0xcb; 20];

    const BATCH: [u8; 32] = [0xba; 32];

    /// The scripted pool's `sqrtPriceX96`: about 0.06 xDAI per xBZZ.
    const HONEST_SQRT_PRICE: u128 = 193_688_776_437_782_838_521_006_693_800;

    impl crate::ChainTransport for ScriptedChain {
        fn serve(&self, request_json: &str) -> Option<String> {
            use serde_json::json;
            let word = |v: u128| format!("0x{v:064x}");
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let id = req["id"].clone();
            let wallet_bzz_read = req["method"] == "eth_call"
                && req["params"][0]["data"].as_str().is_some_and(|d| {
                    d.starts_with("0x70a08231") && d[34..74] != hex::encode(CHEQUEBOOK)
                });
            if wallet_bzz_read || req["method"] == "eth_getBalance" {
                self.wallet_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if self.wallet_xdai_stalls && req["method"] == "eth_getBalance" {
                return None;
            }
            if self.wallet_bzz_unreadable && req["method"] == "eth_call" {
                let data = req["params"][0]["data"].as_str().unwrap();
                if data.starts_with("0x70a08231") && data[34..74] != hex::encode(CHEQUEBOOK) {
                    return Some(
                        json!({"jsonrpc": "2.0", "id": id,
                               "error": {"code": -32603, "message": "upstream hiccup"}})
                        .to_string(),
                    );
                }
            }
            if self.wallet_xdai_unreadable && req["method"] == "eth_getBalance" {
                return Some(
                    json!({"jsonrpc": "2.0", "id": id,
                           "error": {"code": -32603, "message": "upstream hiccup"}})
                    .to_string(),
                );
            }
            let helper_hex = format!("0x{}", hex::encode(crate::tx::swap_helper_address()));
            if req["method"] == "eth_call" && req["params"][0]["to"] == helper_hex.as_str() {
                let call = &req["params"][0];
                assert_eq!(call["from"], format!("0x{}", hex::encode(node_eth())));
                assert_eq!(
                    call["gas"],
                    format!("0x{:x}", crate::tx::SWAP_HELPER_SWAP_GAS)
                );
                let value = call["value"].as_str().unwrap();
                self.dry_run_values
                    .lock()
                    .unwrap()
                    .push(u128::from_str_radix(&value[2..], 16).unwrap());
                let error = if self.swap_reverts {
                    json!({"code": 3, "message": "execution reverted: Too little received"})
                } else if self.swap_dry_run_fails {
                    json!({"code": -32005, "message": "rate limit exceeded"})
                } else if self.swap_dry_run_empty {
                    // What a backend that doesn't see the helper's code
                    // answers.
                    return Some(json!({"jsonrpc": "2.0", "id": id, "result": "0x"}).to_string());
                } else {
                    // `bzzOut`: exactly the `amountOutMin` asked for.
                    let data = call["data"].as_str().unwrap();
                    let out = format!("0x{}", &data[2 + 8 + 64..]);
                    return Some(json!({"jsonrpc": "2.0", "id": id, "result": out}).to_string());
                };
                return Some(json!({"jsonrpc": "2.0", "id": id, "error": error}).to_string());
            }
            let result = match req["method"].as_str().unwrap() {
                "eth_call" => {
                    let data = req["params"][0]["data"].as_str().unwrap();
                    let deployed_sel = format!(
                        "0x{}",
                        hex::encode(
                            &crate::chequebook::factory_deployed_contracts_calldata(&CHEQUEBOOK)
                                [..4]
                        )
                    );
                    let issuer_sel = format!(
                        "0x{}",
                        hex::encode(crate::chequebook::chequebook_issuer_selector())
                    );
                    match &data[..10] {
                        // balanceOf(address): the chequebook's deposit or
                        // the node wallet's xBZZ.
                        "0x70a08231" if data[34..74] == hex::encode(CHEQUEBOOK) => {
                            json!(word(self.chequebook_bzz))
                        }
                        // A swap delivers the xBZZ it was asked for (and
                        // then some).
                        "0x70a08231" if self.sent.lock().unwrap().contains(&"swap") => {
                            json!(word(self.wallet_bzz + 10 * TARGET))
                        }
                        "0x70a08231" => json!(word(self.wallet_bzz)),
                        sel if sel == deployed_sel => json!(word(u128::from(self.registered))),
                        sel if sel == issuer_sel => {
                            json!(format!("0x{}{}", "00".repeat(12), hex::encode(self.issuer)))
                        }
                        // slot0(): the price, then padding.
                        "0x3850c7bd" => json!(format!(
                            "{}{}",
                            word(self.sqrt_price.unwrap_or(HONEST_SQRT_PRICE)),
                            "0".repeat(64 * 6)
                        )),
                        // lastPrice()
                        _ => json!(word(24_000)),
                    }
                }
                "eth_getBalance" => json!(format!("0x{:x}", self.wallet_xdai)),
                "eth_getCode" if self.no_helper => json!("0x"),
                "eth_getCode" => json!("0x6080"),
                "eth_getTransactionCount" => json!("0x0"),
                "eth_sendRawTransaction" => {
                    let raw = hex::decode(&req["params"][0].as_str().unwrap()[2..]).unwrap();
                    let to: Vec<u8> = rlp::Rlp::new(&raw).at(3).unwrap().data().unwrap().to_vec();
                    let input: Vec<u8> =
                        rlp::Rlp::new(&raw).at(5).unwrap().data().unwrap().to_vec();
                    let kind = if hex::encode(&to) == crate::CREATE2_DEPLOYER[2..].to_lowercase() {
                        "deploy"
                    } else if to == crate::tx::swap_helper_address() {
                        let value = rlp::Rlp::new(&raw).at(4).unwrap().data().unwrap().to_vec();
                        let mut word = [0u8; 16];
                        word[16 - value.len()..].copy_from_slice(&value);
                        self.swap_values
                            .lock()
                            .unwrap()
                            .push(u128::from_be_bytes(word));
                        "swap"
                    } else if to == crate::chequebook::GNOSIS_BZZ_TOKEN_BYTES {
                        // transfer(address,uint256) vs approve.
                        if input.starts_with(&[0xa9, 0x05, 0x9c, 0xbb]) {
                            "transfer"
                        } else {
                            "approve"
                        }
                    } else {
                        "createBatch"
                    };
                    self.sent.lock().unwrap().push(kind);
                    json!(format!("0x{}", hex::encode(ant_crypto::keccak256(&raw))))
                }
                "eth_getTransactionReceipt" => json!({
                    "transactionHash": req["params"][0],
                    "status": "0x1",
                    "blockNumber": "0x1",
                    "logs": [{
                        "address": crate::GNOSIS_POSTAGE_STAMP,
                        "topics": [
                            format!("0x{}", hex::encode(crate::tx::batch_created_event_topic())),
                            format!("0x{}", hex::encode(BATCH)),
                        ],
                        "data": "0x",
                    }],
                }),
                other => panic!("unscripted method {other}"),
            };
            Some(json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string())
        }
    }

    async fn buy_with(
        wallet_bzz: u128,
        wallet_xdai: u128,
    ) -> (Result<NewBatch, FundingError>, Vec<&'static str>) {
        let chain = std::sync::Arc::new(ScriptedChain {
            wallet_bzz,
            wallet_xdai,
            ..ScriptedChain::default()
        });
        // An unroutable URL: a fall-through would fail loudly.
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain.clone()));
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let payer = Payer::gnosis(&client, &wallet);
        let policy = DepositPolicy::Managed {
            chequebook: None,
            target: TARGET,
        };
        let amount = amount_per_chunk_for(24_000, 30);
        let result = buy_plan_with_xdai(&payer, policy, 20, amount, true, u128::MAX).await;
        let sent = chain.sent.lock().unwrap().clone();
        (result, sent)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_xdai_buy_swaps_then_approves_then_creates_the_batch() {
        let (result, sent) = buy_with(0, WEI_PER_XDAI).await;
        // The id and the block come from the `createBatch` receipt.
        assert_eq!(
            result.unwrap(),
            NewBatch {
                id: BATCH,
                block: 1
            }
        );
        assert_eq!(sent, ["swap", "approve", "createBatch"]);
    }

    /// A retry after the swap landed finds the xBZZ in the wallet and
    /// doesn't swap again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wallet_holding_the_xbzz_skips_the_swap() {
        let plan = plan_cost_plur(amount_per_chunk_for(24_000, 30), 20).unwrap();
        let (result, sent) = buy_with(plan + TARGET, 0).await;
        assert_eq!(result.unwrap().id, BATCH);
        assert_eq!(sent, ["approve", "createBatch"]);
    }

    /// Short on xDAI: refuse before sending anything, and say how much
    /// more to send.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn too_little_xdai_sends_nothing() {
        let (result, sent) = buy_with(0, GAS_RESERVE_WEI).await;
        assert!(
            matches!(result, Err(FundingError::InsufficientXdai { .. })),
            "{result:?}"
        );
        assert!(sent.is_empty(), "nothing may be sent: {sent:?}");
    }

    async fn fund_deposit_with(
        chain: ScriptedChain,
    ) -> (Result<DepositStatus, FundingError>, Vec<&'static str>) {
        fund_deposit_adding(chain, None).await
    }

    async fn fund_deposit_adding(
        chain: ScriptedChain,
        add: Option<u128>,
    ) -> (Result<DepositStatus, FundingError>, Vec<&'static str>) {
        let chain = std::sync::Arc::new(chain);
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain.clone()));
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let payer = Payer::gnosis(&client, &wallet);
        let result = fund_deposit_with_xdai(
            &payer,
            &CHEQUEBOOK,
            TARGET,
            add,
            WalletRead::Always,
            u128::MAX,
        )
        .await;
        let sent = chain.sent.lock().unwrap().clone();
        (result, sent)
    }

    fn node_eth() -> [u8; 20] {
        *Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID)
            .unwrap()
            .address()
    }

    /// An empty deposit on a chequebook that checks out: swap the
    /// missing xBZZ, then deposit through the shared top-up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deposit_swaps_then_transfers_into_a_checked_chequebook() {
        let (result, sent) = fund_deposit_with(ScriptedChain {
            wallet_xdai: WEI_PER_XDAI,
            registered: true,
            issuer: node_eth(),
            ..ScriptedChain::default()
        })
        .await;
        result.unwrap();
        assert_eq!(sent, ["swap", "transfer"]);
    }

    /// The chain says no to the chequebook: nothing is swapped or sent,
    /// whichever check refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_chequebook_gets_no_swap_and_no_deposit() {
        let stranger = [0x5e; 20];
        for (registered, issuer, want) in [
            (false, node_eth(), ChequebookVerdict::NotRegistered),
            (true, stranger, ChequebookVerdict::IssuerMismatch(stranger)),
        ] {
            for wallet_bzz in [0, TARGET] {
                let (result, sent) = fund_deposit_with(ScriptedChain {
                    wallet_bzz,
                    wallet_xdai: WEI_PER_XDAI,
                    registered,
                    issuer,
                    ..ScriptedChain::default()
                })
                .await;
                match result {
                    Err(FundingError::ChequebookRefused {
                        chequebook,
                        verdict,
                    }) => {
                        assert_eq!(chequebook, CHEQUEBOOK);
                        assert_eq!(verdict, want);
                    }
                    other => panic!("expected a refusal, got {other:?}"),
                }
                assert!(sent.is_empty(), "nothing may be sent: {sent:?}");
            }
        }
    }

    /// Already at its target: nothing to check, swap or send.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_deposit_is_left_alone() {
        let (result, sent) = fund_deposit_with(ScriptedChain {
            chequebook_bzz: TARGET,
            ..ScriptedChain::default()
        })
        .await;
        assert!(!result.unwrap().needs_top_up());
        assert_eq!(sent, [] as [&str; 0]);
    }

    async fn status_of(chain: ScriptedChain) -> DepositStatus {
        status_reading(
            std::sync::Arc::new(chain),
            WalletRead::Always,
            "http://127.0.0.1:1",
        )
        .await
    }

    async fn status_reading(
        chain: std::sync::Arc<ScriptedChain>,
        read: WalletRead,
        fallback_url: &str,
    ) -> DepositStatus {
        let client = ChainClient::new(fallback_url).with_transport(Some(chain));
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        deposit_status(
            &Payer::gnosis(&client, &wallet),
            &CHEQUEBOOK,
            TARGET,
            None,
            read,
        )
        .await
        .unwrap()
    }

    /// The C API's deposit card carries no wallet balances: with
    /// `WalletRead::WhenShort`, a deposit at its target makes no wallet
    /// read at all, and a shortfall still reads both to price the top-up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_deposit_skips_the_wallet_when_asked() {
        use std::sync::atomic::Ordering::SeqCst;
        let chain = |chequebook_bzz| {
            std::sync::Arc::new(ScriptedChain {
                chequebook_bzz,
                wallet_bzz: 165 * TARGET,
                wallet_xdai: WEI_PER_XDAI,
                ..ScriptedChain::default()
            })
        };
        let full = chain(TARGET);
        let status =
            status_reading(full.clone(), WalletRead::WhenShort, "http://127.0.0.1:1").await;
        assert!(!status.needs_top_up());
        assert_eq!(status.funding, Funding::default());
        assert_eq!(full.wallet_reads.load(SeqCst), 0);

        let short = chain(0);
        let status =
            status_reading(short.clone(), WalletRead::WhenShort, "http://127.0.0.1:1").await;
        assert!(status.needs_top_up());
        assert_eq!(status.funding.wallet_bzz, Some(165 * TARGET));
        assert_eq!(status.funding.wallet_xdai, Some(WEI_PER_XDAI));
        assert_eq!(short.wallet_reads.load(SeqCst), 2);

        let reported = chain(TARGET);
        status_reading(reported.clone(), WalletRead::Always, "http://127.0.0.1:1").await;
        assert_eq!(reported.wallet_reads.load(SeqCst), 2);
    }

    /// A wallet read that stalls (not errors) is cut off at
    /// [`WALLET_REPORT_TIMEOUT`] and reported as unknown, well inside
    /// the gateway's 30 s deposit-read bound, instead of holding up a
    /// deposit status that needs no wallet figure to be priced.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_wallet_read_is_reported_as_unknown() {
        // Accepts and never answers: the stalled RPC.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let held = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((conn, _)) = listener.accept().await {
                open.push(conn);
            }
        });
        let chain = std::sync::Arc::new(ScriptedChain {
            chequebook_bzz: TARGET,
            wallet_bzz: 165 * TARGET,
            wallet_xdai_stalls: true,
            ..ScriptedChain::default()
        });
        let started = tokio::time::Instant::now();
        let status = status_reading(chain, WalletRead::Always, &url).await;
        let took = started.elapsed();
        held.abort();
        assert!(!status.needs_top_up());
        assert_eq!(
            (status.funding.wallet_bzz, status.funding.wallet_xdai),
            (Some(165 * TARGET), None)
        );
        assert!(took >= WALLET_REPORT_TIMEOUT, "{took:?}");
        assert!(took < WALLET_REPORT_TIMEOUT * 2, "{took:?}");
    }

    /// #241: a deposit above its target still reports the wallet's real
    /// balances; only the top-up figures are 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_deposit_still_reports_the_wallet() {
        let status = status_of(ScriptedChain {
            chequebook_bzz: 15 * TARGET,
            wallet_bzz: 165 * TARGET,
            wallet_xdai: WEI_PER_XDAI / 50,
            ..ScriptedChain::default()
        })
        .await;
        assert!(!status.needs_top_up());
        assert_eq!(
            status.funding,
            Funding {
                wallet_bzz: Some(165 * TARGET),
                wallet_xdai: Some(WEI_PER_XDAI / 50),
                ..Funding::default()
            }
        );
    }

    /// #241: a balance that couldn't be read is unknown, not 0, with and
    /// without a shortfall. The shortfall is still priced as if the
    /// wallet held nothing (over-quote).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreadable_balance_is_reported_as_unknown() {
        let unreadable = |chequebook_bzz| ScriptedChain {
            chequebook_bzz,
            wallet_bzz: 165 * TARGET,
            wallet_xdai: WEI_PER_XDAI,
            wallet_bzz_unreadable: true,
            wallet_xdai_unreadable: true,
            ..ScriptedChain::default()
        };
        let full = status_of(unreadable(TARGET)).await;
        assert_eq!(full.funding, Funding::default());
        assert_eq!(
            (full.funding.wallet_bzz, full.funding.wallet_xdai),
            (None, None)
        );

        let short = status_of(unreadable(0)).await;
        let f = short.funding;
        assert_eq!((f.wallet_bzz, f.wallet_xdai), (None, None));
        assert_eq!(
            f.bzz_to_acquire, TARGET,
            "an unread xBZZ balance prices as 0"
        );
        assert!(f.swap_input_wei > 0);
        assert_eq!(f.xdai_to_send_wei, f.xdai_required_wei);
        assert!(!f.sufficient);

        // Only the xDAI read fails: the xBZZ balance is real.
        let f = status_of(ScriptedChain {
            wallet_bzz_unreadable: false,
            ..unreadable(0)
        })
        .await
        .funding;
        assert_eq!((f.wallet_bzz, f.wallet_xdai), (Some(165 * TARGET), None));
        assert_eq!(f.bzz_to_acquire, 0);
    }

    /// An explicit amount (Freedom's "add browsing credit") deposits that
    /// much more even when the deposit already meets the node's target,
    /// swapping only what the wallet lacks, through the same checked
    /// top-up; a zero amount is refused before anything is sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deposit_amount_grows_the_deposit_past_its_target() {
        let full = || ScriptedChain {
            chequebook_bzz: TARGET,
            wallet_xdai: WEI_PER_XDAI,
            registered: true,
            issuer: node_eth(),
            ..ScriptedChain::default()
        };
        let (result, sent) = fund_deposit_adding(
            ScriptedChain {
                wallet_bzz: 5 * TARGET,
                ..full()
            },
            Some(5 * TARGET),
        )
        .await;
        result.unwrap();
        assert_eq!(sent, ["transfer"], "the wallet holds it: no swap");
        let (result, sent) = fund_deposit_adding(full(), Some(5 * TARGET)).await;
        result.unwrap();
        assert_eq!(sent, ["swap", "transfer"]);
        let (result, sent) = fund_deposit_adding(full(), Some(0)).await;
        assert!(
            matches!(result, Err(FundingError::Invalid(_))),
            "{result:?}"
        );
        assert!(sent.is_empty(), "nothing may be sent: {sent:?}");
    }

    /// R1-F3: every paying path sizes its swap from the wallet's xBZZ. A
    /// failed read of it fails the operation before anything is sent,
    /// rather than reading as "no xBZZ" and swapping again for xBZZ a
    /// partial earlier attempt already bought.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreadable_xbzz_balance_sends_nothing() {
        let plan = plan_cost_plur(amount_per_chunk_for(24_000, 30), 20).unwrap();
        let chain = || {
            std::sync::Arc::new(ScriptedChain {
                wallet_bzz: plan + TARGET,
                wallet_xdai: WEI_PER_XDAI,
                registered: true,
                issuer: node_eth(),
                wallet_bzz_unreadable: true,
                ..ScriptedChain::default()
            })
        };
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let amount = amount_per_chunk_for(24_000, 30);

        let c = chain();
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(c.clone()));
        let payer = Payer::gnosis(&client, &wallet);
        let policy = DepositPolicy::Managed {
            chequebook: None,
            target: TARGET,
        };
        let r = buy_plan_with_xdai(&payer, policy, 20, amount, true, u128::MAX).await;
        assert!(matches!(r, Err(FundingError::Read { .. })), "buy: {r:?}");
        assert!(c.sent.lock().unwrap().is_empty(), "buy sent something");

        let c = chain();
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(c.clone()));
        let payer = Payer::gnosis(&client, &wallet);
        let r = extend_with_xdai(&payer, &BATCH, 20, None, amount, u128::MAX).await;
        assert!(matches!(r, Err(FundingError::Read { .. })), "extend: {r:?}");
        assert!(c.sent.lock().unwrap().is_empty(), "extend sent something");

        let c = chain();
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(c.clone()));
        let payer = Payer::gnosis(&client, &wallet);
        let r = fund_deposit_with_xdai(
            &payer,
            &CHEQUEBOOK,
            TARGET,
            None,
            WalletRead::Always,
            u128::MAX,
        )
        .await;
        assert!(
            matches!(r, Err(FundingError::Read { .. })),
            "deposit: {r:?}"
        );
        assert!(c.sent.lock().unwrap().is_empty(), "deposit sent something");
    }

    /// One of the three paying calls against `chain`, capped at
    /// `max_xdai_wei`, and what it sent.
    async fn pay_capped(
        chain: ScriptedChain,
        call: &str,
        max_xdai_wei: u128,
    ) -> (Result<(), FundingError>, Vec<&'static str>) {
        let (result, sent, _) = pay_capped_valued(chain, call, max_xdai_wei).await;
        (result, sent)
    }

    /// [`pay_capped`], plus the xDAI each swap carried.
    async fn pay_capped_valued(
        chain: ScriptedChain,
        call: &str,
        max_xdai_wei: u128,
    ) -> (Result<(), FundingError>, Vec<&'static str>, Vec<u128>) {
        let chain = std::sync::Arc::new(chain);
        let result = pay_capped_on(&chain, call, max_xdai_wei).await;
        let sent = chain.sent.lock().unwrap().clone();
        let values = chain.swap_values.lock().unwrap().clone();
        (result, sent, values)
    }

    /// One paying `call` capped at `max_xdai_wei` against `chain`.
    async fn pay_capped_on(
        chain: &std::sync::Arc<ScriptedChain>,
        call: &str,
        max_xdai_wei: u128,
    ) -> Result<(), FundingError> {
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain.clone()));
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let payer = Payer::gnosis(&client, &wallet);
        let amount = amount_per_chunk_for(24_000, 30);
        let result = match call {
            "buy" => {
                let policy = DepositPolicy::Managed {
                    chequebook: None,
                    target: TARGET,
                };
                buy_plan_with_xdai(&payer, policy, 20, amount, true, max_xdai_wei)
                    .await
                    .map(drop)
            }
            "extend" => extend_with_xdai(&payer, &BATCH, 20, None, amount, max_xdai_wei).await,
            "deposit" => fund_deposit_with_xdai(
                &payer,
                &CHEQUEBOOK,
                TARGET,
                None,
                WalletRead::Always,
                max_xdai_wei,
            )
            .await
            .map(drop),
            other => unreachable!("{other}"),
        };
        result
    }

    /// The `swap_input_wei` each paying call's quote shows the user, at
    /// the honest pool price.
    async fn quoted_swap_input(call: &str) -> u128 {
        let chain = std::sync::Arc::new(ScriptedChain {
            wallet_xdai: WEI_PER_XDAI,
            ..ScriptedChain::default()
        });
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain));
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let payer = Payer::gnosis(&client, &wallet);
        let funding = match call {
            "buy" => {
                let policy = DepositPolicy::Managed {
                    chequebook: None,
                    target: TARGET,
                };
                quote_plan(&payer, policy, 20, 30).await.unwrap().funding
            }
            "extend" => {
                quote_extend(&payer, &BATCH, 20, None, 30)
                    .await
                    .unwrap()
                    .funding
            }
            "deposit" => {
                deposit_status(&payer, &CHEQUEBOOK, TARGET, None, WalletRead::Always)
                    .await
                    .unwrap()
                    .funding
            }
            other => unreachable!("{other}"),
        };
        assert!(funding.swap_input_wei > 0, "{call}: the quote swaps");
        funding.swap_input_wei
    }

    fn payable(sqrt_price: u128) -> ScriptedChain {
        ScriptedChain {
            wallet_xdai: 1_000_000 * WEI_PER_XDAI,
            registered: true,
            issuer: node_eth(),
            sqrt_price: Some(sqrt_price),
            no_helper: true,
            ..ScriptedChain::default()
        }
    }

    /// #305 (audit MY-03): the swap is re-priced from `slot0()` when it
    /// runs. A forged (or sandwiched) high price would make it send far
    /// more xDAI than the quote the user approved; capped at that quote,
    /// every paying call refuses before sending anything, not even the
    /// one-time helper deploy, however much xDAI the wallet holds.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_forged_high_price_is_refused_before_anything_is_sent() {
        for call in ["buy", "extend", "deposit"] {
            let quoted = quoted_swap_input(call).await;
            let (result, sent) = pay_capped(payable(HONEST_SQRT_PRICE * 10), call, quoted).await;
            match result {
                Err(FundingError::PriceMoved {
                    input_wei,
                    max_wei,
                    helper_deployed,
                }) => {
                    assert!(!helper_deployed, "{call}");
                    assert_eq!(max_wei, quoted, "{call}");
                    assert!(input_wei > 50 * quoted, "{call}: {input_wei} vs {quoted}");
                }
                other => panic!("{call}: expected PriceMoved, got {other:?}"),
            }
            assert!(sent.is_empty(), "{call}: nothing may be sent: {sent:?}");
        }
    }

    /// At the price the user was quoted, the swap sends exactly the
    /// quote (deploying the helper first). A cap a little under the quote
    /// is still covered by the swap buffer, so it goes through sending
    /// the cap itself, never more.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_quoted_swap_input_is_what_is_sent() {
        for (call, rest) in [
            ("buy", &["approve", "createBatch"][..]),
            ("extend", &["approve", "createBatch"][..]),
            ("deposit", &["transfer"][..]),
        ] {
            let quoted = quoted_swap_input(call).await;
            let (result, sent, values) =
                pay_capped_valued(payable(HONEST_SQRT_PRICE), call, quoted).await;
            result.unwrap_or_else(|e| panic!("{call}: {e}"));
            let mut want = vec!["deploy", "swap"];
            want.extend_from_slice(rest);
            assert_eq!(sent, want, "{call}");
            assert_eq!(values, [quoted], "{call}");

            let (result, _, values) =
                pay_capped_valued(payable(HONEST_SQRT_PRICE), call, quoted - 1).await;
            result.unwrap_or_else(|e| panic!("{call}: {e}"));
            assert_eq!(values, [quoted - 1], "{call}: never past the cap");
        }
    }

    /// R1-M1: a small upward tick between the quote and the call (here
    /// the price up 3%) is absorbed by the quote's 5% swap buffer: the
    /// swap goes through sending exactly the cap, with `amountOutMin`
    /// still the xBZZ needed. A move the cap no longer covers by
    /// [`SWAP_MIN_MARGIN_NUM`] (here about 6%) is refused with nothing
    /// sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_small_price_tick_is_absorbed_by_the_cap() {
        for call in ["buy", "extend", "deposit"] {
            let quoted = quoted_swap_input(call).await;
            // sqrtPrice +1.5% is the price +3.02%.
            let (result, _, values) =
                pay_capped_valued(payable(HONEST_SQRT_PRICE / 1000 * 1015), call, quoted).await;
            result.unwrap_or_else(|e| panic!("{call}: {e}"));
            assert_eq!(values, [quoted], "{call}: the cap, not the re-priced input");

            // sqrtPrice +3% is the price +6.09%: past buffer and margin.
            let (result, sent) =
                pay_capped(payable(HONEST_SQRT_PRICE / 100 * 103), call, quoted).await;
            match result {
                Err(FundingError::PriceMoved {
                    input_wei,
                    max_wei,
                    helper_deployed,
                }) => {
                    assert!(!helper_deployed, "{call}");
                    assert_eq!(max_wei, quoted, "{call}");
                    assert!(input_wei > quoted, "{call}");
                }
                other => panic!("{call}: expected PriceMoved, got {other:?}"),
            }
            assert!(sent.is_empty(), "{call}: nothing may be sent: {sent:?}");
        }
    }

    /// R3-M2: a capped swap sends the cap with as little as 1% over fair
    /// value, which the pool fee plus a large swap's price impact can
    /// eat. It is dry-run first, with exactly the value it would send,
    /// and a would-be revert is refused as `PriceMoved` instead of being
    /// sent to revert on chain and burn its gas. An uncapped swap (the
    /// full 5% buffer) is not dry-run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_capped_swap_that_would_revert_is_not_sent() {
        for call in ["buy", "extend", "deposit"] {
            let quoted = quoted_swap_input(call).await;
            // sqrtPrice +1.5%: capped, and the dry run reverts.
            let chain = std::sync::Arc::new(ScriptedChain {
                swap_reverts: true,
                no_helper: false,
                ..payable(HONEST_SQRT_PRICE / 1000 * 1015)
            });
            match pay_capped_on(&chain, call, quoted).await {
                Err(FundingError::PriceMoved {
                    input_wei,
                    max_wei,
                    helper_deployed,
                }) => {
                    assert!(!helper_deployed, "{call}");
                    assert_eq!(max_wei, quoted, "{call}");
                    assert!(input_wei > quoted, "{call}");
                }
                other => panic!("{call}: expected PriceMoved, got {other:?}"),
            }
            assert_eq!(*chain.dry_run_values.lock().unwrap(), [quoted], "{call}");
            let sent = chain.sent.lock().unwrap().clone();
            assert!(sent.is_empty(), "{call}: nothing may be sent: {sent:?}");

            // A dry run that fails without reverting fails the call as a
            // read, still sending nothing.
            let chain = std::sync::Arc::new(ScriptedChain {
                swap_dry_run_fails: true,
                no_helper: false,
                ..payable(HONEST_SQRT_PRICE / 1000 * 1015)
            });
            match pay_capped_on(&chain, call, quoted).await {
                Err(FundingError::Read { what, .. }) => {
                    assert_eq!(what, "check the capped swap", "{call}");
                }
                other => panic!("{call}: expected Read, got {other:?}"),
            }
            assert!(chain.sent.lock().unwrap().is_empty(), "{call}");

            // The same capped swap whose dry run passes is sent, at the
            // value the dry run carried.
            let chain = std::sync::Arc::new(ScriptedChain {
                no_helper: false,
                ..payable(HONEST_SQRT_PRICE / 1000 * 1015)
            });
            pay_capped_on(&chain, call, quoted)
                .await
                .unwrap_or_else(|e| panic!("{call}: {e}"));
            assert_eq!(*chain.dry_run_values.lock().unwrap(), [quoted], "{call}");
            assert_eq!(*chain.swap_values.lock().unwrap(), [quoted], "{call}");

            // Uncapped (at the quoted price): no dry run, even one that
            // would revert.
            let chain = std::sync::Arc::new(ScriptedChain {
                swap_reverts: true,
                no_helper: false,
                ..payable(HONEST_SQRT_PRICE)
            });
            pay_capped_on(&chain, call, quoted)
                .await
                .unwrap_or_else(|e| panic!("{call}: {e}"));
            assert!(chain.dry_run_values.lock().unwrap().is_empty(), "{call}");
            assert_eq!(*chain.swap_values.lock().unwrap(), [quoted], "{call}");
        }
    }

    /// R4-M1: a fresh wallet's capped swap deploys the helper before its
    /// dry run, so a dry run that then reverts has sent that deploy. The
    /// refusal says so instead of "nothing was sent".
    /// R4-M2: a dry run answering `0x` (an `eth_call` to an address with
    /// no code, e.g. a backend that doesn't see the just-deployed helper
    /// yet) is not a pass: the call fails as a read and the swap isn't
    /// sent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_capped_swap_after_the_helper_deploy() {
        for call in ["buy", "extend", "deposit"] {
            let quoted = quoted_swap_input(call).await;
            let chain = std::sync::Arc::new(ScriptedChain {
                swap_reverts: true,
                ..payable(HONEST_SQRT_PRICE / 1000 * 1015)
            });
            match pay_capped_on(&chain, call, quoted).await {
                Err(
                    e @ FundingError::PriceMoved {
                        helper_deployed: true,
                        ..
                    },
                ) => {
                    let m = e.to_string();
                    assert!(!m.contains("nothing was sent"), "{call}: {m}");
                    assert!(
                        m.contains("only the one-time swap helper setup was sent"),
                        "{call}: {m}"
                    );
                }
                other => panic!("{call}: expected PriceMoved after a deploy, got {other:?}"),
            }
            assert_eq!(*chain.sent.lock().unwrap(), ["deploy"], "{call}");

            let chain = std::sync::Arc::new(ScriptedChain {
                swap_dry_run_empty: true,
                ..payable(HONEST_SQRT_PRICE / 1000 * 1015)
            });
            match pay_capped_on(&chain, call, quoted).await {
                Err(FundingError::Read { what, message }) => {
                    assert_eq!(what, "check the capped swap", "{call}");
                    assert!(message.contains("returned 0 bytes"), "{call}: {message}");
                }
                other => panic!("{call}: expected Read, got {other:?}"),
            }
            assert_eq!(*chain.dry_run_values.lock().unwrap(), [quoted], "{call}");
            assert_eq!(*chain.sent.lock().unwrap(), ["deploy"], "{call}: no swap");
        }
    }

    #[test]
    fn a_capped_swap_never_sends_past_the_cap() {
        let fair = 1_000_000u128;
        // Within the cap: the buffered input.
        assert_eq!(capped_swap_value(fair, 1_050_000), Some(1_050_000));
        assert_eq!(capped_swap_value(fair, 2_000_000), Some(1_050_000));
        // Under the buffered input but still 1% over fair: the cap.
        assert_eq!(capped_swap_value(fair, 1_049_999), Some(1_049_999));
        assert_eq!(capped_swap_value(fair, 1_010_000), Some(1_010_000));
        // Less than 1% over fair: refused.
        assert_eq!(capped_swap_value(fair, 1_009_999), None);
        assert_eq!(capped_swap_value(fair, 0), None);
        assert_eq!(capped_swap_value(0, 0), Some(0));
    }

    /// A cap of 0 forbids any swap but doesn't stand in the way of a call
    /// whose xBZZ the wallet already holds (a retry after the swap landed,
    /// or the apps' "add credit" from wallet xBZZ), whatever the pool
    /// reads.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_zero_cap_still_pays_from_held_xbzz() {
        let plan = plan_cost_plur(amount_per_chunk_for(24_000, 30), 20).unwrap();
        for (call, want) in [
            ("buy", &["approve", "createBatch"][..]),
            ("extend", &["approve", "createBatch"][..]),
            ("deposit", &["transfer"][..]),
        ] {
            let (result, sent) = pay_capped(
                ScriptedChain {
                    wallet_bzz: plan + TARGET,
                    ..payable(HONEST_SQRT_PRICE * 10)
                },
                call,
                0,
            )
            .await;
            result.unwrap_or_else(|e| panic!("{call}: {e}"));
            assert_eq!(sent, want, "{call}");
        }
    }

    /// An explicit deposit amount is quoted by `deposit_status` with the
    /// same `add`, and that quote is exactly the cap the funding call
    /// with that `add` needs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deposit_amount_has_its_own_quote() {
        let add = 5 * TARGET;
        let full = || ScriptedChain {
            chequebook_bzz: TARGET,
            ..payable(HONEST_SQRT_PRICE)
        };
        let chain = std::sync::Arc::new(full());
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain));
        let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
        let payer = Payer::gnosis(&client, &wallet);
        let at_target = deposit_status(&payer, &CHEQUEBOOK, TARGET, None, WalletRead::Always)
            .await
            .unwrap();
        assert_eq!(at_target.funding.swap_input_wei, 0);
        let quote = deposit_status(&payer, &CHEQUEBOOK, TARGET, Some(add), WalletRead::Always)
            .await
            .unwrap();
        assert!(!quote.needs_top_up(), "still described against the target");
        assert_eq!(quote.funding.bzz_to_acquire, add);
        let quoted = quote.funding.swap_input_wei;
        assert!(quoted > 0);

        let pay = |max| async move {
            let chain = std::sync::Arc::new(full());
            let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain.clone()));
            let wallet = Wallet::new([5u8; 32], crate::tx::GNOSIS_CHAIN_ID).unwrap();
            let payer = Payer::gnosis(&client, &wallet);
            let r = fund_deposit_with_xdai(
                &payer,
                &CHEQUEBOOK,
                TARGET,
                Some(add),
                WalletRead::Always,
                max,
            )
            .await;
            let sent = chain.sent.lock().unwrap().clone();
            (r, sent)
        };
        // Under what the quote's buffer absorbs: refused.
        let (r, sent) = pay(quoted / 106 * 100).await;
        assert!(matches!(r, Err(FundingError::PriceMoved { .. })), "{r:?}");
        assert!(sent.is_empty(), "{sent:?}");
        let (r, sent) = pay(quoted).await;
        r.unwrap();
        assert_eq!(sent, ["deploy", "swap", "transfer"]);
    }

    /// R1-M1 (refuted): a resize priced from `remaining` at quote time and
    /// executed later, once the batch has paid out `elapsed` per chunk,
    /// still leaves at least `remaining_now + add` per chunk after
    /// `increaseDepth`: the batch's expiry *as of execution* plus the
    /// extra days. The quote's top-up grows with `remaining`, so a
    /// balance that decayed since only makes it more than enough.
    #[test]
    fn a_resize_executed_after_its_quote_still_keeps_the_expiry() {
        let price = 24_000u128;
        let add = amount_per_chunk_for(price, 7);
        for delta in 1..=4u8 {
            for remaining_q in [0, price * 17_280, price * 17_280 * 90] {
                let amount = resize_top_up_per_chunk(remaining_q, add, delta).unwrap();
                for elapsed in [0, price * 100, remaining_q / 2, remaining_q] {
                    let Some(remaining_now) = remaining_q.checked_sub(elapsed) else {
                        continue;
                    };
                    let after = (remaining_now + amount) >> delta;
                    assert!(
                        after >= remaining_now + add,
                        "delta {delta}, remaining {remaining_q}, elapsed {elapsed}"
                    );
                }
            }
        }
    }
}
