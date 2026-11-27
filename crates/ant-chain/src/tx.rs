//! EIP-155 raw transaction signing and Gnosis JSON-RPC submission.
//!
//! Built for the postage-stamp workflows in M3 Phase 8: BZZ
//! `approve()` followed by `PostageStamp.createBatch / topUp /
//! increaseDepth`. We avoid pulling in the full alloy / ethers stack
//! by implementing the narrowly-scoped subset we need:
//!
//! - Legacy (pre-EIP-1559) tx envelope; Gnosis still accepts these and
//!   bee's own tx layer (`pkg/transaction`) does the same.
//! - EIP-155 chain-id replay protection (`v = chain_id * 2 + 35 + recid`).
//! - Calldata builders for `IERC20.approve(address,uint256)` and the
//!   three `PostageStamp` write methods.
//! - Receipt polling (`eth_getTransactionReceipt`) with a configurable
//!   timeout and per-poll backoff so a stuck mempool surfaces as a
//!   structured error instead of hanging forever.
//!
//! The signing module re-uses `ant_crypto::sign_prehash`, which already
//! returns a 65-byte `r ‖ s ‖ v` signature over a raw digest — exactly
//! what RLP-encoded EIP-155 expects.

#[cfg(feature = "chain-rpc")]
use std::time::Duration;
#[cfg(feature = "chain-rpc")]
use tokio::time::Instant;

use ant_crypto::{sign_prehash, SECP256K1_SECRET_LEN};
use primitive_types::U256;
#[cfg(feature = "chain-rpc")]
use serde_json::json;
use sha3::{Digest, Keccak256};
use thiserror::Error;

#[cfg(feature = "chain-rpc")]
use crate::{ChainClient, RpcError};

/// Gnosis chain id (mainnet xDai). Used as the EIP-155 replay prefix
/// for every tx we sign here.
pub const GNOSIS_CHAIN_ID: u64 = 100;

/// Default gas price used when the operator doesn't pass one explicitly:
/// 2 gwei. Gnosis treats a flat 1 gwei minimum as the network floor;
/// 2 gwei keeps us above any rounding-up most validators do without
/// burning unnecessary xDAI.
pub const DEFAULT_GAS_PRICE_WEI: u64 = 2_000_000_000;

/// Conservative gas-limit defaults per workflow, sized off live
/// `eth_estimateGas` against the deployed Gnosis `PostageStamp`
/// (`0x45a1502382541Cd610CC9068e88727426b696293`):
///
/// - `topUp`: ~370 k → 600 k cap
/// - `increaseDepth`: ~530 k → 800 k cap
/// - `createBatch`: bee uses 600 k → 800 k cap
/// - `approve` on the BZZ ERC-20: ~50 k → 100 k cap
///
/// Each cap is ~1.5× the real cost so a busier-than-typical block
/// (which can spike storage write costs by ~10 %) still fits under
/// the limit.
pub const ERC20_APPROVE_GAS: u64 = 100_000;
pub const POSTAGE_CREATE_BATCH_GAS: u64 = 800_000;
pub const POSTAGE_TOP_UP_GAS: u64 = 600_000;
pub const POSTAGE_INCREASE_DEPTH_GAS: u64 = 800_000;
/// `Chequebook.cashChequeBeneficiary` gas limit — bee uses ~115k in
/// production; we round up to 200k so a cold paidOut slot or a
/// 0-balance fail still has runway.
pub const CHEQUEBOOK_CASH_GAS: u64 = 200_000;
/// `SimpleSwapFactory.deploySimpleSwap` gas limit. The factory does
/// a CREATE2 of the `SimpleSwap` proxy, calls `setIssuer` on it, and
/// flips a `deployedContracts[swap]` storage slot. Empirically
/// ~700 K on Gnosis; we cap at 1.5 M for headroom against future
/// EIP gas-cost bumps. A successful tx still only burns the actual
/// gas used, so over-capping costs nothing.
pub const FACTORY_DEPLOY_CHEQUEBOOK_GAS: u64 = 1_500_000;
/// Plain `IERC20.transfer(to, value)` to fund a freshly-deployed
/// chequebook. Same shape as `approve`, so 100 K is plenty.
pub const ERC20_TRANSFER_GAS: u64 = 100_000;
/// `SimpleSwap.withdraw` gas: an issuer check, a liquid-balance read and
/// one xBZZ transfer back to the issuer.
pub const CHEQUEBOOK_WITHDRAW_GAS: u64 = 150_000;
/// One-time CREATE2 deployment of the `BzzSwapHelper` (~1.2 KB of init
/// code) through the deterministic deployer. Empirically ~250 K; cap at
/// 600 K for headroom.
pub const SWAP_HELPER_DEPLOY_GAS: u64 = 600_000;
/// `BzzSwapHelper.swapXdaiForBzz`: WXDAI `deposit` + a single V3 pool
/// `swap` + ERC-20 transfer in the callback. Measured ~180 K; cap 400 K.
pub const SWAP_HELPER_SWAP_GAS: u64 = 400_000;

/// Creation bytecode for `contracts/BzzSwapHelper.sol`, compiled with
/// `solc --optimize`. Embedded so the node can self-deploy the helper
/// (via CREATE2) the first time anyone funds storage with xDAI. Keep in
/// sync with the committed `.bin` artifact; [`tests`] pins its hash.
pub const SWAP_HELPER_INITCODE_HEX: &str = include_str!("../../../contracts/BzzSwapHelper.bin");

/// `BzzSwapHelper.swapXdaiForBzz(address recipient, uint256 amountOutMin)`.
#[must_use]
pub fn swap_helper_calldata(recipient: &[u8; 20], amount_out_min: &U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"swapXdaiForBzz(address,uint256)")[0..4]);
    data.extend_from_slice(&pad_word_address(recipient));
    data.extend_from_slice(&u256_be_word(amount_out_min));
    data
}

/// Whether a successful `swapXdaiForBzz` dry run really ran the swap:
/// its `uint256 bzzOut` return word must be there and cover
/// `amount_out_min`. Empty output is what an `eth_call` to an address
/// with no code returns (a lagging RPC backend that doesn't see the
/// helper yet), not a passed check.
#[cfg(feature = "chain-rpc")]
pub(crate) fn check_swap_dry_run(out: &[u8], amount_out_min: U256) -> Result<(), crate::RpcError> {
    let Some(word) = out.get(..32) else {
        return Err(crate::RpcError::Decode(format!(
            "the swap dry run returned {} bytes, not the xBZZ it buys (is the swap helper \
             deployed on this RPC yet?)",
            out.len()
        )));
    };
    let bzz_out = U256::from_big_endian(word);
    if bzz_out < amount_out_min {
        return Err(crate::RpcError::Decode(format!(
            "the swap dry run buys {bzz_out} PLUR of xBZZ, less than the {amount_out_min} needed"
        )));
    }
    Ok(())
}

/// Salt used for the helper's CREATE2 deployment. Zero — the init code
/// already fully determines the address, and we only ever want one
/// canonical instance.
const SWAP_HELPER_SALT: [u8; 32] = [0u8; 32];

/// Decode a `0x`-prefixed 20-byte hex address constant. Panics on a
/// malformed literal — only ever called on our own `const` strings, so a
/// bad value is a build-time bug, not a runtime input.
fn decode_addr_const(s: &str) -> [u8; 20] {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out).expect("valid 20-byte address constant");
    out
}

/// The helper's creation bytecode as bytes.
#[must_use]
pub fn swap_helper_initcode() -> Vec<u8> {
    hex::decode(SWAP_HELPER_INITCODE_HEX.trim()).expect("valid helper initcode hex")
}

/// Deterministic address the `BzzSwapHelper` deploys to via the CREATE2
/// deployer: `keccak256(0xff ‖ deployer ‖ salt ‖ keccak256(initcode))[12..]`.
#[must_use]
pub fn swap_helper_address() -> [u8; 20] {
    let deployer = decode_addr_const(crate::CREATE2_DEPLOYER);
    let init_hash = keccak256(&swap_helper_initcode());
    let mut pre = Vec::with_capacity(1 + 20 + 32 + 32);
    pre.push(0xff);
    pre.extend_from_slice(&deployer);
    pre.extend_from_slice(&SWAP_HELPER_SALT);
    pre.extend_from_slice(&init_hash);
    let h = keccak256(&pre);
    let mut addr = [0u8; 20];
    addr.copy_from_slice(&h[12..32]);
    addr
}

/// Calldata for the deterministic deployer: `salt(32) ‖ initcode`.
#[cfg(feature = "chain-rpc")]
#[must_use]
fn create2_deploy_calldata() -> Vec<u8> {
    let init = swap_helper_initcode();
    let mut data = Vec::with_capacity(32 + init.len());
    data.extend_from_slice(&SWAP_HELPER_SALT);
    data.extend_from_slice(&init);
    data
}

#[derive(Debug, Error)]
pub enum TxError {
    #[cfg(feature = "chain-rpc")]
    #[error("rpc: {0}")]
    Rpc(#[from] RpcError),
    #[error("crypto: {0}")]
    Crypto(#[from] ant_crypto::CryptoError),
    #[error("rlp: {0}")]
    Rlp(String),
    #[error("transaction reverted: {receipt_status}")]
    Reverted { receipt_status: String },
    /// No receipt for a transaction that was sent, within the wait. It
    /// may still be mined: not a "nothing happened" answer, so a caller
    /// must not send the same write again on this error alone.
    /// `last_error` is the last receipt poll's transport error (without
    /// the RPC URL), when the RPC wasn't answering at the end (#253).
    #[error(
        "transaction 0x{tx_hash} was sent but no receipt arrived within {wait_secs}s; it may \
         still be mined{}",
        last_error.as_ref().map_or_else(String::new, |e| format!(" (last receipt poll: {e})"))
    )]
    Timeout {
        tx_hash: String,
        wait_secs: u64,
        last_error: Option<String>,
    },
    #[error("decode: {0}")]
    Decode(String),
    /// The receipt polled for the transaction we signed carries another
    /// (or no) `transactionHash`: its logs would describe someone else's
    /// tx (issue #284). An `eth_sendRawTransaction` answering another
    /// hash is not this error; the write waits on its own hash instead.
    #[error("{what} is {answered}, not the signed transaction 0x{sent}")]
    HashMismatch {
        what: &'static str,
        sent: String,
        answered: String,
    },
}

/// Parameters that fully describe an EIP-155 legacy transaction.
#[derive(Debug, Clone)]
pub struct LegacyTx {
    pub nonce: u64,
    pub gas_price_wei: u64,
    pub gas_limit: u64,
    /// Recipient contract / address. `None` for contract creation —
    /// not supported by the workflows here, so always `Some` in this
    /// module's helpers.
    pub to: [u8; 20],
    pub value_wei: U256,
    pub data: Vec<u8>,
    pub chain_id: u64,
}

/// RLP-encode and sign `tx` with `secret`, returning the raw bytes
/// suitable for `eth_sendRawTransaction` and the keccak256 tx hash.
pub fn sign_legacy_tx(
    secret: &[u8; SECP256K1_SECRET_LEN],
    tx: &LegacyTx,
) -> Result<(Vec<u8>, [u8; 32]), TxError> {
    let signing_payload = rlp_encode_legacy(tx, /* signing */ true, /* sig */ None);
    let digest = keccak256(&signing_payload);

    let sig = sign_prehash(secret, &digest)?;
    let r = &sig[0..32];
    let s = &sig[32..64];
    // `sign_prehash` returns recid in {27, 28}. EIP-155 wants
    // v = chain_id * 2 + 35 + recid where recid is in {0, 1}.
    let recid = sig[64]
        .checked_sub(27)
        .ok_or_else(|| TxError::Decode(format!("invalid recovery id {} from signer", sig[64])))?;
    let v = tx
        .chain_id
        .saturating_mul(2)
        .saturating_add(35)
        .saturating_add(u64::from(recid));

    let raw = rlp_encode_legacy(
        tx,
        /* signing */ false,
        Some(SignatureBytes {
            v,
            r: r.try_into().unwrap(),
            s: s.try_into().unwrap(),
        }),
    );
    let tx_hash = keccak256(&raw);
    Ok((raw, tx_hash))
}

struct SignatureBytes {
    v: u64,
    r: [u8; 32],
    s: [u8; 32],
}

/// RLP-encode the transaction. When `signing` is true and `sig` is
/// `None`, emit the EIP-155 signing payload (nine fields, last three
/// being `[chain_id, 0, 0]`). When `sig` is `Some`, emit the final
/// signed envelope (nine fields, last three being `[v, r, s]`).
fn rlp_encode_legacy(tx: &LegacyTx, signing: bool, sig: Option<SignatureBytes>) -> Vec<u8> {
    let mut s = rlp::RlpStream::new_list(9);
    rlp_append_u64(&mut s, tx.nonce);
    rlp_append_u64(&mut s, tx.gas_price_wei);
    rlp_append_u64(&mut s, tx.gas_limit);
    s.append(&tx.to.as_ref());
    rlp_append_u256(&mut s, &tx.value_wei);
    s.append(&tx.data.as_slice());
    match (signing, sig) {
        (true, None) => {
            rlp_append_u64(&mut s, tx.chain_id);
            s.append_empty_data();
            s.append_empty_data();
        }
        (false, Some(sig)) => {
            rlp_append_u64(&mut s, sig.v);
            rlp_append_truncated_be(&mut s, &sig.r);
            rlp_append_truncated_be(&mut s, &sig.s);
        }
        _ => unreachable!("rlp_encode_legacy called with conflicting (signing, sig)"),
    }
    s.out().to_vec()
}

fn rlp_append_u64(s: &mut rlp::RlpStream, v: u64) {
    if v == 0 {
        s.append_empty_data();
    } else {
        let bytes = v.to_be_bytes();
        let off = bytes.iter().position(|&b| b != 0).unwrap_or(7);
        s.append(&&bytes[off..]);
    }
}

fn rlp_append_u256(s: &mut rlp::RlpStream, v: &U256) {
    if v.is_zero() {
        s.append_empty_data();
        return;
    }
    let buf = v.to_big_endian();
    let off = buf.iter().position(|&b| b != 0).unwrap_or(31);
    s.append(&&buf[off..]);
}

fn rlp_append_truncated_be(s: &mut rlp::RlpStream, v: &[u8; 32]) {
    let off = v.iter().position(|&b| b != 0).unwrap_or(31);
    s.append(&&v[off..]);
}

fn keccak256(data: &[u8]) -> [u8; 32] {
    let h = Keccak256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&h);
    out
}

// --- ABI calldata builders ---

/// `IERC20.approve(address spender, uint256 value)` calldata.
#[must_use]
pub fn erc20_approve_calldata(spender: &[u8; 20], value: &U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"approve(address,uint256)")[0..4]);
    data.extend_from_slice(&pad_word_address(spender));
    data.extend_from_slice(&u256_be_word(value));
    data
}

/// `IERC20.transfer(address to, uint256 value)` calldata. Used by
/// `Wallet::erc20_transfer` to fund a freshly-deployed chequebook
/// directly from the operator's wallet (the `SimpleSwap` contract
/// has no `deposit()` view; its `balance()` is just
/// `BZZ.balanceOf(swap)`, so a plain transfer is the funding path).
#[must_use]
pub fn erc20_transfer_calldata(to: &[u8; 20], value: &U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"transfer(address,uint256)")[0..4]);
    data.extend_from_slice(&pad_word_address(to));
    data.extend_from_slice(&u256_be_word(value));
    data
}

/// `PostageStamp.createBatch(address owner, uint256 initialBalancePerChunk,
///                           uint8 depth, uint8 bucketDepth, bytes32 nonce, bool immutableFlag)`.
#[must_use]
pub fn postage_create_batch_calldata(
    owner: &[u8; 20],
    initial_balance_per_chunk: &U256,
    depth: u8,
    bucket_depth: u8,
    nonce: &[u8; 32],
    immutable: bool,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 * 6);
    data.extend_from_slice(
        &keccak256(b"createBatch(address,uint256,uint8,uint8,bytes32,bool)")[0..4],
    );
    data.extend_from_slice(&pad_word_address(owner));
    data.extend_from_slice(&u256_be_word(initial_balance_per_chunk));
    data.extend_from_slice(&pad_word_u8(depth));
    data.extend_from_slice(&pad_word_u8(bucket_depth));
    data.extend_from_slice(nonce);
    data.extend_from_slice(&pad_word_bool(immutable));
    data
}

/// `PostageStamp.topUp(bytes32 batchId, uint256 topupAmountPerChunk)`.
#[must_use]
pub fn postage_top_up_calldata(batch_id: &[u8; 32], topup_per_chunk: &U256) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"topUp(bytes32,uint256)")[0..4]);
    data.extend_from_slice(batch_id);
    data.extend_from_slice(&u256_be_word(topup_per_chunk));
    data
}

/// `PostageStamp.increaseDepth(bytes32 batchId, uint8 newDepth)`.
#[must_use]
pub fn postage_increase_depth_calldata(batch_id: &[u8; 32], new_depth: u8) -> Vec<u8> {
    let mut data = Vec::with_capacity(4 + 32 + 32);
    data.extend_from_slice(&keccak256(b"increaseDepth(bytes32,uint8)")[0..4]);
    data.extend_from_slice(batch_id);
    data.extend_from_slice(&pad_word_u8(new_depth));
    data
}

/// 4-byte selector for the `BatchCreated` event (used by
/// `eth_getLogs` filtering when watching for our own create txs).
#[must_use]
pub fn batch_created_event_topic() -> [u8; 32] {
    keccak256(b"BatchCreated(bytes32,uint256,uint256,address,uint8,uint8,bool)")
}

fn pad_word_address(addr: &[u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..32].copy_from_slice(addr);
    w
}

const fn pad_word_u8(v: u8) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[31] = v;
    w
}

fn pad_word_bool(v: bool) -> [u8; 32] {
    pad_word_u8(u8::from(v))
}

fn u256_be_word(v: &U256) -> [u8; 32] {
    v.to_big_endian()
}

// --- RPC helpers (write side) ---

#[cfg(feature = "chain-rpc")]
impl ChainClient {
    /// Get the next nonce (`pending` block tag, so two back-to-back tx
    /// submissions don't collide on the same nonce).
    pub async fn eth_get_transaction_count_pending(
        &self,
        addr: &[u8; 20],
    ) -> Result<u64, RpcError> {
        let params = json!([format!("0x{}", hex::encode(addr)), "pending"]);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_getTransactionCount",
            "params": params,
        });
        let v = self.rpc(&body).await?;
        if let Some(err) = crate::rpc_error_json(&v) {
            return Err(RpcError::Rpc(err));
        }
        let s = crate::rpc_result_str(&v)?;
        let s = s.strip_prefix("0x").unwrap_or(s);
        let s = if s.is_empty() { "0" } else { s };
        u64::from_str_radix(s, 16).map_err(|e| RpcError::Decode(e.to_string()))
    }

    /// Submit a pre-signed raw transaction. Returns the 32-byte tx hash
    /// the RPC answered with. That should be the one `sign_legacy_tx`
    /// already computed locally, but a broken or malicious relay can
    /// answer with any hash; [`Wallet`] compares the two and waits on
    /// its own (issue #284).
    pub async fn eth_send_raw_transaction(&self, raw: &[u8]) -> Result<[u8; 32], RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_sendRawTransaction",
            "params": [format!("0x{}", hex::encode(raw))],
        });
        let v = self.rpc(&body).await?;
        if let Some(err) = crate::rpc_error_json(&v) {
            return Err(RpcError::Rpc(err));
        }
        let s = crate::rpc_result_str(&v)?;
        let s = s.strip_prefix("0x").unwrap_or(s);
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).map_err(|e| RpcError::Decode(e.to_string()))?;
        Ok(out)
    }

    /// One-shot `eth_getTransactionReceipt`. Returns `Ok(None)` when the
    /// tx isn't mined yet (or is unknown to the node), `Ok(Some(_))`
    /// with the parsed logs once it is. Unlike [`Self::wait_for_receipt`]
    /// this does not poll and does not treat a reverted tx as an error —
    /// the recovery scan only reads logs from already-confirmed txs and
    /// just needs whatever events are present.
    pub async fn eth_get_transaction_receipt(
        &self,
        tx_hash: &[u8; 32],
    ) -> Result<Option<TxReceipt>, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_getTransactionReceipt",
            "params": [format!("0x{}", hex::encode(tx_hash))],
        });
        let v = self.rpc(&body).await?;
        if let Some(err) = crate::rpc_error_json(&v) {
            return Err(RpcError::Rpc(err));
        }
        let Some(receipt) = v.get("result").filter(|r| !r.is_null()) else {
            return Ok(None);
        };
        let block_number = receipt
            .get("blockNumber")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        let logs = receipt.get("logs").and_then(|l| l.as_array()).map_or_else(
            || Ok(Vec::new()),
            |arr| arr.iter().map(parse_log).collect::<Result<Vec<_>, _>>(),
        )?;
        Ok(Some(TxReceipt {
            tx_hash: *tx_hash,
            block_number: parse_hex_u64(block_number).unwrap_or(0),
            logs,
        }))
    }

    /// Poll `eth_getTransactionReceipt` until the receipt arrives or
    /// `wait` elapses. Returns the receipt as a structured `TxReceipt`;
    /// reverted txs become `TxError::Reverted` so a successful return
    /// always means the EVM accepted the tx. A receipt whose
    /// `transactionHash` is missing or isn't `tx_hash` is an error
    /// ([`TxError::HashMismatch`]), checked before its status: its logs
    /// would describe some other transaction (issue #284).
    ///
    /// The receipt read is idempotent and the transaction is already
    /// out, so a poll the RPC didn't answer (a transport error: a
    /// dropped keep-alive connection, a refused connect, an unreadable
    /// body) says nothing about it: it is logged and counted as "not
    /// mined yet", and polling goes on until `wait` runs out (#253). A
    /// JSON-RPC error answer still fails at once. Each poll is cut off
    /// at the time left until the deadline, but never below
    /// [`RECEIPT_POLL_MIN`], and the back-off sleep never passes the
    /// deadline, so this returns within `wait + RECEIPT_POLL_MIN` (plus
    /// one poll's JSON parsing) whatever the RPC does. It only reads:
    /// nothing is ever sent again from here, and a
    /// [`TxError::Timeout`] means "sent, outcome unknown".
    pub async fn wait_for_receipt(
        &self,
        tx_hash: &[u8; 32],
        wait: Duration,
    ) -> Result<TxReceipt, TxError> {
        let deadline = Instant::now() + wait;
        let mut delay = Duration::from_millis(500);
        let mut last_error = None;
        loop {
            let body = json!({
                "jsonrpc": "2.0",
                "id": 1u64,
                "method": "eth_getTransactionReceipt",
                "params": [format!("0x{}", hex::encode(tx_hash))],
            });
            let budget = deadline
                .saturating_duration_since(Instant::now())
                .max(RECEIPT_POLL_MIN);
            let answer = match tokio::time::timeout(budget, self.rpc(&body)).await {
                Ok(Ok(v)) => Some(v),
                Ok(Err(RpcError::Http(e))) => {
                    last_error = Some(e.without_url().to_string());
                    None
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    last_error = Some(format!("no answer within {}s", budget.as_secs()));
                    None
                }
            };
            if let Some(v) = answer {
                last_error = None;
                if let Some(err) = crate::rpc_error_json(&v) {
                    return Err(RpcError::Rpc(err).into());
                }
                if let Some(receipt) = v.get("result").filter(|r| !r.is_null()) {
                    return receipt_for(tx_hash, receipt);
                }
            } else {
                tracing::warn!(
                    target: "ant_chain::tx",
                    tx = %format!("0x{}", hex::encode(tx_hash)),
                    error = last_error.as_deref().unwrap_or(""),
                    "receipt poll got no answer; the transaction is already sent, polling on",
                );
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(TxError::Timeout {
                    tx_hash: hex::encode(tx_hash),
                    wait_secs: wait.as_secs(),
                    last_error,
                });
            }
            tokio::time::sleep(delay.min(left)).await;
            delay = (delay * 2).min(Duration::from_secs(8));
        }
    }
}

/// The floor of one receipt poll's time cut-off in
/// [`ChainClient::wait_for_receipt`]: the poll sent at the deadline
/// still gets this long to answer.
#[cfg(feature = "chain-rpc")]
pub const RECEIPT_POLL_MIN: Duration = Duration::from_secs(10);

/// Check a `eth_getTransactionReceipt` result for `tx_hash` and turn it
/// into a [`TxReceipt`] (see [`ChainClient::wait_for_receipt`]).
#[cfg(feature = "chain-rpc")]
fn receipt_for(tx_hash: &[u8; 32], receipt: &serde_json::Value) -> Result<TxReceipt, TxError> {
    let answered = receipt
        .get("transactionHash")
        .and_then(|h| h.as_str())
        .unwrap_or("");
    if !answered.eq_ignore_ascii_case(&format!("0x{}", hex::encode(tx_hash))) {
        return Err(TxError::HashMismatch {
            what: "receipt transactionHash",
            sent: hex::encode(tx_hash),
            answered: answered.to_string(),
        });
    }
    let status = receipt.get("status").and_then(|s| s.as_str()).unwrap_or("");
    let block_number = receipt
        .get("blockNumber")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    if status != "0x1" {
        return Err(TxError::Reverted {
            receipt_status: status.to_string(),
        });
    }
    let logs = receipt.get("logs").and_then(|l| l.as_array()).map_or_else(
        || Ok(Vec::new()),
        |arr| arr.iter().map(parse_log).collect::<Result<Vec<_>, _>>(),
    )?;
    Ok(TxReceipt {
        tx_hash: *tx_hash,
        block_number: parse_hex_u64(block_number).unwrap_or(0),
        logs,
    })
}

#[cfg(feature = "chain-rpc")]
fn parse_log(v: &serde_json::Value) -> Result<EventLog, RpcError> {
    let address_hex = v
        .get("address")
        .and_then(|a| a.as_str())
        .ok_or_else(|| RpcError::Decode("log missing address".into()))?
        .trim_start_matches("0x");
    let mut address = [0u8; 20];
    hex::decode_to_slice(address_hex, &mut address)
        .map_err(|e| RpcError::Decode(format!("address: {e}")))?;
    let topics = v
        .get("topics")
        .and_then(|t| t.as_array())
        .ok_or_else(|| RpcError::Decode("log missing topics".into()))?
        .iter()
        .map(|t| {
            let s = t
                .as_str()
                .ok_or_else(|| RpcError::Decode("non-string topic".into()))?;
            let s = s.trim_start_matches("0x");
            let mut out = [0u8; 32];
            hex::decode_to_slice(s, &mut out)
                .map_err(|e| RpcError::Decode(format!("topic: {e}")))?;
            Ok(out)
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    let data_hex = v
        .get("data")
        .and_then(|d| d.as_str())
        .unwrap_or("0x")
        .trim_start_matches("0x");
    let data = hex::decode(data_hex).map_err(|e| RpcError::Decode(format!("data: {e}")))?;
    Ok(EventLog {
        address,
        topics,
        data,
    })
}

#[cfg(feature = "chain-rpc")]
fn parse_hex_u64(s: &str) -> Option<u64> {
    let s = s.trim_start_matches("0x");
    let s = if s.is_empty() { "0" } else { s };
    u64::from_str_radix(s, 16).ok()
}

/// Decoded event log emitted by a tx we sent. Just enough fields to
/// pull `BatchCreated.batchId` out of a `createBatch` receipt — see
/// [`extract_created_batch_id`].
#[derive(Debug, Clone)]
pub struct EventLog {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

/// Tx receipt reduced to the fields the postage workflow needs:
/// confirmation block number plus all emitted logs.
#[derive(Debug, Clone)]
pub struct TxReceipt {
    pub tx_hash: [u8; 32],
    pub block_number: u64,
    pub logs: Vec<EventLog>,
}

/// Pull the new `batchId` out of a `createBatch` receipt by matching
/// the `BatchCreated` event topic and reading the batch id from
/// topic[1] (it's an indexed parameter). Only a log emitted by
/// `postage` (the `PostageStamp` contract the batch was bought from)
/// counts: any contract can emit a look-alike event in the same tx.
/// Returns `None` if no such event is in the receipt.
#[must_use]
pub fn extract_created_batch_id(receipt: &TxReceipt, postage: &[u8; 20]) -> Option<[u8; 32]> {
    let topic = batch_created_event_topic();
    receipt
        .logs
        .iter()
        .find(|l| &l.address == postage && l.topics.first() == Some(&topic) && l.topics.len() > 1)
        .map(|l| l.topics[1])
}

/// One-shot helper that holds a signing key + chain id and submits
/// signed txs against a [`ChainClient`]. The high-level methods
/// (`approve_bzz`, `create_batch`, `top_up`, `increase_depth`)
/// pipeline nonce-fetch → calldata → sign → send → wait so the
/// operator only deals with a `Result<TxReceipt, TxError>`.
///
/// The whole struct is `chain-rpc`-only — without that feature the
/// crate exposes only the offline tx-construction helpers
/// (`sign_legacy_tx`, RLP, calldata builders), which is all the iOS
/// download client needs.
#[cfg(feature = "chain-rpc")]
pub struct Wallet {
    secret: [u8; SECP256K1_SECRET_LEN],
    address: [u8; 20],
    chain_id: u64,
    /// Default per-tx confirmation cap. 60 s comfortably covers
    /// Gnosis's ~5 s block time even when the mempool is busy; bigger
    /// callers can override with [`Wallet::wait_for`].
    pub default_wait: Duration,
    /// Default gas price (wei). Set to [`DEFAULT_GAS_PRICE_WEI`]
    /// unless the operator has a reason to bid higher (chain
    /// congestion).
    pub default_gas_price_wei: u64,
}

#[cfg(feature = "chain-rpc")]
impl Wallet {
    /// Build a wallet from a 32-byte secret. Derives the Ethereum
    /// address locally so the operator can cross-check it before
    /// sending value-bearing txs.
    pub fn new(secret: [u8; SECP256K1_SECRET_LEN], chain_id: u64) -> Result<Self, TxError> {
        let sk = k256_signing_key(&secret)?;
        let vk = *sk.verifying_key();
        let address = ant_crypto::ethereum_address_from_public_key(&vk);
        Ok(Self {
            secret,
            address,
            chain_id,
            default_wait: Duration::from_mins(1),
            default_gas_price_wei: DEFAULT_GAS_PRICE_WEI,
        })
    }

    #[must_use]
    pub const fn address(&self) -> &[u8; 20] {
        &self.address
    }

    /// Override the default wait timeout.
    #[must_use]
    pub const fn wait_for(mut self, d: Duration) -> Self {
        self.default_wait = d;
        self
    }
}

#[cfg(feature = "chain-rpc")]
impl Wallet {
    /// `IERC20.approve(spender, value)` against `token`. Returns the
    /// receipt once the tx has confirmed. Use this before
    /// `create_batch` / `top_up` so the `PostageStamp` contract has
    /// permission to pull BZZ from the wallet.
    pub async fn approve_bzz(
        &self,
        client: &ChainClient,
        token: &[u8; 20],
        spender: &[u8; 20],
        value: U256,
    ) -> Result<TxReceipt, TxError> {
        self.send_signed(
            client,
            *token,
            erc20_approve_calldata(spender, &value),
            ERC20_APPROVE_GAS,
        )
        .await
    }

    /// `PostageStamp.createBatch(...)`. Returns the receipt; pull the
    /// new batch id with [`extract_created_batch_id`], passing the same
    /// `postage` address.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_batch(
        &self,
        client: &ChainClient,
        postage: &[u8; 20],
        owner: &[u8; 20],
        initial_balance_per_chunk: U256,
        depth: u8,
        bucket_depth: u8,
        nonce: &[u8; 32],
        immutable: bool,
    ) -> Result<TxReceipt, TxError> {
        let data = postage_create_batch_calldata(
            owner,
            &initial_balance_per_chunk,
            depth,
            bucket_depth,
            nonce,
            immutable,
        );
        self.send_signed(client, *postage, data, POSTAGE_CREATE_BATCH_GAS)
            .await
    }

    /// `PostageStamp.topUp(batchId, topupAmountPerChunk)`. Re-fills an
    /// existing batch's per-chunk balance, extending its TTL.
    pub async fn top_up(
        &self,
        client: &ChainClient,
        postage: &[u8; 20],
        batch_id: &[u8; 32],
        topup_per_chunk: U256,
    ) -> Result<TxReceipt, TxError> {
        let data = postage_top_up_calldata(batch_id, &topup_per_chunk);
        self.send_signed(client, *postage, data, POSTAGE_TOP_UP_GAS)
            .await
    }

    /// `PostageStamp.increaseDepth(batchId, newDepth)` (a.k.a "dilute").
    /// Doubles capacity per +1 of `new_depth`; halves the per-chunk
    /// balance because the same total cost now covers twice the
    /// chunks.
    pub async fn increase_depth(
        &self,
        client: &ChainClient,
        postage: &[u8; 20],
        batch_id: &[u8; 32],
        new_depth: u8,
    ) -> Result<TxReceipt, TxError> {
        let data = postage_increase_depth_calldata(batch_id, new_depth);
        self.send_signed(client, *postage, data, POSTAGE_INCREASE_DEPTH_GAS)
            .await
    }

    /// `Chequebook.cashChequeBeneficiary(recipient, cumulativePayout, sig)`.
    ///
    /// Submitter (`self.address`) **must equal** the beneficiary in
    /// the EIP-712 cheque digest, because the chequebook contract
    /// recomputes that digest with `msg.sender` as the beneficiary
    /// before recovering the issuer. Mismatch → `invalid signature`
    /// revert.
    pub async fn cash_cheque_beneficiary(
        &self,
        client: &ChainClient,
        chequebook: &[u8; 20],
        recipient: &[u8; 20],
        cumulative_payout: U256,
        signature: &[u8; 65],
    ) -> Result<TxReceipt, TxError> {
        let data = crate::chequebook::cash_cheque_beneficiary_calldata(
            recipient,
            cumulative_payout,
            signature,
        );
        self.send_signed(client, *chequebook, data, CHEQUEBOOK_CASH_GAS)
            .await
    }

    /// `SimpleSwapFactory.deploySimpleSwap(issuer, defaultHardDepositTimeoutDuration, salt)`.
    /// Returns the receipt; pull the new chequebook address with
    /// [`crate::chequebook::extract_deployed_chequebook`], passing the
    /// same `factory`. The
    /// wallet (`self`) just pays gas — the *issuer* baked into the
    /// chequebook is the `issuer` argument, which can be any EOA
    /// (so the operator can run cold-storage issuer keys without
    /// ever exposing them to a Gnosis RPC node).
    pub async fn deploy_chequebook(
        &self,
        client: &ChainClient,
        factory: &[u8; 20],
        issuer: &[u8; 20],
        default_hard_deposit_timeout: U256,
        salt: &[u8; 32],
    ) -> Result<TxReceipt, TxError> {
        let data = crate::chequebook::deploy_chequebook_calldata(
            issuer,
            default_hard_deposit_timeout,
            salt,
        );
        self.send_signed(client, *factory, data, FACTORY_DEPLOY_CHEQUEBOOK_GAS)
            .await
    }

    /// `IERC20.transfer(to, value)` against `token`. The chequebook
    /// `balance()` view just reads `BZZ.balanceOf(swap)`, so
    /// funding a chequebook is literally a token transfer.
    pub async fn erc20_transfer(
        &self,
        client: &ChainClient,
        token: &[u8; 20],
        to: &[u8; 20],
        value: U256,
    ) -> Result<TxReceipt, TxError> {
        self.send_signed(
            client,
            *token,
            erc20_transfer_calldata(to, &value),
            ERC20_TRANSFER_GAS,
        )
        .await
    }

    /// `SimpleSwap.withdraw(amount)` on `chequebook`: the contract sends
    /// `amount` xBZZ back to its issuer, which must be this wallet (it
    /// reverts otherwise, and above `liquidBalance()`). Use
    /// [`crate::chequebook_store::withdraw_from_chequebook`], which caps
    /// the amount at what issued cheques leave free, rather than this
    /// directly.
    pub async fn withdraw_from_chequebook(
        &self,
        client: &ChainClient,
        chequebook: &[u8; 20],
        amount: U256,
    ) -> Result<TxReceipt, TxError> {
        self.send_signed(
            client,
            *chequebook,
            crate::chequebook::chequebook_withdraw_calldata(amount),
            CHEQUEBOOK_WITHDRAW_GAS,
        )
        .await
    }

    async fn send_signed(
        &self,
        client: &ChainClient,
        to: [u8; 20],
        data: Vec<u8>,
        gas_limit: u64,
    ) -> Result<TxReceipt, TxError> {
        self.send_signed_value(client, to, data, gas_limit, U256::zero())
            .await
    }

    /// Like [`Self::send_signed`] but attaches native xDAI `value` — used
    /// for the swap helper, which is `payable` and wraps the attached
    /// xDAI into WXDAI before swapping.
    ///
    /// Holds [`sender_lock`] for this address from the nonce read until
    /// the receipt, so every transaction this process signs with one key
    /// goes out one after the other.
    async fn send_signed_value(
        &self,
        client: &ChainClient,
        to: [u8; 20],
        data: Vec<u8>,
        gas_limit: u64,
        value: U256,
    ) -> Result<TxReceipt, TxError> {
        let lock = sender_lock(&self.address);
        let _one_at_a_time = lock.lock().await;
        let nonce = client
            .eth_get_transaction_count_pending(&self.address)
            .await?;
        let tx = LegacyTx {
            nonce,
            gas_price_wei: self.default_gas_price_wei,
            gas_limit,
            to,
            value_wei: value,
            data,
            chain_id: self.chain_id,
        };
        let (raw, tx_hash) = sign_legacy_tx(&self.secret, &tx)?;
        // Wait on the hash we computed, never on the one the RPC hands
        // back: a relay answering with some other mined tx's hash would
        // otherwise get that tx's receipt (and its logs: a chequebook
        // or batch that isn't ours) accepted as ours (issue #284).
        //
        // A different answer is not an error by itself: the signed tx
        // is already with the relay and may still be mined. Failing
        // here would report a write that lands as failed, and a retry
        // would spend a second time (a top-up re-reading its shortfall
        // before the first transfer shows) or orphan a deploy. Waiting
        // on our own hash is as safe as failing, since
        // `wait_for_receipt` only accepts a receipt whose
        // `transactionHash` is that hash.
        let answered = client.eth_send_raw_transaction(&raw).await?;
        if answered != tx_hash {
            tracing::warn!(
                target: "ant_chain::tx",
                sent = %format!("0x{}", hex::encode(tx_hash)),
                answered = %format!("0x{}", hex::encode(answered)),
                "eth_sendRawTransaction answered another tx's hash; ignoring it and \
                 waiting on the hash we signed",
            );
        }
        client.wait_for_receipt(&tx_hash, self.default_wait).await
    }

    /// Ensure the `BzzSwapHelper` is deployed and return its (constant,
    /// CREATE2-derived) address. If no bytecode is present yet, deploy it
    /// once through the deterministic deployer — a one-time cost the first
    /// xDAI-funder pays; everyone afterwards reuses the same instance.
    /// The `bool` is whether this call sent that deploy, so a caller
    /// refusing the swap afterwards can say so instead of claiming
    /// nothing was sent.
    pub async fn ensure_swap_helper(
        &self,
        client: &ChainClient,
    ) -> Result<([u8; 20], bool), TxError> {
        let helper = swap_helper_address();
        let code = client
            .eth_get_code(&format!("0x{}", hex::encode(helper)))
            .await?;
        if !code.is_empty() {
            return Ok((helper, false));
        }
        let deployer = decode_addr_const(crate::CREATE2_DEPLOYER);
        self.send_signed(
            client,
            deployer,
            create2_deploy_calldata(),
            SWAP_HELPER_DEPLOY_GAS,
        )
        .await?;
        Ok((helper, true))
    }

    /// Swap `value` wei of native xDAI into xBZZ via the helper,
    /// delivering the xBZZ to `recipient` and reverting unless at least
    /// `amount_out_min` (PLUR) comes out.
    pub async fn swap_xdai_for_bzz(
        &self,
        client: &ChainClient,
        helper: &[u8; 20],
        recipient: &[u8; 20],
        value: U256,
        amount_out_min: U256,
    ) -> Result<TxReceipt, TxError> {
        let data = swap_helper_calldata(recipient, &amount_out_min);
        self.send_signed_value(client, *helper, data, SWAP_HELPER_SWAP_GAS, value)
            .await
    }

    /// Dry-run [`Self::swap_xdai_for_bzz`] with `eth_call` (same sender,
    /// value, calldata and gas limit) without sending anything. `Ok`
    /// when the swap would go through at the latest block; a would-be
    /// revert (`amountOutMin` not met) is an [`crate::RpcError::Rpc`].
    ///
    /// `Ok` also needs the call's `bzzOut` return word, at least
    /// `amount_out_min`. An `eth_call` to an address with no code
    /// succeeds with empty output, so a load-balanced RPC whose backend
    /// doesn't see the just-deployed helper yet would otherwise pass a
    /// swap it never ran. A missing or short answer is an
    /// [`crate::RpcError::Decode`]: the check couldn't be made.
    pub async fn simulate_swap_xdai_for_bzz(
        &self,
        client: &ChainClient,
        helper: &[u8; 20],
        recipient: &[u8; 20],
        value: u128,
        amount_out_min: U256,
    ) -> Result<(), crate::RpcError> {
        let data = swap_helper_calldata(recipient, &amount_out_min);
        let out = client
            .eth_call_value(&self.address, helper, &data, value, SWAP_HELPER_SWAP_GAS)
            .await?;
        check_swap_dry_run(&out, amount_out_min)
    }
}

/// The lock every transaction signed by `address` holds from its nonce
/// read until its receipt.
///
/// The nonce comes from `eth_getTransactionCount(…, "pending")`. Two
/// transactions from one key sent concurrently can read the same value,
/// and the second is then rejected or replaces the first. That happened
/// when a stamp buy overlapped a chequebook deposit, or a second buy.
/// `antd` and `ant-ffi` both send from the node key through several
/// independent paths (gateway routes, after-buy settlement, the C API),
/// so the lock is process-wide and keyed by address rather than held by
/// one `Wallet` value. Waiting for the receipt, not just the broadcast,
/// also keeps a load-balanced RPC whose nodes disagree about the pending
/// count from handing out a nonce twice.
///
/// It is the innermost of the node wallet's locks (after the gateway's
/// `WriteGate` and the embedder's `WalletTxLock`, which guard whole
/// read-then-write sequences rather than one send): nothing is taken
/// while it's held, so it can't deadlock with them. It also covers a
/// send that shares no `WalletTxLock`.
#[cfg(feature = "chain-rpc")]
fn sender_lock(address: &[u8; 20]) -> std::sync::Arc<tokio::sync::Mutex<()>> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock, PoisonError};

    type Locks = Mutex<HashMap<[u8; 20], Arc<tokio::sync::Mutex<()>>>>;
    static LOCKS: OnceLock<Locks> = OnceLock::new();
    LOCKS
        .get_or_init(Locks::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .entry(*address)
        .or_default()
        .clone()
}

#[cfg(any(feature = "chain-rpc", test))]
fn k256_signing_key(
    secret: &[u8; SECP256K1_SECRET_LEN],
) -> Result<k256::ecdsa::SigningKey, TxError> {
    k256::ecdsa::SigningKey::from_bytes(secret.into())
        .map_err(|e| TxError::Decode(format!("signing key from secret: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Selector for `transfer(address,uint256)` — well-known ERC-20
    /// constant `0xa9059cbb`. Catches any keccak-vs-keccak256 mistake
    /// in our calldata builders.
    #[test]
    fn keccak_selector_matches_known() {
        let sel = &keccak256(b"transfer(address,uint256)")[0..4];
        assert_eq!(hex::encode(sel), "a9059cbb");
    }

    /// The embedded `BzzSwapHelper` bytecode must hash to the same value
    /// it had when its CREATE2 address was computed — otherwise the node
    /// would deploy to / look for the wrong address. Pins both the init
    /// code hash and the resulting deterministic address (cross-checked
    /// against a live Gnosis CREATE2-deployer `eth_call`).
    #[test]
    fn swap_helper_address_is_pinned() {
        let init_hash = keccak256(&swap_helper_initcode());
        assert_eq!(
            hex::encode(init_hash),
            "3f1b4a8dd6abc44dbf26a787aa9f7828520af83754f4b04df70bd8ffc2c3ea3a",
            "helper bytecode changed — recompute the CREATE2 address"
        );
        assert_eq!(
            hex::encode(swap_helper_address()),
            "5f346eee2056edfae2a98e6675f63f34702c3f68",
        );
    }

    /// `swapXdaiForBzz(address,uint256)` selector matches the compiled
    /// dispatcher (`0x3b88d7af` in the helper's bytecode).
    #[test]
    fn swap_helper_selector() {
        let calldata = swap_helper_calldata(&[0u8; 20], &U256::zero());
        assert_eq!(hex::encode(&calldata[0..4]), "3b88d7af");
        assert_eq!(calldata.len(), 4 + 32 + 32);
    }

    /// `approve(address,uint256)` known selector `0x095ea7b3`.
    #[test]
    fn erc20_approve_selector() {
        let calldata = erc20_approve_calldata(&[0u8; 20], &U256::zero());
        assert_eq!(hex::encode(&calldata[0..4]), "095ea7b3");
        assert_eq!(calldata.len(), 4 + 32 + 32);
    }

    /// `createBatch(address,uint256,uint8,uint8,bytes32,bool)` —
    /// known selector verified against a live Gnosis `BatchCreated` tx
    /// (`0xc9e741a0…` at `PostageStamp` `0x45a1502382541Cd610CC9068e88727426b696293`):
    /// the tx input begins with `0x5239af71`, the keccak256-derived
    /// 4-byte selector for the canonical signature.
    #[test]
    fn create_batch_selector() {
        let calldata =
            postage_create_batch_calldata(&[0u8; 20], &U256::zero(), 17, 16, &[0u8; 32], true);
        assert_eq!(hex::encode(&calldata[0..4]), "5239af71");
        // 4 selector + 6 words.
        assert_eq!(calldata.len(), 4 + 32 * 6);
    }

    /// `topUp(bytes32,uint256)` known selector — used by bee's tx layer.
    /// Computed manually: keccak256("topUp(bytes32,uint256)")[0..4].
    #[test]
    fn top_up_selector_round_trip() {
        let want = &keccak256(b"topUp(bytes32,uint256)")[0..4];
        let calldata = postage_top_up_calldata(&[0u8; 32], &U256::from(1u64));
        assert_eq!(&calldata[0..4], want);
    }

    /// `increaseDepth(bytes32,uint8)` selector round-trip.
    #[test]
    fn increase_depth_selector_round_trip() {
        let want = &keccak256(b"increaseDepth(bytes32,uint8)")[0..4];
        let calldata = postage_increase_depth_calldata(&[0u8; 32], 18);
        assert_eq!(&calldata[0..4], want);
    }

    /// EIP-155 v computation: given Gnosis chain id 100 and recid 0,
    /// v should be 100 * 2 + 35 + 0 = 235. Validates the math without
    /// requiring a real RPC.
    #[test]
    fn eip155_v_computation() {
        // Build a tiny tx, sign it with a known key, decode v from the
        // signed envelope's RLP. We only verify v lives in the
        // EIP-155 range (235 or 236) — not the exact recid, since
        // that depends on the random k.
        let secret = [0x42u8; SECP256K1_SECRET_LEN];
        let tx = LegacyTx {
            nonce: 0,
            gas_price_wei: DEFAULT_GAS_PRICE_WEI,
            gas_limit: ERC20_APPROVE_GAS,
            to: [0x11u8; 20],
            value_wei: U256::zero(),
            data: vec![],
            chain_id: GNOSIS_CHAIN_ID,
        };
        let (raw, _hash) = sign_legacy_tx(&secret, &tx).unwrap();
        // Decode the outer RLP list and inspect the seventh field (v).
        let list: Vec<rlp::Rlp> = rlp::Rlp::new(&raw).into_iter().collect();
        assert_eq!(list.len(), 9);
        let v: u64 = list[6].as_val().unwrap();
        assert!(
            v == 235 || v == 236,
            "EIP-155 v for Gnosis must be in {{235, 236}}, got {v}",
        );
    }

    /// Round-trip the `BatchCreated` topic computation against a
    /// hex pulled from a live Gnosis `BatchCreated` log so a future
    /// signature change in the contract is caught immediately.
    /// Verified by `eth_getLogs` against `PostageStamp`
    /// `0x45a1502382541Cd610CC9068e88727426b696293` — the topic[0]
    /// of every observed `BatchCreated` event matches.
    #[test]
    fn batch_created_topic_matches_known() {
        let topic = batch_created_event_topic();
        assert_eq!(
            hex::encode(topic),
            "9b088e2c89b322a3c1d81515e1c88db3d386d022926f0e2d0b9b5813b7413d58",
        );
    }

    /// Sign a tx with a known key, then recover the signer from the
    /// outer (raw) RLP. Recovery must yield the same Ethereum address
    /// the wallet derives from the same secret. This exercises the
    /// full happy path of the signing pipeline (RLP, EIP-155 v
    /// compution, recid translation back to {0, 1}) end-to-end
    /// without needing a real RPC.
    #[test]
    fn round_trip_sign_then_recover() {
        let secret = [0x42u8; SECP256K1_SECRET_LEN];
        let want_addr = {
            let sk = k256_signing_key(&secret).unwrap();
            let vk = *sk.verifying_key();
            ant_crypto::ethereum_address_from_public_key(&vk)
        };

        let tx = LegacyTx {
            nonce: 7,
            gas_price_wei: DEFAULT_GAS_PRICE_WEI,
            gas_limit: ERC20_APPROVE_GAS,
            to: [0xaau8; 20],
            value_wei: U256::zero(),
            data: erc20_approve_calldata(&[0xbbu8; 20], &U256::from(123u64)),
            chain_id: GNOSIS_CHAIN_ID,
        };
        let signing_payload = rlp_encode_legacy(&tx, true, None);
        let digest = keccak256(&signing_payload);

        let (raw, _) = sign_legacy_tx(&secret, &tx).unwrap();

        // Decode the v / r / s out of the raw RLP envelope.
        let parsed: Vec<rlp::Rlp> = rlp::Rlp::new(&raw).into_iter().collect();
        assert_eq!(parsed.len(), 9);
        let v: u64 = parsed[6].as_val().unwrap();
        let r: Vec<u8> = parsed[7].as_val().unwrap();
        let s: Vec<u8> = parsed[8].as_val().unwrap();
        // Build a 65-byte sig in the {r, s, v=27|28} shape ant-crypto expects.
        let mut sig = [0u8; 65];
        sig[32 - r.len()..32].copy_from_slice(&r);
        sig[64 - s.len()..64].copy_from_slice(&s);
        // Reverse EIP-155: recid = v - chain_id*2 - 35; then add 27.
        let recid = (v - tx.chain_id * 2 - 35) as u8;
        sig[64] = recid + 27;

        let recovered = ant_crypto::recover_public_key_from_prehash(&sig, &digest).unwrap();
        let recovered_addr = ant_crypto::ethereum_address_from_public_key(&recovered);
        assert_eq!(recovered_addr, want_addr);
    }

    /// `extract_created_batch_id` pulls topic[1] when the topic[0] matches
    /// and the postage contract emitted the log.
    #[test]
    fn extract_batch_id_from_synthetic_receipt() {
        let postage = [0x45; 20];
        let mut topics = vec![batch_created_event_topic()];
        let mut want_batch = [0u8; 32];
        want_batch[0] = 0xab;
        want_batch[31] = 0xcd;
        topics.push(want_batch);
        let receipt = TxReceipt {
            tx_hash: [0u8; 32],
            block_number: 1,
            logs: vec![EventLog {
                address: postage,
                topics,
                data: vec![],
            }],
        };
        assert_eq!(
            extract_created_batch_id(&receipt, &postage),
            Some(want_batch)
        );
    }

    /// Issue #284 (d): a `BatchCreated` log some other contract emitted
    /// in the same tx is not our batch, even ahead of the real one.
    #[test]
    fn batch_id_from_a_non_postage_emitter_is_none() {
        let postage = [0x45; 20];
        let log = |address, batch| EventLog {
            address,
            topics: vec![batch_created_event_topic(), batch],
            data: vec![],
        };
        let decoy = TxReceipt {
            tx_hash: [0u8; 32],
            block_number: 1,
            logs: vec![log([0x66; 20], [0xee; 32])],
        };
        assert_eq!(extract_created_batch_id(&decoy, &postage), None);
        let both = TxReceipt {
            logs: vec![log([0x66; 20], [0xee; 32]), log(postage, [0x11; 32])],
            ..decoy
        };
        assert_eq!(extract_created_batch_id(&both, &postage), Some([0x11; 32]));
    }

    /// Two transactions from one key started at the same moment go out
    /// one after the other: the second reads its nonce only after the
    /// first is mined. Without [`sender_lock`] both read the same pending
    /// nonce, which is how a stamp buy and a chequebook deposit collided.
    #[cfg(feature = "chain-rpc")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_key_sends_one_transaction_at_a_time() {
        use crate::ChainTransport;
        use std::sync::{Arc, Mutex};

        /// Scripted chain: the pending nonce is the number of mined
        /// transactions, and each one is mined on its second receipt
        /// poll, so an unserialised sender has time to read a stale
        /// nonce.
        #[derive(Default)]
        struct Chain {
            events: Mutex<Vec<String>>,
            mined: Mutex<u64>,
            sent: Mutex<u64>,
            polls: Mutex<std::collections::HashMap<String, u32>>,
        }

        impl ChainTransport for Chain {
            fn serve(&self, request_json: &str) -> Option<String> {
                let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
                let id = req["id"].clone();
                let result = match req["method"].as_str().unwrap() {
                    "eth_getTransactionCount" => {
                        let nonce = *self.mined.lock().unwrap();
                        self.events.lock().unwrap().push(format!("nonce {nonce}"));
                        json!(format!("0x{nonce:x}"))
                    }
                    "eth_sendRawTransaction" => {
                        let raw = hex::decode(&req["params"][0].as_str().unwrap()[2..]).unwrap();
                        *self.sent.lock().unwrap() += 1;
                        self.events.lock().unwrap().push("send".into());
                        json!(format!("0x{}", hex::encode(ant_crypto::keccak256(&raw))))
                    }
                    "eth_getTransactionReceipt" => {
                        let hash = req["params"][0].as_str().unwrap().to_string();
                        let mut polls = self.polls.lock().unwrap();
                        let n = polls.entry(hash.clone()).or_default();
                        *n += 1;
                        if *n < 2 {
                            serde_json::Value::Null
                        } else {
                            *self.mined.lock().unwrap() += 1;
                            self.events.lock().unwrap().push("mined".into());
                            json!({"transactionHash": hash, "status": "0x1",
                                "blockNumber": "0x1", "logs": []})
                        }
                    }
                    other => panic!("unscripted method {other}"),
                };
                Some(json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string())
            }
        }

        let chain = Arc::new(Chain::default());
        // An unroutable URL: a fall-through would fail loudly.
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(chain.clone()));
        let a = Wallet::new([9u8; 32], GNOSIS_CHAIN_ID).unwrap();
        let b = Wallet::new([9u8; 32], GNOSIS_CHAIN_ID).unwrap();
        let (ra, rb) = tokio::join!(
            a.approve_bzz(&client, &[1u8; 20], &[2u8; 20], U256::one()),
            b.erc20_transfer(&client, &[1u8; 20], &[3u8; 20], U256::one()),
        );
        ra.unwrap();
        rb.unwrap();

        let events = chain.events.lock().unwrap().clone();
        assert_eq!(
            events,
            ["nonce 0", "send", "mined", "nonce 1", "send", "mined"],
            "the second transaction must read its nonce after the first is mined",
        );
    }
}

/// Receipt waits against a real HTTP stub for the configured RPC URL
/// (issue #253): a transport error on one receipt poll is "no answer
/// yet", never the end of the wait, and never a reason to send again.
#[cfg(all(test, feature = "chain-rpc"))]
mod receipt_wait_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// What the stub does with one request.
    enum Reply {
        /// Answer with this `result`.
        Result(serde_json::Value),
        /// Answer with this JSON-RPC `error`.
        Error(serde_json::Value),
        /// Read the request, then close the connection without a word:
        /// a keep-alive connection the server dropped under the client.
        Drop,
        /// Read the request and never answer.
        Hang,
    }

    /// A scripted JSON-RPC server. `script` gets the request and, for a
    /// receipt poll, how many receipt polls came before it.
    struct Stub {
        url: String,
        sends: Arc<AtomicUsize>,
        polls: Arc<AtomicUsize>,
        methods: Arc<Mutex<Vec<String>>>,
    }

    type Script = dyn Fn(&serde_json::Value, usize) -> Reply + Send + Sync;

    async fn spawn_stub(script: Arc<Script>) -> Stub {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/rpc/secret-key", listener.local_addr().unwrap());
        let sends = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let methods = Arc::new(Mutex::new(Vec::new()));
        let (s, p, m) = (sends.clone(), polls.clone(), methods.clone());
        tokio::spawn(async move {
            let mut hung = Vec::new();
            while let Ok((mut sock, _)) = listener.accept().await {
                let Some(req) = read_request(&mut sock).await else {
                    continue;
                };
                let method = req["method"].as_str().unwrap_or("?").to_string();
                m.lock().unwrap().push(method.clone());
                let reply = match method.as_str() {
                    "eth_sendRawTransaction" => {
                        s.fetch_add(1, Ordering::SeqCst);
                        script(&req, 0)
                    }
                    "eth_getTransactionReceipt" => script(&req, p.fetch_add(1, Ordering::SeqCst)),
                    _ => script(&req, 0),
                };
                let answer = match reply {
                    Reply::Result(result) => {
                        Some(json!({"jsonrpc": "2.0", "id": req["id"], "result": result}))
                    }
                    Reply::Error(error) => {
                        Some(json!({"jsonrpc": "2.0", "id": req["id"], "error": error}))
                    }
                    Reply::Drop => None,
                    Reply::Hang => {
                        hung.push(sock);
                        continue;
                    }
                };
                // `Drop`: the socket closes here, unanswered.
                if let Some(answer) = answer {
                    let body = answer.to_string();
                    let out = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len(),
                    );
                    let _ = sock.write_all(out.as_bytes()).await;
                    let _ = sock.flush().await;
                }
            }
        });
        Stub {
            url,
            sends,
            polls,
            methods,
        }
    }

    async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<serde_json::Value> {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let body_at = loop {
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break i + 4;
            }
        };
        let head = String::from_utf8_lossy(&buf[..body_at]).to_lowercase();
        let len: usize = head
            .split("content-length:")
            .nth(1)
            .and_then(|s| s.split("\r\n").next())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        while buf.len() < body_at + len {
            let n = sock.read(&mut chunk).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        serde_json::from_slice(&buf[body_at..body_at + len]).ok()
    }

    fn mined(req: &serde_json::Value) -> Reply {
        Reply::Result(json!({
            "transactionHash": req["params"][0],
            "status": "0x1",
            "blockNumber": "0x10",
            "logs": [],
        }))
    }

    /// Answers the nonce and send reads a funding step makes before
    /// its receipt wait; `receipt` scripts the polls.
    fn chain(
        receipt: impl Fn(&serde_json::Value, usize) -> Reply + Send + Sync + 'static,
    ) -> Arc<Script> {
        Arc::new(move |req, n| match req["method"].as_str().unwrap() {
            "eth_getTransactionCount" => Reply::Result(json!("0x0")),
            "eth_sendRawTransaction" => {
                let raw = hex::decode(&req["params"][0].as_str().unwrap()[2..]).unwrap();
                Reply::Result(json!(format!("0x{}", hex::encode(keccak256(&raw)))))
            }
            "eth_getTransactionReceipt" => receipt(req, n),
            other => panic!("unscripted method {other}"),
        })
    }

    /// Freedom's keep-alive race (#253): pending, then a dropped
    /// connection, then the receipt. The wait carries on through the
    /// dropped poll and returns the receipt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_receipt_poll_is_not_the_end_of_the_wait() {
        let stub = spawn_stub(chain(|req, n| match n {
            0 => Reply::Result(serde_json::Value::Null),
            1 | 2 => Reply::Drop,
            _ => mined(req),
        }))
        .await;
        let client = ChainClient::new(&stub.url);
        let hash = [0x11u8; 32];
        let receipt = client
            .wait_for_receipt(&hash, Duration::from_secs(30))
            .await
            .unwrap();
        assert_eq!(receipt.tx_hash, hash);
        assert_eq!(receipt.block_number, 0x10);
        assert_eq!(stub.polls.load(Ordering::SeqCst), 4);
    }

    /// A JSON-RPC error answer still fails at once: only a missing
    /// answer is retried.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_json_rpc_error_answer_still_fails_at_once() {
        let stub = spawn_stub(chain(|_, _| {
            Reply::Error(json!({"code": -32601, "message": "nope"}))
        }))
        .await;
        let err = ChainClient::new(&stub.url)
            .wait_for_receipt(&[0x22u8; 32], Duration::from_secs(30))
            .await
            .unwrap_err();
        assert!(matches!(err, TxError::Rpc(RpcError::Rpc(_))), "{err}");
        assert_eq!(stub.polls.load(Ordering::SeqCst), 1);
    }

    /// An RPC that is gone for the whole wait ends it at the deadline
    /// with [`TxError::Timeout`]: it names the hash, says the
    /// transaction may still be mined, and carries the last transport
    /// error without the RPC URL (which can hold an API key).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_rpc_times_out_at_the_deadline() {
        let stub = spawn_stub(chain(|_, _| Reply::Drop)).await;
        let client = ChainClient::new(&stub.url);
        let wait = Duration::from_secs(2);
        let started = std::time::Instant::now();
        let err = client
            .wait_for_receipt(&[0x33u8; 32], wait)
            .await
            .unwrap_err();
        let took = started.elapsed();
        let TxError::Timeout {
            ref tx_hash,
            wait_secs,
            ref last_error,
        } = err
        else {
            panic!("expected a timeout, got {err}");
        };
        assert_eq!(tx_hash, &hex::encode([0x33u8; 32]));
        assert_eq!(wait_secs, 2);
        assert!(last_error.is_some());
        let shown = err.to_string();
        assert!(shown.contains("may still be mined"), "{shown}");
        assert!(!shown.contains("secret-key"), "{shown}");
        assert!(!shown.contains("127.0.0.1"), "{shown}");
        assert!(took >= wait, "{took:?}");
        assert!(took < wait + Duration::from_secs(2), "{took:?}");
        // Polled more than once: the first dropped poll didn't end it.
        assert!(stub.polls.load(Ordering::SeqCst) >= 3);
    }

    /// A poll the RPC never answers is cut off, so the wait still ends
    /// within `wait + RECEIPT_POLL_MIN` (and with it the sender lock
    /// it runs under).
    #[tokio::test(start_paused = true)]
    async fn a_hung_receipt_poll_is_cut_off() {
        let stub = spawn_stub(chain(|_, _| Reply::Hang)).await;
        let client = ChainClient::new(&stub.url);
        let wait = Duration::from_secs(30);
        let started = tokio::time::Instant::now();
        let err = client
            .wait_for_receipt(&[0x44u8; 32], wait)
            .await
            .unwrap_err();
        let took = started.elapsed();
        assert!(matches!(err, TxError::Timeout { .. }), "{err}");
        assert!(took >= wait, "{took:?}");
        assert!(took <= wait + RECEIPT_POLL_MIN, "{took:?}");
    }

    /// Through a real funding step (`top_up_batch`: approve, then
    /// `topUp`): dropped receipt polls under both transactions still
    /// complete the step, and each transaction is sent exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_funding_step_survives_dropped_polls_and_sends_each_tx_once() {
        let stub = spawn_stub(chain(|req, n| {
            // Per transaction: pending, dropped, dropped, mined.
            match n % 4 {
                0 => Reply::Result(serde_json::Value::Null),
                1 | 2 => Reply::Drop,
                _ => mined(req),
            }
        }))
        .await;
        let client = ChainClient::new(&stub.url);
        let wallet = Wallet::new([0x53u8; 32], GNOSIS_CHAIN_ID).unwrap();
        let payer = crate::funding::Payer::gnosis(&client, &wallet);
        crate::funding::top_up_batch(&payer, &[7u8; 32], 17, 1_000)
            .await
            .unwrap();
        assert_eq!(stub.sends.load(Ordering::SeqCst), 2);
        assert_eq!(stub.polls.load(Ordering::SeqCst), 8);
    }

    /// The same step when the receipt never shows (the RPC drops every
    /// poll): it fails at the first transaction with a timeout, after
    /// sending it once, and never sends it again nor goes on to `topUp`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_timed_out_funding_step_never_sends_again() {
        let stub = spawn_stub(chain(|_, _| Reply::Drop)).await;
        let client = ChainClient::new(&stub.url);
        let wallet = Wallet::new([0x54u8; 32], GNOSIS_CHAIN_ID)
            .unwrap()
            .wait_for(Duration::from_secs(2));
        let payer = crate::funding::Payer::gnosis(&client, &wallet);
        let err = crate::funding::top_up_batch(&payer, &[7u8; 32], 17, 1_000)
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                crate::funding::FundingError::Tx {
                    what: "authorise payment",
                    source: TxError::Timeout { .. },
                }
            ),
            "{err}"
        );
        assert_eq!(stub.sends.load(Ordering::SeqCst), 1);
        let methods = stub.methods.lock().unwrap().clone();
        assert_eq!(
            methods
                .iter()
                .filter(|m| m.as_str() != "eth_getTransactionReceipt")
                .collect::<Vec<_>>(),
            ["eth_getTransactionCount", "eth_sendRawTransaction"],
        );
    }
}
