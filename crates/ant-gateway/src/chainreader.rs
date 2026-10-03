//! `ant-chain`-backed [`ant_gateway::ChainReader`] for the gateway's
//! `/wallet`, `/chequebook/*`, `/status`, `/chainstate` endpoints
//! (PLAN.md J.5 A2/A3/D1/D2).
//!
//! Keeps the chain wiring in the binary: `ant-gateway` only sees the
//! trait, so it stays free of `ant-chain` / `reqwest`. Built when either
//! an operator RPC or a read-only fallback RPC is configured (see
//! [`build`]); with neither the gateway gets `chain: None` and those
//! endpoints degrade to the bee zero-stub / `501`.

use std::sync::Arc;

use crate::{
    ChainContext, ChainReader, ChainWriter, ChequebookRefusal, ChequebookSlot, DepositView,
    FundingFailure, FundingView, NewBatch, StorageQuoteView, WriteGate,
};
use ant_chain::funding::{self, DepositPolicy, FundingError, Payer};
use ant_chain::tx::Wallet;
use ant_chain::{ChainClient, GNOSIS_BZZ_TOKEN};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use primitive_types::U256;

struct AntChainReader {
    client: ChainClient,
    postage_contract: String,
    bzz_token: String,
}

#[async_trait]
impl ChainReader for AntChainReader {
    async fn block_number(&self) -> Result<u64, String> {
        self.client
            .eth_block_number()
            .await
            .map_err(|e| e.to_string())
    }

    async fn current_price(&self) -> Result<u128, String> {
        self.client
            .postage_last_price(&self.postage_contract)
            .await
            .map_err(|e| e.to_string())
    }

    async fn total_amount(&self) -> Result<u128, String> {
        self.client
            .postage_total_amount(&self.postage_contract)
            .await
            .map_err(|e| e.to_string())
    }

    async fn bzz_balance(&self, who: [u8; 20]) -> Result<u128, String> {
        self.client
            .erc20_balance_of_lower128(&self.bzz_token, &who)
            .await
            .map_err(|e| e.to_string())
    }

    async fn native_balance(&self, who: [u8; 20]) -> Result<u128, String> {
        self.client
            .eth_get_balance_lower128(&who)
            .await
            .map_err(|e| e.to_string())
    }

    async fn chequebook_balance(&self, chequebook: [u8; 20]) -> Result<u128, String> {
        self.client
            .erc20_balance_of_lower128(&self.bzz_token, &chequebook)
            .await
            .map_err(|e| e.to_string())
    }

    async fn chequebook_total_paid_out(&self, chequebook: [u8; 20]) -> Result<u128, String> {
        let selector = ant_chain::chequebook::chequebook_total_paid_out_selector();
        let out = self
            .client
            .eth_call(
                &format!("0x{}", hex::encode(chequebook)),
                &format!("0x{}", hex::encode(selector)),
            )
            .await
            .map_err(|e| e.to_string())?;
        let word = out
            .get(..32)
            .ok_or_else(|| format!("totalPaidOut returned {} bytes", out.len()))?;
        if word[..16].iter().any(|&b| b != 0) {
            return Err("totalPaidOut overflows u128".into());
        }
        Ok(u128::from_be_bytes(
            word[16..].try_into().expect("16 bytes"),
        ))
    }

    async fn batch_remaining_balance(&self, batch_id: [u8; 32]) -> Result<u128, String> {
        self.client
            .postage_remaining_balance(&self.postage_contract, &batch_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn batch_owner(&self, batch_id: [u8; 32]) -> Result<[u8; 20], String> {
        ant_chain::fetch_postage_batch_owner(&self.client, &self.postage_contract, &batch_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn batch_meta(&self, batch_id: [u8; 32]) -> Result<crate::BatchMetaView, String> {
        let meta =
            ant_chain::fetch_postage_batch_meta(&self.client, &self.postage_contract, &batch_id)
                .await
                .map_err(|e| e.to_string())?;
        Ok(crate::BatchMetaView {
            owner: meta.batch_owner_eth,
            depth: meta.depth,
            bucket_depth: meta.bucket_depth,
            immutable: meta.immutable,
        })
    }
}

/// `ant-chain::Wallet`-backed [`ChainWriter`] for the on-chain postage /
/// chequebook write endpoints (PLAN.md J.5 B2/B3, D3) and the xDAI
/// storage funding routes. Both run through `ant_chain::funding`, the
/// same helpers `ant-ffi`'s C API calls.
///
/// The node wallet pays, owns the batches it buys (the key `antd` signs
/// stamps with), and issues the chequebook's cheques.
struct AntChainWriter {
    wallet: Wallet,
    client: ChainClient,
    postage_contract: [u8; 20],
    bzz_token: [u8; 20],
    /// Shared with the [`ChainContext`], so a chequebook resolved after
    /// startup can be deposited into without a restart.
    chequebook: ChequebookSlot,
    /// The chequebook deposit the embedder keeps, or `None` when it
    /// doesn't manage one (`antd --no-auto-chequebook` or a manual
    /// `--chequebook`): a plan is then priced and bought alone.
    deposit_target: Option<u128>,
}

impl AntChainWriter {
    fn payer(&self) -> Payer<'_> {
        Payer {
            client: &self.client,
            wallet: &self.wallet,
            postage: self.postage_contract,
        }
    }

    /// A chequebook the chain check disqualified
    /// ([`ChequebookSlot::refused`]) is neither funded nor replaced by a
    /// buy, so a plan is then priced and bought alone.
    fn deposit_policy(&self) -> DepositPolicy {
        match self.deposit_target {
            _ if self.chequebook.refused().is_some() => DepositPolicy::Unmanaged,
            Some(target) => DepositPolicy::Managed {
                chequebook: self.chequebook.get(),
                target,
            },
            None => DepositPolicy::Unmanaged,
        }
    }

    /// A batch's current depth, read from chain. With `resize`, the
    /// batch must also be the node wallet's: `increaseDepth` is
    /// owner-only, so resizing anyone else's batch would pay the swap
    /// and the `topUp` and then revert. A plain extension (`topUp`) is
    /// open to any payer, so it isn't checked.
    async fn batch_depth(&self, batch_id: &[u8; 32], resize: bool) -> Result<u8, FundingFailure> {
        let postage_hex = format!("0x{}", hex::encode(self.postage_contract));
        let meta = ant_chain::fetch_postage_batch_meta(&self.client, &postage_hex, batch_id)
            .await
            .map_err(|e| FundingFailure::Chain(format!("read batch: {e}")))?;
        check_batch_owner(meta.batch_owner_eth, self.wallet.address(), resize)?;
        Ok(meta.depth)
    }

    fn deposit_view(&self, status: Option<&funding::DepositStatus>) -> DepositView {
        let target = self
            .deposit_target
            .unwrap_or(ant_chain::chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR);
        DepositView {
            chequebook: status.map(|s| s.chequebook),
            managed: self.deposit_target.is_some(),
            deposited_plur: status.map_or(0, |s| s.deposited_plur),
            target_plur: status.map_or(target, |s| s.target_plur),
            shortfall_plur: status.map_or(0, |s| s.shortfall_plur),
            funding: status.map_or_else(FundingView::default, |s| funding_view(&s.funding)),
        }
    }

    fn deposit_target_or_default(&self) -> u128 {
        self.deposit_target
            .unwrap_or(ant_chain::chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR)
    }
}

/// The owner rule [`AntChainWriter::batch_depth`] applies: a zero owner
/// is a batch the contract doesn't have; a resize needs `node` to own it.
fn check_batch_owner(owner: [u8; 20], node: &[u8; 20], resize: bool) -> Result<(), FundingFailure> {
    if owner == [0u8; 20] {
        return Err(FundingFailure::NotFound("batch not found on chain".into()));
    }
    if resize && &owner != node {
        return Err(FundingFailure::Rejected(format!(
            "batch is owned by 0x{}, not this node (0x{}); only its owner can resize it",
            hex::encode(owner),
            hex::encode(node),
        )));
    }
    Ok(())
}

fn funding_view(f: &funding::Funding) -> FundingView {
    FundingView {
        wallet_bzz: f.wallet_bzz,
        wallet_xdai: f.wallet_xdai,
        bzz_to_acquire: f.bzz_to_acquire,
        swap_input_wei: f.swap_input_wei,
        gas_reserve_wei: f.gas_reserve_wei,
        xdai_required_wei: f.xdai_required_wei,
        xdai_to_send_wei: f.xdai_to_send_wei,
        sufficient: f.sufficient,
    }
}

fn new_batch(b: funding::NewBatch) -> NewBatch {
    NewBatch {
        id: b.id,
        block: b.block,
    }
}

fn quote_view(q: &funding::PlanQuote) -> StorageQuoteView {
    StorageQuoteView {
        depth: q.depth,
        days: q.days,
        amount_per_chunk: q.amount_per_chunk,
        plan_cost_plur: q.plan_cost_plur,
        deposit_due_plur: q.deposit_due_plur,
        funding: funding_view(&q.funding),
    }
}

/// Bad input and a short wallet are the caller's to fix (`400`, with
/// the message the apps show); anything else is the chain's (`502`).
fn failure(e: FundingError) -> FundingFailure {
    match e {
        FundingError::ChequebookRefused {
            chequebook,
            verdict,
        } => FundingFailure::ChequebookRefused {
            chequebook,
            refusal: match verdict {
                ant_chain::chequebook_store::ChequebookVerdict::IssuerMismatch(issuer) => {
                    ChequebookRefusal::IssuerMismatch(issuer)
                }
                _ => ChequebookRefusal::NotRegistered,
            },
            message: e.to_string(),
        },
        FundingError::Invalid(_) | FundingError::InsufficientXdai { .. } => {
            FundingFailure::Rejected(e.to_string())
        }
        _ => FundingFailure::Chain(e.to_string()),
    }
}

#[async_trait]
impl ChainWriter for AntChainWriter {
    async fn buy_batch(
        &self,
        amount_per_chunk: u128,
        depth: u8,
        immutable: bool,
    ) -> Result<NewBatch, String> {
        funding::buy_batch(&self.payer(), amount_per_chunk, depth, immutable)
            .await
            .map(new_batch)
            .map_err(|e| e.to_string())
    }

    async fn topup_batch(&self, batch_id: [u8; 32], amount_per_chunk: u128) -> Result<(), String> {
        // topUp pulls `amount_per_chunk × 2^depth` BZZ via transferFrom, so
        // it needs an allowance just like createBatch. The batch's depth
        // lives on-chain (the gateway doesn't carry it), so read it back to
        // size the approval exactly.
        let postage_hex = format!("0x{}", hex::encode(self.postage_contract));
        let meta = ant_chain::fetch_postage_batch_meta(&self.client, &postage_hex, &batch_id)
            .await
            .map_err(|e| format!("read batch depth: {e}"))?;
        funding::top_up_batch(&self.payer(), &batch_id, meta.depth, amount_per_chunk)
            .await
            .map_err(|e| e.to_string())
    }

    async fn dilute_batch(&self, batch_id: [u8; 32], new_depth: u8) -> Result<(), String> {
        self.wallet
            .increase_depth(&self.client, &self.postage_contract, &batch_id, new_depth)
            .await
            .map(|_| ())
            .map_err(|e| format!("increaseDepth: {e}"))
    }

    async fn deposit_chequebook(&self, amount: u128) -> Result<[u8; 32], String> {
        let cb = self
            .chequebook
            .get()
            .ok_or_else(|| "no chequebook configured to deposit into".to_string())?;
        let receipt = self
            .wallet
            .erc20_transfer(&self.client, &self.bzz_token, &cb, U256::from(amount))
            .await
            .map_err(|e| format!("deposit transfer: {e}"))?;
        Ok(receipt.tx_hash)
    }

    async fn quote_plan(&self, depth: u8, days: u64) -> Result<StorageQuoteView, FundingFailure> {
        funding::quote_plan(&self.payer(), self.deposit_policy(), depth, days)
            .await
            .map(|q| quote_view(&q))
            .map_err(failure)
    }

    async fn quote_extend(
        &self,
        batch_id: [u8; 32],
        new_depth: Option<u8>,
        days: u64,
    ) -> Result<StorageQuoteView, FundingFailure> {
        let depth = self.batch_depth(&batch_id, new_depth.is_some()).await?;
        funding::quote_extend(&self.payer(), &batch_id, depth, new_depth, days)
            .await
            .map(|q| quote_view(&q))
            .map_err(failure)
    }

    async fn buy_with_xdai(
        &self,
        depth: u8,
        amount_per_chunk: u128,
        immutable: bool,
    ) -> Result<NewBatch, FundingFailure> {
        funding::buy_plan_with_xdai(
            &self.payer(),
            self.deposit_policy(),
            depth,
            amount_per_chunk,
            immutable,
        )
        .await
        .map(new_batch)
        .map_err(failure)
    }

    async fn extend_with_xdai(
        &self,
        batch_id: [u8; 32],
        new_depth: Option<u8>,
        amount_per_chunk: u128,
    ) -> Result<u8, FundingFailure> {
        let depth = self.batch_depth(&batch_id, new_depth.is_some()).await?;
        funding::extend_with_xdai(&self.payer(), &batch_id, depth, new_depth, amount_per_chunk)
            .await
            .map_err(failure)?;
        Ok(new_depth.unwrap_or(depth))
    }

    async fn deposit_status(&self) -> Result<DepositView, FundingFailure> {
        let Some(cb) = self.chequebook.get() else {
            return Ok(self.deposit_view(None));
        };
        let status = funding::deposit_status(&self.payer(), &cb, self.deposit_target_or_default())
            .await
            .map_err(failure)?;
        Ok(self.deposit_view(Some(&status)))
    }

    async fn fund_deposit_with_xdai(&self) -> Result<DepositView, FundingFailure> {
        if let Some(cb) = self.chequebook.refused() {
            return Err(FundingFailure::Rejected(format!(
                "chequebook 0x{} failed its on-chain checks, so settlement is off for it; \
                 not depositing into it",
                hex::encode(cb),
            )));
        }
        let cb = self.chequebook.get().ok_or_else(|| {
            FundingFailure::Rejected(
                "this node has no chequebook yet; buying storage creates one".into(),
            )
        })?;
        let status =
            funding::fund_deposit_with_xdai(&self.payer(), &cb, self.deposit_target_or_default())
                .await
                .map_err(failure)?;
        Ok(self.deposit_view(Some(&status)))
    }
}

fn parse_addr(s: &str) -> Result<[u8; 20]> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut a = [0u8; 20];
    hex::decode_to_slice(s, &mut a).map_err(|e| anyhow!("bad address {s}: {e}"))?;
    Ok(a)
}

/// Build the gateway's [`ChainContext`]. `wallet_eth` is the node's own
/// Ethereum address (the key that funds postage + SWAP, matching bee's
/// `/wallet`).
///
/// Reads and writes are sourced separately:
///
/// * **Reads** (`/wallet`, `/chainstate`, `/stamps` `amount`/`batchTTL`)
///   use the operator's explicit `rpc_url` when set, else fall back to
///   `read_fallback_rpc_url` — the always-available public endpoint
///   `antd` already contacts for startup recovery (its `--gnosis-logs-rpc-url`).
///   This is what lets an ultra-light node with no `--gnosis-rpc-url`
///   still report a real postage `batchTTL` from chainstate (issue #21)
///   instead of the long placeholder. Returns `None` only when *neither*
///   RPC is set, so the gateway falls back to the bee zero-stub.
/// * **Writes** (`buy`/`topup`/`dilute`/`deposit`) sign real Gnosis
///   transactions, so the [`AntChainWriter`] is built only from the
///   operator's explicit `rpc_url` plus a funded `wallet_secret` — never
///   the shared public fallback. Absent either, the write endpoints
///   degrade to `501`.
///
/// `deposit_target` is the chequebook deposit the embedder keeps (see
/// `/v0/storage/quote`), or `None` when it doesn't manage one.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build(
    rpc_url: Option<String>,
    read_fallback_rpc_url: Option<String>,
    postage_contract: String,
    wallet_eth: [u8; 20],
    chequebook: Option<[u8; 20]>,
    chain_id: u64,
    wallet_secret: Option<[u8; 32]>,
    deposit_target: Option<u128>,
) -> Option<Arc<ChainContext>> {
    build_with_transport(
        rpc_url,
        read_fallback_rpc_url,
        postage_contract,
        wallet_eth,
        chequebook,
        chain_id,
        wallet_secret,
        deposit_target,
        None,
        crate::WalletTxLock::default(),
    )
}

/// [`build`], but with a host-provided chain transport (issue #77)
/// installed on the reader *and* the writer. The host gets first
/// refusal on every JSON-RPC request the gateway's chain surfaces
/// issue; a can't-serve answer falls through to the URLs above exactly
/// as it would without a transport. `None` is identical to [`build`].
///
/// `chequebook` may be an embedder-owned [`ChequebookSlot`] (cloning
/// shares it), so the embedder can update the address after startup.
///
/// `tx_lock` becomes [`ChainContext::tx_lock`]. An embedder that also
/// sends from the node wallet outside the gateway, and rebuilds the
/// gateway (each start builds a fresh context), passes the one lock its
/// own transactions hold, so every context it builds shares it.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_with_transport(
    rpc_url: Option<String>,
    read_fallback_rpc_url: Option<String>,
    postage_contract: String,
    wallet_eth: [u8; 20],
    chequebook: impl Into<ChequebookSlot>,
    chain_id: u64,
    wallet_secret: Option<[u8; 32]>,
    deposit_target: Option<u128>,
    transport: Option<ant_chain::SharedChainTransport>,
    tx_lock: crate::WalletTxLock,
) -> Option<Arc<ChainContext>> {
    // Treat blank strings as unset so an empty env/config value behaves
    // like an absent one.
    let write_rpc = rpc_url.filter(|s| !s.trim().is_empty());
    let read_fallback = read_fallback_rpc_url.filter(|s| !s.trim().is_empty());
    // Reads prefer the operator's RPC and fall back to the read-only
    // endpoint; with neither there is no chain to read, so the gateway
    // keeps its bee zero-stubs.
    let read_rpc = write_rpc.clone().or(read_fallback)?;
    let reader = AntChainReader {
        client: ChainClient::new(read_rpc).with_transport(transport.clone()),
        postage_contract: postage_contract.clone(),
        bzz_token: GNOSIS_BZZ_TOKEN.to_string(),
    };

    // The writer is optional: it needs the operator's explicit RPC, a
    // funded wallet key, and parseable contract addresses. Any missing
    // piece (or a parse failure) disables writes (endpoints 501) rather
    // than refusing to start the daemon — and crucially keeps a
    // read-only fallback node from silently signing transactions against
    // a shared public RPC.
    let chequebook = chequebook.into();
    let writer: Option<Arc<dyn ChainWriter>> = match (write_rpc, wallet_secret) {
        (Some(rpc), Some(secret)) => {
            let wallet = Wallet::new(secret, chain_id).ok();
            let postage = parse_addr(&postage_contract).ok();
            let bzz = parse_addr(GNOSIS_BZZ_TOKEN).ok();
            match (wallet, postage, bzz) {
                (Some(wallet), Some(postage), Some(bzz)) => Some(Arc::new(AntChainWriter {
                    wallet,
                    client: ChainClient::new(rpc).with_transport(transport),
                    postage_contract: postage,
                    bzz_token: bzz,
                    chequebook: chequebook.clone(),
                    deposit_target,
                })
                    as Arc<dyn ChainWriter>),
                _ => None,
            }
        }
        _ => None,
    };

    Some(Arc::new(ChainContext {
        reader: Arc::new(reader),
        wallet_eth,
        chequebook,
        chain_id,
        writer,
        writes: WriteGate::default(),
        tx_lock,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const RPC: &str = "https://rpc.example/operator";
    const FALLBACK: &str = "https://rpc.gnosischain.com";
    const SECRET: [u8; 32] = [0x11; 32];

    #[test]
    fn no_rpc_at_all_yields_no_chain() {
        assert!(build(
            None,
            None,
            "0xpostage".into(),
            [0; 20],
            None,
            100,
            Some(SECRET),
            None,
        )
        .is_none());
        // Blank strings count as unset.
        assert!(build(
            Some("  ".into()),
            Some(String::new()),
            "0xpostage".into(),
            [0; 20],
            None,
            100,
            Some(SECRET),
            None,
        )
        .is_none());
    }

    #[test]
    fn read_fallback_enables_reads_but_not_writes() {
        // Ultra-light: no operator RPC, only the read-only fallback.
        let ctx = build(
            None,
            Some(FALLBACK.into()),
            "0x45a1502382541Cd610CC9068e88727426b696293".into(),
            [0xAB; 20],
            None,
            100,
            Some(SECRET),
            None,
        )
        .expect("reader built from fallback");
        // Reads are available (so /stamps can compute a real batchTTL)...
        // ...but writes are NOT, even with a wallet secret present.
        assert!(ctx.writer.is_none());
    }

    #[test]
    fn explicit_rpc_with_secret_enables_writes() {
        let ctx = build(
            Some(RPC.into()),
            Some(FALLBACK.into()),
            "0x45a1502382541Cd610CC9068e88727426b696293".into(),
            [0xAB; 20],
            None,
            100,
            Some(SECRET),
            None,
        )
        .expect("context built");
        assert!(ctx.writer.is_some());
    }

    #[test]
    fn explicit_rpc_without_secret_reads_only() {
        let ctx = build(
            Some(RPC.into()),
            None,
            "0x45a1502382541Cd610CC9068e88727426b696293".into(),
            [0xAB; 20],
            None,
            100,
            None,
            None,
        )
        .expect("context built");
        assert!(ctx.writer.is_none());
    }

    /// A chequebook the embedder recorded as refused (the chain check
    /// disqualified it) is neither priced into a new plan nor funded:
    /// both answer without a chain call (the RPC is unroutable).
    #[tokio::test]
    async fn a_refused_chequebook_gets_no_deposit() {
        const CB: [u8; 20] = [0xcb; 20];
        let slot = ChequebookSlot::new(Some(CB));
        slot.refuse(CB);
        let ctx = build_with_transport(
            Some("http://127.0.0.1:1".into()),
            None,
            "0x45a1502382541Cd610CC9068e88727426b696293".into(),
            [0x22; 20],
            slot.clone(),
            100,
            Some(SECRET),
            Some(ant_chain::chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR),
            None,
            crate::WalletTxLock::default(),
        )
        .expect("context built");
        let writer = ctx.writer.clone().expect("writer");
        match writer.fund_deposit_with_xdai().await {
            Err(FundingFailure::Rejected(m)) => {
                assert!(m.contains("failed its on-chain checks"), "{m}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        let status = writer.deposit_status().await.expect("status");
        assert_eq!(status.chequebook, None);
        assert_eq!(status.shortfall_plur, 0);
        assert_eq!(ctx.chequebook.get(), None);

        // A chequebook that checks out lifts it.
        slot.set(CB);
        assert_eq!(slot.refused(), None);
        assert_eq!(ctx.chequebook.get(), Some(CB));
    }

    /// R1-M2: a resize (owner-only `increaseDepth`) is refused up front
    /// for a batch the node wallet doesn't own, before any swap or
    /// `topUp` is paid; a plain extension of it is still allowed.
    #[test]
    fn only_the_owner_may_resize() {
        let node = [0xaa; 20];
        let other = [0xbb; 20];
        assert!(check_batch_owner(node, &node, true).is_ok());
        assert!(check_batch_owner(other, &node, false).is_ok());
        assert!(matches!(
            check_batch_owner(other, &node, true),
            Err(FundingFailure::Rejected(m)) if m.contains("only its owner can resize")
        ));
        for resize in [false, true] {
            assert!(matches!(
                check_batch_owner([0; 20], &node, resize),
                Err(FundingFailure::NotFound(_))
            ));
        }
    }
}
