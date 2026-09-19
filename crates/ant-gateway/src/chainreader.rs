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

use crate::{ChainContext, ChainReader, ChainWriter};
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

    async fn batch_remaining_balance(&self, batch_id: [u8; 32]) -> Result<u128, String> {
        self.client
            .postage_remaining_balance(&self.postage_contract, &batch_id)
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

/// Default postage bucket depth (bee's constant). A batch's `depth` must
/// exceed it; we pass it verbatim to `createBatch`.
const POSTAGE_BUCKET_DEPTH: u8 = 16;

/// `ant-chain::Wallet`-backed [`ChainWriter`] for the on-chain postage /
/// chequebook write endpoints (PLAN.md J.5 B2/B3, D3).
///
/// NOTE: these submit real Gnosis transactions and have not been
/// validated against live mainnet in this build — only the calldata /
/// amount math (covered by `ant-chain`'s unit tests) and the
/// request-plumbing (covered by the gateway's fake-writer tests). Live
/// validation is the remaining step before relying on them.
struct AntChainWriter {
    wallet: Wallet,
    client: ChainClient,
    postage_contract: [u8; 20],
    bzz_token: [u8; 20],
    /// Batch owner baked into `createBatch`. For stamps issued after a
    /// buy to be accepted, this must match the key `antd` signs stamps
    /// with — by default the node wallet itself.
    owner: [u8; 20],
    chequebook: Option<[u8; 20]>,
}

#[async_trait]
impl ChainWriter for AntChainWriter {
    async fn buy_batch(
        &self,
        amount_per_chunk: u128,
        depth: u8,
        immutable: bool,
    ) -> Result<[u8; 32], String> {
        // Total cost to approve = amount-per-chunk × 2^depth.
        let total = U256::from(amount_per_chunk)
            .checked_mul(U256::one() << u32::from(depth))
            .ok_or_else(|| "amount × 2^depth overflows u256".to_string())?;
        self.wallet
            .approve_bzz(&self.client, &self.bzz_token, &self.postage_contract, total)
            .await
            .map_err(|e| format!("approve: {e}"))?;
        let nonce = ant_crypto::random_overlay_nonce();
        let receipt = self
            .wallet
            .create_batch(
                &self.client,
                &self.postage_contract,
                &self.owner,
                U256::from(amount_per_chunk),
                depth,
                POSTAGE_BUCKET_DEPTH,
                &nonce,
                immutable,
            )
            .await
            .map_err(|e| format!("createBatch: {e}"))?;
        ant_chain::tx::extract_created_batch_id(&receipt)
            .ok_or_else(|| "createBatch receipt had no BatchCreated event".to_string())
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
        let total = U256::from(amount_per_chunk)
            .checked_mul(U256::one() << u32::from(meta.depth))
            .ok_or_else(|| "amount × 2^depth overflows u256".to_string())?;
        self.wallet
            .approve_bzz(&self.client, &self.bzz_token, &self.postage_contract, total)
            .await
            .map_err(|e| format!("approve: {e}"))?;
        self.wallet
            .top_up(
                &self.client,
                &self.postage_contract,
                &batch_id,
                U256::from(amount_per_chunk),
            )
            .await
            .map(|_| ())
            .map_err(|e| format!("topUp: {e}"))
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
            .ok_or_else(|| "no chequebook configured to deposit into".to_string())?;
        let receipt = self
            .wallet
            .erc20_transfer(&self.client, &self.bzz_token, &cb, U256::from(amount))
            .await
            .map_err(|e| format!("deposit transfer: {e}"))?;
        Ok(receipt.tx_hash)
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
#[must_use]
pub fn build(
    rpc_url: Option<String>,
    read_fallback_rpc_url: Option<String>,
    postage_contract: String,
    wallet_eth: [u8; 20],
    chequebook: Option<[u8; 20]>,
    chain_id: u64,
    wallet_secret: Option<[u8; 32]>,
) -> Option<Arc<ChainContext>> {
    build_with_transport(
        rpc_url,
        read_fallback_rpc_url,
        postage_contract,
        wallet_eth,
        chequebook,
        chain_id,
        wallet_secret,
        None,
    )
}

/// [`build`], but with a host-provided chain transport (issue #77)
/// installed on the reader *and* the writer. The host gets first
/// refusal on every JSON-RPC request the gateway's chain surfaces
/// issue; a can't-serve answer falls through to the URLs above exactly
/// as it would without a transport. `None` is identical to [`build`].
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_with_transport(
    rpc_url: Option<String>,
    read_fallback_rpc_url: Option<String>,
    postage_contract: String,
    wallet_eth: [u8; 20],
    chequebook: Option<[u8; 20]>,
    chain_id: u64,
    wallet_secret: Option<[u8; 32]>,
    transport: Option<ant_chain::SharedChainTransport>,
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
                    owner: wallet_eth,
                    chequebook,
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
            Some(SECRET)
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
        )
        .expect("context built");
        assert!(ctx.writer.is_none());
    }
}
