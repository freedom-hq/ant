//! On-chain recovery of node-owned state from the node EOA (PLAN.md
//! "node-owned on-chain state recovery").
//!
//! Two discoveries, both driven purely by the node's public Ethereum
//! address (the `swarm.key` EOA) over JSON-RPC, so a node started on a
//! data dir carried over from bee — same key, no sidecar registry —
//! comes back up with its existing on-chain state intact:
//!
//! 1. [`discover_owned_batches`] — the postage batches this EOA owns
//!    and that are still funded, so they can be re-registered as usable
//!    stamp issuers.
//! 2. [`discover_owned_chequebook`] — the SWAP chequebook this EOA
//!    deployed, so the node adopts it instead of deploying a fresh one
//!    (stranding the old balance).
//!
//! Both reuse a single cheap scan: the ERC-20 `Transfer` event on the
//! xBZZ token, filtered by the indexed `from` topic = node EOA. That
//! filter returns only the node's own outgoing transfers (a tiny set),
//! and every funded chequebook / bought batch was paid for by an
//! ERC-20 transfer *from* the node EOA, so the target addresses are all
//! in the `to` field of that set.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::json;

use crate::chequebook::{chequebook_issuer_selector, factory_deployed_contracts_calldata};
use crate::tx::batch_created_event_topic;
use crate::{ChainClient, RpcError};

/// `keccak256("Transfer(address,address,uint256)")` — topic[0] of the
/// ERC-20 `Transfer(address indexed from, address indexed to, uint256
/// value)` event. Pinned as a literal (a keccak round-trip test guards
/// it) so the scan filter can't silently drift.
pub const ERC20_TRANSFER_TOPIC: [u8; 32] = [
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
];

/// xBZZ token deploy block on Gnosis — the lower bound for the log
/// scan (no node transfer predates the token). Bee's hardcoded mainnet
/// constant.
pub const GNOSIS_XBZZ_DEPLOY_BLOCK: u64 = 16_514_506;

/// Initial scan window. Capable RPCs (e.g. `rpc.gnosischain.com`)
/// answer the full token-to-head range in one call because the indexed
/// `from` filter matches almost nothing; range-capped RPCs cause the
/// scanner to shrink the window adaptively.
const INITIAL_SCAN_CHUNK: u64 = 50_000_000;

/// One decoded `eth_getLogs` entry. Richer than [`crate::tx::EventLog`]
/// (which is receipt-shaped): the scan needs the originating tx hash
/// (to match a `Transfer` against the `BatchCreated` logs mined in the
/// same block) and block number (to aim that per-block query, and to
/// prefer the most-recent chequebook on a tie).
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub address: [u8; 20],
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
    pub tx_hash: [u8; 32],
    pub block_number: u64,
}

/// A postage batch the node EOA owns on-chain and that is still funded.
#[derive(Debug, Clone)]
pub struct DiscoveredBatch {
    pub batch_id: [u8; 32],
    pub depth: u8,
    pub bucket_depth: u8,
    pub immutable: bool,
    /// `PostageStamp.remainingBalance(batchId)` — the per-chunk balance
    /// left (normalised minus outpayment). `> 0` means unexpired.
    pub remaining_balance: u128,
}

impl ChainClient {
    /// `eth_getLogs` for a single contract address over an inclusive
    /// block range, with the given topic filter (already JSON-encoded
    /// as an array of topic strings / nulls).
    pub async fn eth_get_logs(
        &self,
        address: &str,
        topics: &serde_json::Value,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LogEntry>, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1u64,
            "method": "eth_getLogs",
            "params": [{
                "address": address,
                "fromBlock": format!("0x{from_block:x}"),
                "toBlock": format!("0x{to_block:x}"),
                "topics": topics,
            }],
        });
        let v = self.rpc(&body).await?;
        if let Some(err) = crate::rpc_error_json(&v) {
            return Err(RpcError::Rpc(err));
        }
        let arr = v
            .get("result")
            .and_then(|r| r.as_array())
            .ok_or_else(|| RpcError::Rpc("eth_getLogs: missing result array".into()))?;
        arr.iter().map(parse_log_entry).collect()
    }

    /// Scan a contract's logs across `[from_block, to_block]`, shrinking
    /// the window on a range-limit RPC error and growing it again after
    /// a success. Public Gnosis RPCs cap `eth_getLogs` ranges (Alchemy's
    /// free tier at 10 blocks, others at 50 000, etc.); the indexed
    /// `from` filter keeps each window cheap so this stays bounded.
    pub async fn scan_logs(
        &self,
        address: &str,
        topics: &serde_json::Value,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<LogEntry>, RpcError> {
        let mut out = Vec::new();
        let mut start = from_block;
        let mut chunk = INITIAL_SCAN_CHUNK.min(to_block.saturating_sub(from_block).max(1));
        while start <= to_block {
            let end = start.saturating_add(chunk.saturating_sub(1)).min(to_block);
            match self.eth_get_logs(address, topics, start, end).await {
                Ok(mut logs) => {
                    out.append(&mut logs);
                    start = end.saturating_add(1);
                    chunk = chunk.saturating_mul(2).min(INITIAL_SCAN_CHUNK);
                }
                Err(RpcError::Rpc(msg)) if chunk > 1 && is_range_limit_error(&msg) => {
                    chunk = (chunk / 2).max(1);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// `PostageStamp.remainingBalance(bytes32)` — per-chunk balance left
    /// on a batch (`> 0` means unexpired). Selector `0xd71ba7c4`.
    pub async fn postage_remaining_balance(
        &self,
        postage_contract: &str,
        batch_id: &[u8; 32],
    ) -> Result<u128, RpcError> {
        let mut data = String::with_capacity(2 + 8 + 64);
        data.push_str("0xd71ba7c4");
        write!(data, "{}", hex::encode(batch_id)).unwrap();
        let out = self.eth_call(postage_contract, &data).await?;
        last_word_u128(&out)
    }
}

/// Discover every postage batch owned by `node_eoa` that is still
/// funded. Reuses the xBZZ `Transfer(from = node_eoa)` scan: batch
/// buys / top-ups pull BZZ via `transferFrom`, emitting a `Transfer`
/// whose `to` is the `PostageStamp` contract.
///
/// For each block that scan hits, a **single-block `eth_getLogs`** for
/// `BatchCreated` on the `PostageStamp` contract recovers the batch ids,
/// matched back to the `Transfer` by `transactionHash` (issue #77). This
/// deliberately does *not* use `eth_getTransactionReceipt`: verified
/// backends (Myotis) serve receipts near head only, so a receipt hop
/// returns `null` for any older batch and silently loses it — while the
/// log index they *do* carry covers exactly this query. It is also
/// cheaper against a plain RPC (one request per hit block instead of one
/// per hit transaction). `BatchCreated.owner` is not an indexed topic,
/// so an owner-filtered query cannot replace the `Transfer` scan itself;
/// the per-batch owner check below still does that job.
pub async fn discover_owned_batches(
    client: &ChainClient,
    postage_contract: &str,
    xbzz_token: &str,
    node_eoa: &[u8; 20],
    from_block: u64,
) -> Result<Vec<DiscoveredBatch>, RpcError> {
    let postage_addr = parse_addr(postage_contract).ok_or_else(|| {
        RpcError::Decode(format!("bad postage contract address {postage_contract}"))
    })?;
    let to_block = client.eth_block_number().await?;
    let topics = json!([
        format!("0x{}", hex::encode(ERC20_TRANSFER_TOPIC)),
        topic_for_address(node_eoa),
    ]);
    let logs = client
        .scan_logs(xbzz_token, &topics, from_block, to_block)
        .await?;

    // Transactions whose Transfer landed in the PostageStamp contract,
    // grouped by the block they were mined in — one `eth_getLogs` per
    // block covers every hit transaction in it.
    let mut hits: BTreeMap<u64, BTreeSet<[u8; 32]>> = BTreeMap::new();
    for log in &logs {
        if log.topics.len() >= 3 && address_from_topic(&log.topics[2]) == postage_addr {
            hits.entry(log.block_number)
                .or_default()
                .insert(log.tx_hash);
        }
    }

    // Pull the BatchCreated batch ids out of each hit block's logs and
    // keep the ones emitted by our own transactions.
    let bc_topic = batch_created_event_topic();
    let bc_topics = json!([format!("0x{}", hex::encode(bc_topic))]);
    let mut batch_ids: BTreeSet<[u8; 32]> = BTreeSet::new();
    for (block, tx_hashes) in &hits {
        let created = client
            .eth_get_logs(postage_contract, &bc_topics, *block, *block)
            .await?;
        for log in &created {
            if log.address == postage_addr
                && log.topics.first() == Some(&bc_topic)
                && tx_hashes.contains(&log.tx_hash)
            {
                if let Some(id) = log.topics.get(1) {
                    batch_ids.insert(*id);
                }
            }
        }
    }

    // Verify each candidate against current chain state. A read that
    // *failed* is not a confirmed negative: swallowing it (skip on a meta
    // error, `unwrap_or(0)` on the balance) would reclassify an RPC /
    // host-backend hiccup as "not ours" / "drained" and hand back a
    // silently truncated list that a restore flow takes as authoritative.
    // Only a chain answer we could actually read may drop a candidate;
    // anything else fails the whole scan, which every caller already
    // handles (antd logs and starts without rediscovery, the FFI surfaces
    // the error).
    let mut out = Vec::new();
    for id in &batch_ids {
        let meta = crate::fetch_postage_batch_meta(client, postage_contract, id).await?;
        if meta.batch_owner_eth != *node_eoa {
            continue;
        }
        let remaining = client
            .postage_remaining_balance(postage_contract, id)
            .await?;
        if remaining == 0 {
            continue; // confirmed expired / drained
        }
        out.push(DiscoveredBatch {
            batch_id: *id,
            depth: meta.depth,
            bucket_depth: meta.bucket_depth,
            immutable: meta.immutable,
            remaining_balance: remaining,
        });
    }
    Ok(out)
}

/// Discover the SWAP chequebook deployed by `node_eoa`. Reuses the same
/// `Transfer(from = node_eoa)` scan: every funded chequebook was
/// deposited into by the node EOA, so it is among the `to` addresses.
/// Each candidate is verified with `factory.deployedContracts(to)` and
/// `to.issuer() == node_eoa`. On more than one match the most-recently
/// funded chequebook wins.
///
/// `Ok(None)` is authoritative — "every candidate was read, none is
/// ours". A failed read is an `Err`, never an `Ok(None)`: callers treat
/// the empty answer as licence to deploy a new chequebook.
pub async fn discover_owned_chequebook(
    client: &ChainClient,
    factory: &[u8; 20],
    postage_contract: &str,
    xbzz_token: &str,
    node_eoa: &[u8; 20],
    from_block: u64,
) -> Result<Option<[u8; 20]>, RpcError> {
    let postage_addr = parse_addr(postage_contract);
    let xbzz_addr = parse_addr(xbzz_token);
    let to_block = client.eth_block_number().await?;
    let topics = json!([
        format!("0x{}", hex::encode(ERC20_TRANSFER_TOPIC)),
        topic_for_address(node_eoa),
    ]);
    let logs = client
        .scan_logs(xbzz_token, &topics, from_block, to_block)
        .await?;

    // Distinct `to` addresses, keyed by the highest block we saw them
    // funded at (so we can prefer the most-recent on a tie).
    let mut candidates: BTreeMap<[u8; 20], u64> = BTreeMap::new();
    for log in &logs {
        if log.topics.len() < 3 {
            continue;
        }
        let to = address_from_topic(&log.topics[2]);
        if &to == node_eoa || Some(to) == postage_addr || Some(to) == xbzz_addr || to == [0u8; 20] {
            continue;
        }
        let entry = candidates.entry(to).or_insert(0);
        *entry = (*entry).max(log.block_number);
    }

    // Most-recent first.
    let mut ordered: Vec<([u8; 20], u64)> = candidates.into_iter().collect();
    ordered.sort_by_key(|c| std::cmp::Reverse(c.1));

    // Same rule as the batch scan above: a read that *failed* is not a
    // confirmed negative. Swallowing it (`Err(_) => continue`) turns an
    // RPC / host-backend hiccup into "this EOA owns no chequebook", and
    // the caller acts on that by deploying *and funding* a second one —
    // burning gas and stranding the first chequebook's deposit on the
    // strength of chain state we could not read. Only an answer we
    // actually read may drop a candidate; anything else fails the whole
    // scan.
    let factory_hex = format!("0x{}", hex::encode(factory));
    for (cb, _block) in ordered {
        let cb_hex = format!("0x{}", hex::encode(cb));
        // factory.deployedContracts(cb) -> bool
        let dc_data = format!(
            "0x{}",
            hex::encode(factory_deployed_contracts_calldata(&cb))
        );
        let deployed = last_word_nonzero(&client.eth_call(&factory_hex, &dc_data).await?);
        if !deployed {
            continue; // the factory says it never deployed this address
        }
        // cb.issuer() -> address
        let iss_data = format!("0x{}", hex::encode(chequebook_issuer_selector()));
        let ret = client.eth_call(&cb_hex, &iss_data).await?;
        // A short return is an answer we read: the address has no
        // `issuer()` to report, so it is not our chequebook.
        let Some(word) = ret.len().checked_sub(32).and_then(|off| ret.get(off..)) else {
            continue;
        };
        let issuer = address_from_topic(word.try_into().unwrap());
        if &issuer == node_eoa {
            return Ok(Some(cb));
        }
    }
    Ok(None)
}

fn parse_log_entry(v: &serde_json::Value) -> Result<LogEntry, RpcError> {
    let address = parse_addr(
        v.get("address")
            .and_then(|a| a.as_str())
            .ok_or_else(|| RpcError::Decode("log missing address".into()))?,
    )
    .ok_or_else(|| RpcError::Decode("log bad address".into()))?;
    let topics = v
        .get("topics")
        .and_then(|t| t.as_array())
        .ok_or_else(|| RpcError::Decode("log missing topics".into()))?
        .iter()
        .map(|t| {
            let s = t
                .as_str()
                .ok_or_else(|| RpcError::Decode("non-string topic".into()))?;
            let mut out = [0u8; 32];
            hex::decode_to_slice(s.trim_start_matches("0x"), &mut out)
                .map_err(|e| RpcError::Decode(format!("topic: {e}")))?;
            Ok(out)
        })
        .collect::<Result<Vec<_>, RpcError>>()?;
    let data = hex::decode(
        v.get("data")
            .and_then(|d| d.as_str())
            .unwrap_or("0x")
            .trim_start_matches("0x"),
    )
    .map_err(|e| RpcError::Decode(format!("log data: {e}")))?;
    let mut tx_hash = [0u8; 32];
    hex::decode_to_slice(
        v.get("transactionHash")
            .and_then(|t| t.as_str())
            .unwrap_or("0x")
            .trim_start_matches("0x"),
        &mut tx_hash,
    )
    .map_err(|e| RpcError::Decode(format!("transactionHash: {e}")))?;
    // Hard error rather than a 0 default: both callers key off this —
    // the batch scan issues its single-block `BatchCreated` query at
    // exactly this height, and the chequebook scan orders candidates by
    // it. Defaulting a missing/unparseable height to 0 would quietly
    // drop the batch (query block 0, find nothing) instead of surfacing
    // a malformed log.
    let block_number = v
        .get("blockNumber")
        .and_then(|b| b.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .ok_or_else(|| RpcError::Decode("log missing blockNumber".into()))?;
    Ok(LogEntry {
        address,
        topics,
        data,
        tx_hash,
        block_number,
    })
}

/// Heuristic: does this RPC error mean "your block range / result set
/// was too big"? Such errors are recoverable by shrinking the window;
/// anything else (auth, malformed) should propagate immediately.
fn is_range_limit_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    [
        "block range",
        "range",
        "more than",
        "exceed",
        "too large",
        "10000",
        "limit",
        "logs matched",
        "response size",
        "up to a",
        "query timeout",
        "too many results",
    ]
    .iter()
    .any(|needle| m.contains(needle))
}

/// 0x-prefixed 64-hex topic word for a 20-byte address (left-padded).
fn topic_for_address(addr: &[u8; 20]) -> String {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(addr);
    format!("0x{}", hex::encode(w))
}

/// The 20-byte address packed into the low bytes of a 32-byte topic /
/// ABI word.
fn address_from_topic(word: &[u8; 32]) -> [u8; 20] {
    let mut a = [0u8; 20];
    a.copy_from_slice(&word[12..32]);
    a
}

fn parse_addr(s: &str) -> Option<[u8; 20]> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut a = [0u8; 20];
    hex::decode_to_slice(s, &mut a).ok().map(|()| a)
}

/// True when the last 32-byte word of an ABI return has any non-zero
/// byte — the bool-return convention used by `deployedContracts`.
fn last_word_nonzero(ret: &[u8]) -> bool {
    ret.len() >= 32 && ret[ret.len() - 32..].iter().any(|&b| b != 0)
}

fn last_word_u128(ret: &[u8]) -> Result<u128, RpcError> {
    if ret.len() < 32 {
        return Err(RpcError::Decode("ABI return shorter than word".into()));
    }
    let w = &ret[ret.len() - 32..];
    Ok(u128::from_be_bytes(w[16..32].try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::ChainTransport;
    use sha3::{Digest, Keccak256};
    use std::sync::Mutex;

    #[test]
    fn transfer_topic_matches_keccak() {
        let want = Keccak256::digest(b"Transfer(address,address,uint256)");
        assert_eq!(ERC20_TRANSFER_TOPIC, want.as_slice());
    }

    #[test]
    fn remaining_balance_selector() {
        let sel = &Keccak256::digest(b"remainingBalance(bytes32)")[0..4];
        assert_eq!(hex::encode(sel), "d71ba7c4");
    }

    #[test]
    fn topic_address_round_trip() {
        let addr = [0xabu8; 20];
        let topic = topic_for_address(&addr);
        let mut word = [0u8; 32];
        hex::decode_to_slice(topic.trim_start_matches("0x"), &mut word).unwrap();
        assert_eq!(address_from_topic(&word), addr);
        assert_eq!(&word[0..12], &[0u8; 12]);
    }

    #[test]
    fn range_limit_classification() {
        assert!(is_range_limit_error(
            "Under the Free tier plan, you can make eth_getLogs requests with up to a 10 block range"
        ));
        assert!(is_range_limit_error("exceed maximum block range: 50000"));
        assert!(is_range_limit_error(
            "query returned more than 10000 results"
        ));
        assert!(!is_range_limit_error("invalid api key"));
    }

    #[test]
    fn last_word_helpers() {
        let mut w = [0u8; 32];
        w[31] = 1;
        assert!(last_word_nonzero(&w));
        assert!(!last_word_nonzero(&[0u8; 32]));
        let mut bal = [0u8; 32];
        bal[28..32].copy_from_slice(&0x3a21_096fu32.to_be_bytes());
        assert_eq!(last_word_u128(&bal).unwrap(), 0x3a21_096f);
    }

    /// A log entry with no `blockNumber` is unusable by both scans (the
    /// batch scan aims a per-block query at it, the chequebook scan
    /// orders by it), so it must be a decode error rather than a
    /// silently-dropped batch at block 0.
    #[test]
    fn log_without_block_number_is_a_decode_error() {
        let v = json!({
            "address": "0x45a1502382541cd610cc9068e88727426b696293",
            "topics": [],
            "data": "0x",
            "transactionHash": format!("0x{}", "11".repeat(32)),
        });
        let err = parse_log_entry(&v).unwrap_err().to_string();
        assert!(err.contains("blockNumber"), "got {err}");
    }

    // --- batch-discovery query plan (issue #77) ---

    const NODE_EOA: [u8; 20] = [0xa1; 20];
    /// Block the node's `Transfer` into `PostageStamp` was mined in.
    const HIT_BLOCK: u64 = 0x1dd_8ac8;
    const OUR_TX: [u8; 32] = [0x77; 32];
    const OTHER_TX: [u8; 32] = [0x99; 32];
    const OUR_BATCH: [u8; 32] = [0xbb; 32];
    const OTHER_BATCH: [u8; 32] = [0xcc; 32];

    /// Scripted chain backend, plugged in through the host-transport
    /// seam so the query plan is observable: every method ant issues is
    /// recorded, and anything unscripted is a hard failure rather than a
    /// fall-through to a real RPC.
    #[derive(Default)]
    struct ScriptedChain {
        seen: Mutex<Vec<String>>,
        get_logs_params: Mutex<Vec<serde_json::Value>>,
        /// When set, the `eth_call` carrying this selector answers with a
        /// JSON-RPC error instead of a value — a transient read failure.
        fail_selector: Option<&'static str>,
    }

    fn word_hex(bytes: &[u8]) -> String {
        let mut w = [0u8; 32];
        w[32 - bytes.len()..].copy_from_slice(bytes);
        format!("0x{}", hex::encode(w))
    }

    fn log_json(
        address: &str,
        topics: &[String],
        tx_hash: &[u8; 32],
        block: u64,
    ) -> serde_json::Value {
        json!({
            "address": address,
            "topics": topics,
            "data": "0x",
            "transactionHash": format!("0x{}", hex::encode(tx_hash)),
            "blockNumber": format!("0x{block:x}"),
        })
    }

    impl ScriptedChain {
        fn result(&self, method: &str, params: &serde_json::Value) -> serde_json::Value {
            match method {
                "eth_blockNumber" => json!(format!("0x{:x}", HIT_BLOCK + 500)),
                "eth_getLogs" => {
                    self.get_logs_params.lock().unwrap().push(params[0].clone());
                    let filter = &params[0];
                    let address = filter["address"].as_str().unwrap().to_ascii_lowercase();
                    if address == crate::GNOSIS_BZZ_TOKEN.to_ascii_lowercase() {
                        // The xBZZ Transfer(from = node EOA) scan: one
                        // hit, paying the PostageStamp contract.
                        return json!([log_json(
                            crate::GNOSIS_BZZ_TOKEN,
                            &[
                                format!("0x{}", hex::encode(ERC20_TRANSFER_TOPIC)),
                                word_hex(&NODE_EOA),
                                word_hex(&parse_addr(crate::GNOSIS_POSTAGE_STAMP).unwrap()),
                            ],
                            &OUR_TX,
                            HIT_BLOCK,
                        )]);
                    }
                    assert_eq!(address, crate::GNOSIS_POSTAGE_STAMP.to_ascii_lowercase());
                    // Single-block BatchCreated query: our batch plus a
                    // stranger's, mined in the same block.
                    let bc = format!("0x{}", hex::encode(batch_created_event_topic()));
                    json!([
                        log_json(
                            crate::GNOSIS_POSTAGE_STAMP,
                            &[bc.clone(), word_hex(&OUR_BATCH)],
                            &OUR_TX,
                            HIT_BLOCK,
                        ),
                        log_json(
                            crate::GNOSIS_POSTAGE_STAMP,
                            &[bc, word_hex(&OTHER_BATCH)],
                            &OTHER_TX,
                            HIT_BLOCK,
                        ),
                    ])
                }
                "eth_call" => {
                    let data = params[0]["data"].as_str().unwrap();
                    let word = match &data[0..10] {
                        "0x2182ddb1" => word_hex(&NODE_EOA), // batchOwner
                        "0x44beae8e" => word_hex(&[17]),     // batchDepth
                        "0x32ac57dd" => word_hex(&[16]),     // bucketDepth
                        "0xd968f44b" => word_hex(&[1]),      // immutableFlag
                        "0xd71ba7c4" => word_hex(&[42]),     // remainingBalance
                        other => panic!("unscripted eth_call selector {other}"),
                    };
                    json!(word)
                }
                other => panic!("unscripted method {other} — the query plan changed"),
            }
        }
    }

    impl ChainTransport for ScriptedChain {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let method = req["method"].as_str().unwrap().to_string();
            if let Some(sel) = self.fail_selector {
                let data = req["params"][0]["data"].as_str().unwrap_or_default();
                if method == "eth_call" && data.starts_with(sel) {
                    self.seen.lock().unwrap().push(method);
                    return Some(
                        json!({
                            "jsonrpc": "2.0",
                            "id": req["id"],
                            // Not the retryable -32000 (that means "I
                            // don't cover this" and falls back to the
                            // URL) — an authoritative backend failure.
                            "error": {"code": -32603, "message": "backend unavailable"},
                        })
                        .to_string(),
                    );
                }
            }
            let result = self.result(&method, &req["params"]);
            self.seen.lock().unwrap().push(method);
            Some(json!({"jsonrpc": "2.0", "id": req["id"], "result": result}).to_string())
        }
    }

    /// Batch discovery recovers the batch id from a **single-block
    /// `eth_getLogs` for `BatchCreated`** matched by `transactionHash`,
    /// and never asks for a transaction receipt — verified backends
    /// serve receipts near head only, so the old receipt hop lost every
    /// older batch (issue #77).
    #[tokio::test]
    async fn batch_discovery_uses_logs_not_receipts() {
        let script = std::sync::Arc::new(ScriptedChain::default());
        // An unroutable URL: any fall-through would fail the discovery
        // instead of silently answering from a real RPC.
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()));

        let found = discover_owned_batches(
            &client,
            crate::GNOSIS_POSTAGE_STAMP,
            crate::GNOSIS_BZZ_TOKEN,
            &NODE_EOA,
            GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
        .unwrap();

        assert_eq!(found.len(), 1, "exactly our batch, not the stranger's");
        assert_eq!(found[0].batch_id, OUR_BATCH);
        assert_eq!(found[0].depth, 17);
        assert_eq!(found[0].bucket_depth, 16);
        assert!(found[0].immutable);
        assert_eq!(found[0].remaining_balance, 42);

        let seen = script.seen.lock().unwrap().clone();
        assert!(
            !seen.iter().any(|m| m == "eth_getTransactionReceipt"),
            "the receipt hop must be gone: {seen:?}",
        );

        // The BatchCreated query is scoped to the single block the
        // Transfer landed in, and filtered on the event topic.
        let params = script.get_logs_params.lock().unwrap().clone();
        let bc = params
            .iter()
            .find(|p| {
                p["address"]
                    .as_str()
                    .unwrap()
                    .eq_ignore_ascii_case(crate::GNOSIS_POSTAGE_STAMP)
            })
            .expect("a BatchCreated query on the PostageStamp contract");
        assert_eq!(bc["fromBlock"], format!("0x{HIT_BLOCK:x}"));
        assert_eq!(bc["toBlock"], format!("0x{HIT_BLOCK:x}"));
        assert_eq!(
            bc["topics"],
            json!([format!("0x{}", hex::encode(batch_created_event_topic()))]),
        );
    }

    async fn discover_with_failing_call(
        selector: &'static str,
    ) -> Result<Vec<DiscoveredBatch>, RpcError> {
        let script = std::sync::Arc::new(ScriptedChain {
            fail_selector: Some(selector),
            ..ScriptedChain::default()
        });
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script));
        discover_owned_batches(
            &client,
            crate::GNOSIS_POSTAGE_STAMP,
            crate::GNOSIS_BZZ_TOKEN,
            &NODE_EOA,
            GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
    }

    /// A failed `remainingBalance` read is not a drained batch: the scan
    /// must fail loudly rather than reclassify an RPC / host-backend
    /// hiccup as "expired" and hand a restore flow a list missing a
    /// funded batch.
    #[tokio::test]
    async fn failed_balance_read_is_not_a_drained_batch() {
        let err = discover_with_failing_call("0xd71ba7c4")
            .await
            .expect_err("a failed balance read must not read as remaining = 0");
        assert!(err.to_string().contains("backend unavailable"), "got {err}");
    }

    /// Same for the per-batch meta read: an unreadable `batchOwner` is
    /// not "someone else's batch".
    #[tokio::test]
    async fn failed_meta_read_is_not_a_foreign_batch() {
        let err = discover_with_failing_call("0x2182ddb1")
            .await
            .expect_err("a failed owner read must not read as not-ours");
        assert!(err.to_string().contains("backend unavailable"), "got {err}");
    }

    // --- chequebook discovery: a failed read is not "no chequebook" ---

    /// The one chequebook the node EOA funded, and so the only `to`
    /// address the scan below has to verify.
    const OUR_CHEQUEBOOK: [u8; 20] = [0xcb; 20];

    fn selector_hex(sel: &[u8]) -> String {
        format!("0x{}", hex::encode(&sel[0..4]))
    }

    fn deployed_contracts_selector() -> String {
        selector_hex(&factory_deployed_contracts_calldata(&OUR_CHEQUEBOOK))
    }

    fn issuer_selector() -> String {
        selector_hex(&chequebook_issuer_selector())
    }

    /// Scripted backend for the chequebook scan: one xBZZ `Transfer`
    /// from the node EOA into `OUR_CHEQUEBOOK`, which then verifies as
    /// factory-deployed and issued by the node EOA — unless the
    /// `eth_call` carrying `fail_selector` answers with a transient
    /// backend error instead.
    struct ScriptedChequebookChain {
        seen: Mutex<Vec<String>>,
        fail_selector: Option<String>,
    }

    impl ChainTransport for ScriptedChequebookChain {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let method = req["method"].as_str().unwrap().to_string();
            self.seen.lock().unwrap().push(method.clone());
            let result = match method.as_str() {
                "eth_blockNumber" => json!(format!("0x{:x}", HIT_BLOCK + 500)),
                "eth_getLogs" => json!([log_json(
                    crate::GNOSIS_BZZ_TOKEN,
                    &[
                        format!("0x{}", hex::encode(ERC20_TRANSFER_TOPIC)),
                        word_hex(&NODE_EOA),
                        word_hex(&OUR_CHEQUEBOOK),
                    ],
                    &OUR_TX,
                    HIT_BLOCK,
                )]),
                "eth_call" => {
                    let data = req["params"][0]["data"].as_str().unwrap();
                    if self
                        .fail_selector
                        .as_ref()
                        .is_some_and(|sel| data.starts_with(sel.as_str()))
                    {
                        return Some(
                            json!({
                                "jsonrpc": "2.0",
                                "id": req["id"],
                                // Deliberately not the retryable -32000:
                                // that means "I don't cover this" and
                                // falls back to the URL.
                                "error": {"code": -32603, "message": "backend unavailable"},
                            })
                            .to_string(),
                        );
                    }
                    if data.starts_with(&deployed_contracts_selector()) {
                        json!(word_hex(&[1])) // factory deployed it
                    } else if data.starts_with(&issuer_selector()) {
                        json!(word_hex(&NODE_EOA))
                    } else {
                        panic!("unscripted eth_call data {data}")
                    }
                }
                other => panic!("unscripted method {other} — the query plan changed"),
            };
            Some(json!({"jsonrpc": "2.0", "id": req["id"], "result": result}).to_string())
        }
    }

    async fn discover_chequebook_with_failing_call(
        fail_selector: Option<String>,
    ) -> Result<Option<[u8; 20]>, RpcError> {
        let script = std::sync::Arc::new(ScriptedChequebookChain {
            seen: Mutex::new(Vec::new()),
            fail_selector,
        });
        // An unroutable URL: any fall-through would fail the scan rather
        // than quietly answer from a real RPC.
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script));
        discover_owned_chequebook(
            &client,
            &crate::chequebook::GNOSIS_CHEQUEBOOK_FACTORY,
            crate::GNOSIS_POSTAGE_STAMP,
            crate::GNOSIS_BZZ_TOKEN,
            &NODE_EOA,
            GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
    }

    /// With every read answered, the scan finds the chequebook — the
    /// control for the two failure tests below.
    #[tokio::test]
    async fn chequebook_scan_finds_the_node_owned_chequebook() {
        let found = discover_chequebook_with_failing_call(None).await.unwrap();
        assert_eq!(found, Some(OUR_CHEQUEBOOK));
    }

    /// A failed `deployedContracts` read is not "the factory never
    /// deployed this": `Ok(None)` here reads as "this EOA owns no
    /// chequebook", which the FFI acts on by deploying *and funding* a
    /// second one.
    #[tokio::test]
    async fn failed_deployed_contracts_read_is_not_a_missing_chequebook() {
        let err = discover_chequebook_with_failing_call(Some(deployed_contracts_selector()))
            .await
            .expect_err("a failed deployedContracts read must not read as not-deployed");
        assert!(err.to_string().contains("backend unavailable"), "got {err}");
    }

    /// Same for the `issuer()` read: unreadable is not "someone else's
    /// chequebook".
    #[tokio::test]
    async fn failed_issuer_read_is_not_a_foreign_chequebook() {
        let err = discover_chequebook_with_failing_call(Some(issuer_selector()))
            .await
            .expect_err("a failed issuer read must not read as not-ours");
        assert!(err.to_string().contains("backend unavailable"), "got {err}");
    }
}
