//! Postage-stamp read surface: `GET /stamps` and `GET /stamps/{id}`.
//!
//! Freedom's publish pre-flight (`checkSwarmPreFlight`) refuses to
//! publish unless `GET /stamps` lists at least one **usable** batch
//! (PLAN.md J.4 / J.7.2). `antd` keeps a live registry of postage
//! batches — any pre-configured `--postage-batch` plus every batch
//! bought at runtime via `POST /stamps/{amount}/{depth}`. This module
//! enumerates them in bee's `/stamps` JSON shape so an unmodified
//! bee-js / Freedom client sees the usable stamps and lets the publish
//! flow proceed end-to-end.
//!
//! The live counters come from the daemon over the
//! [`ControlCommand::PostageList`] channel (one view per registered
//! batch). Fields the daemon can't know locally without an extra chain
//! read (`amount` per-chunk balance, `batchTTL`) are filled with safe
//! placeholders; see the field comments.
//!
//! On-chain **writes** (`POST /stamps/{amount}/{depth}` to buy,
//! `PATCH /stamps/topup|dilute/...`) live in [`crate::chain`]; a buy
//! registers the new issuer with the running node before returning, so
//! the bought batch shows up here at once. It reads `usable: false`
//! (with `exists: true` and a positive `batchTTL`) for bee's
//! confirmation window, about 70 s, until the storers have synced its
//! creation block; see `BATCH_PROPAGATION` in `ant-p2p`.

use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Serialize;
use tokio::sync::oneshot;

use ant_control::{ControlAck, ControlCommand, PostageStatusView};

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// Bound on how long `GET /stamps` waits for the daemon's postage
/// snapshot. The reply is a synchronous in-memory read of the issuer
/// state on the node loop, so it returns in microseconds in practice;
/// the timeout only exists so a wedged or shutting-down node loop
/// surfaces a clean `503` instead of pinning the hyper task forever
/// (the same failure mode the retrieval handlers guard against).
const POSTAGE_STATUS_TIMEOUT: Duration = Duration::from_secs(10);

/// Placeholder TTL (seconds) emitted for the configured batch until a
/// real `batchTTL` chain read lands. The operator configured a genuine
/// on-chain batch, so reporting a long life keeps bee-js / Freedom from
/// treating it as expired; bee uses `-1` only for batches it can't find
/// on chain, which would make Freedom hide the batch. ~10 years.
const PLACEHOLDER_BATCH_TTL_SECS: i64 = 315_360_000;

/// One entry of bee's `GET /stamps.stamps[]`. Field names + value types
/// match bee's `postage.StampIssuer` JSON exactly (bee-js indexes by
/// these strings and crashes on a wrong type, PLAN.md J.4.2).
#[derive(Debug, Serialize)]
struct StampEntry {
    #[serde(rename = "batchID")]
    batch_id: String,
    utilization: u32,
    usable: bool,
    /// Ant-specific, not in bee's shape (bee-js ignores it): `true`
    /// while `usable` is `false` only because a batch this node just
    /// bought is still propagating to the storers (bee's confirmation
    /// window, about 70 s). Uploads with it are accepted and wait the
    /// window out. `usable: false` with `propagating: false` means the
    /// batch won't become usable by waiting: peers rejected it, or the
    /// chain says it's gone or expired.
    propagating: bool,
    label: String,
    depth: u8,
    /// Per-chunk balance as a string integer (PLUR). Not known locally
    /// without a chain read; `"0"` is a safe placeholder — bee-js's
    /// `size` getter only needs `depth`, and Freedom's pre-flight only
    /// gates on `usable`.
    amount: String,
    #[serde(rename = "bucketDepth")]
    bucket_depth: u8,
    #[serde(rename = "blockNumber")]
    block_number: u64,
    #[serde(rename = "immutableFlag")]
    immutable_flag: bool,
    exists: bool,
    #[serde(rename = "batchTTL")]
    batch_ttl: i64,
    /// Seconds since the node registered this batch at runtime (see
    /// [`PostageStatusView::registered_secs_ago`]). Not part of bee's
    /// shape; only used to gate [`Self::mark_not_on_chain`].
    #[serde(skip)]
    registered_secs_ago: Option<u64>,
}

#[derive(Debug, Serialize)]
struct StampsBody {
    stamps: Vec<StampEntry>,
}

impl StampEntry {
    /// Map a daemon postage snapshot to bee's stamp shape. Returns
    /// `None` when uploads are disabled (no batch configured) so the
    /// caller emits an empty `stamps` list, matching bee on a
    /// freshly-started node.
    fn from_view(view: &PostageStatusView) -> Option<Self> {
        if !view.enabled {
            return None;
        }
        // bee reports `batchID` as bare lowercase hex (no `0x`); the
        // daemon snapshot carries it `0x`-prefixed.
        let batch_id = view
            .batch_id
            .strip_prefix("0x")
            .or_else(|| view.batch_id.strip_prefix("0X"))
            .unwrap_or(&view.batch_id)
            .to_ascii_lowercase();
        Some(Self {
            batch_id,
            // bee's `utilization` is the fill of the fullest bucket.
            utilization: view.bucket_fill_max,
            // Peer-attested: flips false once storer peers reject the
            // batch's stamps as not-on-chain (phantom batch) — the
            // node-side registry can't vouch for chain presence by
            // itself (ant has no batchstore), so a batch is reported
            // usable only while the network isn't refuting it.
            usable: view.usable,
            propagating: view.propagating,
            label: String::new(),
            depth: view.batch_depth,
            amount: "0".to_string(),
            bucket_depth: view.bucket_depth,
            // The creation block, when the node bought the batch
            // itself; `0` (bee's "unknown") otherwise.
            block_number: view.block_number.unwrap_or(0),
            immutable_flag: view.immutable,
            exists: true,
            batch_ttl: PLACEHOLDER_BATCH_TTL_SECS,
            registered_secs_ago: view.registered_secs_ago,
        })
    }

    /// The chain confirms this batch does not exist — never created, or
    /// expired and since evicted (`expireLimited`). Report it the way bee does
    /// a batch missing from its batchstore (`exists: false`,
    /// `batchTTL: -1`) and not `usable`, so a client learns it is dead
    /// from the listing rather than from a peer rejecting its first
    /// push. (`ant-ffi` also unregisters such a batch once it has an
    /// RPC; this covers the window before that, and a batch that
    /// expires while the node runs.)
    fn mark_not_on_chain(&mut self) {
        self.exists = false;
        self.usable = false;
        self.propagating = false;
        self.batch_ttl = -1;
    }

    /// Whether a "not on chain" read may be believed for this batch.
    /// A batch the node registered at runtime (just bought / connected)
    /// is exempt for [`FRESH_BATCH_GRACE`]: the buy was confirmed by
    /// one RPC backend, and a load-balanced sibling that hasn't seen
    /// the `BatchCreated` block yet reverts `remainingBalance` and
    /// reads `batchOwner` as zero for it — exactly the not-on-chain
    /// signature. Batches reloaded from disk have no registration age
    /// and are checked immediately.
    fn may_be_not_on_chain(&self) -> bool {
        self.registered_secs_ago
            .is_none_or(|age| age >= FRESH_BATCH_GRACE.as_secs())
    }

    /// Still on chain but `remainingBalance` reads `0`: expired, just
    /// not evicted yet (the contract keeps `batchOwner` until someone
    /// calls `expireLimited`). Storers already reject its stamps and it
    /// can't be topped up, so it is not `usable`; bee reports such a
    /// batch with `batchTTL: 0` while it still `exists`.
    fn mark_expired(&mut self) {
        self.usable = false;
        self.propagating = false;
        self.batch_ttl = 0;
    }
}

/// Fetch every registered postage batch over the control channel.
/// Returns `Err(response)` already shaped as an HTTP error when the
/// node loop is gone or replies unexpectedly.
#[allow(clippy::result_large_err)]
async fn fetch_postage_list(handle: &GatewayHandle) -> Result<Vec<PostageStatusView>, Response> {
    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    if handle
        .commands
        .send(ControlCommand::PostageList { ack: ack_tx })
        .await
        .is_err()
    {
        return Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop is no longer accepting commands",
        ));
    }
    match tokio::time::timeout(POSTAGE_STATUS_TIMEOUT, ack_rx).await {
        Ok(Ok(ControlAck::PostageList(views))) => Ok(views),
        Ok(Ok(other)) => Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unexpected node ack for postage list: {other:?}"),
        )),
        Ok(Err(_)) => Err(json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop dropped the postage list request",
        )),
        Err(_) => Err(json_error(
            StatusCode::GATEWAY_TIMEOUT,
            "postage list request timed out",
        )),
    }
}

/// `GET /stamps`. Lists every registered batch (those bought at runtime
/// plus any pre-configured one), bee-shaped — or an empty list when
/// none are registered.
pub async fn stamps(State(handle): State<GatewayHandle>) -> Response {
    let views = match fetch_postage_list(&handle).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let mut stamps: Vec<StampEntry> = views.iter().filter_map(StampEntry::from_view).collect();
    enrich_with_chain(&mut stamps, &handle).await;
    json_ok(&StampsBody { stamps })
}

/// Gnosis block time (seconds). Used to convert a batch's remaining
/// per-chunk balance (denominated in price-per-block units) into a TTL,
/// matching bee's `estimateBatchTTL` (`((value-totalAmount)/price) *
/// blockTime`).
const GNOSIS_BLOCK_TIME_SECS: u128 = 5;

/// bee's `batchTTL` (seconds) from a batch's remaining per-chunk balance
/// and the current storage price. `remaining` is
/// `PostageStamp.remainingBalance(id)` — already `normalisedBalance −
/// currentTotalOutPayment`, clamped to `0` on chain for an expired batch
/// — so the only edge cases left are this layer's:
///
/// * `price == 0` → `-1` ("never expires"), matching bee's semantics for
///   a chain that hasn't priced storage; also dodges divide-by-zero.
/// * `remaining == 0` (drained / expired) → `0`.
/// * an astronomically long life is clamped to `i64::MAX` rather than
///   wrapping negative.
fn batch_ttl_secs(remaining: u128, price: u128) -> i64 {
    match remaining
        .saturating_mul(GNOSIS_BLOCK_TIME_SECS)
        .checked_div(price)
    {
        Some(ttl) => i64::try_from(ttl).unwrap_or(i64::MAX),
        None => -1, // price == 0 → never expires (bee semantics)
    }
}

/// How long after a runtime registration (buy / connect) `/stamps`
/// keeps trusting the node over a chain read that says the batch
/// doesn't exist. RPC backends behind a load balancer lag each other
/// by a few blocks (5 s each on Gnosis); five minutes is far past that
/// while still surfacing a genuinely vanished batch promptly.
const FRESH_BATCH_GRACE: Duration = Duration::from_secs(300);

/// How long the per-batch chain enrichment may take before `/stamps`
/// gives up and returns the placeholder `amount` / `batchTTL`. The
/// listing must stay responsive even if the RPC is slow.
const STAMPS_ENRICH_TIMEOUT: Duration = Duration::from_secs(8);

/// Fill in bee's `amount` (the batch's normalised per-chunk balance)
/// and `batchTTL` from the chain when a chain context is configured.
/// Best-effort: any RPC failure / absent chain leaves the placeholders
/// (`amount = "0"`, a long TTL), which already keep bee-js / Freedom
/// happy. `amount = remainingBalance + totalOutPayment` and
/// `batchTTL = (remainingBalance / currentPrice) * blockTime`, matching
/// bee.
///
/// A batch whose balance read fails *and* whose `batchOwner` reads as
/// the zero address is one the chain doesn't hold: it is marked
/// [`StampEntry::mark_not_on_chain`] — unless the node registered it
/// within [`FRESH_BATCH_GRACE`], where a lagging RPC backend is the
/// likelier explanation. A balance that reads `0` is an
/// expired batch ([`StampEntry::mark_expired`]). Any other failure keeps
/// the placeholders — an RPC hiccup must not make a funded batch look
/// dead.
async fn enrich_with_chain(stamps: &mut [StampEntry], handle: &GatewayHandle) {
    let Some(chain) = handle.chain() else {
        return;
    };
    if stamps.is_empty() {
        return;
    }
    let _ = tokio::time::timeout(
        STAMPS_ENRICH_TIMEOUT,
        enrich_entries(stamps, chain.reader.as_ref()),
    )
    .await;
}

async fn enrich_entries(stamps: &mut [StampEntry], reader: &dyn crate::ChainReader) {
    let price = reader.current_price().await.unwrap_or(0);
    let total_out = reader.total_amount().await.unwrap_or(0);
    for entry in stamps.iter_mut() {
        let mut id = [0u8; 32];
        if hex::decode_to_slice(&entry.batch_id, &mut id).is_err() {
            continue;
        }
        let Ok(remaining) = reader.batch_remaining_balance(id).await else {
            // `remainingBalance` reverts for a batch the contract no
            // longer holds; confirm with the owner view before calling
            // it dead. One `batchOwner` call, not the four-view
            // `batch_meta`: this runs under the shared enrichment timeout.
            // A just-bought batch is exempt (lagging RPC backend; see
            // `may_be_not_on_chain`).
            if entry.may_be_not_on_chain()
                && reader
                    .batch_owner(id)
                    .await
                    .is_ok_and(|owner| owner == [0u8; 20])
            {
                entry.mark_not_on_chain();
            }
            continue;
        };
        // Normalised per-chunk balance bee reports as `amount`.
        entry.amount = remaining.saturating_add(total_out).to_string();
        entry.batch_ttl = batch_ttl_secs(remaining, price);
        if remaining == 0 {
            entry.mark_expired();
        }
    }
}

/// `GET /stamps/{id}`. Returns the registered batch whose id matches,
/// else `404` (bee's "batch not found"). Used by bee-js's
/// extension-cost math (PLAN.md J.2.2).
pub async fn stamp(State(handle): State<GatewayHandle>, Path(id): Path<String>) -> Response {
    let views = match fetch_postage_list(&handle).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let want = id
        .strip_prefix("0x")
        .or_else(|| id.strip_prefix("0X"))
        .unwrap_or(&id)
        .to_ascii_lowercase();
    match views
        .iter()
        .filter_map(StampEntry::from_view)
        .find(|e| e.batch_id == want)
    {
        Some(entry) => {
            let mut one = [entry];
            enrich_with_chain(&mut one, &handle).await;
            let [entry] = one;
            json_ok(&entry)
        }
        None => json_error(StatusCode::NOT_FOUND, "batch not found"),
    }
}

/// Bee's `postageBatchResponse` (`GET /batches`, `GET /batches/{id}`),
/// field order as declared in `bee/pkg/api/postage.go`. `value` is
/// bee's `bigint.BigInt`, which marshals as a decimal string.
#[derive(Debug, Serialize)]
struct BatchEntry {
    #[serde(rename = "batchID")]
    batch_id: String,
    value: String,
    start: u64,
    owner: String,
    depth: u8,
    #[serde(rename = "bucketDepth")]
    bucket_depth: u8,
    immutable: bool,
    #[serde(rename = "batchTTL")]
    batch_ttl: i64,
}

/// `GET /batches` — bee lists **every** batch on the network from its
/// chain-event-synced batchstore. Ant deliberately has no postage
/// event sync (all chain interaction is direct RPC reads), so the
/// global enumeration is not implementable without importing bee's
/// whole listener/batchstore subsystem; ant honestly returns bee's
/// shape with an empty list rather than fabricating entries. Single
/// batches remain individually addressable via `GET /batches/{id}`
/// (below), which *is* answerable from direct contract reads.
pub async fn batches(State(_handle): State<GatewayHandle>) -> Response {
    json_ok(&serde_json::json!({ "batches": [] }))
}

/// `GET /batches/{id}` — light single-batch lookup: bee reads its
/// synced batchstore; ant reads the `PostageStamp` contract views over
/// the configured RPC (`batchOwner` / `batchDepth` / `batchBucketDepth`
/// / `batchImmutableFlag` / `remainingBalance`). Differences from bee,
/// by design (no event sync):
///
/// * `start` (creation block) is not a contract view — reported as `0`.
/// * without a configured RPC every batch is bee's 404
///   `"batch not found"`.
pub async fn batch(State(handle): State<GatewayHandle>, Path(id): Path<String>) -> Response {
    let mut reasons = Vec::new();
    let Some(batch_id) = crate::error::parse_hex_param::<32>("batch_id", &id, &mut reasons) else {
        return crate::error::params_error(crate::error::ParamKind::Path, reasons);
    };
    let Some(chain) = handle.chain() else {
        if handle.chain_state().is_none() {
            return crate::error::chain_initializing();
        }
        return json_error(StatusCode::NOT_FOUND, "batch not found");
    };
    let result = tokio::time::timeout(STAMPS_ENRICH_TIMEOUT, async {
        let meta = chain.reader.batch_meta(batch_id).await?;
        // A never-created batch reads as all-zero views on chain; bee's
        // batchstore would return storage.ErrNotFound → 404.
        if meta.owner == [0u8; 20] {
            return Err("batch not found".to_string());
        }
        let remaining = chain.reader.batch_remaining_balance(batch_id).await?;
        let total_out = chain.reader.total_amount().await.unwrap_or(0);
        let price = chain.reader.current_price().await.unwrap_or(0);
        Ok::<_, String>(BatchEntry {
            batch_id: hex::encode(batch_id),
            value: remaining.saturating_add(total_out).to_string(),
            start: 0,
            owner: hex::encode(meta.owner),
            depth: meta.depth,
            bucket_depth: meta.bucket_depth,
            immutable: meta.immutable,
            batch_ttl: batch_ttl_secs(remaining, price),
        })
    })
    .await;
    match result {
        Ok(Ok(entry)) => json_ok(&entry),
        Ok(Err(e)) if e == "batch not found" => {
            json_error(StatusCode::NOT_FOUND, "batch not found")
        }
        Ok(Err(e)) => {
            tracing::warn!(target: "ant_gateway", error = %e, "batch lookup failed");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "unable to get batch")
        }
        Err(_) => json_error(StatusCode::GATEWAY_TIMEOUT, "batch lookup timed out"),
    }
}

/// `GET /stamps/{id}/buckets` — bee's `postageGetStampBucketsHandler`:
/// the per-bucket collision counters of a registered batch's stamp
/// issuer, `{depth, bucketDepth, bucketUpperBound, buckets:
/// [{bucketID, collisions}]}`. Unknown batch → bee's
/// `404 "issuer does not exist"`.
pub async fn stamp_buckets(
    State(handle): State<GatewayHandle>,
    Path(id): Path<String>,
) -> Response {
    let mut reasons = Vec::new();
    let Some(batch_id) = crate::error::parse_hex_param::<32>("batch_id", &id, &mut reasons) else {
        return crate::error::params_error(crate::error::ParamKind::Path, reasons);
    };

    let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
    if handle
        .commands
        .send(ControlCommand::PostageBuckets {
            batch_id,
            ack: ack_tx,
        })
        .await
        .is_err()
    {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "node loop is no longer accepting commands",
        );
    }
    let ack = match tokio::time::timeout(POSTAGE_STATUS_TIMEOUT, ack_rx).await {
        Ok(Ok(ack)) => ack,
        Ok(Err(_)) => {
            return json_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "node loop dropped the bucket request",
            )
        }
        Err(_) => return json_error(StatusCode::GATEWAY_TIMEOUT, "bucket request timed out"),
    };
    match ack {
        ControlAck::PostageBuckets(view) => {
            // Bee's field order: depth, bucketDepth, bucketUpperBound,
            // buckets (Go struct declaration order).
            #[derive(Serialize)]
            struct Bucket {
                #[serde(rename = "bucketID")]
                bucket_id: u32,
                collisions: u32,
            }
            #[derive(Serialize)]
            struct Body {
                depth: u8,
                #[serde(rename = "bucketDepth")]
                bucket_depth: u8,
                #[serde(rename = "bucketUpperBound")]
                bucket_upper_bound: u32,
                buckets: Vec<Bucket>,
            }
            json_ok(&Body {
                depth: view.depth,
                bucket_depth: view.bucket_depth,
                bucket_upper_bound: view.bucket_upper_bound,
                buckets: view
                    .collisions
                    .iter()
                    .enumerate()
                    .map(|(i, &collisions)| Bucket {
                        bucket_id: i as u32,
                        collisions,
                    })
                    .collect(),
            })
        }
        ControlAck::Error { message } => {
            if message.contains("issuer does not exist") {
                json_error(StatusCode::NOT_FOUND, "issuer does not exist")
            } else {
                json_error(StatusCode::INTERNAL_SERVER_ERROR, "get issuer failed")
            }
        }
        other => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unexpected node ack for postage buckets: {other:?}"),
        ),
    }
}

fn json_ok<T: Serialize>(value: &T) -> Response {
    use axum::response::IntoResponse;
    axum::Json(value).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_is_remaining_over_price_times_block_time() {
        // 1_000_000 PLUR left, 100 PLUR/chunk/block → 10_000 blocks
        // × 5s = 50_000s.
        assert_eq!(batch_ttl_secs(1_000_000, 100), 50_000);
    }

    #[test]
    fn zero_price_never_expires() {
        // bee reports -1 for an unpriced chain; also guards the divide.
        assert_eq!(batch_ttl_secs(1_000_000, 0), -1);
        assert_eq!(batch_ttl_secs(0, 0), -1);
    }

    #[test]
    fn drained_batch_is_zero() {
        // remainingBalance() already clamps an expired batch to 0 on
        // chain, so 0 in means a 0s TTL out (not the placeholder).
        assert_eq!(batch_ttl_secs(0, 100), 0);
    }

    #[test]
    fn astronomical_ttl_clamps_to_i64_max() {
        // Would overflow i64 → clamp instead of wrapping negative.
        assert_eq!(batch_ttl_secs(u128::MAX, 1), i64::MAX);
    }

    const LIVE: [u8; 32] = [0x11; 32];
    const GONE: [u8; 32] = [0x22; 32];
    const FLAKY: [u8; 32] = [0x33; 32];
    const EXPIRED: [u8; 32] = [0x44; 32];

    /// `LIVE` is funded; `GONE` is not on chain (`remainingBalance`
    /// reverts, zero owner); `FLAKY`'s reads all fail; `EXPIRED` is
    /// still owned but drained (`remainingBalance` 0). Counts
    /// `batch_meta` calls, which enrichment must not need.
    #[derive(Default)]
    struct Chain {
        meta_calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::ChainReader for Chain {
        async fn block_number(&self) -> Result<u64, String> {
            Ok(1)
        }
        async fn current_price(&self) -> Result<u128, String> {
            Ok(100)
        }
        async fn total_amount(&self) -> Result<u128, String> {
            Ok(7)
        }
        async fn bzz_balance(&self, _who: [u8; 20]) -> Result<u128, String> {
            Ok(0)
        }
        async fn native_balance(&self, _who: [u8; 20]) -> Result<u128, String> {
            Ok(0)
        }
        async fn chequebook_balance(&self, _cb: [u8; 20]) -> Result<u128, String> {
            Ok(0)
        }
        async fn batch_remaining_balance(&self, id: [u8; 32]) -> Result<u128, String> {
            match id {
                LIVE => Ok(1_000),
                EXPIRED => Ok(0),
                GONE => Err("execution reverted: 0x4ee9bc0f".into()),
                _ => Err("backend unavailable".into()),
            }
        }
        async fn batch_owner(&self, id: [u8; 32]) -> Result<[u8; 20], String> {
            match id {
                GONE => Ok([0u8; 20]),
                _ => Err("backend unavailable".into()),
            }
        }
        async fn batch_meta(&self, _id: [u8; 32]) -> Result<crate::BatchMetaView, String> {
            self.meta_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err("batch_meta is four eth_calls; enrichment must not use it".into())
        }
    }

    fn entry(id: [u8; 32]) -> StampEntry {
        StampEntry::from_view(&PostageStatusView {
            usable: true,
            enabled: true,
            batch_id: format!("0x{}", hex::encode(id)),
            batch_depth: 20,
            bucket_depth: 16,
            ..PostageStatusView::default()
        })
        .unwrap()
    }

    /// A batch the chain confirms is gone is reported dead (bee's
    /// `exists: false` / `batchTTL: -1`, not `usable`) instead of green
    /// until a peer rejects it; a batch whose reads merely fail keeps
    /// the optimistic placeholders.
    #[tokio::test]
    async fn batch_missing_on_chain_is_not_usable() {
        let chain = Chain::default();
        let mut stamps = [entry(LIVE), entry(GONE), entry(FLAKY), entry(EXPIRED)];
        enrich_entries(&mut stamps, &chain).await;
        let [live, gone, flaky, expired] = &stamps;
        assert_eq!(
            chain.meta_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "the gone-check reads only batchOwner",
        );

        assert!(live.usable && live.exists);
        assert_eq!(live.amount, "1007");
        assert_eq!(live.batch_ttl, 50);

        assert!(!gone.usable, "a batch the chain doesn't hold is not usable");
        assert!(!gone.exists);
        assert_eq!(gone.batch_ttl, -1);

        assert!(
            flaky.usable && flaky.exists,
            "a failed read is not a dead batch"
        );
        assert_eq!(flaky.batch_ttl, PLACEHOLDER_BATCH_TTL_SECS);

        assert!(
            !expired.usable,
            "an expired (drained, not yet evicted) batch is not usable"
        );
        assert!(expired.exists, "still on chain until evicted");
        assert_eq!(expired.batch_ttl, 0);
    }

    /// `propagating` tells a client a fresh batch (wait, or just upload:
    /// pushes wait it out) from a dead one; both read `usable: false`.
    /// A dead verdict from the chain clears it.
    #[test]
    fn stamps_say_when_a_batch_is_only_propagating() {
        let view = PostageStatusView {
            usable: false,
            propagating: true,
            enabled: true,
            batch_id: format!("0x{}", hex::encode([0x43u8; 32])),
            batch_depth: 20,
            bucket_depth: 16,
            ..PostageStatusView::default()
        };
        let fresh = StampEntry::from_view(&view).unwrap();
        let json = serde_json::to_value(&fresh).unwrap();
        assert_eq!(json["usable"], false);
        assert_eq!(json["propagating"], true);
        assert_eq!(json["exists"], true);

        let rejected = StampEntry::from_view(&PostageStatusView {
            propagating: false,
            ..view.clone()
        })
        .unwrap();
        assert_eq!(
            serde_json::to_value(&rejected).unwrap()["propagating"],
            false
        );

        let mut gone = StampEntry::from_view(&view).unwrap();
        gone.mark_not_on_chain();
        assert!(!gone.propagating);
        let mut expired = StampEntry::from_view(&view).unwrap();
        expired.mark_expired();
        assert!(!expired.propagating);
    }

    /// bee's `blockNumber`: the creation block of a batch the node
    /// bought itself, `0` when it doesn't know.
    #[test]
    fn block_number_is_the_buy_receipts_block() {
        let view = |block_number| PostageStatusView {
            enabled: true,
            batch_id: format!("0x{}", hex::encode([0x42u8; 32])),
            batch_depth: 20,
            bucket_depth: 16,
            block_number,
            ..PostageStatusView::default()
        };
        let bought = StampEntry::from_view(&view(Some(48_500_000))).unwrap();
        assert_eq!(bought.block_number, 48_500_000);
        let connected = StampEntry::from_view(&view(None)).unwrap();
        assert_eq!(connected.block_number, 0);
    }

    /// A batch the node registered at runtime moments ago (just bought)
    /// is not declared dead on a not-on-chain read: a load-balanced RPC
    /// backend may not have the `BatchCreated` block yet. Once the grace
    /// window passes, the same read is believed again; a batch reloaded
    /// from disk (no registration age) is checked immediately.
    #[tokio::test]
    async fn freshly_registered_batch_survives_lagging_rpc() {
        let chain = Chain::default();
        let fresh_view = |age| PostageStatusView {
            usable: true,
            enabled: true,
            batch_id: format!("0x{}", hex::encode(GONE)),
            batch_depth: 20,
            bucket_depth: 16,
            registered_secs_ago: Some(age),
            ..PostageStatusView::default()
        };
        let mut stamps = [
            StampEntry::from_view(&fresh_view(3)).unwrap(),
            StampEntry::from_view(&fresh_view(FRESH_BATCH_GRACE.as_secs())).unwrap(),
            entry(GONE),
        ];
        enrich_entries(&mut stamps, &chain).await;
        let [fresh, stale, reloaded] = &stamps;

        assert!(
            fresh.usable && fresh.exists,
            "just-bought batch stays usable"
        );
        assert_eq!(fresh.batch_ttl, PLACEHOLDER_BATCH_TTL_SECS);

        assert!(!stale.usable && !stale.exists, "grace window elapsed");
        assert_eq!(stale.batch_ttl, -1);

        assert!(!reloaded.usable && !reloaded.exists);
        assert_eq!(reloaded.batch_ttl, -1);

        // `registeredSecsAgo` is internal, not part of bee's shape.
        let json = serde_json::to_value(fresh).unwrap();
        assert!(json.get("registered_secs_ago").is_none());
    }
}
