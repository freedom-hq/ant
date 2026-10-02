//! Client-side accounting mirror for bee retrieval.
//!
//! # Why this exists
//!
//! Bee's `pkg/retrieval/retrieval.go::RetrieveChunk` calls
//! `accounting.PrepareCredit(peer, price)` *before* dispatching a
//! request. The check fails with `ErrOverdraft` if the dispatch
//! would push our expected debt with that peer over the
//! `paymentThreshold + 1s × refreshRate` envelope, at which point
//! bee silently picks the next-closest peer instead and re-tries the
//! original 600 ms later (after one `lightRefreshRate` worth of
//! allowance has accumulated). The dispatched request therefore
//! never lands on a peer that's about to disconnect us — the worst
//! case is an extra 600 ms of latency for a single saturated chunk.
//!
//! Without this mirror, we instead **find out** that a peer is over
//! its disconnect limit by being RST'd at the TCP layer (bee's
//! `accounting.go::debitAction.Apply()::if nextBalance >=
//! disconnectLimit { Blocklist }`). A 4-track bench against
//! production saw the 112 MiB file drop ~52 peers and finish in
//! 312 s vs bee's 195 s on the identical file under identical
//! handshake terms. The 117 s gap is almost entirely
//! "dispatch-then-find-out" overhead: we hedge harder, the hedge
//! lands on a saturated peer, the peer RSTs us, we cancel +
//! re-dispatch, peer set degrades, retry budget drains.
//!
//! # What this does
//!
//! [`Accounting`] tracks per-peer state mirroring bee's
//! `accountingPeer` (just the four fields that matter for
//! admission):
//!
//! - `balance`: real debt accumulated on this peer since the last
//!   accepted pseudosettle, in chunk-price units.
//! - `reserved`: prospective debt for in-flight retrievals (added
//!   on `try_reserve`, cleared on `apply` or guard drop).
//! - `last_refresh`: when the pseudosettle driver last credited
//!   this peer with an accepted refresh, so the same time-elapsed
//!   allowance bee uses can be added to the overdraft envelope.
//! - `last_used`: last successful chunk fetch from this peer, used
//!   only to age out idle peers when the gateway processes
//!   thousands of distinct peers per hour.
//!
//! [`Accounting::try_reserve`] returns a [`DebitGuard`] (RAII) on
//! success; on failure (`balance + reserved + price` >
//! `OVERDRAFT_LIMIT`) it returns `None`, and the fetcher must skip
//! this peer with a 600 ms TTL, mirroring bee's
//! `skip.Add(chunkAddr, peer, overDraftRefresh)`.
//!
//! On success the [`DebitGuard`] is moved into the per-chunk fetch
//! future. Either `apply()` is called when the chunk arrives (debit
//! moves from reserved into balance, hot hint sent if balance just
//! crossed [`HOT_DEBT_THRESHOLD`]) or the guard is dropped without
//! `apply()` (reserved is released — the chunk fetch failed and the
//! request never reached the peer's `PrepareDebit`, so bee's
//! `creditAction.Cleanup` cleared its reserve too without touching
//! ghost balance).
//!
//! [`Accounting::credit`] is the pseudosettle ack callback: when
//! the driver receives a `PaymentAck { accepted, timestamp }`,
//! call `credit(peer, accepted)` to drop our balance by that
//! amount. `last_refresh` is bumped to `Instant::now()` so the
//! envelope's `1 s × refreshRate` term reopens.
//!
//! # Sizing
//!
//! - Chunk price at typical proximity (PO 8): ~240 k units.
//! - `lightDisconnectLimit` (the line bee enforces on us when we
//!   declare `full_node = false`): 1.69 M units.
//! - `lightRefreshRate`: 450 k units / sec.
//!
//! [`OVERDRAFT_LIMIT`] is set to `lightDisconnectLimit + 1 s ×
//! lightRefreshRate ≈ 2.14 M units` — that's the exact ceiling bee
//! enforces in `debitAction.Apply` (line 1357), the boundary at
//! which a peer disconnects us. We refuse to dispatch any request
//! that would cross it.
//!
//! [`HOT_DEBT_THRESHOLD`] is set at 50 % of the disconnect limit
//! (≈ 850 k units), matching bee's `earlyPayment = 50 %` in
//! `accounting.go::PrepareCredit`. Crossing it triggers a hot hint
//! into the pseudosettle driver, which dispatches a refresh on the
//! next driver tick (100 ms) instead of waiting for the 1 s
//! periodic walk.
//!
//! # Conservatism
//!
//! Our mirror is intentionally conservative: we only ever
//! *increase* `balance` on `apply()`, so the worst-case error in
//! our envelope check is "we think this peer is more loaded than
//! it really is" → we skip them for 600 ms → tiny per-chunk latency
//! penalty. The opposite error ("we think the peer has more
//! headroom than it does") would let us blow past
//! `lightDisconnectLimit`, which is exactly the regression we're
//! trying to prevent. Cancellations release the reserve (no debit
//! ever happened on bee's side either, see
//! `creditAction.Cleanup`); only successful applies stay on the
//! books until pseudosettle clears them.

use libp2p::PeerId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify};

use crate::fetcher::Overlay;

type SharedBalances = Arc<Mutex<HashMap<PeerId, PeerBalance>>>;

/// Bee's `lightDisconnectLimit` for a peer that declares itself
/// `full_node = false` in the BZZ handshake. Computed by bee as
/// `(100 + paymentTolerance) % * lightPaymentThreshold` =
/// `1.25 * (paymentThreshold / lightFactor)` =
/// `1.25 * (13.5 M / 10)` = 1.6875 M units.
///
/// See `bee/pkg/accounting/accounting.go::NotifyPeerConnect` for
/// the wiring and `pkg/node/node.go` for the
/// `paymentThreshold = 13.5M` and `lightFactor = 10` constants.
pub const LIGHT_DISCONNECT_LIMIT: u64 = 1_687_500;

/// Bee's per-peer light-mode refresh allowance. From `pkg/node/node.go`:
/// `lightRefreshRate = refreshRate / lightFactor = 4.5 M / 10`.
/// Bee's `debitAction.Apply` allows up to `1 s × refreshRate`
/// of "in-flight" allowance on top of the disconnect limit, capped
/// at one second's worth (`min(elapsed_seconds, 1)`).
pub const LIGHT_REFRESH_RATE_PER_SEC: u64 = 450_000;

/// Combined ceiling we refuse to cross. Bee's `debitAction.Apply`
/// blocklists at `disconnectLimit + min(int64(elapsed_seconds), 1) * refreshRate`,
/// which is *either* `disconnectLimit` (when
/// elapsed < 1 s) *or* `disconnectLimit + refreshRate` (when
/// elapsed ≥ 1 s) — bee's `min` operates on `int64` second
/// granularity, no fractional seconds. See
/// `bee/pkg/accounting/accounting.go:1346`:
///
/// ```text
/// timeElapsedInSeconds := min(timeNow().Unix() - refreshReceivedTimestamp, 1)
/// ```
///
/// We pick the *lower* of the two boundaries — `disconnectLimit`
/// alone, no allowance — to leave headroom for the
/// `min(elapsed, 1) * refreshRate` we don't precisely model. With
/// 100 peers each consuming ≥ 4 chunks/sec (24-s download of a
/// 44 MiB file), the difference between 1.687 M and 2.137 M is
/// roughly one second of saturation, which a single
/// pseudosettle round-trip can clear. Anything beyond is
/// indistinguishable from a stall on bee's end.
pub const OVERDRAFT_LIMIT: u64 = LIGHT_DISCONNECT_LIMIT;

/// Threshold at which the fetcher hints to the pseudosettle driver
/// that a peer's debt is approaching the limit and a refresh
/// should fire on the next tick rather than waiting for the
/// periodic walk.
///
/// 50 % of `LIGHT_DISCONNECT_LIMIT` matches bee's
/// `earlyPayment = 50 %` constant in `pkg/accounting/accounting.go`.
pub const HOT_DEBT_THRESHOLD: u64 = LIGHT_DISCONNECT_LIMIT / 2;

/// Per-chunk skip TTL for peers that fail [`Accounting::try_reserve`].
///
/// Mirrors bee's `overDraftRefresh = time.Millisecond * 600` in
/// `pkg/retrieval/retrieval.go`. The fetcher's per-chunk skip
/// list lifts the entry after this elapses, so the same peer
/// becomes available again on the next preemptive tick once
/// pseudosettle has had a chance to clear its debt.
pub const OVERDRAFT_REFRESH: Duration = Duration::from_millis(600);

/// Longest a single [`crate::RoutingFetcher`] fetch waits, in total, for
/// credit when every candidate peer is overdraft-skipped (issue #117).
///
/// Bee's `RetrieveChunk` waits for credit without a bound of its own:
/// it sleeps [`OVERDRAFT_REFRESH`] and retries until the request context
/// ends. We have no per-chunk context, so the wait gets its own bound,
/// sized to fit inside every caller's budget:
///
/// - the gateway's 90 s `BODY_STALL_TIMEOUT` mid-body: a data child
///   that waits this long and then falls into the joiner's 60 s
///   recovery-retry window, whose last sweep can wait this long again,
///   stalls the body for at most 60 s + 2 × 10 s = 80 s;
/// - the 30 s `/bzz` and `/bytes` resolution budget: a starved root or
///   manifest fetch still gets a second attempt inside it;
/// - the chunk API's 60 s request timeout.
///
/// Feed probes past the anchor carry their own 800 ms deadline, which
/// cuts a wait short exactly as it cuts a slow peer walk short; the
/// undeadlined anchor probe doesn't re-probe a starved fetch (that would
/// stack one wait per probe retry), so it too ends after one budget. The
/// root fetch's dispersed-replica probes don't wait at all
/// ([`crate::ChunkFetcher::fetch_speculative`]), so a starved root
/// fetch with its replica fallback still ends after one budget.
pub const CREDIT_WAIT_BUDGET: Duration = Duration::from_secs(10);

/// Per-peer mirror of bee's `accountingPeer`, restricted to the
/// fields that affect admission control. We don't track
/// `ghostBalance`, `paymentThresholdForPeer`, or any of bee's
/// pricing-protocol state — those are the receiver's concern
/// (they decide when to disconnect us), and the only number we
/// can act on locally is "what does the receiver think we owe
/// them right now."
#[derive(Debug)]
struct PeerBalance {
    balance: u64,
    reserved: u64,
    last_refresh: Option<Instant>,
    last_used: Instant,
    /// Cumulative pseudosettle amount this peer has accepted from our
    /// refreshes since the connection was established (bee's
    /// `SettlementsSent` counterpart for the time-based settlement).
    /// Only ever grows via [`Accounting::credit`]; dropped with the
    /// rest of the entry on disconnect (`forget`), matching the
    /// mirror's connection-scoped lifetime — unlike bee, which
    /// persists totals in its statestore.
    time_settled: u64,
}

impl Default for PeerBalance {
    fn default() -> Self {
        Self {
            balance: 0,
            reserved: 0,
            last_refresh: None,
            last_used: Instant::now(),
            time_settled: 0,
        }
    }
}

/// Shared accounting state.
///
/// Held behind an `Arc<Mutex<_>>` so all sibling chunk fetches of
/// a single user request observe the same balance per peer. The
/// joiner fans out concurrent fetches against the same fetcher
/// (`&self`), so admission decisions on chunk N must see the
/// reservations made by chunks N-1, N-2, ... still in flight.
///
/// We never hold the lock across an `.await`; the `try_reserve`
/// path is purely synchronous, and `apply` / `credit` /
/// `cleanup_reserved` are likewise.
pub struct Accounting {
    peers: SharedBalances,
    /// Channel into the pseudosettle driver. Each event is a
    /// peer that has just crossed [`HOT_DEBT_THRESHOLD`] and
    /// needs an out-of-band refresh ASAP. The driver coalesces
    /// duplicates and respects the 1.1 s minimum spacing on
    /// bee's side.
    hot_hint: Option<mpsc::Sender<HotHint>>,
    /// Wakes fetches waiting for credit ([`Accounting::wait_for_credit`])
    /// when credit may have come free: a pseudosettle refresh landed
    /// ([`Accounting::credit`]) or a reservation was released unused
    /// ([`DebitGuard`] dropped without `apply`). Woken one at a time,
    /// oldest first; see [`Accounting::pass_credit`].
    credit_freed: Arc<Notify>,
    /// Count of credit releases (each `credit_freed` event), so a woken
    /// waiter can tell a release it hasn't looked at yet from one it
    /// already has; see [`Accounting::credit_epoch`].
    credit_epoch: Arc<AtomicU64>,
}

/// Hint payload sent from the fetcher hot path into the
/// pseudosettle driver. Currently just carries the peer id, but
/// is wrapped in a struct so future fields (e.g., observed debt)
/// can be added without touching every call site.
#[derive(Debug, Clone, Copy)]
pub struct HotHint {
    pub peer: PeerId,
}

impl Default for Accounting {
    fn default() -> Self {
        Self::new()
    }
}

impl Accounting {
    #[must_use]
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
            hot_hint: None,
            credit_freed: Arc::new(Notify::new()),
            credit_epoch: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn with_hot_hint(mut self, tx: mpsc::Sender<HotHint>) -> Self {
        self.hot_hint = Some(tx);
        self
    }

    /// Compute the chunk price for a peer at a given chunk address.
    /// Mirrors `bee/pkg/pricer/pricer.go::PeerPrice`:
    ///
    /// ```text
    /// price = (MaxPO - proximity(peer, chunk) + 1) * basePrice
    /// ```
    ///
    /// where `MaxPO = 31` (256-bit address space, byte boundary)
    /// and `basePrice = 10_000`. Closer peers (higher proximity)
    /// charge less; far peers charge more, because the far peer's
    /// forwarding chain is longer and more nodes earn along the
    /// way.
    #[must_use]
    pub fn peer_price(peer_overlay: &Overlay, chunk_addr: &[u8; 32]) -> u64 {
        const MAX_PO: u64 = 31;
        const BASE_PRICE: u64 = 10_000;
        (MAX_PO - proximity(peer_overlay, chunk_addr) + 1) * BASE_PRICE
    }

    /// Try to reserve `price` against `peer` for chunk `chunk_addr`.
    ///
    /// Returns `Some(DebitGuard)` if the dispatch is admissible —
    /// i.e., `balance + reserved + price <= OVERDRAFT_LIMIT` after
    /// adding back any `lightRefreshRate × elapsed` allowance that
    /// has accrued since the last successful refresh.
    ///
    /// Returns `None` if the dispatch would cross the limit; the
    /// caller is expected to skip this peer for
    /// [`OVERDRAFT_REFRESH`] and try the next-closest one. This
    /// matches bee's
    /// `pkg/retrieval/retrieval.go::case ErrOverdraft` arm.
    #[must_use]
    pub fn try_reserve(&self, peer: PeerId, price: u64) -> Option<DebitGuard> {
        let mut peers = self.peers.lock().ok()?;
        let now = Instant::now();
        let entry = peers.entry(peer).or_insert_with(|| PeerBalance {
            last_used: now,
            ..PeerBalance::default()
        });

        // Mirror bee's `min(timeNow().Unix() - refreshReceivedTimestamp, 1)`
        // exactly: the allowance is binary on a one-second boundary,
        // not interpolated. Bee grants the full
        // `lightRefreshRate` only after a full second has elapsed
        // since the last *accepted* refresh.
        //
        // For never-refreshed peers (`last_refresh = None`) we
        // intentionally use the smaller, zero-allowance branch so a
        // burst of dispatches at startup doesn't pile reservations
        // up to bee's max line before pseudosettle has had a chance
        // to land.
        let allowance = match entry.last_refresh {
            Some(last) if now.duration_since(last) >= Duration::from_secs(1) => {
                LIGHT_REFRESH_RATE_PER_SEC
            }
            _ => 0,
        };
        let limit = OVERDRAFT_LIMIT.saturating_add(allowance);
        let next = entry
            .balance
            .saturating_add(entry.reserved)
            .saturating_add(price);
        if next > limit {
            return None;
        }
        entry.reserved = entry.reserved.saturating_add(price);
        entry.last_used = now;
        Some(DebitGuard {
            peer,
            price,
            applied: false,
            balances: self.peers.clone(),
            hot_hint: self.hot_hint.clone(),
            credit_freed: self.credit_freed.clone(),
            credit_epoch: self.credit_epoch.clone(),
        })
    }

    /// Wait until credit may have come free, or `max` elapses, whichever
    /// is first. Returns `true` when woken by freed credit.
    ///
    /// Used by [`crate::RoutingFetcher`] when every candidate peer for a
    /// chunk is overdraft-skipped (issue #117), in place of bee's plain
    /// `time.After(overDraftRefresh)` sleep in `RetrieveChunk`. The
    /// caller passes [`OVERDRAFT_REFRESH`] (or less, near the end of its
    /// [`CREDIT_WAIT_BUDGET`]) as `max`, so it still re-checks on bee's
    /// cadence; that also catches credit that opens without an event
    /// (the one-second refresh allowance, newly connected peers).
    ///
    /// No stampede: a refresh or a released reservation wakes exactly
    /// one waiter, the one that has waited longest (`Notify::notify_one`
    /// is FIFO), not every fetch parked on the pool. A woken waiter that
    /// gets a reservation calls [`Accounting::pass_credit`] to wake the
    /// next, since the credit may cover more than one chunk. One that
    /// can't use the credit (it's on a peer this waiter has no use for)
    /// passes it on too, unless it already looked at the pool after that
    /// release ([`Accounting::credit_epoch`] unchanged); since waiters
    /// re-queue at the back, an unusable wake-up visits each waiter at
    /// most once and then stops. A waiter whose wait ended on its timer
    /// passes nothing. So a refill admitting `k` chunks wakes the
    /// waiters up to the `k`-th one that can use it, plus one, not all
    /// of them. A wake-up delivered while nobody waits is kept for the
    /// next waiter.
    pub async fn wait_for_credit(&self, max: Duration) -> bool {
        tokio::time::timeout(max, self.credit_freed.notified())
            .await
            .is_ok()
    }

    /// Hand a credit wake-up on to the next waiter. Called by a fetch
    /// that was waiting for credit and just got a reservation; see
    /// [`Accounting::wait_for_credit`].
    pub fn pass_credit(&self) {
        self.credit_freed.notify_one();
    }

    /// How many times credit has been released so far (pseudosettle
    /// refreshes and unused reservations dropped). A woken waiter
    /// compares it against the value it last saw to tell whether the
    /// wake-up carries a release it hasn't looked at yet; see
    /// [`Accounting::wait_for_credit`].
    #[must_use]
    pub fn credit_epoch(&self) -> u64 {
        self.credit_epoch.load(Ordering::SeqCst)
    }

    /// Record a credit release and wake one waiter.
    fn release_credit(epoch: &AtomicU64, notify: &Notify) {
        epoch.fetch_add(1, Ordering::SeqCst);
        notify.notify_one();
    }

    /// Credit a peer with the bee-side accepted refresh amount.
    /// Subtracts `accepted` from `balance` (saturating) and bumps
    /// `last_refresh` to `Instant::now()`, opening the per-second
    /// allowance window again.
    pub fn credit(&self, peer: PeerId, accepted: u64) {
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        let entry = peers.entry(peer).or_default();
        entry.balance = entry.balance.saturating_sub(accepted);
        entry.time_settled = entry.time_settled.saturating_add(accepted);
        entry.last_refresh = Some(Instant::now());
        drop(peers);
        if accepted > 0 {
            Self::release_credit(&self.credit_epoch, &self.credit_freed);
        }
    }

    /// Drop all per-peer state for `peer`. Called when the swarm
    /// `ConnectionClosed` event fires (peers reconnect with a
    /// fresh balance on bee's side too — bee's
    /// `notifyPeerConnect` resets the `accountingPeer`, so we
    /// should match).
    pub fn forget(&self, peer: &PeerId) {
        let Ok(mut peers) = self.peers.lock() else {
            return;
        };
        peers.remove(peer);
    }

    /// Snapshot a peer's balance for diagnostic logging. Returns
    /// `(balance, reserved)`. Cheap (single lock acquisition).
    #[must_use]
    pub fn debug_snapshot(&self, peer: &PeerId) -> Option<(u64, u64)> {
        let peers = self.peers.lock().ok()?;
        peers.get(peer).map(|b| (b.balance, b.reserved))
    }

    /// Enumerate every tracked peer as `(peer, balance,
    /// time_settled)` — the outstanding debt we owe the peer and the
    /// cumulative pseudosettle amount it has accepted from us. Backs
    /// the gateway's `/balances`, `/consumed`, and `/timesettlements`
    /// endpoints via `ControlCommand::AccountingSnapshot`. Cheap
    /// (single lock acquisition, one row per connected peer).
    #[must_use]
    pub fn settlement_snapshot(&self) -> Vec<(PeerId, u64, u64)> {
        let Ok(peers) = self.peers.lock() else {
            return Vec::new();
        };
        peers
            .iter()
            .map(|(peer, b)| (*peer, b.balance, b.time_settled))
            .collect()
    }
}

/// RAII guard returned by [`Accounting::try_reserve`]. Wraps the
/// reserved debit; either [`DebitGuard::apply`] is called (debit
/// moves from `reserved` into `balance`) or the guard drops with
/// `applied = false` (reserved is released without touching
/// balance).
///
/// The guard does NOT hold the `peers` lock — the `Mutex` is
/// re-acquired in `apply`/`Drop` for the brief moment of
/// state mutation.
pub struct DebitGuard {
    peer: PeerId,
    price: u64,
    applied: bool,
    balances: SharedBalances,
    hot_hint: Option<mpsc::Sender<HotHint>>,
    credit_freed: Arc<Notify>,
    credit_epoch: Arc<AtomicU64>,
}

impl DebitGuard {
    /// Mark the debit as applied. Moves `price` from `reserved`
    /// into `balance`. If the new `balance` crosses
    /// [`HOT_DEBT_THRESHOLD`], fire a [`HotHint`] into the
    /// pseudosettle driver so it can dispatch a refresh on the
    /// next 100 ms tick rather than waiting for the periodic
    /// walk.
    pub fn apply(mut self) {
        let crossed = {
            let Ok(mut peers) = self.balances.lock() else {
                return;
            };
            let entry = peers.entry(self.peer).or_default();
            entry.reserved = entry.reserved.saturating_sub(self.price);
            let prev_balance = entry.balance;
            entry.balance = entry.balance.saturating_add(self.price);
            prev_balance < HOT_DEBT_THRESHOLD && entry.balance >= HOT_DEBT_THRESHOLD
        };
        self.applied = true;
        if crossed {
            if let Some(tx) = &self.hot_hint {
                let _ = tx.try_send(HotHint { peer: self.peer });
            }
        }
    }

    /// Peer this guard is for. Useful for logging.
    #[must_use]
    pub const fn peer(&self) -> PeerId {
        self.peer
    }
}

impl Drop for DebitGuard {
    fn drop(&mut self) {
        if self.applied {
            return;
        }
        // Cancellation path: release the reservation without
        // touching balance. Mirrors bee's
        // `creditAction.Cleanup` (line 430 of
        // `pkg/accounting/accounting.go`): if the action wasn't
        // applied, just decrement the per-peer reserve. No
        // ghost-balance increment on the *credit* side; the
        // ghost-balance penalty is on bee's *debit* side and is
        // the receiver's concern, not ours.
        let Ok(mut peers) = self.balances.lock() else {
            return;
        };
        if let Some(entry) = peers.get_mut(&self.peer) {
            entry.reserved = entry.reserved.saturating_sub(self.price);
        }
        drop(peers);
        // The released reserve is credit a fetch waiting on this peer
        // can use now.
        Accounting::release_credit(&self.credit_epoch, &self.credit_freed);
    }
}

/// XOR-distance proximity order between two 32-byte addresses,
/// matching `bee/pkg/swarm/swarm.go::Proximity`. Returns the
/// number of leading zero bits in `a XOR b`, capped at `MAX_PO =
/// 31` (we don't bother with the full 256 because chunk price
/// uses `MAX_PO - po + 1` and any `po >= MAX_PO` collapses to
/// the same minimum price anyway).
fn proximity(a: &[u8; 32], b: &[u8; 32]) -> u64 {
    const MAX_PO: u64 = 31;
    let mut po = 0u64;
    for i in 0..32 {
        let x = a[i] ^ b[i];
        if x == 0 {
            po = po.saturating_add(8);
            if po >= MAX_PO {
                return MAX_PO;
            }
            continue;
        }
        po = po.saturating_add(u64::from(x.leading_zeros()));
        return po.min(MAX_PO);
    }
    MAX_PO
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(b: u8) -> [u8; 32] {
        let mut a = [0u8; 32];
        a[0] = b;
        a
    }

    #[test]
    fn proximity_identical_addrs_is_max_po() {
        let a = addr(0xab);
        assert_eq!(proximity(&a, &a), 31);
    }

    #[test]
    fn proximity_first_byte_differs() {
        let a = addr(0b1000_0000);
        let b = addr(0b0000_0000);
        assert_eq!(proximity(&a, &b), 0);
    }

    #[test]
    fn proximity_capped_at_max_po() {
        // 8 leading zero bytes = 64 matching bits, far above MAX_PO = 31.
        let mut a = [0u8; 32];
        a[8] = 0x80;
        let b = [0u8; 32];
        assert_eq!(proximity(&a, &b), 31);
    }

    #[test]
    fn peer_price_higher_for_far_peer() {
        let chunk = addr(0xff);
        let close = addr(0xff);
        let far = addr(0x00);
        let close_price = Accounting::peer_price(&close, &chunk);
        let far_price = Accounting::peer_price(&far, &chunk);
        assert!(far_price > close_price);
        // (MAX_PO - po + 1) * BASE_PRICE = (31 - 31 + 1) * 10_000 = 10_000.
        assert_eq!(close_price, 10_000);
        // (MAX_PO - po + 1) * BASE_PRICE = (31 -  0 + 1) * 10_000 = 320_000.
        assert_eq!(far_price, 320_000);
    }

    /// Issue #117: a refill wakes one fetch waiting for credit, not
    /// every fetch parked on the pool. Each woken waiter that got a
    /// reservation passes the wake-up on (`pass_credit`), so a refill
    /// covering `k` chunks wakes `k + 1` waiters in FIFO order.
    #[tokio::test]
    async fn freed_credit_wakes_one_waiter_at_a_time() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let acc = Arc::new(Accounting::new());
        let woken = Arc::new(AtomicUsize::new(0));
        let mut waiters = Vec::new();
        for _ in 0..20 {
            let acc = acc.clone();
            let woken = woken.clone();
            waiters.push(tokio::spawn(async move {
                if acc.wait_for_credit(Duration::from_mins(1)).await {
                    woken.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }
        let settle = || tokio::time::sleep(Duration::from_millis(50));
        settle().await;
        assert_eq!(woken.load(Ordering::SeqCst), 0);

        // A pseudosettle refresh lands: one waiter, not twenty.
        let p = PeerId::random();
        acc.credit(p, 450_000);
        settle().await;
        assert_eq!(
            woken.load(Ordering::SeqCst),
            1,
            "a refill must not stampede"
        );

        // That waiter got its reservation and hands the wake-up on.
        acc.pass_credit();
        settle().await;
        assert_eq!(woken.load(Ordering::SeqCst), 2);

        // A reservation released unused is freed credit too.
        let g = acc.try_reserve(p, 240_000).expect("admits");
        drop(g);
        settle().await;
        assert_eq!(woken.load(Ordering::SeqCst), 3);

        // An applied debit frees nothing and wakes nobody.
        acc.try_reserve(p, 240_000).expect("admits").apply();
        settle().await;
        assert_eq!(woken.load(Ordering::SeqCst), 3);

        for w in waiters {
            w.abort();
        }
    }

    #[test]
    fn try_reserve_admits_under_limit() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 240_000);
        assert!(g.is_some());
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (0, 240_000));
        drop(g);
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (0, 0), "guard drop should release the reservation");
    }

    #[test]
    fn try_reserve_rejects_over_limit() {
        let acc = Accounting::new();
        let p = PeerId::random();
        // Each call before any successful refresh gets a full 1 s
        // allowance: limit = OVERDRAFT_LIMIT + 450 k = 2.5875 M.
        // 11 chunks × 240 k = 2.64 M crosses it.
        let mut guards = Vec::new();
        for _ in 0..15 {
            if let Some(g) = acc.try_reserve(p, 240_000) {
                guards.push(g);
            }
        }
        assert!(
            !guards.is_empty(),
            "first reservation should always succeed",
        );
        assert!(
            guards.len() < 15,
            "got {} reservations, expected the cap to bite before 15",
            guards.len(),
        );
    }

    #[test]
    fn apply_moves_reserved_to_balance() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 240_000).unwrap();
        g.apply();
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (240_000, 0));
    }

    #[test]
    fn credit_reduces_balance() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 500_000).unwrap();
        g.apply();
        acc.credit(p, 300_000);
        let snap = acc.debug_snapshot(&p).unwrap();
        assert_eq!(snap, (200_000, 0));
    }

    #[test]
    fn forget_removes_peer_state() {
        let acc = Accounting::new();
        let p = PeerId::random();
        let g = acc.try_reserve(p, 240_000).unwrap();
        g.apply();
        assert!(acc.debug_snapshot(&p).is_some());
        acc.forget(&p);
        assert!(acc.debug_snapshot(&p).is_none());
    }

    #[test]
    fn hot_hint_fires_when_crossing_threshold() {
        let (tx, mut rx) = mpsc::channel::<HotHint>(8);
        let acc = Accounting::new().with_hot_hint(tx);
        let p = PeerId::random();
        // First 3 chunks of 240 k = 720 k, just under HOT_DEBT_THRESHOLD = 843 k.
        for _ in 0..3 {
            acc.try_reserve(p, 240_000).unwrap().apply();
        }
        assert!(rx.try_recv().is_err(), "should not fire below threshold");
        // Fourth chunk crosses the threshold (3 × 240 k + 240 k = 960 k > 843 k).
        acc.try_reserve(p, 240_000).unwrap().apply();
        let hint = rx.try_recv().expect("hot hint should fire");
        assert_eq!(hint.peer, p);
    }
}
