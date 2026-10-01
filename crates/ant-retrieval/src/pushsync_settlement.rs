//! Pushsync-side settlement hook.
//!
//! # Why this exists
//!
//! `RoutingFetcher::push_stamped_chunk` opens a `/swarm/pushsync/1.3.1/pushsync`
//! stream to the closest peer for each chunk we upload. Bee runs accounting
//! on its end of that stream: every chunk we push adds `chunk_price` PLUR to
//! the receiver's view of *our* outbound debt to *them*
//! (`pkg/accounting/accounting.go::Debit`). When that debt crosses bee's
//! `paymentThreshold` (13.5 M PLUR full-mode, 1.35 M PLUR light-mode), bee
//! either accepts a few more chunks under `paymentTolerance` or silently
//! RST's the next pushsync stream. Without us settling the debt with a
//! [`/swarm/swap/1.0.0/swap`](crate::pushsync) cheque before we cross the
//! line, every peer eventually freezes us out — exactly the behaviour
//! observed in production on 2026-05-08 when the GGUF upload stalled at
//! ~20K successful chunks across the whole peer set (≈ 200 chunks per
//! peer ≈ bee's `paymentTolerance` envelope).
//!
//! # What this trait does
//!
//! Decouples the pushsync hot path from the SWAP / cheque issuance code.
//! The fetcher knows nothing about chequebooks, EIP-712, libp2p swap
//! streams, or chain ids; it just calls
//! [`PushsyncSettlement::note_pushsync`] right after every accepted
//! pushsync receipt. The implementation in `ant-p2p::pushsync_swap`:
//!
//! 1. Tracks per-peer cumulative debt (in PLUR units) and adds `price`.
//! 2. If the new total crosses the (configurable, default ½×bee's
//!    `lightPaymentThreshold`) trigger, looks up the peer's beneficiary
//!    EOA from the [BZZ handshake info](`ant_p2p::HandshakeInfo`)
//!    cache, builds + signs an EIP-712 cheque against our chequebook for
//!    `cumulative_debit + safety_margin`, opens a swap stream and emits
//!    it via [`ant_p2p::swap::issue_and_emit`].
//! 3. On a successful emit, zeros the local debt counter (the receiver
//!    has now been paid up to the cumulative amount we recorded in the
//!    [`ant_p2p::swap::OutboundLedger`]).
//!
//! # Failure mode
//!
//! Settlement is best-effort. A failure to emit the cheque (peer
//! disconnect mid-stream, swap protocol Reset, signature mismatch) does
//! NOT fail the upload. Instead, the next `note_pushsync` for the same
//! peer re-evaluates: if the debt is still over the trigger, we try to
//! emit again with the *new* (unchanged) cumulative payout. Because
//! cheques are cumulative the retry is idempotent — bee accepts the
//! highest cumulative it has seen and ignores duplicates. If a peer
//! refuses every cheque, the fetcher's per-peer skip list will eventually
//! blacklist it, exactly as today; we just stop wasting stream opens
//! against a settled-but-still-rejecting peer.
//!
//! # Live integration
//!
//! [`RoutingFetcher::with_pushsync_settlement`](crate::RoutingFetcher::with_pushsync_settlement)
//! installs an `Arc<dyn PushsyncSettlement>`. `None` (the default) keeps
//! the legacy "push without settlement" behaviour for tests and for the
//! ultra-light read-only build.

use async_trait::async_trait;
use libp2p::PeerId;

/// Bee's `pkg/pricer/pricer.go::PeerPrice`:
///
/// ```text
/// price = (MaxPO - proximity(peer, chunk) + 1) * basePrice
/// ```
///
/// where `MaxPO = 31` and `basePrice = 10_000` PLUR. This is the same
/// constant used by [`crate::accounting::Accounting::peer_price`] on the
/// retrieval side; we re-export the calculation here so callers don't
/// have to construct an `Accounting` just to learn the price.
///
/// Returns the chunk price in PLUR for `peer` retrieving / pushing
/// `chunk_addr`.
#[must_use]
pub fn peer_chunk_price(peer_overlay: &[u8; 32], chunk_addr: &[u8; 32]) -> u64 {
    crate::accounting::Accounting::peer_price(peer_overlay, chunk_addr)
}

/// Settlement hook for pushsync.
///
/// Implemented by `ant-p2p::pushsync_swap::PushsyncSwap`. The fetcher
/// holds an `Arc<dyn PushsyncSettlement>` and calls `note_pushsync`
/// after every accepted pushsync receipt; if the implementation decides
/// it's time to emit a cheque, it does so synchronously *inside*
/// `note_pushsync` so the next `push_stamped_chunk` call observes the
/// peer paid up.
///
/// The trait is deliberately small (one method) to keep the fetcher
/// crate ignorant of SWAP semantics. Returning `()` (no `Result`) is a
/// matter of policy: the fetcher must not refuse a successful pushsync
/// just because the settlement leg failed — we want best-effort
/// emission, not "block uploads if cheques can't be sent". Errors are
/// logged inside the implementation.
// `#[async_trait]` marks each boxed-future method `#[must_use]`, and
// clippy 1.99's `double_must_use` flags that expansion, not our code.
#[allow(clippy::double_must_use)]
#[async_trait]
pub trait PushsyncSettlement: Send + Sync {
    /// Called once per accepted pushsync receipt. `price` is the chunk
    /// price in PLUR (typically [`peer_chunk_price`] of `peer_overlay`
    /// vs `chunk_addr`). Implementations that want richer state can
    /// recover the chunk address and overlay from prior calls; for
    /// minimal-API reasons the fetcher only forwards what's strictly
    /// needed for the threshold check.
    async fn note_pushsync(&self, peer: PeerId, price: u64);

    /// Drop all settlement state for `peer`, called from the swarm's
    /// `ConnectionClosed` handler. Bee's accounting resets per peer
    /// connection (`notifyPeerConnect`), so we must too — otherwise a
    /// reconnected peer carries over a phantom debt.
    fn forget(&self, peer: &PeerId);
}
