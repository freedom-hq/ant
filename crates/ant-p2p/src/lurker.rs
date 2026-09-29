//! GSOC/PSS lurker driver — the persistent receive loop.
//!
//! Given a [`WatchState`] (watched GSOC addresses, plus registered PSS
//! topics and secret) and a target neighborhood, the lurker keeps peers
//! resident near the target, pulls the neighborhood bin live from a peer
//! (see [`crate::pullsync`]), classifies each delivered chunk
//! ([`crate::messaging::classify`]), and forwards decoded messages to a
//! subscriber channel. It runs until the subscriber drops its receiver.
//!
//! Bandwidth shape:
//!
//! - **GSOC** is precise — we watch exact SOC addresses, so the `want`
//!   bitvector requests *only* those (delivery bandwidth ≈ 0 until an
//!   update lands).
//! - **PSS** cannot be identified from the offered address (the trojan
//!   address is mined, not derivable), so when PSS is enabled the lurker
//!   must download candidate CACs and attempt unwrap — exactly what a bee
//!   full node does when it `TryUnwrap`s every passing chunk. Callers
//!   that only need GSOC leave `pss_secret` unset and pay nothing.

use crate::lurker_registry::SharedWatch;
use crate::messaging::{classify, DecodedMessage, WatchState};
use crate::pullsync::{self, OfferedChunk};
use crate::routing::{proximity, Overlay};
use ant_crypto::keccak256;
use libp2p::PeerId;
use libp2p_stream::Control;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// Cap on the dedup set; beyond it the **oldest** entries are evicted
/// (never the whole set — a wholesale clear would re-deliver everything
/// a busy bin re-offers). An evicted entry can at worst re-deliver one
/// old message.
const SEEN_CAP: usize = 8192;
/// Bound on a single blocking live-sync round. The server long-blocks on
/// an empty bin, so we cap each round to periodically re-dial toward the
/// neighborhood and re-pick a deeper covering peer.
const SYNC_ROUND_TIMEOUT: Duration = Duration::from_secs(20);
/// Bound on a per-peer cursor fetch during coverage setup. The cursor
/// exchange is a quick request/response (unlike the long-blocking sync
/// round), so a peer that hasn't answered in this long is silent — we
/// skip it this pass rather than let it stall every other peer's
/// coverage. Fetches also run concurrently, so this is a per-peer, not
/// a cumulative, bound.
const CURSOR_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
/// How long the driver waits after a connectivity change before topping
/// up coverage — coalesces a burst of peer churn into one pass.
const TOPUP_DEBOUNCE: Duration = Duration::from_millis(700);
/// Fallback tick: how often the driver re-dials and re-picks covering
/// peers even when connectivity is quiet.
const RE_RESIDE_INTERVAL: Duration = Duration::from_secs(30);
/// Backstop on coordinated handover: an obsolete puller is normally
/// retired only once every desired replacement is ready, but if a
/// desired peer can never open a stream (permanently unresponsive), the
/// replacements would never all become ready and obsolete pullers would
/// accumulate without bound. After this long as obsolete, a puller is
/// retired regardless — bounding the puller set even against a wedged
/// peer, at the cost of a possible brief coverage dip in that rare case.
const HANDOVER_MAX_OVERLAP: Duration = Duration::from_mins(1);
/// How far behind a peer's cursor a **fresh** (peer, bin) puller starts.
/// A light node takes seconds to dial the neighborhood's storers into
/// its connection set; a message that landed on a storer just before we
/// read its cursor would live below `cursor + 1` and be skipped forever.
/// Pulling a short backlog closes that window at the cost of bounded
/// replay near subscribe/handover time — the seen-set dedups across
/// peers, so delivery is at-least-once, not N-times. Replaced pullers
/// don't use this: they resume exactly where their predecessor stopped.
const PULL_BACKLOG: u64 = 8;
/// Earliest binID (reserves are 1-indexed; binID 0 is never used).
const HISTORY_FLOOR: u64 = 1;
/// Mailbox lookback, in binIDs, per (peer, bin). Mailbox mode starts a
/// fresh puller this far behind the peer's cursor instead of at
/// [`PULL_BACKLOG`], recovering messages sent while offline.
///
/// It is a **bounded** window on purpose. binIDs count chunks that
/// landed in one bin on one peer, and a light node pulls a shallow
/// covering peer's bin (`b_p ≈ 9-14`), which is busy — so an unbounded
/// `start = 1` sweep would drag the whole history of a hot bin. This
/// window caps the sweep at a few thousand recent chunks per bin, which
/// on a busy bin is a recent-history mailbox (minutes-to-hours,
/// depending on the bin's fill rate) rather than the complete backlog.
///
/// A *complete* backlog sweep would need the trojan concentrated into a
/// sparse deep bin (a deeper mining prefix pulled by a deeply-resident
/// receiver) — see [`PSS_MINED_PREFIX_BITS`] for why that trade doesn't
/// pay at light-node residency. So today the mailbox is "recent", and a
/// larger [`HISTORY_BACKLOG`] simply extends how far back it reaches at
/// linear cost.
///
/// A sweep may exceed [`SEEN_CAP`] and re-deliver its oldest chunks;
/// that is within the documented at-least-once/may-duplicate contract.
const HISTORY_BACKLOG: u64 = 4096;
/// Number of closest connected peers to pull from concurrently. A
/// freshly-pushed chunk lands on the storer(s) nearest its address and
/// replicates outward; pulling several covering peers catches it
/// regardless of which one got (or replicated) it first — the difference
/// between reliable and flaky reception on a light node.
const COVERING_PEERS: usize = 5;
/// The PSS mined-prefix lengths `L`, in bits, a receiver covers: one
/// per *deliverable* target length `/pss/send` accepts — 2-byte
/// (`L = 16`) and 3-byte (`L = 24`) targets. The receiver cannot tell
/// which length a sender mined to, so [`covering_bins`] takes the union
/// of every listed `L`'s bins. 2-byte targets are the de-facto practice
/// and what ant's own gateway demos use, but a 3-byte sender is equally
/// valid and must not be silently dropped by a deep covering peer (the
/// `b_p >= 16` regime would otherwise pull only `16..=19` while the
/// trojan sits at `b_p` or `24 + Geom`).
///
/// **1-byte targets (`L = 8`) are deliberately not covered.** The API
/// accepts them (bee's cap is
/// 1..=[`ant_crypto::pss::MAX_TARGET_LEN`] bytes and ant mirrors it),
/// but a trojan that agrees with the target on only 8 bits is pushed
/// to whichever neighbourhood is closest to its *mined* address. With
/// mainnet storage depth `d ≈ 9-11 > 8` it lands in the target's
/// depth-`d` neighbourhood only by chance, with probability
/// `2^-(d-8)` (≈ 1/2 at `d = 9`, 1/4 at `d = 10`, 1/8 at `d = 11`).
/// Landing there is not enough to be received, though: a trojan `c`
/// sits at bin `b_p` on a covering peer `p` only if it agrees with the
/// target *past* bit `b_p` (`PO(c, target) > b_p`, the same
/// deterministic argument as the `b_p < L` regime below), and bin
/// `b_p` is pulled anyway for the 3-byte case whenever `b_p < 24`. One
/// with `d <= PO(c, target) <= b_p` is stored in the neighbourhood but
/// filed under bin `PO(c, target)` (or deeper) on `p`, which is not
/// pulled. So ant receives only about `2^-(b_p-7)` of 1-byte messages
/// (`b_p` of the shallowest covering peer; ≈ 1/64 at `b_p = 13`) —
/// lossy at best, and well below the `2^-(d-8)` that reach the
/// neighbourhood. What is *not* done is pull the `8 + Geom(1/2)` bins
/// a `b_p >= 8` peer would file the remaining 1-byte trojans under:
/// covering `L = 8` would make every PSS subscription pull bins
/// `8..=11` from each covering peer, and at
/// `d ≈ 10` those are the storer's *fullest* reserve bins (≈ 75-90 % of
/// its ingest vs ≈ 2-25 % for bin `b_p`), all of which `want()`
/// downloads and trial-unwraps — a many-fold bandwidth cost on a light
/// node for messages that mostly weren't stored in the target's
/// neighbourhood to begin with. Senders wanting reliable delivery must
/// use ≥ 2-byte targets.
///
/// Correction due to Viktor Trón: which bin a trojan `c` occupies on a
/// covering peer `p` depends on how `b_p = PO(p, target)` compares to
/// the mined prefix length `L` — the trie geometry gives two regimes:
///
/// - **`b_p < L`**: `p` diverges from the target at bit `b_p` while `c`
///   still agrees there, so `PO(c, p) = b_p` **exactly**. One
///   deterministic bin; any window is dead weight.
/// - **`b_p >= L`**: `c` agrees with `p` through bit `L` and is mined
///   noise beyond, so `PO(c, p) = L + Geom(1/2)` — *independent of
///   `b_p`*, i.e. the trojan sits around bin `L`, **shallower** than
///   `b_p`. Pulling `b_p` and deeper (the previous behaviour) misses
///   it; the correct base is `L`, with a small deeper window for the
///   geometric tail (each +1 bin halves the missed mass).
///
/// `L` must exceed the storage depth `d` for the target's neighbourhood
/// to reliably keep the trojan (below `d` it does so only with
/// probability `2^-(d-L)`) — why `L = 8` is excluded above. Beyond
/// that the sender picks `L` freely (via its target length); the
/// receiver does not need to agree on one, since it covers every
/// listed `L`.
///
/// **Why senders should use 16, not 24.** A deeper prefix concentrates
/// a trojan into a smaller slice of the reserve (`~2^(reserve-(L-d))`),
/// which in principle cuts a *deeply-resident* receiver's candidate
/// traffic and shrinks the mailbox backlog. But that benefit needs
/// covering peers at `b_p >= L`: a light node's covering peers sit at
/// `b_p ≈ 9-14`, below both listed `L`, so for them `covering_bins`
/// pulls exactly bin `b_p` and nothing else (the `b_p < L` regime for
/// every `L`). Measured on mainnet: at both `L=16` and `L=24` the
/// receiver pulls the same bins and downloads the same candidates — the
/// deeper prefix buys a light-node receiver nothing. It only *costs*: `L`
/// bits is `~2^L` mine hashes, so `L=24` is ~256× the sender work
/// (seconds on a phone, and it trips the send timeout), and PSS already
/// carries an economic spam gate via the postage stamp every send burns.
/// So senders should keep `L=16` (the receive side still covers `L=24`,
/// above): cheap to mine (mobile-friendly), identical receive at
/// light-node residency. (A deeper prefix as a network-wide anti-spam
/// proof-of-work is a protocol-incentive question, not a
/// receiver-efficiency one; tracked in the SWIP messaging extension's
/// "PSS mining depth" section.)
const PSS_MINED_PREFIX_BITS: [u8; 2] = [16, 24];
/// Deeper bins pulled past bin `L` (each of [`PSS_MINED_PREFIX_BITS`])
/// in the `b_p >= L` regime, covering the geometric tail of `PO(c, p) =
/// L + Geom(1/2)`: a window of 3 captures 15/16 of the mass per peer,
/// and the [`COVERING_PEERS`]-way redundancy covers the rest. Unused in
/// the `b_p < L` regime, where the bin is exact.
const PSS_BIN_WINDOW: u8 = 3;
/// Highest proximity-order bin.
const MAX_BIN: u8 = 31;
/// After this many *consecutive* timed-out (quiet) rounds, a puller
/// re-probes the peer's cursors before trusting it again. `on_ready`
/// fires on the Get send — before any byte comes back — so without a
/// liveness check a peer that accepts streams and never responds (or
/// silently wiped its reserve, changing its epoch) would count as
/// coverage forever: a free censorship lever. A probe that fails, times
/// out, or reports a different epoch retires the puller so the driver
/// rotates in a live peer; a genuinely quiet bin passes the probe
/// cheaply. With `SYNC_ROUND_TIMEOUT` this probes roughly once a minute
/// on an idle bin.
const STALENESS_PROBE_ROUNDS: u32 = 3;

/// Per-`(peer, bin)` resume position `(reserve_epoch, next_start)`,
/// shared with the pullers. The epoch tag lets the driver discard a
/// stale position after a peer wipes its reserve.
type Positions = Arc<Mutex<HashMap<(PeerId, u8), (u64, u64)>>>;
/// Readiness map: `(peer, bin) → generation` of the puller that marked
/// it ready (stream open, `Get` sent). Tagged with a per-puller
/// generation so a dying puller's [`ReadyGuard`] removes only *its own*
/// entry on exit, never a newer replacement puller's — the handover
/// waits on this, so a stale entry would falsely retire live coverage.
type ReadySet = Arc<Mutex<HashMap<(PeerId, u8), u64>>>;

/// Removes a puller's readiness the instant it exits (return, error, or
/// handover abort) — not just at the next driver pass — so a puller that
/// dies during the multi-second cursor-fetch window can't leave `ready`
/// asserting coverage it no longer provides. Only removes the entry if
/// it still carries this puller's generation.
struct ReadyGuard {
    ready: ReadySet,
    key: (PeerId, u8),
    generation: u64,
}

impl Drop for ReadyGuard {
    fn drop(&mut self) {
        let mut r = self.ready.lock().unwrap_or_else(PoisonError::into_inner);
        if r.get(&self.key) == Some(&self.generation) {
            r.remove(&self.key);
        }
    }
}

/// A puller `JoinHandle` that aborts its task when dropped.
///
/// The registry tears a shared lurker down with `JoinHandle::abort()` on
/// the *driver* task; a bare abort would drop the driver's `active` map
/// and **detach** every puller (a dropped `JoinHandle` never aborts), so
/// up to `COVERING_PEERS × bins` pullers would keep pulling until their
/// next `out.is_closed()` check — up to `SYNC_ROUND_TIMEOUT` each. With
/// this wrapper, dropping the map aborts them all immediately, however
/// the driver ends.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A live lurker subscription: the target neighborhood and what to watch.
pub struct LurkerConfig {
    /// Overlay whose neighborhood we reside in and pull.
    pub target: [u8; 32],
    /// What to decode (GSOC addresses, PSS topics + secret). Shared with
    /// the registry, which grows/shrinks it as subscribers attach and
    /// leave — the lurker re-reads it every pull round, so watch changes
    /// take effect without restarting pullers or losing positions.
    pub watch: SharedWatch,
}

/// Bounded delivered-chunk dedup, keyed by `(address, keccak(data))`.
///
/// The key must include the content: GSOC deliberately reuses one stable
/// SOC address for every update, so keying by address alone would drop
/// every update after the first — and would let an invalid first
/// delivery poison the address for the real one. Including the address
/// keeps distinct watched SOCs distinct even if two ever carried equal
/// bytes. Eviction is oldest-first, never wholesale.
struct Seen {
    set: HashSet<[u8; 64]>,
    order: VecDeque<[u8; 64]>,
}

impl Seen {
    fn new() -> Self {
        Self {
            set: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    /// Dedup key for one content version.
    fn key_for(address: &[u8; 32], data: &[u8]) -> [u8; 64] {
        let mut key = [0u8; 64];
        key[..32].copy_from_slice(address);
        key[32..].copy_from_slice(&keccak256(data));
        key
    }

    /// Record `(address, content)`; `false` if this exact version was
    /// already seen (another covering peer usually delivers it too).
    /// Production goes through [`Self::key_for`] + [`Self::insert_key`]
    /// so the delivery path can roll a reservation back on cancel.
    #[cfg(test)]
    fn insert(&mut self, address: &[u8; 32], data: &[u8]) -> bool {
        self.insert_key(Self::key_for(address, data))
    }

    fn insert_key(&mut self, key: [u8; 64]) -> bool {
        if !self.set.insert(key) {
            return false;
        }
        self.order.push_back(key);
        if self.order.len() > SEEN_CAP {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }

    /// Roll back a reservation made by [`Seen::insert_key`]. Only called
    /// on the rare cancel/error path, so the O(n) order scan is fine.
    fn remove(&mut self, key: &[u8; 64]) {
        if self.set.remove(key) {
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                self.order.remove(pos);
            }
        }
    }
}

/// Rolls a [`Seen`] reservation back on drop unless committed.
///
/// The delivery send (`out.send(...).await`) is a **cancellation point**:
/// the driver's handover aborts obsolete pullers, and an abort landing
/// while the channel is full would otherwise leave the chunk marked seen
/// but never delivered — every other covering peer's copy of it then
/// dedups against the phantom entry and the message is lost for good,
/// defeating the very redundancy the covering set exists for. Reserving
/// first (keeping the cross-puller mutual exclusion) and rolling back on
/// an uncommitted drop restores at-least-once.
///
/// Known residual (accepted): if a *second* puller dedup-skips this
/// chunk while the reservation is uncommitted and the holder is then
/// aborted, the rollback lands after the skipper already moved past it.
/// Actual loss additionally requires the chunk to sit more than
/// `PULL_BACKLOG` back in the bin's history for every other covering
/// peer — the aborted puller's own position is published per *page*, so
/// its replacement re-pulls the unfinished page — which stacks three
/// independent rarities. Closing it entirely would need cross-puller
/// delivery sequencing; not worth the coupling.
struct SeenReservation {
    seen: Arc<Mutex<Seen>>,
    key: [u8; 64],
    committed: bool,
}

impl SeenReservation {
    fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for SeenReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.seen
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&self.key);
        }
    }
}

/// Dedup, classify, and forward one delivered chunk (cancel-safely — see
/// [`SeenReservation`]). Returns `false` when the subscriber is gone and
/// the puller should exit.
async fn deliver_chunk(
    seen: &Arc<Mutex<Seen>>,
    watch: &SharedWatch,
    out: &mpsc::Sender<DecodedMessage>,
    address: &[u8; 32],
    data: &[u8],
) -> bool {
    // Dedup one *content version* — the same chunk arrives from several
    // covering peers, but a GSOC update reuses its address with new
    // content and must still go through.
    let key = Seen::key_for(address, data);
    if !seen
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert_key(key)
    {
        return true;
    }
    let decoded = classify(
        address,
        data,
        &watch.read().unwrap_or_else(PoisonError::into_inner),
    );
    if let Some(msg) = decoded {
        tracing::info!(target: "ant_p2p::lurker", "lurker decoded a message");
        let reservation = SeenReservation {
            seen: Arc::clone(seen),
            key,
            committed: false,
        };
        if out.send(msg).await.is_err() {
            // Subscriber gone: the rollback is moot but harmless.
            return false;
        }
        reservation.commit();
    }
    true
}

/// Run the lurker until `out` is closed (subscriber gone) or the peer
/// source ends. Emits every decoded GSOC/PSS message for the watch set.
///
/// `neighborhood_dial`, when present, is pinged each round with the
/// target so the swarm keeps peers resident in that neighborhood (the
/// dial primitive from the retrieval path). `peers` is the live
/// connected-peer snapshot the swarm publishes.
pub async fn run(
    control: Control,
    mut peers: watch::Receiver<Vec<(PeerId, Overlay)>>,
    neighborhood_dial: Option<mpsc::Sender<[u8; 32]>>,
    config: LurkerConfig,
    out: mpsc::Sender<DecodedMessage>,
) {
    let LurkerConfig { target, watch } = config;
    if watch
        .read()
        .unwrap_or_else(PoisonError::into_inner)
        .is_empty()
    {
        return;
    }
    let seen: Arc<Mutex<Seen>> = Arc::new(Mutex::new(Seen::new()));
    // Last position each (peer, bin) puller synced to, tagged with the
    // peer's reserve epoch, shared with the pullers. A replacement
    // puller resumes where its predecessor stopped instead of jumping to
    // the peer's *current* cursor — which would silently skip everything
    // that arrived during the outage. The epoch tag matters: a peer that
    // wiped its reserve resets its cursors, and resuming an old (now
    // absurdly high) position would park the puller above every new
    // binID forever. Bee's puller likewise drops saved intervals on an
    // epoch change; we drop the saved position and start fresh from the
    // new cursor.
    let positions: Positions = Arc::new(Mutex::new(HashMap::new()));
    // (peer, bin) pullers that have completed at least one successful
    // pull round — proof the stream actually opened and the bin is
    // covered. The coordinated handover retires an obsolete puller only
    // once every *replacement* it's covering for is in this set, so old
    // coverage is never dropped while its replacement is still dialing.
    let ready: ReadySet = Arc::new(Mutex::new(HashMap::new()));
    // Monotonic generation stamped on each spawned puller so its
    // ReadyGuard removes only its own readiness entry (see [`ReadyGuard`]).
    let mut next_generation: u64 = 0;

    // One long-lived puller per (peer, bin). Pullers run **continuously** —
    // we top up coverage for newly-connected peers on each tick but never
    // abort a live puller (except in the coordinated handover below, and
    // only once its replacement is ready), so a message is never dropped
    // in a gap while the driver re-dials (an earlier design
    // aborted-and-restarted every tick and lost messages that arrived
    // during the re-reside window).
    let mut active: HashMap<(PeerId, u8), AbortOnDrop> = HashMap::new();
    // When each currently-obsolete (not-desired) puller first became
    // obsolete, so the handover backstop can force-retire one that has
    // outlived HANDOVER_MAX_OVERLAP waiting for a wedged replacement.
    let mut obsolete_since: HashMap<(PeerId, u8), tokio::time::Instant> = HashMap::new();

    loop {
        if out.is_closed() {
            break;
        }
        // Keep dialing toward the target every pass. Pullers start
        // immediately on whatever peers are already connected (no
        // blocking reside phase — the first pullers matter for messages
        // arriving *now*), and coverage deepens as closer peers connect:
        // each connectivity change re-runs this top-up within
        // `TOPUP_DEBOUNCE`.
        if let Some(dial) = &neighborhood_dial {
            let _ = dial.try_send(target);
        }
        // Drop handles for pullers that ended (peer dropped / stream
        // died), and forget their readiness so a dead puller can't keep
        // satisfying the handover gate.
        active.retain(|_, h| !h.is_finished());
        ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|k, _| active.contains_key(k));

        // Which bins each covering peer needs depends on the watch
        // kinds (see `covering_bins`). Re-read each pass: the registry
        // may have added or removed GSOC addresses / PSS topics since
        // the last one, and the desired-set handover below then grows
        // or retires pullers to match.
        let (want_gsoc, want_pss, history) = {
            let w = watch.read().unwrap_or_else(PoisonError::into_inner);
            (
                !w.gsoc_addresses.is_empty(),
                !w.pss_topics.is_empty(),
                w.history,
            )
        };

        // Which covering (peer, bin)s do we want, and which peers still
        // need a cursor fetch to start a missing puller? Fetch those
        // cursors concurrently (bounded) so one silent peer can't stall
        // every other peer's coverage.
        let mut desired: HashSet<(PeerId, u8)> = HashSet::new();
        let mut need_cursors: Vec<(PeerId, Vec<u8>)> = Vec::new();
        for (peer_id, peer_overlay) in closest_n(&mut peers, &target, COVERING_PEERS) {
            let bins = covering_bins(proximity(&peer_overlay, &target), want_gsoc, want_pss);
            for &bin in &bins {
                desired.insert((peer_id, bin));
            }
            if bins.iter().any(|b| !active.contains_key(&(peer_id, *b))) {
                need_cursors.push((peer_id, bins));
            }
        }
        let fetches = need_cursors.into_iter().map(|(peer_id, bins)| {
            let mut ctl = control.clone();
            async move {
                let cursors = tokio::time::timeout(
                    CURSOR_FETCH_TIMEOUT,
                    pullsync::get_cursors(&mut ctl, peer_id),
                )
                .await;
                (peer_id, bins, cursors)
            }
        });
        let results = futures::future::join_all(fetches).await;

        for (peer_id, bins, cursors) in results {
            let Ok(Ok(cursors)) = cursors else {
                continue; // timed out or errored → try again next pass
            };
            for bin in bins {
                if active.contains_key(&(peer_id, bin)) {
                    continue;
                }
                let cursor = cursors.cursors.get(bin as usize).copied().unwrap_or(0);
                // Resume a replaced puller where it stopped — but only if
                // the peer's reserve epoch is unchanged. A wiped reserve
                // resets cursors, so an old position would resume above
                // the new binIDs and skip every fresh update. On an epoch
                // change (or a fresh (peer, bin)) start a short backlog
                // behind the current cursor instead.
                let resume = positions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&(peer_id, bin))
                    .copied()
                    .filter(|(epoch, _)| *epoch == cursors.epoch)
                    .map(|(_, start)| start);
                // A replaced puller resumes exactly where it stopped.
                // Otherwise: mailbox mode sweeps the whole bin backlog
                // (offline delivery); the default just tails live with a
                // small backlog to cover the reside/cursor-read window.
                let start = start_bin_id(resume, cursor, history);
                tracing::info!(
                    target: "ant_p2p::lurker",
                    peer = %peer_id, bin, start, epoch = cursors.epoch,
                    resumed = resume.is_some(), history,
                    "lurker pulling neighborhood bin",
                );
                next_generation += 1;
                let handle = tokio::spawn(pull_bin(
                    control.clone(),
                    peer_id,
                    bin,
                    start,
                    cursors.epoch,
                    next_generation,
                    Arc::clone(&watch),
                    Arc::clone(&seen),
                    Arc::clone(&positions),
                    Arc::clone(&ready),
                    out.clone(),
                ));
                active.insert((peer_id, bin), AbortOnDrop(handle));
            }
        }

        // Coordinated handover: a peer that fell out of the covering set
        // keeps its pullers until every desired (peer, bin) has a
        // **ready** puller (stream open, Get sent), then they're retired
        // — churn never gaps coverage, but the puller set can't grow
        // without bound as closest-peers turn over either.
        let now = tokio::time::Instant::now();
        // Track how long each obsolete puller has been obsolete; drop the
        // timers for pullers that are desired again or already gone.
        obsolete_since.retain(|k, _| active.contains_key(k) && !desired.contains(k));
        for k in active.keys() {
            if !desired.contains(k) {
                obsolete_since.entry(*k).or_insert(now);
            }
        }
        let all_ready = {
            let r = ready.lock().unwrap_or_else(PoisonError::into_inner);
            !desired.is_empty() && desired.iter().all(|k| r.contains_key(k))
        };
        let retired: Vec<(PeerId, u8)> = active
            .keys()
            .filter(|k| !desired.contains(*k))
            .filter(|k| {
                // Retire when replacements are all live, OR as a backstop
                // when this puller has been obsolete past the deadline
                // (a desired replacement that can never open a stream
                // must not pin obsolete coverage forever).
                all_ready
                    || obsolete_since
                        .get(*k)
                        .is_some_and(|t| now.duration_since(*t) >= HANDOVER_MAX_OVERLAP)
            })
            .copied()
            .collect();
        for k in retired {
            drop(active.remove(&k)); // AbortOnDrop aborts the puller
            obsolete_since.remove(&k);
        }
        ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|k, _| active.contains_key(k));

        // Wait for connectivity to change (top up new closer peers right
        // away) or the fallback tick. The pullers keep running
        // throughout — no gap.
        tokio::select! {
            () = out.closed() => break,
            changed = peers.changed() => {
                if changed.is_err() {
                    break; // peer source gone: node shutting down
                }
                tokio::time::sleep(TOPUP_DEBOUNCE).await;
            }
            () = tokio::time::sleep(RE_RESIDE_INTERVAL) => {}
        }
    }
    // `active` drops here; AbortOnDrop retires every remaining puller.
}

/// Live-pull one bin from one peer, forwarding decoded messages. Runs
/// until the peer's stream errors (peer likely gone → the driver drops
/// this puller) or the subscriber leaves. A round timeout is *not* fatal:
/// the server long-blocks on a quiet bin, so a timeout just means "no new
/// chunk yet" and we re-open at the same `start`.
#[allow(clippy::too_many_arguments)]
async fn pull_bin(
    mut control: Control,
    peer_id: PeerId,
    bin: u8,
    mut start: u64,
    epoch: u64,
    generation: u64,
    watch: SharedWatch,
    seen: Arc<Mutex<Seen>>,
    positions: Positions,
    ready: ReadySet,
    out: mpsc::Sender<DecodedMessage>,
) {
    // Whenever this puller exits — return, stream error, or handover
    // abort — its readiness is removed immediately (not just at the next
    // driver pass), so it can't leave a stale entry that falsely passes
    // the handover check during the multi-second cursor-fetch window.
    let _ready_guard = ReadyGuard {
        ready: Arc::clone(&ready),
        key: (peer_id, bin),
        generation,
    };
    // Consecutive timed-out rounds since the last sign of life — drives
    // the staleness probe below.
    let mut idle_rounds: u32 = 0;
    loop {
        if out.is_closed() {
            return;
        }
        // Clear readiness before every (re)open. On a quiet bin the
        // SYNC_ROUND_TIMEOUT fires and we loop back here to reopen; if
        // that reopen *wedges* (stream open hangs), the puller is alive
        // but no longer covering — leaving the previous round's readiness
        // set would let the handover retire valid coverage. `mark_ready`
        // re-sets it only once the new Get lands, so between reopen and a
        // fresh Get this puller doesn't count as ready. (The task-exit
        // ReadyGuard only fires when the whole task ends, which a wedge
        // inside the loop never triggers.)
        {
            let mut r = ready.lock().unwrap_or_else(PoisonError::into_inner);
            if r.get(&(peer_id, bin)) == Some(&generation) {
                r.remove(&(peer_id, bin));
            }
        }
        // Mark this (peer, bin) ready the moment the stream opens and the
        // Get is sent — *not* after the first page. On a quiet bin the
        // server holds the Offer indefinitely, so waiting for a page would
        // leave a legitimately-covering puller "not ready" forever,
        // stalling the handover and letting obsolete pullers pile up.
        let ready_c = Arc::clone(&ready);
        let mark_ready = move || {
            ready_c
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert((peer_id, bin), generation);
        };
        let round = pullsync::sync_once(
            &mut control,
            peer_id,
            bin,
            start,
            |o: &OfferedChunk| want(o, &watch.read().unwrap_or_else(PoisonError::into_inner)),
            mark_ready,
        );
        let page = match tokio::time::timeout(SYNC_ROUND_TIMEOUT, round).await {
            Ok(Ok(page)) => page,
            Ok(Err(e)) => {
                tracing::debug!(
                    target: "ant_p2p::lurker",
                    peer = %peer_id, bin, start, error = %e,
                    "pull round failed; dropping puller",
                );
                return; // stream/peer error → drop this puller
            }
            // Long-block timeout: normally just a quiet bin → re-open at
            // the same start. But every STALENESS_PROBE_ROUNDS of pure
            // silence, verify the peer is actually alive and still on
            // the reserve epoch we're pulling under — a wedged peer or a
            // wiped reserve looks *identical* to a quiet bin from here
            // and would otherwise hold this coverage slot forever.
            Err(_) => {
                idle_rounds += 1;
                if idle_rounds >= STALENESS_PROBE_ROUNDS {
                    idle_rounds = 0;
                    match pullsync::get_cursors(&mut control, peer_id).await {
                        Ok(c) if c.epoch == epoch => {} // alive, same reserve
                        outcome => {
                            tracing::debug!(
                                target: "ant_p2p::lurker",
                                peer = %peer_id, bin,
                                alive = outcome.is_ok(),
                                "staleness probe failed or epoch changed; rotating puller",
                            );
                            return; // driver re-picks a live peer
                        }
                    }
                }
                continue;
            }
        };
        idle_rounds = 0;
        tracing::debug!(
            target: "ant_p2p::lurker",
            peer = %peer_id, bin, start, topmost = page.topmost,
            delivered = page.chunks.len(),
            "pull round",
        );
        for chunk in &page.chunks {
            if !deliver_chunk(&seen, &watch, &out, &chunk.address, &chunk.data).await {
                return;
            }
        }
        start = page.topmost.saturating_add(1);
        // Publish how far this (peer, bin) got, tagged with the reserve
        // epoch it was read under, so a replacement puller resumes here
        // rather than skipping the gap — but only while the epoch holds
        // (the driver discards the position on an epoch change).
        positions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((peer_id, bin), (epoch, start));
    }
}

/// The bins to pull from one covering peer at proximity `b_p` to the
/// target, per watch kind. Small (a handful of entries), duplicates
/// removed, order irrelevant (each bin becomes its own puller).
///
/// - **GSOC**: the watch target *is* the chunk address, so
///   `PO(chunk, peer) = b_p` exactly — one bin.
/// - **PSS**: two regimes by the trie geometry (see
///   [`PSS_MINED_PREFIX_BITS`]), evaluated for *every* covered mined
///   prefix length `L` (the sender's choice is invisible to us) and
///   unioned: a peer shallower than `L` holds the trojan at exactly
///   `b_p`; a peer at or deeper than `L` holds it around bin `L`
///   (geometric tail), which can be *shallower* than `b_p` — the bug
///   the old `b_p..=b_p+window` selection had.
fn covering_bins(b_p: u8, want_gsoc: bool, want_pss: bool) -> Vec<u8> {
    let mut bins: Vec<u8> = Vec::new();
    let mut add = |bin: u8| {
        if !bins.contains(&bin) {
            bins.push(bin);
        }
    };
    if want_gsoc {
        add(b_p);
    }
    if want_pss {
        // The sender's prefix length is unknown to us: cover every
        // deliverable length (2- and 3-byte targets; see
        // PSS_MINED_PREFIX_BITS for why 1-byte is excluded — the
        // 1-byte trojans that agree with the target past bit b_p sit
        // at `b_p`, which the L = 24 pass below adds for any b_p < 24;
        // shallower ones are not received).
        for l in PSS_MINED_PREFIX_BITS {
            if b_p < l {
                // Deterministic regime: PO(c, p) = b_p exactly.
                add(b_p);
            } else {
                // Geometric regime: PO(c, p) = L + Geom(1/2),
                // independent of b_p.
                let top = l.saturating_add(PSS_BIN_WINDOW).min(MAX_BIN);
                for bin in l..=top {
                    add(bin);
                }
            }
        }
    }
    bins
}

/// The binID a fresh or resumed puller starts at.
///
/// - **Resume** (epoch-matched replacement puller): exactly where the
///   predecessor stopped — never re-pull, never gap.
/// - **Mailbox** (`history`, fresh puller): [`HISTORY_BACKLOG`] behind
///   the cursor — sweep the recent bin backlog so messages sent while
///   offline are recovered. The seen-set dedups the sweep against live
///   traffic.
/// - **Default** (fresh puller): a short [`PULL_BACKLOG`] behind the
///   cursor — covers the window between a storer accepting a chunk and
///   us reading its cursor, without pulling history.
fn start_bin_id(resume: Option<u64>, cursor: u64, history: bool) -> u64 {
    if let Some(start) = resume {
        return start;
    }
    let backlog = if history {
        HISTORY_BACKLOG
    } else {
        PULL_BACKLOG
    };
    cursor
        .saturating_add(1)
        .saturating_sub(backlog)
        .max(HISTORY_FLOOR)
}

/// The `n` connected peers whose overlays are closest to `target`,
/// deepest first. Marks the snapshot seen (`borrow_and_update`) so the
/// driver's `changed()` wait really waits for the *next* change.
fn closest_n(
    peers: &mut watch::Receiver<Vec<(PeerId, Overlay)>>,
    target: &[u8; 32],
    n: usize,
) -> Vec<(PeerId, Overlay)> {
    let mut v: Vec<(PeerId, Overlay)> = peers.borrow_and_update().clone();
    // Deepest proximity first (descending).
    v.sort_by_key(|(_, ov)| std::cmp::Reverse(proximity(ov, target)));
    v.truncate(n);
    v
}

/// Decide whether to request a chunk's delivery. GSOC is precise (exact
/// watched address); PSS needs the chunk body to attempt unwrap, so any
/// chunk is a candidate when PSS is enabled.
fn want(offered: &OfferedChunk, watch: &WatchState) -> bool {
    if watch.gsoc_addresses.contains(&offered.address) {
        return true;
    }
    // PSS: the trojan address isn't derivable, so any chunk is a candidate
    // once a topic is registered (topic-broadcast needs no node secret).
    !watch.pss_topics.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;

    fn overlay(first: u8) -> Overlay {
        let mut o = [0u8; 32];
        o[0] = first;
        o
    }

    /// A watched GSOC chunk plus the watch that matches it.
    fn watched_gsoc(payload: &[u8]) -> ([u8; 32], Vec<u8>, SharedWatch) {
        let identifier = ant_crypto::gsoc::identifier_from_string("lurker-test");
        let secret = ant_crypto::gsoc::gsoc_mine(&[0u8; 32], &identifier, 1).unwrap();
        let chunk = ant_crypto::gsoc::build_gsoc_chunk(&secret, &identifier, payload).unwrap();
        let watch: SharedWatch = Arc::new(RwLock::new(WatchState {
            gsoc_addresses: std::collections::HashSet::from([chunk.address]),
            ..WatchState::default()
        }));
        (chunk.address, chunk.wire, watch)
    }

    /// Blocker-1 regression: a puller aborted while `out.send` is parked
    /// on a full channel must NOT leave the chunk marked seen — the next
    /// covering peer's copy has to go through, or the message is lost
    /// for good (at-least-once).
    #[tokio::test]
    async fn aborted_delivery_rolls_back_the_seen_reservation() {
        let (address, data, watch) = watched_gsoc(b"must-not-vanish");
        let seen: Arc<Mutex<Seen>> = Arc::new(Mutex::new(Seen::new()));
        let (out, mut rx) = mpsc::channel::<DecodedMessage>(1);
        // Fill the channel so the delivery send parks.
        out.try_send(DecodedMessage::Pss {
            topic: [0u8; 32],
            message: vec![],
        })
        .unwrap();

        let task = {
            let (seen, watch, out) = (Arc::clone(&seen), Arc::clone(&watch), out.clone());
            let (address, data) = (address, data.clone());
            tokio::spawn(async move { deliver_chunk(&seen, &watch, &out, &address, &data).await })
        };
        // Let the task reach the parked send, then abort it (the
        // handover path).
        tokio::task::yield_now().await;
        task.abort();
        let _ = task.await;

        // The reservation must have rolled back: a second covering
        // peer's identical delivery still goes through.
        assert!(rx.try_recv().is_ok()); // drain the filler
        assert!(deliver_chunk(&seen, &watch, &out, &address, &data).await);
        match rx.try_recv() {
            Ok(DecodedMessage::Gsoc { payload, .. }) => {
                assert_eq!(payload, b"must-not-vanish");
            }
            other => panic!("expected the re-delivered GSOC message, got {other:?}"),
        }
    }

    /// A committed delivery stays seen: re-deliveries dedup as before.
    #[tokio::test]
    async fn committed_delivery_stays_deduped() {
        let (address, data, watch) = watched_gsoc(b"once-only");
        let seen: Arc<Mutex<Seen>> = Arc::new(Mutex::new(Seen::new()));
        let (out, mut rx) = mpsc::channel::<DecodedMessage>(4);
        assert!(deliver_chunk(&seen, &watch, &out, &address, &data).await);
        assert!(rx.try_recv().is_ok());
        // Second covering peer delivers the same version: deduped.
        assert!(deliver_chunk(&seen, &watch, &out, &address, &data).await);
        assert!(rx.try_recv().is_err());
    }

    /// PSS bins expected for one prefix length `l` alone (test oracle).
    fn pss_bins_for(b_p: u8, l: u8) -> Vec<u8> {
        if b_p < l {
            vec![b_p]
        } else {
            (l..=(l + PSS_BIN_WINDOW).min(MAX_BIN)).collect()
        }
    }

    fn sorted(mut v: Vec<u8>) -> Vec<u8> {
        v.sort_unstable();
        v
    }

    /// Covered prefix lengths are the 2- and 3-byte targets `/pss/send`
    /// takes; 1-byte (`L = 8`, below storage depth) is deliberately
    /// excluded (its in-neighbourhood fraction still arrives via bin
    /// `b_p`) — pulling bins 8..=11 costs a light node most of each
    /// storer's reserve ingest (R2-F1 on PR #54). The deepest covered
    /// `L` must track the API's max target length.
    #[test]
    fn pss_prefix_lengths_cover_deliverable_targets_only() {
        assert_eq!(PSS_MINED_PREFIX_BITS, [16, 24]);
        assert_eq!(
            PSS_MINED_PREFIX_BITS.iter().max().copied(),
            Some(8 * ant_crypto::pss::MAX_TARGET_LEN as u8)
        );
    }

    /// Viktor Trón's correction, deterministic regime: a covering peer
    /// SHALLOWER than every mined prefix (`b_p < 16`) holds the trojan at
    /// exactly bin `b_p` — the old `b_p..=b_p+3` window pulled three
    /// bins that cannot contain it.
    #[test]
    fn pss_bins_shallow_peer_is_exact() {
        assert_eq!(covering_bins(0, false, true), vec![0]);
        assert_eq!(covering_bins(7, false, true), vec![7]);
        // Light-node residency (b_p 8..=15): exactly bin b_p — no extra
        // bins (in particular not the full reserve bins 8..=11).
        assert_eq!(covering_bins(9, false, true), vec![9]);
        assert_eq!(covering_bins(11, false, true), vec![11]);
        assert_eq!(covering_bins(15, false, true), vec![15]);
    }

    /// Geometric regime: a covering peer AT or DEEPER than a mined
    /// prefix `L` holds that trojan at `L + Geom(1/2)` — independent of
    /// `b_p`, possibly SHALLOWER than it. Every accepted `L` is covered,
    /// since the receiver cannot know the sender's target length.
    #[test]
    fn pss_bins_deep_peer_pulls_every_mined_prefix_window() {
        for b_p in 0..=MAX_BIN {
            let got = sorted(covering_bins(b_p, false, true));
            for l in PSS_MINED_PREFIX_BITS {
                for bin in pss_bins_for(b_p, l) {
                    assert!(got.contains(&bin), "b_p={b_p} L={l}: missing bin {bin}");
                }
            }
        }
        // b_p = 20: 2-byte trojans at 16..=19, 3-byte ones exactly at 20.
        assert_eq!(covering_bins(20, false, true), vec![16, 17, 18, 19, 20]);
        // Very deep peer (co-resident rendezvous node): every window.
        assert_eq!(
            covering_bins(30, false, true),
            vec![16, 17, 18, 19, 24, 25, 26, 27]
        );
    }

    /// Regression (R1-F1 on PR #54): a trojan mined to a 3-byte target
    /// must be found on a covering peer at `b_p >= 20`, both where it
    /// sits deterministically (`b_p < 24`) and in the geometric regime.
    /// Drives real `proximity` on synthetic trojans.
    #[test]
    fn pss_three_byte_target_found_on_deep_peer() {
        let target = overlay(0xc7);
        let mut rng: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for b_p in 20u8..=31 {
            // Peer agreeing with the target for exactly b_p bits.
            let mut peer = target;
            peer[usize::from(b_p / 8)] ^= 0x80 >> (b_p % 8);
            assert_eq!(proximity(&peer, &target), b_p);
            let bins = covering_bins(b_p, false, true);
            let mut hit = 0;
            for _ in 0..2000 {
                // Trojan: 3-byte prefix from the target, mined noise after.
                let mut c = [0u8; 32];
                for chunk in c.chunks_mut(8) {
                    chunk.copy_from_slice(&next().to_le_bytes()[..chunk.len()]);
                }
                c[..3].copy_from_slice(&target[..3]);
                if bins.contains(&proximity(&c, &peer).min(MAX_BIN)) {
                    hit += 1;
                }
            }
            // b_p < 24 is exact; deeper peers catch 15/16 per peer.
            let floor = if b_p < 24 { 2000 } else { 1800 };
            assert!(hit >= floor, "b_p={b_p}: {hit}/2000");
        }
    }

    /// GSOC is unaffected by the correction: the watch target IS the
    /// chunk address, so `PO(chunk, peer) = b_p` exactly — one bin,
    /// window never applies.
    #[test]
    fn gsoc_bins_are_always_exact() {
        assert_eq!(covering_bins(5, true, false), vec![5]);
        assert_eq!(covering_bins(20, true, false), vec![20]);
    }

    /// A mixed watch (GSOC + PSS on one shared lurker) takes the UNION:
    /// the exact GSOC bin plus the PSS regime bins, deduplicated.
    #[test]
    fn mixed_watch_takes_the_union_of_both_kinds() {
        // Deep peer: GSOC bin 26 lands inside the L=24 window.
        let bins = covering_bins(26, true, true);
        assert_eq!(bins.len(), 8);
        assert!(bins.contains(&26));
        // b_p = 20: GSOC and the L=24 exact bin coincide.
        assert_eq!(covering_bins(20, true, true).len(), 5);
        // Shallow peer: both kinds want exactly b_p — deduplicated.
        assert_eq!(covering_bins(5, true, true), vec![5]);
    }

    /// Nothing watched → no bins (the driver skips idle watches
    /// upstream, but the function must not invent coverage).
    #[test]
    fn no_watch_no_bins() {
        assert!(covering_bins(12, false, false).is_empty());
    }

    /// Mailbox mode: a FRESH puller sweeps a bounded backlog behind the
    /// cursor (`HISTORY_BACKLOG`), recovering offline messages; the
    /// default only tails a short `PULL_BACKLOG`.
    #[test]
    fn start_bin_id_mailbox_sweeps_the_backlog_window() {
        // Deep cursor: history reaches HISTORY_BACKLOG back, default only PULL_BACKLOG.
        assert_eq!(
            start_bin_id(None, 50_000, true),
            50_000 + 1 - HISTORY_BACKLOG
        );
        assert_eq!(start_bin_id(None, 50_000, false), 50_000 + 1 - PULL_BACKLOG);
        // Sparse bin (fewer chunks than the window): backlog underflows
        // to the floor → the mailbox recovers the ENTIRE bin history.
        assert_eq!(start_bin_id(None, 500, true), HISTORY_FLOOR);
        // A near-empty bin can't go below the floor either way.
        assert_eq!(start_bin_id(None, 2, false), HISTORY_FLOOR);
    }

    /// A resumed (epoch-matched replacement) puller ALWAYS continues
    /// exactly where its predecessor stopped — mailbox mode must not
    /// rewind it to the floor and re-pull the whole backlog every
    /// handover.
    #[test]
    fn start_bin_id_resume_overrides_mailbox() {
        assert_eq!(start_bin_id(Some(12_345), 50_000, true), 12_345);
        assert_eq!(start_bin_id(Some(12_345), 50_000, false), 12_345);
    }

    #[test]
    fn closest_n_orders_deepest_first_and_marks_seen() {
        let target = overlay(0xff);
        let (tx, mut rx) = watch::channel(vec![
            (PeerId::random(), overlay(0x00)),
            (PeerId::random(), overlay(0xf0)), // shares top nibble → closest
            (PeerId::random(), overlay(0x80)),
        ]);
        let expect_closest = rx.borrow()[1].0;
        let picked = closest_n(&mut rx, &target, 2);
        assert_eq!(picked.len(), 2);
        assert_eq!(picked[0].1, overlay(0xf0));
        assert_eq!(picked[0].0, expect_closest);
        assert_eq!(picked[1].1, overlay(0x80));
        // borrow_and_update marked the snapshot seen.
        assert!(!rx.has_changed().unwrap());
        drop(tx);
    }

    #[test]
    fn seen_passes_gsoc_updates_and_dedups_exact_versions() {
        let mut seen = Seen::new();
        let addr = [0xaau8; 32];
        // First GSOC update at the stable address.
        assert!(seen.insert(&addr, b"update-1"));
        // The same version re-delivered by another covering peer: deduped.
        assert!(!seen.insert(&addr, b"update-1"));
        // A NEW update reusing the same address must pass — this is the
        // whole point of keying on (address, content), not address alone.
        assert!(seen.insert(&addr, b"update-2"));
        // An invalid/spoofed delivery must not poison the address for a
        // later legitimate version.
        assert!(seen.insert(&addr, b"garbage"));
        assert!(seen.insert(&addr, b"update-3"));
    }

    #[test]
    fn seen_evicts_oldest_first_not_wholesale() {
        let mut seen = Seen::new();
        let addr_for = |i: usize| {
            let mut a = [0u8; 32];
            a[..8].copy_from_slice(&(i as u64).to_le_bytes());
            a
        };
        for i in 0..=SEEN_CAP {
            assert!(seen.insert(&addr_for(i), b"x"));
        }
        // Only the single oldest entry was evicted; a recent one is
        // still deduped (a wholesale clear would forget it).
        assert!(!seen.insert(&addr_for(SEEN_CAP), b"x"));
        assert!(!seen.insert(&addr_for(1), b"x"));
        assert!(seen.insert(&addr_for(0), b"x"), "oldest was evicted");
        assert_eq!(seen.order.len(), seen.set.len());
        assert!(seen.set.len() <= SEEN_CAP + 1);
    }

    #[test]
    fn ready_guard_removes_own_entry_but_not_a_newer_generation() {
        let ready: ReadySet = Arc::new(Mutex::new(HashMap::new()));
        let key = (PeerId::random(), 12u8);

        // A puller (gen 1) marks itself ready.
        ready.lock().unwrap().insert(key, 1);
        // Its guard drops → its own entry is removed.
        {
            let _g = ReadyGuard {
                ready: Arc::clone(&ready),
                key,
                generation: 1,
            };
        }
        assert!(
            !ready.lock().unwrap().contains_key(&key),
            "guard must remove its own readiness on exit"
        );

        // A replacement puller (gen 2) is ready; a *stale* gen-1 guard
        // dropping late must NOT clobber the newer entry.
        ready.lock().unwrap().insert(key, 2);
        {
            let _stale = ReadyGuard {
                ready: Arc::clone(&ready),
                key,
                generation: 1,
            };
        }
        assert_eq!(
            ready.lock().unwrap().get(&key),
            Some(&2),
            "a stale-generation guard must not remove a newer puller's readiness"
        );
    }

    #[test]
    fn want_is_precise_for_gsoc_and_broad_for_pss() {
        let addr = [0x11u8; 32];
        let gsoc_only = WatchState {
            gsoc_addresses: HashSet::from([addr]),
            ..Default::default()
        };
        let offered = OfferedChunk {
            address: addr,
            batch_id: [0u8; 32],
            stamp_hash: [0u8; 32],
        };
        let other = OfferedChunk {
            address: [0x22u8; 32],
            batch_id: [0u8; 32],
            stamp_hash: [0u8; 32],
        };
        // GSOC-only: want the watched address, ignore others.
        assert!(want(&offered, &gsoc_only));
        assert!(!want(&other, &gsoc_only));

        // PSS enabled (even broadcast, no secret): any chunk is a
        // candidate — must download to attempt unwrap.
        let with_pss = WatchState {
            pss_topics: vec![[1u8; 32]],
            pss_secret: None,
            ..Default::default()
        };
        assert!(want(&other, &with_pss));
    }
}
