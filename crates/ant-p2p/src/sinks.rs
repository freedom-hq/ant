//! Inbound-stream sinks for bee protocols that would otherwise cause an
//! immediate post-handshake disconnect.
//!
//! After the BZZ handshake, bee runs two things that can tear our connection
//! down if we're not prepared:
//!
//! 1. **`ConnectIn`** callbacks on each registered p2p protocol (libp2p-go
//!    `p2p.ProtocolSpec`). Only `pricing` and `pseudosettle` expose one
//!    in bee 2.7.x and 2.8.0, and of those only [`pricing`] opens a
//!    stream back to us — `/swarm/pricing/1.0.0/pricing` — and returns
//!    an error if the stream can't be negotiated. Bee's stream handler
//!    catches that as `Disconnect(overlay, "failed to process inbound
//!    connection notifier")`.
//!
//! 2. **Kademlia's `Announce`** then calls
//!    `discovery.BroadcastPeers(ctx, peer, addrs...)` on every new peer,
//!    which dials hive's `peers` stream to us (`/swarm/hive/2.0.0/peers`
//!    since bee 2.8, `1.1.0` before). On failure bee calls
//!    `Disconnect(peer, "failed broadcasting to peer")`.
//!
//! For pricing we drain and close — that's all bee wants. For hive we parse
//! the protobuf (`Peers { repeated BzzAddress }`) and check each entry the
//! way bee's `bzz.ParseAddress` does — a decodable underlay with a
//! `/p2p/<peer id>`, a signature that recovers to the declared overlay,
//! and on 2.0.0 a sane timestamp (see [`verify_hive_entries`]). Only entries
//! that pass are forwarded to the swarm loop as hints, via an mpsc channel.
//! Each check costs a signature recovery, so a peer gets bee's own hive
//! budget of them ([`HiveBudget`]); entries past it are drained unread.
//! That turns bee's bookkeeping chatter into free peer discovery:
//! bootnodes broadcast their neighbourhood to us the moment our BZZ
//! handshake finishes.
//!
//! # The bee-headers framing
//!
//! `bee` wraps every protocol stream (except `/swarm/handshake/...`) with a
//! mandatory pair of length-delimited `pb.Headers` messages before the
//! actual protocol payload. The dialer (bee, when it calls
//! `streamer.NewStream` and then writes pricing / hive payload) goes:
//!
//! ```text
//! dialer → listener : varint + Headers pb        // bee sendHeaders
//! listener → dialer : varint + Headers pb        // our response
//! dialer → listener : varint + <protocol body>   // e.g. pricing.AnnouncePaymentThreshold
//! ```
//!
//! Headers are typed `map<string,bytes>`; bee passes `nil` for pricing and
//! hive so the encoded `pb.Headers` is zero bytes long. We respond with the
//! same empty message. Dropping the stream before responding causes bee's
//! `sendHeaders` to see `io.EOF` on its read, which bubbles up as
//! `ConnectIn` failure on pricing and triggers an immediate disconnect.
//!
//! # Who may open these streams (#300)
//!
//! Every sink here is only meaningful *after* a BZZ handshake, so each
//! accept loop runs through [`serve_gated`]: a stream from a peer we have
//! no handshake with (or in flight) is reset on the spot, before any task
//! is spawned for it. A peer whose handshake is still in flight gets
//! [`HANDSHAKE_GATE_WAIT`] to finish, because bee opens pricing from its
//! side of the handshake, which can land a moment before our swarm loop
//! has recorded the outcome. Spawned stream tasks are capped globally
//! ([`MAX_SINK_TASKS`]) and per peer ([`MAX_SINK_TASKS_PER_PEER`]); a
//! stream over either cap is reset too.
//!
//! [`pricing`]: https://github.com/ethersphere/bee/blob/v2.7.1/pkg/pricing/pricing.go
//! [`bee-headers`]: https://github.com/ethersphere/bee/blob/v2.7.1/pkg/p2p/libp2p/headers.go

use crate::dial::extract_peer_id;
use crate::handshake::check_wire_timestamp;
use crate::underlay::deserialize_underlays;
use ant_crypto::{verify_bzz_address_signature, HandshakeWireVersion, OVERLAY_NONCE_LEN};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use libp2p::{Multiaddr, PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use libp2p_swarm::Stream;
use prost::Message;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
use tracing::{debug, trace, warn};

/// bee `pkg/pricing`, sends a single `AnnouncePaymentThreshold` (<= ~256 bits).
pub const PROTOCOL_PRICING: &str = "/swarm/pricing/1.0.0/pricing";
/// bee 2.8.0 `pkg/hive`, sends a `Peers{ repeated BzzAddress }` with up to
/// 30 entries. The protocol id bumped from `1.1.0` (bee 2.7.x) to `2.0.0`
/// in the same `peer validation bundle` commit that reshaped the handshake
/// — bee disconnects peers it can't broadcast hive gossip to (`"failed
/// broadcasting to peer"`), so we must claim both ids to stay connected
/// across the cutover.
pub const PROTOCOL_HIVE_PEERS_V2: &str = "/swarm/hive/2.0.0/peers";
pub const PROTOCOL_HIVE_PEERS_V1: &str = "/swarm/hive/1.1.0/peers";
/// Default protocol id used by external callers. Points at the new bee
/// 2.8 id now that ant 0.5.0 is on the V15 handshake.
#[allow(dead_code)]
pub const PROTOCOL_HIVE_PEERS: &str = PROTOCOL_HIVE_PEERS_V2;

/// Tight bound for the pricing payload — it's a single big.Int.
const PRICING_MAX: usize = 1024;
/// Bee's hive `maxBatchSize`: it never puts more peers in one `Peers`
/// message. Each entry costs us a signature recovery, so a message
/// carrying more than this only has its first entries looked at.
const HIVE_MAX_PEERS: usize = 30;
/// Bee's hive inbound limiter (`limitBurst = 4 * swarm.MaxBins`): a peer
/// may hand us this many entries in a burst. Bee's outbound limiter for
/// the same peer is identical, so an honest bee never sends more.
const HIVE_ENTRY_BURST: u64 = 128;
/// Bee's hive `limitRate`: the burst refills one entry per this long.
const HIVE_ENTRY_REFILL: Duration = Duration::from_secs(60);
/// Past this many tracked peers, [`HiveBudget`] forgets the ones whose
/// budget has refilled in full (they'd start from a full budget anyway).
const HIVE_BUDGET_PRUNE_AT: usize = 4096;
/// Hive batches up to 30 peers × (overlay + underlay + sig + nonce) per stream.
/// Round up to a comfortable cap so we don't reject valid gossip from bee.
const HIVE_MAX: usize = 128 * 1024;
/// bee caps its own headers exchange at `defaultHeadersRWTimeout = 10s`. We
/// never send non-trivial headers so a much tighter cap is safe here — but
/// giving bee the same 8 KiB window it uses lets us stay compatible with
/// tracing, settlement, and any future non-empty headers payload.
pub const HEADERS_MAX: usize = 8 * 1024;
/// Per-stream wall-clock cap; bee's `pricing.AnnouncePaymentThreshold` uses
/// a 5 s context and its headers exchange a 10 s one, so 15 s is enough to
/// survive both without letting a stuck stream leak forever.
pub const STREAM_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a stream from a peer whose BZZ handshake is still in flight
/// waits for the swarm loop to record the outcome before it is reset.
/// Bee opens pricing (`ConnectIn`) as soon as *its* side of our handshake
/// completes, which is a moment before our driver task hands the result
/// back to the swarm loop. Kept well under the 5 s context bee gives
/// `AnnouncePaymentThreshold`.
pub const HANDSHAKE_GATE_WAIT: Duration = Duration::from_secs(2);
/// Inbound sink streams being served at once, across all peers and all
/// sink protocols. Bee opens one pricing and one hive stream per
/// handshake and the odd pseudosettle / swap stream afterwards, each
/// done in milliseconds, so this is far above steady-state use.
pub const MAX_SINK_TASKS: usize = 256;
/// Inbound sink streams one peer may have served at once, so a single
/// handshaked peer can't take every [`MAX_SINK_TASKS`] permit and get
/// everyone else's pricing stream reset (which makes bee disconnect).
pub const MAX_SINK_TASKS_PER_PEER: usize = 8;

/// Where a peer stands in our BZZ handshake, as far as the inbound sinks
/// are concerned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerPhase {
    /// Connected and our outbound handshake is in flight.
    Pending,
    /// Handshake done; the peer is in the BZZ peer set.
    Ready,
}

/// Shared view of which peers have (or are finishing) a BZZ handshake.
/// Written by the swarm loop, read by the sink accept loops. Cheap to
/// clone; every clone sees the same set.
#[derive(Clone, Default)]
pub struct HandshakeGate(Arc<GateInner>);

#[derive(Default)]
struct GateInner {
    peers: Mutex<HashMap<PeerId, PeerPhase>>,
    changed: Notify,
}

impl HandshakeGate {
    /// Record `peer`'s phase; `None` forgets it (disconnected, or the
    /// handshake failed).
    pub fn set(&self, peer: PeerId, phase: Option<PeerPhase>) {
        let changed = {
            let mut peers = self.0.peers.lock().expect("handshake gate poisoned");
            match phase {
                Some(p) => peers.insert(peer, p) != Some(p),
                None => peers.remove(&peer).is_some(),
            }
        };
        if changed {
            self.0.changed.notify_waiters();
        }
    }

    pub fn phase(&self, peer: &PeerId) -> Option<PeerPhase> {
        self.0
            .peers
            .lock()
            .expect("handshake gate poisoned")
            .get(peer)
            .copied()
    }

    /// Wait up to `wait` for `peer` to become [`PeerPhase::Ready`].
    /// Returns `false` as soon as the peer is forgotten, or when the wait
    /// runs out with the handshake still pending.
    pub async fn wait_ready(&self, peer: &PeerId, wait: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            // Register before reading, so a change between the read and
            // the await still wakes us.
            notified.as_mut().enable();
            match self.phase(peer) {
                Some(PeerPhase::Ready) => return true,
                None => return false,
                Some(PeerPhase::Pending) => {}
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.phase(peer) == Some(PeerPhase::Ready);
            }
        }
    }
}

/// Admission control for inbound sink streams: the handshake gate plus
/// the global and per-peer task caps. One guard is shared by every sink.
#[derive(Clone)]
pub struct InboundGuard {
    gate: HandshakeGate,
    tasks: Arc<Semaphore>,
    per_peer: Arc<Mutex<HashMap<PeerId, usize>>>,
    per_peer_cap: usize,
}

impl InboundGuard {
    pub fn new(gate: HandshakeGate) -> Self {
        Self::with_caps(gate, MAX_SINK_TASKS, MAX_SINK_TASKS_PER_PEER)
    }

    fn with_caps(gate: HandshakeGate, total: usize, per_peer: usize) -> Self {
        Self {
            gate,
            tasks: Arc::new(Semaphore::new(total)),
            per_peer: Arc::new(Mutex::new(HashMap::new())),
            per_peer_cap: per_peer,
        }
    }

    /// Decide on a fresh inbound stream from `peer` without waiting.
    fn admit(&self, peer: PeerId) -> Result<Admission, Refusal> {
        let phase = self.gate.phase(&peer).ok_or(Refusal::NotHandshaked)?;
        let slot = {
            let mut counts = self.per_peer.lock().expect("sink peer counts poisoned");
            let n = counts.entry(peer).or_insert(0);
            if *n >= self.per_peer_cap {
                return Err(Refusal::PeerBusy);
            }
            *n += 1;
            PeerSlot {
                counts: self.per_peer.clone(),
                peer,
            }
        };
        let permit = self
            .tasks
            .clone()
            .try_acquire_owned()
            .map_err(|_| Refusal::Busy)?;
        Ok(Admission {
            gate: self.gate.clone(),
            peer,
            pending: phase == PeerPhase::Pending,
            _slot: slot,
            _permit: permit,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Refusal {
    NotHandshaked,
    PeerBusy,
    Busy,
}

impl Refusal {
    const fn as_str(self) -> &'static str {
        match self {
            Self::NotHandshaked => "peer not handshaked",
            Self::PeerBusy => "per-peer sink cap reached",
            Self::Busy => "sink task cap reached",
        }
    }
}

/// Releases one per-peer slot on drop.
struct PeerSlot {
    counts: Arc<Mutex<HashMap<PeerId, usize>>>,
    peer: PeerId,
}

impl Drop for PeerSlot {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().expect("sink peer counts poisoned");
        if let Some(n) = counts.get_mut(&self.peer) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                counts.remove(&self.peer);
            }
        }
    }
}

/// A stream let in by [`InboundGuard::admit`]; holds its task permit and
/// per-peer slot until dropped.
struct Admission {
    gate: HandshakeGate,
    peer: PeerId,
    pending: bool,
    _slot: PeerSlot,
    _permit: OwnedSemaphorePermit,
}

impl Admission {
    /// `true` once the peer is handshaked; a peer that was still pending
    /// gets [`HANDSHAKE_GATE_WAIT`] to get there.
    async fn ready(&self) -> bool {
        !self.pending || self.gate.wait_ready(&self.peer, HANDSHAKE_GATE_WAIT).await
    }
}

/// Accept loop shared by every inbound sink. A stream the [`InboundGuard`]
/// refuses is dropped right here, with no task spawned; dropping an open
/// yamux stream resets it. An admitted stream is served by `handle` on
/// its own task, once its peer is handshaked.
pub(crate) async fn serve_gated<F, Fut>(
    mut incoming: IncomingStreams,
    guard: InboundGuard,
    protocol: &'static str,
    handle: F,
) where
    F: Fn(PeerId, Stream) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    while let Some((peer_id, stream)) = incoming.next().await {
        let admission = match guard.admit(peer_id) {
            Ok(a) => a,
            Err(why) => {
                debug!(
                    target: "ant_p2p::sinks",
                    %peer_id,
                    protocol,
                    "resetting inbound stream: {}",
                    why.as_str(),
                );
                drop(stream);
                continue;
            }
        };
        // An async fn's future does nothing until polled, so building it
        // here only moves the stream in; dropping it unpolled resets it.
        let serve = handle(peer_id, stream);
        tokio::spawn(async move {
            if admission.ready().await {
                serve.await;
            } else {
                debug!(
                    target: "ant_p2p::sinks",
                    %peer_id,
                    protocol,
                    "resetting inbound stream: handshake did not complete in {:?}",
                    HANDSHAKE_GATE_WAIT,
                );
            }
            drop(admission);
        });
    }
}

/// Per-peer budget of hive entries we spend a signature recovery on,
/// mirroring bee v2.8.0's hive `inLimiter` (a token bucket of
/// [`HIVE_ENTRY_BURST`] entries refilling one per [`HIVE_ENTRY_REFILL`],
/// charged by entry count). Where bee resets an over-budget stream, we
/// still drain it and only skip checking the entries past the budget:
/// a reset makes bee disconnect us. A peer's budget is kept across
/// reconnects, so redialing doesn't buy a fresh burst.
#[derive(Clone, Default)]
pub(crate) struct HiveBudget {
    buckets: Arc<Mutex<HashMap<PeerId, HiveBucket>>>,
}

#[derive(Clone, Copy)]
struct HiveBucket {
    tokens: u64,
    refilled_at: Instant,
}

impl HiveBucket {
    /// Credit the refills due by `now`; returns the tokens held.
    fn refill(&mut self, now: Instant) -> u64 {
        let periods =
            now.saturating_duration_since(self.refilled_at).as_secs() / HIVE_ENTRY_REFILL.as_secs();
        if periods > 0 {
            self.tokens = self.tokens.saturating_add(periods).min(HIVE_ENTRY_BURST);
            self.refilled_at = if self.tokens == HIVE_ENTRY_BURST {
                now
            } else {
                self.refilled_at + HIVE_ENTRY_REFILL * periods as u32
            };
        }
        self.tokens
    }
}

impl HiveBudget {
    /// Spend up to `want` entries of `peer`'s budget; returns how many
    /// may be checked.
    pub(crate) fn take(&self, peer: PeerId, want: usize, now: Instant) -> usize {
        let mut buckets = self.buckets.lock().expect("hive budget poisoned");
        if buckets.len() >= HIVE_BUDGET_PRUNE_AT && !buckets.contains_key(&peer) {
            buckets.retain(|_, b| b.refill(now) < HIVE_ENTRY_BURST);
        }
        let bucket = buckets.entry(peer).or_insert(HiveBucket {
            tokens: HIVE_ENTRY_BURST,
            refilled_at: now,
        });
        let granted = bucket.refill(now).min(want as u64);
        bucket.tokens -= granted;
        granted as usize
    }
}

/// A decoded hive `BzzAddress` reduced to the fields the swarm loop needs
/// to dial the peer. The overlay is what neighbourhood and bin-balance
/// dials rank the peer by.
#[derive(Debug, Clone)]
pub struct PeerHint {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
    /// From a hive entry this is a signature-verified overlay, though
    /// nothing binds it to `peer_id` (the swarm loop's
    /// `enqueue_gossip_hint` handles that); a warm hint carries the
    /// overlay its last handshake verified.
    pub overlay: [u8; 32],
}

/// hive.proto `message Peers { repeated BzzAddress peers = 1; }`.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct PeersPb {
    #[prost(message, repeated, tag = "1")]
    pub(crate) peers: Vec<BzzAddressPb>,
}

/// hive.proto `message BzzAddress`. Bee 2.7 ships tags 1-4
/// (`Underlay`, `Signature`, `Overlay`, `Nonce`); bee 2.8 added tag 5
/// (`Timestamp`) and tag 6 (`ChequebookAddress`) alongside the
/// peer-validation-bundle handshake change. Prost ignores unknown
/// fields on decode, so the new tags would parse correctly even
/// without listing them here — but we surface them for parity with
/// the bee schema and so future changes (e.g. dialing prioritised by
/// fresh-timestamp gossip) can read them without a second wire
/// migration.
///
/// Underlay is `bzz.SerializeUnderlays(addrs)` — either raw multiaddr
/// bytes (single address, backward-compat) or the 0x99-prefixed
/// length-delimited list. Every entry is checked like bee's
/// `ParseAddress` before it becomes a hint (see [`verify_hive_entries`]):
/// the signature over underlay ‖ overlay ‖ network id (‖ nonce ‖
/// timestamp ‖ chequebook on 2.0.0) must recover to a key whose overlay
/// is the declared one.
#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct BzzAddressPb {
    #[prost(bytes = "vec", tag = "1")]
    pub(crate) underlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub(crate) signature: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub(crate) overlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub(crate) nonce: Vec<u8>,
    /// Bee 2.8+. Unix seconds of the signed `BzzAddress` record. Part
    /// of the signed preimage on hive 2.0.0, and must be positive and
    /// not in the future there.
    #[prost(int64, tag = "5")]
    pub(crate) timestamp: i64,
    /// Bee 2.8+. Peer's chequebook contract address (20 bytes) or
    /// empty when the peer has no chequebook. Part of the signed
    /// preimage on hive 2.0.0; otherwise not acted on.
    #[prost(bytes = "vec", tag = "6")]
    pub(crate) chequebook_address: Vec<u8>,
}

/// Handles on the `IncomingStreams` we need to keep alive while the node runs.
#[must_use = "register() must be paired with a spawn() call to drive the sinks"]
pub struct Registrations {
    pricing: IncomingStreams,
    hive_v1: IncomingStreams,
    hive_v2: IncomingStreams,
    pseudosettle: IncomingStreams,
    swap: IncomingStreams,
    hint_tx: mpsc::Sender<PeerHint>,
    guard: InboundGuard,
    network_id: u64,
}

/// Claim every bee protocol we need to silently drain. Must run before the
/// first outbound handshake completes: if bee's `ConnectIn` races ahead of
/// our registration, the first peer gets kicked and we start the backoff
/// loop for nothing.
///
/// `gate` is the swarm loop's view of who is handshaked; every sink
/// resets streams from anyone else (see the module docs). `network_id`
/// is the one hive entries' signatures are checked against.
pub fn register(
    control: &mut Control,
    hint_tx: mpsc::Sender<PeerHint>,
    gate: HandshakeGate,
    network_id: u64,
) -> Registrations {
    let pricing = control
        .accept(StreamProtocol::new(PROTOCOL_PRICING))
        .expect("pricing protocol registered exactly once");
    // Bee 2.8 bumped hive to 2.0.0; 2.7.x peers still dial 1.1.0. Claim
    // both ids so we keep talking to whichever side of the cutover the
    // remote is on — both speak the same protobuf shape (with tags 5 +
    // 6 added in 2.8, which prost already tolerates as unknown fields
    // on the 1.1.0 path).
    let hive_v2 = control
        .accept(StreamProtocol::new(PROTOCOL_HIVE_PEERS_V2))
        .expect("hive 2.0.0 protocol registered exactly once");
    let hive_v1 = control
        .accept(StreamProtocol::new(PROTOCOL_HIVE_PEERS_V1))
        .expect("hive 1.1.0 protocol registered exactly once");
    let pseudosettle = control
        .accept(StreamProtocol::new(
            crate::pseudosettle::PROTOCOL_PSEUDOSETTLE,
        ))
        .expect("pseudosettle protocol registered exactly once");
    let swap = control
        .accept(StreamProtocol::new(crate::swap::PROTOCOL_SWAP))
        .expect("swap protocol registered exactly once");
    Registrations {
        pricing,
        hive_v1,
        hive_v2,
        pseudosettle,
        swap,
        hint_tx,
        guard: InboundGuard::new(gate),
        network_id,
    }
}

/// Drive every long-lived sink task spawned by [`register`].
///
/// The swap listener is wired to a [`SwapWiring`] when the operator
/// configured a chequebook (we know our beneficiary EOA + chain id);
/// without that wiring we still register the protocol — silently
/// draining cheques and rejecting them as "no chequebook configured"
/// — so peers that emit one don't see an unsupported-protocol
/// disconnect on the libp2p layer.
pub fn spawn(reg: Registrations, swap: Option<SwapWiring>) {
    let guard = reg.guard;
    // One budget for both hive versions: a peer can't double it by
    // opening the other protocol id.
    let budget = HiveBudget::default();
    tokio::spawn(run_pricing(reg.pricing, guard.clone()));
    // Hive 1.1.0 (bee 2.7) carries records signed over the V14 preimage,
    // hive 2.0.0 (bee 2.8) over the V15 one.
    tokio::spawn(run_hive(
        reg.hive_v1,
        reg.hint_tx.clone(),
        guard.clone(),
        HandshakeWireVersion::V14,
        reg.network_id,
        budget.clone(),
    ));
    tokio::spawn(run_hive(
        reg.hive_v2,
        reg.hint_tx,
        guard.clone(),
        HandshakeWireVersion::V15,
        reg.network_id,
        budget,
    ));
    tokio::spawn(crate::pseudosettle::run_inbound(
        reg.pseudosettle,
        guard.clone(),
    ));
    match swap {
        Some(s) => {
            tokio::spawn(crate::swap::run_inbound(
                reg.swap, guard, s.ledger, s.events,
            ));
        }
        None => {
            tokio::spawn(crate::swap::drain_inbound_unconfigured(reg.swap, guard));
        }
    }
}

/// Configuration for the inbound swap listener spawned by [`spawn`].
/// The operator hands in the credit ledger (already loaded with any
/// previously-accepted cheques), and an optional event channel that
/// the listener publishes accepted cheques to.
pub struct SwapWiring {
    pub ledger: std::sync::Arc<crate::swap::CreditLedger>,
    pub events: Option<mpsc::Sender<crate::swap::InboundCheque>>,
}

async fn run_pricing(incoming: IncomingStreams, guard: InboundGuard) {
    serve_gated(
        incoming,
        guard,
        PROTOCOL_PRICING,
        |peer_id, stream| async move {
            let res = tokio::time::timeout(STREAM_TIMEOUT, drain_stream(stream, PRICING_MAX)).await;
            match res {
                Ok(Ok(d)) => trace!(
                    target: "ant_p2p::pricing",
                    %peer_id, headers = d.headers, body = d.body.len(), "drained"
                ),
                Ok(Err(e)) => debug!(target: "ant_p2p::pricing", %peer_id, "drain failed: {e}"),
                Err(_) => warn!(
                    target: "ant_p2p::pricing",
                    %peer_id,
                    "stream timed out after {}s",
                    STREAM_TIMEOUT.as_secs(),
                ),
            }
        },
    )
    .await;
}

async fn run_hive(
    incoming: IncomingStreams,
    hint_tx: mpsc::Sender<PeerHint>,
    guard: InboundGuard,
    version: HandshakeWireVersion,
    network_id: u64,
    budget: HiveBudget,
) {
    serve_gated(incoming, guard, "hive", move |peer_id, stream| {
        let tx = hint_tx.clone();
        let budget = budget.clone();
        async move {
            let res = tokio::time::timeout(STREAM_TIMEOUT, drain_stream(stream, HIVE_MAX)).await;
            match res {
                Ok(Ok(d)) => {
                    let (headers, body_len) = (d.headers, d.body.len());
                    let mut entries = PeersPb::decode(d.body.as_slice())
                        .map(|m| m.peers)
                        .unwrap_or_default();
                    entries.truncate(HIVE_MAX_PEERS);
                    let granted = budget.take(peer_id, entries.len(), Instant::now());
                    if granted < entries.len() {
                        debug!(
                            target: "ant_p2p::hive",
                            %peer_id,
                            skipped = entries.len() - granted,
                            "hive budget spent; not checking the rest of the message",
                        );
                        entries.truncate(granted);
                    }
                    // Up to `HIVE_MAX_PEERS` signature recoveries: keep
                    // them off the async workers.
                    let hints = if entries.is_empty() {
                        Vec::new()
                    } else {
                        tokio::task::spawn_blocking(move || {
                            verify_hive_entries(&entries, version, network_id)
                        })
                        .await
                        .unwrap_or_default()
                    };
                    trace!(
                        target: "ant_p2p::hive",
                        %peer_id,
                        headers,
                        ?version,
                        body = body_len,
                        peers = hints.len(),
                        "drained",
                    );
                    for hint in hints {
                        // Bounded channel — drop on backpressure rather than
                        // stalling the hive handler (and blocking bee).
                        if let Err(mpsc::error::TrySendError::Full(dropped)) = tx.try_send(hint) {
                            trace!(
                                target: "ant_p2p::hive",
                                %peer_id,
                                dropped_peer = %dropped.peer_id,
                                "hint channel full",
                            );
                        }
                    }
                }
                Ok(Err(e)) => debug!(target: "ant_p2p::hive", %peer_id, "drain failed: {e}"),
                Err(_) => warn!(
                    target: "ant_p2p::hive",
                    %peer_id,
                    "stream timed out after {}s",
                    STREAM_TIMEOUT.as_secs(),
                ),
            }
        }
    })
    .await;
}

/// Turn decoded hive entries into `PeerHint`s, checking each the way
/// bee's hive does with `bzz.ParseAddress`: the underlay must decode to a
/// non-empty list, and the signature over the record (V14 preimage on
/// hive 1.1.0, V15 with timestamp and chequebook on 2.0.0) must recover
/// to a key whose overlay is the declared one; on 2.0.0 the timestamp
/// must be positive and not in the future. An entry that fails is dropped
/// on its own, so a forged record never becomes a hint. Entries without a
/// `/p2p/<peer_id>` component in their underlay are dropped too: libp2p's
/// `Dial` needs a peer id to multiplex concurrent dials, and bee's hive
/// `broadcastPeers` always includes one. Only the first
/// [`HIVE_MAX_PEERS`] entries are looked at, and `run_hive` passes no
/// more than the sender's [`HiveBudget`] allows.
///
/// The signature proves the overlay, not the `/p2p/<peer id>`: bee's
/// libp2p key is separate from its overlay key, so a peer can sign a
/// record of its own naming someone else's peer id. The swarm loop's
/// `enqueue_gossip_hint` refuses such a hint when it holds another
/// overlay for that peer id, so it can't displace a real dial-book entry.
fn verify_hive_entries(
    peers: &[BzzAddressPb],
    version: HandshakeWireVersion,
    network_id: u64,
) -> Vec<PeerHint> {
    peers
        .iter()
        .take(HIVE_MAX_PEERS)
        .filter_map(|p| verified_hint(p, version, network_id))
        .collect()
}

/// A hive `Peers` payload through [`verify_hive_entries`], as `run_hive`
/// does it (less the budget).
#[cfg(test)]
pub(crate) fn decode_hive_peers(
    body: &[u8],
    version: HandshakeWireVersion,
    network_id: u64,
) -> Vec<PeerHint> {
    let Ok(msg) = PeersPb::decode(body) else {
        return Vec::new();
    };
    verify_hive_entries(&msg.peers, version, network_id)
}

/// One hive entry as a hint, or `None` when it fails any check of
/// [`verify_hive_entries`].
fn verified_hint(
    p: &BzzAddressPb,
    version: HandshakeWireVersion,
    network_id: u64,
) -> Option<PeerHint> {
    let addrs = deserialize_underlays(&p.underlay).ok()?;
    // All underlays of a single BzzAddress describe the same peer, so
    // pull the peer id from the first one that has it.
    let peer_id = addrs.iter().find_map(extract_peer_id)?;
    let overlay: [u8; 32] = p.overlay.as_slice().try_into().ok()?;
    let signature: [u8; 65] = p.signature.as_slice().try_into().ok()?;
    let nonce: [u8; OVERLAY_NONCE_LEN] = p.nonce.as_slice().try_into().ok()?;
    let timestamp = match version {
        HandshakeWireVersion::V14 => 0,
        HandshakeWireVersion::V15 => {
            if let Err(e) = check_wire_timestamp(p.timestamp) {
                trace!(target: "ant_p2p::hive", %peer_id, "dropping hive entry: {e}");
                return None;
            }
            p.timestamp
        }
    };
    if let Err(e) = verify_bzz_address_signature(
        version,
        &p.underlay,
        &overlay,
        &signature,
        &nonce,
        network_id,
        timestamp,
        &p.chequebook_address,
    ) {
        trace!(target: "ant_p2p::hive", %peer_id, "dropping unverified hive entry: {e}");
        return None;
    }
    Some(PeerHint {
        peer_id,
        addrs,
        overlay,
    })
}

#[derive(Debug)]
struct Drained {
    headers: usize,
    body: Vec<u8>,
}

async fn drain_stream(mut stream: Stream, max: usize) -> std::io::Result<Drained> {
    let headers = read_delimited(&mut stream, HEADERS_MAX).await?;
    // Bee sends its headers first, then blocks reading ours inside
    // `sendHeaders`. An empty `pb.Headers` encodes to 0 bytes, so a varint
    // length of 0 is enough to make bee's `ReadMsgWithContext` succeed and
    // unblock the write of the protocol body.
    write_empty_delimited(&mut stream).await?;

    let body = read_delimited(&mut stream, max).await?;

    // Half-close so bee's `stream.FullClose()` (`CloseWrite` +
    // `io.Copy(io.Discard, stream)`) unblocks promptly. Pricing uses
    // FullClose; hive uses `stream.Close()` which also waits for our
    // close-write via yamux flow control, so this is required on both.
    let _ = stream.close().await;

    // Drain anything bee sends before its own close and return cleanly on
    // either EOF or an already-reset stream — yamux reports the latter as
    // `UnexpectedEof` on macOS / `ConnectionReset` on Linux when the peer
    // resets the stream right after its final write.
    let mut tail = [0u8; 64];
    loop {
        match stream.read(&mut tail).await {
            Ok(0) => {
                return Ok(Drained {
                    headers: headers.len(),
                    body,
                });
            }
            Ok(_) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                ) =>
            {
                return Ok(Drained {
                    headers: headers.len(),
                    body,
                });
            }
            Err(e) => return Err(e),
        }
    }
}

async fn read_delimited<R: AsyncReadExt + Unpin>(
    r: &mut R,
    max: usize,
) -> std::io::Result<Vec<u8>> {
    let len = read_varint_len(r).await?;
    if len > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("message too large: {len} bytes (cap {max})"),
        ));
    }
    // Grows with the bytes that arrive, not the declared `len`, so a peer
    // can't pin the cap per stream by declaring a frame and stalling (#203).
    ant_retrieval::frame::read_frame_body(r, len).await
}

async fn write_empty_delimited<W: AsyncWriteExt + Unpin>(w: &mut W) -> std::io::Result<()> {
    // varint(0) — single zero byte, length prefix for an empty `pb.Headers`.
    w.write_all(&[0u8]).await?;
    w.flush().await?;
    Ok(())
}

async fn read_varint_len<R: AsyncReadExt + Unpin>(r: &mut R) -> std::io::Result<usize> {
    let mut byte = [0u8; 1];
    let mut acc: Vec<u8> = Vec::with_capacity(10);
    loop {
        r.read_exact(&mut byte).await?;
        acc.push(byte[0]);
        match unsigned_varint::decode::u64(&acc) {
            Ok((v, [])) => {
                return usize::try_from(v).map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "varint overflow")
                });
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "invalid varint framing",
                ));
            }
            Err(unsigned_varint::decode::Error::Insufficient) => {
                if acc.len() > 10 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "varint too long",
                    ));
                }
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("varint: {e}"),
                ));
            }
        }
    }
}

/// A hive entry for `addrs`, signed by `secret` like bee signs its
/// `BzzAddress` (V15 records stamped now).
#[cfg(test)]
pub(crate) fn signed_hive_entry(
    secret: &[u8; 32],
    version: HandshakeWireVersion,
    network_id: u64,
    addrs: &[Multiaddr],
) -> BzzAddressPb {
    let underlay = crate::underlay::serialize_underlays(addrs);
    let timestamp = match version {
        HandshakeWireVersion::V14 => 0,
        HandshakeWireVersion::V15 => crate::handshake::tests_now(),
    };
    let (overlay, signature, nonce) =
        crate::handshake::sign_test_record(secret, version, network_id, &underlay, timestamp);
    BzzAddressPb {
        underlay,
        signature: signature.to_vec(),
        overlay: overlay.to_vec(),
        nonce: nonce.to_vec(),
        timestamp,
        chequebook_address: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::underlay::serialize_underlays;

    /// A pending peer that never completes its handshake is refused once
    /// `HANDSHAKE_GATE_WAIT` runs out; one that completes, or is
    /// forgotten, resolves at once.
    #[tokio::test(start_paused = true)]
    async fn gate_waits_for_pending_peers_then_gives_up() {
        let gate = HandshakeGate::default();
        let peer = PeerId::random();
        assert!(
            !gate.wait_ready(&peer, HANDSHAKE_GATE_WAIT).await,
            "unknown"
        );

        gate.set(peer, Some(PeerPhase::Pending));
        let started = tokio::time::Instant::now();
        assert!(!gate.wait_ready(&peer, HANDSHAKE_GATE_WAIT).await);
        assert_eq!(started.elapsed(), HANDSHAKE_GATE_WAIT);

        for (outcome, ready) in [(Some(PeerPhase::Ready), true), (None, false)] {
            gate.set(peer, Some(PeerPhase::Pending));
            let g = gate.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                g.set(peer, outcome);
            });
            let started = tokio::time::Instant::now();
            assert_eq!(gate.wait_ready(&peer, HANDSHAKE_GATE_WAIT).await, ready);
            assert_eq!(started.elapsed(), Duration::from_millis(100));
        }
    }

    /// Unknown peers are refused outright; one peer can't take more than
    /// its share of task slots, and the global cap holds across peers.
    /// A finished stream gives its slots back.
    #[test]
    fn guard_caps_tasks_per_peer_and_in_total() {
        let gate = HandshakeGate::default();
        let guard = InboundGuard::with_caps(gate.clone(), 3, 2);
        let (a, b, c) = (PeerId::random(), PeerId::random(), PeerId::random());
        assert_eq!(guard.admit(a).err(), Some(Refusal::NotHandshaked));
        for p in [a, b, c] {
            gate.set(p, Some(PeerPhase::Ready));
        }
        let a1 = guard.admit(a).unwrap();
        let _a2 = guard.admit(a).unwrap();
        assert_eq!(guard.admit(a).err(), Some(Refusal::PeerBusy));
        let _b1 = guard.admit(b).unwrap();
        assert_eq!(guard.admit(c).err(), Some(Refusal::Busy));
        drop(a1);
        let _c1 = guard.admit(c).unwrap();
        assert_eq!(guard.admit(a).err(), Some(Refusal::Busy));
    }

    /// A frame that declares more bytes than the peer sends fails with
    /// `UnexpectedEof` (the body buffer only grows with bytes received).
    #[tokio::test]
    async fn read_delimited_rejects_short_body() {
        let mut wire: &[u8] = &[100, 0xAA, 0xBB, 0xCC]; // says 100, carries 3
        let err = read_delimited(&mut wire, 1024).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    /// A complete frame reads back exactly and leaves trailing bytes on
    /// the stream for the next read.
    #[tokio::test]
    async fn read_delimited_reads_exact_frame() {
        let mut wire: &[u8] = &[3, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE];
        let body = read_delimited(&mut wire, 1024).await.unwrap();
        assert_eq!(body, vec![0xAA, 0xBB, 0xCC]);
        assert_eq!(wire, &[0xDD, 0xEE]);
    }

    const NET: u64 = 1;

    fn addr_of(peer: PeerId, ip: &str) -> Multiaddr {
        format!("/{ip}/tcp/1634/p2p/{peer}").parse().unwrap()
    }

    fn encode(peers: Vec<BzzAddressPb>) -> Vec<u8> {
        let mut buf = Vec::new();
        PeersPb { peers }.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn decodes_signed_v15_peer() {
        let peer = PeerId::random();
        let addr = addr_of(peer, "ip4/1.2.3.4");
        let entry = signed_hive_entry(
            &[5; 32],
            HandshakeWireVersion::V15,
            NET,
            std::slice::from_ref(&addr),
        );
        let overlay = entry.overlay.clone();

        let hints = decode_hive_peers(&encode(vec![entry]), HandshakeWireVersion::V15, NET);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].peer_id, peer);
        assert_eq!(hints[0].addrs, vec![addr]);
        assert_eq!(hints[0].overlay.as_slice(), overlay.as_slice());
    }

    #[test]
    fn decodes_signed_v14_multi_underlay_peer() {
        let peer = PeerId::random();
        let a4 = addr_of(peer, "ip4/1.2.3.4");
        let a6 = addr_of(peer, "ip6/2001:db8::1");
        let entry = signed_hive_entry(
            &[5; 32],
            HandshakeWireVersion::V14,
            NET,
            &[a4.clone(), a6.clone()],
        );
        let hints = decode_hive_peers(&encode(vec![entry]), HandshakeWireVersion::V14, NET);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![a4, a6]);
    }

    /// #299 (P2P-12): a forged entry, one signed for another network or
    /// over the other hive version's preimage, or one carrying a bad
    /// timestamp produces no hint, and doesn't take the valid entry next
    /// to it down with it.
    #[test]
    fn forged_hive_entries_produce_no_hint() {
        let v15 = HandshakeWireVersion::V15;
        let good_peer = PeerId::random();
        let good = signed_hive_entry(&[5; 32], v15, NET, &[addr_of(good_peer, "ip4/1.2.3.4")]);
        let entry = || {
            signed_hive_entry(
                &[6; 32],
                v15,
                NET,
                &[addr_of(PeerId::random(), "ip4/5.6.7.8")],
            )
        };

        // A victim's overlay with an attacker's underlay: the signature no
        // longer covers the record.
        let mut stolen_overlay = entry();
        stolen_overlay.overlay.clone_from(&good.overlay);
        // Someone else's signed record with the underlay swapped out.
        let mut swapped_underlay = entry();
        swapped_underlay.underlay =
            serialize_underlays(&[addr_of(PeerId::random(), "ip4/6.6.6.6")]);
        let unsigned = BzzAddressPb {
            signature: vec![0u8; 65],
            ..entry()
        };
        let no_signature = BzzAddressPb {
            signature: Vec::new(),
            ..entry()
        };
        let other_network = signed_hive_entry(
            &[6; 32],
            v15,
            NET + 1,
            &[addr_of(PeerId::random(), "ip4/5.6.7.8")],
        );
        let v14_signed = signed_hive_entry(
            &[6; 32],
            HandshakeWireVersion::V14,
            NET,
            &[addr_of(PeerId::random(), "ip4/5.6.7.8")],
        );
        let mut zero_ts = entry();
        zero_ts.timestamp = 0;
        // A future timestamp, validly signed.
        let mut future = entry();
        let ts = future.timestamp + 3600;
        let (overlay, sig, nonce) =
            crate::handshake::sign_test_record(&[6; 32], v15, NET, &future.underlay, ts);
        future.overlay = overlay.to_vec();
        future.signature = sig.to_vec();
        future.nonce = nonce.to_vec();
        future.timestamp = ts;

        let forged = vec![
            stolen_overlay,
            swapped_underlay,
            unsigned,
            no_signature,
            other_network,
            v14_signed,
            zero_ts,
            future,
        ];
        for (i, f) in forged.iter().enumerate() {
            assert!(
                decode_hive_peers(&encode(vec![f.clone()]), v15, NET).is_empty(),
                "forged entry {i} produced a hint"
            );
        }
        let mut all = forged;
        all.push(good);
        let hints = decode_hive_peers(&encode(all), v15, NET);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].peer_id, good_peer);
    }

    /// #279: hostile underlays in hive gossip (an overflowing list
    /// length, a bare list prefix) drop that one peer, not the process
    /// and not the rest of the batch — even when the hostile bytes are
    /// what the record's signature covers.
    #[test]
    fn skips_peers_with_hostile_underlay_lists() {
        let v15 = HandshakeWireVersion::V15;
        let good = addr_of(PeerId::random(), "ip4/1.2.3.4");
        let mut overflow = vec![0x99];
        overflow.extend_from_slice(&[0xff; 9]);
        overflow.push(0x01);
        let signed_raw = |underlay: Vec<u8>| {
            let ts = crate::handshake::tests_now();
            let (overlay, sig, nonce) =
                crate::handshake::sign_test_record(&[6; 32], v15, NET, &underlay, ts);
            BzzAddressPb {
                underlay,
                signature: sig.to_vec(),
                overlay: overlay.to_vec(),
                nonce: nonce.to_vec(),
                timestamp: ts,
                chequebook_address: Vec::new(),
            }
        };
        let body = encode(vec![
            signed_raw(overflow),
            signed_raw(vec![0x99]),
            signed_hive_entry(&[5; 32], v15, NET, std::slice::from_ref(&good)),
        ]);
        let hints = decode_hive_peers(&body, v15, NET);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![good]);
    }

    /// Bee never batches more than `HIVE_MAX_PEERS` entries per message;
    /// past that we don't spend signature recoveries.
    #[test]
    fn looks_at_most_hive_max_peers_entries() {
        let v15 = HandshakeWireVersion::V15;
        let entries: Vec<_> = (0..HIVE_MAX_PEERS + 5)
            .map(|_| {
                signed_hive_entry(
                    &[5; 32],
                    v15,
                    NET,
                    &[addr_of(PeerId::random(), "ip4/1.2.3.4")],
                )
            })
            .collect();
        assert_eq!(
            decode_hive_peers(&encode(entries), v15, NET).len(),
            HIVE_MAX_PEERS
        );
    }

    /// R1-M1: a peer gets bee's hive budget of signature checks — a
    /// burst of `HIVE_ENTRY_BURST` entries refilling one per
    /// `HIVE_ENTRY_REFILL` — kept per peer and capped at the burst.
    #[test]
    fn hive_budget_mirrors_bee_inbound_limiter() {
        let budget = HiveBudget::default();
        let (peer, other) = (PeerId::random(), PeerId::random());
        let t0 = Instant::now();
        let msg = HIVE_MAX_PEERS;
        let full_msgs = HIVE_ENTRY_BURST as usize / msg;
        for _ in 0..full_msgs {
            assert_eq!(budget.take(peer, msg, t0), msg);
        }
        let rest = HIVE_ENTRY_BURST as usize - full_msgs * msg;
        assert_eq!(budget.take(peer, msg, t0), rest);
        assert_eq!(budget.take(peer, msg, t0), 0);
        // Another peer has its own budget.
        assert_eq!(budget.take(other, msg, t0), msg);
        // One entry per refill period, not more.
        assert_eq!(budget.take(peer, msg, t0 + HIVE_ENTRY_REFILL / 2), 0);
        assert_eq!(budget.take(peer, msg, t0 + HIVE_ENTRY_REFILL), 1);
        assert_eq!(budget.take(peer, msg, t0 + HIVE_ENTRY_REFILL * 3), 2);
        // A long silence refills to the burst, no further.
        let later = t0 + HIVE_ENTRY_REFILL * 10_000;
        assert_eq!(
            budget.take(peer, 10 * HIVE_ENTRY_BURST as usize, later),
            HIVE_ENTRY_BURST as usize
        );
    }

    /// The budget map stays bounded: past `HIVE_BUDGET_PRUNE_AT` peers,
    /// those whose budget has refilled in full are forgotten, those still
    /// owing are kept.
    #[test]
    fn hive_budget_forgets_refilled_peers() {
        let budget = HiveBudget::default();
        let t0 = Instant::now();
        let owing = PeerId::random();
        for _ in 0..HIVE_BUDGET_PRUNE_AT - 1 {
            budget.take(PeerId::random(), 1, t0);
        }
        budget.take(owing, HIVE_ENTRY_BURST as usize, t0);
        let later = t0 + HIVE_ENTRY_REFILL * 2;
        budget.take(PeerId::random(), 1, later);
        let buckets = budget.buckets.lock().unwrap();
        assert_eq!(buckets.len(), 2);
        assert!(buckets.contains_key(&owing));
    }

    #[test]
    fn drops_peers_without_p2p_id() {
        let bare: Multiaddr = "/ip4/1.2.3.4/tcp/1634".parse().unwrap();
        let entry = signed_hive_entry(&[5; 32], HandshakeWireVersion::V15, NET, &[bare]);
        assert!(decode_hive_peers(&encode(vec![entry]), HandshakeWireVersion::V15, NET).is_empty());
    }
}
