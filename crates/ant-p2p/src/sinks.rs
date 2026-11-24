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
//!    which dials `/swarm/hive/1.1.0/peers` to us. On failure bee calls
//!    `Disconnect(peer, "failed broadcasting to peer")`.
//!
//! For pricing we drain and close — that's all bee wants. For hive we parse
//! the protobuf (`Peers { repeated BzzAddress }`) and forward each underlay
//! multiaddr to the swarm loop via an mpsc channel. That turns bee's
//! bookkeeping chatter into free peer discovery: bootnodes broadcast their
//! neighbourhood to us the moment our BZZ handshake finishes.
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
use crate::underlay::deserialize_underlays;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::StreamExt;
use libp2p::{Multiaddr, PeerId, StreamProtocol};
use libp2p_stream::{Control, IncomingStreams};
use libp2p_swarm::Stream;
use prost::Message;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;
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

/// A decoded hive `BzzAddress` reduced to the fields the swarm loop needs
/// to dial the peer. The overlay is carried through so the swarm loop can
/// correlate hive hints with handshake outcomes in future work.
#[derive(Debug, Clone)]
pub struct PeerHint {
    pub peer_id: PeerId,
    pub addrs: Vec<Multiaddr>,
    #[allow(dead_code)]
    pub overlay: [u8; 32],
}

/// hive.proto `message Peers { repeated BzzAddress peers = 1; }`.
#[derive(Clone, PartialEq, prost::Message)]
struct PeersPb {
    #[prost(message, repeated, tag = "1")]
    peers: Vec<BzzAddressPb>,
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
/// length-delimited list. Signature and Nonce are required on the
/// wire (both empty is a decode error for bee) but we ignore them:
/// we don't verify overlays for hints.
#[derive(Clone, PartialEq, prost::Message)]
struct BzzAddressPb {
    #[prost(bytes = "vec", tag = "1")]
    underlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    signature: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    overlay: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    nonce: Vec<u8>,
    /// Bee 2.8+. Unix seconds of the signed `BzzAddress` record. We
    /// currently ignore the value but accept it for forward-compat.
    #[prost(int64, tag = "5")]
    #[allow(dead_code)]
    timestamp: i64,
    /// Bee 2.8+. Peer's chequebook contract address (20 bytes) or
    /// empty when the peer has no chequebook. We accept it for
    /// forward-compat without acting on it: hive hints feed the
    /// dialer, which still re-verifies via the BZZ handshake itself.
    #[prost(bytes = "vec", tag = "6")]
    #[allow(dead_code)]
    chequebook_address: Vec<u8>,
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
}

/// Claim every bee protocol we need to silently drain. Must run before the
/// first outbound handshake completes: if bee's `ConnectIn` races ahead of
/// our registration, the first peer gets kicked and we start the backoff
/// loop for nothing.
///
/// `gate` is the swarm loop's view of who is handshaked; every sink
/// resets streams from anyone else (see the module docs).
pub fn register(
    control: &mut Control,
    hint_tx: mpsc::Sender<PeerHint>,
    gate: HandshakeGate,
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
    tokio::spawn(run_pricing(reg.pricing, guard.clone()));
    tokio::spawn(run_hive(reg.hive_v1, reg.hint_tx.clone(), guard.clone()));
    tokio::spawn(run_hive(reg.hive_v2, reg.hint_tx, guard.clone()));
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

async fn run_hive(incoming: IncomingStreams, hint_tx: mpsc::Sender<PeerHint>, guard: InboundGuard) {
    serve_gated(incoming, guard, "hive", move |peer_id, stream| {
        let tx = hint_tx.clone();
        async move {
            let res = tokio::time::timeout(STREAM_TIMEOUT, drain_stream(stream, HIVE_MAX)).await;
            match res {
                Ok(Ok(d)) => {
                    let hints = decode_hive_peers(&d.body);
                    trace!(
                        target: "ant_p2p::hive",
                        %peer_id,
                        headers = d.headers,
                        body = d.body.len(),
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

/// Decode a hive `Peers` payload into `PeerHint`s. Entries without a
/// `/p2p/<peer_id>` component in their underlay are dropped: libp2p's `Dial`
/// needs a peer id to multiplex concurrent dials, and bee's hive
/// `broadcastPeers` always includes one, so a missing id is a corrupted peer.
fn decode_hive_peers(body: &[u8]) -> Vec<PeerHint> {
    let Ok(msg) = PeersPb::decode(body) else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(msg.peers.len());
    for p in msg.peers {
        let Ok(addrs) = deserialize_underlays(&p.underlay) else {
            continue;
        };
        // All underlays of a single BzzAddress describe the same peer, so
        // pull the peer id from the first one that has it.
        let Some(peer_id) = addrs.iter().find_map(extract_peer_id) else {
            continue;
        };
        let mut overlay = [0u8; 32];
        if p.overlay.len() == 32 {
            overlay.copy_from_slice(&p.overlay);
        }
        out.push(PeerHint {
            peer_id,
            addrs,
            overlay,
        });
    }
    out
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

    #[test]
    fn decodes_single_peer_hive_message() {
        let addr: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmP9b7MxjyEfrJrch5jUThmuFaGzvUPpWEJewCpx5Ln6i8"
                .parse()
                .unwrap();
        let underlay = serialize_underlays(std::slice::from_ref(&addr));
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![0u8; 65],
                overlay: vec![0xab; 32],
                nonce: vec![0xcd; 32],
                ..Default::default()
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let hints = decode_hive_peers(&buf);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![addr]);
        assert_eq!(hints[0].overlay, [0xab; 32]);
    }

    #[test]
    fn decodes_multi_underlay_peer() {
        let a4: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        let a6: Multiaddr =
            "/ip6/2001:db8::1/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        let underlay = serialize_underlays(&[a4.clone(), a6.clone()]);
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![],
                overlay: vec![0u8; 32],
                nonce: vec![],
                ..Default::default()
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let hints = decode_hive_peers(&buf);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![a4, a6]);
    }

    /// #279: hostile underlays in hive gossip (an overflowing list
    /// length, a bare list prefix) drop that one peer, not the process
    /// and not the rest of the batch.
    #[test]
    fn skips_peers_with_hostile_underlay_lists() {
        let good: Multiaddr =
            "/ip4/1.2.3.4/tcp/1634/p2p/QmTxX73q8dDiVbmXU7GqMNwG3gWmjSFECuMoCsTW4xp6CK"
                .parse()
                .unwrap();
        let mut overflow = vec![0x99];
        overflow.extend_from_slice(&[0xff; 9]);
        overflow.push(0x01);
        let peer = |underlay: Vec<u8>| BzzAddressPb {
            underlay,
            overlay: vec![0u8; 32],
            ..Default::default()
        };
        let msg = PeersPb {
            peers: vec![
                peer(overflow),
                peer(vec![0x99]),
                peer(serialize_underlays(std::slice::from_ref(&good))),
            ],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let hints = decode_hive_peers(&buf);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![good]);
    }

    #[test]
    fn decodes_bee_2_8_payload_with_new_tags() {
        // A bee-2.8 hive Peers payload carries Timestamp (tag 5) and
        // ChequebookAddress (tag 6) inside each BzzAddress. We don't
        // act on either yet but the decoder must still produce the
        // correct hint (dropping the new fields cleanly).
        let addr: Multiaddr =
            "/ip4/4.3.2.1/tcp/1634/p2p/QmP9b7MxjyEfrJrch5jUThmuFaGzvUPpWEJewCpx5Ln6i8"
                .parse()
                .unwrap();
        let underlay = serialize_underlays(std::slice::from_ref(&addr));
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![0u8; 65],
                overlay: vec![0xab; 32],
                nonce: vec![0xcd; 32],
                timestamp: 1_715_000_000,
                chequebook_address: vec![0xef; 20],
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let hints = decode_hive_peers(&buf);
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].addrs, vec![addr]);
        assert_eq!(hints[0].overlay, [0xab; 32]);
    }

    #[test]
    fn drops_peers_without_p2p_id() {
        let bare: Multiaddr = "/ip4/1.2.3.4/tcp/1634".parse().unwrap();
        let underlay = serialize_underlays(&[bare]);
        let msg = PeersPb {
            peers: vec![BzzAddressPb {
                underlay,
                signature: vec![],
                overlay: vec![0u8; 32],
                nonce: vec![],
                ..Default::default()
            }],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert!(decode_hive_peers(&buf).is_empty());
    }
}
