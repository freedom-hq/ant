//! Runtime handle the gateway uses to talk to the node loop.
//!
//! The gateway owns no node state itself; everything it needs is passed
//! in via `GatewayHandle`:
//!
//! - `status`: the same `watch::Receiver<StatusSnapshot>` `antctl status`
//!   reads from. Cheap to clone, so each request can call `.borrow()`.
//! - `commands`: the single-producer channel into the node loop, used to
//!   dispatch retrieval requests (`GetChunkRaw`, `GetBytes`, `GetBzz`).
//! - `identity`: the static bits resolved once at `antd` startup
//!   (overlay / ethereum address / libp2p public key). The gateway
//!   never derives these itself — keeping the identity loader in `antd`
//!   means the Web3 v3 keystore (PLAN.md D.3.2) can land later without
//!   touching `ant-gateway`.

use ant_control::{ControlCommand, GatewayActivity, StatusSnapshot};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::chain::ChainContext;
use crate::cors::CorsConfig;
use crate::tags::TagRegistry;

/// Static identity surface used by `/addresses`. All fields are owned
/// strings so cloning the handle is cheap and the gateway can drop the
/// original without affecting in-flight requests.
#[derive(Debug, Clone)]
pub struct GatewayIdentity {
    /// Swarm overlay as bare lowercase hex (no `0x` prefix), matching
    /// bee's `swarm.Address.MarshalJSON`.
    pub overlay_hex: String,
    /// 20-byte Ethereum address as `0x`-prefixed lowercase hex.
    pub ethereum_hex: String,
    /// Compressed secp256k1 public key (33 bytes) as bare lowercase hex.
    pub public_key_hex: String,
    /// libp2p peer-id, used internally for diagnostics / topology rows
    /// — not part of the bee `/addresses` payload.
    pub peer_id: String,
}

/// Embedder callback run after `POST /stamps` bought a batch and
/// registered it with the node (see [`GatewayHandle::on_batch_bought`]).
/// It receives the new batch id. It runs on the request's task, so it
/// must not block: spawn any chain work.
pub type BatchBoughtHook = Arc<dyn Fn([u8; 32]) + Send + Sync>;

/// Live wiring the gateway needs to serve every Tier-A endpoint.
///
/// Cloning is cheap: the `watch::Receiver`, `mpsc::Sender`, and `Arc`
/// fields are all reference-counted handles. Each request handler clones
/// the bits it cares about (status snapshot for status, commands sender
/// for retrieval) so axum's per-request handler runs without borrowing
/// the shared state.
#[derive(Clone)]
pub struct GatewayHandle {
    /// Daemon agent string (e.g. `"antd/0.1.0"`). Surfaced via
    /// `/health.version`. Wrapped in `Arc` so the cloned handle in
    /// every request handler doesn't realloc.
    pub agent: Arc<String>,
    /// Pinned bee API version we claim wire-compatibility against.
    /// Surfaced via `/health.apiVersion`. See PLAN.md §C.5.
    pub api_version: Arc<String>,
    /// Static identity bits.
    pub identity: Arc<GatewayIdentity>,
    /// Live status snapshot maintained by the node loop. Cloning the
    /// receiver is constant-time; `.borrow()` returns a guard with no
    /// allocation.
    pub status: watch::Receiver<StatusSnapshot>,
    /// Dispatch channel into the node loop for retrieval and
    /// peerstore commands.
    pub commands: mpsc::Sender<ControlCommand>,
    /// Live registry the gateway writes every in-flight retrieval
    /// request into. The same `Arc` is held by the daemon's status
    /// publisher, which snapshots it for `StatusSnapshot::retrieval`
    /// so `antop` can render the Retrieval tab.
    pub activity: Arc<GatewayActivity>,
    /// Whether the node is running in **light** mode (uploads + SWAP
    /// settlement configured) versus **ultra-light** (read-only) — see
    /// [`GatewayChainState::light_mode`]. Together with the chain
    /// reader it is filled once by `antd` when the startup chain init
    /// resolves, which happens *after* the HTTP listener binds so the
    /// API answers from t≈0 instead of refusing connections for the
    /// whole chain-init window (issue #38). Endpoints that need the
    /// contents answer `503 chain init in progress` until it's set;
    /// everything snapshot-driven (`/health`, `/peers`, `/topology`,
    /// retrieval) works from the start.
    pub chain_state: Arc<std::sync::OnceLock<GatewayChainState>>,
    /// In-process upload-tag registry backing `POST /tags` and
    /// `GET /tags/{uid}`. bee auto-creates a tag on every `POST /bzz`
    /// / `POST /bytes` and returns its uid in the `Swarm-Tag-Uid`
    /// response header; bee-js then polls `GET /tags/{uid}` for upload
    /// progress (PLAN.md J.2.4 / C3). Because the gateway's uploads are
    /// synchronous (every chunk is pushsynced before the response),
    /// the tag is marked fully synced as soon as it's created.
    pub tags: Arc<TagRegistry>,
    /// CORS policy applied to every response. Driven by bee's
    /// `cors-allowed-origins` config (PLAN.md J.4.8): Freedom sets it to
    /// `"null"` because its dweb pages `fetch`/upload from an opaque
    /// (`null`) origin. With the default (deny) config no CORS headers
    /// are emitted, matching a bee node started without the option.
    pub cors: Arc<CorsConfig>,
    /// The node's secp256k1 signing secret, used as the ACT
    /// **publisher** identity — exactly bee, whose access-control
    /// session is `accesscontrol.NewDefaultSession(swarmPrivateKey)`
    /// (`cmd/bee/cmd/start.go`). Every `swarm-act` upload and
    /// `/grantee` mutation derives its session keys from this secret,
    /// and `identity.public_key_hex` must be its public key so clients
    /// can read the publisher from `GET /addresses`.
    pub act_secret: Arc<[u8; 32]>,
    /// Called after a successful `POST /stamps` buy. A buy makes the
    /// node publish-capable, but uploads also need outbound SWAP
    /// settlement (a chequebook), and the gateway can't set that up
    /// itself. `ant-ffi` uses this to resolve or deploy the chequebook
    /// and switch settlement on, so a first buy in a fresh app session
    /// doesn't upload without paying peers. `None` does nothing (`antd`
    /// resolves its chequebook at startup).
    pub on_batch_bought: Option<BatchBoughtHook>,
}

/// The chain-derived slice of gateway wiring, resolved by `antd`'s
/// startup Gnosis reads (postage batch validation/rediscovery,
/// chequebook resolution) and installed into
/// [`GatewayHandle::chain_state`] once available.
#[derive(Clone)]
pub struct GatewayChainState {
    /// Whether the node is running in **light** mode (uploads + SWAP
    /// settlement configured) versus **ultra-light** (read-only).
    /// Drives `GET /node.beeMode`, which Freedom's
    /// `checkSwarmPreFlight` gates every publish/feed/SOC write on —
    /// an `ultra-light` answer makes Freedom refuse to publish at all
    /// (PLAN.md J.4.1).
    pub light_mode: bool,
    /// Optional live Gnosis chain reader + addresses backing
    /// `/wallet`, `/chequebook/*`, `/status`, `/chainstate` (PLAN.md J.5
    /// A2/A3/D1/D2). `None` on a node started without an RPC endpoint:
    /// balances then report the bee zero-stub and the chain-state
    /// endpoints fall to `501`, matching bee with no configured backend.
    pub chain: Option<Arc<ChainContext>>,
}

impl GatewayChainState {
    /// Wrap into the pre-resolved slot shape [`GatewayHandle`] wants —
    /// for embedders (ant-ffi) and tests whose chain wiring is known
    /// at handle-construction time and never arrives late.
    #[must_use]
    pub fn preset(self) -> Arc<std::sync::OnceLock<GatewayChainState>> {
        Arc::new(std::sync::OnceLock::from(self))
    }
}

impl GatewayHandle {
    /// The chain-derived wiring, or `None` while the daemon's startup
    /// chain init is still resolving. Handlers that need it should
    /// return [`crate::error::chain_initializing`] on `None` — a
    /// retryable `503`, not a wrong-but-plausible stub answer.
    #[must_use]
    pub fn chain_state(&self) -> Option<&GatewayChainState> {
        self.chain_state.get()
    }

    /// Convenience: the chain reader, when chain init has resolved AND
    /// an RPC endpoint is configured. Callers that must distinguish
    /// "still initializing" from "no RPC configured" use
    /// [`Self::chain_state`] directly.
    #[must_use]
    pub fn chain(&self) -> Option<Arc<ChainContext>> {
        self.chain_state().and_then(|s| s.chain.clone())
    }
}
