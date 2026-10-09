//! Shared test helpers for `ant-gateway` integration tests.
//!
//! These tests don't stand up a real listener / node; they build the
//! same `axum::Router` production uses via [`testkit::build_router`] and
//! drive it through `tower::ServiceExt::oneshot`. Two flavours of
//! handle:
//!
//! - [`status_only_router`] hands the router a fake `StatusSnapshot`
//!   and an `mpsc` receiver that's deliberately left dangling — useful
//!   for endpoint groups (status, stubs, fallback) that never dispatch
//!   a `ControlCommand`.
//! - [`handle_with_fixture_node`] additionally spawns a fake "node loop"
//!   task that handles `GetBzz` / `GetBytes` / `GetChunkRaw` against the
//!   live `bzz_fixture.rs` directory in `ant-retrieval/tests/fixtures`,
//!   exercising the production code paths end-to-end through the HTTP
//!   layer.
//!
//! Cargo compiles each `tests/*.rs` as its own crate, so any helper
//! that isn't called by *one specific* binary will warn. The whole
//! module is marked dead-code-allowed for that reason; the upstream
//! `cargo test -p ant-gateway` invocation still exercises every helper
//! across the test binaries.

#![allow(dead_code)]

/// Fixed ACT publisher secret for the gateway test fixtures — any
/// valid secp256k1 scalar works; tests that exercise the ACT surface
/// derive the matching public key from this.
pub const TEST_ACT_SECRET: [u8; 32] = [0xa7; 32];

use ant_control::{
    ControlAck, ControlCommand, GatewayActivity, IdentityInfo, PeerConnectionInfo, PeerInfo,
    RetrievalInfo, RoutingInfo, StatusSnapshot, StreamRange, PROTOCOL_VERSION,
};
use ant_gateway::testkit::build_router;
use ant_gateway::{
    ChainContext, CorsConfig, GatewayChainState, GatewayHandle, GatewayIdentity, TagRegistry,
};
use ant_retrieval::{
    join_to_sender_range, join_with_options, list_manifest, lookup_path, ByteRange, ChunkFetcher,
    JoinOptions, DEFAULT_MAX_FILE_BYTES,
};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response};
use axum::Router;
use http_body_util::BodyExt;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tower::ServiceExt;

/// Path to the chunk fixture that backs the retrieval tests. Reuses
/// the directory already maintained by `ant-retrieval/tests/bzz_fixture.rs`
/// — see that test file for capture instructions. Pointed at via a
/// relative path from `CARGO_MANIFEST_DIR` rather than copied into the
/// gateway crate so a future refresh of the fixture lands once.
pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("ant-retrieval")
        .join("tests")
        .join("fixtures")
        .join("bzz-ab7720-13-4358-2645")
}

/// HashMap-backed `ChunkFetcher` mirroring the one in `bzz_fixture.rs`.
/// Duplicated here so the gateway tests don't depend on a private
/// helper from another crate's `tests/`.
pub struct DirFetcher {
    chunks: HashMap<[u8; 32], Vec<u8>>,
}

impl DirFetcher {
    pub fn from_dir(dir: &Path) -> std::io::Result<Self> {
        let mut chunks = HashMap::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if path.extension().and_then(|s| s.to_str()) != Some("bin") {
                continue;
            }
            let mut addr = [0u8; 32];
            if hex::decode_to_slice(stem, &mut addr).is_err() {
                continue;
            }
            chunks.insert(addr, std::fs::read(&path)?);
        }
        Ok(Self { chunks })
    }
}

#[async_trait]
impl ChunkFetcher for DirFetcher {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        self.chunks
            .get(&addr)
            .cloned()
            .ok_or_else(|| -> Box<dyn StdError + Send + Sync> {
                format!("chunk {} not found (fixture)", hex::encode(addr)).into()
            })
    }
}

/// A useful default snapshot: one connected peer with a BZZ overlay,
/// a populated routing table, and an obviously-fake but well-formed
/// identity. Caller can mutate the returned `StatusSnapshot` in place
/// before stuffing it in the watch channel.
pub fn snapshot_with_one_peer() -> StatusSnapshot {
    StatusSnapshot {
        agent: "antd/test".to_string(),
        protocol_version: PROTOCOL_VERSION,
        network_id: 1,
        pid: 0,
        started_at_unix: 1_700_000_000,
        identity: IdentityInfo {
            eth_address: "0x0102030405060708090a0b0c0d0e0f1011121314".to_string(),
            overlay: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                .to_string(),
            peer_id: "12D3KooWFakePeerId000000000000000000000000000".to_string(),
        },
        peers: PeerInfo {
            connected: 1,
            node_limit: 16,
            connected_peers: vec![PeerConnectionInfo {
                peer_id: "12D3KooWPeerOne00000000000000000000000000".to_string(),
                direction: "Outbound".to_string(),
                address: "/ip4/127.0.0.1/tcp/1634".to_string(),
                connected_at_unix: 1_700_000_001,
                agent_version: "bee/2.7.0".to_string(),
                bzz_overlay: Some(
                    "0xdeadbeef00000000000000000000000000000000000000000000000000000000"
                        .to_string(),
                ),
                full_node: Some(true),
                last_bzz_at_unix: Some(1_700_000_002),
                stale: false,
            }],
            peer_pipeline: Vec::new(),
            last_handshake: None,
            time_to_first_peer_s: Some(0.5),
            time_to_node_limit_s: None,
            routing: RoutingInfo {
                base_overlay: "0xaabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
                    .to_string(),
                size: 1,
                bins: {
                    let mut bins = vec![0u32; 32];
                    bins[5] = 1;
                    bins
                },
                serving: 1,
                ..Default::default()
            },
        },
        listeners: vec!["/ip4/127.0.0.1/tcp/1634".to_string()],
        external_addresses: Vec::new(),
        control_socket: "/tmp/antd.sock".to_string(),
        retrieval: RetrievalInfo::default(),
        chain_ready: true,
        settlement: ant_control::SettlementInfo::default(),
    }
}

/// `StatusSnapshot` with no connected peers and an empty routing table.
/// Used by `/readiness` to assert the not-ready response.
pub fn empty_snapshot() -> StatusSnapshot {
    let mut s = snapshot_with_one_peer();
    s.peers = PeerInfo::default();
    s
}

/// Default identity used by every test handle. Bee-shaped: bare hex for
/// overlay / publicKey, `0x`-prefixed for ethereum.
pub fn test_identity() -> GatewayIdentity {
    GatewayIdentity {
        overlay_hex: "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899".to_string(),
        ethereum_hex: "0x0102030405060708090a0b0c0d0e0f1011121314".to_string(),
        public_key_hex: "020102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
            .to_string(),
        peer_id: "12D3KooWFakePeerId000000000000000000000000000".to_string(),
    }
}

/// Build a router whose `commands` channel is wired to a receiver
/// nobody reads — fine for status / stubs / 501 tests, fatal for any
/// retrieval test (it would deadlock waiting for an ack). For retrieval
/// tests use [`handle_with_fixture_node`] instead.
pub fn status_only_router(snapshot: StatusSnapshot) -> Router {
    let (status_tx, status_rx) = watch::channel(snapshot);
    // Keep the sender alive for the lifetime of the router so the
    // receiver doesn't observe `Closed` mid-test.
    Box::leak(Box::new(status_tx));
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(8);
    Box::leak(Box::new(cmd_rx));
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(CorsConfig::default()),
        chain_state: GatewayChainState {
            light_mode: false,
            chain: None,
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache: None,
    };
    build_router(handle)
}

/// [`status_only_router`] holding `cache` for the `/v0/cache` routes,
/// with a `cors-allowed-origins` policy.
pub fn cache_router(cache: Option<ant_retrieval::ChunkCaches>, cors: CorsConfig) -> Router {
    let (status_tx, status_rx) = watch::channel(empty_snapshot());
    Box::leak(Box::new(status_tx));
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(8);
    Box::leak(Box::new(cmd_rx));
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(cors),
        chain_state: GatewayChainState {
            light_mode: false,
            chain: None,
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache,
    };
    build_router(handle)
}

/// A light node whose loop answers `SetSwapEnabled` as the production
/// loop does: publish the switch into `StatusSnapshot::settlement`, then
/// ack. For the `swap-enable` route and `/node` capability tests. Pages
/// from `cors` origins are allowed through the switch's guard.
pub fn swap_switch_router(snapshot: StatusSnapshot, cors: CorsConfig) -> Router {
    let (status_tx, status_rx) = watch::channel(snapshot);
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(8);
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            if let ControlCommand::SetSwapEnabled { enabled, ack } = cmd {
                status_tx.send_modify(|s| {
                    s.settlement.swap_enabled = enabled;
                    s.settlement.paying = enabled && s.settlement.chequebook.is_some();
                });
                let _ = ack.send(ControlAck::Ok {
                    message: "switched (fixture)".into(),
                });
            }
        }
    });
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(cors),
        chain_state: GatewayChainState {
            light_mode: true,
            chain: None,
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache: None,
    };
    build_router(handle)
}

/// Like [`status_only_router`] but with a caller-supplied chain context,
/// so the chain-backed endpoints (`/wallet`, `/chequebook/*`,
/// `/status`, `/chainstate`) can be driven end-to-end with a fake
/// reader. `light_mode` is on so `/status.beeMode` reports `light`.
pub fn status_router_with_chain(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
) -> Router {
    status_router_with_chain_and_hook(snapshot, chain, None)
}

/// [`status_router_with_chain`] with an embedder after-buy hook, for the
/// tests that check `POST /stamps` calls it.
pub fn status_router_with_chain_and_hook(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    on_batch_bought: Option<ant_gateway::BatchBoughtHook>,
) -> Router {
    status_router_with_chain_and_hooks(snapshot, chain, on_batch_bought, None)
}

/// [`status_router_with_chain_and_hook`] plus an embedder
/// refused-chequebook hook, for the deposit top-up tests.
pub fn status_router_with_chain_and_hooks(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    on_batch_bought: Option<ant_gateway::BatchBoughtHook>,
    on_chequebook_refused: Option<ant_gateway::ChequebookRefusedHook>,
) -> Router {
    status_router_with_chain_hooks_and_cors(
        snapshot,
        chain,
        on_batch_bought,
        on_chequebook_refused,
        CorsConfig::default(),
    )
}

/// [`status_router_with_chain_and_hooks`] with a `cors-allowed-origins`
/// policy, for the wallet-spend guard tests.
pub fn status_router_with_chain_hooks_and_cors(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    on_batch_bought: Option<ant_gateway::BatchBoughtHook>,
    on_chequebook_refused: Option<ant_gateway::ChequebookRefusedHook>,
    cors: CorsConfig,
) -> Router {
    chain_router(
        snapshot,
        chain,
        on_batch_bought,
        on_chequebook_refused,
        cors,
        None,
        Ok((None, None, None)),
    )
}

/// [`status_router_with_chain`] whose node answers `AccountingSnapshot`
/// with `issued` as the PLUR its chequebook has promised in cheques
/// (`None`: outbound settlement not running).
pub fn status_router_with_chain_and_issued(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    issued: Option<u128>,
) -> Router {
    chain_router(
        snapshot,
        chain,
        None,
        None,
        CorsConfig::default(),
        None,
        Ok((issued, None, None)),
    )
}

/// [`status_router_with_chain_and_issued`] whose snapshot names the
/// chequebook the issued total counts (`issued: None` with a chequebook
/// is an unreadable outbound ledger).
pub fn status_router_with_chain_and_issued_by(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    issued: Option<u128>,
    chequebook: [u8; 20],
) -> Router {
    chain_router(
        snapshot,
        chain,
        None,
        None,
        CorsConfig::default(),
        None,
        Ok((issued, Some(chequebook), None)),
    )
}

/// [`status_router_with_chain_and_issued_by`] whose snapshot also says
/// the chequebook's cheque figures were lost (`lost`).
pub fn status_router_with_chain_and_lost_ledger(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    issued: u128,
    chequebook: [u8; 20],
    lost: &str,
) -> Router {
    chain_router(
        snapshot,
        chain,
        None,
        None,
        CorsConfig::default(),
        None,
        Ok((Some(issued), Some(chequebook), Some(lost.to_string()))),
    )
}

/// [`status_router_with_chain`] whose node fails every
/// `AccountingSnapshot`.
pub fn status_router_with_chain_and_failing_snapshot(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
) -> Router {
    chain_router(
        snapshot,
        chain,
        None,
        None,
        CorsConfig::default(),
        None,
        Err("snapshot fixture fails".into()),
    )
}

/// The `RegisterBatch` commands a chain test router received, as
/// `(batch_id, depth, immutable, bought_at_block)`.
pub type Registrations = Arc<std::sync::Mutex<Vec<([u8; 32], u8, bool, Option<u64>)>>>;

/// [`status_router_with_chain`] that also records every batch the
/// gateway registers with the node.
pub fn status_router_recording_registrations(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
) -> (Router, Registrations) {
    let seen = Registrations::default();
    let router = chain_router(
        snapshot,
        chain,
        None,
        None,
        CorsConfig::default(),
        Some(seen.clone()),
        Ok((None, None, None)),
    );
    (router, seen)
}

/// What the chain test router's node answers `AccountingSnapshot` with:
/// `(issued total, its chequebook, lost-ledger reason)`, or an error.
type SnapshotFixture = Result<(Option<u128>, Option<[u8; 20]>, Option<String>), String>;

fn chain_router(
    snapshot: StatusSnapshot,
    chain: std::sync::Arc<ChainContext>,
    on_batch_bought: Option<ant_gateway::BatchBoughtHook>,
    on_chequebook_refused: Option<ant_gateway::ChequebookRefusedHook>,
    cors: CorsConfig,
    registrations: Option<Registrations>,
    issued: SnapshotFixture,
) -> Router {
    let (status_tx, status_rx) = watch::channel(snapshot);
    Box::leak(Box::new(status_tx));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(8);
    // The buy / dilute handlers register the resulting batch with the
    // node loop before replying; answer those so the write endpoints
    // don't time out. Everything else is ignored (the chain tests don't
    // exercise the node loop).
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            if let ControlCommand::AccountingSnapshot { ack } = cmd {
                let _ = ack.send(match &issued {
                    Ok((issued, chequebook, lost)) => {
                        ControlAck::Accounting(ant_control::AccountingSnapshotView {
                            peers: Vec::new(),
                            cheques_issued_plur: issued.map(|i| i.to_string()),
                            cheques_issued_chequebook: chequebook.map(hex::encode),
                            cheques_ledger_lost: lost.clone(),
                        })
                    }
                    Err(message) => ControlAck::Error {
                        message: message.clone(),
                    },
                });
                continue;
            }
            if let ControlCommand::RegisterBatch {
                batch_id,
                depth,
                immutable,
                bought_at_block,
                ack,
                ..
            } = cmd
            {
                if let Some(seen) = &registrations {
                    seen.lock()
                        .unwrap()
                        .push((batch_id, depth, immutable, bought_at_block));
                }
                let _ = ack.send(ControlAck::Ok {
                    message: "registered (chain test fixture)".into(),
                });
            }
        }
    });
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(cors),
        chain_state: GatewayChainState {
            light_mode: true,
            chain: Some(chain),
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought,
        on_chequebook_refused,
        cache: None,
    };
    build_router(handle)
}

/// Like [`status_only_router`] but with a caller-supplied CORS policy,
/// so the CORS integration tests can drive the middleware end-to-end.
pub fn status_router_with_cors(snapshot: StatusSnapshot, cors: CorsConfig) -> Router {
    let (status_tx, status_rx) = watch::channel(snapshot);
    Box::leak(Box::new(status_tx));
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(8);
    Box::leak(Box::new(cmd_rx));
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(cors),
        chain_state: GatewayChainState {
            light_mode: false,
            chain: None,
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache: None,
    };
    build_router(handle)
}

/// Build a router whose `commands` channel is consumed by a fake node
/// loop that resolves retrieval commands against the on-disk fixture.
/// Returns the router only — the fake task lives for the test process
/// lifetime.
pub fn handle_with_fixture_node() -> Router {
    let snapshot = snapshot_with_one_peer();
    let (status_tx, status_rx) = watch::channel(snapshot);
    Box::leak(Box::new(status_tx));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(8);

    let fetcher = Arc::new(DirFetcher::from_dir(&fixture_dir()).expect("load fixture chunks"));
    let pins: PinStore = Arc::new(std::sync::Mutex::new(Vec::new()));
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let f = fetcher.clone();
            let p = pins.clone();
            tokio::spawn(async move {
                handle_command_with_pins(f.as_ref(), &p, cmd).await;
            });
        }
    });

    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(CorsConfig::default()),
        chain_state: GatewayChainState {
            light_mode: true,
            chain: None,
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache: None,
    };
    build_router(handle)
}

/// Pin membership rows: `(reference, member addresses)`.
pub type PinRows = Vec<(Vec<u8>, Vec<[u8; 32]>)>;

/// In-memory pin registry for the fixture node, in pin-creation order,
/// mirroring what the production disk cache records.
pub type PinStore = Arc<std::sync::Mutex<PinRows>>;

/// Fixture stand-ins for the pin commands, mirroring the production
/// node's pin handlers over the `DirFetcher` corpus: traversal via the
/// real `ant-retrieval` walkers, membership recorded in `pins`.
async fn handle_pin_command(fetcher: &DirFetcher, pins: &PinStore, cmd: ControlCommand) {
    fn lock(p: &PinStore) -> std::sync::MutexGuard<'_, PinRows> {
        p.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    match cmd {
        ControlCommand::PinAdd { reference, ack } => {
            if lock(pins).iter().any(|(r, _)| *r == reference) {
                let _ = ack.send(ControlAck::PinAdded {
                    already_pinned: true,
                });
                return;
            }
            let addrs = match reference.len() {
                32 => {
                    let root: [u8; 32] = reference.as_slice().try_into().expect("len checked");
                    ant_retrieval::traverse_chunk_addresses(fetcher, root, DEFAULT_MAX_FILE_BYTES)
                        .await
                }
                64 => {
                    let root: [u8; 64] = reference.as_slice().try_into().expect("len checked");
                    ant_retrieval::traverse_encrypted_chunk_addresses(
                        fetcher,
                        root,
                        DEFAULT_MAX_FILE_BYTES,
                    )
                    .await
                }
                n => {
                    let _ = ack.send(ControlAck::Error {
                        message: format!("invalid pin reference length: {n} bytes"),
                    });
                    return;
                }
            };
            let reply = match addrs {
                Ok(addrs) => {
                    let mut missing = None;
                    for addr in &addrs {
                        if fetcher.fetch(*addr).await.is_err() {
                            missing = Some(*addr);
                            break;
                        }
                    }
                    match missing {
                        None => {
                            lock(pins).push((reference, addrs));
                            ControlAck::PinAdded {
                                already_pinned: false,
                            }
                        }
                        Some(addr) => ControlAck::Error {
                            message: format!("fetch {}: chunk not found", hex::encode(addr)),
                        },
                    }
                }
                Err(e) => ControlAck::Error {
                    message: format!("pin traversal failed: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::PinRemove { reference, ack } => {
            let mut g = lock(pins);
            let before = g.len();
            g.retain(|(r, _)| *r != reference);
            let _ = ack.send(ControlAck::PinRemoved {
                was_pinned: g.len() < before,
            });
        }
        ControlCommand::PinHas { reference, ack } => {
            let pinned = lock(pins).iter().any(|(r, _)| *r == reference);
            let _ = ack.send(ControlAck::PinPresent { pinned });
        }
        ControlCommand::PinList { ack } => {
            let references = lock(pins).iter().map(|(r, _)| r.clone()).collect();
            let _ = ack.send(ControlAck::PinList { references });
        }
        ControlCommand::PinCheck { reference, ack } => {
            let entries: PinRows = lock(pins)
                .iter()
                .filter(|(r, _)| reference.as_ref().is_none_or(|want| r == want))
                .cloned()
                .collect();
            let mut stats = Vec::new();
            for (r, members) in entries {
                let mut stat = ant_control::PinCheckStat {
                    reference: r,
                    total: members.len() as u64,
                    ..Default::default()
                };
                for addr in members {
                    match fetcher.fetch(addr).await {
                        Ok(wire) => {
                            if !(ant_crypto::cac_valid(&addr, &wire)
                                || ant_crypto::soc_valid(&addr, &wire))
                            {
                                stat.invalid += 1;
                            }
                        }
                        Err(_) => stat.missing += 1,
                    }
                }
                stats.push(stat);
            }
            let _ = ack.send(ControlAck::PinCheck { stats });
        }
        other => handle_command(fetcher, other).await,
    }
}

/// Pin-aware wrapper used by [`handle_with_fixture_node`].
async fn handle_command_with_pins(fetcher: &DirFetcher, pins: &PinStore, cmd: ControlCommand) {
    handle_pin_command(fetcher, pins, cmd).await;
}

/// Single-command dispatcher used by the fake node loop: mirrors the
/// production handlers in `ant-p2p` but pulls chunks from the fixture
/// `DirFetcher`. Keeping the dispatch shape identical means the test
/// exercises the same `ControlAck` parsing in `gateway/src/retrieval.rs`
/// production hits.
async fn handle_command(fetcher: &DirFetcher, cmd: ControlCommand) {
    match cmd {
        // Pin commands are handled by `handle_pin_command` when the
        // caller wires a `PinStore`; a bare fixture without one rejects
        // them like a daemon without a disk cache.
        ControlCommand::PinAdd { ack, .. }
        | ControlCommand::PinRemove { ack, .. }
        | ControlCommand::PinHas { ack, .. }
        | ControlCommand::PinList { ack }
        | ControlCommand::PinCheck { ack, .. } => {
            let _ = ack.send(ControlAck::Error {
                message:
                    "pinning requires the disk chunk cache; pass --disk-cache-bytes > 0 at startup"
                        .into(),
            });
        }
        // The fixture models a node with no registered postage batches:
        // bee's 404 "issuer does not exist" shape.
        ControlCommand::PostageBuckets { batch_id, ack } => {
            let _ = ack.send(ControlAck::Error {
                message: format!("issuer does not exist: 0x{}", hex::encode(batch_id)),
            });
        }
        ControlCommand::GetChunkRaw { reference, ack, .. } => {
            let reply = match fetcher.fetch(reference).await {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error {
                    message: format!("fetch chunk {}: {e}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        // Live-network P2P surfaces the HTTP fixture doesn't model.
        ControlCommand::PullsyncProbe { ack, .. } => {
            let _ = ack.send(ControlAck::PullsyncProbe(
                ant_control::PullsyncProbeView::default(),
            ));
        }
        // Fixture lurker: a watched GSOC address of all-0xEE simulates
        // the registry's neighborhood-cap rejection (terminal Error →
        // the gateway closes the WS with a reason); anything else
        // delivers one fixture message and then ends the subscription
        // (ack drop → node-side close). Enough surface to exercise the
        // WebSocket handler end-to-end without a live network.
        ControlCommand::LurkerSubscribe {
            gsoc_addresses,
            pss_topics,
            history,
            ack,
            ..
        } => {
            if gsoc_addresses.contains(&[0xEE; 32]) {
                let _ = ack
                    .send(ControlAck::Error {
                        message: "lurker subscription rejected: neighborhood limit reached (8)"
                            .into(),
                    })
                    .await;
                return;
            }
            let key = gsoc_addresses
                .first()
                .map(hex::encode)
                .or_else(|| pss_topics.first().map(hex::encode))
                .unwrap_or_default();
            let kind = if gsoc_addresses.is_empty() {
                ant_control::LurkerMessageKind::Pss
            } else {
                ant_control::LurkerMessageKind::Gsoc
            };
            let _ = ack
                .send(ControlAck::LurkerMessage {
                    kind,
                    key,
                    // Echo the mailbox flag so tests can see it reached
                    // the node command (not just that the query parsed).
                    payload: if history {
                        b"fixture-lurker-payload+history".to_vec()
                    } else {
                        b"fixture-lurker-payload".to_vec()
                    },
                })
                .await;
            // An all-0xCD address models a LIVE subscription: hold the
            // ack sender open until the gateway side hangs up, so tests
            // can exercise keep-alive traffic without racing a
            // node-side close. Everything else closes after the single
            // delivery (exercising the close-with-reason path).
            if gsoc_addresses.contains(&[0xCD; 32]) {
                tokio::spawn(async move { ack.closed().await });
            }
        }
        ControlCommand::GetChunk { reference, ack } => {
            let reply = match fetcher.fetch(reference).await {
                Ok(data) => {
                    // Strip the 8-byte span to match `GetChunk` (payload only).
                    let payload = if data.len() >= 8 {
                        data[8..].to_vec()
                    } else {
                        data
                    };
                    ControlAck::Bytes { data: payload }
                }
                Err(e) => ControlAck::Error {
                    message: format!("fetch chunk {}: {e}", hex::encode(reference)),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::GetBytes {
            reference,
            bypass_cache: _,
            progress: _,
            max_bytes,
            ack,
        } => {
            let result = async {
                let root = fetcher.fetch(reference).await.map_err(|e| e.to_string())?;
                let opts = JoinOptions {
                    allow_degraded_redundancy: true,
                };
                let cap = max_bytes.map_or(DEFAULT_MAX_FILE_BYTES, |n| {
                    usize::try_from(n).unwrap_or(usize::MAX)
                });
                join_with_options(fetcher, &root, cap, opts)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            let reply = match result {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error { message: e },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::GetBytesEncrypted {
            reference,
            key,
            bypass_cache: _,
            max_bytes,
            ack,
        } => {
            let mut root_ref = [0u8; ant_retrieval::ENCRYPTED_REF_SIZE];
            root_ref[..32].copy_from_slice(&reference);
            root_ref[32..].copy_from_slice(&key);
            let cap = max_bytes.map_or(DEFAULT_MAX_FILE_BYTES, |n| {
                usize::try_from(n).unwrap_or(usize::MAX)
            });
            let reply = match ant_retrieval::join_encrypted(fetcher, root_ref, cap).await {
                Ok(data) => ControlAck::Bytes { data },
                Err(e) => ControlAck::Error {
                    message: e.to_string(),
                },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::StreamBytes {
            reference,
            bypass_cache: _,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            stream_via_fetcher(
                fetcher, &reference, /* path */ None, /* allow_degraded */ true,
                max_bytes, range, head_only, ack,
            )
            .await;
        }
        ControlCommand::StreamBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache: _,
            max_bytes,
            range,
            head_only,
            ack,
        } => {
            stream_via_fetcher(
                fetcher,
                &reference,
                Some(path),
                allow_degraded_redundancy,
                max_bytes,
                range,
                head_only,
                ack,
            )
            .await;
        }
        ControlCommand::GetBzz {
            reference,
            path,
            allow_degraded_redundancy,
            bypass_cache: _,
            progress: _,
            max_bytes,
            ack,
        } => {
            let result = async {
                let lookup = lookup_path(fetcher, &reference, &path)
                    .await
                    .map_err(|e| e.to_string())?;
                let cap = max_bytes.map_or(DEFAULT_MAX_FILE_BYTES, |n| {
                    usize::try_from(n).unwrap_or(usize::MAX)
                });
                // Encrypted manifest entry: buffered decrypt-join, like
                // the production node loop.
                if let Ok(enc_ref) =
                    <[u8; ant_retrieval::ENCRYPTED_REF_SIZE]>::try_from(lookup.data_ref.as_slice())
                {
                    let body = ant_retrieval::join_encrypted(fetcher, enc_ref, cap)
                        .await
                        .map_err(|e| e.to_string())?;
                    return Ok::<_, String>((body, lookup));
                }
                let data_ref: [u8; 32] = lookup
                    .data_ref
                    .as_slice()
                    .try_into()
                    .map_err(|_| "manifest entry has invalid width".to_string())?;
                let root = fetcher.fetch(data_ref).await.map_err(|e| e.to_string())?;
                let opts = JoinOptions {
                    allow_degraded_redundancy,
                };
                let body = join_with_options(fetcher, &root, cap, opts)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok::<_, String>((body, lookup))
            }
            .await;
            let reply = match result {
                Ok((data, lookup)) => ControlAck::BzzBytes {
                    data,
                    content_type: lookup.content_type,
                    filename: lookup.metadata.get("Filename").cloned(),
                },
                Err(e) => ControlAck::Error { message: e },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::ListBzz {
            reference,
            bypass_cache: _,
            ack,
        } => {
            let reply = match list_manifest(fetcher, &reference).await {
                Ok(entries) => ControlAck::Manifest {
                    entries: entries
                        .into_iter()
                        .map(|entry| ant_control::ManifestEntryInfo {
                            path: entry.path,
                            reference: entry.reference.map(hex::encode),
                            metadata: entry.metadata,
                            size: entry.size,
                        })
                        .collect(),
                },
                Err(e) => ControlAck::Error {
                    message: e.to_string(),
                },
            };
            let _ = ack.send(reply).await;
        }
        ControlCommand::ResetPeerstore { ack } => {
            let _ = ack.send(ControlAck::Ok {
                message: "ok".to_string(),
            });
        }
        ControlCommand::Resume { ack } => {
            // The fake loop has no swarm to re-warm; ack Ok like the real
            // node loop so the match stays exhaustive.
            let _ = ack.send(ControlAck::Ok {
                message: "ok".to_string(),
            });
        }
        ControlCommand::PushSoc {
            address, wire, ack, ..
        } => {
            // Mirror the real handler: trust the gateway's `soc_valid`
            // pre-check and reply with the supplied address. Test
            // fixtures exercise the gateway's validation path; the
            // node-side stamping path is covered by `ant-p2p`.
            let reply = if wire.len() < 32 + 65 + 8 || wire.len() > 32 + 65 + 8 + 4096 {
                ControlAck::Error {
                    message: format!("soc wire size out of range: {} bytes", wire.len()),
                }
            } else {
                ControlAck::ChunkUploaded {
                    reference: format!("0x{}", hex::encode(address)),
                }
            };
            let _ = ack.send(reply);
        }
        ControlCommand::PushChunk { wire, ack, .. } => {
            // Mirror what `ant-p2p`'s `PushChunk` handler does: BMT-hash
            // the wire bytes and reply with the resulting reference.
            // Validation lives in the gateway itself; test fixtures
            // never need to exercise the size-cap branch because that
            // already short-circuits before the channel send.
            let reply = if wire.len() < 8 || wire.len() > 8 + 4096 {
                ControlAck::Error {
                    message: format!("chunk wire size out of range: {} bytes", wire.len()),
                }
            } else {
                let mut span = [0u8; 8];
                span.copy_from_slice(&wire[..8]);
                let payload = &wire[8..];
                match ant_crypto::bmt::bmt_hash_with_span(&span, payload) {
                    Some(addr) => ControlAck::ChunkUploaded {
                        reference: format!("0x{}", hex::encode(addr)),
                    },
                    None => ControlAck::Error {
                        message: "failed to BMT-hash chunk".into(),
                    },
                }
            };
            let _ = ack.send(reply);
        }
        // Upload* control commands are only meaningful with a real
        // `UploadManager`; gateway tests don't model the upload
        // pipeline, so reject any that leak in here as a test bug.
        ControlCommand::UploadStart { ack, .. }
        | ControlCommand::UploadList { ack }
        | ControlCommand::UploadStatus { ack, .. }
        | ControlCommand::UploadPause { ack, .. }
        | ControlCommand::UploadResume { ack, .. }
        | ControlCommand::UploadRepush { ack, .. }
        | ControlCommand::UploadCancel { ack, .. }
        | ControlCommand::UploadSuspendAll { ack }
        | ControlCommand::UploadWakeAll { ack } => {
            let _ = ack.send(ControlAck::Error {
                message: "Upload* not supported in gateway test fixtures".into(),
            });
        }
        ControlCommand::UploadFollow { ack, .. } => {
            let _ = ack.try_send(ControlAck::Error {
                message: "Upload* not supported in gateway test fixtures".into(),
            });
        }
        // `PostageStatus` is the daemon's stamp issuer view; the
        // gateway test fixtures don't model an issuer, so respond
        // with the default "uploads disabled" snapshot. Mirrors
        // what a daemon started without `--postage-batch` would
        // return — exercises the antctl renderer's "disabled"
        // branch when wired into a future end-to-end test.
        ControlCommand::PostageStatus { ack } => {
            let _ = ack.send(ControlAck::PostageStatus(
                ant_control::PostageStatusView::default(),
            ));
        }
        // The fixture models a node with no registered batches; the
        // multi-batch list is therefore empty (matching `GET /stamps`
        // on a freshly-started node).
        ControlCommand::PostageList { ack } => {
            let _ = ack.send(ControlAck::PostageList(Vec::new()));
        }
        // Runtime batch registration touches the daemon's issuer
        // registry, which the gateway fixture doesn't model; ack OK so
        // the buy/dilute handlers' register step doesn't error.
        ControlCommand::RegisterBatch { ack, .. } => {
            let _ = ack.send(ControlAck::Ok {
                message: "registered (test fixture)".into(),
            });
        }
        // Local chunk-cache writes are an antctl-only escape hatch
        // (`antctl pin`) that doesn't go through the gateway HTTP
        // path; the test fixture has no disk cache to mutate, so
        // surface a clear "not implemented" rather than silently
        // accepting bytes that wouldn't be retrievable.
        ControlCommand::PutChunkLocal { ack, .. } => {
            let _ = ack.send(ControlAck::Error {
                message: "PutChunkLocal not supported in gateway test fixtures".into(),
            });
        }
        // Feed lookups don't go through the on-disk `DirFetcher`
        // fixture: the feed tests stage SOC updates in memory and use
        // `router_with_dispatcher` directly. Surfacing a clear "not
        // implemented in this fixture" error keeps any new test that
        // accidentally hits the standard fixture from silently
        // succeeding against an empty corpus.
        ControlCommand::GetFeed { ack, .. } => {
            let _ = ack.send(ControlAck::Error {
                message: "GetFeed is not wired through the standard test fixture; \
                         use router_with_dispatcher to stage feed updates"
                    .into(),
            });
        }
        // Outbound SWAP settlement bootstrap is an `ant-ffi`-only path
        // (first storage buy on a phone node); the gateway never emits
        // it, so the fixture just acks OK rather than modelling a
        // chequebook install.
        ControlCommand::EnablePushsyncSwap { ack, .. } => {
            let _ = ack.send(ControlAck::Ok {
                message: "settlement enable ignored (test fixture)".into(),
            });
        }
        ControlCommand::DisablePushsyncSwap { ack, .. } => {
            let _ = ack.send(ControlAck::Ok {
                message: "settlement disable ignored (test fixture)".into(),
            });
        }
        ControlCommand::SetRetrievalFunds { ack, .. }
        | ControlCommand::SetSwapEnabled { ack, .. } => {
            let _ = ack.send(ControlAck::Ok {
                message: "retrieval payments ignored (test fixture)".into(),
            });
        }
        // Read-back propagation check. The fixture has a single source
        // (the `DirFetcher`), so report `sources = 1` when the chunk is
        // present and `0` otherwise, matching the production JSON shape.
        ControlCommand::VerifyPropagation {
            reference,
            samples,
            probes,
            progress: _,
            cancel: _,
            ack,
        } => {
            // The gateway fixture's `DirFetcher` is a flat chunk store,
            // not a manifest resolver, so this stand-in just reports
            // root reachability in the deep check's JSON shape.
            let retrievable = fetcher.fetch(reference).await.is_ok();
            let checked = u64::from(retrievable);
            let _ = (samples, probes);
            let body = serde_json::json!({
                "reference": format!("0x{}", hex::encode(reference)),
                "retrievable": retrievable,
                "total_chunks": 1,
                "leaf_chunks": 1,
                "intermediate_chunks": 0,
                "checked_chunks": checked,
                "retrievable_chunks": checked,
                "sampled_leaves": checked,
                "sources": checked,
            });
            let _ = ack.send(ControlAck::Ok {
                message: body.to_string(),
            });
        }
        // Envelope issuance needs a stamp issuer + signing key, which
        // the gateway fixture doesn't model (the real path is covered
        // by `ant-p2p` and the conformance MemNode); surface a clear
        // "not usable" so the handler's bee-shaped 422 branch is what a
        // stray test observes.
        ControlCommand::Envelope { batch_id, ack, .. } => {
            let _ = ack.send(ControlAck::Error {
                message: format!(
                    "batch 0x{} not usable (no issuer in gateway test fixture)",
                    hex::encode(batch_id)
                ),
            });
        }
        // Stewardship check: run the real traversal over the fixture's
        // `DirFetcher` (it implements `ChunkFetcher`), then confirm
        // every listed chunk is present — the same shape the production
        // node-loop handler has.
        ControlCommand::StewardshipGet { reference, ack } => {
            let retrievable = match ant_retrieval::traverse_chunk_addresses(
                fetcher,
                reference,
                DEFAULT_MAX_FILE_BYTES,
            )
            .await
            {
                Ok(addrs) => {
                    let mut ok = true;
                    for addr in addrs {
                        if fetcher.fetch(addr).await.is_err() {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }
                Err(_) => false,
            };
            let _ = ack.send(ControlAck::Retrievable { retrievable });
        }
        // Stewardship re-upload: the fixture has no network to push to;
        // traversing + re-reading the fixture store is the closest
        // no-op analogue, and it keeps the success/failure split real
        // (missing content still errors like the production handler).
        ControlCommand::StewardshipPut { reference, ack, .. } => {
            let reply = match ant_retrieval::traverse_chunk_addresses(
                fetcher,
                reference,
                DEFAULT_MAX_FILE_BYTES,
            )
            .await
            {
                Ok(addrs) => ControlAck::Ok {
                    message: format!("re-uploaded {} chunks (test fixture)", addrs.len()),
                },
                Err(e) => ControlAck::Error {
                    message: format!("re-upload traversal failed: {e}"),
                },
            };
            let _ = ack.send(reply);
        }
        ControlCommand::VerifyChunksPresent {
            addresses,
            probes: _,
            include_shallow: _,
            ack,
        } => {
            // Stand-in: the fixture's `DirFetcher` is the source of
            // truth for "present", so report any address it can't serve
            // as missing, in the heal loop's JSON shape.
            let mut missing = Vec::new();
            for addr in &addresses {
                if fetcher.fetch(*addr).await.is_err() {
                    missing.push(format!("0x{}", hex::encode(addr)));
                }
            }
            let body = serde_json::json!({
                "checked": addresses.len(),
                "missing": missing,
            });
            let _ = ack.send(ControlAck::Ok {
                message: body.to_string(),
            });
        }
        ControlCommand::AccountingSnapshot { ack } => {
            // The fixture has no peers: an empty snapshot renders the
            // settlement endpoints' empty-collection shapes.
            let _ = ack.send(ControlAck::Accounting(
                ant_control::AccountingSnapshotView::default(),
            ));
        }
    }
}

/// Build a router whose `commands` channel is wired to a caller-provided
/// dispatcher. Lets a test inspect the exact `ControlCommand` the
/// gateway emitted (e.g. to assert that `max_bytes` carries the
/// gateway's larger ceiling, not the joiner default).
pub fn router_with_dispatcher<F, Fut>(dispatch: F) -> Router
where
    F: Fn(ControlCommand) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let snapshot = snapshot_with_one_peer();
    let (status_tx, status_rx) = watch::channel(snapshot);
    Box::leak(Box::new(status_tx));
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(8);
    let dispatch = Arc::new(dispatch);
    tokio::spawn(async move {
        while let Some(cmd) = cmd_rx.recv().await {
            let d = dispatch.clone();
            tokio::spawn(async move {
                d(cmd).await;
            });
        }
    });
    let handle = GatewayHandle {
        agent: Arc::new("antd/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(test_identity()),
        status: status_rx,
        commands: cmd_tx,
        activity: GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(CorsConfig::default()),
        chain_state: GatewayChainState {
            light_mode: true,
            chain: None,
        }
        .preset(),
        act_secret: std::sync::Arc::new(TEST_ACT_SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache: None,
    };
    build_router(handle)
}

/// Materialise a response body to `Vec<u8>`. Tests don't deal with
/// streaming here — the gateway joiner already collects everything
/// before handing it to axum, so a single buffer is sufficient.
pub async fn body_bytes(resp: Response<Body>) -> Vec<u8> {
    let (_, body) = resp.into_parts();
    body.collect()
        .await
        .expect("collect body")
        .to_bytes()
        .to_vec()
}

/// Helper: send a request through a router via `tower::ServiceExt::oneshot`.
pub async fn send(router: Router, req: Request<Body>) -> Response<Body> {
    router.oneshot(req).await.expect("router oneshot")
}

/// Streaming dispatcher used by both `StreamBytes` and `StreamBzz` in
/// the fake node loop. `path = None` means "treat `reference` as a raw
/// `/bytes` data root"; `path = Some(_)` walks the manifest first.
#[allow(clippy::too_many_arguments)]
async fn stream_via_fetcher(
    fetcher: &DirFetcher,
    reference: &[u8],
    path: Option<String>,
    allow_degraded: bool,
    max_bytes: Option<u64>,
    range: Option<StreamRange>,
    head_only: bool,
    ack: tokio::sync::mpsc::Sender<ControlAck>,
) {
    let is_bzz = path.is_some();
    let result = async {
        let (data_ref, content_type, filename, mutable) = match path {
            None => (reference.to_vec(), None, None, false),
            Some(p) => {
                let lookup = lookup_path(fetcher, reference, &p)
                    .await
                    .map_err(|e| e.to_string())?;
                let filename = lookup.metadata.get("Filename").cloned().or_else(|| {
                    let trimmed = p.trim_matches('/');
                    if trimmed.is_empty() {
                        None
                    } else {
                        trimmed
                            .rsplit('/')
                            .next()
                            .map(std::string::ToString::to_string)
                    }
                });
                (
                    lookup.data_ref,
                    lookup.content_type,
                    filename,
                    lookup.is_feed,
                )
            }
        };
        // The dir-fixture loop serves plain fixtures only; encrypted
        // manifest entries are exercised through the mem gateway.
        let data_ref: [u8; 32] = data_ref
            .as_slice()
            .try_into()
            .map_err(|_| "encrypted manifest entries not supported by the dir fixture")?;
        let root = fetcher.fetch(data_ref).await.map_err(|e| e.to_string())?;
        // Mask the redundancy level off the high byte of the span so
        // the reported total reflects the real file size, matching the
        // production node loop's `root_span_to_u64_masked`.
        let mut span_bytes: [u8; 8] = root
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| "root shorter than span".to_string())?;
        span_bytes[7] = 0;
        let total = u64::from_le_bytes(span_bytes);
        let prologue = if is_bzz {
            ControlAck::BzzStreamStart {
                total_bytes: total,
                reference: data_ref.to_vec(),
                content_type,
                filename,
                mutable,
            }
        } else {
            ControlAck::BytesStreamStart { total_bytes: total }
        };
        ack.send(prologue)
            .await
            .map_err(|_| "stream closed".to_string())?;
        if head_only {
            return Ok::<(), String>(());
        }
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel(8);
        let opts = JoinOptions {
            allow_degraded_redundancy: allow_degraded,
        };
        let cap = max_bytes.map_or(DEFAULT_MAX_FILE_BYTES, |n| {
            usize::try_from(n).unwrap_or(usize::MAX)
        });
        let body_range = range.and_then(|r| ByteRange::clamp(r.start, r.end_inclusive, total));
        let mut join_fut = Box::pin(join_to_sender_range(
            fetcher, &root, cap, opts, body_range, chunk_tx,
        ));
        let mut join_result = None;
        loop {
            tokio::select! {
                result = &mut join_fut, if join_result.is_none() => {
                    join_result = Some(result.map_err(|e| e.to_string()));
                }
                maybe_chunk = chunk_rx.recv() => {
                    match maybe_chunk {
                        Some(data) => {
                            ack.send(ControlAck::BytesChunk { data })
                                .await
                                .map_err(|_| "stream closed".to_string())?;
                        }
                        None => return join_result.unwrap_or(Ok(())),
                    }
                }
            }
        }
    }
    .await;
    let terminal = match result {
        Ok(()) => ControlAck::StreamDone,
        Err(e) => ControlAck::Error { message: e },
    };
    let _ = ack.send(terminal).await;
}
