//! `antd` — Swarm light node daemon (M1.0: mainnet dial + BZZ handshake).

mod config;
mod keystore;

use ant_control::{
    ControlCommand, GatewayActivity, IdentityInfo, PeerInfo, RetrievalInfo, StatusSnapshot,
    PROTOCOL_VERSION,
};
use ant_crypto::{
    ethereum_address_from_public_key, overlay_from_ethereum_address, random_overlay_nonce,
    random_secp256k1_secret, SECP256K1_SECRET_LEN,
};
use ant_gateway::{Gateway, GatewayHandle, GatewayIdentity, TagRegistry};
use ant_node::{run_node, NodeConfig};
use ant_p2p::UploadRuntime;
use anyhow::{anyhow, Context, Result};
use clap::parser::ValueSource;
use clap::{CommandFactory, FromArgMatches, Parser};
use fs4::{FileExt, TryLockError};
use k256::ecdsa::SigningKey;
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tracing_subscriber::EnvFilter;

const AGENT: &str = concat!("antd/", env!("CARGO_PKG_VERSION"));
/// Pinned bee API version this build advertises via `/health.apiVersion`.
/// Bumped per Ant release per PLAN.md §C.5; the constant lives in the
/// binary (not in `ant-gateway`) so the gateway crate stays bee-version
/// agnostic and can be reused in other embedders.
const BEE_API_VERSION: &str = "7.2.0";

/// Default BZZ deposit for an auto-deployed chequebook, in PLUR
/// (1 BZZ = 1e16 PLUR). 0.1 BZZ is a modest float that lets the
/// chequebook honour the first wave of cashed cheques without
/// draining a thin wallet of the BZZ it also needs for postage; the
/// actual transfer is capped to the wallet's BZZ balance, so this is
/// an upper bound, not a hard requirement.
const DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR: u128 = 1_000_000_000_000_000;

#[derive(Parser, Debug)]
#[command(name = "antd", version, about = "Ant Swarm light node (M1.0)")]
struct Opt {
    /// Path to a bee-compatible YAML config file (PLAN.md J.5.E1/E2).
    /// Lets Freedom launch `antd` with the same config it writes for
    /// bee. Explicit CLI flags override values from this file, which
    /// override the built-in defaults. Recognised keys: `api-addr`,
    /// `data-dir`, `password` / `password-file`, `mainnet` /
    /// `network-id`, `blockchain-rpc-endpoint`, `cors-allowed-origins`,
    /// `verbosity`; other bee keys are accepted and ignored.
    #[arg(long)]
    config: Option<PathBuf>,

    /// Password used to decrypt a bee `keys/swarm.key` v3 keystore
    /// (PLAN.md J.5.E3). Usually supplied via the config file's
    /// `password` / `password-file`; this flag is the CLI override.
    #[arg(long)]
    password: Option<String>,

    /// File whose (trimmed) contents are the keystore password. CLI
    /// equivalent of bee's `password-file`.
    #[arg(long)]
    password_file: Option<PathBuf>,

    /// Data directory (identity + nonce persisted here).
    #[arg(long, default_value = "~/.antd")]
    data_dir: PathBuf,

    /// Swarm network id (mainnet = 1).
    #[arg(long, default_value_t = 1)]
    network_id: u64,

    /// Comma-separated bootnode multiaddrs (default: mainnet bootnodes).
    #[arg(long, value_delimiter = ',')]
    bootnodes: Option<Vec<String>>,

    /// Log level (`RUST_LOG` syntax), e.g. `info`, `debug`.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Path to secp256k1 signing key file (32 raw bytes hex). Generated if missing.
    #[arg(long)]
    key_file: Option<PathBuf>,

    /// Path to the `antctl` control socket. Defaults to `<data-dir>/antd.sock`.
    #[arg(long)]
    control_socket: Option<PathBuf>,

    /// Disable the `antctl` control socket entirely.
    #[arg(long, default_value_t = false)]
    no_control_socket: bool,

    /// Address `ant-gateway` listens on for the bee-shaped HTTP API.
    /// Default mirrors bee's own default (`127.0.0.1:1633`) so unmodified
    /// `bee-js` clients work against `antd` without configuration.
    #[arg(long, default_value = "127.0.0.1:1633")]
    api_addr: SocketAddr,

    /// Disable the bee-shaped HTTP API entirely. Useful for headless
    /// deployments that only need `antctl` access via the control
    /// socket.
    #[arg(long, default_value_t = false)]
    no_http_api: bool,

    /// CORS allowed origins for the HTTP API (bee's
    /// `cors-allowed-origins`). Comma-separated; `*` allows any origin
    /// and the literal `null` allows opaque-origin pages. Freedom sets
    /// this to `null` so its `bzz://` dweb pages can call `window.swarm`
    /// (PLAN.md J.4.8). Empty (default) disables CORS, matching a bee
    /// node started without the option.
    #[arg(long, value_delimiter = ',')]
    cors_allowed_origins: Vec<String>,

    /// Externally reachable multiaddrs to advertise via libp2p-identify.
    /// Bee bootnodes stall our handshake by 10 s when their peerstore lacks a
    /// publicly-routable multiaddr for us, so operators behind NAT should
    /// pass their public ip/tcp multiaddr here (comma-separated for multiple).
    #[arg(long, value_delimiter = ',')]
    external_address: Vec<String>,

    /// Path to the JSON peer snapshot. Defaults to `<data-dir>/peers.json`.
    /// Loaded at startup to warm the dial pipeline so the next run skips the
    /// bootnode hop, flushed every 30 s and on shutdown.
    #[arg(long)]
    peers_file: Option<PathBuf>,

    /// Disable the peer snapshot entirely. Every restart will re-bootstrap
    /// through the bootnodes; useful for diagnosing peer-discovery issues.
    #[arg(long, default_value_t = false)]
    no_peerstore: bool,

    /// Override the peer-set target — how many BZZ-handshake-complete peers
    /// the swarm loop tries to keep online. Default (`None`) leaves
    /// `ant-p2p`'s baked-in `DEFAULT_TARGET_PEERS` (100), which is the
    /// right size for cheap-CPU light-mode operation. Override (e.g. 200)
    /// to widen the network-wide credit budget for long uploads — bee
    /// grants each peer ~5 K PLUR/sec of pseudosettle credit, so doubling
    /// the peer set roughly doubles per-second debit headroom and shaves
    /// the throughput-throttling wall.
    #[arg(long)]
    target_peers: Option<usize>,

    /// Delete `<data-dir>/peers.json` (or `--peers-file`) before startup.
    /// One-shot equivalent of `antctl peers reset` for when the daemon isn't
    /// running yet.
    #[arg(long, default_value_t = false)]
    reset_peerstore: bool,

    /// Scope the in-memory chunk cache to a single `antctl get` invocation
    /// instead of sharing it across the daemon's lifetime. Each request
    /// still gets a cache (so retries within the same request skip the
    /// network for chunks already pulled), but a second request for the
    /// same reference starts cold. Useful for reproducing transient
    /// retrieval failures and benchmarking cold-path latency.
    #[arg(long, default_value_t = false)]
    per_request_chunk_cache: bool,

    /// Dump every chunk fetched during a `GetBytes` / `GetBzz` request
    /// to `<DIR>/<hex_addr>.bin` (raw wire bytes: 8-byte LE span ||
    /// payload). Combined with the `MapFetcher::from_dir` helper this
    /// lets the integration tests in `ant-retrieval` replay a real
    /// `antctl get` offline. Debug builds only; release `antd` does not
    /// expose the flag. The directory is created if missing.
    #[cfg(debug_assertions)]
    #[arg(long, value_name = "DIR")]
    record_chunks: Option<PathBuf>,

    /// Path to the persistent (SQLite-backed) chunk cache. Defaults to
    /// `<data-dir>/chunks.sqlite`. Holds the second-tier cache that
    /// sits between the in-memory LRU and the network retrieval path
    /// (see PLAN.md § 6.1).
    #[arg(long)]
    disk_cache_path: Option<PathBuf>,

    /// Hard upper bound on the persistent chunk cache size, in
    /// gigabytes. Defaults to 10 GB (matches the `antd` desktop /
    /// Raspberry Pi default in PLAN.md § 6.1). When the on-disk total
    /// crosses this cap, the cache evicts oldest-by-`last_access`
    /// rows down to ~95% of the cap.
    #[arg(long, default_value_t = 10)]
    disk_cache_max_gb: u64,

    /// Disable the persistent chunk cache entirely. Retrieval falls
    /// back to the legacy `memory -> network` lookup order. Useful
    /// when the disk has no spare capacity for a long-lived cache,
    /// or when you want every restart to start cold.
    #[arg(long, default_value_t = false)]
    no_disk_cache: bool,

    /// Gnosis Chain JSON-RPC endpoint used to read the postage batch
    /// metadata at startup (depth, bucket depth, immutability flag).
    /// Falls back to `GNOSIS_RPC_URL` env var. Required when
    /// `--postage-batch` is set.
    #[arg(long)]
    gnosis_rpc_url: Option<String>,

    /// Gnosis RPC endpoint used **only** for the startup recovery log
    /// scans (`eth_getLogs` over the xBZZ `Transfer` event) that
    /// rediscover this node's owned postage batches and chequebook from
    /// its EOA. Kept separate from `--gnosis-rpc-url` because many
    /// general-purpose RPCs (e.g. Alchemy's free tier, capped at a
    /// 10-block `eth_getLogs` range) cannot serve a wide historical log
    /// scan, while the default public endpoint here can. Falls back to
    /// the `GNOSIS_LOGS_RPC_URL` env. Set to an empty string to disable
    /// on-chain recovery entirely.
    #[arg(
        long,
        env = "GNOSIS_LOGS_RPC_URL",
        default_value = "https://rpc.gnosischain.com"
    )]
    gnosis_logs_rpc_url: String,

    /// `PostageStamp` contract address (mainnet default keeps matching
    /// upstream bee). Override only when running against a fork.
    #[arg(long, default_value = "0x45a1502382541Cd610CC9068e88727426b696293")]
    postage_contract: String,

    /// 32-byte postage batch id (`0x` + 64 hex). Enables uploads
    /// (`POST /chunks`, gateway `/bytes`) by binding stamps to this
    /// batch on the chain. Falls back to `STORAGE_STAMP_BATCH_ID` env.
    #[arg(long)]
    postage_batch: Option<String>,

    /// 32-byte secp256k1 secret of the batch owner (the address
    /// returned by `batchOwner(batch_id)` on the `PostageStamp`
    /// contract). Required to sign stamps. Falls back to
    /// `STORAGE_STAMP_PRIVATE_KEY` env. The bee node mnemonic at
    /// derivation path `m/44'/60'/0'/0/1` produces this key for the
    /// default Ant test setup.
    #[arg(long)]
    postage_owner_key: Option<String>,

    /// On startup, do **not** auto-resume upload jobs that were in
    /// `running` state when the daemon last shut down. They're
    /// loaded into memory and listed by `antctl upload list` as
    /// `paused`, so the operator can inspect them and `antctl
    /// upload resume <id>` deliberately. Default behaviour
    /// (auto-resume) matches the "I closed my laptop"
    /// expectation and is what most operators want.
    #[arg(long, default_value_t = false)]
    no_resume_uploads: bool,

    /// 20-byte chequebook contract address (`0x` + 40 hex). When set
    /// alongside `--swap-key`, the daemon enables outbound SWAP
    /// settlement (Phase 7b): every successful pushsync push accrues
    /// debt against the receiver's beneficiary EOA, and an EIP-712
    /// cheque is emitted automatically once the per-peer debt crosses
    /// `LIGHT_PAYMENT_THRESHOLD / 2` (≈ 675 K PLUR). Required for
    /// sustained uploads — without it pushsync stalls after a few
    /// hundred chunks per peer (bee paymentTolerance). Falls back to
    /// `CHEQUEBOOK_ADDRESS` env. The chequebook's on-chain
    /// `issuer()` view must return the EOA derived from `--swap-key`.
    #[arg(long)]
    chequebook: Option<String>,

    /// 32-byte secp256k1 secret of the chequebook's issuer EOA
    /// (`0x` + 64 hex). Falls back to `SWAP_OWNER_KEY` and then
    /// `WALLET_PRIVATE_KEY`. Same key bee derives at
    /// `m/44'/60'/0'/0/0` of the node mnemonic by default; we keep
    /// the convention that the wallet key is also the chequebook
    /// owner. Required when `--chequebook` is set; ignored otherwise.
    #[arg(long)]
    swap_key: Option<String>,

    /// Skip the startup `factory.deployedContracts(chequebook)`
    /// sanity check. Bee silently rejects cheques drawn on
    /// chequebooks that aren't registered with the official
    /// `SimpleSwapFactory`, so by default we refuse to enable
    /// outbound SWAP settlement against an unregistered
    /// chequebook (the daemon still starts, but with pushsync-swap
    /// disabled, so we don't waste bandwidth signing cheques
    /// peers will silently drop). Pass this flag for devnet /
    /// custom-factory scenarios where the check is the wrong
    /// answer.
    #[arg(long, default_value_t = false)]
    chequebook_allow_unverified: bool,

    /// Disable first-run chequebook auto-deploy. By default, on the
    /// first light start with a funded node wallet and no chequebook
    /// configured (`--chequebook`) or persisted, `antd` deploys a
    /// fresh factory-registered chequebook (issuer = node EOA),
    /// funds it with `--chequebook-deposit-plur` xBZZ, and persists
    /// the association at `<data-dir>/chequebook.json` so subsequent
    /// starts reuse it (deploy-once, reuse-forever — same shape as
    /// bee's statestore). Pass this to opt out and run without
    /// outbound SWAP settlement until you supply a chequebook
    /// manually.
    #[arg(long, default_value_t = false)]
    no_auto_chequebook: bool,

    /// BZZ to deposit into an auto-deployed chequebook, in PLUR
    /// (1 BZZ = 1e16 PLUR). Capped to the node wallet's BZZ balance
    /// so a thin wallet still gets a (smaller, or zero) deposit
    /// rather than a failed transfer — a factory-registered but
    /// unfunded chequebook already unblocks uploads (bee accepts the
    /// cheque; cashing waits for a later deposit). Ignored when a
    /// chequebook is supplied manually or reloaded from disk. Falls
    /// back to `CHEQUEBOOK_DEPOSIT_PLUR` env.
    #[arg(long, env = "CHEQUEBOOK_DEPOSIT_PLUR", default_value_t = DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR)]
    chequebook_deposit_plur: u128,
}

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    /// 32-byte secp256k1 secret (hex).
    signing_key: String,
    /// 32-byte overlay nonce (hex).
    overlay_nonce: String,
    /// libp2p identity as protobuf (hex), or regenerated from signing key if absent.
    #[serde(default)]
    libp2p_keypair: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let process_start = Instant::now();
    // Parse via `ArgMatches` (not the `Opt::parse()` shortcut) so the
    // config-file merge can tell which settings came from the command
    // line — those win over the config file (PLAN.md J.5.E2).
    let matches = Opt::command().get_matches();
    let mut opt = match Opt::from_arg_matches(&matches) {
        Ok(o) => o,
        Err(e) => e.exit(),
    };
    let resolved_password = apply_config_file(&mut opt, &matches)?;
    let data_dir = expand_tilde(&opt.data_dir);
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("create data dir {}", data_dir.display()))?;

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&opt.log_level)),
        )
        .init();

    // Held for the lifetime of the daemon: dropping the `File` releases the
    // advisory `flock`. Bound at function scope (not in a helper) so it stays
    // alive until `main` returns.
    let _instance_lock = acquire_instance_lock(&data_dir.join("antd.lock"))?;

    raise_nofile_soft_limit();

    let id_path = data_dir.join("identity.json");
    let key_path = opt
        .key_file
        .clone()
        .unwrap_or_else(|| data_dir.join("signing.key"));

    // A bee-managed Web3 v3 keystore at `<data-dir>/keys/swarm.key`
    // (Freedom's injected identity) takes precedence over our own
    // `identity.json` (PLAN.md J.5.E3); only fall back to generate /
    // load our native identity when no keystore is present.
    let swarm_key_path = data_dir.join("keys").join("swarm.key");
    let (signing_secret, overlay_nonce, libp2p_keypair) = if swarm_key_path.exists() {
        load_identity_from_keystore(&swarm_key_path, resolved_password.as_deref())?
    } else {
        load_or_create_identity(&id_path, &key_path)?
    };

    let vk = *(SigningKey::from_bytes((&signing_secret).into())
        .context("invalid signing key")?
        .verifying_key());
    let eth = ethereum_address_from_public_key(&vk);
    let overlay = overlay_from_ethereum_address(&eth, opt.network_id, &overlay_nonce);
    let public_key_compressed = vk.to_encoded_point(true).as_bytes().to_vec();
    tracing::info!(
        target: "antd",
        "loaded identity eth=0x{} overlay=0x{}",
        hex::encode(eth),
        hex::encode(overlay),
    );

    let bootnodes: Vec<_> = match &opt.bootnodes {
        Some(v) => v.iter().filter_map(|s| s.parse().ok()).collect(),
        None => ant_p2p::default_mainnet_bootnodes(),
    };

    let external_addrs: Vec<libp2p::multiaddr::Multiaddr> = opt
        .external_address
        .iter()
        .filter_map(|s| match s.parse() {
            Ok(m) => Some(m),
            Err(e) => {
                tracing::warn!(target: "antd", "skipping invalid --external-address {s:?}: {e}");
                None
            }
        })
        .collect();

    let peer_id = libp2p_keypair.public().to_peer_id();
    let control_socket = opt
        .control_socket
        .clone()
        .map_or_else(|| data_dir.join("antd.sock"), |p| expand_tilde(&p));

    // Bind the control socket NOW, before anything else spawns: a bad
    // path (deep data dirs exceed `sun_path`'s ~104-byte limit) must
    // surface here — with a temp-dir fallback and, failing that, a
    // warning — never as a fatal error seconds after the node started
    // serving (issue #39). The accept loop is spawned after the node
    // task below; connections made in between just wait in the backlog.
    #[cfg(unix)]
    let control_bound = if opt.no_control_socket {
        None
    } else {
        bind_control_socket(&control_socket, &data_dir)
    };
    #[cfg(unix)]
    let control_socket_display = control_bound
        .as_ref()
        .map_or_else(String::new, |b| b.socket_path().display().to_string());
    #[cfg(not(unix))]
    let control_socket_display = String::new();

    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let initial_snapshot = StatusSnapshot {
        agent: AGENT.to_string(),
        protocol_version: PROTOCOL_VERSION,
        network_id: opt.network_id,
        pid: std::process::id(),
        started_at_unix,
        identity: IdentityInfo {
            eth_address: format!("0x{}", hex::encode(eth)),
            overlay: format!("0x{}", hex::encode(overlay)),
            peer_id: peer_id.to_string(),
        },
        peers: PeerInfo {
            node_limit: ant_p2p::DEFAULT_TARGET_PEERS as u32,
            ..PeerInfo::default()
        },
        listeners: Vec::new(),
        external_addresses: Vec::new(),
        // The path the socket actually bound to (fallback-aware), not
        // the intended one; empty when disabled or bind failed.
        control_socket: control_socket_display,
        retrieval: RetrievalInfo::default(),
        // Chain init hasn't run yet; the swarm loop republishes the
        // real value as soon as it starts, and flips it to `true` when
        // the `LateChainInit` lands.
        chain_ready: false,
    };
    let (status_tx, status_rx) = watch::channel(initial_snapshot);

    let peerstore_path = if opt.no_peerstore {
        None
    } else {
        Some(
            opt.peers_file
                .clone()
                .map_or_else(|| data_dir.join("peers.json"), |p| expand_tilde(&p)),
        )
    };
    if let Some(p) = &peerstore_path {
        tracing::info!(target: "antd", "peer snapshot at {}", p.display());
        if opt.reset_peerstore {
            match std::fs::remove_file(p) {
                Ok(()) => tracing::info!(
                    target: "antd",
                    "--reset-peerstore: removed {}",
                    p.display(),
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => tracing::info!(
                    target: "antd",
                    "--reset-peerstore: {} did not exist, nothing to do",
                    p.display(),
                ),
                Err(e) => tracing::warn!(
                    target: "antd",
                    "--reset-peerstore: failed to unlink {}: {e}",
                    p.display(),
                ),
            }
        }
    } else if opt.reset_peerstore {
        tracing::warn!(
            target: "antd",
            "--reset-peerstore ignored: peerstore is disabled (--no-peerstore)",
        );
    }

    // Control socket → node loop command channel. Sized at 32: control
    // commands are rare (operator-driven), and even a burst from a scripted
    // client is trivially below this depth. Allocating the channel even when
    // `--no-control-socket` is set keeps `NodeConfig` shape consistent; the
    // sender is dropped below and the receiver's select arm idles on a
    // perpetual `recv().await`.
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(32);

    if opt.per_request_chunk_cache {
        tracing::info!(
            target: "antd",
            "chunk cache scoped to per-request (--per-request-chunk-cache)",
        );
    }

    let chunk_record_dir = resolve_chunk_record_dir(
        #[cfg(debug_assertions)]
        opt.record_chunks.as_deref(),
    )?;

    let disk_cache = if opt.no_disk_cache {
        tracing::info!(target: "antd", "persistent chunk cache disabled (--no-disk-cache)");
        None
    } else {
        let path = opt
            .disk_cache_path
            .clone()
            .map_or_else(|| data_dir.join("chunks.sqlite"), |p| expand_tilde(&p));
        // Convert the GB cap to bytes once at startup. Saturating mul
        // means a hypothetical operator typo of `--disk-cache-max-gb
        // 18446744073` (a u64 GB count that overflows on multiplication)
        // produces "as big as we can represent", not a 0-byte cap.
        let max_bytes = opt.disk_cache_max_gb.saturating_mul(1024 * 1024 * 1024);
        match ant_retrieval::DiskChunkCache::open(&path, max_bytes) {
            Ok(c) => {
                tracing::info!(
                    target: "antd",
                    path = %path.display(),
                    max_gb = opt.disk_cache_max_gb,
                    used_bytes = c.used_bytes(),
                    "opened persistent chunk cache",
                );
                Some(Arc::new(c))
            }
            Err(e) => {
                // A failed disk-cache open is recoverable: log loudly
                // and fall back to the in-memory tier alone, rather
                // than refusing to start the daemon. Operators on a
                // full disk would otherwise lose remote retrieval
                // entirely until the disk gets cleared.
                tracing::warn!(
                    target: "antd",
                    path = %path.display(),
                    "failed to open persistent chunk cache: {e}; falling back to in-memory only",
                );
                None
            }
        }
    };

    // Single shared registry for in-flight gateway HTTP requests.
    // Built unconditionally so the node loop can read from it; only
    // the gateway side ever writes when `--no-http-api` is set, in
    // which case `gateway_activity` stays empty and `antop`
    // shows zero gateway rows. Empty registry costs one `Arc` and
    // a `Mutex<HashMap>` — well below a rounding error.
    let gateway_activity: Arc<GatewayActivity> = GatewayActivity::new();

    // `antctl upload` job manager. Built unconditionally — listing
    // and inspecting jobs work even when no postage batch is
    // configured. Actually starting a new job will fail (with the
    // standard "uploads not configured" error) on `PushChunk`
    // until the operator wires postage. Persistent state lives at
    // `<data-dir>/uploads/<job_id>.json`, atomically rewritten on
    // each checkpoint (same idiom as `StampIssuer`).
    // Default batch for `antctl upload` jobs that don't name one: the
    // operator's startup `--postage-batch`, if any. Gateway uploads pass
    // the batch via the `Swarm-Postage-Batch-Id` header instead.
    let default_upload_batch = opt
        .postage_batch
        .clone()
        .or_else(|| std::env::var("STORAGE_STAMP_BATCH_ID").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .and_then(|hex_id| {
            let stripped = strip_0x(&hex_id);
            let mut out = [0u8; 32];
            hex::decode_to_slice(stripped, &mut out).ok().map(|()| out)
        });
    let upload_manager = ant_node::UploadManager::new(
        data_dir.join("uploads"),
        cmd_tx.clone(),
        default_upload_batch,
    )
    .with_context(|| {
        format!(
            "open upload state dir at {}",
            data_dir.join("uploads").display()
        )
    })?
    // Let heal re-push missing chunks from the local chunk store
    // (where the PushChunk handler already persisted them) instead of
    // re-reading the source file — so heal survives a deleted source
    // and a node restart.
    .with_disk_cache(disk_cache.clone())
    // Read the live status watch so the automatic post-upload heal can
    // also promote shallow placements once the node is well-connected
    // enough for its closest-peer probe to be trustworthy (Sketch B).
    .with_status_watch(Some(status_rx.clone()));
    let restored_jobs = upload_manager
        .rehydrate_from_disk(!opt.no_resume_uploads)
        .with_context(|| {
            format!(
                "scan upload state dir {}",
                upload_manager.state_dir().display()
            )
        })?;
    if restored_jobs > 0 {
        tracing::info!(
            target: "antd",
            count = restored_jobs,
            auto_resume = !opt.no_resume_uploads,
            "rehydrated upload jobs from disk",
        );
    }

    // SWAP listener always-on. Inbound cheques don't require us to own
    // a chequebook (we're the beneficiary, not the issuer), so the
    // only inputs we need are our EOA and the chain id. The ledger
    // persists at `<data-dir>/swap_credits.json`; cheque monotonicity
    // survives restarts.
    let swap_cfg = ant_p2p::SwapConfig {
        our_beneficiary: eth,
        chain_id: ant_chain::tx::GNOSIS_CHAIN_ID,
        ledger_path: data_dir.join("swap_credits.json"),
        events_tx: None,
    };

    // Spawn the node loop BEFORE the chain init below. The swarm needs
    // none of the chain state to bootstrap, and the startup Gnosis RPC
    // reads (postage batch validation/rediscovery, chequebook
    // resolution) cost seconds — serialized in front of `run_node` they
    // were the dominant term of `time_to_first_peer_s` (~2.8 s of a
    // 3.1 s cold start, measured 2026-07-07). The chain-derived inputs
    // (`upload`, `pushsync_swap`) follow through `late_chain_tx`; until
    // they arrive the loop treats uploads as unconfigured
    // (`StatusSnapshot::chain_ready = false` is the client-visible
    // signal). The gateway still comes up after chain init — it needs
    // `light_mode` and the chequebook address at construction.
    let (late_chain_tx, late_chain_rx) = mpsc::channel::<ant_node::LateChainInit>(1);
    let mut node_task = tokio::spawn(run_node(
        NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
            .with_status(status_tx)
            .with_process_start(process_start)
            .with_external_addrs(external_addrs)
            .with_peerstore_path(peerstore_path)
            .with_commands(cmd_rx)
            .with_target_peers(opt.target_peers)
            .with_per_request_chunk_cache(opt.per_request_chunk_cache)
            .with_chunk_record_dir(chunk_record_dir)
            .with_gateway_activity(Some(gateway_activity.clone()))
            .with_disk_cache(disk_cache)
            .with_swap(Some(swap_cfg))
            .with_upload_manager(Some(upload_manager))
            .with_late_chain(Some(late_chain_rx)),
    ));

    // Serve the already-bound control socket immediately — it only
    // needs the status watch and the command channel, and the swarm is
    // already running. Waiting for chain init here (as the gateway
    // must) made the whole warm bootstrap (0→100 peers in ~3 s)
    // invisible to `antop`, which can only observe from the moment the
    // socket answers. During the chain-init window, upload-affecting
    // commands answer "not configured"; clients poll
    // `StatusSnapshot::chain_ready` — not the socket's existence — for
    // readiness.
    #[cfg(unix)]
    let control_task = control_bound.map(|bound| {
        tokio::spawn(bound.serve(AGENT.to_string(), status_rx.clone(), Some(cmd_tx.clone())))
    });

    // Bind the public HTTP API immediately as well (issue #38): every
    // snapshot-driven endpoint (`/health`, `/peers`, `/topology`,
    // retrieval) works from t≈0, so API consumers (Freedom polls
    // `/peers` every 500 ms) can watch the peer ramp instead of getting
    // connection-refused for the whole chain-init window — which is
    // unbounded against a slow RPC and was tripping Freedom's 60 s
    // health deadline. Chain-derived wiring arrives via
    // `gateway_chain_state` after chain init below; until then the
    // affected endpoints answer a retryable `503` and
    // `/health.chainReady` reports the progress.
    let gateway_chain_state: Arc<std::sync::OnceLock<ant_gateway::GatewayChainState>> =
        Arc::new(std::sync::OnceLock::new());
    let api_addr = opt.api_addr;
    let gateway_task = if opt.no_http_api {
        None
    } else {
        let handle = GatewayHandle {
            agent: Arc::new(AGENT.to_string()),
            api_version: Arc::new(BEE_API_VERSION.to_string()),
            identity: Arc::new(GatewayIdentity {
                overlay_hex: hex::encode(overlay),
                ethereum_hex: format!("0x{}", hex::encode(eth)),
                public_key_hex: hex::encode(&public_key_compressed),
                peer_id: peer_id.to_string(),
            }),
            status: status_rx.clone(),
            commands: cmd_tx.clone(),
            activity: gateway_activity.clone(),
            chain_state: gateway_chain_state.clone(),
            tags: Arc::new(TagRegistry::new()),
            cors: Arc::new(ant_gateway::CorsConfig::new(
                opt.cors_allowed_origins.iter(),
            )),
            // ACT publisher identity = the node's swarm key, exactly
            // bee's `accesscontrol.NewDefaultSession(swarmPrivateKey)`.
            act_secret: Arc::new(signing_secret),
        };
        Some(tokio::spawn(Gateway::serve(handle, api_addr)))
    };

    let upload = build_upload_runtime(
        opt.gnosis_rpc_url.clone(),
        resolve_logs_rpc(&opt),
        opt.postage_contract.clone(),
        opt.postage_batch.clone(),
        opt.postage_owner_key.clone(),
        signing_secret,
        eth,
        data_dir.clone(),
    )
    .await
    .map_err(|e| anyhow!("upload runtime: {e}"))?;

    // The node is "light" (publish-capable) once it can stamp + pushsync
    // uploads — i.e. whenever an upload runtime exists. With a chain RPC
    // configured that's true even before any batch is bought, so Freedom
    // (which gates its whole publish UI on `GET /node.beeMode == "light"`,
    // PLAN.md J.4.1) can buy a batch at runtime and immediately upload.
    // Captured before `upload` is moved into the node loop below.
    let light_mode = upload.is_some();

    // Outbound SWAP settlement (Phase 7b). Resolve the chequebook in
    // priority order: an operator-supplied `--chequebook` (+ `--swap-key`),
    // else a chequebook this node auto-deployed on an earlier start and
    // persisted at `<data-dir>/chequebook.json`, else — on first light
    // start with a funded node wallet — a freshly deployed,
    // factory-registered chequebook issued by the node EOA. The returned
    // address is what the chain context reports via `GET /chequebook/*`;
    // the config (when present and factory-verified) is what pushsync
    // signs cheques with. Without any of these, sustained pushsync
    // uploads stall after a few hundred chunks per peer.
    let ResolvedChequebook {
        address: chequebook_addr,
        pushsync: pushsync_swap_cfg,
    } = resolve_chequebook(&opt, &data_dir, signing_secret, eth, light_mode).await?;

    if pushsync_swap_cfg.is_some() {
        tracing::info!(
            target: "antd",
            "outbound SWAP settlement configured — pushsync will emit cheques",
        );
    } else {
        tracing::warn!(
            target: "antd",
            "outbound SWAP settlement NOT configured — pushsync uploads will stall \
             after ~20K chunks across the peer set; fund the node wallet (xDAI + xBZZ) \
             so antd can auto-deploy a chequebook on the next start, or pass \
             --chequebook + --swap-key (CHEQUEBOOK_ADDRESS + WALLET_PRIVATE_KEY)",
        );
    }

    // Hand the chain-derived inputs to the already-running swarm loop.
    // Capacity-1 channel and a single send: this never blocks. A send
    // error means the node loop already exited — its JoinHandle in the
    // select below will surface the reason.
    let _ = late_chain_tx
        .send(ant_node::LateChainInit {
            upload,
            pushsync_swap: pushsync_swap_cfg,
        })
        .await;

    // Chain context for the gateway's wallet / chequebook / status /
    // chainstate endpoints (PLAN.md J.5 A2/A3/D1/D2). Built only when a
    // Gnosis RPC endpoint is configured; the node's own Ethereum address
    // is the wallet bee-js reports balances for.
    let chain_ctx = {
        let rpc = opt
            .gnosis_rpc_url
            .clone()
            .or_else(|| std::env::var("GNOSIS_RPC_URL").ok())
            .filter(|s| !s.trim().is_empty());
        // `chequebook_addr` was resolved above (manual flag, persisted
        // auto-deploy, or a fresh first-run deploy), so `GET
        // /chequebook/{address,balance}` and the deposit write endpoint
        // all report the chequebook pushsync is actually drawing on.
        // The node's own signing key funds postage buys / chequebook
        // deposits and is the batch owner — same key that derives `eth`.
        // Reads (incl. `/stamps` `batchTTL`) fall back to the public logs
        // RPC when no operator `--gnosis-rpc-url` is set, so an
        // ultra-light node still reports a real chain-derived TTL instead
        // of the long placeholder (issue #21). Setting
        // `--gnosis-logs-rpc-url=""` (which already disables recovery)
        // opts a fully-offline node back out. Writes stay gated on `rpc`.
        ant_gateway::chainreader::build(
            rpc,
            resolve_logs_rpc(&opt),
            opt.postage_contract.clone(),
            eth,
            chequebook_addr,
            ant_chain::tx::GNOSIS_CHAIN_ID,
            Some(signing_secret),
        )
    };

    // Install the chain-derived wiring into the already-serving
    // gateway: `/node`, `/wallet`, `/chequebook/*`, `/stamps` writes
    // stop answering the chain-initializing `503` from here on, and
    // `/health.chainReady` flips to `true`. `set` only fails if the
    // slot were already filled, which nothing else does.
    let _ = gateway_chain_state.set(ant_gateway::GatewayChainState {
        light_mode,
        chain: chain_ctx,
    });

    if opt.no_control_socket && opt.no_http_api {
        drop(cmd_tx);
        return join_node_task(node_task.await);
    }

    let gateway_fut = async move {
        match gateway_task {
            Some(task) => match task.await {
                Ok(res) => res.map_err(|e| anyhow::anyhow!("gateway: {e}")),
                Err(e) => Err(anyhow::anyhow!("gateway task: {e}")),
            },
            None => std::future::pending::<Result<()>>().await,
        }
    };

    // The `antctl`/`antop` control socket is a Unix domain socket; it
    // only exists on Unix targets. On Windows antd runs the node loop +
    // HTTP gateway and operators drive it through the bee-shaped HTTP
    // API instead. `--no-control-socket` forces that same path on Unix,
    // and so does a failed bind (issue #39): the daemon then runs on
    // the node loop + HTTP API alone instead of exiting.
    #[cfg(unix)]
    let serve_control = control_task.is_some();
    #[cfg(not(unix))]
    let serve_control = false;

    if !serve_control {
        drop(cmd_tx);
        return tokio::select! {
            res = &mut node_task => join_node_task(res),
            res = gateway_fut => res,
            () = shutdown_signal() => Ok(()),
        };
    }

    #[cfg(unix)]
    {
        // The socket itself has been serving since right after the node
        // spawn (see `control_task` above); here we only join its task
        // into the daemon's exit conditions.
        let mut control_task =
            control_task.expect("serve_control implies the control task was spawned");
        tokio::select! {
            res = &mut node_task => join_node_task(res),
            res = &mut control_task => match res {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(anyhow::anyhow!("control socket: {e}")),
                Err(e) => Err(anyhow::anyhow!("control socket task: {e}")),
            },
            res = gateway_fut => res,
            () = shutdown_signal() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    unreachable!("serve_control is always false on non-unix targets")
}

/// Bind the control socket at its intended path, falling back to a
/// short per-data-dir path in the system temp dir when that fails
/// (deep data dirs — Electron `userData`, containers, CI workspaces —
/// routinely exceed `sun_path`'s ~104-byte limit; issue #39). On
/// fallback, a [`ant_control::SOCKET_POINTER_FILE`] in the data dir
/// tells `antctl`/`antop` where the socket really is. Returns `None`
/// (daemon runs without a control socket, WARN logged) only when both
/// binds fail — never aborts the daemon.
#[cfg(unix)]
fn bind_control_socket(
    intended: &Path,
    data_dir: &Path,
) -> Option<ant_control::BoundControlSocket> {
    let pointer = data_dir.join(ant_control::SOCKET_POINTER_FILE);
    match ant_control::bind(intended.to_path_buf()) {
        Ok(bound) => {
            // A pointer left over from an earlier fallback run would
            // send clients to a dead socket now that the intended path
            // works.
            let _ = std::fs::remove_file(&pointer);
            tracing::info!(
                target: "antd",
                "control socket at {}",
                bound.socket_path().display(),
            );
            Some(bound)
        }
        Err(primary) => {
            let fallback = fallback_control_socket_path(data_dir);
            match ant_control::bind(fallback.clone()) {
                Ok(bound) => {
                    tracing::warn!(
                        target: "antd",
                        "control socket cannot bind at {} ({primary}); using fallback {} \
                         (pointer file at {})",
                        intended.display(),
                        fallback.display(),
                        pointer.display(),
                    );
                    if let Err(e) = std::fs::write(&pointer, format!("{}\n", fallback.display())) {
                        tracing::warn!(
                            target: "antd",
                            "cannot write socket pointer file {}: {e}; pass --socket {} \
                             to antctl/antop explicitly",
                            pointer.display(),
                            fallback.display(),
                        );
                    }
                    Some(bound)
                }
                Err(secondary) => {
                    tracing::warn!(
                        target: "antd",
                        "control socket disabled: bind failed at {} ({primary}) and at \
                         fallback {} ({secondary}); antctl/antop will be unavailable, \
                         the node and HTTP API are unaffected",
                        intended.display(),
                        fallback.display(),
                    );
                    let _ = std::fs::remove_file(&pointer);
                    None
                }
            }
        }
    }
}

/// Short, deterministic-per-run fallback socket path in the system
/// temp dir: `antd-<hash-of-data-dir>.sock` stays under `sun_path`
/// limits on every platform we ship. Clients don't need to recompute
/// the hash — the pointer file carries the actual path.
#[cfg(unix)]
fn fallback_control_socket_path(data_dir: &Path) -> PathBuf {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    data_dir.hash(&mut h);
    std::env::temp_dir().join(format!("antd-{:016x}.sock", h.finish()))
}

/// Flatten the spawned node loop's `JoinHandle` outcome into the
/// daemon's exit result: a task-level `JoinError` (panic/cancel) is as
/// fatal as a `NodeError` from the loop itself.
fn join_node_task(
    res: Result<Result<(), ant_node::NodeError>, tokio::task::JoinError>,
) -> Result<()> {
    match res {
        Ok(inner) => inner.map_err(|e| anyhow::anyhow!("{e}")),
        Err(e) => Err(anyhow::anyhow!("node loop task: {e}")),
    }
}

/// Resolve when the process receives `SIGTERM` or `SIGINT` (Ctrl-C).
///
/// systemd / Freedom stop the daemon with `SIGTERM` and expect a prompt
/// (< 5 s) exit (PLAN.md J.5.E4). Returning from `main` drops every
/// runtime handle and exits cleanly; the durable state
/// (`StampIssuer`, upload jobs, peerstore, SWAP ledger) is already
/// checkpointed incrementally, so a clean return needs no extra flush
/// step. We win over the OS default disposition only to log the cause
/// and to give the `select!` an arm that completes — that's what lets
/// the racing futures (node loop, gateway, control socket) be dropped
/// in order rather than the process being torn down mid-syscall.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "antd", "cannot install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
                return;
            }
        };
        let mut int = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "antd", "cannot install SIGINT handler: {e}");
                std::future::pending::<()>().await;
                return;
            }
        };
        let sig = tokio::select! {
            _ = term.recv() => "SIGTERM",
            _ = int.recv() => "SIGINT",
        };
        tracing::info!(target: "antd", "received {sig}; shutting down");
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(target: "antd", "received Ctrl-C; shutting down");
    }
}

/// Resolve the optional `--record-chunks <DIR>` value to an absolute path
/// (with `~` expanded) and ensure the directory exists, so the
/// `RoutingFetcher` write path can drop a chunk into it without first
/// stat-ing the parent. Returns `Ok(None)` in release builds (the flag
/// is hidden from clap there) and when the operator didn't pass the
/// flag in debug builds; `Err` if `mkdir -p` failed.
fn resolve_chunk_record_dir(
    #[cfg(debug_assertions)] record_chunks: Option<&Path>,
) -> Result<Option<PathBuf>> {
    #[cfg(debug_assertions)]
    {
        if let Some(p) = record_chunks {
            let dir = expand_tilde(p);
            std::fs::create_dir_all(&dir)
                .with_context(|| format!("create --record-chunks dir {}", dir.display()))?;
            tracing::info!(
                target: "antd",
                "recording every fetched chunk to {} (--record-chunks)",
                dir.display(),
            );
            return Ok(Some(dir));
        }
    }
    Ok(None)
}

/// Take an exclusive advisory lock on `<data-dir>/antd.lock` so a second
/// `antd` against the same data directory fails fast instead of silently
/// joining the listen queue on `antd.sock` (which leads to split-brain
/// answers — half your `antctl` calls hit the new daemon, half the old).
///
/// Uses `flock(2)` via fs4. The lock lives on the file *descriptor*, so it
/// is released automatically when the process exits — clean shutdown,
/// panic, OOM, or `kill -9`. No stale-pid guesswork required.
fn acquire_instance_lock(lock_path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(lock_path)
        .with_context(|| format!("open instance lock {}", lock_path.display()))?;
    match FileExt::try_lock(&file) {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => {
            let owner = std::fs::read_to_string(lock_path).unwrap_or_default();
            let owner = owner.trim();
            let hint = if owner.is_empty() {
                String::new()
            } else {
                format!(" (pid {owner})")
            };
            return Err(anyhow!(
                "another antd is already running against {}{hint}; \
                 refusing to start (delete the file only if you're sure no antd is alive)",
                lock_path.display(),
            ));
        }
        Err(TryLockError::Error(e)) => {
            return Err(anyhow!(e)).with_context(|| format!("flock {}", lock_path.display()));
        }
    }
    let mut writable = &file;
    writable.set_len(0).ok();
    writable.seek(SeekFrom::Start(0)).ok();
    writeln!(writable, "{}", std::process::id()).ok();
    tracing::info!(
        target: "antd",
        "acquired instance lock {} (pid {})",
        lock_path.display(),
        std::process::id(),
    );
    Ok(file)
}

/// Bump the soft `RLIMIT_NOFILE` to the hard cap so a busy peer set + dial
/// fan-out doesn't trip the default 1024-fd ulimit on macOS / Linux. We
/// silently keep whatever the OS already gave us if the bump fails — running
/// with too few fds is a degraded but legitimate state, not a startup error.
///
/// No-op on non-Unix (Windows has no `RLIMIT_NOFILE`); the descriptor
/// ceiling there is governed differently and isn't a startup concern.
#[cfg(not(unix))]
fn raise_nofile_soft_limit() {}

#[cfg(unix)]
fn raise_nofile_soft_limit() {
    use rlimit::{getrlimit, setrlimit, Resource};
    let Ok((soft, hard)) = getrlimit(Resource::NOFILE) else {
        return;
    };
    // macOS' real per-process cap is `OPEN_MAX = 10240`, even when `hard`
    // reports `RLIM_INFINITY`. Clamp so `setrlimit` doesn't EINVAL on Darwin.
    let target = hard.min(10240).max(soft);
    if target == soft {
        return;
    }
    if let Err(e) = setrlimit(Resource::NOFILE, target, hard) {
        tracing::warn!(
            target: "antd",
            "could not raise RLIMIT_NOFILE from {soft} to {target}: {e}; \
             expect 'too many open files' under load",
        );
        return;
    }
    tracing::info!(target: "antd", "raised RLIMIT_NOFILE soft limit {soft} → {target}");
}

fn expand_tilde(p: &Path) -> PathBuf {
    if let Some(s) = p.to_str() {
        if let Some(rest) = s.strip_prefix("~/") {
            return dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(rest);
        }
    }
    p.to_path_buf()
}

/// Load `--config` (if given), merging its values into `opt` for every
/// setting the operator did **not** pass on the command line. Returns
/// the resolved keystore password (from `--password` / `--password-file`
/// or the config's `password` / `password-file`), if any.
///
/// CLI > config file > default — the same precedence bee uses, so a
/// Freedom-written config behaves predictably while an operator can
/// still override one knob on the command line.
fn apply_config_file(opt: &mut Opt, matches: &clap::ArgMatches) -> Result<Option<String>> {
    let from_cli = |id: &str| matches.value_source(id) == Some(ValueSource::CommandLine);

    // CLI password flags take precedence; fall back to the config file
    // below once it's loaded.
    let cli_password = opt.password.clone();
    let cli_password_file = opt.password_file.clone();

    let cfg = match opt.config.as_ref() {
        Some(path) => Some(config::BeeConfig::load(&expand_tilde(path))?),
        None => None,
    };

    if let Some(cfg) = &cfg {
        if !from_cli("data_dir") {
            if let Some(d) = &cfg.data_dir {
                opt.data_dir = PathBuf::from(d);
            }
        }
        if !from_cli("api_addr") {
            if let Some(addr) = cfg.api_socket_addr()? {
                opt.api_addr = addr;
            }
        }
        if !from_cli("network_id") {
            if let Some(n) = cfg.network_id() {
                opt.network_id = n;
            }
        }
        if !from_cli("gnosis_rpc_url") {
            if let Some(rpc) = &cfg.blockchain_rpc_endpoint {
                opt.gnosis_rpc_url = Some(rpc.clone());
            }
        }
        if !from_cli("cors_allowed_origins") {
            let origins = cfg.cors_origins_vec();
            if !origins.is_empty() {
                opt.cors_allowed_origins = origins;
            }
        }
        if !from_cli("log_level") {
            if let Some(level) = cfg.log_level() {
                opt.log_level = level;
            }
        }
        let ignored = cfg.extra.len();
        if ignored > 0 {
            let keys: Vec<&str> = cfg.extra.keys().map(String::as_str).collect();
            tracing::debug!(
                target: "antd",
                "ignoring {ignored} unmodelled bee config key(s): {}",
                keys.join(", "),
            );
        }
    }

    // Resolve the password: CLI flag wins, then CLI password-file, then
    // the config file's `password` / `password-file`.
    if let Some(p) = cli_password {
        return Ok(Some(p));
    }
    if let Some(file) = cli_password_file {
        let raw = std::fs::read_to_string(&file)
            .with_context(|| format!("read --password-file {}", file.display()))?;
        return Ok(Some(raw.trim_end_matches(['\n', '\r']).to_string()));
    }
    if let Some(cfg) = &cfg {
        return cfg.resolve_password();
    }
    Ok(None)
}

/// Load the node identity from a bee Web3 v3 keystore at
/// `<data-dir>/keys/swarm.key` (PLAN.md J.5.E3). The decrypted 32-byte
/// secp256k1 secret becomes our signing key; the libp2p keypair is
/// derived from it and the overlay nonce is zero (bee's default for a
/// node whose nonce isn't separately configured). Using the
/// Freedom-managed keystore as the source of truth means the daemon's
/// Ethereum identity matches the one Freedom provisioned.
fn load_identity_from_keystore(
    swarm_key_path: &std::path::Path,
    password: Option<&str>,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; 32], Keypair)> {
    let password = password.ok_or_else(|| {
        anyhow!(
            "found a bee keystore at {} but no password is configured; \
             set `password` / `password-file` in the --config file or pass --password",
            swarm_key_path.display(),
        )
    })?;
    let json = std::fs::read_to_string(swarm_key_path)
        .with_context(|| format!("read keystore {}", swarm_key_path.display()))?;
    let signing_secret = keystore::decrypt_v3(&json, password)
        .with_context(|| format!("decrypt keystore {}", swarm_key_path.display()))?;
    let overlay_nonce = [0u8; 32];
    let kp = secp256k1_keypair_from_signing_secret(&signing_secret)?;
    tracing::info!(
        target: "antd",
        keystore = %swarm_key_path.display(),
        "loaded node identity from bee v3 keystore",
    );
    Ok((signing_secret, overlay_nonce, kp))
}

fn load_or_create_identity(
    id_path: &std::path::Path,
    key_path: &std::path::Path,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; 32], Keypair)> {
    if id_path.exists() {
        let raw = std::fs::read_to_string(id_path)
            .with_context(|| format!("read {}", id_path.display()))?;
        let id: IdentityFile = serde_json::from_str(&raw).context("parse identity.json")?;
        let mut signing_secret = [0u8; SECP256K1_SECRET_LEN];
        hex::decode_to_slice(&id.signing_key, &mut signing_secret).context("decode signing_key")?;
        let mut overlay_nonce = [0u8; 32];
        hex::decode_to_slice(&id.overlay_nonce, &mut overlay_nonce)
            .context("decode overlay_nonce")?;
        let kp = if let Some(ref enc) = id.libp2p_keypair {
            let bytes = hex::decode(enc).context("decode libp2p_keypair")?;
            Keypair::from_protobuf_encoding(&bytes).context("libp2p keypair protobuf")?
        } else {
            secp256k1_keypair_from_signing_secret(&signing_secret)?
        };
        return Ok((signing_secret, overlay_nonce, kp));
    }

    let signing_secret = random_secp256k1_secret();
    let overlay_nonce = random_overlay_nonce();
    let kp = secp256k1_keypair_from_signing_secret(&signing_secret)?;

    let id = IdentityFile {
        signing_key: hex::encode(signing_secret),
        overlay_nonce: hex::encode(overlay_nonce),
        libp2p_keypair: Some(hex::encode(kp.to_protobuf_encoding()?)),
    };
    std::fs::write(
        id_path,
        serde_json::to_string_pretty(&id).context("serialize identity")?,
    )
    .with_context(|| format!("write {}", id_path.display()))?;

    // Optional raw key file for tooling.
    let _ = std::fs::write(key_path, hex::encode(signing_secret));

    Ok((signing_secret, overlay_nonce, kp))
}

/// Build a [`UploadRuntime`] — the live registry of postage batches the
/// node can stamp uploads with.
///
/// The node wallet (`signing_secret` / `node_eth`) owns every batch it
/// buys on-chain, so a single stamp key signs for all of them; the
/// registry is keyed by batch id and grows at runtime via
/// [`ant_control::ControlCommand::RegisterBatch`] when Freedom buys a
/// batch through `POST /stamps/{amount}/{depth}`.
///
/// At startup we:
///  1. **Reload persisted batches** by scanning `<data_dir>/postage/*.bin`
///     and reopening each [`StampIssuer`] from its header, so a restart
///     resumes every previously-bought batch's counters (no index is
///     ever re-issued — bee peers reject double-spends).
///  2. **Pre-register `--postage-batch`** (back-compat for operators):
///     fetch its on-chain `batchOwner / depth / bucketDepth / immutable`,
///     check the owner against the stamp key, and open its store.
///
/// Returns `Ok(None)` (ultra-light, read-only) only when the node can
/// neither buy (no RPC / chain writer) nor stamp an existing batch.
/// Whenever an RPC endpoint is configured we return `Some` even with an
/// empty registry, so `GET /node` reports `beeMode:"light"` and Freedom
/// lets the publish flow proceed.
///
/// `cli_key` (`--postage-owner-key` / `STORAGE_STAMP_PRIVATE_KEY`) keeps
/// the legacy single-owner behaviour: when set it becomes the stamp key
/// and the configured batch must be owned by it. When absent the node's
/// own wallet key signs stamps (the bee light-node model).
// Startup wiring: each argument is an independent config/identity input
// threaded from `main`; bundling them into a struct would only move the
// list elsewhere without improving clarity.
#[allow(clippy::too_many_arguments)]
async fn build_upload_runtime(
    cli_rpc: Option<String>,
    logs_rpc: Option<String>,
    postage_contract: String,
    cli_batch: Option<String>,
    cli_key: Option<String>,
    signing_secret: [u8; SECP256K1_SECRET_LEN],
    node_eth: [u8; 20],
    data_dir: PathBuf,
) -> Result<Option<Arc<UploadRuntime>>> {
    let postage_dir = data_dir.join("postage");

    let rpc_url = cli_rpc
        .or_else(|| std::env::var("GNOSIS_RPC_URL").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let can_stamp = rpc_url.is_some();

    let batch_hex = cli_batch
        .or_else(|| std::env::var("STORAGE_STAMP_BATCH_ID").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let key_hex = cli_key
        .or_else(|| std::env::var("STORAGE_STAMP_PRIVATE_KEY").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    // Stamp key + owner. Default to the node wallet so batches bought at
    // runtime (owned by the node EOA) validate; a legacy owner key keeps
    // the old single-owner pre-configured-batch behaviour.
    let (stamp_key, batch_owner) = match &key_hex {
        Some(k) => {
            let mut sk = [0u8; SECP256K1_SECRET_LEN];
            hex::decode_to_slice(strip_0x(k), &mut sk).context("decode postage owner key")?;
            let eth = {
                let s =
                    SigningKey::from_bytes((&sk).into()).context("postage owner key invalid")?;
                ethereum_address_from_public_key(s.verifying_key())
            };
            (sk, eth)
        }
        None => (signing_secret, node_eth),
    };

    let mut issuers: std::collections::HashMap<[u8; 32], ant_postage::StampIssuer> =
        std::collections::HashMap::new();

    // 1. Reload batches persisted from a previous run.
    if postage_dir.is_dir() {
        match std::fs::read_dir(&postage_dir) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) != Some("bin") {
                        continue;
                    }
                    match ant_postage::StampIssuer::open_existing(path.clone()) {
                        Ok(iss) => {
                            let id = *iss.batch_id();
                            // Phantom-batch guard (2026-07-12): a
                            // persisted issuer proves only that WE once
                            // held the batch — not that the chain still
                            // does (expiry evicts it; a failed/foreign-
                            // chain buy never registered it). Storer
                            // peers validate every stamp against their
                            // chain-synced batchstore, so an
                            // unconfirmable batch means every push is
                            // rejected while /stamps reads green.
                            // When an RPC is configured, confirm each
                            // reloaded batch on-chain and SKIP the ones
                            // the chain disowns (the .bin stays on disk
                            // — a later re-buy or re-sync recovers).
                            // RPC read errors keep the batch
                            // (unconfirmed ≠ dead); no RPC keeps the
                            // historical trust-the-disk behaviour.
                            if let Some(rpc) = rpc_url.clone() {
                                let chain = ant_chain::ChainClient::new(rpc);
                                match ant_chain::fetch_postage_batch_meta(
                                    &chain,
                                    &postage_contract,
                                    &id,
                                )
                                .await
                                {
                                    Ok(meta) if meta.batch_owner_eth == [0u8; 20] => {
                                        tracing::warn!(
                                            target: "antd",
                                            batch = %format!("0x{}", hex::encode(id)),
                                            store = %path.display(),
                                            "persisted batch NOT FOUND on-chain (expired or never created) — not registering it; uploads with it would be rejected by every storer",
                                        );
                                        continue;
                                    }
                                    Ok(meta) if meta.batch_owner_eth != batch_owner => {
                                        tracing::warn!(
                                            target: "antd",
                                            batch = %format!("0x{}", hex::encode(id)),
                                            on_chain_owner = %format!("0x{}", hex::encode(meta.batch_owner_eth)),
                                            our_owner = %format!("0x{}", hex::encode(batch_owner)),
                                            "persisted batch is owned by a different key on-chain — not registering it (stamps we sign would be rejected)",
                                        );
                                        continue;
                                    }
                                    Ok(_) => {}
                                    Err(e) => tracing::warn!(
                                        target: "antd",
                                        batch = %format!("0x{}", hex::encode(id)),
                                        "could not confirm persisted batch on-chain ({e}); registering it unverified",
                                    ),
                                }
                            }
                            tracing::info!(
                                target: "antd",
                                batch = %format!("0x{}", hex::encode(id)),
                                store = %path.display(),
                                resumed_indices = iss.issued_count(),
                                "reloaded persisted postage batch",
                            );
                            issuers.insert(id, iss);
                        }
                        Err(e) => tracing::warn!(
                            target: "antd",
                            store = %path.display(),
                            "skipping unreadable postage store: {e}",
                        ),
                    }
                }
            }
            Err(e) => tracing::warn!(
                target: "antd",
                dir = %postage_dir.display(),
                "scan postage dir: {e}",
            ),
        }
    }

    // 2. Pre-register an operator-configured batch (--postage-batch).
    if let Some(batch_hex) = &batch_hex {
        let rpc = rpc_url.clone().ok_or_else(|| {
            anyhow!("--postage-batch set but --gnosis-rpc-url / GNOSIS_RPC_URL missing")
        })?;
        let mut batch_id = [0u8; 32];
        hex::decode_to_slice(strip_0x(batch_hex), &mut batch_id)
            .with_context(|| format!("decode batch id {batch_hex}"))?;
        let chain = ant_chain::ChainClient::new(rpc);
        let meta = ant_chain::fetch_postage_batch_meta(&chain, &postage_contract, &batch_id)
            .await
            .with_context(|| format!("fetch postage batch meta from {postage_contract}"))?;
        if batch_owner != meta.batch_owner_eth {
            return Err(anyhow!(
                "postage stamp owner 0x{} does not match on-chain batchOwner 0x{} for batch {}",
                hex::encode(batch_owner),
                hex::encode(meta.batch_owner_eth),
                batch_hex,
            ));
        }
        // The postage-dir scan above may have already reloaded this
        // batch's store — and it holds the store's exclusive lock, so
        // re-opening here would refuse against *ourselves* and fail the
        // boot. The on-chain owner check above still ran; the reloaded
        // issuer's counters/sidecar are strictly fresher than a re-open.
        match issuers.entry(batch_id) {
            std::collections::hash_map::Entry::Occupied(_) => {
                tracing::info!(
                    target: "antd",
                    batch = %format!("0x{}", hex::encode(batch_id)),
                    "operator postage batch already reloaded from the postage dir",
                );
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
                let store_path = postage_dir.join(format!("{}.bin", hex::encode(batch_id)));
                let issuer = ant_postage::StampIssuer::open_or_new(
                    store_path.clone(),
                    batch_id,
                    meta.depth,
                    meta.bucket_depth,
                    meta.immutable,
                )
                .map_err(|e| anyhow!("StampIssuer: {e}"))?;
                tracing::info!(
                    target: "antd",
                    batch = %format!("0x{}", hex::encode(batch_id)),
                    owner = %format!("0x{}", hex::encode(meta.batch_owner_eth)),
                    depth = meta.depth,
                    bucket_depth = meta.bucket_depth,
                    immutable = meta.immutable,
                    store = %store_path.display(),
                    "pre-registered operator postage batch",
                );
                slot.insert(issuer);
            }
        }
    }

    // 3. Rediscover owned batches from chain (bee-parity recovery). A
    //    node started on a bee data dir owns funded batches on-chain
    //    that aren't in any local registry; surface them as usable
    //    issuers, carrying over bee's bucket counters when a
    //    `stamperstore` is present. Independent of storage incentives
    //    (a light node tracks its own batches). Best-effort: a flaky or
    //    range-capped RPC must never stop the daemon from starting.
    if let Some(logs_rpc) = logs_rpc {
        let client = ant_chain::ChainClient::new(logs_rpc);
        match ant_chain::discover::discover_owned_batches(
            &client,
            &postage_contract,
            ant_chain::GNOSIS_BZZ_TOKEN,
            &batch_owner,
            ant_chain::discover::GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
        {
            Ok(found) => {
                let stamperstore = data_dir.join("stamperstore");
                for b in found {
                    if issuers.contains_key(&b.batch_id) {
                        continue; // already reloaded / pre-registered
                    }
                    let store_path = postage_dir.join(format!("{}.bin", hex::encode(b.batch_id)));
                    match open_recovered_issuer(&stamperstore, &store_path, &b) {
                        Ok(iss) => {
                            tracing::info!(
                                target: "antd",
                                batch = %format!("0x{}", hex::encode(b.batch_id)),
                                depth = b.depth,
                                bucket_depth = b.bucket_depth,
                                immutable = b.immutable,
                                remaining_balance = b.remaining_balance,
                                "rediscovered owned postage batch from chain",
                            );
                            issuers.insert(b.batch_id, iss);
                        }
                        Err(e) => tracing::warn!(
                            target: "antd",
                            batch = %format!("0x{}", hex::encode(b.batch_id)),
                            "could not open rediscovered batch: {e}",
                        ),
                    }
                }
            }
            Err(e) => tracing::warn!(
                target: "antd",
                "postage batch rediscovery scan failed: {e}; continuing without it",
            ),
        }
    }

    // Nothing to stamp with and no way to buy → ultra-light.
    if !can_stamp && issuers.is_empty() {
        tracing::info!(
            target: "antd",
            "uploads disabled: no blockchain-rpc-endpoint and no postage batch — node is ultra-light (read-only)",
        );
        return Ok(None);
    }

    tracing::info!(
        target: "antd",
        batches = issuers.len(),
        can_buy = can_stamp,
        owner = %format!("0x{}", hex::encode(batch_owner)),
        "upload runtime ready (postage stamping)",
    );

    Ok(Some(Arc::new(UploadRuntime {
        issuers: std::sync::Mutex::new(issuers),
        stamp_key,
        batch_owner,
        postage_dir,
    })))
}

/// Open a [`ant_postage::StampIssuer`] for a batch rediscovered on
/// chain, carrying over bee's per-bucket counters from a
/// `stamperstore` when one is present (option a) and falling back to a
/// fresh issuer with all-zero counters otherwise (option b). The
/// counters are the only batch state not on-chain; seeding them keeps
/// the node from re-stamping `(batchId, bucket, index)` slots the
/// network has already seen.
fn open_recovered_issuer(
    stamperstore: &Path,
    store_path: &Path,
    b: &ant_chain::discover::DiscoveredBatch,
) -> Result<ant_postage::StampIssuer> {
    let want = 1usize << u32::from(b.bucket_depth);
    let fresh = || {
        ant_postage::StampIssuer::open_or_new(
            store_path.to_path_buf(),
            b.batch_id,
            b.depth,
            b.bucket_depth,
            b.immutable,
        )
        .map_err(|e| anyhow!("{e}"))
    };
    match ant_postage::beestore::recover_bee_buckets(stamperstore, &b.batch_id) {
        Ok(Some(rec)) if rec.bucket_depth == b.bucket_depth && rec.buckets.len() == want => {
            let issued: u64 = rec.buckets.iter().map(|&c| u64::from(c)).sum();
            tracing::info!(
                target: "antd",
                batch = %format!("0x{}", hex::encode(b.batch_id)),
                issued,
                "carried over bee stamp-issuer bucket counters (recovery option a)",
            );
            ant_postage::StampIssuer::open_or_new_seeded(
                store_path.to_path_buf(),
                b.batch_id,
                b.depth,
                b.bucket_depth,
                b.immutable,
                &rec.buckets,
            )
            .map_err(|e| anyhow!("{e}"))
        }
        Ok(Some(rec)) => {
            tracing::warn!(
                target: "antd",
                batch = %format!("0x{}", hex::encode(b.batch_id)),
                bee_bucket_depth = rec.bucket_depth,
                chain_bucket_depth = b.bucket_depth,
                "bee bucket-counter shape mismatch; starting issuer fresh (recovery option b)",
            );
            fresh()
        }
        Ok(None) => {
            tracing::info!(
                target: "antd",
                batch = %format!("0x{}", hex::encode(b.batch_id)),
                "no bee stamperstore counters for batch; fresh issuer at 0 (recovery option b)",
            );
            fresh()
        }
        Err(e) => {
            tracing::warn!(
                target: "antd",
                batch = %format!("0x{}", hex::encode(b.batch_id)),
                "bee stamperstore recovery failed: {e}; fresh issuer at 0 (recovery option b)",
            );
            fresh()
        }
    }
}

/// Outcome of resolving the outbound-settlement chequebook: the
/// address to report through the chain context's `GET /chequebook/*`
/// endpoints, and the (factory-verified) pushsync config used to sign
/// cheques. Both are `None` when no settlement is configured.
struct ResolvedChequebook {
    address: Option<[u8; 20]>,
    pushsync: Option<ant_p2p::PushsyncSwapConfig>,
}

/// Resolve the chequebook for outbound SWAP settlement, in priority
/// order:
///
/// 1. **Manual** — an operator-supplied `--chequebook` (+ `--swap-key`
///    / env). Never auto-persisted; the operator owns its lifecycle.
/// 2. **Persisted** — a chequebook this node auto-deployed on an
///    earlier start, reloaded from `<data-dir>/chequebook.json`. Issuer
///    is the node EOA, so the node `signing_secret` signs cheques.
/// 3. **Rediscovered** — on a data dir carried over from bee (or one
///    whose `chequebook.json` was lost), the node EOA's chequebook is
///    rediscovered on-chain from the node key, adopted, and persisted.
/// 4. **Auto-deploy** — on first light start with a funded node wallet
///    and none of the above, deploy a fresh factory-registered
///    chequebook (issuer = node EOA), fund it with xBZZ, and persist it.
///
/// A standalone `--swap-key` without a chequebook keeps the historical
/// "disabled" behaviour rather than auto-deploying with a non-node
/// issuer — auto-deploy never injects an external key. Only an
/// authoritative step-3 answer ("this EOA owns no chequebook") reaches
/// step 4: a rediscovery scan that *failed* starts the node without
/// settlement instead, since deploying on unread chain state would
/// strand the deposit in a chequebook we simply could not see.
async fn resolve_chequebook(
    opt: &Opt,
    data_dir: &Path,
    signing_secret: [u8; SECP256K1_SECRET_LEN],
    node_eth: [u8; 20],
    light_mode: bool,
) -> Result<ResolvedChequebook> {
    let rpc_url = opt
        .gnosis_rpc_url
        .clone()
        .or_else(|| std::env::var("GNOSIS_RPC_URL").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let manual_cb = opt
        .chequebook
        .clone()
        .or_else(|| std::env::var("CHEQUEBOOK_ADDRESS").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let manual_key = opt
        .swap_key
        .clone()
        .or_else(|| std::env::var("SWAP_OWNER_KEY").ok())
        .or_else(|| std::env::var("WALLET_PRIVATE_KEY").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let ledger_path = data_dir.join("pushsync_outbound.json");
    let persist_path = data_dir.join("chequebook.json");

    // 1. Manual chequebook — operator-managed, never auto-persisted.
    if let Some(cb_hex) = manual_cb {
        let mut chequebook = [0u8; 20];
        hex::decode_to_slice(strip_0x(&cb_hex), &mut chequebook)
            .with_context(|| format!("decode --chequebook {cb_hex}"))?;
        let Some(key_hex) = manual_key else {
            return Err(anyhow!(
                "--chequebook is set but no --swap-key / SWAP_OWNER_KEY / WALLET_PRIVATE_KEY \
                 found; outbound SWAP settlement needs both",
            ));
        };
        let mut swap_secret = [0u8; SECP256K1_SECRET_LEN];
        hex::decode_to_slice(strip_0x(&key_hex), &mut swap_secret).context("decode --swap-key")?;
        let swap_eoa = {
            let sk = SigningKey::from_bytes((&swap_secret).into()).context("--swap-key invalid")?;
            ethereum_address_from_public_key(sk.verifying_key())
        };
        tracing::info!(
            target: "antd",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            issuer_eoa = %format!("0x{}", hex::encode(swap_eoa)),
            "outbound SWAP settlement: chequebook + issuer key configured (manual)",
        );
        let pushsync = verify_then_build_swap(
            chequebook,
            swap_secret,
            rpc_url.as_deref(),
            opt.chequebook_allow_unverified,
            &ledger_path,
        )
        .await;
        return Ok(ResolvedChequebook {
            address: Some(chequebook),
            pushsync,
        });
    }

    // A standalone swap key without a chequebook: if it nominates a
    // *different* issuer EOA than the node, the operator wants a
    // specific issuer but hasn't deployed its contract — keep the
    // historical "disabled" behaviour rather than auto-deploying one
    // issued by the node EOA. If it just matches the node key (the
    // common case — e.g. `WALLET_PRIVATE_KEY` == the node identity in a
    // dev `.env`), fall through to persisted / auto-deploy.
    if let Some(key_hex) = &manual_key {
        let mut swap_secret = [0u8; SECP256K1_SECRET_LEN];
        hex::decode_to_slice(strip_0x(key_hex), &mut swap_secret)
            .context("decode SWAP_OWNER_KEY / WALLET_PRIVATE_KEY")?;
        let swap_eoa = {
            let sk = SigningKey::from_bytes((&swap_secret).into())
                .context("SWAP_OWNER_KEY / WALLET_PRIVATE_KEY invalid")?;
            ethereum_address_from_public_key(sk.verifying_key())
        };
        if swap_eoa != node_eth {
            tracing::info!(
                target: "antd",
                issuer_eoa = %format!("0x{}", hex::encode(swap_eoa)),
                "swap key nominates a non-node issuer EOA but no --chequebook / \
                 CHEQUEBOOK_ADDRESS; outbound SWAP settlement disabled (run \
                 `antctl chequebook deploy` for that issuer, or unset the swap key to let \
                 antd auto-deploy a chequebook issued by the node EOA)",
            );
            return Ok(ResolvedChequebook {
                address: None,
                pushsync: None,
            });
        }
    }

    // 2. Persisted auto-deployed chequebook — reuse forever.
    if let Some(persisted) = ant_chain::chequebook_store::load_persisted_chequebook(&persist_path)?
    {
        tracing::info!(
            target: "antd",
            chequebook = %format!("0x{}", hex::encode(persisted)),
            store = %persist_path.display(),
            "reusing persisted auto-deployed chequebook (issuer = node EOA)",
        );
        let pushsync = verify_then_build_swap(
            persisted,
            signing_secret,
            rpc_url.as_deref(),
            opt.chequebook_allow_unverified,
            &ledger_path,
        )
        .await;
        return Ok(ResolvedChequebook {
            address: Some(persisted),
            pushsync,
        });
    }

    // 3. Rediscover a chequebook this node already deployed (bee-parity
    //    recovery). On a data dir carried over from bee, or one whose
    //    `chequebook.json` was lost, the node EOA's chequebook is
    //    rediscoverable on-chain from the node key. Adopt it (persist
    //    the association so future starts skip the scan) rather than
    //    deploying a fresh one and stranding the old balance.
    if let Some(logs_rpc) = resolve_logs_rpc(opt) {
        match ant_chain::discover::discover_owned_chequebook(
            &ant_chain::ChainClient::new(logs_rpc),
            &ant_chain::chequebook::GNOSIS_CHEQUEBOOK_FACTORY,
            &opt.postage_contract,
            ant_chain::GNOSIS_BZZ_TOKEN,
            &node_eth,
            ant_chain::discover::GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
        {
            Ok(Some(cb)) => {
                tracing::info!(
                    target: "antd",
                    chequebook = %format!("0x{}", hex::encode(cb)),
                    issuer = %format!("0x{}", hex::encode(node_eth)),
                    "rediscovered node-owned chequebook on-chain; adopting it (no fresh deploy)",
                );
                // Persist so subsequent starts reload it directly (the
                // scan becomes a one-time cost). Salt / deploy tx are
                // unknown for a rediscovered chequebook; the issuer is
                // the node EOA, so `signing_secret` signs cheques.
                if let Err(e) = ant_chain::chequebook_store::persist_chequebook(
                    &persist_path,
                    &ant_chain::chequebook_store::ChequebookFile::rediscovered(&cb, &node_eth),
                ) {
                    tracing::warn!(
                        target: "antd",
                        error = %format!("{e:#}"),
                        "could not persist rediscovered chequebook association; will rediscover next start",
                    );
                }
                let pushsync = verify_then_build_swap(
                    cb,
                    signing_secret,
                    rpc_url.as_deref(),
                    opt.chequebook_allow_unverified,
                    &ledger_path,
                )
                .await;
                return Ok(ResolvedChequebook {
                    address: Some(cb),
                    pushsync,
                });
            }
            // Authoritative: every candidate was read, none is ours —
            // the only answer that may lead to an auto-deploy.
            Ok(None) => tracing::info!(
                target: "antd",
                "no node-owned chequebook found on-chain; will auto-deploy if enabled",
            ),
            // A scan that *failed* is not "no chequebook exists":
            // auto-deploying on it burns gas and strands the deposit in
            // the chequebook we could not see. Start without outbound
            // settlement instead and let the next start rescan — that is
            // recoverable, a stranded deposit is not.
            Err(e) => {
                tracing::warn!(
                    target: "antd",
                    "chequebook rediscovery scan failed: {e}; starting without outbound SWAP \
                     settlement rather than deploying a chequebook on chain state we could not \
                     read (the next start retries the scan)",
                );
                return Ok(ResolvedChequebook {
                    address: None,
                    pushsync: None,
                });
            }
        }
    }

    // 4. First-run auto-deploy. Gated on light mode (we only issue
    //    cheques when we can upload), an RPC endpoint, and the operator
    //    not opting out.
    if opt.no_auto_chequebook {
        return Ok(ResolvedChequebook {
            address: None,
            pushsync: None,
        });
    }
    let Some(rpc) = rpc_url.clone() else {
        return Ok(ResolvedChequebook {
            address: None,
            pushsync: None,
        });
    };
    if !light_mode {
        tracing::info!(
            target: "antd",
            "node is read-only (no upload runtime); skipping chequebook auto-deploy",
        );
        return Ok(ResolvedChequebook {
            address: None,
            pushsync: None,
        });
    }

    let client = ant_chain::ChainClient::new(&rpc);
    let wallet = match ant_chain::tx::Wallet::new(signing_secret, ant_chain::tx::GNOSIS_CHAIN_ID) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(
                target: "antd",
                error = %format!("{e:#}"),
                "could not derive node wallet for chequebook auto-deploy; starting without outbound SWAP settlement",
            );
            return Ok(ResolvedChequebook {
                address: None,
                pushsync: None,
            });
        }
    };
    match ant_chain::chequebook_store::auto_deploy_chequebook(
        &client,
        &wallet,
        &node_eth,
        opt.chequebook_deposit_plur,
        &persist_path,
    )
    .await
    {
        Ok(cb) => {
            // Freshly factory-deployed, so it's registered by construction
            // — skip the redundant `deployedContracts` round-trip.
            let pushsync = ant_p2p::PushsyncSwapConfig::new(
                cb,
                signing_secret,
                ant_chain::tx::GNOSIS_CHAIN_ID,
                ledger_path,
                ant_p2p::PeerEthMap::new(),
            );
            Ok(ResolvedChequebook {
                address: Some(cb),
                pushsync: Some(pushsync),
            })
        }
        Err(e) => {
            // Auto-deploy is best-effort: a thin/unfunded wallet or a
            // flaky RPC must not stop the node from starting (reads,
            // small uploads still work). Log loudly and run without
            // outbound settlement.
            tracing::warn!(
                target: "antd",
                error = %format!("{e:#}"),
                "chequebook auto-deploy failed; starting without outbound SWAP settlement",
            );
            Ok(ResolvedChequebook {
                address: None,
                pushsync: None,
            })
        }
    }
}

/// Run the startup `factory.deployedContracts(chequebook)` check and,
/// unless it fails (and `--chequebook-allow-unverified` is unset),
/// build the pushsync swap config. Cheques drawn on a chequebook the
/// factory doesn't know about are silently dropped by bee's
/// `chequeStore.ReceiveCheque`, so emitting them just wastes bandwidth.
/// Without an RPC we can't check and build unconditionally, matching
/// the historical manual behaviour.
async fn verify_then_build_swap(
    chequebook: [u8; 20],
    swap_secret: [u8; SECP256K1_SECRET_LEN],
    rpc_url: Option<&str>,
    allow_unverified: bool,
    ledger_path: &Path,
) -> Option<ant_p2p::PushsyncSwapConfig> {
    if let Some(rpc) = rpc_url {
        let client = ant_chain::ChainClient::new(rpc);
        match ant_chain::chequebook_store::verify_chequebook_with_factory(&client, &chequebook)
            .await
        {
            Ok(true) => tracing::info!(
                target: "antd",
                chequebook = %format!("0x{}", hex::encode(chequebook)),
                "factory check passed — chequebook is registered with the Swarm chequebook factory",
            ),
            Ok(false) if allow_unverified => tracing::warn!(
                target: "antd",
                chequebook = %format!("0x{}", hex::encode(chequebook)),
                "chequebook is NOT registered with the Swarm chequebook factory, but --chequebook-allow-unverified was set; bee will reject cheques drawn on this chequebook",
            ),
            Ok(false) => {
                tracing::error!(
                    target: "antd",
                    chequebook = %format!("0x{}", hex::encode(chequebook)),
                    "chequebook is NOT registered with the Swarm chequebook factory; \
                     bee will silently reject every cheque we emit. \
                     Disabling outbound SWAP settlement. \
                     Run `antctl chequebook deploy` to deploy a new factory-registered chequebook, \
                     or pass --chequebook-allow-unverified to override (devnet only).",
                );
                return None;
            }
            Err(e) => tracing::warn!(
                target: "antd",
                chequebook = %format!("0x{}", hex::encode(chequebook)),
                error = %e,
                "factory.deployedContracts call failed; skipping startup factory check",
            ),
        }

        // Issuer-match check (Fix 4). bee accepts a cheque only when its
        // signer matches the chequebook's on-chain `issuer()`; a node
        // whose cheque-signing key differs emits cheques every peer
        // silently drops, leaving uploads on pseudosettle credit only.
        // Verify it before enabling outbound SWAP rather than discover
        // it as a settlement stall under load.
        let swap_eoa = match SigningKey::from_bytes((&swap_secret).into()) {
            Ok(sk) => Some(ethereum_address_from_public_key(sk.verifying_key())),
            Err(e) => {
                tracing::warn!(
                    target: "antd",
                    error = %e,
                    "could not derive cheque-signing EOA from swap key; skipping issuer-match check",
                );
                None
            }
        };
        if let Some(swap_eoa) = swap_eoa {
            match ant_chain::chequebook_store::read_chequebook_issuer(&client, &chequebook).await {
                Ok(issuer) if issuer == swap_eoa => tracing::info!(
                    target: "antd",
                    chequebook = %format!("0x{}", hex::encode(chequebook)),
                    issuer = %format!("0x{}", hex::encode(issuer)),
                    "issuer check passed — chequebook issuer() matches the cheque-signing key",
                ),
                Ok(issuer) if allow_unverified => tracing::warn!(
                    target: "antd",
                    chequebook = %format!("0x{}", hex::encode(chequebook)),
                    issuer = %format!("0x{}", hex::encode(issuer)),
                    swap_eoa = %format!("0x{}", hex::encode(swap_eoa)),
                    "chequebook issuer() does NOT match the cheque-signing key, but --chequebook-allow-unverified was set; bee peers will reject every cheque we emit",
                ),
                Ok(issuer) => {
                    tracing::error!(
                        target: "antd",
                        chequebook = %format!("0x{}", hex::encode(chequebook)),
                        issuer = %format!("0x{}", hex::encode(issuer)),
                        swap_eoa = %format!("0x{}", hex::encode(swap_eoa)),
                        "chequebook issuer() ({}) does not match the cheque-signing key ({}); \
                         bee peers silently reject every cheque drawn on this chequebook, so \
                         uploads would run on pseudosettle credit only. Disabling outbound SWAP \
                         settlement. Use a chequebook whose issuer() equals the swap key's EOA \
                         (deploy one with `antctl chequebook deploy`), or pass \
                         --chequebook-allow-unverified to override (devnet only).",
                        format!("0x{}", hex::encode(issuer)),
                        format!("0x{}", hex::encode(swap_eoa)),
                    );
                    return None;
                }
                Err(e) => tracing::warn!(
                    target: "antd",
                    chequebook = %format!("0x{}", hex::encode(chequebook)),
                    error = %e,
                    "chequebook.issuer() call failed; skipping issuer-match check",
                ),
            }
        }
    }
    // Placeholder `PeerEthMap`; `ant-node::run_node` overwrites the
    // field with the live one shared with the swarm loop before the
    // config reaches `run`.
    Some(ant_p2p::PushsyncSwapConfig::new(
        chequebook,
        swap_secret,
        ant_chain::tx::GNOSIS_CHAIN_ID,
        ledger_path.to_path_buf(),
        ant_p2p::PeerEthMap::new(),
    ))
}

/// Resolve the RPC endpoint used for the startup recovery log scans.
/// Defaults to the public Gnosis endpoint (`--gnosis-logs-rpc-url`);
/// an empty value disables on-chain recovery entirely.
fn resolve_logs_rpc(opt: &Opt) -> Option<String> {
    let s = opt.gnosis_logs_rpc_url.trim();
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

fn secp256k1_keypair_from_signing_secret(secret: &[u8; SECP256K1_SECRET_LEN]) -> Result<Keypair> {
    let mut sk_copy = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut sk_copy)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(kp))
}
