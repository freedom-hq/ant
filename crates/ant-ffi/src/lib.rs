//! Hand-written `extern "C"` FFI surface for the iOS download smoke test
//! (PLAN.md § 9, "iOS download smoke test (pre-FFI)").
//!
//! Four entry points — deliberately small, deliberately C-shaped, no
//! `UniFFI`, no `.udl`, no codegen. The full mobile artefact in Phase 4
//! replaces this with a UniFFI-generated `.xcframework`; this crate is
//! expected to be deleted (or promoted) when that lands.
//!
//! * [`ant_init`] spawns a Tokio runtime, drives the `ant-node` loop in
//!   the background, and returns an opaque handle.
//! * [`ant_download`] takes a reference string (`<64-hex>`,
//!   `bytes://<hex>`, or `bzz://<hex>[/path]`), drives the retrieval
//!   through the existing `ControlCommand` channel, and hands back an
//!   allocated byte buffer.
//! * [`ant_free_buffer`] / [`ant_free_string`] free buffers owned by
//!   this crate.
//! * [`ant_shutdown`] drops the handle and cleans up the runtime.
//!
//! **Thread-safety:** the handle itself is `Send + Sync`, so the host
//! app can call `ant_download` from any thread. Concurrent
//! `ant_download` calls are allowed and share the node's cache /
//! retrieval pipeline; the mpsc command channel serialises dispatch.

pub mod bench;
mod chain_transport;
mod drive;
mod gateway;
#[cfg(feature = "jni")]
mod jni;
mod manifest;
mod stream;

// The gateway FFI lives in a private submodule; re-export its C-ABI
// entry points at the crate root so workspace Rust callers (and tests)
// can reference them by path, the same way `ant_init` is reachable.
// The `#[no_mangle]` symbols are unaffected — this only adds Rust paths.
pub use chain_transport::{
    ant_set_chain_transport, AntChainTransportFn, ANT_CHAIN_TRANSPORT_NULL_HANDLE,
    ANT_CHAIN_TRANSPORT_OK, ANT_CHAIN_TRANSPORT_UNSUPPORTED,
};
pub use gateway::{ant_start_gateway, ant_stop_gateway};

use ant_control::{
    ControlAck, ControlCommand, GetProgress, IdentityInfo, PeerInfo, RetrievalInfo, StatusSnapshot,
};
use ant_crypto::{
    ethereum_address_from_public_key, keccak256, overlay_from_ethereum_address,
    random_overlay_nonce, random_secp256k1_secret, OVERLAY_NONCE_LEN, SECP256K1_SECRET_LEN,
};
use ant_node::{run_node, NodeConfig, UploadManager};
use ant_p2p::UploadRuntime;
use ant_retrieval::DiskChunkCache;
use k256::ecdsa::SigningKey;
use libp2p::identity::{self, Keypair};
use serde::{Deserialize, Serialize};
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use stream::AntStream;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch, Notify};

/// Overall deadline for a single `ant_download` call, covering both
/// cold-start peer warmup and the actual chunk retrieval. 10 minutes
/// is comfortably above mainnet-from-scratch observations (~5-20 s
/// warmup, then 50–200 KiB/s until the hot chunk set settles) and
/// lets the iOS smoke test pull sub-gigabyte bzz payloads end-to-end
/// without hand-tuning per file. Phase-4's `UniFFI` surface will expose
/// this so hosts can pick their own.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_mins(10);

/// How often to retry a `GetBytes` / `GetBzz` that bounces off an
/// empty BZZ peer set. iOS cold starts frequently hit this: the node
/// loop is up, TCP+Noise have dialed dozens of peers, but no one has
/// finished the BZZ handshake yet. Letting the outer timeout cover
/// the whole warmup window is friendlier than forcing the Swift
/// side to poll.
const NO_PEERS_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// How long [`ant_shutdown`] waits for the runtime's tasks to stop
/// before giving up on them. Long enough for an in-flight checkpoint
/// write to finish (that is the point — see [`ant_shutdown`]), short
/// enough that a wedged dial can't hold an app teardown or a restore
/// hostage. Matches the node's own suspend checkpoint bound (~5 s).
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// On-disk `SQLite` chunk cache cap for the embedded node. The whole
/// point of running a Swarm node on-device is to amortise fetches
/// across app launches; without a persistent cache every cold start
/// re-walks the manifest tree from the network and re-pays the
/// per-chunk latency, which is what makes iOS feel "always slow" to
/// users who actually re-open the same content. 512 MiB is the size we
/// can comfortably commit to on a phone (sandboxed app caches survive
/// app restarts but get reaped by the OS under memory pressure;
/// half a gigabyte is well under that threshold on every device
/// shipped in the last five years), and covers thousands of small
/// media files or a couple of long-form videos.
const DISK_CACHE_MAX_BYTES: u64 = 512 * 1024 * 1024;

/// Gnosis chain id. Baked into the EIP-712 cheque domain separator for
/// both the inbound SWAP listener and outbound settlement; bee's
/// chequebook code is hard-coded to expect `chainId = 100` on mainnet.
const GNOSIS_CHAIN_ID: u64 = 100;

/// Per-request joiner size ceiling. `ant_retrieval::DEFAULT_MAX_FILE_BYTES`
/// is 32 MiB, which is the right cap for interactive `antctl get` but
/// trips on realistic bzz payloads (photos, short videos, archives).
/// Match `ant-gateway`'s 1 GiB ceiling here — it keeps the
/// "malformed span claims 10 TiB" safety net intact while letting
/// browser-shaped files through. The Phase-4 artefact will expose
/// this through the `UniFFI` surface so hosts can tune it per app.
const MAX_DOWNLOAD_BYTES: u64 = 1024 * 1024 * 1024;

/// Agent string this build advertises — through the node status channel
/// (`ant_agent_string`) and the in-process gateway's `/health.version`.
/// Shared by `init_inner` and [`gateway`] so the two can't drift.
const ANT_FFI_AGENT: &str = concat!("ant-ffi/", env!("CARGO_PKG_VERSION"));

/// Opaque handle returned to the Swift side. Lives for as long as the
/// embedded node loop is running.
pub struct AntHandle {
    /// Held to keep the runtime alive for the lifetime of the handle.
    /// Dropping the runtime aborts every spawned task, so `ant_shutdown`
    /// does all the cleanup we need.
    runtime: Runtime,
    cmd_tx: mpsc::Sender<ControlCommand>,
    /// Watch channel the node loop writes into on every status tick.
    /// The FFI reads `status.peers.connected` through `ant_peer_count`
    /// so the host app can render a live peer counter without polling
    /// `antctl`-style control commands.
    status_rx: watch::Receiver<StatusSnapshot>,
    /// Most recent `ControlAck::Progress` sample captured inside
    /// `attempt_download`. Swift polls this through
    /// `ant_download_progress` to render the progress bar + throughput
    /// without needing a callback bridge across the FFI boundary.
    progress: Mutex<DownloadProgressState>,
    /// Cooperative cancel flag for an in-flight `ant_download`. Flipped
    /// to `true` by `ant_cancel_download`; checked at every ack recv
    /// and the no-peers backoff in `download_inner`. Reset to `false`
    /// at the start of each new download so a cancel request can only
    /// ever affect the download it was aimed at.
    cancel_flag: AtomicBool,
    /// Notifier woken alongside `cancel_flag` so a download that's
    /// blocked inside `ack_rx.recv()` or the retry sleep unblocks
    /// immediately instead of waiting up to the next progress tick
    /// (~150 ms) / backoff interval (2 s).
    cancel_notify: Notify,
    /// Cooperative cancel flag for an in-flight propagation verification.
    /// `Arc` because it's shared into the node's verify task via the
    /// `VerifyPropagation` command; flipped to `true` by
    /// `ant_verify_cancel` and reset at the start of each new check so a
    /// cancel only ever aborts the check it was aimed at.
    verify_cancel: Arc<AtomicBool>,
    /// The node's secp256k1 signing secret. Stamps every postage batch
    /// the node owns (so `UploadRuntime::stamp_key` can sign) and backs
    /// `ant_account_export_key` for the `AntDrive` "back up your account"
    /// flow. Held here so the FFI doesn't re-read `identity.json`.
    signing_secret: [u8; SECP256K1_SECRET_LEN],
    /// The node's Ethereum address (derived from `signing_secret`). The
    /// postage batch owner and the "account address" `AntDrive` shows.
    eth: [u8; 20],
    /// The node's data directory (the path passed to `ant_init`). Held
    /// so the on-chain storage flow can persist the auto-deployed
    /// chequebook association (`chequebook.json`) and locate the
    /// outbound cheque ledger (`pushsync_outbound.json`) next to the
    /// rest of the node state. Only the on-chain storage flow reads it,
    /// so it's dead weight in the download-only (`chain`-off) slice.
    #[cfg_attr(not(feature = "chain"), allow(dead_code))]
    data_dir: PathBuf,
    /// Background task running the in-process bee-shaped HTTP gateway
    /// (`ant-gateway`) when `ant_start_gateway` has been called. `None`
    /// until started; aborted + cleared by `ant_stop_gateway` (and
    /// implicitly by `ant_shutdown` when the runtime is dropped). Lets
    /// the iOS app serve `http://127.0.0.1:<port>` in-process instead of
    /// spawning the `antd` daemon. See [`gateway`].
    gateway_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// The `AntStream` publisher throughput benchmark currently running
    /// on this handle, if any (issue #67 stage 1). `None` until
    /// [`ant_bench_start`]; cleared by [`ant_bench_stop`]. Exactly one
    /// run at a time — two concurrent runs would each measure the
    /// other's upload contention rather than the network's.
    bench: Mutex<Option<Arc<bench::BenchRun>>>,
    /// Host-provided JSON-RPC transport for chain reads/writes (issue
    /// #77), installed by [`ant_set_chain_transport`]. `None` — the
    /// default — means every chain request goes to the configured
    /// `gnosis_rpc` URL, exactly as before.
    #[cfg(feature = "chain")]
    chain_transport: Mutex<Option<Arc<chain_transport::HostChainTransport>>>,
}

/// Chain wiring shared by the storage / settlement calls and the
/// in-process gateway.
#[cfg(feature = "chain")]
impl AntHandle {
    /// The host transport currently installed on this handle, if any.
    pub(crate) fn host_chain_transport(&self) -> Option<ant_chain::SharedChainTransport> {
        // Poison-tolerant: a panic elsewhere must not wedge chain reads.
        self.chain_transport
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .map(|t| t as ant_chain::SharedChainTransport)
    }

    /// A [`ant_chain::ChainClient`] for `rpc`, routed through the host
    /// transport when one is installed. **Every** chain client this
    /// crate builds must come from here, so a host that plugs in a
    /// verified source is not bypassed by one forgotten call site.
    pub(crate) fn chain_client(&self, rpc: impl Into<String>) -> ant_chain::ChainClient {
        ant_chain::ChainClient::new(rpc).with_transport(self.host_chain_transport())
    }
}

/// Live snapshot of the in-flight download, maintained by the
/// `attempt_download` loop. Lives on the handle (not on a request
/// object) because the FFI intentionally keeps a single-command shape:
/// one `AntHandle`, one `ant_download` at a time is the common path
/// in the smoke test, and a shared progress slot is the minimum
/// plumbing that gives Swift a live view.
#[derive(Clone, Copy, Default)]
struct DownloadProgressState {
    in_progress: bool,
    bytes_done: u64,
    total_bytes: u64,
    chunks_done: u64,
    total_chunks: u64,
    elapsed_ms: u64,
    peers_used: u32,
    in_flight: u32,
    cache_hits: u64,
}

impl DownloadProgressState {
    fn start() -> Self {
        Self {
            in_progress: true,
            ..Default::default()
        }
    }

    const fn apply(&mut self, p: &GetProgress) {
        self.in_progress = true;
        self.bytes_done = p.bytes_done;
        self.total_bytes = p.total_bytes_estimate;
        self.chunks_done = p.chunks_done;
        self.total_chunks = p.total_chunks_estimate;
        self.elapsed_ms = p.elapsed_ms;
        self.peers_used = p.peers_used;
        self.in_flight = p.in_flight;
        self.cache_hits = p.cache_hits;
    }

    const fn finish(&mut self) {
        self.in_progress = false;
        self.in_flight = 0;
    }
}

// ---------------------------------------------------------------------------
// Identity file (a trimmed-down mirror of `antd::IdentityFile`; ant-ffi
// intentionally duplicates this rather than reaching into `antd`'s
// binary crate).
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct IdentityFile {
    signing_key: String,
    overlay_nonce: String,
    libp2p_keypair: Option<String>,
}

/// Domain separator for the overlay nonce we derive when an identity is
/// rebuilt from a bare account key ([`ant_identity_from_key`]). Deriving
/// it from the *public* Ethereum address (never the secret) keeps a
/// key-only restore reproducible — the same key always yields the same
/// overlay — without publishing any function of the private key.
const OVERLAY_NONCE_DOMAIN: &[u8] = b"ant-ffi/overlay-nonce/v1";

/// Where the node's identity (account key) comes from.
enum IdentitySource<'a> {
    /// Legacy/desktop behaviour: `identity.json` inside the data dir,
    /// created on first run. The library owns the key material on disk.
    DataDir,
    /// Host-provided identity JSON (same shape as `identity.json`). The
    /// library never reads or writes the key on disk — this is the
    /// `KeyProvider` backend PLAN.md § 5.10 plans for mobile, where the
    /// host keeps the key in the iOS Keychain / Android Keystore.
    Provided(&'a str),
}

#[derive(Debug, thiserror::Error)]
enum FfiError {
    #[error("null pointer")]
    NullPointer,
    #[error("invalid UTF-8 in argument")]
    InvalidUtf8,
    #[error("{0}")]
    Io(String),
    #[error("{0}")]
    Crypto(String),
    #[error("{0}")]
    Runtime(String),
    #[error("{0}")]
    Reference(String),
    #[error("{0}")]
    Download(String),
}

// ---------------------------------------------------------------------------
// ant_init
// ---------------------------------------------------------------------------

/// Spin up an embedded node. `data_dir` is a UTF-8 C string pointing at
/// a writable directory (typically the iOS `Application Support/`
/// sandbox). The directory is created if it doesn't exist.
///
/// Returns a non-null handle on success. On failure, returns null and
/// writes an allocated NUL-terminated error string into `*out_err`;
/// the caller owns that string and must free it with [`ant_free_string`].
///
/// # Safety
///
/// * `data_dir` must be a valid NUL-terminated UTF-8 string.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null
///   to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_init(
    data_dir: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut AntHandle {
    unsafe { ant_init_with_options(data_dir, std::ptr::null(), out_err) }
}

/// Like [`ant_init`], with embedder options. `source_root` (nullable)
/// names the directory the host stages upload sources under (the iOS
/// app's `Application Support/antdrive/imports`); the upload manager
/// then records each job's source *relative* to it and re-anchors the
/// absolute path when the OS relocates the app container (iOS assigns a
/// new container UUID on updates/reinstalls), so persisted uploads keep
/// resuming and self-healing across the move. The root is applied
/// *before* the persisted jobs are rehydrated, which is why this is an
/// init option rather than a post-init call. Pass NULL to disable the
/// rebase (identical to `ant_init`).
///
/// # Safety
///
/// * `data_dir` must be a valid NUL-terminated UTF-8 string.
/// * `source_root` must be a valid NUL-terminated UTF-8 string, or null.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null
///   to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_init_with_options(
    data_dir: *const c_char,
    source_root: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut AntHandle {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<AntHandle, FfiError> {
            let path = cstr_to_path(data_dir)?;
            let source_root = if source_root.is_null() {
                None
            } else {
                Some(cstr_to_path(source_root)?)
            };
            init_inner(&path, source_root.as_deref(), IdentitySource::DataDir)
        }));
        match result {
            Ok(Ok(handle)) => Box::into_raw(Box::new(handle)),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_init");
                std::ptr::null_mut()
            }
        }
    }
}

/// Like [`ant_init_with_options`], but the *host* owns the account key:
/// `identity_json` carries the identity document (the same shape
/// [`ant_identity_generate`] returns) and the library neither reads nor
/// writes `identity.json` in the data dir. This is the `KeyProvider`
/// backend PLAN.md § 5.10 plans for mobile — on iOS the document lives in
/// the Keychain (optionally Secure-Enclave-wrapped), so an attacker with
/// the app container never gets the key.
///
/// Everything else behaves exactly like [`ant_init_with_options`].
///
/// # Safety
///
/// * `data_dir` and `identity_json` must be valid NUL-terminated UTF-8
///   strings.
/// * `source_root` must be a valid NUL-terminated UTF-8 string, or null.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null
///   to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_init_with_identity(
    data_dir: *const c_char,
    source_root: *const c_char,
    identity_json: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut AntHandle {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<AntHandle, FfiError> {
            let path = cstr_to_path(data_dir)?;
            let source_root = if source_root.is_null() {
                None
            } else {
                Some(cstr_to_path(source_root)?)
            };
            let identity = cstr_to_str(identity_json)?;
            init_inner(
                &path,
                source_root.as_deref(),
                IdentitySource::Provided(identity),
            )
        }));
        match result {
            Ok(Ok(handle)) => Box::into_raw(Box::new(handle)),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_init_with_identity");
                std::ptr::null_mut()
            }
        }
    }
}

/// Mint a fresh node identity without starting a node, so a host that
/// keeps the key itself (iOS Keychain / Android Keystore) can create one
/// on first run and feed it back to [`ant_init_with_identity`].
///
/// Returns an allocated JSON document
/// `{"signing_key","overlay_nonce","libp2p_keypair"}` — all hex, and all
/// secret: `signing_key` *is* the account. Free with
/// [`ant_free_string`]. On failure returns null and writes an allocated
/// message into `*out_err`.
///
/// # Safety
///
/// * `out_err` must point at a writable `*mut c_char` slot, or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_identity_generate(out_err: *mut *mut c_char) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_identity_generate", || {
            let id = new_identity().map_err(|e| e.to_string())?;
            serde_json::to_string(&id).map_err(|e| format!("serialize identity: {e}"))
        })
    }
}

/// Rebuild a node identity from a backed-up account key (64 hex chars,
/// `0x` prefix tolerated) — the "restore my account" path when the
/// Keychain copy is gone but the user still has their key. Returns the
/// same JSON document as [`ant_identity_generate`], with the overlay
/// nonce derived from the account address so the restore is
/// reproducible. Rejects malformed or out-of-range keys.
///
/// # Safety
///
/// * `signing_key_hex` must be a valid NUL-terminated UTF-8 string.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_identity_from_key(
    signing_key_hex: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_identity_from_key", || {
            let hex_str = cstr_to_str(signing_key_hex).map_err(|e| e.to_string())?;
            let id = identity_from_signing_key(hex_str).map_err(|e| e.to_string())?;
            serde_json::to_string(&id).map_err(|e| format!("serialize identity: {e}"))
        })
    }
}

// ---------------------------------------------------------------------------
// Account-scoped on-disk state
// ---------------------------------------------------------------------------

/// Data-dir entries that belong to one specific *account* (the node EOA)
/// rather than to the device.
///
/// None of these transfer between accounts. A postage store issues
/// stamps over a batch whose owner is recorded on-chain; a chequebook is
/// bound on-chain to its issuer; both SWAP ledgers are denominated in
/// cheques signed by (or payable to) one account; an upload job stamps
/// against a specific batch. Signing any of them with a different key
/// produces state that looks healthy locally and is rejected by every
/// peer, so they travel with the account instead of staying put (see
/// [`bind_account_state`]).
///
/// Everything else in the data dir is account-independent and stays:
/// `peers.json` (a network peer list), `chunks.sqlite` (a content cache
/// keyed by chunk address), and `identity.json` — which *is* the
/// account, and only exists in [`IdentitySource::DataDir`] mode, where
/// the account can't change behind our back in the first place.
const ACCOUNT_SCOPED_ENTRIES: &[&str] = &[
    "postage",
    "uploads",
    "chequebook.json",
    "swap_credits.json",
    "pushsync_outbound.json",
];

/// Records which account the [`ACCOUNT_SCOPED_ENTRIES`] currently at
/// their canonical paths belong to. Holds the *public* Ethereum address
/// only — no function of the key — so the host-held-identity guarantee
/// ("the library never writes key material to disk") is unaffected.
const ACCOUNT_MARKER_FILE: &str = "account.json";

/// Parent of the per-account parking dirs: `<data_dir>/accounts/<0xeth>/`.
const ACCOUNT_PARK_DIR: &str = "accounts";

/// Parking name for state whose marker exists but is unreadable. We know
/// it isn't ours (a marker we wrote is well-formed), but not whose it is.
const UNKNOWN_ACCOUNT: &str = "unknown";

/// The `account.json` marker.
#[derive(Serialize, Deserialize)]
struct AccountMarker {
    /// `0x` + 40 hex: the node EOA that owns the account-scoped state.
    account: String,
}

/// Make the data dir's account-scoped state belong to `eth` before
/// anything reads it.
///
/// Without this, a key swap silently mixes two accounts: the postage
/// reload (`drive::reload_persisted_issuers`) and the chequebook
/// association are keyed by path, not by owner, so the new key would
/// sign stamps over the *old* account's batch and cheques against the
/// *old* account's chequebook. Nothing local notices — the plan reads as
/// active and settlement as ready — while every peer drops both, which
/// is precisely the failure mode that has to be caught before startup
/// rather than at first use.
///
/// The previous account's state is *parked* under
/// `<data_dir>/accounts/<its address>/` rather than deleted, and this
/// account's parked state (from an earlier switch) is swapped back in,
/// so switching keys back and forth loses nothing.
///
/// Ordering is crash-safe: parking runs before the marker is rewritten
/// (a crash in between just re-runs a now-empty park), and the adopt
/// step runs on every start (a crash mid-adopt is finished by the next
/// one).
fn bind_account_state(data_dir: &Path, eth: &[u8; 20]) -> Result<(), FfiError> {
    let current = format!("0x{}", hex::encode(eth));
    let marker = data_dir.join(ACCOUNT_MARKER_FILE);
    match read_account_marker(&marker)? {
        // Same account as last launch: the canonical paths are its own.
        Some(previous) if previous.eq_ignore_ascii_case(&current) => {}
        // Someone else's (or unattributable) state sitting where this
        // account's belongs — park it before anything opens it.
        Some(previous) => {
            tracing::warn!(
                target: "ant-ffi",
                previous = %previous,
                current = %current,
                "data dir belongs to a different account; parking its postage / chequebook / settlement state",
            );
            move_account_entries(data_dir, &account_park_dir(data_dir, &previous))?;
            write_account_marker(&marker, &current)?;
        }
        // No marker: either a fresh data dir, or the first start under a
        // build that keeps one. Whatever is here was written by the
        // account starting now — before host-held identities the key
        // came from this very directory and could not change.
        None => write_account_marker(&marker, &current)?,
    }

    // Swap this account's own parked state (if any) back in. Runs on
    // every start so an interrupted adopt is completed on the next one.
    let parked = account_park_dir(data_dir, &current);
    if parked.is_dir() {
        move_account_entries(&parked, data_dir)?;
        // Empty now; a leftover (something else was put in there) is
        // left alone rather than removed.
        let _ = std::fs::remove_dir(&parked);
    }
    Ok(())
}

/// `<data_dir>/accounts/<owner>` — `owner` is always either a validated
/// `0x` + 40-hex address or [`UNKNOWN_ACCOUNT`], so it can never escape
/// the data dir.
fn account_park_dir(data_dir: &Path, owner: &str) -> PathBuf {
    data_dir.join(ACCOUNT_PARK_DIR).join(owner)
}

/// Move every [`ACCOUNT_SCOPED_ENTRIES`] entry present in `from` into
/// `to`. Refuses (rather than clobbering) when the destination already
/// holds an entry of the same name: two accounts' copies of one name
/// means we can no longer tell which is whose, and guessing is how the
/// wrong batch gets stamped.
fn move_account_entries(from: &Path, to: &Path) -> Result<(), FfiError> {
    for name in ACCOUNT_SCOPED_ENTRIES {
        let src = from.join(name);
        if !src.exists() {
            continue;
        }
        std::fs::create_dir_all(to)
            .map_err(|e| FfiError::Io(format!("create {}: {e}", to.display())))?;
        let dst = to.join(name);
        if dst.exists() {
            return Err(FfiError::Io(format!(
                "refusing to start: {} and {} both exist; move one aside by hand",
                src.display(),
                dst.display(),
            )));
        }
        std::fs::rename(&src, &dst).map_err(|e| {
            FfiError::Io(format!("move {} to {}: {e}", src.display(), dst.display()))
        })?;
    }
    Ok(())
}

/// The account the data dir's state belongs to: `Ok(None)` when there is
/// no marker at all, `Ok(Some(`[`UNKNOWN_ACCOUNT`]`))` when one exists but
/// its *contents* aren't a valid marker (fail closed — state we can't
/// attribute is treated as another account's).
///
/// A marker we can't *read* (I/O error, not a missing file) is an error,
/// not an unknown account: reporting it as unattributable would park this
/// account's own postage / chequebook state under `accounts/unknown`,
/// where the adopt step — which only ever looks at
/// `accounts/<own address>` — never brings it back. A transient read
/// failure has to fail the start it happened on, so the next one (which
/// can read the marker) comes up with the account intact.
fn read_account_marker(path: &Path) -> Result<Option<String>, FfiError> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(FfiError::Io(format!(
                "refusing to start: cannot read the account marker {}: {e}",
                path.display(),
            )))
        }
    };
    let account = serde_json::from_slice::<AccountMarker>(&raw)
        .ok()
        .map(|m| m.account)
        .filter(|a| is_eth_address(a));
    if account.is_none() {
        tracing::warn!(
            target: "ant-ffi",
            path = %path.display(),
            "corrupt account marker; treating the data dir's state as another account's",
        );
    }
    Ok(Some(account.unwrap_or_else(|| UNKNOWN_ACCOUNT.to_string())))
}

fn write_account_marker(path: &Path, account: &str) -> Result<(), FfiError> {
    let json = serde_json::to_string(&AccountMarker {
        account: account.to_string(),
    })
    .map_err(|e| FfiError::Io(format!("serialize account marker: {e}")))?;
    // Write-tmp + rename: a torn marker would make the next start park
    // this account's own state as a stranger's.
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json)
        .map_err(|e| FfiError::Io(format!("write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path).map_err(|e| FfiError::Io(format!("write {}: {e}", path.display())))
}

fn is_eth_address(s: &str) -> bool {
    s.len() == 42 && s.starts_with("0x") && s[2..].bytes().all(|b| b.is_ascii_hexdigit())
}

fn init_inner(
    data_dir: &Path,
    source_root: Option<&Path>,
    identity: IdentitySource<'_>,
) -> Result<AntHandle, FfiError> {
    install_log_subscriber();

    std::fs::create_dir_all(data_dir)
        .map_err(|e| FfiError::Io(format!("create data dir {}: {e}", data_dir.display())))?;

    let (signing_secret, overlay_nonce, libp2p_keypair) = match identity {
        IdentitySource::DataDir => load_or_create_identity(&data_dir.join("identity.json"))?,
        IdentitySource::Provided(json) => decode_identity_json(json)?,
    };

    let vk = *SigningKey::from_bytes((&signing_secret).into())
        .map_err(|e| FfiError::Crypto(format!("invalid signing key: {e}")))?
        .verifying_key();
    let eth = ethereum_address_from_public_key(&vk);
    let overlay = overlay_from_ethereum_address(&eth, 1, &overlay_nonce);
    let peer_id = libp2p_keypair.public().to_peer_id();

    // The host can hand us a *different* account than it did last launch
    // (`ant_identity_from_key` + a Restore flow), so make the data dir's
    // account-scoped state belong to this account before anything below
    // opens it.
    bind_account_state(data_dir, &eth)?;

    let started_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let initial_snapshot = StatusSnapshot {
        agent: ANT_FFI_AGENT.to_string(),
        protocol_version: ant_control::PROTOCOL_VERSION,
        network_id: 1,
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
        control_socket: String::new(),
        retrieval: RetrievalInfo::default(),
        // The FFI path resolves chain state before starting the loop
        // (no late-chain channel), so it is ready by construction.
        chain_ready: true,
    };
    let (status_tx, status_rx) = watch::channel(initial_snapshot);

    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(32);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ant-ffi")
        .build()
        .map_err(|e| FfiError::Runtime(format!("build tokio runtime: {e}")))?;

    let bootnodes = ant_p2p::default_mainnet_bootnodes();
    let peerstore_path = Some(data_dir.join("peers.json"));
    let process_start = Instant::now();

    // Persistent chunk cache. On-device retrieval is dominated by
    // per-chunk close-peer latency, so amortising fetches across app
    // launches is the single biggest win we can hand the host: a
    // re-opened photo album / video is served from local SQLite
    // instead of re-walking the manifest tree against a small (NAT'd,
    // ~100 peer) BZZ peer set. A failed open is non-fatal — the node
    // still serves uploads and live retrievals, it just doesn't
    // amortise them.
    let disk_cache_path = data_dir.join("chunks.sqlite");
    let disk_cache = match DiskChunkCache::open(&disk_cache_path, DISK_CACHE_MAX_BYTES) {
        Ok(c) => {
            tracing::info!(
                target: "ant-ffi",
                path = %disk_cache_path.display(),
                max_bytes = DISK_CACHE_MAX_BYTES,
                used_bytes = c.used_bytes(),
                "opened persistent chunk cache",
            );
            Some(Arc::new(c))
        }
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                path = %disk_cache_path.display(),
                "failed to open persistent chunk cache: {e}; falling back to in-memory only",
            );
            None
        }
    };

    // Upload + postage wiring (`AntDrive`). The node wallet owns every
    // batch it stamps with, so a single `stamp_key` (the node signing
    // secret) signs for all of them. We reload any batch persisted from
    // a previous run so the user's storage plan survives app restarts
    // without re-reading the chain; new batches are registered at
    // runtime by `ant_storage_connect_batch` (RegisterBatch). The
    // `UploadManager` drives `antctl upload`-style jobs through the same
    // PushChunk → postage → pushsync pipeline `antd` uses.
    let postage_dir = data_dir.join("postage");
    let issuers = drive::reload_persisted_issuers(&postage_dir);
    let upload_runtime = Arc::new(UploadRuntime {
        issuers: Mutex::new(issuers),
        stamp_key: signing_secret,
        batch_owner: eth,
        postage_dir: postage_dir.clone(),
    });
    let upload_manager = UploadManager::new(data_dir.join("uploads"), cmd_tx.clone(), None)
        .map_err(|e| FfiError::Io(format!("open upload state dir: {e}")))?
        // Read the live status watch so the automatic post-upload heal can
        // also promote shallow placements once the node is well-connected
        // enough for its closest-peer probe to be trustworthy (Sketch B).
        .with_status_watch(Some(status_rx.clone()))
        // Container-move rebase root (see `ant_init_with_options`). Set
        // before `rehydrate_from_disk` so stranded sources re-anchor as
        // the persisted jobs load.
        .with_source_root(source_root.map(Path::to_path_buf))
        // Attach the persistent chunk store so heal can re-push — and,
        // with the source file gone, enumerate a completed upload's
        // chunk set by store-only traversal from its reference.
        .with_disk_cache(disk_cache.clone());
    {
        // `rehydrate_from_disk` may `tokio::spawn` (auto-resuming an
        // interrupted upload, or kicking off a startup self-heal for a
        // completed-but-unverified job), so it must run inside the
        // runtime context — unlike `antd`'s async main, this FFI init
        // path isn't already on a runtime thread.
        let _enter = runtime.enter();
        if let Err(e) = upload_manager.rehydrate_from_disk(true) {
            tracing::warn!(target: "ant-ffi", "rehydrate upload jobs: {e}");
        }
    }

    // SWAP settlement (parity with `antd`'s startup wiring).
    //
    // Inbound listener: always on. Accepting cheques doesn't require us
    // to own a chequebook (we're the beneficiary, not the issuer), so
    // the node can be paid for serving content from the very first
    // launch. The credit ledger persists across restarts.
    let swap_cfg = ant_p2p::SwapConfig {
        our_beneficiary: eth,
        chain_id: GNOSIS_CHAIN_ID,
        ledger_path: data_dir.join("swap_credits.json"),
        events_tx: None,
    };
    // Outbound settlement: wire it now only if this device already
    // deployed a chequebook on an earlier run (persisted at
    // `<data_dir>/chequebook.json`, issuer = node EOA, so the node
    // signing key signs cheques). On a fresh install there's no
    // chequebook yet — the node wallet is unfunded until the user buys
    // storage — so it stays disabled here and gets installed at runtime
    // by the storage-buy flow once a chequebook exists (see
    // `drive::ensure_settlement`). Without an RPC at init we can't run
    // the factory-registration check; the chequebook was factory-built
    // when we deployed it, so building unconditionally matches antd's
    // no-RPC manual path. Gated on `chain`: a download-only build never
    // uploads, so it never needs (or can deploy) a chequebook.
    #[cfg(feature = "chain")]
    let pushsync_cfg = match ant_chain::chequebook_store::load_persisted_chequebook_for(
        &data_dir.join("chequebook.json"),
        &eth,
    ) {
        Ok(Some(chequebook)) => {
            tracing::info!(
                target: "ant-ffi",
                chequebook = %format!("0x{}", hex::encode(chequebook)),
                "reusing persisted chequebook — outbound SWAP settlement enabled at startup",
            );
            Some(ant_p2p::PushsyncSwapConfig::new(
                chequebook,
                signing_secret,
                GNOSIS_CHAIN_ID,
                data_dir.join("pushsync_outbound.json"),
                ant_p2p::PeerEthMap::new(),
            ))
        }
        Ok(None) => None,
        Err(e) => {
            // Self-healing: a malformed association must not stop the
            // node coming up for reads; a fresh deploy on the next buy
            // overwrites it.
            tracing::warn!(target: "ant-ffi", "ignoring chequebook association: {e}");
            None
        }
    };
    #[cfg(not(feature = "chain"))]
    let pushsync_cfg: Option<ant_p2p::PushsyncSwapConfig> = None;

    let cfg = NodeConfig::mainnet_default(signing_secret, overlay_nonce, bootnodes, libp2p_keypair)
        .with_status(status_tx)
        .with_process_start(process_start)
        .with_peerstore_path(peerstore_path)
        .with_commands(cmd_rx)
        .with_disk_cache(disk_cache)
        .with_upload(Some(upload_runtime))
        .with_swap(Some(swap_cfg))
        .with_pushsync_swap(pushsync_cfg)
        .with_upload_manager(Some(upload_manager));

    runtime.spawn(async move {
        if let Err(e) = run_node(cfg).await {
            tracing::error!(target: "ant-ffi", "node loop exited with error: {e}");
        } else {
            tracing::info!(target: "ant-ffi", "node loop exited cleanly");
        }
    });

    tracing::info!(
        target: "ant-ffi",
        eth = %format!("0x{}", hex::encode(eth)),
        overlay = %format!("0x{}", hex::encode(overlay)),
        "ant-ffi node started",
    );

    Ok(AntHandle {
        runtime,
        cmd_tx,
        status_rx,
        progress: Mutex::new(DownloadProgressState::default()),
        cancel_flag: AtomicBool::new(false),
        cancel_notify: Notify::new(),
        verify_cancel: Arc::new(AtomicBool::new(false)),
        signing_secret,
        eth,
        data_dir: data_dir.to_path_buf(),
        gateway_task: Mutex::new(None),
        bench: Mutex::new(None),
        #[cfg(feature = "chain")]
        chain_transport: Mutex::new(None),
    })
}

// ---------------------------------------------------------------------------
// ant_download
// ---------------------------------------------------------------------------

/// Download a reference. `reference` may be:
///
/// * `<64-hex>` — treated as a `/bytes/<hex>` reference (single chunk
///   or multi-chunk byte tree).
/// * `bytes://<64-hex>` — same as bare hex, explicit form.
/// * `bzz://<64-hex>[/path]` — walk the mantaray manifest at `<hex>`,
///   resolve `/path` (or the manifest's `website-index-document`), and
///   join the resulting chunk tree.
///
/// On success, returns a heap-allocated buffer and writes its length
/// into `*out_len`. Free with [`ant_free_buffer`].
///
/// On failure, returns null and writes an allocated NUL-terminated
/// error string into `*out_err`. Free with [`ant_free_string`].
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been passed
///   to [`ant_shutdown`].
/// * `reference` must be a valid NUL-terminated UTF-8 string.
/// * `out_len` and `out_err` must point at writable slots (either may
///   be null to opt out).
#[no_mangle]
pub unsafe extern "C" fn ant_download(
    handle: *mut AntHandle,
    reference: *const c_char,
    out_len: *mut usize,
    out_err: *mut *mut c_char,
) -> *mut u8 {
    unsafe {
        clear_out_err(out_err);
        if !out_len.is_null() {
            *out_len = 0;
        }
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<Vec<u8>, FfiError> {
            let handle = handle.as_ref().ok_or(FfiError::NullPointer)?;
            let reference = cstr_to_str(reference)?;
            download_inner(handle, reference)
        }));
        match result {
            Ok(Ok(bytes)) => {
                let len = bytes.len();
                let mut boxed = bytes.into_boxed_slice();
                let ptr = boxed.as_mut_ptr();
                // Ownership crosses the FFI boundary; freed by `ant_free`.
                #[allow(clippy::mem_forget)]
                std::mem::forget(boxed);
                if !out_len.is_null() {
                    *out_len = len;
                }
                ptr
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_download");
                std::ptr::null_mut()
            }
        }
    }
}

enum ParsedRef {
    Bytes([u8; 32]),
    Bzz { root: [u8; 32], path: String },
}

fn parse_reference(input: &str) -> Result<ParsedRef, FfiError> {
    let trimmed = input.trim();
    if let Some(rest) = trimmed.strip_prefix("bzz://") {
        let (hex_part, path_part) = match rest.find('/') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        Ok(ParsedRef::Bzz {
            root: parse_hex32(hex_part)?,
            path: decode_bzz_path(path_part)?,
        })
    } else if let Some(rest) = trimmed.strip_prefix("bytes://") {
        Ok(ParsedRef::Bytes(parse_hex32(rest)?))
    } else {
        Ok(ParsedRef::Bytes(parse_hex32(trimmed)?))
    }
}

/// Percent-decode a `bzz://` path component the same way `ant-gateway`'s
/// axum extractor does, so manifest entries containing literal spaces /
/// unicode survive a round trip through a URL the user pastes in. A
/// path like `tracks/02%20butterfly.wav` decodes to `tracks/02 butterfly.wav`,
/// which is what the mantaray manifest stores. Invalid UTF-8 in the
/// decoded bytes is rejected loudly rather than masked with the lossy
/// replacement character — a manifest path is an exact byte sequence.
fn decode_bzz_path(raw: &str) -> Result<String, FfiError> {
    percent_encoding::percent_decode_str(raw)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| FfiError::Reference(format!("invalid percent-encoding in bzz path: {e}")))
}

fn parse_hex32(s: &str) -> Result<[u8; 32], FfiError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 64 {
        return Err(FfiError::Reference(format!(
            "reference must be 32 bytes (64 hex chars), got {} chars",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out)
        .map_err(|e| FfiError::Reference(format!("invalid hex: {e}")))?;
    Ok(out)
}

fn download_inner(handle: &AntHandle, reference: &str) -> Result<Vec<u8>, FfiError> {
    let parsed = parse_reference(reference)?;
    let cmd_tx = handle.cmd_tx.clone();
    // Reset the progress slot AND the cancel flag at the start of
    // each download. A stale `true` here would otherwise instantly
    // abort the next download the moment it starts.
    if let Ok(mut slot) = handle.progress.lock() {
        *slot = DownloadProgressState::start();
    }
    handle.cancel_flag.store(false, Ordering::SeqCst);
    let progress = &handle.progress;
    let cancel_flag = &handle.cancel_flag;
    let cancel_notify = &handle.cancel_notify;
    let out = handle.runtime.block_on(async move {
        let deadline = tokio::time::Instant::now() + DOWNLOAD_TIMEOUT;
        loop {
            match attempt_download(
                &cmd_tx,
                &parsed,
                deadline,
                progress,
                cancel_flag,
                cancel_notify,
            )
            .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(FfiError::Download(msg)) if is_no_peers_error(&msg) => {
                    // The node hasn't completed the BZZ handshake with
                    // any peer yet. Sleep briefly and retry until the
                    // outer deadline — turns the cold-start pain into a
                    // "first download takes a while" experience rather
                    // than a hard failure that forces the Swift side to
                    // poll.
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(FfiError::Download(format!(
                            "no BZZ peers completed handshake within {} seconds",
                            DOWNLOAD_TIMEOUT.as_secs()
                        )));
                    }
                    let wait = NO_PEERS_RETRY_INTERVAL.min(deadline.saturating_duration_since(now));
                    // Wake up early on cancel so the user doesn't have
                    // to stare at a 2 s backoff after hitting Stop.
                    tokio::select! {
                        biased;
                        () = cancel_notify.notified() => {}
                        () = tokio::time::sleep(wait) => {}
                    }
                    if cancel_flag.load(Ordering::SeqCst) {
                        return Err(FfiError::Download("download canceled".to_string()));
                    }
                }
                Err(e) => return Err(e),
            }
        }
    });
    // Success or failure, clear the "in progress" flag so Swift can
    // hide the progress HUD on the next poll without racing on an
    // explicit stop signal.
    if let Ok(mut slot) = handle.progress.lock() {
        slot.finish();
    }
    out
}

fn is_no_peers_error(msg: &str) -> bool {
    msg.contains("no peers available")
}

async fn attempt_download(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    parsed: &ParsedRef,
    deadline: tokio::time::Instant,
    progress_slot: &Mutex<DownloadProgressState>,
    cancel_flag: &AtomicBool,
    cancel_notify: &Notify,
) -> Result<Vec<u8>, FfiError> {
    let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(32);
    let cmd = match parsed {
        ParsedRef::Bytes(root) => ControlCommand::GetBytes {
            reference: *root,
            bypass_cache: false,
            progress: true,
            max_bytes: Some(MAX_DOWNLOAD_BYTES),
            ack: ack_tx,
        },
        ParsedRef::Bzz { root, path } => ControlCommand::GetBzz {
            reference: *root,
            path: path.clone(),
            // Smoke-test app cares about "bytes come back", not
            // "bytes come back Reed-Solomon verified". Masking the
            // redundancy level off the root span lets us read files
            // uploaded with bee's default redundancy without shipping
            // the RS recovery machinery yet (see PLAN.md §E.8).
            allow_degraded_redundancy: true,
            bypass_cache: false,
            progress: true,
            max_bytes: Some(MAX_DOWNLOAD_BYTES),
            ack: ack_tx,
        },
    };
    cmd_tx
        .send(cmd)
        .await
        .map_err(|_| FfiError::Download("node loop is not accepting commands".to_string()))?;

    loop {
        if cancel_flag.load(Ordering::SeqCst) {
            return Err(FfiError::Download("download canceled".to_string()));
        }
        // Race the ack recv against the cancel notifier so a Stop
        // press unblocks us immediately instead of waiting for the
        // next progress tick (~150 ms) or a long idle stretch. The
        // `biased` keyword picks the cancel branch first on every
        // wake so we never miss a signal that landed a microsecond
        // before an ack.
        let next = tokio::select! {
            biased;
            () = cancel_notify.notified() => {
                return Err(FfiError::Download("download canceled".to_string()));
            }
            r = tokio::time::timeout_at(deadline, ack_rx.recv()) => r,
        };
        match next {
            Ok(Some(ControlAck::Bytes { data } | ControlAck::BzzBytes { data, .. })) => {
                return Ok(data)
            }
            Ok(Some(ControlAck::Error { message })) => return Err(FfiError::Download(message)),
            Ok(Some(ControlAck::Progress(p))) => {
                if let Ok(mut slot) = progress_slot.lock() {
                    slot.apply(&p);
                }
            }
            Ok(Some(
                ControlAck::Ok { .. }
                | ControlAck::Manifest { .. }
                | ControlAck::BytesStreamStart { .. }
                | ControlAck::BzzStreamStart { .. }
                | ControlAck::BytesChunk { .. }
                | ControlAck::StreamDone
                | ControlAck::ChunkUploaded { .. }
                | ControlAck::UploadStarted { .. }
                | ControlAck::UploadJob(_)
                | ControlAck::UploadList(_)
                | ControlAck::UploadProgress(_)
                | ControlAck::PostageStatus(_)
                | ControlAck::PostageList(_)
                | ControlAck::Accounting(_)
                | ControlAck::FeedResolved { .. }
                | ControlAck::FeedNotFound
                | ControlAck::Envelope { .. }
                | ControlAck::Retrievable { .. }
                | ControlAck::PinAdded { .. }
                | ControlAck::PinRemoved { .. }
                | ControlAck::PinPresent { .. }
                | ControlAck::PinList { .. }
                | ControlAck::PinCheck { .. }
                | ControlAck::PostageBuckets(_)
                | ControlAck::PullsyncProbe(_)
                | ControlAck::LurkerMessage { .. }
                | ControlAck::NotReady { .. },
            )) => {}
            Ok(None) => {
                return Err(FfiError::Download(
                    "node dropped the ack channel without a terminal response".to_string(),
                ))
            }
            Err(_) => {
                return Err(FfiError::Download(format!(
                    "download did not complete within {} seconds",
                    DOWNLOAD_TIMEOUT.as_secs()
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Live node status
// ---------------------------------------------------------------------------

/// Current number of BZZ peers we've completed the handshake with.
/// The node loop updates this on every status tick; the FFI reads the
/// latest snapshot from the shared watch channel without blocking.
///
/// Returns `-1` on a null / poisoned handle (never happens in practice
/// unless the Swift side calls this after `ant_shutdown`).
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_peer_count(handle: *const AntHandle) -> i32 {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return -1;
        };
        let connected = handle.status_rx.borrow().peers.connected;
        i32::try_from(connected).unwrap_or(i32::MAX)
    }
}

/// Prompt the running node to recover after an OS suspension (e.g. an iOS
/// background reap): re-warm the dial queue and re-dial bootstrap + known
/// peers, **without** a full [`ant_shutdown`] / [`ant_init`]. Cheap and
/// idempotent — safe to call on every foreground transition.
///
/// After a long suspension the in-process node's libp2p connections are
/// half-open (the kernel sockets were reaped but no FIN arrived, so the
/// peer counter still looks healthy) and nothing re-dials, so the next
/// `bzz://` retrieval hangs and the page renders blank. This re-opens live
/// sockets to the bootnodes in parallel so retrieval has working routes
/// again; the dead connections fall away as they're touched. It does not
/// forcibly disconnect surviving peers — a healthy short-background resume
/// stays close to a no-op rather than paying a full re-handshake.
///
/// This recovers the *swarm* only. If the in-process gateway's localhost
/// listener was also torn down, rebind it separately with
/// [`ant_stop_gateway`] + [`ant_start_gateway`] (a plain re-call to
/// `ant_start_gateway` is a no-op while its serve task is still alive).
///
/// Returns `0` on success. Returns `-1` on a null handle and `-2` if the
/// node loop didn't ack (already shut down); in the `-2` case an allocated
/// error string is written into `*out_err` (free it with
/// [`ant_free_string`]).
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been passed to
///   [`ant_shutdown`].
/// * `out_err` must point at a writable `*mut c_char` slot, or be null to
///   opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_resume(handle: *const AntHandle, out_err: *mut *mut c_char) -> i32 {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_resume: null handle");
            return -1;
        };
        match catch_unwind(AssertUnwindSafe(|| drive::resume(handle))) {
            Ok(Ok(msg)) => {
                tracing::info!(target: "ant-ffi", "{msg}");
                0
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -2
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_resume");
                -2
            }
        }
    }
}

/// System-suspend the upload subsystem — call when the app is moving to
/// the background or the device just went offline. Every in-flight
/// upload is paused with the "resumes automatically" marker (a job the
/// user paused is left alone) and running securing passes stop at their
/// next opportunity. **Blocks** until the paused upload drivers have
/// drained their in-flight pushes and written their resume checkpoint
/// (bounded node-side at ~5 s), so wrap the call in a
/// `beginBackgroundTask` and treat its return as "state is safely on
/// disk". Idempotent — safe to call repeatedly, in any order with
/// [`ant_wake`], and with nothing uploading.
///
/// Returns `0` on success, `-1` on a null handle, `-2` if the node loop
/// didn't ack (already shut down); in the `-2` case an allocated error
/// string is written into `*out_err` (free with [`ant_free_string`]).
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been passed
///   to [`ant_shutdown`].
/// * `out_err` must point at a writable `*mut c_char` slot, or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_suspend(handle: *const AntHandle, out_err: *mut *mut c_char) -> i32 {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_suspend: null handle");
            return -1;
        };
        match catch_unwind(AssertUnwindSafe(|| drive::suspend(handle))) {
            Ok(Ok(msg)) => {
                tracing::info!(target: "ant-ffi", "{msg}");
                0
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -2
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_suspend");
                -2
            }
        }
    }
}

/// Undo [`ant_suspend`] — call on foreground / network-restored
/// transitions. Restarts only the uploads the suspend paused (a user
/// pause stays paused) and re-queues the securing pass for any
/// completed-but-unverified upload, exactly like a fresh launch does.
/// Cheap and idempotent; safe without a prior suspend. Pair with
/// [`ant_resume`], which re-warms the *peer connections* after the same
/// suspension — the two recover different halves of the node.
///
/// Return values and safety: same contract as [`ant_suspend`].
///
/// # Safety
///
/// See [`ant_suspend`].
#[no_mangle]
pub unsafe extern "C" fn ant_wake(handle: *const AntHandle, out_err: *mut *mut c_char) -> i32 {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_wake: null handle");
            return -1;
        };
        match catch_unwind(AssertUnwindSafe(|| drive::wake(handle))) {
            Ok(Ok(msg)) => {
                tracing::info!(target: "ant-ffi", "{msg}");
                0
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -2
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_wake");
                -2
            }
        }
    }
}

/// Snapshot the running node's `agent` string from the live status
/// channel — currently `ant-ffi/<crate-version>` set at
/// [`ant_init`] time. Lets the host display the version that is
/// actually running, rather than e.g. `CFBundleShortVersionString`
/// from the iOS bundle which can drift from the embedded library.
///
/// Returns a freshly-allocated UTF-8 C string the caller must release
/// with [`ant_free_string`], or `NULL` if `handle` is null / the
/// agent string contains an interior NUL (cannot happen in practice
/// since we control the format).
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_agent_string(handle: *const AntHandle) -> *mut c_char {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return std::ptr::null_mut();
        };
        let agent = handle.status_rx.borrow().agent.clone();
        match CString::new(agent) {
            Ok(cs) => cs.into_raw(),
            Err(_) => std::ptr::null_mut(),
        }
    }
}

/// C-ABI mirror of [`DownloadProgressState`]. Kept as `#[repr(C)]` so
/// the Swift side can treat it as a plain POD struct without a
/// bridging header beyond `ant.h`. Field order is locked to the header
/// declaration — do not reorder without regenerating the Swift view.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct AntProgress {
    /// `1` while a download is in-flight, `0` once it finished (or
    /// before the first call). Swift uses this to hide / show the
    /// progress HUD.
    pub in_progress: u8,
    /// Bytes of chunk wire data delivered to the joiner so far.
    pub bytes_done: u64,
    /// Expected file-body size derived from the data root's span.
    /// `0` until the root chunk has been fetched (and for single-chunk
    /// files, `1` chunk / root span bytes right away).
    pub total_bytes: u64,
    /// Number of chunks delivered to the joiner so far (cache hits +
    /// successful network fetches).
    pub chunks_done: u64,
    /// Estimated total chunks from the root span. `0` until known.
    pub total_chunks: u64,
    /// Wall-clock milliseconds since the node accepted the request.
    pub elapsed_ms: u64,
    /// Distinct peers we've fetched at least one chunk from. `0`
    /// means everything came from cache so far.
    pub peers_used: u32,
    /// Chunk fetches currently dispatched but not yet returned (a
    /// proxy for "how busy is the pipe right now").
    pub in_flight: u32,
    /// Cache hits served without going to the network.
    pub cache_hits: u64,
}

/// Request cancellation of any in-flight `ant_download` call on this
/// handle. The flag is checked cooperatively on the next ack loop
/// iteration (≤ one progress tick, i.e. ~150 ms under normal
/// retrieval) and wakes the ack recv / no-peers backoff immediately
/// via the handle's cancel notifier. The in-flight `ant_download`
/// returns with the error string `"download canceled"`.
///
/// Safe to call at any time, including when no download is running
/// (no-op) or from a different thread than the one blocked in
/// `ant_download`.
///
/// Returns `0` on success, `-1` if `handle` is null.
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_cancel_download(handle: *const AntHandle) -> i32 {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return -1;
        };
        handle.cancel_flag.store(true, Ordering::SeqCst);
        handle.cancel_notify.notify_waiters();
        0
    }
}

/// Sample the current download progress. Swift polls this every few
/// hundred milliseconds while a download is running to drive the
/// progress bar and throughput readout.
///
/// Returns `0` on success (and fills `*out`), `-1` on a null handle
/// or a null `out`. Never blocks beyond a short mutex acquisition;
/// the mutex is held only while `attempt_download` copies the latest
/// `ControlAck::Progress` sample in.
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been
///   passed to [`ant_shutdown`].
/// * `out` must point at a writable `AntProgress` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_download_progress(
    handle: *const AntHandle,
    out: *mut AntProgress,
) -> i32 {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return -1;
        };
        let Some(out) = out.as_mut() else {
            return -1;
        };
        let snapshot = match handle.progress.lock() {
            Ok(guard) => *guard,
            Err(poisoned) => *poisoned.into_inner(),
        };
        *out = AntProgress {
            in_progress: u8::from(snapshot.in_progress),
            bytes_done: snapshot.bytes_done,
            total_bytes: snapshot.total_bytes,
            chunks_done: snapshot.chunks_done,
            total_chunks: snapshot.total_chunks,
            elapsed_ms: snapshot.elapsed_ms,
            peers_used: snapshot.peers_used,
            in_flight: snapshot.in_flight,
            cache_hits: snapshot.cache_hits,
        };
        0
    }
}

// ---------------------------------------------------------------------------
// Manifest listing — feeds the iOS player's "Up next" queue.
// ---------------------------------------------------------------------------

/// List the entries of a mantaray manifest. `reference` is a
/// `bzz://<64-hex>` (or bare 64-hex) string. Returns a heap-allocated
/// NUL-terminated JSON document of the form
/// `{"entries":[{"path":"...","reference":"...","size":N,"content_type":"..."},...]}`,
/// or null + error string on failure.
///
/// Free the returned string with [`ant_free_string`].
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been
///   passed to [`ant_shutdown`].
/// * `reference` must be a valid NUL-terminated UTF-8 string.
/// * `out_err` must point at a writable `*mut c_char` slot, or be null
///   to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_list_bzz(
    handle: *mut AntHandle,
    reference: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<String, FfiError> {
            let handle = handle.as_ref().ok_or(FfiError::NullPointer)?;
            let reference = cstr_to_str(reference)?;
            let parsed = parse_reference(reference)?;
            // Manifest listing only makes sense for `bzz://` references.
            // Reject `bytes://` here so callers don't get the gateway's
            // generic "not a manifest" error halfway down the stack.
            let root = match parsed {
                ParsedRef::Bzz { root, .. } => root,
                ParsedRef::Bytes(_) => {
                    return Err(FfiError::Reference(
                        "ant_list_bzz requires a bzz:// reference".into(),
                    ))
                }
            };
            manifest::list_to_json(&handle.runtime.handle().clone(), &handle.cmd_tx, root)
                .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(json)) => {
                let cstring = CString::new(json.replace('\0', "?"))
                    .unwrap_or_else(|_| CString::new("manifest contained NUL byte").unwrap());
                cstring.into_raw()
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_list_bzz");
                std::ptr::null_mut()
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming session — backs the AVAssetResourceLoaderDelegate path.
// ---------------------------------------------------------------------------

/// Open a streaming session for `reference` (a `bzz://<hex>/path`
/// string). Sends a `head_only` request to learn the file size and
/// content type, then returns an opaque `AntStream*` the caller passes
/// to [`ant_stream_read`] / [`ant_stream_progress`] / [`ant_stream_close`].
///
/// On success: writes the file size into `*out_total_bytes` (when
/// non-null) and the manifest's `Content-Type` into `*out_content_type`
/// (when non-null and the manifest carried one — caller frees the
/// string with [`ant_free_string`]).
///
/// On failure: returns null and writes an allocated error into
/// `*out_err`.
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must outlive the returned
///   stream (calling [`ant_shutdown`] before [`ant_stream_close`] is
///   undefined behaviour).
/// * `reference` must be a valid NUL-terminated UTF-8 string of the
///   form `bzz://<64-hex>[/path]`.
/// * `out_total_bytes`, `out_content_type`, `out_err` may each be null
///   or point at a writable slot.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_open(
    handle: *mut AntHandle,
    reference: *const c_char,
    out_total_bytes: *mut u64,
    out_content_type: *mut *mut c_char,
    out_err: *mut *mut c_char,
) -> *mut AntStream {
    unsafe {
        clear_out_err(out_err);
        if !out_content_type.is_null() {
            *out_content_type = std::ptr::null_mut();
        }
        if !out_total_bytes.is_null() {
            *out_total_bytes = 0;
        }
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<AntStream, FfiError> {
            let handle = handle.as_ref().ok_or(FfiError::NullPointer)?;
            let reference = cstr_to_str(reference)?;
            let parsed = parse_reference(reference)?;
            let (root, path) = match parsed {
                ParsedRef::Bzz { root, path } => (root, path),
                ParsedRef::Bytes(_) => {
                    return Err(FfiError::Reference(
                        "ant_stream_open requires a bzz:// reference".into(),
                    ))
                }
            };
            AntStream::open(
                handle.runtime.handle().clone(),
                handle.cmd_tx.clone(),
                root,
                path,
            )
            .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(stream)) => {
                if !out_total_bytes.is_null() {
                    *out_total_bytes = stream.total_bytes();
                }
                if !out_content_type.is_null() {
                    if let Some(ct) = stream.content_type() {
                        let cstring = CString::new(ct.replace('\0', "?")).unwrap_or_else(|_| {
                            CString::new("content_type contained NUL byte").unwrap()
                        });
                        *out_content_type = cstring.into_raw();
                    }
                }
                Box::into_raw(Box::new(stream))
            }
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_stream_open");
                std::ptr::null_mut()
            }
        }
    }
}

/// Pull `len` bytes starting at `offset` into `dst`. Blocks the calling
/// thread until the bytes are ready (or the per-read timeout fires).
/// Returns the number of bytes actually written into `dst` on success,
/// or `-1` on error (with an allocated error string in `*out_err`).
///
/// Reads past `total_bytes` are truncated to the available tail; the
/// return value reports the truncated length.
///
/// # Safety
///
/// * `stream` must come from [`ant_stream_open`] and must not have been
///   passed to [`ant_stream_close`].
/// * `dst` must point at a writable buffer of at least `len` bytes.
/// * `out_err` may be null to opt out of error reporting.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_read(
    stream: *mut AntStream,
    offset: u64,
    len: usize,
    dst: *mut u8,
    out_err: *mut *mut c_char,
) -> isize {
    unsafe {
        clear_out_err(out_err);
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<usize, FfiError> {
            let stream = stream.as_ref().ok_or(FfiError::NullPointer)?;
            if dst.is_null() && len > 0 {
                return Err(FfiError::NullPointer);
            }
            let dst_slice = std::slice::from_raw_parts_mut(dst, len);
            stream
                .read(offset, len, dst_slice)
                .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(n)) => isize::try_from(n).unwrap_or(isize::MAX),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -1
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_stream_read");
                -1
            }
        }
    }
}

/// Pull a contiguous byte range and deliver each joiner-produced
/// chunk to `on_chunk` as it arrives. Unlike [`ant_stream_read`] —
/// which holds the calling thread until the entire `len` bytes have
/// been buffered — this drives a single `StreamBzz` request all the
/// way to its terminal `StreamDone`, invoking the callback for every
/// `BytesChunk` ack along the way. That matches the
/// `AVAssetResourceLoaderDelegate` shape on the Swift side: a long-
/// lived `dataRequest` that wants `respond(with:)` called as soon as
/// each piece of body lands, not after the whole window has buffered.
///
/// `on_chunk(ctx, ptr, len) -> i32`: returning a non-zero value asks
/// the FFI to stop pulling early (e.g. `AVPlayer` canceled the
/// `dataRequest`). The remaining acks are drained for clean shutdown.
/// The callback runs on a tokio runtime worker thread and must be
/// non-blocking and Send-safe.
///
/// Returns the total number of bytes delivered, or `-1` on error
/// (with `*out_err` set when the slot is non-null).
///
/// # Safety
///
/// * `stream` must come from [`ant_stream_open`] and must not have
///   been passed to [`ant_stream_close`].
/// * `on_chunk` must be a valid C ABI function pointer; `ctx` is
///   passed through opaquely and may be null.
/// * The callback receives a non-null `data` pointer and `len > 0`.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_pull(
    stream: *mut AntStream,
    offset: u64,
    len: u64,
    on_chunk: Option<
        unsafe extern "C" fn(ctx: *mut std::ffi::c_void, data: *const u8, len: usize) -> i32,
    >,
    ctx: *mut std::ffi::c_void,
    out_err: *mut *mut c_char,
) -> i64 {
    unsafe {
        clear_out_err(out_err);
        // SAFETY: the FFI shape uses a raw `*mut c_void` ctx that
        // Swift will only pass back into us through `on_chunk`. Once we
        // wrap it in a `usize` it crosses the `catch_unwind` boundary
        // without UnwindSafe complaints, and we cast back inside the
        // callback closure.
        let ctx_addr = ctx as usize;
        let result = catch_unwind(AssertUnwindSafe(|| -> Result<u64, FfiError> {
            let stream = stream.as_ref().ok_or(FfiError::NullPointer)?;
            let cb = on_chunk.ok_or_else(|| FfiError::Download("on_chunk is null".into()))?;
            let mut closure = |chunk: &[u8]| -> bool {
                if chunk.is_empty() {
                    return false;
                }
                let rc = cb(
                    ctx_addr as *mut std::ffi::c_void,
                    chunk.as_ptr(),
                    chunk.len(),
                );
                rc != 0
            };
            stream
                .pull(offset, len, &mut closure)
                .map_err(|e| FfiError::Download(e.to_string()))
        }));
        match result {
            Ok(Ok(n)) => i64::try_from(n).unwrap_or(i64::MAX),
            Ok(Err(e)) => {
                write_out_err(out_err, &e.to_string());
                -1
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_stream_pull");
                -1
            }
        }
    }
}

/// Sample the cumulative state of a stream (bytes downloaded so far,
/// total file size, last `Progress` chunk/peer counters). Fills `*out`
/// and returns 0 on success; returns `-1` if either pointer is null.
///
/// `bytes_done` is the running sum of bytes the stream has handed back
/// across all `ant_stream_read` calls — i.e. the number of bytes
/// `AVPlayer` has actually consumed for *this* file. The Network tab uses
/// this for the "2.4 MB / 117.5 MB" progress card. The chunk / peer /
/// in-flight numbers reflect the *most recent* range request the node
/// emitted a `Progress` ack for, which is the most useful proxy for
/// "what is the network doing right now" while a stream plays.
///
/// # Safety
///
/// * `stream` must come from [`ant_stream_open`] and must not have been
///   passed to [`ant_stream_close`].
/// * `out` must point at a writable [`AntProgress`] slot.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_progress(
    stream: *const AntStream,
    out: *mut AntProgress,
) -> i32 {
    unsafe {
        let Some(stream) = stream.as_ref() else {
            return -1;
        };
        let Some(out) = out.as_mut() else {
            return -1;
        };
        let (
            bytes_done,
            total_bytes,
            chunks_done,
            total_chunks,
            elapsed_ms,
            peers,
            in_flight,
            cache,
        ) = stream::snapshot_progress(stream);
        *out = AntProgress {
            in_progress: 1,
            bytes_done,
            total_bytes,
            chunks_done,
            total_chunks,
            elapsed_ms,
            peers_used: peers,
            in_flight,
            cache_hits: cache,
        };
        0
    }
}

/// Close a stream and free its handle. After this returns, `stream`
/// must not be used again.
///
/// # Safety
///
/// `stream` must have come from [`ant_stream_open`]. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_stream_close(stream: *mut AntStream) {
    unsafe {
        if stream.is_null() {
            return;
        }
        drop(Box::from_raw(stream));
    }
}

// ---------------------------------------------------------------------------
// `AntDrive`: uploads, storage plan, account
//
// Each entry point returns a heap-allocated NUL-terminated string the
// caller frees with `ant_free_string` (JSON for the structured calls,
// a plain id / hex string for the rest). On failure it returns null and
// writes an allocated error string into `*out_err`. The heavy lifting
// lives in `drive.rs`; these wrappers only marshal C strings and trap
// panics so a Rust panic can never unwind across the FFI boundary.
// ---------------------------------------------------------------------------

/// Start an upload job for the file at `path`. `batch_id` selects the
/// storage plan to stamp against (pass null to use the node's default);
/// `name` / `content_type` are optional manifest metadata (pass null to
/// let the daemon infer them). Returns the new job id.
///
/// # Safety
///
/// * `handle` must come from [`ant_init`] and must not have been passed
///   to [`ant_shutdown`].
/// * `path` must be a valid NUL-terminated UTF-8 string; the optional
///   args may each be null or a valid NUL-terminated UTF-8 string.
/// * `out_err` must point at a writable slot or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_upload_start(
    handle: *const AntHandle,
    path: *const c_char,
    batch_id: *const c_char,
    name: *const c_char,
    content_type: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_upload_start", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let path = cstr_to_string(path)?;
            let batch_id = cstr_to_opt_string(batch_id)?;
            let name = cstr_to_opt_string(name)?;
            let content_type = cstr_to_opt_string(content_type)?;
            drive::upload_start(h, PathBuf::from(path), batch_id, name, content_type)
                .map_err(|e| e.to_string())
        })
    }
}

/// Snapshot every upload job as a JSON document `{"jobs":[...]}`.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_upload_list(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_upload_list", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            drive::upload_list(h).map_err(|e| e.to_string())
        })
    }
}

/// Snapshot one upload job by id (or unique 8-hex-char prefix) as a JSON
/// object. See [`ant_upload_start`] for safety requirements.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_upload_status(
    handle: *const AntHandle,
    job_id: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        upload_job_call(
            handle,
            job_id,
            out_err,
            drive::JobCommand::Status,
            "ant_upload_status",
        )
    }
}

/// Pause a running upload. Returns the updated job JSON.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_upload_pause(
    handle: *const AntHandle,
    job_id: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        upload_job_call(
            handle,
            job_id,
            out_err,
            drive::JobCommand::Pause,
            "ant_upload_pause",
        )
    }
}

/// Resume a paused (or failed) upload. Returns the updated job JSON. For a
/// *completed* job this instead triggers a background self-heal that
/// re-pushes only the job's missing chunks on the same job (the app's "push
/// again"), returning the current job JSON immediately.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_upload_resume(
    handle: *const AntHandle,
    job_id: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        upload_job_call(
            handle,
            job_id,
            out_err,
            drive::JobCommand::Resume,
            "ant_upload_resume",
        )
    }
}

/// "Push again" with streamed progress: re-push a completed job's missing
/// chunks on the same job (no new job), invoking `on_progress` (with `ctx`)
/// for each `{"phase":"checking"|"repushing","checked"?,"total"?}` update
/// as the heal runs, then returning the updated job JSON. The callback
/// fires on the calling thread inline with the (blocking) call, so keep it
/// cheap. A null `on_progress` runs the heal without progress.
///
/// # Safety
///
/// See [`ant_upload_start`]. `on_progress`, if non-null, must be a valid
/// function pointer that does not unwind across the FFI boundary; `ctx` is
/// passed through opaquely and may be null.
#[no_mangle]
pub unsafe extern "C" fn ant_upload_repush_progress(
    handle: *const AntHandle,
    job_id: *const c_char,
    on_progress: Option<AntVerifyProgressCb>,
    ctx: *mut c_void,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_upload_repush_progress", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let job_id = cstr_to_string(job_id)?;
            let ctx = ctx as usize;
            drive::upload_repush_progress(h, job_id, |line: &str| {
                if let (Some(cb), Ok(c)) = (on_progress, CString::new(line)) {
                    cb(c.as_ptr(), ctx as *mut c_void);
                }
            })
            .map_err(|e| e.to_string())
        })
    }
}

/// Cancel an upload. Returns the updated job JSON.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_upload_cancel(
    handle: *const AntHandle,
    job_id: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        upload_job_call(
            handle,
            job_id,
            out_err,
            drive::JobCommand::Cancel,
            "ant_upload_cancel",
        )
    }
}

/// Snapshot the local storage plan (postage issuer) as a JSON object.
/// `{"enabled":false,...}` when no plan is connected yet; otherwise
/// carries capacity / usage so the UI can render a "X GB of Y GB used"
/// meter.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_storage_status(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_status", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            drive::storage_status(h).map_err(|e| e.to_string())
        })
    }
}

/// Outbound-settlement status as JSON `{"enabled":bool,"chequebook":…}`.
/// `enabled` is `true` once a chequebook is deployed, which is what lets
/// uploads actually propagate (bee charges the uploader per pushed chunk
/// and freezes out a node that can't pay). The Storage tab reads this to
/// warn when a connected plan still won't upload reliably. On a build
/// without `chain` support settlement is never available, so this
/// reports `{"enabled":false,"chequebook":null}`.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_storage_settlement_status(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_settlement_status", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            #[cfg(feature = "chain")]
            {
                drive::settlement_status(h).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = h;
                Ok(r#"{"enabled":false,"chequebook":null}"#.to_string())
            }
        })
    }
}

/// Settlement-deposit status as JSON `{"enabled","chequebook",
/// "deposit_plur","deposit_bzz","target_plur","target_bzz",
/// "shortfall_plur","shortfall_bzz","needs_top_up","xdai_required",
/// "xdai_required_display","xdai_to_send","xdai_to_send_display",
/// "sufficient_funds"}`.
///
/// [`ant_storage_settlement_status`] answers "is a chequebook deployed?";
/// this answers "does it actually back the cheques it signs?". A
/// chequebook at deposit 0 — what every install before this deployed —
/// publishes fine until the peers' payment tolerance runs out, then
/// collapses into pushsync timeouts, so the Storage tab reads this to
/// detect that state and offer a top-up ([`ant_storage_settlement_topup`]).
/// `enabled=false` (zeroed, `needs_top_up=false`) when this account has
/// no chequebook yet; buying or connecting a plan deploys one, funded.
///
/// Reads chain (two or three light `eth_call`s), so call it on an
/// explicit refresh rather than every status poll. Requires the `chain`
/// build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_settlement_deposit(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_settlement_deposit", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                if rpc.trim().is_empty() {
                    return Err("ant_storage_settlement_deposit: gnosis_rpc required".to_string());
                }
                drive::settlement_deposit(h, rpc).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Fund this account's chequebook up to the settlement deposit target
/// (0.001 xBZZ), funding **only with xDAI**: the node swaps the xBZZ
/// shortfall on-chain if it doesn't already hold it, then transfers the
/// deposit to the chequebook. The explicit top-up path — a chequebook's
/// deposit is only read at deploy time, so an already-deployed one can be
/// funded no other way.
///
/// Idempotent: a chequebook already at the target is a no-op. Errors when
/// this account has no chequebook yet. Returns the refreshed
/// [`ant_storage_settlement_deposit`] JSON. **Submits real transactions
/// and spends real funds** and **blocks** until they confirm, so the app
/// gates it behind explicit confirmation. Requires the `chain` build
/// feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_settlement_topup(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_settlement_topup", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                if rpc.trim().is_empty() {
                    return Err("ant_storage_settlement_topup: gnosis_rpc required".to_string());
                }
                drive::settlement_topup_xdai(h, rpc).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Deep read-back propagation check for an uploaded `reference`.
///
/// Resolves the manifest at `reference` to its data root, enumerates the
/// file's chunk tree (fetching every interior node network-only, which
/// proves the skeleton is retrievable), then probes an evenly-spread
/// sample of up to `samples` real data **leaves** across up to `probes`
/// distinct closest BZZ peers each. All probes bypass the daemon's local
/// caches so our own store-then-push copy can't mask a failed push.
///
/// Returns JSON `{"reference","retrievable","total_chunks",
/// "leaf_chunks","intermediate_chunks","checked_chunks",
/// "retrievable_chunks","sampled_leaves","sources","error"?}`.
/// `retrievable` is true iff the data root and every interior node were
/// fetched and every sampled leaf came back from at least one peer;
/// `sources` is the minimum distinct-route count observed across sampled
/// leaves (a replication floor).
///
/// `reference` accepts a bare/`0x` 64-hex address, a `bytes://` ref, or
/// a `bzz://<ref>/<path>` URL (the root address is used; the path is
/// ignored — verification always resolves the manifest's default file).
/// `samples` is clamped to `1..=32` and `probes` to `1..=8` by the node
/// loop; pass `0` for either to use a sensible default.
///
/// # Safety
///
/// See [`ant_upload_start`]. `reference` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_verify_propagation(
    handle: *const AntHandle,
    reference: *const c_char,
    samples: u8,
    probes: u8,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_verify_propagation", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let reference = cstr_to_string(reference)?;
            let root = match parse_reference(&reference).map_err(|e| e.to_string())? {
                ParsedRef::Bytes(root) | ParsedRef::Bzz { root, .. } => root,
            };
            drive::verify_propagation(h, root, samples, probes).map_err(|e| e.to_string())
        })
    }
}

/// Callback invoked with each progress update during a streaming
/// verification (see [`ant_storage_verify_propagation_progress`]). The
/// `progress_json` pointer is a borrowed, NUL-terminated UTF-8 string of
/// the shape `{"phase":"resolving"|"enumerating"|"checking",
/// "checked"?:int,"total"?:int}` — valid only for the duration of the
/// call, so copy out anything you need to keep. `ctx` is the opaque
/// pointer passed through at call time.
pub type AntVerifyProgressCb = extern "C" fn(progress_json: *const c_char, ctx: *mut c_void);

/// Like [`ant_storage_verify_propagation`], but invokes `on_progress`
/// (with `ctx`) for each incremental progress update as the check runs,
/// then returns the same final verdict JSON. Progress is best-effort: a
/// null `on_progress` simply behaves like the non-streaming variant. The
/// callback fires on the calling thread, inline with the (blocking) call,
/// so keep it cheap.
///
/// # Safety
///
/// See [`ant_storage_verify_propagation`]. `on_progress`, if non-null,
/// must be a valid function pointer that does not unwind across the FFI
/// boundary; `ctx` is passed through opaquely and may be null.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_verify_propagation_progress(
    handle: *const AntHandle,
    reference: *const c_char,
    samples: u8,
    probes: u8,
    on_progress: Option<AntVerifyProgressCb>,
    ctx: *mut c_void,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_verify_propagation_progress", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let reference = cstr_to_string(reference)?;
            let root = match parse_reference(&reference).map_err(|e| e.to_string())? {
                ParsedRef::Bytes(root) | ParsedRef::Bzz { root, .. } => root,
            };
            // `ctx` is a bare pointer the host owns; the FFI just relays it.
            let ctx = ctx as usize;
            drive::verify_propagation_progress(h, root, samples, probes, |line: &str| {
                if let (Some(cb), Ok(c)) = (on_progress, CString::new(line)) {
                    cb(c.as_ptr(), ctx as *mut c_void);
                }
            })
            .map_err(|e| e.to_string())
        })
    }
}

/// Request cancellation of the in-flight propagation verification for this
/// handle (started by `ant_storage_verify_propagation_progress` or the
/// non-streaming variant). Cooperative: the node aborts at the next phase
/// or leaf boundary and the verify call returns a `{"cancelled":true,...}`
/// body. A no-op if nothing is being verified. Safe to call from any
/// thread while the verify call is blocked on another.
///
/// # Safety
///
/// `handle` must be a live handle from [`ant_init`] (or null, which is a
/// no-op).
#[no_mangle]
pub unsafe extern "C" fn ant_verify_cancel(handle: *const AntHandle) {
    unsafe {
        if let Some(h) = handle.as_ref() {
            h.verify_cancel.store(true, Ordering::Relaxed);
        }
    }
}

/// Account identity as a JSON object
/// `{"eth_address","overlay","peer_id","agent"}`.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_account_info(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_account_info", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            drive::account_info(h).map_err(|e| e.to_string())
        })
    }
}

/// Export the account's raw secp256k1 signing key as 64 hex chars, for a
/// "back up your account" flow. Treat as a secret.
///
/// # Safety
///
/// See [`ant_upload_start`].
#[no_mangle]
pub unsafe extern "C" fn ant_account_export_key(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_account_export_key", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            Ok(drive::account_export_key(h))
        })
    }
}

/// Connect a storage plan this account already owns on Gnosis by its id.
/// Reads the plan's parameters from `gnosis_rpc`, verifies the account
/// owns it, registers it for stamping, and returns the refreshed
/// [`ant_storage_status`] JSON. Requires the `chain` build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` and `batch_id` must be valid
/// NUL-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_connect_batch(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    batch_id: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_connect_batch", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            let batch = cstr_to_string(batch_id)?;
            #[cfg(feature = "chain")]
            {
                drive::storage_connect_batch(h, rpc, batch).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc, batch);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Auto-discover and connect every funded storage plan this account owns
/// on Gnosis (an on-chain log scan, so it can take a while). Returns
/// `{"registered":[...ids],"status":<plan>}`. Requires the `chain`
/// build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_discover(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_discover", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                drive::storage_discover(h, rpc).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Deploy (or return the already-persisted) node-owned chequebook so the
/// iOS publish-setup checklist's "chequebook deployed" step can complete.
///
/// Idempotent: if this device already deployed a chequebook (persisted at
/// `<data_dir>/chequebook.json`) it's returned as-is, no redeploy —
/// though one still short of its settlement deposit is topped up from
/// spare xBZZ. Otherwise this signs an on-chain `factory.deploySimpleSwap`
/// (issuer = node EOA), funds it with the 0.001 xBZZ settlement deposit
/// so its cheques are actually backed, persists the association, and
/// returns the new address. These are on-chain transactions: they spend
/// gas plus the deposit and **block** until confirmed. Light-mode
/// (`chain`-feature) builds only.
///
/// Returns a heap C string `{"chequebookAddress":"0x<40hex>"}` on success
/// (free with [`ant_free_string`]), or `NULL` with an error written to
/// `out_err`. The caller should restart the gateway afterwards (stop +
/// start) so [`ant_start_gateway`] reloads the persisted chequebook into
/// its `ChainContext` and `/chequebook/address` reflects it.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string. `out_err`, if non-null, must point to a writable
/// `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_deploy_chequebook(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_deploy_chequebook", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                if rpc.trim().is_empty() {
                    return Err("ant_deploy_chequebook: gnosis_rpc required".to_string());
                }
                drive::deploy_chequebook(h, rpc).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc);
                Err("ant_deploy_chequebook: built without the `chain` feature".to_string())
            }
        })
    }
}

/// Price a storage plan: returns a JSON object with the plan cost
/// (`total_cost_plur` / `total_cost_bzz`), the one-time settlement
/// deposit the account's chequebook still needs
/// (`settlement_deposit_plur` / `settlement_deposit_bzz`, zero once it is
/// funded), the account's xBZZ / xDAI balances, and whether they cover
/// the lot — the "payment information" the Get Started flow shows before
/// activating. The all-in figures (`needed_bzz`, `xdai_required`,
/// `xdai_to_send`, `sufficient_funds`) include the deposit, because
/// activating a plan is also what deploys and funds the chequebook. No
/// transaction is sent. Requires the `chain` build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_quote(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    depth: u8,
    days: u64,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_quote", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                drive::storage_quote(h, rpc, depth, days).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc, depth, days);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Remaining lifetime of the connected storage plan as JSON
/// `{enabled,remaining_seconds,expires_unix}`: reads the batch's on-chain
/// remaining balance and the current postage price and converts to time.
/// `enabled=false` (with zeroed fields) when no plan is connected.
/// Requires the `chain` build feature. One light RPC round-trip — meant for
/// an explicit refresh, not every status poll.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_validity(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_validity", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                drive::storage_validity(h, rpc).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Price a top-up (lifetime extension) of the *connected* storage plan:
/// what extending it by `days` costs at the current postage price, and
/// whether the account's funds cover it. Same JSON shape as
/// [`ant_storage_quote`] (`depth` is the connected plan's depth). Errors
/// when no plan is connected. No transaction is sent. Requires the
/// `chain` build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` must be a valid NUL-terminated
/// UTF-8 string.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_topup_quote(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    days: u64,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_topup_quote", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            #[cfg(feature = "chain")]
            {
                drive::storage_topup_quote(h, rpc, days).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc, days);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Top up (extend the lifetime of) the connected storage plan, funding
/// **only with xDAI**: the node swaps the xBZZ shortfall on-chain if
/// needed, then runs `approve` + `PostageStamp.topUp`. `amount_per_chunk`
/// is the value returned by [`ant_storage_topup_quote`] (so the charge
/// matches the quote the user approved). Returns the refreshed
/// [`ant_storage_validity`] JSON (the new expiry). Submits real
/// transactions and spends real funds. Requires the `chain` build
/// feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` and `amount_per_chunk` must be
/// valid NUL-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_topup_xdai(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    amount_per_chunk: *const c_char,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_topup_xdai", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            let amount = cstr_to_string(amount_per_chunk)?;
            #[cfg(feature = "chain")]
            {
                drive::storage_topup_xdai(h, rpc, amount).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc, amount);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Buy and activate a storage plan on Gnosis (`approve` + `createBatch`,
/// then register it so uploads can stamp against it). `amount_per_chunk`
/// is the value returned by [`ant_storage_quote`] (so the charge matches
/// the quote the user approved); `immutable` is 0 or 1. Returns the
/// refreshed [`ant_storage_status`] JSON. Submits real transactions and
/// spends real funds. Requires the `chain` build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` and `amount_per_chunk` must be
/// valid NUL-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_buy(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    depth: u8,
    amount_per_chunk: *const c_char,
    immutable: c_int,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_buy", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            let amount = cstr_to_string(amount_per_chunk)?;
            let imm = immutable != 0;
            #[cfg(feature = "chain")]
            {
                drive::storage_buy(h, rpc, depth, amount, imm).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc, depth, amount, imm);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Buy and activate a storage plan funding **only with xDAI**: the node
/// swaps the xBZZ shortfall on-chain (deploying a tiny stateless swap
/// helper the first time), then runs the same `approve` + `createBatch`
/// flow as [`ant_storage_buy`]. `amount_per_chunk` is the value returned
/// by [`ant_storage_quote`]; `immutable` is 0 or 1. Returns the refreshed
/// [`ant_storage_status`] JSON. Submits real transactions and spends real
/// funds. Requires the `chain` build feature.
///
/// # Safety
///
/// See [`ant_upload_start`]. `gnosis_rpc` and `amount_per_chunk` must be
/// valid NUL-terminated UTF-8 strings.
#[no_mangle]
pub unsafe extern "C" fn ant_storage_buy_xdai(
    handle: *const AntHandle,
    gnosis_rpc: *const c_char,
    depth: u8,
    amount_per_chunk: *const c_char,
    immutable: c_int,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_storage_buy_xdai", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let rpc = cstr_to_string(gnosis_rpc)?;
            let amount = cstr_to_string(amount_per_chunk)?;
            let imm = immutable != 0;
            #[cfg(feature = "chain")]
            {
                drive::storage_buy_xdai(h, rpc, depth, amount, imm).map_err(|e| e.to_string())
            }
            #[cfg(not(feature = "chain"))]
            {
                let _ = (h, rpc, depth, amount, imm);
                Err(
                    "this build has no chain support (rebuild ant-ffi with --features chain)"
                        .to_string(),
                )
            }
        })
    }
}

/// Shared body for the single-job upload commands (status / pause /
/// resume / cancel), which all take a job id and return job JSON.
unsafe fn upload_job_call(
    handle: *const AntHandle,
    job_id: *const c_char,
    out_err: *mut *mut c_char,
    kind: drive::JobCommand,
    name: &'static str,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, name, || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let job_id = cstr_to_string(job_id)?;
            drive::upload_job_command(h, job_id, kind).map_err(|e| e.to_string())
        })
    }
}

/// Run `f`, trap panics, and marshal its `Result<String, String>` into
/// an owned C string / `out_err` pair the Swift side can consume.
unsafe fn run_string_call<F>(out_err: *mut *mut c_char, name: &str, f: F) -> *mut c_char
where
    F: FnOnce() -> Result<String, String>,
{
    unsafe {
        clear_out_err(out_err);
        match catch_unwind(AssertUnwindSafe(f)) {
            Ok(Ok(s)) => {
                if let Ok(cs) = CString::new(s.replace('\0', "?")) {
                    cs.into_raw()
                } else {
                    write_out_err(out_err, "response contained a NUL byte");
                    std::ptr::null_mut()
                }
            }
            Ok(Err(msg)) => {
                write_out_err(out_err, &msg);
                std::ptr::null_mut()
            }
            Err(_) => {
                write_out_err(out_err, &format!("panic in {name}"));
                std::ptr::null_mut()
            }
        }
    }
}

fn null_handle() -> String {
    "null handle".to_string()
}

unsafe fn cstr_to_string(ptr: *const c_char) -> Result<String, String> {
    unsafe {
        cstr_to_str(ptr)
            .map(str::to_string)
            .map_err(|e| e.to_string())
    }
}

/// Like [`cstr_to_string`] but a null pointer (or empty string) maps to
/// `None` rather than an error — used for the optional upload metadata
/// args the Swift side leaves unset.
unsafe fn cstr_to_opt_string(ptr: *const c_char) -> Result<Option<String>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    let s = unsafe { cstr_to_str(ptr).map_err(|e| e.to_string())? };
    Ok(if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    })
}

// ---------------------------------------------------------------------------
// Memory management
// ---------------------------------------------------------------------------

/// Free a buffer returned by [`ant_download`]. `len` must be the value
/// the function wrote into `*out_len`; passing a different length is
/// undefined behaviour (we allocate with that exact length).
///
/// # Safety
///
/// See above. Calling with a null pointer is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_free_buffer(ptr: *mut u8, len: usize) {
    unsafe {
        if ptr.is_null() || len == 0 {
            return;
        }
        let slice = std::slice::from_raw_parts_mut(ptr, len);
        drop(Box::from_raw(std::ptr::from_mut::<[u8]>(slice)));
    }
}

/// Free a NUL-terminated string written by the FFI into an `out_err`
/// out-parameter.
///
/// # Safety
///
/// `ptr` must have come from this crate. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_free_string(ptr: *mut c_char) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        drop(CString::from_raw(ptr));
    }
}

/// Shut the embedded node down. Stops the Tokio runtime and frees the
/// handle. After this returns, `handle` must not be used again.
///
/// Blocks until the node's tasks have stopped (bounded by
/// [`SHUTDOWN_GRACE`]), so call it off the host's main thread. It has to
/// block: a restore does `ant_shutdown(A)` then `ant_init(B)` over the
/// same data dir, and a task of A's still running after this returns
/// (an upload checkpoint or postage persist is a `create_dir_all`, a
/// write and a rename) would recreate A's canonical files *after*
/// `ant_init(B)` parked them — attributing A's state to B, or leaving
/// both copies for the next switch to abort on in
/// `move_account_entries`.
///
/// # Safety
///
/// `handle` must have come from [`ant_init`]. Null is a no-op.
#[no_mangle]
pub unsafe extern "C" fn ant_shutdown(handle: *mut AntHandle) {
    unsafe {
        if handle.is_null() {
            return;
        }
        let handle = Box::from_raw(handle);
        // Cancels every spawned task (including the node loop) at its
        // next await point and joins the worker / blocking threads. The
        // timeout keeps a task wedged in a syscall (a dial holding a
        // socket open) from hanging the host for good; it leaks the
        // thread rather than the wait.
        handle.runtime.shutdown_timeout(SHUTDOWN_GRACE);
    }
}

// ---------------------------------------------------------------------------
// AntStream publisher throughput benchmark (issue #67 stage 1)
// ---------------------------------------------------------------------------

/// How long [`ant_bench_stop`] waits for a cancelled run to settle
/// before returning the report anyway. An in-flight `POST /bzz` is
/// bounded by the bench's own 60 s publish deadline, so a run that
/// hasn't finished by then is not going to.
const BENCH_STOP_GRACE: Duration = Duration::from_secs(65);

/// Poll interval while waiting for a cancelled run to settle.
const BENCH_STOP_POLL: Duration = Duration::from_millis(50);

/// Start an `AntStream` publisher throughput benchmark on this node.
///
/// `config_json` is a [`bench::BenchConfig`] document; only `label` is
/// required. With a `batch_id` the run publishes real segments through
/// `POST /bzz` on `gateway` (start it first with
/// [`ant_start_gateway`]); without one it measures the local
/// chunk + stamp pipeline only, which needs no network and no batch.
///
/// Returns immediately — the run drives itself on the node's runtime.
/// Poll it with [`ant_bench_progress`] and finish it with
/// [`ant_bench_stop`]. Only one run at a time per handle: a second
/// start while one is live fails rather than silently measuring two
/// publishers competing for the same uplink.
///
/// Returns `true` on success, `false` with an allocated message in
/// `out_err` (free with [`ant_free_string`]) otherwise.
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`]. `config_json` must be a NUL-terminated UTF-8
/// string. `out_err`, if non-null, must point at a writable
/// `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_bench_start(
    handle: *const AntHandle,
    config_json: *const c_char,
    out_err: *mut *mut c_char,
) -> bool {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_bench_start: null handle");
            return false;
        };
        let config = match cstr_to_str(config_json)
            .map_err(|e| e.to_string())
            .and_then(|raw| {
                serde_json::from_str::<bench::BenchConfig>(raw)
                    .map_err(|e| format!("ant_bench_start: invalid config: {e}"))
            }) {
            Ok(c) => c,
            Err(msg) => {
                write_out_err(out_err, &msg);
                return false;
            }
        };

        // Hold the slot across check → start → store, so two concurrent
        // starts can't both pass the "already running" check.
        let mut slot = handle
            .bench
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.as_ref().is_some_and(|run| !run.is_finished()) {
            write_out_err(
                out_err,
                "ant_bench_start: a benchmark is already running on this node",
            );
            return false;
        }
        match bench::start(
            handle.runtime.handle(),
            config,
            handle.signing_secret,
            Some(handle.status_rx.clone()),
        ) {
            Ok(run) => {
                *slot = Some(run);
                true
            }
            Err(e) => {
                write_out_err(out_err, &format!("ant_bench_start: {e}"));
                false
            }
        }
    }
}

/// Live progress of the run started by [`ant_bench_start`], as an
/// allocated [`bench::BenchSnapshot`] JSON string (free with
/// [`ant_free_string`]). Non-blocking. Returns null with an error when
/// no run has been started on this handle.
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`]. `out_err`, if non-null, must point at a
/// writable `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_bench_progress(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_bench_progress", || {
            let handle = handle.as_ref().ok_or_else(null_handle)?;
            let run = handle
                .bench
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .ok_or_else(|| "no benchmark has been started on this node".to_string())?;
            serde_json::to_string(&run.snapshot()).map_err(|e| format!("serialize snapshot: {e}"))
        })
    }
}

/// Stop the run and return its final [`bench::BenchReport`] as an
/// allocated JSON string (free with [`ant_free_string`]).
///
/// **Blocking**: the cancel is cooperative, so this waits (up to ~65 s,
/// the bench's own per-segment publish deadline) for the in-flight
/// segments to land — a truncated tail would understate the sustained
/// figure the report exists to state. Call it off the UI thread.
///
/// Safe to call on an already-finished run: it returns the same report.
///
/// # Safety
///
/// `handle` must come from [`ant_init`] and must not have been passed
/// to [`ant_shutdown`]. `out_err`, if non-null, must point at a
/// writable `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_bench_stop(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_bench_stop", || {
            let handle = handle.as_ref().ok_or_else(null_handle)?;
            let run = handle
                .bench
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .ok_or_else(|| "no benchmark has been started on this node".to_string())?;
            run.cancel();
            let deadline = Instant::now() + BENCH_STOP_GRACE;
            while !run.is_finished() && Instant::now() < deadline {
                std::thread::sleep(BENCH_STOP_POLL);
            }
            let report = run.report();
            // Only release the slot once the loop is actually done —
            // otherwise the next `ant_bench_start` would run against a
            // node that is still uploading the previous run's tail. And
            // only if the slot still holds *this* run: while we waited,
            // the run could have settled and a concurrent
            // `ant_bench_start` legitimately installed a new one, which
            // clearing the slot would orphan (and let a third start run
            // two publishers on one uplink).
            if run.is_finished() {
                let mut slot = handle
                    .bench
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if slot.as_ref().is_some_and(|cur| Arc::ptr_eq(cur, &run)) {
                    *slot = None;
                }
            }
            serde_json::to_string(&report).map_err(|e| format!("serialize report: {e}"))
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, FfiError> {
    unsafe {
        if ptr.is_null() {
            return Err(FfiError::NullPointer);
        }
        CStr::from_ptr(ptr)
            .to_str()
            .map_err(|_| FfiError::InvalidUtf8)
    }
}

unsafe fn cstr_to_path(ptr: *const c_char) -> Result<PathBuf, FfiError> {
    unsafe { Ok(PathBuf::from(cstr_to_str(ptr)?)) }
}

unsafe fn clear_out_err(out_err: *mut *mut c_char) {
    unsafe {
        if !out_err.is_null() {
            *out_err = std::ptr::null_mut();
        }
    }
}

unsafe fn write_out_err(out_err: *mut *mut c_char, msg: &str) {
    unsafe {
        if out_err.is_null() {
            return;
        }
        let cstring = CString::new(msg.replace('\0', "?"))
            .unwrap_or_else(|_| CString::new("error message contained a NUL byte").unwrap());
        *out_err = cstring.into_raw();
    }
}

fn load_or_create_identity(
    id_path: &Path,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; OVERLAY_NONCE_LEN], Keypair), FfiError> {
    if id_path.exists() {
        let raw = std::fs::read_to_string(id_path)
            .map_err(|e| FfiError::Io(format!("read {}: {e}", id_path.display())))?;
        return decode_identity_json(&raw);
    }

    let id = new_identity()?;
    let decoded = decode_identity(&id)?;
    let pretty = serde_json::to_string_pretty(&id)
        .map_err(|e| FfiError::Io(format!("serialize identity: {e}")))?;
    std::fs::write(id_path, pretty)
        .map_err(|e| FfiError::Io(format!("write {}: {e}", id_path.display())))?;
    Ok(decoded)
}

/// Parse an identity JSON document (`identity.json`'s shape) into the
/// three pieces the node loop needs. Shared by the on-disk path and the
/// host-provided (`ant_init_with_identity`) path.
fn decode_identity_json(
    raw: &str,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; OVERLAY_NONCE_LEN], Keypair), FfiError> {
    let id: IdentityFile =
        serde_json::from_str(raw).map_err(|e| FfiError::Io(format!("parse identity json: {e}")))?;
    decode_identity(&id)
}

fn decode_identity(
    id: &IdentityFile,
) -> Result<([u8; SECP256K1_SECRET_LEN], [u8; OVERLAY_NONCE_LEN], Keypair), FfiError> {
    let signing_secret = decode_signing_key(&id.signing_key)?;
    let mut overlay_nonce = [0u8; OVERLAY_NONCE_LEN];
    hex::decode_to_slice(&id.overlay_nonce, &mut overlay_nonce)
        .map_err(|e| FfiError::Io(format!("decode overlay_nonce: {e}")))?;
    let kp = if let Some(ref enc) = id.libp2p_keypair {
        let bytes =
            hex::decode(enc).map_err(|e| FfiError::Io(format!("decode libp2p_keypair: {e}")))?;
        Keypair::from_protobuf_encoding(&bytes)
            .map_err(|e| FfiError::Io(format!("libp2p keypair protobuf: {e}")))?
    } else {
        secp256k1_keypair_from_signing_secret(&signing_secret)?
    };
    Ok((signing_secret, overlay_nonce, kp))
}

/// Decode a 64-hex account key (an optional `0x` prefix is tolerated,
/// since that's how wallets hand keys to users) and reject anything the
/// secp256k1 group won't accept — zero, or ≥ the curve order. Doing this
/// here means a mistyped restore fails with a clear message instead of
/// surfacing as an opaque node-startup error.
fn decode_signing_key(hex_str: &str) -> Result<[u8; SECP256K1_SECRET_LEN], FfiError> {
    let trimmed = hex_str.trim();
    let body = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    let mut secret = [0u8; SECP256K1_SECRET_LEN];
    hex::decode_to_slice(body, &mut secret)
        .map_err(|e| FfiError::Crypto(format!("decode signing_key: {e}")))?;
    SigningKey::from_bytes((&secret).into())
        .map_err(|e| FfiError::Crypto(format!("invalid signing key: {e}")))?;
    Ok(secret)
}

/// A brand-new identity: random account key, random overlay nonce, and
/// the libp2p keypair derived from the key.
fn new_identity() -> Result<IdentityFile, FfiError> {
    identity_file(random_secp256k1_secret(), random_overlay_nonce())
}

/// Rebuild an identity from a bare account key — the "I still have my
/// backed-up key" restore path. The overlay nonce is derived from the
/// account's Ethereum address so the same key always produces the same
/// overlay, making the restore reproducible across devices.
fn identity_from_signing_key(hex_str: &str) -> Result<IdentityFile, FfiError> {
    let secret = decode_signing_key(hex_str)?;
    let vk = *SigningKey::from_bytes((&secret).into())
        .map_err(|e| FfiError::Crypto(format!("invalid signing key: {e}")))?
        .verifying_key();
    let eth = ethereum_address_from_public_key(&vk);
    let mut preimage = Vec::with_capacity(OVERLAY_NONCE_DOMAIN.len() + eth.len());
    preimage.extend_from_slice(OVERLAY_NONCE_DOMAIN);
    preimage.extend_from_slice(&eth);
    identity_file(secret, keccak256(&preimage))
}

fn identity_file(
    signing_secret: [u8; SECP256K1_SECRET_LEN],
    overlay_nonce: [u8; OVERLAY_NONCE_LEN],
) -> Result<IdentityFile, FfiError> {
    let kp = secp256k1_keypair_from_signing_secret(&signing_secret)?;
    Ok(IdentityFile {
        signing_key: hex::encode(signing_secret),
        overlay_nonce: hex::encode(overlay_nonce),
        libp2p_keypair: Some(hex::encode(
            kp.to_protobuf_encoding()
                .map_err(|e| FfiError::Io(format!("encode libp2p keypair: {e}")))?,
        )),
    })
}

fn secp256k1_keypair_from_signing_secret(
    secret: &[u8; SECP256K1_SECRET_LEN],
) -> Result<Keypair, FfiError> {
    let mut sk_copy = *secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut sk_copy)
        .map_err(|e| FfiError::Crypto(format!("libp2p secp256k1 secret: {e}")))?;
    let kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(kp))
}

// ---------------------------------------------------------------------------
// Logging
//
// iOS surfaces process stderr through os_log, so a plain
// tracing_subscriber::fmt() pointed at stderr shows up in the Xcode
// console unmodified. Android does *not* — release-build stderr is
// silently discarded — so on `target_os = "android"` we pipe through
// __android_log_write instead, which routes to logcat under the tag
// "ant-ffi". Both branches stay behind a `Once` so a Kotlin /
// SwiftUI host that calls ant_init twice doesn't stack two
// subscribers.
//
// The proper mobile artefact (PLAN.md § 11) replaces both with a
// `set_log_sink` callback so the host owns sink lifecycle; the smoke
// tests don't need that yet.
// ---------------------------------------------------------------------------

fn install_log_subscriber() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let filter = std::env::var("ANT_LOG")
            .or_else(|_| std::env::var("RUST_LOG"))
            .unwrap_or_else(|_| "info,ant_p2p=info,ant_retrieval=info".to_string());
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(filter))
            .with_writer(default_log_writer())
            .try_init();
    });
}

#[cfg(not(target_os = "android"))]
fn default_log_writer() -> impl for<'a> tracing_subscriber::fmt::MakeWriter<'a> + 'static {
    std::io::stderr
}

/// Logcat-backed writer. `tracing_subscriber::fmt` calls
/// `make_writer()` once per event, then writes the formatted line in
/// a single `write` call followed by a `flush`. Each `write` is one
/// fully-formed line, so we hand it straight to `__android_log_write`
/// after stripping the trailing newline (logcat appends its own).
#[cfg(target_os = "android")]
fn default_log_writer() -> android_log::MakeWriter {
    android_log::MakeWriter
}

#[cfg(target_os = "android")]
mod android_log {
    use std::ffi::CString;
    use std::io::{self, Write};
    use std::os::raw::c_char;

    /// `android_LogPriority::ANDROID_LOG_INFO`. Hard-coded rather than
    /// pulled in via `android_log_sys` to keep the Android branch
    /// dependency-free.
    const ANDROID_LOG_INFO: i32 = 4;

    /// Tag passed to logcat; `adb logcat ant-ffi:V *:S` filters on
    /// just our output.
    const LOG_TAG: &str = "ant-ffi";

    // Linked from the NDK's `liblog.so`, which is part of the standard
    // Android system libraries — every NDK toolchain ships it and
    // every Android process can link against it without extra Gradle
    // wiring.
    #[link(name = "log")]
    extern "C" {
        fn __android_log_write(prio: i32, tag: *const c_char, text: *const c_char) -> i32;
    }

    pub struct LogcatWriter;

    impl Write for LogcatWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            // tracing_subscriber::fmt always writes valid UTF-8, but
            // be defensive against any future formatter that chunks
            // mid-codepoint.
            let s = std::str::from_utf8(buf).unwrap_or("(non-utf8 log line)");
            let trimmed = s.trim_end_matches(|c| c == '\r' || c == '\n');
            if !trimmed.is_empty() {
                if let (Ok(tag), Ok(msg)) = (CString::new(LOG_TAG), CString::new(trimmed)) {
                    unsafe { __android_log_write(ANDROID_LOG_INFO, tag.as_ptr(), msg.as_ptr()) };
                }
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    pub struct MakeWriter;

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for MakeWriter {
        type Writer = LogcatWriter;
        fn make_writer(&'a self) -> Self::Writer {
            LogcatWriter
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reference_accepts_bare_hex() {
        let hex = "a".repeat(64);
        match parse_reference(&hex) {
            Ok(ParsedRef::Bytes(addr)) => assert_eq!(addr, [0xaa; 32]),
            other => panic!("expected ParsedRef::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_accepts_0x_prefix() {
        let mut hex = "0x".to_string();
        hex.push_str(&"b".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bytes(addr)) => assert_eq!(addr, [0xbb; 32]),
            other => panic!("expected ParsedRef::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_accepts_bytes_scheme() {
        let hex = format!("bytes://{}", "c".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bytes(addr)) => assert_eq!(addr, [0xcc; 32]),
            other => panic!("expected ParsedRef::Bytes, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_accepts_bzz_with_path() {
        let hex = format!("bzz://{}/index.html", "d".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bzz { root, path }) => {
                assert_eq!(root, [0xdd; 32]);
                assert_eq!(path, "index.html");
            }
            other => panic!("expected ParsedRef::Bzz, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_percent_decodes_bzz_path() {
        let hex = format!("bzz://{}/tracks/02%20butterfly.wav", "e".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bzz { root, path }) => {
                assert_eq!(root, [0xee; 32]);
                assert_eq!(path, "tracks/02 butterfly.wav");
            }
            other => panic!("expected ParsedRef::Bzz, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_decodes_unicode_bzz_path() {
        let hex = format!("bzz://{}/caf%C3%A9/men%C3%BC.txt", "f".repeat(64));
        match parse_reference(&hex) {
            Ok(ParsedRef::Bzz { path, .. }) => {
                assert_eq!(path, "café/menü.txt");
            }
            other => panic!("expected ParsedRef::Bzz, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_rejects_invalid_percent_utf8() {
        let hex = format!("bzz://{}/bad%FFpath", "1".repeat(64));
        match parse_reference(&hex) {
            Err(FfiError::Reference(msg)) => {
                assert!(msg.contains("percent-encoding"), "got {msg:?}");
            }
            other => panic!("expected FfiError::Reference, got {other:?}"),
        }
    }

    #[test]
    fn parse_reference_rejects_bad_length() {
        let err = parse_reference("abc").unwrap_err();
        assert!(matches!(err, FfiError::Reference(_)), "got {err:?}");
    }

    #[test]
    fn generated_identity_round_trips_through_json() {
        let id = new_identity().expect("generate identity");
        let json = serde_json::to_string(&id).expect("serialize");
        let (secret, nonce, kp) = decode_identity_json(&json).expect("decode");
        assert_eq!(hex::encode(secret), id.signing_key);
        assert_eq!(hex::encode(nonce), id.overlay_nonce);
        // The embedded libp2p keypair must be the one derived from the
        // account key, or the node would announce a peer id that doesn't
        // match the overlay it signs handshakes for.
        let derived = secp256k1_keypair_from_signing_secret(&secret).expect("derive keypair");
        assert_eq!(kp.public().to_peer_id(), derived.public().to_peer_id());
    }

    #[test]
    fn generated_identities_are_distinct() {
        let a = new_identity().expect("generate a");
        let b = new_identity().expect("generate b");
        assert_ne!(a.signing_key, b.signing_key);
        assert_ne!(a.overlay_nonce, b.overlay_nonce);
    }

    #[test]
    fn identity_from_key_is_reproducible() {
        let key = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let a = identity_from_signing_key(key).expect("from key");
        let b = identity_from_signing_key(key).expect("from key again");
        assert_eq!(a.signing_key, key);
        // Same key ⇒ same overlay nonce ⇒ same overlay, so restoring on a
        // second device lands the node in the same neighbourhood.
        assert_eq!(a.overlay_nonce, b.overlay_nonce);
        assert_eq!(a.libp2p_keypair, b.libp2p_keypair);
    }

    #[test]
    fn identity_from_key_accepts_0x_prefix_and_whitespace() {
        let key = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
        let bare = identity_from_signing_key(key).expect("bare");
        let prefixed = identity_from_signing_key(&format!("  0x{key}\n")).expect("prefixed");
        assert_eq!(bare.signing_key, prefixed.signing_key);
        assert_eq!(bare.overlay_nonce, prefixed.overlay_nonce);
    }

    #[test]
    fn identity_from_key_rejects_bad_keys() {
        // Too short, not hex, zero, and ≥ the secp256k1 group order.
        for bad in [
            "abcd",
            "zz0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318",
            &"0".repeat(64),
            "fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141",
        ] {
            assert!(
                identity_from_signing_key(bad).is_err(),
                "expected {bad:?} to be rejected"
            );
        }
    }

    #[test]
    fn decode_identity_json_rejects_malformed_documents() {
        assert!(decode_identity_json("not json").is_err());
        // Valid JSON, but the nonce isn't 32 bytes of hex.
        let id = new_identity().expect("generate identity");
        let bad = format!(
            r#"{{"signing_key":"{}","overlay_nonce":"beef","libp2p_keypair":null}}"#,
            id.signing_key
        );
        assert!(decode_identity_json(&bad).is_err());
    }

    #[test]
    fn data_dir_identity_is_written_once_and_reused() {
        let dir = std::env::temp_dir().join(format!(
            "ant-ffi-identity-datadir-{}-{:p}",
            std::process::id(),
            &0u8
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let id_path = dir.join("identity.json");

        let (first, nonce, _) = load_or_create_identity(&id_path).expect("create");
        assert!(id_path.exists(), "first call must persist identity.json");
        let (second, nonce2, _) = load_or_create_identity(&id_path).expect("reload");
        assert_eq!(first, second, "reload must return the same account key");
        assert_eq!(nonce, nonce2);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn provided_identity_leaves_no_key_on_disk() {
        // The whole point of the host-held path: nothing key-shaped may
        // land in the data dir when the embedder supplies the identity.
        let dir = std::env::temp_dir().join(format!(
            "ant-ffi-identity-provided-{}-{:p}",
            std::process::id(),
            &0u8
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");

        let id = new_identity().expect("generate identity");
        let json = serde_json::to_string(&id).expect("serialize");
        let (secret, ..) = decode_identity_json(&json).expect("decode provided identity");
        assert_eq!(hex::encode(secret), id.signing_key);

        let entries: Vec<_> = std::fs::read_dir(&dir)
            .expect("read dir")
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();
        assert!(
            entries.is_empty(),
            "expected an empty data dir, got {entries:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A data dir seeded with one batch store, a chequebook association
    /// and both SWAP ledgers — the state a working account leaves behind.
    fn seed_account_state(dir: &Path) {
        std::fs::create_dir_all(dir.join("postage")).expect("create postage dir");
        std::fs::write(dir.join("postage").join("batch.bin"), b"batch").expect("write batch");
        std::fs::create_dir_all(dir.join("uploads")).expect("create uploads dir");
        std::fs::write(dir.join("chequebook.json"), b"{}").expect("write chequebook");
        std::fs::write(dir.join("swap_credits.json"), b"{}").expect("write credits");
        std::fs::write(dir.join("pushsync_outbound.json"), b"{}").expect("write outbound");
    }

    fn scratch_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("ant-ffi-{tag}-{}-{:p}", std::process::id(), &0u8));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn account_switch_parks_the_previous_accounts_state() {
        let dir = scratch_dir("account-switch");
        let a = [0xaau8; 20];
        let b = [0xbbu8; 20];

        // Account A runs once and leaves a plan + chequebook behind.
        bind_account_state(&dir, &a).expect("bind A");
        seed_account_state(&dir);

        // Account B is restored onto the same device. None of A's state
        // may still be at the canonical paths B's node reads.
        bind_account_state(&dir, &b).expect("bind B");
        for name in ACCOUNT_SCOPED_ENTRIES {
            assert!(
                !dir.join(name).exists(),
                "{name} must not be visible to the new account",
            );
        }
        // Parked, not destroyed: A can still be restored.
        let parked = dir
            .join(ACCOUNT_PARK_DIR)
            .join(format!("0x{}", hex::encode(a)));
        assert!(parked.join("postage").join("batch.bin").exists());
        assert!(parked.join("chequebook.json").exists());

        // Switching back hands A its own state again, and parks B's.
        seed_account_state(&dir);
        bind_account_state(&dir, &a).expect("bind A again");
        assert!(dir.join("postage").join("batch.bin").exists());
        assert!(dir.join("chequebook.json").exists());
        assert!(!parked.exists(), "A's park dir is emptied on adopt");
        assert!(dir
            .join(ACCOUNT_PARK_DIR)
            .join(format!("0x{}", hex::encode(b)))
            .join("chequebook.json")
            .exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_restored_account_does_not_inherit_the_previous_plan() {
        // The path the node actually takes at startup, with a real
        // postage store: account A's batch must not be reloaded into
        // account B's issuer registry (B's key would sign stamps over a
        // batch A owns, and every peer would drop them), and must come
        // back intact when A is restored.
        let dir = scratch_dir("account-issuers");
        let (a, b) = ([0x44u8; 20], [0x55u8; 20]);
        let batch = [0x66u8; 32];

        bind_account_state(&dir, &a).expect("bind A");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).expect("create postage dir");
        drop(
            ant_postage::StampIssuer::open_or_new(
                postage.join(format!("{}.bin", hex::encode(batch))),
                batch,
                20,
                16,
                false,
            )
            .expect("create batch store"),
        );
        assert!(drive::reload_persisted_issuers(&postage).contains_key(&batch));

        bind_account_state(&dir, &b).expect("bind B");
        assert!(
            drive::reload_persisted_issuers(&postage).is_empty(),
            "the restored account must not stamp against the previous account's batch",
        );

        bind_account_state(&dir, &a).expect("bind A again");
        assert!(
            drive::reload_persisted_issuers(&postage).contains_key(&batch),
            "restoring the original account must return its plan",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn same_account_restart_leaves_state_in_place() {
        let dir = scratch_dir("account-restart");
        let a = [0x11u8; 20];
        bind_account_state(&dir, &a).expect("first start");
        seed_account_state(&dir);
        bind_account_state(&dir, &a).expect("restart");
        assert!(dir.join("postage").join("batch.bin").exists());
        assert!(dir.join("chequebook.json").exists());
        assert!(
            !dir.join(ACCOUNT_PARK_DIR).exists(),
            "a plain restart must not move anything",
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_data_dir_without_a_marker_keeps_its_state() {
        // Upgrade path: state written before the marker existed belongs
        // to the account starting now (that build had no way to change
        // the key under a fixed data dir).
        let dir = scratch_dir("account-legacy");
        seed_account_state(&dir);
        bind_account_state(&dir, &[0x22u8; 20]).expect("adopt legacy state");
        assert!(dir.join("postage").join("batch.bin").exists());
        assert!(dir.join(ACCOUNT_MARKER_FILE).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_marker_parks_state_as_unattributable() {
        let dir = scratch_dir("account-corrupt");
        seed_account_state(&dir);
        std::fs::write(dir.join(ACCOUNT_MARKER_FILE), b"{ truncated").expect("corrupt marker");
        bind_account_state(&dir, &[0x33u8; 20]).expect("bind over corrupt marker");
        assert!(
            !dir.join("chequebook.json").exists(),
            "state we can't attribute must not be adopted",
        );
        assert!(dir
            .join(ACCOUNT_PARK_DIR)
            .join(UNKNOWN_ACCOUNT)
            .join("chequebook.json")
            .exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_marker_that_cannot_be_read_fails_the_start_instead_of_parking() {
        // A read *failure* (as opposed to a marker whose contents are
        // corrupt) says nothing about who the state belongs to. Parking
        // it as unattributable would be one-way: adopt only ever looks
        // at `accounts/<own address>`, so the account's own plan and
        // chequebook would never come back, and the next buy would
        // deploy a second chequebook. Fail the start; the next one can
        // read the marker and comes up intact.
        let dir = scratch_dir("account-unreadable");
        seed_account_state(&dir);
        // A directory where the marker belongs: `read` fails with
        // EISDIR, not NotFound, on every platform we ship.
        std::fs::create_dir(dir.join(ACCOUNT_MARKER_FILE)).expect("marker as a dir");

        let err = bind_account_state(&dir, &[0x77u8; 20]).expect_err("must not succeed");
        assert!(
            err.to_string().contains("account marker"),
            "unhelpful error: {err}",
        );
        assert!(
            dir.join("chequebook.json").exists() && dir.join("postage").join("batch.bin").exists(),
            "the account's own state must stay where it is",
        );
        assert!(
            !dir.join(ACCOUNT_PARK_DIR).exists(),
            "nothing may be parked on an unreadable marker",
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A handle over `runtime` with everything else inert — enough for
    /// the [`ant_shutdown`] contract, which only touches the runtime.
    fn test_handle(runtime: Runtime, data_dir: &Path) -> AntHandle {
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let (_status_tx, status_rx) = watch::channel(StatusSnapshot::default());
        AntHandle {
            runtime,
            cmd_tx,
            status_rx,
            progress: Mutex::new(DownloadProgressState::default()),
            cancel_flag: AtomicBool::new(false),
            cancel_notify: Notify::new(),
            verify_cancel: Arc::new(AtomicBool::new(false)),
            signing_secret: [0u8; SECP256K1_SECRET_LEN],
            eth: [0u8; 20],
            data_dir: data_dir.to_path_buf(),
            gateway_task: Mutex::new(None),
            bench: Mutex::new(None),
            #[cfg(feature = "chain")]
            chain_transport: Mutex::new(None),
        }
    }

    #[test]
    fn shutdown_does_not_return_while_a_task_is_still_persisting() {
        // Restore is `ant_shutdown(A)` then `ant_init(B)` over one data
        // dir. If a task of A's is still inside its checkpoint persist
        // (`create_dir_all` + write + rename) when shutdown returns, it
        // recreates A's canonical files *after* B's `bind_account_state`
        // parked them: A's state is then attributed to B, and the next
        // switch back to A aborts in `move_account_entries`.
        let dir = scratch_dir("shutdown-drain");
        let checkpoint = dir.join("postage").join("late.bin");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build runtime");
        let target = checkpoint.clone();
        runtime.spawn(async move {
            // Sync from here on, exactly like the real persist: nothing
            // for a cancel to land on until it has finished.
            std::thread::sleep(Duration::from_millis(300));
            std::fs::create_dir_all(target.parent().expect("parent")).expect("create dir");
            std::fs::write(&target, b"late").expect("write checkpoint");
        });
        // Let the task reach the worker before we tear the runtime down.
        std::thread::sleep(Duration::from_millis(100));

        let handle = Box::into_raw(Box::new(test_handle(runtime, &dir)));
        unsafe { ant_shutdown(handle) };

        assert!(
            checkpoint.exists(),
            "ant_shutdown returned while a task was still writing to the data dir",
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Local pipeline mode (no `batch_id`): no gateway, no batch, no
    /// network — the loop runs entirely on the runtime.
    fn bench_config(label: &str, duration_s: u64) -> CString {
        CString::new(format!(
            r#"{{"label":"{label}","duration_s":{duration_s},"warmup_s":0,"segment_ms":200,"bitrate_kbps":64,"max_in_flight":2}}"#
        ))
        .expect("config json")
    }

    fn bench_slot(handle: *const AntHandle) -> Option<Arc<bench::BenchRun>> {
        unsafe { &*handle }
            .bench
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    #[test]
    fn bench_stop_does_not_clear_a_run_it_did_not_stop() {
        // `ant_bench_stop` waits for the cancelled run to settle and then
        // frees the slot. If it frees whatever is *in* the slot rather
        // than the run it stopped, a start that legitimately lands inside
        // that wait (run 1 has settled, so the "already running" check
        // passes) is orphaned: run 2 keeps publishing for its whole
        // duration with nobody holding its handle, and the next start
        // sees an empty slot and adds a second publisher on one uplink.
        let dir = scratch_dir("bench-stop-race");
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build runtime");
        let handle = Box::into_raw(Box::new(test_handle(runtime, &dir)));

        let first = bench_config("race-first", 30);
        assert!(
            unsafe { ant_bench_start(handle, first.as_ptr(), std::ptr::null_mut()) },
            "first start",
        );
        let run1 = bench_slot(handle).expect("run 1 in the slot");

        // The stopper cancels run 1 and then sits in its settle poll.
        let stopper = {
            let addr = handle as usize;
            std::thread::spawn(move || {
                let report =
                    unsafe { ant_bench_stop(addr as *const AntHandle, std::ptr::null_mut()) };
                assert!(!report.is_null(), "stop must return run 1's report");
                unsafe { ant_free_string(report) };
            })
        };
        // Give it time to take its clone of run 1 and cancel it. Run 1
        // only settles at its next 200 ms segment boundary, so we are
        // polling below well before it does.
        std::thread::sleep(Duration::from_millis(100));

        // The moment run 1 has settled, a second start is legitimate —
        // and it lands inside the stopper's poll window.
        while !run1.is_finished() {
            std::thread::sleep(Duration::from_millis(1));
        }
        let second = bench_config("race-second", 30);
        assert!(
            unsafe { ant_bench_start(handle, second.as_ptr(), std::ptr::null_mut()) },
            "second start once run 1 settled",
        );
        let run2 = bench_slot(handle).expect("run 2 in the slot");
        assert!(!Arc::ptr_eq(&run1, &run2), "run 2 must be a fresh run");

        stopper.join().expect("stopper thread");

        assert!(
            bench_slot(handle).is_some_and(|cur| Arc::ptr_eq(&cur, &run2)),
            "stop released the slot of a run it never stopped: run 2 is orphaned",
        );
        let third = bench_config("race-third", 30);
        let mut err: *mut c_char = std::ptr::null_mut();
        let admitted = unsafe { ant_bench_start(handle, third.as_ptr(), &raw mut err) };
        if !err.is_null() {
            unsafe { ant_free_string(err) };
        }
        assert!(
            !admitted,
            "a third start was admitted while run 2 is still publishing",
        );

        run2.cancel();
        while !run2.is_finished() {
            std::thread::sleep(Duration::from_millis(1));
        }
        unsafe { ant_shutdown(handle) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ParsedRef doesn't derive Debug because [u8; 32] wouldn't print
    // usefully; provide it locally for the test diagnostics above.
    impl std::fmt::Debug for ParsedRef {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Bytes(root) => write!(f, "Bytes(0x{})", hex::encode(root)),
                Self::Bzz { root, path } => {
                    write!(f, "Bzz(0x{}, {path:?})", hex::encode(root))
                }
            }
        }
    }
}
