//! Upload, storage-plan, and account helpers backing the `AntDrive` demo
//! app's FFI surface.
//!
//! The download/stream entry points in [`crate`] cover the "read" half
//! of a Swarm light node; this module covers the "write" + "account"
//! half `AntDrive` needs:
//!
//! * **Uploads** drive the same daemon-resident [`UploadManager`] jobs
//!   `antctl upload` uses (start / list / status / pause / resume /
//!   cancel) through the node's control-command channel. The actual
//!   chunking + postage stamping + pushsync happens on the manager's
//!   own task; these helpers only kick jobs off and read their state.
//! * **Storage plan** maps the node's local postage-stamp issuer to a
//!   Dropbox-style "how much room do I have" view (`PostageStatus`).
//!   With the `chain` feature, `connect_batch` / `discover` read the
//!   batch the account owns on Gnosis and register it so uploads can
//!   stamp against it.
//! * **Account** exposes the node identity (address / overlay / peer
//!   id) and the raw signing key for a "back up your account" flow.
//!
//! Everything returns a JSON string (or a plain string / error) so the
//! Swift side decodes one shape per call instead of marshalling a
//! length-prefixed array of variable-size C structs.

use ant_control::{ControlAck, ControlCommand};
use ant_postage::StampIssuer;
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
#[cfg(feature = "chain")]
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

use crate::AntHandle;

/// Upper bound on a single control round-trip. Upload start / list /
/// status / postage reads are all local daemon ops that complete in
/// milliseconds; the timeout only guards against a wedged node loop.
const OP_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound for a propagation check. Unlike the other control ops this
/// is a network operation — it fetches every interior node plus a large
/// sample of data leaves over the live network — so it legitimately runs
/// for many seconds on a big file. A generous ceiling keeps it from being
/// cut short (which surfaced as a missing verdict / silent failure) while
/// still guarding against a wedged loop. Raised now that a full check is
/// uncapped (every leaf, however large the file); the user can also cancel
/// a long check from the detail view, so a high ceiling is safe.
const VERIFY_TIMEOUT: Duration = Duration::from_mins(30);

#[derive(Debug, thiserror::Error)]
pub(crate) enum DriveError {
    #[error("{0}")]
    Op(String),
}

/// Reload every postage batch persisted under `<data_dir>/postage/*.bin`
/// into a fresh issuer registry, so a user's storage plan survives an
/// app restart. Mirrors `antd`'s startup reload. Unreadable stores are
/// skipped with a warning rather than failing the whole node bring-up.
///
/// Init has no RPC, so the batches are registered *unconfirmed*; with
/// the `chain` feature, [`ChainInit::run`] drops the ones the chain
/// disowns once the host supplies one ([`crate::ant_start_gateway`]).
///
/// A store records the batch but not its owner, so this cannot tell a
/// batch *this* account paid for from one the previous account did —
/// stamping the latter with the current key produces stamps every peer
/// rejects. Ownership is enforced one level up instead:
/// [`crate::bind_account_state`] parks the whole `postage` directory
/// with the account that wrote it, so by the time this runs the
/// directory only holds the running account's batches.
pub(crate) fn reload_persisted_issuers(
    postage_dir: &std::path::Path,
) -> HashMap<[u8; 32], StampIssuer> {
    let mut issuers = HashMap::new();
    if !postage_dir.is_dir() {
        return issuers;
    }
    let entries = match std::fs::read_dir(postage_dir) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(target: "ant-ffi", dir = %postage_dir.display(), "scan postage dir: {e}");
            return issuers;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("bin") {
            continue;
        }
        match StampIssuer::open_existing(path.clone()) {
            Ok(iss) => {
                let id = *iss.batch_id();
                tracing::info!(
                    target: "ant-ffi",
                    batch = %format!("0x{}", hex::encode(id)),
                    "reloaded persisted postage batch",
                );
                issuers.insert(id, iss);
            }
            Err(e) => tracing::warn!(
                target: "ant-ffi",
                store = %path.display(),
                "skipping unreadable postage store: {e}",
            ),
        }
    }
    issuers
}

/// ant-ffi's counterpart of `antd`'s startup chain block, run by
/// [`crate::ant_start_gateway`] once the host supplies an RPC.
///
/// `antd` does all of its chain-derived startup in one place before
/// the node gets its upload wiring: confirm the reloaded batches
/// (issue #49), rediscover owned batches (step 3), and resolve the
/// chequebook. `ant_init` takes no RPC (on iOS the Gnosis endpoint
/// only arrives with [`crate::ant_start_gateway`]), so until [`Self::run`]
/// runs:
///
/// - a batch that expired or was never created reads as `usable` in
///   `GET /stamps`;
/// - a batch the account owns on-chain but not on disk (reinstall,
///   restore from key) is missing;
/// - a chequebook that exists only on-chain isn't used for settlement.
///
/// The decisions come from the same shared helpers `antd` uses, so the
/// two entry points can't disagree on them (see
/// `docs/ffi-parity-audit.md`).
#[cfg(feature = "chain")]
pub(crate) struct ChainInit {
    upload: std::sync::Arc<ant_p2p::UploadRuntime>,
    /// Reloaded batches not yet confirmed on-chain.
    unverified: std::sync::Mutex<std::collections::BTreeSet<[u8; 32]>>,
    /// Holds `true` once an owned-batch rediscovery scan has completed
    /// and registered every batch it found, so a gateway restart in the
    /// same process doesn't rescan. A failed scan or a failed
    /// registration leaves it `false`, so the next run retries. Held
    /// across the scan, so two overlapping runs (an idempotent
    /// `ant_start_gateway` re-call while the first run is in flight)
    /// don't both scan and double-register the same batch: the second
    /// waits and then sees the flag.
    batches_rediscovered: tokio::sync::Mutex<bool>,
    /// The background retry of a failed rediscovery
    /// ([`Self::retry_rediscovery`]): at most one per handle, however
    /// often the host re-calls `ant_start_gateway`.
    rediscovery_retry: std::sync::Mutex<RediscoveryRetry>,
    /// The rediscovery read part of the history only from the unverified
    /// source ([`crate::ant_set_unverified_logs_rpc`]), and that isn't
    /// confirmed yet ([`Self::confirm_unverified`]).
    unconfirmed: std::sync::atomic::AtomicBool,
    /// A background confirmation is running: at most one per handle.
    confirming: std::sync::atomic::AtomicBool,
    /// Set once settlement has been switched on for an adopted
    /// chequebook, so a later run (every idempotent `ant_start_gateway`
    /// re-call spawns one) doesn't re-resolve it over RPC.
    settlement_on: std::sync::atomic::AtomicBool,
    /// Serializes [`Self::verify_pass`] sweeps: every
    /// `ant_start_gateway` call with an RPC spawns one (including the
    /// idempotent "already running" path), so a host re-calling it on
    /// foreground can overlap a pass still in flight. The second waits
    /// and then only sees what the first left pending.
    pass: tokio::sync::Mutex<()>,
    /// Batches a read reported `NotFound` for, and when it first did.
    /// One such read isn't believed on its own: a batch bought seconds
    /// before an app relaunch was confirmed by one RPC backend, and a
    /// load-balanced sibling that hasn't seen the `BatchCreated` block
    /// yet reads `batchOwner` as zero for it — the same signature as a
    /// dead batch. It is unregistered only when a read at least
    /// [`Self::not_found_grace`] later still says so (the rule
    /// `ant-gateway`'s `/stamps` applies to a just-registered batch).
    not_found_since: std::sync::Mutex<HashMap<[u8; 32], tokio::time::Instant>>,
    not_found_grace: Duration,
}

/// [`ChainInit::rediscovery_retry`]: whether the loop runs, and the chain
/// client its next attempt reads through — the latest gateway start's,
/// or `None` once the gateway stopped, which ends the loop — and how
/// many times the gateway has stopped. A start captures that count
/// ([`ChainInit::retry_epoch`]) before its first attempt, and only
/// starts or feeds the loop if no stop landed since: a stop during the
/// first attempt (no loop yet to end) must not be undone by that
/// attempt failing afterwards.
#[cfg(feature = "chain")]
#[derive(Default)]
struct RediscoveryRetry {
    running: bool,
    chain: Option<ant_chain::ChainClient>,
    stops: u64,
}

/// How long a persisted batch's first `NotFound` read must stand before
/// a second one unregisters it — see
/// [`ChainInit::not_found_since`]. A lagging load-balanced
/// backend trails by a few blocks (5 s each on Gnosis), so 45 s covers
/// it. Deliberately much shorter than `ant-gateway`'s five-minute
/// `FRESH_BATCH_GRACE`: the suspect clock lives only in memory, so a
/// grace longer than a typical mobile session (an iOS app foregrounded
/// for a couple of minutes, then suspended or killed) would restart on
/// every launch and never let a dead batch be unregistered.
#[cfg(feature = "chain")]
const PERSISTED_NOT_FOUND_GRACE: Duration = Duration::from_secs(45);

#[cfg(feature = "chain")]
impl ChainInit {
    /// Track every batch currently registered in `upload` — call it
    /// right after the reload, before anything registers at runtime, so
    /// only batches that came from disk are checked (a batch bought this
    /// session was just confirmed by its own buy).
    pub(crate) fn new(upload: std::sync::Arc<ant_p2p::UploadRuntime>) -> Self {
        Self::with_not_found_grace(upload, PERSISTED_NOT_FOUND_GRACE)
    }

    pub(crate) fn with_not_found_grace(
        upload: std::sync::Arc<ant_p2p::UploadRuntime>,
        not_found_grace: Duration,
    ) -> Self {
        let unverified = upload
            .issuers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect();
        Self {
            upload,
            unverified: std::sync::Mutex::new(unverified),
            batches_rediscovered: tokio::sync::Mutex::new(false),
            rediscovery_retry: std::sync::Mutex::new(RediscoveryRetry::default()),
            unconfirmed: std::sync::atomic::AtomicBool::new(false),
            confirming: std::sync::atomic::AtomicBool::new(false),
            settlement_on: std::sync::atomic::AtomicBool::new(false),
            pass: tokio::sync::Mutex::new(()),
            not_found_since: std::sync::Mutex::new(HashMap::new()),
            not_found_grace,
        }
    }

    /// Run the chain init: the ant-ffi equivalent of `antd`'s startup
    /// chain block. Each step is best-effort and independent; a failed
    /// step logs and is retried by the next `ant_start_gateway` call
    /// with an RPC — including an idempotent one that finds the gateway
    /// already running, which spawns this again. Overlapping runs are
    /// safe: step 1 is serialized per sweep, step 2 holds its own lock
    /// across the scan, step 3 takes the process-wide chequebook setup
    /// lock.
    ///
    /// 1. Unregister reloaded batches the chain disowns (#49). A first
    ///    `NotFound` read only marks the batch suspect; it is re-read
    ///    once the grace window has passed, after steps 2 and 3, so the
    ///    wait never delays them.
    /// 2. Register batches this account owns on-chain but not on disk.
    /// 3. Adopt the persisted or on-chain chequebook and switch outbound
    ///    settlement on. Nothing is deployed or funded here: both spend
    ///    the user's funds, which only an explicit host call
    ///    ([`deploy_chequebook`]) or a storage buy may do.
    ///
    /// Returns the chequebook this run switched settlement on for, if
    /// any; [`Self::run_reporting`] hands it over as soon as step 3
    /// ends instead of after step 1's recheck wait.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn run(
        &self,
        chain: &ant_chain::ChainClient,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        data_dir: &std::path::Path,
        swap_secret: [u8; 32],
    ) -> Option<[u8; 20]> {
        let mut adopted = None;
        self.run_reporting(chain, cmd_tx, data_dir, swap_secret, None, |cb| {
            adopted = cb;
        })
        .await;
        adopted
    }

    /// [`Self::run`], calling `report` with step 3's outcome (the
    /// chequebook settlement was switched on for, or `None`) right when
    /// it's known, so the gateway's chequebook slot doesn't wait out a
    /// batch recheck. With `retry` (the gateway start's
    /// [`Self::retry_epoch`], taken when it started), a failed step 2 is
    /// retried in the background with backoff
    /// ([`Self::retry_rediscovery`]) alongside step 1's recheck, as `antd`
    /// does, so `/health.walletScan` moves on from `retrying` without
    /// waiting for the host's next start call — unless the gateway has
    /// stopped since; and a rediscovery that read part of the history only
    /// from the unverified source is confirmed afterwards
    /// ([`Self::confirm_unverified`]), calling `report` again with the
    /// chequebook that adopts, if any.
    pub(crate) async fn run_reporting(
        &self,
        chain: &ant_chain::ChainClient,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        data_dir: &std::path::Path,
        swap_secret: [u8; 32],
        retry: Option<u64>,
        mut report: impl FnMut(Option<[u8; 20]>),
    ) {
        let recheck_at = self
            .verify_pass(chain, ant_chain::GNOSIS_POSTAGE_STAMP)
            .await;
        let rediscovered = self.rediscover_owned(chain, cmd_tx, data_dir).await;
        report(
            self.adopt_settlement(chain, cmd_tx, data_dir, swap_secret)
                .await,
        );
        let recheck = async {
            if let Some(recheck_at) = recheck_at {
                tokio::time::sleep_until(recheck_at).await;
                self.verify_pass(chain, ant_chain::GNOSIS_POSTAGE_STAMP)
                    .await;
            }
        };
        let follow_up = async {
            let Some(epoch) = retry else {
                return;
            };
            // The confirmation reads through the RPC the rediscovery last
            // succeeded with: a retry loop's success may have come through
            // a later start's.
            let mut chain = chain.clone();
            if !rediscovered {
                if let Some(latest) = self
                    .retry_rediscovery(&chain, cmd_tx, data_dir, epoch)
                    .await
                {
                    chain = latest;
                }
            }
            if self.unconfirmed.load(std::sync::atomic::Ordering::Acquire) {
                if let Some(chequebook) = self
                    .confirm_unverified(&chain, cmd_tx, data_dir, swap_secret)
                    .await
                {
                    report(Some(chequebook));
                }
            }
        };
        tokio::join!(recheck, follow_up);
    }

    /// Report a rediscovery that will run (`/health.walletScan` =
    /// `pending`) unless one already finished for this handle (then
    /// `done`, or `confirming` while part of the history it read from the
    /// unverified source isn't confirmed yet). Called before the gateway serves, so a host never reads
    /// the chain as ready with no rediscovery reported. A run in flight
    /// holds the flag's lock and reports for itself — unless a gateway
    /// stop dropped its status ([`Self::stop_retrying`]), so an
    /// untracked status is announced as `pending` then too; that run
    /// turns it back into `scanning`, with its progress, at its next
    /// scan window.
    pub(crate) fn note_pending(&self) {
        use ant_chain::discover::{wallet_scan_pending, wallet_scan_track, WalletScanState};
        let owner = &self.upload.batch_owner;
        match self.batches_rediscovered.try_lock() {
            // Rediscovered, but part of the history isn't confirmed yet.
            Ok(done) if *done && self.unconfirmed.load(std::sync::atomic::Ordering::Acquire) => {
                wallet_scan_track(owner, WalletScanState::Confirming);
            }
            Ok(done) if *done => wallet_scan_track(owner, WalletScanState::Done),
            Ok(_) => wallet_scan_pending(owner),
            Err(_) => wallet_scan_track(owner, WalletScanState::Pending),
        }
    }

    /// Retry a failed [`Self::rediscover_owned`] with the shared backoff
    /// until it succeeds. A scan that failed kept its progress, so each
    /// retry only reads what's left. One loop per handle: a call while it
    /// runs only hands it `chain`, so its next attempt reads through the
    /// latest gateway start's RPC. [`Self::stop_retrying`] (the gateway
    /// stopped) ends it before its next attempt. `epoch` is the
    /// [`Self::retry_epoch`] the calling start took before its own
    /// attempt: if the gateway stopped since, this does nothing — no new
    /// loop, and no stopped gateway's RPC handed to a running one.
    ///
    /// Returns the chain client the loop's successful attempt read
    /// through, when this call ran the loop and it succeeded (`None` when
    /// it handed `chain` to a running loop, did nothing, or was stopped).
    async fn retry_rediscovery(
        &self,
        chain: &ant_chain::ChainClient,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        data_dir: &std::path::Path,
        epoch: u64,
    ) -> Option<ant_chain::ChainClient> {
        {
            let mut retry = self
                .rediscovery_retry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if retry.stops != epoch {
                return None;
            }
            retry.chain = Some(chain.clone());
            if retry.running {
                return None;
            }
            retry.running = true;
        }
        let mut succeeded = None;
        for attempt in 0u32.. {
            let delay = ant_chain::discover::rediscovery_retry_delay(attempt);
            tokio::time::sleep(delay).await;
            let chain = {
                let mut retry = self
                    .rediscovery_retry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(chain) = retry.chain.clone() else {
                    retry.running = false;
                    return None;
                };
                chain
            };
            if self.rediscover_owned(&chain, cmd_tx, data_dir).await {
                succeeded = Some(chain);
                break;
            }
        }
        let mut retry = self
            .rediscovery_retry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retry.running = false;
        retry.chain = None;
        succeeded
    }

    /// The gateway stop count a start passes to [`Self::run_reporting`]:
    /// take it before the start's first rediscovery attempt (before
    /// spawning it), so a stop landing at any point after it keeps that
    /// attempt's failure from starting a retry loop.
    pub(crate) fn retry_epoch(&self) -> u64 {
        self.rediscovery_retry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stops
    }

    /// Test hook: hold the retry state's lock, so a concurrent
    /// [`Self::stop_retrying`] blocks until the guard drops.
    #[cfg(test)]
    pub(crate) fn hold_retry_lock(&self) -> impl Sized + '_ {
        self.rediscovery_retry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// End the background rediscovery retry before its next attempt: the
    /// gateway stopped (`ant_stop_gateway`), so nothing reads its status,
    /// and its RPC may be one the host is replacing. The next start with
    /// an RPC runs its own attempt and, if that fails, a new loop.
    ///
    /// An unfinished `/health.walletScan` status is dropped with it: a
    /// next start *without* an RPC runs no rediscovery and must not serve
    /// a frozen `retrying`/`scanning` nothing will move on; a next start
    /// with one announces `pending` again ([`Self::note_pending`]).
    pub(crate) fn stop_retrying(&self) {
        let mut retry = self
            .rediscovery_retry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retry.chain = None;
        retry.stops += 1;
        ant_chain::discover::wallet_scan_abandon(&self.upload.batch_owner);
    }

    /// Step 3 of [`Self::run`]: adopt the chequebook without spending,
    /// once per handle. Returns the chequebook this call switched
    /// settlement on for (`None` when an earlier run already did, or
    /// there's none to use).
    async fn adopt_settlement(
        &self,
        chain: &ant_chain::ChainClient,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        data_dir: &std::path::Path,
        swap_secret: [u8; 32],
    ) -> Option<[u8; 20]> {
        use std::sync::atomic::Ordering;

        if self.settlement_on.load(Ordering::Acquire) {
            return None;
        }
        let node_eth = self.upload.batch_owner;
        let wallet = match ant_chain::tx::Wallet::new(swap_secret, GNOSIS_CHAIN_ID) {
            Ok(wallet) => wallet,
            Err(e) => {
                tracing::warn!(target: "ant-ffi", "settlement skipped (wallet init): {e}");
                return None;
            }
        };
        match setup_settlement(
            cmd_tx,
            chain,
            &wallet,
            data_dir,
            swap_secret,
            node_eth,
            false,
        )
        .await
        {
            Ok(Some(chequebook)) => {
                self.settlement_on.store(true, Ordering::Release);
                Some(chequebook)
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    target: "ant-ffi",
                    "could not adopt a chequebook for network settlement: {e}",
                );
                None
            }
        }
    }

    /// `antd` step 3: register every funded batch this account owns
    /// on-chain that isn't registered yet (a reinstall or restore from
    /// key leaves them on-chain but not on disk). Uses the same saved
    /// transfer scan as `antd` and `ant_storage_discover`, so only the
    /// first start reads the full history (#118); `ant_storage_discover_full`
    /// is the way to read it all again. A new issuer starts at
    /// index 0, as in `antd` without a bee `stamperstore`. Returns whether
    /// the rediscovery is complete for this handle, and reports it to
    /// `/health.walletScan`.
    async fn rediscover_owned(
        &self,
        chain: &ant_chain::ChainClient,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        data_dir: &std::path::Path,
    ) -> bool {
        let owner = self.upload.batch_owner;
        let mut rediscovered = self.batches_rediscovered.lock().await;
        if *rediscovered {
            // A `pending` announced while an attempt that then succeeded
            // held the lock (see `note_pending`) ends here.
            ant_chain::discover::wallet_scan_done(&owner);
            return true;
        }
        let found = match owned_batches(chain, &owner, data_dir, false, Some(&owner)).await {
            Ok((found, unconfirmed)) => {
                self.unconfirmed
                    .store(unconfirmed, std::sync::atomic::Ordering::Release);
                found
            }
            Err(e) => {
                tracing::warn!(
                    target: "ant-ffi",
                    "postage batch rediscovery scan failed: {e}; retrying",
                );
                ant_chain::discover::wallet_scan_failed(&owner, &e.to_string());
                return false;
            }
        };
        let all_registered = self.register_found(cmd_tx, found).await;
        // Only a clean pass ends the rediscovery: a batch whose
        // registration failed is picked up by the next run's scan (the
        // ones registered now are skipped as known).
        *rediscovered = all_registered;
        if all_registered {
            ant_chain::discover::wallet_scan_done(&owner);
        } else {
            ant_chain::discover::wallet_scan_failed(
                &owner,
                "could not register a rediscovered batch",
            );
        }
        all_registered
    }

    /// Register the batches in `found` this handle doesn't hold yet.
    /// Returns whether every one of them registered.
    async fn register_found(
        &self,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        found: Vec<ant_chain::discover::DiscoveredBatch>,
    ) -> bool {
        let mut all_registered = true;
        for b in found {
            let known = self
                .upload
                .issuers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&b.batch_id);
            if known {
                continue;
            }
            match register_batch(
                cmd_tx,
                b.batch_id,
                b.depth,
                b.bucket_depth,
                b.immutable,
                None,
            )
            .await
            {
                Ok(()) => tracing::info!(
                    target: "ant-ffi",
                    batch = %format!("0x{}", hex::encode(b.batch_id)),
                    depth = b.depth,
                    bucket_depth = b.bucket_depth,
                    immutable = b.immutable,
                    remaining_balance = b.remaining_balance,
                    "rediscovered owned postage batch from chain",
                ),
                Err(e) => {
                    all_registered = false;
                    tracing::warn!(
                        target: "ant-ffi",
                        batch = %format!("0x{}", hex::encode(b.batch_id)),
                        "could not register rediscovered batch: {e}; retrying",
                    );
                }
            }
        }
        all_registered
    }

    /// Confirm a rediscovery that read part of the history only from the
    /// unverified source: [`ant_chain::discover::confirm_transfer_scan`]
    /// through the verified transport, at most 64 windows per try (one
    /// request once the transport serves the whole span) with the shared
    /// backoff, never a crawl — as `antd` does. Once it's
    /// confirmed, register any batch the unverified read missed, move
    /// `/health.walletScan` to `done`, and adopt the chequebook if
    /// settlement isn't on yet (nothing is deployed here, as in
    /// [`Self::run`]). Returns the chequebook adopted, if any. One loop
    /// per handle.
    async fn confirm_unverified(
        &self,
        chain: &ant_chain::ChainClient,
        cmd_tx: &mpsc::Sender<ControlCommand>,
        data_dir: &std::path::Path,
        swap_secret: [u8; 32],
    ) -> Option<[u8; 20]> {
        use std::sync::atomic::Ordering;
        if self.confirming.swap(true, Ordering::AcqRel) {
            return None;
        }
        let owner = self.upload.batch_owner;
        let mut adopted = None;
        for attempt in 0u32.. {
            tokio::time::sleep(ant_chain::discover::confirm_retry_delay(attempt)).await;
            let scan = match ant_chain::discover::confirm_transfer_scan(
                chain,
                ant_chain::GNOSIS_BZZ_TOKEN,
                &owner,
                data_dir,
                &owner,
            )
            .await
            {
                Ok(Some(scan)) => scan,
                Ok(None) => {
                    tracing::info!(
                        target: "ant-ffi",
                        "the transport can't confirm the wallet history read from the unverified \
                         source yet; trying again",
                    );
                    continue;
                }
                Err(e) => {
                    tracing::warn!(
                        target: "ant-ffi",
                        "confirming the wallet history failed: {e}; trying again",
                    );
                    continue;
                }
            };
            let found = match ant_chain::discover::owned_batches_in(
                chain,
                ant_chain::GNOSIS_POSTAGE_STAMP,
                &scan,
            )
            .await
            {
                Ok(found) => found,
                Err(e) => {
                    tracing::warn!(
                        target: "ant-ffi",
                        "reading the confirmed wallet history failed: {e}; trying again",
                    );
                    continue;
                }
            };
            if !self.register_found(cmd_tx, found).await {
                continue;
            }
            self.unconfirmed.store(false, Ordering::Release);
            ant_chain::discover::wallet_scan_done(&owner);
            tracing::info!(
                target: "ant-ffi",
                "the wallet history read from the unverified source is confirmed",
            );
            adopted = self
                .adopt_settlement(chain, cmd_tx, data_dir, swap_secret)
                .await;
            break;
        }
        self.confirming.store(false, Ordering::Release);
        adopted
    }

    /// Confirm every still-unverified reloaded batch against the chain
    /// and **unregister** the ones it disowns — `NotFound` (evicted or
    /// never created), `Expired` (still ours but `remainingBalance` 0)
    /// and `ForeignOwner` — with a loud warning. An RPC read error keeps
    /// the batch registered (unconfirmed ≠ dead) and leaves it pending,
    /// so the next `ant_start_gateway` call with an RPC retries it —
    /// including one that finds the gateway already running. The
    /// `.bin` / `.stamps` files stay on disk, as in `antd`: a later
    /// re-buy or re-sync recovers them, and the logged id lets the user
    /// clean up.
    ///
    /// A first `NotFound` only marks the batch suspect (it stays
    /// registered and pending — see [`Self::not_found_since`]); this
    /// call then waits out the grace window and reads it once more,
    /// unregistering it only if that read agrees. A suspect whose
    /// re-read fails stays pending for the next `ant_start_gateway`.
    /// [`Self::run`] does the same, but runs its other steps before
    /// waiting out the grace.
    #[cfg(test)]
    async fn verify_persisted(&self, chain: &ant_chain::ChainClient, postage_contract: &str) {
        if let Some(recheck_at) = self.verify_pass(chain, postage_contract).await {
            tokio::time::sleep_until(recheck_at).await;
            self.verify_pass(chain, postage_contract).await;
        }
    }

    /// One serialized sweep over the pending batches (the `pass` lock is
    /// held only for the sweep, never across the grace wait). Returns
    /// when the last `NotFound` suspect it left pending becomes due for
    /// its confirming re-read, if any.
    async fn verify_pass(
        &self,
        chain: &ant_chain::ChainClient,
        postage_contract: &str,
    ) -> Option<tokio::time::Instant> {
        use ant_chain::discover::PersistedBatchVerdict;

        let _pass = self.pass.lock().await;
        let mut recheck_at: Option<tokio::time::Instant> = None;
        let pending: Vec<[u8; 32]> = self.lock_unverified().iter().copied().collect();
        let our_owner = self.upload.batch_owner;
        for id in pending {
            let batch = format!("0x{}", hex::encode(id));
            match ant_chain::discover::verify_persisted_batch(
                chain,
                postage_contract,
                &id,
                &our_owner,
            )
            .await
            {
                PersistedBatchVerdict::Owned => {
                    tracing::debug!(target: "ant-ffi", batch, "persisted batch confirmed on-chain");
                }
                PersistedBatchVerdict::NotFound => {
                    let now = tokio::time::Instant::now();
                    let due = *self.lock_not_found_since().entry(id).or_insert(now)
                        + self.not_found_grace;
                    if now < due {
                        tracing::info!(
                            target: "ant-ffi",
                            batch,
                            "persisted batch reads as not on-chain — re-checking in {}s before unregistering (an RPC backend may not have seen its creation yet)",
                            (due - now).as_secs(),
                        );
                        recheck_at = Some(recheck_at.map_or(due, |at| at.max(due)));
                        continue;
                    }
                    self.unregister(&id);
                    tracing::warn!(
                        target: "ant-ffi",
                        batch,
                        store = %self.store_path(&id).display(),
                        "persisted batch NOT FOUND on-chain (expired or never created) — unregistering it; uploads with it would be rejected by every storer",
                    );
                }
                PersistedBatchVerdict::Expired => {
                    self.unregister(&id);
                    tracing::warn!(
                        target: "ant-ffi",
                        batch,
                        store = %self.store_path(&id).display(),
                        "persisted batch has EXPIRED on-chain (remainingBalance 0) — unregistering it; uploads with it would be rejected by every storer",
                    );
                }
                PersistedBatchVerdict::ForeignOwner(on_chain_owner) => {
                    self.unregister(&id);
                    tracing::warn!(
                        target: "ant-ffi",
                        batch,
                        on_chain_owner = %format!("0x{}", hex::encode(on_chain_owner)),
                        our_owner = %format!("0x{}", hex::encode(our_owner)),
                        "persisted batch is owned by a different key on-chain — unregistering it (stamps we sign would be rejected)",
                    );
                }
                PersistedBatchVerdict::Unverified(e) => {
                    tracing::warn!(
                        target: "ant-ffi",
                        batch,
                        "could not confirm persisted batch on-chain ({e}); keeping it registered unverified",
                    );
                    continue;
                }
            }
            self.lock_unverified().remove(&id);
            self.lock_not_found_since().remove(&id);
        }
        recheck_at
    }

    /// Drop `id` from the live registry the node loop stamps from. The
    /// node already tolerates a batch disappearing under it (the
    /// rejected-batch self-probe clears its mark; a push naming it fails
    /// up-front with "batch 0x… not usable").
    fn unregister(&self, id: &[u8; 32]) {
        self.upload
            .issuers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(id);
    }

    fn store_path(&self, id: &[u8; 32]) -> PathBuf {
        self.upload
            .postage_dir
            .join(format!("{}.bin", hex::encode(id)))
    }

    fn lock_unverified(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeSet<[u8; 32]>> {
        self.unverified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn lock_not_found_since(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<[u8; 32], tokio::time::Instant>> {
        self.not_found_since
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn unverified(&self) -> Vec<[u8; 32]> {
        self.lock_unverified().iter().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Uploads
// ---------------------------------------------------------------------------

pub(crate) fn upload_start(
    h: &AntHandle,
    source_path: PathBuf,
    batch_id: Option<String>,
    name: Option<String>,
    content_type: Option<String>,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(
            &cmd_tx,
            ControlCommand::UploadStart {
                source_path,
                batch_id,
                name,
                content_type,
                raw: false,
                ack: ack_tx,
            },
        )
        .await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::UploadStarted { job_id } => Ok(job_id),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

pub(crate) fn upload_list(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(&cmd_tx, ControlCommand::UploadList { ack: ack_tx }).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::UploadList(jobs) => to_json(&Jobs { jobs }),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// Shared body for the single-job commands that all ack with
/// [`ControlAck::UploadJob`]: status, pause, resume, cancel.
pub(crate) fn upload_job_command(
    h: &AntHandle,
    job_id: String,
    kind: JobCommand,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        let cmd = match kind {
            JobCommand::Status => ControlCommand::UploadStatus {
                job_id,
                ack: ack_tx,
            },
            JobCommand::Pause => ControlCommand::UploadPause {
                job_id,
                ack: ack_tx,
            },
            JobCommand::Resume => ControlCommand::UploadResume {
                job_id,
                ack: ack_tx,
            },
            JobCommand::Cancel => ControlCommand::UploadCancel {
                job_id,
                ack: ack_tx,
            },
        };
        send(&cmd_tx, cmd).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::UploadJob(view) => to_json(&view),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// "Push again" with streamed progress: re-push a completed job's missing
/// chunks on the same job, handing each `{"phase":...,"checked"?,"total"?}`
/// progress line to `on_progress` as it arrives, then return the updated
/// job JSON. Mirrors [`verify_propagation_progress`]: the callback fires on
/// the FFI runtime thread inline with the blocking call.
pub(crate) fn upload_repush_progress(
    h: &AntHandle,
    job_id: String,
    mut on_progress: impl FnMut(&str),
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (prog_tx, mut prog_rx) = mpsc::unbounded_channel::<String>();
        send(
            &cmd_tx,
            ControlCommand::UploadRepush {
                job_id,
                progress: Some(prog_tx),
                ack: ack_tx,
            },
        )
        .await?;

        let finish = |ack: Result<ControlAck, oneshot::error::RecvError>| match ack {
            Ok(ControlAck::UploadJob(view)) => to_json(&view),
            Ok(ControlAck::NotReady { message } | ControlAck::Error { message }) => {
                Err(DriveError::Op(message))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(_) => Err(DriveError::Op("node dropped the ack channel".into())),
        };

        // Heal can run a few read-back/re-push rounds; reuse the generous
        // verification timeout as an upper bound.
        let mut ack_rx = ack_rx;
        let deadline = tokio::time::sleep(VERIFY_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                ack = &mut ack_rx => return finish(ack),
                line = prog_rx.recv() => {
                    match line {
                        Some(l) => on_progress(&l),
                        None => return finish((&mut ack_rx).await),
                    }
                }
                () = &mut deadline => {
                    return Err(DriveError::Op("operation timed out".into()));
                }
            }
        }
    })
}

#[derive(Clone, Copy)]
pub(crate) enum JobCommand {
    Status,
    Pause,
    Resume,
    Cancel,
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

/// Nudge the running node to recover after an OS suspension: re-warm the
/// dial queue and re-dial bootstrap, without a full `ant_shutdown` /
/// `ant_init`. Returns the node's status message (how many peer hints were
/// re-queued). Backs [`crate::ant_resume`]; see
/// [`ControlCommand::Resume`] for the full rationale.
pub(crate) fn resume(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(&cmd_tx, ControlCommand::Resume { ack: ack_tx }).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::Ok { message } => Ok(message),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// System suspend of the upload subsystem (app backgrounded / network
/// gone): auto-pause every in-flight job and stop securing passes. The
/// node acks only once the paused drivers have drained and checkpointed
/// (bounded server-side), so this blocking call returning means upload
/// state is durably on disk — exactly what the iOS background grace
/// window should be spent on. Backs [`crate::ant_suspend`].
pub(crate) fn suspend(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(&cmd_tx, ControlCommand::UploadSuspendAll { ack: ack_tx }).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::Ok { message } => Ok(message),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// Undo [`suspend`]: restart the jobs it paused (a user pause stays
/// paused) and re-queue securing for completed-but-unverified jobs.
/// Backs [`crate::ant_wake`].
pub(crate) fn wake(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(&cmd_tx, ControlCommand::UploadWakeAll { ack: ack_tx }).await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::Ok { message } => Ok(message),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

/// Bee's node-wide `swap-enable` switch, which governs retrieval payments
/// (issue #121). Backs [`crate::ant_set_swap_enabled`].
pub(crate) fn set_swap_enabled(h: &AntHandle, enabled: bool) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(
            &cmd_tx,
            ControlCommand::SetSwapEnabled {
                enabled,
                ack: ack_tx,
            },
        )
        .await?;
        match recv_oneshot(ack_rx).await? {
            ControlAck::Ok { message } => Ok(message),
            ControlAck::Error { message } => Err(DriveError::Op(message)),
            other => Err(unexpected(&other)),
        }
    })
}

// ---------------------------------------------------------------------------
// Storage plan
// ---------------------------------------------------------------------------

pub(crate) fn storage_status(h: &AntHandle) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime
        .block_on(async move { postage_status_json(&cmd_tx).await })
}

/// Read-back propagation check for an uploaded `reference` (the root
/// chunk / manifest address). Asks the node loop to probe up to
/// `probes` distinct closest peers with a network-only retrieval and
/// returns the JSON `{reference, retrievable, sources, probes}` the
/// handler builds. `sources` counts how many distinct entry peers
/// served the chunk back — a route-diversity / replication estimate —
/// so the app can show a "verified retrievable" badge instead of the
/// bare "online" state, which only reflects that we *attempted* the
/// push.
pub(crate) fn verify_propagation(
    h: &AntHandle,
    reference: [u8; 32],
    samples: u8,
    probes: u8,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        send(
            &cmd_tx,
            ControlCommand::VerifyPropagation {
                reference,
                samples,
                probes,
                progress: None,
                cancel: None,
                ack: ack_tx,
            },
        )
        .await?;
        match recv_oneshot_within(ack_rx, VERIFY_TIMEOUT).await? {
            ControlAck::Ok { message } => Ok(message),
            ControlAck::NotReady { message } | ControlAck::Error { message } => {
                Err(DriveError::Op(message))
            }
            other => Err(unexpected(&other)),
        }
    })
}

/// Like [`verify_propagation`], but streams incremental progress: each
/// `{"phase":...,"checked"?,"total"?}` JSON line the node emits is handed
/// to `on_progress` as it arrives, before the final verdict JSON is
/// returned. The callback runs on the FFI runtime thread inline with the
/// blocking call, so the caller (Swift) keeps it cheap (a channel hop).
pub(crate) fn verify_propagation_progress(
    h: &AntHandle,
    reference: [u8; 32],
    samples: u8,
    probes: u8,
    mut on_progress: impl FnMut(&str),
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    // Fresh cancel flag for this check: clear any prior request so a cancel
    // can only ever abort the check it was aimed at. Shared with the node's
    // verify task via the command.
    h.verify_cancel
        .store(false, std::sync::atomic::Ordering::Relaxed);
    let cancel = h.verify_cancel.clone();
    h.runtime.block_on(async move {
        let (ack_tx, ack_rx) = oneshot::channel();
        let (prog_tx, mut prog_rx) = mpsc::unbounded_channel::<String>();
        send(
            &cmd_tx,
            ControlCommand::VerifyPropagation {
                reference,
                samples,
                probes,
                progress: Some(prog_tx),
                cancel: Some(cancel),
                ack: ack_tx,
            },
        )
        .await?;

        let finish = |ack: Result<ControlAck, oneshot::error::RecvError>| match ack {
            Ok(ControlAck::Ok { message }) => Ok(message),
            Ok(ControlAck::NotReady { message } | ControlAck::Error { message }) => {
                Err(DriveError::Op(message))
            }
            Ok(other) => Err(unexpected(&other)),
            Err(_) => Err(DriveError::Op("node dropped the ack channel".into())),
        };

        // Drain progress until the terminal ack lands (or we time out).
        // Bias toward the ack so completion isn't delayed behind a backlog
        // of progress lines.
        let mut ack_rx = ack_rx;
        let deadline = tokio::time::sleep(VERIFY_TIMEOUT);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                ack = &mut ack_rx => return finish(ack),
                line = prog_rx.recv() => {
                    match line {
                        Some(l) => on_progress(&l),
                        // Progress channel closed (verify finished); the ack
                        // is next — await it directly rather than re-looping
                        // on a now-closed receiver.
                        None => return finish((&mut ack_rx).await),
                    }
                }
                () = &mut deadline => {
                    return Err(DriveError::Op("operation timed out".into()));
                }
            }
        }
    })
}

async fn postage_status_json(cmd_tx: &mpsc::Sender<ControlCommand>) -> Result<String, DriveError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    send(cmd_tx, ControlCommand::PostageStatus { ack: ack_tx }).await?;
    match recv_oneshot(ack_rx).await? {
        ControlAck::PostageStatus(view) => to_json(&view),
        ControlAck::Error { message } => Err(DriveError::Op(message)),
        other => Err(unexpected(&other)),
    }
}

// ---------------------------------------------------------------------------
// Account
// ---------------------------------------------------------------------------

pub(crate) fn account_info(h: &AntHandle) -> Result<String, DriveError> {
    let snap = h.status_rx.borrow();
    to_json(&AccountInfo {
        eth_address: format!("0x{}", hex::encode(h.eth)),
        overlay: snap.identity.overlay.clone(),
        peer_id: snap.identity.peer_id.clone(),
        agent: snap.agent.clone(),
    })
}

pub(crate) fn account_export_key(h: &AntHandle) -> String {
    hex::encode(h.signing_secret)
}

// ---------------------------------------------------------------------------
// On-chain "connect storage" — only with the `chain` feature.
// ---------------------------------------------------------------------------

/// Register the postage batch `batch_hex` (owned by this account on
/// Gnosis) so uploads can stamp against it. One `eth_call` for the
/// batch metadata, then a `RegisterBatch` into the live issuer
/// registry. Returns the refreshed [`storage_status`] JSON.
#[cfg(feature = "chain")]
pub(crate) fn storage_connect_batch(
    h: &AntHandle,
    rpc: String,
    batch_hex: String,
) -> Result<String, DriveError> {
    let batch_id = parse_batch_id(&batch_hex)?;
    let cmd_tx = h.cmd_tx.clone();
    let eth = h.eth;
    let secret = h.signing_secret;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        let chain = h.chain_client(rpc);
        let meta =
            ant_chain::fetch_postage_batch_meta(&chain, ant_chain::GNOSIS_POSTAGE_STAMP, &batch_id)
                .await
                .map_err(|e| DriveError::Op(format!("read storage plan from chain: {e}")))?;
        if meta.batch_owner_eth != eth {
            return Err(DriveError::Op(format!(
                "this storage plan belongs to 0x{}, not your account (0x{})",
                hex::encode(meta.batch_owner_eth),
                hex::encode(eth),
            )));
        }
        register_batch(
            &cmd_tx,
            batch_id,
            meta.depth,
            meta.bucket_depth,
            meta.immutable,
            None,
        )
        .await?;
        // Connecting a plan must also turn on outbound settlement —
        // otherwise uploads stamp fine but never propagate (bee freezes
        // an unpaying node out past its payment threshold). Best-effort:
        // a thin wallet or flaky RPC just leaves settlement off, which
        // the Storage UI surfaces via `settlement_status`. The running
        // gateway follows the outcome.
        let chequebook =
            ensure_settlement_best_effort(&cmd_tx, &chain, secret, &data_dir, eth).await;
        sync_gateway_chequebook(&h.gateway_chequebook, &eth, chequebook);
        postage_status_json(&cmd_tx).await
    })
}

/// The funded postage batches `owner` holds on Gnosis, from the saved
/// transfer scan in `data_dir` (brought up to the chain head first), or
/// with `full_rescan` from a scan of the whole history that replaces it;
/// and whether that scan still holds blocks read only from the unverified
/// source ([`crate::ant_set_unverified_logs_rpc`]). With `status_key` (the
/// background rediscovery), the scan reports to `/health.walletScan`; a
/// host's explicit discover doesn't.
#[cfg(feature = "chain")]
async fn owned_batches(
    chain: &ant_chain::ChainClient,
    owner: &[u8; 20],
    data_dir: &std::path::Path,
    full_rescan: bool,
    status_key: Option<&[u8; 20]>,
) -> Result<(Vec<ant_chain::discover::DiscoveredBatch>, bool), ant_chain::RpcError> {
    let scan = if let Some(status_key) = status_key {
        ant_chain::discover::rediscovery_scan(
            chain,
            ant_chain::GNOSIS_BZZ_TOKEN,
            owner,
            data_dir,
            full_rescan,
            status_key,
        )
        .await?
    } else if full_rescan {
        ant_chain::discover::rescan_transfer_history(
            chain,
            ant_chain::GNOSIS_BZZ_TOKEN,
            owner,
            data_dir,
        )
        .await?
    } else {
        ant_chain::discover::refresh_transfer_scan(
            chain,
            ant_chain::GNOSIS_BZZ_TOKEN,
            owner,
            data_dir,
        )
        .await?
    };
    let found =
        ant_chain::discover::owned_batches_in(chain, ant_chain::GNOSIS_POSTAGE_STAMP, &scan)
            .await?;
    Ok((found, scan.provisional_since.is_some()))
}

/// Auto-discover every funded postage batch this account owns on Gnosis
/// and register each one. Returns `{"registered":[...],"status":<plan>}`.
///
/// Continues the saved transfer scan (only the blocks since, #118), or
/// with `full_rescan` (`ant_storage_discover_full`) reads the account's
/// whole transfer history again and replaces it: the way back from a
/// saved scan an RPC once answered incompletely, which nothing automatic
/// revisits. Hosts call the plain form at every start, so it stays cheap.
///
/// A plain discover that read part of the history only from the
/// unverified source ([`crate::ant_set_unverified_logs_rpc`]) starts the
/// handle's background confirmation ([`ChainInit::confirm_unverified`],
/// at most one loop per handle) as the gateway's chain init does, so the
/// blocks don't stay unconfirmed until the next `ant_start_gateway`. A
/// full rescan reads through the transport only and is never unconfirmed.
#[cfg(feature = "chain")]
pub(crate) fn storage_discover(
    h: &AntHandle,
    rpc: String,
    full_rescan: bool,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    let eth = h.eth;
    let secret = h.signing_secret;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        let chain = h.chain_client(rpc);
        let (found, unconfirmed) = owned_batches(&chain, &eth, &data_dir, full_rescan, None)
            .await
            .map_err(|e| DriveError::Op(format!("search the chain for your storage: {e}")))?;
        if unconfirmed {
            spawn_confirm_unverified(h, chain.clone());
        }
        let mut registered = Vec::new();
        for b in &found {
            register_batch(
                &cmd_tx,
                b.batch_id,
                b.depth,
                b.bucket_depth,
                b.immutable,
                None,
            )
            .await?;
            registered.push(format!("0x{}", hex::encode(b.batch_id)));
        }
        // If we connected at least one plan, make sure outbound
        // settlement is on so uploads against it actually reach the
        // network. Best-effort; never fails discovery.
        if !registered.is_empty() {
            let chequebook =
                ensure_settlement_best_effort(&cmd_tx, &chain, secret, &data_dir, eth).await;
            sync_gateway_chequebook(&h.gateway_chequebook, &eth, chequebook);
        }
        let status = postage_status_json(&cmd_tx).await?;
        // `status` is already a JSON document; splice it in raw.
        Ok(format!(
            "{{\"registered\":{},\"status\":{}}}",
            serde_json::to_string(&registered)
                .map_err(|e| DriveError::Op(format!("encode: {e}")))?,
            status,
        ))
    })
}

/// Mark `h`'s rediscovery unconfirmed and confirm it in the background
/// ([`ChainInit::confirm_unverified`]), reporting an adopted chequebook to
/// the gateway's slot. A no-op while a confirmation already runs.
#[cfg(feature = "chain")]
fn spawn_confirm_unverified(h: &AntHandle, chain: ant_chain::ChainClient) {
    h.chain_init
        .unconfirmed
        .store(true, std::sync::atomic::Ordering::Release);
    let init = std::sync::Arc::clone(&h.chain_init);
    let cmd_tx = h.cmd_tx.clone();
    let data_dir = h.data_dir.clone();
    let secret = h.signing_secret;
    let eth = h.eth;
    let slot = h.gateway_chequebook.clone();
    h.runtime.spawn(async move {
        if let Some(chequebook) = init
            .confirm_unverified(&chain, &cmd_tx, &data_dir, secret)
            .await
        {
            sync_gateway_chequebook(&slot, &eth, Some(chequebook));
        }
    });
}

#[cfg(feature = "chain")]
/// Register a batch with the running node. `bought_at_block` is the
/// `createBatch` receipt's block for a batch this node just bought (the
/// node then holds it back as `usable: false` until the storers have
/// synced it), and `None` for a batch connected or rediscovered.
async fn register_batch(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    batch_id: [u8; 32],
    depth: u8,
    bucket_depth: u8,
    immutable: bool,
    bought_at_block: Option<u64>,
) -> Result<(), DriveError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    send(
        cmd_tx,
        ControlCommand::RegisterBatch {
            batch_id,
            depth,
            bucket_depth,
            immutable,
            bought_at_block,
            ack: ack_tx,
        },
    )
    .await?;
    match recv_oneshot(ack_rx).await? {
        ControlAck::Ok { .. } => Ok(()),
        ControlAck::Error { message } => Err(DriveError::Op(message)),
        other => Err(unexpected(&other)),
    }
}

/// Gnosis chain id, re-exported from the crate root so the on-chain
/// helpers here and the node bring-up in `lib.rs` share one constant.
#[cfg(feature = "chain")]
use crate::GNOSIS_CHAIN_ID;

/// Pricing and paying for storage with xDAI is shared with the
/// gateway's `/v0/storage/*` routes (AGENTS.md, "one orchestration, two
/// sequencers"). This file maps its results onto the C API's JSON.
#[cfg(feature = "chain")]
use ant_chain::funding::{self, DepositPolicy, Payer, POSTAGE_BUCKET_DEPTH};

/// The settlement deposit the storage flows fund the chequebook to:
/// **0.001 xBZZ**, the same default `antd` uses (rationale at the
/// constant).
///
/// A chequebook with deposit 0 backs no cheque: swap is enabled and
/// peers accept the cheques, so publishing runs clean right up until the
/// peers' payment tolerance is exhausted, then collapses into 60 s
/// pushsync timeouts (issue #73; the #67 soak measured 0.57 Mbit/s with
/// 25 failures and a 10-minute live-edge lag). The same soak with a
/// funded chequebook ran 899/899 segments at 0.89 Mbit/s flat.
#[cfg(feature = "chain")]
use ant_chain::chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR as DEPOSIT_TARGET_PLUR;

/// Swarm chunk size in bytes (capacity math).
#[cfg(feature = "chain")]
const BYTES_PER_CHUNK: u64 = 4096;

/// The node wallet: it pays for storage, owns the batches and issues the
/// chequebook's cheques.
#[cfg(feature = "chain")]
fn node_wallet(h: &AntHandle) -> Result<ant_chain::tx::Wallet, DriveError> {
    ant_chain::tx::Wallet::new(h.signing_secret, GNOSIS_CHAIN_ID)
        .map_err(|e| DriveError::Op(format!("wallet: {e}")))
}

/// What a storage buy funds besides the plan: this account's chequebook
/// deposit, from the persisted association (`None` when the buy will
/// deploy the chequebook).
#[cfg(feature = "chain")]
fn deposit_policy(h: &AntHandle) -> DepositPolicy {
    deposit_policy_for(&h.data_dir, &h.eth)
}

/// [`deposit_policy`] for `owner`'s record in `data_dir`. A chequebook
/// this process's chain check disqualified ([`DISQUALIFIED`]) is
/// neither funded nor replaced by a buy (see [`setup_settlement`]), so
/// there's no deposit to price in or acquire: the plan goes alone.
#[cfg(feature = "chain")]
fn deposit_policy_for(data_dir: &std::path::Path, owner: &[u8; 20]) -> DepositPolicy {
    match persisted_chequebook(data_dir, owner) {
        Some(cb) if lock_disqualified().contains(&(*owner, cb)) => DepositPolicy::Unmanaged,
        chequebook => DepositPolicy::Managed {
            chequebook,
            target: DEPOSIT_TARGET_PLUR,
        },
    }
}

#[cfg(feature = "chain")]
fn funding_err(e: funding::FundingError) -> DriveError {
    DriveError::Op(e.to_string())
}

/// Parse the per-chunk amount the app got from a quote.
#[cfg(feature = "chain")]
fn parse_amount(amount_per_chunk: &str, what: &str) -> Result<u128, DriveError> {
    amount_per_chunk
        .trim()
        .parse()
        .map_err(|_| DriveError::Op(format!("invalid {what} price")))
}

/// The C API's quote JSON (see [`Quote`]) for a shared quote.
#[cfg(feature = "chain")]
fn quote_json(q: &funding::PlanQuote) -> Result<String, DriveError> {
    let f = &q.funding;
    let capacity_bytes = 1u64
        .checked_shl(u32::from(q.depth))
        .map_or(u64::MAX, |chunks| chunks.saturating_mul(BYTES_PER_CHUNK));
    to_json(&Quote {
        depth: q.depth,
        days: q.days,
        amount_per_chunk: q.amount_per_chunk.to_string(),
        total_cost_plur: q.plan_cost_plur.to_string(),
        total_cost_bzz: format_bzz(q.plan_cost_plur),
        settlement_deposit_plur: q.deposit_due_plur.to_string(),
        settlement_deposit_bzz: format_bzz(q.deposit_due_plur),
        capacity_bytes,
        account_bzz: f.wallet_bzz.to_string(),
        account_bzz_display: format_bzz(f.wallet_bzz),
        account_xdai: f.wallet_xdai.to_string(),
        account_xdai_display: format_native(f.wallet_xdai),
        needed_bzz: f.bzz_to_acquire.to_string(),
        needed_bzz_display: format_bzz(f.bzz_to_acquire),
        xdai_required: f.xdai_required_wei.to_string(),
        xdai_required_display: format_native(f.xdai_required_wei),
        xdai_to_send: f.xdai_to_send_wei.to_string(),
        xdai_to_send_display: format_native(f.xdai_to_send_wei),
        sufficient_funds: f.sufficient,
    })
}

/// Price a storage plan: read the current postage price + the account's
/// xBZZ / xDAI balances from Gnosis and compute what a `depth`-sized
/// plan lasting `days` costs. Returns everything the "payment
/// information" screen needs to show cost vs. the account's funds. No
/// transaction is sent.
///
/// The all-in figures (`needed_bzz` / `xdai_required` / `xdai_to_send` /
/// `sufficient_funds`) include the one-time settlement deposit this
/// account's chequebook still needs (`settlement_deposit_plur`, zero once
/// it is funded), because activating a plan is also what deploys and
/// funds that chequebook — see [`ensure_settlement`].
/// `total_cost_plur` / `total_cost_bzz` stay the plan's own cost.
#[cfg(feature = "chain")]
pub(crate) fn storage_quote(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    days: u64,
) -> Result<String, DriveError> {
    let wallet = node_wallet(h)?;
    let policy = deposit_policy(h);
    h.runtime.block_on(async move {
        let client = h.chain_client(rpc);
        let payer = Payer::gnosis(&client, &wallet);
        let quote = funding::quote_plan(&payer, policy, depth, days)
            .await
            .map_err(funding_err)?;
        quote_json(&quote)
    })
}

/// Price a top-up of the *connected* storage plan: how much it costs to
/// extend its lifetime by `days` at the current postage price, and
/// whether the account's xBZZ / xDAI funds cover it. Reads the batch
/// depth from the local issuer (a top-up pays per chunk, so cost scales
/// with the plan's size). Returns the same quote shape as
/// [`storage_quote`]; extending a plan deploys nothing, so its
/// settlement deposit is always zero. No transaction is sent.
#[cfg(feature = "chain")]
pub(crate) fn storage_topup_quote(
    h: &AntHandle,
    rpc: String,
    days: u64,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    let wallet = node_wallet(h)?;
    h.runtime.block_on(async move {
        let view = connected_plan(&cmd_tx).await?;
        let batch_id = parse_batch_id(&view.batch_id)?;
        let client = h.chain_client(rpc);
        let payer = Payer::gnosis(&client, &wallet);
        let quote = funding::quote_extend(&payer, &batch_id, view.batch_depth, None, days)
            .await
            .map_err(funding_err)?;
        quote_json(&quote)
    })
}

/// Top up (extend) the connected storage plan, funding **only with
/// xDAI**: swap the xBZZ shortfall on-chain if needed, `approve` the
/// postage contract for `amount_per_chunk × 2^depth`, then submit
/// `PostageStamp.topUp`. `amount_per_chunk` is the value from
/// [`storage_topup_quote`] so the charge matches the approved quote.
/// The batch's depth doesn't change, so no re-registration is needed.
/// Returns the refreshed [`storage_validity`] JSON so the caller can
/// show the new expiry immediately.
///
/// Submits real Gnosis transactions and spends real funds, so the app
/// gates it behind an explicit confirmation.
#[cfg(feature = "chain")]
pub(crate) fn storage_topup_xdai(
    h: &AntHandle,
    rpc: String,
    amount_per_chunk: String,
) -> Result<String, DriveError> {
    let amount = parse_amount(&amount_per_chunk, "top-up")?;
    let cmd_tx = h.cmd_tx.clone();
    let wallet = node_wallet(h)?;
    let tx_rpc = rpc.clone();
    h.runtime.block_on(async move {
        let view = connected_plan(&cmd_tx).await?;
        let batch_id = parse_batch_id(&view.batch_id)?;
        let client = h.chain_client(tx_rpc);
        let payer = Payer::gnosis(&client, &wallet);
        // Balance reads, swap, approve and top-up under the account's
        // wallet tx lock (see [`wallet_tx_lock`]).
        let _tx = wallet_tx_lock(&h.eth).lock_owned().await;
        funding::extend_with_xdai(&payer, &batch_id, view.batch_depth, None, amount)
            .await
            .map_err(funding_err)
    })?;
    // Depth is unchanged, so the local issuer needs no update; return the
    // fresh on-chain validity so the UI shows the new expiry right away.
    storage_validity(h, rpc)
}

/// The local issuer view of the connected plan, or an error when no
/// plan is connected — shared pre-check for the top-up quote/execute
/// pair (both need the batch id + depth).
#[cfg(feature = "chain")]
async fn connected_plan(
    cmd_tx: &mpsc::Sender<ControlCommand>,
) -> Result<ant_control::PostageStatusView, DriveError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    send(cmd_tx, ControlCommand::PostageStatus { ack: ack_tx }).await?;
    let view = match recv_oneshot(ack_rx).await? {
        ControlAck::PostageStatus(v) => v,
        ControlAck::Error { message } => return Err(DriveError::Op(message)),
        other => return Err(unexpected(&other)),
    };
    if !view.enabled || view.batch_id.is_empty() {
        return Err(DriveError::Op(
            "no storage plan is connected to extend".into(),
        ));
    }
    Ok(view)
}

/// Remaining lifetime of the connected storage plan. Reads the batch's
/// on-chain remaining per-chunk balance (`PostageStamp.remainingBalance`,
/// i.e. normalised balance minus the cumulative outpayment) and the
/// current price per chunk per block, then converts to wall-clock seconds
/// via Gnosis block time. Returns `{enabled, remaining_seconds,
/// expires_unix}`; `enabled = false` when no plan is connected. One
/// `eth_call` each for price and balance — cheap enough to call from a
/// pull-to-refresh, not on every status poll.
#[cfg(feature = "chain")]
pub(crate) fn storage_validity(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        // The connected batch id comes from the local issuer status.
        let (ack_tx, ack_rx) = oneshot::channel();
        send(&cmd_tx, ControlCommand::PostageStatus { ack: ack_tx }).await?;
        let view = match recv_oneshot(ack_rx).await? {
            ControlAck::PostageStatus(v) => v,
            ControlAck::Error { message } => return Err(DriveError::Op(message)),
            other => return Err(unexpected(&other)),
        };
        if !view.enabled || view.batch_id.is_empty() {
            return to_json(&Validity {
                enabled: false,
                remaining_seconds: 0,
                expires_unix: 0,
            });
        }
        let batch_id = parse_batch_id(&view.batch_id)?;
        let client = h.chain_client(rpc);
        // Price per chunk per block; clamp to 1 so a transient zero read
        // doesn't blow the division up into a bogus eternity.
        let price = client
            .postage_last_price(ant_chain::GNOSIS_POSTAGE_STAMP)
            .await
            .map_err(|e| DriveError::Op(format!("read storage price: {e}")))?
            .max(1);
        let remaining = client
            .postage_remaining_balance(ant_chain::GNOSIS_POSTAGE_STAMP, &batch_id)
            .await
            .map_err(|e| DriveError::Op(format!("read storage balance: {e}")))?;
        let remaining_seconds = (remaining / price).saturating_mul(funding::GNOSIS_BLOCK_SECS);
        let remaining_seconds = u64::try_from(remaining_seconds).unwrap_or(u64::MAX);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        to_json(&Validity {
            enabled: true,
            remaining_seconds,
            expires_unix: now.saturating_add(remaining_seconds),
        })
    })
}

/// Buy and activate a storage plan: `approve` the postage contract for
/// `amount_per_chunk × 2^depth` xBZZ, submit `createBatch`, pull the new
/// batch id from the receipt, and register it with the running node so
/// uploads can stamp against it immediately. Returns the refreshed
/// [`storage_status`] JSON.
///
/// This submits two real Gnosis transactions and spends real funds, so
/// the app gates it behind an explicit confirmation.
#[cfg(feature = "chain")]
pub(crate) fn storage_buy(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    amount_per_chunk: String,
    immutable: bool,
) -> Result<String, DriveError> {
    let amount = parse_amount(&amount_per_chunk, "plan")?;
    let wallet = node_wallet(h)?;
    h.runtime.block_on(async move {
        let client = h.chain_client(rpc);
        let payer = Payer::gnosis(&client, &wallet);
        // Approve and createBatch under the account's wallet tx lock (see
        // [`wallet_tx_lock`]); released before the settlement step below,
        // which takes it itself.
        let tx = wallet_tx_lock(&h.eth).lock_owned().await;
        let new = funding::buy_batch(&payer, amount, depth, immutable)
            .await
            .map_err(funding_err)?;
        drop(tx);
        activate_bought_batch(h, &client, &wallet, new, depth, immutable).await
    })
}

/// Buy and activate a storage plan funding **only with xDAI**: the node
/// swaps the xBZZ the plan and the settlement deposit still need through
/// the on-chain helper, then runs the same `approve` + `createBatch`
/// flow as [`storage_buy`].
///
/// Submits up to six real Gnosis transactions (one-time helper deploy,
/// swap, approve, createBatch, then the one-time chequebook deploy and
/// its settlement deposit) and spends real funds, so the app gates it
/// behind explicit confirmation.
#[cfg(feature = "chain")]
pub(crate) fn storage_buy_xdai(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    amount_per_chunk: String,
    immutable: bool,
) -> Result<String, DriveError> {
    let amount = parse_amount(&amount_per_chunk, "plan")?;
    let wallet = node_wallet(h)?;
    let policy = deposit_policy(h);
    h.runtime.block_on(async move {
        let client = h.chain_client(rpc);
        let payer = Payer::gnosis(&client, &wallet);
        // Balance reads, swap, approve and createBatch under the
        // account's wallet tx lock (see [`wallet_tx_lock`]), so the
        // shortfall is read after any other spend finished; released
        // before the settlement step below, which takes it itself.
        let tx = wallet_tx_lock(&h.eth).lock_owned().await;
        let new = funding::buy_plan_with_xdai(&payer, policy, depth, amount, immutable)
            .await
            .map_err(funding_err)?;
        drop(tx);
        activate_bought_batch(h, &client, &wallet, new, depth, immutable).await
    })
}

/// After a buy: register the batch with the running node so uploads can
/// stamp against it immediately, then make sure outbound settlement is
/// on so the upload actually reaches the network (best-effort; never
/// fails the purchase). Returns the refreshed [`storage_status`] JSON.
#[cfg(feature = "chain")]
async fn activate_bought_batch(
    h: &AntHandle,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    new: funding::NewBatch,
    depth: u8,
    immutable: bool,
) -> Result<String, DriveError> {
    register_batch(
        &h.cmd_tx,
        new.id,
        depth,
        POSTAGE_BUCKET_DEPTH,
        immutable,
        Some(new.block),
    )
    .await?;
    ensure_settlement_for_gateway(
        &h.cmd_tx,
        client,
        wallet,
        &h.data_dir,
        h.signing_secret,
        h.eth,
        &h.gateway_chequebook,
    )
    .await;
    postage_status_json(&h.cmd_tx).await
}

/// Ensure this node has a factory-registered chequebook and switch on
/// outbound SWAP settlement for the *running* node, so the batch the
/// user just bought can actually be pushed to the network. Without a
/// chequebook, bee's accounting locks us out after ~20 K chunks across
/// the peer set and the upload stalls — the user sees "uploaded" but
/// the content never propagates.
///
/// Resolution order mirrors `antd`:
///   1. **Persisted / startup** — a chequebook we deployed on an
///      earlier run (already enabled at `ant_init`); re-send the enable
///      command (idempotent) so a chequebook deployed *this* session is
///      also covered.
///   2. **Rediscover** — a chequebook this node EOA already owns
///      on-chain (e.g. a reinstalled app with the same backed-up key);
///      adopt + persist it rather than stranding its balance.
///   3. **Auto-deploy** — first run with a funded wallet: deploy a
///      fresh factory-registered chequebook (issuer = node EOA),
///      persist it, and fund it with [`DEPOSIT_TARGET_PLUR`] xBZZ so
///      the cheques it signs are actually backed.
///
/// A chequebook reached through step 1 or 2 predates this and can be
/// sitting at deposit 0 (every install before #73 is), so it is topped
/// up to the same target from spare wallet xBZZ before settlement is
/// switched on.
///
/// Best-effort: never fails the surrounding storage purchase. A thin
/// wallet (no spare xDAI for the one-time deploy, no spare xBZZ for the
/// deposit) or flaky RPC just logs a warning; settlement enables — and
/// the deposit lands — the next time one of this function's callers
/// runs: a storage buy (including one through the gateway's
/// `POST /stamps`), a plan connect, a plan discover, or an explicit
/// [`deploy_chequebook`] ([`settlement_topup_xdai`] also lands the
/// deposit). A gateway start adopts an existing chequebook
/// ([`ChainInit::run`]) but never deploys or funds one.
/// The node wallet both pays gas and is the issuer, so no external key
/// is ever introduced. Returns the chequebook settlement now runs on,
/// if any.
#[cfg(feature = "chain")]
pub(crate) async fn ensure_settlement(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    swap_secret: [u8; 32],
    node_eth: [u8; 20],
) -> Option<[u8; 20]> {
    match setup_settlement(
        cmd_tx,
        client,
        wallet,
        data_dir,
        swap_secret,
        node_eth,
        true,
    )
    .await
    {
        Ok(chequebook) => chequebook,
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                "could not enable network settlement (uploads still work for a while, \
                 then stall until a chequebook exists): {e}",
            );
            None
        }
    }
}

/// Serialises chequebook setup across every caller. The host's
/// launch-time [`deploy_chequebook`], the gateway's after-buy hook, a
/// storage buy and the gateway-start [`ChainInit::run`] can overlap, and
/// two of them concluding "no chequebook yet" at once would deploy and
/// fund two (or top one up twice). Process-wide rather than
/// per-handle: one node per process is the norm, and serialising two
/// handles' setups costs nothing but a short wait.
#[cfg(feature = "chain")]
static CHEQUEBOOK_SETUP: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The one chequebook routine behind every settlement path: resolve the
/// account's chequebook, optionally spend on it, and switch outbound
/// settlement on in the running node. Returns the chequebook, or `None`
/// when there is none to use yet (no chequebook and either `may_spend`
/// is off or the wallet can't pay for the deploy).
///
/// `may_spend` allows deploying a chequebook and topping up an adopted
/// one's deposit (a chequebook reached as persisted or rediscovered can
/// be sitting at deposit 0, as every install before #73 is). Only host
/// calls and buys pass `true`; the gateway-start chain init passes
/// `false`.
#[cfg(feature = "chain")]
pub(crate) async fn setup_settlement(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    swap_secret: [u8; 32],
    node_eth: [u8; 20],
    may_spend: bool,
) -> Result<Option<[u8; 20]>, DriveError> {
    let _setup = CHEQUEBOOK_SETUP.lock().await;
    let resolved =
        match resolve_or_deploy_chequebook(client, wallet, data_dir, node_eth, may_spend).await? {
            Resolution::Use(resolved) => resolved,
            Resolution::NoneYet => return Ok(None),
            Resolution::Disqualified { chequebook, reason } => {
                // `ant_init` has no RPC, so it switched settlement on
                // from the persisted record unchecked. Now that the
                // chain says no, switch it off again: cheques every peer
                // drops only burn bandwidth while the status and logs
                // claim settlement works.
                lock_disqualified().insert((node_eth, chequebook));
                disable_settlement(cmd_tx, chequebook).await;
                return Err(DriveError::Op(reason));
            }
        };
    if resolved.verified {
        lock_disqualified().remove(&(node_eth, resolved.address));
    } else if lock_disqualified().contains(&(node_eth, resolved.address)) {
        // An earlier check in this process said no; a read that failed
        // now (or a scan that doesn't run the checks) doesn't overturn
        // it. Neither enabled nor funded.
        disable_settlement(cmd_tx, resolved.address).await;
        return Err(DriveError::Op(format!(
            "chequebook 0x{} failed its on-chain checks and could not be re-verified; \
             settlement stays off for it",
            hex::encode(resolved.address),
        )));
    }
    // A chequebook we just deployed was funded as part of the deploy; an
    // adopted one carries whatever deposit it already had and is topped
    // up — the top-up re-runs the chain checks right before it sends.
    if may_spend && !resolved.deployed {
        if let Some(reason) =
            fund_chequebook_best_effort(client, wallet, data_dir, &node_eth, &resolved.address)
                .await
        {
            lock_disqualified().insert((node_eth, resolved.address));
            disable_settlement(cmd_tx, resolved.address).await;
            return Err(DriveError::Op(reason));
        }
    }
    enable_settlement(cmd_tx, resolved.address, swap_secret, data_dir).await;
    watch_retrieval_funds(cmd_tx, client, resolved.address);
    Ok(Some(resolved.address))
}

/// The retrieval-funds watch running for this process, if any: the
/// chequebook, the node it publishes to, and its task.
#[cfg(feature = "chain")]
type FundsWatch = (
    [u8; 20],
    mpsc::Sender<ControlCommand>,
    tokio::task::AbortHandle,
);

/// Keep the node's view of what `chequebook` can pay for downloads and
/// uploads current (issues #121, #127): run the shared
/// `ant_chain::chequebook_store::watch_retrieval_funds` (as `antd`
/// does), publishing each read into `ControlCommand::SetRetrievalFunds`,
/// over `client` (so through the host chain transport when one is
/// installed). Started by every successful [`setup_settlement`] and
/// every successful deposit top-up ([`refresh_retrieval_funds`]); one
/// watch per node, replaced when its chequebook changes.
///
/// `ant_init` reloads a persisted chequebook without an RPC, so until the
/// first settlement setup or top-up after it (the gateway-start chain
/// init, a storage call, `ant_deploy_chequebook`,
/// `ant_storage_settlement_topup*`) the node knows no funds and
/// downloads and uploads stay on the free tier.
#[cfg(feature = "chain")]
fn watch_retrieval_funds(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    chequebook: [u8; 20],
) {
    static WATCHES: std::sync::Mutex<Vec<FundsWatch>> = std::sync::Mutex::new(Vec::new());
    let mut watches = WATCHES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    watches.retain(|(_, _, task)| !task.is_finished());
    if let Some(at) = watches
        .iter()
        .position(|(_, tx, _)| tx.same_channel(cmd_tx))
    {
        if watches[at].0 == chequebook {
            return;
        }
        watches.swap_remove(at).2.abort();
    }
    let commands = cmd_tx.clone();
    let task = tokio::spawn(ant_chain::chequebook_store::watch_retrieval_funds(
        client.clone(),
        chequebook,
        move |funds| {
            let commands = commands.clone();
            async move { publish_retrieval_funds(&commands, chequebook, funds).await }
        },
    ));
    watches.push((chequebook, cmd_tx.clone(), task.abort_handle()));
}

/// Hand one read of `chequebook`'s funds to the node
/// (`ControlCommand::SetRetrievalFunds`). `false` once the node is gone.
#[cfg(feature = "chain")]
async fn publish_retrieval_funds(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    chequebook: [u8; 20],
    funds: ant_chain::chequebook_store::RetrievalFunds,
) -> bool {
    let (ack, ack_rx) = oneshot::channel();
    let sent = send(
        cmd_tx,
        ControlCommand::SetRetrievalFunds {
            chequebook,
            deposited_plur: funds.deposited_plur,
            exchange_rate_plur: funds.exchange_rate_plur,
            deduction_plur: funds.deduction_plur,
            ack,
        },
    )
    .await;
    sent.is_ok() && recv_oneshot(ack_rx).await.is_ok()
}

/// After a deposit into `chequebook` (PR #126 R1-M3): tell the node its
/// new funds now, rather than at the watch's next read up to
/// [`ant_chain::chequebook_store::RETRIEVAL_FUNDS_REFRESH`] later (a
/// payer removed for exhausted credit comes back at once), and make sure
/// the funds watch runs. `ant_init` reloads a persisted chequebook
/// without an RPC and starts no watch, so without this a host that only
/// tops up would never get the node paying. A failed read publishes
/// nothing (the node keeps what it knew); the watch retries it.
#[cfg(feature = "chain")]
async fn refresh_retrieval_funds(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    chequebook: [u8; 20],
) {
    match ant_chain::chequebook_store::read_retrieval_funds(client, &chequebook).await {
        Ok(funds) => {
            publish_retrieval_funds(cmd_tx, chequebook, funds).await;
        }
        Err(e) => tracing::debug!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            "funds not re-read after the deposit (the watch retries): {e}",
        ),
    }
    watch_retrieval_funds(cmd_tx, client, chequebook);
}

/// One [`ant_gateway::WalletTxLock`] per node account, process-wide.
///
/// Every transaction ant-ffi sends from the node wallet holds it: the
/// storage buy / top-up / xDAI-swap flows, the chequebook deploy, and
/// the deposit transfers (including the gateway's spawned after-buy
/// settlement task). The in-process gateway's `ChainContext` gets the
/// same lock (see [`crate::gateway`]), so a `POST /stamps` can't race
/// that background deposit for a pending nonce, or for the xBZZ its
/// balance guard just counted. Keyed by account because the wallet
/// (and its nonce sequence) is; process-wide because gateways are
/// rebuilt on every start and more than one handle can drive the same
/// account.
///
/// Not reentrant: hold it around the transaction steps only, never
/// across a call into another path that takes it (e.g. drop it before
/// [`ensure_settlement`]). Taken after the chequebook-setup lock and
/// before `ant-chain`'s per-key sender lock, the order
/// `ant_gateway::WriteGate` documents.
#[cfg(feature = "chain")]
pub(crate) fn wallet_tx_lock(owner: &[u8; 20]) -> ant_gateway::WalletTxLock {
    static LOCKS: std::sync::Mutex<
        std::collections::BTreeMap<[u8; 20], ant_gateway::WalletTxLock>,
    > = std::sync::Mutex::new(std::collections::BTreeMap::new());
    LOCKS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .entry(*owner)
        .or_default()
        .clone()
}

/// `(owner, chequebook)` pairs the last chain check in this process
/// disqualified (see [`setup_settlement`] and
/// [`settlement_topup_xdai_for`]). [`settlement_status`] reads it so a
/// persisted record the chain rejected isn't reported as working
/// settlement. Process-wide like [`CHEQUEBOOK_SETUP`], so keyed by the
/// signing account too: an issuer mismatch is relative to the account
/// that checked, and the chequebook account A was refused (issued by B)
/// is exactly the one an in-process switch to B may legitimately use.
#[cfg(feature = "chain")]
type DisqualifiedSet = std::collections::BTreeSet<([u8; 20], [u8; 20])>;

#[cfg(feature = "chain")]
static DISQUALIFIED: std::sync::Mutex<DisqualifiedSet> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

#[cfg(feature = "chain")]
fn lock_disqualified() -> std::sync::MutexGuard<'static, DisqualifiedSet> {
    DISQUALIFIED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Whether this process's last chain check disqualified `chequebook`
/// for `owner` (see [`DISQUALIFIED`]).
#[cfg(feature = "chain")]
pub(crate) fn is_disqualified(owner: &[u8; 20], chequebook: &[u8; 20]) -> bool {
    lock_disqualified().contains(&(*owner, *chequebook))
}

/// Bring the in-process gateway's chequebook slot in line after a
/// settlement setup: `adopted` (the chequebook settlement now runs on)
/// is written to it; otherwise a chequebook in it that the chain check
/// disqualified is cleared, so the gateway neither reports it nor lets
/// `POST /chequebook/deposit` fund it.
#[cfg(feature = "chain")]
pub(crate) fn sync_gateway_chequebook(
    slot: &ant_gateway::ChequebookSlot,
    owner: &[u8; 20],
    adopted: Option<[u8; 20]>,
) {
    match adopted {
        Some(cb) => slot.set(cb),
        None => {
            if let Some(cb) = slot.get().filter(|cb| is_disqualified(owner, cb)) {
                slot.refuse(cb);
            }
        }
    }
}

/// [`ensure_settlement`] for a C-API storage call, then point the
/// in-process gateway's chequebook `slot` at the outcome, as the
/// gateway's own after-buy hook and chain init do. Without that, a
/// chequebook set up by `ant_storage_buy*` stayed invisible to the
/// running gateway's `/chequebook/*`, `/wallet` and
/// `POST /chequebook/deposit` until the next `ant_start_gateway`.
#[cfg(feature = "chain")]
async fn ensure_settlement_for_gateway(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    secret: [u8; 32],
    node_eth: [u8; 20],
    slot: &ant_gateway::ChequebookSlot,
) -> Option<[u8; 20]> {
    let chequebook = ensure_settlement(cmd_tx, client, wallet, data_dir, secret, node_eth).await;
    sync_gateway_chequebook(slot, &node_eth, chequebook);
    chequebook
}

/// [`setup_settlement`] with spending allowed (`ant_deploy_chequebook`),
/// then point the in-process gateway's chequebook `slot` at the outcome.
/// The slot is synced on the error path too: a persisted chequebook the
/// chain check disqualifies makes [`setup_settlement`] return `Err` (not
/// `Ok(None)`), and the gateway must stop reporting and funding it.
#[cfg(feature = "chain")]
async fn setup_settlement_for_gateway(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    secret: [u8; 32],
    node_eth: [u8; 20],
    slot: &ant_gateway::ChequebookSlot,
) -> Result<Option<[u8; 20]>, DriveError> {
    let result = setup_settlement(cmd_tx, client, wallet, data_dir, secret, node_eth, true).await;
    sync_gateway_chequebook(slot, &node_eth, result.as_ref().ok().copied().flatten());
    result
}

/// Switch outbound SWAP settlement off in the running node if it runs on
/// `chequebook` (`DisablePushsyncSwap`). Logs the node's answer.
#[cfg(feature = "chain")]
async fn disable_settlement(cmd_tx: &mpsc::Sender<ControlCommand>, chequebook: [u8; 20]) {
    let (ack_tx, ack_rx) = oneshot::channel();
    if send(
        cmd_tx,
        ControlCommand::DisablePushsyncSwap {
            chequebook,
            ack: ack_tx,
        },
    )
    .await
    .is_err()
    {
        return;
    }
    match recv_oneshot(ack_rx).await {
        Ok(ControlAck::Ok { message }) => tracing::info!(target: "ant-ffi", "{message}"),
        Ok(ControlAck::Error { message }) => {
            tracing::warn!(target: "ant-ffi", "disable settlement: {message}");
        }
        _ => {}
    }
}

/// Switch outbound SWAP settlement on in the running node for
/// `chequebook` (`EnablePushsyncSwap`, idempotent for the same
/// chequebook). Logs the node's answer.
#[cfg(feature = "chain")]
async fn enable_settlement(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    chequebook: [u8; 20],
    swap_secret: [u8; 32],
    data_dir: &std::path::Path,
) {
    let (ack_tx, ack_rx) = oneshot::channel();
    if send(
        cmd_tx,
        ControlCommand::EnablePushsyncSwap {
            chequebook,
            swap_secret,
            chain_id: GNOSIS_CHAIN_ID,
            outbound_ledger_path: data_dir
                .join("pushsync_outbound.json")
                .to_string_lossy()
                .into_owned(),
            ack: ack_tx,
        },
    )
    .await
    .is_err()
    {
        return;
    }
    match recv_oneshot(ack_rx).await {
        Ok(ControlAck::Ok { message }) => {
            tracing::info!(target: "ant-ffi", "{message}");
        }
        Ok(ControlAck::Error { message }) => {
            tracing::warn!(target: "ant-ffi", "enable settlement: {message}");
        }
        _ => {}
    }
}

/// Build the node wallet and run [`ensure_settlement`] — the wrapper the
/// discover / connect flows use so they share the buy flow's settlement
/// bootstrap. Best-effort: a wallet-init failure (bad key) just logs and
/// leaves settlement off; the Storage UI then shows the "settlement not
/// set up" state via [`settlement_status`].
#[cfg(feature = "chain")]
pub(crate) async fn ensure_settlement_best_effort(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    secret: [u8; 32],
    data_dir: &std::path::Path,
    node_eth: [u8; 20],
) -> Option<[u8; 20]> {
    match ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID) {
        Ok(wallet) => ensure_settlement(cmd_tx, client, &wallet, data_dir, secret, node_eth).await,
        Err(e) => {
            tracing::warn!(target: "ant-ffi", "settlement skipped (wallet init): {e}");
            None
        }
    }
}

/// Whether outbound SWAP settlement is active for this device, as a JSON
/// object `{"enabled":bool,"chequebook":"0x…"|null}`. Settlement is the
/// thing that lets uploads actually propagate (bee charges the uploader
/// for every pushed chunk and freezes out a node that can't pay), so the
/// Storage tab reads this to warn the user when a connected plan still
/// won't upload reliably. Local + cheap: it just reads the persisted
/// `chequebook.json` association (written when a chequebook was
/// deployed), no chain round-trip — minus a chequebook this process's
/// last chain check disqualified ([`setup_settlement`] switched
/// settlement off for it).
#[cfg(feature = "chain")]
pub(crate) fn settlement_status(h: &AntHandle) -> Result<String, DriveError> {
    let path = h.data_dir.join("chequebook.json");
    let (enabled, chequebook) =
        match ant_chain::chequebook_store::load_persisted_chequebook_for(&path, &h.eth) {
            // A chequebook the chain check disqualified has settlement
            // switched off (see `setup_settlement`).
            Ok(Some(cb)) if !lock_disqualified().contains(&(h.eth, cb)) => {
                (true, Some(format!("0x{}", hex::encode(cb))))
            }
            _ => (false, None),
        };
    to_json(&SettlementStatus {
        enabled,
        chequebook,
    })
}

/// The chequebook this account owns on this device, if any: the
/// persisted association, checked against `owner` so a record left by a
/// different account is never adopted (bee only honours a cheque signed
/// by the chequebook's own issuer). A record we can't read is reported
/// as "none" — non-destructive, and the caller then treats the account
/// as one whose chequebook is still to come.
#[cfg(feature = "chain")]
fn persisted_chequebook(data_dir: &std::path::Path, owner: &[u8; 20]) -> Option<[u8; 20]> {
    let path = data_dir.join("chequebook.json");
    match ant_chain::chequebook_store::load_persisted_chequebook_for(&path, owner) {
        Ok(cb) => cb,
        Err(e) => {
            tracing::warn!(target: "ant-ffi", "read chequebook association: {e}");
            None
        }
    }
}

/// Bring `chequebook` up to [`DEPOSIT_TARGET_PLUR`] from the node
/// wallet's spare xBZZ. Best-effort in every direction: an already-funded
/// chequebook, an unreadable balance, an empty wallet or a failed
/// transfer all just log — the caller is a storage flow that must not
/// fail over settlement housekeeping. A partial deposit (thin wallet) is
/// deliberately kept: some backing beats none.
///
/// Nothing is sent unless the shared top-up's own chain checks, run
/// right before the transfer, both read "yes". When they say no, the
/// lag-aware [`check_persisted_chequebook`] decides: a disqualifying
/// verdict comes back as `Some(reason)` for the caller to switch
/// settlement off; a just-deployed chequebook a lagging backend doesn't
/// know yet is just left unfunded this time.
#[cfg(feature = "chain")]
async fn fund_chequebook_best_effort(
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    node_eth: &[u8; 20],
    chequebook: &[u8; 20],
) -> Option<String> {
    use ant_chain::chequebook_store::{top_up_chequebook, TopUp};

    let funded = {
        let _tx = wallet_tx_lock(node_eth).lock_owned().await;
        top_up_chequebook(client, wallet, node_eth, chequebook, DEPOSIT_TARGET_PLUR).await
    };
    match funded {
        Ok(TopUp::Refused(_)) => {
            match check_persisted_chequebook(
                client,
                &data_dir.join("chequebook.json"),
                chequebook,
                node_eth,
            )
            .await
            {
                PersistedCheck::Disqualified(reason) => return Some(reason),
                PersistedCheck::Usable | PersistedCheck::Unverified(_) => tracing::warn!(
                    target: "ant-ffi",
                    chequebook = %format!("0x{}", hex::encode(chequebook)),
                    "the RPC doesn't confirm our chequebook yet; not depositing into it this time",
                ),
            }
        }
        Ok(TopUp::NotNeeded) => {}
        Ok(TopUp::Funded { amount, tx }) => tracing::info!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            deposit_plur = amount,
            tx = %format!("0x{}", hex::encode(tx)),
            "funded the chequebook so its cheques are backed",
        ),
        Ok(TopUp::WalletEmpty { .. }) => tracing::warn!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            "chequebook holds no settlement deposit and the wallet has no spare xBZZ; \
             uploads will stall once peers stop extending credit — top it up from the Storage tab",
        ),
        Err(e) => tracing::warn!(
            target: "ant-ffi",
            "settlement deposit left as-is; the chequebook still works but may back no cheque: {e}",
        ),
    }
    None
}

/// The chequebook's settlement deposit, read from chain, plus what a
/// top-up to [`DEPOSIT_TARGET_PLUR`] would cost — the "is this
/// chequebook actually backing its cheques?" card in the Storage tab,
/// and the migration path for every install that deployed one at deposit
/// 0 (issue #73).
///
/// Returns JSON `{"enabled","chequebook","deposit_plur","deposit_bzz",
/// "target_plur","target_bzz","shortfall_plur","shortfall_bzz",
/// "needs_top_up","xdai_required","xdai_required_display","xdai_to_send",
/// "xdai_to_send_display","sufficient_funds"}`. `enabled=false` (with
/// zeroed fields) when this account has no chequebook yet — there is
/// nothing to top up then; buying or connecting a plan deploys one,
/// funded — or when its chequebook was disqualified (see
/// [`settlement_deposit_for`]). Two or three light `eth_call`s, so it belongs on an explicit
/// refresh, not on every status poll.
#[cfg(feature = "chain")]
pub(crate) fn settlement_deposit(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let wallet = node_wallet(h)?;
    h.runtime.block_on(async move {
        settlement_deposit_for(&h.chain_client(rpc), &wallet, &h.data_dir, &h.eth).await
    })
}

/// [`settlement_deposit`]'s body, against an explicit chain client. A
/// chequebook this process's chain check disqualified reads as "none",
/// the same answer [`settlement_status`] gives: its settlement is off,
/// so the card must not offer to top it up.
#[cfg(feature = "chain")]
async fn settlement_deposit_for(
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    owner: &[u8; 20],
) -> Result<String, DriveError> {
    let Some(cb) = persisted_chequebook(data_dir, owner) else {
        return to_json(&SettlementDeposit::none());
    };
    if lock_disqualified().contains(&(*owner, cb)) {
        return to_json(&SettlementDeposit::none());
    }
    let payer = Payer::gnosis(client, wallet);
    let status = funding::deposit_status(&payer, &cb, DEPOSIT_TARGET_PLUR)
        .await
        .map_err(funding_err)?;
    to_json(&SettlementDeposit::of(&status))
}

/// Fund the node's chequebook up to [`DEPOSIT_TARGET_PLUR`], funding
/// **only with xDAI**: swap the xBZZ shortfall through the on-chain
/// helper if the wallet doesn't already hold it, then transfer the
/// deposit to the chequebook. The explicit top-up path — a deposit is
/// only read at deploy time, so an already-deployed chequebook can be
/// funded no other way.
///
/// Idempotent: a chequebook already at the target is a no-op. Returns the
/// refreshed [`settlement_deposit`] JSON. Submits real Gnosis
/// transactions and spends real funds, so the app gates it behind an
/// explicit confirmation.
///
/// With `amount`, deposits that many PLUR more instead, whatever the
/// target (a host topping up browsing credit beyond the default
/// deposit); the gateway's `POST /v0/settlement/deposit?amount=` runs
/// the same shared step.
#[cfg(feature = "chain")]
pub(crate) fn settlement_topup_xdai(
    h: &AntHandle,
    rpc: String,
    amount: Option<u128>,
) -> Result<String, DriveError> {
    let data_dir = h.data_dir.clone();
    let owner = h.eth;
    let secret = h.signing_secret;
    let cmd_tx = h.cmd_tx.clone();
    h.runtime.block_on(async move {
        settlement_topup_xdai_adding(
            &cmd_tx,
            &h.chain_client(rpc),
            &data_dir,
            owner,
            secret,
            &h.gateway_chequebook,
            amount,
        )
        .await
    })
}

/// [`settlement_topup_xdai`]'s body, against an explicit chain client.
/// Refuses a chequebook that fails its chain checks — peers drop its
/// cheques, so a deposit would only strand xBZZ in it. The checks run
/// here, before anything is spent, rather than trusting that a chain
/// init already ran them: a gateway started without an RPC (or whose
/// init is still in flight) leaves [`DISQUALIFIED`] empty. A check whose
/// read *failed* refuses too: unlike switching settlement on, a deposit
/// can't be taken back, so it waits for a verified "yes". A chequebook
/// found disqualified here gets the same treatment as in
/// [`setup_settlement`]: recorded and switched off in the node.
///
/// The spend itself is the shared [`funding::fund_deposit_with_xdai`]
/// (the gateway's `POST /v0/settlement/deposit` runs it too), which
/// checks the chequebook again before its swap and right before the
/// transfer; a refusal there goes through [`chequebook_refused`].
///
/// On a refusal the in-process gateway's chequebook `slot` follows: a
/// chequebook disqualified here is cleared from it, so `/wallet` and
/// `POST /chequebook/deposit` stop using it without a gateway restart.
#[cfg(all(feature = "chain", test))]
async fn settlement_topup_xdai_for(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    data_dir: &std::path::Path,
    owner: [u8; 20],
    secret: [u8; 32],
    slot: &ant_gateway::ChequebookSlot,
) -> Result<String, DriveError> {
    settlement_topup_xdai_adding(cmd_tx, client, data_dir, owner, secret, slot, None).await
}

/// [`settlement_topup_xdai_for`] with an optional explicit amount (see
/// [`settlement_topup_xdai`]).
#[cfg(feature = "chain")]
async fn settlement_topup_xdai_adding(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    data_dir: &std::path::Path,
    owner: [u8; 20],
    secret: [u8; 32],
    slot: &ant_gateway::ChequebookSlot,
    amount: Option<u128>,
) -> Result<String, DriveError> {
    let result =
        settlement_topup_xdai_checked(cmd_tx, client, data_dir, owner, secret, amount).await;
    if result.is_err() {
        // Clears the slot only if it holds a chequebook now in
        // `DISQUALIFIED`; any other failure leaves it alone.
        sync_gateway_chequebook(slot, &owner, None);
    }
    result
}

/// [`settlement_topup_xdai_for`] without the gateway-slot sync.
#[cfg(feature = "chain")]
async fn settlement_topup_xdai_checked(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    data_dir: &std::path::Path,
    owner: [u8; 20],
    secret: [u8; 32],
    amount: Option<u128>,
) -> Result<String, DriveError> {
    let cb = persisted_chequebook(data_dir, &owner).ok_or_else(|| {
        DriveError::Op(
            "no chequebook for this account yet — connect or buy a storage plan first".into(),
        )
    })?;
    if lock_disqualified().contains(&(owner, cb)) {
        return Err(DriveError::Op(format!(
            "chequebook 0x{} failed its on-chain checks, so settlement is off for it; \
             not depositing into it",
            hex::encode(cb),
        )));
    }
    match check_persisted_chequebook(client, &data_dir.join("chequebook.json"), &cb, &owner).await {
        PersistedCheck::Usable => {}
        PersistedCheck::Unverified(e) => {
            return Err(DriveError::Op(format!(
                "could not verify chequebook 0x{} on-chain ({e}); not depositing into it \
                 until it checks out — try again",
                hex::encode(cb),
            )));
        }
        PersistedCheck::Disqualified(reason) => {
            lock_disqualified().insert((owner, cb));
            disable_settlement(cmd_tx, cb).await;
            return Err(DriveError::Op(format!("{reason}; not depositing into it")));
        }
    }
    let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
        .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
    let payer = Payer::gnosis(client, &wallet);
    // Deposit read, balance reads, swap and deposit transfer all under
    // the account's wallet tx lock (see [`wallet_tx_lock`]). The
    // shortfall is read *after* taking the lock (inside
    // `fund_deposit_with_xdai`): an after-buy
    // [`fund_chequebook_best_effort`] (or a second tap) holding it may
    // be depositing that very shortfall right now, and a value read
    // before we waited would send it a second time.
    let funded = {
        let _tx = wallet_tx_lock(&owner).lock_owned().await;
        funding::fund_deposit_with_xdai(&payer, &cb, DEPOSIT_TARGET_PLUR, amount).await
    };
    match funded {
        Ok(status) => {
            refresh_retrieval_funds(cmd_tx, client, cb).await;
            to_json(&SettlementDeposit::of(&status))
        }
        Err(
            e @ funding::FundingError::ChequebookRefused {
                chequebook,
                verdict,
            },
        ) => {
            let not_registered =
                verdict == ant_chain::chequebook_store::ChequebookVerdict::NotRegistered;
            if chequebook_refused(cmd_tx, client, data_dir, owner, chequebook, not_registered).await
            {
                Err(funding_err(e))
            } else {
                Err(DriveError::Op(CHEQUEBOOK_NOT_CAUGHT_UP.into()))
            }
        }
        Err(e) => Err(funding_err(e)),
    }
}

/// The answer to a deposit top-up whose chequebook refusal may just be
/// an RPC that hasn't seen our own deploy yet (see [`chequebook_refused`]).
#[cfg(feature = "chain")]
const CHEQUEBOOK_NOT_CAUGHT_UP: &str = "the chain RPC hasn't caught up with this account's \
     just-deployed chequebook yet; nothing was sent, try again in a few minutes";

/// A chain refusal of `chequebook` for `owner` that a deposit top-up hit
/// right before spending (nothing was sent): whether it stands.
///
/// A factory "not registered" (`not_registered`) for the chequebook we
/// deployed moments ago goes through the shared
/// [`not_registered_may_be_lag`](ant_chain::chequebook_store::not_registered_may_be_lag)
/// first: `false`, nothing switched off. Otherwise the chequebook gets
/// [`setup_settlement`]'s treatment — recorded in [`DISQUALIFIED`] and
/// switched off in the node — and this returns `true`. Shared by the C
/// API's [`settlement_topup_xdai`] and the in-process gateway's
/// `POST /v0/settlement/deposit` (through its refused-chequebook hook).
#[cfg(feature = "chain")]
pub(crate) async fn chequebook_refused(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    data_dir: &std::path::Path,
    owner: [u8; 20],
    chequebook: [u8; 20],
    not_registered: bool,
) -> bool {
    if not_registered
        && ant_chain::chequebook_store::not_registered_may_be_lag(
            client,
            &data_dir.join("chequebook.json"),
            &chequebook,
        )
        .await
    {
        tracing::warn!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            "the RPC doesn't confirm our just-deployed chequebook yet; not depositing into it \
             this time",
        );
        return false;
    }
    lock_disqualified().insert((owner, chequebook));
    disable_settlement(cmd_tx, chequebook).await;
    true
}

/// Deploy (or return the already-persisted) node-owned chequebook for the
/// iOS publish-setup checklist's "chequebook deployed" step. Idempotent:
/// if `<data_dir>/chequebook.json` already records a chequebook, it's
/// returned without a redeploy (though a chequebook still short of its
/// settlement deposit is topped up); otherwise this signs an on-chain
/// `factory.deploySimpleSwap`, funds it with [`DEPOSIT_TARGET_PLUR`]
/// xBZZ so its cheques are backed, persists the association, and returns
/// the new address. Blocks on the handle's tokio runtime.
///
/// It also switches outbound settlement on in the running node, so a
/// chequebook deployed now is used this session rather than from the
/// next launch.
///
/// Returns `{"chequebookAddress":"0x<40hex>"}` JSON. A running
/// in-process gateway's `/chequebook/address` and `/wallet` report the
/// chequebook at once (no gateway restart needed); one the chain check
/// disqualified is cleared from them.
#[cfg(feature = "chain")]
pub(crate) fn deploy_chequebook(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let secret = h.signing_secret;
    let node_eth = h.eth;
    let data_dir = h.data_dir.clone();
    let cmd_tx = h.cmd_tx.clone();

    h.runtime.block_on(async move {
        let client = h.chain_client(rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("derive node wallet: {e}")))?;

        // persisted → rediscover the wallet's existing on-chain chequebook
        // → deploy a fresh one (persisting the association). Shared with
        // `antd`'s resolve/rediscover/deploy mechanics, so a wallet that
        // already deployed a chequebook (e.g. on desktop with the same
        // vault) is adopted rather than duplicated. An adopted chequebook
        // may still be at deposit 0, so it's funded here too; this
        // checklist step means the same thing whichever branch produced
        // the address.
        // The running gateway follows, so `/chequebook/address` reports
        // the new chequebook without another `ant_start_gateway` (and
        // drops one the chain check just disqualified).
        let chequebook = setup_settlement_for_gateway(
            &cmd_tx,
            &client,
            &wallet,
            &data_dir,
            secret,
            node_eth,
            &h.gateway_chequebook,
        )
        .await?;
        match chequebook {
            Some(chequebook) => to_json(&DeployedChequebook {
                chequebook_address: format!("0x{}", hex::encode(chequebook)),
            }),
            None => Err(DriveError::Op(
                "chequebook could not be deployed — wallet has no xDAI for gas".into(),
            )),
        }
    })
}

/// What [`resolve_or_deploy_chequebook`] concluded.
#[cfg(feature = "chain")]
enum Resolution {
    /// Use this chequebook.
    Use(ResolvedChequebook),
    /// No chequebook to use yet (none exists, and deploying one isn't
    /// allowed or affordable).
    NoneYet,
    /// The persisted chequebook failed a chain check. Not a reason to
    /// deploy another — which one to use is for the user to sort out —
    /// but settlement must not stay on it.
    Disqualified {
        chequebook: [u8; 20],
        reason: String,
    },
}

/// What [`check_persisted_chequebook`] concluded about a persisted
/// chequebook.
#[cfg(feature = "chain")]
enum PersistedCheck {
    /// Every check answered and passed.
    Usable,
    /// A check answered "no"; the reason, for the user.
    Disqualified(String),
    /// No check said "no", but at least one read failed.
    Unverified(String),
}

/// The chain checks `antd` runs before signing cheques on a chequebook
/// (factory registration, `issuer()`), with the shared
/// [`ChequebookVerdict`](ant_chain::chequebook_store::ChequebookVerdict)
/// rule plus the just-deployed lag grace. Whether an unverified result
/// is good enough is the caller's call: enabling settlement accepts it,
/// spending on the chequebook doesn't.
#[cfg(feature = "chain")]
async fn check_persisted_chequebook(
    client: &ant_chain::ChainClient,
    persist_path: &std::path::Path,
    cb: &[u8; 20],
    node_eth: &[u8; 20],
) -> PersistedCheck {
    use ant_chain::chequebook_store::{self, ChequebookVerdict, IssuerRead};

    let checks =
        chequebook_store::check_chequebook(client, cb, IssuerRead::UnlessUnregistered).await;
    let mut verdict = checks.verdict(node_eth);
    if verdict == ChequebookVerdict::NotRegistered
        && chequebook_store::not_registered_may_be_lag(client, persist_path, cb).await
    {
        tracing::warn!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(cb)),
            "the RPC reports our just-deployed chequebook as unregistered, but it hasn't \
             caught up with the deploy yet; using it",
        );
        verdict = ChequebookVerdict::Usable;
    }
    match verdict {
        ChequebookVerdict::Usable => {
            let failed = [
                checks.registered.as_ref().err(),
                checks.issuer.as_ref().and_then(|r| r.as_ref().err()),
            ];
            match failed.into_iter().flatten().next() {
                Some(e) => PersistedCheck::Unverified(e.to_string()),
                None => PersistedCheck::Usable,
            }
        }
        ChequebookVerdict::NotRegistered => PersistedCheck::Disqualified(format!(
            "chequebook 0x{} is not registered with the Swarm chequebook factory; \
             peers drop every cheque drawn on it, so settlement stays off",
            hex::encode(cb),
        )),
        ChequebookVerdict::IssuerMismatch(issuer) => PersistedCheck::Disqualified(format!(
            "chequebook 0x{} is issued by 0x{}, not this account (0x{}); \
             peers would drop every cheque we sign on it, so settlement stays off",
            hex::encode(cb),
            hex::encode(issuer),
            hex::encode(node_eth),
        )),
    }
}

/// A chequebook resolved for this account, and how we got to it.
#[cfg(feature = "chain")]
struct ResolvedChequebook {
    /// The 20-byte chequebook contract address.
    address: [u8; 20],
    /// `true` when *this* call deployed it — and therefore already
    /// funded it with [`DEPOSIT_TARGET_PLUR`] as part of the deploy.
    /// `false` for a persisted / rediscovered chequebook, whose deposit
    /// is whatever it happens to hold (zero, for anything deployed
    /// before #73).
    deployed: bool,
    /// `true` when this resolution *positively* checked it out (every
    /// chain check read and passed), so it may lift an earlier
    /// disqualification in [`DISQUALIFIED`]. A chequebook adopted on an
    /// unverified check (a read failed) or rediscovered without the
    /// checks keeps a disqualification it already has.
    verified: bool,
}

/// Reuse / rediscover / deploy a chequebook for `node_eth`, persisting
/// the association so future launches reload it directly. Returns the
/// resolved chequebook, [`Resolution::NoneYet`] when there's none to
/// use (the account has no chequebook and either `may_deploy` is off or
/// the wallet can't afford the one-time deploy — a soft skip, not an
/// error), or [`Resolution::Disqualified`] when the persisted one fails
/// its chain checks. A rediscovery scan that *failed* is an error, never
/// a fall-through to the deploy: only an authoritative "this account
/// owns no chequebook" may trigger one — and when the persisted record
/// was unreadable, only once the record's own bytes name no chequebook
/// of ours (see step 2). Call it through [`setup_settlement`], which holds
/// the lock that keeps two callers from both deploying.
/// The persist / rediscover / deploy mechanics are shared with `antd` via
/// [`ant_chain::chequebook_store`]; only the resolution *order* (no
/// operator-flag branches) and the deposit sizing live here.
#[cfg(feature = "chain")]
async fn resolve_or_deploy_chequebook(
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    node_eth: [u8; 20],
    may_deploy: bool,
) -> Result<Resolution, DriveError> {
    use ant_chain::chequebook_store::{self, ChequebookError, ChequebookFile};

    let persist_path = data_dir.join("chequebook.json");

    // 1. Already known (persisted from a prior run / this session), and
    //    issued by *this* account — a record left behind by a different
    //    account is skipped, so we rediscover / deploy our own below
    //    instead of signing cheques nobody will honour.
    //
    //    An unreadable record isn't the end of settlement: as a hard
    //    error it disabled settlement for good, since every buy failed on
    //    it and nothing ever rewrote the file. The rediscovery below finds
    //    the chequebook it pointed at if that one holds xBZZ, and rewrites
    //    the record. But the scan can't see a deposit-0 chequebook (every
    //    install before #73), so its "none" doesn't prove there's no
    //    chequebook behind the record: step 2 then recovers the address
    //    from the record's bytes before it will deploy.
    let (persisted, record_unreadable) = match chequebook_store::load_persisted_chequebook_for(
        &persist_path,
        &node_eth,
    ) {
        Ok(p) => (p, false),
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                "ignoring unreadable chequebook association ({e}); looking the chequebook up on-chain instead",
            );
            (None, true)
        }
    };
    if let Some(cb) = persisted {
        // Same checks `antd` runs before signing cheques on a chequebook
        // (factory registration, `issuer()`), same shared rule: a "no"
        // disqualifies it, a failed read doesn't. A disqualified
        // chequebook is not a reason to deploy another; which one to use
        // is for the user to sort out.
        match check_persisted_chequebook(client, &persist_path, &cb, &node_eth).await {
            // Unverified is not bad: dropping settlement over an RPC
            // hiccup stalls uploads.
            check @ (PersistedCheck::Usable | PersistedCheck::Unverified(_)) => {
                return Ok(Resolution::Use(ResolvedChequebook {
                    address: cb,
                    deployed: false,
                    verified: matches!(check, PersistedCheck::Usable),
                }))
            }
            PersistedCheck::Disqualified(reason) => {
                return Ok(Resolution::Disqualified {
                    chequebook: cb,
                    reason,
                })
            }
        }
    }

    // 2. Rediscover a chequebook this node EOA already owns on-chain
    //    (reinstall with a restored key). Adopt + persist it.
    //    Reads the saved transfer scan; a "none" that would lead to the
    //    deploy below is first confirmed by reading again the blocks no
    //    full pass has confirmed (the whole history only once per wallet;
    //    the mark is saved with the scan), so a saved scan that missed a
    //    deposit can't strand it behind a second chequebook.
    let owned = ant_chain::discover::find_owned_chequebook(
        client,
        &ant_chain::chequebook::GNOSIS_CHEQUEBOOK_FACTORY,
        ant_chain::GNOSIS_POSTAGE_STAMP,
        ant_chain::GNOSIS_BZZ_TOKEN,
        &node_eth,
        data_dir,
        may_deploy,
    )
    .await;
    match owned {
        Ok(Some(cb)) => {
            if let Err(e) = chequebook_store::persist_chequebook(
                &persist_path,
                &ChequebookFile::rediscovered(&cb, &node_eth),
            ) {
                tracing::warn!(target: "ant-ffi", "persist rediscovered chequebook: {e}");
            }
            tracing::info!(
                target: "ant-ffi",
                chequebook = %format!("0x{}", hex::encode(cb)),
                "rediscovered node-owned chequebook on-chain; adopting it",
            );
            return Ok(Resolution::Use(ResolvedChequebook {
                address: cb,
                deployed: false,
                verified: false,
            }));
        }
        // Authoritative "this EOA owns no funded chequebook" — the only
        // answer that may fall through to the deploy below.
        //
        // An unreadable record may still name a deposit-0 chequebook the
        // funded-only scan can't see. Recover it from the file's bytes
        // when they still hold its address (the common truncated / half-
        // written case) and it checks out on-chain as ours. When they
        // don't, the file is moved aside rather than left to block every
        // deploy: a mobile user can't reach it to fix or remove it, and
        // whatever it named holds no deposit (the scan would have found
        // one), so a second chequebook costs only deploy gas.
        Ok(None) => {
            if record_unreadable {
                match salvage_unreadable_record(client, &persist_path, &node_eth).await {
                    Salvage::Found(cb) => {
                        if let Err(e) = chequebook_store::persist_chequebook(
                            &persist_path,
                            &ChequebookFile::rediscovered(&cb, &node_eth),
                        ) {
                            tracing::warn!(target: "ant-ffi", "rewrite chequebook association: {e}");
                        }
                        tracing::info!(
                            target: "ant-ffi",
                            chequebook = %format!("0x{}", hex::encode(cb)),
                            "recovered our chequebook from the unreadable association; adopting it",
                        );
                        // Salvage only accepts a positive answer to both
                        // checks.
                        return Ok(Resolution::Use(ResolvedChequebook {
                            address: cb,
                            deployed: false,
                            verified: true,
                        }));
                    }
                    Salvage::Unknown(e) if may_deploy => {
                        return Err(DriveError::Op(format!(
                            "{} is unreadable and checking the chequebook it names failed ({e}); \
                             not deploying a second one on chain state we could not read",
                            persist_path.display(),
                        )));
                    }
                    Salvage::Unknown(_) | Salvage::Nothing => {}
                }
            }
            if !may_deploy {
                return Ok(Resolution::NoneYet);
            }
            if record_unreadable {
                let parked = unused_park_path(&persist_path);
                std::fs::rename(&persist_path, &parked).map_err(|e| {
                    DriveError::Op(format!(
                        "move unreadable {} aside: {e}",
                        persist_path.display()
                    ))
                })?;
                tracing::warn!(
                    target: "ant-ffi",
                    parked = %parked.display(),
                    "the unreadable chequebook association names no chequebook of ours; \
                     moved it aside and deploying a fresh one",
                );
            }
        }
        Err(e) => {
            // A scan that *failed* is not "no chequebook exists". Falling
            // through would deploy and fund a second chequebook on chain
            // state we could not read, burning gas and stranding the
            // existing chequebook's deposit. Skip chequebook setup for
            // this run instead: staying without settlement is
            // recoverable — the next gateway start, buy, plan connect,
            // discover or explicit deploy rescans — a stranded deposit is
            // not. The cost of the trade is real and deliberate: a node
            // whose chain reads keep failing runs without a chequebook
            // rather than deploying one.
            tracing::warn!(
                target: "ant-ffi",
                "chequebook rediscovery scan failed: {e}; network settlement stays OFF — not \
                 deploying a chequebook on chain state we could not read (the next gateway \
                 start, buy, plan connect, discover or explicit deploy retries the scan)",
            );
            return Err(DriveError::Op(format!(
                "chequebook rediscovery scan failed, so we cannot tell whether this account \
                 already owns a chequebook; not deploying a second one: {e}"
            )));
        }
    }

    // 3. Auto-deploy, funded with [`DEPOSIT_TARGET_PLUR`]: bee accepts
    //    the cheques either way, but a chequebook backing nothing only
    //    publishes until the peers' payment tolerance runs out and then
    //    stalls (#73) — so the deposit goes in at deploy time, capped by
    //    the wallet's xBZZ balance (a thin wallet gets a smaller deposit
    //    rather than a failed transfer). Insufficient gas is a soft skip
    //    — settlement turns on once the wallet has a little more xDAI.
    let deployed = {
        let _tx = wallet_tx_lock(&node_eth).lock_owned().await;
        chequebook_store::auto_deploy_chequebook(
            client,
            wallet,
            &node_eth,
            DEPOSIT_TARGET_PLUR,
            &persist_path,
        )
        .await
    };
    match deployed {
        Ok(cb) => {
            // Brand new, so it has issued no cheques: a lost outbound
            // ledger on record doesn't apply to it (PR #126 R1-M3).
            if let Err(e) =
                ant_p2p::swap::note_fresh_chequebook(&data_dir.join("pushsync_outbound.json"), cb)
            {
                tracing::warn!(
                    target: "ant-ffi",
                    "can't record the new chequebook in the lost-ledger marker: {e}; \
                     downloads and uploads stay on the free tier until confirmed",
                );
            }
            Ok(Resolution::Use(ResolvedChequebook {
                address: cb,
                deployed: true,
                verified: true,
            }))
        }
        Err(ChequebookError::InsufficientGas { need, .. }) => {
            tracing::warn!(
                target: "ant-ffi",
                "not enough spare xDAI to deploy a chequebook (need ~{need} wei); \
                 network settlement will turn on after you add a little more xDAI and buy again",
            );
            Ok(Resolution::NoneYet)
        }
        Err(e) => Err(map_cb_err(e)),
    }
}

/// Where to move an unreadable `chequebook.json` aside:
/// `chequebook.json.unreadable`, or the first free
/// `chequebook.json.unreadable.N` when an earlier episode's parked file
/// is still there — a rename would silently replace it, and it is kept
/// for diagnosis. Called under [`CHEQUEBOOK_SETUP`], so nothing else
/// parks between the probe and the rename.
#[cfg(feature = "chain")]
fn unused_park_path(persist_path: &std::path::Path) -> std::path::PathBuf {
    let mut parked = persist_path.with_extension("json.unreadable");
    let mut n = 0u64;
    while parked.exists() {
        n += 1;
        parked = persist_path.with_extension(format!("json.unreadable.{n}"));
    }
    parked
}

/// What an unreadable `chequebook.json` still tells us, per
/// [`salvage_unreadable_record`].
#[cfg(feature = "chain")]
enum Salvage {
    /// An address in the file is a factory-registered chequebook issued
    /// by this account.
    Found([u8; 20]),
    /// A candidate's checks couldn't be read, so it may be ours.
    Unknown(String),
    /// No candidate address, or the chain positively rejected each one.
    Nothing,
}

/// Look for our chequebook's address in the raw bytes of an unreadable
/// record (every `0x` + 40-hex run other than `owner`, i.e. the
/// `chequebook` field when it survived) and confirm it on-chain: only a
/// positively registered chequebook whose `issuer()` is `owner` counts.
/// Unlike [`ChequebookChecks::verdict`], a failed read isn't a pass here —
/// the bytes are untrusted, so it yields [`Salvage::Unknown`].
#[cfg(feature = "chain")]
async fn salvage_unreadable_record(
    client: &ant_chain::ChainClient,
    persist_path: &std::path::Path,
    owner: &[u8; 20],
) -> Salvage {
    use ant_chain::chequebook_store::{check_chequebook, IssuerRead};

    // A file we can't even read (I/O error) isn't known to be garbage.
    let bytes = match std::fs::read(persist_path) {
        Ok(b) => b,
        Err(e) => return Salvage::Unknown(format!("read {}: {e}", persist_path.display())),
    };
    let mut unknown = None;
    for cb in addresses_in(&bytes) {
        if cb == *owner {
            continue;
        }
        let checks = check_chequebook(client, &cb, IssuerRead::UnlessUnregistered).await;
        match (&checks.registered, &checks.issuer) {
            (Ok(true), Some(Ok(issuer))) if issuer == owner => return Salvage::Found(cb),
            // A "no" from the chain: not ours.
            (Ok(false), _) => {}
            (_, Some(Ok(issuer))) if issuer != owner => {}
            // Registered unknown, or issuer unread: may still be ours.
            (Err(e), _) | (_, Some(Err(e))) => unknown = Some(e.to_string()),
            (Ok(true), _) => unknown = Some("issuer() not read".into()),
        }
    }
    unknown.map_or(Salvage::Nothing, Salvage::Unknown)
}

/// Every distinct `0x`-prefixed 40-hex-digit run in `bytes` (a longer
/// run, like a tx hash, doesn't count).
#[cfg(feature = "chain")]
fn addresses_in(bytes: &[u8]) -> Vec<[u8; 20]> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < bytes.len() {
        if bytes[i] == b'0' && bytes[i + 1].eq_ignore_ascii_case(&b'x') {
            let run = bytes[i + 2..]
                .iter()
                .take_while(|b| b.is_ascii_hexdigit())
                .count();
            let mut a = [0u8; 20];
            if run == 40
                && hex::decode_to_slice(&bytes[i + 2..i + 42], &mut a).is_ok()
                && !out.contains(&a)
            {
                out.push(a);
            }
            i += 2 + run;
        } else {
            i += 1;
        }
    }
    out
}

/// Map a shared-chequebook-store error into the drive op error.
#[cfg(feature = "chain")]
fn map_cb_err(e: ant_chain::chequebook_store::ChequebookError) -> DriveError {
    DriveError::Op(e.to_string())
}

/// Render a PLUR amount as a short xBZZ decimal string (4 dp).
#[cfg(feature = "chain")]
fn format_bzz(plur: u128) -> String {
    use funding::PLUR_PER_BZZ;
    let whole = plur / PLUR_PER_BZZ;
    let frac = (plur % PLUR_PER_BZZ) / (PLUR_PER_BZZ / 10_000); // 4 decimals
    format!("{whole}.{frac:04}")
}

/// Render a wei amount as a short xDAI decimal string (4 dp).
#[cfg(feature = "chain")]
fn format_native(wei: u128) -> String {
    use funding::WEI_PER_XDAI;
    let whole = wei / WEI_PER_XDAI;
    let frac = (wei % WEI_PER_XDAI) / (WEI_PER_XDAI / 10_000); // 4 decimals
    format!("{whole}.{frac:04}")
}

#[cfg(feature = "chain")]
fn parse_batch_id(s: &str) -> Result<[u8; 32], DriveError> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != 64 {
        return Err(DriveError::Op(format!(
            "storage id must be 32 bytes (64 hex chars), got {}",
            s.len()
        )));
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out)
        .map_err(|e| DriveError::Op(format!("invalid storage id: {e}")))?;
    Ok(out)
}

// ---------------------------------------------------------------------------
// Wire shapes + helpers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Jobs {
    jobs: Vec<ant_control::UploadJobView>,
}

#[derive(Serialize)]
struct AccountInfo {
    eth_address: String,
    overlay: String,
    peer_id: String,
    agent: String,
}

/// Wire shape for [`settlement_status`].
#[cfg(feature = "chain")]
#[derive(Serialize)]
struct SettlementStatus {
    /// `true` once a chequebook is deployed + persisted, i.e. outbound
    /// pushsync cheques can be issued and uploads will propagate.
    enabled: bool,
    /// `0x`-prefixed chequebook contract address when enabled.
    chequebook: Option<String>,
}

/// Wire shape for [`settlement_deposit`] / [`settlement_topup_xdai`]:
/// what actually stands behind this account's cheques, and what closing
/// the gap would cost.
#[cfg(feature = "chain")]
#[derive(Serialize)]
struct SettlementDeposit {
    /// `true` once a chequebook exists for this account — i.e. there is
    /// something a deposit could go into.
    enabled: bool,
    /// `0x`-prefixed chequebook contract address when enabled.
    chequebook: Option<String>,
    /// xBZZ currently behind the chequebook (PLUR, and a short display
    /// string). Zero is the pre-#73 state: cheques are issued and
    /// accepted, but nothing backs them.
    deposit_plur: String,
    deposit_bzz: String,
    /// The deposit we aim for — [`DEPOSIT_TARGET_PLUR`].
    target_plur: String,
    target_bzz: String,
    /// What is still missing (`target − deposit`, saturating).
    shortfall_plur: String,
    shortfall_bzz: String,
    /// `shortfall > 0`, via `DepositStatus::needs_top_up` — the one
    /// predicate the UI summary and the top-up action both read.
    needs_top_up: bool,
    /// Total xDAI (wei + display) the account must hold to run the
    /// top-up: the swap input for the missing xBZZ plus a gas reserve.
    /// Zero when nothing is missing.
    xdai_required: String,
    xdai_required_display: String,
    /// Additional xDAI the user still needs to send (`required −
    /// balance`).
    xdai_to_send: String,
    xdai_to_send_display: String,
    sufficient_funds: bool,
}

#[cfg(feature = "chain")]
impl SettlementDeposit {
    /// The "no chequebook on this device yet" payload: nothing to top
    /// up, because buying or connecting a plan deploys one already
    /// funded.
    /// The card payload for a chequebook's deposit, priced like a plan
    /// quote: the swap input for the missing xBZZ plus a gas reserve,
    /// against the account's xDAI (all zero when nothing is missing).
    fn of(status: &funding::DepositStatus) -> Self {
        let f = &status.funding;
        Self {
            enabled: true,
            chequebook: Some(format!("0x{}", hex::encode(status.chequebook))),
            deposit_plur: status.deposited_plur.to_string(),
            deposit_bzz: format_bzz(status.deposited_plur),
            target_plur: status.target_plur.to_string(),
            target_bzz: format_bzz(status.target_plur),
            shortfall_plur: status.shortfall_plur.to_string(),
            shortfall_bzz: format_bzz(status.shortfall_plur),
            needs_top_up: status.needs_top_up(),
            xdai_required: f.xdai_required_wei.to_string(),
            xdai_required_display: format_native(f.xdai_required_wei),
            xdai_to_send: f.xdai_to_send_wei.to_string(),
            xdai_to_send_display: format_native(f.xdai_to_send_wei),
            sufficient_funds: f.sufficient,
        }
    }

    fn none() -> Self {
        Self {
            enabled: false,
            chequebook: None,
            deposit_plur: "0".into(),
            deposit_bzz: format_bzz(0),
            target_plur: DEPOSIT_TARGET_PLUR.to_string(),
            target_bzz: format_bzz(DEPOSIT_TARGET_PLUR),
            shortfall_plur: "0".into(),
            shortfall_bzz: format_bzz(0),
            needs_top_up: false,
            xdai_required: "0".into(),
            xdai_required_display: format_native(0),
            xdai_to_send: "0".into(),
            xdai_to_send_display: format_native(0),
            sufficient_funds: true,
        }
    }
}

/// Wire shape for [`deploy_chequebook`]: the resolved (persisted or
/// freshly deployed) chequebook contract address.
#[cfg(feature = "chain")]
#[derive(Serialize)]
struct DeployedChequebook {
    /// `0x`-prefixed chequebook contract address (`0x` + 40 hex).
    #[serde(rename = "chequebookAddress")]
    chequebook_address: String,
}

/// Remaining-lifetime payload for the storage card (see
/// [`storage_validity`]).
#[cfg(feature = "chain")]
#[derive(Serialize)]
struct Validity {
    /// False when no plan is connected; the other fields are then 0.
    enabled: bool,
    /// Seconds of storage left before the batch expires.
    remaining_seconds: u64,
    /// Absolute Unix expiry (now + `remaining_seconds`), for a date label.
    expires_unix: u64,
}

#[cfg(feature = "chain")]
#[derive(Serialize)]
struct Quote {
    depth: u8,
    days: u64,
    amount_per_chunk: String,
    total_cost_plur: String,
    total_cost_bzz: String,
    /// One-time xBZZ this purchase also puts behind the node's
    /// chequebook so its cheques are backed (PLUR + display string).
    /// `0` when the chequebook is already funded, or for a plan
    /// extension, which deploys nothing. Included in `needed_bzz` /
    /// `xdai_required` / `xdai_to_send` / `sufficient_funds`, not in
    /// `total_cost_*`.
    settlement_deposit_plur: String,
    settlement_deposit_bzz: String,
    capacity_bytes: u64,
    account_bzz: String,
    account_bzz_display: String,
    account_xdai: String,
    account_xdai_display: String,
    /// xBZZ still needed for the plan (PLUR), i.e. cost minus balance.
    needed_bzz: String,
    needed_bzz_display: String,
    /// Total xDAI the account must hold to activate via auto-swap: the
    /// (buffered) swap input plus a gas reserve.
    xdai_required: String,
    xdai_required_display: String,
    /// Additional xDAI the user still needs to send (`required − balance`).
    xdai_to_send: String,
    xdai_to_send_display: String,
    sufficient_funds: bool,
}

async fn send(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    cmd: ControlCommand,
) -> Result<(), DriveError> {
    cmd_tx
        .send(cmd)
        .await
        .map_err(|_| DriveError::Op("node loop is not accepting commands".into()))
}

async fn recv_oneshot(rx: oneshot::Receiver<ControlAck>) -> Result<ControlAck, DriveError> {
    recv_oneshot_within(rx, OP_TIMEOUT).await
}

async fn recv_oneshot_within(
    rx: oneshot::Receiver<ControlAck>,
    timeout: Duration,
) -> Result<ControlAck, DriveError> {
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(ack)) => Ok(ack),
        Ok(Err(_)) => Err(DriveError::Op("node dropped the ack channel".into())),
        Err(_) => Err(DriveError::Op("operation timed out".into())),
    }
}

fn to_json<T: Serialize>(v: &T) -> Result<String, DriveError> {
    serde_json::to_string(v).map_err(|e| DriveError::Op(format!("encode response: {e}")))
}

fn unexpected(ack: &ControlAck) -> DriveError {
    DriveError::Op(format!("unexpected node response: {ack:?}"))
}

#[cfg(all(test, feature = "chain"))]
mod chain_tests {
    use super::resolve_or_deploy_chequebook;
    use ant_chain::{ChainClient, ChainTransport};
    use serde_json::json;
    use std::sync::Mutex;

    /// Records every method and fails it: enough to see whether a path
    /// touched the chain at all.
    struct FailingChain {
        seen: Mutex<Vec<String>>,
    }

    impl ChainTransport for FailingChain {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            self.seen
                .lock()
                .unwrap()
                .push(req["method"].as_str().unwrap().to_string());
            Some(
                json!({"jsonrpc": "2.0", "id": req["id"],
                       "error": {"code": -32603, "message": "scripted failure"}})
                .to_string(),
            )
        }
    }

    /// R2-M1: the gateway's `ChainContext` and ant-ffi's own spend paths
    /// share one wallet tx lock per account, so the after-buy deposit
    /// top-up can't start its balance reads and transfer while a
    /// `POST /stamps` (which holds the context's lock) is mid-buy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deposit_top_up_waits_for_the_gateway_wallet_tx_lock() {
        let wallet = ant_chain::tx::Wallet::new([0x5a; 32], crate::GNOSIS_CHAIN_ID).unwrap();
        let node_eth = *wallet.address();
        let lock = super::wallet_tx_lock(&node_eth);
        assert!(std::sync::Arc::ptr_eq(
            &lock,
            &super::wallet_tx_lock(&node_eth)
        ));
        assert!(!std::sync::Arc::ptr_eq(
            &lock,
            &super::wallet_tx_lock(&[0x11; 20])
        ));

        let ctx = ant_gateway::chainreader::build_with_transport(
            Some("http://127.0.0.1:1".into()),
            None,
            ant_chain::GNOSIS_POSTAGE_STAMP.to_string(),
            node_eth,
            None,
            crate::GNOSIS_CHAIN_ID,
            Some([0x5a; 32]),
            Some(super::DEPOSIT_TARGET_PLUR),
            None,
            lock.clone(),
        )
        .unwrap();
        assert!(std::sync::Arc::ptr_eq(&ctx.tx_lock, &lock));

        let script = std::sync::Arc::new(FailingChain {
            seen: Mutex::new(Vec::new()),
        });
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()));
        let dir = std::env::temp_dir().join(format!("ant-txlock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // A gateway buy in flight.
        let buy = ctx.tx_lock.clone().lock_owned().await;
        let task = {
            let dir = dir.clone();
            tokio::spawn(async move {
                super::fund_chequebook_best_effort(&client, &wallet, &dir, &node_eth, &[0xcb; 20])
                    .await
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            script.seen.lock().unwrap().is_empty(),
            "the top-up touched the chain while the buy held the lock",
        );
        drop(buy);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .expect("top-up finishes once the buy releases the lock");
        assert!(!script.seen.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A `to` address the node EOA once funded — the single candidate
    /// the rediscovery scan has to verify.
    const CANDIDATE: [u8; 20] = [0xcb; 20];
    const HIT_BLOCK: u64 = 0x1dd_8ac8;

    fn word_hex(bytes: &[u8]) -> String {
        let mut w = [0u8; 32];
        w[32 - bytes.len()..].copy_from_slice(bytes);
        format!("0x{}", hex::encode(w))
    }

    fn deployed_contracts_selector() -> String {
        let data = ant_chain::chequebook::factory_deployed_contracts_calldata(&CANDIDATE);
        format!("0x{}", hex::encode(&data[0..4]))
    }

    /// Scripted Gnosis backend, plugged in through this PR's own
    /// host-transport seam: the `Transfer` scan finds one candidate, and
    /// the `eth_call` that would verify it fails transiently. Every
    /// method is recorded so the test can assert what was *not* sent.
    struct ScriptedChain {
        node_eth: [u8; 20],
        seen: Mutex<Vec<String>>,
    }

    impl ChainTransport for ScriptedChain {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let method = req["method"].as_str().unwrap().to_string();
            self.seen.lock().unwrap().push(method.clone());
            let id = req["id"].clone();
            let rpc_error = |code: i64, message: &str| {
                Some(
                    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
                        .to_string(),
                )
            };
            let result = match method.as_str() {
                "eth_chainId" => json!("0x64"),
                "eth_blockNumber" => json!(format!("0x{:x}", HIT_BLOCK + 500)),
                "eth_getLogs" => json!([{
                    "address": ant_chain::GNOSIS_BZZ_TOKEN,
                    "topics": [
                        format!("0x{}", hex::encode(ant_chain::discover::ERC20_TRANSFER_TOPIC)),
                        word_hex(&self.node_eth),
                        word_hex(&CANDIDATE),
                    ],
                    "data": "0x",
                    "transactionHash": format!("0x{}", "77".repeat(32)),
                    "blockNumber": format!("0x{HIT_BLOCK:x}"),
                }]),
                // The verifying read fails transiently. Deliberately not
                // -32000: that code means "I don't cover this range" and
                // would fall back to the configured URL instead.
                "eth_call" => {
                    let data = req["params"][0]["data"].as_str().unwrap_or_default();
                    assert!(
                        data.starts_with(&deployed_contracts_selector()),
                        "unexpected eth_call {data}",
                    );
                    return rpc_error(-32603, "backend unavailable");
                }
                // Everything below is only reachable by the auto-deploy,
                // which must never run on an unread scan. Scripted
                // (rather than panicking) so the pre-fix code gets all
                // the way to the broadcast and the assertion below is
                // what fails.
                "eth_getBalance" => json!("0xde0b6b3a7640000"), // 1 xDAI of gas
                "eth_getTransactionCount" => json!("0x0"),
                "eth_sendRawTransaction" => {
                    return rpc_error(-32603, "scripted: no tx should be broadcast here")
                }
                other => panic!("unscripted method {other}"),
            };
            Some(json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string())
        }
    }

    /// A rediscovery scan that *failed* must not be read as "this
    /// account owns no chequebook": deploying on that basis burns gas and
    /// strands the deposit sitting in the chequebook we could not see
    /// (R3-M1). Settlement is skipped for this run instead.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_rediscovery_scan_does_not_auto_deploy() {
        let wallet = ant_chain::tx::Wallet::new([7u8; 32], crate::GNOSIS_CHAIN_ID).unwrap();
        let node_eth = *wallet.address();
        let script = std::sync::Arc::new(ScriptedChain {
            node_eth,
            seen: Mutex::new(Vec::new()),
        });
        // An unroutable URL: a fall-through would fail the call rather
        // than quietly reach a real RPC.
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()));

        let dir = std::env::temp_dir().join(format!("ant-cb-scan-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        // `Resolution` is not `Debug`, so unwrap by hand.
        let err = match resolve_or_deploy_chequebook(&client, &wallet, &dir, node_eth, true).await {
            Err(e) => e,
            Ok(super::Resolution::Use(r)) => panic!(
                "a failed scan must not resolve to a chequebook: 0x{} (deployed = {})",
                hex::encode(r.address),
                r.deployed,
            ),
            Ok(super::Resolution::NoneYet) => {
                panic!("a failed scan must not read as an affordability skip")
            }
            Ok(super::Resolution::Disqualified { reason, .. }) => {
                panic!("nothing was persisted to disqualify: {reason}")
            }
        };

        let seen = script.seen.lock().unwrap().clone();
        assert!(
            !seen.iter().any(|m| m == "eth_sendRawTransaction"),
            "no chequebook deploy may be broadcast after a failed scan: {seen:?}",
        );
        assert!(
            !dir.join("chequebook.json").exists(),
            "nothing may be persisted for a chequebook we never deployed",
        );
        assert!(
            err.to_string().contains("backend unavailable"),
            "the real reason must survive: {err}",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // --- persisted-issuer verification (issue #49) ---

    /// Per-batch `PostageStamp` views for the persisted-issuer check:
    /// `Some(owner)` is the on-chain `batchOwner` (zero = not found),
    /// `None` a failed read. `remainingBalance` is `1` unless the batch
    /// is in `drained` (then `0`). Records which batches were asked about.
    struct OwnerScript {
        owners: Mutex<std::collections::HashMap<[u8; 32], Option<[u8; 20]>>>,
        drained: std::collections::HashSet<[u8; 32]>,
        queried: Mutex<Vec<[u8; 32]>>,
    }

    impl ChainTransport for OwnerScript {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            assert_eq!(req["method"], "eth_call", "only contract views expected");
            let data = req["params"][0]["data"].as_str().unwrap();
            let mut id = [0u8; 32];
            hex::decode_to_slice(&data[10..74], &mut id).unwrap();
            self.queried.lock().unwrap().push(id);
            let owner = self.owners.lock().unwrap().get(&id).copied();
            let result = match (&data[0..10], owner) {
                (_, None | Some(None)) => {
                    // Not the retryable -32000 (that falls back to the
                    // configured URL) — an authoritative backend failure.
                    return Some(
                        json!({"jsonrpc": "2.0", "id": req["id"],
                               "error": {"code": -32603, "message": "backend unavailable"}})
                        .to_string(),
                    );
                }
                ("0x2182ddb1", Some(Some(o))) => word_hex(&o), // batchOwner
                ("0x44beae8e", _) => word_hex(&[20]),          // batchDepth
                ("0x32ac57dd", _) => word_hex(&[16]),          // bucketDepth
                ("0xd968f44b", _) => word_hex(&[0]),           // immutableFlag
                ("0xd71ba7c4", _) => word_hex(&[u8::from(!self.drained.contains(&id))]), // remainingBalance
                (other, _) => panic!("unscripted selector {other}"),
            };
            Some(json!({"jsonrpc": "2.0", "id": req["id"], "result": result}).to_string())
        }
    }

    fn store(postage: &std::path::Path, id: [u8; 32]) -> std::path::PathBuf {
        postage.join(format!("{}.bin", hex::encode(id)))
    }

    /// The persisted-batch `NotFound` grace must fit inside a short
    /// mobile session: its suspect clock is in-memory only, so a grace
    /// longer than a foreground stint restarts on every launch and a
    /// dead batch is never unregistered. It must still cover a backend
    /// lagging a few Gnosis blocks.
    #[test]
    fn persisted_not_found_grace_fits_a_short_session() {
        let grace = super::PERSISTED_NOT_FOUND_GRACE;
        assert!(grace >= std::time::Duration::from_secs(30), "{grace:?}");
        assert!(grace <= std::time::Duration::from_secs(60), "{grace:?}");
    }

    /// A lagging RPC backend (a batch bought just before a relaunch,
    /// read on a load-balanced sibling that hasn't seen `BatchCreated`
    /// yet) reads `batchOwner` as zero. One such read must not
    /// unregister the batch: it stays registered while the pass waits
    /// out the grace window, and survives if the re-read finds it. A
    /// batch still missing on the re-read is unregistered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn not_found_is_rechecked_after_grace_before_unregistering() {
        use super::ChainInit;
        use std::collections::{BTreeSet, HashMap};
        use std::sync::Arc;
        use std::time::Duration;

        let ours = [0x0a; 20];
        let lagging = [0x71; 32];
        let gone = [0x72; 32];

        let dir = std::env::temp_dir().join(format!("ant-not-found-grace-{}", std::process::id()));
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let issuers: HashMap<_, _> = [lagging, gone]
            .iter()
            .map(|&id| {
                let iss =
                    ant_postage::StampIssuer::open_or_new(store(&postage, id), id, 20, 16, false)
                        .unwrap();
                (id, iss)
            })
            .collect();
        let upload = Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(issuers),
            stamp_key: [1u8; 32],
            batch_owner: ours,
            postage_dir: postage.clone(),
        });
        let grace = Duration::from_millis(400);
        let persisted = Arc::new(ChainInit::with_not_found_grace(Arc::clone(&upload), grace));
        let script = Arc::new(OwnerScript {
            owners: Mutex::new(HashMap::from([
                (lagging, Some([0u8; 20])),
                (gone, Some([0u8; 20])),
            ])),
            drained: std::collections::HashSet::new(),
            queried: Mutex::new(Vec::new()),
        });
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()));
        let registered =
            || -> BTreeSet<[u8; 32]> { upload.issuers.lock().unwrap().keys().copied().collect() };

        let pass = {
            let persisted = Arc::clone(&persisted);
            tokio::spawn(async move {
                persisted
                    .verify_persisted(&client, ant_chain::GNOSIS_POSTAGE_STAMP)
                    .await;
            })
        };
        // First sweep done, pass now waiting out the grace window.
        tokio::time::sleep(grace / 2).await;
        assert!(!pass.is_finished(), "the pass waits to re-check");
        assert_eq!(
            registered(),
            BTreeSet::from([lagging, gone]),
            "one NotFound read unregisters nothing",
        );
        assert_eq!(persisted.unverified(), vec![lagging, gone]);
        // The lagging backend catches up before the re-read.
        script.owners.lock().unwrap().insert(lagging, Some(ours));

        tokio::time::timeout(grace * 10, pass)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            registered(),
            BTreeSet::from([lagging]),
            "the batch found on re-read is kept; the one still missing is dropped",
        );
        assert_eq!(persisted.unverified(), [] as [[u8; 32]; 0]);
        assert!(persisted.lock_not_found_since().is_empty());
        let queried = script.queried.lock().unwrap().clone();
        assert_eq!(
            queried.iter().filter(|id| **id == gone).count(),
            2,
            "a missing batch is read twice before it is unregistered",
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Overlapping passes (a host re-calling `ant_start_gateway` while
    /// the previous check is still in flight) are serialized: the
    /// second one only sees what the first left pending, so each batch
    /// is asked about once and unregistered once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_passes_query_each_batch_once() {
        use super::ChainInit;
        use std::collections::HashMap;
        use std::sync::Arc;

        struct SlowGone(Mutex<Vec<[u8; 32]>>);
        impl ChainTransport for SlowGone {
            fn serve(&self, request_json: &str) -> Option<String> {
                let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
                let data = req["params"][0]["data"].as_str().unwrap();
                assert_eq!(&data[0..10], "0x2182ddb1", "only batchOwner expected");
                let mut id = [0u8; 32];
                hex::decode_to_slice(&data[10..74], &mut id).unwrap();
                self.0.lock().unwrap().push(id);
                // Slow enough that the second pass starts mid-flight.
                std::thread::sleep(std::time::Duration::from_millis(100));
                Some(
                    json!({"jsonrpc": "2.0", "id": req["id"], "result": word_hex(&[0])})
                        .to_string(),
                )
            }
        }

        let dir =
            std::env::temp_dir().join(format!("ant-overlapping-passes-{}", std::process::id()));
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let ids = [[0xa1; 32], [0xa2; 32]];
        let issuers: HashMap<_, _> = ids
            .iter()
            .map(|&id| {
                let iss =
                    ant_postage::StampIssuer::open_or_new(store(&postage, id), id, 20, 16, false)
                        .unwrap();
                (id, iss)
            })
            .collect();
        let upload = Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(issuers),
            stamp_key: [1u8; 32],
            batch_owner: [0x0a; 20],
            postage_dir: postage.clone(),
        });
        // Grace 0: a single `NotFound` read is believed (the grace
        // re-check has its own test).
        let persisted = Arc::new(ChainInit::with_not_found_grace(
            Arc::clone(&upload),
            std::time::Duration::ZERO,
        ));
        let script = Arc::new(SlowGone(Mutex::new(Vec::new())));

        let passes: Vec<_> = (0..2)
            .map(|_| {
                let persisted = Arc::clone(&persisted);
                let client = ChainClient::new("http://127.0.0.1:1")
                    .with_transport(Some(script.clone() as Arc<dyn ChainTransport>));
                tokio::spawn(async move {
                    persisted
                        .verify_persisted(&client, ant_chain::GNOSIS_POSTAGE_STAMP)
                        .await;
                })
            })
            .collect();
        for pass in passes {
            pass.await.unwrap();
        }

        let mut queried = script.0.lock().unwrap().clone();
        queried.sort_unstable();
        assert_eq!(queried, ids.to_vec(), "each batch is asked about once");
        assert!(upload.issuers.lock().unwrap().is_empty());
        assert_eq!(persisted.unverified(), [] as [[u8; 32]; 0]);

        std::fs::remove_dir_all(&dir).ok();
    }

    /// The phantom-batch fix for `ant-ffi`: once an RPC is known, a
    /// reloaded batch the chain reports as missing or foreign-owned is
    /// unregistered (its files stay on disk), while one whose read
    /// failed stays registered and pending, and is confirmed on retry.
    /// A batch registered at runtime is never re-checked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_issuers_the_chain_disowns_are_unregistered() {
        use super::{reload_persisted_issuers, ChainInit};
        use std::collections::{BTreeSet, HashMap};
        use std::sync::Arc;

        let ours = [0x0a; 20];
        let live = [0x11; 32];
        let dead = [0x22; 32];
        let foreign = [0x33; 32];
        let unreadable = [0x44; 32];
        let bought = [0x55; 32];
        // Expired but not yet evicted: still ours, balance 0.
        let expired = [0x66; 32];

        let dir =
            std::env::temp_dir().join(format!("ant-persisted-issuers-{}", std::process::id()));
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        for id in [live, dead, foreign, unreadable, expired] {
            drop(
                ant_postage::StampIssuer::open_or_new(store(&postage, id), id, 20, 16, false)
                    .unwrap(),
            );
        }

        let upload = Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(reload_persisted_issuers(&postage)),
            stamp_key: [1u8; 32],
            batch_owner: ours,
            postage_dir: postage.clone(),
        });
        let persisted =
            ChainInit::with_not_found_grace(Arc::clone(&upload), std::time::Duration::ZERO);
        // Registered after the reload, as a runtime buy would be.
        upload.issuers.lock().unwrap().insert(
            bought,
            ant_postage::StampIssuer::open_or_new(store(&postage, bought), bought, 20, 16, false)
                .unwrap(),
        );

        let script = Arc::new(OwnerScript {
            owners: Mutex::new(HashMap::from([
                (live, Some(ours)),
                (dead, Some([0u8; 20])),
                (foreign, Some([0x5e; 20])),
                (unreadable, None),
                (expired, Some(ours)),
            ])),
            drained: std::collections::HashSet::from([expired]),
            queried: Mutex::new(Vec::new()),
        });
        let client = ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()));
        let registered =
            || -> BTreeSet<[u8; 32]> { upload.issuers.lock().unwrap().keys().copied().collect() };

        persisted
            .verify_persisted(&client, ant_chain::GNOSIS_POSTAGE_STAMP)
            .await;
        assert_eq!(
            registered(),
            BTreeSet::from([live, unreadable, bought]),
            "dead + expired + foreign batches must be unregistered, the rest kept",
        );
        assert_eq!(
            persisted.unverified(),
            vec![unreadable],
            "only the batch whose read failed is still pending",
        );
        for id in [dead, foreign, expired] {
            assert!(store(&postage, id).exists(), "the store stays on disk");
        }
        assert!(
            !script.queried.lock().unwrap().contains(&bought),
            "a batch registered at runtime is not re-checked",
        );

        // The next start (RPC healthy again) confirms the pending batch
        // and asks the chain about nothing else.
        script.owners.lock().unwrap().insert(unreadable, Some(ours));
        script.queried.lock().unwrap().clear();
        persisted
            .verify_persisted(&client, ant_chain::GNOSIS_POSTAGE_STAMP)
            .await;
        assert_eq!(registered(), BTreeSet::from([live, unreadable, bought]));
        assert_eq!(persisted.unverified(), [] as [[u8; 32]; 0]);
        assert!(script
            .queried
            .lock()
            .unwrap()
            .iter()
            .all(|id| *id == unreadable));

        std::fs::remove_dir_all(&dir).ok();
    }

    // --- chain init + chequebook setup (parity catch-up) ---

    /// A scripted Gnosis backend covering what [`super::ChainInit::run`]
    /// and [`super::setup_settlement`] read: the xBZZ `Transfer` scan,
    /// `BatchCreated`, the postage batch views, and the chequebook
    /// factory / `issuer()` views. Anything that would spend (gas
    /// pre-flight, nonce, broadcast) answers an error and is recorded,
    /// so a test can assert nothing was spent.
    struct ChainScript {
        node_eth: [u8; 20],
        /// `Transfer(from = node_eth, to)` hits, each in its own tx.
        transfers: Vec<([u8; 20], [u8; 32])>,
        /// `BatchCreated` logs in `HIT_BLOCK`: `(batch, tx)`.
        created: Vec<([u8; 32], [u8; 32])>,
        /// Chequebook views: `(registered with the factory, issuer)`.
        chequebooks: std::collections::HashMap<[u8; 20], (bool, [u8; 20])>,
        /// xBZZ `balanceOf` overrides; everyone else holds the full
        /// deposit target.
        balances: std::collections::HashMap<[u8; 20], u128>,
        /// Answer the chequebook checks (factory, `issuer()`) with an
        /// RPC error.
        checks_fail: bool,
        /// The factory says "registered" only for this many reads, then
        /// "no" (the chain changing its answer between two checks).
        registered_reads: Option<usize>,
        factory_reads: Mutex<usize>,
        /// Refuse xBZZ `eth_getLogs` ranges wider than this, as a
        /// verified route that serves a few thousand blocks at a time
        /// does; `0` serves any range.
        cap: std::sync::atomic::AtomicU64,
        seen: Mutex<Vec<String>>,
    }

    impl ChainScript {
        fn new(node_eth: [u8; 20]) -> Self {
            Self {
                node_eth,
                transfers: Vec::new(),
                created: Vec::new(),
                chequebooks: std::collections::HashMap::new(),
                balances: std::collections::HashMap::new(),
                checks_fail: false,
                registered_reads: None,
                factory_reads: Mutex::new(0),
                cap: std::sync::atomic::AtomicU64::new(0),
                seen: Mutex::new(Vec::new()),
            }
        }

        fn seen(&self, method: &str) -> usize {
            self.seen
                .lock()
                .unwrap()
                .iter()
                .filter(|m| *m == method)
                .count()
        }

        fn eth_call(&self, to: &str, data: &str) -> Option<serde_json::Value> {
            let to = to.to_ascii_lowercase();
            let sel = &data[0..10];
            if to == ant_chain::GNOSIS_POSTAGE_STAMP.to_ascii_lowercase() {
                return Some(json!(match sel {
                    "0x2182ddb1" => word_hex(&self.node_eth), // batchOwner
                    "0x44beae8e" => word_hex(&[20]),          // batchDepth
                    "0x32ac57dd" => word_hex(&[16]),          // bucketDepth
                    "0xd968f44b" => word_hex(&[0]),           // immutableFlag
                    "0xd71ba7c4" => word_hex(&[42]),          // remainingBalance
                    other => panic!("unscripted postage view {other}"),
                }));
            }
            let mut addr = [0u8; 20];
            if to
                == format!(
                    "0x{}",
                    hex::encode(ant_chain::chequebook::GNOSIS_CHEQUEBOOK_FACTORY)
                )
            {
                // deployedContracts(address): the address is the
                // right-aligned argument word.
                hex::decode_to_slice(&data[34..74], &mut addr).unwrap();
                let reads = {
                    let mut n = self.factory_reads.lock().unwrap();
                    *n += 1;
                    *n
                };
                let registered = self.chequebooks.get(&addr).is_some_and(|c| c.0)
                    && self.registered_reads.is_none_or(|max| reads <= max);
                return Some(json!(word_hex(&[u8::from(registered)])));
            }
            if to == ant_chain::GNOSIS_BZZ_TOKEN.to_ascii_lowercase() && sel == "0x70a08231" {
                // balanceOf: every account holds the full deposit target
                // unless overridden, so an adopted chequebook needs no
                // top-up by default.
                hex::decode_to_slice(&data[34..74], &mut addr).unwrap();
                let bal = self
                    .balances
                    .get(&addr)
                    .copied()
                    .unwrap_or(super::DEPOSIT_TARGET_PLUR);
                return Some(json!(format!("0x{bal:064x}")));
            }
            if to
                == format!(
                    "0x{}",
                    hex::encode(ant_chain::chequebook::GNOSIS_SWAP_PRICE_ORACLE)
                )
            {
                // getPrice(): bee's mainnet oracle, 100 000 PLUR/unit, 100.
                return Some(json!(format!("0x{:064x}{:064x}", 100_000, 100)));
            }
            hex::decode_to_slice(to.trim_start_matches("0x"), &mut addr).unwrap();
            let (_, issuer) = self.chequebooks.get(&addr)?;
            if sel[2..] == hex::encode(ant_chain::chequebook::chequebook_total_paid_out_selector())
            {
                return Some(json!(word_hex(&[0])));
            }
            Some(json!(word_hex(issuer)))
        }
    }

    impl ChainTransport for ChainScript {
        fn serve(&self, request_json: &str) -> Option<String> {
            let req: serde_json::Value = serde_json::from_str(request_json).unwrap();
            let method = req["method"].as_str().unwrap().to_string();
            self.seen.lock().unwrap().push(method.clone());
            let id = req["id"].clone();
            let result = match method.as_str() {
                "eth_chainId" => json!("0x64"),
                "eth_blockNumber" => json!(format!("0x{:x}", HIT_BLOCK + 500)),
                // A backend that hasn't seen the tx (yet).
                "eth_getTransactionReceipt" => serde_json::Value::Null,
                "eth_getLogs" => {
                    let filter = &req["params"][0];
                    let address = filter["address"].as_str().unwrap().to_ascii_lowercase();
                    let block = |key: &str| {
                        u64::from_str_radix(&filter[key].as_str().unwrap()[2..], 16).unwrap()
                    };
                    let cap = self.cap.load(std::sync::atomic::Ordering::SeqCst);
                    if cap > 0 && block("toBlock") - block("fromBlock") + 1 > cap {
                        return Some(
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": {"code": -32005, "message": "query exceeds max block range"},
                            })
                            .to_string(),
                        );
                    }
                    if address == ant_chain::GNOSIS_BZZ_TOKEN.to_ascii_lowercase() {
                        json!(self
                            .transfers
                            .iter()
                            .map(|(to, tx)| json!({
                                "address": ant_chain::GNOSIS_BZZ_TOKEN,
                                "topics": [
                                    format!("0x{}", hex::encode(ant_chain::discover::ERC20_TRANSFER_TOPIC)),
                                    word_hex(&self.node_eth),
                                    word_hex(to),
                                ],
                                "data": "0x",
                                "transactionHash": format!("0x{}", hex::encode(tx)),
                                "blockNumber": format!("0x{HIT_BLOCK:x}"),
                            }))
                            .collect::<Vec<_>>())
                    } else {
                        let bc = format!(
                            "0x{}",
                            hex::encode(ant_chain::tx::batch_created_event_topic())
                        );
                        json!(self
                            .created
                            .iter()
                            .map(|(batch, tx)| json!({
                                "address": ant_chain::GNOSIS_POSTAGE_STAMP,
                                "topics": [bc.clone(), word_hex(batch)],
                                "data": "0x",
                                "transactionHash": format!("0x{}", hex::encode(tx)),
                                "blockNumber": format!("0x{HIT_BLOCK:x}"),
                            }))
                            .collect::<Vec<_>>())
                    }
                }
                "eth_call" => {
                    let call = &req["params"][0];
                    let to = call["to"].as_str().unwrap().to_ascii_lowercase();
                    let is_check =
                        to == format!(
                            "0x{}",
                            hex::encode(ant_chain::chequebook::GNOSIS_CHEQUEBOOK_FACTORY)
                        ) || self
                            .chequebooks
                            .keys()
                            .any(|cb| to == format!("0x{}", hex::encode(cb)));
                    if self.checks_fail && is_check {
                        return Some(
                            json!({"jsonrpc": "2.0", "id": id,
                                   "error": {"code": -32603, "message": "scripted: backend down"}})
                            .to_string(),
                        );
                    }
                    match self
                        .eth_call(call["to"].as_str().unwrap(), call["data"].as_str().unwrap())
                    {
                        Some(v) => v,
                        None => panic!("unscripted eth_call {call}"),
                    }
                }
                // Spending paths: answer an error, but record them.
                _ => {
                    return Some(
                        json!({"jsonrpc": "2.0", "id": id,
                               "error": {"code": -32603, "message": "scripted: not allowed here"}})
                        .to_string(),
                    )
                }
            };
            Some(json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string())
        }
    }

    /// What a fake node loop saw: registered batches and the chequebooks
    /// settlement was switched on for.
    #[derive(Default)]
    struct NodeLog {
        registered: Vec<[u8; 32]>,
        enabled: Vec<[u8; 20]>,
        disabled: Vec<[u8; 20]>,
        /// `SetRetrievalFunds`: `(chequebook, deposited, exchange rate)`.
        funds: Vec<([u8; 20], u128, u128)>,
        /// Fail this many `RegisterBatch` commands before accepting.
        fail_registers: usize,
    }

    fn fake_node() -> (
        tokio::sync::mpsc::Sender<ant_control::ControlCommand>,
        std::sync::Arc<Mutex<NodeLog>>,
    ) {
        use ant_control::{ControlAck, ControlCommand};
        let (tx, mut rx) = tokio::sync::mpsc::channel::<ControlCommand>(16);
        let log = std::sync::Arc::new(Mutex::new(NodeLog::default()));
        let sink = std::sync::Arc::clone(&log);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    ControlCommand::RegisterBatch { batch_id, ack, .. } => {
                        let mut log = sink.lock().unwrap();
                        if log.fail_registers > 0 {
                            log.fail_registers -= 1;
                            let _ = ack.send(ControlAck::Error {
                                message: "scripted register failure".into(),
                            });
                            continue;
                        }
                        log.registered.push(batch_id);
                        let _ = ack.send(ControlAck::Ok {
                            message: "registered".into(),
                        });
                    }
                    ControlCommand::DisablePushsyncSwap {
                        chequebook, ack, ..
                    } => {
                        sink.lock().unwrap().disabled.push(chequebook);
                        let _ = ack.send(ControlAck::Ok {
                            message: "disabled".into(),
                        });
                    }
                    ControlCommand::EnablePushsyncSwap {
                        chequebook, ack, ..
                    } => {
                        sink.lock().unwrap().enabled.push(chequebook);
                        let _ = ack.send(ControlAck::Ok {
                            message: "enabled".into(),
                        });
                    }
                    ControlCommand::SetRetrievalFunds {
                        chequebook,
                        deposited_plur,
                        exchange_rate_plur,
                        ack,
                        ..
                    } => {
                        sink.lock().unwrap().funds.push((
                            chequebook,
                            deposited_plur,
                            exchange_rate_plur,
                        ));
                        let _ = ack.send(ControlAck::Ok {
                            message: "funds".into(),
                        });
                    }
                    other => panic!("unexpected command {other:?}"),
                }
            }
        });
        (tx, log)
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ant-ffi-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn client(script: &std::sync::Arc<ChainScript>) -> ChainClient {
        ChainClient::new("http://127.0.0.1:1").with_transport(Some(script.clone()))
    }

    const NODE_KEY: [u8; 32] = [7u8; 32];

    fn node_wallet() -> (ant_chain::tx::Wallet, [u8; 20]) {
        let wallet = ant_chain::tx::Wallet::new(NODE_KEY, crate::GNOSIS_CHAIN_ID).unwrap();
        let eth = *wallet.address();
        (wallet, eth)
    }

    /// A corrupt `chequebook.json` no longer disables settlement for
    /// good: the chequebook is found on-chain, the record rewritten, and
    /// settlement switched on. Adopt-only mode, so nothing is spent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupt_chequebook_record_is_recovered_on_chain() {
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-corrupt");
        std::fs::write(dir.join("chequebook.json"), b"{ truncated").unwrap();
        let mut script = ChainScript::new(eth);
        script.transfers.push((CANDIDATE, [0x71; 32]));
        script.chequebooks.insert(CANDIDATE, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .expect("recovered");

        assert_eq!(got, Some(CANDIDATE));
        assert_eq!(
            ant_chain::chequebook_store::load_persisted_chequebook_for(
                &dir.join("chequebook.json"),
                &eth
            )
            .unwrap(),
            Some(CANDIDATE),
            "the corrupt record is rewritten",
        );
        assert_eq!(node.lock().unwrap().enabled, vec![CANDIDATE]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Issue #121: once settlement runs on a chequebook, the node learns
    /// what it can pay for downloads (deposit and oracle rates, read over
    /// the same chain client) through the shared funds watch, as `antd`'s
    /// does.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn settlement_setup_tells_the_node_its_retrieval_funds() {
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-funds");
        let mut script = ChainScript::new(eth);
        script.transfers.push((CANDIDATE, [0x71; 32]));
        script.chequebooks.insert(CANDIDATE, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .expect("adopted");
        assert_eq!(got, Some(CANDIDATE));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while node.lock().unwrap().funds.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            node.lock().unwrap().funds.first().copied(),
            Some((CANDIDATE, super::DEPOSIT_TARGET_PLUR, 100_000)),
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A chequebook a C-API storage call sets settlement up on reaches
    /// the running gateway's slot at once (`/chequebook/address`,
    /// `/wallet`), without another `ant_start_gateway`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_c_api_settlement_points_the_gateway_at_its_chequebook() {
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-capi-slot");
        persist(&dir, CANDIDATE, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(CANDIDATE, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();
        let slot = ant_gateway::ChequebookSlot::default();

        let got = super::ensure_settlement_for_gateway(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            &slot,
        )
        .await;

        assert_eq!(got, Some(CANDIDATE));
        assert_eq!(slot.get(), Some(CANDIDATE), "the gateway reports it");
        assert_eq!(node.lock().unwrap().enabled, vec![CANDIDATE]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R1-F1 (#109): `ant_deploy_chequebook`'s path on a persisted
    /// chequebook the chain check disqualifies errors (not `Ok(None)`),
    /// and the gateway slot that still held it is cleared all the same.
    /// A usable one is written to the slot.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deploy_chequebook_path_syncs_the_gateway_slot_on_error() {
        const BAD: [u8; 20] = [0xdb; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-deploy-slot");
        persist(&dir, BAD, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(BAD, (false, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, _node) = fake_node();
        let slot = ant_gateway::ChequebookSlot::default();
        slot.set(BAD);

        let err = super::setup_settlement_for_gateway(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            &slot,
        )
        .await
        .expect_err("a disqualified chequebook must not be used");

        assert!(err.to_string().contains("not registered"), "got {err}");
        assert_eq!(slot.get(), None, "the gateway stops reporting it");
        assert_eq!(slot.refused(), Some(BAD));
        super::lock_disqualified().remove(&(eth, BAD));
        std::fs::remove_dir_all(&dir).ok();
    }

    fn persist(dir: &std::path::Path, cb: [u8; 20], issuer: [u8; 20]) {
        ant_chain::chequebook_store::persist_chequebook(
            &dir.join("chequebook.json"),
            &ant_chain::chequebook_store::ChequebookFile::rediscovered(&cb, &issuer),
        )
        .unwrap();
    }

    /// A persisted chequebook that passes antd's checks is switched on,
    /// including by `ant_deploy_chequebook`'s path (F5).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn persisted_chequebook_is_checked_then_enabled() {
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-ok");
        persist(&dir, CANDIDATE, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(CANDIDATE, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .unwrap();

        assert_eq!(got, Some(CANDIDATE));
        assert_eq!(node.lock().unwrap().enabled, vec![CANDIDATE]);
        assert_eq!(script.seen("eth_call"), 2, "factory + issuer() checks");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// F4: a persisted chequebook the factory doesn't know, or one issued
    /// by another key, is not switched on, and no replacement is deployed.
    /// Settlement `ant_init` switched on for it unchecked is switched off
    /// again (R1-F1), and the status stops reporting it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disqualified_persisted_chequebook_is_not_enabled() {
        // Its own address: `DISQUALIFIED` is process-wide, and other
        // tests adopt `CANDIDATE` concurrently.
        const BAD: [u8; 20] = [0xd1; 20];
        let (wallet, eth) = node_wallet();
        for (registered, issuer, why, issuer_reads) in [
            (false, eth, "not registered", 0),
            (true, [0x5e; 20], "is issued by", 1),
        ] {
            let dir = scratch("cb-bad");
            persist(&dir, BAD, eth);
            let mut script = ChainScript::new(eth);
            script.chequebooks.insert(BAD, (registered, issuer));
            let script = std::sync::Arc::new(script);
            let (cmd_tx, node) = fake_node();

            let err = super::setup_settlement(
                &cmd_tx,
                &client(&script),
                &wallet,
                &dir,
                NODE_KEY,
                eth,
                true,
            )
            .await
            .expect_err("a disqualified chequebook must not be used");

            assert!(err.to_string().contains(why), "got {err}");
            assert_eq!(node.lock().unwrap().enabled, [] as [[u8; 20]; 0]);
            assert_eq!(
                node.lock().unwrap().disabled,
                vec![BAD],
                "settlement enabled unchecked at init is switched off",
            );
            assert!(super::lock_disqualified().contains(&(eth, BAD)));
            // The factory read, then `issuer()` only when it can matter.
            assert_eq!(script.seen("eth_call"), 1 + issuer_reads);
            assert_eq!(script.seen("eth_sendRawTransaction"), 0);
            assert_eq!(script.seen("eth_getBalance"), 0, "no deploy pre-flight");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Adopt-only mode (gateway start) with no chequebook anywhere: a
    /// clean "none", and nothing that would spend.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn adopt_only_never_deploys() {
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-none");
        let script = std::sync::Arc::new(ChainScript::new(eth));
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .unwrap();

        assert_eq!(got, None);
        assert_eq!(node.lock().unwrap().enabled, [] as [[u8; 20]; 0]);
        for spend in [
            "eth_getBalance",
            "eth_getTransactionCount",
            "eth_sendRawTransaction",
        ] {
            assert_eq!(script.seen(spend), 0, "{spend} must not be called");
        }
        assert!(!dir.join("chequebook.json").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Gateway-start chain init registers the funded batches this account
    /// owns on-chain that aren't registered yet (antd step 3), and
    /// adopts the on-chain chequebook without spending. A second run in
    /// the same process doesn't rescan for batches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chain_init_rediscovers_batches_and_adopts_the_chequebook() {
        let (_, eth) = node_wallet();
        let dir = scratch("chain-init");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let (known, lost) = ([0xa1u8; 32], [0xa2u8; 32]);
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::from([(
                known,
                ant_postage::StampIssuer::open_or_new(
                    postage.join(format!("{}.bin", hex::encode(known))),
                    known,
                    20,
                    16,
                    false,
                )
                .unwrap(),
            )])),
            stamp_key: NODE_KEY,
            batch_owner: eth,
            postage_dir: postage,
        });
        let init = super::ChainInit::new(std::sync::Arc::clone(&upload));

        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let mut script = ChainScript::new(eth);
        script.transfers.push((postage_addr, [0x01; 32]));
        script.transfers.push((postage_addr, [0x02; 32]));
        script.transfers.push((CANDIDATE, [0x03; 32]));
        script.created.push((known, [0x01; 32]));
        script.created.push((lost, [0x02; 32]));
        script.chequebooks.insert(CANDIDATE, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let adopted = init.run(&client(&script), &cmd_tx, &dir, NODE_KEY).await;

        assert_eq!(adopted, Some(CANDIDATE), "reported for the gateway");
        {
            let node = node.lock().unwrap();
            assert_eq!(
                node.registered,
                vec![lost],
                "only the batch missing locally"
            );
            assert_eq!(node.enabled, vec![CANDIDATE], "on-chain chequebook adopted");
        }
        for spend in [
            "eth_getBalance",
            "eth_getTransactionCount",
            "eth_sendRawTransaction",
        ] {
            assert_eq!(
                script.seen(spend),
                0,
                "{spend} must not be called at gateway start"
            );
        }

        // A gateway restart: the batch scan doesn't run again, and
        // settlement, already on, isn't set up a second time.
        let scans = script.seen("eth_getLogs");
        init.run(&client(&script), &cmd_tx, &dir, NODE_KEY).await;
        assert_eq!(script.seen("eth_getLogs"), scans, "no rescan on restart");
        assert_eq!(node.lock().unwrap().registered, vec![lost]);
        assert_eq!(node.lock().unwrap().enabled, vec![CANDIDATE]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R1-M1: a rediscovered batch whose registration failed is retried
    /// by the next chain-init run instead of being dropped for the rest
    /// of the process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_rediscovered_registration_is_retried() {
        let (_, eth) = node_wallet();
        let dir = scratch("chain-init-retry");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let lost = [0xa3u8; 32];
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: eth,
            postage_dir: postage,
        });
        let init = super::ChainInit::new(std::sync::Arc::clone(&upload));
        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let mut script = ChainScript::new(eth);
        script.transfers.push((postage_addr, [0x04; 32]));
        script.created.push((lost, [0x04; 32]));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();
        node.lock().unwrap().fail_registers = 1;

        init.rediscover_owned(&client(&script), &cmd_tx, &dir).await;
        assert_eq!(node.lock().unwrap().registered, [] as [[u8; 32]; 0]);
        assert!(!*init.batches_rediscovered.lock().await);

        init.rediscover_owned(&client(&script), &cmd_tx, &dir).await;
        assert_eq!(node.lock().unwrap().registered, vec![lost], "retried");
        assert!(*init.batches_rediscovered.lock().await);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A failed rediscovery reads as `retrying` in `/health.walletScan`
    /// and is retried in the background with the shared backoff until it
    /// succeeds, without waiting for the host's next `ant_start_gateway`.
    /// Its own wallet: the status registry is process-wide.
    #[tokio::test(start_paused = true)]
    async fn failed_rediscovery_is_retried_in_the_background() {
        use ant_chain::discover::{wallet_scan_status, WalletScanState};
        const EOA: [u8; 20] = [0x77; 20];
        let dir = scratch("chain-init-backoff");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let lost = [0xa4u8; 32];
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: EOA,
            postage_dir: postage,
        });
        let init = super::ChainInit::new(std::sync::Arc::clone(&upload));
        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let mut script = ChainScript::new(EOA);
        script.transfers.push((postage_addr, [0x05; 32]));
        script.created.push((lost, [0x05; 32]));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();
        node.lock().unwrap().fail_registers = 1;

        init.note_pending();
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Pending
        );
        assert!(!init.rediscover_owned(&client(&script), &cmd_tx, &dir).await);
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Retrying
        );

        let started = tokio::time::Instant::now();
        init.retry_rediscovery(&client(&script), &cmd_tx, &dir, init.retry_epoch())
            .await;
        assert!(started.elapsed() >= std::time::Duration::from_secs(15));
        assert_eq!(node.lock().unwrap().registered, vec![lost], "retried");
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Done
        );
        assert!(!init.rediscovery_retry.lock().unwrap().running);

        // A later gateway start finds it done and doesn't announce another.
        init.note_pending();
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Done
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A first rediscovery the transport can only serve window by window
    /// reads the history once from the unverified source
    /// (`ant_set_unverified_logs_rpc`), registers what it finds and reads
    /// `confirming`; the background confirmation registers the batch the
    /// unverified source left out once the transport serves the span, and
    /// ends `done`. Its own wallet: the status registry is process-wide.
    #[tokio::test(start_paused = true)]
    async fn an_unverified_first_scan_is_confirmed_in_the_background() {
        use ant_chain::discover::{wallet_scan_status, WalletScanState};
        const EOA: [u8; 20] = [0x78; 20];
        let dir = scratch("chain-init-unverified");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let (seen, missed) = ([0xa5u8; 32], [0xa6u8; 32]);
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: EOA,
            postage_dir: postage,
        });
        let init = super::ChainInit::new(std::sync::Arc::clone(&upload));
        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let mut verified = ChainScript::new(EOA);
        verified.transfers.push((postage_addr, [0x07; 32]));
        verified.transfers.push((postage_addr, [0x08; 32]));
        verified.created.push((seen, [0x07; 32]));
        verified.created.push((missed, [0x08; 32]));
        verified
            .cap
            .store(10_000, std::sync::atomic::Ordering::SeqCst);
        let mut unverified = ChainScript::new(EOA);
        unverified.transfers.push((postage_addr, [0x07; 32]));
        let (verified, unverified) = (
            std::sync::Arc::new(verified),
            std::sync::Arc::new(unverified),
        );
        let chain = client(&verified).with_unverified_logs_client(Some(client(&unverified)));
        let (cmd_tx, node) = fake_node();

        init.note_pending();
        assert!(init.rediscover_owned(&chain, &cmd_tx, &dir).await);
        assert_eq!(node.lock().unwrap().registered, vec![seen]);
        assert!(init.unconfirmed.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Confirming
        );

        // The transport's second source is back: it serves the span.
        verified.cap.store(0, std::sync::atomic::Ordering::SeqCst);
        let adopted = init
            .confirm_unverified(&chain, &cmd_tx, &dir, NODE_KEY)
            .await;
        assert_eq!(adopted, None, "no chequebook to adopt, and none deployed");
        // The fake node doesn't fill the issuer map, so the batch already
        // registered is sent again here; a real node fills it, and
        // `register_found` skips the batch as known.
        let registered = node.lock().unwrap().registered.clone();
        assert_eq!(registered.last(), Some(&missed), "{registered:?}");
        assert_eq!(registered.iter().filter(|b| **b == missed).count(), 1);
        assert!(!init.unconfirmed.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Done
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A gateway stop drops an unfinished `/health.walletScan` status
    /// (nothing retries it until the next start with an RPC) but keeps a
    /// finished one; a next start with an RPC announces `pending` again,
    /// even while an attempt from before the stop still holds the
    /// rediscovery lock, and that `pending` ends once the lock frees.
    #[tokio::test]
    async fn gateway_stop_drops_an_unfinished_wallet_scan_status() {
        use ant_chain::discover::{wallet_scan_failed, wallet_scan_status, WalletScanState};
        const EOA: [u8; 20] = [0x7a; 20];
        let dir = scratch("chain-init-stop-status");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: EOA,
            postage_dir: postage,
        });
        let init = super::ChainInit::new(upload);

        init.note_pending();
        wallet_scan_failed(&EOA, "rpc down");
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Retrying
        );
        init.stop_retrying();
        assert_eq!(wallet_scan_status(&EOA), None);
        // An in-flight attempt's late failure doesn't bring it back.
        wallet_scan_failed(&EOA, "rpc down");
        assert_eq!(wallet_scan_status(&EOA), None);

        // A start while that attempt still holds the lock re-announces.
        {
            let mut held = init.batches_rediscovered.lock().await;
            init.note_pending();
            assert_eq!(
                wallet_scan_status(&EOA).unwrap().state,
                WalletScanState::Pending
            );
            *held = true; // ...and the attempt succeeded.
        }
        // The new start's own run finds it done and says so.
        let script = std::sync::Arc::new(ChainScript::new(EOA));
        let (cmd_tx, _node) = fake_node();
        assert!(init.rediscover_owned(&client(&script), &cmd_tx, &dir).await);
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Done
        );

        // A finished status survives a stop; a later start keeps it.
        init.stop_retrying();
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Done
        );
        ant_chain::discover::wallet_scan_forget(&EOA);
        init.note_pending();
        assert_eq!(
            wallet_scan_status(&EOA).unwrap().state,
            WalletScanState::Done
        );
        ant_chain::discover::wallet_scan_forget(&EOA);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The background retry reads through the latest gateway start's
    /// chain client, and stops once the gateway does, instead of polling
    /// the first start's RPC for the handle's lifetime.
    #[tokio::test(start_paused = true)]
    async fn rediscovery_retry_follows_the_gateway() {
        const EOA: [u8; 20] = [0x7b; 20];
        let dir = scratch("chain-init-retry-stop");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: EOA,
            postage_dir: postage,
        });
        let init = std::sync::Arc::new(super::ChainInit::new(std::sync::Arc::clone(&upload)));
        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let script = || {
            let mut script = ChainScript::new(EOA);
            script.transfers.push((postage_addr, [0x05; 32]));
            script.created.push(([0xa5u8; 32], [0x05; 32]));
            std::sync::Arc::new(script)
        };
        let (first, second) = (script(), script());
        let (cmd_tx, node) = fake_node();
        // Every attempt fails at registration, so the loop keeps going.
        node.lock().unwrap().fail_registers = usize::MAX;

        let task = {
            let (init, cmd_tx, dir, chain) = (
                std::sync::Arc::clone(&init),
                cmd_tx.clone(),
                dir.clone(),
                client(&first),
            );
            let epoch = init.retry_epoch();
            tokio::spawn(async move { init.retry_rediscovery(&chain, &cmd_tx, &dir, epoch).await })
        };
        tokio::task::yield_now().await;
        // A restart with another RPC: the running loop takes it over.
        init.retry_rediscovery(&client(&second), &cmd_tx, &dir, init.retry_epoch())
            .await;
        // Until an attempt (the first after 15 s) has read the chain and
        // failed its registration. Bounded in real time: the transfer
        // scan lock is process-wide, so a parallel test can hold it while
        // the paused clock runs ahead.
        let waited = std::time::Instant::now();
        while node.lock().unwrap().fail_registers == usize::MAX
            && waited.elapsed() < std::time::Duration::from_secs(30)
        {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(first.seen.lock().unwrap().is_empty(), "old RPC unused");
        assert!(
            !second.seen.lock().unwrap().is_empty(),
            "the attempt read through the new RPC"
        );

        init.stop_retrying();
        let waited = std::time::Instant::now();
        while !task.is_finished() && waited.elapsed() < std::time::Duration::from_secs(30) {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        assert!(task.is_finished(), "the loop ends once the gateway stops");
        task.await.unwrap();
        assert!(!init.rediscovery_retry.lock().unwrap().running);
        ant_chain::discover::wallet_scan_forget(&EOA);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R2-F1: a gateway stop that lands while a start's first
    /// rediscovery attempt is still running (no retry loop yet for the
    /// stop to end) keeps that attempt's failure from starting one, so
    /// the stopped gateway's RPC isn't polled for the handle's lifetime.
    #[tokio::test(start_paused = true)]
    async fn stop_during_first_rediscovery_starts_no_retry_loop() {
        const EOA: [u8; 20] = [0x79; 20];
        let dir = scratch("chain-init-stop-first");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: EOA,
            postage_dir: postage,
        });
        let init = std::sync::Arc::new(super::ChainInit::new(std::sync::Arc::clone(&upload)));
        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let mut script = ChainScript::new(EOA);
        script.transfers.push((postage_addr, [0x06; 32]));
        script.created.push(([0xa6u8; 32], [0x06; 32]));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();
        node.lock().unwrap().fail_registers = usize::MAX;

        // The start takes its epoch, then the gateway stops before its
        // first attempt has failed.
        let epoch = init.retry_epoch();
        init.stop_retrying();
        let task = {
            let (init, cmd_tx, dir, chain) = (
                std::sync::Arc::clone(&init),
                cmd_tx.clone(),
                dir.clone(),
                client(&script),
            );
            tokio::spawn(async move {
                init.run_reporting(&chain, &cmd_tx, &dir, [0x11; 32], Some(epoch), |_| {})
                    .await;
            })
        };
        // Bounded in real time, not on the paused clock: the transfer
        // scan lock is process-wide, so a parallel test can hold it while
        // the paused clock runs ahead. With the bug, the run never ends
        // (it retries every <= 5 min, forever).
        let waited = std::time::Instant::now();
        while !task.is_finished() && waited.elapsed() < std::time::Duration::from_secs(30) {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        assert!(
            task.is_finished(),
            "no retry loop after the gateway stopped"
        );
        task.await.unwrap();
        assert!(
            node.lock().unwrap().fail_registers == usize::MAX - 1,
            "exactly the start's own attempt registered"
        );
        assert!(!init.rediscovery_retry.lock().unwrap().running);
        assert!(init.rediscovery_retry.lock().unwrap().chain.is_none());
        assert!(!*init.batches_rediscovered.lock().await);
        ant_chain::discover::wallet_scan_forget(&EOA);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R1-M2: a chequebook we just deployed (the record carries its
    /// deploy tx) that a lagging backend reports as unregistered is
    /// still used while that backend hasn't seen the deploy receipt —
    /// and disqualified once a visible receipt shows no such deploy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn just_deployed_chequebook_survives_a_lagging_factory_read() {
        const FRESH: [u8; 20] = [0xd2; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-lag");
        ant_chain::chequebook_store::persist_chequebook(
            &dir.join("chequebook.json"),
            &ant_chain::chequebook_store::ChequebookFile {
                chequebook: format!("0x{}", hex::encode(FRESH)),
                issuer: format!("0x{}", hex::encode(eth)),
                salt: String::new(),
                deploy_tx: format!("0x{}", hex::encode([0x77u8; 32])),
            },
        )
        .unwrap();
        // Lagging: the backend hasn't seen the deploy block, so the
        // factory says "no" and the receipt isn't there yet.
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(FRESH, (false, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .expect("a lagging read is not a disqualification");
        assert_eq!(got, Some(FRESH));
        assert_eq!(node.lock().unwrap().enabled, vec![FRESH]);
        assert_eq!(node.lock().unwrap().disabled, [] as [[u8; 20]; 0]);
        assert_eq!(script.seen("eth_getTransactionReceipt"), 1);

        // R2-M2: the same record long after the deploy: a backend
        // doesn't lag that far behind, so the factory's "no" stands.
        std::fs::File::options()
            .write(true)
            .open(dir.join("chequebook.json"))
            .unwrap()
            .set_modified(
                std::time::SystemTime::now() - ant_chain::chequebook_store::DEPLOY_LAG_GRACE * 2,
            )
            .unwrap();
        let err = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .expect_err("past the grace, an unregistered chequebook is disqualified");
        assert!(err.to_string().contains("not registered"), "got {err}");
        assert_eq!(node.lock().unwrap().disabled, vec![FRESH]);
        assert_eq!(
            script.seen("eth_getTransactionReceipt"),
            1,
            "no receipt read"
        );
        super::lock_disqualified().remove(&(eth, FRESH));

        // A rediscovered record has no deploy tx to vouch for it: the
        // factory's "no" stands.
        persist(&dir, FRESH, eth);
        let err = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .expect_err("no deploy tx, so the factory answer stands");
        assert!(err.to_string().contains("not registered"), "got {err}");
        assert_eq!(node.lock().unwrap().disabled, vec![FRESH, FRESH]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R1-M3 / R2-M3: an unreadable record may name a deposit-0
    /// chequebook the funded-only rediscovery scan can't see. When its
    /// bytes still hold that address and the chain confirms it's ours,
    /// it's adopted (no deploy) and the record rewritten.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreadable_record_is_salvaged_from_its_bytes() {
        const UNFUNDED: [u8; 20] = [0xd3; 20];
        let (wallet, eth) = node_wallet();
        for may_spend in [false, true] {
            let dir = scratch(&format!("cb-corrupt-salvage-{may_spend}"));
            // Half-written: the address survived, the JSON didn't.
            std::fs::write(
                dir.join("chequebook.json"),
                format!(
                    "{{\"chequebook\":\"0x{}\",\"issuer\":\"0x{}\",\"sa",
                    hex::encode(UNFUNDED),
                    hex::encode(eth)
                ),
            )
            .unwrap();
            let mut script = ChainScript::new(eth);
            script.chequebooks.insert(UNFUNDED, (true, eth));
            let script = std::sync::Arc::new(script);
            let (cmd_tx, node) = fake_node();

            let got = super::setup_settlement(
                &cmd_tx,
                &client(&script),
                &wallet,
                &dir,
                NODE_KEY,
                eth,
                may_spend,
            )
            .await
            .unwrap();

            assert_eq!(got, Some(UNFUNDED));
            assert_eq!(node.lock().unwrap().enabled, vec![UNFUNDED]);
            assert_eq!(
                ant_chain::chequebook_store::load_persisted_chequebook_for(
                    &dir.join("chequebook.json"),
                    &eth
                )
                .unwrap(),
                Some(UNFUNDED),
                "record rewritten",
            );
            assert!(!dir.join("chequebook.json.unreadable").exists());
            assert_eq!(script.seen("eth_getBalance"), 0, "no deploy pre-flight");
            assert_eq!(script.seen("eth_sendRawTransaction"), 0);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// R2-M3: an unreadable record that names no chequebook of ours is
    /// moved aside instead of blocking every deploy for good (a mobile
    /// user can't reach the file to fix it); adopt-only leaves it alone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreadable_record_naming_nothing_is_parked_before_a_deploy() {
        const FOREIGN: [u8; 20] = [0xd4; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-corrupt-none");
        let record = dir.join("chequebook.json");
        std::fs::write(
            &record,
            format!("{{ \"chequebook\": \"0x{}\", trunc", hex::encode(FOREIGN)),
        )
        .unwrap();
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(FOREIGN, (true, [0x5e; 20]));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            false,
        )
        .await
        .unwrap();
        assert_eq!(got, None);
        assert!(record.exists(), "adopt-only doesn't touch the record");
        assert_eq!(script.seen("eth_getBalance"), 0);

        // A spending caller moves it aside and goes on to deploy (which
        // the script refuses — the deploy pre-flight is what we look for).
        let _ = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            true,
        )
        .await;
        assert_eq!(node.lock().unwrap().enabled, [] as [[u8; 20]; 0]);
        assert!(!record.exists());
        assert!(dir.join("chequebook.json.unreadable").exists());
        assert!(script.seen("eth_getBalance") > 0, "deploy attempted");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R2-M3: the salvage only trusts addresses the chain confirms, and
    /// skips longer hex runs (tx hashes, salts).
    #[test]
    fn addresses_in_finds_only_address_runs() {
        let a = [0xabu8; 20];
        let text = format!(
            "x0x{} 0x{} 0X{} 0x{}",
            hex::encode(a),
            hex::encode([0x11u8; 32]),
            hex::encode(a),
            &hex::encode(a)[..39],
        );
        assert_eq!(super::addresses_in(text.as_bytes()), vec![a]);
    }

    /// R2-M1: a chequebook the chain check disqualified reads as "none"
    /// on the deposit card (like `settlement_status`) and is never
    /// funded by the top-up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disqualified_chequebook_is_neither_shown_nor_funded() {
        const BAD: [u8; 20] = [0xd5; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-dq-topup");
        persist(&dir, BAD, eth);
        let script = std::sync::Arc::new(ChainScript::new(eth));
        super::lock_disqualified().insert((eth, BAD));

        let card = super::settlement_deposit_for(&client(&script), &wallet, &dir, &eth)
            .await
            .unwrap();
        let card: serde_json::Value = serde_json::from_str(&card).unwrap();
        assert_eq!(card["enabled"], false);
        assert_eq!(card["needs_top_up"], false);
        let (cmd_tx, _node) = fake_node();
        let slot = ant_gateway::ChequebookSlot::default();
        slot.set(BAD);
        let err =
            super::settlement_topup_xdai_for(&cmd_tx, &client(&script), &dir, eth, NODE_KEY, &slot)
                .await
                .expect_err("no deposit into a disqualified chequebook");
        assert_eq!(slot.get(), None, "the gateway stops reporting it");
        assert!(
            err.to_string().contains("failed its on-chain checks"),
            "got {err}"
        );
        assert_eq!(
            super::deposit_policy_for(&dir, &eth),
            ant_chain::funding::DepositPolicy::Unmanaged,
            "no deposit is priced in or bought for it",
        );
        assert!(
            script.seen.lock().unwrap().is_empty(),
            "no chain call at all"
        );
        super::lock_disqualified().remove(&(eth, BAD));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R3-M1: the top-up runs the chequebook's chain checks itself
    /// before spending, instead of relying on a chain init (none without
    /// an RPC, or still in flight) to have filled `DISQUALIFIED`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topup_checks_the_chequebook_before_spending() {
        const BAD: [u8; 20] = [0xd6; 20];
        let (_, eth) = node_wallet();
        for (registered, issuer, why, issuer_reads) in [
            (false, eth, "not registered", 0),
            (true, [0x5e; 20], "is issued by", 1),
        ] {
            let dir = scratch("cb-topup-check");
            persist(&dir, BAD, eth);
            let mut script = ChainScript::new(eth);
            script.chequebooks.insert(BAD, (registered, issuer));
            let script = std::sync::Arc::new(script);
            let (cmd_tx, node) = fake_node();
            assert!(!super::lock_disqualified().contains(&(eth, BAD)));
            // The gateway started with it (chain init not yet run).
            let slot = ant_gateway::ChequebookSlot::default();
            slot.set(BAD);

            let err = super::settlement_topup_xdai_for(
                &cmd_tx,
                &client(&script),
                &dir,
                eth,
                NODE_KEY,
                &slot,
            )
            .await
            .expect_err("no deposit into a chequebook that fails its checks");

            assert!(err.to_string().contains(why), "got {err}");
            assert_eq!(
                script.seen("eth_call"),
                1 + issuer_reads,
                "only the checks ran"
            );
            assert_eq!(script.seen("eth_getBalance"), 0);
            assert_eq!(script.seen("eth_sendRawTransaction"), 0);
            assert!(super::lock_disqualified().contains(&(eth, BAD)));
            assert_eq!(node.lock().unwrap().disabled, vec![BAD]);
            assert_eq!(slot.get(), None, "the gateway stops reporting it");
            assert_eq!(slot.refused(), Some(BAD));
            super::lock_disqualified().remove(&(eth, BAD));
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// R3-M1: a chequebook that passes its checks goes on to the deposit
    /// read (here: already at target, so a no-op), nothing disqualified.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topup_of_a_checked_chequebook_proceeds() {
        const GOOD: [u8; 20] = [0xd7; 20];
        let (_, eth) = node_wallet();
        let dir = scratch("cb-topup-ok");
        persist(&dir, GOOD, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(GOOD, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();
        let slot = ant_gateway::ChequebookSlot::default();
        slot.set(GOOD);

        let card =
            super::settlement_topup_xdai_for(&cmd_tx, &client(&script), &dir, eth, NODE_KEY, &slot)
                .await
                .unwrap();
        let card: serde_json::Value = serde_json::from_str(&card).unwrap();
        assert_eq!(card["enabled"], true);
        assert_eq!(card["needs_top_up"], false);
        assert_eq!(script.seen("eth_sendRawTransaction"), 0);
        assert_eq!(node.lock().unwrap().disabled, [] as [[u8; 20]; 0]);
        assert!(!super::lock_disqualified().contains(&(eth, GOOD)));
        assert_eq!(slot.get(), Some(GOOD));
        // PR #126 R1-M3: the top-up tells the node its funds at once (no
        // settlement setup ran before it, as after a bare `ant_init`),
        // and leaves the funds watch running.
        assert_eq!(
            node.lock().unwrap().funds.first().copied(),
            Some((GOOD, super::DEPOSIT_TARGET_PLUR, 100_000)),
        );
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while node.lock().unwrap().funds.len() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            node.lock().unwrap().funds.len(),
            2,
            "the watch's first read"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R3-M1 (round 3 of #99): the top-up reads the deposit shortfall
    /// only once it holds the wallet tx lock. While an after-buy
    /// deposit (or any other spend) holds the lock, only the chequebook
    /// checks may run — reading the deposit then would transfer a stale
    /// shortfall once the lock is released, double-funding it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topup_reads_the_shortfall_under_the_wallet_tx_lock() {
        const CB: [u8; 20] = [0xd9; 20];
        let (_, eth) = node_wallet();
        let dir = scratch("cb-topup-lock");
        persist(&dir, CB, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(CB, (true, eth));
        script.balances.insert(CB, 0);
        let script = std::sync::Arc::new(script);
        let (cmd_tx, _node) = fake_node();

        let held = super::wallet_tx_lock(&eth).lock_owned().await;
        let task = {
            let (script, dir) = (script.clone(), dir.clone());
            tokio::spawn(async move {
                let slot = ant_gateway::ChequebookSlot::default();
                super::settlement_topup_xdai_for(
                    &cmd_tx,
                    &client(&script),
                    &dir,
                    eth,
                    NODE_KEY,
                    &slot,
                )
                .await
            })
        };
        // Wait (bounded, not a fixed sleep) for the two pre-lock
        // chequebook checks, so a slow runner can't fail this spuriously.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        while script.seen("eth_call") < 2 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the chequebook checks never ran"
            );
            assert!(
                !task.is_finished(),
                "top-up finished while the lock was held"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Give a deposit read that wrongly skipped the lock time to show
        // up. A slow runner can only make this pass vacuously, never fail
        // correct code.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            script.seen("eth_call"),
            2,
            "only the factory + issuer checks run before the lock; the deposit read waits",
        );
        assert!(!task.is_finished(), "the top-up is parked on the lock");
        drop(held);
        let res = tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .expect("top-up proceeds once the lock is released")
            .unwrap();
        // The script refuses the transfer; what matters is that the
        // deposit was read after the lock was taken.
        assert!(res.is_err());
        assert!(script.seen("eth_call") > 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The shared deposit top-up (`funding::fund_deposit_with_xdai`)
    /// re-checks the chequebook right before its transfer. A "no" there,
    /// after the C API's own pre-check passed, gets the same treatment:
    /// nothing sent, recorded as disqualified, settlement switched off —
    /// unless it's our just-deployed chequebook on an RPC that hasn't
    /// seen the deploy yet (`not_registered_may_be_lag`), which switches
    /// nothing off and asks to retry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn topup_refused_at_the_transfer_is_lag_checked_before_disabling() {
        const CB: [u8; 20] = [0xda; 20];
        let (_, eth) = node_wallet();
        for just_deployed in [false, true] {
            let dir = scratch("cb-topup-refused");
            if just_deployed {
                ant_chain::chequebook_store::persist_chequebook(
                    &dir.join("chequebook.json"),
                    &ant_chain::chequebook_store::ChequebookFile {
                        chequebook: format!("0x{}", hex::encode(CB)),
                        issuer: format!("0x{}", hex::encode(eth)),
                        salt: String::new(),
                        deploy_tx: format!("0x{}", hex::encode([0x78u8; 32])),
                    },
                )
                .unwrap();
            } else {
                persist(&dir, CB, eth);
            }
            let mut script = ChainScript::new(eth);
            script.chequebooks.insert(CB, (true, eth));
            script.balances.insert(CB, 0);
            // The pre-check reads "registered"; the shared top-up's own
            // check right before the transfer reads "no".
            script.registered_reads = Some(1);
            let script = std::sync::Arc::new(script);
            let (cmd_tx, node) = fake_node();
            let slot = ant_gateway::ChequebookSlot::default();
            slot.set(CB);

            let err = super::settlement_topup_xdai_for(
                &cmd_tx,
                &client(&script),
                &dir,
                eth,
                NODE_KEY,
                &slot,
            )
            .await
            .expect_err("refused before the transfer");

            for spend in ["eth_getTransactionCount", "eth_sendRawTransaction"] {
                assert_eq!(script.seen(spend), 0, "{spend}: nothing may be sent");
            }
            if just_deployed {
                assert!(err.to_string().contains("hasn't caught up"), "got {err}");
                assert_eq!(node.lock().unwrap().disabled, [] as [[u8; 20]; 0]);
                assert!(!super::is_disqualified(&eth, &CB));
                assert_eq!(slot.get(), Some(CB), "lag: the gateway keeps it");
            } else {
                assert!(err.to_string().contains("not registered"), "got {err}");
                assert_eq!(node.lock().unwrap().disabled, vec![CB]);
                assert!(super::is_disqualified(&eth, &CB));
                assert_eq!(slot.get(), None, "the gateway stops reporting it");
            }
            super::lock_disqualified().remove(&(eth, CB));
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// R3-M2: a disqualification is scoped to the account that checked.
    /// Account A refusing X (issued by B) must not hide X from B after
    /// an in-process switch to B.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn disqualification_is_per_account() {
        const X: [u8; 20] = [0xd8; 20];
        const A: [u8; 20] = [0xa1; 20];
        let (wallet, b) = node_wallet();
        let dir = scratch("cb-dq-per-account");
        persist(&dir, X, b);
        let script = std::sync::Arc::new(ChainScript::new(b));
        super::lock_disqualified().insert((A, X));

        assert_eq!(
            super::deposit_policy_for(&dir, &b),
            ant_chain::funding::DepositPolicy::Managed {
                chequebook: Some(X),
                target: super::DEPOSIT_TARGET_PLUR,
            },
            "B's chequebook is priced in, not treated as disqualified",
        );
        let card = super::settlement_deposit_for(&client(&script), &wallet, &dir, &b)
            .await
            .unwrap();
        let card: serde_json::Value = serde_json::from_str(&card).unwrap();
        assert_eq!(card["enabled"], true);
        assert!(script.seen("eth_call") > 0, "B's deposit was read on-chain");
        super::lock_disqualified().remove(&(A, X));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// R3-M3: parking a second unreadable record keeps the first parked
    /// file instead of renaming over it.
    #[test]
    fn park_path_never_reuses_a_parked_file() {
        let dir = scratch("cb-park");
        let record = dir.join("chequebook.json");
        let first = super::unused_park_path(&record);
        assert_eq!(first, dir.join("chequebook.json.unreadable"));
        std::fs::write(&first, b"episode 1").unwrap();
        let second = super::unused_park_path(&record);
        assert_eq!(second, dir.join("chequebook.json.unreadable.1"));
        std::fs::write(&second, b"episode 2").unwrap();
        assert_eq!(
            super::unused_park_path(&record),
            dir.join("chequebook.json.unreadable.2")
        );
        assert_eq!(std::fs::read(&first).unwrap(), b"episode 1");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Two overlapping chain-init runs (an idempotent `ant_start_gateway`
    /// re-call while the first run is still in flight) scan for batches
    /// once and register a rediscovered batch once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn overlapping_chain_init_runs_rediscover_once() {
        let (_, eth) = node_wallet();
        let dir = scratch("chain-init-overlap");
        let postage = dir.join("postage");
        std::fs::create_dir_all(&postage).unwrap();
        let lost = [0xa2u8; 32];
        let upload = std::sync::Arc::new(ant_p2p::UploadRuntime {
            issuers: Mutex::new(std::collections::HashMap::new()),
            stamp_key: NODE_KEY,
            batch_owner: eth,
            postage_dir: postage,
        });
        let init = std::sync::Arc::new(super::ChainInit::new(std::sync::Arc::clone(&upload)));

        let postage_addr = {
            let mut a = [0u8; 20];
            hex::decode_to_slice(&ant_chain::GNOSIS_POSTAGE_STAMP[2..], &mut a).unwrap();
            a
        };
        let mut script = ChainScript::new(eth);
        script.transfers.push((postage_addr, [0x02; 32]));
        script.transfers.push((CANDIDATE, [0x03; 32]));
        script.created.push((lost, [0x02; 32]));
        script.chequebooks.insert(CANDIDATE, (true, eth));
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        // One run alone, for the scan count to compare against.
        let solo_dir = scratch("chain-init-solo");
        let solo_script = std::sync::Arc::new({
            let mut s = ChainScript::new(eth);
            s.transfers.push((postage_addr, [0x02; 32]));
            s.transfers.push((CANDIDATE, [0x03; 32]));
            s.created.push((lost, [0x02; 32]));
            s.chequebooks.insert(CANDIDATE, (true, eth));
            s
        });
        let (solo_tx, _solo_node) = fake_node();
        super::ChainInit::new(std::sync::Arc::clone(&upload))
            .run(&client(&solo_script), &solo_tx, &solo_dir, NODE_KEY)
            .await;
        let one_scan = solo_script.seen("eth_getLogs");

        let runs: Vec<_> = (0..2)
            .map(|_| {
                let (init, chain, cmd_tx, dir) = (
                    std::sync::Arc::clone(&init),
                    client(&script),
                    cmd_tx.clone(),
                    dir.clone(),
                );
                tokio::spawn(async move { init.run(&chain, &cmd_tx, &dir, NODE_KEY).await })
            })
            .collect();
        for run in runs {
            run.await.unwrap();
        }

        assert_eq!(script.seen("eth_getLogs"), one_scan, "one scan, not two");
        assert_eq!(
            node.lock().unwrap().registered,
            vec![lost],
            "registered once"
        );
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&solo_dir).ok();
    }

    /// A buy (`may_spend`) adopting a persisted chequebook whose checks
    /// can't be read switches settlement on (an RPC hiccup mustn't
    /// stall uploads) but sends it nothing: the top-up wants a verified
    /// "yes" before a deposit it can't take back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unverified_chequebook_is_enabled_but_not_funded() {
        const CB: [u8; 20] = [0xe1; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-unverified-fund");
        persist(&dir, CB, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(CB, (true, eth));
        script.balances.insert(CB, 0);
        script.checks_fail = true;
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let got = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            true,
        )
        .await
        .unwrap();

        assert_eq!(got, Some(CB));
        assert_eq!(node.lock().unwrap().enabled, vec![CB]);
        for spend in [
            "eth_getTransactionCount",
            "eth_sendRawTransaction",
            "eth_gasPrice",
        ] {
            assert_eq!(script.seen(spend), 0, "{spend}: nothing may be sent");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A chequebook an earlier check in this process disqualified stays
    /// off when the next check can't be read: a failed read doesn't
    /// lift a "no", so it is neither switched on nor funded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_read_does_not_lift_a_disqualification() {
        const CB: [u8; 20] = [0xe2; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-dq-unverified");
        persist(&dir, CB, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(CB, (true, eth));
        script.balances.insert(CB, 0);
        script.checks_fail = true;
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();
        super::lock_disqualified().insert((eth, CB));

        let err = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            true,
        )
        .await
        .expect_err("still disqualified");

        assert!(
            err.to_string().contains("could not be re-verified"),
            "got {err}"
        );
        assert_eq!(node.lock().unwrap().enabled, [] as [[u8; 20]; 0]);
        assert_eq!(node.lock().unwrap().disabled, vec![CB]);
        assert!(super::is_disqualified(&eth, &CB));
        assert_eq!(script.seen("eth_sendRawTransaction"), 0);
        super::lock_disqualified().remove(&(eth, CB));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The top-up's pre-transfer check saying "no" (the chain changed
    /// its answer since the resolution's check) is a disqualifying
    /// verdict like any other: nothing is sent, settlement is switched
    /// off and not on, and the chequebook is recorded as disqualified.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pre_transfer_rejection_switches_settlement_off() {
        const CB: [u8; 20] = [0xe3; 20];
        let (wallet, eth) = node_wallet();
        let dir = scratch("cb-pre-transfer-no");
        persist(&dir, CB, eth);
        let mut script = ChainScript::new(eth);
        script.chequebooks.insert(CB, (true, eth));
        script.balances.insert(CB, 0);
        script.registered_reads = Some(1);
        let script = std::sync::Arc::new(script);
        let (cmd_tx, node) = fake_node();

        let err = super::setup_settlement(
            &cmd_tx,
            &client(&script),
            &wallet,
            &dir,
            NODE_KEY,
            eth,
            true,
        )
        .await
        .expect_err("rejected before the transfer");

        assert!(err.to_string().contains("not registered"), "got {err}");
        assert_eq!(node.lock().unwrap().enabled, [] as [[u8; 20]; 0]);
        assert_eq!(node.lock().unwrap().disabled, vec![CB]);
        assert!(super::is_disqualified(&eth, &CB));
        for spend in ["eth_getTransactionCount", "eth_sendRawTransaction"] {
            assert_eq!(script.seen(spend), 0, "{spend}: nothing may be sent");
        }
        super::lock_disqualified().remove(&(eth, CB));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The gateway's chequebook slot follows settlement: an adopted
    /// chequebook is set, a disqualified one cleared, anything else kept.
    #[test]
    fn gateway_chequebook_slot_follows_disqualification() {
        const CB: [u8; 20] = [0xe4; 20];
        const OWNER: [u8; 20] = [0xe5; 20];
        let slot = ant_gateway::ChequebookSlot::default();
        super::sync_gateway_chequebook(&slot, &OWNER, Some(CB));
        assert_eq!(slot.get(), Some(CB));
        super::sync_gateway_chequebook(&slot, &OWNER, None);
        assert_eq!(slot.get(), Some(CB), "no verdict: kept");
        super::lock_disqualified().insert((OWNER, CB));
        super::sync_gateway_chequebook(&slot, &OWNER, None);
        assert_eq!(slot.get(), None, "disqualified: cleared");
        assert_eq!(
            slot.refused(),
            Some(CB),
            "and remembered, so the gateway prices no deposit for it",
        );
        super::sync_gateway_chequebook(&slot, &OWNER, Some(CB));
        assert_eq!(slot.refused(), None, "a usable chequebook lifts it");
        super::lock_disqualified().remove(&(OWNER, CB));
    }
}
