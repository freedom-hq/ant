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
/// app restart without re-reading the chain. Mirrors `antd`'s startup
/// reload. Unreadable stores are skipped with a warning rather than
/// failing the whole node bring-up.
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
        )
        .await?;
        // Connecting a plan must also turn on outbound settlement —
        // otherwise uploads stamp fine but never propagate (bee freezes
        // an unpaying node out past its payment threshold). Best-effort:
        // a thin wallet or flaky RPC just leaves settlement off, which
        // the Storage UI surfaces via `settlement_status`.
        ensure_settlement_best_effort(&cmd_tx, &chain, secret, &data_dir, eth).await;
        postage_status_json(&cmd_tx).await
    })
}

/// Auto-discover every funded postage batch this account owns on Gnosis
/// (a log scan from the xBZZ deploy block) and register each one.
/// Returns `{"registered":[...],"status":<plan>}`.
#[cfg(feature = "chain")]
pub(crate) fn storage_discover(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    let eth = h.eth;
    let secret = h.signing_secret;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        let chain = h.chain_client(rpc);
        let found = ant_chain::discover::discover_owned_batches(
            &chain,
            ant_chain::GNOSIS_POSTAGE_STAMP,
            ant_chain::GNOSIS_BZZ_TOKEN,
            &eth,
            ant_chain::discover::GNOSIS_XBZZ_DEPLOY_BLOCK,
        )
        .await
        .map_err(|e| DriveError::Op(format!("search the chain for your storage: {e}")))?;
        let mut registered = Vec::new();
        for b in &found {
            register_batch(&cmd_tx, b.batch_id, b.depth, b.bucket_depth, b.immutable).await?;
            registered.push(format!("0x{}", hex::encode(b.batch_id)));
        }
        // If we connected at least one plan, make sure outbound
        // settlement is on so uploads against it actually reach the
        // network. Best-effort; never fails discovery.
        if !registered.is_empty() {
            ensure_settlement_best_effort(&cmd_tx, &chain, secret, &data_dir, eth).await;
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

#[cfg(feature = "chain")]
async fn register_batch(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    batch_id: [u8; 32],
    depth: u8,
    bucket_depth: u8,
    immutable: bool,
) -> Result<(), DriveError> {
    let (ack_tx, ack_rx) = oneshot::channel();
    send(
        cmd_tx,
        ControlCommand::RegisterBatch {
            batch_id,
            depth,
            bucket_depth,
            immutable,
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

/// Gnosis block time in seconds — postage price is denominated
/// per-chunk *per block*, so a storage duration in days converts to a
/// per-chunk balance through the block count.
#[cfg(feature = "chain")]
const GNOSIS_BLOCK_SECS: u128 = 5;

/// Gnosis chain id, re-exported from the crate root so the on-chain
/// helpers here and the node bring-up in `lib.rs` share one constant.
#[cfg(feature = "chain")]
use crate::GNOSIS_CHAIN_ID;

/// Postage collision-bucket depth (bee's constant). Every `createBatch`
/// uses it and the registered issuer must match.
#[cfg(feature = "chain")]
const POSTAGE_BUCKET_DEPTH: u8 = 16;

/// xBZZ has 16 decimals; one whole xBZZ is `10^16` PLUR.
const PLUR_PER_BZZ: u128 = 10_000_000_000_000_000;

/// Deposit sizing for the chequebook the storage flows deploy.
///
/// A chequebook with **deposit 0** — what `ensure_settlement` used to
/// leave behind — is a chequebook that backs no cheque: swap is enabled
/// and peers accept the cheques, so publishing runs clean right up until
/// the peers' payment tolerance is exhausted, then collapses into 60 s
/// pushsync timeouts (issue #73; the #67 soak measured 0.57 Mbit/s with
/// 25 failures and a 10-minute live-edge lag). The same soak with a
/// funded chequebook ran 899/899 segments at 0.89 Mbit/s flat.
///
/// Compiled unconditionally: the on-chain callers are `chain`-only, but
/// this sizing math is the part the default `cargo test --workspace
/// --lib` gate can cover.
#[cfg_attr(not(feature = "chain"), allow(dead_code))]
pub(crate) mod deposit {
    /// Target xBZZ deposit behind the node's chequebook, in PLUR:
    /// **0.001 xBZZ**. Grounded in the #67 benchmark, where that deposit
    /// backed 65 K+ cheques with huge margin (~300× one soak's measured
    /// settlement demand). Small enough not to compete with the postage
    /// batch the user came to buy, and not spent money either — an
    /// unspent deposit stays withdrawable by the issuer.
    pub(crate) const TARGET_PLUR: u128 = super::PLUR_PER_BZZ / 1_000;

    /// xBZZ (PLUR) a chequebook already holding `deposited_plur` still
    /// needs to reach [`TARGET_PLUR`]. Zero once it is at (or above) the
    /// target, so a funded account is never charged twice.
    pub(crate) fn shortfall(deposited_plur: u128) -> u128 {
        TARGET_PLUR.saturating_sub(deposited_plur)
    }

    /// Whether a deployed chequebook still needs funding. The single
    /// predicate the quote, the status surface and the top-up action all
    /// read, so "needs a top-up" can't mean one thing in the summary and
    /// another in the detail underneath.
    pub(crate) fn needs_top_up(deposited_plur: u128) -> bool {
        shortfall(deposited_plur) > 0
    }

    /// xBZZ (PLUR) a storage buy has to end up acquiring: the plan's own
    /// cost plus whatever the chequebook is still short, less what the
    /// wallet already holds.
    ///
    /// This is the accounting half of #73 — a buy that sizes its swap
    /// from the plan alone spends every xBZZ it acquires on the batch and
    /// leaves the deposit at zero, which is exactly the collapse the
    /// benchmark measured.
    pub(crate) fn bzz_to_acquire(
        plan_plur: u128,
        deposit_shortfall_plur: u128,
        wallet_bzz: u128,
    ) -> u128 {
        plan_plur
            .saturating_add(deposit_shortfall_plur)
            .saturating_sub(wallet_bzz)
    }
}

/// Swarm chunk size in bytes (capacity math).
#[cfg(feature = "chain")]
const BYTES_PER_CHUNK: u64 = 4096;

/// xDAI (the Gnosis native gas token) has 18 decimals.
#[cfg(feature = "chain")]
const WEI_PER_XDAI: u128 = 1_000_000_000_000_000_000;

/// Gas reserve (wei) we keep aside / ask the user to fund on top of the
/// swap input, covering the up-to-six txs a first xDAI buy submits
/// (helper deploy + swap + approve + createBatch, then the one-time
/// chequebook deploy + its settlement deposit transfer) at Gnosis gas
/// prices — ~0.007 xDAI at the 2 gwei default. 0.015 xDAI keeps that
/// comfortable: running the wallet dry right before the chequebook step
/// would leave settlement off, which is the very failure #73 fixes.
#[cfg(feature = "chain")]
const GAS_RESERVE_WEI: u128 = 15_000_000_000_000_000;

/// Slippage + fee buffer applied to the fair-value swap input: we send
/// `fair × 105 / 100` xDAI so the 0.3% pool fee and a few percent of
/// price impact still clear the `amountOutMin` floor. Any excess simply
/// becomes a little extra xBZZ in the account.
#[cfg(feature = "chain")]
const SWAP_BUFFER_NUM: u128 = 105;
#[cfg(feature = "chain")]
const SWAP_BUFFER_DEN: u128 = 100;

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
/// funds that chequebook — see [`deposit`] and [`ensure_settlement`].
/// `total_cost_plur` / `total_cost_bzz` stay the plan's own cost.
#[cfg(feature = "chain")]
pub(crate) fn storage_quote(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    days: u64,
) -> Result<String, DriveError> {
    let eth = h.eth;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        let client = h.chain_client(rpc);
        let price = client
            .postage_last_price(ant_chain::GNOSIS_POSTAGE_STAMP)
            .await
            .map_err(|e| DriveError::Op(format!("read storage price: {e}")))?;
        // Balances are best-effort: a flaky RPC shouldn't block showing a
        // quote, so default to 0 ("you need to add funds").
        let bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &eth)
            .await
            .unwrap_or(0);
        let xdai = client.eth_get_balance_lower128(&eth).await.unwrap_or(0);

        let blocks = (u128::from(days) * 86_400 / GNOSIS_BLOCK_SECS).max(1);
        // A batch needs a non-zero per-chunk balance; if the RPC reports a
        // zero price, fall back to 1 PLUR/chunk/block so the plan is valid.
        let amount_per_chunk = price.max(1).saturating_mul(blocks);
        let total_plur = amount_per_chunk.saturating_mul(1u128 << depth);
        let capacity_bytes = (1u64 << depth).saturating_mul(BYTES_PER_CHUNK);

        // Activating a plan also brings settlement up, and a chequebook
        // that backs no cheque stalls publishing a few tens of thousands
        // of chunks in (#73), so the deposit it still needs is part of
        // this plan's all-in price — not a surprise the user meets later.
        let deposit_shortfall = quote_deposit_shortfall(&client, &data_dir, &eth).await;

        // xDAI-only flow: the user funds plain xDAI, the node swaps the
        // shortfall into xBZZ. Total to hold = swap input + gas reserve.
        let needed_bzz = deposit::bzz_to_acquire(total_plur, deposit_shortfall, bzz);
        let swap_input = if needed_bzz > 0 {
            buffered_swap_input(&client, needed_bzz).await?
        } else {
            0
        };
        let xdai_required = swap_input.saturating_add(GAS_RESERVE_WEI);
        let xdai_to_send = xdai_required.saturating_sub(xdai);

        to_json(&Quote {
            depth,
            days,
            amount_per_chunk: amount_per_chunk.to_string(),
            total_cost_plur: total_plur.to_string(),
            total_cost_bzz: format_bzz(total_plur),
            settlement_deposit_plur: deposit_shortfall.to_string(),
            settlement_deposit_bzz: format_bzz(deposit_shortfall),
            capacity_bytes,
            account_bzz: bzz.to_string(),
            account_bzz_display: format_bzz(bzz),
            account_xdai: xdai.to_string(),
            account_xdai_display: format_native(xdai),
            needed_bzz: needed_bzz.to_string(),
            needed_bzz_display: format_bzz(needed_bzz),
            xdai_required: xdai_required.to_string(),
            xdai_required_display: format_native(xdai_required),
            xdai_to_send: xdai_to_send.to_string(),
            xdai_to_send_display: format_native(xdai_to_send),
            sufficient_funds: xdai >= xdai_required,
        })
    })
}

/// Price a top-up of the *connected* storage plan: how much it costs to
/// extend its lifetime by `days` at the current postage price, and
/// whether the account's xBZZ / xDAI funds cover it. Reads the batch
/// depth from the local issuer (a top-up pays per chunk, so cost scales
/// with the plan's size). Returns the same quote shape as
/// [`storage_quote`]. No transaction is sent.
#[cfg(feature = "chain")]
pub(crate) fn storage_topup_quote(
    h: &AntHandle,
    rpc: String,
    days: u64,
) -> Result<String, DriveError> {
    let cmd_tx = h.cmd_tx.clone();
    let eth = h.eth;
    h.runtime.block_on(async move {
        let view = connected_plan(&cmd_tx).await?;
        let client = h.chain_client(rpc);
        let price = client
            .postage_last_price(ant_chain::GNOSIS_POSTAGE_STAMP)
            .await
            .map_err(|e| DriveError::Op(format!("read storage price: {e}")))?;
        let bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &eth)
            .await
            .unwrap_or(0);
        let xdai = client.eth_get_balance_lower128(&eth).await.unwrap_or(0);

        let depth = view.batch_depth;
        let blocks = (u128::from(days) * 86_400 / GNOSIS_BLOCK_SECS).max(1);
        let amount_per_chunk = price.max(1).saturating_mul(blocks);
        let total_plur = amount_per_chunk.saturating_mul(1u128 << depth);
        let capacity_bytes = (1u64 << depth).saturating_mul(BYTES_PER_CHUNK);

        let needed_bzz = total_plur.saturating_sub(bzz);
        let swap_input = if needed_bzz > 0 {
            buffered_swap_input(&client, needed_bzz).await?
        } else {
            0
        };
        let xdai_required = swap_input.saturating_add(GAS_RESERVE_WEI);
        let xdai_to_send = xdai_required.saturating_sub(xdai);

        to_json(&Quote {
            depth,
            days,
            amount_per_chunk: amount_per_chunk.to_string(),
            total_cost_plur: total_plur.to_string(),
            total_cost_bzz: format_bzz(total_plur),
            // Extending a plan neither deploys nor funds a chequebook —
            // it is pure postage — so no settlement deposit is folded
            // into this price. A chequebook that needs one is surfaced
            // (and topped up) on its own, via [`settlement_deposit`].
            settlement_deposit_plur: "0".to_string(),
            settlement_deposit_bzz: format_bzz(0),
            capacity_bytes,
            account_bzz: bzz.to_string(),
            account_bzz_display: format_bzz(bzz),
            account_xdai: xdai.to_string(),
            account_xdai_display: format_native(xdai),
            needed_bzz: needed_bzz.to_string(),
            needed_bzz_display: format_bzz(needed_bzz),
            xdai_required: xdai_required.to_string(),
            xdai_required_display: format_native(xdai_required),
            xdai_to_send: xdai_to_send.to_string(),
            xdai_to_send_display: format_native(xdai_to_send),
            sufficient_funds: xdai >= xdai_required,
        })
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
    let amount: u128 = amount_per_chunk
        .trim()
        .parse()
        .map_err(|_| DriveError::Op("invalid top-up price".into()))?;
    if amount == 0 {
        return Err(DriveError::Op(
            "top-up amount must be greater than zero".into(),
        ));
    }
    let cmd_tx = h.cmd_tx.clone();
    let secret = h.signing_secret;
    let owner = h.eth;
    let tx_rpc = rpc.clone();
    h.runtime.block_on(async move {
        use primitive_types::U256;

        let view = connected_plan(&cmd_tx).await?;
        let batch_id = parse_batch_id(&view.batch_id)?;
        let depth = view.batch_depth;

        let client = h.chain_client(tx_rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
        let postage = parse_addr(ant_chain::GNOSIS_POSTAGE_STAMP)?;
        let bzz = parse_addr(ant_chain::GNOSIS_BZZ_TOKEN)?;

        let total_plur = amount
            .checked_mul(1u128 << depth)
            .ok_or_else(|| DriveError::Op("top-up cost overflows".into()))?;

        // 1) Cover the xBZZ shortfall by swapping xDAI, if any — the
        //    same flow as storage_buy_xdai.
        let have_bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &owner)
            .await
            .unwrap_or(0);
        let needed_bzz = total_plur.saturating_sub(have_bzz);
        if needed_bzz > 0 {
            let xdai = client
                .eth_get_balance_lower128(&owner)
                .await
                .map_err(|e| DriveError::Op(format!("read xDAI balance: {e}")))?;
            let swap_input = buffered_swap_input(&client, needed_bzz).await?;
            let required = swap_input.saturating_add(GAS_RESERVE_WEI);
            if xdai < required {
                return Err(DriveError::Op(format!(
                    "not enough xDAI: send {} more xDAI to your account, then try again",
                    format_native(required - xdai)
                )));
            }
            let helper = wallet
                .ensure_swap_helper(&client)
                .await
                .map_err(|e| DriveError::Op(format!("prepare swap: {e}")))?;
            wallet
                .swap_xdai_for_bzz(
                    &client,
                    &helper,
                    &owner,
                    U256::from(swap_input),
                    U256::from(needed_bzz),
                )
                .await
                .map_err(|e| DriveError::Op(format!("swap xDAI for xBZZ: {e}")))?;
        }

        // 2) Authorise + top up the batch on-chain.
        wallet
            .approve_bzz(&client, &bzz, &postage, U256::from(total_plur))
            .await
            .map_err(|e| DriveError::Op(format!("authorise payment: {e}")))?;
        wallet
            .top_up(&client, &postage, &batch_id, U256::from(amount))
            .await
            .map_err(|e| DriveError::Op(format!("extend storage: {e}")))?;
        Ok(())
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
        let remaining_seconds = (remaining / price).saturating_mul(GNOSIS_BLOCK_SECS);
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

/// Fair-value xDAI (wei) to swap for `needed_bzz` PLUR of xBZZ, plus a
/// fee/slippage buffer. Reads the pool's live `sqrtPriceX96`: the raw
/// price `(sqrtPriceX96/2^96)^2` is WXDAI-wei per BZZ-plur (token1 has
/// 18 decimals, token0 16), so `plur × price` is the WXDAI to send.
#[cfg(feature = "chain")]
async fn buffered_swap_input(
    client: &ant_chain::ChainClient,
    needed_bzz: u128,
) -> Result<u128, DriveError> {
    use primitive_types::U512;
    let word = client
        .pool_sqrt_price_x96(ant_chain::GNOSIS_BZZ_WXDAI_POOL)
        .await
        .map_err(|e| DriveError::Op(format!("read swap price: {e}")))?;
    let sp = U512::from_big_endian(&word);
    let fair = sp
        .checked_mul(sp)
        .and_then(|v| v.checked_mul(U512::from(needed_bzz)))
        .map(|v| v >> 192)
        .ok_or_else(|| DriveError::Op("swap price math overflowed".into()))?;
    let fair = u512_to_u128(fair)?;
    Ok(fair.saturating_mul(SWAP_BUFFER_NUM) / SWAP_BUFFER_DEN)
}

/// Narrow a `U512` into a `u128`, erroring if it doesn't fit (a swap
/// input above 2^128 wei would be a nonsensical multi-billion-xDAI buy).
#[cfg(feature = "chain")]
fn u512_to_u128(v: primitive_types::U512) -> Result<u128, DriveError> {
    if v > primitive_types::U512::from(u128::MAX) {
        return Err(DriveError::Op("swap amount too large".into()));
    }
    Ok(v.low_u128())
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
    let amount: u128 = amount_per_chunk
        .trim()
        .parse()
        .map_err(|_| DriveError::Op("invalid plan price".into()))?;
    if amount == 0 {
        return Err(DriveError::Op(
            "plan price must be greater than zero".into(),
        ));
    }
    let cmd_tx = h.cmd_tx.clone();
    let secret = h.signing_secret;
    let owner = h.eth;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        use primitive_types::U256;

        let client = h.chain_client(rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
        let postage = parse_addr(ant_chain::GNOSIS_POSTAGE_STAMP)?;
        let bzz = parse_addr(ant_chain::GNOSIS_BZZ_TOKEN)?;

        let amount_u256 = U256::from(amount);
        let total = amount_u256
            .checked_mul(U256::one() << u32::from(depth))
            .ok_or_else(|| DriveError::Op("plan cost overflows".into()))?;

        wallet
            .approve_bzz(&client, &bzz, &postage, total)
            .await
            .map_err(|e| DriveError::Op(format!("authorise payment: {e}")))?;
        let nonce = ant_crypto::random_overlay_nonce();
        let receipt = wallet
            .create_batch(
                &client,
                &postage,
                &owner,
                amount_u256,
                depth,
                POSTAGE_BUCKET_DEPTH,
                &nonce,
                immutable,
            )
            .await
            .map_err(|e| DriveError::Op(format!("buy storage: {e}")))?;
        let batch_id = ant_chain::tx::extract_created_batch_id(&receipt)
            .ok_or_else(|| DriveError::Op("storage purchase receipt had no batch".into()))?;
        register_batch(&cmd_tx, batch_id, depth, POSTAGE_BUCKET_DEPTH, immutable).await?;
        // Now that the wallet is funded and a batch exists, make sure
        // outbound settlement is on so the upload actually reaches the
        // network (best-effort; never fails the purchase).
        ensure_settlement(&cmd_tx, &client, &wallet, &data_dir, secret, owner).await;
        postage_status_json(&cmd_tx).await
    })
}

/// Buy and activate a storage plan funding **only with xDAI**: the node
/// swaps the xBZZ shortfall through the on-chain helper, then runs the
/// same `approve` + `createBatch` flow as [`storage_buy`].
///
/// Submits up to six real Gnosis transactions (one-time helper deploy,
/// swap, approve, createBatch, then the one-time chequebook deploy and
/// its settlement deposit) and spends real funds, so the app gates it
/// behind explicit confirmation. The swap is sized to cover the deposit
/// too — see [`deposit::bzz_to_acquire`].
#[cfg(feature = "chain")]
pub(crate) fn storage_buy_xdai(
    h: &AntHandle,
    rpc: String,
    depth: u8,
    amount_per_chunk: String,
    immutable: bool,
) -> Result<String, DriveError> {
    let amount: u128 = amount_per_chunk
        .trim()
        .parse()
        .map_err(|_| DriveError::Op("invalid plan price".into()))?;
    if amount == 0 {
        return Err(DriveError::Op(
            "plan price must be greater than zero".into(),
        ));
    }
    let cmd_tx = h.cmd_tx.clone();
    let secret = h.signing_secret;
    let owner = h.eth;
    let data_dir = h.data_dir.clone();
    h.runtime.block_on(async move {
        use primitive_types::U256;

        let client = h.chain_client(rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
        let postage = parse_addr(ant_chain::GNOSIS_POSTAGE_STAMP)?;
        let bzz = parse_addr(ant_chain::GNOSIS_BZZ_TOKEN)?;

        let total_plur = amount
            .checked_mul(1u128 << depth)
            .ok_or_else(|| DriveError::Op("plan cost overflows".into()))?;

        // 1) Top up xBZZ by swapping xDAI for the shortfall, if any. The
        //    shortfall covers the plan *and* the settlement deposit the
        //    chequebook still needs (step 3 below): sizing the swap from
        //    the plan alone spends every acquired xBZZ on the batch and
        //    leaves the chequebook backing nothing (#73). Same expression
        //    the quote priced with, so the user pays what they were shown.
        let have_bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &owner)
            .await
            .unwrap_or(0);
        let deposit_shortfall = quote_deposit_shortfall(&client, &data_dir, &owner).await;
        let needed_bzz = deposit::bzz_to_acquire(total_plur, deposit_shortfall, have_bzz);
        if needed_bzz > 0 {
            let xdai = client
                .eth_get_balance_lower128(&owner)
                .await
                .map_err(|e| DriveError::Op(format!("read xDAI balance: {e}")))?;
            let swap_input = buffered_swap_input(&client, needed_bzz).await?;
            let required = swap_input.saturating_add(GAS_RESERVE_WEI);
            if xdai < required {
                return Err(DriveError::Op(format!(
                    "not enough xDAI: send {} more xDAI to your account, then try again",
                    format_native(required - xdai)
                )));
            }
            let helper = wallet
                .ensure_swap_helper(&client)
                .await
                .map_err(|e| DriveError::Op(format!("prepare swap: {e}")))?;
            wallet
                .swap_xdai_for_bzz(
                    &client,
                    &helper,
                    &owner,
                    U256::from(swap_input),
                    U256::from(needed_bzz),
                )
                .await
                .map_err(|e| DriveError::Op(format!("swap xDAI for xBZZ: {e}")))?;
        }

        // 2) Authorise + create the batch, identical to the funded path.
        let amount_u256 = U256::from(amount);
        let total = amount_u256
            .checked_mul(U256::one() << u32::from(depth))
            .ok_or_else(|| DriveError::Op("plan cost overflows".into()))?;
        wallet
            .approve_bzz(&client, &bzz, &postage, total)
            .await
            .map_err(|e| DriveError::Op(format!("authorise payment: {e}")))?;
        let nonce = ant_crypto::random_overlay_nonce();
        let receipt = wallet
            .create_batch(
                &client,
                &postage,
                &owner,
                amount_u256,
                depth,
                POSTAGE_BUCKET_DEPTH,
                &nonce,
                immutable,
            )
            .await
            .map_err(|e| DriveError::Op(format!("buy storage: {e}")))?;
        let batch_id = ant_chain::tx::extract_created_batch_id(&receipt)
            .ok_or_else(|| DriveError::Op("storage purchase receipt had no batch".into()))?;
        register_batch(&cmd_tx, batch_id, depth, POSTAGE_BUCKET_DEPTH, immutable).await?;
        // Now that the wallet is funded and a batch exists, make sure
        // outbound settlement is on so the upload actually reaches the
        // network (best-effort; never fails the purchase).
        ensure_settlement(&cmd_tx, &client, &wallet, &data_dir, secret, owner).await;
        postage_status_json(&cmd_tx).await
    })
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
///      persist it, and fund it with [`deposit::TARGET_PLUR`] xBZZ so
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
/// the deposit lands — on the next buy or app launch.
/// The node wallet both pays gas and is the issuer, so no external key
/// is ever introduced.
#[cfg(feature = "chain")]
pub(crate) async fn ensure_settlement(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    swap_secret: [u8; 32],
    node_eth: [u8; 20],
) {
    let resolved = match resolve_or_deploy_chequebook(client, wallet, data_dir, node_eth).await {
        Ok(Some(r)) => r,
        Ok(None) => return,
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                "could not enable network settlement (uploads still work for a while, \
                 then stall until a chequebook exists): {e}",
            );
            return;
        }
    };
    let chequebook = resolved.address;
    // A chequebook we just deployed was funded as part of the deploy; an
    // adopted one carries whatever deposit it already had, which for
    // anything deployed before #73 is nothing at all.
    if !resolved.deployed {
        fund_chequebook_best_effort(client, wallet, &node_eth, &chequebook).await;
    }

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
async fn ensure_settlement_best_effort(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    client: &ant_chain::ChainClient,
    secret: [u8; 32],
    data_dir: &std::path::Path,
    node_eth: [u8; 20],
) {
    match ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID) {
        Ok(wallet) => ensure_settlement(cmd_tx, client, &wallet, data_dir, secret, node_eth).await,
        Err(e) => tracing::warn!(target: "ant-ffi", "settlement skipped (wallet init): {e}"),
    }
}

/// Whether outbound SWAP settlement is active for this device, as a JSON
/// object `{"enabled":bool,"chequebook":"0x…"|null}`. Settlement is the
/// thing that lets uploads actually propagate (bee charges the uploader
/// for every pushed chunk and freezes out a node that can't pay), so the
/// Storage tab reads this to warn the user when a connected plan still
/// won't upload reliably. Local + cheap: it just reads the persisted
/// `chequebook.json` association (written when a chequebook was
/// deployed), no chain round-trip.
#[cfg(feature = "chain")]
pub(crate) fn settlement_status(h: &AntHandle) -> Result<String, DriveError> {
    let path = h.data_dir.join("chequebook.json");
    let (enabled, chequebook) =
        match ant_chain::chequebook_store::load_persisted_chequebook_for(&path, &h.eth) {
            Ok(Some(cb)) => (true, Some(format!("0x{}", hex::encode(cb)))),
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

/// The xBZZ (PLUR) currently behind `chequebook` — the contract's own
/// xBZZ balance, which is exactly what its `balance()` view reports and
/// what a cashed cheque is paid out of.
#[cfg(feature = "chain")]
async fn chequebook_deposit_plur(
    client: &ant_chain::ChainClient,
    chequebook: &[u8; 20],
) -> Result<u128, DriveError> {
    client
        .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, chequebook)
        .await
        .map_err(|e| DriveError::Op(format!("read settlement deposit: {e}")))
}

/// The settlement deposit a storage buy should price in for this
/// account: the full target when no chequebook exists yet (the buy
/// deploys one, funded), otherwise whatever the existing one is short.
///
/// Best-effort like the balance reads around it: when the deposit can't
/// be read we quote the plan alone rather than charging for a deposit we
/// couldn't size — an under-quote is recoverable through the top-up
/// path, an over-quote takes the user's money for nothing.
#[cfg(feature = "chain")]
async fn quote_deposit_shortfall(
    client: &ant_chain::ChainClient,
    data_dir: &std::path::Path,
    owner: &[u8; 20],
) -> u128 {
    let Some(cb) = persisted_chequebook(data_dir, owner) else {
        return deposit::TARGET_PLUR;
    };
    match chequebook_deposit_plur(client, &cb).await {
        Ok(have) => deposit::shortfall(have),
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                "could not read the settlement deposit; pricing the plan alone: {e}",
            );
            0
        }
    }
}

/// Bring `chequebook` up to [`deposit::TARGET_PLUR`] from the node
/// wallet's spare xBZZ. Best-effort in every direction: an already-funded
/// chequebook, an unreadable balance, an empty wallet or a failed
/// transfer all just log — the caller is a storage flow that must not
/// fail over settlement housekeeping. A partial deposit (thin wallet) is
/// deliberately kept: some backing beats none.
#[cfg(feature = "chain")]
async fn fund_chequebook_best_effort(
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    node_eth: &[u8; 20],
    chequebook: &[u8; 20],
) {
    use primitive_types::U256;

    let have = match chequebook_deposit_plur(client, chequebook).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(target: "ant-ffi", "settlement deposit left as-is: {e}");
            return;
        }
    };
    let short = deposit::shortfall(have);
    if short == 0 {
        return;
    }
    let wallet_bzz = client
        .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, node_eth)
        .await
        .unwrap_or(0);
    let amount = short.min(wallet_bzz);
    if amount == 0 {
        tracing::warn!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            "chequebook holds no settlement deposit and the wallet has no spare xBZZ; \
             uploads will stall once peers stop extending credit — top it up from the Storage tab",
        );
        return;
    }
    match wallet
        .erc20_transfer(
            client,
            &ant_chain::chequebook::GNOSIS_BZZ_TOKEN_BYTES,
            chequebook,
            U256::from(amount),
        )
        .await
    {
        Ok(r) => tracing::info!(
            target: "ant-ffi",
            chequebook = %format!("0x{}", hex::encode(chequebook)),
            deposit_plur = amount,
            tx = %format!("0x{}", hex::encode(r.tx_hash)),
            "funded the chequebook so its cheques are backed",
        ),
        Err(e) => tracing::warn!(
            target: "ant-ffi",
            "settlement deposit transfer failed; the chequebook still works but backs no cheque: {e}",
        ),
    }
}

/// The chequebook's settlement deposit, read from chain, plus what a
/// top-up to [`deposit::TARGET_PLUR`] would cost — the "is this
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
/// funded. Two or three light `eth_call`s, so it belongs on an explicit
/// refresh, not on every status poll.
#[cfg(feature = "chain")]
pub(crate) fn settlement_deposit(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let data_dir = h.data_dir.clone();
    let owner = h.eth;
    h.runtime.block_on(async move {
        let Some(cb) = persisted_chequebook(&data_dir, &owner) else {
            return to_json(&SettlementDeposit::none());
        };
        let client = h.chain_client(rpc);
        let deposited = chequebook_deposit_plur(&client, &cb).await?;
        settlement_deposit_json(&client, &owner, &cb, deposited).await
    })
}

/// Fund the node's chequebook up to [`deposit::TARGET_PLUR`], funding
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
#[cfg(feature = "chain")]
pub(crate) fn settlement_topup_xdai(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let data_dir = h.data_dir.clone();
    let owner = h.eth;
    let secret = h.signing_secret;
    h.runtime.block_on(async move {
        use primitive_types::U256;

        let cb = persisted_chequebook(&data_dir, &owner).ok_or_else(|| {
            DriveError::Op(
                "no chequebook for this account yet — connect or buy a storage plan first".into(),
            )
        })?;
        let client = h.chain_client(rpc);
        let deposited = chequebook_deposit_plur(&client, &cb).await?;
        let short = deposit::shortfall(deposited);
        if short == 0 {
            return settlement_deposit_json(&client, &owner, &cb, deposited).await;
        }

        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("wallet: {e}")))?;
        let have_bzz = client
            .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, &owner)
            .await
            .unwrap_or(0);
        let to_acquire = short.saturating_sub(have_bzz);
        if to_acquire > 0 {
            let xdai = client
                .eth_get_balance_lower128(&owner)
                .await
                .map_err(|e| DriveError::Op(format!("read xDAI balance: {e}")))?;
            let swap_input = buffered_swap_input(&client, to_acquire).await?;
            let required = swap_input.saturating_add(GAS_RESERVE_WEI);
            if xdai < required {
                return Err(DriveError::Op(format!(
                    "not enough xDAI: send {} more xDAI to your account, then try again",
                    format_native(required - xdai)
                )));
            }
            let helper = wallet
                .ensure_swap_helper(&client)
                .await
                .map_err(|e| DriveError::Op(format!("prepare swap: {e}")))?;
            wallet
                .swap_xdai_for_bzz(
                    &client,
                    &helper,
                    &owner,
                    U256::from(swap_input),
                    U256::from(to_acquire),
                )
                .await
                .map_err(|e| DriveError::Op(format!("swap xDAI for xBZZ: {e}")))?;
        }

        wallet
            .erc20_transfer(
                &client,
                &ant_chain::chequebook::GNOSIS_BZZ_TOKEN_BYTES,
                &cb,
                U256::from(short),
            )
            .await
            .map_err(|e| DriveError::Op(format!("deposit into chequebook: {e}")))?;

        // Re-read rather than assuming: the card should show what the
        // chain says the deposit is now, not what we intended it to be.
        let deposited = chequebook_deposit_plur(&client, &cb).await?;
        settlement_deposit_json(&client, &owner, &cb, deposited).await
    })
}

/// Render the settlement-deposit card payload for a known chequebook,
/// pricing the outstanding top-up the same way the plan quote prices a
/// buy (swap input for the missing xBZZ + gas reserve, against the
/// account's xDAI).
#[cfg(feature = "chain")]
async fn settlement_deposit_json(
    client: &ant_chain::ChainClient,
    owner: &[u8; 20],
    chequebook: &[u8; 20],
    deposited: u128,
) -> Result<String, DriveError> {
    let short = deposit::shortfall(deposited);
    let wallet_bzz = client
        .erc20_balance_of_lower128(ant_chain::GNOSIS_BZZ_TOKEN, owner)
        .await
        .unwrap_or(0);
    let to_acquire = short.saturating_sub(wallet_bzz);
    let swap_input = if to_acquire > 0 {
        buffered_swap_input(client, to_acquire).await?
    } else {
        0
    };
    // Nothing to fund → nothing to ask for, not "a gas reserve please".
    let xdai_required = if short > 0 {
        swap_input.saturating_add(GAS_RESERVE_WEI)
    } else {
        0
    };
    let xdai = client.eth_get_balance_lower128(owner).await.unwrap_or(0);
    let xdai_to_send = xdai_required.saturating_sub(xdai);
    to_json(&SettlementDeposit {
        enabled: true,
        chequebook: Some(format!("0x{}", hex::encode(chequebook))),
        deposit_plur: deposited.to_string(),
        deposit_bzz: format_bzz(deposited),
        target_plur: deposit::TARGET_PLUR.to_string(),
        target_bzz: format_bzz(deposit::TARGET_PLUR),
        shortfall_plur: short.to_string(),
        shortfall_bzz: format_bzz(short),
        needs_top_up: deposit::needs_top_up(deposited),
        xdai_required: xdai_required.to_string(),
        xdai_required_display: format_native(xdai_required),
        xdai_to_send: xdai_to_send.to_string(),
        xdai_to_send_display: format_native(xdai_to_send),
        sufficient_funds: xdai >= xdai_required,
    })
}

/// Deploy (or return the already-persisted) node-owned chequebook for the
/// iOS publish-setup checklist's "chequebook deployed" step. Idempotent:
/// if `<data_dir>/chequebook.json` already records a chequebook, it's
/// returned without a redeploy (though a chequebook still short of its
/// settlement deposit is topped up); otherwise this signs an on-chain
/// `factory.deploySimpleSwap`, funds it with [`deposit::TARGET_PLUR`]
/// xBZZ so its cheques are backed, persists the association, and returns
/// the new address. Blocks on the handle's tokio runtime.
///
/// Returns `{"chequebookAddress":"0x<40hex>"}` JSON. The caller restarts
/// the gateway afterwards so [`crate::ant_start_gateway`] reloads the
/// persisted address into its `ChainContext` and `/chequebook/address`
/// reports it.
#[cfg(feature = "chain")]
pub(crate) fn deploy_chequebook(h: &AntHandle, rpc: String) -> Result<String, DriveError> {
    let secret = h.signing_secret;
    let node_eth = h.eth;
    let data_dir = h.data_dir.clone();

    h.runtime.block_on(async move {
        let client = h.chain_client(rpc);
        let wallet = ant_chain::tx::Wallet::new(secret, GNOSIS_CHAIN_ID)
            .map_err(|e| DriveError::Op(format!("derive node wallet: {e}")))?;

        // persisted → rediscover the wallet's existing on-chain chequebook
        // → deploy a fresh one (persisting the association). Shared with
        // `antd`'s resolve/rediscover/deploy mechanics, so a wallet that
        // already deployed a chequebook (e.g. on desktop with the same
        // vault) is adopted rather than duplicated.
        match resolve_or_deploy_chequebook(&client, &wallet, &data_dir, node_eth).await? {
            Some(resolved) => {
                // An adopted chequebook may still be at deposit 0 — fund
                // it here too, so this checklist step means the same
                // thing whichever branch produced the address.
                if !resolved.deployed {
                    fund_chequebook_best_effort(&client, &wallet, &node_eth, &resolved.address)
                        .await;
                }
                to_json(&DeployedChequebook {
                    chequebook_address: format!("0x{}", hex::encode(resolved.address)),
                })
            }
            None => Err(DriveError::Op(
                "chequebook could not be deployed — wallet has no xDAI for gas".into(),
            )),
        }
    })
}

/// A chequebook resolved for this account, and how we got to it.
#[cfg(feature = "chain")]
struct ResolvedChequebook {
    /// The 20-byte chequebook contract address.
    address: [u8; 20],
    /// `true` when *this* call deployed it — and therefore already
    /// funded it with [`deposit::TARGET_PLUR`] as part of the deploy.
    /// `false` for a persisted / rediscovered chequebook, whose deposit
    /// is whatever it happens to hold (zero, for anything deployed
    /// before #73).
    deployed: bool,
}

/// Reuse / rediscover / deploy a chequebook for `node_eth`, persisting
/// the association so future launches reload it directly. Returns the
/// resolved chequebook, or `None` when the wallet can't afford the
/// one-time deploy (a soft skip, not an error). The persist /
/// rediscover / deploy mechanics are shared with `antd` via
/// [`ant_chain::chequebook_store`]; only the resolution *order* (no
/// operator-flag branches) and the deposit sizing live here.
#[cfg(feature = "chain")]
async fn resolve_or_deploy_chequebook(
    client: &ant_chain::ChainClient,
    wallet: &ant_chain::tx::Wallet,
    data_dir: &std::path::Path,
    node_eth: [u8; 20],
) -> Result<Option<ResolvedChequebook>, DriveError> {
    use ant_chain::chequebook_store::{self, ChequebookError, ChequebookFile};

    let persist_path = data_dir.join("chequebook.json");

    // 1. Already known (persisted from a prior run / this session), and
    //    issued by *this* account — a record left behind by a different
    //    account is skipped, so we rediscover / deploy our own below
    //    instead of signing cheques nobody will honour.
    if let Some(cb) = chequebook_store::load_persisted_chequebook_for(&persist_path, &node_eth)
        .map_err(map_cb_err)?
    {
        return Ok(Some(ResolvedChequebook {
            address: cb,
            deployed: false,
        }));
    }

    // 2. Rediscover a chequebook this node EOA already owns on-chain
    //    (reinstall with a restored key). Adopt + persist it.
    match ant_chain::discover::discover_owned_chequebook(
        client,
        &ant_chain::chequebook::GNOSIS_CHEQUEBOOK_FACTORY,
        ant_chain::GNOSIS_POSTAGE_STAMP,
        ant_chain::GNOSIS_BZZ_TOKEN,
        &node_eth,
        ant_chain::discover::GNOSIS_XBZZ_DEPLOY_BLOCK,
    )
    .await
    {
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
            return Ok(Some(ResolvedChequebook {
                address: cb,
                deployed: false,
            }));
        }
        Ok(None) => {}
        Err(e) => tracing::warn!(target: "ant-ffi", "chequebook rediscovery scan failed: {e}"),
    }

    // 3. Auto-deploy, funded with [`deposit::TARGET_PLUR`]: bee accepts
    //    the cheques either way, but a chequebook backing nothing only
    //    publishes until the peers' payment tolerance runs out and then
    //    stalls (#73) — so the deposit goes in at deploy time, capped by
    //    the wallet's xBZZ balance (a thin wallet gets a smaller deposit
    //    rather than a failed transfer). Insufficient gas is a soft skip
    //    — settlement turns on once the wallet has a little more xDAI.
    match chequebook_store::auto_deploy_chequebook(
        client,
        wallet,
        &node_eth,
        deposit::TARGET_PLUR,
        &persist_path,
    )
    .await
    {
        Ok(cb) => Ok(Some(ResolvedChequebook {
            address: cb,
            deployed: true,
        })),
        Err(ChequebookError::InsufficientGas { need, .. }) => {
            tracing::warn!(
                target: "ant-ffi",
                "not enough spare xDAI to deploy a chequebook (need ~{need} wei); \
                 network settlement will turn on after you add a little more xDAI and buy again",
            );
            Ok(None)
        }
        Err(e) => Err(map_cb_err(e)),
    }
}

/// Map a shared-chequebook-store error into the drive op error.
#[cfg(feature = "chain")]
fn map_cb_err(e: ant_chain::chequebook_store::ChequebookError) -> DriveError {
    DriveError::Op(e.to_string())
}

/// Render a PLUR amount as a short xBZZ decimal string (4 dp).
#[cfg(feature = "chain")]
fn format_bzz(plur: u128) -> String {
    let whole = plur / PLUR_PER_BZZ;
    let frac = (plur % PLUR_PER_BZZ) / (PLUR_PER_BZZ / 10_000); // 4 decimals
    format!("{whole}.{frac:04}")
}

/// Render a wei amount as a short xDAI decimal string (4 dp).
#[cfg(feature = "chain")]
fn format_native(wei: u128) -> String {
    let whole = wei / WEI_PER_XDAI;
    let frac = (wei % WEI_PER_XDAI) / (WEI_PER_XDAI / 10_000); // 4 decimals
    format!("{whole}.{frac:04}")
}

#[cfg(feature = "chain")]
fn parse_addr(s: &str) -> Result<[u8; 20], DriveError> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let mut out = [0u8; 20];
    hex::decode_to_slice(s, &mut out)
        .map_err(|e| DriveError::Op(format!("invalid contract address: {e}")))?;
    Ok(out)
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
    /// The deposit we aim for — [`deposit::TARGET_PLUR`].
    target_plur: String,
    target_bzz: String,
    /// What is still missing (`target − deposit`, saturating).
    shortfall_plur: String,
    shortfall_bzz: String,
    /// `shortfall > 0`, via [`deposit::needs_top_up`] — the one
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
    fn none() -> Self {
        Self {
            enabled: false,
            chequebook: None,
            deposit_plur: "0".into(),
            deposit_bzz: format_bzz(0),
            target_plur: deposit::TARGET_PLUR.to_string(),
            target_bzz: format_bzz(deposit::TARGET_PLUR),
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

#[cfg(test)]
mod tests {
    use super::deposit;
    use super::PLUR_PER_BZZ;

    /// The default deposit is 0.001 xBZZ — the figure the #67 benchmark
    /// measured backing 65 K+ cheques. Pinned against the PLUR scale so a
    /// decimals slip (xBZZ has 16, not 18) can't quietly turn it into
    /// 0.1 xBZZ of the user's money, or into dust that stalls again.
    #[test]
    fn deposit_target_is_one_thousandth_of_a_bzz() {
        assert_eq!(deposit::TARGET_PLUR, 10_000_000_000_000);
        assert_eq!(deposit::TARGET_PLUR * 1_000, PLUR_PER_BZZ);
    }

    #[test]
    fn shortfall_is_what_is_missing_and_never_negative() {
        // The pre-#73 chequebook: deployed, backing nothing.
        assert_eq!(deposit::shortfall(0), deposit::TARGET_PLUR);
        assert!(deposit::needs_top_up(0));
        // Partially funded (a thin wallet got a capped deposit).
        assert_eq!(
            deposit::shortfall(deposit::TARGET_PLUR / 4),
            deposit::TARGET_PLUR - deposit::TARGET_PLUR / 4
        );
        assert!(deposit::needs_top_up(deposit::TARGET_PLUR / 4));
        // At or above target: nothing owed, and no second charge.
        assert_eq!(deposit::shortfall(deposit::TARGET_PLUR), 0);
        assert!(!deposit::needs_top_up(deposit::TARGET_PLUR));
        assert_eq!(deposit::shortfall(deposit::TARGET_PLUR * 10), 0);
        assert!(!deposit::needs_top_up(deposit::TARGET_PLUR * 10));
    }

    #[test]
    fn buy_acquires_the_plan_plus_the_missing_deposit() {
        let plan = 5 * PLUR_PER_BZZ;
        // Fresh account: nothing in the wallet, nothing behind the
        // chequebook — the buy has to acquire both, or it spends
        // everything on postage and deploys a chequebook backing nothing.
        assert_eq!(
            deposit::bzz_to_acquire(plan, deposit::shortfall(0), 0),
            plan + deposit::TARGET_PLUR
        );
        // Chequebook already funded: the plan alone, no double charge.
        assert_eq!(
            deposit::bzz_to_acquire(plan, deposit::shortfall(deposit::TARGET_PLUR), 0),
            plan
        );
        // Wallet already holds the deposit: only the plan is missing.
        assert_eq!(
            deposit::bzz_to_acquire(plan, deposit::shortfall(0), deposit::TARGET_PLUR),
            plan
        );
        // Wallet covers everything: nothing to swap.
        assert_eq!(
            deposit::bzz_to_acquire(plan, deposit::shortfall(0), plan + deposit::TARGET_PLUR),
            0
        );
        assert_eq!(
            deposit::bzz_to_acquire(plan, deposit::shortfall(0), u128::MAX),
            0
        );
    }
}
