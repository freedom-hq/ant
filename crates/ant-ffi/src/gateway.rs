//! In-process bee-shaped HTTP gateway for the iOS app.
//!
//! [`ant_start_gateway`] boots the same [`ant_gateway`] HTTP surface
//! `antd` serves — `/bzz`, `/bytes`, `/chunks`, `/feeds`, `/soc`,
//! `/tags`, status, and the bee-stubbed `/wallet` / `/stamps` /
//! `/chequebook` — on a loopback address **inside the app process**,
//! driven by the node loop [`crate::ant_init`] already started.
//!
//! iOS apps cannot spawn the `antd` daemon (no subprocesses in the app
//! sandbox), so this is the in-process equivalent: the existing
//! bee-HTTP Swift layer (`BeeAPIClient`, `BzzSchemeHandler`) points at
//! `http://127.0.0.1:<port>` unchanged. There is exactly one gateway
//! per handle; a second `ant_start_gateway` while one is running
//! succeeds without touching it; with a `gnosis_rpc` it re-runs the
//! chain startup work (persisted-batch check, owned-batch rediscovery,
//! chequebook adoption) so a step that failed earlier is retried —
//! a step that already succeeded is skipped.
//!
//! CORS is off by default: the gateway has no auth, so any page a
//! browser lets read its responses can read `/wallet`, `/addresses`,
//! `/stamps`, … A host whose own pages need cross-origin access opts
//! in with [`ant_set_gateway_cors`] before starting (issue #101).
//!
//! CORS only governs whether a page may *read* a response. It does not
//! stop a page from *sending* a request: a CORS-simple request (a
//! `POST` with no body or a form/text body and no custom headers) needs
//! no preflight, so any page can still fire the state-changing routes —
//! `POST /stamps/{amount}/{depth}` (buys a batch and, with a
//! `gnosis_rpc`, then deploys a chequebook if the account has none and
//! funds it, or tops up an existing one below the settlement-deposit
//! target, by transferring the wallet's existing xBZZ — every one of
//! those transactions costs xDAI gas; nothing is swapped),
//! `POST /chequebook/deposit` (moves xBZZ, paying xDAI gas) —
//! with `mode: 'no-cors'`, whatever
//! the allow-list says. Those routes are unprotected against
//! cross-site requests; see issue #105.
//!
//! The gateway also serves the xDAI storage-funding routes,
//! `POST /v0/storage/buy`, `POST /v0/storage/extend` and
//! `POST /v0/settlement/deposit`, which **do** swap the wallet's xDAI
//! into xBZZ (and then buy/extend a batch or fund the chequebook
//! deposit). Those refuse (`403`) any request carrying a browser
//! `Origin` or cross-origin `Sec-Fetch-Site`, unless the origin is listed
//! exactly in [`ant_set_gateway_cors`] — `*` and `null` don't count. So
//! a page can't spend xDAI through them, but an exactly listed origin
//! can; the host itself (no `Origin`) always can.

use crate::{clear_out_err, write_out_err, AntHandle};
use ant_control::GatewayActivity;
use ant_gateway::{
    CorsConfig, Gateway, GatewayChainState, GatewayHandle, GatewayIdentity, TagRegistry,
};
use k256::ecdsa::SigningKey;
use std::ffi::{c_char, CStr};
use std::net::SocketAddr;
use std::sync::Arc;

/// Bee API version advertised via `/health.apiVersion`. Mirrors `antd`'s
/// `BEE_API_VERSION` so `bee-js`/Freedom see the same wire contract.
const BEE_API_VERSION: &str = "7.2.0";

/// Default loopback bind when `api_addr` is null/empty. Mirrors bee's
/// default API port so the iOS bee-HTTP client needs no base-URL change.
const DEFAULT_API_ADDR: &str = "127.0.0.1:1633";

/// Start the in-process bee-shaped HTTP gateway on `api_addr`
/// (default `127.0.0.1:1633` when null/empty), serving the node the
/// handle owns.
///
/// `light_mode` drives `GET /node.beeMode`: `true` advertises
/// `light` (Freedom's `checkSwarmPreFlight` allows publish/feed/SOC
/// writes), `false` advertises `ultra-light` (read-only browsing).
///
/// `gnosis_rpc` is the Gnosis JSON-RPC endpoint backing the on-chain
/// `/wallet`, `/stamps`, and `/chequebook` surfaces. Pass null/empty to
/// disable on-chain reads (those endpoints fall back to the bee
/// zero-stub / `501`). When set together with `light_mode`, it enables
/// real `/wallet` balances and `/stamps` postage state, reaching desktop
/// (`antd`) parity. Only honoured when the crate is built with the
/// `chain` feature; ignored otherwise.
///
/// A `gnosis_rpc` also triggers the chain-derived startup work
/// [`crate::ant_init`] can't do without an RPC (`drive::ChainInit`,
/// the counterpart of `antd`'s startup chain block). It runs in the
/// background right after the gateway starts:
///
/// 1. Reloaded `postage/*.bin` batches the chain reports as missing
///    (evicted or never created), expired (`remainingBalance` 0) or
///    owned by another key are unregistered, with a `WARN` naming the
///    batch id. They're no longer listed by `GET /stamps` and can't be
///    stamped with; their files stay on disk. "Missing" must be read
///    twice, 45 seconds apart, before it counts: a batch bought just
///    before a relaunch can read as missing on an RPC backend that
///    hasn't seen its creation block yet, so the first such read only
///    schedules a background re-check (the batch stays registered
///    meanwhile, and steps 2 and 3 don't wait for it).
/// 2. Funded batches the account owns on-chain but not on disk
///    (reinstall, restore from key) are registered.
/// 3. The persisted or on-chain chequebook is adopted and outbound
///    settlement switched on. Nothing is deployed or funded.
///
/// A step that fails (a batch whose read fails stays registered, a
/// failed rediscovery scan, a chequebook that couldn't be resolved) is
/// retried by the next call with a `gnosis_rpc` — including an
/// idempotent one that finds the gateway already running, so a host may
/// simply re-call this (e.g. on foreground) to retry. Steps that already
/// succeeded are not repeated.
///
/// With a `gnosis_rpc`, a batch bought through `POST /stamps` also makes
/// sure settlement is on afterwards, as `ant_storage_buy` does: it
/// deploys a chequebook if the account has none (paying gas in xDAI)
/// and brings the deposit of a new or existing chequebook up to the
/// settlement target from the wallet's existing xBZZ (capped to what
/// the wallet holds; nothing is swapped).
///
/// The gateway's chain wiring is captured **here, once**. A host that
/// serves chain reads itself must therefore call
/// [`crate::ant_set_chain_transport`] *before* this; installing one
/// later only affects the per-call `ant_storage_*` / `ant_deploy_chequebook`
/// paths until the gateway is stopped and started again. What is
/// captured is the handle's transport *slot*, though, so replacing or
/// clearing a transport that was installed before the start does reach
/// this gateway immediately — a cleared one falls back to `gnosis_rpc`
/// rather than calling a `host_ctx` the host has been told it may free.
///
/// CORS: the gateway lets cross-origin pages read its responses only
/// for the origins last set with [`ant_set_gateway_cors`]; by default
/// none, so it sends no CORS headers and no page from another origin can
/// read its responses. That does not stop a page from *sending*
/// CORS-simple requests, which still execute — including spending ones
/// like `POST /stamps/{amount}/{depth}` (which may also deploy a
/// chequebook and move xBZZ into a new or under-funded one, above) and
/// `POST /chequebook/deposit` (see the module docs). The xDAI-swapping
/// `/v0/storage/buy`, `/v0/storage/extend` and `POST
/// /v0/settlement/deposit` routes it also serves refuse any request
/// from a web page unless its origin is listed exactly (not `*` or
/// `null`) in [`ant_set_gateway_cors`].
///
/// Returns `true` on success (or if a gateway is already running),
/// `false` on error with an allocated message written to `out_err`
/// (free with [`crate::ant_free_string`]). Idempotent: a second call
/// while one is live is a success that leaves the running gateway
/// untouched — it keeps the CORS list and chain wiring it started with
/// (a changed [`ant_set_gateway_cors`] list only applies after
/// [`ant_stop_gateway`] + start). With a `gnosis_rpc` it re-runs the
/// chain startup work above, retrying only what hasn't succeeded yet: a
/// batch whose check failed or is still pending is re-read, and a
/// rediscovery scan or chequebook adoption that failed is retried. A
/// rediscovery scan or adoption that already succeeded is not repeated
/// in this process — a batch bought on another device only shows up
/// after an explicit `ant_storage_discover`, or a fresh `ant_init`
/// followed by a start with a `gnosis_rpc` (`ant_init` alone only
/// reloads persisted state; the rescan runs here, in `ChainInit::run`).
///
/// # Safety
///
/// `handle` must come from [`crate::ant_init`] and must not have been
/// passed to [`crate::ant_shutdown`]. `api_addr` and `gnosis_rpc`, if
/// non-null, must be NUL-terminated UTF-8 strings. `out_err`, if
/// non-null, must point to a writable `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_start_gateway(
    handle: *const AntHandle,
    api_addr: *const c_char,
    light_mode: bool,
    gnosis_rpc: *const c_char,
    out_err: *mut *mut c_char,
) -> bool {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_start_gateway: null handle");
            return false;
        };

        let addr_str = if api_addr.is_null() {
            DEFAULT_API_ADDR.to_string()
        } else {
            match CStr::from_ptr(api_addr).to_str().map(str::trim) {
                Ok(s) if !s.is_empty() => s.to_string(),
                Ok(_) => DEFAULT_API_ADDR.to_string(),
                Err(_) => {
                    write_out_err(out_err, "ant_start_gateway: api_addr is not valid UTF-8");
                    return false;
                }
            }
        };
        let addr: SocketAddr = match addr_str.parse() {
            Ok(a) => a,
            Err(e) => {
                write_out_err(
                    out_err,
                    &format!("ant_start_gateway: invalid api_addr `{addr_str}`: {e}"),
                );
                return false;
            }
        };

        // Optional Gnosis JSON-RPC endpoint backing the on-chain
        // `/wallet` / `/stamps` / `/chequebook` surfaces. Same parse as
        // `api_addr`: null/empty -> None, invalid UTF-8 is a hard error.
        let gnosis_rpc = if gnosis_rpc.is_null() {
            None
        } else {
            match CStr::from_ptr(gnosis_rpc).to_str().map(str::trim) {
                Ok(s) if !s.is_empty() => Some(s.to_string()),
                Ok(_) => None,
                Err(_) => {
                    write_out_err(out_err, "ant_start_gateway: gnosis_rpc is not valid UTF-8");
                    return false;
                }
            }
        };

        // Idempotent: if a gateway is already live, do nothing. A task
        // that has finished (bind error / aborted) is cleared so a
        // retry can rebind.
        //
        // Hold this guard across the whole start (check → bind → build →
        // spawn → store): releasing it after the check would let two
        // concurrent starts both pass, both spawn `Gateway::serve` (the
        // loser hits the bind race), and the dead `JoinHandle` overwrite
        // the live one — leaving a gateway that `ant_stop_gateway` can't
        // abort. Poison-tolerant (`into_inner`) so a panic elsewhere
        // doesn't unwind out of this `extern "C"` fn and abort the host.
        let mut slot = handle
            .gateway_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.as_ref().is_some_and(|task| !task.is_finished()) {
            // Still retry the chain init: a run whose RPC reads failed
            // left work pending (unconfirmed batches, a failed
            // rediscovery scan, no chequebook adopted), and re-calling
            // start (e.g. on foreground) is how a host asks for that
            // retry.
            #[cfg(feature = "chain")]
            if let Some(rpc) = gnosis_rpc {
                spawn_chain_init(handle, handle.chain_client(rpc));
            }
            return true;
        }
        // A finished task (bind error / aborted) is cleared so a retry
        // can rebind.
        *slot = None;

        // Identity surface for `/addresses`. Overlay + peer-id come from
        // the live status snapshot; the compressed secp256k1 public key
        // isn't carried there, so derive it from the signing secret
        // (same key the node loaded at init).
        let (overlay_hex, ethereum_hex, peer_id) = {
            let snap = handle.status_rx.borrow();
            (
                snap.identity.overlay.trim_start_matches("0x").to_string(),
                snap.identity.eth_address.clone(),
                snap.identity.peer_id.clone(),
            )
        };
        let public_key_hex = match SigningKey::from_bytes((&handle.signing_secret).into()) {
            Ok(sk) => hex::encode(sk.verifying_key().to_encoded_point(true).as_bytes()),
            Err(e) => {
                write_out_err(
                    out_err,
                    &format!("ant_start_gateway: invalid signing key: {e}"),
                );
                return false;
            }
        };

        // The host's CORS allow-list (empty unless it called
        // `ant_set_gateway_cors`). Read under the `gateway_task` guard,
        // which the setter also takes, so it can't change mid-start.
        let cors = CorsConfig::new(
            handle
                .gateway_cors
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter(),
        );

        // Probe-bind synchronously so a port clash surfaces as a clean
        // FFI error instead of a silently-dead background task. There's
        // a tiny TOCTOU window before `Gateway::serve` rebinds, but on
        // iOS this process is the only thing touching the port.
        if let Err(e) = std::net::TcpListener::bind(addr) {
            write_out_err(out_err, &format!("ant_start_gateway: bind {addr}: {e}"));
            return false;
        }

        // On-chain context for `/wallet` / `/stamps` / `/chequebook`.
        // Built only when the `chain` feature is on AND a Gnosis RPC was
        // supplied; otherwise the gateway falls back to the bee
        // zero-stub / `501`. Mirrors how `antd` wires its `ChainContext`
        // via `ant_gateway::chainreader::build`.
        #[cfg(feature = "chain")]
        let chain = if gnosis_rpc.is_some() {
            // Report a previously-deployed chequebook (persisted at
            // `<data_dir>/chequebook.json` by `ant_deploy_chequebook` /
            // the storage-buy flow) so `/chequebook/address` reflects it.
            // Read-only + fast: this NEVER deploys here (that's
            // `ant_deploy_chequebook`, which spends gas) and a load error
            // degrades to `None` rather than failing gateway start. A
            // chequebook this process's chain check disqualified isn't
            // reported (settlement is off for it), so
            // `POST /chequebook/deposit` can't fund it either.
            let chequebook = match ant_chain::chequebook_store::load_persisted_chequebook_for(
                &handle.data_dir.join("chequebook.json"),
                &handle.eth,
            ) {
                Ok(cb) => cb,
                Err(e) => {
                    tracing::warn!(
                        target: "ant-ffi",
                        "ant_start_gateway: ignoring chequebook association: {e}",
                    );
                    None
                }
            };
            // The handle's one slot, reset for this start and kept
            // current afterwards (see `AntHandle::gateway_chequebook`).
            // A disqualified one is recorded as refused: the gateway
            // then prices no deposit for it and funds none.
            match chequebook {
                Some(cb) if crate::drive::is_disqualified(&handle.eth, &cb) => {
                    handle.gateway_chequebook.refuse(cb);
                }
                Some(cb) => handle.gateway_chequebook.set(cb),
                None => handle.gateway_chequebook.clear(),
            }
            ant_gateway::chainreader::build_with_transport(
                gnosis_rpc.clone(),
                // No read-only fallback on mobile: chain reads stay gated
                // on the host-supplied `gnosis_rpc` (this branch only runs
                // when it's set), so behavior is unchanged.
                None,
                // No per-node postage-contract override on mobile: use
                // the Gnosis mainnet default (matches `antd`'s default).
                ant_chain::GNOSIS_POSTAGE_STAMP.to_string(),
                handle.eth,
                handle.gateway_chequebook.clone(),
                ant_chain::tx::GNOSIS_CHAIN_ID,
                Some(handle.signing_secret),
                // The storage flows keep the chequebook at the shared
                // 0.001 xBZZ deposit, so `/v0/storage/quote` prices it in.
                Some(ant_chain::chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR),
                // Host-provided chain transport (issue #77), if the app
                // installed one with `ant_set_chain_transport` before
                // starting the gateway. `None` — the default — leaves
                // `/wallet`, `/stamps`, `/chainstate` and `/chequebook`
                // reading the `gnosis_rpc` URL exactly as before.
                handle.host_chain_transport(),
                // The account's process-wide wallet tx lock, which the
                // drive flows and the after-buy settlement task below
                // hold too: a `POST /stamps` can't race their deposit
                // transfer or deploy for a nonce or for xBZZ.
                crate::drive::wallet_tx_lock(&handle.eth),
            )
        } else {
            None
        };
        #[cfg(not(feature = "chain"))]
        let _ = gnosis_rpc;
        // One chain client, routed through the host transport like every
        // client this crate builds, for the background chain init and
        // the after-buy hook below.
        #[cfg(feature = "chain")]
        let chain_client = gnosis_rpc.clone().map(|rpc| handle.chain_client(rpc));
        #[cfg(feature = "chain")]
        let on_batch_bought = chain_client
            .clone()
            .map(|client| after_buy_hook(handle, client));
        #[cfg(not(feature = "chain"))]
        let on_batch_bought = None;
        #[cfg(feature = "chain")]
        let on_chequebook_refused = chain_client
            .clone()
            .map(|client| chequebook_refused_hook(handle, client));
        #[cfg(not(feature = "chain"))]
        let on_chequebook_refused = None;

        let gw = GatewayHandle {
            agent: Arc::new(crate::ANT_FFI_AGENT.to_string()),
            api_version: Arc::new(BEE_API_VERSION.to_string()),
            identity: Arc::new(GatewayIdentity {
                overlay_hex,
                ethereum_hex,
                public_key_hex,
                peer_id,
            }),
            status: handle.status_rx.clone(),
            commands: handle.cmd_tx.clone(),
            // Standalone activity registry: the gateway is the only
            // writer on iOS (no `antop` Retrieval tab consuming it).
            activity: GatewayActivity::new(),
            tags: Arc::new(TagRegistry::new()),
            // Host-chosen allow-list, empty (= no CORS headers, like
            // bee without `--cors-allowed-origins`) by default. This
            // used to be pinned to `null`, which any page can send: a
            // fetch that is redirected after a cross-origin hop carries
            // `Origin: null` (issue #101).
            cors: Arc::new(cors),
            // The FFI path resolves its chain wiring before starting
            // the gateway, so the slot is preset — no chain-init 503
            // window here. On-chain reader/writer when built with the
            // `chain` feature and a Gnosis RPC was supplied (see
            // `chain` above); otherwise `None`, so `/wallet` +
            // `/chequebook` report bee zero-stubs and chain-state
            // endpoints fall to 501.
            chain_state: GatewayChainState {
                light_mode,
                #[cfg(feature = "chain")]
                chain,
                #[cfg(not(feature = "chain"))]
                chain: None,
            }
            .preset(),
            // ACT publisher identity: the node signing key, like bee's
            // accesscontrol session over the swarm key.
            act_secret: Arc::new(handle.signing_secret),
            on_batch_bought,
            on_chequebook_refused,
        };

        // `/health.walletScan` reads `pending` from the first request on:
        // the chain state is preset, so `chainReady` is already true.
        #[cfg(feature = "chain")]
        if chain_client.is_some() {
            handle.chain_init.note_pending();
        }
        let task = handle.runtime.spawn(async move {
            if let Err(e) = Gateway::serve(gw, addr).await {
                tracing::error!(target: "ant-ffi", "in-process gateway ended: {e}");
            }
        });
        *slot = Some(task);

        // First point an RPC is known: run the chain-derived startup
        // work `ant_init` couldn't (see the doc comment above). Off the
        // caller's thread so the gateway start never waits on the RPC;
        // failed steps are retried by the next call with an RPC.
        #[cfg(feature = "chain")]
        if let Some(chain) = chain_client {
            spawn_chain_init(handle, chain);
        }
        true
    }
}

/// The gateway's after-buy hook: once `POST /stamps` has bought and
/// registered a batch, make sure outbound settlement is on, resolving,
/// deploying or funding the chequebook exactly as `ant_storage_buy`
/// does. Without it, the first batch bought through the gateway in a
/// fresh install's session uploads without paying peers until the next
/// launch. Spawned so the buy response doesn't wait on it. The
/// chequebook setup lock serialises it against a concurrent
/// `ant_deploy_chequebook` or chain init. The outcome goes to the
/// handle's gateway chequebook slot, so `/chequebook/*` reports one
/// deployed here without a gateway restart, and one the chain check
/// disqualified is dropped from it.
#[cfg(feature = "chain")]
fn after_buy_hook(
    handle: &AntHandle,
    client: ant_chain::ChainClient,
) -> ant_gateway::BatchBoughtHook {
    let rt = handle.runtime.handle().clone();
    let cmd_tx = handle.cmd_tx.clone();
    let data_dir = handle.data_dir.clone();
    let secret = handle.signing_secret;
    let eth = handle.eth;
    let slot = handle.gateway_chequebook.clone();
    Arc::new(move |_batch_id| {
        let (client, cmd_tx, data_dir) = (client.clone(), cmd_tx.clone(), data_dir.clone());
        let slot = slot.clone();
        rt.spawn(async move {
            let chequebook = crate::drive::ensure_settlement_best_effort(
                &cmd_tx, &client, secret, &data_dir, eth,
            )
            .await;
            crate::drive::sync_gateway_chequebook(&slot, &eth, chequebook);
        });
    })
}

/// `POST /v0/settlement/deposit` found the chain refusing the account's
/// chequebook (nothing was sent): the C API's top-up treatment
/// ([`crate::drive::chequebook_refused`]: the shared lag check, then
/// recorded as disqualified and settlement switched off), and a
/// refusal that stands drops the chequebook from the gateway's slot.
#[cfg(feature = "chain")]
fn chequebook_refused_hook(
    handle: &AntHandle,
    client: ant_chain::ChainClient,
) -> ant_gateway::ChequebookRefusedHook {
    let cmd_tx = handle.cmd_tx.clone();
    let data_dir = handle.data_dir.clone();
    let eth = handle.eth;
    let slot = handle.gateway_chequebook.clone();
    Arc::new(move |chequebook, refusal| {
        let (client, cmd_tx, data_dir, slot) = (
            client.clone(),
            cmd_tx.clone(),
            data_dir.clone(),
            slot.clone(),
        );
        Box::pin(async move {
            let stands = crate::drive::chequebook_refused(
                &cmd_tx,
                &client,
                &data_dir,
                eth,
                chequebook,
                refusal == ant_gateway::ChequebookRefusal::NotRegistered,
            )
            .await;
            if stands {
                crate::drive::sync_gateway_chequebook(&slot, &eth, None);
            }
            stands
        })
    })
}

/// Run [`crate::drive::ChainInit::run`] in the background against
/// `chain`. Overlapping runs are safe (see `ChainInit::run`), and one
/// with nothing left to do issues no reads, so calling this on every
/// `ant_start_gateway` is cheap. Its settlement outcome goes to the
/// handle's gateway chequebook slot — the live gateway's, also on the
/// idempotent "already running" retry.
#[cfg(feature = "chain")]
fn spawn_chain_init(handle: &AntHandle, chain: ant_chain::ChainClient) {
    let init = Arc::clone(&handle.chain_init);
    let cmd_tx = handle.cmd_tx.clone();
    let data_dir = handle.data_dir.clone();
    let secret = handle.signing_secret;
    let eth = handle.eth;
    let slot = handle.gateway_chequebook.clone();
    init.note_pending();
    handle.runtime.spawn(async move {
        init.run_reporting(&chain, &cmd_tx, &data_dir, secret, true, |adopted| {
            crate::drive::sync_gateway_chequebook(&slot, &eth, adopted);
        })
        .await;
    });
}

/// Set the CORS origins the in-process gateway allows. Takes effect at
/// the next [`ant_start_gateway`]; must be called while no gateway is
/// running on `handle` (before the first start, or after
/// [`ant_stop_gateway`]) and fails otherwise, so a running gateway never
/// serves a list other than the one the host last saw accepted.
///
/// `origins` points at `origins_len` NUL-terminated UTF-8 strings,
/// matched like bee's `cors-allowed-origins`: an exact origin such as
/// `https://app.example` (case-insensitive), `*` for any origin, or
/// `null` for opaque origins. A null `origins` or `origins_len == 0`
/// clears the list — the default — so the gateway sends no CORS headers
/// and no page from another origin can read its responses. Blank
/// entries are ignored.
///
/// Beyond bee, an entry may be a wildcard subdomain `scheme://*.host`
/// (e.g. `https://*.bzz.freedom.baby`): it allows every direct-or-deeper
/// subdomain of `host` on exactly that scheme with no port
/// (`https://abc.bzz.freedom.baby`, `https://a.b.bzz.freedom.baby`), but
/// not the apex `https://bzz.freedom.baby`, not a lookalike such as
/// `https://x.bzz.freedom.baby.evil.example`, not `http://`, and not
/// `https://x.bzz.freedom.baby:8443`. Matching is case-insensitive. The
/// `*` must be the whole leftmost label and `host` a plain DNS name of
/// at least two labels; any other entry containing `*` (`https://*.`,
/// `*.host` without scheme, `https://a.*.host`, a bare TLD such as
/// `https://*.com`, a wildcard with a port or path) is rejected.
/// This is for hosts that serve each content root from its own synthetic
/// origin (Freedom Android's virtual origins): the set of origins is
/// unbounded so it can't be listed exactly, and a wildcard keeps it to
/// the host's own namespace, where `*` would let any page in any other
/// browser on the device read the API.
///
/// Only allow origins whose pages you trust with the whole API: there
/// is no auth, so an allowed page can read `/wallet`, `/addresses`,
/// `/stamps` etc. and also send non-simple (preflighted) requests, e.g.
/// uploads with `Swarm-*` headers. `null` in particular matches *any*
/// page whose request was redirected across origins (the Fetch spec
/// taints the origin to `null`), and `*` matches every page.
///
/// This list protects *reads* only. An empty list does not block
/// writes: a CORS-simple request needs no preflight, so any page can
/// still `POST /stamps/{amount}/{depth}` or `POST /chequebook/deposit`
/// (`fetch(url, {method: 'POST', mode: 'no-cors'})`) and the gateway
/// executes it — spending the wallet's xBZZ (for `/stamps` both the
/// batch and a transfer into a new or under-funded chequebook's
/// deposit) and xDAI gas for every transaction it sends — the batch
/// purchase and, for `/stamps`, deploying a chequebook if there is none
/// yet and the deposit transfer into a new or under-funded one; nothing
/// is swapped — even though the page cannot read the reply. Don't rely
/// on this call to protect the wallet's funds.
///
/// The routes that *do* swap xDAI (`POST /v0/storage/buy`,
/// `POST /v0/storage/extend`, `POST /v0/settlement/deposit`) are
/// guarded separately: they refuse (`403`) requests from web pages
/// whatever this list says, except from an origin listed here exactly —
/// `*` and `null` never unlock them. Listing an exact origin therefore
/// also lets that site spend the wallet's xDAI.
///
/// Returns `true` on success; `false` with an allocated message in
/// `out_err` (free with [`crate::ant_free_string`]) on a null handle, a
/// running gateway, or a null/non-UTF-8/malformed-wildcard entry — the
/// stored list is then
/// left unchanged.
///
/// # Safety
///
/// `handle` must come from [`crate::ant_init`] and must not have been
/// passed to [`crate::ant_shutdown`]. If `origins_len > 0` and `origins`
/// is non-null, `origins` must point to `origins_len` readable
/// `*const c_char`s, each a NUL-terminated string (a null entry is
/// reported as an error). `out_err`, if non-null, must point to a
/// writable `*mut c_char` slot.
#[no_mangle]
pub unsafe extern "C" fn ant_set_gateway_cors(
    handle: *const AntHandle,
    origins: *const *const c_char,
    origins_len: usize,
    out_err: *mut *mut c_char,
) -> bool {
    unsafe {
        clear_out_err(out_err);
        let Some(handle) = handle.as_ref() else {
            write_out_err(out_err, "ant_set_gateway_cors: null handle");
            return false;
        };

        let mut list = Vec::new();
        if !origins.is_null() {
            for i in 0..origins_len {
                let ptr = *origins.add(i);
                if ptr.is_null() {
                    write_out_err(
                        out_err,
                        &format!("ant_set_gateway_cors: origin {i} is null"),
                    );
                    return false;
                }
                match CStr::from_ptr(ptr).to_str().map(str::trim) {
                    Ok("") => {}
                    Ok(o) => {
                        if let Err(e) = CorsConfig::check_entry(o) {
                            write_out_err(
                                out_err,
                                &format!("ant_set_gateway_cors: origin {i}: {e}"),
                            );
                            return false;
                        }
                        list.push(o.to_string());
                    }
                    Err(_) => {
                        write_out_err(
                            out_err,
                            &format!("ant_set_gateway_cors: origin {i} is not valid UTF-8"),
                        );
                        return false;
                    }
                }
            }
        }

        // Same guard `ant_start_gateway` holds across its whole start,
        // so this can't slip in between a start's read of the list and
        // its spawn.
        let task = handle
            .gateway_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if task.as_ref().is_some_and(|t| !t.is_finished()) {
            write_out_err(
                out_err,
                "ant_set_gateway_cors: gateway is running; call ant_stop_gateway first",
            );
            return false;
        }
        *handle
            .gateway_cors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = list;
        true
    }
}

/// Stop the in-process HTTP gateway started by [`ant_start_gateway`].
/// Returns `true` if a gateway was running and was aborted, `false` if
/// none was running (or `handle` is null). Safe to call repeatedly.
///
/// # Safety
///
/// `handle` must come from [`crate::ant_init`] and must not have been
/// passed to [`crate::ant_shutdown`].
#[no_mangle]
pub unsafe extern "C" fn ant_stop_gateway(handle: *const AntHandle) -> bool {
    unsafe {
        let Some(handle) = handle.as_ref() else {
            return false;
        };
        // Poison-tolerant so a panic elsewhere can't unwind out of this
        // `extern "C"` fn and abort the host (matches `ant_start_gateway`).
        let task = handle
            .gateway_task
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match task {
            Some(task) => {
                task.abort();
                true
            }
            None => false,
        }
    }
}
