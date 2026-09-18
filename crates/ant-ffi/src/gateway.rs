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
//! per handle; a second `ant_start_gateway` while one is running is a
//! no-op success.

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
/// The gateway's chain wiring is captured **here, once**. A host that
/// serves chain reads itself must therefore call
/// [`crate::ant_set_chain_transport`] *before* this; installing one
/// later only affects the per-call `ant_storage_*` / `ant_settlement_*`
/// paths until the gateway is stopped and started again. What is
/// captured is the handle's transport *slot*, though, so replacing or
/// clearing a transport that was installed before the start does reach
/// this gateway immediately — a cleared one falls back to `gnosis_rpc`
/// rather than calling a `host_ctx` the host has been told it may free.
///
/// Returns `true` on success (or if a gateway is already running),
/// `false` on error with an allocated message written to `out_err`
/// (free with [`crate::ant_free_string`]). Idempotent: a second call
/// while one is live is a no-op success.
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
            // degrades to `None` rather than failing gateway start.
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
            ant_gateway::chainreader::build_with_transport(
                gnosis_rpc,
                // No read-only fallback on mobile: chain reads stay gated
                // on the host-supplied `gnosis_rpc` (this branch only runs
                // when it's set), so behavior is unchanged.
                None,
                // No per-node postage-contract override on mobile: use
                // the Gnosis mainnet default (matches `antd`'s default).
                ant_chain::GNOSIS_POSTAGE_STAMP.to_string(),
                handle.eth,
                chequebook,
                ant_chain::tx::GNOSIS_CHAIN_ID,
                Some(handle.signing_secret),
                // Host-provided chain transport (issue #77), if the app
                // installed one with `ant_set_chain_transport` before
                // starting the gateway. `None` — the default — leaves
                // `/wallet`, `/stamps`, `/chainstate` and `/chequebook`
                // reading the `gnosis_rpc` URL exactly as before.
                handle.host_chain_transport(),
            )
        } else {
            None
        };
        #[cfg(not(feature = "chain"))]
        let _ = gnosis_rpc;

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
            // Freedom's dweb pages fetch/upload from an opaque (`null`)
            // origin, so the gateway must echo `null` in CORS — matches
            // bee started with `--cors-allowed-origins=null`.
            cors: Arc::new(CorsConfig::new(["null"])),
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
        };

        let task = handle.runtime.spawn(async move {
            if let Err(e) = Gateway::serve(gw, addr).await {
                tracing::error!(target: "ant-ffi", "in-process gateway ended: {e}");
            }
        });
        *slot = Some(task);
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
