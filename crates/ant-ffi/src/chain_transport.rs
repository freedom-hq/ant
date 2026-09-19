//! Host-provided chain transport for embedded hosts (issue #77).
//!
//! An iOS/Android host that already runs a *verified* chain source —
//! Freedom's embedded Myotis light client, a Colibri stateless verifier,
//! an RPC quorum — can serve ant's Gnosis JSON-RPC requests from it
//! instead of ant trusting one pinned URL. [`ant_set_chain_transport`]
//! installs a C callback; everything else about ant's chain behaviour is
//! unchanged, including the set of requests it issues.
//!
//! ## Why a blocking callback rather than an async correlation-id queue
//!
//! The issue allowed either. A blocking callback wins here because
//! `ChainClient`'s seam already runs the transport on the runtime's
//! blocking pool (`spawn_blocking`), so a host that blocks — a Merkle
//! proof verification, a lock, a nested runtime `block_on` — starves
//! nothing, and there is no per-request state to leak if the host never
//! answers a correlation id. It also keeps the C surface to two symbols
//! and no ant-owned queue, which is what the reporters sketched.

use crate::AntHandle;
use std::ffi::{c_char, c_int, c_void};
#[cfg(feature = "chain")]
use std::ffi::{CStr, CString};

#[cfg(feature = "chain")]
extern "C" {
    /// `free(3)`. The host's response body is `malloc`'d on its side and
    /// released here — see [`ant_set_chain_transport`].
    fn free(ptr: *mut c_void);
}

/// Host-provided JSON-RPC transport callback.
///
/// Receives a complete JSON-RPC request body and returns a `malloc`'d
/// JSON-RPC response body, or `NULL` for "can't serve".
pub type AntChainTransportFn =
    Option<unsafe extern "C" fn(request_json: *const c_char, host_ctx: *mut c_void) -> *mut c_char>;

/// The transport was installed (or cleared).
pub const ANT_CHAIN_TRANSPORT_OK: c_int = 0;
/// `handle` was `NULL`.
pub const ANT_CHAIN_TRANSPORT_NULL_HANDLE: c_int = -1;
/// This build has no chain support (`ant-ffi`'s `chain` feature is off),
/// so there are no chain requests to route. Nothing was installed.
pub const ANT_CHAIN_TRANSPORT_UNSUPPORTED: c_int = -2;

/// The host's opaque context pointer.
///
/// Opaque to ant: never dereferenced, only handed back to the callback.
/// The `Send`/`Sync` promise is the host's: `ant.h`'s threading section
/// requires `host_ctx` to be safe to use from any thread, because the
/// callback runs on ant's blocking pool and may be invoked concurrently
/// from several of those threads.
#[cfg(feature = "chain")]
struct HostCtx(*mut c_void);

#[cfg(feature = "chain")]
unsafe impl Send for HostCtx {}
#[cfg(feature = "chain")]
unsafe impl Sync for HostCtx {}

/// One installed `(callback, host_ctx)` pair.
#[cfg(feature = "chain")]
struct Installed {
    call: unsafe extern "C" fn(*const c_char, *mut c_void) -> *mut c_char,
    ctx: HostCtx,
}

/// The transport *slot*, adapted to [`ant_chain::ChainTransport`].
///
/// One slot lives on the handle for its whole life and every chain
/// client — including the one the in-process gateway captures once at
/// `ant_start_gateway` — shares that single `Arc`. The installed
/// callback sits behind an `RwLock` *inside* it, so
/// [`ant_set_chain_transport`] is what makes a captured client stop
/// calling the old `host_ctx`, not the client being rebuilt. Two
/// properties the C contract depends on follow from that:
///
/// * a clear/replace is effective **everywhere at once** — an
///   already-built client (gateway, in-flight `ant_storage_*`) sees the
///   new slot contents, and a cleared slot is can't-serve, which falls
///   back to the configured URL exactly as if none were installed; and
/// * a clear/replace **drains** — [`Self::serve`] holds the read lock
///   across the host call, so the write lock waits for every in-flight
///   callback to return. Once `ant_set_chain_transport` returns, no
///   thread is inside the old callback and none can enter it again, so
///   the host may free `host_ctx`.
#[cfg(feature = "chain")]
pub(crate) struct HostChainTransport {
    installed: std::sync::RwLock<Option<Installed>>,
}

#[cfg(feature = "chain")]
impl HostChainTransport {
    /// An empty slot: every chain request goes to the configured URL.
    pub(crate) fn new() -> Self {
        Self {
            installed: std::sync::RwLock::new(None),
        }
    }

    /// Install, replace, or (with `transport == None`) clear the
    /// callback. Returns whether one is installed afterwards.
    ///
    /// Blocks until every in-flight callback has returned — that is the
    /// drain the `host_ctx` lifetime contract rests on.
    pub(crate) fn set(&self, transport: AntChainTransportFn, host_ctx: *mut c_void) -> bool {
        // Poison-tolerant so a panic elsewhere can't wedge chain reads
        // (or unwind out of the `extern "C"` caller and abort the host).
        let mut slot = self
            .installed
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = transport.map(|call| Installed {
            call,
            ctx: HostCtx(host_ctx),
        });
        slot.is_some()
    }

    /// Whether a host transport is installed right now.
    pub(crate) fn is_installed(&self) -> bool {
        self.installed
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }
}

#[cfg(feature = "chain")]
impl ant_chain::ChainTransport for HostChainTransport {
    fn serve(&self, request_json: &str) -> Option<String> {
        // An interior NUL can't cross a C string boundary. Ant never
        // builds one (the request is `serde_json`'s own output), but
        // degrade to can't-serve rather than panicking in a callback.
        let request = CString::new(request_json).ok()?;
        // Held across the whole host call: `set` (install / replace /
        // clear) takes the write lock, so it cannot return while this
        // callback is running with the `host_ctx` the host is about to
        // free.
        let slot = self
            .installed
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Cleared (or never installed) -> can't serve, and ant falls
        // back to the configured URL.
        let installed = slot.as_ref()?;
        // SAFETY: `call` and `ctx` came from the host through
        // `ant_set_chain_transport`, which documents both as valid until
        // the transport is replaced/cleared — and a replace/clear blocks
        // on the write lock this read guard holds off. `request` is a
        // live NUL-terminated buffer for the duration of the call.
        let raw = unsafe { (installed.call)(request.as_ptr(), installed.ctx.0) };
        if raw.is_null() {
            return None; // can't serve -> ant falls back to the URL
        }
        // SAFETY: non-null return, documented as a NUL-terminated,
        // `malloc`'d buffer owned by ant from here on.
        let body = unsafe { CStr::from_ptr(raw) }
            .to_str()
            .ok()
            .map(ToString::to_string);
        // SAFETY: as above — released exactly once, before returning.
        unsafe { free(raw.cast::<c_void>()) };
        // Non-UTF-8 is not a JSON-RPC response; treat it as can't-serve
        // so a buggy host degrades to today's behaviour.
        body
    }
}

/// Install a host-provided JSON-RPC transport for every chain request
/// this handle makes — `eth_call`, `eth_getBalance`, `eth_getLogs`,
/// `eth_getTransactionReceipt`, `eth_sendRawTransaction`,
/// `eth_getTransactionCount`, `eth_blockNumber`, `eth_getCode`.
///
/// `transport` is called with a complete JSON-RPC request body and must
/// return either:
///
/// * a `malloc`'d, NUL-terminated JSON-RPC response body — ant takes
///   ownership and releases it with `free(3)`; or
/// * `NULL` to signal **can't serve**, in which case ant falls back to
///   the configured `gnosis_rpc` URL exactly as it would without a
///   transport installed.
///
/// A JSON-RPC `error` with code **-32000** (the retryable
/// "index doesn't cover this range yet" shape, carrying the backend's
/// covered window in `data`) also counts as can't-serve and falls back.
/// It is never surfaced to callers as an empty result — that would
/// silently truncate postage-batch discovery. Any other `error` is
/// treated as a genuine answer and returned to the caller.
///
/// **Emit -32000 only for a coverage gap.** geth and Nethermind also use
/// it as a catch-all for genuine failures (`nonce too low`, `already
/// known`, `insufficient funds …`, `replacement transaction
/// underpriced`, `execution reverted`), so a host forwarding backend
/// replies verbatim turns those into can't-serve and ant replays them
/// against `gnosis_rpc` — for `eth_sendRawTransaction` that is a second
/// broadcast of an already-signed transaction, and the caller sees the
/// fallback URL's error instead of the real one. Map any non-coverage
/// failure to a different code, or answer authoritatively.
///
/// Pass `NULL` for `transport` to clear a previously-installed one.
/// `host_ctx` is opaque and simply handed back on every call.
///
/// Returns [`ANT_CHAIN_TRANSPORT_OK`] (0) on success,
/// [`ANT_CHAIN_TRANSPORT_NULL_HANDLE`] (-1) for a null handle, or
/// [`ANT_CHAIN_TRANSPORT_UNSUPPORTED`] (-2) when this build has no
/// chain support at all (the `chain` feature is off, so no chain
/// request exists to route).
///
/// **Threading:** the callback runs on ant's runtime blocking pool, so
/// blocking inside it is fine and expected. It may be invoked
/// concurrently from several such threads.
///
/// **Ordering:** takes effect immediately for the storage / settlement
/// calls (`ant_storage_*`, `ant_settlement_*`, `ant_deploy_chequebook`),
/// which build a chain client per call. The in-process gateway
/// (`ant_start_gateway`) captures its chain wiring once at start, so a
/// transport installed while none was installed at that start is only
/// guaranteed to reach it after a restart (`ant_stop_gateway` then
/// `ant_start_gateway`) — install it before starting the gateway.
/// Replacing or **clearing** a transport the gateway did start with, by
/// contrast, is effective everywhere at once, including in that running
/// gateway; see the lifetime rule below.
///
/// **Lifetime:** this call does not return until every in-flight
/// invocation of the *previous* callback has returned, and no new one
/// can start with the old `host_ctx` (a cleared slot falls back to the
/// configured `gnosis_rpc` URL). So the host may free `host_ctx` as
/// soon as this returns — including while a gateway is running. The
/// flip side: a callback that never returns wedges this call, and
/// calling `ant_set_chain_transport` *from inside* the callback
/// deadlocks. `ant_shutdown` performs the same drain before tearing the
/// runtime down.
///
/// # Safety
///
/// * `handle` must come from `ant_init` and must not have been passed
///   to `ant_shutdown`.
/// * `transport`, if non-null, must stay callable — and `host_ctx`
///   valid — until this function is called again to replace/clear it
///   (or `ant_shutdown` returns); both calls drain first, so no
///   callback is running once they return.
#[no_mangle]
pub unsafe extern "C" fn ant_set_chain_transport(
    handle: *mut AntHandle,
    transport: AntChainTransportFn,
    host_ctx: *mut c_void,
) -> c_int {
    // SAFETY: caller contract above.
    let Some(handle) = (unsafe { handle.as_ref() }) else {
        return ANT_CHAIN_TRANSPORT_NULL_HANDLE;
    };

    #[cfg(not(feature = "chain"))]
    {
        let _ = (handle, transport, host_ctx);
        ANT_CHAIN_TRANSPORT_UNSUPPORTED
    }

    #[cfg(feature = "chain")]
    {
        // Swaps the callback inside the handle's one shared slot, so
        // clients built earlier (the gateway's, an in-flight
        // `ant_storage_*`) see the change too — and blocks until any
        // in-flight callback has returned, which is what lets the host
        // free `host_ctx` once this returns.
        let installed = handle.chain_transport.set(transport, host_ctx);
        tracing::info!(
            target: "ant-ffi",
            installed,
            "host chain transport updated",
        );
        ANT_CHAIN_TRANSPORT_OK
    }
}

#[cfg(all(test, feature = "chain"))]
mod tests {
    use super::*;
    use ant_chain::ChainTransport;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // One counter per callback: the test binary runs tests in parallel,
    // so a shared counter would race between them.
    static ADAPTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static NULL_CALLS: AtomicUsize = AtomicUsize::new(0);
    static FFI_CALLS: AtomicUsize = AtomicUsize::new(0);
    static CAPTURED_CALLS: AtomicUsize = AtomicUsize::new(0);
    static SLOW_ENTERED: AtomicBool = AtomicBool::new(false);
    static SLOW_RETURNED: AtomicBool = AtomicBool::new(false);
    static SHUTDOWN_ENTERED: AtomicBool = AtomicBool::new(false);
    static SHUTDOWN_RETURNED: AtomicBool = AtomicBool::new(false);

    /// The host's side of one `eth_blockNumber`: checks the request and
    /// `malloc`s its reply, exactly as `ant.h` requires, so the `free(3)`
    /// on ant's side is the real contract. Counter-free so each callback
    /// below can keep its own count (tests run in parallel).
    unsafe fn block_number_reply(request: *const c_char, ctx: *mut c_void) -> *mut c_char {
        assert_eq!(ctx as usize, 0xBEEF);
        let req = unsafe { CStr::from_ptr(request) }.to_str().unwrap();
        assert!(req.contains("\"method\":\"eth_blockNumber\""), "{req}");
        malloc_cstr(r#"{"jsonrpc":"2.0","id":1,"result":"0x7b"}"#)
    }

    /// Stand-in for the host, on the adapter test's counter.
    unsafe extern "C" fn serving_host(request: *const c_char, ctx: *mut c_void) -> *mut c_char {
        ADAPTER_CALLS.fetch_add(1, Ordering::SeqCst);
        unsafe { block_number_reply(request, ctx) }
    }

    unsafe extern "C" fn cant_serve_host(
        _request: *const c_char,
        _ctx: *mut c_void,
    ) -> *mut c_char {
        NULL_CALLS.fetch_add(1, Ordering::SeqCst);
        std::ptr::null_mut()
    }

    /// Same contract as `serving_host`, on its own counter so the
    /// end-to-end test can assert exact call counts.
    unsafe extern "C" fn serving_host_ffi(request: *const c_char, ctx: *mut c_void) -> *mut c_char {
        FFI_CALLS.fetch_add(1, Ordering::SeqCst);
        unsafe { block_number_reply(request, ctx) }
    }

    /// Stands in for a gateway's captured client: same contract as
    /// `serving_host`, on its own counter.
    unsafe extern "C" fn serving_host_captured(
        request: *const c_char,
        ctx: *mut c_void,
    ) -> *mut c_char {
        CAPTURED_CALLS.fetch_add(1, Ordering::SeqCst);
        unsafe { block_number_reply(request, ctx) }
    }

    /// A host that is slow to answer — long enough for the test to call
    /// `ant_set_chain_transport(NULL)` while this call is in flight.
    unsafe extern "C" fn slow_host(request: *const c_char, ctx: *mut c_void) -> *mut c_char {
        SLOW_ENTERED.store(true, Ordering::SeqCst);
        std::thread::sleep(std::time::Duration::from_millis(300));
        let reply = unsafe { block_number_reply(request, ctx) };
        SLOW_RETURNED.store(true, Ordering::SeqCst);
        reply
    }

    /// `slow_host`'s twin, on its own flags, for the shutdown drain.
    unsafe extern "C" fn slow_host_shutdown(
        request: *const c_char,
        ctx: *mut c_void,
    ) -> *mut c_char {
        SHUTDOWN_ENTERED.store(true, Ordering::SeqCst);
        // Deliberately longer than `SHUTDOWN_GRACE`: past that the
        // runtime stops waiting and leaks the blocking thread, so only a
        // drain in `ant_shutdown` itself keeps the callback from
        // outliving the call.
        std::thread::sleep(crate::SHUTDOWN_GRACE + std::time::Duration::from_millis(1_500));
        let reply = unsafe { block_number_reply(request, ctx) };
        SHUTDOWN_RETURNED.store(true, Ordering::SeqCst);
        reply
    }

    extern "C" {
        fn malloc(size: usize) -> *mut c_void;
    }

    fn malloc_cstr(s: &str) -> *mut c_char {
        let bytes = s.as_bytes();
        // SAFETY: allocate len+1 and write the bytes plus the NUL.
        unsafe {
            let p = malloc(bytes.len() + 1).cast::<u8>();
            assert!(!p.is_null());
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
            *p.add(bytes.len()) = 0;
            p.cast::<c_char>()
        }
    }

    /// The adapter hands the request over, takes ownership of the
    /// `malloc`'d reply (freeing it), and returns the body.
    #[test]
    fn adapter_round_trips_a_malloced_reply() {
        let t = HostChainTransport::new();
        t.set(Some(serving_host), 0xBEEF as *mut c_void);
        let out = t.serve(r#"{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}"#);
        assert_eq!(
            out.as_deref(),
            Some(r#"{"jsonrpc":"2.0","id":1,"result":"0x7b"}"#)
        );
        assert_eq!(ADAPTER_CALLS.load(Ordering::SeqCst), 1);
    }

    /// A `NULL` return is can't-serve, which `ant-chain` turns into a
    /// fall-through to the configured URL.
    #[test]
    fn null_reply_is_cant_serve() {
        let t = HostChainTransport::new();
        t.set(Some(cant_serve_host), std::ptr::null_mut());
        assert_eq!(t.serve(r#"{"method":"eth_blockNumber"}"#), None);
        assert_eq!(NULL_CALLS.load(Ordering::SeqCst), 1);
    }

    /// An empty slot is can't-serve too — that is what makes a cleared
    /// transport fall back to the configured URL for everyone holding
    /// the slot, rather than keep calling a freed `host_ctx`.
    #[test]
    fn empty_slot_is_cant_serve() {
        let t = HostChainTransport::new();
        assert_eq!(t.serve(r#"{"method":"eth_blockNumber"}"#), None);
    }

    /// The channel peers a test handle's node loop would own. Returned
    /// alongside the handle so the caller keeps them alive for the
    /// duration of the test.
    type NodePeers = (
        tokio::sync::mpsc::Receiver<ant_control::ControlCommand>,
        tokio::sync::watch::Sender<ant_control::StatusSnapshot>,
    );

    /// A handle with an inert node behind it — enough to exercise the
    /// chain wiring, which only touches the runtime and the transport
    /// slot.
    fn handle_for_test() -> (AntHandle, NodePeers) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(1);
        let (status_tx, status_rx) =
            tokio::sync::watch::channel(ant_control::StatusSnapshot::default());
        let handle = AntHandle {
            runtime,
            cmd_tx,
            status_rx,
            progress: std::sync::Mutex::new(crate::DownloadProgressState::default()),
            cancel_flag: std::sync::atomic::AtomicBool::new(false),
            cancel_notify: tokio::sync::Notify::new(),
            verify_cancel: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            signing_secret: [0u8; 32],
            eth: [0u8; 20],
            data_dir: std::path::PathBuf::from("/nonexistent"),
            gateway_task: std::sync::Mutex::new(None),
            bench: std::sync::Mutex::new(None),
            chain_transport: std::sync::Arc::new(HostChainTransport::new()),
        };
        (handle, (cmd_rx, status_tx))
    }

    /// End-to-end through the real FFI entry point: once a host
    /// transport is installed, a `ChainClient` built the way every
    /// storage / settlement call builds one is answered by the host —
    /// with an unroutable URL underneath, so a fall-through would fail
    /// the call rather than quietly succeed.
    #[test]
    fn ffi_install_routes_a_real_chain_client() {
        let (mut handle, _peers) = handle_for_test();

        let rc = unsafe {
            ant_set_chain_transport(
                std::ptr::from_mut(&mut handle),
                Some(serving_host_ffi),
                0xBEEF as *mut c_void,
            )
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);

        let client = handle.chain_client("http://127.0.0.1:1");
        let block = handle
            .runtime
            .block_on(client.eth_block_number())
            .expect("the host serves this, so the dead URL is never used");
        assert_eq!(block, 0x7b);
        assert_eq!(FFI_CALLS.load(Ordering::SeqCst), 1);

        // Clearing puts the handle back on the URL path: the same call
        // now fails against the unroutable endpoint.
        let rc = unsafe {
            ant_set_chain_transport(std::ptr::from_mut(&mut handle), None, std::ptr::null_mut())
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);
        let client = handle.chain_client("http://127.0.0.1:1");
        assert!(handle.runtime.block_on(client.eth_block_number()).is_err());
        assert_eq!(
            FFI_CALLS.load(Ordering::SeqCst),
            1,
            "a cleared transport is not called again",
        );
    }

    /// The gateway captures its chain wiring once, at
    /// `ant_start_gateway`. Clearing the transport must stop *that*
    /// captured client from calling the host too — the host is allowed
    /// to free `host_ctx` as soon as the clear returns, so one more
    /// call through a stale capture is a use-after-free.
    #[test]
    fn clearing_stops_a_client_captured_before_the_clear() {
        let (mut handle, _peers) = handle_for_test();

        let rc = unsafe {
            ant_set_chain_transport(
                std::ptr::from_mut(&mut handle),
                Some(serving_host_captured),
                0xBEEF as *mut c_void,
            )
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);

        // What `ant_start_gateway` does: build the chain wiring once and
        // keep it for the life of the gateway.
        let captured = handle.chain_client("http://127.0.0.1:1");
        assert_eq!(
            handle.runtime.block_on(captured.eth_block_number()).ok(),
            Some(0x7b),
        );
        assert_eq!(CAPTURED_CALLS.load(Ordering::SeqCst), 1);

        // Host clears the transport and frees its `host_ctx`.
        let rc = unsafe {
            ant_set_chain_transport(std::ptr::from_mut(&mut handle), None, std::ptr::null_mut())
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);

        assert!(
            handle
                .runtime
                .block_on(captured.eth_block_number())
                .is_err(),
            "a cleared transport must fall back to the (dead) URL",
        );
        assert_eq!(
            CAPTURED_CALLS.load(Ordering::SeqCst),
            1,
            "a client captured before the clear must not call the freed host_ctx",
        );
    }

    /// Clearing must not return while a callback is still running with
    /// the old `host_ctx`: the host frees that pointer the moment
    /// `ant_set_chain_transport` returns.
    #[test]
    fn clearing_waits_for_an_in_flight_callback() {
        let (mut handle, _peers) = handle_for_test();

        let rc = unsafe {
            ant_set_chain_transport(
                std::ptr::from_mut(&mut handle),
                Some(slow_host),
                0xBEEF as *mut c_void,
            )
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);

        let client = handle.chain_client("http://127.0.0.1:1");
        let inflight = handle
            .runtime
            .spawn(async move { client.eth_block_number().await.is_ok() });

        // Wait until the host callback is actually inside its body.
        while !SLOW_ENTERED.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let rc = unsafe {
            ant_set_chain_transport(std::ptr::from_mut(&mut handle), None, std::ptr::null_mut())
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);
        assert!(
            SLOW_RETURNED.load(Ordering::SeqCst),
            "the clear returned while the host callback was still running with the old host_ctx",
        );

        assert!(handle.runtime.block_on(inflight).unwrap());
    }

    /// `ant_shutdown` is the other point where `ant.h` lets the host
    /// free `host_ctx`, and the runtime's shutdown grace *leaks* a
    /// blocking thread that outruns it — so the drain has to happen in
    /// `ant_shutdown` itself.
    #[test]
    fn shutdown_waits_for_an_in_flight_callback() {
        let (handle, _peers) = handle_for_test();
        let handle = Box::into_raw(Box::new(handle));

        let rc = unsafe {
            ant_set_chain_transport(handle, Some(slow_host_shutdown), 0xBEEF as *mut c_void)
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_OK);

        // SAFETY: the handle is live until `ant_shutdown` below.
        let client = unsafe { (*handle).chain_client("http://127.0.0.1:1") };
        unsafe {
            (*handle)
                .runtime
                .spawn(async move { client.eth_block_number().await })
        };

        while !SHUTDOWN_ENTERED.load(Ordering::SeqCst) {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        unsafe { crate::ant_shutdown(handle) };
        assert!(
            SHUTDOWN_RETURNED.load(Ordering::SeqCst),
            "ant_shutdown returned while the host callback was still running",
        );
    }

    #[test]
    fn null_handle_is_rejected() {
        let rc = unsafe {
            ant_set_chain_transport(
                std::ptr::null_mut(),
                Some(serving_host),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(rc, ANT_CHAIN_TRANSPORT_NULL_HANDLE);
    }
}
