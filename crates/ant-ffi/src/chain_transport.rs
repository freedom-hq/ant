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
/// The `Send`/`Sync` promise is the host's — documented in `ant.h` as
/// "the callback may be invoked from any thread, one request at a time
/// per call but possibly concurrently".
#[cfg(feature = "chain")]
struct HostCtx(*mut c_void);

#[cfg(feature = "chain")]
unsafe impl Send for HostCtx {}
#[cfg(feature = "chain")]
unsafe impl Sync for HostCtx {}

/// The installed callback, adapted to [`ant_chain::ChainTransport`].
#[cfg(feature = "chain")]
pub(crate) struct HostChainTransport {
    call: unsafe extern "C" fn(*const c_char, *mut c_void) -> *mut c_char,
    ctx: HostCtx,
}

#[cfg(feature = "chain")]
impl ant_chain::ChainTransport for HostChainTransport {
    fn serve(&self, request_json: &str) -> Option<String> {
        // An interior NUL can't cross a C string boundary. Ant never
        // builds one (the request is `serde_json`'s own output), but
        // degrade to can't-serve rather than panicking in a callback.
        let request = CString::new(request_json).ok()?;
        // SAFETY: `call` and `ctx` came from the host through
        // `ant_set_chain_transport`, which documents both as valid for
        // the lifetime of the handle (or until replaced). `request` is
        // a live NUL-terminated buffer for the duration of the call.
        let raw = unsafe { (self.call)(request.as_ptr(), self.ctx.0) };
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
/// (`ant_start_gateway`) captures its chain wiring once at start, so
/// install the transport *before* starting the gateway — or restart the
/// gateway (`ant_stop_gateway` then `ant_start_gateway`) to pick up a
/// later change.
///
/// # Safety
///
/// * `handle` must come from `ant_init` and must not have been passed
///   to `ant_shutdown`.
/// * `transport`, if non-null, must stay callable — and `host_ctx`
///   valid — until it is replaced/cleared or the handle is shut down.
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
        // Poison-tolerant so a panic elsewhere can't unwind out of this
        // `extern "C"` fn and abort the host (matches the other FFI
        // entry points that take handle locks).
        let mut slot = handle
            .chain_transport
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = transport.map(|call| {
            std::sync::Arc::new(HostChainTransport {
                call,
                ctx: HostCtx(host_ctx),
            })
        });
        tracing::info!(
            target: "ant-ffi",
            installed = slot.is_some(),
            "host chain transport updated",
        );
        ANT_CHAIN_TRANSPORT_OK
    }
}

#[cfg(all(test, feature = "chain"))]
mod tests {
    use super::*;
    use ant_chain::ChainTransport;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // One counter per callback: the test binary runs tests in parallel,
    // so a shared counter would race between them.
    static ADAPTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static NULL_CALLS: AtomicUsize = AtomicUsize::new(0);
    static FFI_CALLS: AtomicUsize = AtomicUsize::new(0);

    /// Stand-in for the host: `malloc`s its reply, exactly as `ant.h`
    /// requires, so the `free(3)` on ant's side is the real contract.
    unsafe extern "C" fn serving_host(request: *const c_char, ctx: *mut c_void) -> *mut c_char {
        ADAPTER_CALLS.fetch_add(1, Ordering::SeqCst);
        assert_eq!(ctx as usize, 0xBEEF);
        let req = unsafe { CStr::from_ptr(request) }.to_str().unwrap();
        assert!(req.contains("\"method\":\"eth_blockNumber\""), "{req}");
        malloc_cstr(r#"{"jsonrpc":"2.0","id":1,"result":"0x7b"}"#)
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
        unsafe { serving_host(request, ctx) }
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
        let t = HostChainTransport {
            call: serving_host,
            ctx: HostCtx(0xBEEF as *mut c_void),
        };
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
        let t = HostChainTransport {
            call: cant_serve_host,
            ctx: HostCtx(std::ptr::null_mut()),
        };
        assert_eq!(t.serve(r#"{"method":"eth_blockNumber"}"#), None);
        assert_eq!(NULL_CALLS.load(Ordering::SeqCst), 1);
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
            chain_transport: std::sync::Mutex::new(None),
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
