//! JNI surface for the Android download smoke test
//! (PLAN.md § 9, "Android download smoke test (pre-FFI)"). Sibling to
//! the hand-written `extern "C"` API in `lib.rs`: the Rust state
//! machine is the same, only the bridge layer differs.
//!
//! Each `Java_at_vibing_ant_downloadsmoke_AntNode_native…` export
//! mirrors a function on `crates/ant-ffi/include/ant.h`. Errors
//! cross the boundary as `at.vibing.ant.downloadsmoke.AntException`
//! (mapped from `FfiError::to_string()`); successful calls return
//! pointers as `jlong`, byte payloads as `jbyteArray`, and short
//! strings as `jstring`.
//!
//! Only compiled when the `jni` Cargo feature is enabled — see the
//! `[features]` block in `Cargo.toml`. The exports themselves are
//! platform-independent C-ABI symbols; they only do anything useful
//! when loaded by a JVM, but `cargo check --features jni` on macOS
//! still compiles them so an IDE can type-check the file.
//!
//! ## API conventions (jni 0.22)
//!
//! Each native method takes `EnvUnowned<'caller>` as its first arg
//! and immediately upgrades it to a full `Env` via `with_env(...)`.
//! That closure also wraps the body in `catch_unwind` so a Rust
//! panic can never unwind across the JNI boundary. We resolve every
//! outcome with `ThrowRuntimeExAndDefault`, which is only reached
//! for panics or `Err(jni::errors::Error)` from JNI itself; our own
//! `FfiError` cases throw `AntException` manually inside the closure
//! and return a default value, so the policy never fires for them.

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JLongArray, JString};
use jni::strings::JNIString;
use jni::sys::{jint, jlong};
use jni::{Env, EnvUnowned};

use crate::{download_inner, init_inner, AntHandle};

/// Throw `at.vibing.ant.downloadsmoke.AntException` with the given
/// message. The throw is queued at the JVM level; the JVM picks it up
/// when control returns from the native method, regardless of the
/// value the Rust function ends up returning.
fn throw_ant_exception(env: &mut Env<'_>, message: &str) {
    let msg = JNIString::new(message);
    let _ = env.throw_new(jni_str!("at/vibing/ant/downloadsmoke/AntException"), &msg);
}

// ---------------------------------------------------------------------------
// nativeInit
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativeInit(dataDir: String): Long`
///
/// Mirror of [`crate::ant_init`]. Returns the raw `*mut AntHandle` cast
/// to `jlong` so the Kotlin side can stash it on the `AntNode`
/// instance. The pointer is valid until `nativeShutdown` is called.
///
/// Throws `AntException` on failure.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativeInit<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    data_dir: JString<'caller>,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<jlong> {
            let path: String = match data_dir.try_to_string(env) {
                Ok(s) => s,
                Err(e) => {
                    throw_ant_exception(env, &format!("read dataDir argument: {e}"));
                    return Ok(0);
                }
            };
            // No container-move rebase on Android (its app data dir is
            // stable across updates), so no source root. The Android host
            // doesn't hold the key yet either (the Keystore-backed
            // counterpart of the iOS Keychain path is future work), so
            // the identity still comes from the data dir.
            match init_inner(&PathBuf::from(path), None, crate::IdentitySource::DataDir) {
                Ok(handle) => Ok(Box::into_raw(Box::new(handle)) as jlong),
                Err(e) => {
                    throw_ant_exception(env, &e.to_string());
                    Ok(0)
                }
            }
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// nativeDownload
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativeDownload(handle: Long, reference: String): ByteArray`
///
/// Mirror of [`crate::ant_download`]. Blocks the calling thread (a
/// `Dispatchers.IO` worker on the Kotlin side) until the retrieval
/// completes or the embedded node returns an error. Throws
/// `AntException` on failure with the same message string the iOS
/// `out_err` slot would carry.
///
/// # Safety
///
/// `handle` must be a non-zero `jlong` returned by `nativeInit` and not
/// yet passed to `nativeShutdown`. The Kotlin layer is expected to
/// enforce this; calling with a stale handle is undefined behaviour.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativeDownload<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    reference: JString<'caller>,
) -> JByteArray<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JByteArray<'caller>> {
            let Some(handle) = handle_ref(env, handle) else {
                return Ok(JByteArray::null());
            };
            let reference: String = match reference.try_to_string(env) {
                Ok(s) => s,
                Err(e) => {
                    throw_ant_exception(env, &format!("read reference argument: {e}"));
                    return Ok(JByteArray::null());
                }
            };
            let data = match download_inner(handle, &reference) {
                Ok(data) => data,
                Err(e) => {
                    throw_ant_exception(env, &e.to_string());
                    return Ok(JByteArray::null());
                }
            };
            match env.byte_array_from_slice(&data) {
                Ok(arr) => Ok(arr),
                Err(e) => {
                    throw_ant_exception(
                        env,
                        &format!("allocate jbyteArray for {} bytes: {e}", data.len()),
                    );
                    Ok(JByteArray::null())
                }
            }
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// nativePeerCount
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativePeerCount(handle: Long): Int`
///
/// Mirror of [`crate::ant_peer_count`]. Reads the latest BZZ peer
/// count from the shared `watch::Receiver` snapshot — non-blocking,
/// safe to poll at any cadence. Returns `-1` on a null handle.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativePeerCount<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jint {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<jint> {
            let ptr = handle as *const AntHandle;
            // SAFETY: caller asserts the pointer was produced by
            // `Box::into_raw` in nativeInit and has not been freed by
            // nativeShutdown.
            let Some(handle) = (unsafe { ptr.as_ref() }) else {
                return Ok(-1);
            };
            let connected = handle.status_rx.borrow().peers.connected;
            Ok(jint::try_from(connected).unwrap_or(jint::MAX))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// nativeAgentString
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativeAgentString(handle: Long): String?`
///
/// Mirror of [`crate::ant_agent_string`]. Returns the live status
/// snapshot's `agent` field (e.g. `ant-ffi/0.4.0`). Returns `null` on
/// a null handle or if the JVM rejects the string allocation.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativeAgentString<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JString<'caller>> {
            let ptr = handle as *const AntHandle;
            // SAFETY: same handle-validity contract as nativePeerCount.
            let Some(handle) = (unsafe { ptr.as_ref() }) else {
                return Ok(JString::null());
            };
            let agent = handle.status_rx.borrow().agent.clone();
            Ok(env.new_string(agent).unwrap_or_else(|_| JString::null()))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// nativeShutdown
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativeShutdown(handle: Long): Unit`
///
/// Mirror of [`crate::ant_shutdown`]. Drops the box, which aborts the
/// embedded Tokio runtime and frees every spawned task. After this
/// returns, the Kotlin side must zero out its cached handle field so
/// no later JNI call dereferences a freed pointer.
///
/// Null / zero handle is a no-op.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativeShutdown<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    let _: () = unowned_env
        .with_env(|_env| -> jni::errors::Result<()> {
            if handle == 0 {
                return Ok(());
            }
            // SAFETY: caller asserts the pointer was produced by
            // `Box::into_raw` in nativeInit and is being freed exactly
            // once. The Kotlin side guards this with a single
            // `nativeHandle: Long` field that's zeroed after this call.
            let handle = unsafe { Box::from_raw(handle as *mut AntHandle) };
            handle.runtime.shutdown_background();
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

// ---------------------------------------------------------------------------
// nativeDownloadProgress
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativeDownloadProgress(handle: Long): LongArray?`
///
/// Mirror of [`crate::ant_download_progress`]. Returns a flat
/// 9-element `long[]` snapshot of the running download's progress
/// counters, or `null` on a null handle. Polled by Kotlin at ~4 Hz
/// to drive the progress bar / throughput readout — same cadence the
/// iOS smoke uses for `ant_download_progress`. The order matches the
/// `AntProgress` C struct in `crates/ant-ffi/include/ant.h`:
///
/// | idx | field            | type |
/// |-----|------------------|------|
/// | 0   | `in_progress`    | u8   |
/// | 1   | `bytes_done`     | u64  |
/// | 2   | `total_bytes`    | u64  |
/// | 3   | `chunks_done`    | u64  |
/// | 4   | `total_chunks`   | u64  |
/// | 5   | `elapsed_ms`     | u64  |
/// | 6   | `peers_used`     | u32  |
/// | 7   | `in_flight`      | u32  |
/// | 8   | `cache_hits`     | u64  |
///
/// We pack into a `long[]` rather than a Kotlin POJO because Kotlin's
/// auto-boxed `LongArray` is the cheapest variable-width primitive
/// container that crosses JNI without per-field method ID lookups.
/// The Kotlin layer reconstructs into a `DownloadProgress` data class.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativeDownloadProgress<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> JLongArray<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JLongArray<'caller>> {
            let ptr = handle as *const AntHandle;
            // SAFETY: same handle-validity contract as nativePeerCount.
            let Some(handle) = (unsafe { ptr.as_ref() }) else {
                return Ok(JLongArray::null());
            };
            let snapshot = match handle.progress.lock() {
                Ok(guard) => *guard,
                Err(poisoned) => *poisoned.into_inner(),
            };
            // u64 -> i64 saturating: progress counters never realistically
            // exceed i64::MAX (8 EiB / 9 quintillion chunks), but be
            // explicit about the conversion so a future "we made
            // bytes_done a multiplied counter" change can't quietly
            // wrap negative.
            let values: [jlong; 9] = [
                jlong::from(u8::from(snapshot.in_progress)),
                jlong::try_from(snapshot.bytes_done).unwrap_or(jlong::MAX),
                jlong::try_from(snapshot.total_bytes).unwrap_or(jlong::MAX),
                jlong::try_from(snapshot.chunks_done).unwrap_or(jlong::MAX),
                jlong::try_from(snapshot.total_chunks).unwrap_or(jlong::MAX),
                jlong::try_from(snapshot.elapsed_ms).unwrap_or(jlong::MAX),
                jlong::from(snapshot.peers_used),
                jlong::from(snapshot.in_flight),
                jlong::try_from(snapshot.cache_hits).unwrap_or(jlong::MAX),
            ];
            let arr = match JLongArray::new(env, values.len()) {
                Ok(arr) => arr,
                Err(e) => {
                    throw_ant_exception(env, &format!("JLongArray::new(9): {e}"));
                    return Ok(JLongArray::null());
                }
            };
            if let Err(e) = arr.set_region(env, 0, &values) {
                throw_ant_exception(env, &format!("JLongArray::set_region: {e}"));
                return Ok(JLongArray::null());
            }
            Ok(arr)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// nativeCancelDownload
// ---------------------------------------------------------------------------

/// `at.vibing.ant.downloadsmoke.AntNode.nativeCancelDownload(handle: Long): Int`
///
/// Mirror of [`crate::ant_cancel_download`]. Flips the handle's
/// cooperative cancel flag and wakes any in-flight ack waiter via the
/// notifier. The racing `nativeDownload` call returns by throwing
/// `AntException("download canceled")` on its next ack-loop iteration
/// (≤ ~150 ms under healthy retrieval).
///
/// Returns `0` on success, `-1` on a null handle. No-op when no
/// download is running — same contract as the iOS C surface.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_downloadsmoke_AntNode_nativeCancelDownload<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jint {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<jint> {
            let ptr = handle as *const AntHandle;
            // SAFETY: same handle-validity contract as nativePeerCount.
            let Some(handle) = (unsafe { ptr.as_ref() }) else {
                return Ok(-1);
            };
            handle.cancel_flag.store(true, Ordering::SeqCst);
            handle.cancel_notify.notify_waiters();
            Ok(0)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Cast a `jlong` carried across the JNI boundary back to a borrowed
/// `&AntHandle`, throwing `AntException` if the value is zero (the
/// Kotlin side's "handle not yet acquired" sentinel).
fn handle_ref<'a>(env: &mut Env<'_>, handle: jlong) -> Option<&'a AntHandle> {
    let ptr = handle as *const AntHandle;
    // SAFETY: caller asserts the pointer was produced by
    // `Box::into_raw` in nativeInit and has not been freed by
    // nativeShutdown.
    let r = unsafe { ptr.as_ref() };
    if r.is_none() {
        throw_ant_exception(env, "handle is null");
    }
    r
}
