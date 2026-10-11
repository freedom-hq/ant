//! JNI surface for the `AntStream` Android app (`examples/android-stream`),
//! the Android sibling of `examples/ios-stream`.
//!
//! Unlike [`crate::jni`], which re-implements the download smoke test's
//! five calls natively, every export here is a thin shim over the
//! `extern "C"` function of the same name in `include/ant.h`: the Swift
//! app calls those directly, so routing the Kotlin app through the very
//! same entry points keeps the two hosts on one code path (node
//! lifecycle, storage plan, settlement, publisher) instead of two that
//! can drift.
//!
//! Exports are `Java_at_vibing_ant_stream_AntNative_native…`, matching
//! the `@JvmStatic external fun` declarations on the Kotlin
//! `at.vibing.ant.stream.AntNative` object. Errors cross the boundary as
//! `at.vibing.ant.stream.AntException` carrying the `out_err` message the
//! C function produced; handles cross as `jlong`.
//!
//! Every call blocks the calling thread for as long as the C function
//! does (chain calls take seconds, `nativePublisherStop` up to ~130 s),
//! so the Kotlin side runs them on `Dispatchers.IO`.

use std::ffi::{c_char, CStr, CString};
use std::ptr;

use jni::errors::ThrowRuntimeExAndDefault;
use jni::jni_str;
use jni::objects::{JByteArray, JClass, JString};
use jni::strings::JNIString;
use jni::sys::{jboolean, jint, jlong};
use jni::{Env, EnvUnowned};

use crate::AntHandle;

fn throw(env: &mut Env<'_>, message: &str) {
    let msg = JNIString::new(message);
    let _ = env.throw_new(jni_str!("at/vibing/ant/stream/AntException"), &msg);
}

/// Read a Java string argument into a `CString`, throwing on failure.
fn c_arg(env: &mut Env<'_>, value: &JString<'_>, name: &str) -> Option<CString> {
    let text = match value.try_to_string(env) {
        Ok(s) => s,
        Err(e) => {
            throw(env, &format!("read {name} argument: {e}"));
            return None;
        }
    };
    if let Ok(c) = CString::new(text) {
        Some(c)
    } else {
        throw(env, &format!("{name} contains a NUL byte"));
        None
    }
}

fn handle_ptr(env: &mut Env<'_>, handle: jlong) -> Option<*const AntHandle> {
    if handle == 0 {
        throw(env, "node is not running");
        return None;
    }
    Some(handle as *const AntHandle)
}

/// Take ownership of an `out_err` message, falling back to `fallback`.
///
/// # Safety
///
/// `err` is null or a string allocated by this library.
unsafe fn take_err(err: *mut c_char, fallback: &str) -> String {
    if err.is_null() {
        return fallback.to_string();
    }
    // SAFETY: per the contract above.
    unsafe {
        let message = CStr::from_ptr(err).to_string_lossy().into_owned();
        crate::ant_free_string(err);
        message
    }
}

/// Run a `char *f(…, char **out_err)` call and hand its result to Java
/// as a string, or throw its error.
fn string_call<'caller>(
    env: &mut Env<'caller>,
    name: &str,
    call: impl FnOnce(*mut *mut c_char) -> *mut c_char,
) -> JString<'caller> {
    let mut err: *mut c_char = ptr::null_mut();
    let out = call(&raw mut err);
    if out.is_null() {
        // SAFETY: `err` was written by the library or left null.
        let message = unsafe { take_err(err, &format!("{name} failed")) };
        throw(env, &message);
        return JString::null();
    }
    // SAFETY: a non-null result is a NUL-terminated string the library
    // allocated; it is freed right after the copy.
    let text = unsafe { CStr::from_ptr(out).to_string_lossy().into_owned() };
    unsafe {
        crate::ant_free_string(out);
        if !err.is_null() {
            crate::ant_free_string(err);
        }
    }
    env.new_string(text).unwrap_or_else(|_| JString::null())
}

/// Run an `int f(…, char **out_err)` call; a negative result throws.
fn int_call(env: &mut Env<'_>, name: &str, call: impl FnOnce(*mut *mut c_char) -> i32) -> jint {
    let mut err: *mut c_char = ptr::null_mut();
    let code = call(&raw mut err);
    // SAFETY: `err` was written by the library or left null.
    let message = unsafe { take_err(err, &format!("{name} failed")) };
    if code < 0 {
        throw(env, &message);
    }
    code
}

/// Run a `bool f(…, char **out_err)` call; `false` throws.
fn bool_call(env: &mut Env<'_>, name: &str, call: impl FnOnce(*mut *mut c_char) -> bool) {
    let mut err: *mut c_char = ptr::null_mut();
    let ok = call(&raw mut err);
    // SAFETY: `err` was written by the library or left null.
    let message = unsafe { take_err(err, &format!("{name} failed")) };
    if !ok {
        throw(env, &message);
    }
}

// ---------------------------------------------------------------------------
// Identity + lifecycle
// ---------------------------------------------------------------------------

/// `AntNative.nativeIdentityGenerate(): String` — `ant_identity_generate`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeIdentityGenerate<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JString<'caller>> {
            // SAFETY: plain FFI call with a valid out-pointer.
            Ok(string_call(env, "generate identity", |err| unsafe {
                crate::ant_identity_generate(err)
            }))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// `AntNative.nativeIdentityFromKey(key: String): String` —
/// `ant_identity_from_key`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeIdentityFromKey<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    key: JString<'caller>,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JString<'caller>> {
            let Some(key) = c_arg(env, &key, "key") else {
                return Ok(JString::null());
            };
            // SAFETY: `key` outlives the call.
            Ok(string_call(env, "restore identity", |err| unsafe {
                crate::ant_identity_from_key(key.as_ptr(), err)
            }))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// `AntNative.nativeInit(dataDir: String, identityJson: String): Long` —
/// `ant_init_with_identity`. The host owns the key (Android Keystore);
/// the library never writes `identity.json`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeInit<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    data_dir: JString<'caller>,
    identity_json: JString<'caller>,
) -> jlong {
    unowned_env
        .with_env(|env| -> jni::errors::Result<jlong> {
            let Some(dir) = c_arg(env, &data_dir, "dataDir") else {
                return Ok(0);
            };
            let Some(identity) = c_arg(env, &identity_json, "identityJson") else {
                return Ok(0);
            };
            let mut err: *mut c_char = ptr::null_mut();
            // SAFETY: both strings outlive the call; a null source root
            // is allowed.
            let handle = unsafe {
                crate::ant_init_with_identity(
                    dir.as_ptr(),
                    ptr::null(),
                    identity.as_ptr(),
                    &raw mut err,
                )
            };
            // SAFETY: `err` was written by the library or left null.
            let message = unsafe { take_err(err, "could not start the node") };
            if handle.is_null() {
                throw(env, &message);
                return Ok(0);
            }
            Ok(handle as jlong)
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// `AntNative.nativeShutdown(handle: Long)` — `ant_stop_gateway` then
/// `ant_shutdown`. The handle is invalid afterwards.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeShutdown<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) {
    let _: () = unowned_env
        .with_env(|_env| -> jni::errors::Result<()> {
            if handle == 0 {
                return Ok(());
            }
            let h = handle as *mut AntHandle;
            // SAFETY: the Kotlin side passes a handle from nativeInit
            // exactly once and zeroes its copy afterwards.
            unsafe {
                crate::ant_stop_gateway(h);
                crate::ant_shutdown(h);
            }
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// `AntNative.nativeStartGateway(handle, addr, lightMode, rpc)` —
/// `ant_start_gateway`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeStartGateway<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    addr: JString<'caller>,
    light_mode: jboolean,
    rpc: JString<'caller>,
) {
    let _: () = unowned_env
        .with_env(|env| -> jni::errors::Result<()> {
            let Some(h) = handle_ptr(env, handle) else {
                return Ok(());
            };
            let Some(addr) = c_arg(env, &addr, "addr") else {
                return Ok(());
            };
            let Some(rpc) = c_arg(env, &rpc, "rpc") else {
                return Ok(());
            };
            // SAFETY: live handle; strings outlive the call.
            bool_call(env, "start gateway", |err| unsafe {
                crate::ant_start_gateway(h, addr.as_ptr(), light_mode, rpc.as_ptr(), err)
            });
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// `AntNative.nativeStopGateway(handle): Boolean` — `ant_stop_gateway`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeStopGateway<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jboolean {
    unowned_env
        .with_env(|env| -> jni::errors::Result<jboolean> {
            let Some(h) = handle_ptr(env, handle) else {
                return Ok(false);
            };
            // SAFETY: live handle.
            Ok(unsafe { crate::ant_stop_gateway(h) })
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// `AntNative.nativePeerCount(handle): Int` — `ant_peer_count`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativePeerCount<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
) -> jint {
    unowned_env
        .with_env(|_env| -> jni::errors::Result<jint> {
            if handle == 0 {
                return Ok(0);
            }
            // SAFETY: live handle.
            Ok(unsafe { crate::ant_peer_count(handle as *const AntHandle) })
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// Generates the `ant_suspend` / `ant_resume` / `ant_wake` shims, which
/// share the `int f(handle, out_err)` shape.
macro_rules! lifecycle_shim {
    ($export:ident, $ffi:path, $name:literal) => {
        #[doc = concat!("`AntNative` shim over `", stringify!($ffi), "`.")]
        #[unsafe(no_mangle)]
        pub extern "system" fn $export<'caller>(
            mut unowned_env: EnvUnowned<'caller>,
            _class: JClass<'caller>,
            handle: jlong,
        ) -> jint {
            unowned_env
                .with_env(|env| -> jni::errors::Result<jint> {
                    let Some(h) = handle_ptr(env, handle) else {
                        return Ok(-1);
                    };
                    // SAFETY: live handle.
                    Ok(int_call(env, $name, |err| unsafe { $ffi(h, err) }))
                })
                .resolve::<ThrowRuntimeExAndDefault>()
        }
    };
}

lifecycle_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeSuspend,
    crate::ant_suspend,
    "suspend"
);
lifecycle_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeResume,
    crate::ant_resume,
    "resume"
);
lifecycle_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeWake,
    crate::ant_wake,
    "wake"
);

// ---------------------------------------------------------------------------
// Account, storage plan, settlement
// ---------------------------------------------------------------------------

/// Generates shims for `char *f(handle, out_err)` calls.
macro_rules! handle_string_shim {
    ($export:ident, $ffi:path, $name:literal) => {
        #[doc = concat!("`AntNative` shim over `", stringify!($ffi), "`.")]
        #[unsafe(no_mangle)]
        pub extern "system" fn $export<'caller>(
            mut unowned_env: EnvUnowned<'caller>,
            _class: JClass<'caller>,
            handle: jlong,
        ) -> JString<'caller> {
            unowned_env
                .with_env(|env| -> jni::errors::Result<JString<'caller>> {
                    let Some(h) = handle_ptr(env, handle) else {
                        return Ok(JString::null());
                    };
                    // SAFETY: live handle.
                    Ok(string_call(env, $name, |err| unsafe { $ffi(h, err) }))
                })
                .resolve::<ThrowRuntimeExAndDefault>()
        }
    };
}

/// Generates shims for `char *f(handle, gnosis_rpc, out_err)` calls.
macro_rules! rpc_string_shim {
    ($export:ident, $ffi:path, $name:literal) => {
        #[doc = concat!("`AntNative` shim over `", stringify!($ffi), "`.")]
        #[unsafe(no_mangle)]
        pub extern "system" fn $export<'caller>(
            mut unowned_env: EnvUnowned<'caller>,
            _class: JClass<'caller>,
            handle: jlong,
            rpc: JString<'caller>,
        ) -> JString<'caller> {
            unowned_env
                .with_env(|env| -> jni::errors::Result<JString<'caller>> {
                    let Some(h) = handle_ptr(env, handle) else {
                        return Ok(JString::null());
                    };
                    let Some(rpc) = c_arg(env, &rpc, "rpc") else {
                        return Ok(JString::null());
                    };
                    // SAFETY: live handle; `rpc` outlives the call.
                    Ok(string_call(env, $name, |err| unsafe {
                        $ffi(h, rpc.as_ptr(), err)
                    }))
                })
                .resolve::<ThrowRuntimeExAndDefault>()
        }
    };
}

handle_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeAccountInfo,
    crate::ant_account_info,
    "account info"
);
handle_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeAccountExportKey,
    crate::ant_account_export_key,
    "export key"
);
handle_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeStorageStatus,
    crate::ant_storage_status,
    "storage status"
);
handle_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeSettlementStatus,
    crate::ant_storage_settlement_status,
    "settlement status"
);
handle_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativePublisherProgress,
    crate::ant_publisher_progress,
    "broadcast progress"
);
handle_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativePublisherStop,
    crate::ant_publisher_stop,
    "stop broadcast"
);
rpc_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeStorageDiscover,
    crate::ant_storage_discover,
    "find storage"
);
rpc_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeStorageValidity,
    crate::ant_storage_validity,
    "storage validity"
);
rpc_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeSettlementDeposit,
    crate::ant_storage_settlement_deposit,
    "settlement deposit"
);
rpc_string_shim!(
    Java_at_vibing_ant_stream_AntNative_nativeSettlementTopup,
    crate::ant_storage_settlement_topup,
    "top up settlement deposit"
);

/// `AntNative.nativeStorageQuote(handle, rpc, depth, days): String` —
/// `ant_storage_quote`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeStorageQuote<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    rpc: JString<'caller>,
    depth: jint,
    days: jlong,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JString<'caller>> {
            let Some(h) = handle_ptr(env, handle) else {
                return Ok(JString::null());
            };
            let Some(rpc) = c_arg(env, &rpc, "rpc") else {
                return Ok(JString::null());
            };
            let (Ok(depth), Ok(days)) = (u8::try_from(depth), u64::try_from(days)) else {
                throw(env, "depth or days out of range");
                return Ok(JString::null());
            };
            // SAFETY: live handle; `rpc` outlives the call.
            Ok(string_call(env, "price plan", |err| unsafe {
                crate::ant_storage_quote(h, rpc.as_ptr(), depth, days, err)
            }))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

/// `AntNative.nativeStorageBuyXdai(handle, rpc, depth, amountPerChunk,
/// immutable): String` — `ant_storage_buy_xdai`. Spends real funds.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativeStorageBuyXdai<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    rpc: JString<'caller>,
    depth: jint,
    amount_per_chunk: JString<'caller>,
    immutable: jboolean,
) -> JString<'caller> {
    unowned_env
        .with_env(|env| -> jni::errors::Result<JString<'caller>> {
            let Some(h) = handle_ptr(env, handle) else {
                return Ok(JString::null());
            };
            let Some(rpc) = c_arg(env, &rpc, "rpc") else {
                return Ok(JString::null());
            };
            let Some(amount) = c_arg(env, &amount_per_chunk, "amountPerChunk") else {
                return Ok(JString::null());
            };
            let Ok(depth) = u8::try_from(depth) else {
                throw(env, "depth out of range");
                return Ok(JString::null());
            };
            // SAFETY: live handle; strings outlive the call.
            Ok(string_call(env, "activate storage", |err| unsafe {
                crate::ant_storage_buy_xdai(
                    h,
                    rpc.as_ptr(),
                    depth,
                    amount.as_ptr(),
                    i32::from(immutable),
                    err,
                )
            }))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------

/// `AntNative.nativePublisherStart(handle, configJson)` —
/// `ant_publisher_start`.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativePublisherStart<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    config_json: JString<'caller>,
) {
    let _: () = unowned_env
        .with_env(|env| -> jni::errors::Result<()> {
            let Some(h) = handle_ptr(env, handle) else {
                return Ok(());
            };
            let Some(config) = c_arg(env, &config_json, "configJson") else {
                return Ok(());
            };
            // SAFETY: live handle; `config` outlives the call.
            bool_call(env, "start broadcast", |err| unsafe {
                crate::ant_publisher_start(h, config.as_ptr(), err)
            });
            Ok(())
        })
        .resolve::<ThrowRuntimeExAndDefault>();
}

/// `AntNative.nativePublisherPushSegment(handle, isInit, data,
/// durationMs, discontinuity): Int` — `ant_publisher_push_segment`.
/// Returns its 0 / 1 / 2 disposition; -1 throws.
#[unsafe(no_mangle)]
pub extern "system" fn Java_at_vibing_ant_stream_AntNative_nativePublisherPushSegment<'caller>(
    mut unowned_env: EnvUnowned<'caller>,
    _class: JClass<'caller>,
    handle: jlong,
    is_init: jboolean,
    data: JByteArray<'caller>,
    duration_ms: jint,
    discontinuity: jboolean,
) -> jint {
    unowned_env
        .with_env(|env| -> jni::errors::Result<jint> {
            let Some(h) = handle_ptr(env, handle) else {
                return Ok(-1);
            };
            let bytes = match env.convert_byte_array(&data) {
                Ok(bytes) => bytes,
                Err(e) => {
                    throw(env, &format!("read segment bytes: {e}"));
                    return Ok(-1);
                }
            };
            let duration = u32::try_from(duration_ms).unwrap_or(0);
            // SAFETY: live handle; `bytes` outlives the call, which copies.
            Ok(int_call(env, "push segment", |err| unsafe {
                crate::ant_publisher_push_segment(
                    h,
                    is_init,
                    bytes.as_ptr(),
                    bytes.len(),
                    duration,
                    discontinuity,
                    err,
                )
            }))
        })
        .resolve::<ThrowRuntimeExAndDefault>()
}
