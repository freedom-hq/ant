package at.vibing.ant.stream

/**
 * Thrown by every [AntNative] call that fails. Constructed by name from
 * `crates/ant-ffi/src/stream_jni.rs`, so its package, name and
 * `(String)` constructor are pinned in `proguard-rules.pro`.
 */
class AntException(message: String) : Exception(message)
