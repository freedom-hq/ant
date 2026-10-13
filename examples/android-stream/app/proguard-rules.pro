# The JNI natives on AntNative are resolved by symbol name from
# libant_ffi.so, and AntException is constructed by name from
# crates/ant-ffi/src/stream_jni.rs; R8 sees neither use.
-keep class at.vibing.ant.stream.AntNative {
    native <methods>;
}
-keep class at.vibing.ant.stream.AntException {
    public <init>(java.lang.String);
}
