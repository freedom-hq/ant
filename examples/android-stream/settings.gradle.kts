// AntStream for Android: live video from the phone camera onto Swarm,
// the Android sibling of `examples/ios-stream`. Embeds `crates/ant-ffi`
// through its `stream_jni.rs` surface as
// `app/src/main/jniLibs/<abi>/libant_ffi.so`, built out-of-band by
// `cargo xtask build-android-* --features chain --app android-stream`.

pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "AntStream"

include(":app")
