# AntStream for Android: live video on Swarm

Android port of [`examples/ios-stream`](../ios-stream). A Compose app that
broadcasts live video from the phone camera onto Swarm through an embedded
light node ([`crates/ant-ffi`](../../crates/ant-ffi)), and plays channels
back.

Package: `at.vibing.ant.stream`. Data dir: `files/antstream/`.

## Download

Every push that touches the app or the crates builds a release APK in
[`.github/workflows/android-stream.yml`](../../.github/workflows/android-stream.yml)
and attaches it as `AntStream.apk` to the rolling
`antstream-android-latest` prerelease. Install it on an arm64 phone
running Android 8 or newer. Builds are signed with the public example key
in `app/antstream-example.keystore`, so each build installs over the
previous one and keeps the account.

## What's here

| File | Role |
|---|---|
| `AntNative.kt` | `external fun` declarations for `crates/ant-ffi/src/stream_jni.rs`. Each is a shim over the `ant.h` function of the same name, so both apps run the same node code. |
| `AntNode.kt` | Node lifecycle: init with a host-held identity, light-mode gateway on 127.0.0.1:1633, storage plan, settlement, publisher, suspend/wake on network loss. |
| `AccountKeystore.kt` | The account key, encrypted under an AES-GCM key held in the Android Keystore (the iOS app uses the Keychain / Secure Enclave). |
| `capture/CaptureEngine.kt` | Camera2 into a hardware H.264 encoder's input surface plus the preview; the mic through AAC-LC. |
| `capture/Fmp4Muxer.kt` | Writes fMP4 HLS segments (`ftyp`+`moov` init segment, `moof`+`mdat` media segments). Android's `MediaMuxer` cannot emit standalone segments. |
| `capture/Segmenter.kt` | Cuts segments on keyframes (requested every second) at about the target duration, keeping audio and video on one timeline. |
| `LiveBroadcast.kt` | Hands capture segments to `ant_publisher_push_segment` in order, polls progress, drains on stop. |
| `ui/` | Broadcast checklist, onboarding (plan pricing, payment detection, auto-activation), live screen, Watch (ExoPlayer HLS through the local gateway), Storage (plan, key backup / restore). |

## Going live

1. **Get started**: pick a plan, send the quoted xDAI on Gnosis to the
   shown address. The screen polls the chain and activates by itself:
   `ant_storage_buy_xdai` swaps the xBZZ shortfall, buys the batch and
   deploys the chequebook.
2. **Go live**: the screen turns landscape. Encoding is H.264 at
   ~900 kbit/s (the stage-1 rendition from #67) with AAC audio, in 2 s
   segments. Each segment is `POST /bzz`'d, the rolling playlist is
   republished and the channel's sequence feed is updated, exactly as on
   iOS. The screen shows the publish lag and the channel reference.
3. **Watch**: paste a channel reference. The player loads
   `http://127.0.0.1:1633/bzz/<channel>/stream.m3u8`, which re-resolves
   the feed on every playlist refresh.

The broadcast is foreground-only: leaving the app ends it cleanly (the
playlist gets `#EXT-X-ENDLIST`). Without microphone permission it
broadcasts video only.

## Building locally

```sh
# Native library (needs the NDK, cargo-ndk and the Android Rust targets)
cargo xtask build-android-arm64  --features chain --app android-stream
cargo xtask build-android-x86_64 --features chain --app android-stream   # emulator

cd examples/android-stream
./gradlew assembleRelease     # app/build/outputs/apk/release/app-release.apk
```

## Self-test without a storage plan

A debug hook records segments to the app's cache instead of publishing,
so the capture pipeline can be checked on a device or emulator without
funds:

```sh
adb shell am start -n at.vibing.ant.stream/.MainActivity --es antstream.selftest st1
# Go live, wait, Stop, then pull cache/st1/ with `adb exec-out run-as …`
```

`init-1.mp4`, `seg-N.m4s` and `durations.txt` (segment number, init flag,
duration in ms) land in `cache/st1/`. A playlist over them plays in
ffmpeg / ffprobe.

## Not yet ported from iOS

- The throughput bench sheet (`ant_bench_*`).
- Extending a plan and topping up the settlement deposit from the Storage
  tab (the JNI shims exist; the UI does not).
- Camera flip, and keeping a broadcast alive in the background.
