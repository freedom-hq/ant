# AntStream for Android: live video on Swarm

Android port of [`examples/ios-stream`](../ios-stream). A Compose app that
broadcasts live video from the phone camera onto Swarm through an embedded
light node ([`crates/ant-ffi`](../../crates/ant-ffi)), and plays channels
back.

Package: `at.vibing.ant.stream`. Data dir: `files/antstream/`.

## Download

Every push that touches the app or the crates builds a release APK in
[`.github/workflows/android-stream.yml`](../../.github/workflows/android-stream.yml)
and publishes it as its own prerelease, tagged
`antstream-android-v0.1.<build>` (the APK's `versionName` matches, its
`versionCode` is the build number). Install it on an arm64 phone running
Android 8 or newer. Builds are signed with the public example key in
`app/antstream-example.keystore`, so each build installs over the
previous one and keeps the account.

To get updates with [Obtainium](https://github.com/ImranR98/Obtainium),
add `https://github.com/freedom-hq/ant` and set:

- **Include prereleases**: on
- **Filter release titles by regular expression**: `^AntStream Android`

The filter matters: the repo's other releases are ant node builds with no
APK.

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
| `Channel.kt` | The account's channel (owner + topic `antstream`), link parsing and the links Share sends. `Keccak.kt` hashes topics and feed slot ids in plain Kotlin. |
| `FeedPlaylist.kt` | Follows a channel's feed for the Watch tab and serves ExoPlayer its playlist. |
| `StreamInfo.kt` | Stream info (#220): the publisher's `"info"` JSON, what the app remembers per account, and reading a channel's entry from its owner's catalog feed. Plain Kotlin, unit-tested. `Thumbnail.kt` scales a camera frame or a picked photo to a thumbnail JPEG. |
| `ui/` | Broadcast checklist, onboarding (plan pricing, payment detection, auto-activation), live screen, Watch (ExoPlayer HLS through the local gateway), Storage (plan, key backup / restore). |

## Going live

1. **Get started**: pick a plan, send the quoted xDAI on Gnosis to the
   shown address. The screen polls the chain and activates by itself:
   `ant_storage_buy_xdai` swaps the xBZZ shortfall, buys the batch and
   deploys the chequebook.
2. **Go live**: the screen turns landscape. Encoding is H.264 at
   ~900 kbit/s (the stage-1 rendition from #67) with AAC audio, in 2 s
   segments. Broadcasts go out in the publisher's `"solar-punk"` format
   only (#217): each segment is `POST /bytes`'d, the rolling playlist
   lists them as bare references, and every update of the channel's
   sequence feed **is** that playlist. Ladder time markers and a master
   playlist go out too (`"ladder": true`), so a viewer joining late
   reaches the live edge in two reads. Each broadcast continues the
   channel's feed after its head (#173); the app tracks no index. The
   screen shows the publish lag and the channel.
3. **Watch**: the field starts on your own channel; enter any other as
   `0x<owner>/<topic>`, `swarm://<owner>/<topic>` or a web player link
   (an owner alone means its `antstream` topic). The player
   (`FeedPlaylist.kt`) joins from a ladder marker or
   `GET /feeds/{owner}/{topic}` on the local gateway, then follows the
   feed slot by slot (`GET /soc/{owner}/{id}`), and hands ExoPlayer each
   playlist with its bare references made
   `http://127.0.0.1:1633/bytes/<ref>` (the `#EXT-X-MAP` too). A
   finished broadcast's recording plays from the start. Channels in the
   older `"bzz"` format don't play any more.

## Stream info

The **Stream info** card (Broadcast tab, and the live screen before and
during a broadcast) edits what the channel's catalog entry shows (#220):

- **Title** (required; starts as the channel's name) and an optional
  **description**.
- **Thumbnail**: by default a frame read back from the encoder's GL
  renderer about 4 s after Go live, scaled to 640 px on its long side
  and JPEG'd at quality 70 (a few tens of KB), uploaded with
  `ant_stream_thumbnail_upload` and set with `ant_publisher_set_info`.
  **Pick photo** (the system photo picker) replaces it: uploaded on save
  when there is a storage plan, at Go live otherwise.
- **Scheduled start**: a date and time, and **Announce**
  (`ant_stream_catalog_publish`, needs a storage plan, not while live)
  lists the upcoming broadcast; the card shows "Starts at …" and whether
  it was announced. The broadcast that follows replaces the entry, and
  the announcement is cleared.

The info is remembered per account (in the channel's preferences) for
the next broadcast; saving while live updates the running broadcast's
entry at once. The publisher config gets it as `"info"` at Go live.

The Watch tab shows the watched channel's title, description and a
scheduled start from its owner's catalog feed
(`GET /feeds/{owner}/{keccak256("antstream-catalog")}`), read beside
playback and again every minute; a channel without a catalog shows
nothing extra.

## Your channel

A channel is an **owner and a topic**: your account address and the
topic text `antstream`, the same for every broadcast, so there is one
channel per account and nothing to create before sharing it. The
channel's name is only a label (the broadcast's notes and the UI), never
part of the topic, so renaming it never forks the channel. The Broadcast
tab shows it and shares:

- the web player, `bzz://<player>/#0x<owner>/antstream` in the Freedom
  browser, and its gateway twin
  `https://api.gateway.ethswarm.org/bzz/<player>/#0x<owner>/antstream` for
  any browser (`<player>` is `PLAYER_REF`, the player's feed manifest, so
  the link outlives player updates);
- `swarm://<owner>/antstream`, the URI Solar Punk's players open a
  channel by.

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
