# ant-ffi — embedding the ant light-node in an iOS app

`ant-ffi` wraps the whole ant stack (libp2p swarm, retrieval pipeline,
upload manager, postage stamping) behind a hand-written `extern "C"`
surface so a Swift app can run a real Swarm light-node *in-process* —
no localhost gateway, no separate daemon. The app links a single
static library, imports one C header, and talks to mainnet directly
over TCP+Noise.

Two working integrations live in this repo and are the best reference
for anything this document glosses over:

| App | What it exercises |
|---|---|
| [`examples/ios-download`](../../examples/ios-download) | minimal download-only surface (`ant_init` / `ant_download` / progress / streaming) |
| [`examples/ios-drive`](../../examples/ios-drive) | full surface: uploads, storage plans, on-chain plan purchase (`chain` feature), account info |

## The pieces

| Artefact | Where |
|---|---|
| C header (the API contract) | [`include/ant.h`](include/ant.h) |
| Static library | `target/<triple>/<profile>/libant_ffi.a`, produced by `cargo xtask` |
| Cross-compile recipe | [`xtask/`](../../xtask) (`cargo xtask build-ios-sim` / `build-ios-device`) |
| Swift wrapper to copy from | [`examples/ios-drive/AntDrive/AntNode.swift`](../../examples/ios-drive/AntDrive/AntNode.swift) |

The crate builds as `staticlib` (plus `rlib` for workspace consumers).
There is deliberately no `cdylib` for Apple platforms: if a `.dylib`
sits next to the `.a`, Apple's linker prefers it and bakes an absolute
build-host path into the app's load commands, which then fails to load
on the simulator or device.

## 0. No-toolchain path: the released `AntFFI.xcframework`

If you don't want a Rust toolchain in your build, use the prebuilt
artifact instead of §1–2. Every `v*` tag on the
[releases page](https://github.com/solardev-xyz/ant/releases) carries
`AntFFI.xcframework.zip` (device `ios-arm64` + fat
`ios-arm64_x86_64-simulator` static libraries, `ant.h`, module map)
and `AntFFI.xcframework.zip.checksum` (the SwiftPM checksum of the
zip).

**Swift Package Manager** — add a `binaryTarget` pinned to the
release URL and checksum:

```swift
// Package.swift
targets: [
    .binaryTarget(
        name: "AntFFI",
        url: "https://github.com/solardev-xyz/ant/releases/download/v0.5.17/AntFFI.xcframework.zip",
        checksum: "<paste the contents of AntFFI.xcframework.zip.checksum>"
    ),
    // your target:
    .target(name: "MyApp", dependencies: ["AntFFI"]),
]
```

**Or plain Xcode** — unzip and drag `AntFFI.xcframework` into the
target's *Frameworks, Libraries, and Embedded Content* (embed setting
*Do Not Embed* — it's a static library; Xcode picks the right slice
per destination automatically).

Either way, the bundled module map makes the C surface available as

```swift
import AntFFI
```

— no bridging header, no header search paths, and no manual
`OTHER_LDFLAGS`: the module map carries `link framework` directives
for `Security` / `SystemConfiguration` / `CoreFoundation`, so they
autolink.

What the prebuilt artifact bakes in:

- **`chain` feature on** (on-chain storage-plan calls work; you carry
  reqwest + rustls even if you never use them; expect ~10–14 MB added
  to the app binary, ~5–7 MB to the thinned App Store download).
- **Min iOS 15.0** (`IPHONEOS_DEPLOYMENT_TARGET` pinned by the xtask).
- **Pre-1.0 ABI** — this is the hand-written pre-UniFFI surface; pin
  an exact release and expect breaking changes between tags.
- **One Rust staticlib per app** — a second Rust-based static library
  in the same app collides at symbol level (see below).

### Combining with other Rust libraries in the same app

Every Rust `staticlib` embeds its own full copy of the Rust runtime —
`std`, `core`, `alloc`, the allocator shims, the panic machinery — so
linking the prebuilt `AntFFI.xcframework` *plus* a second,
separately-built Rust `.a` fails with duplicate-symbol errors (and
two interleaved std/allocator copies would be unsound even if it
linked). The fix is to compile both into **one** staticlib instead of
linking two: `ant-ffi` also builds as an `rlib` for exactly this.
Make a small aggregator crate that depends on both and emits the
single `.a`:

```toml
# myapp-ffi/Cargo.toml
[lib]
crate-type = ["staticlib"]

[dependencies]
ant-ffi = { git = "https://github.com/solardev-xyz/ant", tag = "v0.5.17", features = ["chain"] }
my-other-lib = { path = "../my-other-lib" }
```

```rust
// myapp-ffi/src/lib.rs — re-export so both C surfaces are kept
pub use ant_ffi::*;
pub use my_other_lib::*;
```

One compilation graph means one copy of std and one allocator — and
cargo unifies shared dependencies, so if both stacks use tokio or
libp2p you ship *one* copy instead of two (a substantial size win
over any two-library setup). Cross-compile and package it with the
same recipe as `cargo xtask build-ios-xcframework`, pointed at your
aggregator crate. The trade-off: you build from source rather than
consuming the prebuilt zip — but if you have your own Rust library,
you have the toolchain anyway.

The alternatives are worse: dylib-style xcframeworks coexist (Apple's
two-level namespace keeps each dylib's std private) but bring
codesigning/embedding obligations and duplicate every shared
dependency, and `ant-ffi` deliberately ships no Apple dylib (see the
note above § 1); prelinking with `ld -r -exported_symbols_list` to
internalize the runtime symbols works but leaves you maintaining
export lists against linker behaviour — a last resort when neither
library can be rebuilt from source.

The **ATS exception** from §2 (*Info.plist — App Transport Security*)
is still required — the prebuilt node speaks the same Noise-over-TCP —
and everything from §3 onward (writable data dir, lifecycle, memory
and threading rules) applies to the prebuilt artifact exactly as it
does to a from-source build. To reproduce the artifact locally:
`cargo xtask build-ios-xcframework` (macOS only).

## 1. Build the static library

One-time setup — install the Rust targets you need:

```sh
rustup target add aarch64-apple-ios-sim   # Apple Silicon simulator
rustup target add aarch64-apple-ios       # real iPhone / iPad
rustup target add x86_64-apple-ios        # Intel-Mac simulator (optional)
```

Then, from the repo root:

```sh
cargo xtask build-ios-sim                      # simulator, release
cargo xtask build-ios-sim --profile debug      # simulator, debug
cargo xtask build-ios-sim --arch x86_64        # Intel simulator
cargo xtask build-ios-device                   # real device (always aarch64)

# Apps that use the on-chain storage-plan calls need the chain feature:
cargo xtask build-ios-sim --features chain
```

Output lands at a stable, predictable path the Xcode project can point
at:

```
target/aarch64-apple-ios-sim/release/libant_ffi.a   # simulator
target/aarch64-apple-ios/release/libant_ffi.a       # device
```

### Cargo features

| Feature | Default | What it adds |
|---|---|---|
| `chain` | off | `ant_storage_connect_batch`, `ant_storage_discover`, `ant_storage_quote`, `ant_storage_buy`, `ant_storage_buy_xdai` — anything that reads or writes Gnosis. Pulls reqwest + rustls into the slice, so download-only apps should leave it off. Without it those functions return an error. |
| `jni` | off | Android-only JNI exports. Never needed on iOS. |

## 2. Wire up the Xcode project

Four build settings plus one script phase. All values below are
relative to a project living at `examples/<your-app>/` — adjust the
`../..` depth to wherever your project sits relative to the repo root.

**Bridging header** (`SWIFT_OBJC_BRIDGING_HEADER`) — a one-liner that
re-exports the C surface:

```c
#import "ant.h"
```

**Header search path** so the basename import resolves:

```
HEADER_SEARCH_PATHS = $(SRCROOT)/../../crates/ant-ffi/include
```

**Library search paths**, SDK-conditional so a device build can never
accidentally link the simulator slice (the linker rejects it with a
confusing `built for 'iOS-simulator'` error otherwise):

```
LIBRARY_SEARCH_PATHS[sdk=iphoneos*]        = $(SRCROOT)/../../target/aarch64-apple-ios/release
LIBRARY_SEARCH_PATHS[sdk=iphonesimulator*] = $(SRCROOT)/../../target/aarch64-apple-ios-sim/release
```

(Debug configurations point at `.../debug` instead.) Add
`libant_ffi.a` to the target's *Link Binary With Libraries* phase —
the file reference's own path only matters for display; linking
resolves by basename through the search paths above.

**Linker flags** for the system frameworks the Rust slice calls into:

```
OTHER_LDFLAGS = -framework SystemConfiguration -framework Security -framework CoreFoundation
```

**"Build Rust" run-script phase**, ordered *before* Compile Sources,
so a plain ⌘B picks up Rust changes. It selects the slice from Xcode's
own variables:

```sh
set -euo pipefail
cd "$SRCROOT/../.."

PROFILE=release
[ "${CONFIGURATION:-Release}" = "Debug" ] && PROFILE=debug

export PATH="$HOME/.cargo/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"

if [ "${PLATFORM_NAME:-iphonesimulator}" = "iphoneos" ]; then
    cargo xtask build-ios-device --profile "$PROFILE"
else
    ARCH=aarch64
    [ "${ARCHS:-arm64}" = "x86_64" ] && ARCH=x86_64
    cargo xtask build-ios-sim --arch "$ARCH" --profile "$PROFILE"
fi
```

(Append `--features chain` to both xtask calls if your app uses the
on-chain storage calls. Disable the *user script sandboxing* build
setting, or the phase can't write to `target/`.)

**Info.plist — App Transport Security.** The node speaks Noise XX over
raw TCP to mainnet peers; that isn't TLS, and ATS would refuse every
dial. The example apps set the blunt global exception:

```xml
<key>NSAppTransportSecurity</key>
<dict>
    <key>NSAllowsArbitraryLoads</key>
    <true/>
</dict>
```

A production app should scope this more narrowly, but some exception
is required — there is no TLS anywhere in the peer protocol.

## 3. Lifecycle and memory model

Everything hangs off one opaque `AntHandle*`:

```
ant_init(data_dir)  ──►  AntHandle*  ──►  ... API calls ...  ──►  ant_shutdown(handle)
```

- `ant_init` spawns a Tokio runtime and the node event loop on
  background threads, loads (or creates) the node identity, and
  returns immediately. Pass a writable directory — the iOS
  *Application Support* sandbox directory is the right home. Identity,
  the peer snapshot, and a 512 MiB on-disk chunk cache live there, so
  re-opened content is served locally across launches.
- Ownership is strict and symmetrical: every pointer the library hands
  out goes back to the library. Body buffers → `ant_free_buffer(ptr,
  len)`; every `char*` (results *and* `out_err` strings) →
  `ant_free_string`. Never `free(3)`. `ant_shutdown` frees the handle
  and aborts all node tasks; close any open `AntStream` first.
- Error convention: fallible calls return `NULL` (or `-1`) and write
  an allocated error string into `char **out_err` (pass `NULL` to opt
  out).
- Thread-safety: the handle is `Send + Sync`. Call from any thread;
  concurrent downloads are fine and share the node's cache. But the
  calls **block** the calling thread (they wait on the embedded
  runtime), so never call them on the main thread — wrap them in
  `Task.detached` from Swift.

## 4. The API at a glance

[`include/ant.h`](include/ant.h) is the authoritative, heavily
commented contract. Summary:

| Group | Functions |
|---|---|
| Lifecycle | `ant_init`, `ant_shutdown`, `ant_free_buffer`, `ant_free_string` |
| Node status | `ant_peer_count`, `ant_agent_string`, `ant_account_info`, `ant_account_export_key` |
| Whole-file download | `ant_download`, `ant_download_progress`, `ant_cancel_download`, `ant_list_bzz` |
| Streaming (ranged reads, e.g. video) | `ant_stream_open`, `ant_stream_read`, `ant_stream_pull`, `ant_stream_progress`, `ant_stream_close` |
| Uploads | `ant_upload_start`, `ant_upload_list`, `ant_upload_status`, `ant_upload_pause` / `_resume` / `_cancel` |
| Storage plans (postage) | `ant_storage_status`, `ant_storage_settlement_status`, `ant_storage_verify_propagation` |
| Storage plans, on-chain (`chain` feature) | `ant_storage_connect_batch`, `ant_storage_discover`, `ant_storage_quote`, `ant_storage_buy`, `ant_storage_buy_xdai` |

References accept `64-hex`, `bytes://<hex>`, or `bzz://<hex>[/path]`.
The structured calls return JSON documents (shapes documented per
function in `ant.h`) — decode them with `Codable` on the Swift side.

Progress reporting is poll-based by design: there is no callback
bridge to keep alive across the FFI. Sample `ant_download_progress` /
`ant_stream_progress` from a SwiftUI timer at whatever cadence you
like; it only takes a short mutex. The one callback exception is
`ant_stream_pull`, which delivers chunks as they arrive — shaped
specifically for `AVAssetResourceLoaderDelegate` so `AVPlayer` can
stream video without buffering the whole range first.

## 5. Calling it from Swift

The pattern, condensed from `AntNode.swift` (an `@MainActor`
`ObservableObject` holding the handle for the app's lifetime):

```swift
// Startup — blocking call, so hop off the main thread.
let dataDir = FileManager.default
    .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
    .appendingPathComponent("ant", isDirectory: true)
try? FileManager.default.createDirectory(at: dataDir, withIntermediateDirectories: true)

let handle: OpaquePointer? = await Task.detached(priority: .userInitiated) {
    var err: UnsafeMutablePointer<CChar>? = nil
    let h = dataDir.path.withCString { ant_init($0, &err) }
    if h == nil, let err {
        print("ant_init failed: \(String(cString: err))")
        ant_free_string(err)
    }
    return h
}.value
```

```swift
// Download — copy out, free, surface errors.
func download(_ reference: String) async throws -> Data {
    try await Task.detached(priority: .userInitiated) {
        var len = 0
        var err: UnsafeMutablePointer<CChar>? = nil
        let body = reference.withCString { ant_download(handle, $0, &len, &err) }
        if let body {
            let data = Data(bytes: body, count: len)
            ant_free_buffer(body, len)
            return data
        }
        let msg = err.flatMap { String(cString: $0) } ?? "download failed"
        if let err { ant_free_string(err) }
        throw AntError.op(msg)
    }.value
}
```

JSON-returning calls all follow one shape worth wrapping once:

```swift
static func string(
    _ call: (inout UnsafeMutablePointer<CChar>?) -> UnsafeMutablePointer<CChar>?
) throws -> String {
    var err: UnsafeMutablePointer<CChar>? = nil
    if let out = call(&err) {
        defer { ant_free_string(out) }
        return String(cString: out)
    }
    let msg = err.flatMap { String(cString: $0) } ?? "call failed"
    if let err { ant_free_string(err) }
    throw AntError.op(msg)
}
```

## 6. Operational notes

- **Cold-start warmup.** After `ant_init` returns, the node still
  needs a few seconds of BZZ handshakes before downloads succeed
  (~5–20 s to a warm peer set on mainnet). The FFI absorbs this:
  `ant_download` internally retries the "no peers" condition rather
  than making Swift poll. Show `ant_peer_count` in the UI so the user
  sees the node warming up.
- **Seed the peerstore for faster cold starts.** Bundle a known-good
  `peers.json` snapshot in the app and copy it to
  `<data_dir>/peers.json` before the first `ant_init` (see
  `seedPeerstoreIfNeeded` in `AntNode.swift` and the bundled
  `peers.seed.json`). Skips most of the bootnode discovery walk on
  first launch.
- **Logs go to stderr**, via `tracing`. Xcode's console shows them
  when running from Xcode. Outside Xcode the simulator does *not*
  forward stderr to the unified log — relaunch with a console
  attached: `xcrun simctl launch --console-pty <UDID> <bundle-id>`.
- **Version introspection.** `ant_agent_string` reports
  `ant-ffi/<crate-version>` from the live node — the version actually
  linked, independent of the app bundle's metadata.
- **Uploads need a storage plan and settlement.** `ant_upload_start`
  stamps against a postage batch; connect or buy one first (Storage
  group). `ant_storage_settlement_status` tells you whether a
  chequebook exists — without one, pushed chunks won't propagate
  (peers charge per chunk and freeze out a node that can't pay). After
  an upload, `ant_storage_verify_propagation` does a cache-bypassing
  read-back to prove the content is actually retrievable from the
  network.
- **Device builds and codesigning.** The SDK-conditional library
  search paths make device-vs-simulator slice selection automatic. The
  interactive one-time setup (Developer Mode on the phone, Apple ID in
  Xcode, team ID) is documented step-by-step in
  [`examples/ios-download/README.md`](../../examples/ios-download/README.md#running-on-a-real-iphone),
  along with the common failure modes.

## Status

This surface is the hand-written pre-UniFFI iteration (PLAN.md § 9):
deliberately C-shaped, no codegen, header maintained by hand next to
`src/lib.rs`. The planned Phase-4 mobile artefact replaces it with a
UniFFI-generated `.xcframework`; until then, this is the supported way
to embed ant in an iOS app.

An interim third-party release of this surface ships as a zipped
`AntFFI.xcframework` on the GitHub releases page (per `v*` tag, built
by `.github/workflows/release-ios.yml` via
`cargo xtask build-ios-xcframework`): device + fat-simulator static
libraries, `ant.h`, and a `module.modulemap` so consumers `import
AntFFI` — consumable as an SPM `binaryTarget` using the published
`AntFFI.xcframework.zip.checksum`. Min iOS 15.0; the `chain` feature
is baked in; the ABI is pre-1.0 and will change when the UniFFI
artefact lands. See PLAN.md § "Interim releasable iOS artifact —
`AntFFI.xcframework` (pre-UniFFI)" for remaining work items (Swift
wrapper package, codesigning) and known limitations.

## AntStream publisher throughput bench

`ant_bench_start` / `ant_bench_progress` / `ant_bench_stop` drive the
AntStream stage-1 throughput benchmark (issue #67) — the publisher loop
with synthetic segments in place of the camera, measuring sustained
Mbit/s, chunks/s, publish latency and live-edge lag. Runnable natively
(`cargo run --release -p ant-ffi --example publish_bench`) and on device
via the AntStream app's **Broadcast → Run bench**. Criteria, results and
how to run it: [`ANTSTREAM_BENCH.md`](ANTSTREAM_BENCH.md).
