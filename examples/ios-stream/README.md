# AntStream — live video on Swarm (iOS)

SwiftUI app that broadcasts live video from the device camera onto Swarm
and plays live streams back, on top of an embedded light node
([`crates/ant-ffi`](../../crates/ant-ffi)).

The app shell (node lifecycle, the ported storage-onboarding flow,
Keychain/Secure-Enclave key storage) now carries a working broadcast:
camera capture (#65) and the live publisher (#67 stage 2). The viewer
(#66) is still to come; the **Broadcast** tab's readiness checklist is
the contract between it and this shell.

Bundle id: `at.vibing.ant.stream`. Data dir:
`Application Support/antstream/`.

## What's here

| File | Role |
|---|---|
| `AntStreamApp.swift` | App entry + scene-phase lifecycle (`ant_suspend` inside a `beginBackgroundTask`, `ant_resume`/`ant_wake` on foreground). |
| `AntNode.swift` | Swift wrapper over the C FFI: init with a host-held identity, gateway, storage plan, account. |
| `AccountKeystore.swift` | The account key: created, stored and restored through the Keychain / Secure Enclave. |
| `BroadcastView.swift` | "Ready to broadcast" checklist and the Go-live entry point. |
| `CaptureEngine.swift` | #65: `AVCaptureSession` → H.264 → `AVAssetWriter` in `.mpeg4AppleHLS` mode, emitting fMP4 segments; interruption handling and the on-disk segment ring. |
| `LiveBroadcast.swift` | Joins capture to `ant_publisher_*`, holds the foreground keep-alive, polls publish progress. |
| `LiveView.swift` | The going-live screen: preview, publish-lag indicator, live counters. |
| `BenchView.swift` | #67 stage 1: the throughput bench sheet. |
| `GetStartedView.swift` | Ported onboarding: plan tiers → on-chain quote → payment detection → one-shot auto-activation. |
| `StorageView.swift` | Ported storage tab: plan meter, extend/top-up, settlement warning, account + key backup/restore. |
| `StreamModels.swift`, `LiquidGlass.swift` | FFI JSON models and the shared glass chrome, lifted from `examples/ios-drive`. |

## Node lifecycle

`AntNode.start()` does, in order:

1. Resolve `Application Support/antstream/` and seed `peers.json` from
   the bundled `peers.seed.json` (cold-start bootstrap).
2. Load or create the account identity via `AccountKeystore` (below).
3. `ant_init_with_identity(dataDir, NULL, identityJSON, &err)` — the
   library gets the key in memory and never writes it to disk.
4. `ant_start_gateway("127.0.0.1:1633", light_mode: true, gnosisRpc)` —
   the in-process bee-shaped HTTP API. `light_mode` is what permits
   publish / feed / SOC writes; ultra-light is read-only. This is the
   surface the publisher (#67) uses and the viewer (#66) will.

`start()`, `shutdown()` and the Restore flow's restart all run on one
lifecycle queue, so only one of them is ever in flight: a Restore tapped
while the launch `start()` is still inside `ant_init_with_identity` waits
for it rather than racing a second init over the same data dir (two
concurrent `bind_account_state` calls would park each other's postage /
chequebook state, and both gateways would want port 1633).

Ordinary FFI calls are not transitions, so the queue does not order them:
each one hands the handle to a detached task and outlives the read that
produced it (a chain-scanning `ant_storage_discover` can still be inside
the node long after its sheet is gone). `AntNode.withHandle` counts those
borrows, and `shutdown()` — including the one inside Restore — clears the
handle and then waits for the count to reach zero before `ant_shutdown`
frees it, so no call is ever left running on freed memory or writing the
old account's state into a data dir `bind_account_state` has just
re-scoped.

Backgrounding spends the grace window inside `beginBackgroundTask` while
`ant_suspend` checkpoints; foregrounding calls `ant_resume` (re-dials the
peer set the OS reaped) then `ant_wake`, and rebinds the gateway — a
suspension can tear down its localhost listener, and `ant_resume`
recovers the swarm only (see `ant.h`).

## Account key: Keychain + Secure Enclave

The other example apps let the Rust library own the key: it writes
plaintext `identity.json` into the app container, which is readable by
anything that reaches the container and rides along in device backups.
AntStream owns the key instead.

New FFI surface (`crates/ant-ffi`):

| Entry point | Purpose |
|---|---|
| `ant_identity_generate` | Mint an identity document without starting a node. |
| `ant_identity_from_key` | Rebuild one from a backed-up 64-hex account key (restore path). |
| `ant_init_with_identity` | Start the node from a host-held identity; never touches `identity.json`. |

**Why the key is not itself an enclave key.** The Secure Enclave only
holds P-256 keys; Swarm/Ethereum accounts are secp256k1. So the enclave
holds a P-256 *wrapping* key that never leaves the chip, and the identity
document is ECIES-encrypted to it. The Keychain then stores ciphertext
that is useless on any other device. That is the strongest protection iOS
offers a secp256k1 secret.

Enclave keys are device-bound, so enclave wrapping and iCloud Keychain
sync are mutually exclusive. The Storage tab exposes the trade-off:

* **Secure Enclave (default)** — strongest at rest; a reinstall on
  another device needs the exported account key.
* **iCloud Keychain** (toggle) — plaintext-in-Keychain but synced, so a
  reinstall or a new device recovers the account by itself.
* **Keychain, this device** — fallback when the enclave is unavailable
  (older simulators return `errSecUnimplemented` for enclave key
  creation).

Every mode uses `kSecAttrAccessibleAfterFirstUnlock` so the node can
start after a reboot the user hasn't unlocked past; the two device-bound
modes add `…ThisDeviceOnly` so the item stays out of unencrypted backups.

**Recovery path.** "Back up key" reveals the raw account key (copied to an
expiring, device-local pasteboard rather than the plain clipboard);
"Restore" accepts one back. The key is validated by `ant_identity_from_key`
*before* anything is written, so a typo can't destroy a working account.
The overlay nonce is derived from the account address, so restoring the
same key twice always lands on the same overlay. Storing a key never
leaves the Keychain empty even for an instant — the new item is written
(add, or update when one is already there) before the old variant is
removed — so a failed write leaves the previous key in place instead of
letting the next launch mint a brand-new account over it.

Two changes reach past this device, and both go through a confirmation
alert stating their real blast radius. Turning the **iCloud Keychain**
toggle *off* deletes the `kSecAttrSynchronizable` item across the whole
iCloud circle, so the user's other iPhone/iPad loses the key too; the
alert offers a "Back up key first" way out. Restoring *while the stored
key syncs* overwrites that same synced item: the restored account takes
over on every device in the circle, and the current key's only synced
copy is gone for good. As a backstop, a device on the receiving end of a
toggle-off deletion — empty Keychain, but a data dir that has already run
an account (`account.json` is there) — refuses to mint a replacement and
reports "This device's account key is gone", pointing at Restore, instead of
silently starting a new account over the funded one's state.

Restoring a *different* account also re-scopes what the node keeps on
disk: `ant_init_with_identity` parks the previous account's postage
batches, chequebook association and SWAP ledgers under
`<data dir>/accounts/<its address>/` and swaps in whatever the restored
account left behind. Those are owned on-chain by one account, so reusing
them under another key would stamp and sign cheques that look valid on
the device and are rejected by every peer. Nothing is deleted, so
switching back restores the original account's plan intact.

**Scope.** This moves the key *at rest* out of the library, which is the
mobile half of PLAN.md § 5.10. It is not yet the full callback-based
`KeyProvider`: the library still holds the raw secret in memory while
running, because signing happens at ~6 independent sites (BZZ handshake,
postage stamps, SWAP cheques, Gnosis txs, ACT ECDH, gateway pubkey
derivation) and ACT needs ECDH, not just ECDSA. Threading a signing
callback through those is a separate change.

## Requirements

- macOS with **Xcode 26** (`IPHONEOS_DEPLOYMENT_TARGET = 26.0`; the UI
  uses iOS 26's `.glassEffect`).
- Rust stable with the iOS targets installed:
  ```sh
  rustup target add aarch64-apple-ios-sim   # simulator
  rustup target add aarch64-apple-ios       # device
  ```

## Running

```sh
open examples/ios-stream/AntStream.xcodeproj
```

The "Build Rust" script phase cross-compiles `ant-ffi` with the `chain`
feature on every build, so workspace changes are picked up by a plain
⌘B. Command-line equivalent:

```sh
cd examples/ios-stream
xcodebuild -project AntStream.xcodeproj -scheme AntStream \
  -configuration Debug \
  -destination 'platform=iOS Simulator,name=iPhone 17' build
```

Install + launch, and capture the embedded node's logs, exactly as for
AntDrive — see the "Driving the iOS Drive app on the simulator" section
in the repo's `AGENTS.md` (the FFI logs to stderr, which the simulator
does not forward to the unified log, so relaunch with `--console-pty`).

## First run

1. Launch — the Broadcast tab shows the readiness checklist. The account
   key is created and stored in the Keychain on this first launch.
2. Tap **Get started**, pick a plan. Plans are priced live against Gnosis
   (`ant_storage_quote`), quoted in hours of 720p video (~1 GB/hour).
3. Send the quoted xDAI to the address/QR shown. The screen polls the
   chain and activates automatically the moment funds land —
   `ant_storage_buy_xdai` swaps the xBZZ shortfall, buys the batch, and
   deploys the chequebook.
4. The checklist goes all-green: connected, key secured, plan active,
   settlement ready, gateway up. No `antctl` at any point.

## Throughput bench (issue #67 stage 1)

**Broadcast → Run bench** runs the publisher loop with synthetic
segments instead of camera output and reports what this device
sustains: Mbit/s, chunks/s, publish latency and how far behind the live
edge it falls. The measurement core is shared with the native CI
harness (`cargo run -p ant-ffi --example publish_bench`), so device and
desktop numbers are directly comparable.

Criteria, results so far and the exact steps for a quotable 30-minute
device run: [`crates/ant-ffi/ANTSTREAM_BENCH.md`](../../crates/ant-ffi/ANTSTREAM_BENCH.md).

## Going live (issues #65 + #67 stage 2)

**Broadcast → Go live** opens `LiveView`, which runs the real thing:

1. `CaptureEngine` starts an `AVCaptureSession` (camera + mic) into an
   `AVAssetWriter` in `.mpeg4AppleHLS` mode. The writer emits one fMP4
   initialization segment and then a media segment every
   `segmentSeconds`; both are handed straight to the node.
2. `ant_publisher_push_segment` queues each one. Per segment the node
   does `POST /bzz`, rebuilds the rolling HLS playlist, publishes it,
   and points the channel's sequence feed at it with `POST /soc` — the
   bee-js update shape, so any bee gateway resolves the channel from the
   feed manifest `POST /feeds` created at start.
3. The screen shows the **publish lag** (capture → the feed update that
   makes the segment playable) the whole time, plus how many segments
   the live-edge discipline dropped to hold it there.

Defaults are the stage-1 go/no-go row: 360p at ~900 kbit/s, 2 s
segments, a 4-deep publish window. Window 4 is measured, not arbitrary —
window 8 collapsed the connection layer in the stage-1 soaks — so it is
not exposed as a UI knob.

Interruptions (incoming call, backgrounding, camera flip, orientation,
thermal downshift) finish the current segment cleanly and start a fresh
writer; the first segment after one is marked `#EXT-X-DISCONTINUITY` in
the playlist. The idle timer is held off and the audio session is
configured for recording for the broadcast's duration.

Camera and broadcast paths are device-only — the simulator has no
capture device. `antstream-visual` covers the screen by running the same
encoder and publisher from a generated test pattern
(`-antstream-shot-live`), which is labelled as such on screen.
