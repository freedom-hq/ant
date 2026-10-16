# AntStream — live video on Swarm (iOS)

SwiftUI app that broadcasts live video from the device camera onto Swarm
and plays live streams back, on top of an embedded light node
([`crates/ant-ffi`](../../crates/ant-ffi)).

The app shell (node lifecycle, the ported storage-onboarding flow,
Keychain/Secure-Enclave key storage) carries a working broadcast: camera
capture (#65) and the live publisher (#67 stage 2), on a permanent
per-account channel link, plus a **Watch** tab that plays any channel
through the embedded node. Feature-for-feature it matches the Android
port in [`examples/android-stream`](../android-stream), with the
differences listed under [Differences from Android](#differences-from-android).

Bundle id: `baby.freedom.antstream`. Data dir:
`Application Support/antstream/`.

## What's here

| File | Role |
|---|---|
| `AntStreamApp.swift` | App entry + scene-phase lifecycle (`ant_suspend` inside a `beginBackgroundTask`, `ant_resume`/`ant_wake` on foreground). |
| `AntNode.swift` | Swift wrapper over the C FFI: init with a host-held identity, gateway (with port fallback), storage plan, account, and the account's permanent `Channel`. |
| `AccountKeystore.swift` | The account key: created, stored and restored through the Keychain / Secure Enclave. |
| `BroadcastView.swift` | "Ready to broadcast" checklist, the "Your channel" card and the Go-live entry point. |
| `CaptureEngine.swift` | #65: `AVCaptureSession` → H.264 → `AVAssetWriter` in `.mpeg4AppleHLS` mode, emitting fMP4 segments; interruption handling and the on-disk segment ring. |
| `LiveBroadcast.swift` | Joins capture to `ant_publisher_*`, holds the foreground keep-alive, polls publish progress. |
| `LiveView.swift` | The going-live screen: quality presets, preview, publish-lag indicator, live counters, channel share. |
| `WatchView.swift` | The Watch tab: `AVPlayer` on a channel from the in-process gateway, in either publisher format (`/bzz/<ref>/stream.m3u8`, or `GET /feeds` with segment references rewritten to `/bytes/`), with automatic retry. |
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
   surface the publisher (#67) uses and the Watch tab plays from. When
   another app (another Swarm node, a Freedom install) already holds
   1633, the next free port up to 1643 is used instead, the same range
   Freedom desktop falls back through. The address that worked is
   remembered and tried first next time; `AntNode.gatewayURL` follows it,
   and so do the publisher's and the bench's `"gateway"`. If every port
   fails, the checklist shows the bind error and the node retries every
   10 s.

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

## TestFlight

[`.github/workflows/antstream-ios-testflight.yml`](../../.github/workflows/antstream-ios-testflight.yml)
archives the app, signs it with Xcode's cloud-managed signing and
uploads it to App Store Connect on every release tag (`v*`), or by hand
from the Actions tab. Builds are numbered `0.1.<run>` / `<run>`.

One-time setup:

1. In App Store Connect, create the app record for
   `baby.freedom.antstream` (team `46XPHRBLH5`) if it doesn't exist.
2. Under Users and Access → Integrations → App Store Connect API,
   create a team key with the **Admin** role (cloud signing creates the
   distribution certificate) and download its `.p8`.
3. Add three repository secrets: `ASC_KEY_ID`, `ASC_ISSUER_ID` and
   `ASC_KEY_P8` (the file's contents).

Until the secrets exist, the workflow skips the upload and succeeds.

## First run

1. Launch — the Broadcast tab shows the readiness checklist. The account
   key is created and stored in the Keychain on this first launch.
2. Tap **Get started**, pick a plan. Plans are priced live against Gnosis
   (`ant_storage_quote`), quoted in hours of 720p video (~1 GB/hour).
3. Pay. The payment screen is kept to what a payer needs: "Send
   exactly" and the amount rounded up to the cent (tap to copy), an
   "on Gnosis Chain" badge, an EIP-681 QR code a wallet fills address,
   chain and amount in from, the short address with one-tap copy, and
   "Waiting for payment". The ⓘ button has the rest: the full address,
   copy buttons, the Gnosis-only warning and what the one-time network
   deposit is for. The screen polls the chain and activates
   automatically the moment funds land — `ant_storage_buy_xdai` swaps
   the xBZZ shortfall, buys the batch, and deploys the chequebook.
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
   publisher's `"bzz"` format, the bee-js update shape, so any bee
   gateway resolves the channel from the feed manifest `POST /feeds`
   created at start. The app passes `format: "bzz"` on purpose: the web
   player the Share links open and the Android Watch tab only read that
   format. The Solar Punk stream format (#167: `POST /bytes`, bare
   segment references, v2 updates whose content is the playlist, read
   with `GET /feeds/{owner}/{topic}`) is a `format: "solar-punk"` away
   once those viewers read it.
   New channels get a text topic (`antstream-<uuid>`, passed as
   `topic_text`), which a Solar Punk catalog entry can name; a channel
   made before keeps its random topic bytes, and with them its link.
   Every broadcast also passes `"ladder": true` (#174): every ten
   seconds the node writes a small ladder time marker at an address
   computed from the clock (`ant_ladder_marker_id`), naming the feed's
   newest index, so a viewer joining hours in reads one marker and then
   one round of feed slots instead of walking the feed from 0. 360
   chunks an hour on the plan. (Under `"solar-punk"` the node would also
   write a one-rung master playlist for Solar Punk's players; not under
   `"bzz"`.)
3. The screen shows the **publish lag** (capture → the feed update that
   makes the segment playable) the whole time, plus how many segments
   the live-edge discipline dropped to hold it there.

Quality is picked on the live screen before going live and remembered:
**360p** (640×360, 600 kbit/s, the default), **540p** (960×540,
1000 kbit/s) or **720p** (1280×720, 1800 kbit/s), the same presets as
the Android app. The preset sets the encoder's frame size and bitrate
and the publisher's `bitrate_kbps`. Segments are 2 s and the publish
window is 4 deep. Window 4 is measured, not arbitrary — window 8
collapsed the connection layer in the stage-1 soaks — so it is not
exposed as a UI knob.

The live screen shows the camera as soon as it opens (#193), before Go
live: camera access is asked for then (the microphone at Go live), and
the capture session runs without a writer, so the shot can be framed,
the camera switched and the quality picked first. Going live adds the
microphone and a writer to that running session rather than restarting
the camera; a stop goes back to the preview, and leaving the screen
turns the camera off. The preview is drawn in the broadcast's own frame
(16:9 or 9:16), aspect-fit, so it shows what viewers get.

The live screen's interface turns with the device (the rest of the app
stays portrait on a phone; an `AppDelegate` orientation lock widens it
while the screen is open), and its controls re-lay out for landscape.
The broadcast's orientation is picked by holding the phone: portrait
(360×640, …) or landscape (640×360, …), following the phone until Go
live and then fixed for the whole broadcast. It follows the camera's
picture (the device as physically held), not the interface, so it is the
same with Portrait Orientation Lock on or in a narrow iPad window.
Turning the phone after that keeps the picture upright, with black bars
inside the fixed frame. A resumed broadcast (#175) keeps the orientation
it went out in (one before this choice existed was landscape).

Interruptions (incoming call, backgrounding, camera flip, orientation,
thermal downshift) finish the current segment cleanly and start a fresh
writer; the first segment after one is marked `#EXT-X-DISCONTINUITY` in
the playlist. The idle timer is held off and the audio session is
configured for recording for the broadcast's duration.

A dropped connection pauses a broadcast; it never ends it (#172). When
the device goes offline the app calls `ant_publisher_pause` with
`ant_suspend`: the camera keeps going, but its segments are withheld
rather than uploaded, and the live playlist just stops advancing (no
`#EXT-X-ENDLIST`), so viewers stay on the broadcast. Back online (and on
every return to the foreground) `ant_publisher_resume` carries it on in
the same feed and the same recording, behind one `#EXT-X-DISCONTINUITY`,
dated from the wall clock again. The broadcast ends only on Stop, or by
itself once a minute passes with nothing reaching the playlist
(`reap_ms` 60000, Solar Punk's `ORPHAN_REAP_MS`; counted from the last
listed segment or the first return after it, whichever is later); the
live screen then shows the report. A minute that runs out while offline
ends the broadcast when the network comes back rather than at once, so
the closing playlist and the recording go up over a live connection
instead of failing against a dead one.

Camera and broadcast paths are device-only — the simulator has no
capture device. `antstream-visual` covers the screen by running the same
encoder and publisher from a generated test pattern
(`-antstream-shot-live`), which is labelled as such on screen. Its other
launch arguments open the app straight onto one surface each:
`-antstream-shot-live-idle` (quality picker), `-antstream-shot-live-paused`
(with `-antstream-shot-live`: the broadcast paused as if offline, then
ending itself a minute on), `-antstream-shot-channel`
(the channel card, with sample state), `-antstream-shot-watch` (the
Watch tab on a sample channel), `-antstream-shot-getstarted-help` (the
payment ⓘ sheet), plus the Storage and Get Started hooks in
`RootView.swift`.

## Permanent channel

Every account has one channel: a random 32-byte feed topic created on
first use and kept in `UserDefaults` under the account address (a
restored account gets its own). The channel reference is the feed
manifest of owner + topic, so it is one link for all broadcasts.

* As soon as a storage plan exists and the gateway is up, the app
  creates that manifest itself (`POST /feeds/{owner}/{topic}` with
  `swarm-postage-batch-id`, retried every 15 s until it lands) and keeps
  the reference, so the link can be shared before the first broadcast.
* **Broadcast → Your channel** shows it, with **Share** (the web player
  links, for the Freedom browser and for any browser), copy, and
  **Watch**. The live screen's copy/share buttons send the same links.
* Every broadcast passes the channel's topic (`"topic_text"`, or
  `"topic"` for a channel older than text topics) to
  `ant_publisher_start` and no `"start_index"`: the node reads the feed
  head and continues after it (#173), so it never rewrites the indices
  that hold the last broadcast, even after a reinstall or from another
  device on the same account. The new broadcast numbers its segments on
  from the last one's (one `#EXT-X-DISCONTINUITY` at the seam, so a
  viewer's media sequence only ever increases), and its recording opens
  with the last one's, so the channel's head plays every broadcast.
  Reading the head needs the node to confirm a miss (an empty feed, or
  the slot after the head) with more than two peers, so **Go Live**
  waits for three connected peers; until the node has them no playlist
  or feed update would go up.

## Watch

The **Watch** tab plays a channel reference (pasted, or handed over by
the channel card's Watch button) with `AVPlayer` through the in-process
gateway. The reference is the channel's feed manifest; the tab reads the
feed's owner and topic out of it (`GET /bytes/<ref>`), then every
playlist load and reload goes through a resource-loader delegate that
plays either publisher format. It reads `GET /feeds/<owner>/<topic>`:
for a `"solar-punk"` channel that is the playlist, and each bare segment
reference is rewritten to `http://<gateway address>/bytes/<ref>`, the
same rewrite Solar Punk's viewers do; for a `"bzz"` channel (what the
app publishes) it is not, so the delegate reads `/bzz/<ref>/stream.m3u8`
instead and makes its `/bzz/...` segment paths absolute on the gateway.
Once a channel is known to be `"bzz"`, the tab's automatic reloads go
straight to `/bzz` without the feed lookup; playing it again by hand
checks the format afresh. The first read of every load joins from the newest
ladder marker when the channel has one (#174): the previous ten-second
period's marker, then the one before, read with `GET /soc`, and the feed
read anchored at the index it names (`GET /feeds/...?after=<index>`),
which the node then remembers for the `/bzz` read and every reload. No
marker (a channel published before markers, a device clock off by more
than a period) just means the feed is walked as before. The player follows the live edge. A channel that is not live
yet, an item that fails to load, and a live playlist that stops moving
(a broadcast that ended without closing its playlist) all reload the
channel after a short pause, with a plain message saying so; a closed
playlist that played to the end says the broadcast has ended. Loopback
HTTP is covered by the ATS exception in `Info.plist`.

## Differences from Android

* **No background broadcasting.** Android keeps a broadcast running
  from a foreground service. iOS stops camera capture when the app
  leaves the foreground, so a broadcast here is foreground-only: going
  to the background is handled as a capture interruption (the segment in
  progress is finished cleanly, and capture resumes with a
  discontinuity on return).
* **Key storage.** Android wraps the key with the Android Keystore; iOS
  uses the Keychain and the Secure Enclave (above).
