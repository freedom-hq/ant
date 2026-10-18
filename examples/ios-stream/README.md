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
| `StreamInfoView.swift` | The stream info editor (#220): title, description, thumbnail, scheduled start and Announce. |
| `WatchView.swift` | The Watch tab: `AVPlayer` on a solar-punk channel from the in-process gateway (`GET /feeds`, segment references rewritten to `/bytes/`), with automatic retry, and the channel's catalog entry (title, description). |
| `ChannelAddress.swift` | A channel as owner + topic: link parsing, the links Share sends, the playlist rewrite, the publisher config, the stream info and reading a catalog entry. Foundation only; `ci/channel-address-tests.swift` tests it in CI. |
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

After the upload, the job waits for Apple to process the build (up to
45 minutes) and attaches it to the app's editable iOS App Store version
("Prepare for Submission", one rejected back to you, or Invalid
Binary), the same as
"Add Build" in App Store Connect; the app's icon there comes from that
build. It never submits anything for review. If no version is editable
(one is in review), the build is left unattached and the job says so in
a notice. AntDrive's workflow does the same through the shared
[`.github/scripts/asc-attach-build.py`](../../.github/scripts/asc-attach-build.py)
(standard library plus the system `openssl`, nothing installed from
PyPI; offline tests: `python3 .github/scripts/test_asc_attach_build.py`).
Runs of one workflow are queued, never overlapped, so an older build
can't overwrite a newer one on the version. A failed attach shows as an
error annotation but leaves the run green, since the upload itself
already landed.

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
2. `ant_publisher_push_segment` queues each one. Every broadcast goes
   out in the publisher's Solar Punk format (`format: "solar-punk"`,
   #167, the only format since #217): per segment the node does
   `POST /bytes`, rebuilds the rolling HLS playlist listing bare segment
   references, uploads it and writes it as the channel's next sequence
   feed update with `POST /soc`, a v2 update whose content *is* the
   playlist, so `GET /feeds/{owner}/{topic}` on any bee or ant gateway
   answers with it. Every broadcast also passes `"ladder": true` (#174):
   every ten seconds the node writes a small ladder time marker at an
   address computed from the clock (`ant_ladder_marker_id`), naming the
   feed's newest index, and a one-rung master playlist naming the
   channel as `swarm://<owner>/antstream`, so a viewer joining hours in
   reads one marker and then one round of feed slots instead of walking
   the feed from 0. 360 chunks an hour on the plan.
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
(16:9 or 9:16), aspect-fit, so it shows what viewers get. With
Portrait Orientation Lock on and the phone held sideways the preview
draws the picture turned against the (portrait) interface, so the frame
is turned with it and still has the broadcast's shape as held.

The live screen's interface turns with the device (the rest of the app
stays portrait on a phone; an `AppDelegate` orientation lock widens it
while the screen is open), and its controls re-lay out for landscape.
The broadcast's orientation is picked by holding the phone: portrait
(360×640, …) or landscape (640×360, …), following the phone until Go
live and then fixed for the whole broadcast. It follows the camera's
picture (the device as physically held), not the interface, so it is the
same with Portrait Orientation Lock on or in a narrow iPad window.
Either way the frames are encoded upright at that size with no track
rotation (#194), so no player has to turn or unsqueeze them. Turning the
phone after that keeps the picture upright, with black bars inside the
fixed frame. A resumed broadcast (#175) keeps the orientation
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
(the channel card, with sample state), `-antstream-shot-info` (the
stream info card with sample info; `-antstream-shot-info-edit` opens its
editor), `-antstream-shot-watch` (the
Watch tab on a sample channel; `-antstream-shot-watch-info` adds a
sample catalog entry), `-antstream-shot-getstarted-help` (the
payment ⓘ sheet), plus the Storage and Get Started hooks in
`RootView.swift`.

## Permanent channel

Every account has one channel: an **owner and a topic**, the account
address and the topic text `antstream` (`ChannelAddress.swift`), the
same for every broadcast (a restored account is another owner, so
another channel). There is nothing to create before sharing it. The
channel's name is only a label (the broadcast's notes and the UI), never
part of the topic, so renaming it never forks the channel.

* **Broadcast → Your channel** shows it with its links, each copied on
  a tap, **Share** and **Watch**:
  - the web player, `bzz://<player>/#0x<owner>/antstream` in the
    Freedom browser, and its gateway twin
    `https://api.gateway.ethswarm.org/bzz/<player>/#0x<owner>/antstream`
    for any browser (`<player>` is `ChannelAddress.playerReference`, the
    player's feed manifest, so the links outlive player updates);
  - `swarm://<owner>/antstream`, the URI Solar Punk's players open a
    channel by.

  The live screen's copy/share buttons send the same links.
* Every broadcast passes `"topic_text": "antstream"` to
  `ant_publisher_start` and no `"topic"` or `"start_index"`: the node
  reads the feed head and continues after it (#173), so it never
  rewrites the indices that hold the last broadcast, even after a
  reinstall or from another device on the same account. The new
  broadcast numbers its segments on from the last one's (one
  `#EXT-X-DISCONTINUITY` at the seam, so a viewer's media sequence only
  ever increases), and its recording opens with the last one's, so the
  channel's head plays every broadcast. Reading the head needs the node
  to confirm a miss (an empty feed, or the slot after the head) with
  more than two peers, so **Go Live** waits for three connected peers;
  until the node has them no playlist or feed update would go up.
* An unfinished broadcast (#175) is resumed or ended on the feed and in
  the format its recovery entry names. One left by a build from before
  #217 (a `"bzz"` broadcast on the old per-install topic) can only be
  ended, in its own format: carrying it on would keep writing a format
  the viewers no longer read.

## Stream info

The Broadcast tab's **Stream info** card (and the stream info row on
the live screen, before going live and while live) opens an editor for
what players list the channel with (#220): a **title** (required; it
starts as the channel's name, and a title left at the name follows it),
an optional **description**, a **thumbnail** and an optional **scheduled
start**. It is remembered per account (`UserDefaults`, keyed by the
account address; the thumbnail image as
`Application Support/antstream/stream-thumbnail-<owner>.jpg`) for the
next broadcast.

Every broadcast passes it as the publisher config's `"info"`, so the node
keeps the account's catalog feed (topic text `antstream-catalog`)
holding the channel's Solar Punk catalog entry: "live" while it runs,
"vod" after. Saving while live calls `ant_publisher_set_info`, which
rewrites the entry at once. **End broadcast** on a killed broadcast
passes the info too, so its "live" entry is replaced with "vod". The live screen shows the progress's
`catalog_error`, if any, under the counters; a catalog write that fails
never stops the broadcast.

The thumbnail is a frame from the camera, taken five seconds after Go
live (`CaptureEngine.thumbnailJPEG()`: the next video frame, upright as
encoded, scaled to 640 px on its long side and saved as a JPEG of a few
tens of KB), unless the broadcaster chose a photo (`PhotosPicker`,
scaled the same way). Either one goes up with
`ant_stream_thumbnail_upload` through the in-process gateway, stamped
with the plan's batch, and its reference goes into the info. A chosen
photo goes up when it is saved if there is a plan, else at the next Go
live; a captured frame replaces the last one on every broadcast.

With a scheduled start, **Announce** (not while live) calls
`ant_stream_catalog_publish`: the entry becomes "scheduled" with the
start time, so players list the broadcast as upcoming; the library's
message shows if it fails. The Broadcast tab's card shows the announced
start. Going live replaces the entry with the live one and clears the
schedule.

## Watch

The **Watch** tab plays a channel with `AVPlayer` through the
in-process gateway. The field starts on the account's own channel and
takes `0x<owner>/<topic>`, `swarm://<owner>/<topic>` or a web player
link (an owner alone means its `antstream` topic; quotes around a pasted
link are dropped, as in the web player); the channel card's
Watch button hands its own over. Channels in the Solar Punk format only:
every playlist load and reload goes through a resource-loader delegate
that reads `GET /feeds/<owner>/<topic>`, whose body is the playlist, and
rewrites each bare segment and `#EXT-X-MAP` reference to
`http://<gateway address>/bytes/<ref>`, the same rewrite Solar Punk's
viewers do. A feed whose update is not a playlist (a channel in the
older `"bzz"` format) fails to load. The first read of every load joins
from the newest ladder marker when the channel has one (#174): the
previous ten-second period's marker, then the one before, read with
`GET /soc`, and the feed read anchored at the index it names
(`GET /feeds/...?after=<index>`), which the node then remembers for
every reload. No marker (a device clock off by more than a period) just
means the feed is walked as before. A finished broadcast's recording
plays from the start. The player follows the live edge. A channel that is not live
yet, an item that fails to load, and a live playlist that stops moving
(a broadcast that ended without closing its playlist) all reload the
channel after a short pause, with a plain message saying so; a closed
playlist that played to the end says the broadcast has ended. Loopback
HTTP is covered by the ATS exception in `Info.plist`.

Over the player the tab shows the channel's title and description (and
"Starts …" for an announced broadcast) from its owner's catalog feed
(`GET /feeds/<owner>/<keccak256("antstream-catalog")>`, whose body is the
catalog's JSON array; #220). It is read beside playback, never in front
of it, when the channel changes and then once a minute; a result for a
channel no longer shown is dropped, and a missing or unreadable catalog
(the JSON is untrusted: wrong types and unknown fields are skipped)
shows nothing extra.

## Differences from Android

* **No background broadcasting.** Android keeps a broadcast running
  from a foreground service. iOS stops camera capture when the app
  leaves the foreground, so a broadcast here is foreground-only: going
  to the background is handled as a capture interruption (the segment in
  progress is finished cleanly, and capture resumes with a
  discontinuity on return).
* **Key storage.** Android wraps the key with the Android Keystore; iOS
  uses the Keychain and the Secure Enclave (above).
