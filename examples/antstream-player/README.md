# AntStream web player

Watches AntStream channels (published by `examples/android-stream` and
`examples/ios-stream`) in a browser: the Freedom desktop browser through
its built-in Swarm node, or any browser through a bee/ant gateway.

Static files only: `index.html`, `player.css`, `player.js` and a vendored
hls.js 1.7.3 light build (`vendor/`, Apache-2.0).

## Published build

```
bzz://664f9718d0aab34ef463c83e52884eb29fd532cab008eacfcf93241ed7f379fe/#<channel>
https://api.gateway.ethswarm.org/bzz/664f9718d0aab34ef463c83e52884eb29fd532cab008eacfcf93241ed7f379fe/#<channel>
```

The Android app's Share button sends both links. Changing the player
means uploading it again (a new reference) and updating `PLAYER_REF` in
`examples/android-stream/app/src/main/java/at/vibing/ant/stream/ui/LiveScreen.kt`:

```sh
tar --format=ustar -cf player.tar index.html player.css player.js vendor/hls.light.min.js vendor/hls.js-LICENSE
curl -X POST http://127.0.0.1:1633/bzz -H 'Content-Type: application/x-tar' \
  -H 'swarm-collection: true' -H 'swarm-index-document: index.html' \
  -H "swarm-postage-batch-id: $BATCH" --data-binary @player.tar
```

## How it plays a channel

A channel reference is a feed manifest; `<channel>/stream.m3u8` resolves
the feed to the newest playlist on every reload, so hls.js follows the
live edge on its own. Two details make it work under `bzz://`:

- The publisher lists segments by gateway-root path
  (`/bzz/<ref>/seg-N.m4s`). Under a `bzz://<channel>/` origin that path
  would resolve inside the channel, so a custom hls.js loader maps every
  `…/bzz/<ref>/<rest>` URL to `bzz://<ref>/<rest>` (or to the gateway's
  own root, sub-path included, over HTTP).
- Freedom forwards no `Cache-Control` request header to its node, so
  playlist reloads carry a `t=` query to defeat caching.

A channel that is not live yet (the feed answers 404) shows "Waiting for
the broadcast" and keeps retrying; a playlist with `#EXT-X-ENDLIST`
switches to recording playback.

## Testing without a live broadcast

`test/fake-gateway.mjs` replays segments recorded by the Android app's
self-test mode as a live channel, shaped exactly like the publisher's
playlist. `test/freedom-harness.cjs` loads the player from a `bzz:`
scheme registered the way Freedom registers it:

```sh
node test/fake-gateway.mjs <segments-dir> 1733 &          # START_DELAY_MS, SLOW_MS, ENDLIST
GATEWAY=http://127.0.0.1:1733 OUT=/tmp/player.png WAIT_S=40 \
  xvfb-run -a electron test/freedom-harness.cjs
```
