# AntStream web player

Watches AntStream channels (published by `examples/android-stream` and
`examples/ios-stream`) in a browser: the Freedom desktop browser through
its built-in Swarm node, or any browser through a bee/ant gateway.

Static files only: `index.html`, `player.css`, `player.js`, `debug.js`
and a vendored hls.js 1.7.3 light build (`vendor/`, Apache-2.0).

## Published build

```
bzz://d36e69f559acbed1bbada8690f3231741bb8975fa7e9e764c43a27d095a088e2/#<channel>
https://api.gateway.ethswarm.org/bzz/d36e69f559acbed1bbada8690f3231741bb8975fa7e9e764c43a27d095a088e2/#<channel>
```

The Android app's Share button sends both links. Changing the player
means uploading it again (a new reference) and updating it in
`examples/android-stream/app/src/main/java/at/vibing/ant/stream/Channel.kt`
(`PLAYER_REF`) and `examples/ios-stream/AntStream/AntNode.swift`:

```sh
tar --format=ustar -cf player.tar index.html player.css player.js debug.js vendor/hls.light.min.js vendor/hls.js-LICENSE
curl -X POST http://127.0.0.1:1633/bzz -H 'Content-Type: application/x-tar' \
  -H 'swarm-collection: true' -H 'swarm-index-document: index.html' \
  -H "swarm-postage-batch-id: $BATCH" --data-binary @player.tar
```

## How it plays a channel

> This is the publisher's default `"bzz"` format, the one both apps
> publish. A channel published with the opt-in `format: "solar-punk"`
> (#167: each feed update *is* the playlist, read with
> `GET /feeds/{owner}/{topic}`, and segments are bare references read
> from `/bytes/<ref>`) has no `stream.m3u8` path, so this player can't
> play it yet; the iOS Watch tab
> (`examples/ios-stream/AntStream/WatchView.swift`) reads both.

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

## Keeping the buffer full

Every playlist reload makes the node resolve the channel feed on Swarm
(seconds), and every fresh segment takes seconds to find. hls.js alone
reloads and fetches one request at a time, so it hears of new segments
late, fetches them one by one, and the buffer runs dry. The player takes
over both loaders:

- **Overlapping feed reads.** While live, a new playlist read starts every
  1.5 s, up to 3 at once; hls.js's reload gets the newest playlist any of
  them returned. (Stale reads, older than one already seen, are dropped.)
- **Segment prefetch.** When hls.js asks for segment N, segments N+1…N+3
  start too, side by side; hls.js then gets them from memory or joins the
  running fetch.
- **Rebuffer gate.** After a start, seek or stall, playback waits until
  4 s are buffered instead of resuming on the first segment and stalling
  again a second later.
- It sits 4 segments behind the newest one, and only jumps forward when
  the playhead falls out of the playlist window.

Against `test/fake-gateway.mjs` with feed reads of 3–7 s and segment reads
of up to 3 s, this took 150 s of playback from 18 stalls (21 s stalled) to
5 (0.5 s); with 4–9 s feed reads and segment reads of up to 5 s, from
16 stalls (37 s) to 6 (2.6 s).

## Debug panel

The **Debug** button (or `?debug` in the URL) opens a panel with:
- the buffer ahead now, and over the last two minutes (stalls in red,
  rebuffer holds in amber);
- medians and p95s of segment and feed-read times;
- how often the live edge moved;
- every feed read and segment fetch with first-byte time, total time,
  size and outcome.

It also has the three knobs above (prefetch depth, overlapping feed
reads, rebuffer target; 0 turns each off), kept in localStorage. **Copy
log** and **Download** export everything as JSON for a bug report.

## Waiting and ended broadcasts

A channel that is not live yet (the feed answers 404) shows "Waiting for
the broadcast" and keeps retrying; a playlist with `#EXT-X-ENDLIST`
switches to recording playback.

## Testing without a live broadcast

`test/fake-gateway.mjs` replays segments recorded by the Android app's
self-test mode as a live channel, shaped exactly like the publisher's
playlist. `test/freedom-harness.cjs` loads the player from a `bzz:`
scheme registered the way Freedom registers it:

```sh
node test/fake-gateway.mjs <segments-dir> 1733 &   # START_DELAY_MS, SLOW_MS, PLAYLIST_MS, STALE_PCT, ENDLIST, ...
GATEWAY=http://127.0.0.1:1733 OUT=/tmp/player.png WAIT_S=40 \
  xvfb-run -a electron test/freedom-harness.cjs
```

The harness prints the player's state, including stalls counted from
outside the player and the debug log's markers. Any fMP4 HLS recording
works as `<segments-dir>`, e.g. one made with ffmpeg:

```sh
ffmpeg -f lavfi -i testsrc2=size=960x540:rate=30 -f lavfi -i sine -t 40 \
  -c:v libx264 -g 60 -sc_threshold 0 -c:a aac -f hls -hls_time 2 \
  -hls_segment_type fmp4 -hls_fmp4_init_filename init-1.mp4 \
  -hls_segment_filename 'seg-%d.m4s' -start_number 2 -hls_playlist_type vod p.m3u8
(echo "1 true 0"; awk -F'[:,]' '/EXTINF/{d=$2} /^seg-/{sub(/seg-/,"");sub(/.m4s/,"");printf "%s false %d\n",$0,d*1000}' p.m3u8) > durations.txt
```
