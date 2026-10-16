# AntStream web player

Watches AntStream channels (published by `examples/android-stream` and
`examples/ios-stream`) in a browser: the Freedom desktop browser through
its built-in Swarm node, or any browser through a bee/ant gateway.

Static files only: `index.html`, `player.css`, `player.js`, `debug.js`,
`keccak.js` (the Keccak-256 behind topics and feed addresses) and a vendored hls.js 1.7.3 light build (`vendor/`, Apache-2.0).

## Published build

```
bzz://b268072928bd99d637992eb54c2a826b83affa36f61cf495297541ac42857c35/#<owner>/<topic>
https://api.gateway.ethswarm.org/bzz/b268072928bd99d637992eb54c2a826b83affa36f61cf495297541ac42857c35/#<owner>/<topic>
```

This is a **feed manifest**: the address stays the same and always serves
the newest build. The Android app's Share button sends both links, and
the apps (`PLAYER_REF` in
`examples/android-stream/app/src/main/java/at/vibing/ant/stream/Channel.kt`,
`playerReference` in `examples/ios-stream/AntStream/AntNode.swift`) never
need a new reference for a player update.

The page footer and the debug panel show which build is being served:
`Player build <n> · <commit> · <build time>`, where `<n>` is the feed index
the build was published at.

### Publishing a new build

```sh
cd examples/antstream-player/deploy
npm install
node deploy-feed.mjs --gateway http://127.0.0.1:1633 --stamp "$BATCH" \
  --key /path/to/antstream-player-feed.key      # --dry-run to stage only
```

`deploy-feed.mjs` uploads the player with a generated `version.js` (build
number, commit, build time) and writes it as the next update of the feed
(topic `antstream-player`, owner `0x0b882aea32aa77a804ad6c506a5024ed56c746d1`).
The key only signs feed updates: it holds no funds and has no rights on any
chain. The stamp pays for the upload on the gateway's node.

A page loaded from a checkout (no `version.js`) says "development build".

## How it plays a channel

The player plays channels in the publisher's `"solar-punk"` format only
(issue #216). Channels published with the older `"bzz"` format
(`/bzz/<channel>/stream.m3u8`) do not play any more.

A **channel is an owner and a topic text**:
`bzz://<player>/#<owner>/<topic>` or
`<gateway>/bzz/<player>/#<owner>/<topic>`, the owner as `0x` + 40 hex, the
topic URL-encoded. The feed topic is `keccak256(utf8(text))`, bee-js
`Topic.fromString`, `topic_from_string` in
`crates/ant-ffi/src/publisher.rs`. There is no default topic: each
AntStream channel has its own (the iOS app's `antstream-<uuid>`, the
publisher's own default `antstream/<channel>`), so a link always names
one. A channel whose topic is random bytes with no text (the Android
app's, an iOS channel created before text topics) is named by the raw
topic, 64 hex digits (`#0x<owner>/0x<topic hex>`, `0x` optional); a
topic of exactly 64 hex digits is always read that way, never as text.
The channel field also takes Solar Punk's `swarm://<owner>/<topic>`
and any player link.

Every feed update **is** the playlist (a v2 update wrapping the
playlist's root chunk), and the playlist lists segments and the
`#EXT-X-MAP` as bare 64-hex references to raw bytes.

| | In Freedom (`bzz:` page) | Over HTTP (gateway page) |
|---|---|---|
| Feed head | `window.swarm` `swarm_readFeedEntry { topic, owner }` | `GET /feeds/{owner}/{topic}` (`swarm-feed-index` names the index) |
| Feed slot | `swarm_readSingleOwnerChunk { owner, identifier }`; a body longer than one chunk (a recording) `swarm_readFeedEntry { topic, owner, index }` | `GET /soc/{owner}/{identifier}` |
| Ladder marker | `swarm_readSingleOwnerChunk` | `GET /soc/{owner}/{identifier}` |
| Segments, init | `bzz://<ref>` (Freedom's node answers raw bytes) | `GET /bytes/<ref>` |

A slot's identifier is `keccak256(topic ‖ index_be8)`, a marker's
`keccak256("ladder-marker" ‖ group topic ‖ period_be8)` for the group
`antstream-ladder-<topic hex>` (`ladder_marker_identifier`,
`ladder_group_text`).

- **Joining.** The head lookup probes the feed for its newest index:
  seconds, and sometimes stale. So it is asked once, together with the
  ladder markers of this ten-second period and the last one. A channel
  published with markers names its newest index there and the player
  reads that slot directly, without waiting for the head. Whichever
  answers first is where it follows from; from a stale head or a marker a
  few updates old it reads on slot by slot until one is not written yet.
- **Following.** From then on every read names an explicit slot, the one
  after the newest read, which a node answers at once with the update or
  "not written yet", as Solar Punk's `feedFollow.ts` does.
- **A new broadcast** on the channel continues the same feed (#173): the
  playlist guard sees a broadcast id it hasn't seen and the player starts
  over on it, from the slot it was found at.
- **The `window.swarm` budget.** Freedom serves permission-free reads from
  120 requests / 512 KiB per origin per minute. The player keeps to 100
  requests / 400 KiB a minute on its own (about 45 a minute while live,
  12 once a broadcast ended), and a `rate_limited` refusal pauses its
  feed reads for 5 s, doubling up to a minute. Segments are `bzz://`
  reads and not counted. A recording's size is known from its slot's
  root chunk before it is read, and the read waits until the budget has
  room for all of it. A recording over the budget (several hours of
  broadcast) can't be read without a connection grant: a live viewer
  steps past its slot without reading it (the next broadcast continues
  the feed after it), and one who opens the channel after such a
  broadcast ended can't join through the head (it returns the whole
  recording) and should use an HTTP gateway.

## Keeping the buffer full

Every fresh segment takes seconds to find on Swarm, and hls.js alone
reloads and fetches one request at a time, so it hears of new segments
late, fetches them one by one, and the buffer runs dry. The player takes
over both loaders:

- **Overlapping feed reads.** While live, a new slot read starts every
  1.5 s (every 5 s once the playlist stopped moving for 20 s), up to 3 at
  once on consecutive slots; hls.js's reload gets the newest playlist any
  of them returned. (Stale reads, older than one already seen, are
  dropped.)
- **Segment prefetch.** When hls.js asks for segment N, segments N+1…N+3
  start too, side by side; hls.js then gets them from memory or joins the
  running fetch.
- **Rebuffer gate.** After a start, seek or stall, playback waits until
  4 s are buffered instead of resuming on the first segment and stalling
  again a second later.
- It sits 4 segments behind the newest one, and only jumps forward when
  it falls more than ten target durations behind.

## Portrait streams

The stage takes the picture's own aspect from `videoWidth`/`videoHeight`
(16:9 until the first frame), at most 80vh tall, with `object-fit:
contain`. `test/portrait-test.mjs` checks it in Freedom's Electron at a
desktop and a phone-sized window, for a portrait stream (480×640, what the
apps publish), an older portrait broadcast stored sideways with a rotate-90
track matrix, and a landscape one: the element box must have the display
aspect and a disc drawn in every frame must come out round. Each run then
opens a channel that never loads, and the stage must go back to 16:9.

```sh
ELECTRON=<path to electron> node test/portrait-test.mjs   # needs ffmpeg, xvfb-run
```

## Debug panel

The **Debug** button (or `?debug` in the URL) opens a panel with:
- the buffer ahead now, and over the last two minutes (stalls in red,
  rebuffer holds in amber);
- medians and p95s of segment and feed-read times;
- how often the live edge moved;
- the feed index followed, how the channel was joined (head, marker), and
  in Freedom the read budget used in the last minute;
- every feed read (head, slot or marker, its index, `HTTP` or
  `window.swarm`, and how it was answered: 200, 404 "not written yet",
  429 rate limited, or the error) and segment fetch with first-byte time,
  total time, size and outcome.

It also has the three knobs above (prefetch depth, overlapping feed
reads, rebuffer target; 0 turns each off), kept in localStorage. **Copy
log** and **Download** export everything as JSON for a bug report.

## Waiting and ended broadcasts

A channel that is not live yet (the feed answers 404) shows "Waiting for
the broadcast" and keeps retrying; a playlist with `#EXT-X-ENDLIST`
switches to recording playback. A viewer who was watching live plays on
from where they were to the end, whether the reload lands on the
broadcast's closing playlist (its last minutes) or straight on the
recording; one who opens the channel after it ended gets the recording
the publisher leaves as the final feed update, the whole broadcast from
its first segment. After the end the player reads the slot after the
recording every 5 s, where the channel's next broadcast will write, and
switches to it when it starts.

## Testing without a live broadcast

`test/fake-gateway.mjs` replays segments recorded by the Android app's
self-test mode as a live solar-punk channel (`/feeds`, `/soc`, `/bytes`,
ladder markers with `LADDER=1`), shaped like the publisher's playlists:
gaps, a fresh `#EXT-X-MAP` behind a discontinuity, a closing window and
the recording, a new broadcast continuing the feed (`RESTART_MS`), stale
and slow head lookups; its header lists every setting.
`test/freedom-harness.cjs` loads the player from a `bzz:` scheme
registered the way Freedom registers it, with a stand-in for
`window.swarm` that answers from the gateway with Freedom's result shapes,
error reasons and read budget:

```sh
node test/fake-gateway.mjs <segments-dir> 1733 &   # LADDER, ENDLIST, RESTART_MS, GAP_EVERY, PLAYLIST_MS, STALE_PCT, ...
GATEWAY=http://127.0.0.1:1733 OUT=$TMPDIR/player.png WAIT_S=40 \
  xvfb-run -a electron test/freedom-harness.cjs
```

For the HTTP path, open the fake gateway's own page instead
(`NO_SWARM=1 URL=http://127.0.0.1:1733/bzz/<64 × a>/#<owner>/<topic>`).
A real channel plays the same way against a real node:
`GATEWAY=http://127.0.0.1:1633 CHANNEL=0x<owner>/<topic>` for the Freedom
path, or `UPSTREAM=http://127.0.0.1:1633 node test/fake-gateway.mjs - 1733`
to serve the player in front of the node for the HTTP path.

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
