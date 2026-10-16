# AntStream publisher throughput bench — issue #67, stage 1

The go/no-go gate for the AntStream publisher track: *which video
rendition can a phone broadcast directly to Swarm, on which network?*

The harness is the publisher loop minus the camera. A generator emits
HLS-shaped segments on a wall clock at a configured bitrate; each
finished segment is published with one `POST /bzz` against the
bee-shaped gateway — in-process (`ant_start_gateway`) on iOS, `antd` on
desktop. Stage 2 swaps the generator for the real capture pipeline and
adds the playlist + `POST /soc` feed writes; everything under the
generator is meant to survive that swap, which is why the measurement
lives in `crates/ant-ffi/src/bench.rs` and not in a throwaway script.

| piece | where |
|---|---|
| measurement core | `crates/ant-ffi/src/bench.rs` |
| C ABI | `ant_bench_start` / `ant_bench_progress` / `ant_bench_stop` (`crates/ant-ffi/include/ant.h`) |
| native driver | `crates/ant-ffi/examples/publish_bench.rs` |
| on-device driver | **Broadcast → Run bench** (`examples/ios-stream/AntStream/BenchView.swift`) |

## Go/no-go criteria (from #67)

1. **Sustained Mbit/s per network type** (Wi-Fi vs LTE), measured over
   **≥ 30 min** of continuous publishing, on **at least two device
   generations**.
2. A **rendition decision**: which rendition(s) phone-direct broadcast
   targets. The shortlist the bench offers is 360p (0.9), 540p (1.8),
   720p (3.4) and 1080p (6 Mbit/s).
3. **If LTE can't sustain a usable rendition**, #68 (relay fallback)
   escalates to the critical path.
4. Alongside the throughput figure: **peer-target sweep** (the desktop
   optimum is 400 peers at ~105 chunks/s ≈ 3.4 Mbit/s — find the phone
   optimum) and **battery/thermal** over the run.

A run "keeps up" when it measured something at all, every segment
published, *and* the final measured segment was still within three
segment durations of the live edge. That single predicate
(`BenchReport::keeps_up`) backs both the `sustained` field and the
**kept up** column below — a run that quietly falls further and further
behind is not a pass, even with zero errors.

The first clause is the one that bites in practice: stop a run before
`warmup_s` has elapsed and *nothing* is in the measured window, so every
figure folds to zero — including `lag_ms_final`, which would otherwise
sit comfortably inside the budget and pass a 0.00 Mbit/s run. Such a run
has no verdict rather than a false one: `measured_segments_total` is 0
and the **kept up** cell reads `n/a (no measured window)`.

## Results

### Measured

| environment | mode | target | sustained | chunks/s | publish p50/p95/max ms | final lag ms | kept up |
|---|---|---|---|---|---|---|---|
| Linux x86_64 8 vCPU / no network | pipeline | 3400 kbit/s | 3.40 Mbit/s | 105.5 | 80 / 102 / 109 | 108 | yes |
| Linux x86_64 8 vCPU / no network | pipeline | 20000 kbit/s | 20.00 Mbit/s | 616.0 | 342 / 453 / 474 | 329 | yes |
| Linux x86_64 8 vCPU / no network | pipeline | 60000 kbit/s | 60.00 Mbit/s | 1846.5 | 985 / 1188 / 1271 | 992 | yes |
| Linux x86_64 8 vCPU / no network | pipeline | 120000 kbit/s | 120.00 Mbit/s | 3691.9 | 1927 / 2217 / 2533 | 1959 | yes |
| Linux x86_64 / local `antd` 0.5.43, no usable batch | publish | 3400 kbit/s | 0.00 Mbit/s | 0.0 | — | — | **no** (`400 … batch … not usable`) |
| iOS simulator (iPhone18,1, macOS CI runner) | pipeline | 3400 kbit/s | 3.34 Mbit/s | 103.5 | 1391 / 2036 / — | 1243 | yes |

Environment: Intel Haswell 8 vCPU, Linux 6.14, `ant-ffi` 0.5.43 release
build, embedded node running alongside (100–114 BZZ peers), 2 s
segments, 4-deep publish window. Pipeline rows: 180 s (3400) / 80 s
(others), 20 s warm-up excluded. Simulator row: 60 s run, 5 s warm-up,
captured by the `antstream-visual` workflow (issue #70) driving the
app's own bench sheet.

**`pipeline` is not a broadcast number.** It measures only the
on-device half of publishing — splitting each segment into its Swarm
chunk tree, BMT-hashing it and signing a postage stamp per chunk — and
stops at the socket. It is the *ceiling imposed by the CPU*, i.e. the
number that says whether the device could keep up if the network were
free.

What the measured rows do establish:

* The pre-network pipeline scales linearly with segment size
  (0.85 MB → 80 ms, 5 MB → 342 ms, 15 MB → 985 ms, 30 MB → 1.93 s):
  **≈ 124 Mbit/s per concurrent publish path** on this CPU, ≈ 36× the
  720p target. Chunking + stamping is not the bottleneck on
  desktop-class hardware, and the same run on a phone (Broadcast → Run
  bench, "Publish to Swarm" off) tells us whether it is one there.
* 3.4 Mbit/s **is** 105 chunks/s, so the desktop `--target-peers`
  sweep in PLAN.md (Phase 7g: 105.3 chunks/s at 400 peers) and this
  issue's 3.4 Mbit/s target are the same number in different units —
  a desktop node with 400 peers sits exactly at 720p.
* The publish path is wired correctly end-to-end against a real
  `ant-gateway`: the `publish` row is a real `POST /bzz` reaching the
  real batch check, failing only for want of a funded batch.
* **The simulator row is the interesting one.** Same 850 KB segments,
  but 1391 ms per segment against the Linux box's 80 ms — ≈ 4.9 Mbit/s
  per publish path, only ~1.4× the 720p target. It still keeps up
  (four segments publish concurrently), but on Apple silicon the
  on-device pipeline is *not* free at 720p the way it is on desktop.
  Treat it as a floor — a CI runner's simulator is virtualised and
  shares a host — and take a real device pipeline row before reading
  anything into it. It is, however, the reason the phone's CPU ceiling
  is worth measuring rather than assumed, and it is measured from the
  app itself: the run below came out of the AntStream UI on a macOS
  runner via `antstream-visual`, i.e. through the whole
  `ant_bench_start` → `_progress` → `_stop` FFI round trip.

### Paid uploads on mainnet, 2026-10-12/13

Linux x86_64 (24 cores, datacenter link), `antd` built from the
`antstream-android` branch, `--target-peers 400`, a funded chequebook
(SWAP cheques on), depth-20 mutable batch, 2 s segments, window 4,
**fresh content per run** (`ANT_BENCH_SEED` randomised: with the default
seed every run at one bitrate re-uploads byte-identical segments, which
measures re-uploads, not a broadcast).

| build | target | sustained | publish p50/p95/max ms | final lag ms | kept up |
|---|---|---|---|---|---|
| before | 360p 600 kbit/s, 10 min | 0.59 Mbit/s | 2881 / 5892 / 10358 | 5156 | yes |
| before | 540p 1000 kbit/s, 10 min | 0.30 Mbit/s | 28297 / 31015 / 33234 | 522682 | **no** |
| before | 720p 1800 kbit/s, 10 min | 0.60 Mbit/s | 28379 / 31020 / 33531 | 481144 | **no** |
| fixes | 360p, 10 min | 0.60 Mbit/s | 2996 / 6633 / 12746 | 789 | yes |
| fixes | 540p, 10 min | 0.98 Mbit/s | 3341 / 7767 / 8781 | 3359 | yes |
| fixes + dedup | 720p, 10 min | 1.77 Mbit/s | 3455 / 8505 / 15546 | 5513 | yes |
| fixes + dedup | 360p, **30 min** | 0.60 Mbit/s | 3087 / 8164 / 15703 | 2889 | yes (899/899) |

Every run published every segment; every one of ~20 000 cheques was
accepted. What the "before" stalls were (~30 s a segment):

* **A neighbourhood without a reachable deep storer.** After the storage
  radius rose to 9, one depth-9 neighbourhood with a single node caught a
  chunk of nearly every segment, and each such chunk hunted for a deep
  storer for the full 12 shallow receipts × 2.5 s. Fixed by
  `ShallowNeighbourhoods`: a hunt that ends shallow marks the
  neighbourhood for 10 minutes.
* **Re-pushed redundancy replicas.** At redundancy level 1 each upload
  also pushes replica SOCs of its manifest nodes; one manifest node is
  identical in every segment, so its replica was re-pushed with every
  segment on the strict SOC path (patience ~30 s) and in-flight copies
  raced with different stamps ("overwriting chunk with newer timestamp",
  1071 refusals in 30 min). Fixed by `push_dedup` (coalesce in-flight
  duplicates, answer chunks stored in the last 10 minutes from memory)
  and the neighbourhood memory on the strict path.

Paid bandwidth cost: ~0.03 xBZZ/GB at 360p (the free pseudosettle
allowance carries most of it), ~0.17 xBZZ/GB at 720p. The 0.001 xBZZ
default chequebook deposit therefore covers only ~6 MB of 720p.

### The whole live loop on mainnet, 2026-10-16 (issue #212)

The rows above time one `POST /bzz` per segment. A viewer waits on
more: a segment is playable once it, every segment before it, its
playlist and the feed update naming that playlist have all landed.
`examples/live_bench.rs` runs the real publisher (`publisher::start`,
segment uploads, playlist uploads, `POST /soc` feed updates, ladder
markers, the recovery entry) from a generator, and measures what a
viewer sees: the gaps between landed feed updates, the publish lag
(capture to the feed update that made the segment playable), the
live-edge lag sampled every second, and drops.

Linux x86_64 (24 cores, datacenter link), the team's funded `antd`
(~115 peers, paying chequebook), a depth-17 mutable batch, 600 kbit/s
(the 360p rung) in 2 s segments, 30 min, fresh content and a fresh
feed per run. v0.5.72 and this fix ran the iOS config (`"bzz"`, ladder
markers, recovery entry); v0.5.68 has neither, so it ran plain `"bzz"`.

| build | dropped | publish lag p50/p95/max s | feed gap p50/p95/max s | gaps > 4 s / > 10 s | live-edge lag p95 s | segment upload p50/p95/max s | cheques xBZZ |
|---|---|---|---|---|---|---|---|
| v0.5.68 | 0 / 899 | 8.3 / 15.7 / 23.5 | 3.8 / 11.9 / 24.1 | 185 / 34 | 19.3 | 5.4 / 10.6 / 30.7 | 0.026 |
| v0.5.72 | 0 / 899 | 7.3 / 14.1 / 28.8 | 3.9 / 12.2 / 27.7 | 178 / 33 | 22.6 | 5.4 / 13.0 / 28.3 | 0.041 |
| fix, without the 1 s hedge | 0 / 899 | 1.3 / 3.9 / 17.9 | 2.0 / 5.0 / 19.0 | 91 / 3 | 6.4 | 0.7 / 3.1 / 5.2 | 0.023 |
| fix, 1 s hedge for the whole hunt | 0 / 899 | 0.9 / 1.6 / 8.8 | 2.0 / 2.9 / 10.3 | 16 / 1 | 3.3 | 0.5 / 1.1 / 1.3 | 0.046 |
| **fix (this code), 10 min** | **0 / 299** | **0.6 / 0.9 / 3.2** | **2.0 / 2.3 / 4.9** | **3 / 0** | **2.5** | **0.3 / 0.4 / 1.1** | 0.007 |

v0.5.68 and v0.5.72 behave the same: the pauses are not a regression
from the publisher series (PRs #177 to #186). The new per-stage
`timings` (v0.5.72 plus timings, 5 min) show where the time went:

| stage | p50 ms | p95 ms | max ms |
|---|---|---|---|
| segment upload | 5309 | 10597 | 28172 |
| head-of-line wait (landed, waiting for the segments before it) | 538 | 11335 | 23207 |
| playlist upload | 309 | 2932 | 7848 |
| recovery entry | 0 | 1 | 1 |
| feed update (`POST /soc`) | 93 | 172 | 2722 |
| whole feed update | 421 | 3045 | 7939 |
| ladder marker (off the critical path) | 94 | 2652 | 2654 |

So the feed paused behind slow *segments*. A paused feed resumed with
several segments at once, as the issue saw. Ladder markers and the
recovery entry were never on the path. A segment was slow for two
reasons, both in the push path:

* **Shallow receipts.** About 1 chunk in 10 drew a first receipt from
  a storer one bit short of its radius. The push then waited 2.5 s for
  a deeper peer to connect, which almost never happened in time, and
  retried. A ~45-push segment nearly always has such a chunk, so every
  segment paid at least 2.5 s, and a chunk that hunted several rounds
  held its segment, and the feed behind it, for 10-30 s.
* **Redundancy replicas after the data.** At redundancy level 1 the
  gateway pushed the dispersed-replica SOCs only after every data chunk
  had a receipt: a second serial phase with its own hunts. A 150 KB
  segment took 7.6 s on average (max 28 s) against 3.2 s at level 0.

What the fix changes:

* The gateway pushes an upload's replicas alongside its data chunks
  (all uploads): level 1 went from 7.6 s to 4.3 s mean.
* After a chunk's first shallow receipt the retry goes straight to the
  next-closest peer (all pushes). It landed deep as often without the
  wait (87-90%) as with it (84%). Level 1 dropped to 2.6 s mean.
* `ant-ack-when-stored: true` (ant's own header), sent by the publisher
  on segments and live windows: the gateway answers once every chunk
  has a receipt, shallow or deep. The hunt for a deep storer goes on in
  the background exactly as before. Until that answer such a push also
  hedges onto the next-closest peer after 1 s instead of bee's 5 s,
  because someone is waiting on it; the background hunt after it goes
  back to 5 s. At most 64 such hunts run in the background node-wide
  (`MAX_BACKGROUND_HUNTS`); with all taken, a push waits for placement
  as without the header. The feed update, the channel manifest, the
  ladder and the recording still answer once placed.

Placement held: hunts that ended without a deep storer were 1, 1, 0
and 4 per run (of ~40 000 pushes each). The last 20 segments of the
final run's recording all read back in full (1.2-2.3 s each) through a
fresh, unconnected node.

An earlier hedge costs money: a push still unanswered after 1 s also
goes to a second peer, and both receipts are paid. The first version
kept the 1 s hedge for the whole background hunt too, and its 30 min
run was the dearest at 0.046 xBZZ (0.013 in its first 10 min). With
the short hedge ending at the stored answer, the last run's 10 min
cost 0.007, about the rate of the fix without the early hedge (0.023
per 30 min). Cheque spend is noisy run to run, though: the two
baselines differ 1.6× on nearly the same push volume.

None of the runs here dropped a segment, though the issue's emulator
dropped 89 of 1713. Drops start when the in-flight window (4) and the
backlog (4) are both full, i.e. several segments in a row take about
8 s or more. Here segments peaked at 28-31 s but rarely in a row; with
the fix they peak at 1.3 s. The emulator run is the one to repeat on a
device.

Caveat: by the last runs the depth-17 test batch was full, and every
new chunk wrapped its bucket ("collision bucket full on mutable batch").
That costs nothing on the publish side measured here, but it means
chunks from early in a run can be evicted, so only the end of a
recording was read back.

### Owed

| environment | what's needed |
|---|---|
| iPhone (gen A) / Wi-Fi, 30 min | a device + a funded storage plan |
| iPhone (gen A) / Cellular, 30 min | ditto, on cellular |
| iPhone (gen B) / Wi-Fi + Cellular, 30 min | a second device generation |
| desktop `antd` / wired, peer-target sweep 100–400 | a funded batch |
| battery + thermal over ≥ 30 min | device only — `UIDevice.batteryLevel` is −1 on the simulator, and the app records "battery unavailable (simulator)" rather than a fictional 0 % |

**Why the network rows are not filled in from CI.** Every `POST /bzz`
needs a postage batch that is funded and unexpired on Gnosis; peers
reject stamps from anything else. The dev box this was built on has no
usable batch — both batches persisted in `~/.antd/postage` read back
from the PostageStamp contract as `depth 0, owner 0x0` (expired) — and
buying one spends real xBZZ, which is not something a CI job or an
automated agent should do unasked. Nothing else is missing: with a
batch id, the commands below produce every owed row.

## Running it

### Linux / CI — pre-network pipeline ceiling (no batch, no network)

```bash
ANT_BENCH_LABEL="Linux x86_64 8 vCPU / no network" \
ANT_BENCH_BITRATE_KBPS=3400 ANT_BENCH_DURATION_S=180 ANT_BENCH_WARMUP_S=20 \
  cargo run --release -p ant-ffi --example publish_bench
```

It prints the Markdown row (paste-ready for the table above) and the
full JSON report. Exit code is 0 only when the run kept up, so it can
gate a CI job directly.

### Desktop — real publish, with the peer-target sweep

The desktop node already has the peer-target knob (`antd
--target-peers N`, PLAN.md Phase 7f/7g), so sweeping it needs no code:
restart `antd` at each value and re-run the bench against it.

```bash
for peers in 100 200 300 400; do
  # restart antd with --target-peers $peers, wait for the peer set to fill, then:
  ANT_BENCH_LABEL="antd wired / target-peers $peers" \
  ANT_BENCH_GATEWAY=http://127.0.0.1:1633 ANT_BENCH_BATCH=0x<batch> \
  ANT_BENCH_DURATION_S=1800 \
    cargo run --release -p ant-ffi --example publish_bench
done
```

Add `ANT_FFI_DATA_DIR=<dir with a funded identity.json>` and drop
`ANT_BENCH_GATEWAY` to publish through an *embedded* node instead —
the shape the phone actually runs (the example then starts the node and
its gateway itself on `127.0.0.1:1733`).

### The live publisher end to end (issue #212)

```bash
ANT_LIVE_GATEWAY=http://127.0.0.1:1833 ANT_LIVE_BATCH=0x<batch> \
ANT_LIVE_BITRATE_KBPS=600 ANT_LIVE_DURATION_S=1800 \
ANT_LIVE_CONFIG='{"ladder":true,"recovery_path":"/path/recovery.json"}' \
ANT_LIVE_EVENTS=feed.csv \
  cargo run --release -p ant-ffi --example live_bench
```

It prints a Markdown row (published/pushed, dropped, failed, publish
lag, feed gaps, segment uploads) and a JSON summary with the report,
including its per-stage `timings`. Every minute it logs progress and,
on a publisher that has them, the stage timings so far. `ANT_LIVE_CONFIG`
is merged into the publisher config as is. The driver reads snapshot
and report as JSON, so the same file builds against older publishers
(copy it into a checkout of the tag).

### On a phone — the rows that gate the decision

The simulator cannot answer any of this: no cellular radio, no battery,
no thermal pressure. Run it on hardware:

1. Build and install `examples/ios-stream` on the device (a real
   signed build — never `CODE_SIGNING_ALLOWED=NO`, which strips
   entitlements and breaks the Keychain at runtime).
2. Get the Broadcast checklist fully green: connected, key secured,
   storage plan active, settlement ready, publishing endpoint up.
   Publishing needs all five — the bench gates on the same
   `isReadyToBroadcast` expression the checklist renders.
3. **Broadcast → Run bench.** Pick the rendition and *30 min
   (quotable)*, leave "Publish to Swarm" on, and start. Keep the app
   foregrounded — it holds the idle timer off for the run's duration,
   but iOS still suspends a backgrounded app.
4. When it finishes, **Copy results table** puts the Markdown row plus
   the run's context (segments, peers, iOS version, battery delta,
   thermal peak) on the clipboard. Paste it under **Measured** here and
   in issue #67.
5. Repeat on cellular (turn Wi-Fi off — the row's network label is
   taken from `NWPathMonitor`, not typed in, so it can't be mislabelled)
   and on a second device generation.

To sweep the peer target on a phone as well, `NodeConfig::target_peers`
has to reach the FFI first: `init_inner` currently always takes
`ant_p2p::DEFAULT_TARGET_PEERS` (100), and there is no
`ant_init_with_options` field for it. That is a small, well-scoped
follow-up — but note that at 100 peers the phone starts a factor below
the desktop's 400-peer optimum, so the first device numbers should be
read as a *floor*, not the phone's ceiling.

## Reading a report

| field | meaning |
|---|---|
| `sustained_mbit_s` | published bytes ÷ measured window. **The go/no-go number.** |
| `sustained_chunks_s` | the same rate in chunks, comparable to PLAN.md's `--target-peers` sweep |
| `publish_ms_p50/p95/max` | how long one `POST /bzz` took |
| `lag_ms_p50/p95/max/final` | how far behind live each segment landed (capture → published). Flat = keeping up; climbing = the uplink can't take the bitrate |
| `segments_failed` + `errors` | first few verbatim gateway errors — an unusable batch or a settlement stall shows up here, not as a slow number |
| `peers_min/max` | BZZ peer set during the run; a run that started cold is visible here |
| `measured_segments_total/ok` | the post-warm-up sample everything above is computed from; `0` = the run was stopped inside the warm-up and measured nothing |
| `sustained` | `measured_segments_ok > 0 && measured_segments_ok == measured_segments_total && lag_ms_final ≤ 3 × segment_ms` |

`warmup_s` (default 30 s) is excluded from the sustained figures *and
from the verdict*: the first segments pay peer-set warm-up and pushsync
skip-cache misses, so a segment that failed in there is reported (in
`segments_failed` / `errors`) but does not fail the run. Only failures
inside the measured window do.

## Scope

Stage 1 only. Not here, by design (they are stage 2 / 3 of #67):
playlist rebuilds, `POST /soc` feed updates, drop-oldest live-edge
discipline, the on-screen publish-lag indicator, foreground keep-alive
beyond the bench's own idle-timer hold, and the VOD finalize path.

### What stage 2 did to this file

Stage 2 (`crates/ant-ffi/src/publisher.rs`) is the same loop with the
generator replaced by the real camera pipeline, so the publish path
moved *out* of `bench.rs` and into `publisher.rs` as product code:
`Target`, the loopback HTTP client, `publish_bzz` and
`data_chunk_count` now live there and `bench.rs` imports them. The
bench therefore keeps measuring exactly the call a broadcast makes —
if the two ever drift, the numbers here stop describing the product.

The stage-1 findings that became stage-2 constants:

| finding | where it landed |
|---|---|
| window 4 sustains, window 8 collapses the connection layer | `DEFAULT_MAX_IN_FLIGHT = 4` |
| 360p @ 900 kbit/s, 2 s segments is the reachable rendition | `DEFAULT_BITRATE_KBPS = 900`, `DEFAULT_SEGMENT_MS = 2000` |
| lag budget = 3 × segment duration | `PublisherReport::kept_up` |
| a publisher that quietly drifts behind is not a pass | drop-oldest backlog + `#EXT-X-GAP` entries (#169) |
