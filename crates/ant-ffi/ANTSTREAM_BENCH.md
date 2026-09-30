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
| a publisher that quietly drifts behind is not a pass | drop-oldest backlog + `#EXT-X-DISCONTINUITY` |
