//! `AntStream` publisher throughput benchmark — issue #67 stage 1.
//!
//! The synthetic-segment benchmark *is* the publisher loop minus the
//! camera: a generator produces HLS-shaped segments on a wall clock at a
//! configured bitrate, and each finished segment is published with one
//! `POST /bzz` against the bee-shaped gateway (`ant_start_gateway`
//! in-process on iOS, `antd` on desktop). Stage 2 replaces the generator
//! with the real capture pipeline and adds the playlist / feed writes;
//! everything below the generator is meant to survive that swap, which
//! is why the measurement harness lives in `ant-ffi` rather than in a
//! throwaway script.
//!
//! Deliberately **not** in stage 1 (they belong to stage 2 of #67):
//! playlist rebuilds, `POST /soc` feed updates, drop-oldest live-edge
//! discipline, and the on-screen lag indicator. What is here is the
//! measurement those decisions need: sustained Mbit/s, chunks/s,
//! per-segment publish latency, and publish *lag* (how far behind the
//! live edge the uploader has fallen), sampled over a long run.
//!
//! Two modes, sharing one loop:
//!
//! * [`Mode::Publish`] — real `POST /bzz` per segment. Needs a usable
//!   postage batch; this is the number the go/no-go gate is about.
//! * [`Mode::Pipeline`] — no network: split each segment into its chunk
//!   tree and sign a postage stamp per chunk, i.e. every CPU cost the
//!   publisher pays before a byte reaches the socket. This is the
//!   pre-network ceiling, and it runs anywhere (CI, a phone in airplane
//!   mode, a box with no funded batch).
//!
//! The core is transport-agnostic about *where* it runs: the native
//! example (`cargo run -p ant-ffi --example publish_bench`) and the iOS
//! app both drive [`start`] through the same config, so the numbers in
//! `crates/ant-ffi/ANTSTREAM_BENCH.md` are comparable across
//! environments.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ant_control::StatusSnapshot;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{watch, Semaphore};

/// Swarm chunk payload size. Segment byte counts are converted to chunk
/// counts with this so `chunks/s` is comparable to the desktop
/// `--target-peers` sweep in PLAN.md (Phase 7g: 105.3 chunks/s at 400
/// peers).
const CHUNK_SIZE: u64 = 4096;

/// Branching factor of the Swarm chunk tree (128 × 32-byte references
/// per intermediate chunk). Mirrors `ant_retrieval::BRANCHES`; kept
/// local so [`data_chunk_count`] stays a pure function usable from the
/// unit tests without pulling the splitter in.
const BRANCHES: u64 = 128;

/// Per-segment publish deadline. A segment that has not landed within
/// this long is recorded as a failure rather than stalling the run: at
/// live-edge bitrates anything past ~1 min is already unusable, and the
/// bench must keep sampling so the report shows *where* it broke.
const PUBLISH_TIMEOUT: Duration = Duration::from_mins(1);

/// How often the run samples the node's peer count (and, on hosts that
/// supply one, the battery/thermal note). Cheap watch-channel read.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// Default HLS segment duration. 2 s is what the #65 capture pipeline
/// targets (Apple's recommendation for low-latency HLS is 2–6 s).
const DEFAULT_SEGMENT_MS: u32 = 2000;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// One benchmark run. Deserialised straight from the host's JSON so the
/// Swift app and the native CLI configure the same knobs.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchConfig {
    /// Free-form environment label that ends up in the results table —
    /// e.g. `"iPhone 15 Pro / LTE"` or `"Linux x86_64 / wired"`. The
    /// numbers are meaningless without it, so it is required.
    pub label: String,
    /// Target video bitrate. Segment size is `bitrate × segment_ms`.
    #[serde(default = "default_bitrate")]
    pub bitrate_kbps: u32,
    /// Segment duration, i.e. the generator's wall-clock cadence.
    #[serde(default = "default_segment_ms")]
    pub segment_ms: u32,
    /// Total run length. The issue asks for ≥ 30 min (1800) for a
    /// quotable number; shorter runs are fine for smoke-testing and are
    /// flagged as such in the report.
    #[serde(default = "default_duration_s")]
    pub duration_s: u64,
    /// Leading slice of the run excluded from the sustained figures:
    /// the first segments pay peer-set warm-up and pushsync skip-cache
    /// misses, which flatter/penalise the average depending on where
    /// you stop.
    #[serde(default = "default_warmup_s")]
    pub warmup_s: u64,
    /// How many segments may be in flight at once. The publisher is not
    /// allowed to run unbounded — that would hide the backlog the lag
    /// metric exists to expose.
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    /// Gateway base URL, `http://host:port` (loopback in both the iOS
    /// and the embedded-node case). No TLS: this is a localhost hop.
    #[serde(default = "default_gateway")]
    pub gateway: String,
    /// Postage batch id (hex, `0x`-optional) the segments are stamped
    /// with. Empty selects [`Mode::Pipeline`] — no network, no batch.
    #[serde(default)]
    pub batch_id: String,
    /// Deterministic segment payload seed, so two runs push identical
    /// bytes (and therefore identical chunk counts) — but see
    /// [`SegmentGenerator`]: the payload is unique *per segment* so no
    /// upload is ever served from a cache hit.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Host-supplied context for the results table (device model, iOS
    /// version, battery/thermal sampling). Free-form; echoed verbatim.
    #[serde(default)]
    pub notes: String,
}

const fn default_bitrate() -> u32 {
    3400
}
const fn default_segment_ms() -> u32 {
    DEFAULT_SEGMENT_MS
}
const fn default_duration_s() -> u64 {
    1800
}
const fn default_warmup_s() -> u64 {
    30
}
const fn default_max_in_flight() -> usize {
    4
}
fn default_gateway() -> String {
    "http://127.0.0.1:1633".to_string()
}
const fn default_seed() -> u64 {
    0x414e_5453_5452_4d31
}

impl BenchConfig {
    /// Bytes per generated segment: `bitrate_kbps × segment_ms / 8`.
    #[must_use]
    pub const fn segment_bytes(&self) -> u64 {
        (self.bitrate_kbps as u64) * 1000 * (self.segment_ms as u64) / 8 / 1000
    }

    /// Which of the two loops this config selects.
    #[must_use]
    pub fn mode(&self) -> Mode {
        if self.batch_id.trim().is_empty() {
            Mode::Pipeline
        } else {
            Mode::Publish
        }
    }

    fn validate(&self) -> Result<(), BenchError> {
        if self.label.trim().is_empty() {
            return Err(BenchError::Config(
                "label is required: an unlabelled throughput number can't go in the table".into(),
            ));
        }
        if self.bitrate_kbps == 0 {
            return Err(BenchError::Config("bitrate_kbps must be > 0".into()));
        }
        if self.segment_ms == 0 {
            return Err(BenchError::Config("segment_ms must be > 0".into()));
        }
        if self.duration_s == 0 {
            return Err(BenchError::Config("duration_s must be > 0".into()));
        }
        if self.max_in_flight == 0 {
            return Err(BenchError::Config("max_in_flight must be > 0".into()));
        }
        if self.warmup_s >= self.duration_s {
            return Err(BenchError::Config(
                "warmup_s must be shorter than duration_s".into(),
            ));
        }
        Ok(())
    }
}

/// Which publish path a run exercises.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// `POST /bzz` per segment against a real gateway.
    Publish,
    /// Local chunking + stamping only; no socket, no batch.
    Pipeline,
}

impl Mode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Pipeline => "pipeline",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BenchError {
    #[error("{0}")]
    Config(String),
    #[error("{0}")]
    Setup(String),
}

// ---------------------------------------------------------------------------
// Segment generation
// ---------------------------------------------------------------------------

/// Synthetic segment payloads.
///
/// Real encoded video is incompressible and never repeats, so every
/// segment must hash to a fresh chunk set — otherwise the node's
/// push-skip cache would short-circuit the upload and the bench would
/// measure a cache, not the network. The generator is a seeded
/// `SplitMix64` keyed by `(seed, sequence)`, which gives both properties
/// plus reproducibility across runs and platforms.
pub struct SegmentGenerator {
    seed: u64,
    bytes: usize,
}

impl SegmentGenerator {
    #[must_use]
    pub const fn new(seed: u64, bytes: u64) -> Self {
        Self {
            seed,
            bytes: bytes as usize,
        }
    }

    /// Build segment `seq`. Deterministic in `(seed, seq)`.
    #[must_use]
    pub fn segment(&self, seq: u64) -> Vec<u8> {
        let mut state = self.seed ^ seq.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut out = Vec::with_capacity(self.bytes);
        while out.len() < self.bytes {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let take = (self.bytes - out.len()).min(8);
            out.extend_from_slice(&z.to_le_bytes()[..take]);
        }
        out
    }
}

/// Number of chunks a `payload_bytes`-long body splits into: the data
/// leaves plus every intermediate level of the Swarm chunk tree.
///
/// Excludes the 1–2 mantaray manifest chunks a `POST /bzz` adds on top
/// (< 0.5 % at segment sizes, and they are the same for every mode), so
/// the reported `chunks/s` is strictly the *content* rate — directly
/// comparable to the desktop `--target-peers` sweep in PLAN.md.
#[must_use]
pub const fn data_chunk_count(payload_bytes: u64) -> u64 {
    if payload_bytes <= CHUNK_SIZE {
        return 1;
    }
    let mut level = payload_bytes.div_ceil(CHUNK_SIZE);
    let mut total = level;
    while level > 1 {
        level = level.div_ceil(BRANCHES);
        total += level;
    }
    total
}

// ---------------------------------------------------------------------------
// Per-segment outcome + rolling stats
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct SegmentOutcome {
    /// Wall-clock offset from run start at which the segment was
    /// "captured" (i.e. would have left the encoder).
    captured_at: Duration,
    /// Offset at which the publish call returned.
    finished_at: Duration,
    /// Time spent inside the publish call itself.
    publish: Duration,
    bytes: u64,
    chunks: u64,
    ok: bool,
}

impl SegmentOutcome {
    /// How far behind the live edge this segment landed: the delay
    /// between the moment it was captured and the moment it was
    /// published. A healthy run holds this flat; an unsustainable
    /// bitrate makes it climb without bound.
    const fn lag(&self) -> Duration {
        self.finished_at.saturating_sub(self.captured_at)
    }
}

/// Everything the report is derived from. Guarded by a mutex so
/// [`BenchRun::snapshot`] can render a live progress view while the loop
/// is still appending.
#[derive(Default)]
struct BenchStats {
    segments: Vec<SegmentOutcome>,
    errors: Vec<String>,
    peer_samples: Vec<u32>,
    finished: bool,
}

impl BenchStats {
    fn record(&mut self, outcome: SegmentOutcome, error: Option<String>) {
        self.segments.push(outcome);
        if let Some(e) = error {
            // Keep the first handful verbatim (the interesting ones are
            // always the first of a kind) and then just count.
            if self.errors.len() < 8 {
                self.errors.push(e);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// Aggregate result of one run. Serialised to JSON for the hosts and
/// rendered to Markdown for the results table in
/// `crates/ant-ffi/ANTSTREAM_BENCH.md` / issue #67.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchReport {
    pub label: String,
    pub mode: String,
    pub notes: String,
    pub target_bitrate_kbps: u32,
    pub segment_ms: u32,
    pub segment_bytes: u64,
    pub max_in_flight: usize,
    pub configured_duration_s: u64,
    pub warmup_s: u64,
    /// Wall-clock length of the measured (post-warm-up) window.
    pub measured_s: f64,
    pub segments_total: u64,
    pub segments_ok: u64,
    pub segments_failed: u64,
    /// Segments captured *inside* the measured window, failures
    /// included — i.e. the sample every figure below is computed from.
    /// Zero means the run never left warm-up, so there is nothing to
    /// judge; see [`BenchReport::has_measurement`].
    pub measured_segments_total: u64,
    /// Of those, the ones that published. The verdict keys off this
    /// rather than `segments_ok`, which also counts warm-up.
    pub measured_segments_ok: u64,
    /// Sustained *published* throughput over the measured window. This
    /// is the go/no-go number.
    pub sustained_mbit_s: f64,
    pub sustained_chunks_s: f64,
    /// Publish call duration percentiles, milliseconds.
    pub publish_ms_p50: u64,
    pub publish_ms_p95: u64,
    pub publish_ms_max: u64,
    /// Live-edge lag percentiles, milliseconds: capture → published.
    pub lag_ms_p50: u64,
    pub lag_ms_p95: u64,
    pub lag_ms_max: u64,
    /// Lag of the final measured segment. Together with `lag_ms_max`
    /// this is what says "kept up" vs "fell behind and never recovered".
    pub lag_ms_final: u64,
    /// `true` when the run published every segment and the lag stayed
    /// bounded — see [`BenchReport::keeps_up`].
    pub sustained: bool,
    pub peers_min: u32,
    pub peers_max: u32,
    pub errors: Vec<String>,
}

impl BenchReport {
    /// Bound on the acceptable live-edge lag: three segment durations.
    /// A publisher that stays within one GOP-ish window of live is
    /// usable for broadcast; past that the viewer's playlist stalls.
    fn lag_budget_ms(segment_ms: u32) -> u64 {
        u64::from(segment_ms) * 3
    }

    /// Whether the run produced a measured (post-warm-up) sample at
    /// all. A run stopped inside `warmup_s` has none: every figure in
    /// the report is then the zero an empty window folds to, and
    /// neither "kept up" nor "did not keep up" is a statement about
    /// anything that was measured.
    #[must_use]
    pub const fn has_measurement(&self) -> bool {
        self.measured_segments_total > 0
    }

    /// "Did this configuration keep up?" — the run measured something,
    /// every *measured* segment published, and the last measured segment
    /// was still inside the lag budget.
    ///
    /// Every clause is scoped to the post-warm-up window on purpose. The
    /// whole-run `segments_failed` must not appear here: a cold-start
    /// failure inside `warmup_s` (peer-set warm-up, a pushsync
    /// skip-cache miss — precisely what `warmup_s` exists to exclude)
    /// would otherwise sink the verdict of an otherwise clean measured
    /// run and put a spurious `**no**` in the go/no-go table. Warm-up
    /// failures stay visible in `segments_failed` and `errors`; they
    /// just don't get a vote.
    ///
    /// The first clause is load-bearing: `lag_ms_final` (like the
    /// sustained figures) defaults to 0 on an empty measured window, so
    /// without it a run stopped during warm-up would report a
    /// 0.00 Mbit/s *pass*.
    #[must_use]
    pub fn keeps_up(&self) -> bool {
        self.measured_segments_ok > 0
            && self.measured_segments_ok == self.measured_segments_total
            && self.lag_ms_final <= Self::lag_budget_ms(self.segment_ms)
    }

    /// The **kept up** cell. Three-state on purpose: a run with no
    /// measured window has no verdict to give, and printing either
    /// "yes" or "no" for it would put a claim nobody measured into the
    /// go/no-go table.
    #[must_use]
    pub const fn verdict(&self) -> &'static str {
        if !self.has_measurement() {
            "n/a (no measured window)"
        } else if self.sustained {
            "yes"
        } else {
            "**no**"
        }
    }

    /// One Markdown table row, matching [`markdown_header`].
    #[must_use]
    pub fn markdown_row(&self) -> String {
        format!(
            "| {} | {} | {} kbit/s | {:.2} Mbit/s | {:.1} | {} / {} / {} | {} | {} |",
            self.label,
            self.mode,
            self.target_bitrate_kbps,
            self.sustained_mbit_s,
            self.sustained_chunks_s,
            self.publish_ms_p50,
            self.publish_ms_p95,
            self.publish_ms_max,
            self.lag_ms_final,
            self.verdict(),
        )
    }
}

/// Header for the results table [`BenchReport::markdown_row`] fills.
#[must_use]
pub fn markdown_header() -> String {
    [
        "| environment | mode | target | sustained | chunks/s | publish p50/p95/max ms | final lag ms | kept up |",
        "|---|---|---|---|---|---|---|---|",
    ]
    .join("\n")
}

// ---------------------------------------------------------------------------
// Live snapshot
// ---------------------------------------------------------------------------

/// Progress view for a run that is still going, so the host can render
/// a live readout instead of a spinner.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchSnapshot {
    pub running: bool,
    pub elapsed_s: f64,
    pub configured_duration_s: u64,
    pub segments_total: u64,
    pub segments_ok: u64,
    pub segments_failed: u64,
    pub sustained_mbit_s: f64,
    pub sustained_chunks_s: f64,
    pub lag_ms_last: u64,
    pub peers: u32,
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// A benchmark in flight. Held by the host (`AntHandle` on the FFI path,
/// the CLI's `main` natively) so progress can be polled and the run
/// stopped early.
pub struct BenchRun {
    config: BenchConfig,
    stats: Arc<Mutex<BenchStats>>,
    cancel: Arc<AtomicBool>,
    started_at: Instant,
}

impl BenchRun {
    /// Live progress. Cheap: locks the stats mutex, folds, releases.
    #[must_use]
    pub fn snapshot(&self) -> BenchSnapshot {
        let stats = lock(&self.stats);
        let elapsed = self.started_at.elapsed();
        let window = measured_window(&stats.segments, self.config.warmup_s, elapsed);
        BenchSnapshot {
            running: !stats.finished,
            elapsed_s: elapsed.as_secs_f64(),
            configured_duration_s: self.config.duration_s,
            segments_total: stats.segments.len() as u64,
            segments_ok: stats.segments.iter().filter(|s| s.ok).count() as u64,
            segments_failed: stats.segments.iter().filter(|s| !s.ok).count() as u64,
            sustained_mbit_s: window.mbit_s,
            sustained_chunks_s: window.chunks_s,
            lag_ms_last: stats.segments.last().map_or(0, |s| ms(s.lag())),
            peers: stats.peer_samples.last().copied().unwrap_or(0),
        }
    }

    /// Ask the loop to stop at the next segment boundary. The loop is
    /// cooperative: an in-flight publish is allowed to finish so the
    /// last data point is a real one.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
    }

    /// Final report. Safe to call while the run is still going (it then
    /// reports what has landed so far); the FFI stop path cancels first
    /// and waits for the loop to settle.
    #[must_use]
    pub fn report(&self) -> BenchReport {
        let stats = lock(&self.stats);
        build_report(&self.config, &stats, self.started_at.elapsed())
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        lock(&self.stats).finished
    }
}

/// Sustained-throughput numbers over the post-warm-up window.
struct Window {
    mbit_s: f64,
    chunks_s: f64,
    seconds: f64,
}

fn measured_window(segments: &[SegmentOutcome], warmup_s: u64, elapsed: Duration) -> Window {
    let warmup = Duration::from_secs(warmup_s);
    let measured: Vec<&SegmentOutcome> = segments
        .iter()
        .filter(|s| s.ok && s.captured_at >= warmup)
        .collect();
    let seconds = elapsed.saturating_sub(warmup).as_secs_f64();
    if measured.is_empty() || seconds <= 0.0 {
        return Window {
            mbit_s: 0.0,
            chunks_s: 0.0,
            seconds: seconds.max(0.0),
        };
    }
    let bytes: u64 = measured.iter().map(|s| s.bytes).sum();
    let chunks: u64 = measured.iter().map(|s| s.chunks).sum();
    Window {
        mbit_s: (bytes as f64) * 8.0 / seconds / 1_000_000.0,
        chunks_s: (chunks as f64) / seconds,
        seconds,
    }
}

fn build_report(config: &BenchConfig, stats: &BenchStats, elapsed: Duration) -> BenchReport {
    let window = measured_window(&stats.segments, config.warmup_s, elapsed);
    let warmup = Duration::from_secs(config.warmup_s);
    let measured: Vec<&SegmentOutcome> = stats
        .segments
        .iter()
        .filter(|s| s.captured_at >= warmup)
        .collect();
    let mut publish_ms: Vec<u64> = measured
        .iter()
        .filter(|s| s.ok)
        .map(|s| ms(s.publish))
        .collect();
    let mut lag_ms: Vec<u64> = measured
        .iter()
        .filter(|s| s.ok)
        .map(|s| ms(s.lag()))
        .collect();
    let lag_final = measured
        .iter()
        .rev()
        .find(|s| s.ok)
        .map_or(0, |s| ms(s.lag()));
    publish_ms.sort_unstable();
    lag_ms.sort_unstable();

    let mut report = BenchReport {
        label: config.label.clone(),
        mode: config.mode().as_str().to_string(),
        notes: config.notes.clone(),
        target_bitrate_kbps: config.bitrate_kbps,
        segment_ms: config.segment_ms,
        segment_bytes: config.segment_bytes(),
        max_in_flight: config.max_in_flight,
        configured_duration_s: config.duration_s,
        warmup_s: config.warmup_s,
        measured_s: window.seconds,
        segments_total: stats.segments.len() as u64,
        segments_ok: stats.segments.iter().filter(|s| s.ok).count() as u64,
        segments_failed: stats.segments.iter().filter(|s| !s.ok).count() as u64,
        measured_segments_total: measured.len() as u64,
        measured_segments_ok: measured.iter().filter(|s| s.ok).count() as u64,
        sustained_mbit_s: window.mbit_s,
        sustained_chunks_s: window.chunks_s,
        publish_ms_p50: percentile(&publish_ms, 50),
        publish_ms_p95: percentile(&publish_ms, 95),
        publish_ms_max: publish_ms.last().copied().unwrap_or(0),
        lag_ms_p50: percentile(&lag_ms, 50),
        lag_ms_p95: percentile(&lag_ms, 95),
        lag_ms_max: lag_ms.last().copied().unwrap_or(0),
        lag_ms_final: lag_final,
        sustained: false,
        peers_min: stats.peer_samples.iter().copied().min().unwrap_or(0),
        peers_max: stats.peer_samples.iter().copied().max().unwrap_or(0),
        errors: stats.errors.clone(),
    };
    // One expression, one source of truth: the summary flag and the
    // `keeps_up()` predicate the doc quotes must never disagree.
    report.sustained = report.keeps_up();
    report
}

/// Nearest-rank percentile (`ceil(pct/100 × n)`, 1-indexed). Chosen
/// over the interpolating variant because a tail latency must never be
/// *understated*: with 5 samples, "p95" here is the worst one, not the
/// fourth.
fn percentile(sorted_ms: &[u64], pct: usize) -> u64 {
    if sorted_ms.is_empty() {
        return 0;
    }
    let rank = (sorted_ms.len() * pct).div_ceil(100).max(1);
    sorted_ms[rank.min(sorted_ms.len()) - 1]
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Poison-tolerant lock: a panicked bench thread must not poison the
/// stats for the reader that is about to render the report.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Start a run on `runtime`, returning the handle immediately.
///
/// The caller keeps the [`BenchRun`] alive for the duration; dropping it
/// does not stop the loop (call [`BenchRun::cancel`] for that), but the
/// loop holds only `Arc`s, so a dropped handle just means nobody is
/// reading the results.
pub fn start(
    runtime: &tokio::runtime::Handle,
    config: BenchConfig,
    signing_secret: [u8; 32],
    status_rx: Option<watch::Receiver<StatusSnapshot>>,
) -> Result<Arc<BenchRun>, BenchError> {
    config.validate()?;
    let target = if config.mode() == Mode::Publish {
        Some(Target::parse(&config.gateway)?)
    } else {
        None
    };
    let batch = if config.mode() == Mode::Publish {
        Some(parse_batch_id(&config.batch_id)?)
    } else {
        None
    };

    let stats = Arc::new(Mutex::new(BenchStats::default()));
    let cancel = Arc::new(AtomicBool::new(false));
    let run = Arc::new(BenchRun {
        config: config.clone(),
        stats: Arc::clone(&stats),
        cancel: Arc::clone(&cancel),
        started_at: Instant::now(),
    });

    let ctx = RunCtx {
        config,
        target,
        batch,
        signing_secret,
        stats,
        cancel,
        status_rx,
    };
    runtime.spawn(async move { drive(ctx).await });
    Ok(run)
}

struct RunCtx {
    config: BenchConfig,
    target: Option<Target>,
    batch: Option<[u8; 32]>,
    signing_secret: [u8; 32],
    stats: Arc<Mutex<BenchStats>>,
    cancel: Arc<AtomicBool>,
    status_rx: Option<watch::Receiver<StatusSnapshot>>,
}

/// The loop both modes share: generate on a wall clock, publish with a
/// bounded window, record.
async fn drive(ctx: RunCtx) {
    let started = Instant::now();
    let cadence = Duration::from_millis(u64::from(ctx.config.segment_ms));
    let total = Duration::from_secs(ctx.config.duration_s);
    let generator = SegmentGenerator::new(ctx.config.seed, ctx.config.segment_bytes());
    let window = Arc::new(Semaphore::new(ctx.config.max_in_flight));
    let sampler = spawn_peer_sampler(&ctx);
    // One issuer for the whole run, behind a mutex, mirroring how the
    // node keeps a single `StampIssuer` per batch: stamp issuance
    // really is serialised across concurrent uploads, and a per-segment
    // issuer would both hide that and pay a 256 KiB bucket-table
    // allocation the real publisher never pays.
    let issuer = match ctx.target {
        Some(_) => None,
        None => match ant_postage::StampIssuer::new([0x11; 32], 24, 16, false) {
            Ok(i) => Some(Arc::new(Mutex::new(i))),
            Err(e) => {
                let mut stats = lock(&ctx.stats);
                stats.errors.push(format!("build stamp issuer: {e}"));
                stats.finished = true;
                return;
            }
        },
    };

    let mut seq: u64 = 0;
    let mut publishes = Vec::new();
    while !ctx.cancel.load(Ordering::SeqCst) {
        // The encoder emits a segment every `segment_ms` no matter how
        // the uploader is doing — that is what makes the lag metric
        // mean something. Sleep to the *absolute* boundary so a slow
        // publish never skews the capture clock.
        let captured_at = cadence * u32::try_from(seq + 1).unwrap_or(u32::MAX);
        if captured_at >= total {
            break;
        }
        sleep_until(started + captured_at).await;
        if ctx.cancel.load(Ordering::SeqCst) {
            break;
        }

        let payload = generator.segment(seq);
        // Bounded in-flight window: waiting here is exactly the
        // backpressure a real publisher feels, and the wait shows up in
        // the segment's lag.
        let Ok(permit) = Arc::clone(&window).acquire_owned().await else {
            break;
        };
        let stats = Arc::clone(&ctx.stats);
        let target = ctx.target.clone();
        let batch = ctx.batch;
        let secret = ctx.signing_secret;
        let issuer = issuer.clone();
        let started_ref = started;
        // Drop the handles of segments that already landed so a long
        // run doesn't accumulate one `JoinHandle` per segment.
        publishes.retain(|task: &tokio::task::JoinHandle<()>| !task.is_finished());
        publishes.push(tokio::spawn(async move {
            let _permit = permit;
            let call_start = Instant::now();
            let outcome = match (target, batch, issuer) {
                (Some(target), Some(batch), _) => {
                    publish_segment(&target, batch, seq, &payload).await
                }
                (_, _, Some(issuer)) => pipeline_segment(&issuer, &secret, seq, &payload),
                _ => Err(format!("segment {seq}: no publish target and no issuer")),
            };
            let publish = call_start.elapsed();
            let (chunks, error) = match outcome {
                Ok(chunks) => (chunks, None),
                Err(e) => (0, Some(e)),
            };
            let ok = error.is_none();
            lock(&stats).record(
                SegmentOutcome {
                    captured_at,
                    finished_at: started_ref.elapsed(),
                    publish,
                    bytes: if ok { payload.len() as u64 } else { 0 },
                    chunks,
                    ok,
                },
                error,
            );
        }));
        seq += 1;
    }

    // Let the in-flight window drain so the last segments are real data
    // points rather than a truncated tail.
    for task in publishes {
        let _ = task.await;
    }
    if let Some(sampler) = sampler {
        sampler.abort();
    }
    lock(&ctx.stats).finished = true;
}

fn spawn_peer_sampler(ctx: &RunCtx) -> Option<tokio::task::JoinHandle<()>> {
    let status_rx = ctx.status_rx.clone()?;
    let stats = Arc::clone(&ctx.stats);
    Some(tokio::spawn(async move {
        loop {
            let peers = status_rx.borrow().peers.connected;
            lock(&stats).peer_samples.push(peers);
            tokio::time::sleep(SAMPLE_INTERVAL).await;
        }
    }))
}

async fn sleep_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now {
        tokio::time::sleep(deadline - now).await;
    }
}

// ---------------------------------------------------------------------------
// Publish path
// ---------------------------------------------------------------------------

/// Publish one segment with `POST /bzz`, exactly as the stage-2 loop
/// will. Returns the segment's data-chunk count on success.
async fn publish_segment(
    target: &Target,
    batch: [u8; 32],
    seq: u64,
    payload: &[u8],
) -> Result<u64, String> {
    let path = format!("{}/bzz?name=seg-{seq}.m4s", target.prefix);
    let headers = [
        ("content-type".to_string(), "video/iso.segment".to_string()),
        ("swarm-postage-batch-id".to_string(), hex::encode(batch)),
    ];
    let response =
        tokio::time::timeout(PUBLISH_TIMEOUT, http_post(target, &path, &headers, payload))
            .await
            .map_err(|_| format!("segment {seq}: publish timed out after {PUBLISH_TIMEOUT:?}"))??;
    if response.status != 201 {
        return Err(format!(
            "segment {seq}: gateway returned {} {}",
            response.status,
            String::from_utf8_lossy(&response.body).trim(),
        ));
    }
    Ok(data_chunk_count(payload.len() as u64))
}

/// The no-network mode: split the segment into its chunk tree and sign
/// a postage stamp for every chunk — the entire CPU cost of publishing,
/// stopping at the socket. The issuer is in-memory (depth 24 over the
/// standard bucket depth 16, so a long run never wraps a bucket and
/// starts measuring re-issue instead of steady-state signing) and its
/// batch id is synthetic: nothing here touches the chain or the network.
fn pipeline_segment(
    issuer: &Mutex<ant_postage::StampIssuer>,
    secret: &[u8; 32],
    seq: u64,
    payload: &[u8],
) -> Result<u64, String> {
    let split = ant_retrieval::split_bytes(payload);
    let mut issuer = lock(issuer);
    for chunk in &split.chunks {
        ant_postage::sign_stamp_bytes(secret, &mut issuer, &chunk.address)
            .map_err(|e| format!("segment {seq}: sign stamp: {e}"))?;
    }
    Ok(data_chunk_count(payload.len() as u64))
}

fn parse_batch_id(raw: &str) -> Result<[u8; 32], BenchError> {
    let trimmed = raw.trim().trim_start_matches("0x");
    let bytes = hex::decode(trimmed)
        .map_err(|e| BenchError::Config(format!("batch_id is not hex: {e}")))?;
    <[u8; 32]>::try_from(bytes.as_slice()).map_err(|_| {
        BenchError::Config(format!(
            "batch_id must be 32 bytes (64 hex chars), got {}",
            trimmed.len(),
        ))
    })
}

// ---------------------------------------------------------------------------
// Minimal loopback HTTP/1.1 client
// ---------------------------------------------------------------------------
//
// The gateway the publisher posts to is always on loopback — in-process
// on iOS (`ant_start_gateway`), `antd` on desktop — so a full HTTP
// client stack would be dead weight in the mobile slice, which
// deliberately drops `reqwest` (see `ant-ffi/Cargo.toml`). This is the
// smallest thing that speaks the one request shape the publisher needs:
// `POST` with a `Content-Length` body, one response, connection closed.

/// Parsed `http://host:port/prefix` gateway base.
#[derive(Debug, Clone)]
struct Target {
    authority: String,
    /// Path prefix, without a trailing slash (`""` for a bare host).
    prefix: String,
}

impl Target {
    fn parse(url: &str) -> Result<Self, BenchError> {
        let rest = url.trim().strip_prefix("http://").ok_or_else(|| {
            BenchError::Config(format!(
                "gateway must be an http:// URL (the publisher posts to a loopback gateway), got `{url}`",
            ))
        })?;
        let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
        if authority.is_empty() {
            return Err(BenchError::Config(format!("gateway has no host: `{url}`")));
        }
        let authority = if authority.contains(':') {
            authority.to_string()
        } else {
            format!("{authority}:80")
        };
        let prefix = path.trim_end_matches('/');
        Ok(Self {
            authority,
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("/{prefix}")
            },
        })
    }
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

async fn http_post(
    target: &Target,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<HttpResponse, String> {
    let mut stream = TcpStream::connect(&target.authority)
        .await
        .map_err(|e| format!("connect {}: {e}", target.authority))?;
    // Loopback + small bodies: Nagle only adds latency to the
    // measurement we are here to take.
    let _ = stream.set_nodelay(true);

    let mut head = format!(
        "POST {path} HTTP/1.1\r\nhost: {}\r\ncontent-length: {}\r\nconnection: close\r\n",
        target.authority,
        body.len(),
    );
    for (name, value) in headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|e| format!("write request head: {e}"))?;
    stream
        .write_all(body)
        .await
        .map_err(|e| format!("write request body: {e}"))?;
    stream
        .flush()
        .await
        .map_err(|e| format!("flush request: {e}"))?;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| format!("read response: {e}"))?;
    parse_response(&raw)
}

/// Parse a `connection: close` response: status line, headers, body to
/// EOF. Chunked transfer-encoding is not handled — the gateway answers
/// uploads with a small `Content-Length` JSON object, and a body we
/// can't parse would show up as a non-201 status anyway.
fn parse_response(raw: &[u8]) -> Result<HttpResponse, String> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "malformed response: no header terminator".to_string())?;
    let head = String::from_utf8_lossy(&raw[..split]);
    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| "malformed response: empty".to_string())?;
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| format!("malformed status line: `{status_line}`"))?;
    Ok(HttpResponse {
        status,
        body: raw[split + 4..].to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> BenchConfig {
        BenchConfig {
            label: "test".into(),
            bitrate_kbps: 3400,
            segment_ms: 2000,
            duration_s: 60,
            warmup_s: 10,
            max_in_flight: 4,
            gateway: "http://127.0.0.1:1633".into(),
            batch_id: String::new(),
            seed: 7,
            notes: String::new(),
        }
    }

    #[test]
    fn segment_bytes_matches_bitrate() {
        // 3400 kbit/s × 2 s = 6800 kbit = 850 000 bytes.
        assert_eq!(config().segment_bytes(), 850_000);
    }

    #[test]
    fn mode_follows_batch_presence() {
        let mut c = config();
        assert_eq!(c.mode(), Mode::Pipeline);
        c.batch_id = format!("0x{}", "ab".repeat(32));
        assert_eq!(c.mode(), Mode::Publish);
    }

    #[test]
    fn segments_are_unique_and_deterministic() {
        let gen = SegmentGenerator::new(7, 8192);
        let a = gen.segment(0);
        let b = gen.segment(1);
        assert_eq!(a.len(), 8192);
        assert_ne!(a, b, "each segment must hash to a fresh chunk set");
        assert_eq!(a, SegmentGenerator::new(7, 8192).segment(0));
    }

    #[test]
    fn chunk_count_covers_the_tree() {
        assert_eq!(data_chunk_count(0), 1);
        assert_eq!(data_chunk_count(4096), 1);
        // 2 leaves + 1 root.
        assert_eq!(data_chunk_count(4097), 3);
        // 850 000 B → 208 leaves → 2 intermediates → 1 root.
        assert_eq!(data_chunk_count(850_000), 208 + 2 + 1);
    }

    #[test]
    fn batch_id_parses_with_and_without_prefix() {
        let hexed = "ab".repeat(32);
        assert_eq!(parse_batch_id(&hexed).unwrap(), [0xab; 32]);
        assert_eq!(parse_batch_id(&format!("0x{hexed}")).unwrap(), [0xab; 32]);
        assert!(parse_batch_id("0xdead").is_err());
    }

    #[test]
    fn target_parses_host_and_prefix() {
        let t = Target::parse("http://127.0.0.1:1633").unwrap();
        assert_eq!(t.authority, "127.0.0.1:1633");
        assert_eq!(t.prefix, "");
        let t = Target::parse("http://example.test/ant/").unwrap();
        assert_eq!(t.authority, "example.test:80");
        assert_eq!(t.prefix, "/ant");
        assert!(Target::parse("https://example.test").is_err());
    }

    #[test]
    fn response_parser_reads_status_and_body() {
        let raw = b"HTTP/1.1 201 Created\r\ncontent-length: 2\r\n\r\n{}";
        let r = parse_response(raw).unwrap();
        assert_eq!(r.status, 201);
        assert_eq!(r.body, b"{}");
        assert!(parse_response(b"garbage").is_err());
    }

    #[test]
    fn config_validation_rejects_unusable_runs() {
        let mut c = config();
        c.label = "  ".into();
        assert!(c.validate().is_err());
        let mut c = config();
        c.warmup_s = c.duration_s;
        assert!(c.validate().is_err());
        assert!(config().validate().is_ok());
    }

    fn outcome(captured_s: u64, finished_s: u64, ok: bool) -> SegmentOutcome {
        SegmentOutcome {
            captured_at: Duration::from_secs(captured_s),
            finished_at: Duration::from_secs(finished_s),
            publish: Duration::from_millis(500),
            bytes: if ok { 850_000 } else { 0 },
            chunks: if ok { 211 } else { 0 },
            ok,
        }
    }

    #[test]
    fn warmup_segments_are_excluded_from_the_sustained_figure() {
        let mut stats = BenchStats::default();
        // Two segments inside the 10 s warm-up, two after.
        for (captured, finished) in [(2, 3), (4, 5), (12, 13), (14, 15)] {
            stats.record(outcome(captured, finished, true), None);
        }
        let report = build_report(&config(), &stats, Duration::from_secs(20));
        // Only the post-warm-up pair counts, over a 10 s window.
        let expected = (2.0 * 850_000.0 * 8.0) / 10.0 / 1_000_000.0;
        assert!((report.sustained_mbit_s - expected).abs() < 1e-9);
        assert_eq!(report.segments_total, 4);
    }

    #[test]
    fn a_failed_segment_sinks_the_sustained_verdict() {
        let mut stats = BenchStats::default();
        stats.record(outcome(12, 13, true), None);
        stats.record(outcome(14, 15, false), Some("boom".into()));
        let report = build_report(&config(), &stats, Duration::from_secs(20));
        assert_eq!(report.segments_failed, 1);
        assert!(!report.sustained);
        assert_eq!(report.errors, vec!["boom".to_string()]);
    }

    #[test]
    fn growing_lag_fails_the_run_even_with_no_errors() {
        let mut stats = BenchStats::default();
        // Captured every 2 s but published ever later: classic
        // "uploader can't keep up" shape. Budget is 3 × 2 s = 6 s.
        for i in 0..5u64 {
            stats.record(outcome(12 + i * 2, 14 + i * 4, true), None);
        }
        let report = build_report(&config(), &stats, Duration::from_secs(40));
        assert_eq!(report.segments_failed, 0);
        assert!(report.lag_ms_final > 6000, "lag {}", report.lag_ms_final);
        assert!(!report.sustained);
    }

    #[test]
    fn a_failure_inside_the_warmup_does_not_sink_a_clean_measured_run() {
        // The cold-start shape `warmup_s` exists to exclude: the first
        // segment misses (peer set still warming, pushsync skip-cache
        // cold), then every measured segment publishes inside the lag
        // budget. The verdict is about the measured window, so this is
        // a pass — the warm-up failure is still reported, it just
        // doesn't get a vote.
        let mut stats = BenchStats::default();
        stats.record(outcome(2, 3, false), Some("cold pushsync".into()));
        for i in 0..10u64 {
            stats.record(outcome(12 + i * 2, 13 + i * 2, true), None);
        }
        let report = build_report(&config(), &stats, Duration::from_secs(40));
        assert_eq!(report.segments_failed, 1, "the miss is still reported");
        assert_eq!(report.measured_segments_total, 10);
        assert_eq!(report.measured_segments_ok, 10);
        assert_eq!(report.lag_ms_final, 1000);
        assert!(report.sustained, "{report:?}");
        assert!(report.markdown_row().contains("| yes |"));
    }

    #[test]
    fn a_run_that_keeps_up_passes() {
        let mut stats = BenchStats::default();
        for i in 0..5u64 {
            stats.record(outcome(12 + i * 2, 13 + i * 2, true), None);
        }
        let report = build_report(&config(), &stats, Duration::from_secs(30));
        assert!(report.sustained);
        assert_eq!(report.lag_ms_final, 1000);
        assert!(report.markdown_row().contains("| yes |"));
    }

    #[test]
    fn a_run_stopped_inside_the_warmup_has_no_verdict() {
        // "Stop and report" tapped 6 s into a run with a 10 s warm-up:
        // both segments published, but none of them was measured. The
        // report must not claim the rendition was sustained on the
        // strength of an empty window (whose lag_ms_final folds to 0).
        let mut stats = BenchStats::default();
        for (captured, finished) in [(2, 3), (4, 5)] {
            stats.record(outcome(captured, finished, true), None);
        }
        let report = build_report(&config(), &stats, Duration::from_secs(6));
        assert_eq!(report.segments_ok, 2);
        assert_eq!(report.measured_segments_total, 0);
        assert!(!report.has_measurement());
        assert!(!report.sustained, "{report:?}");
        assert!(report.sustained_mbit_s.abs() < f64::EPSILON);
        // And the row says so, rather than passing *or* failing a run
        // that measured nothing.
        assert!(
            report
                .markdown_row()
                .contains("| n/a (no measured window) |"),
            "row: {}",
            report.markdown_row(),
        );
    }

    #[test]
    fn a_failure_inside_the_measured_window_still_reads_as_a_failure() {
        // The other empty-`measured_segments_ok` shape: segments were
        // measured, they just all failed. That is a real "**no**", not
        // an absent verdict.
        let mut stats = BenchStats::default();
        stats.record(outcome(12, 13, false), Some("boom".into()));
        let report = build_report(&config(), &stats, Duration::from_secs(20));
        assert!(report.has_measurement());
        assert_eq!(report.measured_segments_ok, 0);
        assert!(!report.sustained);
        assert!(report.markdown_row().contains("**no**"));
    }

    #[test]
    fn percentiles_are_stable_on_small_samples() {
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[5], 95), 5);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 50), 3);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 95), 5);
    }

    // -----------------------------------------------------------------
    // End-to-end publish path, against a stub that speaks the gateway's
    // `POST /bzz` contract. This is the half the unit tests above can't
    // reach: the request the publisher actually writes, and what the
    // loop does with the answer.
    // -----------------------------------------------------------------

    /// What the stub gateway saw on its first request.
    #[derive(Default)]
    struct SeenRequest {
        head: String,
        body_len: usize,
    }

    /// Serve `status` to every upload until the test drops the returned
    /// receiver. Returns the bound address and a slot the first request
    /// is recorded into.
    async fn stub_gateway(
        status: &'static str,
        delay: Duration,
    ) -> (String, Arc<Mutex<SeenRequest>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let seen = Arc::new(Mutex::new(SeenRequest::default()));
        let seen_writer = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let seen = Arc::clone(&seen_writer);
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 8192];
                    // The publisher sends `connection: close` and one
                    // request per connection, so "read until the body is
                    // complete" is just "read until content-length is
                    // satisfied".
                    loop {
                        let Ok(n) = socket.read(&mut buf).await else {
                            return;
                        };
                        if n == 0 {
                            break;
                        }
                        raw.extend_from_slice(&buf[..n]);
                        if let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&raw[..split]).to_string();
                            let want: usize = head
                                .lines()
                                .find_map(|l| {
                                    l.strip_prefix("content-length: ")?.trim().parse().ok()
                                })
                                .unwrap_or(0);
                            if raw.len() - split - 4 >= want {
                                let mut slot = lock(&seen);
                                if slot.head.is_empty() {
                                    slot.head = head;
                                    slot.body_len = raw.len() - split - 4;
                                }
                                break;
                            }
                        }
                    }
                    tokio::time::sleep(delay).await;
                    let body = br#"{"reference":"00"}"#;
                    let response = format!(
                        "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len(),
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                    let _ = socket.shutdown().await;
                });
            }
        });
        (addr, seen)
    }

    fn publish_config(addr: &str) -> BenchConfig {
        BenchConfig {
            label: "stub".into(),
            bitrate_kbps: 64,
            segment_ms: 100,
            duration_s: 1,
            warmup_s: 0,
            max_in_flight: 4,
            gateway: format!("http://{addr}"),
            batch_id: "ab".repeat(32),
            seed: 3,
            notes: String::new(),
        }
    }

    async fn run_to_completion(run: &Arc<BenchRun>) {
        for _ in 0..200 {
            if run.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("bench run never finished");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn publishes_every_segment_and_measures_it() {
        let (addr, seen) = stub_gateway("201 Created", Duration::from_millis(5)).await;
        let config = publish_config(&addr);
        let run = start(
            &tokio::runtime::Handle::current(),
            config.clone(),
            [7u8; 32],
            None,
        )
        .unwrap();
        run_to_completion(&run).await;

        let report = run.report();
        // 1 s at one segment per 100 ms, minus the boundary segment.
        assert!(
            report.segments_ok >= 8,
            "only {} segments landed: {:?}",
            report.segments_ok,
            report.errors,
        );
        assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
        assert!(report.sustained, "{report:?}");
        // 64 kbit/s of segments really did move through the socket.
        assert!(
            report.sustained_mbit_s > 0.04 && report.sustained_mbit_s < 0.2,
            "sustained {} Mbit/s",
            report.sustained_mbit_s,
        );

        // The request is the one the bee gateway expects: `POST /bzz`
        // with the batch header and the whole segment as the body.
        let seen = lock(&seen);
        assert!(
            seen.head.starts_with("POST /bzz?name=seg-"),
            "request line: {}",
            seen.head,
        );
        assert!(
            seen.head
                .contains(&format!("swarm-postage-batch-id: {}", "ab".repeat(32))),
            "headers: {}",
            seen.head,
        );
        assert_eq!(seen.body_len as u64, config.segment_bytes());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejecting_gateway_fails_the_run_instead_of_reporting_throughput() {
        // Exactly what an expired / unfunded batch looks like from the
        // publisher's side: the gateway answers, but not with 201.
        let (addr, _seen) = stub_gateway("402 Payment Required", Duration::ZERO).await;
        let run = start(
            &tokio::runtime::Handle::current(),
            publish_config(&addr),
            [7u8; 32],
            None,
        )
        .unwrap();
        run_to_completion(&run).await;

        let report = run.report();
        assert!(report.segments_failed > 0);
        assert_eq!(report.segments_ok, 0);
        assert!(!report.sustained);
        assert!(report.sustained_mbit_s.abs() < f64::EPSILON);
        assert!(
            report.errors.iter().any(|e| e.contains("402")),
            "errors: {:?}",
            report.errors,
        );
        assert!(report.markdown_row().contains("**no**"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_gateway_slower_than_the_camera_shows_up_as_growing_lag() {
        // 250 ms per publish with a 2-deep window against a 100 ms
        // capture cadence: the uploader cannot keep up, and the run has
        // to say so rather than quietly publishing fewer segments.
        let (addr, _seen) = stub_gateway("201 Created", Duration::from_millis(250)).await;
        let mut config = publish_config(&addr);
        config.max_in_flight = 2;
        config.duration_s = 2;
        let run = start(&tokio::runtime::Handle::current(), config, [7u8; 32], None).unwrap();
        run_to_completion(&run).await;

        let report = run.report();
        assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
        assert!(
            report.lag_ms_final > 300,
            "final lag {} ms should have grown past the 3 × 100 ms budget",
            report.lag_ms_final,
        );
        assert!(!report.sustained, "{report:?}");
    }
}
