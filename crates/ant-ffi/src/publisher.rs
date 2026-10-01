//! `AntStream` live publisher loop — issue #67 stage 2.
//!
//! Stage 1 ([`crate::bench`]) established that the publisher loop is the
//! camera pipeline plus one `POST /bzz` per segment, and measured what
//! that path sustains. Stage 2 keeps the loop and replaces the synthetic
//! generator with the real capture pipeline (#65): the iOS side hands
//! finished fMP4 segments to [`LiveRun::push`], and everything below
//! that point is the code stage 1 measured — same bounded in-flight
//! window, same loopback HTTP client, same lag accounting — now carrying
//! real video.
//!
//! What stage 2 adds on top of the measured path, per segment:
//!
//! 1. `POST /bzz` the segment (and, when the writer restarts, its fMP4
//!    initialization segment).
//! 2. Rebuild the HLS media playlist over a sliding window of the most
//!    recent segments.
//! 3. `POST /bzz` the playlist, then publish its reference as a
//!    **sequence-feed update** with `POST /soc/{owner}/{id}` — the
//!    bee-js shape (`id = keccak256(topic ‖ index_be8)`, payload
//!    `timestamp_be8 ‖ reference`), so any bee gateway resolves the
//!    channel with `GET /feeds/{owner}/{topic}` or through the feed
//!    manifest this module creates at start with `POST /feeds`.
//!
//! Live-edge discipline, straight from the stage-1 findings:
//!
//! * The in-flight window defaults to **4**. Stage 1 measured window 4
//!   as the stable point (899/899 segments at 900 kbit/s) and window 8
//!   as a *collapse* (26/316) — per-peer stream pressure destroys
//!   connections — so this is a tuned constant, not an arbitrary one.
//! * The default rendition is 360p at ~900 kbit/s with 2 s segments,
//!   the row the stage-1 go/no-go landed on.
//! * When the uplink cannot keep up, the backlog **drops its oldest
//!   pending segment** rather than growing without bound: a live
//!   broadcast that falls minutes behind is worse than one with a gap.
//!   Dropped and failed segments leave a hole, and the next segment that
//!   does land is tagged `#EXT-X-DISCONTINUITY`.
//!
//! Uploads go straight to the in-process gateway (`ant_start_gateway`)
//! rather than through `UploadManager` jobs on purpose: that machinery is
//! resume-oriented VOD tooling, which is the wrong shape for a live edge
//! (a resumed segment is a segment nobody will ever play). It becomes the
//! right tool in stage 3, for the VOD finalize path.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ant_control::StatusSnapshot;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{watch, Notify, Semaphore};

// ---------------------------------------------------------------------------
// Shared publish primitives (used by the live loop and by `crate::bench`)
// ---------------------------------------------------------------------------

/// Swarm chunk payload size. Segment byte counts are converted to chunk
/// counts with this so `chunks/s` is comparable to the desktop
/// `--target-peers` sweep in PLAN.md (Phase 7g: 105.3 chunks/s at 400
/// peers).
pub(crate) const CHUNK_SIZE: u64 = 4096;

/// Branching factor of the Swarm chunk tree (128 × 32-byte references
/// per intermediate chunk). Mirrors `ant_retrieval::BRANCHES`; kept
/// local so [`data_chunk_count`] stays a pure function usable from the
/// unit tests without pulling the splitter in.
const BRANCHES: u64 = 128;

/// Per-segment publish deadline. A segment that has not landed within
/// this long is recorded as a failure rather than stalling the run: at
/// live-edge bitrates anything past ~1 min is already unusable, and both
/// the bench and the live publisher must keep going so the report (or
/// the on-screen indicator) shows *where* it broke.
pub(crate) const PUBLISH_TIMEOUT: Duration = Duration::from_mins(1);

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

/// Nearest-rank percentile (`ceil(pct/100 × n)`, 1-indexed). Chosen
/// over the interpolating variant because a tail latency must never be
/// *understated*: with 5 samples, "p95" here is the worst one, not the
/// fourth.
pub(crate) fn percentile(sorted_ms: &[u64], pct: usize) -> u64 {
    if sorted_ms.is_empty() {
        return 0;
    }
    let rank = (sorted_ms.len() * pct).div_ceil(100).max(1);
    sorted_ms[rank.min(sorted_ms.len()) - 1]
}

pub(crate) fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Poison-tolerant lock: a panicked publisher task must not poison the
/// state for the reader that is about to render the progress view.
pub(crate) fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Default HLS segment duration. 2 s is what the #65 capture pipeline
/// targets and what the stage-1 go/no-go row (899/899 at 900 kbit/s)
/// was measured with.
pub const DEFAULT_SEGMENT_MS: u32 = 2000;

/// Default video bitrate: 360p, the rendition stage 1 landed on. 540p
/// (1800) was measured as out of reach at window 4 and is gated on #74
/// (chunk-level pusher) or #68 (relay).
pub const DEFAULT_BITRATE_KBPS: u32 = 900;

/// Default in-flight publish window. Stage 1: window 4 sustained
/// 899/899 segments; window 8 collapsed the same run to 26/316 because
/// per-peer stream pressure destroys connections. Do not raise this
/// without re-measuring per-chunk pace (see `ANTSTREAM_BENCH.md`).
pub const DEFAULT_MAX_IN_FLIGHT: usize = 4;

/// How many captured-but-not-yet-started segments may queue behind the
/// in-flight window before the oldest is dropped. One window's worth:
/// past that the publisher is a window *and* a backlog behind live, and
/// the oldest pending segment is the one a viewer is least likely to
/// still want.
pub const DEFAULT_MAX_BACKLOG: usize = 4;

/// Segments listed in the rolling media playlist. Apple recommends a
/// live playlist hold at least 3 target durations; 6 × 2 s = 12 s gives
/// a joining viewer a little more to buffer without making the playlist
/// (which is republished per segment) meaningfully bigger.
pub const DEFAULT_PLAYLIST_WINDOW: usize = 6;

/// One live broadcast. Deserialised straight from the host's JSON so the
/// Swift app configures the same knobs the tests do.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PublisherConfig {
    /// Human-readable channel name. Also the default topic seed (see
    /// [`PublisherConfig::topic`]).
    pub channel: String,
    /// Feed topic, 32-byte hex (`0x` optional). Empty derives a topic
    /// unique to this broadcast —
    /// `keccak256("antstream/<channel>/<unix_ms>/<nonce>")`, the nonce a
    /// process-wide counter — so a second broadcast of the same channel
    /// starts a fresh feed at index 0 instead of having to discover the
    /// previous head (or, restarted quickly, overwriting it).
    #[serde(default)]
    pub topic: String,
    /// Gateway base URL, `http://host:port` (loopback: the in-process
    /// `ant_start_gateway` on iOS, `antd` on desktop).
    #[serde(default = "default_gateway")]
    pub gateway: String,
    /// Postage batch id (hex, `0x`-optional) every upload is stamped
    /// with. Required: a live publisher with no batch has nowhere to put
    /// the video.
    pub batch_id: String,
    /// Nominal segment duration. Only used for the playlist's
    /// `EXT-X-TARGETDURATION` and the "keeping up" budget — the real
    /// duration of each segment comes from the capture side.
    #[serde(default = "default_segment_ms")]
    pub segment_ms: u32,
    /// Encoder target bitrate. Echoed in the report; the publisher does
    /// not enforce it (the capture pipeline does).
    #[serde(default = "default_bitrate")]
    pub bitrate_kbps: u32,
    #[serde(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    #[serde(default = "default_max_backlog")]
    pub max_backlog: usize,
    #[serde(default = "default_playlist_window")]
    pub playlist_window: usize,
    /// Free-form host context (device, network, rendition) echoed in the
    /// final report.
    #[serde(default)]
    pub notes: String,
}

fn default_gateway() -> String {
    "http://127.0.0.1:1633".to_string()
}
const fn default_segment_ms() -> u32 {
    DEFAULT_SEGMENT_MS
}
const fn default_bitrate() -> u32 {
    DEFAULT_BITRATE_KBPS
}
const fn default_max_in_flight() -> usize {
    DEFAULT_MAX_IN_FLIGHT
}
const fn default_max_backlog() -> usize {
    DEFAULT_MAX_BACKLOG
}
const fn default_playlist_window() -> usize {
    DEFAULT_PLAYLIST_WINDOW
}

impl PublisherConfig {
    fn validate(&self) -> Result<(), PublisherError> {
        if self.channel.trim().is_empty() {
            return Err(PublisherError::Config(
                "channel is required: it names the feed viewers subscribe to".into(),
            ));
        }
        if self.batch_id.trim().is_empty() {
            return Err(PublisherError::Config(
                "batch_id is required: a broadcast needs a storage plan to stamp its segments"
                    .into(),
            ));
        }
        if self.segment_ms == 0 {
            return Err(PublisherError::Config("segment_ms must be > 0".into()));
        }
        if self.max_in_flight == 0 {
            return Err(PublisherError::Config("max_in_flight must be > 0".into()));
        }
        if self.max_backlog == 0 {
            return Err(PublisherError::Config("max_backlog must be > 0".into()));
        }
        if self.playlist_window == 0 {
            return Err(PublisherError::Config("playlist_window must be > 0".into()));
        }
        Ok(())
    }

    /// The feed topic this broadcast writes to: the configured one, or a
    /// per-broadcast derivation of the channel name.
    fn resolve_topic(&self) -> Result<[u8; 32], PublisherError> {
        // A wall-clock seed alone is not unique enough: stop + restart
        // of the same channel inside one clock tick would silently reuse
        // the topic, and the new broadcast's index 0 would overwrite the
        // old feed's head. The process-wide counter makes every
        // derivation distinct regardless of clock granularity.
        static TOPIC_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let trimmed = self.topic.trim();
        if trimmed.is_empty() {
            let unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let nonce = TOPIC_NONCE.fetch_add(1, Ordering::Relaxed);
            return Ok(ant_crypto::keccak256(
                format!("antstream/{}/{unix_ms}/{nonce}", self.channel).as_bytes(),
            ));
        }
        parse_hex32(trimmed).map_err(|e| PublisherError::Config(format!("topic {e}")))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("{0}")]
    Config(String),
}

fn parse_hex32(raw: &str) -> Result<[u8; 32], String> {
    let trimmed = raw.trim().trim_start_matches("0x");
    let bytes = hex::decode(trimmed).map_err(|e| format!("is not hex: {e}"))?;
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| format!("must be 32 bytes (64 hex chars), got {}", trimmed.len()))
}

// ---------------------------------------------------------------------------
// Segments pushed in from the capture pipeline
// ---------------------------------------------------------------------------

/// Which kind of fMP4 payload the capture pipeline handed over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentKind {
    /// The fMP4 initialization segment (`ftyp` + `moov`). Referenced by
    /// `#EXT-X-MAP`; re-emitted whenever the writer restarts (camera
    /// flip, interruption recovery, bitrate downshift).
    Init,
    /// A media segment (`moof` + `mdat`).
    Media,
}

impl SegmentKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Init => "init",
            Self::Media => "media",
        }
    }
}

/// What [`LiveRun::push`] did with a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// Queued for publishing.
    Queued,
    /// Queued, but the backlog was full so the oldest pending segment
    /// was dropped to stay at the live edge.
    QueuedDroppingOldest,
    /// The run is stopping and no longer accepts segments.
    Closed,
}

struct Pending {
    seq: u64,
    kind: SegmentKind,
    data: Vec<u8>,
    duration_ms: u32,
    /// Capture-side discontinuity: the writer restarted, so the next
    /// media segment does not continue the previous one's timeline.
    discontinuity: bool,
    captured_at: Instant,
}

/// A segment that landed, in playlist terms.
#[derive(Debug, Clone)]
struct MediaEntry {
    /// HLS media sequence number (counts only listed segments).
    number: u64,
    uri: String,
    duration_ms: u32,
    /// `#EXT-X-DISCONTINUITY` precedes this entry.
    discontinuity: bool,
    /// The `#EXT-X-MAP` in force for this entry.
    init_uri: String,
}

/// Per-sequence outcome, collected out of order by the concurrent
/// publish tasks and consumed **in order** by the committer.
enum Committed {
    Init {
        uri: String,
    },
    Media {
        uri: String,
        duration_ms: u32,
        discontinuity: bool,
        captured_at: Instant,
    },
    /// Dropped before it was ever uploaded (backlog full). Only media
    /// segments are ever dropped.
    Dropped,
    /// Upload failed. The kind matters: a failed *initialization*
    /// segment retires the `#EXT-X-MAP` currently in force, because the
    /// writer that emitted it has already been replaced and its map does
    /// not describe what follows.
    Failed(SegmentKind),
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// How many latency samples are kept for the percentile figures. A
/// multi-hour broadcast must not grow an unbounded vector, and the tail
/// of a live run is what the indicator is about anyway.
const LATENCY_SAMPLES: usize = 2048;

#[derive(Default)]
struct PublisherState {
    /// Captured, not yet started. Drop-oldest when longer than
    /// `max_backlog`.
    pending: VecDeque<Pending>,
    /// Completed-but-not-yet-committed outcomes, keyed by sequence.
    done: BTreeMap<u64, Committed>,
    /// Set by the pump once it has drained the backlog *and* awaited
    /// every in-flight upload. The committer's exit condition keys off
    /// this rather than "queues look empty": a segment the pump has
    /// popped but whose upload has not landed yet is in neither queue,
    /// and treating that instant as "drained" would end the broadcast
    /// with its last segments never reaching a playlist.
    pump_finished: bool,
    /// Set by the pump, under the same lock as its "backlog is empty"
    /// observation, once the run is cancelled and it will pop no more.
    /// [`LiveRun::push`] refuses from then on. Distinct from
    /// `pump_finished` (which additionally waits for in-flight uploads):
    /// closing intake only once uploads had drained left a window in
    /// which a push that raced `cancel()` was accepted as `Queued` and
    /// then swept up as dropped instead of published.
    intake_closed: bool,
    /// Next sequence the committer will fold into the playlist.
    next_commit: u64,
    /// Next sequence [`LiveRun::push`] hands out.
    next_seq: u64,

    /// The rolling media playlist.
    playlist: VecDeque<MediaEntry>,
    /// Media sequence number for the next listed segment.
    next_number: u64,
    /// `#EXT-X-DISCONTINUITY-SEQUENCE`: discontinuities that have
    /// already slid out of the playlist window.
    discontinuity_sequence: u64,
    /// `#EXT-X-MAP` currently in force.
    init_uri: Option<String>,
    /// A gap (drop or failure) since the last listed segment, so the
    /// next one that lands starts a new continuous run.
    gap_since_last_listed: bool,

    segments_pushed: u64,
    segments_published: u64,
    /// Media segments that made it into a *published* playlist, i.e.
    /// that a viewer could actually play. Distinct from
    /// `segments_published`: a segment whose initialization segment
    /// never landed uploads fine and is still unplayable.
    segments_listed: u64,
    segments_failed: u64,
    segments_dropped: u64,
    bytes_published: u64,
    chunks_published: u64,
    playlists_published: u64,
    feed_updates: u64,
    /// Next sequence-feed index to write. Only advances on a SOC write
    /// that actually landed, so a failed update is retried at the same
    /// index rather than leaving a hole the finder would stop at.
    feed_index: u64,
    channel_reference: Option<String>,
    /// A `POST /feeds` is already in flight. The start-time attempt and
    /// the committer's retry would otherwise both fire while the first
    /// one is still on the wire — harmless (the manifest is
    /// content-addressed) but a wasted upload on the live path.
    channel_manifest_in_flight: bool,
    playlist_reference: Option<String>,

    publish_ms: VecDeque<u64>,
    lag_ms: VecDeque<u64>,
    lag_ms_max: u64,
    /// **The live edge**: the capture instant of the newest media
    /// segment a viewer can actually play, i.e. the one carried by the
    /// last feed update that *landed*. Its age is the live-edge lag, so
    /// it keeps growing while updates fail — unlike the lag of the last
    /// landed update, which freezes the moment they stop landing.
    ///
    /// Bootstrapped in [`PublisherState::advance`] with the first
    /// committed segment, so a broadcast whose very first feed update
    /// never lands measures its lag from the content nobody saw rather
    /// than from nothing at all.
    live_edge: Option<Instant>,
    /// [`PublisherState::live_edge_lag_ms`] frozen when the run
    /// finished, so a report read later does not keep ageing.
    lag_frozen_ms: Option<u64>,
    /// When the run finished, so the duration (and the throughput
    /// figures divided by it) freezes with the broadcast — the same
    /// principle as `lag_frozen_ms`: a report read a minute later must
    /// describe the broadcast, not how long the host waited to ask.
    ended_at: Option<Instant>,
    /// Longest media segment ever listed, in ms. Drives
    /// `#EXT-X-TARGETDURATION`, which RFC 8216 §6.2.1 says MUST NOT
    /// change across playlist reloads — so it is a run-max that can
    /// only ratchet up (a keyframe-aligned cut routinely overruns the
    /// configured target), never drop back when the long segment slides
    /// out of the window.
    longest_listed_ms: u32,

    errors: Vec<String>,
    last_error: Option<String>,
    error_count: u64,
    peers: u32,
    finished: bool,
}

impl PublisherState {
    fn record_error(&mut self, message: String) {
        self.error_count += 1;
        if self.errors.len() < 8 {
            self.errors.push(message.clone());
        }
        self.last_error = Some(message);
    }

    fn record_latency(queue: &mut VecDeque<u64>, value: u64) {
        if queue.len() == LATENCY_SAMPLES {
            queue.pop_front();
        }
        queue.push_back(value);
    }

    /// A feed update landed, making `captured_at`'s segment playable:
    /// record its latency and move the live edge up to it.
    fn note_lag(&mut self, captured_at: Instant) {
        let value = ms(captured_at.elapsed());
        Self::record_latency(&mut self.lag_ms, value);
        self.lag_ms_max = self.lag_ms_max.max(value);
        self.live_edge = Some(captured_at);
    }

    /// How far behind live a viewer is **right now**: the age of the
    /// newest segment a landed feed update made playable.
    ///
    /// Deliberately not "the lag of the last update that landed": that
    /// figure only moves when an update succeeds, so once the feed
    /// stops updating — a failing `POST /soc`, a playlist that will not
    /// upload — it freezes at its last good value and the badge stays
    /// green while the viewer is stuck on an old playlist. This one
    /// grows for exactly as long as nothing new reaches a viewer.
    fn live_edge_lag_ms(&self) -> u64 {
        if let Some(frozen) = self.lag_frozen_ms {
            return frozen;
        }
        self.live_edge.map_or(0, |at| ms(at.elapsed()))
    }

    /// Fold every already-finished outcome from `next_commit` forward
    /// into the playlist. Returns the newest committed media segment's
    /// capture instant when the playlist changed (that segment's lag is
    /// only known once the feed update lands), `None` when it didn't.
    fn advance(&mut self, window: usize) -> Option<Instant> {
        let mut newest: Option<Instant> = None;
        while let Some(outcome) = self.done.remove(&self.next_commit) {
            self.next_commit += 1;
            match outcome {
                Committed::Init { uri } => {
                    // A fresh initialization segment means a fresh
                    // encoder timeline: whatever follows is discontinuous
                    // with what came before.
                    self.init_uri = Some(uri);
                    if !self.playlist.is_empty() {
                        self.gap_since_last_listed = true;
                    }
                }
                Committed::Dropped | Committed::Failed(SegmentKind::Media) => {
                    self.gap_since_last_listed = true;
                }
                Committed::Failed(SegmentKind::Init) => {
                    // Listing later segments under the *previous*
                    // writer's map would hand a player an initialization
                    // segment that does not describe them. Better a
                    // shorter playlist than an unplayable one: nothing
                    // is listed again until a fresh init lands.
                    self.init_uri = None;
                    self.gap_since_last_listed = true;
                }
                Committed::Media {
                    uri,
                    duration_ms,
                    discontinuity,
                    captured_at,
                } => {
                    let Some(init_uri) = self.init_uri.clone() else {
                        // A media segment whose initialization segment
                        // never landed is unplayable; list nothing and
                        // leave the gap flag set.
                        self.gap_since_last_listed = true;
                        continue;
                    };
                    let discontinuous = discontinuity
                        || self.gap_since_last_listed
                        || self
                            .playlist
                            .back()
                            .is_some_and(|prev| prev.init_uri != init_uri);
                    self.gap_since_last_listed = false;
                    self.segments_listed += 1;
                    self.longest_listed_ms = self.longest_listed_ms.max(duration_ms);
                    self.playlist.push_back(MediaEntry {
                        number: self.next_number,
                        uri,
                        duration_ms,
                        discontinuity: discontinuous && self.next_number > 0,
                        init_uri,
                    });
                    self.next_number += 1;
                    // Start the live-edge clock at the first segment
                    // that could have been seen. Later segments do not
                    // move it — only a landed feed update does, in
                    // `note_lag` — because a segment nobody can resolve
                    // yet is not the live edge.
                    self.live_edge.get_or_insert(captured_at);
                    while self.playlist.len() > window {
                        if let Some(evicted) = self.playlist.pop_front() {
                            if evicted.discontinuity {
                                self.discontinuity_sequence += 1;
                            }
                        }
                    }
                    newest = Some(captured_at);
                }
            }
        }
        newest
    }

    /// Render the current playlist. `endlist` closes the broadcast.
    fn render_playlist(&self, target_ms: u32, endlist: bool) -> String {
        // Run-max, not window-max: see `longest_listed_ms`.
        let target_s = self.longest_listed_ms.max(target_ms).div_ceil(1000).max(1);
        use std::fmt::Write as _;
        let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:7\n");
        // Writing into a `String` is infallible, so the `write!` results
        // are deliberately dropped rather than unwrapped.
        let _ = writeln!(out, "#EXT-X-TARGETDURATION:{target_s}");
        let _ = writeln!(
            out,
            "#EXT-X-MEDIA-SEQUENCE:{}",
            self.playlist.front().map_or(self.next_number, |e| e.number),
        );
        if self.discontinuity_sequence > 0 {
            let _ = writeln!(
                out,
                "#EXT-X-DISCONTINUITY-SEQUENCE:{}",
                self.discontinuity_sequence,
            );
        }
        let mut current_map: Option<&str> = None;
        for entry in &self.playlist {
            if entry.discontinuity {
                out.push_str("#EXT-X-DISCONTINUITY\n");
            }
            if current_map != Some(entry.init_uri.as_str()) {
                let _ = writeln!(out, "#EXT-X-MAP:URI=\"{}\"", entry.init_uri);
                current_map = Some(entry.init_uri.as_str());
            }
            let _ = writeln!(
                out,
                "#EXTINF:{:.3},\n{}",
                f64::from(entry.duration_ms) / 1000.0,
                entry.uri,
            );
        }
        if endlist {
            out.push_str("#EXT-X-ENDLIST\n");
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Snapshot / report
// ---------------------------------------------------------------------------

/// Live progress of a broadcast, for the on-screen indicator.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PublisherSnapshot {
    pub running: bool,
    pub elapsed_s: f64,
    pub channel: String,
    /// Feed topic, hex. With `owner` this is the address any bee gateway
    /// resolves the live playlist from (`GET /feeds/{owner}/{topic}`).
    pub topic: String,
    pub owner: String,
    /// Feed-manifest reference created at start (`POST /feeds`) — the
    /// single reference that identifies the channel to a viewer.
    pub channel_reference: String,
    /// Reference of the most recently published playlist.
    pub playlist_reference: String,
    /// Sequence-feed index of the next update (i.e. how many updates
    /// have landed).
    pub feed_index: u64,
    pub segments_pushed: u64,
    pub segments_published: u64,
    /// Of those, the ones that reached a published playlist — what a
    /// viewer could actually play.
    pub segments_listed: u64,
    pub segments_failed: u64,
    /// Segments the live-edge discipline dropped rather than falling
    /// further behind.
    pub segments_dropped: u64,
    pub bytes_published: u64,
    pub playlists_published: u64,
    /// Publish latency of one `POST /bzz`, milliseconds.
    pub publish_ms_p50: u64,
    pub publish_ms_p95: u64,
    /// **The live-edge lag**: how far behind live a viewer is right
    /// now, i.e. the age of the newest segment a landed feed update
    /// made playable. This is what the on-screen indicator shows.
    ///
    /// It is an age, not the latency of the last update that landed, so
    /// a broadcast whose feed updates stop landing keeps climbing here
    /// instead of freezing at its last good figure while the segments
    /// go on uploading.
    pub lag_ms: u64,
    /// Highest capture → playable latency any single update recorded.
    pub lag_ms_max: u64,
    /// `lag_ms` inside the three-segment budget the stage-1 predicate
    /// uses. The indicator turns from "live" to "behind" on this.
    pub keeping_up: bool,
    pub sustained_mbit_s: f64,
    pub peers: u32,
    pub last_error: String,
    pub error_count: u64,
}

/// Final result of a broadcast, returned by the stop call.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PublisherReport {
    pub channel: String,
    pub topic: String,
    pub owner: String,
    pub channel_reference: String,
    pub playlist_reference: String,
    pub notes: String,
    pub target_bitrate_kbps: u32,
    pub segment_ms: u32,
    pub max_in_flight: usize,
    /// Broadcast duration. Frozen when the run finished (like
    /// `lag_ms_final`), so it — and the sustained figures divided by
    /// it — stays put no matter how much later the report is read.
    pub duration_s: f64,
    pub segments_pushed: u64,
    pub segments_published: u64,
    /// Of those, the ones that reached a published playlist. This — not
    /// `segments_published` — is what [`PublisherReport::kept_up`]
    /// counts, because a segment that uploaded but never got listed was
    /// never playable.
    pub segments_listed: u64,
    pub segments_failed: u64,
    pub segments_dropped: u64,
    pub bytes_published: u64,
    pub chunks_published: u64,
    pub playlists_published: u64,
    pub feed_updates: u64,
    pub sustained_mbit_s: f64,
    pub sustained_chunks_s: f64,
    pub publish_ms_p50: u64,
    pub publish_ms_p95: u64,
    pub publish_ms_max: u64,
    pub lag_ms_p50: u64,
    pub lag_ms_p95: u64,
    pub lag_ms_max: u64,
    /// The live-edge lag where the broadcast ended: the age of the
    /// newest playable segment at stop, frozen there so a report read
    /// later still describes the broadcast.
    pub lag_ms_final: u64,
    /// Every media segment reached a viewer-visible playlist and the
    /// live edge ended inside the lag budget — the live-edge equivalent
    /// of the bench's `keeps_up`.
    pub kept_up: bool,
    pub errors: Vec<String>,
    pub error_count: u64,
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// A broadcast in flight. Held by the FFI handle so the host can push
/// segments, poll progress and stop it.
pub struct LiveRun {
    config: PublisherConfig,
    topic: [u8; 32],
    owner: [u8; 20],
    state: Arc<Mutex<PublisherState>>,
    cancel: Arc<AtomicBool>,
    push_notify: Arc<Notify>,
    commit_notify: Arc<Notify>,
    started_at: Instant,
}

impl LiveRun {
    /// Hand a finished capture segment to the publisher. Never blocks:
    /// the capture pipeline must not be stalled by the uplink, which is
    /// what the drop-oldest backlog is for.
    ///
    /// Call order is the broadcast order: the sequence handed out here
    /// is the commit order, which fixes both the playlist order and the
    /// `#EXT-X-MAP` a media segment is listed under. Callers must push
    /// in capture order.
    #[must_use]
    pub fn push(
        &self,
        kind: SegmentKind,
        data: Vec<u8>,
        duration_ms: u32,
        discontinuity: bool,
    ) -> PushOutcome {
        if self.cancel.load(Ordering::SeqCst) {
            return PushOutcome::Closed;
        }
        self.enqueue(kind, data, duration_ms, discontinuity)
    }

    /// [`Self::push`] past its cancel check. Split out so tests can
    /// drive the push that raced `cancel()` deterministically.
    fn enqueue(
        &self,
        kind: SegmentKind,
        data: Vec<u8>,
        duration_ms: u32,
        discontinuity: bool,
    ) -> PushOutcome {
        let mut dropped_oldest = false;
        {
            let mut state = lock(&self.state);
            // `cancel()` can land between the check above and this lock.
            // Once the pump has closed intake nothing will ever pop
            // `pending` again, so enqueueing now would orphan the
            // segment. The pump closes intake under this same lock, in
            // the same critical section where it sees `pending` empty,
            // so the race has exactly two outcomes: enqueue before that
            // (and the pump pops and publishes it, as stop promises) or
            // observe the flag here and refuse.
            if state.intake_closed {
                return PushOutcome::Closed;
            }
            let seq = state.next_seq;
            state.next_seq += 1;
            state.segments_pushed += 1;
            state.pending.push_back(Pending {
                seq,
                kind,
                data,
                duration_ms,
                discontinuity,
                captured_at: Instant::now(),
            });
            // Drop-oldest: the backlog is bounded, and when it overflows
            // the *oldest* pending segment is the one least worth
            // sending — it is already the furthest behind live. An
            // initialization segment is never dropped: without it every
            // later media segment is unplayable.
            while state.pending.len() > self.config.max_backlog {
                let Some(index) = state
                    .pending
                    .iter()
                    .position(|p| p.kind == SegmentKind::Media)
                else {
                    break;
                };
                let Some(victim) = state.pending.remove(index) else {
                    break;
                };
                state.segments_dropped += 1;
                state.done.insert(victim.seq, Committed::Dropped);
                dropped_oldest = true;
            }
        }
        self.push_notify.notify_one();
        if dropped_oldest {
            self.commit_notify.notify_one();
            PushOutcome::QueuedDroppingOldest
        } else {
            PushOutcome::Queued
        }
    }

    /// Elapsed broadcast time: still ticking while the run is live,
    /// frozen at `ended_at` once it finished.
    fn elapsed_s(&self, state: &PublisherState) -> f64 {
        state
            .ended_at
            .unwrap_or_else(Instant::now)
            .duration_since(self.started_at)
            .as_secs_f64()
    }

    /// Live progress. Cheap: locks the state, folds, releases.
    #[must_use]
    pub fn snapshot(&self) -> PublisherSnapshot {
        let state = lock(&self.state);
        let elapsed = self.elapsed_s(&state);
        let mut publish_ms: Vec<u64> = state.publish_ms.iter().copied().collect();
        publish_ms.sort_unstable();
        PublisherSnapshot {
            running: !state.finished,
            elapsed_s: elapsed,
            channel: self.config.channel.clone(),
            topic: hex::encode(self.topic),
            owner: hex::encode(self.owner),
            channel_reference: state.channel_reference.clone().unwrap_or_default(),
            playlist_reference: state.playlist_reference.clone().unwrap_or_default(),
            feed_index: state.feed_index,
            segments_pushed: state.segments_pushed,
            segments_published: state.segments_published,
            segments_listed: state.segments_listed,
            segments_failed: state.segments_failed,
            segments_dropped: state.segments_dropped,
            bytes_published: state.bytes_published,
            playlists_published: state.playlists_published,
            publish_ms_p50: percentile(&publish_ms, 50),
            publish_ms_p95: percentile(&publish_ms, 95),
            lag_ms: state.live_edge_lag_ms(),
            lag_ms_max: state.lag_ms_max,
            keeping_up: state.feed_updates > 0
                && state.live_edge_lag_ms() <= lag_budget_ms(self.config.segment_ms),
            sustained_mbit_s: throughput_mbit_s(state.bytes_published, elapsed),
            peers: state.peers,
            last_error: state.last_error.clone().unwrap_or_default(),
            error_count: state.error_count,
        }
    }

    /// Ask the loop to stop. Cooperative: already-captured segments are
    /// drained (the last seconds of a broadcast are worth publishing)
    /// and the playlist is closed with `#EXT-X-ENDLIST`.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::SeqCst);
        // `notify_one` rather than `notify_waiters`: the pump and the
        // committer each register their waiter *after* checking their
        // queue, so a cancel that lands in that gap would be missed by
        // `notify_waiters` (which stores no permit) and leave both tasks
        // asleep for good.
        self.push_notify.notify_one();
        self.commit_notify.notify_one();
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        lock(&self.state).finished
    }

    /// Final report. Safe to call while the run is still going.
    #[must_use]
    pub fn report(&self) -> PublisherReport {
        let state = lock(&self.state);
        let elapsed = self.elapsed_s(&state);
        let mut publish_ms: Vec<u64> = state.publish_ms.iter().copied().collect();
        let mut lag_ms: Vec<u64> = state.lag_ms.iter().copied().collect();
        publish_ms.sort_unstable();
        lag_ms.sort_unstable();
        let media_pushed = state
            .segments_published
            .saturating_add(state.segments_failed)
            .saturating_add(state.segments_dropped);
        PublisherReport {
            channel: self.config.channel.clone(),
            topic: hex::encode(self.topic),
            owner: hex::encode(self.owner),
            channel_reference: state.channel_reference.clone().unwrap_or_default(),
            playlist_reference: state.playlist_reference.clone().unwrap_or_default(),
            notes: self.config.notes.clone(),
            target_bitrate_kbps: self.config.bitrate_kbps,
            segment_ms: self.config.segment_ms,
            max_in_flight: self.config.max_in_flight,
            duration_s: elapsed,
            segments_pushed: state.segments_pushed,
            segments_published: state.segments_published,
            segments_listed: state.segments_listed,
            segments_failed: state.segments_failed,
            segments_dropped: state.segments_dropped,
            bytes_published: state.bytes_published,
            chunks_published: state.chunks_published,
            playlists_published: state.playlists_published,
            feed_updates: state.feed_updates,
            sustained_mbit_s: throughput_mbit_s(state.bytes_published, elapsed),
            sustained_chunks_s: if elapsed > 0.0 {
                state.chunks_published as f64 / elapsed
            } else {
                0.0
            },
            publish_ms_p50: percentile(&publish_ms, 50),
            publish_ms_p95: percentile(&publish_ms, 95),
            publish_ms_max: publish_ms.last().copied().unwrap_or(0),
            lag_ms_p50: percentile(&lag_ms, 50),
            lag_ms_p95: percentile(&lag_ms, 95),
            lag_ms_max: state.lag_ms_max,
            lag_ms_final: state.live_edge_lag_ms(),
            // Same shape as the stage-1 predicate, scoped to what a
            // viewer saw: every media segment that was captured reached
            // a published playlist, at least one feed update landed, and
            // the live edge finished inside the lag budget.
            //
            // The count is `segments_listed`, not `segments_published`:
            // a segment whose initialization segment never landed
            // uploads perfectly well and is still unplayable, so
            // counting uploads here would let that pass as "kept up".
            //
            // The lag is the *live edge*, not the last landed update's
            // latency: segments keep uploading and listing while the
            // feed is stuck, so a frozen latency would call a broadcast
            // no viewer could follow "kept up".
            kept_up: state.feed_updates > 0
                && state.segments_listed > 0
                && state.segments_listed == media_pushed
                && state.live_edge_lag_ms() <= lag_budget_ms(self.config.segment_ms),
            errors: state.errors.clone(),
            error_count: state.error_count,
        }
    }
}

/// Bound on the acceptable live-edge lag: three segment durations —
/// the same budget the stage-1 bench verdict uses.
const fn lag_budget_ms(segment_ms: u32) -> u64 {
    segment_ms as u64 * 3
}

fn throughput_mbit_s(bytes: u64, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        return 0.0;
    }
    bytes as f64 * 8.0 / seconds / 1_000_000.0
}

/// Start a broadcast on `runtime`, returning the handle immediately.
pub fn start(
    runtime: &tokio::runtime::Handle,
    config: PublisherConfig,
    signing_secret: [u8; 32],
    owner: [u8; 20],
    status_rx: Option<watch::Receiver<StatusSnapshot>>,
) -> Result<Arc<LiveRun>, PublisherError> {
    config.validate()?;
    let topic = config.resolve_topic()?;
    let target = Target::parse(&config.gateway).map_err(PublisherError::Config)?;
    let batch = parse_hex32(&config.batch_id)
        .map_err(|e| PublisherError::Config(format!("batch_id {e}")))?;

    let state = Arc::new(Mutex::new(PublisherState::default()));
    let cancel = Arc::new(AtomicBool::new(false));
    let push_notify = Arc::new(Notify::new());
    let commit_notify = Arc::new(Notify::new());
    let run = Arc::new(LiveRun {
        config: config.clone(),
        topic,
        owner,
        state: Arc::clone(&state),
        cancel: Arc::clone(&cancel),
        push_notify: Arc::clone(&push_notify),
        commit_notify: Arc::clone(&commit_notify),
        started_at: Instant::now(),
    });

    let ctx = Arc::new(RunCtx {
        config,
        topic,
        owner,
        batch,
        signing_secret,
        target,
        state,
        cancel,
        push_notify,
        commit_notify,
        status_rx,
        manifest_task: Mutex::new(None),
    });
    runtime.spawn(async move { drive(ctx).await });
    Ok(run)
}

struct RunCtx {
    config: PublisherConfig,
    topic: [u8; 32],
    owner: [u8; 20],
    batch: [u8; 32],
    signing_secret: [u8; 32],
    target: Target,
    state: Arc<Mutex<PublisherState>>,
    cancel: Arc<AtomicBool>,
    push_notify: Arc<Notify>,
    commit_notify: Arc<Notify>,
    status_rx: Option<watch::Receiver<StatusSnapshot>>,
    /// The one channel-manifest attempt that may be running (start-time
    /// or a committer retry). Kept here rather than detached so
    /// [`drive`] can join it before marking the run finished: a
    /// reference that lands after the final report would be lost to
    /// the host, and it is the only shareable one.
    manifest_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl RunCtx {
    /// Start a channel-manifest attempt unless one is already running.
    fn spawn_channel_manifest(self: &Arc<Self>) {
        let mut slot = lock(&self.manifest_task);
        if slot.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        let ctx = Arc::clone(self);
        *slot = Some(tokio::spawn(async move {
            ensure_channel_manifest(&ctx).await;
        }));
    }

    fn cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

/// How often the peer count is sampled for the progress view.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

async fn drive(ctx: Arc<RunCtx>) {
    let sampler = spawn_peer_sampler(&ctx);
    // The channel's feed manifest: one immutable reference a viewer can
    // be handed (`bzz://<ref>`) that resolves through any bee gateway to
    // whatever the latest feed update points at.
    //
    // Concurrent with the loop, not before it: this is a real upload and
    // can sit on the 60 s publish deadline if the peer set is still
    // warming up, and the first minute of a broadcast must not be spent
    // waiting for a reference nobody has been given yet. It is
    // best-effort for the same reason — the feed updates are what carry
    // the stream — and the committer retries it until it lands.
    ctx.spawn_channel_manifest();

    let pump = tokio::spawn(pump(Arc::clone(&ctx)));
    let committer = tokio::spawn(commit_loop(Arc::clone(&ctx)));
    let _ = pump.await;
    let _ = committer.await;
    // Join, don't abort, the manifest attempt still on the wire: it ran
    // alongside the drain, so this rarely waits, and a reference that
    // lands after `finished` would be missing from the final report. The
    // committer has exited, so no new attempt can be spawned behind it.
    let manifest = lock(&ctx.manifest_task).take();
    if let Some(manifest) = manifest {
        let _ = manifest.await;
    }

    if let Some(sampler) = sampler {
        sampler.abort();
    }
    // The lag is an age, so a report read a minute after the broadcast
    // ended must still describe the broadcast rather than how long the
    // host waited to ask. The committer normally freezes it with the
    // last content; this is the fallback for a run that never got that
    // far.
    {
        let mut state = lock(&ctx.state);
        let lag = state.live_edge_lag_ms();
        state.lag_frozen_ms.get_or_insert(lag);
        // Duration freezes with the run for the same reason the lag
        // does: `duration_s` and the throughput figures divided by it
        // must describe the broadcast, not the wait before the report
        // was read.
        state.ended_at.get_or_insert_with(Instant::now);
        state.finished = true;
    }
}

fn spawn_peer_sampler(ctx: &RunCtx) -> Option<tokio::task::JoinHandle<()>> {
    let status_rx = ctx.status_rx.clone()?;
    let state = Arc::clone(&ctx.state);
    Some(tokio::spawn(async move {
        loop {
            let peers = status_rx.borrow().peers.connected;
            lock(&state).peers = peers;
            tokio::time::sleep(SAMPLE_INTERVAL).await;
        }
    }))
}

/// Take pending segments and publish them through a bounded in-flight
/// window. Returns once the run is cancelled *and* the backlog has
/// drained — the tail of a broadcast is real content, not a truncated
/// measurement.
async fn pump(ctx: Arc<RunCtx>) {
    let window = Arc::new(Semaphore::new(ctx.config.max_in_flight));
    let mut in_flight: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    loop {
        // Register interest *before* looking at the queue, so a push
        // that lands between the check and the await still wakes us.
        let notified = ctx.push_notify.notified();
        tokio::pin!(notified);

        let next = {
            let mut state = lock(&ctx.state);
            let next = state.pending.pop_front();
            if next.is_none() && ctx.cancelled() {
                // Close intake atomically with seeing the backlog
                // empty: every push accepted before this point is in
                // `pending` and has been popped for publishing; every
                // later one observes the flag and is refused.
                state.intake_closed = true;
                break;
            }
            next
        };
        let Some(pending) = next else {
            notified.await;
            continue;
        };

        let Ok(permit) = Arc::clone(&window).acquire_owned().await else {
            break;
        };
        in_flight.retain(|task| !task.is_finished());
        let ctx2 = Arc::clone(&ctx);
        in_flight.push(tokio::spawn(async move {
            let _permit = permit;
            publish_pending(&ctx2, pending).await;
        }));
    }
    for task in in_flight {
        let _ = task.await;
    }
    // The pump is the only producer of commit work; publish that it is
    // done and wake the committer so it can fold in the final segments
    // and close the playlist. Intake closed with `pending` empty, so
    // there is nothing left to sweep.
    {
        let mut state = lock(&ctx.state);
        debug_assert!(state.pending.is_empty());
        state.pump_finished = true;
    }
    ctx.commit_notify.notify_one();
}

async fn publish_pending(ctx: &RunCtx, pending: Pending) {
    let name = match pending.kind {
        SegmentKind::Init => format!("init-{}.mp4", pending.seq),
        SegmentKind::Media => format!("seg-{}.m4s", pending.seq),
    };
    let content_type = match pending.kind {
        SegmentKind::Init => "video/mp4",
        SegmentKind::Media => "video/iso.segment",
    };
    let bytes = pending.data.len() as u64;
    let started = Instant::now();
    let result = publish_bzz(&ctx.target, ctx.batch, &name, content_type, &pending.data).await;
    let elapsed = started.elapsed();

    {
        let mut state = lock(&ctx.state);
        match result {
            Ok(reference) => {
                PublisherState::record_latency(&mut state.publish_ms, ms(elapsed));
                let uri = bzz_uri(&reference, &name);
                match pending.kind {
                    SegmentKind::Init => {
                        state.done.insert(pending.seq, Committed::Init { uri });
                    }
                    SegmentKind::Media => {
                        state.segments_published += 1;
                        state.bytes_published += bytes;
                        state.chunks_published += data_chunk_count(bytes);
                        state.done.insert(
                            pending.seq,
                            Committed::Media {
                                uri,
                                duration_ms: pending.duration_ms,
                                discontinuity: pending.discontinuity,
                                captured_at: pending.captured_at,
                            },
                        );
                    }
                }
            }
            Err(message) => {
                if pending.kind == SegmentKind::Media {
                    state.segments_failed += 1;
                }
                state.record_error(format!(
                    "{} segment {}: {message}",
                    pending.kind.as_str(),
                    pending.seq,
                ));
                state
                    .done
                    .insert(pending.seq, Committed::Failed(pending.kind));
            }
        }
    }
    ctx.commit_notify.notify_one();
}

/// Fold finished segments into the playlist **in capture order**, then
/// republish the playlist and point the feed at it. Single task, so
/// feed indices are written strictly in order.
async fn commit_loop(ctx: Arc<RunCtx>) {
    loop {
        let notified = ctx.commit_notify.notified();
        tokio::pin!(notified);

        let newest = {
            let mut state = lock(&ctx.state);
            state.advance(ctx.config.playlist_window)
        };
        if let Some(captured_at) = newest {
            publish_playlist(&ctx, false, Some(captured_at)).await;
            continue;
        }
        // Nothing new. If the pump has finished (backlog drained *and*
        // every upload awaited) and every sequence has been folded in,
        // close the broadcast out.
        let drained = {
            let state = lock(&ctx.state);
            state.pump_finished && state.done.is_empty()
        };
        if drained {
            break;
        }
        notified.await;
    }
    // Freeze the live-edge lag *with the last content*, before the
    // closing playlist: what follows is bookkeeping, and a slow final
    // upload must not read as a broadcast that fell behind.
    {
        let mut state = lock(&ctx.state);
        let lag = state.live_edge_lag_ms();
        state.lag_frozen_ms = Some(lag);
    }
    // Final playlist: `#EXT-X-ENDLIST` turns the live channel into a
    // finished recording for anyone still resolving the feed.
    if !lock(&ctx.state).playlist.is_empty() {
        publish_playlist(&ctx, true, None).await;
    }
}

/// Upload the current playlist and publish its reference as the next
/// feed update. `captured_at`, when present, is the newest committed
/// segment — its live-edge lag is only known once the feed update lands.
async fn publish_playlist(ctx: &Arc<RunCtx>, endlist: bool, captured_at: Option<Instant>) {
    let body = {
        let state = lock(&ctx.state);
        state.render_playlist(ctx.config.segment_ms, endlist)
    };
    let playlist_ref = match publish_bzz(
        &ctx.target,
        ctx.batch,
        PLAYLIST_NAME,
        "application/vnd.apple.mpegurl",
        body.as_bytes(),
    )
    .await
    {
        Ok(reference) => reference,
        Err(message) => {
            lock(&ctx.state).record_error(format!("playlist: {message}"));
            return;
        }
    };
    let reference = match parse_hex32(&playlist_ref) {
        Ok(r) => r,
        Err(e) => {
            lock(&ctx.state).record_error(format!("playlist reference {e}"));
            return;
        }
    };
    {
        let mut state = lock(&ctx.state);
        state.playlists_published += 1;
        state.playlist_reference = Some(playlist_ref);
    }

    let index = lock(&ctx.state).feed_index;
    match publish_feed_update(ctx, index, &reference).await {
        Ok(()) => {
            let mut state = lock(&ctx.state);
            state.feed_index += 1;
            state.feed_updates += 1;
            if let Some(captured_at) = captured_at {
                state.note_lag(captured_at);
            }
        }
        Err(message) => {
            lock(&ctx.state).record_error(format!("feed update {index}: {message}"));
        }
    }
    // A channel manifest that could not be created at start is retried
    // here, so a broadcast that began before the peer set was warm still
    // ends up with a shareable reference — but *spawned*, never awaited:
    // this runs on the committer, and a `POST /feeds` hanging on its
    // 60 s deadline must not stall the playlist and feed updates for
    // segments that already landed. One attempt at a time, and `drive`
    // joins it before the run reports finished, so its reference lands
    // in the final report. Not after the closing playlist: a fresh
    // attempt there would add a whole publish deadline to stop's drain
    // (an attempt already in flight is still joined).
    if !endlist && lock(&ctx.state).channel_reference.is_none() {
        ctx.spawn_channel_manifest();
    }
}

/// Playlist file name. Constant so every republish lands at the same
/// manifest path and a viewer's URI stays stable across updates.
const PLAYLIST_NAME: &str = "stream.m3u8";

/// Create the channel's feed manifest (`POST /feeds/{owner}/{topic}`,
/// bee-js `createFeedManifest`). Idempotent in effect: the manifest is
/// content-addressed, so re-creating it yields the same reference.
async fn ensure_channel_manifest(ctx: &RunCtx) {
    {
        let mut state = lock(&ctx.state);
        if state.channel_reference.is_some() || state.channel_manifest_in_flight {
            return;
        }
        state.channel_manifest_in_flight = true;
    }
    let path = format!(
        "{}/feeds/{}/{}",
        ctx.target.prefix,
        hex::encode(ctx.owner),
        hex::encode(ctx.topic),
    );
    let headers = [("swarm-postage-batch-id".to_string(), hex::encode(ctx.batch))];
    let outcome = post_reference(&ctx.target, &path, &headers, &[]).await;
    let mut state = lock(&ctx.state);
    state.channel_manifest_in_flight = false;
    match outcome {
        Ok(reference) => state.channel_reference = Some(reference),
        Err(message) => state.record_error(format!("channel manifest: {message}")),
    }
}

/// Write one sequence-feed update: a single-owner chunk at
/// `id = keccak256(topic ‖ index_be8)` whose payload is bee's v1 update
/// layout `timestamp_be8 ‖ reference`.
///
/// This is the shape bee-js's `FeedWriter.upload` produces and every bee
/// version's feed getter resolves, which is what lets a **public** bee
/// gateway serve the channel even though the segments were pushed from a
/// phone.
async fn publish_feed_update(ctx: &RunCtx, index: u64, reference: &[u8; 32]) -> Result<(), String> {
    let id = ant_retrieval::sequence_update_id(&ctx.topic, index);
    let mut payload = Vec::with_capacity(8 + 32);
    payload.extend_from_slice(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_be_bytes(),
    );
    payload.extend_from_slice(reference);

    // Inner content-addressed chunk: `span(8 LE) ‖ payload`, which is
    // both what the SOC signature covers and what `POST /soc` takes as
    // its body.
    let (cac_address, cac_wire) =
        ant_crypto::cac_new(&payload).ok_or_else(|| "feed payload too large".to_string())?;
    // Bee signs `keccak256(id ‖ inner_cac_address)` through its default
    // (EIP-191-wrapping) signer — see `bee/pkg/soc/soc.go::Sign` and
    // `ant_crypto::soc_valid`, which recovers the owner the same way.
    let mut digest_input = [0u8; 64];
    digest_input[..32].copy_from_slice(&id);
    digest_input[32..].copy_from_slice(&cac_address);
    let signature =
        ant_crypto::sign_handshake_data(&ctx.signing_secret, &ant_crypto::keccak256(&digest_input))
            .map_err(|e| format!("sign feed update: {e}"))?;

    let path = format!(
        "{}/soc/{}/{}?sig={}",
        ctx.target.prefix,
        hex::encode(ctx.owner),
        hex::encode(id),
        hex::encode(signature),
    );
    let headers = [
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("swarm-postage-batch-id".to_string(), hex::encode(ctx.batch)),
    ];
    post_reference(&ctx.target, &path, &headers, &cac_wire).await?;
    Ok(())
}

/// The URI a viewer's HLS client fetches an uploaded object from.
/// Root-relative so the same playlist works against the in-process
/// gateway, `antd`, and a public bee gateway (all serve `/bzz/` at the
/// root of their API).
fn bzz_uri(reference: &str, name: &str) -> String {
    format!("/bzz/{reference}/{name}")
}

// ---------------------------------------------------------------------------
// Publish path
// ---------------------------------------------------------------------------

/// Publish one body with `POST /bzz`, returning its reference. This is
/// the call stage 1 measured; the live loop and the bench both go
/// through it.
pub(crate) async fn publish_bzz(
    target: &Target,
    batch: [u8; 32],
    name: &str,
    content_type: &str,
    payload: &[u8],
) -> Result<String, String> {
    let path = format!("{}/bzz?name={name}", target.prefix);
    let headers = [
        ("content-type".to_string(), content_type.to_string()),
        ("swarm-postage-batch-id".to_string(), hex::encode(batch)),
    ];
    post_reference(target, &path, &headers, payload).await
}

/// `POST` something the gateway answers with bee's
/// `{"reference":"<hex>"}` and hand back that reference.
async fn post_reference(
    target: &Target,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<String, String> {
    let response = tokio::time::timeout(PUBLISH_TIMEOUT, http_post(target, path, headers, body))
        .await
        .map_err(|_| format!("timed out after {}s", PUBLISH_TIMEOUT.as_secs()))??;
    if response.status != 201 {
        return Err(format!(
            "gateway returned {} {}",
            response.status,
            String::from_utf8_lossy(&response.body).trim(),
        ));
    }
    let parsed: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("gateway response is not JSON: {e}"))?;
    parsed
        .get("reference")
        .and_then(serde_json::Value::as_str)
        .map(|r| r.trim_start_matches("0x").to_ascii_lowercase())
        .ok_or_else(|| "gateway response carries no reference".to_string())
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
pub(crate) struct Target {
    pub(crate) authority: String,
    /// Path prefix, without a trailing slash (`""` for a bare host).
    pub(crate) prefix: String,
}

impl Target {
    pub(crate) fn parse(url: &str) -> Result<Self, String> {
        let rest = url.trim().strip_prefix("http://").ok_or_else(|| {
            format!(
                "gateway must be an http:// URL (the publisher posts to a loopback gateway), got `{url}`",
            )
        })?;
        let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
        if authority.is_empty() {
            return Err(format!("gateway has no host: `{url}`"));
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

pub(crate) struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
}

pub(crate) async fn http_post(
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
pub(crate) fn parse_response(raw: &[u8]) -> Result<HttpResponse, String> {
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

    // -----------------------------------------------------------------
    // Pure helpers shared with the bench
    // -----------------------------------------------------------------

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
    fn percentiles_are_stable_on_small_samples() {
        assert_eq!(percentile(&[], 50), 0);
        assert_eq!(percentile(&[5], 95), 5);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 50), 3);
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 95), 5);
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

    // -----------------------------------------------------------------
    // Playlist assembly
    // -----------------------------------------------------------------

    fn config() -> PublisherConfig {
        PublisherConfig {
            channel: "test channel".into(),
            topic: format!("0x{}", "11".repeat(32)),
            gateway: "http://127.0.0.1:1633".into(),
            batch_id: "ab".repeat(32),
            segment_ms: 2000,
            bitrate_kbps: 900,
            max_in_flight: 4,
            max_backlog: 4,
            playlist_window: 3,
            notes: String::new(),
        }
    }

    fn media(uri: &str, discontinuity: bool) -> Committed {
        Committed::Media {
            uri: uri.into(),
            duration_ms: 2000,
            discontinuity,
            captured_at: Instant::now(),
        }
    }

    #[test]
    fn playlist_lists_segments_in_capture_order_behind_one_map() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        // Deliberately out of order: the uploads finish concurrently, the
        // playlist must not.
        state.done.insert(2, media("/bzz/cc/seg-2.m4s", false));
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        assert!(state.advance(3).is_some());

        let playlist = state.render_playlist(2000, false);
        assert!(
            playlist.starts_with("#EXTM3U\n#EXT-X-VERSION:7\n"),
            "{playlist}"
        );
        assert!(playlist.contains("#EXT-X-TARGETDURATION:2\n"));
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert_eq!(playlist.matches("#EXT-X-MAP").count(), 1);
        let seg1 = playlist.find("/bzz/bb/seg-1.m4s").unwrap();
        let seg2 = playlist.find("/bzz/cc/seg-2.m4s").unwrap();
        assert!(seg1 < seg2, "segments out of order:\n{playlist}");
        assert!(!playlist.contains("#EXT-X-ENDLIST"));
    }

    #[test]
    fn a_dropped_segment_becomes_a_discontinuity_not_a_silent_gap() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        state.done.insert(2, Committed::Dropped);
        state.done.insert(3, media("/bzz/dd/seg-3.m4s", false));
        assert!(state.advance(5).is_some());

        let playlist = state.render_playlist(2000, false);
        assert_eq!(
            playlist.matches("#EXT-X-DISCONTINUITY\n").count(),
            1,
            "{playlist}"
        );
        // The tag belongs to the segment *after* the gap.
        let tag = playlist.find("#EXT-X-DISCONTINUITY\n").unwrap();
        let after = playlist.find("/bzz/dd/seg-3.m4s").unwrap();
        let before = playlist.find("/bzz/bb/seg-1.m4s").unwrap();
        assert!(before < tag && tag < after, "{playlist}");
        // Media sequence numbering counts only listed segments.
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
    }

    #[test]
    fn a_new_init_segment_starts_a_new_map_and_discontinuity() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        // Interruption recovery: the writer restarts and emits a fresh
        // initialization segment.
        state.done.insert(
            2,
            Committed::Init {
                uri: "/bzz/cc/init-2.mp4".into(),
            },
        );
        state.done.insert(3, media("/bzz/dd/seg-3.m4s", true));
        assert!(state.advance(5).is_some());

        let playlist = state.render_playlist(2000, false);
        assert_eq!(playlist.matches("#EXT-X-MAP").count(), 2, "{playlist}");
        assert_eq!(
            playlist.matches("#EXT-X-DISCONTINUITY\n").count(),
            1,
            "{playlist}"
        );
        assert!(playlist.contains("#EXT-X-MAP:URI=\"/bzz/cc/init-2.mp4\""));
        // The old writer's last segment stays under the *old* map — the
        // reason the capture-side hand-off has to be order-preserving.
        let old_segment = playlist.find("/bzz/bb/seg-1.m4s").unwrap();
        let new_map = playlist.find("/bzz/cc/init-2.mp4").unwrap();
        assert!(old_segment < new_map, "{playlist}");
    }

    #[test]
    fn the_window_slides_and_carries_the_discontinuity_sequence() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/b1/seg-1.m4s", false));
        state.done.insert(2, Committed::Dropped);
        state.done.insert(3, media("/bzz/b3/seg-3.m4s", false));
        state.done.insert(4, media("/bzz/b4/seg-4.m4s", false));
        state.done.insert(5, media("/bzz/b5/seg-5.m4s", false));
        state.done.insert(6, media("/bzz/b6/seg-6.m4s", false));
        // Window of 2: the first two listed segments (including the one
        // carrying the discontinuity) fall out.
        assert!(state.advance(2).is_some());

        let playlist = state.render_playlist(2000, false);
        assert!(!playlist.contains("/bzz/b1/seg-1.m4s"), "{playlist}");
        // Five segments were listed (numbers 0-4); the window holds the
        // last two, so the playlist starts at number 3.
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:3\n"), "{playlist}");
        assert!(
            playlist.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"),
            "{playlist}"
        );
        // Every listed segment still carries a map.
        assert!(playlist.contains("#EXT-X-MAP:URI=\"/bzz/aa/init.mp4\""));
    }

    #[test]
    fn target_duration_never_decreases_when_a_long_segment_slides_out() {
        // RFC 8216 §6.2.1: `EXT-X-TARGETDURATION` MUST NOT change across
        // reloads. Keyframe-aligned cutting routinely overruns the
        // configured target, so a window-max would raise the tag while
        // the long segment is listed and lower it again when it slides
        // out — strict players size reload timers off it and treat that
        // as a malformed live stream.
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/b1/seg-1.m4s", false));
        state.done.insert(
            2,
            Committed::Media {
                uri: "/bzz/b2/seg-2.m4s".into(),
                duration_ms: 3400,
                discontinuity: false,
                captured_at: Instant::now(),
            },
        );
        state.advance(2);
        assert!(
            state
                .render_playlist(2000, false)
                .contains("#EXT-X-TARGETDURATION:4\n"),
            "an overrunning segment must raise the target",
        );
        // Two more nominal segments slide the 3.4 s one out of the
        // window; the tag must stay ratcheted rather than dropping back.
        state.done.insert(3, media("/bzz/b3/seg-3.m4s", false));
        state.done.insert(4, media("/bzz/b4/seg-4.m4s", false));
        state.advance(2);
        let playlist = state.render_playlist(2000, false);
        assert!(!playlist.contains("seg-2.m4s"), "{playlist}");
        assert!(playlist.contains("#EXT-X-TARGETDURATION:4\n"), "{playlist}");
    }

    #[test]
    fn media_before_its_init_segment_is_never_listed() {
        // The initialization segment failed to upload: listing the media
        // segments anyway would produce a playlist no player can start.
        let mut state = PublisherState::default();
        state.done.insert(0, Committed::Failed(SegmentKind::Init));
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        assert!(state.advance(3).is_none());
        assert!(state.playlist.is_empty());
    }

    #[test]
    fn the_final_playlist_is_closed_with_endlist() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        state.advance(3);
        assert!(state
            .render_playlist(2000, true)
            .ends_with("#EXT-X-ENDLIST\n"));
    }

    #[test]
    fn config_validation_rejects_unusable_broadcasts() {
        let mut c = config();
        c.channel = "  ".into();
        assert!(c.validate().is_err());
        let mut c = config();
        c.batch_id = String::new();
        assert!(c.validate().is_err());
        let mut c = config();
        c.max_in_flight = 0;
        assert!(c.validate().is_err());
        assert!(config().validate().is_ok());
    }

    #[test]
    fn an_omitted_topic_is_derived_per_broadcast() {
        let mut c = config();
        c.topic = String::new();
        let a = c.resolve_topic().unwrap();
        assert_ne!(a, [0u8; 32]);
        // Two derivations must differ even inside one clock tick —
        // otherwise a quick stop + restart of the same channel would
        // write its fresh index 0 over the old feed's head.
        let b = c.resolve_topic().unwrap();
        assert_ne!(a, b, "derived topics must be per-broadcast");
        // An explicit topic is honoured verbatim, with or without `0x`.
        c.topic = "cd".repeat(32);
        assert_eq!(c.resolve_topic().unwrap(), [0xcd; 32]);
        c.topic = "0xnothex".into();
        assert!(c.resolve_topic().is_err());
    }

    // -----------------------------------------------------------------
    // End-to-end against a stub gateway that speaks bee's contract and
    // validates what it is sent.
    // -----------------------------------------------------------------

    /// One request the stub gateway saw.
    #[derive(Clone, Debug)]
    struct SeenRequest {
        method_path: String,
        headers: Vec<String>,
        body: Vec<u8>,
    }

    impl SeenRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find_map(|h| h.strip_prefix(&format!("{name}: ")))
        }
    }

    #[derive(Default)]
    struct StubLog {
        seen: Vec<SeenRequest>,
        /// `POST /bzz` responses are content-addressed in the stub too
        /// (a counter, not a real BMT hash) so the publisher's playlist
        /// URIs are distinguishable.
        next_reference: u64,
        fail_bzz: bool,
        /// Refuse every feed update from now on, the segments
        /// themselves still uploading fine.
        fail_soc: bool,
        /// Refuse this many `POST /feeds` before accepting one.
        fail_feeds: u32,
        /// Extra latency on `POST /feeds` only.
        feeds_delay: Duration,
    }

    /// A gateway stub that answers bee-shaped `{"reference":...}` to
    /// `/bzz`, `/feeds` and `/soc`, recording every request.
    async fn stub_gateway(log: Arc<Mutex<StubLog>>, delay: Duration) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let log = Arc::clone(&log);
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 8192];
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
                                let mut lines = head.lines();
                                let method_path = lines
                                    .next()
                                    .unwrap_or_default()
                                    .split_whitespace()
                                    .take(2)
                                    .collect::<Vec<_>>()
                                    .join(" ");
                                let request = SeenRequest {
                                    method_path,
                                    headers: lines.map(str::to_string).collect(),
                                    body: raw[split + 4..split + 4 + want].to_vec(),
                                };
                                let is_feeds = request.method_path.contains("/feeds/");
                                let (fail, feeds_delay) = {
                                    let mut slot = lock(&log);
                                    slot.seen.push(request.clone());
                                    let fail_feeds = is_feeds && slot.fail_feeds > 0;
                                    if fail_feeds {
                                        slot.fail_feeds -= 1;
                                    }
                                    let fail = fail_feeds
                                        || (slot.fail_bzz && request.method_path.contains("/bzz"))
                                        || (slot.fail_soc && request.method_path.contains("/soc/"));
                                    (
                                        fail,
                                        if is_feeds {
                                            slot.feeds_delay
                                        } else {
                                            Duration::ZERO
                                        },
                                    )
                                };
                                tokio::time::sleep(delay + feeds_delay).await;
                                let response = if fail {
                                    let body = br#"{"message":"batch not usable"}"#;
                                    let mut r = format!(
                                        "HTTP/1.1 400 Bad Request\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                                        body.len(),
                                    )
                                    .into_bytes();
                                    r.extend_from_slice(body);
                                    r
                                } else {
                                    let reference = {
                                        let mut slot = lock(&log);
                                        slot.next_reference += 1;
                                        let mut r = [0u8; 32];
                                        r[..8].copy_from_slice(&slot.next_reference.to_be_bytes());
                                        hex::encode(r)
                                    };
                                    let body =
                                        format!("{{\"reference\":\"{reference}\"}}").into_bytes();
                                    let mut r = format!(
                                        "HTTP/1.1 201 Created\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                                        body.len(),
                                    )
                                    .into_bytes();
                                    r.extend_from_slice(&body);
                                    r
                                };
                                let _ = socket.write_all(&response).await;
                                let _ = socket.shutdown().await;
                                return;
                            }
                        }
                    }
                });
            }
        });
        addr
    }

    const TEST_SECRET: [u8; 32] = [0x2a; 32];

    fn test_owner() -> [u8; 20] {
        let sk = k256::ecdsa::SigningKey::from_bytes(&TEST_SECRET.into()).unwrap();
        ant_crypto::ethereum_address_from_public_key(sk.verifying_key())
    }

    async fn settle(run: &Arc<LiveRun>) {
        for _ in 0..400 {
            if run.is_finished() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("publisher never finished");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broadcast_publishes_segments_playlist_and_a_valid_feed_update() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        let owner = test_owner();
        let run = start(
            &tokio::runtime::Handle::current(),
            config.clone(),
            TEST_SECRET,
            owner,
            None,
        )
        .unwrap();

        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..3u32 {
            let _ = run.push(SegmentKind::Media, vec![i as u8; 4096], 2000, false);
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
        run.cancel();
        settle(&run).await;

        let report = run.report();
        assert_eq!(report.segments_published, 3, "{report:?}");
        assert_eq!(report.segments_failed, 0);
        assert!(report.feed_updates > 0);
        assert_ne!(report.channel_reference, "");
        assert!(report.kept_up, "{report:?}");
        // The lag is an age and the duration a stopwatch, both frozen
        // with the run: asking again later must still describe the
        // broadcast, not the wait.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let again = run.report();
        assert_eq!(again.lag_ms_final, report.lag_ms_final, "{again:?}");
        assert!(again.kept_up, "{again:?}");
        assert!(
            (again.duration_s - report.duration_s).abs() < 1e-9,
            "duration kept ticking after the run: {} vs {}",
            again.duration_s,
            report.duration_s,
        );
        assert!(
            (again.sustained_mbit_s - report.sustained_mbit_s).abs() < 1e-9,
            "throughput drifted after the run: {} vs {}",
            again.sustained_mbit_s,
            report.sustained_mbit_s,
        );

        let seen = lock(&log).seen.clone();
        // 1. the channel's feed manifest.
        let manifest = seen
            .iter()
            .find(|r| r.method_path.contains("/feeds/"))
            .expect("feed manifest created");
        assert!(
            manifest.method_path.contains(&format!(
                "/feeds/{}/{}",
                hex::encode(owner),
                "11".repeat(32)
            )),
            "{}",
            manifest.method_path,
        );
        // 2. segments, stamped with the batch.
        let segment = seen
            .iter()
            .find(|r| r.method_path.contains("/bzz?name=seg-"))
            .expect("segment uploaded");
        assert_eq!(
            segment.header("swarm-postage-batch-id"),
            Some("ab".repeat(32).as_str()),
        );
        assert_eq!(segment.header("content-type"), Some("video/iso.segment"));
        // 3. the playlist, as an HLS media playlist.
        let playlist = seen
            .iter()
            .rev()
            .find(|r| r.method_path.contains("/bzz?name=stream.m3u8"))
            .expect("playlist uploaded");
        assert_eq!(
            playlist.header("content-type"),
            Some("application/vnd.apple.mpegurl"),
        );
        let body = String::from_utf8(playlist.body.clone()).unwrap();
        assert!(body.starts_with("#EXTM3U"), "{body}");
        assert!(body.contains("#EXT-X-ENDLIST"), "final playlist:\n{body}");
        assert_eq!(body.matches("#EXTINF").count(), 3, "{body}");

        // 4. the feed update: a single-owner chunk ant's own validator
        //    accepts, at the bee-js sequence id for index 0.
        let soc = seen
            .iter()
            .find(|r| r.method_path.contains("/soc/"))
            .expect("feed update written");
        let (path, sig_hex) = soc.method_path.split_once("?sig=").expect("sig query");
        let id_hex = path.rsplit('/').next().unwrap();
        assert_eq!(
            id_hex,
            hex::encode(ant_retrieval::sequence_update_id(&[0x11; 32], 0)),
            "feed update must sit at keccak256(topic ‖ index_be8)",
        );
        let id: [u8; 32] = hex::decode(id_hex).unwrap().try_into().unwrap();
        let sig: [u8; 65] = hex::decode(sig_hex).unwrap().try_into().unwrap();
        let mut wire = Vec::new();
        wire.extend_from_slice(&id);
        wire.extend_from_slice(&sig);
        wire.extend_from_slice(&soc.body);
        let mut addr_input = [0u8; 52];
        addr_input[..32].copy_from_slice(&id);
        addr_input[32..].copy_from_slice(&owner);
        assert!(
            ant_crypto::soc_valid(&ant_crypto::keccak256(&addr_input), &wire),
            "the SOC ant's own gateway validates must accept our feed update",
        );
        // v1 update payload: `timestamp_be8 ‖ reference`, after the
        // 8-byte little-endian CAC span.
        assert_eq!(soc.body.len(), 8 + 8 + 32, "bee v1 feed payload shape");
        assert_eq!(
            u64::from_le_bytes(soc.body[..8].try_into().unwrap()),
            40,
            "inner CAC span covers the 40-byte update payload",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_backlog_drops_its_oldest_segment_rather_than_falling_behind() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        // Every upload takes 200 ms and only one runs at a time, so a
        // burst of segments arriving back to back with a backlog of 2
        // forces the publisher to shed rather than queue them all.
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(200)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.max_in_flight = 1;
        config.max_backlog = 2;
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();

        // Get one segment safely published first, so the gap the drops
        // create sits *between* listed segments — which is where a
        // player needs the discontinuity marker.
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![0u8; 64], 2000, false);
        for _ in 0..40 {
            if run.snapshot().segments_published > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(run.snapshot().segments_published, 1);

        let mut outcomes = Vec::new();
        for i in 1..8u32 {
            outcomes.push(run.push(SegmentKind::Media, vec![i as u8; 64], 2000, false));
        }
        assert!(
            outcomes.contains(&PushOutcome::QueuedDroppingOldest),
            "a full backlog must report the drop: {outcomes:?}",
        );
        run.cancel();
        settle(&run).await;

        let report = run.report();
        assert!(report.segments_dropped > 0, "{report:?}");
        assert_eq!(
            report.segments_published + report.segments_dropped + report.segments_failed,
            8,
            "{report:?}",
        );
        // Falling behind is not a pass, even though nothing errored.
        assert!(!report.kept_up, "{report:?}");
        // The dropped segments left a discontinuity rather than a silent
        // gap in the published playlist.
        let seen = lock(&log).seen.clone();
        let playlist = seen
            .iter()
            .rev()
            .find(|r| r.method_path.contains("stream.m3u8"))
            .expect("playlist uploaded");
        let body = String::from_utf8(playlist.body.clone()).unwrap();
        assert!(body.contains("#EXT-X-DISCONTINUITY"), "{body}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejecting_gateway_is_reported_rather_than_counted_as_published() {
        let log = Arc::new(Mutex::new(StubLog {
            fail_bzz: true,
            ..StubLog::default()
        }));
        let addr = stub_gateway(Arc::clone(&log), Duration::ZERO).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        tokio::time::sleep(Duration::from_millis(50)).await;
        run.cancel();
        settle(&run).await;

        let report = run.report();
        assert_eq!(report.segments_published, 0);
        assert_eq!(report.segments_failed, 1);
        assert!(!report.kept_up);
        assert!(
            report.errors.iter().any(|e| e.contains("400")),
            "{:?}",
            report.errors,
        );
        // No playlist can be built without an initialization segment, so
        // nothing bogus was published either.
        assert_eq!(report.playlists_published, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_feed_that_stops_updating_stops_keeping_up() {
        // The failure the last landed update's latency cannot see: the
        // segments keep uploading and listing, so every count still
        // looks perfect, but no feed update lands — a viewer is frozen
        // on the playlist the last one pointed at. Lag is an age, not a
        // latency, so it must keep growing.
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::ZERO).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        // Lag budget of 3 × 300 ms, so the stall below clears it.
        config.segment_ms = 300;
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();

        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![0u8; 64], 300, false);
        for _ in 0..100 {
            if run.snapshot().feed_index > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let live = run.snapshot();
        assert!(live.keeping_up, "a broadcast at the live edge: {live:?}");

        // From here every `POST /soc` is refused; the segments still
        // upload and still get listed.
        lock(&log).fail_soc = true;
        for i in 1..5u32 {
            let _ = run.push(SegmentKind::Media, vec![i as u8; 64], 300, false);
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        let stalled = run.snapshot();
        assert!(
            stalled.lag_ms > 900,
            "lag must age with the stall: {stalled:?}",
        );
        assert!(!stalled.keeping_up, "{stalled:?}");

        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert!(!report.kept_up, "{report:?}");
        // Every other input to the verdict still reads clean — the lag
        // is the only thing standing between this run and a "kept up".
        assert!(report.feed_updates > 0, "{report:?}");
        assert_eq!(report.segments_listed, report.segments_published);
        assert_eq!(report.segments_failed + report.segments_dropped, 0);
    }

    #[test]
    fn a_segment_that_uploaded_but_never_got_listed_is_not_kept_up() {
        // The narrow case `segments_published` alone would let pass: a
        // *later* initialization segment fails, so the media segments
        // after it upload fine and are counted published — but they
        // carry no `#EXT-X-MAP` and can never be listed, i.e. no viewer
        // can play them.
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/b1/seg-1.m4s", false));
        state.advance(6);
        assert_eq!(state.segments_listed, 1);
        // Writer restart whose initialization segment fails to upload.
        state.done.insert(2, Committed::Failed(SegmentKind::Init));
        state.done.insert(3, media("/bzz/b3/seg-3.m4s", false));
        state.advance(6);
        // Listed count did not move even though the upload succeeded,
        // which is what keeps `kept_up` honest.
        assert_eq!(state.segments_listed, 1);
        assert_eq!(state.playlist.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pushing_after_stop_is_refused() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(log, Duration::ZERO).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        run.cancel();
        assert_eq!(
            run.push(SegmentKind::Media, vec![1u8; 64], 2000, false),
            PushOutcome::Closed,
        );
        settle(&run).await;
        assert_eq!(run.report().segments_pushed, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_push_racing_stop_is_published_or_refused_never_queued_then_dropped() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        // Slow enough that the first segment is still uploading when the
        // pump sees the cancel and starts draining in-flight work.
        let addr = stub_gateway(log, Duration::from_millis(300)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        tokio::time::sleep(Duration::from_millis(60)).await;
        run.cancel();
        tokio::time::sleep(Duration::from_millis(60)).await;
        // The push that passed its cancel check just before `cancel()`
        // and takes the lock only now, while uploads still drain.
        let outcome = run.enqueue(SegmentKind::Media, vec![2u8; 4096], 2000, false);
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.segments_dropped, 0, "{outcome:?} {report:?}");
        match outcome {
            PushOutcome::Closed => assert_eq!(report.segments_published, 1, "{report:?}"),
            PushOutcome::Queued => assert_eq!(report.segments_published, 2, "{report:?}"),
            PushOutcome::QueuedDroppingOldest => panic!("unexpected {outcome:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_channel_manifest_lands_in_the_final_report() {
        let log = Arc::new(Mutex::new(StubLog {
            feeds_delay: Duration::from_millis(600),
            ..StubLog::default()
        }));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        run.cancel();
        settle(&run).await;
        assert!(
            !run.report().channel_reference.is_empty(),
            "manifest still in flight at stop must be joined, not lost",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_retried_channel_manifest_lands_in_the_final_report() {
        // The start-time `POST /feeds` fails; the committer's retry is
        // still on the wire when the broadcast is stopped.
        let log = Arc::new(Mutex::new(StubLog {
            fail_feeds: 1,
            feeds_delay: Duration::from_millis(400),
            ..StubLog::default()
        }));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        // Let the failing start-time attempt finish before the first
        // playlist, so the playlist spawns the retry.
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        tokio::time::sleep(Duration::from_millis(100)).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        let feeds = lock(&log)
            .seen
            .iter()
            .filter(|r| r.method_path.contains("/feeds/"))
            .count();
        assert!(feeds >= 2, "retry never fired ({feeds} POST /feeds)");
        assert!(!report.channel_reference.is_empty(), "{report:?}");
    }
}
