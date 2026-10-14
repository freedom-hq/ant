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
//! What stage 2 adds on top of the measured path, per segment, in one of
//! two formats ([`StreamFormat`], chosen per broadcast; `"bzz"` is the
//! default because it is what the repository's viewers read):
//!
//! 1. Upload the segment (and, when the writer restarts, its fMP4
//!    initialization segment): `POST /bzz` (`"bzz"`, what stage 1
//!    measured) or `POST /bytes` (`"solar-punk"`, the same content
//!    chunks without the manifest on top).
//! 2. Rebuild the HLS media playlist over a sliding window of the most
//!    recent segments that fit in one chunk ([`LIVE_WINDOW_MAX_BYTES`],
//!    4096 bytes, issue #171), so each update is a single chunk read for
//!    a viewer. `"bzz"` lists segments and the `#EXT-X-MAP` as
//!    gateway-root paths (`/bzz/<ref>/seg-N.m4s`); `"solar-punk"` lists
//!    their **bare 64-hex reference**, as Solar Punk's uploader does, and
//!    a viewer resolves each against `/bytes/` on whichever gateway it
//!    reads from. Either way each segment carries an
//!    `#EXT-X-PROGRAM-DATE-TIME` (issue #170), UTC to the millisecond,
//!    dated once at push from one go-live anchor stepped by the
//!    segments' real durations ([`ProgramClock`]).
//! 3. Upload the playlist and publish it as the next **sequence-feed
//!    update** with `POST /soc/{owner}/{id}`
//!    (`id = keccak256(topic ‖ index_be8)`). Under `"bzz"` the playlist
//!    goes up with `POST /bzz` and the update is bee's v1
//!    `timestamp_be8 ‖ reference`, so `/bzz/<feed manifest>/stream.m3u8`
//!    resolves the channel on any bee gateway. Under `"solar-punk"` it
//!    goes up with `POST /bytes` and the update is v2, the single-owner
//!    chunk wrapping the playlist's own root chunk (bee-js
//!    `FeedWriter.uploadPayload`), so `GET /soc/{owner}/{id}` and
//!    `GET /feeds/{owner}/{topic}` both answer with the playlist text, on
//!    any bee gateway and on ant; the feed manifest then names the
//!    channel (owner and topic) but does not resolve to the playlist
//!    through `/bzz`, because the feed's content is not a manifest.
//!
//! At stop the live window is closed with `#EXT-X-ENDLIST` (so a viewer
//! following the live edge stops there) and then the **whole broadcast**
//! goes up the same way as one `#EXT-X-PLAYLIST-TYPE:VOD` playlist and
//! becomes the feed's final update (issue #168): every segment ever
//! listed, gaps and discontinuities included, so the channel's head
//! plays from the first segment to the last. Its reference is the stop
//! report's `recording_reference`.
//!
//! **An interruption pauses a broadcast; it never ends it** (issue
//! #172). Phones lose their uplink all the time (an LTE handover, a
//! call, a minute in a lift), so the host reports the connection going
//! with [`LiveRun::pause`] and coming back with [`LiveRun::resume`].
//! While paused the broadcast is silent: media segments the camera keeps
//! handing over are withheld (never uploaded, never listed, not even as
//! `#EXT-X-GAP` entries, since there is no uplink to lose them on),
//! initialization segments wait for the return, and the live playlist
//! simply stops advancing, with no `#EXT-X-ENDLIST`. The first segment
//! after the return continues the same feed and the same recording
//! behind one `#EXT-X-DISCONTINUITY`, dated afresh from the wall clock.
//! Only [`LiveRun::cancel`] (the host's stop) or the opt-in reap timeout
//! ([`PublisherConfig::reap_ms`]: that long with no media reaching the
//! playlist) closes the window and writes the recording. Solar Punk's
//! uploader treats an encoder that disconnects the same way.
//!
//! The topic is either text, hashed the way Solar Punk catalog entries
//! and bee-js `Topic.fromString` name a feed ([`topic_from_string`]), or
//! raw bytes from the host. A `"solar-punk"` broadcast with a text topic
//! plays in a Solar Punk viewer given only the owner and that text.
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
//!   A dropped or failed media segment keeps its place in the playlist
//!   as an `#EXT-X-GAP` entry (issue #169): the encoder kept running, so
//!   the media either side of the hole is one timeline, and
//!   `#EXT-X-DISCONTINUITY` stays reserved for a real break (a writer
//!   restart).
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
/// Excludes the 1–2 mantaray manifest chunks a `POST /bzz` (the bench's
/// upload) adds on top (< 0.5 % at segment sizes, and they are the same
/// for every mode), so the reported `chunks/s` is strictly the *content*
/// rate — directly comparable to the desktop `--target-peers` sweep in
/// PLAN.md. The live loop's `POST /bytes` (under `"solar-punk"`) adds
/// none.
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

/// Most bytes a live playlist may take: one Swarm chunk (issue #171).
///
/// Under [`StreamFormat::SolarPunk`] the feed update wraps the
/// playlist's root chunk, so a playlist that fits in one chunk *is* the
/// SOC payload and a viewer's poll (`GET /soc` or `GET /feeds`) is a
/// single chunk read. One byte over and the update wraps the root of a
/// chunk tree instead, and every poll pays for the leaves as well. Under
/// [`StreamFormat::Bzz`] it keeps the playlist file a single chunk
/// behind its manifest. Solar Punk's uploader budgets its live window
/// against the same figure (`LIVE_WINDOW_MAX_BYTES`).
pub const LIVE_WINDOW_MAX_BYTES: usize = CHUNK_SIZE as usize;

/// Upper bound on the segments listed in the rolling media playlist.
/// The window is sized by bytes ([`LIVE_WINDOW_MAX_BYTES`]): it holds
/// the newest segments that fit in one chunk, which at the default 2 s
/// segments is about 26 (`"bzz"` paths) to 29 (bare references), each
/// with its `#EXT-X-PROGRAM-DATE-TIME` (issue #170): 52 to 58 s a
/// joining or slow viewer can buffer from with no extra request.
/// This cap is set above both so the byte budget is what binds; a host
/// can still lower it. Apple recommends a live playlist hold at least 3
/// target durations.
pub const DEFAULT_PLAYLIST_WINDOW: usize = 64;

/// One live broadcast. Deserialised straight from the host's JSON so the
/// Swift app configures the same knobs the tests do.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PublisherConfig {
    /// Human-readable channel name. Also the default topic seed (see
    /// [`PublisherConfig::topic`]).
    pub channel: String,
    /// Feed topic as raw bytes: 32-byte hex (`0x` optional). Use
    /// [`PublisherConfig::topic_text`] instead for a topic a catalog can
    /// name. Setting both is a configuration error.
    ///
    /// With neither set, the topic is the text
    /// `antstream/<channel>/<unix_ms>/<nonce>` (the nonce a process-wide
    /// counter), unique to this broadcast, so a second broadcast of the
    /// same channel starts a fresh feed at index 0 instead of having to
    /// discover the previous head (or, restarted quickly, overwriting
    /// it). That text is reported as `topic_text`.
    #[serde(default)]
    pub topic: String,
    /// Feed topic as text, the way Solar Punk catalog entries and every
    /// bee-js reader name one: the 32 topic bytes are
    /// `keccak256(utf8(topic_text))`, bee-js `Topic.fromString` (see
    /// [`topic_from_string`]). Used verbatim: no trimming, no case
    /// folding, since a reader hashes exactly the string it was given.
    #[serde(default)]
    pub topic_text: String,
    /// What a broadcast writes to Swarm; see [`StreamFormat`]. Defaults
    /// to [`StreamFormat::Bzz`], the format every `AntStream` viewer in
    /// this repository reads.
    #[serde(default)]
    pub format: StreamFormat,
    /// Sequence-feed index of this broadcast's first update. A host that
    /// reuses one `topic` across broadcasts (a permanent channel link)
    /// passes the previous broadcast's final `feed_index` here, so the
    /// new updates continue the feed instead of rewriting index 0 and
    /// up, which already hold the previous broadcast's playlists. With a
    /// derived per-broadcast topic it stays 0.
    #[serde(default)]
    pub start_index: u64,
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
    /// Most segments the live playlist lists. An upper bound, not the
    /// size: the playlist holds the newest segments that fit in
    /// [`LIVE_WINDOW_MAX_BYTES`], and at most this many of them.
    /// `#EXT-X-GAP` entries for lost segments count against the bytes,
    /// not against this.
    #[serde(default = "default_playlist_window")]
    pub playlist_window: usize,
    /// Free-form host context (device, network, rendition) echoed in the
    /// final report.
    #[serde(default)]
    pub notes: String,
    /// End the broadcast by itself after this many milliseconds with no
    /// media reaching the playlist (issue #172): the closing window and
    /// the recording go up exactly as on a stop, and the run reports
    /// `reaped`. The window runs from the latest of go-live, the last
    /// media segment listed and the first [`LiveRun::resume`] after it,
    /// so a return late in an outage still gets a whole window to
    /// deliver its first segment; later resumes with no media in between
    /// buy nothing more. [`ORPHAN_REAP_MS`] is Solar Punk's figure.
    ///
    /// 0, the default, never reaps: a host only opts in once it handles
    /// a broadcast that ended without its stop call (the iOS app does;
    /// the Android app does not yet).
    #[serde(default)]
    pub reap_ms: u64,
}

/// The reap window Solar Punk's uploader runs with (`ORPHAN_REAP_MS`, 60
/// s), and the one the iOS app passes as [`PublisherConfig::reap_ms`].
pub const ORPHAN_REAP_MS: u64 = 60_000;

/// The shape a broadcast takes on Swarm. The two are not
/// interchangeable for a viewer: each reads only its own.
///
/// `"bzz"` stays the default while the `AntStream` viewers (the web
/// player the apps' Share links open, and the Android Watch tab) read
/// the channel through `/bzz/<channel>/stream.m3u8`: switching the
/// default would leave every broadcast from a host that doesn't ask
/// unplayable there. A host opts into `"solar-punk"` to be playable in
/// the Solar Punk viewers (issue #167).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum StreamFormat {
    /// Segments and the playlist go up with `POST /bzz` (one manifest
    /// each); the playlist lists them as gateway-root paths
    /// (`/bzz/<ref>/seg-N.m4s`); each feed update is bee's v1 shape,
    /// `timestamp_be8 ‖ reference` of the playlist's manifest. A viewer
    /// reads `/bzz/<channel>/stream.m3u8`, which re-resolves the feed.
    #[default]
    Bzz,
    /// The Solar Punk stream format: segments and the playlist go up
    /// with `POST /bytes`, the playlist lists bare references, and each
    /// feed update is a v2 update wrapping the playlist's own root chunk,
    /// so `GET /feeds/{owner}/{topic}` and `GET /soc/{owner}/{id}` return
    /// the playlist text. The channel's feed manifest does not resolve to
    /// the playlist through `/bzz`.
    SolarPunk,
}

impl StreamFormat {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bzz => "bzz",
            Self::SolarPunk => "solar-punk",
        }
    }
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

    /// The feed topic this broadcast writes to, and its text form when
    /// it has one: the configured raw topic, the configured text topic,
    /// or a per-broadcast text derived from the channel name.
    fn resolve_topic(&self) -> Result<([u8; 32], String), PublisherError> {
        // A wall-clock seed alone is not unique enough: stop + restart
        // of the same channel inside one clock tick would silently reuse
        // the topic, and the new broadcast's index 0 would overwrite the
        // old feed's head. The process-wide counter makes every
        // derivation distinct regardless of clock granularity.
        static TOPIC_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let raw = self.topic.trim();
        if !raw.is_empty() && !self.topic_text.is_empty() {
            return Err(PublisherError::Config(
                "set topic (raw hex) or topic_text, not both: they name different feeds".into(),
            ));
        }
        if !raw.is_empty() {
            let topic =
                parse_hex32(raw).map_err(|e| PublisherError::Config(format!("topic {e}")))?;
            return Ok((topic, String::new()));
        }
        let text = if self.topic_text.is_empty() {
            let unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let nonce = TOPIC_NONCE.fetch_add(1, Ordering::Relaxed);
            format!("antstream/{}/{unix_ms}/{nonce}", self.channel)
        } else {
            self.topic_text.clone()
        };
        Ok((topic_from_string(&text), text))
    }
}

/// A feed topic from its text name: `keccak256(utf8(text))`, byte for
/// byte what bee-js `Topic.fromString` computes, and so what every Solar
/// Punk reader computes from a catalog entry's `topic`.
#[must_use]
pub fn topic_from_string(text: &str) -> [u8; 32] {
    ant_crypto::keccak256(text.as_bytes())
}

/// Length of one ladder-marker period, in seconds of global (epoch)
/// time. `MARKER_PERIOD_SECONDS` in Solar Punk's `ladderMarker.ts`.
pub const LADDER_MARKER_PERIOD_SECONDS: u64 = 10;

/// The single-owner-chunk identifier of a ladder's time marker for one
/// period, the convention Solar Punk's uploader writes and its players
/// read (`ladderMarkerIdentifier` in `src/shared/ladderMarker.ts`,
/// marker version 2):
/// `keccak256("ladder-marker" ‖ group topic ‖ period as u64 big-endian)`,
/// `group` being the ladder's master feed topic
/// ([`topic_from_string`] of the group id) and `period`
/// `unix_ms / (LADDER_MARKER_PERIOD_SECONDS × 1000)`.
///
/// A marker names the newest index of every rung of an ABR ladder. This
/// publisher writes a single rendition, which Solar Punk players find by
/// searching the feed, so it writes no markers; this is the address it
/// would write them at.
#[must_use]
pub fn ladder_marker_identifier(group: &[u8; 32], period: u64) -> [u8; 32] {
    const PREFIX: &[u8] = b"ladder-marker";
    let mut input = Vec::with_capacity(PREFIX.len() + 32 + 8);
    input.extend_from_slice(PREFIX);
    input.extend_from_slice(group);
    input.extend_from_slice(&period.to_be_bytes());
    ant_crypto::keccak256(&input)
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
    /// Not uploaded: the run is paused ([`LiveRun::pause`]), so a media
    /// segment is withheld rather than sent to an uplink that is not
    /// there. The next segment after [`LiveRun::resume`] starts a new
    /// run of the timeline.
    Withheld,
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
    /// `#EXT-X-PROGRAM-DATE-TIME` of a media segment, Unix ms, decided
    /// once at push by [`ProgramClock::place`]. 0 for an init segment.
    program_ms: u64,
    /// [`PublisherState::outages`] when it was pushed: a media segment
    /// whose upload fails across an outage is withheld, not a gap.
    outage: u64,
}

/// One playlist entry: a segment that landed, or the gap a lost one
/// left.
#[derive(Debug, Clone)]
struct MediaEntry {
    /// HLS media sequence number. Every media segment the capture side
    /// pushed under a landed `#EXT-X-MAP` takes one, lost ones included,
    /// so a segment keeps its number as the window slides past a hole.
    number: u64,
    /// The uploaded segment, or `gap-<number>` for a gap entry.
    uri: String,
    /// `#EXT-X-GAP`: the segment was dropped or its upload failed. RFC
    /// 8216bis §4.4.4.7: the entry names no media and a client skips it.
    gap: bool,
    duration_ms: u32,
    /// `#EXT-X-DISCONTINUITY` precedes this entry.
    discontinuity: bool,
    /// The `#EXT-X-MAP` in force for this entry.
    init_uri: String,
    /// `#EXT-X-PROGRAM-DATE-TIME`, Unix ms: stored on the segment when
    /// it was placed, never recomputed at render time.
    program_ms: u64,
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
        program_ms: u64,
    },
    /// A media segment that never landed: dropped before it was ever
    /// uploaded (backlog full) or its upload failed. Listed as an
    /// `#EXT-X-GAP` entry carrying the date and duration it was placed
    /// with, so the media after it keeps both its number and its date.
    Lost {
        duration_ms: u32,
        /// The capture side flagged it as the first of a new writer.
        discontinuity: bool,
        program_ms: u64,
    },
    /// An *initialization* segment's upload failed. It retires the
    /// `#EXT-X-MAP` currently in force, because the writer that emitted
    /// it has already been replaced and its map does not describe what
    /// follows.
    InitFailed,
    /// A media segment the outage swallowed (issue #172): captured while
    /// the run was paused, so never uploaded, or uploaded across the
    /// outage and failed. Unlike [`Committed::Lost`] it is not listed at
    /// all, not even as a gap: the broadcast went silent there, and the
    /// media after it is a new run of the timeline behind one
    /// `#EXT-X-DISCONTINUITY`.
    Withheld,
}

impl Committed {
    /// What a pending segment that never landed becomes.
    fn lost(pending: &Pending) -> Self {
        match pending.kind {
            SegmentKind::Init => Self::InitFailed,
            SegmentKind::Media => Self::Lost {
                duration_ms: pending.duration_ms,
                discontinuity: pending.discontinuity,
                program_ms: pending.program_ms,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Wall-clock dating (#EXT-X-PROGRAM-DATE-TIME, issue #170)
// ---------------------------------------------------------------------------

/// Dates every media segment from **one anchor taken at go-live**,
/// stepped by each segment's real media duration — the Solar Punk
/// uploader's manifest contract, with the capture side's own measured
/// durations as the step.
///
/// The anchor is the wall clock at the first media segment, less that
/// segment's duration: a segment is pushed once it has closed, so
/// `now − duration` is when its first frame was captured, which is what
/// `#EXT-X-PROGRAM-DATE-TIME` names. Every later segment is dated where
/// the previous one ended. Never the arrival time: arrival jitters by
/// however long the encoder took to hand the segment over, and dating
/// by it would make neighbouring stamps disagree with their own
/// `#EXTINF`.
///
/// A dropped or failed segment still advances the clock (it is placed
/// at push, before the uplink decides its fate), so its `#EXT-X-GAP`
/// entry carries the time it covered and the segment after the gap the
/// time it was really captured at.
///
/// **Writer restarts re-anchor.** An initialization segment after the
/// first, or a media segment flagged discontinuous, means the encoder's
/// timeline broke — an interruption, backgrounding, a camera flip — and
/// the media after it did not follow on from the media before. The next
/// media segment re-anchors on the wall clock, floored at where the
/// previous segment ended so a date **never moves backwards** (hls.js
/// reports a backwards stamp as a parsing error).
#[derive(Debug, Default)]
struct ProgramClock {
    /// Where the next media segment starts, Unix ms. `None` until the
    /// first media segment anchors the broadcast.
    next_ms: Option<u64>,
    /// The writer restarted since the last placed segment.
    reanchor: bool,
}

impl ProgramClock {
    /// Date one pushed segment. `now_ms` is the wall clock at push.
    /// Returns the media segment's `#EXT-X-PROGRAM-DATE-TIME`, or 0 for
    /// an initialization segment (which only arms a re-anchor).
    /// The timeline broke without a segment saying so (an outage
    /// withheld some, issue #172): the next media segment re-anchors on
    /// the wall clock, as after a writer restart.
    fn break_timeline(&mut self) {
        self.reanchor = self.next_ms.is_some();
    }

    fn place(
        &mut self,
        kind: SegmentKind,
        duration_ms: u32,
        discontinuity: bool,
        now_ms: u64,
    ) -> u64 {
        if kind == SegmentKind::Init || discontinuity {
            self.reanchor = self.next_ms.is_some();
            if kind == SegmentKind::Init {
                return 0;
            }
        }
        let duration = u64::from(duration_ms);
        let wall = now_ms.saturating_sub(duration);
        let at = match self.next_ms {
            None => wall,
            Some(next) if self.reanchor => wall.max(next),
            Some(next) => next,
        };
        self.reanchor = false;
        self.next_ms = Some(at + duration);
        at
    }
}

/// Wall clock, Unix ms.
fn unix_ms_now() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

/// Unix ms as RFC 8216 / ISO 8601 UTC to the millisecond,
/// `2026-09-01T12:00:00.000Z` — byte-for-byte what JavaScript's
/// `Date.prototype.toISOString` writes, which is what Solar Punk's
/// uploader stamps and its clients parse.
fn iso8601_utc_ms(unix_ms: u64) -> String {
    let millis = unix_ms % 1000;
    let secs = unix_ms / 1000;
    let (hour, minute, second) = (secs / 3600 % 24, secs / 60 % 60, secs % 60);
    // Civil date from days since 1970-01-01 (Howard Hinnant's
    // `civil_from_days`), so no date crate is needed for one format.
    let z = secs / 86_400 + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
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
    /// Identifies this broadcast in every playlist it writes
    /// (`#ANTSTREAM-BROADCAST:<id>`, a comment line players ignore). A
    /// channel keeps its feed across broadcasts, and a stale feed read can
    /// return an older playlist; the id lets a viewer tell "a new
    /// broadcast started" from "this read is behind".
    broadcast_id: String,
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
    /// Every entry the live window ever listed, in order, never evicted:
    /// the whole broadcast, written at stop as the recording
    /// ([`PublisherState::render_recording`], issue #168). A few hundred
    /// bytes an entry, so about 400 KB of memory for an hour at 2 s
    /// segments.
    recording: Vec<MediaEntry>,
    /// Media sequence number for the next listed segment.
    next_number: u64,
    /// `#EXT-X-DISCONTINUITY-SEQUENCE`: discontinuities that have
    /// already slid out of the playlist window.
    discontinuity_sequence: u64,
    /// `#EXT-X-MAP` currently in force.
    init_uri: Option<String>,
    /// Dates each media segment as it is pushed.
    program_clock: ProgramClock,
    /// A real timeline break (a writer restart, or media the playlist
    /// could not list under any map) since the last entry, so the next
    /// one starts a new continuous run with `#EXT-X-DISCONTINUITY`. A
    /// lost segment is not one: see [`Committed::Lost`].
    break_since_last_entry: bool,
    /// Gap entries waiting for the next listed segment. A gap never
    /// ends a playlist, so a lost segment is only listed once media
    /// behind it lands; one still waiting when the broadcast ends is
    /// never listed.
    pending_gaps: Vec<MediaEntry>,

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
    /// A feed update is on the wire at `feed_index`: it may still land
    /// and take that index. See [`LiveRun::seal_feed`].
    feed_update_in_flight: bool,
    /// The stop call returned before the run finished and handed
    /// `feed_index` to the host as the next broadcast's `start_index`:
    /// no further update may be written, or a closing window or
    /// recording landing late would take the index the next broadcast
    /// starts at. See [`LiveRun::seal_feed`].
    feed_sealed: bool,
    channel_reference: Option<String>,
    /// A `POST /feeds` is already in flight. The start-time attempt and
    /// the committer's retry would otherwise both fire while the first
    /// one is still on the wire — harmless (the manifest is
    /// content-addressed) but a wasted upload on the live path.
    channel_manifest_in_flight: bool,
    playlist_reference: Option<String>,
    /// Reference of the uploaded recording, set once it landed at stop.
    recording_reference: Option<String>,

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
    /// When the broadcast's content ended (the committer folded in the
    /// last segment), so the duration (and the throughput figures divided
    /// by it) freezes with the broadcast — the same principle as
    /// `lag_frozen_ms`: a report read a minute later must describe the
    /// broadcast, not how long the host waited to ask, and the closing
    /// window and recording uploaded after it are bookkeeping, not
    /// broadcast time.
    ended_at: Option<Instant>,
    /// Longest media segment ever listed, in ms. Drives
    /// `#EXT-X-TARGETDURATION`, which RFC 8216 §6.2.1 says MUST NOT
    /// change across playlist reloads — so it is a run-max that can
    /// only ratchet up (a keyframe-aligned cut routinely overruns the
    /// configured target), never drop back when the long segment slides
    /// out of the window.
    longest_listed_ms: u32,

    /// The host reported the connection gone ([`LiveRun::pause`]) and
    /// not yet back.
    paused: bool,
    /// How many times the run was paused; see [`Pending::outage`].
    outages: u64,
    /// Media segments the outages withheld: captured while paused, never
    /// uploaded. (One sent across an outage that failed is committed as
    /// [`Committed::Withheld`] too, but counted in `segments_failed`.)
    segments_withheld: u64,
    /// The last time a media segment was listed: the reap window runs
    /// from here, or from `returned_at`.
    last_media_at: Option<Instant>,
    /// The first [`LiveRun::resume`] since `last_media_at`.
    returned_at: Option<Instant>,
    /// The run ended itself: [`PublisherConfig::reap_ms`] passed with no
    /// media.
    reaped: bool,

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
    ///
    /// The playlist keeps the newest segments that fit, rendered, in
    /// [`LIVE_WINDOW_MAX_BYTES`], and at most `window` of them.
    /// `target_ms` is the configured segment duration
    /// [`PublisherState::render_playlist`] is called with, since it
    /// shapes the header the budget is measured on.
    fn advance(&mut self, window: usize, target_ms: u32) -> Option<Instant> {
        let mut newest: Option<Instant> = None;
        while let Some(outcome) = self.done.remove(&self.next_commit) {
            self.next_commit += 1;
            match outcome {
                Committed::Init { uri } => {
                    // A fresh initialization segment means a fresh
                    // encoder timeline: whatever follows is discontinuous
                    // with what came before.
                    self.init_uri = Some(uri);
                    if self.segments_listed > 0 || !self.pending_gaps.is_empty() {
                        self.break_since_last_entry = true;
                    }
                }
                Committed::Withheld => {
                    // The broadcast went silent here: whatever is listed
                    // next is a new run of the timeline. Only a break
                    // once something was listed, as for a fresh init.
                    if self.segments_listed > 0 || !self.pending_gaps.is_empty() {
                        self.break_since_last_entry = true;
                    }
                }
                Committed::InitFailed => {
                    // Listing later segments under the *previous*
                    // writer's map would hand a player an initialization
                    // segment that does not describe them. Better a
                    // shorter playlist than an unplayable one: nothing
                    // is listed again until a fresh init lands.
                    self.init_uri = None;
                    self.break_since_last_entry = true;
                }
                Committed::Lost {
                    duration_ms,
                    discontinuity,
                    program_ms,
                } => {
                    // Not a break: the encoder kept running, so the
                    // media behind the hole continues the media in
                    // front of it. The hole keeps its own number as an
                    // `#EXT-X-GAP` entry, listed once media behind it
                    // lands.
                    let Some(entry) = self.next_entry(None, duration_ms, discontinuity, program_ms)
                    else {
                        continue;
                    };
                    self.pending_gaps.push(entry);
                }
                Committed::Media {
                    uri,
                    duration_ms,
                    discontinuity,
                    captured_at,
                    program_ms,
                } => {
                    let Some(entry) =
                        self.next_entry(Some(uri), duration_ms, discontinuity, program_ms)
                    else {
                        // A media segment whose initialization segment
                        // never landed is unplayable; list nothing.
                        continue;
                    };
                    // The gaps in front of it are between two entries
                    // now, so they are listed with it.
                    for gap in std::mem::take(&mut self.pending_gaps) {
                        self.longest_listed_ms = self.longest_listed_ms.max(gap.duration_ms);
                        // The recording never starts on a gap either: a
                        // hole in front of its first segment is not
                        // worth a line, as in the live window.
                        if !self.recording.is_empty() {
                            self.recording.push(gap.clone());
                        }
                        self.playlist.push_back(gap);
                    }
                    self.segments_listed += 1;
                    self.longest_listed_ms = self.longest_listed_ms.max(duration_ms);
                    self.recording.push(entry.clone());
                    self.playlist.push_back(entry);
                    // Start the live-edge clock at the first segment
                    // that could have been seen. Later segments do not
                    // move it — only a landed feed update does, in
                    // `note_lag` — because a segment nobody can resolve
                    // yet is not the live edge.
                    self.live_edge.get_or_insert(captured_at);
                    self.last_media_at = Some(Instant::now());
                    self.slide(window, target_ms);
                    newest = Some(captured_at);
                }
            }
        }
        newest
    }

    /// The next entry, numbered and flagged: a segment that landed
    /// (`uri`), or a gap entry (`None`). `None` when no `#EXT-X-MAP` is
    /// in force, which makes the segment unlistable: it takes no number
    /// and is a break for whatever is listed next.
    fn next_entry(
        &mut self,
        uri: Option<String>,
        duration_ms: u32,
        discontinuity: bool,
        program_ms: u64,
    ) -> Option<MediaEntry> {
        let Some(init_uri) = self.init_uri.clone() else {
            self.break_since_last_entry = true;
            return None;
        };
        let previous = self.pending_gaps.last().or(self.playlist.back());
        let discontinuous = discontinuity
            || self.break_since_last_entry
            || previous.is_some_and(|prev| prev.init_uri != init_uri);
        self.break_since_last_entry = false;
        let number = self.next_number;
        self.next_number += 1;
        Some(MediaEntry {
            number,
            gap: uri.is_none(),
            uri: uri.unwrap_or_else(|| format!("gap-{number}")),
            duration_ms,
            // The broadcast's first listed segment starts the timeline;
            // there is nothing before it to be discontinuous with.
            discontinuity: discontinuous && self.segments_listed > 0,
            init_uri,
            program_ms,
        })
    }

    /// Slide the window: at most `window` listed segments, within
    /// [`LIVE_WINDOW_MAX_BYTES`], and never starting on a gap entry
    /// (`#EXT-X-MEDIA-SEQUENCE` is always a segment that landed, and a
    /// hole behind the window's first segment is not worth a line).
    /// Gap entries count against the byte budget but not against
    /// `window`, which counts media.
    fn slide(&mut self, window: usize, target_ms: u32) {
        let mut listed = self.playlist.iter().filter(|e| !e.gap).count();
        loop {
            while self.playlist.front().is_some_and(|e| e.gap) {
                self.evict_front();
            }
            if listed > window || (listed > 1 && self.over_budget(target_ms)) {
                self.evict_front();
                listed -= 1;
            } else {
                break;
            }
        }
    }

    fn evict_front(&mut self) {
        if let Some(evicted) = self.playlist.pop_front() {
            if evicted.discontinuity {
                self.discontinuity_sequence += 1;
            }
        }
    }

    /// The playlist, as it would render now, is past
    /// [`LIVE_WINDOW_MAX_BYTES`]. Measured on the real rendering, so
    /// everything a segment brings with it counts (its `#EXTINF` and URI,
    /// the `#EXT-X-GAP` entries for a hole in front of it, a
    /// `#EXT-X-DISCONTINUITY` for a restart, a fresh `#EXT-X-MAP`), as do
    /// header values that move with the
    /// window (`#EXT-X-MEDIA-SEQUENCE`, `#EXT-X-DISCONTINUITY-SEQUENCE`).
    /// Measured *with* `#EXT-X-ENDLIST`, so the closing playlist, which
    /// is this same window plus that line, fits too.
    fn over_budget(&self, target_ms: u32) -> bool {
        self.render_playlist(target_ms, true).len() > LIVE_WINDOW_MAX_BYTES
    }

    /// Render the current playlist. `endlist` closes the broadcast.
    fn render_playlist(&self, target_ms: u32, endlist: bool) -> String {
        render_entries(
            &self.broadcast_id,
            self.target_duration_s(target_ms),
            self.playlist.front().map_or(self.next_number, |e| e.number),
            self.discontinuity_sequence,
            self.playlist.iter(),
            if endlist {
                PlaylistShape::Closing
            } else {
                PlaylistShape::Live
            },
        )
    }

    /// Render the whole broadcast as a recording (issue #168): every
    /// entry the live window ever listed, in order, gaps and
    /// discontinuities where they were, as an
    /// `#EXT-X-PLAYLIST-TYPE:VOD` playlist closed with `#EXT-X-ENDLIST`.
    /// The shape Solar Punk's uploader writes as its recording
    /// (`buildVODManifest`): the media sequence is the number the live
    /// window gave the first segment, so a segment keeps its number from
    /// the live playlists into the recording, and there is no
    /// `#EXT-X-DISCONTINUITY-SEQUENCE`, since nothing came before the
    /// first entry. Not budgeted: past one chunk it uploads as a chunk
    /// tree.
    fn render_recording(&self, target_ms: u32) -> String {
        render_entries(
            &self.broadcast_id,
            self.target_duration_s(target_ms),
            self.recording
                .first()
                .map_or(self.next_number, |e| e.number),
            0,
            self.recording.iter(),
            PlaylistShape::Recording,
        )
    }

    /// When the reap timeout (`reap`) runs out: that long after the
    /// latest of go-live (`started`), the last media segment listed, and
    /// the first return after it.
    fn reap_deadline(&self, started: Instant, reap: Duration) -> Instant {
        let since = [self.last_media_at, self.returned_at]
            .into_iter()
            .flatten()
            .fold(started, Instant::max);
        since + reap
    }

    /// `#EXT-X-TARGETDURATION`, whole seconds. Run-max, not window-max:
    /// see `longest_listed_ms`. The same value in every playlist of the
    /// broadcast, the recording included.
    fn target_duration_s(&self, target_ms: u32) -> u32 {
        self.longest_listed_ms.max(target_ms).div_ceil(1000).max(1)
    }
}

/// Which playlist [`render_entries`] writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlaylistShape {
    /// The rolling live window.
    Live,
    /// The live window closed with `#EXT-X-ENDLIST`, published at stop
    /// before the recording.
    Closing,
    /// The whole broadcast: `#EXT-X-PLAYLIST-TYPE:VOD` and
    /// `#EXT-X-ENDLIST`.
    Recording,
}

/// Write an HLS media playlist over `entries`, every playlist of a
/// broadcast (live window, closing window, recording) in one shape.
fn render_entries<'a>(
    broadcast_id: &str,
    target_s: u32,
    media_sequence: u64,
    discontinuity_sequence: u64,
    entries: impl Iterator<Item = &'a MediaEntry>,
    shape: PlaylistShape,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::from("#EXTM3U\n#EXT-X-VERSION:7\n");
    if !broadcast_id.is_empty() {
        let _ = writeln!(out, "#ANTSTREAM-BROADCAST:{broadcast_id}");
    }
    // Writing into a `String` is infallible, so the `write!` results
    // are deliberately dropped rather than unwrapped.
    let _ = writeln!(out, "#EXT-X-TARGETDURATION:{target_s}");
    if shape == PlaylistShape::Recording {
        out.push_str("#EXT-X-PLAYLIST-TYPE:VOD\n");
    }
    let _ = writeln!(out, "#EXT-X-MEDIA-SEQUENCE:{media_sequence}");
    if discontinuity_sequence > 0 {
        let _ = writeln!(
            out,
            "#EXT-X-DISCONTINUITY-SEQUENCE:{discontinuity_sequence}"
        );
    }
    let mut current_map: Option<&str> = None;
    for entry in entries {
        if entry.discontinuity {
            out.push_str("#EXT-X-DISCONTINUITY\n");
        }
        if current_map != Some(entry.init_uri.as_str()) {
            let _ = writeln!(out, "#EXT-X-MAP:URI=\"{}\"", entry.init_uri);
            current_map = Some(entry.init_uri.as_str());
        }
        if entry.gap {
            out.push_str("#EXT-X-GAP\n");
        }
        let _ = writeln!(
            out,
            "#EXT-X-PROGRAM-DATE-TIME:{}",
            iso8601_utc_ms(entry.program_ms),
        );
        let _ = writeln!(
            out,
            "#EXTINF:{:.3},\n{}",
            f64::from(entry.duration_ms) / 1000.0,
            entry.uri,
        );
    }
    if shape != PlaylistShape::Live {
        out.push_str("#EXT-X-ENDLIST\n");
    }
    out
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
    /// Feed topic, hex. With `owner` this is the feed any bee gateway
    /// resolves the channel from.
    pub topic: String,
    /// The topic's text name, what a Solar Punk catalog entry carries
    /// (`topic` is `keccak256` of it). Empty when the host configured
    /// raw topic bytes, which no catalog entry can name.
    pub topic_text: String,
    pub owner: String,
    /// Feed-manifest reference created at start (`POST /feeds`) — the
    /// single reference that identifies the channel to a viewer. It
    /// carries the feed's owner and topic. Under `"bzz"` a viewer plays
    /// `/bzz/<it>/stream.m3u8`; under `"solar-punk"` the playlist is read
    /// with `GET /feeds/{owner}/{topic}`.
    pub channel_reference: String,
    /// The broadcast's [`StreamFormat`], `"bzz"` or `"solar-punk"`: which
    /// of the two reads above plays it.
    pub format: StreamFormat,
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
    /// Publish latency of one segment's upload, milliseconds.
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
    /// The host reported the connection gone ([`LiveRun::pause`]): the
    /// broadcast is silent, not over, until it comes back.
    pub paused: bool,
    /// Media segments withheld by an outage instead of uploaded (see
    /// [`PublisherReport::segments_withheld`]).
    pub segments_withheld: u64,
    /// The run is ending by itself: [`PublisherConfig::reap_ms`] passed
    /// with no media. It finishes like a stop (closing window, then the
    /// recording); a host that sees this calls the stop call for the
    /// report.
    pub reaped: bool,
}

/// Final result of a broadcast, returned by the stop call.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PublisherReport {
    pub channel: String,
    pub topic: String,
    /// See [`PublisherSnapshot::topic_text`].
    pub topic_text: String,
    pub owner: String,
    pub channel_reference: String,
    /// See [`PublisherSnapshot::format`].
    pub format: StreamFormat,
    pub playlist_reference: String,
    /// Reference of the **recording** (issue #168): the whole broadcast
    /// as one `#EXT-X-PLAYLIST-TYPE:VOD` playlist, uploaded at stop and
    /// published as the feed's final update, so the channel's head
    /// plays from the first segment to the last. Kept by a host that
    /// lists past broadcasts: under `"bzz"` it plays as
    /// `/bzz/<it>/stream.m3u8`, under `"solar-punk"` as `/bytes/<it>`,
    /// with no feed lookup. Empty when nothing was listed or its upload
    /// failed (see `errors`). Set as soon as the upload lands, even if
    /// the feed update after it fails or is skipped (the stop call's
    /// grace ran out first, see [`LiveRun::seal_feed`]); the recording
    /// then still plays by this reference but is not the feed's head.
    pub recording_reference: String,
    pub notes: String,
    pub target_bitrate_kbps: u32,
    pub segment_ms: u32,
    pub max_in_flight: usize,
    /// Broadcast duration. Frozen with the last content (like
    /// `lag_ms_final`), before the closing window and the recording go
    /// up, so it — and the sustained figures divided by it — counts
    /// only the broadcast and stays put no matter how much later the
    /// report is read.
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
    /// Sequence-feed index the *next* update would take: what a host
    /// reusing this channel's topic passes as the next broadcast's
    /// [`PublisherConfig::start_index`]. Final in the report the stop
    /// call returns, even when its grace ran out first: the run is then
    /// sealed ([`LiveRun::seal_feed`]) and writes no further update.
    pub feed_index: u64,
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
    /// Media segments an outage withheld (issue #172): captured while
    /// the run was paused, so never uploaded. Never listed, not even as
    /// gaps; the media after them follows one `#EXT-X-DISCONTINUITY`. (A
    /// segment already on the wire when the outage began whose upload
    /// then failed is not listed either, but counts in
    /// `segments_failed`.)
    pub segments_withheld: u64,
    /// The run ended itself after [`PublisherConfig::reap_ms`] with no
    /// media, rather than on the host's stop call.
    pub reaped: bool,
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// A broadcast in flight. Held by the FFI handle so the host can push
/// segments, poll progress and stop it.
pub struct LiveRun {
    config: PublisherConfig,
    topic: [u8; 32],
    /// The topic's text name ([`topic_from_string`]), empty when the
    /// host configured raw topic bytes.
    topic_text: String,
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
        self.enqueue(kind, data, duration_ms, discontinuity, unix_ms_now())
    }

    /// [`Self::push`] past its cancel check. Split out so tests can
    /// drive the push that raced `cancel()` deterministically, and on a
    /// wall clock of their choosing (`now_ms`, Unix ms).
    fn enqueue(
        &self,
        kind: SegmentKind,
        data: Vec<u8>,
        duration_ms: u32,
        discontinuity: bool,
        now_ms: u64,
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
            // Paused: there is no uplink for this media, and queueing it
            // for the return would put the broadcast that far behind
            // live. It takes its place in the commit order only to say
            // the timeline broke there. An initialization segment is
            // queued all the same (the pump holds it until the return):
            // every segment after it needs its map.
            if state.paused && kind == SegmentKind::Media {
                state.segments_withheld += 1;
                state.program_clock.break_timeline();
                state.done.insert(seq, Committed::Withheld);
                drop(state);
                self.commit_notify.notify_one();
                return PushOutcome::Withheld;
            }
            // Dated here, in push (= capture) order and before the
            // backlog can drop it, so a dropped segment still moves the
            // clock on by the media it held.
            let program_ms = state
                .program_clock
                .place(kind, duration_ms, discontinuity, now_ms);
            let outage = state.outages;
            state.pending.push_back(Pending {
                seq,
                kind,
                data,
                duration_ms,
                discontinuity,
                captured_at: Instant::now(),
                program_ms,
                outage,
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
                state.done.insert(victim.seq, Committed::lost(&victim));
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

    /// The connection is gone (issue #172): the device went offline, or
    /// the app is about to lose its network. The broadcast goes silent
    /// rather than ending: media segments pushed from now on are
    /// withheld ([`PushOutcome::Withheld`]), and so is any still waiting
    /// in the backlog; initialization segments wait for
    /// [`LiveRun::resume`]; uploads already on the wire finish either
    /// way. Nothing is closed and no `#EXT-X-ENDLIST` is written: a
    /// viewer just sees the live playlist stop advancing. Idempotent.
    pub fn pause(&self) {
        let mut state = lock(&self.state);
        // A run already stopping drains what it has rather than
        // withholding it.
        if state.paused || state.intake_closed || self.cancel.load(Ordering::SeqCst) {
            return;
        }
        state.paused = true;
        state.outages += 1;
        let mut kept = VecDeque::with_capacity(state.pending.len());
        let mut withheld = false;
        for pending in std::mem::take(&mut state.pending) {
            if pending.kind == SegmentKind::Media {
                state.segments_withheld += 1;
                state.done.insert(pending.seq, Committed::Withheld);
                withheld = true;
            } else {
                kept.push_back(pending);
            }
        }
        state.pending = kept;
        if withheld {
            state.program_clock.break_timeline();
        }
        drop(state);
        self.commit_notify.notify_one();
    }

    /// The connection is back. The broadcast carries on in the same feed
    /// and the same recording: the next media segment is listed behind
    /// one `#EXT-X-DISCONTINUITY` (if the outage withheld any) and dated
    /// from the wall clock again. The first return after the last listed
    /// segment also restarts the reap window
    /// ([`PublisherConfig::reap_ms`]), so a return late in an outage
    /// still has a whole window to deliver its first segment. Idempotent,
    /// and harmless without a prior [`LiveRun::pause`] (a host can call
    /// it on every return to the foreground).
    pub fn resume(&self) {
        {
            let mut state = lock(&self.state);
            state.paused = false;
            let first_return = match (state.returned_at, state.last_media_at) {
                (None, _) => true,
                (Some(returned), Some(media)) => media >= returned,
                (Some(_), None) => false,
            };
            if first_return {
                state.returned_at = Some(Instant::now());
            }
        }
        // The pump may be holding an initialization segment for this.
        self.push_notify.notify_one();
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
            topic_text: self.topic_text.clone(),
            owner: hex::encode(self.owner),
            channel_reference: state.channel_reference.clone().unwrap_or_default(),
            format: self.config.format,
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
            paused: state.paused,
            segments_withheld: state.segments_withheld,
            reaped: state.reaped,
        }
    }

    /// Ask the loop to stop. Cooperative: already-captured segments are
    /// drained (the last seconds of a broadcast are worth publishing),
    /// the live playlist is closed with `#EXT-X-ENDLIST`, and the whole
    /// broadcast is published as the recording, the feed's final update.
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

    /// Write no further feed update: the stop call is about to hand
    /// `feed_index` to the host before the run finished (its grace ran
    /// out on a slow uplink), and the host passes it on as the next
    /// broadcast's `start_index`. Without the seal, a closing window or
    /// recording still uploading in the background would land at that
    /// very index, so the next broadcast's first update would collide
    /// with it. Segments and playlists already on the wire still
    /// finish; only their feed updates are skipped (and reported).
    ///
    /// Returns whether an update is in flight right now: it may still
    /// land and take `feed_index`, so the caller must wait for
    /// [`LiveRun::feed_update_in_flight`] to clear (at most
    /// [`PUBLISH_TIMEOUT`]) before reading the index it hands out.
    #[must_use]
    pub fn seal_feed(&self) -> bool {
        let mut state = lock(&self.state);
        state.feed_sealed = true;
        state.feed_update_in_flight
    }

    /// A feed update is on the wire; see [`LiveRun::seal_feed`].
    #[must_use]
    pub fn feed_update_in_flight(&self) -> bool {
        lock(&self.state).feed_update_in_flight
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
            .saturating_add(state.segments_dropped)
            .saturating_add(state.segments_withheld);
        PublisherReport {
            channel: self.config.channel.clone(),
            topic: hex::encode(self.topic),
            topic_text: self.topic_text.clone(),
            owner: hex::encode(self.owner),
            channel_reference: state.channel_reference.clone().unwrap_or_default(),
            format: self.config.format,
            playlist_reference: state.playlist_reference.clone().unwrap_or_default(),
            recording_reference: state.recording_reference.clone().unwrap_or_default(),
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
            feed_index: state.feed_index,
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
            segments_withheld: state.segments_withheld,
            reaped: state.reaped,
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
    let (topic, topic_text) = config.resolve_topic()?;
    let target = Target::parse(&config.gateway).map_err(PublisherError::Config)?;
    let batch = parse_hex32(&config.batch_id)
        .map_err(|e| PublisherError::Config(format!("batch_id {e}")))?;

    let started_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let state = Arc::new(Mutex::new(PublisherState {
        broadcast_id: format!("{started_ms:x}"),
        feed_index: config.start_index,
        ..PublisherState::default()
    }));
    let cancel = Arc::new(AtomicBool::new(false));
    let push_notify = Arc::new(Notify::new());
    let commit_notify = Arc::new(Notify::new());
    let started_at = Instant::now();
    let run = Arc::new(LiveRun {
        config: config.clone(),
        topic,
        topic_text,
        owner,
        state: Arc::clone(&state),
        cancel: Arc::clone(&cancel),
        push_notify: Arc::clone(&push_notify),
        commit_notify: Arc::clone(&commit_notify),
        started_at,
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
        started_at,
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
    /// Go-live, where the reap window first runs from.
    started_at: Instant,
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

    /// End the run the way the host's stop does, as `reaped`.
    fn reap(&self) {
        {
            let mut state = lock(&self.state);
            if state.intake_closed || self.cancelled() {
                return;
            }
            state.reaped = true;
        }
        self.cancel.store(true, Ordering::SeqCst);
        // See `LiveRun::cancel` for why `notify_one`.
        self.push_notify.notify_one();
        self.commit_notify.notify_one();
    }
}

/// End the broadcast once [`PublisherConfig::reap_ms`] passes with no
/// media reaching the playlist (issue #172): the host is gone (the app
/// was backgrounded and never came back, the connection never
/// returned), and a viewer must not be left on a live playlist that
/// will never advance again. Re-reads the deadline every time it wakes,
/// since media and returns only ever push it later.
async fn reaper(ctx: Arc<RunCtx>, reap: Duration) {
    loop {
        let deadline = {
            let state = lock(&ctx.state);
            if state.intake_closed || ctx.cancelled() {
                return;
            }
            state.reap_deadline(ctx.started_at, reap)
        };
        let now = Instant::now();
        if now >= deadline {
            ctx.reap();
            return;
        }
        tokio::time::sleep(deadline - now).await;
    }
}

/// How often the peer count is sampled for the progress view.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

async fn drive(ctx: Arc<RunCtx>) {
    let sampler = spawn_peer_sampler(&ctx);
    // The channel's feed manifest: one immutable reference a viewer can
    // be handed that names the feed (owner and topic, in its metadata).
    // Under "bzz" it resolves through any bee gateway to whatever the
    // latest feed update points at (`/bzz/<ref>/stream.m3u8`). Under
    // "solar-punk" a viewer reads the playlist from the feed with
    // `GET /feeds`; the manifest does not resolve to it through `/bzz`,
    // because a v2 update's content is the playlist itself, not a
    // manifest.
    //
    // Concurrent with the loop, not before it: this is a real upload and
    // can sit on the 60 s publish deadline if the peer set is still
    // warming up, and the first minute of a broadcast must not be spent
    // waiting for a reference nobody has been given yet. It is
    // best-effort for the same reason — the feed updates are what carry
    // the stream — and the committer retries it until it lands.
    ctx.spawn_channel_manifest();

    let reaper = (ctx.config.reap_ms > 0).then(|| {
        tokio::spawn(reaper(
            Arc::clone(&ctx),
            Duration::from_millis(ctx.config.reap_ms),
        ))
    });
    let pump = tokio::spawn(pump(Arc::clone(&ctx)));
    let committer = tokio::spawn(commit_loop(Arc::clone(&ctx)));
    let _ = pump.await;
    if let Some(reaper) = reaper {
        reaper.abort();
    }
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
        // was read. The committer freezes it with the last content; this
        // too is the fallback.
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
            // Paused, only initialization segments are pending (media is
            // withheld), and they wait for the return rather than fail
            // against an uplink that is not there. A stop overrides the
            // hold: it drains whatever is left.
            let held = state.paused && !ctx.cancelled();
            let next = if held {
                None
            } else {
                state.pending.pop_front()
            };
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
    let bytes = pending.data.len() as u64;
    let started = Instant::now();
    let result = match ctx.config.format {
        StreamFormat::Bzz => {
            let (name, content_type) = match pending.kind {
                SegmentKind::Init => (format!("init-{}.mp4", pending.seq), "video/mp4"),
                SegmentKind::Media => (format!("seg-{}.m4s", pending.seq), "video/iso.segment"),
            };
            publish_bzz(&ctx.target, ctx.batch, &name, content_type, &pending.data)
                .await
                .map(|reference| bzz_uri(&reference, &name))
        }
        // The bare reference is the URI: see `publish_bytes`.
        StreamFormat::SolarPunk => publish_bytes(&ctx.target, ctx.batch, &pending.data, &[]).await,
    };
    let elapsed = started.elapsed();

    {
        let mut state = lock(&ctx.state);
        match result {
            Ok(uri) => {
                PublisherState::record_latency(&mut state.publish_ms, ms(elapsed));
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
                                program_ms: pending.program_ms,
                            },
                        );
                    }
                }
            }
            Err(message) => {
                state.record_error(format!(
                    "{} segment {}: {message}",
                    pending.kind.as_str(),
                    pending.seq,
                ));
                // An initialization segment that fails while the run is
                // paused waits for the return and is tried again then:
                // without it every later segment of its writer is
                // unplayable, and the outage is why it failed.
                if pending.kind == SegmentKind::Init && state.paused && !ctx.cancelled() {
                    state.pending.push_front(pending);
                    drop(state);
                    ctx.push_notify.notify_one();
                    return;
                }
                if pending.kind == SegmentKind::Media {
                    state.segments_failed += 1;
                }
                // A media segment that was on the wire across an outage
                // is the outage's, not a hole in a running broadcast:
                // not listed as a gap, and the media after it starts a
                // new run of the timeline.
                let across_outage = state.paused || state.outages != pending.outage;
                let outcome = if pending.kind == SegmentKind::Media && across_outage {
                    Committed::Withheld
                } else {
                    Committed::lost(&pending)
                };
                state.done.insert(pending.seq, outcome);
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
            state.advance(ctx.config.playlist_window, ctx.config.segment_ms)
        };
        if let Some(captured_at) = newest {
            publish_playlist(&ctx, PlaylistShape::Live, Some(captured_at)).await;
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
    // Freeze the live-edge lag and the duration *with the last
    // content*, before the closing playlist: what follows is
    // bookkeeping, and a slow final upload must not read as a broadcast
    // that fell behind, nor the recording's chunk tree (hundreds of
    // chunks after a long broadcast) as broadcast time that dilutes the
    // sustained figures.
    {
        let mut state = lock(&ctx.state);
        let lag = state.live_edge_lag_ms();
        state.lag_frozen_ms = Some(lag);
        state.ended_at = Some(Instant::now());
    }
    if lock(&ctx.state).recording.is_empty() {
        return;
    }
    // First the live window closed with `#EXT-X-ENDLIST`: a viewer
    // following the live edge stops reloading there, the way it always
    // has. It must reach that before the recording, which it could not
    // take in place of a live playlist: the recording's
    // `#EXT-X-MEDIA-SEQUENCE` is the broadcast's first segment, behind
    // the window's, and a live client (the web player's stale-read guard,
    // Solar Punk's feed poller) reads a media sequence that moves back as
    // an older playlist, not as the end. Solar Punk's uploader closes its
    // window and then writes the recording in the same order.
    publish_playlist(&ctx, PlaylistShape::Closing, None).await;
    // Then the whole broadcast as the final update (issue #168), so the
    // feed's head plays from the first segment to the last rather than
    // the closing window's last minute.
    publish_playlist(&ctx, PlaylistShape::Recording, None).await;
}

/// Upload a playlist (`shape`: the live window, the closing window or
/// the recording) and publish it as the next feed update.
/// `captured_at`, when present, is the newest committed segment — its
/// live-edge lag is only known once the feed update lands. How the
/// playlist goes up and what the update wraps depends on the
/// [`StreamFormat`]; see [`upload_playlist`].
async fn publish_playlist(ctx: &Arc<RunCtx>, shape: PlaylistShape, captured_at: Option<Instant>) {
    let body = {
        let state = lock(&ctx.state);
        match shape {
            PlaylistShape::Live => state.render_playlist(ctx.config.segment_ms, false),
            PlaylistShape::Closing => state.render_playlist(ctx.config.segment_ms, true),
            PlaylistShape::Recording => state.render_recording(ctx.config.segment_ms),
        }
    };
    let (playlist_ref, root) = match upload_playlist(ctx, &body).await {
        Ok(uploaded) => uploaded,
        Err(message) => {
            let what = if shape == PlaylistShape::Recording {
                "recording"
            } else {
                "playlist"
            };
            lock(&ctx.state).record_error(format!("{what}: {message}"));
            return;
        }
    };
    {
        let mut state = lock(&ctx.state);
        state.playlists_published += 1;
        if shape == PlaylistShape::Recording {
            state.recording_reference = Some(playlist_ref.clone());
        }
        state.playlist_reference = Some(playlist_ref);
    }

    // Take the index and mark it in flight in one lock, against
    // `LiveRun::seal_feed`: once the stop call has handed `feed_index`
    // out, nothing may land there.
    let index = {
        let mut state = lock(&ctx.state);
        if state.feed_sealed {
            let index = state.feed_index;
            state.record_error(format!(
                "feed update {index} skipped: stop returned before it, and \
                 its report gave {index} as the next broadcast's start_index"
            ));
            return;
        }
        state.feed_update_in_flight = true;
        state.feed_index
    };
    let result = publish_feed_update(ctx, index, &root).await;
    {
        let mut state = lock(&ctx.state);
        state.feed_update_in_flight = false;
        match result {
            Ok(()) => {
                state.feed_index += 1;
                state.feed_updates += 1;
                if let Some(captured_at) = captured_at {
                    state.note_lag(captured_at);
                }
            }
            Err(message) => {
                state.record_error(format!("feed update {index}: {message}"));
            }
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
    if shape == PlaylistShape::Live && lock(&ctx.state).channel_reference.is_none() {
        ctx.spawn_channel_manifest();
    }
}

/// Upload `body` (the playlist) and build the content-addressed chunk the
/// next feed update wraps, `(address, span ‖ payload)`. Returns the
/// playlist's reference alongside.
///
/// - [`StreamFormat::Bzz`]: `POST /bzz` the playlist as `stream.m3u8`,
///   and wrap bee's **v1** update payload `timestamp_be8 ‖ reference`
///   (bee-js `FeedWriter.upload`). `/bzz/<channel>/stream.m3u8` then
///   resolves the feed to the playlist's manifest and its file.
/// - [`StreamFormat::SolarPunk`]: `POST /bytes` the playlist and wrap its
///   own root chunk, a **v2** update (bee-js `FeedWriter.uploadPayload`,
///   what Solar Punk's uploader publishes). The SOC payload *is* the
///   playlist: the live window is budgeted to fit in one chunk
///   ([`LIVE_WINDOW_MAX_BYTES`]), so a viewer's poll is a single chunk
///   read. The recording written at stop is not budgeted (it lists the
///   whole broadcast), so past one chunk the update wraps the root of
///   its chunk tree, span = the whole playlist's length, and a reader
///   joins the leaves under it (as does a lone live segment whose own
///   lines are past the budget, which no real reference is). Both
///   `GET /soc/{owner}/{id}` and `GET /feeds/{owner}/{topic}` answer
///   with the playlist text, on bee and on ant. (Readers tell a v1
///   update by its 40- or 72-byte payload; the playlist's fixed header
///   lines alone are longer, so it is never mistaken for one.)
async fn upload_playlist(
    ctx: &RunCtx,
    body: &str,
) -> Result<(String, ([u8; 32], Vec<u8>)), String> {
    match ctx.config.format {
        StreamFormat::Bzz => {
            let playlist_ref = publish_bzz(
                &ctx.target,
                ctx.batch,
                PLAYLIST_NAME,
                "application/vnd.apple.mpegurl",
                body.as_bytes(),
            )
            .await?;
            let reference = parse_hex32(&playlist_ref).map_err(|e| format!("reference {e}"))?;
            let wrapped = v1_update_chunk(&reference)?;
            Ok((playlist_ref, wrapped))
        }
        StreamFormat::SolarPunk => {
            // The level is sent rather than left to the gateway's default
            // so the root chunk rebuilt below is the one the gateway
            // stored. It is that default (bee's MEDIUM), which is also
            // what bee-js `uploadPayload` gets for a playlist too big for
            // one chunk.
            let level = PLAYLIST_REDUNDANCY_LEVEL.to_string();
            let headers = [("swarm-redundancy-level".to_string(), level)];
            let playlist_ref =
                publish_bytes(&ctx.target, ctx.batch, body.as_bytes(), &headers).await?;
            let root = playlist_root_chunk(body.as_bytes(), &playlist_ref)?;
            Ok((playlist_ref, root))
        }
    }
}

/// Bee's v1 feed-update chunk for `reference`: the content-addressed
/// chunk of `timestamp_be8 ‖ reference`, the payload every bee version's
/// feed getter dereferences.
fn v1_update_chunk(reference: &[u8; 32]) -> Result<([u8; 32], Vec<u8>), String> {
    let mut payload = Vec::with_capacity(8 + 32);
    payload.extend_from_slice(
        &SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .to_be_bytes(),
    );
    payload.extend_from_slice(reference);
    ant_crypto::cac_new(&payload).ok_or_else(|| "feed payload too large".to_string())
}

/// Playlist file name under [`StreamFormat::Bzz`]. Constant so every
/// republish lands at the same manifest path and a viewer's URI stays
/// stable across updates.
const PLAYLIST_NAME: &str = "stream.m3u8";

/// The URI a viewer's HLS client fetches an uploaded object from under
/// [`StreamFormat::Bzz`]. Root-relative so the same playlist works
/// against the in-process gateway, `antd`, and a public bee gateway (all
/// serve `/bzz/` at the root of their API).
fn bzz_uri(reference: &str, name: &str) -> String {
    format!("/bzz/{reference}/{name}")
}

/// Redundancy level the playlist is uploaded at: bee's
/// `DefaultUploadLevel` (MEDIUM), what both bee and ant apply to an
/// upload that names none.
const PLAYLIST_REDUNDANCY_LEVEL: u8 = 1;

/// The root chunk (`span ‖ payload` wire) of `body` uploaded with
/// `POST /bytes` at [`PLAYLIST_REDUNDANCY_LEVEL`], rebuilt locally rather
/// than read back from the gateway, and checked against the reference
/// the gateway answered: a feed update must never wrap a chunk that
/// isn't the stored playlist's root.
fn playlist_root_chunk(body: &[u8], reference: &str) -> Result<([u8; 32], Vec<u8>), String> {
    let split = ant_retrieval::split_bytes_with_redundancy(body, PLAYLIST_REDUNDANCY_LEVEL);
    if hex::encode(split.root) != reference {
        return Err(format!(
            "gateway stored it as {reference}, expected root {}",
            hex::encode(split.root),
        ));
    }
    Ok((split.root, split.root_wire))
}

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
/// `id = keccak256(topic ‖ index_be8)` wrapping `wrapped`, the chunk
/// [`upload_playlist`] built: bee's v1 `timestamp ‖ reference` chunk
/// ([`StreamFormat::Bzz`]) or the playlist's own root chunk, a v2 update
/// ([`StreamFormat::SolarPunk`]).
///
/// These are the shapes bee-js's `FeedWriter.upload` and
/// `FeedWriter.uploadPayload` produce and bee's feed getter resolves,
/// which is what lets a **public** bee
/// gateway serve the channel even though the segments were pushed from a
/// phone.
async fn publish_feed_update(
    ctx: &RunCtx,
    index: u64,
    wrapped: &([u8; 32], Vec<u8>),
) -> Result<(), String> {
    let id = ant_retrieval::sequence_update_id(&ctx.topic, index);
    // `wrapped` is `span(8 LE) ‖ payload`, which is both what the SOC
    // signature covers (through its address) and what `POST /soc` takes
    // as its body.
    let (cac_address, cac_wire) = wrapped;
    // Bee signs `keccak256(id ‖ inner_cac_address)` through its default
    // (EIP-191-wrapping) signer — see `bee/pkg/soc/soc.go::Sign` and
    // `ant_crypto::soc_valid`, which recovers the owner the same way.
    let mut digest_input = [0u8; 64];
    digest_input[..32].copy_from_slice(&id);
    digest_input[32..].copy_from_slice(cac_address);
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
    post_reference(&ctx.target, &path, &headers, cac_wire).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Publish path
// ---------------------------------------------------------------------------

/// Publish one body with `POST /bytes`, returning its reference.
///
/// Under [`StreamFormat::SolarPunk`], segments, initialization segments
/// and playlists all go up this way. A segment is listed in the playlist by this **bare reference**, the
/// form Solar Punk's uploader writes: a viewer turns it into
/// `<the gateway it chose>/bytes/<ref>`, which is what lets it pick a
/// gateway or its own node. A raw upload also carries none of the
/// mantaray chunks a `POST /bzz` adds on top of every body.
async fn publish_bytes(
    target: &Target,
    batch: [u8; 32],
    payload: &[u8],
    extra_headers: &[(String, String)],
) -> Result<String, String> {
    let path = format!("{}/bytes", target.prefix);
    let mut headers = vec![
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("swarm-postage-batch-id".to_string(), hex::encode(batch)),
    ];
    headers.extend_from_slice(extra_headers);
    post_reference(target, &path, &headers, payload).await
}

/// Publish one body with `POST /bzz`, returning its reference. This is
/// the call stage 1 measured with the bench, and the live loop's upload
/// under [`StreamFormat::Bzz`]; [`StreamFormat::SolarPunk`] uses
/// [`publish_bytes`] instead.
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
            topic_text: String::new(),
            format: StreamFormat::SolarPunk,
            gateway: "http://127.0.0.1:1633".into(),
            batch_id: "ab".repeat(32),
            segment_ms: 2000,
            bitrate_kbps: 900,
            max_in_flight: 4,
            max_backlog: 4,
            playlist_window: 3,
            notes: String::new(),
            start_index: 0,
            reap_ms: 0,
        }
    }

    // `render_playlist` writes each URI exactly as the upload produced
    // it, whatever the format, so the rendering tests below use the
    // default `"bzz"` format's shape (`/bzz/<ref>/<name>`, short fake
    // references). The `"solar-punk"` shape, a bare 64-hex reference,
    // is checked end to end against what was really uploaded in
    // `a_broadcast_publishes_segments_playlist_and_a_valid_feed_update`.
    fn media(uri: &str, discontinuity: bool) -> Committed {
        Committed::Media {
            uri: uri.into(),
            duration_ms: 2000,
            discontinuity,
            captured_at: Instant::now(),
            program_ms: 1_788_264_000_000,
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
        assert!(state.advance(3, 2000).is_some());

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

    /// A media segment that never landed (dropped or failed), placed
    /// at `program_ms` with 2 s of media.
    fn lost(discontinuity: bool, program_ms: u64) -> Committed {
        Committed::Lost {
            duration_ms: 2000,
            discontinuity,
            program_ms,
        }
    }

    /// Issue #169: a dropped segment keeps its place as an `#EXT-X-GAP`
    /// entry, in Solar Punk's shape (gap tag, its date, its `#EXTINF`,
    /// `gap-<sequence>`), and arms no discontinuity: the encoder kept
    /// running, so the media behind the hole continues the media in
    /// front of it.
    #[test]
    fn a_dropped_segment_is_a_gap_entry_not_a_discontinuity() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        let at = |n: u64| 1_788_264_000_000 + 2000 * n;
        let dated = |uri: &str, n: u64| Committed::Media {
            uri: uri.into(),
            duration_ms: 2000,
            discontinuity: false,
            captured_at: Instant::now(),
            program_ms: at(n),
        };
        state.done.insert(1, dated("/bzz/bb/seg-1.m4s", 0));
        state.done.insert(2, lost(false, at(1)));
        state.done.insert(3, lost(false, at(2)));
        state.done.insert(4, dated("/bzz/ee/seg-4.m4s", 3));
        assert!(state.advance(5, 2000).is_some());

        assert_eq!(
            state.render_playlist(2000, false),
            concat!(
                "#EXTM3U\n",
                "#EXT-X-VERSION:7\n",
                "#EXT-X-TARGETDURATION:2\n",
                "#EXT-X-MEDIA-SEQUENCE:0\n",
                "#EXT-X-MAP:URI=\"/bzz/aa/init-0.mp4\"\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n",
                "#EXTINF:2.000,\n",
                "/bzz/bb/seg-1.m4s\n",
                "#EXT-X-GAP\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:02.000Z\n",
                "#EXTINF:2.000,\n",
                "gap-1\n",
                "#EXT-X-GAP\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:04.000Z\n",
                "#EXTINF:2.000,\n",
                "gap-2\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:06.000Z\n",
                "#EXTINF:2.000,\n",
                "/bzz/ee/seg-4.m4s\n",
            ),
        );
        // A gap is not something a viewer can play.
        assert_eq!(state.segments_listed, 2);
    }

    /// A failed media upload is the same hole as a drop: `Committed::lost`
    /// is what both the backlog and a failed upload record.
    #[test]
    fn a_failed_media_upload_is_a_gap_and_a_failed_init_is_a_break() {
        let pending = |kind| Pending {
            seq: 0,
            kind,
            data: Vec::new(),
            duration_ms: 1980,
            discontinuity: false,
            captured_at: Instant::now(),
            program_ms: 1_788_264_000_000,
            outage: 0,
        };
        assert!(matches!(
            Committed::lost(&pending(SegmentKind::Media)),
            Committed::Lost {
                duration_ms: 1980,
                discontinuity: false,
                program_ms: 1_788_264_000_000,
            }
        ));
        assert!(matches!(
            Committed::lost(&pending(SegmentKind::Init)),
            Committed::InitFailed
        ));
    }

    /// The point of numbering the holes: a segment's media sequence
    /// number never changes while it is listed, however the window
    /// slides past the gaps around it. Each playlist numbers its entries
    /// consecutively from `#EXT-X-MEDIA-SEQUENCE`, so read every URI's
    /// number off every playlist and check they agree.
    #[test]
    fn segment_numbers_stay_put_as_the_window_slides_past_a_gap() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init.mp4".into(),
            },
        );
        let mut numbers = std::collections::HashMap::<String, u64>::new();
        for seq in 1..=40u64 {
            // Drops at 5, 6, 13 and every seventh from 20.
            let outcome = if [5, 6, 13].contains(&seq) || (seq >= 20 && seq % 7 == 0) {
                lost(false, 1_788_264_000_000 + 2000 * seq)
            } else {
                let mut m = media(&format!("/bzz/{seq:02x}/seg-{seq}.m4s"), false);
                if let Committed::Media { program_ms, .. } = &mut m {
                    *program_ms += 2000 * seq;
                }
                m
            };
            state.done.insert(seq, outcome);
            state.advance(4, 2000);
            let body = state.render_playlist(2000, false);
            if state.playlist.is_empty() {
                continue;
            }
            assert!(!body.contains("#EXT-X-DISCONTINUITY"), "{body}");
            let first: u64 = body
                .lines()
                .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
                .unwrap()
                .parse()
                .unwrap();
            let uris: Vec<&str> = body.lines().filter(|l| !l.starts_with('#')).collect();
            // Never starts or ends on a gap.
            assert!(!uris[0].starts_with("gap-"), "{body}");
            assert!(!uris.last().unwrap().starts_with("gap-"), "{body}");
            for (i, uri) in uris.iter().enumerate() {
                let number = first + i as u64;
                // The number is the segment's own place in the
                // broadcast (sequence 0 is the init segment).
                if let Some(gap) = uri.strip_prefix("gap-") {
                    assert_eq!(gap.parse::<u64>().unwrap(), number, "{body}");
                }
                let seen = *numbers.entry((*uri).to_string()).or_insert(number);
                assert_eq!(seen, number, "{uri} renumbered:\n{body}");
            }
        }
        // Every media segment took a number, lost ones included.
        assert_eq!(state.next_number, 40);
        assert_eq!(numbers["/bzz/28/seg-40.m4s"], 39);
        assert_eq!(numbers["gap-34"], 34);
    }

    /// A gap entry never starts a window: once the segment in front of a
    /// hole slides out, the hole goes with it, so
    /// `#EXT-X-MEDIA-SEQUENCE` is always a segment that landed. Nor does
    /// one end a playlist: a lost segment with nothing behind it yet is
    /// held back, and one still held back when the broadcast closes is
    /// never listed.
    #[test]
    fn a_gap_never_starts_or_ends_a_playlist() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/b1/seg-1.m4s", false));
        state.done.insert(2, lost(false, 1));
        state.done.insert(3, media("/bzz/b3/seg-3.m4s", false));
        state.advance(2, 2000);
        assert!(state.render_playlist(2000, false).contains("gap-1\n"));
        // seg-1 slides out, and the gap behind it with it.
        state.done.insert(4, media("/bzz/b4/seg-4.m4s", false));
        state.done.insert(5, lost(false, 1));
        assert!(state.advance(2, 2000).is_some());
        let closing = state.render_playlist(2000, true);
        assert!(closing.contains("#EXT-X-MEDIA-SEQUENCE:2\n"), "{closing}");
        assert!(!closing.contains("#EXT-X-GAP"), "{closing}");
        assert!(
            closing.ends_with("/bzz/b4/seg-4.m4s\n#EXT-X-ENDLIST\n"),
            "{closing}"
        );
        // The trailing loss waits for the next segment, and is listed
        // with it at its own number.
        state.done.insert(6, media("/bzz/b6/seg-6.m4s", false));
        state.advance(2, 2000);
        let body = state.render_playlist(2000, false);
        assert!(body.contains("#EXT-X-GAP\n"), "{body}");
        assert!(
            body.contains("\ngap-4\n#EXT-X-PROGRAM-DATE-TIME:"),
            "{body}"
        );
        assert!(body.ends_with("/bzz/b6/seg-6.m4s\n"), "{body}");
        assert!(body.contains("#EXT-X-MEDIA-SEQUENCE:3\n"), "{body}");
    }

    /// Discontinuity stays for real breaks: a lost segment the capture
    /// side flagged as a new writer's first carries the break on its gap
    /// entry, where the new timeline starts, and a writer restart behind
    /// a hole still marks the restart.
    #[test]
    fn a_writer_restart_next_to_a_gap_is_still_a_discontinuity() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/b1/seg-1.m4s", false));
        state.done.insert(2, lost(false, 1_788_264_002_000));
        state.done.insert(
            3,
            Committed::Init {
                uri: "/bzz/cc/init-3.mp4".into(),
            },
        );
        // The new writer's first segment is lost too.
        state.done.insert(4, lost(true, 1_788_264_064_000));
        state.done.insert(5, media("/bzz/b5/seg-5.m4s", false));
        state.advance(8, 2000);
        let body = state.render_playlist(2000, false);
        assert_eq!(body.matches("#EXT-X-DISCONTINUITY\n").count(), 1, "{body}");
        assert!(
            body.contains(concat!(
                "/bzz/b1/seg-1.m4s\n",
                "#EXT-X-GAP\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:02.000Z\n",
                "#EXTINF:2.000,\n",
                "gap-1\n",
                "#EXT-X-DISCONTINUITY\n",
                "#EXT-X-MAP:URI=\"/bzz/cc/init-3.mp4\"\n",
                "#EXT-X-GAP\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:01:04.000Z\n",
                "#EXTINF:2.000,\n",
                "gap-2\n",
                "#EXT-X-PROGRAM-DATE-TIME:",
            )),
            "{body}"
        );
        assert!(body.ends_with("/bzz/b5/seg-5.m4s\n"), "{body}");
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
        assert!(state.advance(5, 2000).is_some());

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

    // -----------------------------------------------------------------
    // #EXT-X-PROGRAM-DATE-TIME from one go-live anchor (#170)
    // -----------------------------------------------------------------

    /// 2026-09-01T12:00:00.000Z, the instant in Solar Punk's README
    /// example.
    const GO_LIVE_MS: u64 = 1_788_264_000_000;

    #[test]
    fn program_date_time_is_iso_8601_utc_to_the_millisecond() {
        // Byte-for-byte what JavaScript's `toISOString` writes (checked
        // against it for each), which is what Solar Punk stamps.
        for (ms, text) in [
            (0, "1970-01-01T00:00:00.000Z"),
            (GO_LIVE_MS, "2026-09-01T12:00:00.000Z"),
            (GO_LIVE_MS + 1_001, "2026-09-01T12:00:01.001Z"),
            (1_709_251_199_999, "2024-02-29T23:59:59.999Z"),
            (951_868_800_000, "2000-03-01T00:00:00.000Z"),
            (4_102_444_800_123, "2100-01-01T00:00:00.123Z"),
        ] {
            assert_eq!(iso8601_utc_ms(ms), text, "{ms}");
        }
    }

    #[test]
    fn segments_are_dated_from_one_anchor_stepped_by_their_media() {
        let mut clock = ProgramClock::default();
        assert_eq!(clock.place(SegmentKind::Init, 0, false, GO_LIVE_MS), 0);
        // The first segment closes 2.033 s after go-live and reaches the
        // publisher 150 ms later: it is dated at its first frame, not at
        // its arrival.
        let first = clock.place(SegmentKind::Media, 2033, false, GO_LIVE_MS + 2033 + 150);
        assert_eq!(first, GO_LIVE_MS + 150);
        // Later arrivals jitter by hundreds of ms either way; the dates
        // step by exactly the media each segment held.
        let mut expected = first + 2033;
        for (duration, jitter) in [(2000, 900), (2067, 0), (1967, 450), (4033, 20)] {
            let at = clock.place(SegmentKind::Media, duration, false, expected + jitter);
            assert_eq!(at, expected);
            expected += u64::from(duration);
        }
    }

    #[test]
    fn a_writer_restart_re_anchors_on_the_wall_clock_and_never_goes_back() {
        let mut clock = ProgramClock::default();
        let _ = clock.place(SegmentKind::Init, 0, false, GO_LIVE_MS);
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS + 2000),
            GO_LIVE_MS,
        );
        // Backgrounded for 40 s: a fresh writer, a fresh init, and the
        // first segment after it carries the time it was really shot.
        let back = GO_LIVE_MS + 2000 + 40_000;
        let _ = clock.place(SegmentKind::Init, 0, false, back);
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, true, back + 2000),
            back
        );
        // Steps on from the new anchor.
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, back + 4000),
            back + 2000,
        );
        // A camera flip right away, on a wall clock that reads earlier
        // than where the dates have got to (it stepped back, or the
        // previous writer's media ran ahead of it): the re-anchor is
        // floored at the previous segment's end, never behind it.
        let _ = clock.place(SegmentKind::Init, 0, false, back + 3000);
        assert_eq!(
            clock.place(SegmentKind::Media, 1000, true, back + 3000),
            back + 4000,
        );
        // A discontinuity flagged on a media segment alone re-anchors too.
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, true, back + 70_000),
            back + 68_000,
        );
    }

    #[test]
    fn an_hour_long_broadcast_ends_within_one_segment_of_the_wall_clock() {
        // Issue #170's acceptance: one hour of keyframe-cut segments that
        // overrun the configured 2 s by up to 67 ms (the shape Solar Punk
        // measured), each reaching the publisher 0-400 ms after it
        // closed, with a 30 s interruption in the middle.
        let durations = [2000_u32, 2033, 2067, 2000, 1967, 2033];
        let mut clock = ProgramClock::default();
        let mut shot_at = GO_LIVE_MS; // true wall clock at a segment's first frame
        let mut last_stamp = 0;
        let mut last_duration = 0;
        let mut nominal = None; // what stepping by `segment_ms` would say
        let mut i = 0_u64;
        let _ = clock.place(SegmentKind::Init, 0, false, shot_at);
        while shot_at < GO_LIVE_MS + 3_600_000 {
            if i == 900 {
                shot_at += 30_000;
                let _ = clock.place(SegmentKind::Init, 0, false, shot_at);
            }
            let duration = durations[(i % 6) as usize];
            let arrival = shot_at + u64::from(duration) + (i * 7919) % 400;
            let stamp = clock.place(SegmentKind::Media, duration, i == 900, arrival);
            assert!(
                stamp >= last_stamp + u64::from(last_duration),
                "segment {i}"
            );
            let n = nominal.get_or_insert(stamp);
            if i == 900 {
                *n = stamp;
            }
            let nominal_stamp = *n;
            *n += 2000;
            last_stamp = stamp;
            last_duration = duration;
            if shot_at + u64::from(duration) >= GO_LIVE_MS + 3_600_000 {
                assert!(
                    stamp.abs_diff(shot_at) <= u64::from(duration),
                    "last segment dated {} ms off its capture",
                    stamp.abs_diff(shot_at),
                );
                // Stepping by the nominal length instead, what Solar
                // Punk found drifting by minutes an hour, misses it even
                // with the drift reset half way by the interruption.
                assert!(
                    nominal_stamp.abs_diff(shot_at) > u64::from(duration),
                    "nominal stepping drifted only {} ms",
                    nominal_stamp.abs_diff(shot_at),
                );
            }
            shot_at += u64::from(duration);
            i += 1;
        }
        assert!(i > 1700, "{i} segments");
    }

    #[test]
    fn every_listed_segment_carries_its_stored_date_in_front_of_extinf() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        let dated = |uri: &str, discontinuity: bool, program_ms: u64| Committed::Media {
            uri: uri.into(),
            duration_ms: 2000,
            discontinuity,
            captured_at: Instant::now(),
            program_ms,
        };
        state
            .done
            .insert(1, dated("/bzz/bb/seg-1.m4s", false, GO_LIVE_MS));
        state.done.insert(2, lost(false, GO_LIVE_MS + 2000));
        state.done.insert(
            3,
            Committed::Init {
                uri: "/bzz/cc/init-3.mp4".into(),
            },
        );
        state
            .done
            .insert(4, dated("/bzz/dd/seg-4.m4s", true, GO_LIVE_MS + 41_500));
        state.advance(5, 2000);
        let playlist = state.render_playlist(2000, false);
        assert!(
            playlist.contains(concat!(
                "#EXT-X-MAP:URI=\"/bzz/aa/init-0.mp4\"\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n",
                "#EXTINF:2.000,\n",
                "/bzz/bb/seg-1.m4s\n",
                "#EXT-X-GAP\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:02.000Z\n",
                "#EXTINF:2.000,\n",
                "gap-1\n",
                "#EXT-X-DISCONTINUITY\n",
                "#EXT-X-MAP:URI=\"/bzz/cc/init-3.mp4\"\n",
                "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:41.500Z\n",
                "#EXTINF:2.000,\n",
                "/bzz/dd/seg-4.m4s\n",
            )),
            "{playlist}",
        );
        assert_eq!(
            playlist.matches("#EXT-X-PROGRAM-DATE-TIME:").count(),
            playlist.matches("#EXTINF:").count(),
        );
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
        state.done.insert(2, lost(false, 0));
        state.done.insert(3, media("/bzz/b3/seg-3.m4s", false));
        // A writer restart: the one real break here.
        state.done.insert(
            4,
            Committed::Init {
                uri: "/bzz/cc/init-4.mp4".into(),
            },
        );
        state.done.insert(5, media("/bzz/b5/seg-5.m4s", true));
        state.done.insert(6, media("/bzz/b6/seg-6.m4s", false));
        state.done.insert(7, media("/bzz/b7/seg-7.m4s", false));
        // Window of 2: everything up to and including the segment
        // carrying the discontinuity falls out.
        assert!(state.advance(2, 2000).is_some());

        let playlist = state.render_playlist(2000, false);
        assert!(!playlist.contains("/bzz/b1/seg-1.m4s"), "{playlist}");
        // Six media segments were pushed (numbers 0-5, the lost one at
        // 1); the window holds the last two, so it starts at number 4.
        assert!(playlist.contains("#EXT-X-MEDIA-SEQUENCE:4\n"), "{playlist}");
        assert!(
            playlist.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"),
            "{playlist}"
        );
        // Every listed segment still carries a map.
        assert!(playlist.contains("#EXT-X-MAP:URI=\"/bzz/cc/init-4.mp4\""));
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
                program_ms: 1_788_264_000_000,
            },
        );
        state.advance(2, 2000);
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
        state.advance(2, 2000);
        let playlist = state.render_playlist(2000, false);
        assert!(!playlist.contains("seg-2.m4s"), "{playlist}");
        assert!(playlist.contains("#EXT-X-TARGETDURATION:4\n"), "{playlist}");
    }

    #[test]
    fn media_before_its_init_segment_is_never_listed() {
        // The initialization segment failed to upload: listing the media
        // segments anyway would produce a playlist no player can start.
        let mut state = PublisherState::default();
        state.done.insert(0, Committed::InitFailed);
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        assert!(state.advance(3, 2000).is_none());
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
        state.advance(3, 2000);
        assert!(state
            .render_playlist(2000, true)
            .ends_with("#EXT-X-ENDLIST\n"));
    }

    // -----------------------------------------------------------------
    // The recording (#168)
    // -----------------------------------------------------------------

    /// The lines of a playlist that name an entry (segment or gap).
    fn entry_uris(playlist: &str) -> Vec<&str> {
        playlist.lines().filter(|l| !l.starts_with('#')).collect()
    }

    /// Long after the window slid past them, the recording still lists
    /// every entry the broadcast ever listed: gaps, a writer restart's
    /// discontinuity and fresh map where they were, the run-max target
    /// duration, closed as VOD and numbered from the first segment with
    /// no discontinuity sequence. A gap still waiting for media when the
    /// broadcast ends is not listed, as in the closing window.
    #[test]
    fn the_recording_lists_every_entry_the_window_slid_past() {
        let mut state = PublisherState {
            broadcast_id: "b1".into(),
            ..PublisherState::default()
        };
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        state.done.insert(1, media("/bzz/b1/seg-1.m4s", false));
        state.done.insert(2, lost(false, 1_788_264_002_000));
        state.done.insert(
            3,
            Committed::Media {
                uri: "/bzz/b3/seg-3.m4s".into(),
                duration_ms: 3400,
                discontinuity: false,
                captured_at: Instant::now(),
                program_ms: 1_788_264_004_000,
            },
        );
        state.done.insert(
            4,
            Committed::Init {
                uri: "/bzz/cc/init-4.mp4".into(),
            },
        );
        state.done.insert(5, media("/bzz/b5/seg-5.m4s", true));
        state.done.insert(6, media("/bzz/b6/seg-6.m4s", false));
        state.done.insert(7, media("/bzz/b7/seg-7.m4s", false));
        state.done.insert(8, lost(false, 1_788_264_020_000));
        assert!(state.advance(2, 2000).is_some());

        let window = state.render_playlist(2000, true);
        assert_eq!(
            entry_uris(&window),
            ["/bzz/b6/seg-6.m4s", "/bzz/b7/seg-7.m4s"],
            "{window}"
        );
        assert!(window.contains("#EXT-X-DISCONTINUITY-SEQUENCE:1\n"));

        let recording = state.render_recording(2000);
        assert_eq!(
            recording,
            "#EXTM3U\n\
             #EXT-X-VERSION:7\n\
             #ANTSTREAM-BROADCAST:b1\n\
             #EXT-X-TARGETDURATION:4\n\
             #EXT-X-PLAYLIST-TYPE:VOD\n\
             #EXT-X-MEDIA-SEQUENCE:0\n\
             #EXT-X-MAP:URI=\"/bzz/aa/init-0.mp4\"\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n\
             #EXTINF:2.000,\n/bzz/b1/seg-1.m4s\n\
             #EXT-X-GAP\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:02.000Z\n\
             #EXTINF:2.000,\ngap-1\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:04.000Z\n\
             #EXTINF:3.400,\n/bzz/b3/seg-3.m4s\n\
             #EXT-X-DISCONTINUITY\n\
             #EXT-X-MAP:URI=\"/bzz/cc/init-4.mp4\"\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n\
             #EXTINF:2.000,\n/bzz/b5/seg-5.m4s\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n\
             #EXTINF:2.000,\n/bzz/b6/seg-6.m4s\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n\
             #EXTINF:2.000,\n/bzz/b7/seg-7.m4s\n\
             #EXT-X-ENDLIST\n",
        );
    }

    /// A segment lost in front of the broadcast's first listed one is
    /// not worth a line in the recording either: it starts on media, at
    /// that media's number.
    #[test]
    fn the_recording_never_starts_on_a_gap() {
        let mut state = PublisherState::default();
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        state.done.insert(1, lost(false, 1_788_264_000_000));
        state.done.insert(2, media("/bzz/b2/seg-2.m4s", false));
        state.advance(3, 2000);
        let recording = state.render_recording(2000);
        assert!(!recording.contains("#EXT-X-GAP"), "{recording}");
        assert!(
            recording.contains("#EXT-X-MEDIA-SEQUENCE:1\n"),
            "{recording}"
        );
        assert_eq!(entry_uris(&recording), ["/bzz/b2/seg-2.m4s"]);
        // …and agrees with the live window on where it starts.
        assert!(state
            .render_playlist(2000, false)
            .contains("#EXT-X-MEDIA-SEQUENCE:1\n"));
    }

    /// A broadcast hundreds of segments long: the live window stays one
    /// chunk while the recording keeps all of them, the window's
    /// segments being its tail at the same numbers.
    #[test]
    fn the_recording_outgrows_the_chunk_the_window_fits_in() {
        let mut state = live_state();
        commit_media(&mut state, 1, 300, DEFAULT_PLAYLIST_WINDOW);
        let window = state.render_playlist(2000, true);
        let recording = state.render_recording(2000);
        assert!(window.len() <= LIVE_WINDOW_MAX_BYTES);
        assert!(
            recording.len() > 8 * LIVE_WINDOW_MAX_BYTES,
            "{}",
            recording.len()
        );
        let all = entry_uris(&recording);
        assert_eq!(all, (1..=300).map(bare_ref).collect::<Vec<_>>());
        let tail = entry_uris(&window);
        assert_eq!(tail, all[all.len() - tail.len()..]);
        assert!(recording.contains("#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert_eq!(
            recording.matches("#EXT-X-PROGRAM-DATE-TIME:").count(),
            300,
            "every segment keeps its date"
        );
    }

    // -----------------------------------------------------------------
    // Live window sized to one chunk (#171)
    // -----------------------------------------------------------------

    /// A bare 64-hex reference, the `"solar-punk"` URI shape, distinct
    /// per `i`.
    fn bare_ref(i: u64) -> String {
        hex::encode(ant_crypto::keccak256(&i.to_be_bytes()))
    }

    /// A state with a broadcast id (the header line every real playlist
    /// carries) and an initialization segment committed at sequence 0.
    fn live_state() -> PublisherState {
        let mut state = PublisherState {
            broadcast_id: format!("{:x}", 1_760_000_000_000_u64),
            ..PublisherState::default()
        };
        state.done.insert(
            0,
            Committed::Init {
                uri: bare_ref(u64::MAX),
            },
        );
        state.next_commit = 0;
        state
    }

    /// Commit `count` more media segments (starting at sequence `from`),
    /// one at a time the way the committer folds them in, and check the
    /// budget after every one.
    fn commit_media(state: &mut PublisherState, from: u64, count: u64, window: usize) {
        for seq in from..from + count {
            let mut entry = media(&bare_ref(seq), false);
            if let Committed::Media { program_ms, .. } = &mut entry {
                *program_ms += seq * 2000;
            }
            state.done.insert(seq, entry);
            state.advance(window, 2000);
            for endlist in [false, true] {
                let body = state.render_playlist(2000, endlist);
                assert!(
                    body.len() <= LIVE_WINDOW_MAX_BYTES,
                    "after seq {seq} the playlist is {} bytes",
                    body.len(),
                );
            }
        }
    }

    /// The bytes one more bare-reference segment would add.
    fn entry_bytes() -> usize {
        "#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n".len()
            + "#EXTINF:2.000,\n".len()
            + 64
            + 1
    }

    #[test]
    fn the_live_window_fills_one_chunk_not_a_segment_count() {
        let mut state = live_state();
        commit_media(&mut state, 1, 300, DEFAULT_PLAYLIST_WINDOW);
        let body = state.render_playlist(2000, false);
        let listed = state.playlist.len();
        // About 30 segments is what Solar Punk's uploader fits, a
        // date-time on each; this playlist also carries an `#EXT-X-MAP`.
        assert!(listed >= 29, "only {listed} segments listed:\n{body}");
        assert!(listed < 300);
        // Full: one more segment (and room for the closing line) would
        // not fit.
        let closing = state.render_playlist(2000, true);
        assert!(
            closing.len() + entry_bytes() > LIVE_WINDOW_MAX_BYTES,
            "{} bytes leaves room for another segment",
            closing.len(),
        );
        // The newest segments, numbered from where the window starts.
        assert_eq!(state.playlist.back().unwrap().uri, bare_ref(300));
        assert!(body.contains(&format!("#EXT-X-MEDIA-SEQUENCE:{}\n", 300 - listed)));
        // The map survives the slide: the first listed segment carries it.
        assert_eq!(body.matches("#EXT-X-MAP").count(), 1, "{body}");

        // What the feed update wraps is the playlist itself, one chunk:
        // `span ‖ payload` with the text as the whole payload.
        for text in [body, closing] {
            let split = ant_retrieval::split_bytes_with_redundancy(
                text.as_bytes(),
                PLAYLIST_REDUNDANCY_LEVEL,
            );
            let (root, wire) =
                playlist_root_chunk(text.as_bytes(), &hex::encode(split.root)).unwrap();
            assert_eq!(root, split.root);
            assert_eq!(
                &wire[8..],
                text.as_bytes(),
                "the SOC payload is the playlist"
            );
            assert_eq!(
                u64::from_le_bytes(wire[..8].try_into().unwrap()),
                text.len() as u64
            );
        }
    }

    #[test]
    fn the_default_window_is_bounded_by_bytes_not_by_its_count() {
        // The default cap never binds at default settings: that many
        // bare-reference segments are already past one chunk.
        assert!(DEFAULT_PLAYLIST_WINDOW * entry_bytes() > LIVE_WINDOW_MAX_BYTES);
        // …and a host's lower cap still does.
        let mut state = live_state();
        commit_media(&mut state, 1, 40, 6);
        assert_eq!(state.playlist.len(), 6);
    }

    #[test]
    fn gap_lines_count_against_the_budget() {
        // Every other segment dropped: each listed one has an
        // `#EXT-X-GAP` entry in front of it, which has to come out of
        // the same 4096 bytes.
        let mut clean = live_state();
        commit_media(&mut clean, 1, 200, DEFAULT_PLAYLIST_WINDOW);

        let mut gappy = live_state();
        for seq in 1..=400u64 {
            let outcome = if seq % 2 == 0 {
                lost(false, GO_LIVE_MS + 2000 * seq)
            } else {
                media(&bare_ref(seq), false)
            };
            gappy.done.insert(seq, outcome);
            gappy.advance(DEFAULT_PLAYLIST_WINDOW, 2000);
            assert!(gappy.render_playlist(2000, true).len() <= LIVE_WINDOW_MAX_BYTES);
        }
        let body = gappy.render_playlist(2000, false);
        let gaps = body.matches("#EXT-X-GAP\n").count();
        assert!(gaps > 10, "{body}");
        let listed = gappy.playlist.iter().filter(|e| !e.gap).count();
        // Between every two listed segments, one gap.
        assert_eq!(gaps + 1, listed, "{body}");
        assert!(
            listed < clean.playlist.len(),
            "gaps cost bytes: {listed} vs {}",
            clean.playlist.len(),
        );
        // A hole is not a break.
        assert!(!body.contains("#EXT-X-DISCONTINUITY"), "{body}");
        assert_eq!(gappy.discontinuity_sequence, 0);
    }

    #[test]
    fn writer_restarts_count_their_map_lines_against_the_budget() {
        // A fresh init before every segment: each listed one brings a
        // discontinuity and its own `#EXT-X-MAP` line.
        let mut state = live_state();
        for i in 1..=100u64 {
            state.done.insert(
                2 * i - 1,
                Committed::Init {
                    uri: bare_ref(1000 + i),
                },
            );
            state.done.insert(2 * i, media(&bare_ref(i), true));
            state.advance(DEFAULT_PLAYLIST_WINDOW, 2000);
            assert!(state.render_playlist(2000, true).len() <= LIVE_WINDOW_MAX_BYTES);
        }
        let body = state.render_playlist(2000, false);
        assert_eq!(
            body.matches("#EXT-X-MAP").count(),
            state.playlist.len(),
            "{body}"
        );
        assert!(state.playlist.len() >= 3, "{body}");
    }

    #[test]
    fn a_segment_too_big_for_the_budget_alone_is_still_listed() {
        // A playlist naming nothing is worse than one past a chunk: the
        // newest segment always stays.
        let mut state = live_state();
        commit_media(&mut state, 1, 5, DEFAULT_PLAYLIST_WINDOW);
        state
            .done
            .insert(6, media(&"/x".repeat(LIVE_WINDOW_MAX_BYTES), false));
        state.advance(DEFAULT_PLAYLIST_WINDOW, 2000);
        assert_eq!(state.playlist.len(), 1);
        // The next ordinary segment pushes it straight back out.
        state.done.insert(7, media(&bare_ref(7), false));
        state.advance(DEFAULT_PLAYLIST_WINDOW, 2000);
        assert_eq!(state.playlist.len(), 1);
        assert_eq!(state.playlist[0].uri, bare_ref(7));
        commit_media(&mut state, 8, 3, DEFAULT_PLAYLIST_WINDOW);
        assert_eq!(state.playlist.len(), 4);
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
        let (a, a_text) = c.resolve_topic().unwrap();
        assert_ne!(a, [0u8; 32]);
        // The derived topic is a text topic, so even a broadcast whose
        // host named none can be put in a catalog entry by its text.
        assert!(a_text.starts_with("antstream/test channel/"), "{a_text}");
        assert_eq!(a, topic_from_string(&a_text));
        // Two derivations must differ even inside one clock tick —
        // otherwise a quick stop + restart of the same channel would
        // write its fresh index 0 over the old feed's head.
        let (b, _) = c.resolve_topic().unwrap();
        assert_ne!(a, b, "derived topics must be per-broadcast");
        // An explicit topic is honoured verbatim, with or without `0x`.
        c.topic = "cd".repeat(32);
        assert_eq!(c.resolve_topic().unwrap(), ([0xcd; 32], String::new()));
        c.topic = "0xnothex".into();
        assert!(c.resolve_topic().is_err());
    }

    #[test]
    fn a_text_topic_is_hashed_like_bee_js_and_raw_hex_stays_an_option() {
        let mut c = config();
        c.topic = String::new();
        c.topic_text = "ladder-marker-vector-group".into();
        let (topic, text) = c.resolve_topic().unwrap();
        assert_eq!(text, "ladder-marker-vector-group");
        assert_eq!(
            hex::encode(topic),
            "0d5e5bb0926a65e53a03775874d917202164fd5d7c84dcdd118ab4294e172cc5",
        );
        // Hashed exactly as given: a reader hashes the catalog's string
        // as it is, so trimming here would name a different feed.
        c.topic_text = " ladder-marker-vector-group".into();
        assert_ne!(c.resolve_topic().unwrap().0, topic);
        // Both at once name two different feeds: refused, not guessed.
        c.topic = "cd".repeat(32);
        assert!(c.resolve_topic().is_err());
    }

    /// `Topic.fromString` vectors. The 64-bit prefixes are the frozen
    /// literals in Solar Punk's own test (streaming-monorepo
    /// `apps/hls-stream/packages/shared/test/ladderMarker.test.ts`,
    /// copied into stream-event-client's `src/shared/ladderMarker.ts`);
    /// the full values were computed with bee-js 13.1.0's
    /// `Topic.fromString(..).toHex()`, the version both pin.
    #[test]
    fn topic_from_string_matches_bee_js() {
        for (text, expected) in [
            (
                "ladder-marker-vector-group",
                "0d5e5bb0926a65e53a03775874d917202164fd5d7c84dcdd118ab4294e172cc5",
            ),
            (
                "3f2b8c4e-1d2a-4b6f-9e0a-7c5d4e3f2a1b",
                "3ee91f4778abbdde6476c3653851338239c10ed60b490f58919d9efa35567aa5",
            ),
            (
                "swarm-hls-feed-follow-vector",
                "e0324f32e2edd73e3cd532076d203c397b9764eac2395fc3baab5738ad1b29bf",
            ),
            // UTF-8, not UTF-16 or Latin-1.
            (
                "Straßenfest 🎥",
                "c1a81655ca877863b2cf445cd6fe3ace95d99b9a9068e6a35bd472a8fce6251c",
            ),
        ] {
            assert_eq!(hex::encode(topic_from_string(text)), expected, "{text}");
        }
    }

    /// The feed slot addresses Solar Punk freezes from bee-js 9.8.1's
    /// `feed/identifier.js` (stream-event-client
    /// `test/shared/feedFollow.test.ts`, topic
    /// `Topic.fromString('swarm-hls-feed-follow-vector')`): the same
    /// `keccak256(topic ‖ index_be8)` this publisher writes its updates
    /// at, from a text topic.
    #[test]
    fn feed_slots_from_a_text_topic_match_solar_punk() {
        let topic = topic_from_string("swarm-hls-feed-follow-vector");
        for (index, expected) in [
            (
                0,
                "1c2c0e3309184791ad5ec0ec2c97568088ecb784e970cd2ef67aa19889f029b3",
            ),
            (
                1,
                "4727d6cdbe1096edddda633d60d2ca9afd372ea0bcf4f9dcd6ba1f58c7caf807",
            ),
            (
                42,
                "8f8e0d58b0b180e8e8ad8775cfb8f9349f4ecfb28973b655e5cff650beee4654",
            ),
        ] {
            assert_eq!(
                hex::encode(ant_retrieval::sequence_update_id(&topic, index)),
                expected,
                "index {index}",
            );
        }
    }

    /// Ladder-marker identifiers (v2). Prefixes: the frozen vectors in
    /// Solar Punk's `ladderMarker.test.ts`; full values: its
    /// `ladderMarkerIdentifier` run against bee-js 13.1.0. The last
    /// period is past 32 bits, so a four-byte or little-endian period
    /// disagrees there.
    #[test]
    fn ladder_marker_identifier_matches_solar_punk() {
        for (group, period, expected) in [
            (
                "ladder-marker-vector-group",
                0,
                "f6b46271eff0479daab3bddee009a929d51ded30f493479af48849a28195261c",
            ),
            (
                "ladder-marker-vector-group",
                175_983_840,
                "a2f6848ee95311e3284ba21a2ea0380175074fdf3a698d82bf75b4d96c372df5",
            ),
            (
                "3f2b8c4e-1d2a-4b6f-9e0a-7c5d4e3f2a1b",
                1_099_511_627_781,
                "3359b2fcd770d32e242160a25918bc56c2393bf4d7b3c29238e8d814ea780669",
            ),
        ] {
            let id = ladder_marker_identifier(&topic_from_string(group), period);
            assert_eq!(hex::encode(id), expected, "{group} @ {period}");
        }
        assert_eq!(LADDER_MARKER_PERIOD_SECONDS, 10);
    }

    #[test]
    fn a_large_playlist_wraps_its_tree_root_and_a_small_one_itself() {
        // Up to one chunk the root *is* the playlist: the SOC payload a
        // Solar Punk viewer reads from `GET /soc` is the text.
        let small = b"#EXTM3U\n#EXT-X-VERSION:7\n".to_vec();
        let (address, wire) = ant_crypto::cac_new(&small).unwrap();
        let root = playlist_root_chunk(&small, &hex::encode(address)).unwrap();
        assert_eq!(root, (address, wire.clone()));
        assert_eq!(&wire[8..], small.as_slice());
        // Past one chunk the SOC wraps the tree's root, as bee-js
        // `uploadPayload` does, at the redundancy level the upload asked
        // for.
        let large = vec![b'#'; 9000];
        let split = ant_retrieval::split_bytes_with_redundancy(&large, 1);
        let root = playlist_root_chunk(&large, &hex::encode(split.root)).unwrap();
        assert_eq!(root.0, split.root);
        assert_ne!(&root.1[8..], &large[..root.1.len() - 8]);
        // A gateway that stored something else is an error, never a
        // feed update wrapping the wrong chunk.
        assert!(playlist_root_chunk(&large, &"00".repeat(32)).is_err());
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
        /// `POST /feeds` and `POST /soc` answer a counter, not a real
        /// address; `POST /bytes` answers the real one (see
        /// [`bytes_reference`]), since the publisher checks the playlist's
        /// against the root chunk it wraps.
        next_reference: u64,
        /// Refuse every `POST /bytes` and `POST /bzz` (segments and
        /// playlists alike, in either format).
        fail_uploads: bool,
        /// Refuse every feed update from now on, the segments
        /// themselves still uploading fine.
        fail_soc: bool,
        /// Refuse this many `POST /feeds` before accepting one.
        fail_feeds: u32,
        /// Extra latency on `POST /feeds` only.
        feeds_delay: Duration,
        /// Refuse the recording's upload (a playlist marked VOD) only.
        fail_recording: bool,
        /// Extra latency on every request carrying the recording (its
        /// upload and, under "solar-punk", the feed update wrapping it).
        recording_delay: Duration,
    }

    /// The reference bee and ant answer a `POST /bytes` with: the root of
    /// the body's chunk tree at the requested redundancy level (default
    /// MEDIUM, as on both).
    fn bytes_reference(request: &SeenRequest) -> String {
        let level = request
            .header("swarm-redundancy-level")
            .map_or(1, |l| l.parse().unwrap());
        hex::encode(ant_retrieval::split_bytes_with_redundancy(&request.body, level).root)
    }

    /// Requests that uploaded a playlist (`POST /bytes` of an HLS text).
    fn is_playlist(request: &SeenRequest) -> bool {
        request.method_path.ends_with("/bytes") && request.body.starts_with(b"#EXTM3U")
    }

    /// Requests that uploaded a segment (any other `POST /bytes`).
    fn is_segment(request: &SeenRequest) -> bool {
        request.method_path.ends_with("/bytes") && !is_playlist(request)
    }

    /// A gateway stub that answers bee-shaped `{"reference":...}` to
    /// `/bytes`, `/feeds` and `/soc`, recording every request.
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
                                    let is_recording = request
                                        .body
                                        .windows(24)
                                        .any(|w| w == b"#EXT-X-PLAYLIST-TYPE:VOD");
                                    slot.seen.push(request.clone());
                                    let fail_feeds = is_feeds && slot.fail_feeds > 0;
                                    if fail_feeds {
                                        slot.fail_feeds -= 1;
                                    }
                                    let fail = fail_feeds
                                        || (slot.fail_uploads
                                            && (request.method_path.ends_with("/bytes")
                                                || request.method_path.contains("/bzz?")))
                                        || (slot.fail_soc && request.method_path.contains("/soc/"))
                                        || (slot.fail_recording && is_recording);
                                    (
                                        fail,
                                        if is_feeds {
                                            slot.feeds_delay
                                        } else if is_recording {
                                            slot.recording_delay
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
                                    let reference = if request.method_path.ends_with("/bytes") {
                                        bytes_reference(&request)
                                    } else {
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
    async fn a_reused_channel_continues_the_feed_at_start_index() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.start_index = 41;
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
        tokio::time::sleep(Duration::from_millis(30)).await;
        run.cancel();
        settle(&run).await;

        let first_soc = lock(&log)
            .seen
            .iter()
            .find(|r| r.method_path.contains("/soc/"))
            .map(|r| r.method_path.clone())
            .expect("feed update written");
        let id_hex = first_soc
            .split_once("?sig=")
            .unwrap()
            .0
            .rsplit('/')
            .next()
            .unwrap()
            .to_string();
        assert_eq!(
            id_hex,
            hex::encode(ant_retrieval::sequence_update_id(&[0x11; 32], 41)),
            "the first update of a continued channel must sit at start_index",
        );
        let report = run.report();
        assert!(report.feed_updates > 0);
        assert_eq!(report.feed_index, 41 + report.feed_updates, "{report:?}");
        let playlist = lock(&log)
            .seen
            .iter()
            .rev()
            .find(|r| is_playlist(r))
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .expect("playlist uploaded");
        assert!(
            playlist.contains("\n#ANTSTREAM-BROADCAST:"),
            "playlists name their broadcast:\n{playlist}"
        );
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
        // 2. segments, raw `POST /bytes` uploads stamped with the batch.
        let segments: Vec<&SeenRequest> = seen.iter().filter(|r| is_segment(r)).collect();
        assert_eq!(segments.len(), 4, "init + 3 media segments");
        assert!(
            !seen.iter().any(|r| r.method_path.contains("/bzz")),
            "nothing goes up as a /bzz manifest any more",
        );
        // Uploads run concurrently, so find each by its bytes rather
        // than by arrival order.
        let by_body = |body: &[u8]| {
            *segments
                .iter()
                .find(|r| r.body == body)
                .expect("segment uploaded")
        };
        let segment = by_body(&[1u8; 4096]);
        assert_eq!(
            segment.header("swarm-postage-batch-id"),
            Some("ab".repeat(32).as_str()),
        );
        assert_eq!(
            segment.header("content-type"),
            Some("application/octet-stream")
        );
        // 3. the playlist, as an HLS media playlist listing every
        //    segment and its map by bare reference — the form Solar
        //    Punk's viewers resolve against `<gateway>/bytes/`.
        let playlists: Vec<&SeenRequest> = seen.iter().filter(|r| is_playlist(r)).collect();
        let playlist = playlists.last().expect("playlist uploaded");
        assert_eq!(playlist.header("swarm-redundancy-level"), Some("1"));
        let body = String::from_utf8(playlist.body.clone()).unwrap();
        assert!(body.starts_with("#EXTM3U"), "{body}");
        assert!(body.contains("#EXT-X-ENDLIST"), "final playlist:\n{body}");
        assert_eq!(body.matches("#EXTINF").count(), 3, "{body}");
        let init_ref = bytes_reference(by_body(&[7u8; 800]));
        assert!(
            body.contains(&format!("#EXT-X-MAP:URI=\"{init_ref}\"\n")),
            "{body}"
        );
        let uris: Vec<&str> = body.lines().filter(|l| !l.starts_with('#')).collect();
        let media_refs: Vec<String> = (0..3u8)
            .map(|i| bytes_reference(by_body(&[i; 4096])))
            .collect();
        assert_eq!(
            uris, media_refs,
            "segment lines are bare references:\n{body}"
        );
        assert!(uris.iter().all(|u| u.len() == 64 && hex::decode(u).is_ok()));

        // 4. the feed update: a single-owner chunk ant's own validator
        //    accepts, at the bee-js sequence id for index 0.
        let socs: Vec<&SeenRequest> = seen
            .iter()
            .filter(|r| r.method_path.contains("/soc/"))
            .collect();
        let soc = socs[0];
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
        // v2 update: the wrapped chunk is the playlist itself, `span(8
        // LE) ‖ playlist`, so the SOC payload a viewer reads is the text
        // — not bee's v1 `timestamp ‖ reference` pointer.
        assert_eq!(
            u64::from_le_bytes(soc.body[..8].try_into().unwrap()),
            playlists[0].body.len() as u64,
        );
        assert_eq!(&soc.body[8..], playlists[0].body.as_slice());
        // …and every update wraps the playlist uploaded right before it.
        assert_eq!(socs.len(), playlists.len(), "one update per playlist");
        for (soc, playlist) in socs.iter().zip(&playlists) {
            assert_eq!(&soc.body[8..], playlist.body.as_slice());
        }
    }

    /// Issue #168: stop closes the live window, then publishes the whole
    /// broadcast as the recording, the final feed update, and reports
    /// its reference. With a 3-segment window and 10 segments the
    /// closing window holds the last 3 and the recording all 10.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stop_publishes_the_closing_window_then_the_whole_recording() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.max_backlog = 16;
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..10u8 {
            let _ = run.push(SegmentKind::Media, vec![i; 1000], 2000, false);
        }
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.segments_listed, 10, "{report:?}");
        assert_eq!(report.error_count, 0, "{:?}", report.errors);

        let seen = lock(&log).seen.clone();
        let playlists: Vec<String> = seen
            .iter()
            .filter(|r| is_playlist(r))
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .collect();
        let [.., live, closing, recording] = playlists.as_slice() else {
            panic!("{playlists:?}");
        };
        assert!(!live.contains("#EXT-X-ENDLIST"), "{live}");
        assert!(closing.ends_with("#EXT-X-ENDLIST\n"), "{closing}");
        assert!(!closing.contains("#EXT-X-PLAYLIST-TYPE"), "{closing}");
        assert_eq!(entry_uris(closing).len(), 3, "{closing}");
        assert!(closing.contains("#EXT-X-MEDIA-SEQUENCE:7\n"), "{closing}");
        assert!(recording.contains("\n#EXT-X-PLAYLIST-TYPE:VOD\n"));
        assert!(recording.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"));
        assert!(recording.ends_with("#EXT-X-ENDLIST\n"));
        let segment_ref = |i: u8| {
            bytes_reference(
                seen.iter()
                    .find(|r| is_segment(r) && r.body == vec![i; 1000])
                    .expect("segment uploaded"),
            )
        };
        assert_eq!(
            entry_uris(recording),
            (0..10).map(segment_ref).collect::<Vec<_>>(),
            "every segment, in capture order:\n{recording}"
        );
        assert_eq!(entry_uris(closing), entry_uris(recording)[7..]);
        // Both were dated in the same broadcast id, which is what tells a
        // viewer the recording is this broadcast's, not a new one.
        let id_line = |p: &str| {
            p.lines()
                .find(|l| l.starts_with("#ANTSTREAM-BROADCAST:"))
                .map(str::to_string)
        };
        assert!(id_line(recording).is_some());
        assert_eq!(id_line(closing), id_line(recording));

        // The final update wraps the recording, at the last index.
        let last_soc = seen
            .iter()
            .rev()
            .find(|r| r.method_path.contains("/soc/"))
            .unwrap();
        assert_eq!(&last_soc.body[8..], recording.as_bytes());
        let id_hex = last_soc
            .method_path
            .split_once("?sig=")
            .unwrap()
            .0
            .rsplit('/')
            .next()
            .unwrap();
        assert_eq!(
            id_hex,
            hex::encode(ant_retrieval::sequence_update_id(
                &[0x11; 32],
                report.feed_updates - 1
            ))
        );
        let recording_request = seen.iter().rev().find(|r| is_playlist(r)).unwrap();
        assert_eq!(
            report.recording_reference,
            bytes_reference(recording_request)
        );
        assert_eq!(report.playlist_reference, report.recording_reference);
    }

    /// Start a broadcast against `log`'s stub, push ten segments, stop,
    /// and wait until the stub has seen a request matching `seen`.
    async fn stop_and_wait_for(
        log: &Arc<Mutex<StubLog>>,
        seen: impl Fn(&SeenRequest) -> bool,
    ) -> Arc<LiveRun> {
        let addr = stub_gateway(Arc::clone(log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.max_backlog = 16;
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..10u8 {
            let _ = run.push(SegmentKind::Media, vec![i; 1000], 2000, false);
        }
        run.cancel();
        for _ in 0..400 {
            if lock(log).seen.iter().any(&seen) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the stub never saw the request");
    }

    fn is_recording(request: &SeenRequest) -> bool {
        request
            .body
            .windows(24)
            .any(|w| w == b"#EXT-X-PLAYLIST-TYPE:VOD")
    }

    /// The SOC ids of every feed update the stub saw.
    fn soc_ids(log: &Arc<Mutex<StubLog>>) -> Vec<String> {
        lock(log)
            .seen
            .iter()
            .filter(|r| r.method_path.contains("/soc/"))
            .map(|r| {
                let path = r.method_path.split_once("?sig=").unwrap().0;
                path.rsplit('/').next().unwrap().to_string()
            })
            .collect()
    }

    /// The duration freezes with the last content: the closing window and
    /// the recording (a chunk tree after a long broadcast) are uploaded
    /// after it and are not broadcast time.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_recording_is_not_counted_as_broadcast_time() {
        let log = Arc::new(Mutex::new(StubLog {
            recording_delay: Duration::from_millis(1500),
            ..StubLog::default()
        }));
        let run = stop_and_wait_for(&log, |r| is_playlist(r) && is_recording(r)).await;
        settle(&run).await;
        let report = run.report();
        assert_ne!(report.recording_reference, "", "{report:?}");
        assert!(
            report.duration_s < 1.0,
            "the recording's 1.5 s upload counted as broadcast time: {report:?}"
        );
    }

    /// A stop that returns while the recording is still uploading (the
    /// grace ran out) seals the feed: the report's `feed_index`, which a
    /// host passes as the next broadcast's `start_index`, is never taken
    /// by the recording landing afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sealed_feed_leaves_the_reported_index_to_the_next_broadcast() {
        let log = Arc::new(Mutex::new(StubLog {
            recording_delay: Duration::from_millis(400),
            ..StubLog::default()
        }));
        let run = stop_and_wait_for(&log, |r| is_playlist(r) && is_recording(r)).await;
        // The closing window's update landed before the recording's
        // upload began; nothing is on the wire at the feed.
        assert!(!run.seal_feed(), "no feed update should be in flight");
        let handed_out = run.report().feed_index;
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.feed_index, handed_out, "{report:?}");
        let taken = hex::encode(ant_retrieval::sequence_update_id(&[0x11; 32], handed_out));
        assert!(
            !soc_ids(&log).contains(&taken),
            "a late update took the next broadcast's start_index {handed_out}"
        );
        // The recording itself still landed and plays by its reference;
        // the skipped update is reported.
        assert_ne!(report.recording_reference, "", "{report:?}");
        assert!(
            report.errors.iter().any(|e| e.contains("skipped")),
            "{:?}",
            report.errors
        );
    }

    /// Sealing while the recording's feed update is already on the wire:
    /// the seal reports it, and once it clears the index counts it, so
    /// what the stop call hands out is past it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sealing_waits_out_a_feed_update_already_on_the_wire() {
        let log = Arc::new(Mutex::new(StubLog {
            recording_delay: Duration::from_millis(400),
            ..StubLog::default()
        }));
        let run =
            stop_and_wait_for(&log, |r| r.method_path.contains("/soc/") && is_recording(r)).await;
        assert!(run.seal_feed(), "the recording's update is in flight");
        for _ in 0..400 {
            if !run.feed_update_in_flight() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!run.feed_update_in_flight());
        let handed_out = run.report().feed_index;
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.feed_index, handed_out, "{report:?}");
        let ids = soc_ids(&log);
        let last = hex::encode(ant_retrieval::sequence_update_id(
            &[0x11; 32],
            handed_out - 1,
        ));
        assert_eq!(
            ids.last(),
            Some(&last),
            "the in-flight update landed below it"
        );
        let taken = hex::encode(ant_retrieval::sequence_update_id(&[0x11; 32], handed_out));
        assert!(!ids.contains(&taken));
        assert_eq!(report.error_count, 0, "{:?}", report.errors);
    }

    /// A recording that will not upload leaves the closing window at the
    /// head (the broadcast's last minute, still closed), no recording
    /// reference, and says why.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_recording_upload_is_reported_and_leaves_the_closing_window() {
        let log = Arc::new(Mutex::new(StubLog {
            fail_recording: true,
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
        let _ = run.push(SegmentKind::Media, vec![1u8; 1000], 2000, false);
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.recording_reference, "", "{report:?}");
        assert!(
            report.errors.iter().any(|e| e.starts_with("recording: ")),
            "{:?}",
            report.errors
        );
        let seen = lock(&log).seen.clone();
        let last_soc = seen
            .iter()
            .rev()
            .find(|r| r.method_path.contains("/soc/"))
            .unwrap();
        let head = String::from_utf8(last_soc.body[8..].to_vec()).unwrap();
        assert!(head.ends_with("#EXT-X-ENDLIST\n"), "{head}");
        assert!(!head.contains("#EXT-X-PLAYLIST-TYPE"), "{head}");
        assert!(
            report.kept_up,
            "the live broadcast itself kept up: {report:?}"
        );
    }

    /// A host that names no `format` (the Android app, and any host that
    /// predates it) gets `"bzz"`: what the web player and the Android
    /// Watch tab read through `/bzz/<channel>/stream.m3u8`.
    #[test]
    fn the_format_defaults_to_bzz_and_parses_solar_punk() {
        let parse = |extra: &str| {
            serde_json::from_str::<PublisherConfig>(&format!(
                r#"{{"channel":"c","batch_id":"{}"{extra}}}"#,
                "ab".repeat(32)
            ))
        };
        assert_eq!(parse("").unwrap().format, StreamFormat::Bzz);
        assert_eq!(
            parse(r#","format":"solar-punk""#).unwrap().format,
            StreamFormat::SolarPunk
        );
        assert_eq!(
            parse(r#","format":"bzz""#).unwrap().format,
            StreamFormat::Bzz
        );
        assert!(
            parse(r#","format":"hls""#).is_err(),
            "unknown formats are refused"
        );
    }

    /// `"bzz"`: segments and the playlist go up as `/bzz` manifests, the
    /// playlist lists gateway-root `/bzz/<ref>/<name>` paths, and each
    /// feed update is bee's v1 `timestamp ‖ reference` of the playlist's
    /// manifest.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_bzz_format_writes_manifests_paths_and_v1_updates() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.format = StreamFormat::Bzz;
        let owner = test_owner();
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
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
        assert_eq!(report.format, StreamFormat::Bzz);
        assert!(report.feed_updates > 0);

        let seen = lock(&log).seen.clone();
        assert!(
            !seen.iter().any(|r| r.method_path.ends_with("/bytes")),
            "nothing goes up with POST /bytes in the bzz format",
        );
        let segments = seen
            .iter()
            .filter(|r| r.method_path.contains("/bzz?name=seg-"))
            .count();
        assert_eq!(segments, 3);
        assert!(seen
            .iter()
            .any(|r| r.method_path.contains("/bzz?name=init-0.mp4")));
        let playlists: Vec<&SeenRequest> = seen
            .iter()
            .filter(|r| r.method_path.contains("/bzz?name=stream.m3u8"))
            .collect();
        let body = String::from_utf8(playlists.last().expect("playlist").body.clone()).unwrap();
        assert!(body.contains("#EXT-X-ENDLIST"), "{body}");
        let uris: Vec<&str> = body.lines().filter(|l| !l.starts_with('#')).collect();
        assert_eq!(uris.len(), 3, "{body}");
        assert!(
            uris.iter()
                .all(|u| u.starts_with("/bzz/") && u.contains("/seg-")),
            "{body}"
        );
        assert!(body.contains("/init-0.mp4\"\n"), "{body}");

        // v1 update: `span(8 LE) = 40 ‖ timestamp_be8 ‖ reference`, the
        // reference being what the gateway answered for the playlist.
        let socs: Vec<&SeenRequest> = seen
            .iter()
            .filter(|r| r.method_path.contains("/soc/"))
            .collect();
        assert_eq!(socs.len(), playlists.len(), "one update per playlist");
        let soc = socs.last().unwrap();
        assert_eq!(soc.body.len(), 8 + 8 + 32);
        assert_eq!(u64::from_le_bytes(soc.body[..8].try_into().unwrap()), 40);
        assert_eq!(hex::encode(&soc.body[16..]), report.playlist_reference);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_text_topic_writes_where_a_solar_punk_reader_looks() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.topic = String::new();
        config.topic_text = "swarm-hls-feed-follow-vector".into();
        let owner = test_owner();
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            owner,
            None,
        )
        .unwrap();
        let snapshot = run.snapshot();
        assert_eq!(snapshot.topic_text, "swarm-hls-feed-follow-vector");
        assert_eq!(
            snapshot.topic,
            "e0324f32e2edd73e3cd532076d203c397b9764eac2395fc3baab5738ad1b29bf",
        );
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        tokio::time::sleep(Duration::from_millis(30)).await;
        run.cancel();
        settle(&run).await;
        assert_eq!(run.report().topic_text, "swarm-hls-feed-follow-vector");

        let seen = lock(&log).seen.clone();
        // Index 0 of `Topic.fromString('swarm-hls-feed-follow-vector')`,
        // the slot stream-event-client's frozen bee-js vector names.
        let soc = seen
            .iter()
            .find(|r| r.method_path.contains("/soc/"))
            .expect("feed update written");
        assert!(
            soc.method_path.starts_with(&format!(
                "POST /soc/{}/1c2c0e3309184791ad5ec0ec2c97568088ecb784e970cd2ef67aa19889f029b3?sig=",
                hex::encode(owner),
            )),
            "{}",
            soc.method_path,
        );
        let manifest = seen
            .iter()
            .find(|r| r.method_path.contains("/feeds/"))
            .expect("feed manifest created");
        assert!(
            manifest
                .method_path
                .ends_with("/e0324f32e2edd73e3cd532076d203c397b9764eac2395fc3baab5738ad1b29bf"),
            "{}",
            manifest.method_path,
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
        // create sits *between* listed segments, where it is listed.
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
        // The dropped segments are `#EXT-X-GAP` entries in the
        // published playlist, not a discontinuity: the encoder never
        // stopped (#169).
        let seen = lock(&log).seen.clone();
        let playlist = seen
            .iter()
            .rev()
            .find(|r| is_playlist(r))
            .expect("playlist uploaded");
        let body = String::from_utf8(playlist.body.clone()).unwrap();
        assert!(body.contains("#EXT-X-GAP\n"), "{body}");
        assert!(!body.contains("#EXT-X-DISCONTINUITY"), "{body}");
    }

    /// `(date, uri)` of every segment a playlist lists, in order.
    fn dated_uris(playlist: &str) -> Vec<(String, String)> {
        let lines: Vec<&str> = playlist.lines().collect();
        lines
            .windows(3)
            .filter_map(|w| {
                let date = w[0].strip_prefix("#EXT-X-PROGRAM-DATE-TIME:")?;
                assert!(w[1].starts_with("#EXTINF:"), "{playlist}");
                Some((date.to_string(), w[2].to_string()))
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_keep_their_capture_date_across_drops_and_a_restart() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        // As in the backlog test: one 200 ms upload at a time and a
        // backlog of 2, so a burst sheds segments.
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(200)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.max_in_flight = 1;
        config.max_backlog = 2;
        config.playlist_window = 64;
        let run = start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();

        // Segment i of the first writer was shot at GO_LIVE + 2 s·i and
        // pushed 2 s later plus jitter. After the first lands, a burst
        // piles the rest up so some are dropped, but every one is dated
        // when it is pushed.
        let push = |kind, byte: u8, duration, discontinuity, now_ms| {
            run.enqueue(kind, vec![byte; 64], duration, discontinuity, now_ms)
        };
        let settled = |published: u64| {
            let run = Arc::clone(&run);
            async move {
                for _ in 0..80 {
                    let s = run.snapshot();
                    if s.segments_published + s.segments_dropped >= published {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                panic!("segments never landed");
            }
        };
        let _ = push(SegmentKind::Init, 0xee, 0, false, GO_LIVE_MS);
        let _ = push(SegmentKind::Media, 0, 2000, false, GO_LIVE_MS + 2000);
        settled(1).await;
        let mut outcomes = Vec::new();
        for i in 1..8u8 {
            let shot = GO_LIVE_MS + 2000 * u64::from(i);
            let jitter = u64::from(i) * 97 % 500;
            outcomes.push(push(
                SegmentKind::Media,
                i,
                2000,
                false,
                shot + 2000 + jitter,
            ));
        }
        assert!(
            outcomes.contains(&PushOutcome::QueuedDroppingOldest),
            "{outcomes:?}",
        );
        settled(8).await;
        // The writer restarts after a minute away.
        let back = GO_LIVE_MS + 16_000 + 60_000;
        let _ = push(SegmentKind::Init, 0xef, 0, false, back);
        let _ = push(SegmentKind::Media, 100, 2000, true, back + 2000 + 300);
        settled(9).await;
        let _ = push(SegmentKind::Media, 101, 2000, false, back + 4000 + 10);
        run.cancel();
        settle(&run).await;
        assert!(run.report().segments_dropped > 0);

        let seen = lock(&log).seen.clone();
        // Segment bytes → the date its upload should be listed under.
        let mut expected = std::collections::HashMap::new();
        for i in 0..8u8 {
            expected.insert(i, iso8601_utc_ms(GO_LIVE_MS + 2000 * u64::from(i)));
        }
        expected.insert(100, iso8601_utc_ms(back + 300));
        expected.insert(101, iso8601_utc_ms(back + 2300));
        let by_reference: std::collections::HashMap<String, u8> = seen
            .iter()
            .filter(|r| is_segment(r) && r.body.len() == 64)
            .map(|r| (bytes_reference(r), r.body[0]))
            .collect();
        // Media sequence number of each pushed media segment: its place
        // in the broadcast, lost ones counted.
        let number = |byte: u8| {
            if byte < 100 {
                u64::from(byte)
            } else {
                u64::from(byte) - 92
            }
        };
        let playlist = seen.iter().rev().find(|r| is_playlist(r)).unwrap();
        let body = String::from_utf8(playlist.body.clone()).unwrap();
        let first: u64 = body
            .lines()
            .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .unwrap()
            .parse()
            .unwrap();
        let listed = dated_uris(&body);
        let gaps = listed
            .iter()
            .filter(|(_, uri)| uri.starts_with("gap-"))
            .count();
        assert_eq!(gaps as u64, run.report().segments_dropped, "{body}");
        assert_eq!(
            (listed.len() - gaps) as u64,
            run.report().segments_listed,
            "{body}"
        );
        for (i, (date, uri)) in listed.iter().enumerate() {
            let at = first + i as u64;
            let byte = match uri.strip_prefix("gap-") {
                // A dropped segment's gap entry carries the date it was
                // placed with, at its own number.
                Some(n) => {
                    assert_eq!(n.parse::<u64>().unwrap(), at, "{body}");
                    u8::try_from(at).unwrap()
                }
                None => by_reference[uri],
            };
            assert_eq!(number(byte), at, "segment {byte} numbered {at}:\n{body}");
            assert_eq!(date, &expected[&byte], "segment {byte}:\n{body}");
        }
        // Strictly increasing: a gap keeps its own date, never a step
        // backwards.
        assert!(listed.windows(2).all(|w| w[0].0 < w[1].0), "{body}");
        assert_eq!(by_reference[&listed[0].1], 0, "{body}");
        assert_eq!(listed.last().unwrap().0, expected[&101]);
        // The one discontinuity is the writer restart.
        assert_eq!(body.matches("#EXT-X-DISCONTINUITY\n").count(), 1, "{body}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejecting_gateway_is_reported_rather_than_counted_as_published() {
        let log = Arc::new(Mutex::new(StubLog {
            fail_uploads: true,
            ..StubLog::default()
        }));
        let addr = stub_gateway(Arc::clone(&log), Duration::ZERO).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        // The default format's `POST /bzz` failures count the same.
        config.format = StreamFormat::Bzz;
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
        state.advance(6, 2000);
        assert_eq!(state.segments_listed, 1);
        // Writer restart whose initialization segment fails to upload.
        state.done.insert(2, Committed::InitFailed);
        state.done.insert(3, media("/bzz/b3/seg-3.m4s", false));
        state.advance(6, 2000);
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
        let outcome = run.enqueue(
            SegmentKind::Media,
            vec![2u8; 4096],
            2000,
            false,
            unix_ms_now(),
        );
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.segments_dropped, 0, "{outcome:?} {report:?}");
        match outcome {
            PushOutcome::Closed => assert_eq!(report.segments_published, 1, "{report:?}"),
            PushOutcome::Queued => assert_eq!(report.segments_published, 2, "{report:?}"),
            PushOutcome::QueuedDroppingOldest | PushOutcome::Withheld => {
                panic!("unexpected {outcome:?}")
            }
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

    // -----------------------------------------------------------------
    // Interruptions pause, they never end (issue #172)
    // -----------------------------------------------------------------

    /// Folded by the committer, an outage is one seam: the withheld
    /// segments take no number and no line (not even `#EXT-X-GAP`), the
    /// first segment after them is behind one `#EXT-X-DISCONTINUITY`,
    /// and nothing closes the live window.
    #[test]
    fn an_outage_is_one_seam_not_a_run_of_gaps() {
        let mut state = live_state();
        commit_media(&mut state, 1, 3, 64);
        // 30 s of 2 s segments the outage withheld.
        for seq in 4..19 {
            state.done.insert(seq, Committed::Withheld);
            assert_eq!(state.advance(64, 2000), None, "silence publishes nothing");
        }
        commit_media(&mut state, 19, 3, 64);

        for body in [
            state.render_playlist(2000, false),
            state.render_recording(2000),
        ] {
            assert_eq!(body.matches("#EXT-X-DISCONTINUITY\n").count(), 1, "{body}");
            assert!(!body.contains("#EXT-X-GAP"), "{body}");
            assert_eq!(body.matches("#EXT-X-MAP:").count(), 1, "{body}");
            let uris = entry_uris(&body);
            assert_eq!(
                uris,
                [1, 2, 3, 19, 20, 21].map(bare_ref).to_vec(),
                "every segment that landed, in order:\n{body}"
            );
            // The seam sits right in front of the first segment back.
            let seam = body.find("#EXT-X-DISCONTINUITY\n").unwrap();
            assert!(body[seam..].find(&bare_ref(3)).is_none(), "{body}");
            assert!(body[seam..].contains(&bare_ref(19)), "{body}");
        }
        assert!(!state
            .render_playlist(2000, false)
            .contains("#EXT-X-ENDLIST"));
        // The numbers run on: the silence took none.
        let numbers: Vec<u64> = state.recording.iter().map(|e| e.number).collect();
        assert_eq!(numbers, vec![0, 1, 2, 3, 4, 5]);
    }

    /// Silence before anything was listed is no seam: there is nothing
    /// in front of the first segment to break from.
    #[test]
    fn an_outage_before_the_first_segment_is_not_a_seam() {
        let mut state = live_state();
        state.done.insert(1, Committed::Withheld);
        state.done.insert(2, Committed::Withheld);
        commit_media(&mut state, 3, 2, 64);
        let body = state.render_playlist(2000, false);
        assert!(!body.contains("#EXT-X-DISCONTINUITY"), "{body}");
    }

    /// The return re-anchors the dating on the wall clock it came back
    /// at, never moving backwards: the segments the outage swallowed
    /// were never placed, so the first one back is dated when it was
    /// really shot, not where the last one before the outage ended.
    #[test]
    fn the_return_re_anchors_the_dating() {
        let mut clock = ProgramClock::default();
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS + 2000),
            GO_LIVE_MS
        );
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS + 4000),
            GO_LIVE_MS + 2000
        );
        clock.break_timeline();
        // Back 30 s later.
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS + 36_000),
            GO_LIVE_MS + 34_000,
        );
        // A clock that ran slow cannot date it before the last segment
        // ended.
        let mut clock = ProgramClock::default();
        let _ = clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS + 2000);
        clock.break_timeline();
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS),
            GO_LIVE_MS + 2000
        );
    }

    /// The reap window runs from the latest of go-live, the last listed
    /// segment and the first return after it.
    #[test]
    fn the_reap_window_runs_from_the_later_of_the_last_segment_and_the_return() {
        let reap = Duration::from_secs(60);
        let started = Instant::now();
        let mut state = PublisherState::default();
        assert_eq!(state.reap_deadline(started, reap), started + reap);
        let media = started + Duration::from_secs(10);
        state.last_media_at = Some(media);
        assert_eq!(state.reap_deadline(started, reap), media + reap);
        // Back 26 s into the outage: the whole window again, not 34 s.
        let back = media + Duration::from_secs(26);
        state.returned_at = Some(back);
        assert_eq!(state.reap_deadline(started, reap), back + reap);
        // A later segment moves it on again.
        let later = back + Duration::from_secs(5);
        state.last_media_at = Some(later);
        assert_eq!(state.reap_deadline(started, reap), later + reap);
    }

    fn quiet_run(log: &Arc<Mutex<StubLog>>, addr: &str, reap_ms: u64) -> Arc<LiveRun> {
        let _ = log;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.reap_ms = reap_ms;
        config.playlist_window = 64;
        config.max_backlog = 64;
        start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap()
    }

    async fn wait_until(run: &Arc<LiveRun>, what: &str, done: impl Fn(&PublisherSnapshot) -> bool) {
        for _ in 0..400 {
            if done(&run.snapshot()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}: {:?}", run.snapshot());
    }

    /// Only the first return after the last listed segment buys a fresh
    /// window: a host that resumes again and again with nothing to show
    /// for it does not hold a dead broadcast open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_the_first_return_after_the_last_segment_restarts_the_window() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 0);
        run.resume();
        let first = lock(&run.state).returned_at.expect("a return");
        tokio::time::sleep(Duration::from_millis(5)).await;
        run.pause();
        run.resume();
        assert_eq!(
            lock(&run.state).returned_at,
            Some(first),
            "a second return buys nothing"
        );
        // Media lands: the next return counts again.
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the segment", |s| s.segments_listed == 1).await;
        run.resume();
        let again = lock(&run.state).returned_at.unwrap();
        assert!(again > first);
        assert!(again >= lock(&run.state).last_media_at.unwrap());
        run.cancel();
        settle(&run).await;
    }

    /// The acceptance case at the publisher: 30 s without a network
    /// (dated, not slept) and back is one feed, one recording, every
    /// segment that was sent in it, and one seam. While paused the
    /// camera's segments are withheld rather than uploaded, no playlist
    /// goes out and none is closed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dropped_connection_pauses_the_broadcast_and_the_return_continues_it() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 0);
        let push =
            |kind, byte: u8, at_ms: u64| run.enqueue(kind, vec![byte; 64], 2000, false, at_ms);

        let _ = push(SegmentKind::Init, 0xee, GO_LIVE_MS);
        for i in 0..3u8 {
            let _ = push(
                SegmentKind::Media,
                i,
                GO_LIVE_MS + 2000 * (u64::from(i) + 1),
            );
        }
        wait_until(&run, "the first three", |s| {
            s.segments_listed == 3 && s.feed_index > 0
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let before = run.snapshot();
        let uploads_before = lock(&log).seen.len();

        // The network goes. The camera keeps going for 30 s.
        run.pause();
        assert!(run.snapshot().paused);
        for i in 3..18u8 {
            assert_eq!(
                push(
                    SegmentKind::Media,
                    i,
                    GO_LIVE_MS + 2000 * (u64::from(i) + 1)
                ),
                PushOutcome::Withheld,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let paused = run.snapshot();
        assert_eq!(paused.segments_withheld, 15, "{paused:?}");
        assert!(paused.running && !paused.reaped, "{paused:?}");
        assert_eq!(
            (paused.feed_index, paused.playlists_published),
            (before.feed_index, before.playlists_published),
            "silence: the live playlist just stops advancing",
        );
        assert_eq!(
            lock(&log).seen.len(),
            uploads_before,
            "nothing goes up while paused"
        );

        // Back.
        run.resume();
        assert!(!run.snapshot().paused);
        // Segment 18, the first back, was pushed as it closed.
        let back_at = GO_LIVE_MS + 2000 * 18;
        for i in 18..21u8 {
            let _ = push(
                SegmentKind::Media,
                i,
                GO_LIVE_MS + 2000 * (u64::from(i) + 1),
            );
        }
        wait_until(&run, "the return", |s| s.segments_listed == 6).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.segments_withheld, 15, "{report:?}");
        assert_eq!(
            report.segments_failed + report.segments_dropped,
            0,
            "{report:?}"
        );
        assert!(!report.reaped, "{report:?}");
        assert!(!report.kept_up, "an outage is not keeping up: {report:?}");

        // One feed: every update at the next index of the one topic, and
        // no live playlist before the closing one carries ENDLIST.
        let seen = lock(&log).seen.clone();
        let ids = soc_ids(&log);
        assert_eq!(ids.len() as u64, report.feed_updates);
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(
                *id,
                hex::encode(ant_retrieval::sequence_update_id(&[0x11; 32], index as u64)),
            );
        }
        let playlists: Vec<String> = seen
            .iter()
            .filter(|r| is_playlist(r))
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .collect();
        let (live, finals) = playlists.split_at(playlists.len() - 2);
        assert!(finals[0].ends_with("#EXT-X-ENDLIST\n") && !finals[0].contains("VOD"));
        for body in live {
            assert!(!body.contains("#EXT-X-ENDLIST"), "{body}");
        }
        // One recording, every segment that was sent, one seam.
        let recording = playlists.last().unwrap();
        assert!(
            recording.contains("#EXT-X-PLAYLIST-TYPE:VOD"),
            "{recording}"
        );
        assert_eq!(
            recording.matches("#EXT-X-DISCONTINUITY\n").count(),
            1,
            "{recording}"
        );
        assert!(!recording.contains("#EXT-X-GAP"), "{recording}");
        let dated = dated_uris(recording);
        assert_eq!(dated.len(), 6, "{recording}");
        // Segment i is 64 bytes of i; the withheld 3-17 never went up.
        let segment_refs: Vec<String> = [0u8, 1, 2, 18, 19, 20]
            .iter()
            .map(|i| hex::encode(ant_retrieval::split_bytes_with_redundancy(&[*i; 64], 1).root))
            .collect();
        assert_eq!(seen.iter().filter(|r| is_segment(r)).count(), 7, "init + 6");
        assert_eq!(
            dated.iter().map(|(_, uri)| uri.clone()).collect::<Vec<_>>(),
            segment_refs,
            "{recording}"
        );
        // Dated afresh where it came back, the rest stepped by media.
        assert_eq!(dated[2].0, iso8601_utc_ms(GO_LIVE_MS + 4000));
        assert_eq!(dated[3].0, iso8601_utc_ms(back_at));
        assert_eq!(dated[5].0, iso8601_utc_ms(back_at + 4000));
        // Same broadcast: the recording is the same broadcast id as the
        // live window before the outage.
        let id_line = |body: &str| {
            body.lines()
                .find(|l| l.starts_with("#ANTSTREAM-BROADCAST:"))
                .map(str::to_string)
        };
        assert_eq!(id_line(recording), id_line(&live[0]));
    }

    /// An initialization segment never fails against the missing uplink:
    /// pushed while paused it waits for the return, and one that fails
    /// while paused is tried again then, so the writer that restarted
    /// during the outage is still playable after it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_init_segment_waits_out_the_outage() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(150)).await;
        let run = quiet_run(&log, &addr, 0);
        let _ = run.push(SegmentKind::Init, vec![0xe1; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the first segment", |s| s.segments_listed == 1).await;

        // A writer restart on the wire as the network goes: its upload
        // fails while paused.
        let held = |byte: u8| {
            lock(&log)
                .seen
                .iter()
                .filter(|r| is_segment(r) && r.body.first() == Some(&byte))
                .count()
        };
        lock(&log).fail_uploads = true;
        let _ = run.push(SegmentKind::Init, vec![0xe2; 64], 0, false);
        for _ in 0..100 {
            if held(0xe2) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        run.pause();
        // Another restart while paused: it is held, not sent.
        let _ = run.push(SegmentKind::Init, vec![0xe3; 64], 0, false);
        assert_eq!(
            run.push(SegmentKind::Media, vec![2u8; 64], 2000, true),
            PushOutcome::Withheld
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(held(0xe2), 1, "the in-flight init went out once and failed");
        assert_eq!(held(0xe3), 0, "an init pushed while paused waits");

        lock(&log).fail_uploads = false;
        run.resume();
        let _ = run.push(SegmentKind::Media, vec![3u8; 64], 2000, false);
        wait_until(&run, "the return", |s| s.segments_listed == 2).await;
        run.cancel();
        settle(&run).await;
        assert_eq!(held(0xe2), 2, "the failed init is retried on the return");
        assert_eq!(held(0xe3), 1);
        let recording = lock(&log)
            .seen
            .iter()
            .rev()
            .find(|r| is_recording(r))
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .expect("recording");
        assert_eq!(recording.matches("#EXTINF").count(), 2, "{recording}");
        assert_eq!(
            recording.matches("#EXT-X-DISCONTINUITY\n").count(),
            1,
            "{recording}"
        );
        // The segment after the outage is listed under the newest map.
        let e3 = lock(&log)
            .seen
            .iter()
            .find(|r| is_segment(r) && r.body.first() == Some(&0xe3))
            .map(bytes_reference)
            .unwrap();
        assert!(
            recording.contains(&format!("#EXT-X-MAP:URI=\"{e3}\"")),
            "{recording}"
        );
    }

    /// Nothing ends a broadcast but the stop call — unless the host asked
    /// for a reap window, and that long passes with no media: then it
    /// ends by itself exactly as a stop would, closing window and
    /// recording, and says so.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broadcast_with_no_media_for_the_reap_window_ends_itself() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 400);
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the segment", |s| s.segments_listed == 1).await;
        run.pause();
        settle(&run).await;
        let report = run.report();
        assert!(report.reaped, "{report:?}");
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        assert_eq!(
            run.push(SegmentKind::Media, vec![2u8; 64], 2000, false),
            PushOutcome::Closed
        );
        let playlists = lock(&log).seen.iter().filter(|r| is_playlist(r)).count();
        assert!(playlists >= 3, "live window, closing window, recording");
        assert!(lock(&log).seen.iter().any(is_recording));

        // Without a reap window the same silence goes on.
        let run = quiet_run(&log, &addr, 0);
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        run.pause();
        tokio::time::sleep(Duration::from_millis(800)).await;
        let snapshot = run.snapshot();
        assert!(snapshot.running && !snapshot.reaped, "{snapshot:?}");
        run.cancel();
        settle(&run).await;
        assert!(!run.report().reaped);
    }

    /// A return late in the window buys a whole window from the return,
    /// so the first segment back is not reaped seconds before it lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_return_late_in_the_window_gets_a_whole_window() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 600);
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the segment", |s| s.segments_listed == 1).await;
        run.pause();
        tokio::time::sleep(Duration::from_millis(400)).await;
        run.resume();
        // Past the first segment's window, inside the return's.
        tokio::time::sleep(Duration::from_millis(400)).await;
        let snapshot = run.snapshot();
        assert!(!snapshot.reaped, "{snapshot:?}");
        let _ = run.push(SegmentKind::Media, vec![2u8; 64], 2000, true);
        wait_until(&run, "the return", |s| s.segments_listed == 2).await;
        run.cancel();
        settle(&run).await;
        assert!(!run.report().reaped);
    }
}
