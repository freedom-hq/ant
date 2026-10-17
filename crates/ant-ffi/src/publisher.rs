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
//! default for hosts that don't ask, while the repository's apps and web
//! player use and read `"solar-punk"` only, issue #217):
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
//! uploader treats an encoder that disconnects the same way. A reap
//! that falls due while paused waits for the return, since the closing
//! window and the recording can't go up without an uplink: that return
//! ends the run instead of continuing it.
//!
//! The topic is either text, hashed the way Solar Punk catalog entries
//! and bee-js `Topic.fromString` name a feed ([`topic_from_string`]), or
//! raw bytes from the host; by default the text `antstream/<channel>`.
//! A `"solar-punk"` broadcast with a text topic plays in a Solar Punk
//! viewer given only the owner and that text.
//!
//! **A channel is one feed for good** (issue #173): every broadcast on a
//! topic continues the feed the earlier ones left. Before its first
//! update a broadcast reads the feed head once, writes after it, numbers
//! its media on from the head playlist's numbering behind one
//! `#EXT-X-DISCONTINUITY`, so the media sequence a viewer follows never
//! moves backwards from one broadcast into the next, and opens its
//! recording with the head playlist's entries, so the head is always the
//! recording of the whole channel. The Solar Punk uploader does the same
//! over a topic that outlives a session. See
//! [`PublisherConfig::start_index`].
//!
//! **A killed app resumes or finalizes its broadcast** (issue #175,
//! [`PublisherConfig::recovery_path`]): ahead of every feed update the
//! publisher saves a small recovery entry (the feed, the index that update
//! takes, the numbering and dating, and every entry listed so far), so a
//! relaunch can start with [`PublisherConfig::resume`] and carry the same
//! broadcast on in the same feed, or stop straight away to close it into
//! its recording. Solar Punk's uploader keeps the same per stream
//! (`RecoveryStore`).
//!
//! **A channel can be a one-rung ladder** (issue #174,
//! [`PublisherConfig::ladder`]), so a viewer joining hours in doesn't
//! walk thousands of feed updates from index 0: every ten seconds a
//! small **ladder time marker** at an address computed from the clock
//! names the feed's newest index (a viewer reads it, then one round of
//! slots from there), and under `"solar-punk"` a **master playlist**
//! with a measured `BANDWIDTH` names the channel's feed on the ladder's
//! own group feed, the way a Solar Punk catalog entry points at a ladder.
//! Both in Solar Punk's version-2 encoding, so their players join the
//! same way.
//!
//! **The postage batch is checked before going live and named when it
//! refuses** (issue #176). [`broadcast_cost`] estimates what a minute of
//! a broadcast stamps (its segments at the configured bitrate, the
//! playlist and feed update after each, the markers, the recording) and
//! [`estimate_capacity`] sets that against the room left in the batch's
//! fullest bucket, so a host can say "about N minutes left on this
//! batch" and warn about a nearly full or mutable one. During the
//! broadcast an upload the gateway refuses for the batch (a full bucket,
//! bee's `402`; an unusable batch) is counted apart from network
//! failures ([`PublisherSnapshot::batch_full_refusals`]), the way Solar
//! Punk's uploader reports a postage refusal in its health.
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
pub use crate::catalog::StreamInfo;
use crate::catalog::{CatalogEntry, CatalogState, CatalogWriter, EntryFacts, CATALOG_REFRESH};

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
    /// With neither set, the topic is the text `antstream/<channel>`
    /// (issue #173): the same for every broadcast of the channel, so a
    /// shared or QR-scanned channel link keeps working from one
    /// broadcast to the next. That text is reported as `topic_text`.
    /// Whichever names it, a broadcast continues the feed already on the
    /// topic; see [`PublisherConfig::start_index`].
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
    /// to [`StreamFormat::Bzz`] for hosts that don't ask; the `AntStream`
    /// apps send `"solar-punk"`, the only format their Watch tabs and the
    /// web player read (issue #217).
    #[serde(default)]
    pub format: StreamFormat,
    /// Override for the sequence-feed index of this broadcast's first
    /// update. Leave it out: a broadcast then **continues the feed** the
    /// topic already holds (issue #173). Before its first update it
    /// reads the feed head once (`GET /feeds/{owner}/{topic}?exact=true`,
    /// which ant's node answers only once the slot after the head is
    /// confirmed empty, so a head slow to answer is never read as the
    /// one before it) and
    ///
    /// - writes from the index after the head, so the previous
    ///   broadcast's playlists are never overwritten;
    /// - numbers its media from where the head playlist's numbering ends
    ///   (its `#EXT-X-MEDIA-SEQUENCE` plus the entries it lists, gaps
    ///   included), with an `#EXT-X-DISCONTINUITY` on its first segment,
    ///   so a viewer following the channel sees the media sequence only
    ///   ever increase from one broadcast into the next (hls.js treats a
    ///   sequence that moves backwards as a fatal parsing error);
    /// - opens its recording with the head playlist's entries, so the
    ///   recording it leaves at the head plays every broadcast the
    ///   channel has carried (see [`PublisherState::render_recording`]).
    ///
    /// The shape Solar Punk's uploader writes over a topic that outlives
    /// a session. Only a `404 "no update found"` (a feed nothing was ever
    /// written to; ant's node answers it only for an anchor miss more
    /// than a couple of peers agree on, and a 503 for a cold node's one
    /// or two) starts the feed at 0. Any other failed read, a v1 head's
    /// playlist that does not retrieve included, is no
    /// answer: segments keep uploading, but no playlist or feed update
    /// is written until a retry reads the head, so a flaky read can never
    /// overwrite index 0 and up.
    ///
    /// Confirming a miss (the empty feed's index 0, or the slot after a
    /// head) takes more than two peers' "not found", so on a node with at
    /// most two peers every head read is a 503 and the broadcast, a
    /// brand-new channel's first one included, publishes no playlist or
    /// feed update until the node reaches three or more. The progress
    /// `errors` say so; with `reap_ms` set the run is reaped once that
    /// long passes, as for any broadcast that lists no media.
    ///
    /// Set, the first update goes exactly here and nothing is read: the
    /// numbering starts at 0 with no seam and the recording holds this
    /// broadcast alone, as before #173. A host passing the previous
    /// broadcast's `feed_index` here must get it right, or it rewrites
    /// the indices that hold earlier playlists. A report's `feed_index`
    /// is `null` when its run never read the head; passed on as is, it
    /// leaves this unset and the next broadcast reads the head.
    #[serde(default)]
    pub start_index: Option<u64>,
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
    /// not enforce it (the capture pipeline does). [`broadcast_cost`]
    /// sizes segments from it.
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
    /// Never while paused: a window that runs out during an outage ends
    /// the run at the [`LiveRun::resume`] instead (that return buys no
    /// fresh window), so the closing window and the recording go up over
    /// a live uplink rather than failing once against a dead one. An
    /// outage that never ends leaves the run paused until the stop.
    ///
    /// 0, the default, never reaps: a host only opts in once it handles
    /// a broadcast that ended without its stop call (the iOS app does;
    /// the Android app does not yet).
    #[serde(default)]
    pub reap_ms: u64,
    /// Publish the channel as a one-rung ladder too (issue #174): a
    /// **ladder time marker** every ten seconds, and under
    /// `"solar-punk"` a **master playlist**, so a viewer joining hours
    /// into a broadcast reaches the live playlist in two round trips
    /// instead of walking the feed from index 0.
    ///
    /// - The ladder's group is the text [`ladder_group_text`] of the
    ///   topic, `antstream-ladder-<topic hex>`, its master feed topic
    ///   [`topic_from_string`] of that. A viewer holding only the
    ///   channel (owner and topic) computes both; a catalog entry points
    ///   at the ladder by the group text, the way it points at a Solar
    ///   Punk ladder.
    /// - Markers: one single-owner chunk per 10 s period of global time,
    ///   at [`ladder_marker_identifier`], naming the newest feed index
    ///   this broadcast has published and [`PublisherConfig::segment_ms`]
    ///   ([`encode_ladder_marker`], version 2, Solar Punk's
    ///   `ladderMarker.ts`). In either format, from the first landed
    ///   feed update until the stop, none while paused. 360 chunks an
    ///   hour. A marker that fails is skipped: a viewer that finds none
    ///   searches the feed as before. Marker and master failures go to
    ///   `ladder_failures`/`ladder_error`, not `errors`.
    /// - Master (`"solar-punk"` with a text topic that fits a URI only:
    ///   the rung's URI is `swarm://<owner>/<topic_text>`, and readers
    ///   hash the text in it, so a raw topic can't be named there, nor a
    ///   text with a `/` in it such as the default `antstream/<channel>`
    ///   ([`fits_swarm_uri`]); a `"bzz"` feed is not one a Solar Punk
    ///   player reads): one `#EXT-X-STREAM-INF` whose
    ///   `BANDWIDTH` is measured from the segments listed so far
    ///   ([`render_master_playlist`]), written to the group's feed once
    ///   three segments are measured and again whenever the measured peak
    ///   outgrows the one it advertises by a quarter. Continues the
    ///   group feed's head like the channel's own feed.
    ///
    /// Off by default: it costs chunks on the batch, and only viewers
    /// that read markers gain from it.
    #[serde(default)]
    pub ladder: bool,
    /// Rendition size, for the master playlist's `RESOLUTION`. Left out
    /// of it unless both are set.
    #[serde(default)]
    pub width: u32,
    #[serde(default)]
    pub height: u32,
    /// Where to keep this broadcast's **recovery entry** (issue #175), a
    /// small JSON file the host names (the iOS app keeps it in
    /// `Application Support/antstream/`). Empty, the default, keeps none.
    ///
    /// Written ahead of every feed update, once the playlist it points at
    /// has uploaded and before the update itself goes out
    /// ([`RecoveryEntry`]): the topic and owner, the index that update
    /// takes, the media and discontinuity sequence, the dating clock, and
    /// every entry the broadcast has listed so far, so whatever a viewer
    /// could have seen is on disk before the feed can name it. Replaced
    /// atomically (a temporary file, then a rename), so a kill mid-write
    /// leaves the previous entry. Removed once the recording is the feed's
    /// head and, with [`PublisherConfig::info`], the closing `vod` catalog
    /// entry has landed; left in place when the recording, its update or
    /// that catalog entry fails, so the broadcast can still be finalized
    /// (and its catalog closed); kept for that catalog entry alone, it is
    /// marked `"recorded": true` (see [`RecoveryEntry`]). A fresh broadcast with the same
    /// path replaces an entry left there at its first feed update, so a
    /// host offers [`PublisherConfig::resume`] first.
    ///
    /// The whole list is rewritten every time, as Solar Punk's
    /// `RecoveryStore` does: about 250 bytes an entry, so some 450 KB
    /// after an hour at 2 s segments.
    #[serde(default)]
    pub recovery_path: String,
    /// Continue the broadcast whose recovery entry is at
    /// [`PublisherConfig::recovery_path`] instead of starting a new one:
    /// the app was killed or crashed mid-broadcast (issue #175). The rest
    /// of the config is this run's as usual (gateway, batch, rendition),
    /// but its topic and format must name the entry's feed, and the node's
    /// owner must be the entry's: anything else is refused at start, as is
    /// a missing or unreadable entry (which is left where it is).
    ///
    /// The broadcast carries on as one: the same broadcast id, its
    /// numbering, `#EXT-X-DISCONTINUITY-SEQUENCE` and `#EXT-X-TARGETDURATION`
    /// where the entry left them, its first new segment behind one
    /// `#EXT-X-DISCONTINUITY` and dated from the wall clock, never before
    /// the last saved segment ended, and a recording that holds every
    /// entry the entry saved plus whatever this run lists. The entry's
    /// own head playlist (Solar Punk's recovered sessions do the same)
    /// is never read as a predecessor: that would glue the broadcast in
    /// front of itself.
    ///
    /// The feed head is still read once (retried until it answers) to
    /// settle the one thing the entry can't know: whether the update it
    /// was written ahead of landed. The run writes after the head if the
    /// head is that update or the one before it; any other head means the
    /// channel moved on since (another broadcast wrote to it), and the run
    /// is refused there and ends with nothing written, the error in its
    /// `errors`.
    ///
    /// To finalize instead of continuing (the entry is older than the reap
    /// window, or the host chose to end it), start with `resume` and stop
    /// straight away: the closing window and the recording go up as on any
    /// stop. Unlike a new broadcast's, a stopping resumed run keeps
    /// retrying a head read that fails (a cold peer's `503`, a `502`) or
    /// does not answer for up to a minute after the stop (a read still in
    /// flight then is dropped) rather than giving up at the first one.
    ///
    /// An entry a stop had already begun closing (written ahead of the
    /// closing window or the recording: its `shape` is past `"live"`) is
    /// only ever finalized: a live viewer may already have stopped at its
    /// `#EXT-X-ENDLIST`, so the run starts stopped, refuses every segment
    /// (`Closed`), and ends with the closing window and the recording. If
    /// the recording is already the head, the run writes no playlist or
    /// feed update: only the closing `vod` catalog entry when it has info
    /// (a stop before landed the recording but not that entry) and the
    /// catalog's head isn't that entry already, then it removes the entry.
    #[serde(default)]
    pub resume: bool,
    /// The broadcaster's **stream info** (issue #220): title (empty is
    /// the channel's name), description, thumbnail reference and
    /// scheduled start; see [`StreamInfo`]. Set, the run keeps the
    /// account's **catalog feed** (topic text
    /// [`crate::catalog::CATALOG_TOPIC_TEXT`], owner the account) holding
    /// one Solar Punk catalog entry for the channel
    /// ([`crate::catalog::CatalogEntry`]), so their player lists the
    /// channel and ours shows the info:
    ///
    /// - `state: "live"` once the run's first feed update landed (before
    ///   that the feed's head is still the last broadcast's);
    /// - again when the info changes ([`LiveRun::set_info`]), at once;
    /// - again while live when something else in it changed (`index`,
    ///   `duration`, the measured bandwidth), at most once every
    ///   [`crate::catalog::CATALOG_REFRESH`] (a minute), never while
    ///   paused;
    /// - at stop, `state: "vod"` with the index and duration of the
    ///   recording (or of the newest update that landed, if the recording
    ///   did not), after the recording.
    ///
    /// Each write reads the catalog feed's head once (`?exact=true`) and
    /// writes after it, like the channel's feed. Under `"solar-punk"` with
    /// a text topic that fits a URI only (the entry names the channel by
    /// its topic text, [`fits_swarm_uri`]); otherwise nothing is written
    /// and `catalog_error` says why. Off the broadcast's critical path:
    /// its own task, never awaited by a playlist or feed update, failures
    /// in `catalog_failures`/`catalog_error`, never in `errors`. The stop
    /// waits for the closing write (one more upload and update) like it
    /// waits for the recording.
    ///
    /// Left out, the run writes no catalog until [`LiveRun::set_info`]
    /// gives it info; a resumed run left without it takes the info its
    /// recovery entry saved (and, when `width`/`height` are left out,
    /// the rendition size), so ending a killed broadcast still closes its
    /// `live` entry with the `vod` one.
    #[serde(default)]
    pub info: Option<StreamInfo>,
}

/// The reap window Solar Punk's uploader runs with (`ORPHAN_REAP_MS`, 60
/// s), and the one the iOS app passes as [`PublisherConfig::reap_ms`].
pub const ORPHAN_REAP_MS: u64 = 60_000;

/// The shape a broadcast takes on Swarm. The two are not
/// interchangeable for a viewer: each reads only its own.
///
/// `"bzz"` stays the default so a host that doesn't ask keeps the shape
/// it always got. The `AntStream` apps, their Watch tabs and the web
/// player their Share links open use `"solar-punk"` only (issue #217),
/// the shape the Solar Punk viewers read too (issue #167); none of them
/// plays a `"bzz"` channel any more.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
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
        if self.resume && self.recovery_path.is_empty() {
            return Err(PublisherError::Config(
                "resume needs recovery_path: the entry of the broadcast to continue".into(),
            ));
        }
        if let Some(info) = &self.info {
            info.validate().map_err(PublisherError::Config)?;
        }
        if self.resume && self.start_index.is_some() {
            return Err(PublisherError::Config(
                "set resume or start_index, not both: a resumed broadcast continues its own feed \
                 index"
                    .into(),
            ));
        }
        Ok(())
    }

    /// The feed topic this broadcast writes to, and its text form when
    /// it has one: the configured raw topic, the configured text topic,
    /// or the channel's stable text topic, `antstream/<channel>`.
    fn resolve_topic(&self) -> Result<([u8; 32], String), PublisherError> {
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
            format!("antstream/{}", self.channel)
        } else {
            self.topic_text.clone()
        };
        Ok((topic_from_string(&text), text))
    }
}

/// Why a run with `config` on the topic `topic_text` can write no catalog
/// entry, or `None` when it can: see [`PublisherConfig::info`].
fn catalog_refusal(config: &PublisherConfig, topic_text: &str) -> Option<&'static str> {
    if config.format != StreamFormat::SolarPunk {
        Some("no catalog entry: a \"bzz\" channel is not one a catalog reader plays")
    } else if !fits_swarm_uri(topic_text) {
        Some(
            "no catalog entry: the topic has no text a catalog entry can name \
             (a raw topic, or a text with a `/`)",
        )
    } else {
        None
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
/// [`marker_period_at`].
///
/// A marker names the newest index of every rung of an ABR ladder. With
/// [`PublisherConfig::ladder`] this publisher writes one for its single
/// rung every period, under the group [`ladder_group_text`].
#[must_use]
pub fn ladder_marker_identifier(group: &[u8; 32], period: u64) -> [u8; 32] {
    const PREFIX: &[u8] = b"ladder-marker";
    let mut input = Vec::with_capacity(PREFIX.len() + 32 + 8);
    input.extend_from_slice(PREFIX);
    input.extend_from_slice(group);
    input.extend_from_slice(&period.to_be_bytes());
    ant_crypto::keccak256(&input)
}

/// Marker period length in ms.
const LADDER_MARKER_PERIOD_MS: u64 = LADDER_MARKER_PERIOD_SECONDS * 1000;

/// The marker format written: `LADDER_MARKER_VERSION` in
/// `ladderMarker.ts`. Readers treat any other version as absent.
pub const LADDER_MARKER_VERSION: u32 = 2;

/// The period a wall-clock instant (Unix ms) falls in:
/// `floor(unix_ms / 10 000)`, `markerPeriodAt`. Global time, so a reader
/// needs nothing but its clock.
#[must_use]
pub const fn marker_period_at(unix_ms: u64) -> u64 {
    unix_ms / LADDER_MARKER_PERIOD_MS
}

/// The group id of a channel's one-rung ladder (issue #174):
/// `antstream-ladder-<topic as 64 lowercase hex>`. Text, because readers
/// name a ladder by a string they hash ([`topic_from_string`] gives the
/// master feed topic, and the marker identifier is keyed by that), and
/// derived from the topic *bytes*, so a viewer that has only the
/// channel's owner and topic (all a channel reference's feed manifest
/// carries) computes it too. No `/`: a Solar Punk player names a feed
/// as `swarm://<owner>/<topic text>` and keeps only what comes before
/// the next `/` of it (see [`fits_swarm_uri`]).
#[must_use]
pub fn ladder_group_text(topic: &[u8; 32]) -> String {
    format!("antstream-ladder-{}", hex::encode(topic))
}

/// Whether `text` reads back whole from a `swarm://<owner>/<text>` URI
/// in a playlist: Solar Punk's `parseSwarmUri` splits the URI on `/`
/// and keeps the second piece, so a `/` would cut it short, and a
/// playlist line can't carry whitespace, `#`, `?` or `%` unescaped
/// without a player reading something else. The default topic text
/// `antstream/<channel>` doesn't fit, so its channel gets markers but
/// no master.
#[must_use]
pub fn fits_swarm_uri(text: &str) -> bool {
    !text.is_empty()
        && !text
            .chars()
            .any(|c| matches!(c, '/' | '?' | '#' | '%') || c.is_whitespace() || c.is_control())
}

/// One marker's chunk payload: the JSON Solar Punk's `encodeLadderMarker`
/// writes for a single rung, byte for byte (`JSON.stringify`'s key order,
/// no whitespace):
/// `{"v":2,"period":P,"writtenAt":W,"rungs":{"<rung topic hex>":I},"segmentMs":S}`.
///
/// Refused, like there, when `parseLadderMarker` would reject it (a write
/// time outside its own period, a zero segment length, a number past
/// JavaScript's safe integers) or it would not fit one chunk.
pub fn encode_ladder_marker(
    period: u64,
    written_at_ms: u64,
    rung: &[u8; 32],
    index: u64,
    segment_ms: u32,
) -> Result<Vec<u8>, String> {
    /// `Number.MAX_SAFE_INTEGER`: `isWholeNumber` refuses anything past it.
    const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
    if marker_period_at(written_at_ms) != period {
        return Err(format!(
            "written at {written_at_ms}, outside its period {period}"
        ));
    }
    if segment_ms == 0 {
        return Err("a marker must name a segment length".into());
    }
    if written_at_ms > MAX_SAFE_INTEGER || index > MAX_SAFE_INTEGER {
        return Err("past JavaScript's safe integers".into());
    }
    let text = format!(
        "{{\"v\":{LADDER_MARKER_VERSION},\"period\":{period},\"writtenAt\":{written_at_ms},\
         \"rungs\":{{\"{}\":{index}}},\"segmentMs\":{segment_ms}}}",
        hex::encode(rung),
    );
    if text.len() > CHUNK_SIZE as usize {
        return Err(format!("{} bytes do not fit one chunk", text.len()));
    }
    Ok(text.into_bytes())
}

/// A one-rung master playlist, the text Solar Punk's
/// `buildMasterPlaylist` writes for a ladder of one: no `CODECS` (the
/// player takes them from the first fragment), the rung named by
/// `swarm://<owner hex>/<rung topic text>`, which a reader resolves by
/// hashing the text ([`topic_from_string`]). `RESOLUTION` only when
/// known (`width`/`height` both non-zero).
#[must_use]
pub fn render_master_playlist(
    owner: &[u8; 20],
    rung_text: &str,
    bandwidth: u64,
    average_bandwidth: u64,
    width: u32,
    height: u32,
) -> String {
    let resolution = if width > 0 && height > 0 {
        format!(",RESOLUTION={width}x{height}")
    } else {
        String::new()
    };
    let attributes =
        format!("BANDWIDTH={bandwidth},AVERAGE-BANDWIDTH={average_bandwidth}{resolution}");
    format!(
        "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n\
         #EXT-X-STREAM-INF:{attributes}\nswarm://{}/{rung_text}\n",
        hex::encode(owner),
    )
}

/// How many consecutive listed segments a peak bitrate is measured
/// across, `PEAK_WINDOW_SEGMENTS` in Solar Punk's `BitrateMeter.ts`: a
/// segment cut short on a keyframe can't dominate a window of three,
/// which still reports the rate a burst costs.
const PEAK_WINDOW_SEGMENTS: usize = 3;

/// The rendition's real bitrate, measured from the segments listed, for
/// the master's `BANDWIDTH` and `AVERAGE-BANDWIDTH` rather than the
/// encoder's target.
#[derive(Debug, Default)]
struct BitrateMeter {
    total_bytes: u64,
    total_ms: u64,
    window: VecDeque<(u64, u32)>,
    /// Highest bits/s over any full window. 0 until one filled.
    peak_bps: f64,
}

impl BitrateMeter {
    fn record(&mut self, bytes: u64, duration_ms: u32) {
        if bytes == 0 || duration_ms == 0 {
            return;
        }
        self.total_bytes += bytes;
        self.total_ms += u64::from(duration_ms);
        self.window.push_back((bytes, duration_ms));
        if self.window.len() > PEAK_WINDOW_SEGMENTS {
            self.window.pop_front();
        }
        if self.window.len() == PEAK_WINDOW_SEGMENTS {
            let (bytes, ms) = self.window.iter().fold((0u64, 0u64), |(b, m), &(sb, sm)| {
                (b + sb, m + u64::from(sm))
            });
            self.peak_bps = self.peak_bps.max(bytes as f64 * 8000.0 / ms as f64);
        }
    }

    /// `(BANDWIDTH, AVERAGE-BANDWIDTH)` in bits/s, `None` until a full
    /// window was measured. The average never exceeds the peak (RFC 8216).
    fn measured(&self) -> Option<(u64, u64)> {
        if self.peak_bps <= 0.0 {
            return None;
        }
        let peak = self.peak_bps.round() as u64;
        let average = (self.total_bytes as f64 * 8000.0 / self.total_ms as f64).round() as u64;
        Some((peak, average.min(peak)))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PublisherError {
    #[error("{0}")]
    Config(String),
}

pub(crate) fn parse_hex32(raw: &str) -> Result<[u8; 32], String> {
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
/// left. Saved as it is in the recovery entry ([`RecoveryEntry`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
        /// Segment size, for the master's measured bitrate.
        bytes: u64,
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
pub(crate) fn iso8601_utc_ms(unix_ms: u64) -> String {
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

/// One stage's latencies: the newest [`LATENCY_SAMPLES`] for the
/// percentiles, plus a whole-run count and maximum.
#[derive(Default)]
struct Samples {
    recent: VecDeque<u64>,
    count: u64,
    max: u64,
}

impl Samples {
    fn record(&mut self, value: u64) {
        PublisherState::record_latency(&mut self.recent, value);
        self.count += 1;
        self.max = self.max.max(value);
    }

    fn timing(&self) -> StageTiming {
        let mut sorted: Vec<u64> = self.recent.iter().copied().collect();
        sorted.sort_unstable();
        StageTiming {
            count: self.count,
            p50_ms: percentile(&sorted, 50),
            p95_ms: percentile(&sorted, 95),
            max_ms: self.max,
        }
    }
}

/// Where a broadcast's time goes, stage by stage (issue #212).
#[derive(Default)]
struct StageSamples {
    head_of_line: Samples,
    playlist: Samples,
    recovery: Samples,
    feed: Samples,
    update: Samples,
    marker: Samples,
}

impl StageSamples {
    fn timings(&self, segment: &VecDeque<u64>) -> PublisherTimings {
        let mut sorted: Vec<u64> = segment.iter().copied().collect();
        sorted.sort_unstable();
        PublisherTimings {
            segment: StageTiming {
                count: sorted.len() as u64,
                p50_ms: percentile(&sorted, 50),
                p95_ms: percentile(&sorted, 95),
                max_ms: sorted.last().copied().unwrap_or(0),
            },
            head_of_line: self.head_of_line.timing(),
            playlist: self.playlist.timing(),
            recovery: self.recovery.timing(),
            feed: self.feed.timing(),
            update: self.update.timing(),
            marker: self.marker.timing(),
        }
    }
}

/// One stage's latency figures, milliseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct StageTiming {
    /// How many times the stage ran (a segment's `count` is capped at
    /// the newest 2048 samples).
    pub count: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub max_ms: u64,
}

/// Per-stage timings of the live loop (issue #212): what a segment
/// waits on between capture and the feed update that makes it playable.
/// Cheap: one sample per stage run, folded only when a snapshot or the
/// report is read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct PublisherTimings {
    /// One media segment's upload (`publish_ms_p50`/`_p95` above).
    pub segment: StageTiming,
    /// A landed media segment waiting for the committer to fold it into
    /// a playlist: the segments before it still uploading (head of
    /// line), or the committer still busy with the previous update.
    pub head_of_line: StageTiming,
    /// The playlist's upload.
    pub playlist: StageTiming,
    /// Writing the recovery entry ([`PublisherConfig::recovery_path`]).
    pub recovery: StageTiming,
    /// The feed update's `POST /soc`.
    pub feed: StageTiming,
    /// One whole feed update: playlist, recovery entry and `POST /soc`,
    /// the committer's critical path.
    pub update: StageTiming,
    /// One ladder marker write (off the critical path).
    pub marker: StageTiming,
}

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
    /// Media sequence number for the next listed segment. Starts where
    /// the predecessor's numbering ended (issue #173), else at 0.
    next_number: u64,
    /// Where this broadcast stands in its feed is known: the index its
    /// first update takes and the numbering it continues (issue #173).
    /// Until then the committer folds nothing in, so no playlist is
    /// rendered with a guessed `#EXT-X-MEDIA-SEQUENCE` and no update is
    /// written at a guessed index. Set at start when the host gave a
    /// [`PublisherConfig::start_index`], else by [`settle_feed_position`].
    feed_settled: bool,
    /// The playlist at the feed's head when this broadcast started,
    /// continued from: its first listed segment carries the seam's
    /// `#EXT-X-DISCONTINUITY`, and the recording opens with its entries.
    predecessor: Option<Predecessor>,
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
    /// The stop call returned before the run finished and reported
    /// `feed_index` as final, where the next broadcast on the channel
    /// starts (it reads the head there, or a host passes it as
    /// `start_index`): no further update may be written, or a closing
    /// window or recording landing late would take the index the next
    /// broadcast starts at. See [`LiveRun::seal_feed`].
    feed_sealed: bool,
    /// Resumed from a recovery entry written ahead of the update at this
    /// index (issue #175): [`resume_feed_position`] reads the head to
    /// settle where the run writes instead of [`settle_feed_position`].
    resume_index: Option<u64>,
    /// The shape of the update the resumed entry was written ahead of:
    /// past [`PlaylistShape::Live`] the run only finalizes, and a
    /// recording that landed means the broadcast is already finished.
    resume_shape: PlaylistShape,
    /// The newest failure to write or remove the recovery entry. Kept out
    /// of `errors`: it costs a viewer nothing, and a full disk would
    /// otherwise fill the eight kept errors every segment.
    recovery_error: Option<String>,
    channel_reference: Option<String>,
    /// A `POST /feeds` is already in flight. The start-time attempt and
    /// the committer's retry would otherwise both fire while the first
    /// one is still on the wire — harmless (the manifest is
    /// content-addressed) but a wasted upload on the live path.
    channel_manifest_in_flight: bool,
    playlist_reference: Option<String>,
    /// Reference of the uploaded recording, set once it landed at stop.
    recording_reference: Option<String>,
    /// The recording is the feed's head: its update landed, or a resumed
    /// run found it there ([`Resumed::Finished`]). The broadcast is over
    /// on the feed; its recovery entry goes once the catalog says so too
    /// ([`drive`]).
    recording_landed: bool,

    publish_ms: VecDeque<u64>,
    /// When each landed media segment's upload finished, until the
    /// committer folds it in: its head-of-line wait.
    landed_at: BTreeMap<u64, Instant>,
    stages: StageSamples,
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
    /// The reap window ran out while paused: the reaper is holding the
    /// end for the return, so the closing window and the recording go up
    /// over a live uplink. The return then ends the run instead of
    /// buying a fresh window.
    reap_due: bool,
    /// The run ended itself: [`PublisherConfig::reap_ms`] passed with no
    /// media.
    reaped: bool,

    /// The listed segments' measured bitrate, for the master playlist.
    bitrate: BitrateMeter,
    /// `BANDWIDTH` of the last master that landed this run.
    master_advertised_bps: Option<u64>,
    /// The group feed's next index, once its head was read.
    master_next_index: Option<u64>,
    /// No master write is started before this: the last one failed.
    master_retry_at: Option<Instant>,
    /// Reference of the last master playlist that landed.
    master_reference: Option<String>,
    /// Ladder markers that landed.
    markers_written: u64,
    /// Marker and master writes that failed, and the newest failure.
    /// Kept apart from `errors`: a ladder write is a shortcut a viewer
    /// can do without, retried every period or [`MASTER_RETRY`], and
    /// would otherwise fill the eight kept errors and become the
    /// broadcast's headline error over the segment and feed failures
    /// that actually cost viewers media.
    ladder_failures: u64,
    ladder_error: Option<String>,

    /// The broadcast's own uploads (segments, playlists, feed updates,
    /// the channel manifest, the recording) the gateway refused because
    /// the postage batch is full ([`BatchRefusal::Full`]) or unusable
    /// ([`BatchRefusal::Unusable`]), counted apart from network failures
    /// (issue #176): no retry and no better uplink brings those back, so
    /// a host names them as the batch's. Each is in `errors` too, and a
    /// refused segment still counts in `segments_failed`.
    batch_full_refusals: u64,
    batch_unusable_refusals: u64,
    /// The newest of those refusals.
    batch_error: Option<String>,

    /// The stream info the catalog entry is built from
    /// ([`PublisherConfig::info`], [`LiveRun::set_info`]).
    info: Option<StreamInfo>,
    /// [`LiveRun::set_info`] changed the info since the last catalog
    /// write began: written at once rather than at the next refresh.
    catalog_info_changed: bool,
    /// A catalog update is on the wire; see [`LiveRun::seal_feed`].
    catalog_update_in_flight: bool,
    /// The newest entry that landed, compared against apart from its
    /// timestamp before spending a slot.
    catalog_last: Option<CatalogEntry>,
    /// The catalog feed index it took.
    catalog_index: Option<u64>,
    catalog_writes: u64,
    catalog_failures: u64,
    catalog_error: Option<String>,

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

    /// One of the broadcast's own uploads failed: in `errors`, and
    /// counted as the batch's when the gateway refused it for the batch.
    fn record_upload_error(&mut self, error: &UploadError, message: String) {
        match error.batch {
            Some(BatchRefusal::Full) => self.batch_full_refusals += 1,
            Some(BatchRefusal::Unusable) => self.batch_unusable_refusals += 1,
            None => {}
        }
        if error.batch.is_some() {
            self.batch_error = Some(message.clone());
        }
        self.record_error(message);
    }

    /// A catalog write failed or was skipped: counted in
    /// `catalog_failures` and shown as `catalog_error`, never in
    /// `errors`/`last_error`.
    fn record_catalog_failure(&mut self, message: String) {
        self.catalog_failures += 1;
        self.catalog_error = Some(message);
    }

    /// A marker or master write failed: counted in `ladder_failures`
    /// and shown as `ladder_error`, never in `errors`/`last_error`.
    fn record_ladder_failure(&mut self, message: String) {
        self.ladder_failures += 1;
        self.ladder_error = Some(message);
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
            if let Some(landed) = self.landed_at.remove(&self.next_commit) {
                self.stages.head_of_line.record(ms(landed.elapsed()));
            }
            self.next_commit += 1;
            match outcome {
                Committed::Init { uri } => {
                    // A fresh initialization segment means a fresh
                    // encoder timeline: whatever follows is discontinuous
                    // with what came before.
                    self.init_uri = Some(uri);
                    if self.listed_any() || !self.pending_gaps.is_empty() {
                        self.break_since_last_entry = true;
                    }
                }
                Committed::Withheld => {
                    // The broadcast went silent here: whatever is listed
                    // next is a new run of the timeline. Only a break
                    // once something was listed, as for a fresh init.
                    if self.listed_any() || !self.pending_gaps.is_empty() {
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
                    bytes,
                } => {
                    if !self.listed_any() && self.predecessor.is_some() && self.init_uri.is_some() {
                        // Gaps in front of the first segment are never
                        // listed (no playlist starts on one), so their
                        // numbers are nobody's. Continuing a predecessor,
                        // give them back: the recording glues this
                        // broadcast's entries straight after the
                        // predecessor's, numbered on from its media
                        // sequence, so a hole here would shift every
                        // segment's number between the live window and
                        // the recording.
                        if let Some(first) = self.pending_gaps.first() {
                            self.next_number = first.number;
                        }
                        self.pending_gaps.clear();
                    }
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
                    self.bitrate.record(bytes, duration_ms);
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
                    // Media moved the deadline on, so a reap held for the
                    // return is no longer due: an upload that was still
                    // in flight when the window ran out landed after
                    // all. Left set, it would deny every later return
                    // its fresh window (see `LiveRun::resume`). The
                    // committer wakes the parked reaper when this clears
                    // the flag, so if the new window runs out paused
                    // too, the reaper marks it due again.
                    self.reap_due = false;
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
        let seam = !self.listed_any() && self.predecessor.is_some() && uri.is_some();
        Some(MediaEntry {
            number,
            gap: uri.is_none(),
            uri: uri.unwrap_or_else(|| format!("gap-{number}")),
            duration_ms,
            // The broadcast's first listed segment starts the timeline;
            // there is nothing before it to be discontinuous with, unless
            // it continues an earlier broadcast's numbering: that seam
            // is a break, said once, on the first segment (not on a gap
            // in front of it, which no playlist starts on).
            discontinuity: if self.listed_any() {
                discontinuous
            } else {
                seam
            },
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
            &[],
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
    ///
    /// A broadcast that continued an earlier one on the channel (issue
    /// #173) opens its recording with that playlist's entries, verbatim,
    /// then the seam's one `#EXT-X-DISCONTINUITY` (carried by this
    /// broadcast's first entry) and its own: the recording takes the
    /// predecessor's `#EXT-X-MEDIA-SEQUENCE` and
    /// `#EXT-X-DISCONTINUITY-SEQUENCE`, and the longer of the two target
    /// durations. The predecessor is itself the recording of every
    /// broadcast before it, so the head recording plays the whole
    /// channel. Solar Punk's `buildVODManifest` glues the same way.
    ///
    /// That glued history survives only while every stop's recording
    /// lands. The recording is published once, with no retry. If its
    /// upload or feed update fails (a 5xx, or `PUBLISH_TIMEOUT` on a
    /// long channel's growing recording), the head stays at the
    /// closing window. The next broadcast then continues from that
    /// window alone, so the head recording drops every earlier
    /// broadcast's entries that fell outside it. The older recordings
    /// are still at earlier feed indices, but nothing points a viewer
    /// at them.
    fn render_recording(&self, target_ms: u32) -> String {
        let own_sequence = self
            .recording
            .first()
            .map_or(self.next_number, |e| e.number);
        let target_s = self.target_duration_s(target_ms);
        match &self.predecessor {
            Some(prior) => render_entries(
                &self.broadcast_id,
                target_s.max(prior.target_duration_s),
                prior.media_sequence,
                prior.discontinuity_sequence,
                &prior.lines,
                self.recording.iter(),
                PlaylistShape::Recording,
            ),
            None => render_entries(
                &self.broadcast_id,
                target_s,
                own_sequence,
                0,
                &[],
                self.recording.iter(),
                PlaylistShape::Recording,
            ),
        }
    }

    /// Continue `prior`, the playlist at the feed's head (issue #173):
    /// number from where it ended, and count the discontinuities in
    /// front of this broadcast's first entry (those its own header said
    /// had slid out of its window, plus the ones it lists) as the live
    /// window's `#EXT-X-DISCONTINUITY-SEQUENCE`.
    fn continue_from(&mut self, prior: Predecessor) {
        self.next_number = prior.next_number;
        self.discontinuity_sequence = prior.discontinuity_sequence
            + prior
                .lines
                .iter()
                .filter(|l| l.as_str() == "#EXT-X-DISCONTINUITY")
                .count() as u64;
        self.predecessor = Some(prior);
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

/// Which playlist [`render_entries`] writes. A [`RecoveryEntry`] keeps
/// the shape of the update it was written ahead of (`"live"`,
/// `"closing"`, `"recording"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PlaylistShape {
    /// The rolling live window.
    #[default]
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
    prefix: &[String],
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
    // An earlier broadcast's entries, verbatim (the recording of a
    // broadcast that continued one, see `render_recording`). The entry
    // after them restates its `#EXT-X-MAP`, since `current_map` starts
    // empty.
    for line in prefix {
        out.push_str(line);
        out.push('\n');
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
// Continuing the channel (issue #173)
// ---------------------------------------------------------------------------

/// The playlist at the head of the channel's feed when a broadcast
/// started: the previous broadcast's recording, closing window, or (if it
/// was killed) its last live window. Read off the head by
/// [`settle_feed_position`] and parsed by [`Predecessor::parse`], which
/// reads only what Solar Punk's uploader reads (`continuesFrom` and
/// `inheritedTimeline` in its `ManifestManager.ts`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Predecessor {
    /// Where its numbering ended: its `#EXT-X-MEDIA-SEQUENCE` plus the
    /// entries it lists (one `#EXTINF` each, gap entries included). A
    /// playlist numbered from 12 that lists 30 entries makes this 42.
    next_number: u64,
    /// Its `#EXT-X-MEDIA-SEQUENCE`, which the recording that glues it in
    /// front declares.
    media_sequence: u64,
    /// Its `#EXT-X-DISCONTINUITY-SEQUENCE` (0 when absent, as on a
    /// recording): the breaks already in front of its first entry.
    discontinuity_sequence: u64,
    /// Its `#EXT-X-TARGETDURATION`, whole seconds.
    target_duration_s: u32,
    /// Its timeline, verbatim: every line from the first entry's first
    /// tag up to, not including, `#EXT-X-ENDLIST`, blank lines dropped.
    /// Unlike Solar Punk's MPEG-TS segments, these are fMP4, so the
    /// `#EXT-X-MAP` lines are part of it.
    lines: Vec<String>,
}

impl Predecessor {
    /// Milliseconds of media its timeline lists: every `#EXTINF` but a
    /// gap's (`#EXT-X-GAP` precedes the `#EXTINF` it marks), the way
    /// Solar Punk's `ManifestManager` sums an inherited timeline.
    fn media_ms(&self) -> u64 {
        let mut gap = false;
        let mut total = 0.0_f64;
        for line in &self.lines {
            if line == "#EXT-X-GAP" {
                gap = true;
            } else if let Some(rest) = line.strip_prefix("#EXTINF:") {
                if !gap {
                    let seconds = rest.split(',').next().unwrap_or("").trim();
                    total += seconds.parse::<f64>().unwrap_or(0.0).max(0.0);
                }
                gap = false;
            }
        }
        (total * 1000.0).round() as u64
    }

    /// Tags that open (or sit inside) the timeline: everything from the
    /// first of them on is entries, not header.
    const TIMELINE_TAGS: [&'static str; 5] = [
        "#EXT-X-DISCONTINUITY",
        "#EXT-X-MAP",
        "#EXT-X-GAP",
        "#EXT-X-PROGRAM-DATE-TIME",
        "#EXTINF",
    ];

    fn is_timeline(line: &str) -> bool {
        Self::TIMELINE_TAGS.iter().any(|tag| {
            line == *tag
                || line
                    .strip_prefix(tag)
                    .is_some_and(|rest| rest.starts_with(':'))
        })
    }

    /// `None` for anything that is not a media playlist with a
    /// `#EXT-X-MEDIA-SEQUENCE` and at least one entry (foreign content on
    /// the topic, or a playlist naming nothing): a broadcast over such a
    /// head still writes after it, but has no numbering to continue and
    /// nothing to glue.
    fn parse(playlist: &str) -> Option<Self> {
        let lines: Vec<&str> = playlist.lines().map(str::trim).collect();
        if lines.first() != Some(&"#EXTM3U") {
            return None;
        }
        let opens_at = lines.iter().position(|l| Self::is_timeline(l))?;
        let header = &lines[..opens_at];
        let number = |tag: &str| -> Option<f64> {
            header
                .iter()
                .find_map(|l| l.strip_prefix(tag)?.strip_prefix(':'))
                .and_then(|v| v.trim().parse::<f64>().ok())
                .filter(|v| v.is_finite() && *v >= 0.0)
        };
        let media_sequence = number("#EXT-X-MEDIA-SEQUENCE")? as u64;
        let timeline: Vec<String> = lines[opens_at..]
            .iter()
            .take_while(|l| **l != "#EXT-X-ENDLIST")
            .filter(|l| !l.is_empty())
            .map(|l| (*l).to_string())
            .collect();
        let entries = timeline
            .iter()
            .filter(|l| l.starts_with("#EXTINF:"))
            .count() as u64;
        if entries == 0 {
            return None;
        }
        Some(Self {
            next_number: media_sequence.saturating_add(entries),
            media_sequence,
            discontinuity_sequence: number("#EXT-X-DISCONTINUITY-SEQUENCE").unwrap_or(0.0) as u64,
            target_duration_s: number("#EXT-X-TARGETDURATION").map_or(0, |v| v.ceil() as u32),
            lines: timeline,
        })
    }
}

// ---------------------------------------------------------------------------
// Recovery entry (issue #175)
// ---------------------------------------------------------------------------

/// Version of the [`RecoveryEntry`] layout. An entry of any other version
/// is refused rather than half-read.
const RECOVERY_VERSION: u32 = 1;

/// What survives the app: everything a relaunch needs to carry a killed
/// broadcast on, or finalize it, at [`PublisherConfig::recovery_path`].
/// Solar Punk's uploader keeps the same per stream (`RecoveryStore`'s
/// `StreamState`: the feed index, the segments, the anchor, the offset
/// it continued from and the timeline it inherited).
///
/// Written ahead of each feed update ([`publish_playlist`]), so it holds
/// every entry that update's playlist lists, and `feed_index` is the
/// index that update takes: if it landed before the kill, the head is
/// there; if not, the head is the one before it. Read back by
/// [`load_recovery`] when a run starts with [`PublisherConfig::resume`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RecoveryEntry {
    version: u32,
    /// When it was written, Unix ms: a host compares it with the reap
    /// window to offer a resume or only a finalize.
    saved_at_ms: u64,
    channel: String,
    /// The feed's owner, 40 hex: only that signer can continue it.
    owner: String,
    /// The feed's topic, 64 hex, and its text when it has one.
    topic: String,
    topic_text: String,
    format: StreamFormat,
    /// The broadcast's id, which every playlist names
    /// (`#ANTSTREAM-BROADCAST`). A resumed run keeps it: it is the same
    /// broadcast.
    broadcast_id: String,
    /// The index of the feed update this entry was written ahead of.
    feed_index: u64,
    /// The media sequence number the next listed entry takes.
    next_number: u64,
    discontinuity_sequence: u64,
    /// How many of `recording`'s last entries the live window held.
    window: usize,
    /// Where the dating clock would place the next media segment, Unix
    /// ms: past the last listed entry when media after it was dropped.
    next_program_ms: Option<u64>,
    longest_listed_ms: u32,
    /// The run's [`PublisherConfig::segment_ms`]: with
    /// `longest_listed_ms`, the floor of `#EXT-X-TARGETDURATION`, which
    /// must not drop when a resumed run is configured with a shorter one.
    #[serde(default)]
    segment_ms: u32,
    /// Every entry the broadcast listed, in order: what the recording
    /// is rendered from.
    recording: Vec<MediaEntry>,
    /// The playlist the broadcast continued at go-live (issue #173),
    /// glued in front of its recording.
    predecessor: Option<Predecessor>,
    /// The playlist the update at `feed_index` carries. Past `"live"` a
    /// stop had begun closing the broadcast: its `#EXT-X-ENDLIST` window
    /// (or its recording) may already be the head, and a live viewer has
    /// stopped there, so a resumed run only finalizes it (see
    /// [`PublisherConfig::resume`]). Missing reads as `"live"`.
    #[serde(default)]
    shape: PlaylistShape,
    /// The stream info the run had (issue #220), the newest
    /// [`LiveRun::set_info`] included, and its rendition size: a resumed
    /// run started without its own takes them, so a killed broadcast's
    /// `live` catalog entry is still closed with a `vod` one when the
    /// host only asks to finalize it. Missing (an older entry, a run
    /// without info) reads as none.
    #[serde(default)]
    info: Option<StreamInfo>,
    #[serde(default)]
    width: u32,
    #[serde(default)]
    height: u32,
    /// The recording is the feed's head, and only the closing `vod`
    /// catalog entry is still to land (issue #220): set when that entry
    /// failed after the recording's update landed. Nothing about the
    /// broadcast is left unfinished for its viewers (they have the
    /// recording); a host need not hold a new broadcast for it, and
    /// ending it only writes that catalog entry. Missing reads as false.
    #[serde(default)]
    recorded: bool,
}

impl RecoveryEntry {
    /// A stop had begun closing the broadcast this entry saved: it can be
    /// finalized, never carried on live.
    fn ended(&self) -> bool {
        self.shape != PlaylistShape::Live
    }
}

/// Read the entry at `path`. A missing file and an unreadable one are
/// both errors, worded apart, and neither touches the file: an entry
/// that won't parse is still the only record of the broadcast it saved.
fn load_recovery(path: &str) -> Result<RecoveryEntry, String> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!("no broadcast to resume: {path} does not exist"));
        }
        Err(e) => return Err(format!("could not read {path}: {e}")),
    };
    let entry: RecoveryEntry = serde_json::from_slice(&raw)
        .map_err(|e| format!("{path} is not a recovery entry this build can read: {e}"))?;
    if entry.version != RECOVERY_VERSION {
        return Err(format!(
            "{path} is a version {} recovery entry; this build reads version {RECOVERY_VERSION}",
            entry.version,
        ));
    }
    if entry.window > entry.recording.len() {
        return Err(format!(
            "{path} is damaged: its live window holds {} entries of {}",
            entry.window,
            entry.recording.len(),
        ));
    }
    Ok(entry)
}

/// Replace the entry at `path` with `entry`: written to `<path>.tmp`
/// and renamed over it, so a kill mid-write leaves the previous entry
/// whole. Not flushed to the disk: what it guards against is the app
/// being killed, which leaves the written pages to the OS.
fn save_recovery(path: &str, entry: &RecoveryEntry) -> Result<(), String> {
    let json = serde_json::to_vec(entry).map_err(|e| format!("encode: {e}"))?;
    let target = std::path::Path::new(path);
    if let Some(dir) = target.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
    }
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, json).map_err(|e| format!("write {tmp}: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename {tmp} over {path}: {e}"))
}

/// Mark the entry at `path` [`RecoveryEntry::recorded`], if it is still
/// `broadcast_id`'s: its recording is the head, and only the closing
/// catalog entry is left.
fn mark_recovery_recorded(path: &str, broadcast_id: &str) -> Result<(), String> {
    let mut entry = match load_recovery(path) {
        Ok(entry) => entry,
        Err(_) if !std::path::Path::new(path).exists() => return Ok(()),
        Err(e) => return Err(e),
    };
    if entry.broadcast_id != broadcast_id || entry.recorded {
        return Ok(());
    }
    entry.recorded = true;
    save_recovery(path, &entry)
}

/// Remove the entry at `path` once the broadcast it saved is finished,
/// but only if it is still that broadcast's (`broadcast_id`): never one a
/// later broadcast wrote over it. A missing entry is fine; an entry that
/// can't be read is left alone.
fn clear_recovery(path: &str, broadcast_id: &str) -> Result<(), String> {
    let entry = match load_recovery(path) {
        Ok(entry) => entry,
        Err(_) if !std::path::Path::new(path).exists() => return Ok(()),
        Err(e) => return Err(e),
    };
    if entry.broadcast_id != broadcast_id {
        return Ok(());
    }
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("remove {path}: {e}")),
    }
}

impl PublisherState {
    /// The recovery entry for the state as it stands, written ahead of
    /// the feed update at `feed_index`.
    fn recovery_entry(
        &self,
        run: &RunIdentity<'_>,
        feed_index: u64,
        shape: PlaylistShape,
    ) -> RecoveryEntry {
        // Gaps still waiting for media took numbers nobody saw; a resumed
        // run hands them out again, so the recording's numbering (its
        // media sequence plus the position) matches the live windows'.
        let next_number = self
            .pending_gaps
            .first()
            .map_or(self.next_number, |gap| gap.number);
        RecoveryEntry {
            version: RECOVERY_VERSION,
            saved_at_ms: unix_ms_now(),
            channel: run.channel.to_string(),
            owner: hex::encode(run.owner),
            topic: hex::encode(run.topic),
            topic_text: run.topic_text.to_string(),
            format: run.format,
            broadcast_id: self.broadcast_id.clone(),
            feed_index,
            next_number,
            discontinuity_sequence: self.discontinuity_sequence,
            window: self.playlist.len(),
            next_program_ms: self.program_clock.next_ms,
            longest_listed_ms: self.longest_listed_ms,
            segment_ms: run.segment_ms,
            recording: self.recording.clone(),
            predecessor: self.predecessor.clone(),
            shape,
            info: self.info.clone(),
            width: run.width,
            height: run.height,
            recorded: false,
        }
    }

    /// A state that carries on the broadcast `entry` saved (see
    /// [`PublisherConfig::resume`]). Where it stands in the feed is not
    /// settled yet: [`resume_feed_position`] reads the head for that.
    fn resumed(entry: RecoveryEntry) -> Self {
        let window = entry.recording.len() - entry.window;
        let playlist: VecDeque<MediaEntry> = entry.recording[window..].iter().cloned().collect();
        // Never before the last saved segment ended, whatever the saved
        // clock says (hls.js reads a date that moves back as an error).
        let last_end = entry
            .recording
            .last()
            .map(|e| e.program_ms + u64::from(e.duration_ms));
        let next_ms = entry.next_program_ms.max(last_end);
        Self {
            broadcast_id: entry.broadcast_id,
            playlist,
            recording: entry.recording,
            next_number: entry.next_number,
            discontinuity_sequence: entry.discontinuity_sequence,
            // `#EXT-X-TARGETDURATION` stays where the broadcast had it.
            longest_listed_ms: entry.longest_listed_ms.max(entry.segment_ms),
            predecessor: entry.predecessor,
            // A new process is a new writer: no map is in force until its
            // init segment lands, the next entry is behind a break, and
            // its date is re-anchored on the wall clock.
            init_uri: None,
            break_since_last_entry: true,
            program_clock: ProgramClock {
                next_ms,
                reanchor: next_ms.is_some(),
            },
            feed_index: entry.feed_index,
            resume_index: Some(entry.feed_index),
            resume_shape: entry.shape,
            ..Self::default()
        }
    }

    /// Milliseconds of media this broadcast listed (the run it resumed
    /// included), gaps left out.
    fn own_media_ms(&self) -> u64 {
        self.recording
            .iter()
            .filter(|e| !e.gap)
            .map(|e| u64::from(e.duration_ms))
            .sum()
    }

    /// Something was listed in this broadcast, by this run or by the run
    /// it resumed.
    fn listed_any(&self) -> bool {
        !self.recording.is_empty()
    }
}

/// The parts of a run a recovery entry names it by.
struct RunIdentity<'a> {
    channel: &'a str,
    owner: &'a [u8; 20],
    topic: &'a [u8; 32],
    topic_text: &'a str,
    format: StreamFormat,
    segment_ms: u32,
    width: u32,
    height: u32,
}

// ---------------------------------------------------------------------------
// Postage capacity before going live (issue #176)
// ---------------------------------------------------------------------------

/// Below this many minutes of room a batch is **nearly full** for a
/// broadcast at the rate it was estimated for: [`CapacityEstimate::low`],
/// which the iOS app warns about before going live. An hour, so a typical
/// broadcast that starts inside the warning is told before it begins, not
/// by a refused segment halfway through.
pub const LOW_CAPACITY_MINUTES: u64 = 60;

/// Headroom on [`PublisherConfig::bitrate_kbps`] when estimating segment
/// sizes, in percent: an average-bitrate encoder overshoots within a
/// segment, and fMP4 boxing adds bytes on top of the media. Erring high
/// makes the estimate warn early rather than late.
pub const BITRATE_HEADROOM_PERCENT: u64 = 10;

/// Redundancy level every upload of a broadcast is stamped at: segments
/// name none, so the gateway applies bee's default (MEDIUM, `1`), and
/// playlists send [`PLAYLIST_REDUNDANCY_LEVEL`], the same level.
const UPLOAD_REDUNDANCY_LEVEL: u8 = PLAYLIST_REDUNDANCY_LEVEL;

/// Bytes one recording entry takes at most, for the recording's share of
/// the estimate: an `#EXT-X-PROGRAM-DATE-TIME` line, an `#EXTINF` line
/// and a `"bzz"` URI (`/bzz/<64 hex>/seg-<n>.m4s`), the longer of the
/// two formats' URIs.
const RECORDING_ENTRY_BYTES: usize = 160;

/// Bytes an fMP4 initialization segment takes, about: the iOS writer's
/// are under a kilobyte. One per writer start.
const INIT_SEGMENT_BYTES: usize = 1024;

/// How many chunks the gateway stamps for one upload of `bytes` under
/// `format`, counted with the same builders it assembles the upload with
/// (`ant_gateway`'s `assemble_bzz`, `upload_bytes`): the erasure-coded
/// chunk tree at [`UPLOAD_REDUNDANCY_LEVEL`], [`ant_retrieval::REPLICA_COUNTS`]
/// dispersed replica SOCs of its root, and under [`StreamFormat::Bzz`]
/// the single-file manifest's nodes with their replicas too. Every one
/// takes a stamp from the batch. Counted on a zero buffer of the right
/// size: the shape of the tree depends only on the length.
fn stamped_upload_chunks(
    format: StreamFormat,
    bytes: usize,
    name: &str,
    content_type: &str,
) -> u64 {
    let level = UPLOAD_REDUNDANCY_LEVEL;
    let replicas = ant_retrieval::REPLICA_COUNTS[usize::from(level)] as u64;
    let split = ant_retrieval::split_bytes_with_redundancy(&vec![0u8; bytes], level);
    let mut chunks = split.chunks.len() as u64 + replicas;
    if format == StreamFormat::Bzz {
        // A file name the manifest builder refuses would fail the upload
        // itself; the publisher's names never are.
        if let Ok(manifest) =
            ant_retrieval::build_single_file_manifest(name, Some(content_type), split.root)
        {
            chunks += manifest.chunks.len() as u64 * (1 + replicas);
        }
    }
    chunks
}

/// What a broadcast stamps on its batch, estimated from its config
/// before it starts (issue #176): see [`broadcast_cost`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct BroadcastCost {
    /// Chunks one minute of broadcast stamps: every media segment
    /// (bitrate × segment duration, plus [`BITRATE_HEADROOM_PERCENT`]),
    /// the live playlist rewritten after each (a full one-chunk window,
    /// [`LIVE_WINDOW_MAX_BYTES`]) and the feed update naming it, the
    /// ladder markers when [`PublisherConfig::ladder`] is on, and the
    /// minute's share of the recording written at stop.
    pub chunks_per_minute: u64,
    /// Chunks a broadcast stamps once whatever its length: the channel
    /// manifest, the initialization segment, the closing window and its
    /// feed update, and the recording's first chunk, manifest and
    /// update.
    pub fixed_chunks: u64,
}

/// Estimate what a broadcast configured as `config` stamps on its
/// batch, from its bitrate and segment duration, plus the playlist,
/// feed and marker overhead each segment brings (issue #176).
///
/// `bitrate_kbps` is taken as everything a segment carries: a host
/// whose segments also carry audio adds its rate (the iOS app does).
/// One initialization segment is counted, in the fixed part; a writer
/// restart adds another, a few chunks.
#[must_use]
pub fn broadcast_cost(config: &PublisherConfig) -> BroadcastCost {
    let format = config.format;
    let segment_ms = u64::from(config.segment_ms.max(1));
    let segment_bytes =
        u64::from(config.bitrate_kbps) * segment_ms / 8 * (100 + BITRATE_HEADROOM_PERCENT) / 100;
    let media = stamped_upload_chunks(
        format,
        usize::try_from(segment_bytes).unwrap_or(usize::MAX / 2),
        "seg-0.m4s",
        "video/iso.segment",
    );
    let playlist_upload = stamped_upload_chunks(
        format,
        LIVE_WINDOW_MAX_BYTES,
        PLAYLIST_NAME,
        "application/vnd.apple.mpegurl",
    );
    // The playlist, then the single-owner chunk of its feed update.
    let playlist = playlist_upload + 1;
    let per_segment = media + playlist;
    let segments_per_hour = 3_600_000 / segment_ms;
    let markers_per_hour = if config.ladder {
        3_600 / LADDER_MARKER_PERIOD_SECONDS
    } else {
        0
    };
    // The recording: one playlist upload at the stop, its manifest and
    // replicas once, its entries growing with the broadcast. The growth
    // of an hour's recording is spread over the hour, a few chunks a
    // minute.
    let recording = |entries: u64| {
        stamped_upload_chunks(
            format,
            usize::try_from(entries).unwrap_or(0) * RECORDING_ENTRY_BYTES,
            PLAYLIST_NAME,
            "application/vnd.apple.mpegurl",
        )
    };
    let recording_base = recording(1);
    let recording_growth = recording(segments_per_hour).saturating_sub(recording_base);
    let per_hour = per_segment * segments_per_hour + markers_per_hour + recording_growth;
    let replicas = ant_retrieval::REPLICA_COUNTS[usize::from(UPLOAD_REDUNDANCY_LEVEL)] as u64;
    // Its shape doesn't depend on the owner or topic it names.
    let feed_manifest_nodes = ant_retrieval::build_feed_manifest(&[0; 20], &[0; 32])
        .map_or(1, |manifest| manifest.chunks.len() as u64);
    BroadcastCost {
        chunks_per_minute: per_hour.div_ceil(60),
        // The channel manifest (`POST /feeds`, its mantaray nodes and
        // their replicas), the initialization segment, the closing window with
        // its update, and the recording's base with its update.
        fixed_chunks: feed_manifest_nodes * (1 + replicas)
            + stamped_upload_chunks(format, INIT_SEGMENT_BYTES, "init-0.mp4", "video/mp4")
            + playlist
            + recording_base
            + 1,
    }
}

/// [`broadcast_cost`], remembered per cost-relevant config: working it
/// out builds every segment's erasure-coded tree, while a host asks for
/// an estimate whenever its plan's room changes (on every redraw that
/// follows a plan refresh, mid-broadcast too). The cost depends only on
/// the format, bitrate, segment duration and ladder, so the memo stays a
/// handful of entries.
fn broadcast_cost_memoized(config: &PublisherConfig) -> BroadcastCost {
    static MEMO: Mutex<BTreeMap<(StreamFormat, u32, u32, bool), BroadcastCost>> =
        Mutex::new(BTreeMap::new());
    let key = (
        config.format,
        config.bitrate_kbps,
        config.segment_ms,
        config.ladder,
    );
    if let Some(cost) = MEMO.lock().ok().and_then(|m| m.get(&key).copied()) {
        return cost;
    }
    // Worked out outside the lock: two callers racing on a new key both
    // compute the same value.
    let cost = broadcast_cost(config);
    if let Ok(mut memo) = MEMO.lock() {
        memo.insert(key, cost);
    }
    cost
}

/// How long a broadcast fits on a batch, checked before going live
/// (issue #176): [`broadcast_cost`] against the batch's room, as
/// [`estimate_capacity`] works it out.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CapacityEstimate {
    /// The batch checked, `0x`-prefixed hex as the node reports it.
    pub batch_id: String,
    /// The batch refuses a stamp once a bucket is full. A mutable one
    /// overwrites the bucket's oldest chunk instead, so a broadcast
    /// longer than [`CapacityEstimate::minutes_left`] **eats its own
    /// beginning** with no error at all: a host warns about a mutable
    /// batch whatever its room.
    pub immutable: bool,
    /// The node will stamp with the batch at all (see the node's
    /// `PostageStatusView::usable`). On one that isn't, every upload
    /// would be refused: a host warns, and asks for a confirmation
    /// before going live on it (the iOS example's "Go live anyway")
    /// rather than blocking it outright. A batch still propagating
    /// after a buy counts as usable here.
    pub usable: bool,
    /// See [`BroadcastCost::chunks_per_minute`].
    pub chunks_per_minute: u64,
    /// Chunks the batch can still stamp counting its **fullest bucket**,
    /// not the average: `(bucket capacity − fullest bucket's fill) ×
    /// buckets` (the node's `worst_case_remaining_chunks`). Chunk
    /// addresses spread evenly over the buckets, so the fullest one
    /// fills first, and on an immutable batch that is when refusals
    /// begin.
    pub chunks_left: u64,
    /// Whole minutes of this broadcast that fit in `chunks_left`, after
    /// [`BroadcastCost::fixed_chunks`]. The "about N minutes left on
    /// this batch" a host shows.
    pub minutes_left: u64,
    /// `minutes_left` under [`LOW_CAPACITY_MINUTES`]: the batch is
    /// nearly full for this broadcast, and a host warns before going
    /// live.
    pub low: bool,
}

/// Check `plan` (the node's postage status, `ant_storage_status`) against
/// a broadcast configured as `config` (issue #176). Refuses a plan that
/// is not connected or is not the config's batch: an estimate for a
/// different batch than the broadcast stamps with would be a guess.
pub fn estimate_capacity(
    config: &PublisherConfig,
    plan: &ant_control::PostageStatusView,
) -> Result<CapacityEstimate, PublisherError> {
    if !plan.enabled {
        return Err(PublisherError::Config(
            "no storage plan is connected".to_string(),
        ));
    }
    let want = parse_hex32(&config.batch_id)
        .map_err(|e| PublisherError::Config(format!("batch_id {e}")))?;
    let have = parse_hex32(&plan.batch_id)
        .map_err(|e| PublisherError::Config(format!("plan batch_id {e}")))?;
    if want != have {
        return Err(PublisherError::Config(format!(
            "the plan's batch is 0x{}, the broadcast's 0x{}",
            hex::encode(have),
            hex::encode(want),
        )));
    }
    let cost = broadcast_cost_memoized(config);
    let chunks_left = plan.worst_case_remaining_chunks;
    let minutes_left =
        chunks_left.saturating_sub(cost.fixed_chunks) / cost.chunks_per_minute.max(1);
    Ok(CapacityEstimate {
        batch_id: format!("0x{}", hex::encode(have)),
        immutable: plan.immutable,
        usable: plan.usable || plan.propagating,
        chunks_per_minute: cost.chunks_per_minute,
        chunks_left,
        minutes_left,
        low: minutes_left < LOW_CAPACITY_MINUTES,
    })
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
    /// Sequence-feed index of the next update. `None` (JSON `null`)
    /// until the broadcast knows where it stands in its feed: with no
    /// [`PublisherConfig::start_index`], until its read of the feed head
    /// answers ([`settle_feed_position`]).
    pub feed_index: Option<u64>,
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
    /// The one-rung ladder's group id ([`ladder_group_text`]), what a
    /// catalog entry names the ladder by. Empty unless
    /// [`PublisherConfig::ladder`].
    pub ladder_group: String,
    /// Reference of the last master playlist that landed, empty while
    /// none has.
    pub master_reference: String,
    /// Ladder time markers that landed.
    pub markers_written: u64,
    /// Marker and master writes that failed. Not in `error_count`: a
    /// ladder write is a shortcut a viewer can do without.
    pub ladder_failures: u64,
    /// The newest marker or master failure, empty while none has
    /// failed. Not in `last_error`/`errors`, for the same reason.
    pub ladder_error: String,
    /// The newest failure to write or remove the recovery entry
    /// ([`PublisherConfig::recovery_path`]), empty while none has failed:
    /// a kill now may lose what the entry missed. Not in
    /// `last_error`/`errors`: it costs a viewer nothing.
    #[serde(default)]
    pub recovery_error: String,
    /// Uploads of this broadcast (segments, playlists, feed updates, the
    /// channel manifest, the recording) the gateway refused because the
    /// postage batch is **full**: bee's `402 "batch is overissued"`, a
    /// full collision bucket on an immutable batch (issue #176). Not a
    /// network failure, and no retry fixes it: the host names it as the
    /// storage plan running out. A batch fills unevenly, so these begin
    /// as the odd refused segment and soon refuse nearly every one. Each
    /// is also in `errors`/`error_count`, and a refused media segment in
    /// `segments_failed`.
    #[serde(default)]
    pub batch_full_refusals: u64,
    /// The same, for uploads refused because the batch itself is
    /// **unusable**: a `400` naming it (unknown to the node, expired) or
    /// a `422` (storer peers attest it is not on chain).
    #[serde(default)]
    pub batch_unusable_refusals: u64,
    /// The newest of those refusals, empty while there has been none.
    #[serde(default)]
    pub batch_error: String,
    /// Catalog entries written to the account's catalog feed this run
    /// ([`PublisherConfig::info`], issue #220).
    #[serde(default)]
    pub catalog_writes: u64,
    /// Catalog writes that failed or were skipped. Not in `error_count`:
    /// the channel plays without its entry.
    #[serde(default)]
    pub catalog_failures: u64,
    /// The newest catalog failure (or why the run writes no catalog),
    /// empty while there has been none. Not in `last_error`/`errors`.
    #[serde(default)]
    pub catalog_error: String,
    /// `state` of the newest entry that landed (`"live"`, `"vod"`), empty
    /// before one did.
    #[serde(default)]
    pub catalog_state: String,
    /// The catalog feed index that entry took.
    #[serde(default)]
    pub catalog_index: Option<u64>,
    /// Where the broadcast's time goes, stage by stage (issue #212).
    #[serde(default)]
    pub timings: PublisherTimings,
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
    /// Sequence-feed index the *next* update would take: where the next
    /// broadcast on this topic starts, whether it finds it by reading the
    /// feed head (the default) or a host passes it as
    /// [`PublisherConfig::start_index`]. Final in the report the stop
    /// call returns, even when its grace ran out first: the run is then
    /// sealed ([`LiveRun::seal_feed`]) and writes no further update.
    ///
    /// `None` (JSON `null`) when the run ended (stopped or reaped)
    /// before its read of the feed head ever answered: it wrote no
    /// update and doesn't know where the feed stands, so there is no
    /// index to hand on. A host must not substitute 0 (that rewrites
    /// the channel from its first update); passing the `null` on as
    /// `start_index` makes the next broadcast read the head itself.
    pub feed_index: Option<u64>,
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
    /// See [`PublisherSnapshot::ladder_group`].
    pub ladder_group: String,
    /// See [`PublisherSnapshot::master_reference`].
    pub master_reference: String,
    /// See [`PublisherSnapshot::markers_written`].
    pub markers_written: u64,
    /// See [`PublisherSnapshot::ladder_failures`].
    pub ladder_failures: u64,
    /// See [`PublisherSnapshot::ladder_error`].
    pub ladder_error: String,
    /// See [`PublisherSnapshot::recovery_error`].
    #[serde(default)]
    pub recovery_error: String,
    /// See [`PublisherSnapshot::batch_full_refusals`].
    #[serde(default)]
    pub batch_full_refusals: u64,
    /// See [`PublisherSnapshot::batch_unusable_refusals`].
    #[serde(default)]
    pub batch_unusable_refusals: u64,
    /// See [`PublisherSnapshot::batch_error`].
    #[serde(default)]
    pub batch_error: String,
    /// See [`PublisherSnapshot::catalog_writes`].
    #[serde(default)]
    pub catalog_writes: u64,
    /// See [`PublisherSnapshot::catalog_failures`].
    #[serde(default)]
    pub catalog_failures: u64,
    /// See [`PublisherSnapshot::catalog_error`].
    #[serde(default)]
    pub catalog_error: String,
    /// See [`PublisherSnapshot::catalog_state`]: `"vod"` once the closing
    /// entry landed.
    #[serde(default)]
    pub catalog_state: String,
    /// See [`PublisherSnapshot::catalog_index`].
    #[serde(default)]
    pub catalog_index: Option<u64>,
    /// See [`PublisherSnapshot::timings`].
    #[serde(default)]
    pub timings: PublisherTimings,
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
    /// Wakes a reaper holding a due reap for the return.
    reap_notify: Arc<Notify>,
    /// Wakes the catalog writer ([`catalog_loop`]).
    catalog_notify: Arc<Notify>,
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
    /// still has a whole window to deliver its first segment. A return
    /// after the window already ran out during the outage ends the run
    /// instead (see [`reaper`]). Idempotent,
    /// and harmless without a prior [`LiveRun::pause`] (a host can call
    /// it on every return to the foreground).
    pub fn resume(&self) {
        {
            let mut state = lock(&self.state);
            state.paused = false;
            // The window already ran out during the outage: the reaper
            // ends the run now that the closing uploads can land, rather
            // than this return buying a fresh window.
            let first_return = !state.reap_due
                && match (state.returned_at, state.last_media_at) {
                    (None, _) => true,
                    (Some(returned), Some(media)) => media >= returned,
                    (Some(_), None) => false,
                };
            if first_return {
                state.returned_at = Some(Instant::now());
            }
        }
        // The pump may be holding an initialization segment for this,
        // and the reaper a due reap.
        self.push_notify.notify_one();
        self.reap_notify.notify_one();
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
            feed_index: state.feed_settled.then_some(state.feed_index),
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
            ladder_group: self.ladder_group(),
            master_reference: state.master_reference.clone().unwrap_or_default(),
            markers_written: state.markers_written,
            ladder_failures: state.ladder_failures,
            ladder_error: state.ladder_error.clone().unwrap_or_default(),
            recovery_error: state.recovery_error.clone().unwrap_or_default(),
            batch_full_refusals: state.batch_full_refusals,
            batch_unusable_refusals: state.batch_unusable_refusals,
            batch_error: state.batch_error.clone().unwrap_or_default(),
            catalog_writes: state.catalog_writes,
            catalog_failures: state.catalog_failures,
            catalog_error: state.catalog_error.clone().unwrap_or_default(),
            catalog_state: state
                .catalog_last
                .as_ref()
                .map(|e| e.state.as_str().to_string())
                .unwrap_or_default(),
            catalog_index: state.catalog_index,
            timings: state.stages.timings(&state.publish_ms),
        }
    }

    fn ladder_group(&self) -> String {
        if self.config.ladder {
            ladder_group_text(&self.topic)
        } else {
            String::new()
        }
    }

    /// Replace the stream info (issue #220): the catalog entry is
    /// rewritten with it at once, while live, and the closing entry at
    /// stop carries it. Before the first feed update landed it is held
    /// for the first entry. Refused when invalid (a thumbnail that is not
    /// a reference); see [`PublisherConfig::info`] for when a run writes
    /// a catalog at all. On a run that can write none (a `"bzz"` channel,
    /// a raw topic) the info is kept but nothing is written, and
    /// `catalog_error` says why, as when the run starts with info.
    pub fn set_info(&self, info: StreamInfo) -> Result<(), String> {
        info.validate()?;
        {
            let mut state = lock(&self.state);
            state.info = Some(info);
            state.catalog_info_changed = true;
            if let Some(why) = catalog_refusal(&self.config, &self.topic_text) {
                state.record_catalog_failure(why.to_string());
            }
        }
        self.catalog_notify.notify_one();
        Ok(())
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
        self.catalog_notify.notify_one();
    }

    #[must_use]
    pub fn is_finished(&self) -> bool {
        lock(&self.state).finished
    }

    /// Write no further feed update: the stop call is about to hand
    /// `feed_index` to the host before the run finished (its grace ran
    /// out on a slow uplink), and the next broadcast on the channel
    /// starts there (reading the head, or given it as `start_index`).
    /// Without the seal, a closing window or recording still uploading
    /// in the background would land at that very index, so the next
    /// broadcast's first update would collide with it. Segments and playlists already on the wire still
    /// finish; only their feed updates are skipped (and reported).
    ///
    /// The seal covers the account's catalog feed too (issue #220): the
    /// next broadcast reads that feed's head and writes after it, so a
    /// closing entry landing late would take the index it writes at.
    ///
    /// Returns whether an update is in flight right now: it may still
    /// land and take `feed_index` (or the catalog's next index), so the
    /// caller must wait for [`LiveRun::feed_update_in_flight`] to clear
    /// (at most [`PUBLISH_TIMEOUT`]) before reading the index it hands
    /// out.
    #[must_use]
    pub fn seal_feed(&self) -> bool {
        let mut state = lock(&self.state);
        state.feed_sealed = true;
        state.feed_update_in_flight || state.catalog_update_in_flight
    }

    /// A feed update (the channel's or the catalog's) is on the wire; see
    /// [`LiveRun::seal_feed`].
    #[must_use]
    pub fn feed_update_in_flight(&self) -> bool {
        let state = lock(&self.state);
        state.feed_update_in_flight || state.catalog_update_in_flight
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
            feed_index: state.feed_settled.then_some(state.feed_index),
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
            ladder_group: self.ladder_group(),
            master_reference: state.master_reference.clone().unwrap_or_default(),
            markers_written: state.markers_written,
            ladder_failures: state.ladder_failures,
            ladder_error: state.ladder_error.clone().unwrap_or_default(),
            recovery_error: state.recovery_error.clone().unwrap_or_default(),
            batch_full_refusals: state.batch_full_refusals,
            batch_unusable_refusals: state.batch_unusable_refusals,
            batch_error: state.batch_error.clone().unwrap_or_default(),
            catalog_writes: state.catalog_writes,
            catalog_failures: state.catalog_failures,
            catalog_error: state.catalog_error.clone().unwrap_or_default(),
            catalog_state: state
                .catalog_last
                .as_ref()
                .map(|e| e.state.as_str().to_string())
                .unwrap_or_default(),
            catalog_index: state.catalog_index,
            timings: state.stages.timings(&state.publish_ms),
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
    start_with_wall_offset(runtime, config, signing_secret, owner, status_rx, 0)
}

/// [`start`] with the wall clock the ladder markers are timed and dated
/// by moved `wall_offset_ms` on, so a test can put a marker period
/// boundary a moment away instead of waiting up to ten seconds for one.
#[doc(hidden)]
pub fn start_with_wall_offset(
    runtime: &tokio::runtime::Handle,
    config: PublisherConfig,
    signing_secret: [u8; 32],
    owner: [u8; 20],
    status_rx: Option<watch::Receiver<StatusSnapshot>>,
    wall_offset_ms: i64,
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
    // An entry a stop had begun closing is only finalized: the run starts
    // stopped, as if stopped straight away (see `PublisherConfig::resume`).
    let mut finalize_only = false;
    let mut config = config;
    let state = if config.resume {
        let entry = load_recovery(&config.recovery_path).map_err(PublisherError::Config)?;
        check_resumable(&entry, &config, &owner, &topic)?;
        finalize_only = entry.ended();
        // What the host left out comes from the broadcast it resumes: its
        // stream info and its rendition size, so the closing catalog entry
        // is written (and names the same rung) even when the host only
        // asks to end a killed broadcast (issue #220).
        if config.info.is_none() {
            config.info.clone_from(&entry.info);
        }
        if config.width == 0 || config.height == 0 {
            config.width = entry.width;
            config.height = entry.height;
        }
        PublisherState::resumed(entry)
    } else {
        PublisherState {
            broadcast_id: format!("{started_ms:x}"),
            // An override is the index, as given: nothing to read. Without
            // one the committer reads the head first (issue #173).
            feed_index: config.start_index.unwrap_or(0),
            feed_settled: config.start_index.is_some(),
            ..PublisherState::default()
        }
    };
    let mut state = state;
    state.info.clone_from(&config.info);
    let state = Arc::new(Mutex::new(state));
    let cancel = Arc::new(AtomicBool::new(finalize_only));
    let push_notify = Arc::new(Notify::new());
    let commit_notify = Arc::new(Notify::new());
    let reap_notify = Arc::new(Notify::new());
    let catalog_notify = Arc::new(Notify::new());
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
        reap_notify: Arc::clone(&reap_notify),
        catalog_notify: Arc::clone(&catalog_notify),
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
        reap_notify,
        catalog_notify,
        status_rx,
        manifest_task: Mutex::new(None),
        master_task: Mutex::new(None),
        started_at,
        topic_text: run.topic_text.clone(),
        wall_offset_ms,
    });
    runtime.spawn(async move { drive(ctx).await });
    Ok(run)
}

/// Refuse to resume `entry` with a run that would write somewhere else:
/// another signer (a different account on the same device), another
/// topic or another format.
fn check_resumable(
    entry: &RecoveryEntry,
    config: &PublisherConfig,
    owner: &[u8; 20],
    topic: &[u8; 32],
) -> Result<(), PublisherError> {
    let path = &config.recovery_path;
    if entry.owner != hex::encode(owner) {
        return Err(PublisherError::Config(format!(
            "{path} is a broadcast by 0x{}, not this account (0x{})",
            entry.owner,
            hex::encode(owner),
        )));
    }
    if entry.topic != hex::encode(topic) {
        return Err(PublisherError::Config(format!(
            "{path} is a broadcast on topic {}, not this config's {}",
            entry.topic,
            hex::encode(topic),
        )));
    }
    if entry.format != config.format {
        return Err(PublisherError::Config(format!(
            "{path} is a \"{}\" broadcast; resume it with that format, not \"{}\"",
            entry.format.as_str(),
            config.format.as_str(),
        )));
    }
    Ok(())
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
    /// Wakes the reaper on [`LiveRun::resume`]; see [`reaper`].
    reap_notify: Arc<Notify>,
    /// Wakes [`catalog_loop`]: new info, the first landed feed update, the
    /// stop.
    catalog_notify: Arc<Notify>,
    /// The master playlist write that may be running; see
    /// [`RunCtx::spawn_master_if_due`].
    master_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Go-live, where the reap window first runs from.
    started_at: Instant,
    /// The topic's text name, empty for a raw topic: the master's rung URI.
    topic_text: String,
    /// See [`start_with_wall_offset`].
    wall_offset_ms: i64,
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

    /// Write the recovery entry ahead of the feed update at `index`, when
    /// the host asked for one. A failure is reported in `recovery_error`
    /// and the update goes out all the same: the broadcast matters more
    /// than its insurance.
    async fn save_recovery_entry(&self, entry: Option<RecoveryEntry>) {
        let Some(entry) = entry else { return };
        let path = self.config.recovery_path.clone();
        let result = tokio::task::spawn_blocking(move || save_recovery(&path, &entry))
            .await
            .unwrap_or_else(|e| Err(format!("write task: {e}")));
        if let Err(message) = result {
            lock(&self.state).recovery_error = Some(format!("recovery entry: {message}"));
        }
    }

    /// The broadcast is finished on the feed (its recording is the head)
    /// and in the catalog (its `vod` entry landed, or none was due):
    /// remove its recovery entry.
    async fn clear_recovery_entry(&self) {
        if self.config.recovery_path.is_empty() {
            return;
        }
        let path = self.config.recovery_path.clone();
        let id = lock(&self.state).broadcast_id.clone();
        let result = tokio::task::spawn_blocking(move || clear_recovery(&path, &id))
            .await
            .unwrap_or_else(|e| Err(format!("remove task: {e}")));
        if let Err(message) = result {
            lock(&self.state).recovery_error = Some(format!("recovery entry: {message}"));
        }
    }

    /// The recording is the feed's head but the closing `vod` catalog
    /// entry did not land: say so in the recovery entry
    /// ([`RecoveryEntry::recorded`]), which stays for a later run to
    /// write that entry.
    async fn mark_recovery_recorded(&self) {
        if self.config.recovery_path.is_empty() {
            return;
        }
        let path = self.config.recovery_path.clone();
        let id = lock(&self.state).broadcast_id.clone();
        let result = tokio::task::spawn_blocking(move || mark_recovery_recorded(&path, &id))
            .await
            .unwrap_or_else(|e| Err(format!("write task: {e}")));
        if let Err(message) = result {
            lock(&self.state).recovery_error = Some(format!("recovery entry: {message}"));
        }
    }

    fn identity(&self) -> RunIdentity<'_> {
        RunIdentity {
            channel: &self.config.channel,
            owner: &self.owner,
            topic: &self.topic,
            topic_text: &self.topic_text,
            format: self.config.format,
            segment_ms: self.config.segment_ms,
            width: self.config.width,
            height: self.config.height,
        }
    }

    /// The wall clock ladder markers are timed and dated by, Unix ms.
    fn wall_ms(&self) -> u64 {
        unix_ms_now().saturating_add_signed(self.wall_offset_ms)
    }

    /// The ladder's master feed topic: [`topic_from_string`] of
    /// [`ladder_group_text`].
    fn group_topic(&self) -> [u8; 32] {
        topic_from_string(&ladder_group_text(&self.topic))
    }

    /// Why this run can write no catalog entry, or `None` when it can:
    /// see [`PublisherConfig::info`].
    fn catalog_refusal(&self) -> Option<&'static str> {
        catalog_refusal(&self.config, &self.topic_text)
    }

    /// Whether this run writes a master playlist at all: see
    /// [`PublisherConfig::ladder`].
    fn writes_master(&self) -> bool {
        self.config.ladder
            && self.config.format == StreamFormat::SolarPunk
            && fits_swarm_uri(&self.topic_text)
    }

    /// Start a master playlist write when one is due and none is
    /// running: the bitrate has been measured, and either no master
    /// landed yet this run or the measured peak outgrew the advertised
    /// one by more than a quarter (`BANDWIDTH` only rises, so this
    /// rewrites a handful of times at most). A failed write holds off
    /// [`MASTER_RETRY`]. Spawned, never awaited, so the committer's
    /// playlist and feed updates never wait on it.
    fn spawn_master_if_due(self: &Arc<Self>) {
        if !self.writes_master() {
            return;
        }
        let mut slot = lock(&self.master_task);
        if slot.as_ref().is_some_and(|task| !task.is_finished()) {
            return;
        }
        {
            let state = lock(&self.state);
            let Some((peak, _)) = state.bitrate.measured() else {
                return;
            };
            if state
                .master_advertised_bps
                .is_some_and(|ad| peak * 4 <= ad * 5)
            {
                return;
            }
            if state.master_retry_at.is_some_and(|at| Instant::now() < at) {
                return;
            }
        }
        let ctx = Arc::clone(self);
        *slot = Some(tokio::spawn(async move {
            publish_master(&ctx).await;
        }));
    }

    /// End the run the way the host's stop does, as `reaped`. Refused
    /// (false) while paused: see [`reaper`].
    fn reap(&self) -> bool {
        {
            let mut state = lock(&self.state);
            if state.intake_closed || self.cancelled() {
                return true;
            }
            if state.paused {
                return false;
            }
            state.reaped = true;
            // Under the lock, so a pause can't land between the check
            // above and the cancel and withhold what the stop drains.
            self.cancel.store(true, Ordering::SeqCst);
        }
        // See `LiveRun::cancel` for why `notify_one`.
        self.push_notify.notify_one();
        self.commit_notify.notify_one();
        self.catalog_notify.notify_one();
        true
    }
}

/// End the broadcast once [`PublisherConfig::reap_ms`] passes with no
/// media reaching the playlist (issue #172): the host is gone (the app
/// was backgrounded and never came back, the connection never
/// returned), and a viewer must not be left on a live playlist that
/// will never advance again. Re-reads the deadline every time it wakes,
/// since media and returns only ever push it later.
///
/// Never while paused: ending the run uploads the closing window and the
/// recording, and with the uplink gone both would fail once and never be
/// tried again, leaving the feed's head a live playlist with no
/// `#EXT-X-ENDLIST` and the channel with no recording. A window that
/// runs out during an outage marks the reap due and waits for
/// [`LiveRun::resume`]; that return ends the run (it buys no fresh
/// window), with the uploads going out over the connection that just
/// came back. Media that lands while it waits (an upload in flight when
/// the window ran out) clears the due reap and wakes the reaper too,
/// which then sleeps out the window that media restarted and, if that
/// one also runs out paused, marks the reap due again. An outage that
/// never ends leaves the run paused until the host's stop.
async fn reaper(ctx: Arc<RunCtx>, reap: Duration) {
    loop {
        // Registered before the state is read, so a return between the
        // read and the await still wakes us.
        let returned = ctx.reap_notify.notified();
        tokio::pin!(returned);
        let deadline = {
            let mut state = lock(&ctx.state);
            if state.intake_closed || ctx.cancelled() {
                return;
            }
            let deadline = state.reap_deadline(ctx.started_at, reap);
            if state.paused && Instant::now() >= deadline {
                state.reap_due = true;
                None
            } else {
                Some(deadline)
            }
        };
        let Some(deadline) = deadline else {
            returned.await;
            continue;
        };
        let now = Instant::now();
        if now >= deadline {
            // A pause that slipped in since the read: go round again,
            // which marks the reap due and waits for the return.
            if ctx.reap() {
                return;
            }
            continue;
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
    let markers = ctx
        .config
        .ladder
        .then(|| tokio::spawn(marker_loop(Arc::clone(&ctx))));
    let catalog = match ctx.catalog_refusal() {
        None => Some(tokio::spawn(catalog_loop(Arc::clone(&ctx)))),
        Some(why) => {
            let mut state = lock(&ctx.state);
            if state.info.is_some() {
                state.record_catalog_failure(why.to_string());
            }
            None
        }
    };
    let pump = tokio::spawn(pump(Arc::clone(&ctx)));
    let committer = tokio::spawn(commit_loop(Arc::clone(&ctx)));
    let _ = pump.await;
    if let Some(reaper) = reaper {
        reaper.abort();
    }
    let _ = committer.await;
    // The ladder's extras are shortcuts for joining viewers, never what
    // a stop waits on: a marker (bounded by its own ten-second period)
    // or a master still on the wire is dropped, not joined. The next
    // broadcast reads the group feed's head before it writes a master.
    if let Some(markers) = markers {
        markers.abort();
    }
    let master = lock(&ctx.master_task).take();
    if let Some(master) = master {
        master.abort();
    }
    // The catalog writer is joined, not aborted: a write it has on the
    // wire may land and take its index, and the closing entry goes after
    // it. It exits at its next wake, which the stop already gave it.
    let catalog_closed = match catalog {
        None => true,
        Some(catalog) => {
            ctx.catalog_notify.notify_one();
            match catalog.await {
                Ok(writer) => close_catalog(&ctx, writer).await,
                Err(_) => false,
            }
        }
    };
    // The recording is the feed's head, so nothing is left to resume or
    // finalize on the feed, and the catalog no longer says `live`: the
    // recovery entry goes. Kept while the `vod` entry has not landed (a
    // failed or sealed write, a kill before it): a relaunch that ends
    // the broadcast finds the recording at the head and writes only that
    // entry (issue #220). Marked `recorded` then, so the host knows the
    // viewers already have the recording and only the catalog is open.
    if lock(&ctx.state).recording_landed {
        if catalog_closed {
            ctx.clear_recovery_entry().await;
        } else {
            ctx.mark_recovery_recorded().await;
        }
    }
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
            publish_bzz_with(
                &ctx.target,
                ctx.batch,
                &name,
                content_type,
                &pending.data,
                &stored_ack_headers(),
            )
            .await
            .map(|reference| bzz_uri(&reference, &name))
        }
        // The bare reference is the URI: see `publish_bytes`.
        StreamFormat::SolarPunk => {
            publish_bytes(&ctx.target, ctx.batch, &pending.data, &stored_ack_headers()).await
        }
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
                        state.landed_at.insert(pending.seq, Instant::now());
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
                                bytes,
                            },
                        );
                    }
                }
            }
            Err(error) => {
                state.record_upload_error(
                    &error,
                    format!("{} segment {}: {error}", pending.kind.as_str(), pending.seq),
                );
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
///
/// Nothing is folded in before the broadcast knows where it stands in
/// its feed ([`settle_feed_position`]): the head decides both the index
/// of the first update and the `#EXT-X-MEDIA-SEQUENCE` the first
/// playlist is rendered with. Segments upload meanwhile and their
/// outcomes wait in `done`.
async fn commit_loop(ctx: Arc<RunCtx>) {
    if !settle_until_known(&ctx).await {
        return;
    }
    loop {
        let notified = ctx.commit_notify.notified();
        tokio::pin!(notified);

        let (newest, held_reap_cleared) = {
            let mut state = lock(&ctx.state);
            let held = state.reap_due;
            let newest = state.advance(ctx.config.playlist_window, ctx.config.segment_ms);
            (newest, held && !state.reap_due)
        };
        if held_reap_cleared {
            // The reaper is parked on the return for a reap that is no
            // longer due: wake it to re-read the deadline the media just
            // moved on, so a window that runs out paused again is marked
            // due again (see `reaper`).
            ctx.reap_notify.notify_one();
        }
        if let Some(captured_at) = newest {
            publish_playlist(&ctx, PlaylistShape::Live, Some(captured_at)).await;
            ctx.spawn_master_if_due();
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

/// How long the committer waits between two reads of the feed head that
/// did not answer (issue #173).
const FEED_HEAD_RETRY: Duration = Duration::from_secs(2);

/// How long a resumed run that is stopping (finalizing, or reaped) keeps
/// reading a feed head that does not answer before it gives up
/// ([`settle_until_known`]). Measured from the stop itself (to within
/// [`STOP_POLL`]), not from the first read that failed after it: a read
/// still in flight at the budget's end is dropped rather than waited out
/// under its own [`PUBLISH_TIMEOUT`]. So the head is given up at most
/// ~60 s into `ant_publisher_stop`'s 130 s grace, leaving ~70 s for the
/// closing window and the recording: seconds on a working uplink. A
/// wedged one can't fit their four requests (each under
/// [`PUBLISH_TIMEOUT`]) in any grace; the stop then seals the feed and
/// the recovery entry is kept for another go, so nothing is lost.
const RESUME_HEAD_STOP_BUDGET: Duration = Duration::from_secs(60);

/// How often a resumed run's head read in flight checks whether a stop
/// arrived and its [`RESUME_HEAD_STOP_BUDGET`] ran out.
const STOP_POLL: Duration = Duration::from_millis(250);

/// Run `read` until it answers, or, once `cancelled` reports a stop,
/// until [`RESUME_HEAD_STOP_BUDGET`] has passed since the stop was first
/// seen (`stopping_since`, carried across reads so retries share one
/// budget). `None` when the budget ran out; the read is dropped then.
async fn read_within_stop_budget<T>(
    read: impl std::future::Future<Output = T>,
    cancelled: impl Fn() -> bool,
    stopping_since: &mut Option<tokio::time::Instant>,
) -> Option<T> {
    tokio::pin!(read);
    loop {
        if cancelled() {
            let since = *stopping_since.get_or_insert_with(tokio::time::Instant::now);
            if since.elapsed() >= RESUME_HEAD_STOP_BUDGET {
                return None;
            }
        }
        tokio::select! {
            out = &mut read => return Some(out),
            () = tokio::time::sleep(STOP_POLL) => {}
        }
    }
}

/// Read the feed head until it answers, so the committer can start. A
/// new broadcast gives up at the first read that fails once the run is
/// stopping or the stop call sealed the feed ([`LiveRun::seal_feed`]), so
/// a stop waits on at most the read in flight (or the one after the
/// retry pause), each under the [`PUBLISH_TIMEOUT`] deadline. The
/// broadcast then writes no playlist and no feed update at all, because
/// without the head every index and every number it could pick might be
/// one an earlier broadcast already holds. Returns whether the position
/// is known.
///
/// A resumed run keeps retrying through a stop, for up to
/// [`RESUME_HEAD_STOP_BUDGET`] after the stop, a read in flight then
/// included (or until the stop call seals the feed): finalizing (resume and stop straight away) is
/// the whole point of such a run, and the broadcast it carries already
/// holds everything a viewer saw, so one cold or `503` head answer must
/// not leave it unrecorded.
///
/// A broadcast that can't read its head for its whole reap window
/// ([`PublisherConfig::reap_ms`]) is reaped like any other that listed no
/// media in that time.
async fn settle_until_known(ctx: &Arc<RunCtx>) -> bool {
    // Tokio's clock (the same as std's outside a paused test runtime),
    // shared with `read_within_stop_budget`.
    let mut stopping_since: Option<tokio::time::Instant> = None;
    loop {
        let resume_index = {
            let state = lock(&ctx.state);
            if state.feed_settled {
                return true;
            }
            state.resume_index
        };
        let result = match resume_index {
            None => settle_feed_position(ctx).await,
            Some(saved) => match read_within_stop_budget(
                resume_feed_position(ctx, saved),
                || ctx.cancelled(),
                &mut stopping_since,
            )
            .await
            {
                None => Err(format!(
                    "no answer within {}s of the stop",
                    RESUME_HEAD_STOP_BUDGET.as_secs()
                )),
                Some(Ok(Resumed::Settled)) => Ok(()),
                Some(Ok(Resumed::Finished)) => {
                    // Its recording is already the head: no playlist to
                    // write. What the entry may still stand for is the
                    // closing `vod` catalog entry (issue #220), which
                    // `drive` writes before it removes the entry.
                    {
                        let mut state = lock(&ctx.state);
                        state.recording_landed = true;
                        // The recording is at `saved`: the next broadcast's
                        // index, and the `vod` entry's is the one before.
                        state.feed_index = saved + 1;
                        state.feed_settled = true;
                    }
                    ctx.cancel.store(true, Ordering::SeqCst);
                    ctx.push_notify.notify_one();
                    ctx.reap_notify.notify_one();
                    return false;
                }
                Some(Ok(Resumed::Refused(message))) => {
                    lock(&ctx.state).record_error(format!(
                        "resume: {message}; nothing was written, and the recovery entry \
                         is left as it was"
                    ));
                    // Ended like a reap, so the host sees the run finish
                    // and reads the reason; the pump drains what it has.
                    ctx.cancel.store(true, Ordering::SeqCst);
                    ctx.push_notify.notify_one();
                    ctx.reap_notify.notify_one();
                    return false;
                }
                Some(Err(message)) => Err(message),
            },
        };
        match result {
            Ok(()) => return true,
            Err(message) => {
                let mut state = lock(&ctx.state);
                let stopping = ctx.cancelled()
                    && (resume_index.is_none()
                        || stopping_since
                            .get_or_insert_with(tokio::time::Instant::now)
                            .elapsed()
                            >= RESUME_HEAD_STOP_BUDGET);
                // A resumed run's retry pause counts against its budget
                // too: the next read starts by checking it.
                let gave_up = stopping || state.feed_sealed;
                state.record_error(if gave_up {
                    format!(
                        "feed head: {message}; no playlist or feed update was written, \
                         since the index and numbering to continue from are unknown"
                    )
                } else {
                    format!(
                        "feed head: {message}; retrying before the first feed update, \
                         so an earlier broadcast on the channel is not overwritten"
                    )
                });
                if gave_up {
                    return false;
                }
            }
        }
        tokio::time::sleep(FEED_HEAD_RETRY).await;
    }
}

/// Find where this broadcast stands in its feed (issue #173): read the
/// head once with `GET /feeds/{owner}/{topic}?exact=true`, then continue
/// the feed after its index and the media numbering after its playlist
/// ([`PublisherState::continue_from`]).
///
/// `exact` makes ant's node answer only once the slot after the head is
/// confirmed empty (`ant_retrieval::resolve_sequence_feed_exact`), and
/// `503` otherwise. Without it a head slow to answer (past the finder's
/// 800 ms look-ahead deadline) or missed by one or two cold peers reads
/// as absent, so the answer lands one or more updates below the real
/// head and the first update here would take a slot already written.
///
/// The answers, as Solar Punk's uploader reads them at the start of a
/// session over a topic that outlives it (`resumeFeedIndex`):
///
/// - `404 "no update found"`: nothing was ever written to the feed, so
///   the broadcast starts it at index 0, numbered from 0.
/// - `200`: the head's index is `swarm-feed-index`. Its playlist is the
///   body of a v2 update (`"solar-punk"`); a v1 update (`"bzz"`) points
///   at the playlist's manifest, whose reference is the response's
///   `ETag`, so the playlist is `GET /bzz/<it>/stream.m3u8`. A head that
///   isn't a playlist (foreign content on the topic, or a v1 manifest
///   with no `stream.m3u8`: `404 "path address not found"`) still moves
///   the index on, with nothing to continue numbering from. A v1
///   playlist that does not retrieve (any other 404, a 5xx) is an error
///   like the head's own.
/// - Anything else, including any other 404 (bee's `"wrapped chunk
///   cannot be retrieved"` is a head that exists and did not retrieve):
///   an error. Never read as an empty feed, which would write index 0
///   and up over the previous broadcast.
async fn settle_feed_position(ctx: &RunCtx) -> Result<(), String> {
    let path = format!(
        "{}/feeds/{}/{}?exact=true",
        ctx.target.prefix,
        hex::encode(ctx.owner),
        hex::encode(ctx.topic),
    );
    let response = http_get_timed(&ctx.target, &path).await?;
    let (next_index, prior) = match response.status {
        404 if is_no_update_found(&response.body) => (0, None),
        200 => {
            require_declared_length(&response, "the head")?;
            let index = feed_index_of(&response)?;
            let playlist = if response.header("swarm-feed-resolved-version") == Some("v1") {
                let reference = response
                    .header("etag")
                    .map(|v| v.trim_matches('"').to_ascii_lowercase())
                    .filter(|r| r.len() == 64 && hex::decode(r).is_ok())
                    .ok_or_else(|| "the v1 head carries no reference".to_string())?;
                let path = format!("{}/bzz/{reference}/{PLAYLIST_NAME}", ctx.target.prefix);
                let playlist = http_get_timed(&ctx.target, &path).await?;
                match playlist.status {
                    200 => {
                        require_declared_length(&playlist, "the head playlist")?;
                        playlist.body
                    }
                    // The manifest has no playlist, or the head is no
                    // manifest at all: not a broadcast.
                    _ if is_not_a_broadcast_manifest(&playlist.body) => Vec::new(),
                    // Any other answer, a 404 for a playlist chunk that
                    // did not retrieve included (the v1 twin of bee's
                    // `"wrapped chunk cannot be retrieved"` above), is no
                    // answer: numbering from 0 would move the channel's
                    // media sequence back and drop every earlier
                    // broadcast from the recording.
                    status => {
                        return Err(format!(
                            "head playlist {reference}: gateway returned {status} {}",
                            String::from_utf8_lossy(&playlist.body).trim(),
                        ))
                    }
                }
            } else {
                response.body
            };
            let prior = std::str::from_utf8(&playlist)
                .ok()
                .and_then(Predecessor::parse);
            (index.checked_add(1).ok_or("the feed is full")?, prior)
        }
        status => {
            return Err(format!(
                "gateway returned {status} {}",
                String::from_utf8_lossy(&response.body).trim(),
            ))
        }
    };
    let mut state = lock(&ctx.state);
    state.feed_index = next_index;
    if let Some(prior) = prior {
        state.continue_from(prior);
    }
    state.feed_settled = true;
    Ok(())
}

/// What the head said about a resumed broadcast's place in its feed.
enum Resumed {
    /// The head is the update the entry was written ahead of, or the one
    /// before it: the run writes after it.
    Settled,
    /// The head is anywhere else: the channel moved on since the entry
    /// was written. Never retried.
    Refused(String),
    /// The entry was written ahead of the recording, and the head is it:
    /// the broadcast is finished on its feed. Its entry was left behind by
    /// a closing `vod` catalog entry that did not land, or a removal that
    /// failed: the run writes only that catalog entry, then removes it.
    Finished,
}

/// Settle a resumed broadcast's place in its feed (issue #175): read the
/// head once with `GET /feeds/{owner}/{topic}?exact=true`, as
/// [`settle_feed_position`] does, but for its index only. The entry
/// was written ahead of the update at `saved`, so the head is either
/// that update (it landed before the kill) or the one before it (it
/// didn't); the run writes after whichever it is. The head playlist is
/// not read: it is this broadcast's own, and continuing from it would
/// number the broadcast on from itself and glue its window in front of
/// its own recording (what Solar Punk's recovered sessions avoid by
/// keeping their offset and inherited timeline).
///
/// `Err` is a read that didn't answer, retried as for a new broadcast.
async fn resume_feed_position(ctx: &RunCtx, saved: u64) -> Result<Resumed, String> {
    let path = format!(
        "{}/feeds/{}/{}?exact=true",
        ctx.target.prefix,
        hex::encode(ctx.owner),
        hex::encode(ctx.topic),
    );
    let response = http_get_timed(&ctx.target, &path).await?;
    let next_index = match response.status {
        404 if is_no_update_found(&response.body) => 0,
        200 => feed_index_of(&response)?
            .checked_add(1)
            .ok_or("the feed is full")?,
        status => {
            return Err(format!(
                "gateway returned {status} {}",
                String::from_utf8_lossy(&response.body).trim(),
            ))
        }
    };
    if next_index != saved && next_index != saved + 1 {
        let head = next_index
            .checked_sub(1)
            .map_or_else(|| "empty".to_string(), |h| format!("at index {h}"));
        return Ok(Resumed::Refused(format!(
            "the feed's head is {head}, but the broadcast stopped writing at index {saved}: \
             the channel moved on since, so continuing would overwrite it"
        )));
    }
    let mut state = lock(&ctx.state);
    if state.resume_shape == PlaylistShape::Recording && next_index == saved + 1 {
        return Ok(Resumed::Finished);
    }
    state.feed_index = next_index;
    state.feed_settled = true;
    Ok(Resumed::Settled)
}

/// A head body read to EOF is only whole if it declared its length
/// ([`parse_response`] has already checked the two agree): a playlist
/// cut short by a later chunk that did not retrieve still parses, as a
/// shorter one, and numbering after it would reuse the cut entries'
/// media sequence numbers and drop them from the recording. Bee and
/// ant's gateway both declare one on every `/feeds` and `/bzz` body.
fn require_declared_length(response: &HttpResponse, what: &str) -> Result<(), String> {
    match response.header("content-length") {
        Some(_) => Ok(()),
        None => Err(format!(
            "{what} declares no content-length; a partial body can't be told from a whole one",
        )),
    }
}

/// Bee's (and ant's) answer for a feed nothing was ever written to:
/// `404 {"message":"no update found"}`. Every other 404 on `GET /feeds`
/// is a head that exists but would not retrieve. Ant's node only gives
/// it for an anchor miss the network corroborated; one or two cold
/// peers' miss is a 503 (`ant_p2p::feed_lookup_ack`), retried like any
/// other failed read.
fn is_no_update_found(body: &[u8]) -> bool {
    gateway_message(body).is_some_and(|m| m == "no update found")
}

/// A `/bzz/<v1 head>/stream.m3u8` answer that says the head is not a
/// broadcast, as opposed to one that did not retrieve: the manifest has
/// no such path (bee's and ant's `404 "path address not found"`), or the
/// reference is not a manifest at all. A plain `404 "Not Found"` is a
/// chunk the network didn't serve, not one of these.
fn is_not_a_broadcast_manifest(body: &[u8]) -> bool {
    gateway_message(body).is_some_and(|m| {
        m == "path address not found" || m.contains("reference is not a mantaray manifest")
    })
}

/// The `message` of a gateway's JSON error body.
fn gateway_message(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()?
        .get("message")?
        .as_str()
        .map(str::to_owned)
}

/// [`http_get`] under the same deadline as an upload.
async fn http_get_timed(target: &Target, path: &str) -> Result<HttpResponse, String> {
    tokio::time::timeout(PUBLISH_TIMEOUT, http_get(target, path))
        .await
        .map_err(|_| format!("timed out after {}s", PUBLISH_TIMEOUT.as_secs()))?
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
    let update_started = Instant::now();
    let (playlist_ref, root) = match upload_playlist(ctx, &body, shape).await {
        Ok(uploaded) => {
            lock(&ctx.state)
                .stages
                .playlist
                .record(ms(update_started.elapsed()));
            uploaded
        }
        Err(error) => {
            let what = if shape == PlaylistShape::Recording {
                "recording"
            } else {
                "playlist"
            };
            lock(&ctx.state).record_upload_error(&error, format!("{what}: {error}"));
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
    let (index, recovery) = {
        let mut state = lock(&ctx.state);
        if state.feed_sealed {
            let index = state.feed_index;
            state.record_error(format!(
                "feed update {index} skipped: stop returned before it, and \
                 its report gave {index} as where the next broadcast starts"
            ));
            return;
        }
        state.feed_update_in_flight = true;
        let index = state.feed_index;
        // Taken with the index, from the state the playlist was rendered
        // from (only this task folds entries in), so it lists everything
        // the update can make visible.
        let recovery = (!ctx.config.recovery_path.is_empty())
            .then(|| state.recovery_entry(&ctx.identity(), index, shape));
        (index, recovery)
    };
    // On disk before the update can land (issue #175): a kill after it
    // went out leaves an entry holding every entry a viewer could have
    // seen, and naming the index it took.
    let saving = Instant::now();
    let saved = recovery.is_some();
    ctx.save_recovery_entry(recovery).await;
    let soc_started = Instant::now();
    if saved {
        lock(&ctx.state)
            .stages
            .recovery
            .record(ms(soc_started - saving));
    }
    let result = publish_feed_update(ctx, index, &root).await;
    {
        let mut state = lock(&ctx.state);
        state.feed_update_in_flight = false;
        match result {
            Ok(()) => {
                state.stages.feed.record(ms(soc_started.elapsed()));
                state.stages.update.record(ms(update_started.elapsed()));
                state.feed_index += 1;
                state.feed_updates += 1;
                if shape == PlaylistShape::Recording {
                    // The broadcast is over on the feed. Its recovery
                    // entry is removed by `drive`, after the closing
                    // catalog entry, so a relaunch can still close a
                    // catalog left saying `live`; kept on any failure up
                    // to here, so a relaunch can still finalize it.
                    state.recording_landed = true;
                }
                if let Some(captured_at) = captured_at {
                    state.note_lag(captured_at);
                }
                if state.feed_updates == 1 {
                    // The channel is this broadcast's now: the catalog
                    // can say it is live.
                    ctx.catalog_notify.notify_one();
                }
            }
            Err(error) => {
                state.record_upload_error(&error, format!("feed update {index}: {error}"));
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
    shape: PlaylistShape,
) -> Result<(String, ([u8; 32], Vec<u8>)), UploadError> {
    // A live window is superseded by the next one seconds later, and the
    // feed update that names it waits on its upload: answered once
    // stored, like the segments (see `stored_ack_headers`). The
    // recording is the broadcast's lasting copy, so its playlist's own
    // chunks wait for placement. The segments it lists were answered at
    // stored: a deep-storer hunt one of them started may still be running
    // in the node's background when this returns, and ends with the node
    // (an app suspended or shut down right after stop), leaving that
    // chunk on its shallow storer only.
    let extra: &[(String, String)] = if shape == PlaylistShape::Recording {
        &[]
    } else {
        &stored_ack_headers()
    };
    match ctx.config.format {
        StreamFormat::Bzz => {
            let playlist_ref = publish_bzz_with(
                &ctx.target,
                ctx.batch,
                PLAYLIST_NAME,
                "application/vnd.apple.mpegurl",
                body.as_bytes(),
                extra,
            )
            .await?;
            let reference = parse_hex32(&playlist_ref).map_err(|e| format!("reference {e}"))?;
            let wrapped = v1_update_chunk(&reference)?;
            Ok((playlist_ref, wrapped))
        }
        StreamFormat::SolarPunk => upload_v2_payload(ctx, body, extra).await,
    }
}

/// `POST /bytes` `body` and rebuild its root chunk, for a v2 feed update
/// wrapping it (bee-js `FeedWriter.uploadPayload`): the
/// [`StreamFormat::SolarPunk`] playlists and the master playlist.
async fn upload_v2_payload(
    ctx: &RunCtx,
    body: &str,
    extra_headers: &[(String, String)],
) -> Result<(String, ([u8; 32], Vec<u8>)), UploadError> {
    upload_v2_payload_to(&ctx.target, ctx.batch, body, extra_headers).await
}

/// [`upload_v2_payload`] to any gateway and batch: also the catalog
/// feed's payload ([`crate::catalog::CatalogWriter`]).
pub(crate) async fn upload_v2_payload_to(
    target: &Target,
    batch: [u8; 32],
    body: &str,
    extra_headers: &[(String, String)],
) -> Result<(String, ([u8; 32], Vec<u8>)), UploadError> {
    // The level is sent rather than left to the gateway's default so the
    // root chunk rebuilt below is the one the gateway stored. It is that
    // default (bee's MEDIUM), which is also what bee-js `uploadPayload`
    // gets for a playlist too big for one chunk.
    let level = PLAYLIST_REDUNDANCY_LEVEL.to_string();
    let mut headers = vec![("swarm-redundancy-level".to_string(), level)];
    headers.extend_from_slice(extra_headers);
    let reference = publish_bytes(target, batch, body.as_bytes(), &headers).await?;
    let root = playlist_root_chunk(body.as_bytes(), &reference)?;
    Ok((reference, root))
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
        Err(error) => state.record_upload_error(&error, format!("channel manifest: {error}")),
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
) -> Result<(), UploadError> {
    publish_soc(
        ctx,
        ant_retrieval::sequence_update_id(&ctx.topic, index),
        wrapped,
    )
    .await
}

/// Write one single-owner chunk, owned by the broadcast's signer, at
/// identifier `id`, wrapping the content-addressed chunk `wrapped`
/// (`(address, span ‖ payload)`): bee-js `SOCWriter.upload`'s shape,
/// which every feed update, master and ladder marker here takes.
async fn publish_soc(
    ctx: &RunCtx,
    id: [u8; 32],
    wrapped: &([u8; 32], Vec<u8>),
) -> Result<(), UploadError> {
    publish_soc_as(
        &ctx.target,
        ctx.batch,
        &ctx.signing_secret,
        &ctx.owner,
        id,
        wrapped,
    )
    .await
}

/// [`publish_soc`] for any signer, gateway and batch: also the catalog
/// feed's updates ([`crate::catalog::CatalogWriter`]).
pub(crate) async fn publish_soc_as(
    target: &Target,
    batch: [u8; 32],
    signing_secret: &[u8; 32],
    owner: &[u8; 20],
    id: [u8; 32],
    wrapped: &([u8; 32], Vec<u8>),
) -> Result<(), UploadError> {
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
        ant_crypto::sign_handshake_data(signing_secret, &ant_crypto::keccak256(&digest_input))
            .map_err(|e| format!("sign feed update: {e}"))?;

    let path = format!(
        "{}/soc/{}/{}?sig={}",
        target.prefix,
        hex::encode(owner),
        hex::encode(id),
        hex::encode(signature),
    );
    let headers = [
        (
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        ),
        ("swarm-postage-batch-id".to_string(), hex::encode(batch)),
    ];
    post_reference(target, &path, &headers, cac_wire).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The account's catalog feed (issue #220)
// ---------------------------------------------------------------------------

/// Keep the channel's catalog entry current while live
/// ([`PublisherConfig::info`]): `live` once the first feed update landed,
/// again at once when the info changes, and again when anything else in
/// the entry changed, at most once every [`CATALOG_REFRESH`]. Nothing
/// while paused or without info. Returns its writer at the stop, for
/// [`close_catalog`], once any write it has on the wire is done.
async fn catalog_loop(ctx: Arc<RunCtx>) -> CatalogWriter {
    let mut writer = CatalogWriter {
        target: ctx.target.clone(),
        batch: ctx.batch,
        signing_secret: ctx.signing_secret,
        owner: ctx.owner,
        next_index: None,
    };
    let mut last_attempt: Option<Instant> = None;
    loop {
        // Registered before the state is read, so a wake in between is
        // not lost.
        let woken = ctx.catalog_notify.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        if ctx.cancelled() {
            return writer;
        }
        let due = {
            let mut state = lock(&ctx.state);
            let refresh_due = last_attempt.is_none_or(|at| at.elapsed() >= CATALOG_REFRESH);
            let entry = (!state.paused && state.feed_updates > 0)
                .then(|| live_entry(&ctx, &state))
                .flatten();
            entry.filter(|entry| {
                let changed = state
                    .catalog_last
                    .as_ref()
                    .is_none_or(|last| !last.same_apart_from_timestamp(entry));
                let now = state.catalog_info_changed || (changed && refresh_due);
                if now {
                    state.catalog_info_changed = false;
                }
                now
            })
        };
        if let Some(entry) = due {
            last_attempt = Some(Instant::now());
            write_catalog(&ctx, &mut writer, entry).await;
            continue;
        }
        let wait = last_attempt.map_or(CATALOG_REFRESH, |at| {
            CATALOG_REFRESH.saturating_sub(at.elapsed())
        });
        tokio::select! {
            () = &mut woken => {}
            () = tokio::time::sleep(wait.max(Duration::from_secs(1))) => {}
        }
    }
}

/// The run's entry in `state` (`None` without info), live: the newest
/// landed feed index and this broadcast's media so far.
fn live_entry(ctx: &RunCtx, state: &PublisherState) -> Option<CatalogEntry> {
    let info = state.info.as_ref()?;
    Some(CatalogEntry::build(
        info,
        &EntryFacts {
            owner: &ctx.owner,
            topic_text: &ctx.topic_text,
            channel: &ctx.config.channel,
            state: CatalogState::Live,
            timestamp_ms: unix_ms_now(),
            index: state.feed_index.checked_sub(1),
            duration_ms: Some(state.own_media_ms()),
            rung: Some(rung_of(ctx, state)),
        },
    ))
}

/// The entry's rung: the configured size and the measured bitrate (peak,
/// average), the encoder's target until three segments are measured.
fn rung_of(ctx: &RunCtx, state: &PublisherState) -> (u32, u32, u64, u64) {
    let target = u64::from(ctx.config.bitrate_kbps) * 1000;
    let (peak, average) = state.bitrate.measured().unwrap_or((target, target));
    (ctx.config.width, ctx.config.height, peak, average)
}

/// Write `entry` as the catalog feed's next update. Refused once the feed
/// is sealed (see [`LiveRun::seal_feed`]); in flight from the update
/// itself on, so a stop that seals waits it out. Whether it landed.
async fn write_catalog(ctx: &RunCtx, writer: &mut CatalogWriter, entry: CatalogEntry) -> bool {
    let outcome = async {
        let topic = crate::catalog::catalog_topic();
        let index = match writer.next_index {
            Some(index) => index,
            None => feed_next_index_at(&writer.target, &writer.owner, &topic).await?,
        };
        let (_, root) =
            upload_v2_payload_to(&writer.target, writer.batch, &entry.catalog_json(), &[])
                .await
                .map_err(String::from)?;
        {
            let mut state = lock(&ctx.state);
            if state.feed_sealed {
                return Err(format!(
                    "catalog update {index} skipped: stop returned before it, and the next \
                     broadcast writes the catalog from there"
                ));
            }
            state.catalog_update_in_flight = true;
        }
        let landed = publish_soc_as(
            &writer.target,
            writer.batch,
            &writer.signing_secret,
            &writer.owner,
            ant_retrieval::sequence_update_id(&topic, index),
            &root,
        )
        .await;
        lock(&ctx.state).catalog_update_in_flight = false;
        landed.map_err(String::from)?;
        writer.next_index = Some(index + 1);
        Ok::<_, String>(index)
    }
    .await;
    let mut state = lock(&ctx.state);
    match outcome {
        Ok(index) => {
            state.catalog_writes += 1;
            state.catalog_index = Some(index);
            state.catalog_last = Some(entry);
            true
        }
        Err(message) => {
            state.record_catalog_failure(format!(
                "catalog entry ({}): {message}",
                entry.state.as_str()
            ));
            false
        }
    }
}

/// The closing entry, after the recording: `vod`, naming the newest
/// update that landed (the recording, when it did) and its media, the
/// whole recording with the broadcasts it continues. Nothing when the
/// run has no info or no update of it ever landed (the feed's head is
/// still the last broadcast's, and so is the catalog's entry), unless a
/// resumed run found its recording already the head: a stop before had
/// landed it, but not this entry. Whether the catalog is closed: the
/// entry landed, or none was due.
async fn close_catalog(ctx: &RunCtx, mut writer: CatalogWriter) -> bool {
    let entry = {
        let state = lock(&ctx.state);
        let Some(info) = state.info.as_ref() else {
            return true;
        };
        if state.feed_updates == 0 && !state.recording_landed {
            return true;
        }
        let recorded = state.recording_landed;
        let duration_ms = state.own_media_ms()
            + if recorded {
                state.predecessor.as_ref().map_or(0, Predecessor::media_ms)
            } else {
                0
            };
        CatalogEntry::build(
            info,
            &EntryFacts {
                owner: &ctx.owner,
                topic_text: &ctx.topic_text,
                channel: &ctx.config.channel,
                state: CatalogState::Vod,
                timestamp_ms: unix_ms_now(),
                index: state.feed_index.checked_sub(1),
                duration_ms: Some(duration_ms),
                rung: Some(rung_of(ctx, &state)),
            },
        )
    };
    // A resumed run that found the recording already the head: the `vod`
    // entry may have landed too, and only the entry's removal failed (or
    // the app went in between). Written again only if the head isn't it.
    if lock(&ctx.state).feed_updates == 0 {
        match catalog_already_closed(&writer, &entry).await {
            Ok((true, next)) => {
                writer.next_index = Some(next);
                return true;
            }
            Ok((false, next)) => writer.next_index = Some(next),
            Err(message) => {
                lock(&ctx.state).record_catalog_failure(format!(
                    "catalog entry ({}): head: {message}",
                    entry.state.as_str()
                ));
                return false;
            }
        }
    }
    write_catalog(ctx, &mut writer, entry).await
}

/// Whether the catalog's head already is `closing`'s `vod` entry (same
/// owner, topic and recording index), and the index after that head.
/// A head that can't be read is an error, not "not closed": writing on
/// a guess could put a second `vod` entry after one that landed.
async fn catalog_already_closed(
    writer: &CatalogWriter,
    closing: &CatalogEntry,
) -> Result<(bool, u64), String> {
    let topic = crate::catalog::catalog_topic();
    let path = format!(
        "{}/feeds/{}/{}?exact=true",
        writer.target.prefix,
        hex::encode(writer.owner),
        hex::encode(topic),
    );
    let response = http_get_timed(&writer.target, &path).await?;
    match response.status {
        404 if is_no_update_found(&response.body) => Ok((false, 0)),
        200 => {
            let next = feed_index_of(&response)?
                .checked_add(1)
                .ok_or_else(|| "the feed is full".to_string())?;
            let closed =
                serde_json::from_slice::<Vec<CatalogEntry>>(&response.body).is_ok_and(|entries| {
                    entries.iter().any(|e| {
                        e.state == CatalogState::Vod
                            && e.owner == closing.owner
                            && e.topic == closing.topic
                            && e.index == closing.index
                    })
                });
            Ok((closed, next))
        }
        status => Err(format!(
            "gateway returned {status} {}",
            String::from_utf8_lossy(&response.body).trim(),
        )),
    }
}

// ---------------------------------------------------------------------------
// One-rung ladder: master playlist and time markers (issue #174)
// ---------------------------------------------------------------------------

/// How long after a period boundary its marker is written, Solar Punk's
/// `MARKER_WRITE_DELAY_MS`: a timer firing a little early still lands in
/// the new period, and a viewer early in a period usually finds the
/// marker already there.
const MARKER_WRITE_DELAY_MS: u64 = 250;

/// How long a master write that failed holds off the next attempt
/// (Solar Punk's `MASTER_REWRITE_RETRY_MS`).
const MASTER_RETRY: Duration = Duration::from_secs(30);

/// Write the ladder's time marker once per period (issue #174), from the
/// first landed feed update until the run is cancelled; [`drive`] aborts
/// it after the committer. Shortly after each period boundary of the
/// global ten-second clock ([`marker_period_at`]) it writes one
/// single-owner chunk at [`ladder_marker_identifier`] naming the newest
/// feed index this broadcast has published ([`encode_ladder_marker`]).
///
/// A marker is a shortcut, never a dependency, so everything gives way:
/// none while paused (there is no uplink), none before this run's first
/// update landed (a viewer would be sent to an earlier broadcast's head),
/// a write is abandoned when its own period ends, and a period is never
/// written twice, not even when the wall clock steps back, since a reader
/// that already read an address keeps its answer. A failure is counted
/// in `ladder_failures` and shown as `ladder_error`, never in `errors`.
async fn marker_loop(ctx: Arc<RunCtx>) {
    let group = ctx.group_topic();
    let mut last_period: Option<u64> = None;
    loop {
        let now = ctx.wall_ms();
        let boundary = (marker_period_at(now) + 1) * LADDER_MARKER_PERIOD_MS;
        tokio::time::sleep(Duration::from_millis(
            boundary + MARKER_WRITE_DELAY_MS - now,
        ))
        .await;
        if ctx.cancelled() {
            return;
        }
        let written_at = ctx.wall_ms();
        let period = marker_period_at(written_at);
        if last_period.is_some_and(|last| period <= last) {
            continue;
        }
        let index = {
            let state = lock(&ctx.state);
            if state.paused || state.feed_updates == 0 {
                continue;
            }
            state.feed_index - 1
        };
        last_period = Some(period);
        let deadline = (period + 1) * LADDER_MARKER_PERIOD_MS;
        let writing = Instant::now();
        let outcome = write_marker(&ctx, &group, period, written_at, index, deadline).await;
        let mut state = lock(&ctx.state);
        match outcome {
            Ok(()) => {
                state.markers_written += 1;
                state.stages.marker.record(ms(writing.elapsed()));
            }
            Err(message) => state.record_ladder_failure(format!(
                "ladder marker for period {period}: {message}; viewers search \
                 the feed instead until one lands"
            )),
        }
    }
}

/// One marker, abandoned at `deadline` (Unix ms, the end of its period).
async fn write_marker(
    ctx: &RunCtx,
    group: &[u8; 32],
    period: u64,
    written_at: u64,
    index: u64,
    deadline: u64,
) -> Result<(), String> {
    let payload =
        encode_ladder_marker(period, written_at, &ctx.topic, index, ctx.config.segment_ms)?;
    let wrapped = ant_crypto::cac_new(&payload).ok_or("marker payload too large")?;
    let id = ladder_marker_identifier(group, period);
    let remaining = Duration::from_millis(deadline.saturating_sub(ctx.wall_ms()));
    tokio::time::timeout(remaining, publish_soc(ctx, id, &wrapped))
        .await
        .map_err(|_| "its period ended before the write finished".to_string())?
        .map_err(String::from)
}

/// Write the one-rung master playlist ([`render_master_playlist`], the
/// bitrate measured so far) as the group feed's next v2 update, the way
/// Solar Punk's `MasterFeedWriter` publishes a ladder's master on the
/// feed whose topic is the group. The first write of a run reads the
/// group feed's head (`?exact=true`, as [`settle_feed_position`] reads
/// the channel's) and writes after it: an earlier broadcast's master is
/// never overwritten, and a failed read writes nothing.
async fn publish_master(ctx: &RunCtx) {
    let outcome = async {
        let (body, peak) = {
            let state = lock(&ctx.state);
            let (peak, average) = state
                .bitrate
                .measured()
                .ok_or("the bitrate is not measured yet")?;
            let body = render_master_playlist(
                &ctx.owner,
                &ctx.topic_text,
                peak,
                average,
                ctx.config.width,
                ctx.config.height,
            );
            (body, peak)
        };
        let group = ctx.group_topic();
        let known = lock(&ctx.state).master_next_index;
        let index = match known {
            Some(index) => index,
            None => feed_next_index(ctx, &group).await?,
        };
        let (reference, root) = upload_v2_payload(ctx, &body, &[]).await?;
        publish_soc(ctx, ant_retrieval::sequence_update_id(&group, index), &root).await?;
        Ok::<_, String>((index, peak, reference))
    }
    .await;
    let mut state = lock(&ctx.state);
    match outcome {
        Ok((index, peak, reference)) => {
            state.master_next_index = Some(index + 1);
            state.master_advertised_bps = Some(peak);
            state.master_reference = Some(reference);
            state.master_retry_at = None;
        }
        Err(message) => {
            state.master_retry_at = Some(Instant::now() + MASTER_RETRY);
            state.record_ladder_failure(format!("master playlist: {message}"));
        }
    }
}

/// The index after the head of the feed `topic` (this broadcast's owner),
/// read with `GET /feeds/{owner}/{topic}?exact=true`: 0 for a feed
/// nothing was written to (`404 "no update found"`), an error for any
/// answer that isn't a head or that confirmed miss.
async fn feed_next_index(ctx: &RunCtx, topic: &[u8; 32]) -> Result<u64, String> {
    feed_next_index_at(&ctx.target, &ctx.owner, topic).await
}

/// [`feed_next_index`] of any owner's feed on any gateway: also the
/// catalog feed's ([`crate::catalog::CatalogWriter`]).
pub(crate) async fn feed_next_index_at(
    target: &Target,
    owner: &[u8; 20],
    topic: &[u8; 32],
) -> Result<u64, String> {
    let path = format!(
        "{}/feeds/{}/{}?exact=true",
        target.prefix,
        hex::encode(owner),
        hex::encode(topic),
    );
    let response = http_get_timed(target, &path).await?;
    match response.status {
        404 if is_no_update_found(&response.body) => Ok(0),
        200 => feed_index_of(&response)?
            .checked_add(1)
            .ok_or_else(|| "the feed is full".to_string()),
        status => Err(format!(
            "gateway returned {status} {}",
            String::from_utf8_lossy(&response.body).trim(),
        )),
    }
}

/// A feed read's `swarm-feed-index`: 16 hex digits, big-endian.
fn feed_index_of(response: &HttpResponse) -> Result<u64, String> {
    response
        .header("swarm-feed-index")
        .and_then(|v| hex::decode(v).ok())
        .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| "the head carries no swarm-feed-index".to_string())
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
) -> Result<String, UploadError> {
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
) -> Result<String, UploadError> {
    publish_bzz_with(target, batch, name, content_type, payload, &[]).await
}

/// [`publish_bzz`] with `extra_headers` on the request.
async fn publish_bzz_with(
    target: &Target,
    batch: [u8; 32],
    name: &str,
    content_type: &str,
    payload: &[u8],
    extra_headers: &[(String, String)],
) -> Result<String, UploadError> {
    let path = format!("{}/bzz?name={name}", target.prefix);
    let mut headers = vec![
        ("content-type".to_string(), content_type.to_string()),
        ("swarm-postage-batch-id".to_string(), hex::encode(batch)),
    ];
    headers.extend_from_slice(extra_headers);
    post_reference(target, &path, &headers, payload).await
}

/// What a segment's and a live window's upload carry on top of the
/// format's own headers: `ant-ack-when-stored: true` (issue #212), so
/// ant's gateway answers once every chunk is stored, its first receipt,
/// and finishes the hunt for a deep storer that a shallow receipt
/// starts in the background. A segment's playlist entry waits for every
/// segment before it, and a feed update for its playlist's upload, so
/// without this one chunk hunting a deep storer (12 shallow receipts at
/// most, a 2.5 s wait after each from the 2nd to the 11th) held back
/// the feed for everything behind it: the 10-30 s pauses of #212. The
/// hunt itself is unchanged. The feed update (`POST /soc`), the channel
/// manifest, the ladder's writes and the recording go without it, and
/// answer once placed. Other gateways (bee) ignore the header.
fn stored_ack_headers() -> [(String, String); 1] {
    [("ant-ack-when-stored".to_string(), "true".to_string())]
}

/// Why the gateway refused an upload, when the refusal is the postage
/// batch's and not the network's (issue #176). Retrying does not help
/// with either: a host has to buy or extend storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchRefusal {
    /// `402`, bee's `"batch is overissued"`: the collision bucket the
    /// chunk falls in is full on an immutable batch, so the stamp is
    /// refused. A batch fills unevenly, so these start as the odd
    /// refused segment and become every segment within minutes.
    Full,
    /// The batch itself is refused: `400` naming it (not usable here:
    /// unknown to the node, expired, or not bought), or `422` (storer
    /// peers attest it is not on chain).
    Unusable,
}

impl BatchRefusal {
    /// Read the refusal off a gateway's answer to an upload. Keyed on the
    /// status, the one thing every bee-shaped gateway agrees on
    /// (`ant_gateway::upload_error_status` maps every write endpoint the
    /// same way): `402` is a full bucket, `422` a batch the network says
    /// is not on chain. A `400`/`404` only counts when its message names
    /// the batch, since those statuses also mean a malformed request;
    /// bee and ant both answer an unusable batch with a message that
    /// does (`"batch not usable"`, `"batch 0x… not usable"`). Any other
    /// status (a `502` pushsync failure, a `503`, a timeout) is the
    /// network's.
    fn of_response(status: u16, body: &[u8]) -> Option<Self> {
        match status {
            402 => Some(Self::Full),
            422 => Some(Self::Unusable),
            400 | 404
                if gateway_message(body)
                    .is_some_and(|m| m.to_ascii_lowercase().contains("batch")) =>
            {
                Some(Self::Unusable)
            }
            _ => None,
        }
    }
}

/// A failed upload: what to report, and whether the batch refused it
/// (see [`BatchRefusal`]). Converts to and from a plain message so the
/// helpers that only report it keep their `Result<_, String>`.
#[derive(Debug, Clone)]
pub(crate) struct UploadError {
    message: String,
    batch: Option<BatchRefusal>,
}

impl std::fmt::Display for UploadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<String> for UploadError {
    fn from(message: String) -> Self {
        Self {
            message,
            batch: None,
        }
    }
}

impl From<UploadError> for String {
    fn from(error: UploadError) -> Self {
        error.message
    }
}

/// `POST` something the gateway answers with bee's
/// `{"reference":"<hex>"}` and hand back that reference.
async fn post_reference(
    target: &Target,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> Result<String, UploadError> {
    let response = tokio::time::timeout(PUBLISH_TIMEOUT, http_post(target, path, headers, body))
        .await
        .map_err(|_| format!("timed out after {}s", PUBLISH_TIMEOUT.as_secs()))??;
    if response.status != 201 {
        return Err(UploadError {
            message: format!(
                "gateway returned {} {}",
                response.status,
                String::from_utf8_lossy(&response.body).trim(),
            ),
            batch: BatchRefusal::of_response(response.status, &response.body),
        });
    }
    let parsed: serde_json::Value = serde_json::from_slice(&response.body)
        .map_err(|e| format!("gateway response is not JSON: {e}"))?;
    Ok(parsed
        .get("reference")
        .and_then(serde_json::Value::as_str)
        .map(|r| r.trim_start_matches("0x").to_ascii_lowercase())
        .ok_or_else(|| "gateway response carries no reference".to_string())?)
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
    /// Header names lower-cased, values trimmed.
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Vec<u8>,
}

impl HttpResponse {
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// `GET` one resource. HTTP/1.0, so the gateway answers with the body to
/// EOF rather than chunked (which [`parse_response`] doesn't decode).
/// A body cut short by a mid-stream failure also ends at EOF;
/// [`parse_response`] rejects it against its `Content-Length`, and a
/// caller that must not act on a partial body ([`settle_feed_position`])
/// also refuses one that declares no length.
pub(crate) async fn http_get(target: &Target, path: &str) -> Result<HttpResponse, String> {
    let mut stream = TcpStream::connect(&target.authority)
        .await
        .map_err(|e| format!("connect {}: {e}", target.authority))?;
    let _ = stream.set_nodelay(true);
    let head = format!("GET {path} HTTP/1.0\r\nhost: {}\r\n\r\n", target.authority);
    stream
        .write_all(head.as_bytes())
        .await
        .map_err(|e| format!("write request head: {e}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .await
        .map_err(|e| format!("read response: {e}"))?;
    parse_response(&raw)
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
/// can't parse would show up as a non-201 status anyway. A body shorter
/// (or longer) than its `Content-Length` is an error, not a response:
/// the gateway sent the head and then failed mid-stream.
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
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    let body = &raw[split + 4..];
    // The body runs to EOF, and EOF is also what a gateway that fails
    // mid-stream (a later chunk of a multi-chunk body not retrieving
    // after the first was sent) leaves behind: only the declared length
    // tells a whole body from a cut one.
    if let Some(declared) = headers
        .iter()
        .find(|(n, _)| n == "content-length")
        .map(|(_, v)| v)
    {
        let declared: usize = declared
            .parse()
            .map_err(|_| format!("malformed content-length: `{declared}`"))?;
        if body.len() != declared {
            return Err(format!(
                "truncated response: content-length {declared}, got {} bytes",
                body.len(),
            ));
        }
    }
    Ok(HttpResponse {
        status,
        headers,
        body: body.to_vec(),
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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

    #[test]
    fn response_parser_rejects_a_body_cut_short() {
        // A 200 whose body stream failed after the head went out: the
        // first leaf arrived, the rest never did.
        let raw = b"HTTP/1.1 200 OK\r\ncontent-length: 8192\r\n\r\n#EXTM3U\n";
        let err = parse_response(raw)
            .err()
            .expect("a truncated body is no response");
        assert!(err.contains("truncated"), "{err}");
        assert!(parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: x\r\n\r\n").is_err());
        // No declared length: parsed, but the head read refuses it.
        let r = parse_response(b"HTTP/1.0 200 OK\r\n\r\n#EXTM3U\n").unwrap();
        assert!(require_declared_length(&r, "the head").is_err());
        let r = parse_response(b"HTTP/1.1 200 OK\r\ncontent-length: 8\r\n\r\n#EXTM3U\n").unwrap();
        assert!(require_declared_length(&r, "the head").is_ok());
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
            start_index: None,
            reap_ms: 0,
            ladder: false,
            width: 0,
            height: 0,
            recovery_path: String::new(),
            resume: false,
            info: None,
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
            bytes: 1000,
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
            bytes: 1000,
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
            bytes: 1000,
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
                bytes: 1000,
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
                bytes: 1000,
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
    fn an_omitted_topic_is_the_channels_stable_text_topic() {
        let mut c = config();
        c.topic = String::new();
        let (a, a_text) = c.resolve_topic().unwrap();
        // Issue #173: a text topic named after the channel, so a catalog
        // entry can name it and every broadcast of the channel writes
        // the same feed: a shared channel link outlives one broadcast.
        assert_eq!(a_text, "antstream/test channel");
        assert_eq!(a, topic_from_string(&a_text));
        assert_eq!(c.resolve_topic().unwrap(), (a, a_text));
        // Another channel is another feed.
        c.channel = "other".into();
        assert_ne!(c.resolve_topic().unwrap().0, a);
        // An explicit topic is honoured verbatim, with or without `0x`.
        c.topic = "cd".repeat(32);
        assert_eq!(c.resolve_topic().unwrap(), ([0xcd; 32], String::new()));
        c.topic = "0xnothex".into();
        assert!(c.resolve_topic().is_err());
    }

    #[test]
    fn start_index_is_an_override_left_out_by_default() {
        let parse =
            |json: serde_json::Value| -> PublisherConfig { serde_json::from_value(json).unwrap() };
        let base = serde_json::json!({ "channel": "c", "batch_id": "ab".repeat(32) });
        assert_eq!(parse(base.clone()).start_index, None);
        let mut set = base;
        set["start_index"] = 0.into();
        assert_eq!(
            parse(set).start_index,
            Some(0),
            "an explicit 0 is an override too"
        );
    }

    // -----------------------------------------------------------------
    // Continuing the channel (issue #173)
    // -----------------------------------------------------------------

    /// A previous broadcast's recording, as this publisher writes one: a
    /// map, then entries numbered from 12, a gap and a writer restart.
    const PRIOR_RECORDING: &str = "#EXTM3U\n#EXT-X-VERSION:7\n#ANTSTREAM-BROADCAST:aa\n\
        #EXT-X-TARGETDURATION:3\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-MEDIA-SEQUENCE:12\n\
        #EXT-X-MAP:URI=\"init-a\"\n\
        #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n#EXTINF:2.000,\nseg-12\n\
        #EXT-X-GAP\n#EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:02.000Z\n#EXTINF:2.000,\ngap-13\n\
        #EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"init-b\"\n\
        #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:10.000Z\n#EXTINF:2.500,\nseg-14\n\
        #EXT-X-ENDLIST\n";

    #[test]
    fn the_head_playlist_is_continued_from_where_its_numbering_ends() {
        let prior = Predecessor::parse(PRIOR_RECORDING).expect("a playlist");
        // Solar Punk's `continuesFrom`: the media sequence plus every
        // entry, the gap included.
        assert_eq!(prior.next_number, 15);
        assert_eq!(prior.media_sequence, 12);
        assert_eq!(prior.discontinuity_sequence, 0);
        assert_eq!(prior.target_duration_s, 3);
        // The timeline verbatim from its first tag (the map, since these
        // are fMP4) to before #EXT-X-ENDLIST; no header lines.
        assert_eq!(prior.lines.first().unwrap(), "#EXT-X-MAP:URI=\"init-a\"");
        assert_eq!(prior.lines.last().unwrap(), "seg-14");
        assert!(!prior
            .lines
            .iter()
            .any(|l| l.starts_with("#EXT-X-MEDIA-SEQUENCE")
                || l.starts_with("#ANTSTREAM-BROADCAST")
                || l == "#EXT-X-ENDLIST"));
        assert_eq!(prior.lines.len(), 13);

        // A killed broadcast's last live window: no #EXT-X-ENDLIST, and
        // its header says how many breaks had already slid out.
        let live = "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n\
            #EXT-X-MEDIA-SEQUENCE:500\n#EXT-X-DISCONTINUITY-SEQUENCE:4\n\
            #EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"i\"\n\
            #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:00.000Z\n#EXTINF:2.000,\ns500\n\
            #EXT-X-PROGRAM-DATE-TIME:2026-09-01T12:00:02.000Z\n#EXTINF:2.000,\ns501\n";
        let prior = Predecessor::parse(live).unwrap();
        assert_eq!(prior.next_number, 502);
        assert_eq!(prior.discontinuity_sequence, 4);
        assert_eq!(
            prior.lines[0], "#EXT-X-DISCONTINUITY",
            "the timeline opens at the break"
        );

        // Nothing to continue from: not a playlist, no numbering, or no
        // entries.
        assert_eq!(Predecessor::parse("hello"), None);
        assert_eq!(Predecessor::parse(""), None);
        assert_eq!(
            Predecessor::parse("#EXTM3U\n#EXT-X-TARGETDURATION:2\n#EXTINF:2.000,\na\n"),
            None
        );
        assert_eq!(
            Predecessor::parse("#EXTM3U\n#EXT-X-MEDIA-SEQUENCE:3\n#EXT-X-ENDLIST\n"),
            None
        );
    }

    #[test]
    fn a_continued_broadcast_numbers_on_with_one_seam_and_glues_its_recording() {
        let mut state = live_state();
        state.continue_from(Predecessor::parse(PRIOR_RECORDING).unwrap());
        // A segment lost before anything was listed: numbered, never the
        // carrier of the seam, and never listed in front of it.
        state.done.insert(1, lost(false, 1_788_264_100_000));
        for seq in 2..6 {
            state
                .done
                .insert(seq, media(&format!("/bzz/s{seq}/seg.m4s"), false));
        }
        state.advance(3, 2000);
        let live = state.render_playlist(2000, false);
        // The lost segment in front gave its number back, so the first
        // segment is 15. The window slid past it (window 3 of 4
        // segments), so the seam counts: the predecessor's own break,
        // plus the seam.
        assert!(live.contains("\n#EXT-X-MEDIA-SEQUENCE:16\n"), "{live}");
        assert!(
            live.contains("\n#EXT-X-DISCONTINUITY-SEQUENCE:2\n"),
            "{live}"
        );
        assert!(!live.contains("\n#EXT-X-DISCONTINUITY\n"), "{live}");

        // Before it slid: the first live window opens on the seam, at the
        // number after the predecessor's last entry and its gap.
        let mut fresh = live_state();
        fresh.continue_from(Predecessor::parse(PRIOR_RECORDING).unwrap());
        fresh.done.insert(1, media("/bzz/first/seg.m4s", false));
        fresh.advance(3, 2000);
        let first = fresh.render_playlist(2000, false);
        assert!(first.contains("\n#EXT-X-MEDIA-SEQUENCE:15\n"), "{first}");
        assert!(
            first.contains("\n#EXT-X-DISCONTINUITY-SEQUENCE:1\n"),
            "{first}"
        );
        assert_eq!(
            first.matches("#EXT-X-DISCONTINUITY\n").count(),
            1,
            "{first}"
        );
        let seam = first.find("#EXT-X-DISCONTINUITY\n").unwrap();
        assert!(
            seam < first.find("#EXT-X-MAP").unwrap(),
            "the seam opens the window:\n{first}"
        );

        // The recording: the predecessor's numbering and entries
        // verbatim, one seam, this broadcast's own map restated, then its
        // own entries; the longer target duration of the two.
        let recording = state.render_recording(2000);
        assert!(
            recording.contains("\n#EXT-X-MEDIA-SEQUENCE:12\n"),
            "{recording}"
        );
        assert!(
            !recording.contains("#EXT-X-DISCONTINUITY-SEQUENCE"),
            "{recording}"
        );
        assert!(
            recording.contains("\n#EXT-X-TARGETDURATION:3\n"),
            "{recording}"
        );
        let prior_body = &PRIOR_RECORDING[PRIOR_RECORDING.find("#EXT-X-MAP").unwrap()
            ..PRIOR_RECORDING.find("#EXT-X-ENDLIST").unwrap()];
        let glued = format!(
            "{prior_body}#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"{}\"\n",
            bare_ref(u64::MAX)
        );
        assert!(recording.contains(&glued), "{recording}");
        assert_eq!(
            recording.matches("#EXT-X-DISCONTINUITY\n").count(),
            2,
            "{recording}"
        );
        assert_eq!(
            entry_uris(&recording),
            [
                "seg-12",
                "gap-13",
                "seg-14",
                "/bzz/s2/seg.m4s",
                "/bzz/s3/seg.m4s",
                "/bzz/s4/seg.m4s",
                "/bzz/s5/seg.m4s"
            ],
        );
        // …and the numbering reads straight through: a third broadcast
        // continues after this one's last entry.
        let next = Predecessor::parse(&recording).unwrap();
        assert_eq!(next.next_number, 12 + 7, "{recording}");
        assert_eq!(next.next_number, state.next_number);
        assert!(recording.ends_with("#EXT-X-ENDLIST\n"));
    }

    #[test]
    fn a_fresh_channel_renders_as_before() {
        let mut state = live_state();
        state.done.insert(1, media("/bzz/a/seg.m4s", false));
        state.advance(3, 2000);
        let live = state.render_playlist(2000, false);
        assert!(live.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"), "{live}");
        assert!(!live.contains("DISCONTINUITY"), "{live}");
        let recording = state.render_recording(2000);
        assert!(
            recording.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"),
            "{recording}"
        );
        assert!(!recording.contains("DISCONTINUITY"), "{recording}");
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
        /// What a refused `POST` answers, `(status line, body)`. `None`
        /// is `400 {"message":"batch not usable"}`, an unusable batch.
        fail_answer: Option<(&'static str, &'static str)>,
        /// Refuse every feed update from now on, the segments
        /// themselves still uploading fine.
        fail_soc: bool,
        /// Refuse this many `POST /feeds` before accepting one.
        fail_feeds: u32,
        /// Refuse the ladder's writes only: a marker's `POST /soc` and
        /// the master playlist's `POST /bytes`.
        fail_ladder: bool,
        /// Refuse the catalog's payload uploads only (a `POST /bytes`
        /// whose body is a JSON array).
        fail_catalog: bool,
        /// Extra latency on `POST /feeds` only.
        feeds_delay: Duration,
        /// Extra latency on the upload of a segment whose body starts
        /// with this byte.
        segment_delay: Option<(u8, Duration)>,
        /// Refuse the recording's upload (a playlist marked VOD) only.
        fail_recording: bool,
        /// Extra latency on every request carrying the recording (its
        /// upload and, under "solar-punk", the feed update wrapping it).
        recording_delay: Duration,
        /// What `GET /feeds` answers: `None` is bee's
        /// `404 "no update found"`, an empty feed.
        head: Option<StubHead>,
        /// Heads of other feeds, by topic hex, answered instead of
        /// `head` for a `GET /feeds` naming that topic (the ladder's
        /// group feed).
        feed_heads: HashMap<String, StubHead>,
        /// Answer this many `GET /feeds` with a 502 first.
        fail_head: u32,
        /// Then this many with the node's 503 for an anchor miss only
        /// one or two cold peers answered (`ant_p2p::feed_lookup_ack`).
        cold_head: u32,
        /// Then this many with bee's other 404, a head that exists but
        /// did not retrieve.
        unretrievable_head: u32,
        /// `GET /bzz/<ref>/stream.m3u8` bodies, by manifest reference.
        /// Any other reference is a manifest with no such path.
        bzz_playlists: HashMap<String, String>,
        /// Answer this many `GET /bzz/<ref>/stream.m3u8` first with the
        /// gateway's plain 404 for a chunk the network didn't serve.
        unretrievable_playlist: u32,
        /// Answer this many `200` `GET /feeds` bodies cut off
        /// mid-stream: the full `content-length`, then only the first
        /// half of the body (a later leaf that did not retrieve after
        /// the first was sent).
        truncated_head: u32,
        /// The same for `200` `GET /bzz/<ref>/stream.m3u8` bodies.
        truncated_playlist: u32,
    }

    /// The feed head `GET /feeds` resolves to.
    #[derive(Clone)]
    struct StubHead {
        index: u64,
        /// `"v1"` or `"v2"`.
        version: &'static str,
        /// The `ETag` reference (v1: the playlist's manifest).
        reference: String,
        /// The response body (v2: the playlist itself).
        body: Vec<u8>,
    }

    /// A gateway answer, as raw HTTP.
    fn stub_response(status: &str, headers: &[(&str, String)], body: &[u8]) -> Vec<u8> {
        use std::fmt::Write as _;
        let mut r = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\n", body.len());
        for (name, value) in headers {
            let _ = write!(r, "{name}: {value}\r\n");
        }
        r.push_str("connection: close\r\n\r\n");
        let mut r = r.into_bytes();
        r.extend_from_slice(body);
        r
    }

    /// Answer the reads a publisher makes (`GET /feeds`, `GET /bzz`).
    fn stub_read(log: &Arc<Mutex<StubLog>>, path: &str) -> Vec<u8> {
        let mut response = stub_read_whole(log, path);
        let mut slot = lock(log);
        let cuts = if path.contains("/feeds/") {
            &mut slot.truncated_head
        } else {
            &mut slot.truncated_playlist
        };
        if *cuts > 0 && response.starts_with(b"HTTP/1.1 200") {
            *cuts -= 1;
            let body_at = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            response.truncate(body_at + (response.len() - body_at) / 2);
        }
        response
    }

    fn stub_read_whole(log: &Arc<Mutex<StubLog>>, path: &str) -> Vec<u8> {
        let mut slot = lock(log);
        if path.contains("/feeds/") {
            if slot.fail_head > 0 {
                slot.fail_head -= 1;
                return stub_response(
                    "502 Bad Gateway",
                    &[],
                    br#"{"message":"feed lookup failed"}"#,
                );
            }
            if slot.cold_head > 0 {
                slot.cold_head -= 1;
                return stub_response(
                    "503 Service Unavailable",
                    &[],
                    br#"{"code":503,"message":"feed lookup: feed has no updates (probed 1 indices); too few peers answered to tell an empty feed from one this node can't reach yet"}"#,
                );
            }
            if slot.unretrievable_head > 0 {
                slot.unretrievable_head -= 1;
                return stub_response(
                    "404 Not Found",
                    &[],
                    br#"{"code":404,"message":"wrapped chunk cannot be retrieved"}"#,
                );
            }
            let head = slot
                .feed_heads
                .iter()
                .find(|(topic, _)| path.contains(topic.as_str()))
                .map(|(_, head)| head)
                .or(slot.head.as_ref());
            return match head {
                None => stub_response(
                    "404 Not Found",
                    &[],
                    br#"{"code":404,"message":"no update found"}"#,
                ),
                Some(head) => stub_response(
                    "200 OK",
                    &[
                        ("swarm-feed-index", hex::encode(head.index.to_be_bytes())),
                        (
                            "swarm-feed-index-next",
                            hex::encode((head.index + 1).to_be_bytes()),
                        ),
                        ("swarm-feed-resolved-version", head.version.to_string()),
                        ("etag", format!("\"{}\"", head.reference)),
                    ],
                    &head.body,
                ),
            };
        }
        let reference = path
            .strip_prefix("/bzz/")
            .and_then(|rest| rest.strip_suffix("/stream.m3u8"))
            .unwrap_or_default();
        if slot.unretrievable_playlist > 0 {
            slot.unretrievable_playlist -= 1;
            return stub_response(
                "404 Not Found",
                &[],
                br#"{"code":404,"message":"Not Found"}"#,
            );
        }
        match slot.bzz_playlists.get(reference) {
            Some(body) => stub_response("200 OK", &[], body.as_bytes()),
            None => stub_response(
                "404 Not Found",
                &[],
                br#"{"code":404,"message":"path address not found"}"#,
            ),
        }
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
                                if let Some(path) = request.method_path.strip_prefix("GET ") {
                                    lock(&log).seen.push(request.clone());
                                    tokio::time::sleep(delay).await;
                                    let response = stub_read(&log, path);
                                    let _ = socket.write_all(&response).await;
                                    let _ = socket.shutdown().await;
                                    return;
                                }
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
                                        || (slot.fail_recording && is_recording)
                                        || (slot.fail_catalog
                                            && request.method_path.ends_with("/bytes")
                                            && request.body.starts_with(b"[{"))
                                        || (slot.fail_ladder
                                            && [&b"\"rungs\":"[..], b"#EXT-X-STREAM-INF"]
                                                .iter()
                                                .any(|needle| {
                                                    request
                                                        .body
                                                        .windows(needle.len())
                                                        .any(|w| w == *needle)
                                                }));
                                    (
                                        fail,
                                        if is_feeds {
                                            slot.feeds_delay
                                        } else if is_recording {
                                            slot.recording_delay
                                        } else {
                                            slot.segment_delay
                                                .filter(|(first, _)| {
                                                    is_segment(&request)
                                                        && request.body.first() == Some(first)
                                                })
                                                .map_or(Duration::ZERO, |(_, d)| d)
                                        },
                                    )
                                };
                                tokio::time::sleep(delay + feeds_delay).await;
                                let response = if fail {
                                    let (status, body) = lock(&log).fail_answer.unwrap_or((
                                        "400 Bad Request",
                                        r#"{"message":"batch not usable"}"#,
                                    ));
                                    stub_response(status, &[], body.as_bytes())
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

    /// Issue #212: the report says where the time went. A slow segment
    /// shows as its own upload time and as the head-of-line wait of the
    /// segment behind it, which landed first and waited for it; every
    /// landed feed update has a playlist, a `POST /soc` and a whole-update
    /// time, and no recovery entry is timed when none is kept.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_report_times_each_stage_and_the_head_of_line_wait() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        lock(&log).segment_delay = Some((1, Duration::from_millis(400)));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.start_index = Some(0);
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
        let _ = run.push(SegmentKind::Media, vec![2u8; 4096], 2000, false);
        tokio::time::sleep(Duration::from_millis(600)).await;
        run.cancel();
        settle(&run).await;

        let report = run.report();
        let t = report.timings;
        assert_eq!(t.segment.count, 3, "{t:?}");
        assert!(t.segment.max_ms >= 400, "{t:?}");
        assert_eq!(t.head_of_line.count, 2, "{t:?}");
        assert!(t.head_of_line.max_ms >= 300, "{t:?}");
        assert_eq!(t.feed.count, report.feed_updates, "{t:?}");
        assert_eq!(t.update.count, report.feed_updates, "{t:?}");
        assert!(t.playlist.count >= report.feed_updates, "{t:?}");
        assert!(t.update.max_ms >= t.feed.max_ms, "{t:?}");
        assert_eq!(t.recovery, StageTiming::default(), "{t:?}");
        assert_eq!(t.marker, StageTiming::default(), "{t:?}");
        assert_eq!(run.snapshot().timings, t);
    }

    /// Issue #212: segments (initialization and media) and the live
    /// windows ask the gateway to answer once their chunks are stored, in
    /// either format; the recording, the feed updates and the channel
    /// manifest don't, and answer once placed, as before.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn segments_and_live_windows_ask_for_the_stored_answer() {
        for format in [StreamFormat::SolarPunk, StreamFormat::Bzz] {
            let log = Arc::new(Mutex::new(StubLog::default()));
            let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
            let mut config = config();
            config.gateway = format!("http://{addr}");
            config.start_index = Some(0);
            config.format = format;
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
            tokio::time::sleep(Duration::from_millis(100)).await;
            run.cancel();
            settle(&run).await;

            let seen = lock(&log).seen.clone();
            let mut kinds = Vec::new();
            for request in seen.iter().filter(|r| r.method_path.starts_with("POST ")) {
                let path = &request.method_path;
                let recording = request
                    .body
                    .windows(24)
                    .any(|w| w == b"#EXT-X-PLAYLIST-TYPE:VOD");
                let (kind, wants) = if path.contains("/soc/") || path.contains("/feeds/") {
                    ("soc or manifest", false)
                } else if recording {
                    ("recording", false)
                } else if path.contains("/bzz?name=stream.m3u8") || is_playlist(request) {
                    ("live window", true)
                } else {
                    ("segment", true)
                };
                assert_eq!(
                    request.header("ant-ack-when-stored") == Some("true"),
                    wants,
                    "{format:?} {kind}: {path}",
                );
                kinds.push(kind);
            }
            for kind in ["segment", "live window", "recording", "soc or manifest"] {
                assert!(kinds.contains(&kind), "{format:?}: no {kind} in {kinds:?}");
            }
            assert_eq!(kinds.iter().filter(|k| **k == "segment").count(), 2);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reused_channel_continues_the_feed_at_start_index() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.start_index = Some(41);
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
        assert_eq!(
            report.feed_index,
            Some(41 + report.feed_updates),
            "{report:?}"
        );
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
        // An override is the index as given: the head is never read, and
        // the numbering starts at 0 with no seam, as before issue #173.
        assert!(
            !lock(&log)
                .seen
                .iter()
                .any(|r| r.method_path.starts_with("GET ")),
            "an override reads nothing"
        );
        assert!(
            playlist.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"),
            "{playlist}"
        );
        assert!(!playlist.contains("#EXT-X-DISCONTINUITY"), "{playlist}");
    }

    /// Start a broadcast on the default test topic against `log`'s stub,
    /// with `format`, and push an init and `media` segments.
    async fn continued_run(
        log: &Arc<Mutex<StubLog>>,
        format: StreamFormat,
        media: u8,
    ) -> Arc<LiveRun> {
        let addr = stub_gateway(Arc::clone(log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.format = format;
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
        for i in 0..media {
            let _ = run.push(SegmentKind::Media, vec![i; 1000], 2000, false);
        }
        run
    }

    /// The feed slot of `index` on the test topic, as [`soc_ids`] lists it.
    fn slot(index: u64) -> String {
        hex::encode(ant_retrieval::sequence_update_id(&[0x11; 32], index))
    }

    /// Every playlist the stub saw uploaded, in order (either format).
    fn uploaded_playlists(log: &Arc<Mutex<StubLog>>) -> Vec<String> {
        lock(log)
            .seen
            .iter()
            .filter(|r| r.method_path.starts_with("POST ") && r.body.starts_with(b"#EXTM3U"))
            .map(|r| String::from_utf8(r.body.clone()).unwrap())
            .collect()
    }

    fn media_sequence(playlist: &str) -> u64 {
        playlist
            .lines()
            .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .unwrap()
            .parse()
            .unwrap()
    }

    /// Issue #173: with no `start_index`, a broadcast reads the feed head
    /// once and continues it: its first update goes after the head, its
    /// numbering after the head playlist's (one seam), and its recording
    /// opens with the head playlist's entries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_new_broadcast_continues_the_feed_head() {
        let log = Arc::new(Mutex::new(StubLog {
            head: Some(StubHead {
                index: 9,
                version: "v2",
                reference: "cd".repeat(32),
                body: PRIOR_RECORDING.as_bytes().to_vec(),
            }),
            ..StubLog::default()
        }));
        let run = continued_run(&log, StreamFormat::SolarPunk, 3).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.error_count, 0, "{:?}", report.errors);
        assert_eq!(report.segments_listed, 3, "{report:?}");

        // The head was read once, before any update was written.
        let seen = lock(&log).seen.clone();
        let reads: Vec<usize> = (0..seen.len())
            .filter(|&i| seen[i].method_path.starts_with("GET "))
            .collect();
        assert_eq!(reads.len(), 1, "one head read");
        let feed_path = format!(
            "GET /feeds/{}/{}?exact=true",
            hex::encode(test_owner()),
            "11".repeat(32)
        );
        assert_eq!(seen[reads[0]].method_path, feed_path);
        let first_soc = seen
            .iter()
            .position(|r| r.method_path.contains("/soc/"))
            .unwrap();
        assert!(reads[0] < first_soc);

        // Every update after the head, none over it.
        let ids = soc_ids(&log);
        assert_eq!(ids[0], slot(10), "the first update follows the head at 9");
        assert!(
            !(0..=9).any(|i| ids.contains(&slot(i))),
            "an earlier slot was rewritten"
        );
        assert_eq!(
            report.feed_index,
            Some(10 + report.feed_updates),
            "{report:?}"
        );

        // Numbered on from 12 + 3 entries, with the seam on the first.
        let playlists = uploaded_playlists(&log);
        let first = &playlists[0];
        assert!(first.contains("\n#EXT-X-MEDIA-SEQUENCE:15\n"), "{first}");
        assert!(
            first.contains("\n#EXT-X-DISCONTINUITY-SEQUENCE:1\n"),
            "{first}"
        );
        assert!(first.contains("\n#EXT-X-DISCONTINUITY\n"), "{first}");
        // The media sequence only ever moves forward across the live
        // and closing windows.
        let windows: Vec<u64> = playlists[..playlists.len() - 1]
            .iter()
            .map(|p| media_sequence(p))
            .collect();
        assert!(windows.windows(2).all(|w| w[0] <= w[1]), "{windows:?}");
        assert!(windows.iter().all(|s| *s >= 15), "{windows:?}");

        // The recording is the whole channel: the head's entries, the
        // seam, then this broadcast's, numbered from the head's 12.
        let recording = playlists.last().unwrap();
        assert!(
            recording.contains("#EXT-X-PLAYLIST-TYPE:VOD"),
            "{recording}"
        );
        assert_eq!(media_sequence(recording), 12, "{recording}");
        let uris = entry_uris(recording);
        assert_eq!(uris[..3], ["seg-12", "gap-13", "seg-14"], "{recording}");
        assert_eq!(uris.len(), 6, "{recording}");
        assert_eq!(Predecessor::parse(recording).unwrap().next_number, 18);
    }

    /// A head playlist whose body stream failed after the response head
    /// went out (a multi-chunk recording, a later leaf not retrieved) is
    /// a failed read, not a shorter predecessor: numbering after the
    /// half that arrived would reuse the rest's media sequence numbers
    /// and drop them from the recording (R4-F1 on PR #183). Same for a
    /// v1 head's `/bzz` playlist.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_head_playlist_cut_short_is_retried_not_continued() {
        let manifest = "ef".repeat(32);
        for (format, head) in [
            (
                StreamFormat::SolarPunk,
                StubHead {
                    index: 9,
                    version: "v2",
                    reference: "cd".repeat(32),
                    body: PRIOR_RECORDING.as_bytes().to_vec(),
                },
            ),
            (
                StreamFormat::Bzz,
                StubHead {
                    index: 9,
                    version: "v1",
                    reference: manifest.clone(),
                    body: b"the manifest's root node".to_vec(),
                },
            ),
        ] {
            let v1 = head.version == "v1";
            let log = Arc::new(Mutex::new(StubLog {
                head: Some(head),
                bzz_playlists: HashMap::from([(manifest.clone(), PRIOR_RECORDING.to_string())]),
                // v1: the head itself arrives whole, its playlist cut.
                truncated_head: u32::from(!v1),
                truncated_playlist: u32::from(v1),
                ..StubLog::default()
            }));
            let run = continued_run(&log, format, 2).await;
            wait_until(&run, "the head to answer", |s| s.feed_index.is_some()).await;
            run.cancel();
            settle(&run).await;
            let report = run.report();
            assert_eq!(report.error_count, 1, "{:?}", report.errors);
            assert!(
                report.errors[0].starts_with("feed head:")
                    && report.errors[0].contains("truncated"),
                "{:?}",
                report.errors
            );
            assert_eq!(soc_ids(&log)[0], slot(10));
            let playlists = uploaded_playlists(&log);
            assert_eq!(media_sequence(&playlists[0]), 15, "{}", playlists[0]);
            let recording = playlists.last().unwrap();
            assert_eq!(entry_uris(recording)[..3], ["seg-12", "gap-13", "seg-14"]);
        }
    }

    /// Under `"bzz"` the head is a v1 update pointing at the playlist's
    /// manifest (the response's `ETag`): its playlist is read through
    /// `/bzz/<it>/stream.m3u8`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_bzz_head_is_read_through_its_playlist_manifest() {
        let manifest = "ef".repeat(32);
        let log = Arc::new(Mutex::new(StubLog {
            head: Some(StubHead {
                index: 4,
                version: "v1",
                reference: manifest.clone(),
                body: b"not the playlist: the manifest's root node".to_vec(),
            }),
            bzz_playlists: HashMap::from([(manifest.clone(), PRIOR_RECORDING.to_string())]),
            // The playlist's chunk misses once: a read to retry, not a
            // manifest with no playlist (R1-M1 on PR #183).
            unretrievable_playlist: 1,
            ..StubLog::default()
        }));
        let run = continued_run(&log, StreamFormat::Bzz, 2).await;
        wait_until(&run, "the head to answer", |s| s.feed_index.is_some()).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.error_count, 1, "{:?}", report.errors);
        assert!(
            report.errors[0].starts_with("feed head: head playlist")
                && report.errors[0].contains("404"),
            "{:?}",
            report.errors
        );
        assert!(lock(&log)
            .seen
            .iter()
            .any(|r| r.method_path == format!("GET /bzz/{manifest}/stream.m3u8")));
        assert_eq!(soc_ids(&log)[0], slot(5));
        let playlists = uploaded_playlists(&log);
        assert_eq!(media_sequence(&playlists[0]), 15, "{}", playlists[0]);
        assert!(playlists.last().unwrap().contains("\nseg-14\n"));

        // A v1 head whose manifest holds no playlist (foreign content on
        // the topic): the feed still continues after it, numbered from 0.
        let log = Arc::new(Mutex::new(StubLog {
            head: Some(StubHead {
                index: 4,
                version: "v1",
                reference: manifest,
                body: Vec::new(),
            }),
            ..StubLog::default()
        }));
        let run = continued_run(&log, StreamFormat::Bzz, 1).await;
        run.cancel();
        settle(&run).await;
        assert_eq!(soc_ids(&log)[0], slot(5));
        let first = &uploaded_playlists(&log)[0];
        assert_eq!(media_sequence(first), 0, "{first}");
        assert!(!first.contains("DISCONTINUITY"), "{first}");
    }

    /// A feed nothing was ever written to (`404 "no update found"`)
    /// starts at index 0, numbered from 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_empty_feed_starts_at_index_zero() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let run = continued_run(&log, StreamFormat::SolarPunk, 2).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.error_count, 0, "{:?}", report.errors);
        assert_eq!(soc_ids(&log)[0], slot(0));
        let first = &uploaded_playlists(&log)[0];
        assert_eq!(media_sequence(first), 0, "{first}");
        assert!(!first.contains("DISCONTINUITY"), "{first}");
    }

    /// A head read that fails (a 502, the node's 503 for a cold anchor
    /// miss, or bee's other 404, a head that exists and did not
    /// retrieve) is no answer: nothing is written, in particular not
    /// index 0, until a retry reads the head. The segments upload
    /// meanwhile and are listed once it answers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_head_read_is_retried_never_read_as_an_empty_feed() {
        let log = Arc::new(Mutex::new(StubLog {
            head: Some(StubHead {
                index: 3,
                version: "v2",
                reference: "cd".repeat(32),
                body: PRIOR_RECORDING.as_bytes().to_vec(),
            }),
            fail_head: 1,
            cold_head: 1,
            unretrievable_head: 1,
            ..StubLog::default()
        }));
        let run = continued_run(&log, StreamFormat::SolarPunk, 3).await;
        wait_until(&run, "the head to answer", |s| s.feed_index > Some(0)).await;
        let reads = lock(&log)
            .seen
            .iter()
            .filter(|r| r.method_path.starts_with("GET /feeds/"))
            .count();
        assert_eq!(reads, 4, "three failed reads, then the answer");
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.segments_listed, 3, "{report:?}");
        let ids = soc_ids(&log);
        assert_eq!(ids[0], slot(4));
        assert!(!ids.iter().any(|id| (0..=3).any(|i| *id == slot(i))));
        assert_eq!(media_sequence(&uploaded_playlists(&log)[0]), 15);
        assert_eq!(report.error_count, 3, "{:?}", report.errors);
        assert!(
            report.errors.iter().all(|e| e.starts_with("feed head: ")),
            "{:?}",
            report.errors
        );
        assert!(report.errors[1].contains("503"), "{:?}", report.errors);
        assert!(
            report.errors[2].contains("wrapped chunk cannot be retrieved"),
            "{:?}",
            report.errors
        );
    }

    /// A head that never answers before the stop: the broadcast writes no
    /// playlist and no feed update at all, rather than guess an index.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_head_that_never_answers_writes_nothing() {
        let log = Arc::new(Mutex::new(StubLog {
            fail_head: u32::MAX,
            ..StubLog::default()
        }));
        let run = continued_run(&log, StreamFormat::SolarPunk, 3).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.segments_published, 3, "segments upload: {report:?}");
        assert_eq!(report.feed_updates, 0, "{report:?}");
        assert_eq!(report.playlists_published, 0, "{report:?}");
        // No position to hand on: `null`, never 0, which a host passing
        // it on as `start_index` would rewrite the channel from
        // (R1-M2 on PR #183).
        assert_eq!(report.feed_index, None, "{report:?}");
        let json = serde_json::to_value(&report).unwrap();
        assert!(json["feed_index"].is_null(), "{json}");
        assert_eq!(soc_ids(&log), Vec::<String>::new());
        assert_eq!(uploaded_playlists(&log), Vec::<String>::new());
        assert!(
            report
                .errors
                .last()
                .is_some_and(|e| e.contains("no playlist or feed update was written")),
            "{:?}",
            report.errors
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
            .find(|r| r.method_path.starts_with("POST /feeds/"))
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
        let handed_out = run.report().feed_index.expect("the position is known");
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.feed_index, Some(handed_out), "{report:?}");
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
        let handed_out = run.report().feed_index.expect("the position is known");
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.feed_index, Some(handed_out), "{report:?}");
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

    /// A host that names no `format` (any host that predates the field)
    /// gets `"bzz"`, the shape it always published, so an older build keeps
    /// writing what it wrote before. Both example apps now send
    /// `"solar-punk"` explicitly, which is what the web player and both
    /// Watch tabs read.
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
            .find(|r| r.method_path.starts_with("POST /feeds/"))
            .expect("feed manifest created");
        assert!(
            manifest
                .method_path
                .ends_with("/e0324f32e2edd73e3cd532076d203c397b9764eac2395fc3baab5738ad1b29bf"),
            "{}",
            manifest.method_path,
        );
    }

    /// The bytes Solar Punk's `encodeLadderMarker`
    /// (stream-event-client `src/shared/ladderMarker.ts`, v2) writes for
    /// this marker, taken from running it under bee-js 13.1.0, the
    /// version both pin; its `parseLadderMarker` accepts them. The write
    /// time is the last millisecond of its period, which a reader still
    /// takes.
    #[test]
    fn a_ladder_marker_is_the_json_solar_punk_writes() {
        let rung = topic_from_string("antstream-3f2b8c4e-1d2a-4b6f-9e0a-7c5d4e3f2a1b");
        assert_eq!(
            hex::encode(rung),
            "b0050c33bf70c99caa3fac075622bc8d699462306c1f3baff35dae253d650416",
        );
        let payload =
            encode_ladder_marker(176_044_800, 1_760_448_009_999, &rung, 1800, 2000).unwrap();
        assert_eq!(
            String::from_utf8(payload).unwrap(),
            r#"{"v":2,"period":176044800,"writtenAt":1760448009999,"rungs":{"b0050c33bf70c99caa3fac075622bc8d699462306c1f3baff35dae253d650416":1800},"segmentMs":2000}"#,
        );
        assert_eq!(marker_period_at(1_760_448_009_999), 176_044_800);
        assert_eq!(marker_period_at(1_760_448_010_000), 176_044_801);
        // What `parseLadderMarker` would read as absent is never written.
        for (period, written_at, segment_ms) in [
            // A write time in the next period, or the one before.
            (176_044_800, 1_760_448_010_000, 2000),
            (176_044_800, 1_760_447_999_999, 2000),
            // No segment length.
            (176_044_800, 1_760_448_000_250, 0),
        ] {
            assert!(
                encode_ladder_marker(period, written_at, &rung, 1, segment_ms).is_err(),
                "{period} {written_at} {segment_ms}",
            );
        }
        assert!(encode_ladder_marker(0, 0, &rung, 1 << 53, 2000).is_err());
    }

    /// The group a channel's ladder hangs off, and where its markers go:
    /// the topic and identifiers here are bee-js 13.1.0's
    /// `Topic.fromString` and stream-event-client's
    /// `ladderMarkerIdentifier`, run on the group text this publisher
    /// derives. The group text survives the `parseSwarmUri` a catalog
    /// entry's source URL goes through (no `/`).
    #[test]
    fn the_ladder_group_and_its_marker_addresses_match_solar_punk() {
        let rung = topic_from_string("antstream-3f2b8c4e-1d2a-4b6f-9e0a-7c5d4e3f2a1b");
        let group_text = ladder_group_text(&rung);
        assert_eq!(
            group_text,
            "antstream-ladder-b0050c33bf70c99caa3fac075622bc8d699462306c1f3baff35dae253d650416",
        );
        assert!(fits_swarm_uri(&group_text));
        let group = topic_from_string(&group_text);
        assert_eq!(
            hex::encode(group),
            "d76e2fab1498998b0b639b0bc757cfe6868fdc4f27dc96985f2ea2f084f86dc9",
        );
        for (period, expected) in [
            (
                176_044_800,
                "bd5b1732eb10afec844d023e950a8979cd05c550146c103cc637622b2666b1c8",
            ),
            (
                0,
                "4f92d4af9ce9f296668c4fff9ae1f007bfa21b831497a973fe41d1e5c3a6a8a0",
            ),
        ] {
            assert_eq!(
                hex::encode(ladder_marker_identifier(&group, period)),
                expected,
                "period {period}",
            );
        }
    }

    /// Solar Punk's `buildMasterPlaylist` (streaming-monorepo
    /// `packages/shared/src/masterPlaylist.ts`) for a ladder of one,
    /// byte for byte, and the URI's topic reads back whole through its
    /// `parseSwarmUri`.
    #[test]
    fn the_master_playlist_is_the_one_solar_punk_writes() {
        let owner: [u8; 20] = hex::decode("d5572300e441b77b72bdd318bbfa7b97a1f03096")
            .unwrap()
            .try_into()
            .unwrap();
        let text = "antstream-3f2b8c4e-1d2a-4b6f-9e0a-7c5d4e3f2a1b";
        assert_eq!(
            render_master_playlist(&owner, text, 1_723_077, 1_578_947, 640, 360),
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n\
             #EXT-X-STREAM-INF:BANDWIDTH=1723077,AVERAGE-BANDWIDTH=1578947,RESOLUTION=640x360\n\
             swarm://d5572300e441b77b72bdd318bbfa7b97a1f03096/antstream-3f2b8c4e-1d2a-4b6f-9e0a-7c5d4e3f2a1b\n",
        );
        // An unknown size is left out rather than guessed.
        assert!(!render_master_playlist(&owner, text, 2, 1, 0, 360).contains("RESOLUTION"));
        assert!(fits_swarm_uri(text));
        for text in ["", "antstream/Kitchen", "my channel", "a#b", "a?b", "50%"] {
            assert!(!fits_swarm_uri(text), "{text:?}");
        }
    }

    /// `BANDWIDTH` is the peak over three consecutive segments and
    /// `AVERAGE-BANDWIDTH` the whole mean, clamped to the peak: the
    /// figures Solar Punk's `BitrateMeter.ts` reports for the same
    /// segments (sizes in bytes, durations in ms), from the run that
    /// fills its window on.
    #[test]
    fn the_bitrate_is_measured_the_way_solar_punk_measures_it() {
        let mut meter = BitrateMeter::default();
        let mut measured = Vec::new();
        for (bytes, ms) in [
            (250_000, 2000),
            (500_000, 1000),
            (260_000, 2000),
            (240_000, 2000),
            (900_000, 2500),
            (100_000, 1900),
        ] {
            meter.record(bytes, ms);
            measured.push(meter.measured());
        }
        assert_eq!(
            measured,
            [
                None,
                None,
                Some((1_616_000, 1_616_000)),
                Some((1_616_000, 1_428_571)),
                Some((1_723_077, 1_723_077)),
                Some((1_723_077, 1_578_947)),
            ],
        );
    }

    /// The wall-clock offset that puts the next marker period boundary
    /// `ms_ahead` from now, for [`start_with_wall_offset`].
    fn boundary_in(ms_ahead: u64) -> i64 {
        let now = unix_ms_now();
        let to_boundary = LADDER_MARKER_PERIOD_MS - now % LADDER_MARKER_PERIOD_MS;
        i64::try_from(to_boundary).unwrap() - i64::try_from(ms_ahead).unwrap()
    }

    /// The SOC requests at identifier `id`.
    fn socs_at<'a>(
        seen: &'a [SeenRequest],
        owner: &[u8; 20],
        id: &[u8; 32],
    ) -> Vec<&'a SeenRequest> {
        let prefix = format!("POST /soc/{}/{}?sig=", hex::encode(owner), hex::encode(id));
        seen.iter()
            .filter(|r| r.method_path.starts_with(&prefix))
            .collect()
    }

    /// Issue #174 under `"solar-punk"`: the master goes up once the
    /// bitrate is measured, as the group feed's update after its head,
    /// and a marker every period names the newest feed update.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_ladder_writes_its_master_after_the_group_head_and_a_marker_each_period() {
        const TEXT: &str = "antstream-ladder-unit";
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let topic = topic_from_string(TEXT);
        let group = topic_from_string(&ladder_group_text(&topic));
        // An earlier broadcast's master at index 4 of the group feed.
        lock(&log).feed_heads.insert(
            hex::encode(group),
            StubHead {
                index: 4,
                version: "v2",
                reference: "00".repeat(32),
                body: b"#EXTM3U\n".to_vec(),
            },
        );
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.topic = String::new();
        config.topic_text = TEXT.into();
        config.ladder = true;
        config.width = 640;
        config.height = 360;
        let owner = test_owner();
        let offset = boundary_in(1500);
        let run = start_with_wall_offset(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            owner,
            None,
            offset,
        )
        .unwrap();
        assert_eq!(run.snapshot().ladder_group, ladder_group_text(&topic));
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..3u8 {
            let _ = run.push(SegmentKind::Media, vec![i; 40_000], 2000, false);
        }
        for _ in 0..200 {
            let snapshot = run.snapshot();
            if !snapshot.master_reference.is_empty() && snapshot.markers_written > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let snapshot = run.snapshot();
        run.cancel();
        settle(&run).await;
        assert_eq!(snapshot.markers_written, 1, "{snapshot:?}");
        assert!(!snapshot.master_reference.is_empty(), "{snapshot:?}");
        assert_eq!(snapshot.error_count, 0, "{snapshot:?}");

        let seen = lock(&log).seen.clone();
        // The group head was read the way the channel's is, then the
        // master went up after it: 3 × 40 000 bytes over 6 s is 160 kbit/s.
        assert!(seen.iter().any(|r| r.method_path
            == format!(
                "GET /feeds/{}/{}?exact=true",
                hex::encode(owner),
                hex::encode(group)
            )));
        let master = render_master_playlist(&owner, TEXT, 160_000, 160_000, 640, 360);
        let uploaded = seen
            .iter()
            .find(|r| r.method_path.ends_with("/bytes") && r.body == master.as_bytes())
            .expect("master uploaded");
        assert_eq!(bytes_reference(uploaded), snapshot.master_reference);
        let slot = socs_at(&seen, &owner, &ant_retrieval::sequence_update_id(&group, 5));
        assert_eq!(slot.len(), 1, "the master is the group feed's index 5");
        assert_eq!(
            &slot[0].body[8..],
            master.as_bytes(),
            "a v2 update of the text"
        );
        assert!(socs_at(&seen, &owner, &ant_retrieval::sequence_update_id(&group, 4)).is_empty());

        // The marker of the period the boundary opened, naming the newest
        // update this broadcast had written.
        let period = marker_period_at(unix_ms_now().saturating_add_signed(offset));
        let markers: Vec<_> = (period.saturating_sub(2)..=period)
            .flat_map(|p| socs_at(&seen, &owner, &ladder_marker_identifier(&group, p)))
            .collect();
        assert_eq!(markers.len(), 1);
        let text = String::from_utf8(markers[0].body[8..].to_vec()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        let p = value["period"].as_u64().unwrap();
        assert_eq!(
            socs_at(&seen, &owner, &ladder_marker_identifier(&group, p)).len(),
            1
        );
        assert_eq!(marker_period_at(value["writtenAt"].as_u64().unwrap()), p);
        assert_eq!(value["segmentMs"], 2000);
        let index = value["rungs"][hex::encode(topic)].as_u64().unwrap();
        assert!(index < snapshot.feed_index.unwrap(), "{text}");
        assert_eq!(
            text.into_bytes(),
            encode_ladder_marker(p, value["writtenAt"].as_u64().unwrap(), &topic, index, 2000)
                .unwrap(),
        );
    }

    /// A ladder write that fails is a shortcut lost, not a broadcast
    /// failure: it counts in `ladder_failures`/`ladder_error` and never
    /// in `errors`/`last_error`/`error_count`, which a host shows as the
    /// broadcast's headline error and which hold only eight entries.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failing_ladder_write_stays_out_of_the_broadcast_errors() {
        const TEXT: &str = "antstream-ladder-failing";
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        lock(&log).fail_ladder = true;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.topic = String::new();
        config.topic_text = TEXT.into();
        config.ladder = true;
        let run = start_with_wall_offset(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
            boundary_in(1500),
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..3u8 {
            let _ = run.push(SegmentKind::Media, vec![i; 40_000], 2000, false);
        }
        // One master and one marker have failed.
        for _ in 0..200 {
            if run.snapshot().ladder_failures >= 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let snapshot = run.snapshot();
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(snapshot.ladder_failures, 2, "{snapshot:?}");
        assert!(
            snapshot.ladder_error.starts_with("ladder marker")
                || snapshot.ladder_error.starts_with("master playlist"),
            "{snapshot:?}"
        );
        assert_eq!(snapshot.markers_written, 0, "{snapshot:?}");
        assert_eq!(snapshot.master_reference, "", "{snapshot:?}");
        assert_eq!(snapshot.error_count, 0, "{snapshot:?}");
        assert_eq!(snapshot.last_error, "", "{snapshot:?}");
        assert!(report.ladder_failures >= 2, "{report:?}");
        assert!(!report.ladder_error.is_empty(), "{report:?}");
        assert_eq!(report.error_count, 0, "{report:?}");
        assert!(report.errors.is_empty(), "{report:?}");
        assert!(report.kept_up, "{report:?}");
    }

    /// Under `"bzz"`, or with a topic text no `swarm://` URI can carry
    /// (the default `antstream/<channel>`), the ladder is markers only:
    /// no group head read, no master. And none at all before the first
    /// feed update landed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_ladder_a_master_cant_name_writes_markers_only() {
        // Concurrently: each waits out a whole marker period.
        tokio::join!(
            markers_only(StreamFormat::Bzz),
            markers_only(StreamFormat::SolarPunk),
        );
    }

    async fn markers_only(format: StreamFormat) {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.topic = String::new();
        config.format = format;
        config.ladder = true;
        let owner = test_owner();
        let topic = topic_from_string(&format!("antstream/{}", config.channel));
        let group = topic_from_string(&ladder_group_text(&topic));
        // The feed head won't answer until the first boundary has
        // passed, so that period has nothing to name yet.
        lock(&log).fail_head = 1;
        let offset = boundary_in(300);
        let run = start_with_wall_offset(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            owner,
            None,
            offset,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..4u8 {
            let _ = run.push(SegmentKind::Media, vec![i; 40_000], 2000, false);
        }
        // Past the first boundary (the head read is retried after 2 s)
        // and the second.
        for _ in 0..600 {
            if run.snapshot().markers_written > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.markers_written, 1, "{format:?}: {report:?}");
        assert_eq!(report.master_reference, "", "{format:?}");
        let seen = lock(&log).seen.clone();
        assert!(
            !seen
                .iter()
                .any(|r| r.method_path.contains(&hex::encode(group))
                    || r.body
                        .starts_with(b"#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT")),
            "{format:?}: a master was written",
        );
        let now = marker_period_at(unix_ms_now().saturating_add_signed(offset));
        let first = now - 1;
        assert!(
            socs_at(&seen, &owner, &ladder_marker_identifier(&group, first)).is_empty(),
            "{format:?}: a marker before the first update",
        );
        assert_eq!(
            socs_at(&seen, &owner, &ladder_marker_identifier(&group, now)).len(),
            1,
            "{format:?}",
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
        // A batch the gateway won't stamp with is named as the batch's
        // problem (issue #176): both segments, the init included.
        assert_eq!(report.batch_unusable_refusals, 2);
        assert_eq!(report.batch_full_refusals, 0);
        assert!(
            report.batch_error.contains("batch not usable"),
            "{}",
            report.batch_error
        );
    }

    /// The answers the gateway gives a refused upload, read for whose
    /// problem it is (issue #176): the status first, the message only
    /// where the status alone is ambiguous.
    #[test]
    fn a_refusal_is_the_batchs_by_its_status_not_by_a_failed_upload() {
        let of = |status, body: &str| BatchRefusal::of_response(status, body.as_bytes());
        // bee's and ant's full bucket on an immutable batch.
        assert_eq!(
            of(402, r#"{"message":"batch is overissued"}"#),
            Some(BatchRefusal::Full)
        );
        assert_eq!(
            of(
                402,
                r#"{"message":"batch 0xab… saturated: collision bucket full at depth 20 on an immutable batch"}"#
            ),
            Some(BatchRefusal::Full),
        );
        // A status alone says it, whatever the words.
        assert_eq!(of(402, ""), Some(BatchRefusal::Full));
        assert_eq!(
            of(
                422,
                r#"{"message":"rejected by 0xpeer: batch 0xab not found on-chain"}"#
            ),
            Some(BatchRefusal::Unusable),
        );
        // A 400 or 404 only when it names the batch.
        assert_eq!(
            of(400, r#"{"message":"batch 0xab not usable"}"#),
            Some(BatchRefusal::Unusable)
        );
        assert_eq!(
            of(
                400,
                r#"{"message":"batch not usable yet or does not exist"}"#
            ),
            Some(BatchRefusal::Unusable),
        );
        assert_eq!(
            of(404, r#"{"message":"batch with id not found"}"#),
            Some(BatchRefusal::Unusable)
        );
        assert_eq!(of(400, r#"{"message":"chunk wire size too large"}"#), None);
        assert_eq!(of(404, "Not Found"), None);
        // The network's: a pushsync failure, a node not ready, a gateway
        // that is down.
        assert_eq!(
            of(502, r#"{"message":"pushsync: exhausted pushsync peers"}"#),
            None
        );
        assert_eq!(of(503, r#"{"message":"batch is still propagating"}"#), None);
        assert_eq!(of(500, ""), None);
    }

    /// Run a broadcast whose every upload the stub refuses with `answer`,
    /// pushing an init and three media segments, and return its report.
    async fn refused_broadcast(answer: (&'static str, &'static str)) -> PublisherReport {
        let log = Arc::new(Mutex::new(StubLog {
            fail_uploads: true,
            fail_answer: Some(answer),
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
        for i in 0..3 {
            let _ = run.push(SegmentKind::Media, vec![i; 64], 2000, false);
        }
        wait_until(&run, "every upload answered", |s| {
            s.segments_failed == 3 && s.error_count >= 4
        })
        .await;
        let snapshot = run.snapshot();
        run.cancel();
        settle(&run).await;
        let report = run.report();
        // The live view reads the same counts the report ends with.
        assert_eq!(snapshot.batch_full_refusals, report.batch_full_refusals);
        assert_eq!(
            snapshot.batch_unusable_refusals,
            report.batch_unusable_refusals
        );
        assert_eq!(snapshot.batch_error, report.batch_error);
        report
    }

    /// A batch that fills mid-broadcast is counted and named as full
    /// (issue #176), apart from a network that refuses the same uploads:
    /// both drop the same segments, but only one is the batch.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_full_batch_is_counted_apart_from_a_failing_network() {
        let full = refused_broadcast((
            "402 Payment Required",
            r#"{"message":"batch is overissued"}"#,
        ))
        .await;
        assert_eq!(full.segments_failed, 3);
        assert_eq!(
            full.batch_full_refusals, 4,
            "the init and three media segments"
        );
        assert_eq!(full.batch_unusable_refusals, 0);
        assert!(full.batch_error.contains("402"), "{}", full.batch_error);
        assert!(
            full.batch_error.contains("overissued"),
            "{}",
            full.batch_error
        );
        // Still a failure like any other in the broadcast's errors.
        assert!(full.error_count >= 4);

        let network = refused_broadcast((
            "502 Bad Gateway",
            r#"{"message":"pushsync: exhausted pushsync peers"}"#,
        ))
        .await;
        assert_eq!(network.segments_failed, 3, "the same segments are lost");
        assert_eq!(network.batch_full_refusals, 0);
        assert_eq!(network.batch_unusable_refusals, 0);
        assert_eq!(network.batch_error, "");
        assert!(network.error_count >= 4);
    }

    /// A refused feed update is the batch's too: the playlist uploads
    /// and the segments land, but the update naming them is refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_refused_feed_update_counts_against_the_batch() {
        let log = Arc::new(Mutex::new(StubLog {
            fail_soc: true,
            fail_answer: Some((
                "402 Payment Required",
                r#"{"message":"batch is overissued"}"#,
            )),
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
        wait_until(&run, "a refused feed update", |s| s.batch_full_refusals > 0).await;
        let snapshot = run.snapshot();
        assert_eq!(snapshot.segments_failed, 0);
        assert!(
            snapshot.batch_error.starts_with("feed update 0:"),
            "{}",
            snapshot.batch_error
        );
        run.cancel();
        settle(&run).await;
    }

    fn plan(worst_case_remaining_chunks: u64, immutable: bool) -> ant_control::PostageStatusView {
        ant_control::PostageStatusView {
            enabled: true,
            usable: true,
            batch_id: format!("0x{}", "ab".repeat(32)),
            batch_depth: 22,
            bucket_depth: 16,
            immutable,
            bucket_count: 1 << 16,
            bucket_capacity: 64,
            total_capacity_chunks: 1 << 22,
            worst_case_remaining_chunks,
            ..ant_control::PostageStatusView::default()
        }
    }

    /// What a minute of broadcast stamps, counted the way the gateway
    /// assembles each upload (issue #176): the bitrate's segments with
    /// their erasure coding and replicas, plus the playlist and feed
    /// update each one brings.
    #[test]
    fn a_broadcast_costs_its_segments_plus_their_playlist_and_feed_overhead() {
        let mut config = config();
        config.format = StreamFormat::SolarPunk;
        config.bitrate_kbps = 900;
        config.segment_ms = 2000;
        // One segment: 900 kbit/s × 2 s = 225 000 bytes, 247 500 with the
        // headroom. 61 data leaves under one intermediate chunk, its
        // MEDIUM parities, and two replicas of the root.
        let segment_bytes = 247_500usize;
        let split = ant_retrieval::split_bytes_with_redundancy(&vec![0u8; segment_bytes], 1);
        let media = split.chunks.len() as u64 + 2;
        assert!(
            media > data_chunk_count(segment_bytes as u64),
            "parities and replicas cost stamps"
        );
        // The one-chunk playlist and its two replicas, then the update.
        let playlist = 1 + 2 + 1;
        let cost = broadcast_cost(&config);
        let per_minute_without_recording = 30 * (media + playlist);
        assert!(
            cost.chunks_per_minute > per_minute_without_recording
                && cost.chunks_per_minute < per_minute_without_recording + 10,
            "{} vs {per_minute_without_recording}",
            cost.chunks_per_minute,
        );

        // The ladder's markers are one a period, six a minute.
        config.ladder = true;
        assert_eq!(
            broadcast_cost(&config).chunks_per_minute,
            cost.chunks_per_minute + 6
        );
        config.ladder = false;

        // Twice the bitrate is about twice the segments' chunks (their
        // trees grow parities and intermediate chunks with them), while
        // the playlist and feed overhead stays put.
        config.bitrate_kbps = 1800;
        let double = broadcast_cost(&config).chunks_per_minute;
        let media_minute = cost.chunks_per_minute - 30 * playlist;
        let doubled = (double - 30 * playlist) as f64 / (2 * media_minute) as f64;
        assert!(
            (0.95..1.05).contains(&doubled),
            "{double} vs {} per minute at 900 kbit/s",
            cost.chunks_per_minute,
        );

        // Under "bzz" every upload also carries its manifest.
        config.bitrate_kbps = 900;
        config.format = StreamFormat::Bzz;
        assert!(broadcast_cost(&config).chunks_per_minute > cost.chunks_per_minute);
    }

    /// The pre-flight check (issue #176): minutes left counted from the
    /// fullest bucket, low under an hour, and the batch's mutability
    /// carried through for the host's warning.
    #[test]
    fn the_capacity_estimate_counts_the_fullest_bucket_and_flags_a_nearly_full_batch() {
        let config = config();
        let per_minute = broadcast_cost(&config).chunks_per_minute;
        let fixed = broadcast_cost(&config).fixed_chunks;

        // Plenty of room: a day and more.
        let roomy = estimate_capacity(&config, &plan(per_minute * 2_000, true)).unwrap();
        assert!(!roomy.low);
        assert!(roomy.immutable && roomy.usable);
        assert_eq!(roomy.chunks_per_minute, per_minute);
        assert_eq!(
            roomy.minutes_left,
            (per_minute * 2_000 - fixed) / per_minute
        );

        // A batch whose average bucket is roomy but whose fullest has
        // twenty minutes left: the fullest is what counts.
        let mut lopsided = plan(fixed + per_minute * 20, true);
        lopsided.remaining_total_chunks = per_minute * 2_000;
        let nearly_full = estimate_capacity(&config, &lopsided).unwrap();
        assert_eq!(nearly_full.minutes_left, 20);
        assert!(nearly_full.low);

        // Right at the line.
        let hour = estimate_capacity(
            &config,
            &plan(fixed + per_minute * LOW_CAPACITY_MINUTES, true),
        )
        .unwrap();
        assert_eq!(hour.minutes_left, LOW_CAPACITY_MINUTES);
        assert!(!hour.low);

        // Full, and no room at all for the fixed overhead.
        let full = estimate_capacity(&config, &plan(0, true)).unwrap();
        assert_eq!(full.minutes_left, 0);
        assert!(full.low);

        // A mutable batch is estimated the same way and reported mutable:
        // past its minutes it overwrites rather than refuses.
        let mutable = estimate_capacity(&config, &plan(per_minute * 2_000, false)).unwrap();
        assert!(!mutable.immutable);
        assert_eq!(mutable.minutes_left, roomy.minutes_left);

        // A batch the node won't stamp with.
        let mut refused = plan(per_minute * 2_000, true);
        refused.usable = false;
        assert!(!estimate_capacity(&config, &refused).unwrap().usable);
        refused.propagating = true;
        assert!(
            estimate_capacity(&config, &refused).unwrap().usable,
            "a just-bought batch waits"
        );
    }

    /// The plan JSON the iOS app hands `ant_publisher_capacity`: its
    /// `StoragePlan` re-encoded, so only the keys it keeps. `usable`
    /// defaults to `true` when absent, so a host that drops it would
    /// make every batch look stampable; the app carries it (and
    /// `propagating`) through, and this is what that JSON estimates to.
    #[test]
    fn the_capacity_estimate_reads_usable_from_the_hosts_plan_json() {
        let config = config();
        let host_plan = |usable: bool, propagating: bool| {
            serde_json::json!({
                "enabled": true,
                "usable": usable,
                "propagating": propagating,
                "batch_id": format!("0x{}", "ab".repeat(32)),
                "batch_depth": 22,
                "immutable": true,
                "total_capacity_chunks": 1u64 << 22,
                "issued_chunks": 1_000,
                "worst_case_remaining_chunks": 1_000_000,
            })
            .to_string()
        };
        let estimate = |json: String| {
            let plan: ant_control::PostageStatusView = serde_json::from_str(&json).unwrap();
            estimate_capacity(&config, &plan).unwrap().usable
        };
        assert!(estimate(host_plan(true, false)));
        assert!(!estimate(host_plan(false, false)), "an expired batch");
        assert!(
            estimate(host_plan(false, true)),
            "a just-bought batch waits"
        );
    }

    /// The memo the estimate reads its cost from answers what
    /// [`broadcast_cost`] does, per config, without mixing them up.
    #[test]
    fn the_memoized_broadcast_cost_matches_the_cost() {
        let mut config = config();
        for format in [StreamFormat::Bzz, StreamFormat::SolarPunk] {
            for ladder in [false, true] {
                for (bitrate_kbps, segment_ms) in [(900, 2000), (900, 4000)] {
                    config.format = format;
                    config.ladder = ladder;
                    config.bitrate_kbps = bitrate_kbps;
                    config.segment_ms = segment_ms;
                    // Twice: once filling the memo, once reading it.
                    assert_eq!(broadcast_cost_memoized(&config), broadcast_cost(&config));
                    assert_eq!(broadcast_cost_memoized(&config), broadcast_cost(&config));
                }
            }
        }
    }

    #[test]
    fn the_capacity_estimate_refuses_a_plan_it_cannot_speak_for() {
        let config = config();
        let mut other = plan(1_000_000, true);
        other.batch_id = format!("0x{}", "cd".repeat(32));
        assert!(estimate_capacity(&config, &other)
            .unwrap_err()
            .to_string()
            .contains("the plan's batch is 0xcdcd"));
        let none = ant_control::PostageStatusView::default();
        assert!(estimate_capacity(&config, &none)
            .unwrap_err()
            .to_string()
            .contains("no storage plan"));
        // The host's plan JSON names its batch with or without `0x`, in
        // any case.
        let mut upper = plan(1_000_000, true);
        upper.batch_id = "AB".repeat(32);
        assert!(estimate_capacity(&config, &upper).is_ok());
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
            if run.snapshot().feed_index > Some(0) {
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
            .filter(|r| r.method_path.starts_with("POST /feeds/"))
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

    /// A reap held for the return stops being due once media is listed
    /// (an upload in flight when the window ran out landed after all):
    /// the run carries on, and a later outage's return must still buy a
    /// whole window rather than inherit the stale verdict.
    #[test]
    fn listed_media_clears_a_held_reap() {
        let mut state = PublisherState {
            reap_due: true,
            ..PublisherState::default()
        };
        state.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/aa/init-0.mp4".into(),
            },
        );
        assert!(state.advance(3, 2000).is_none());
        assert!(state.reap_due, "an init is not media");
        state.done.insert(1, media("/bzz/bb/seg-1.m4s", false));
        assert!(state.advance(3, 2000).is_some());
        assert!(!state.reap_due);
    }

    /// End to end: a held reap cleared by media that landed leaves the
    /// next return its fresh window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_return_after_a_cleared_reap_gets_a_whole_window() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 0);
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the segment", |s| s.segments_listed == 1).await;
        run.pause();
        // As the reaper would have, then an in-flight upload landing.
        lock(&run.state).reap_due = true;
        {
            let mut state = lock(&run.state);
            let seq = state.next_commit;
            state.done.insert(seq, media("/bzz/cc/seg-late.m4s", false));
            assert!(state.advance(64, 2000).is_some());
        }
        run.resume();
        {
            let state = lock(&run.state);
            assert!(!state.reap_due);
            let returned = state.returned_at.expect("the return restarts the window");
            assert!(returned >= state.last_media_at.unwrap());
        }
        run.cancel();
        settle(&run).await;
    }

    /// A held reap cleared by media that landed while still paused must
    /// not leave the reaper parked on the return: when the window that
    /// media restarted runs out paused too, the reap is due again, and
    /// the return ends the run rather than buying yet another window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cleared_reap_falls_due_again_if_the_new_window_runs_out_paused() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 300);
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the segment", |s| s.segments_listed == 1).await;
        run.pause();
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(lock(&run.state).reap_due, "the first window ran out paused");
        // An upload that was in flight when the window ran out lands,
        // folded in by the real committer.
        {
            let mut state = lock(&run.state);
            let seq = state.next_commit;
            state.done.insert(seq, media("/bzz/cc/seg-late.m4s", false));
        }
        run.commit_notify.notify_one();
        wait_until(&run, "the late segment", |s| s.segments_listed == 2).await;
        assert!(!lock(&run.state).reap_due, "media cleared the held reap");
        // The outage outlasts the window that media restarted.
        tokio::time::sleep(Duration::from_millis(450)).await;
        let snapshot = run.snapshot();
        assert!(snapshot.running && !snapshot.reaped, "{snapshot:?}");
        assert!(
            lock(&run.state).reap_due,
            "the second window ran out paused: due again"
        );
        let back = Instant::now();
        run.resume();
        settle(&run).await;
        assert!(
            back.elapsed() < Duration::from_millis(300),
            "the return ends it, it buys no window: {:?}",
            back.elapsed()
        );
        assert!(run.report().reaped);
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

    /// Polls for up to 10 s: long enough for three failed head reads
    /// and their [`FEED_HEAD_RETRY`] pauses.
    async fn wait_until(run: &Arc<LiveRun>, what: &str, done: impl Fn(&PublisherSnapshot) -> bool) {
        for _ in 0..1000 {
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
            s.segments_listed == 3 && s.feed_index > Some(0)
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
        // The camera stops handing segments over, the uplink is fine.
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
        tokio::time::sleep(Duration::from_millis(800)).await;
        let snapshot = run.snapshot();
        assert!(snapshot.running && !snapshot.reaped, "{snapshot:?}");
        run.cancel();
        settle(&run).await;
        assert!(!run.report().reaped);
    }

    /// A window that runs out while paused does not end the run against
    /// the dead uplink, where the closing window and the recording would
    /// each fail once and never go up: the reap waits for the return,
    /// and the return ends the run (no fresh window), with both uploads
    /// going out over the connection that came back.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_reap_due_while_paused_waits_for_the_return() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = quiet_run(&log, &addr, 300);
        let _ = run.push(SegmentKind::Init, vec![7u8; 64], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 64], 2000, false);
        wait_until(&run, "the segment", |s| s.segments_listed == 1).await;
        run.pause();
        {
            let mut log = lock(&log);
            log.fail_uploads = true;
            log.fail_soc = true;
        }
        // Well past the window, still offline.
        tokio::time::sleep(Duration::from_millis(900)).await;
        let snapshot = run.snapshot();
        assert!(
            snapshot.running && snapshot.paused && !snapshot.reaped,
            "{snapshot:?}"
        );
        assert!(lock(&run.state).reap_due);
        let closing = |r: &SeenRequest| r.body.windows(14).any(|w| w == b"#EXT-X-ENDLIST");
        assert!(
            !lock(&log)
                .seen
                .iter()
                .any(|r| closing(r) || is_recording(r)),
            "nothing is closed against the dead uplink"
        );

        // The network is back: the run ends at once, not a window later.
        {
            let mut log = lock(&log);
            log.fail_uploads = false;
            log.fail_soc = false;
        }
        let back = Instant::now();
        run.resume();
        settle(&run).await;
        assert!(
            back.elapsed() < Duration::from_millis(300),
            "the return ends it, it buys no window: {:?}",
            back.elapsed()
        );
        let report = run.report();
        assert!(report.reaped, "{report:?}");
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        assert!(lock(&log).seen.iter().any(is_recording));
        assert!(lock(&log).seen.iter().any(closing));
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

    // -----------------------------------------------------------------
    // Recovery entry (issue #175)
    // -----------------------------------------------------------------

    fn test_identity() -> ([u8; 20], [u8; 32]) {
        (test_owner(), [0x11; 32])
    }

    /// A media segment dated `n` segments after go-live.
    fn media_at(uri: &str, n: u64) -> Committed {
        let mut m = media(uri, false);
        if let Committed::Media { program_ms, .. } = &mut m {
            *program_ms = GO_LIVE_MS + 2000 * n;
        }
        m
    }

    /// A broadcast that continued [`PRIOR_RECORDING`], listed six
    /// entries (a gap among them) through a window of three, and has a
    /// lost segment still waiting for media behind it.
    fn broadcast_to_save() -> PublisherState {
        let mut state = live_state();
        state.continue_from(Predecessor::parse(PRIOR_RECORDING).unwrap());
        for seq in 1..=6 {
            let outcome = if seq == 4 {
                lost(false, GO_LIVE_MS + 2000 * seq)
            } else {
                media_at(&format!("/bzz/s{seq}/seg.m4s"), seq)
            };
            state.done.insert(seq, outcome);
            state.advance(3, 2000);
        }
        state.done.insert(7, lost(false, GO_LIVE_MS + 2000 * 7));
        state.advance(3, 2000);
        state
    }

    /// Issue #175: what a recovery entry holds comes back as the same
    /// broadcast. The resumed state renders the very live window and
    /// recording the saved one did, then carries on as one broadcast:
    /// a new writer's first segment takes the next number (the lost
    /// segment no viewer saw gives its number back), sits behind one
    /// `#EXT-X-DISCONTINUITY` under its own map, and the recording holds
    /// the predecessor, everything saved and everything new.
    #[test]
    fn a_recovery_entry_resumes_the_same_broadcast() {
        let saved = broadcast_to_save();
        let (owner, topic) = test_identity();
        let identity = RunIdentity {
            channel: "test channel",
            owner: &owner,
            topic: &topic,
            topic_text: "",
            format: StreamFormat::Bzz,
            segment_ms: 2000,
            width: 0,
            height: 0,
        };
        let entry = saved.recovery_entry(&identity, 9, PlaylistShape::Live);
        assert_eq!(entry.feed_index, 9);
        assert_eq!(entry.owner, hex::encode(owner));
        assert_eq!(
            entry.next_number, 21,
            "the waiting gap's number is free again"
        );
        let json = serde_json::to_vec(&entry).unwrap();
        let entry: RecoveryEntry = serde_json::from_slice(&json).unwrap();

        let mut resumed = PublisherState::resumed(entry);
        assert_eq!(resumed.resume_index, Some(9));
        assert!(!resumed.feed_settled, "the head still decides the index");
        assert_eq!(
            resumed.render_playlist(2000, false),
            saved.render_playlist(2000, false)
        );
        assert_eq!(resumed.render_recording(2000), saved.render_recording(2000));

        // The relaunched app's writer: a fresh init, then media.
        resumed.done.insert(
            0,
            Committed::Init {
                uri: "/bzz/new/init.mp4".into(),
            },
        );
        resumed.done.insert(1, media_at("/bzz/n1/seg.m4s", 40));
        resumed.done.insert(2, media_at("/bzz/n2/seg.m4s", 41));
        assert!(resumed.advance(3, 2000).is_some());
        assert_eq!(resumed.segments_listed, 2, "this run's own count");

        let live = resumed.render_playlist(2000, false);
        assert!(live.contains("\n#EXT-X-MEDIA-SEQUENCE:20\n"), "{live}");
        assert_eq!(
            live.matches("\n#EXT-X-DISCONTINUITY\n#EXT-X-MAP:URI=\"/bzz/new/init.mp4\"\n")
                .count(),
            1,
            "{live}"
        );
        assert!(live.contains(&format!(
            "#ANTSTREAM-BROADCAST:{:x}\n",
            1_760_000_000_000_u64
        )));

        let recording = resumed.render_recording(2000);
        assert_eq!(
            entry_uris(&recording),
            [
                "seg-12",
                "gap-13",
                "seg-14",
                "/bzz/s1/seg.m4s",
                "/bzz/s2/seg.m4s",
                "/bzz/s3/seg.m4s",
                "gap-18",
                "/bzz/s5/seg.m4s",
                "/bzz/s6/seg.m4s",
                "/bzz/n1/seg.m4s",
                "/bzz/n2/seg.m4s",
            ],
        );
        // The predecessor's own break, the go-live seam, the resume.
        assert_eq!(
            recording.matches("#EXT-X-DISCONTINUITY\n").count(),
            3,
            "{recording}"
        );
        // A resume configured with shorter segments keeps the target.
        let mut short = saved.recovery_entry(&identity, 9, PlaylistShape::Live);
        short.segment_ms = 6000;
        let short = PublisherState::resumed(short);
        assert!(short
            .render_playlist(1000, false)
            .contains("\n#EXT-X-TARGETDURATION:6\n"));
        // Numbering reads straight through: 12 + 11 entries.
        assert_eq!(Predecessor::parse(&recording).unwrap().next_number, 23);
        assert_eq!(resumed.next_number, 23);
    }

    /// A clock resurrected from disk floors at the last saved segment's
    /// end, whatever the entry's own clock says (missing here) and
    /// however far behind the wall clock reads: a resumed broadcast's
    /// first date never moves back (hls.js reads that as a parsing
    /// error). A wall clock ahead of it is taken as is.
    #[test]
    fn a_resumed_clock_never_dates_before_the_saved_segments() {
        let saved = broadcast_to_save();
        let (owner, topic) = test_identity();
        let identity = RunIdentity {
            channel: "c",
            owner: &owner,
            topic: &topic,
            topic_text: "",
            format: StreamFormat::Bzz,
            segment_ms: 2000,
            width: 0,
            height: 0,
        };
        let mut entry = saved.recovery_entry(&identity, 0, PlaylistShape::Live);
        // The last listed segment, seq 6, ended at go-live + 14 s.
        let last_end = GO_LIVE_MS + 2000 * 7;
        entry.next_program_ms = None;
        let mut clock = PublisherState::resumed(entry.clone()).program_clock;
        let _ = clock.place(SegmentKind::Init, 0, false, GO_LIVE_MS);
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS),
            last_end
        );
        // A saved clock behind the last entry (as a zero would be).
        entry.next_program_ms = Some(0);
        let mut clock = PublisherState::resumed(entry.clone()).program_clock;
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS + 5000),
            last_end,
            "no init first: a media segment alone re-anchors too"
        );
        // A clock past it (media after the last entry was dropped) wins.
        entry.next_program_ms = Some(last_end + 4000);
        let mut clock = PublisherState::resumed(entry.clone()).program_clock;
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, GO_LIVE_MS),
            last_end + 4000
        );
        // The relaunch, a minute later: dated from the wall clock.
        let mut clock = PublisherState::resumed(entry).program_clock;
        let relaunch = last_end + 60_000;
        assert_eq!(
            clock.place(SegmentKind::Media, 2000, false, relaunch),
            relaunch - 2000
        );
    }

    #[test]
    fn resume_needs_a_recovery_path_and_no_start_index() {
        let mut c = config();
        c.resume = true;
        assert!(c.validate().is_err());
        c.recovery_path = "/x/broadcast.json".into();
        assert!(c.validate().is_ok());
        c.start_index = Some(3);
        assert!(c.validate().is_err());
        // Serde defaults: a host that never heard of #175 keeps none.
        let c: PublisherConfig =
            serde_json::from_str(r#"{"channel":"c","batch_id":"ab"}"#).unwrap();
        assert!(c.recovery_path.is_empty() && !c.resume);
    }

    /// A fresh recovery path in the test's temp dir.
    fn recovery_path(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "ant-publisher-recovery-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("broadcast.json").to_string_lossy().into_owned()
    }

    /// A broadcast that keeps a recovery entry at `path`, listed three
    /// segments, and was then "killed": never stopped, nothing more
    /// pushed. Returns its entry as it stands and its playlists.
    async fn killed_broadcast(
        log: &Arc<Mutex<StubLog>>,
        addr: &str,
        path: &str,
    ) -> (RecoveryEntry, Vec<String>) {
        let mut c = config();
        c.gateway = format!("http://{addr}");
        c.format = StreamFormat::Bzz;
        c.max_backlog = 16;
        c.recovery_path = path.to_string();
        let run = start(
            &tokio::runtime::Handle::current(),
            c,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..3 {
            let _ = run.push(SegmentKind::Media, vec![i; 1000], 2000, false);
        }
        for _ in 0..400 {
            let s = run.snapshot();
            if s.segments_listed == 3
                && !run.feed_update_in_flight()
                && s.feed_index == Some(run.report().feed_updates)
                && load_recovery(path).is_ok_and(|e| e.recording.len() == 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let entry = load_recovery(path).expect("an entry ahead of every update");
        assert_eq!(entry.recording.len(), 3);
        assert_eq!(
            Some(entry.feed_index + 1),
            run.snapshot().feed_index,
            "written ahead of the last update, naming its index"
        );
        assert_eq!(run.report().recovery_error, "");
        (entry, uploaded_playlists(log))
    }

    /// The broadcast id a playlist names.
    fn playlist_broadcast(playlist: &str) -> &str {
        playlist
            .lines()
            .find_map(|l| l.strip_prefix("#ANTSTREAM-BROADCAST:"))
            .unwrap()
    }

    /// Start a run resuming the entry at `path` on `addr`.
    fn resumed_run(addr: &str, path: &str) -> Result<Arc<LiveRun>, PublisherError> {
        let mut c = config();
        c.gateway = format!("http://{addr}");
        c.format = StreamFormat::Bzz;
        c.max_backlog = 16;
        c.recovery_path = path.to_string();
        c.resume = true;
        start(
            &tokio::runtime::Handle::current(),
            c,
            TEST_SECRET,
            test_owner(),
            None,
        )
    }

    /// The stub's feed head at `index`, as a v1 update.
    fn head_at(index: u64) -> StubHead {
        StubHead {
            index,
            version: "v1",
            reference: "cd".repeat(32),
            body: Vec::new(),
        }
    }

    /// Issue #175's acceptance on the stub: the app is killed mid-
    /// broadcast with its last update landed, and the relaunch resumes.
    /// The feed continues right after that update (no slot rewritten),
    /// without reading its own head playlist as a predecessor; the media
    /// sequence numbers on, behind one seam; and the head ends as one
    /// recording, of the same broadcast, holding every segment from
    /// before the kill and after it. The entry goes once that lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_killed_broadcast_resumes_on_its_feed_and_records_it_whole() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("resume");
        let (entry, before) = killed_broadcast(&log, &addr, &path).await;
        let id = playlist_broadcast(&before[0]).to_string();
        // The kill came after the last update landed: the head is it.
        lock(&log).head = Some(head_at(entry.feed_index));
        let socs_before = soc_ids(&log).len();
        let reads_before = lock(&log)
            .seen
            .iter()
            .filter(|r| r.method_path.starts_with("GET "))
            .count();

        let run = resumed_run(&addr, &path).unwrap();
        let _ = run.push(SegmentKind::Init, vec![9u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![0x20; 1000], 2000, false);
        let _ = run.push(SegmentKind::Media, vec![0x21; 1000], 2000, false);
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.error_count, 0, "{:?}", report.errors);
        assert_eq!(report.segments_listed, 2);

        let socs = soc_ids(&log);
        assert_eq!(
            socs[socs_before],
            slot(entry.feed_index + 1),
            "the resumed run writes after the head"
        );
        assert_eq!(
            report.feed_index,
            Some(entry.feed_index + 1 + report.feed_updates)
        );
        // One head read (the index), no head playlist fetched.
        let reads: Vec<String> = lock(&log)
            .seen
            .iter()
            .filter(|r| r.method_path.starts_with("GET "))
            .skip(reads_before)
            .map(|r| r.method_path.clone())
            .collect();
        assert_eq!(reads.len(), 1, "{reads:?}");
        assert!(reads[0].contains("/feeds/") && reads[0].ends_with("?exact=true"));

        let after = &uploaded_playlists(&log)[before.len()..];
        assert!(
            after.iter().all(|p| playlist_broadcast(p) == id),
            "{after:?}"
        );
        let first_live = &after[0];
        assert!(!first_live.contains("#EXT-X-ENDLIST"));
        assert!(
            first_live.contains("\n#EXT-X-DISCONTINUITY\n#EXT-X-MAP:"),
            "{first_live}"
        );
        let recording = after.last().unwrap();
        assert!(
            recording.contains("#EXT-X-PLAYLIST-TYPE:VOD"),
            "{recording}"
        );
        assert_eq!(media_sequence(recording), 0, "{recording}");
        assert_eq!(entry_uris(recording).len(), 5, "{recording}");
        let old: Vec<&str> = entry_uris(&before[before.len() - 1]);
        assert_eq!(&entry_uris(recording)[..3], &old[..], "{recording}");
        assert_eq!(
            recording.matches("#EXT-X-DISCONTINUITY\n").count(),
            1,
            "{recording}"
        );
        assert_eq!(recording.matches("#EXT-X-MAP:").count(), 2, "{recording}");
        // Dates never move back across the kill.
        let dates: Vec<&str> = recording
            .lines()
            .filter_map(|l| l.strip_prefix("#EXT-X-PROGRAM-DATE-TIME:"))
            .collect();
        assert!(dates.windows(2).all(|w| w[0] <= w[1]), "{recording}");
        assert!(
            !std::path::Path::new(&path).exists(),
            "the recording is the head: nothing left to resume"
        );
    }

    /// The kill came with the update the entry was written ahead of
    /// still on the wire, and it never landed: the head is the one
    /// before it, so the resumed run writes that index, leaving no hole
    /// a feed reader would stop at.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_resume_writes_the_update_that_never_landed() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("unlanded");
        let (entry, _) = killed_broadcast(&log, &addr, &path).await;
        lock(&log).head = entry.feed_index.checked_sub(1).map(head_at);
        let socs_before = soc_ids(&log).len();
        let run = resumed_run(&addr, &path).unwrap();
        let _ = run.push(SegmentKind::Init, vec![9u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![0x20; 1000], 2000, false);
        run.cancel();
        settle(&run).await;
        assert_eq!(run.report().error_count, 0, "{:?}", run.report().errors);
        assert_eq!(soc_ids(&log)[socs_before], slot(entry.feed_index));
    }

    /// The head moved past the entry (another broadcast went out on the
    /// channel since): resuming would overwrite it, so the run is
    /// refused, ends by itself and writes nothing, and the entry stays.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_resume_over_a_channel_that_moved_on_is_refused() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("moved-on");
        let (entry, before) = killed_broadcast(&log, &addr, &path).await;
        let saved = std::fs::read(&path).unwrap();
        lock(&log).head = Some(head_at(entry.feed_index + 3));
        let socs_before = soc_ids(&log).len();
        let run = resumed_run(&addr, &path).unwrap();
        let _ = run.push(SegmentKind::Init, vec![9u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![0x20; 1000], 2000, false);
        // No stop call: it ends on its own.
        settle(&run).await;
        let report = run.report();
        assert!(
            report.errors.iter().any(|e| e.contains("moved on")),
            "{report:?}"
        );
        assert_eq!(report.feed_index, None);
        assert_eq!(soc_ids(&log).len(), socs_before);
        assert_eq!(uploaded_playlists(&log).len(), before.len());
        assert_eq!(std::fs::read(&path).unwrap(), saved);
    }

    /// Refused at start, entry untouched: another account's broadcast,
    /// another topic, another format, no entry, an entry that won't
    /// parse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_resume_is_refused_at_start_unless_the_entry_is_this_feeds() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("refused");
        let _ = killed_broadcast(&log, &addr, &path).await;
        let saved = std::fs::read(&path).unwrap();
        let mut c = config();
        c.gateway = format!("http://{addr}");
        c.format = StreamFormat::Bzz;
        c.recovery_path = path.clone();
        c.resume = true;
        let handle = tokio::runtime::Handle::current();
        let refused =
            |c: PublisherConfig, owner: [u8; 20]| match start(&handle, c, TEST_SECRET, owner, None)
            {
                Err(PublisherError::Config(message)) => message,
                Ok(_) => panic!("resumed"),
            };
        assert!(refused(c.clone(), [9; 20]).contains("not this account"));
        let mut other = c.clone();
        other.topic = format!("0x{}", "22".repeat(32));
        assert!(refused(other, test_owner()).contains("topic"));
        let mut other = c.clone();
        other.format = StreamFormat::SolarPunk;
        assert!(refused(other, test_owner()).contains("format"));
        assert_eq!(std::fs::read(&path).unwrap(), saved);

        let mut missing = c.clone();
        missing.recovery_path = format!("{path}.missing");
        assert!(refused(missing, test_owner()).contains("does not exist"));
        std::fs::write(&path, b"{\"version\":1,").unwrap();
        assert!(refused(c, test_owner()).contains("not a recovery entry"));
        assert_eq!(std::fs::read(&path).unwrap(), b"{\"version\":1,");
    }

    /// Finalizing a killed broadcast: resume and stop straight away. The
    /// closing window and the recording go up with every saved segment.
    /// If the recording fails, the entry stays for another go; once it
    /// lands, the entry is gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_killed_broadcast_finalizes_into_its_recording() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("finalize");
        let (entry, before) = killed_broadcast(&log, &addr, &path).await;
        lock(&log).head = Some(head_at(entry.feed_index));
        lock(&log).fail_recording = true;

        let run = resumed_run(&addr, &path).unwrap();
        run.cancel();
        settle(&run).await;
        assert_eq!(run.report().recording_reference, "");
        let kept = load_recovery(&path).expect("kept for another go");
        // The closing window, saved ahead of its update right after the
        // head; the recording's upload failed, so nothing was saved for
        // it.
        assert_eq!(kept.feed_index, entry.feed_index + 1);
        assert_eq!(kept.recording, entry.recording);
        let closing = uploaded_playlists(&log)[before.len()].clone();
        assert!(closing.contains("#EXT-X-ENDLIST"), "{closing}");
        assert!(!closing.contains("#EXT-X-PLAYLIST-TYPE:VOD"), "{closing}");

        // Again, with the recording landing this time (the closing
        // window is the head now).
        lock(&log).fail_recording = false;
        lock(&log).head = Some(head_at(kept.feed_index));
        let run = resumed_run(&addr, &path).unwrap();
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        let recording = uploaded_playlists(&log).last().unwrap().clone();
        assert!(
            recording.contains("#EXT-X-PLAYLIST-TYPE:VOD"),
            "{recording}"
        );
        assert_eq!(
            entry_uris(&recording),
            entry_uris(&before[before.len() - 1]),
            "{recording}"
        );
        // The closing window again, then the recording.
        assert_eq!(soc_ids(&log).last().unwrap(), &slot(kept.feed_index + 2));
        assert!(!std::path::Path::new(&path).exists());
    }

    /// Finalizing right after a relaunch: the stop lands before the first
    /// head read answers, and the node answers it with a 502 and its
    /// cold-peer 503. The run keeps reading instead of giving up at the
    /// first failure, and the recording still goes up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn finalizing_retries_a_head_that_did_not_answer() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("finalize-cold");
        let (entry, _) = killed_broadcast(&log, &addr, &path).await;
        lock(&log).head = Some(head_at(entry.feed_index));
        lock(&log).fail_head = 1;
        lock(&log).cold_head = 1;

        let run = resumed_run(&addr, &path).unwrap();
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        assert!(
            report.errors.iter().any(|e| e.contains("retrying")),
            "{report:?}"
        );
        assert!(!std::path::Path::new(&path).exists());
    }

    /// R2-M1: a finalizing run's head budget runs from the stop, not from
    /// the first read that failed after it. A read hanging on a wedged
    /// uplink when the stop lands is dropped once the budget has passed
    /// since the stop, instead of being waited out under its own
    /// `PUBLISH_TIMEOUT` and only then starting the budget.
    #[tokio::test(start_paused = true)]
    async fn a_resumed_head_read_gives_up_a_budget_after_the_stop() {
        let cancel = Arc::new(AtomicBool::new(false));
        let stop_after = Duration::from_secs(10);
        tokio::spawn({
            let cancel = Arc::clone(&cancel);
            async move {
                tokio::time::sleep(stop_after).await;
                cancel.store(true, Ordering::SeqCst);
            }
        });
        let began = tokio::time::Instant::now();
        let mut since = None;
        let out = read_within_stop_budget(
            std::future::pending::<()>(),
            || cancel.load(Ordering::SeqCst),
            &mut since,
        )
        .await;
        assert!(out.is_none());
        let took = began.elapsed();
        assert!(took >= stop_after + RESUME_HEAD_STOP_BUDGET, "{took:?}");
        assert!(
            took <= stop_after + RESUME_HEAD_STOP_BUDGET + 2 * STOP_POLL,
            "{took:?}"
        );

        // A later read shares the spent budget: it gives up at once.
        let again = tokio::time::Instant::now();
        let out = read_within_stop_budget(
            std::future::pending::<()>(),
            || cancel.load(Ordering::SeqCst),
            &mut since,
        )
        .await;
        assert!(out.is_none());
        assert_eq!(again.elapsed(), Duration::ZERO);

        // A read that answers inside the budget is returned.
        let mut since = None;
        let out = read_within_stop_budget(
            async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                7
            },
            || true,
            &mut since,
        )
        .await;
        assert_eq!(out, Some(7));
    }

    /// Rewrite the entry at `path` as one written ahead of a `shape`
    /// update.
    fn reshape_entry(path: &str, shape: PlaylistShape) -> RecoveryEntry {
        let mut entry = load_recovery(path).unwrap();
        entry.shape = shape;
        save_recovery(path, &entry).unwrap();
        entry
    }

    /// An entry written ahead of the closing window: a stop had begun,
    /// and a live viewer may already have stopped at its
    /// `#EXT-X-ENDLIST`. Resuming it only finalizes: the run starts
    /// stopped, takes no segments, writes no live window, and ends with
    /// the closing window and the recording.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_entry_a_stop_began_closing_is_only_finalized() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("ended");
        let (_, before) = killed_broadcast(&log, &addr, &path).await;
        let entry = reshape_entry(&path, PlaylistShape::Closing);
        assert!(entry.ended());
        // The closing window landed before the kill.
        lock(&log).head = Some(head_at(entry.feed_index));

        let run = resumed_run(&addr, &path).unwrap();
        assert_eq!(
            run.push(SegmentKind::Media, vec![9; 1000], 2000, false),
            PushOutcome::Closed
        );
        settle(&run).await;
        let report = run.report();
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        let after = &uploaded_playlists(&log)[before.len()..];
        assert_eq!(after.len(), 2, "{after:?}");
        assert!(after[0].contains("#EXT-X-ENDLIST"), "{}", after[0]);
        assert!(
            after[1].contains("#EXT-X-PLAYLIST-TYPE:VOD"),
            "{}",
            after[1]
        );
        assert!(!std::path::Path::new(&path).exists());
    }

    /// An entry written ahead of the recording whose update landed (only
    /// its removal failed): the broadcast is finished, so the run writes
    /// nothing and clears the entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_entry_whose_recording_landed_is_only_cleared() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("recorded");
        let (_, before) = killed_broadcast(&log, &addr, &path).await;
        let entry = reshape_entry(&path, PlaylistShape::Recording);
        lock(&log).head = Some(head_at(entry.feed_index));
        let socs = soc_ids(&log).len();

        let run = resumed_run(&addr, &path).unwrap();
        settle(&run).await;
        assert_eq!(uploaded_playlists(&log).len(), before.len());
        assert_eq!(soc_ids(&log).len(), socs, "no feed update");
        assert!(!std::path::Path::new(&path).exists());

        // Its update did not land: the head is the closing window before
        // it, so the run finalizes again.
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("recorded-unlanded");
        let _ = killed_broadcast(&log, &addr, &path).await;
        let entry = reshape_entry(&path, PlaylistShape::Recording);
        // (No head at all if the first update listed every segment.)
        lock(&log).head = entry.feed_index.checked_sub(1).map(head_at);
        let run = resumed_run(&addr, &path).unwrap();
        settle(&run).await;
        assert_ne!(run.report().recording_reference, "");
        assert_eq!(soc_ids(&log).last().unwrap(), &slot(entry.feed_index + 1));
        assert!(!std::path::Path::new(&path).exists());
    }

    // -----------------------------------------------------------------
    // The account's catalog feed (issue #220)
    // -----------------------------------------------------------------

    fn catalog_run(addr: &str, info: Option<StreamInfo>) -> Arc<LiveRun> {
        let mut config = config();
        config.gateway = format!("http://{addr}");
        config.topic = String::new();
        config.topic_text = "antstream".into();
        config.start_index = Some(0);
        config.width = 480;
        config.height = 640;
        config.info = info;
        start(
            &tokio::runtime::Handle::current(),
            config,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap()
    }

    fn morning_walk() -> StreamInfo {
        StreamInfo {
            title: "Morning walk".into(),
            description: "Along the river.".into(),
            thumbnail: "cd".repeat(32),
            scheduled_start_ms: None,
        }
    }

    /// The catalog payloads uploaded, in order, each parsed: a
    /// `POST /bytes` of a JSON array.
    fn catalog_payloads(log: &Arc<Mutex<StubLog>>) -> Vec<serde_json::Value> {
        lock(log)
            .seen
            .iter()
            .filter(|r| r.method_path.ends_with("/bytes") && r.body.starts_with(b"[{"))
            .map(|r| serde_json::from_slice(&r.body).unwrap())
            .collect()
    }

    /// The catalog feed updates written, by index: `POST /soc` at the
    /// catalog topic's sequence ids.
    fn catalog_updates(log: &Arc<Mutex<StubLog>>) -> Vec<u64> {
        let topic = crate::catalog::catalog_topic();
        let ids: Vec<(u64, String)> = (0..16)
            .map(|i| (i, hex::encode(ant_retrieval::sequence_update_id(&topic, i))))
            .collect();
        lock(log)
            .seen
            .iter()
            .filter(|r| r.method_path.starts_with("POST /soc/"))
            .filter_map(|r| {
                ids.iter()
                    .find(|(_, id)| r.method_path.contains(id.as_str()))
                    .map(|(i, _)| *i)
            })
            .collect()
    }

    async fn wait_for(run: &Arc<LiveRun>, what: &str, done: impl Fn(&PublisherSnapshot) -> bool) {
        for _ in 0..400 {
            if done(&run.snapshot()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("never got {what}: {:?}", run.snapshot());
    }

    /// A broadcast with info lists the channel `live` once its first feed
    /// update landed, rewrites the entry at once when the info changes,
    /// and closes it `vod` after the recording, naming the recording's
    /// index and duration, at consecutive catalog indices from an empty
    /// catalog feed's 0.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broadcast_with_info_lists_its_channel_live_then_vod() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = catalog_run(&addr, Some(morning_walk()));
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        wait_for(&run, "the live entry", |s| s.catalog_writes == 1).await;
        let live = run.snapshot();
        assert_eq!(live.catalog_state, "live", "{live:?}");
        assert_eq!(live.catalog_index, Some(0), "{live:?}");

        run.set_info(StreamInfo {
            title: "Evening walk".into(),
            ..morning_walk()
        })
        .unwrap();
        wait_for(&run, "the edited entry", |s| s.catalog_writes == 2).await;
        let _ = run.push(SegmentKind::Media, vec![2u8; 4096], 1500, false);
        tokio::time::sleep(Duration::from_millis(100)).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.catalog_writes, 3, "{report:?}");
        assert_eq!(report.catalog_state, "vod", "{report:?}");
        assert_eq!(report.catalog_index, Some(2), "{report:?}");
        assert_eq!(report.catalog_failures, 0, "{report:?}");
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        assert_eq!(catalog_updates(&log), vec![0, 1, 2]);

        let payloads = catalog_payloads(&log);
        assert_eq!(payloads.len(), 3, "{payloads:?}");
        let entries: Vec<&serde_json::Value> = payloads
            .iter()
            .map(|p| {
                assert_eq!(p.as_array().unwrap().len(), 1, "{p}");
                &p[0]
            })
            .collect();
        let owner = hex::encode(test_owner());
        for entry in &entries {
            assert_eq!(entry["owner"], owner.as_str());
            assert_eq!(entry["topic"], "antstream");
            assert_eq!(entry["mediatype"], "video");
            assert_eq!(entry["description"], "Along the river.");
            assert_eq!(entry["thumbnail"], "cd".repeat(32).as_str());
            assert!(entry["timestamp"].as_u64().unwrap() > 1_700_000_000_000);
            assert_eq!(entry["renditions"][0]["width"], 480);
            assert_eq!(entry["renditions"][0]["height"], 640);
            assert_eq!(entry["renditions"][0]["topic"], "antstream");
        }
        assert_eq!(entries[0]["state"], "live");
        assert_eq!(entries[0]["title"], "Morning walk");
        assert_eq!(entries[0]["index"], 0);
        assert_eq!(entries[1]["state"], "live");
        assert_eq!(entries[1]["title"], "Evening walk");
        assert_eq!(entries[2]["state"], "vod");
        assert_eq!(entries[2]["title"], "Evening walk");
        // The recording is the newest feed update, and holds 3.5 s.
        let recording_index = report.feed_index.unwrap() - 1;
        assert_eq!(entries[2]["index"], recording_index);
        assert_eq!(entries[2]["duration"], 3.5);
        assert_eq!(entries[2]["renditions"][0]["index"], recording_index);
        assert_eq!(entries[2]["renditions"][0]["duration"], 3.5);
    }

    /// A catalog feed that already holds entries is continued after its
    /// head, never written from 0 again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_catalog_continues_after_its_head() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        lock(&log).feed_heads.insert(
            hex::encode(crate::catalog::catalog_topic()),
            StubHead {
                index: 6,
                version: "v2",
                reference: "ab".repeat(32),
                body: b"[]".to_vec(),
            },
        );
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = catalog_run(&addr, Some(morning_walk()));
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        tokio::time::sleep(Duration::from_millis(100)).await;
        run.cancel();
        settle(&run).await;
        assert_eq!(catalog_updates(&log), vec![7, 8]);
        assert_eq!(run.report().catalog_index, Some(8));
    }

    /// No info, no catalog: the run writes nothing to the catalog feed
    /// until `set_info` gives it some.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_without_info_writes_no_catalog_until_it_gets_some() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = catalog_run(&addr, None);
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        wait_for(&run, "a feed update", |s| s.playlists_published > 0).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(catalog_payloads(&log), Vec::<serde_json::Value>::new());
        assert_eq!(run.snapshot().catalog_error, "");
        run.set_info(morning_walk()).unwrap();
        wait_for(&run, "the live entry", |s| s.catalog_writes == 1).await;
        assert!(run
            .set_info(StreamInfo {
                thumbnail: "not a reference".into(),
                ..StreamInfo::default()
            })
            .is_err());
        run.cancel();
        settle(&run).await;
        assert_eq!(run.report().catalog_state, "vod");
    }

    /// A failing catalog write is the catalog's: counted in
    /// `catalog_failures`, never in the broadcast's `errors`, and the
    /// broadcast keeps up regardless.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failing_catalog_write_stays_out_of_the_broadcast_errors() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        lock(&log).fail_catalog = true;
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = catalog_run(&addr, Some(morning_walk()));
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        wait_for(&run, "a failed live entry", |s| s.catalog_failures == 1).await;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.catalog_writes, 0, "{report:?}");
        assert_eq!(report.catalog_failures, 2, "{report:?}");
        assert!(
            report.catalog_error.starts_with("catalog entry (vod)"),
            "{report:?}"
        );
        assert_eq!(report.error_count, 0, "{report:?}");
        assert!(report.errors.is_empty(), "{report:?}");
        assert!(report.kept_up, "{report:?}");
        assert_eq!(catalog_updates(&log), Vec::<u64>::new());
    }

    /// A `vod` entry that does not land after the recording did keeps the
    /// recovery entry: the catalog still says `live`, and a relaunch that
    /// ends the broadcast finds the recording at the head and writes only
    /// that entry, naming the recording, then removes the recovery entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_vod_entry_is_written_by_the_relaunch_that_ends_it() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("failed-vod");
        let mut c = config();
        c.gateway = format!("http://{addr}");
        c.topic = String::new();
        c.topic_text = "antstream".into();
        c.width = 480;
        c.height = 640;
        c.info = Some(morning_walk());
        c.recovery_path = path.clone();
        let run = start(
            &tokio::runtime::Handle::current(),
            c.clone(),
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        wait_for(&run, "the live entry", |s| s.catalog_writes == 1).await;
        lock(&log).fail_catalog = true;
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert!(!report.recording_reference.is_empty(), "{report:?}");
        assert_eq!(report.catalog_state, "live", "{report:?}");
        assert!(
            report.catalog_error.starts_with("catalog entry (vod)"),
            "{report:?}"
        );
        let recording = run.snapshot().feed_index.unwrap() - 1;
        let kept = load_recovery(&path).expect("kept until the vod entry lands");
        assert_eq!(kept.shape, PlaylistShape::Recording);
        assert_eq!(kept.feed_index, recording);
        assert!(kept.recorded, "marked: only the catalog is left open");

        // The relaunch ends it: the head is the recording.
        {
            let mut log = lock(&log);
            log.fail_catalog = false;
            log.head = Some(head_at(recording));
            log.feed_heads
                .insert(hex::encode(crate::catalog::catalog_topic()), head_at(0));
        }
        let before = lock(&log).seen.len();
        c.info = None;
        c.resume = true;
        let ended = start(
            &tokio::runtime::Handle::current(),
            c,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        ended.cancel();
        settle(&ended).await;
        let report = ended.report();
        assert_eq!(report.catalog_state, "vod", "{report:?}");
        assert_eq!(report.catalog_index, Some(1), "{report:?}");
        assert_eq!(report.feed_updates, 0, "nothing on the channel's feed");
        assert_eq!(ended.snapshot().feed_index, Some(recording + 1));
        let vod = catalog_payloads(&log).pop().unwrap();
        assert_eq!(vod[0]["state"], "vod", "{vod}");
        assert_eq!(vod[0]["index"], recording, "{vod}");
        assert_eq!(vod[0]["title"], "Morning walk", "{vod}");
        assert!(
            !lock(&log).seen[before..]
                .iter()
                .any(|r| r.body.windows(7).any(|w| w == b"#EXTM3U")),
            "no playlist uploaded again"
        );
        assert!(!std::path::Path::new(&path).exists(), "removed once closed");
    }

    /// A recovery entry whose `vod` entry did land (only its removal
    /// failed, or the app went before it) is ended without a second
    /// `vod` entry: the catalog's head already is it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_landed_vod_entry_is_not_written_twice() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("landed-vod");
        let mut c = config();
        c.gateway = format!("http://{addr}");
        c.topic = String::new();
        c.topic_text = "antstream".into();
        c.width = 480;
        c.height = 640;
        c.info = Some(morning_walk());
        c.recovery_path = path.clone();
        let run = start(
            &tokio::runtime::Handle::current(),
            c.clone(),
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        wait_for(&run, "the live entry", |s| s.catalog_writes == 1).await;
        // The `vod` entry fails here only to keep the recovery entry, as a
        // failed removal would; its payload is what then "landed".
        lock(&log).fail_catalog = true;
        run.cancel();
        settle(&run).await;
        let recording = run.snapshot().feed_index.unwrap() - 1;
        let vod = catalog_payloads(&log).pop().unwrap();
        assert_eq!(vod[0]["state"], "vod", "{vod}");
        assert!(load_recovery(&path).is_ok());
        let catalog_hex = hex::encode(crate::catalog::catalog_topic());
        let written = catalog_updates(&log).len();
        let uploads = catalog_payloads(&log).len();
        c.info = None;
        c.resume = true;

        {
            let mut log = lock(&log);
            log.fail_catalog = false;
            log.head = Some(head_at(recording));
            log.feed_heads.insert(
                catalog_hex,
                StubHead {
                    index: 1,
                    version: "v2",
                    reference: "ab".repeat(32),
                    body: serde_json::to_vec(&vod).unwrap(),
                },
            );
        }
        let ended = start(
            &tokio::runtime::Handle::current(),
            c,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        ended.cancel();
        settle(&ended).await;
        assert_eq!(catalog_updates(&log).len(), written, "no second vod entry");
        assert_eq!(catalog_payloads(&log).len(), uploads, "nothing uploaded");
        assert!(!std::path::Path::new(&path).exists(), "removed: closed");
    }

    /// A channel no catalog entry can name (a raw topic, or `"bzz"`)
    /// writes none, and says why: with info at start, and with info only
    /// from a later `set_info`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_channel_a_catalog_cant_name_writes_none() {
        for (raw, info_at_start) in [(true, true), (false, true), (true, false), (false, false)] {
            let log = Arc::new(Mutex::new(StubLog::default()));
            let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
            let mut config = config();
            config.gateway = format!("http://{addr}");
            config.start_index = Some(0);
            config.info = info_at_start.then(morning_walk);
            if !raw {
                config.topic = String::new();
                config.topic_text = "antstream".into();
                config.format = StreamFormat::Bzz;
            }
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
            if !info_at_start {
                // Past the start's own check, which only sees info given
                // at start.
                wait_for(&run, "a feed update", |s| s.playlists_published > 0).await;
                assert_eq!(run.snapshot().catalog_error, "");
                run.set_info(morning_walk()).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            run.cancel();
            settle(&run).await;
            let report = run.report();
            assert!(
                report.catalog_error.starts_with("no catalog entry"),
                "raw {raw}, info at start {info_at_start}: {report:?}"
            );
            assert_eq!(report.catalog_writes, 0, "{report:?}");
            assert_eq!(catalog_payloads(&log), Vec::<serde_json::Value>::new());
        }
    }

    /// A killed broadcast's `live` entry is closed with a `vod` one when
    /// the relaunch only ends it, even with no info in the resumed run's
    /// config: the recovery entry kept the info and the rung's size.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ending_a_killed_broadcast_closes_its_catalog_entry() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let path = recovery_path("catalog");
        let solar_punk = |resume: bool| {
            let mut c = config();
            c.gateway = format!("http://{addr}");
            c.topic = String::new();
            c.topic_text = "antstream".into();
            c.max_backlog = 16;
            c.recovery_path.clone_from(&path);
            c.resume = resume;
            c
        };
        let mut first = solar_punk(false);
        first.start_index = Some(0);
        first.width = 480;
        first.height = 640;
        first.info = Some(morning_walk());
        let run = start(
            &tokio::runtime::Handle::current(),
            first,
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        for i in 0..3 {
            let _ = run.push(SegmentKind::Media, vec![i; 1000], 2000, false);
        }
        wait_for(&run, "the live entry", |s| {
            s.catalog_writes == 1 && s.segments_listed == 3
        })
        .await;
        for _ in 0..400 {
            if !run.feed_update_in_flight()
                && run.snapshot().feed_index == Some(run.report().feed_updates)
                && load_recovery(&path).is_ok_and(|e| e.recording.len() == 3)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let entry = load_recovery(&path).unwrap();
        assert_eq!(entry.info, Some(morning_walk()));
        assert_eq!((entry.width, entry.height), (480, 640));
        // "Killed": never stopped. The relaunch ends it, with no info.
        {
            let mut log = lock(&log);
            log.head = Some(head_at(entry.feed_index));
            log.feed_heads.insert(
                hex::encode(crate::catalog::catalog_topic()),
                StubHead {
                    index: 0,
                    version: "v2",
                    reference: "ab".repeat(32),
                    body: b"[]".to_vec(),
                },
            );
        }
        let resumed = start(
            &tokio::runtime::Handle::current(),
            solar_punk(true),
            TEST_SECRET,
            test_owner(),
            None,
        )
        .unwrap();
        resumed.cancel();
        settle(&resumed).await;
        let report = resumed.report();
        assert_eq!(report.catalog_state, "vod", "{report:?}");
        assert_eq!(report.catalog_index, Some(1), "{report:?}");
        assert_eq!(report.catalog_failures, 0, "{report:?}");
        let closing = catalog_payloads(&log).pop().unwrap();
        assert_eq!(closing[0]["state"], "vod");
        assert_eq!(closing[0]["title"], "Morning walk");
        assert_eq!(closing[0]["renditions"][0]["width"], 480);
        assert_eq!(closing[0]["renditions"][0]["height"], 640);
    }

    /// Once the stop returned and sealed the feed, the closing entry is
    /// skipped: the next broadcast writes the catalog from the index the
    /// sealed run would have taken.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_sealed_run_writes_no_closing_entry() {
        let log = Arc::new(Mutex::new(StubLog::default()));
        let addr = stub_gateway(Arc::clone(&log), Duration::from_millis(2)).await;
        let run = catalog_run(&addr, Some(morning_walk()));
        let _ = run.push(SegmentKind::Init, vec![7u8; 800], 0, false);
        let _ = run.push(SegmentKind::Media, vec![1u8; 4096], 2000, false);
        wait_for(&run, "the live entry", |s| s.catalog_writes == 1).await;
        let _ = run.seal_feed();
        run.cancel();
        settle(&run).await;
        let report = run.report();
        assert_eq!(report.catalog_writes, 1, "{report:?}");
        assert_eq!(report.catalog_state, "live", "{report:?}");
        assert!(report.catalog_error.contains("skipped"), "{report:?}");
        assert_eq!(catalog_updates(&log), vec![0]);
    }

    /// A continued broadcast's recording duration counts the entries it
    /// glued in front, gaps left out, as Solar Punk's `getTotalDuration`.
    #[test]
    fn a_predecessors_media_leaves_its_gaps_out() {
        let prior = Predecessor::parse(
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:2\n#EXT-X-PLAYLIST-TYPE:VOD\n\
             #EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-MAP:URI=\"i\"\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-10-17T08:00:00.000Z\n#EXTINF:2.000,\na\n\
             #EXT-X-GAP\n#EXT-X-PROGRAM-DATE-TIME:2026-10-17T08:00:02.000Z\n#EXTINF:2.000,\ngap-1\n\
             #EXT-X-PROGRAM-DATE-TIME:2026-10-17T08:00:04.000Z\n#EXTINF:1.500,\nb\n#EXT-X-ENDLIST\n",
        )
        .unwrap();
        assert_eq!(prior.media_ms(), 3500);
    }
}
