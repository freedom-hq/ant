//! Stream info and the account's Solar Punk catalog feed (issue #220).
//!
//! A broadcaster gives a channel a title, a description, a thumbnail and
//! optionally a scheduled start ([`StreamInfo`]). It goes out as one
//! **catalog entry** ([`CatalogEntry`]) in the shape Solar Punk's viewers
//! read (`viewerCatalogEntrySchema` in Solar-Punk-Ltd/stream-event-client
//! `src/shared/contracts/catalog.ts`), on a feed each account owns: topic
//! text [`CATALOG_TOPIC_TEXT`], owner the account. Every update of that
//! feed is a v2 update whose payload is the whole catalog, a JSON array,
//! here the one entry of the account's channel, as Solar Punk's
//! `StreamCatalog` writes it (bee-js `FeedWriter.uploadPayload`).
//!
//! The field encodings are the ones Solar Punk's writers use: `owner` as
//! 40 lowercase hex digits without `0x` (bee-js `address().toHex()`),
//! `topic` the channel's topic text, `timestamp` Unix milliseconds,
//! `duration` seconds as a number, `scheduledStartTime` an ISO 8601 UTC
//! string (the admin's `toISOString()`), `state` one of `"scheduled"`,
//! `"live"`, `"vod"` (a finished broadcast is `vod` there, the state their
//! player plays from the start), `thumbnail` a reference their player
//! opens as `/bzz/<ref>/` (the admin uploads it with bee-js
//! `file.upload`, a one-file manifest; so does
//! [`upload_thumbnail`]).

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::publisher::{
    feed_next_index_at, iso8601_utc_ms, publish_bzz, publish_soc_as, topic_from_string,
    upload_v2_payload_to, Target, UploadError,
};

/// Topic text of every account's catalog feed. Solar Punk's player lists
/// an account's channel when its config's catalog is
/// `{ owner: <account>, topic: "antstream-catalog" }`.
pub const CATALOG_TOPIC_TEXT: &str = "antstream-catalog";

/// How often a live broadcast rewrites its catalog entry at most, and
/// only when something in it changed (`index`, `duration`, the measured
/// bandwidth). Info the broadcaster edits goes out at once.
pub const CATALOG_REFRESH: Duration = Duration::from_mins(1);

/// What `mediatype` the apps' channels carry: they broadcast video.
const MEDIATYPE: &str = "video";

/// The broadcaster's stream info: the publisher config's `info` object,
/// [`crate::ant_publisher_set_info`]'s argument and part of
/// [`crate::ant_stream_catalog_publish`]'s.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct StreamInfo {
    /// The stream's title. Empty (or only whitespace) is the channel's
    /// name: a catalog entry always has one.
    #[serde(default)]
    pub title: String,
    /// Free text, optional. Left out of the entry when empty.
    #[serde(default)]
    pub description: String,
    /// Reference of the thumbnail image, uploaded with
    /// [`crate::ant_stream_thumbnail_upload`] (64 hex digits, `0x`
    /// optional). Empty: no thumbnail.
    #[serde(default)]
    pub thumbnail: String,
    /// Announced start of an upcoming broadcast, Unix ms. Written as the
    /// entry's `scheduledStartTime` in every state, as Solar Punk's admin
    /// does; only a `scheduled` entry shows it as "Starts at …".
    #[serde(default)]
    pub scheduled_start_ms: Option<u64>,
}

impl StreamInfo {
    /// Refuse info no entry can carry: a thumbnail that is not a
    /// reference.
    pub fn validate(&self) -> Result<(), String> {
        let thumbnail = self.thumbnail.trim();
        if thumbnail.is_empty() {
            return Ok(());
        }
        let hex = thumbnail.strip_prefix("0x").unwrap_or(thumbnail);
        if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!(
                "info.thumbnail must be a 64-hex reference, got `{thumbnail}`"
            ));
        }
        Ok(())
    }

    /// The title the entry carries: the broadcaster's, else `channel`.
    #[must_use]
    pub fn title_or(&self, channel: &str) -> String {
        let title = self.title.trim();
        if title.is_empty() {
            channel.trim().to_string()
        } else {
            title.to_string()
        }
    }

    fn thumbnail_ref(&self) -> String {
        let thumbnail = self.thumbnail.trim();
        thumbnail
            .strip_prefix("0x")
            .unwrap_or(thumbnail)
            .to_ascii_lowercase()
    }
}

/// An entry's `state`, the values Solar Punk's writers use
/// (`CATALOG_STATES` in their contracts: `live`, `vod`, `scheduled`). A
/// broadcast that ended is `vod`: its recording is what a viewer plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogState {
    Scheduled,
    Live,
    Vod,
}

impl CatalogState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scheduled => "scheduled",
            Self::Live => "live",
            Self::Vod => "vod",
        }
    }
}

/// The channel's one rung (`viewerCatalogRungSchema`, Solar Punk's
/// `Rendition`): the size the broadcast is captured at and the bitrate
/// measured from its segments, `BANDWIDTH` and `AVERAGE-BANDWIDTH` of the
/// master playlist. A finished entry's rung carries the recording's
/// `index` and `duration` too, as their uploader's does: their player
/// offers a finished entry's rungs only when they have one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogRung {
    pub name: String,
    pub width: u32,
    pub height: u32,
    pub topic: String,
    pub bandwidth: u64,
    pub avg_bandwidth: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
}

/// One catalog entry, serialised field for field as Solar Punk's viewer
/// schema reads it; see the module docs for the encodings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEntry {
    pub owner: String,
    pub topic: String,
    pub title: String,
    /// Not in the viewer schema, which passes it through
    /// (`z.looseObject`); their stream cards and watch page show it.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    pub state: CatalogState,
    pub mediatype: String,
    pub timestamp: u64,
    /// The feed index of the newest playlist (live) or of the recording
    /// (`vod`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u64>,
    /// Seconds of media: what this broadcast listed so far (live), the
    /// whole recording (`vod`, earlier broadcasts it continues
    /// included, as Solar Punk's `getTotalDuration`). Gaps don't count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub thumbnail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_start_time: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renditions: Vec<CatalogRung>,
}

/// What an entry is built from, beyond the info.
#[derive(Debug, Clone)]
pub struct EntryFacts<'a> {
    pub owner: &'a [u8; 20],
    pub topic_text: &'a str,
    pub channel: &'a str,
    pub state: CatalogState,
    pub timestamp_ms: u64,
    pub index: Option<u64>,
    pub duration_ms: Option<u64>,
    /// Width, height, peak and average bits/s, when the rendition's size
    /// is known.
    pub rung: Option<(u32, u32, u64, u64)>,
}

/// Seconds from ms, to the millisecond, as a JSON number.
fn seconds(ms: u64) -> f64 {
    ms as f64 / 1000.0
}

impl CatalogEntry {
    #[must_use]
    pub fn build(info: &StreamInfo, facts: &EntryFacts<'_>) -> Self {
        let finished = facts.state == CatalogState::Vod;
        let duration = facts.duration_ms.map(seconds);
        let renditions = facts
            .rung
            .filter(|(w, h, _, _)| *w > 0 && *h > 0)
            .map(|(width, height, bandwidth, avg_bandwidth)| CatalogRung {
                name: format!("{}p", width.min(height)),
                width,
                height,
                topic: facts.topic_text.to_string(),
                bandwidth,
                avg_bandwidth,
                index: if finished { facts.index } else { None },
                duration: if finished { duration } else { None },
            })
            .into_iter()
            .collect();
        Self {
            owner: hex::encode(facts.owner),
            topic: facts.topic_text.to_string(),
            title: info.title_or(facts.channel),
            description: info.description.trim().to_string(),
            state: facts.state,
            mediatype: MEDIATYPE.to_string(),
            timestamp: facts.timestamp_ms,
            index: facts.index,
            duration,
            thumbnail: info.thumbnail_ref(),
            scheduled_start_time: info.scheduled_start_ms.map(iso8601_utc_ms),
            renditions,
        }
    }

    /// Whether `other` says the same as this entry apart from its
    /// `timestamp`, which only says when an entry was written: the test
    /// Solar Punk's admin makes (`carriesEntry`) before spending a slot.
    #[must_use]
    pub fn same_apart_from_timestamp(&self, other: &Self) -> bool {
        Self {
            timestamp: other.timestamp,
            ..self.clone()
        } == *other
    }

    /// The catalog feed's payload: the whole catalog, this one entry.
    #[must_use]
    pub fn catalog_json(&self) -> String {
        serde_json::to_string(&[self]).unwrap_or_else(|_| "[]".to_string())
    }
}

/// The catalog feed's topic bytes: [`topic_from_string`] of
/// [`CATALOG_TOPIC_TEXT`].
#[must_use]
pub fn catalog_topic() -> [u8; 32] {
    topic_from_string(CATALOG_TOPIC_TEXT)
}

/// Writes the account's catalog feed: the update after the head, read
/// once (`?exact=true`, like the channel's own feed) and counted on from
/// there, so an earlier entry is never overwritten and a failed read
/// writes nothing.
pub(crate) struct CatalogWriter {
    pub(crate) target: Target,
    pub(crate) batch: [u8; 32],
    pub(crate) signing_secret: [u8; 32],
    pub(crate) owner: [u8; 20],
    /// The index the next update takes, once the head was read.
    pub(crate) next_index: Option<u64>,
}

impl CatalogWriter {
    /// Write `entry` as the next update; returns the index it took.
    pub(crate) async fn write(&mut self, entry: &CatalogEntry) -> Result<u64, UploadError> {
        let topic = catalog_topic();
        let index = match self.next_index {
            Some(index) => index,
            None => feed_next_index_at(&self.target, &self.owner, &topic).await?,
        };
        let (_, root) =
            upload_v2_payload_to(&self.target, self.batch, &entry.catalog_json(), &[]).await?;
        publish_soc_as(
            &self.target,
            self.batch,
            &self.signing_secret,
            &self.owner,
            ant_retrieval::sequence_update_id(&topic, index),
            &root,
        )
        .await?;
        self.next_index = Some(index + 1);
        Ok(index)
    }
}

/// Upload a thumbnail image the way Solar Punk's admin does (bee-js
/// `file.upload`: `POST /bzz` of one file, `thumbnail.jpg`), so both
/// their player (`/bzz/<ref>/`) and ours can show it. Returns the
/// reference, 64 hex digits.
pub(crate) async fn upload_thumbnail(
    target: &Target,
    batch: [u8; 32],
    image: &[u8],
) -> Result<String, UploadError> {
    let content_type = if image.starts_with(&[0x89, b'P', b'N', b'G']) {
        "image/png"
    } else {
        "image/jpeg"
    };
    let name = if content_type == "image/png" {
        "thumbnail.png"
    } else {
        "thumbnail.jpg"
    };
    publish_bzz(target, batch, name, content_type, image).await
}

/// [`crate::ant_stream_catalog_publish`]'s argument: where to write and
/// what. `gateway`, `batch_id` and `channel` as in the publisher config;
/// `topic_text` the channel's topic text (the apps' `antstream`).
#[derive(Debug, Clone, Deserialize)]
pub struct CatalogPublishConfig {
    #[serde(default = "default_gateway")]
    pub gateway: String,
    pub batch_id: String,
    pub channel: String,
    pub topic_text: String,
    #[serde(default)]
    pub info: StreamInfo,
    /// Rendition size for the entry's rung, optional (as in the
    /// publisher config).
    #[serde(default)]
    pub width: u32,
    #[serde(default)]
    pub height: u32,
    /// The encoder's target, the rung's bandwidth before any is measured.
    #[serde(default)]
    pub bitrate_kbps: u32,
}

fn default_gateway() -> String {
    "http://127.0.0.1:1633".to_string()
}

/// What [`crate::ant_stream_catalog_publish`] returns.
#[derive(Debug, Clone, Serialize)]
pub struct CatalogPublished {
    /// The catalog feed index the entry took.
    pub index: u64,
    pub entry: CatalogEntry,
}

/// Announce an upcoming broadcast: write the channel's `scheduled` entry
/// to the account's catalog feed. Refused without a
/// `info.scheduled_start_ms`, or with a topic no catalog reader can name.
pub async fn publish_scheduled(
    config: &CatalogPublishConfig,
    signing_secret: [u8; 32],
    owner: [u8; 20],
    now_ms: u64,
) -> Result<CatalogPublished, String> {
    config.info.validate()?;
    if config.info.scheduled_start_ms.is_none() {
        return Err("info.scheduled_start_ms is required: it is what the entry announces".into());
    }
    if config.channel.trim().is_empty() {
        return Err("channel is required".into());
    }
    if !crate::publisher::fits_swarm_uri(&config.topic_text) {
        return Err(format!(
            "topic_text `{}` can't be named by a catalog entry (empty, or has a `/`)",
            config.topic_text
        ));
    }
    let target = Target::parse(&config.gateway)?;
    let batch =
        crate::publisher::parse_hex32(&config.batch_id).map_err(|e| format!("batch_id {e}"))?;
    let bandwidth = u64::from(config.bitrate_kbps) * 1000;
    let entry = CatalogEntry::build(
        &config.info,
        &EntryFacts {
            owner: &owner,
            topic_text: &config.topic_text,
            channel: &config.channel,
            state: CatalogState::Scheduled,
            timestamp_ms: now_ms,
            index: None,
            duration_ms: None,
            rung: Some((config.width, config.height, bandwidth, bandwidth)),
        },
    );
    let mut writer = CatalogWriter {
        target,
        batch,
        signing_secret,
        owner,
        next_index: None,
    };
    let index = writer
        .write(&entry)
        .await
        .map_err(|e| format!("catalog update: {e}"))?;
    Ok(CatalogPublished { index, entry })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner() -> [u8; 20] {
        let mut owner = [0u8; 20];
        hex::decode_to_slice("d5572300e441b77b72bdd318bbfa7b97a1f03096", &mut owner).unwrap();
        owner
    }

    fn info() -> StreamInfo {
        StreamInfo {
            title: "  Morning walk ".into(),
            description: "Along the river.".into(),
            thumbnail: format!("0x{}", "AB".repeat(32)),
            scheduled_start_ms: Some(1_792_224_000_000),
        }
    }

    fn facts(state: CatalogState) -> EntryFacts<'static> {
        static OWNER: std::sync::OnceLock<[u8; 20]> = std::sync::OnceLock::new();
        EntryFacts {
            owner: OWNER.get_or_init(owner),
            topic_text: "antstream",
            channel: "My channel",
            state,
            timestamp_ms: 1_792_224_100_000,
            index: Some(41),
            duration_ms: Some(125_500),
            rung: Some((480, 640, 1_050_000, 880_000)),
        }
    }

    fn fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/testdata/catalog/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path}: {e}"));
        serde_json::from_str(&raw).unwrap()
    }

    /// The JSON is the fixture `examples/antstream-player/test/catalog-schema.mjs`
    /// checks against Solar Punk's zod schema: a change here that the
    /// fixture doesn't follow fails, and a fixture change is re-checked
    /// there.
    #[test]
    fn a_live_entry_is_the_json_solar_punk_reads() {
        let entry = CatalogEntry::build(&info(), &facts(CatalogState::Live));
        let json: serde_json::Value = serde_json::from_str(&entry.catalog_json()).unwrap();
        assert_eq!(json, fixture("live"));
    }

    #[test]
    fn a_finished_entry_is_vod_and_its_rung_names_the_recording() {
        let entry = CatalogEntry::build(&info(), &facts(CatalogState::Vod));
        let json: serde_json::Value = serde_json::from_str(&entry.catalog_json()).unwrap();
        assert_eq!(json, fixture("vod"));
    }

    #[test]
    fn a_scheduled_entry_carries_its_start_and_no_index() {
        let mut facts = facts(CatalogState::Scheduled);
        facts.index = None;
        facts.duration_ms = None;
        let entry = CatalogEntry::build(&info(), &facts);
        let json: serde_json::Value = serde_json::from_str(&entry.catalog_json()).unwrap();
        assert_eq!(json, fixture("scheduled"));
    }

    #[test]
    fn an_empty_title_is_the_channel_and_empty_fields_are_left_out() {
        let entry = CatalogEntry::build(
            &StreamInfo::default(),
            &EntryFacts {
                rung: Some((0, 0, 1, 1)),
                ..facts(CatalogState::Live)
            },
        );
        let json: serde_json::Value = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["title"], "My channel");
        for absent in [
            "description",
            "thumbnail",
            "scheduledStartTime",
            "renditions",
        ] {
            assert!(json.get(absent).is_none(), "{absent} should be left out");
        }
        assert_eq!(json["owner"], "d5572300e441b77b72bdd318bbfa7b97a1f03096");
        assert_eq!(json["mediatype"], "video");
    }

    #[test]
    fn the_timestamp_alone_is_no_change() {
        let a = CatalogEntry::build(&info(), &facts(CatalogState::Live));
        let mut b = a.clone();
        b.timestamp += 60_000;
        assert!(a.same_apart_from_timestamp(&b));
        b.index = Some(42);
        assert!(!a.same_apart_from_timestamp(&b));
    }

    #[test]
    fn a_thumbnail_must_be_a_reference() {
        assert!(info().validate().is_ok());
        assert!(StreamInfo::default().validate().is_ok());
        let bad = StreamInfo {
            thumbnail: "not-a-ref".into(),
            ..StreamInfo::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn the_catalog_topic_is_the_hash_of_its_text() {
        assert_eq!(catalog_topic(), ant_crypto::keccak256(b"antstream-catalog"),);
    }
}
